import copy
import io
import json
import secrets
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from runpod_ci_broker.export import RunPodResultReader, collect_https, result_server
from runpod_ci_broker.fake import FakeProvider
from runpod_ci_broker.models import Task
from runpod_ci_broker.provider import ProviderError
from runpod_ci_broker.results import save_bundle, verify_saved


class BytesReader:
    def __init__(self, manifest, files):
        self.payloads = {
            "/v1/manifest": json.dumps(manifest).encode(),
            **{"/v1/files/" + k: v for k, v in files.items()},
        }
        self.calls = []

    def open(self, path, timeout):
        self.calls.append((path, timeout))
        return io.BytesIO(self.payloads[path])


@pytest.fixture
def bundle(task, candidates):
    return FakeProvider(candidates).collect("abc123", task)


def test_streamed_bundle_publish_and_reverify(tmp_path, task, bundle):
    manifest, files = bundle
    reader = BytesReader(manifest, files)
    dest = tmp_path / "saved"
    assert collect_https(task, "abc123", reader, dest, deadline=time.monotonic() + 10)
    assert verify_saved(dest, task, "abc123")
    assert all(0 < timeout <= 5 for _, timeout in reader.calls)
    assert not list(tmp_path.glob(".collect-*"))
    with pytest.raises(ValueError, match="exists"):
        collect_https(task, "abc123", reader, dest, deadline=time.monotonic() + 10)


@pytest.mark.parametrize(
    "failure",
    [
        "truncated",
        "corrupt",
        "oversized",
        "provenance",
        "file_set",
        "negative_size",
        "bool_size",
        "metrics",
    ],
)
def test_bad_bundle_never_publishes_complete_directory(tmp_path, task, bundle, failure):
    manifest, files = copy.deepcopy(bundle)
    if failure == "truncated":
        files["worker.log"] = b""
    if failure == "corrupt":
        files["worker.log"] = b"X" * len(files["worker.log"])
    if failure == "oversized":
        files["worker.log"] += b"excess"
    if failure == "provenance":
        manifest["source_sha"] = "f" * 40
    if failure == "file_set":
        manifest["files"]["../outside"] = manifest["files"]["worker.log"]
    if failure == "negative_size":
        manifest["files"]["worker.log"]["bytes"] = -1
    if failure == "bool_size":
        manifest["files"]["worker.log"]["bytes"] = True
    if failure == "metrics":
        manifest["metrics"]["macro_f1"] = float("nan")
    with pytest.raises(ProviderError):
        collect_https(
            task,
            "abc123",
            BytesReader(manifest, files),
            tmp_path / "saved",
            deadline=time.monotonic() + 10,
        )
    assert not (tmp_path / "saved").exists() and not list(tmp_path.glob(".collect-*"))


def test_deadline_during_stream_is_bounded_and_discards_staging(tmp_path, task, bundle):
    clock = [0]

    class SlowReader(BytesReader):
        def open(self, path, timeout):
            stream = super().open(path, timeout)

            class Slow(io.BytesIO):
                def read1(self, n):
                    clock[0] += 2
                    return super().read(n)

            return Slow(stream.getvalue())

    with pytest.raises(ProviderError, match="deadline"):
        collect_https(
            task,
            "abc123",
            SlowReader(*bundle),
            tmp_path / "saved",
            deadline=1,
            now=lambda: clock[0],
        )
    assert not (tmp_path / "saved").exists() and not list(tmp_path.glob(".collect-*"))


def test_actual_loopback_endpoint_auth_allowlist_and_collection(tmp_path, task, bundle, capsys):
    token = secrets.token_hex(32)
    server = result_server(task, "abc123", *bundle, token)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"

    class LoopbackReader:
        def open(self, path, timeout):
            return urlopen(
                Request(base + path, headers={"Authorization": "Bearer " + token}), timeout=timeout
            )

    try:
        with pytest.raises(HTTPError) as exc:
            urlopen(base + "/v1/manifest", timeout=2)
        assert exc.value.code == 401
        with pytest.raises(HTTPError) as exc:
            LoopbackReader().open("/v1/files/../../etc/passwd", 2)
        assert exc.value.code == 404
        with pytest.raises(HTTPError) as exc:
            LoopbackReader().open("/v1/manifest?token=" + token, 2)
        assert exc.value.code == 404
        assert collect_https(
            task, "abc123", LoopbackReader(), tmp_path / "saved", deadline=time.monotonic() + 10
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)
    out = capsys.readouterr()
    assert token not in out.out + out.err


@pytest.mark.parametrize("pod", ["../x", "abc.example.org", "abc/secret", "ABC", ""])
def test_result_host_cannot_be_injected(pod):
    with pytest.raises(ValueError):
        RunPodResultReader(pod, 8000, "a" * 64)


def test_fixed_https_bearer_header_and_no_redirect_handler(monkeypatch):
    seen = []

    class Opener:
        def open(self, request, timeout):
            seen.append(request)
            raise HTTPError(request.full_url, 302, "redirect", {}, None)

    def opener(handler):
        assert handler.redirect_request(None, None, None, None, None, None) is None
        return Opener()

    monkeypatch.setattr("runpod_ci_broker.export.build_opener", opener)
    with pytest.raises(HTTPError):
        RunPodResultReader("abc123", 8000, "a" * 64).open("/v1/manifest", 2)
    assert seen[0].full_url == "https://abc123-8000.proxy.runpod.net/v1/manifest"
    assert "a" * 64 not in seen[0].full_url


def test_resume_verification_rechecks_provenance_and_manifest_file_size(tmp_path, task, bundle):
    manifest, files = bundle
    save_bundle(task, "abc123", manifest, files, tmp_path)
    assert not verify_saved(tmp_path, task, "wrong-pod")
    manifest["source_sha"] = "f" * 40
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    assert not verify_saved(tmp_path, task, "abc123")


@pytest.mark.parametrize("paths", [["manifest.json"], ["a", "a/b"], ["metrics.json"] * 2])
def test_reserved_or_overlapping_result_paths_refused(task, paths):
    with pytest.raises(ValueError):
        Task.model_validate(
            {**task.model_dump(), "results": {**task.results.model_dump(), "required_files": paths}}
        )


def test_metric_failure_still_saves_diagnostics(tmp_path, task, candidates):
    task.results.metric_min = {"macro_f1": 0.99}
    bundle = FakeProvider(candidates).collect("abc123", task)
    assert not collect_https(
        task, "abc123", BytesReader(*bundle), tmp_path / "saved", deadline=time.monotonic() + 10
    )
    assert verify_saved(tmp_path / "saved", task, "abc123")
