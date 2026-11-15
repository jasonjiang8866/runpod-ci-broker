"""Bounded diagnostics against fake SSE and a real local streaming HTTP server."""

import io
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from runpod_ci_broker.live import LiveAPI
from runpod_ci_broker.startup import summarize_logs


def event(line, source="container"):
    return b"data: " + json.dumps({"source": source, "line": line}).encode() + b"\n\n"


def test_sse_projection_discards_raw_secrets_and_malformed_events():
    raw = (
        b"data: not-json\n\n"
        + event("SECRET", [])
        + event("SECRET", "untrusted")
        + event('{"error":"worker_refused","gpu_verified":false}')
        + event("pull access denied SECRET", "system")
        + event("Traceback (most recent call last): SECRET")
        + event("libcuda.so SECRET")
        + event("Address already in use SECRET")
        + b'data: {"source":"container","line":"SECRET"}'
    )
    result = summarize_logs(raw)
    assert result["observed_events"] == {"container": 4, "system": 1}
    assert len(result["signals"]) == 5
    assert "SECRET" not in json.dumps(result)
    assert result["root_cause"] == "unverified"


class Response(io.BytesIO):
    status = 200


@pytest.fixture
def api(monkeypatch):
    monkeypatch.setenv("RUNPOD_API_KEY", "SECRET")
    return LiveAPI()


def install(monkeypatch, response):
    calls = []

    class Opener:
        def open(self, request, timeout):
            calls.append((request, timeout))
            if isinstance(response, Exception):
                raise response
            return response

    monkeypatch.setattr("runpod_ci_broker.live.build_opener", lambda *args: Opener())
    return calls


def test_logs_fixed_endpoint_no_create_and_bounded_bytes(api, monkeypatch):
    response = Response(event("SECRET") * 5000)
    calls = install(monkeypatch, response)
    result = api.startup_logs("abc123", 3)
    assert len(calls) == 1
    request, timeout = calls[0]
    assert request.full_url == "https://api.runpod.io/v2/pods/abc123/logs?tail=100"
    assert request.get_method() == "GET" and timeout == 1
    assert result["status"] == "byte_limit" and response.closed
    assert "SECRET" not in json.dumps(result)


@pytest.mark.parametrize("status", [302, 401, 403, 404, 500])
def test_log_denial_never_retries_reads_or_exports_body(api, monkeypatch, status):
    class ForbiddenBody(io.BytesIO):
        def read(self, *args):
            pytest.fail("must not read error body")

    body = ForbiddenBody(b"SECRET")
    calls = install(monkeypatch, HTTPError("SECRET", status, "SECRET", {}, body))
    result = api.startup_logs("abc123", 3)
    assert len(calls) == 1 and body.closed
    assert result["http_status"] == status
    assert "SECRET" not in json.dumps(result)


@pytest.mark.parametrize(
    "pod,timeout", [("../x", 3), ("abc123", 0), ("abc123", 4), ("abc123", float("nan"))]
)
def test_log_bounds_checked_before_network(api, monkeypatch, pod, timeout):
    calls = install(monkeypatch, Response())
    with pytest.raises(ValueError):
        api.startup_logs(pod, timeout)
    assert not calls


def test_trickle_stream_is_stopped_by_deadline(api, monkeypatch):
    clock = [0.0]

    class Trickle(Response):
        def read1(self, size):
            clock[0] += 0.8
            return event("SECRET")

    response = Trickle()
    calls = install(monkeypatch, response)
    monkeypatch.setattr("runpod_ci_broker.live.time.monotonic", lambda: clock[0])
    result = api.startup_logs("abc123", 3)
    assert result["status"] == "deadline"
    assert clock[0] < 4 and len(calls) == 1 and response.closed


def test_local_sse_partial_evidence_survives_idle_timeout(api, monkeypatch):
    # Real HTTP socket, only loopback; the production management credential is
    # intentionally not forwarded by this test adapter.
    release = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            assert self.headers.get("Authorization") is None
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(event('{"error":"worker_refused"}'))
            self.wfile.flush()
            release.wait(5)

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    class Opener:
        def open(self, request, timeout):
            return urlopen(Request(f"http://127.0.0.1:{server.server_port}/"), timeout=timeout)

    monkeypatch.setattr("runpod_ci_broker.live.build_opener", lambda *args: Opener())
    start = time.monotonic()
    try:
        result = api.startup_logs("abc123", 3)
        assert time.monotonic() - start < 4
        assert result["signals"] == ["worker_refusal_hint"]
        assert result["status"] == "timeout_or_transport"
    finally:
        release.set()
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
