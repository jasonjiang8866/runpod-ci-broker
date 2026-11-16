import io
import json
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from runpod_ci_broker.export import RunPodResultReader, result_payloads, result_server
from runpod_ci_broker.fake import FakeProvider
from runpod_ci_broker.live import CreateHTTPError
from runpod_ci_broker.provider import ProviderError
from runpod_ci_broker.transport_diagnostics import create_reason

CAPACITY = "There are no longer any instances available with the requested specifications. Please refresh and try again."


def test_create_capacity_classification_preserves_only_fixed_reason():
    body = io.BytesIO(json.dumps({"detail": CAPACITY, "env": {"TOKEN": "SECRET"}}).encode())
    error = CreateHTTPError(
        HTTPError(
            "https://SECRET", 400, "SECRET", {"Content-Type": "application/problem+json"}, body
        )
    )
    assert error.diagnostic["reason_class"] == "capacity"
    assert error.diagnostic["header_hints"]["media_type"] == "application/problem+json"
    assert "SECRET" not in json.dumps(vars(error))
    assert not error.ambiguous


@pytest.mark.parametrize(
    "detail", ["no capacity SECRET", CAPACITY + " SECRET", {"env": "SECRET"}, None]
)
def test_noncanonical_capacity_never_enables_fallback(detail):
    assert create_reason(json.dumps({"detail": detail}).encode())["reason_class"] == "unclassified"


def test_oversized_or_plaintext_error_is_not_capacity():
    assert create_reason(b"x" * 16385)["reason_class"] == "unclassified"
    assert create_reason(CAPACITY.encode())["reason_class"] == "unclassified"


def test_authenticated_readiness_precedes_complete_bundle(task, candidates):
    state = {"phase": "starting", "payloads": {}}
    token = "a" * 64
    server = result_server(task, "abc123", None, {}, token, state=state)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    reader = RunPodResultReader("abc123", 8000, token)
    reader.base = f"http://127.0.0.1:{server.server_port}"
    try:
        with pytest.raises(HTTPError) as exc:
            urlopen(reader.base + "/v1/status", timeout=2)
        assert exc.value.code == 401
        assert reader.readiness(task, "abc123", 2) == "starting"
        with pytest.raises(HTTPError) as exc:
            reader.open("/v1/manifest", 2)
        assert exc.value.code == 503
        manifest, files = FakeProvider(candidates).collect("abc123", task)
        state.update(payloads=result_payloads(task, "abc123", manifest, files), phase="ready")
        assert reader.readiness(task, "abc123", 2) == "ready"
        with reader.open("/v1/manifest", 2) as response:
            assert json.load(response) == manifest
        with pytest.raises(ProviderError, match="identity"):
            reader.readiness(task, "differentpod", 2)
        bad = Request(reader.base + "/v1/status", headers={"Authorization": "Bearer " + "b" * 64})
        with pytest.raises(HTTPError) as exc:
            urlopen(bad, timeout=2)
        assert exc.value.code == 401
    finally:
        server.shutdown()
        thread.join(3)
        server.server_close()
