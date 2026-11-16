"""Secret-safe diagnostic projections; no RunPod connection is made."""

import io
import json
import time
from email.message import Message
from urllib.error import HTTPError

import pytest

from runpod_ci_broker.export import ResultHTTPError, collect_https
from runpod_ci_broker.transport_diagnostics import (
    header_hints,
    proxy_environment,
    record_startup,
)


@pytest.mark.parametrize("server", ["cloudflare", "nginx", "envoy", "SECRET", "x" * 10000])
def test_header_projection_only_emits_fixed_categories(server):
    headers = Message()
    headers["sErVeR"] = server
    headers["content-type"] = "text/html; secret=SECRET"
    for key in ["CF-Ray", "Via", "Location", "WWW-Authenticate", "Set-Cookie", "Authorization"]:
        headers[key] = "SECRET"
    hints = header_hints(headers)
    assert hints["server_family"] == (
        server if server in {"cloudflare", "nginx", "envoy"} else "other_or_missing"
    )
    assert hints["media_type"] == "text/html"
    assert hints["cf_ray_present"] and hints["location_present"]
    assert "SECRET" not in json.dumps(hints)
    assert len(json.dumps(hints)) < 350


@pytest.mark.parametrize("value,expected", [("challenge", True), ("SECRET", False), ("", False)])
def test_cloudflare_challenge_header_is_a_fixed_hint_only(value, expected):
    headers = Message()
    headers["cf-mitigated"] = value
    hints = header_hints(headers)
    assert hints["cf_challenge_hint"] is expected
    assert "SECRET" not in json.dumps(hints)


def test_missing_and_unrecognized_headers_remain_unknown():
    assert header_hints(None)["server_family"] == "other_or_missing"
    assert header_hints({"Server": "nginx SECRET"})["server_family"] == "other_or_missing"
    assert header_hints({"Content-Type": "SECRET"})["media_type"] == "other_or_missing"


def test_proxy_config_presence_never_exports_addresses_or_mutates_environment():
    env = {
        "https_proxy": "https://user:SECRET@host",
        "NO_PROXY": "SECRET",
        "GITHUB_TOKEN": "SECRET",
    }
    before = dict(env)
    assert proxy_environment(env) == {
        "http_proxy_configured": False,
        "https_proxy_configured": True,
        "all_proxy_configured": False,
        "no_proxy_configured": True,
    }
    assert env == before


@pytest.mark.parametrize("status", [401, 403, 302])
def test_http_diagnostics_close_without_reading_body_or_retry(task, tmp_path, status):
    class Unreadable(io.BytesIO):
        def read(self, *args):
            pytest.fail("must not read error body")

    body = Unreadable(b"SECRET")
    headers = Message()
    headers["Server"] = "cloudflare"
    headers["Location"] = "https://SECRET/"
    calls = []

    class Reader:
        def open(self, path, timeout):
            calls.append(path)
            raise HTTPError("https://SECRET/", status, "SECRET", headers, body)

    with pytest.raises(ResultHTTPError) as caught:
        collect_https(task, "abc123", Reader(), tmp_path / "out", deadline=time.monotonic() + 5)
    assert body.closed
    assert calls == ["/v1/manifest"]
    assert not caught.value.retryable
    assert caught.value.diagnostic["response_origin"] == "unverified"
    assert caught.value.diagnostic["header_hints"]["server_family"] == "cloudflare"
    assert "SECRET" not in json.dumps(vars(caught.value))
    assert not (tmp_path / "out").exists()
    assert not list(tmp_path.glob(".collect-*"))


def test_startup_timeline_bounded_retains_first_and_latest_without_raw_pod():
    record = {"campaign": {"first_intent_at": 1000}}
    pod = {
        "status": "RUNNING",
        "ports": ["8000/http"],
        "runtime": {"ip": "SECRET"},
        "env": {"RCB_RESULT_TOKEN": "SECRET"},
    }
    for n in range(100):
        record_startup(record, pod, 1000 + n)
    diagnostic = record["startup_diagnostics"]
    assert diagnostic["observations"] == 100
    samples = diagnostic["samples"]
    assert len(samples) == 16
    assert samples[0]["seconds_since_intent"] == 0
    assert samples[-1]["seconds_since_intent"] == 99
    assert samples[-1]["result_port_configured"] is True
    assert samples[-1]["worker_readiness"] == "unverified"
    assert "SECRET" not in json.dumps(diagnostic)


@pytest.mark.parametrize(
    "ports,expected", [(None, None), ([], False), (["8000/tcp"], False), ([123], None)]
)
def test_missing_or_wrong_port_is_not_reported_as_configured(ports, expected):
    record = {"campaign": {"first_intent_at": 0}}
    record_startup(record, {"status": "SECRET", "ports": ports, "runtime": "SECRET"}, 1)
    sample = record["startup_diagnostics"]["samples"][0]
    assert sample["result_port_configured"] is expected
    assert sample["pod_status"] == "unknown"
    assert sample["runtime_metadata"] == "null_or_missing"
    assert "SECRET" not in json.dumps(sample)
