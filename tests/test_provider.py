import json
from unittest.mock import patch

import pytest

from runpod_ci_broker.provider import (
    NoRedirect,
    ProviderError,
    ReadOnlyRunPod,
    Transport,
    http_error,
)


def reply(path, params):
    if path == "/catalog/gpus":
        assert params["include"] == "AVAILABILITY" and params["product"] == "POD"
        return {
            "gpus": [
                {
                    "id": "NVIDIA GeForce RTX 4090",
                    "memory": 24,
                    "price": {"secure": 0.5},
                    "maxCount": {"secure": 8},
                    "availability": "HIGH",
                    "dataCenters": [{"id": "US-KS-2", "availability": "LOW"}],
                    "cudaVersions": [{"version": "12.8", "available": True}],
                }
            ]
        }
    if path == "/pods":
        return {
            "pods": [
                {"id": "user-pod", "env": {"RUNPOD_API_KEY": "MUST_NOT_LEAK"}, "status": "RUNNING"}
            ],
            "pagination": {"hasNextPage": False, "nextCursor": None},
        }
    if path == "/catalog/datacenters":
        return {
            "dataCenters": [{"id": "US-KS-2", "region": "NORTH_AMERICA", "globalNetwork": True}]
        }
    raise AssertionError(path)


def test_documented_fields_and_secret_projection(task):
    c, obs = ReadOnlyRunPod(get=reply).snapshot(task)
    assert len(c) == 1 and c[0].memory_gb == 24 and c[0].hourly_usd == 0.5
    assert c[0].capacity == "available" and c[0].cuda_versions == ["12.8"]
    assert c[0].driver is None and c[0].architecture is None
    assert "MUST_NOT_LEAK" not in json.dumps(obs)
    assert obs["active_pod_count"] == 1


def test_max_count_and_catalog_presence_are_not_stock(task):
    def get(path, params):
        value = reply(path, params)
        if path == "/catalog/gpus":
            g = value["gpus"][0]
            g.pop("availability")
            g.pop("dataCenters")
        return value

    c, _ = ReadOnlyRunPod(get=get).snapshot(task)
    assert c[0].capacity == "unknown"


def test_explicit_none_is_different_from_missing_capacity(task):
    def get(path, params):
        value = reply(path, params)
        if path == "/catalog/gpus":
            g = value["gpus"][0]
            g["availability"] = "NONE"
            g.pop("dataCenters")
        return value

    c, _ = ReadOnlyRunPod(get=get).snapshot(task)
    assert c[0].capacity == "none"


@pytest.mark.parametrize(
    "status,code,retryable",
    [
        (401, "authentication_failed", False),
        (402, "account_restricted", False),
        (403, "permission_denied", False),
        (429, "rate_limited", True),
        (503, "provider_unavailable", True),
        (400, "invalid_request", False),
    ],
)
def test_http_errors_are_typed_without_body(status, code, retryable):
    exc = http_error(status)
    assert exc.code == code and exc.retryable == retryable


def test_query_failure_bounded_unknown_not_no_capacity(task):
    calls = []

    def broken(path, params):
        calls.append(path)
        raise ProviderError("query_failed", retryable=True)

    c, obs = ReadOnlyRunPod(get=broken, sleep=lambda _: None).snapshot(task)
    assert len(calls) == task.limits.max_queries
    assert obs["query_status"] == "query_failed"
    assert all(x.capacity == "unknown" for x in c)


def test_pagination_complete_and_cursor_checked():
    calls = []

    def get(path, params):
        calls.append(params)
        if "cursor" not in params:
            return {"pods": [{"id": "a"}], "pagination": {"hasNextPage": True, "nextCursor": "two"}}
        return {"pods": [{"id": "b"}], "pagination": {"hasNextPage": False, "nextCursor": None}}

    assert len(ReadOnlyRunPod(get=get).listing("/pods", "pods")) == 2
    assert calls[1]["cursor"] == "two"
    with pytest.raises(ProviderError, match="inventory_incomplete"):
        ReadOnlyRunPod(get=lambda *_: {"pods": []}).listing("/pods", "pods")
    with pytest.raises(ProviderError, match="inventory_incomplete"):
        ReadOnlyRunPod(
            get=lambda *_: {"pods": [], "pagination": {"hasNextPage": True, "nextCursor": "same"}}
        ).listing("/pods", "pods")


def test_network_volume_uses_data_center_field(task):
    from runpod_ci_broker.models import Volume

    task.volume = Volume(kind="network", id="existing", data_center="US-KS-2")

    def get(path, params):
        if path == "/network-volumes":
            return {
                "networkVolumes": [{"id": "existing", "dataCenter": "US-KS-2"}],
                "pagination": {"hasNextPage": False},
            }
        return reply(path, params)

    c, obs = ReadOnlyRunPod(get=get).snapshot(task)
    assert c[0].volume_compatible is True
    assert obs["volume_status"] == "compatible"


def test_live_adapter_is_get_only_and_never_redirects(monkeypatch):
    monkeypatch.setenv("RUNPOD_API_KEY", "fake-secret")
    transport = Transport()
    with patch("runpod_ci_broker.provider.build_opener") as opener:
        with pytest.raises(ProviderError, match="endpoint_not_allowed"):
            transport.get("/pods/arbitrary", {})
        opener.assert_not_called()
    assert NoRedirect().redirect_request(None, None, None, None, None, None) is None
    assert not hasattr(ReadOnlyRunPod, "create") and not hasattr(ReadOnlyRunPod, "delete")


def test_missing_live_catalog_rows_never_become_mock_evidence(task):
    from runpod_ci_broker.planner import plan

    report = plan(task, [], mode="read_only")
    assert all(d["candidate"]["evidence"] == "read_only" for d in report["decisions"])
