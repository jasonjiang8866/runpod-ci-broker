import json
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from runpod_ci_broker.cli import main
from runpod_ci_broker.diagnostics import diagnose_cuda
from runpod_ci_broker.models import Compatibility, Task
from runpod_ci_broker.planner import authorize, plan, reasons
from runpod_ci_broker.provider import ProviderError, ReadOnlyRunPod, Transport


def response(task, path, query):
    if path == "/pods":
        return {"pods": [], "pagination": {"hasNextPage": False}}
    if path == "/catalog/datacenters":
        return {"dataCenters": [{"id": "US-KS-2", "region": "NORTH_AMERICA"}]}
    assert path == "/catalog/gpus"
    assert not ("cudaVersions" in query and "minCudaVersion" in query)
    exact = "cudaVersions" in query
    return {
        "gpus": [
            {
                "id": task.gpu_allowlist[0],
                "memory": 24,
                "price": {"secure": 0.69},
                "availability": "NONE" if exact else "HIGH",
                "cudaVersions": [] if exact else [{"version": "12.9", "available": True}],
                "dataCenters": [] if exact else [{"id": "US-KS-2", "availability": "LOW"}],
            }
        ]
    }


def observe(task, get=None):
    calls = []

    def tracked(path, params):
        calls.append((path, params.copy()))
        return (get or (lambda p, q: response(task, p, q)))(path, params)

    provider = ReadOnlyRunPod(get=tracked, now=lambda: 0, sleep=lambda _: None)
    candidates, primary = provider.snapshot(task)
    diagnostic = diagnose_cuda(task, provider, primary, deadline=20)
    return candidates, primary, diagnostic, calls


@pytest.mark.parametrize("exact", [False, True])
def test_compare_exact_floor_and_unfiltered_without_mutating_task(task, exact):
    if exact:
        task.compatibility = Compatibility(cuda_versions=["12.8"])
    fingerprint = task.fingerprint()
    candidates, primary, diagnostic, calls = observe(task)
    assert task.fingerprint() == fingerprint
    assert diagnostic["admission_unchanged"] and diagnostic["diagnostic_only"]
    assert diagnostic["comparisons"][0]["inference"] == "cuda_filter_effect_possible"
    assert diagnostic["catalog_attempts_including_primary"] == 3
    queries = [q for p, q in calls if p == "/catalog/gpus"]
    assert len(queries) == 3
    assert all(q["cloud"] == task.cloud and q["count"] == task.gpu_count for q in queries)
    assert any(q.get("minCudaVersion") == "12.8" for q in queries)
    assert any(q.get("cudaVersions") == "12.8" for q in queries)
    assert any(not ({"minCudaVersion", "cudaVersions"} & q.keys()) for q in queries)
    if exact:
        assert plan(task, candidates)["selected"] is None
    else:
        assert plan(task, candidates)["selected"] is not None
    assert primary["catalog_query"] == queries[0]


@pytest.mark.parametrize(
    "cap,expected,status",
    [(1, 1, "query_limit_reached"), (2, 2, "query_limit_reached"), (3, 3, "complete")],
)
def test_diagnostic_attempts_share_primary_budget(task, cap, expected, status):
    task.limits.max_queries = cap
    _, _, diagnostic, calls = observe(task)
    assert sum(path == "/catalog/gpus" for path, _ in calls) == expected
    assert diagnostic["catalog_attempts_including_primary"] == expected
    assert diagnostic["status"] == status


def test_diagnostics_never_start_after_deadline(task):
    provider = ReadOnlyRunPod(get=lambda p, q: response(task, p, q), now=lambda: 0)
    _, primary = provider.snapshot(task)
    provider.get = lambda *_: pytest.fail("late query")
    assert diagnose_cuda(task, provider, primary, deadline=0)["status"] == "deadline_reached"


@pytest.mark.parametrize("code", ["authentication_failed", "permission_denied", "query_failed"])
def test_diagnostic_error_never_means_no_capacity_or_retries(task, code):
    def get(path, params):
        if path == "/catalog/gpus" and "minCudaVersion" not in params:
            raise ProviderError(code, retryable=code == "query_failed")
        return response(task, path, params)

    _, primary, diagnostic, calls = observe(task, get)
    assert primary["query_status"] == "ok"
    assert diagnostic["status"] == "incomplete"
    assert diagnostic["queries"][0]["status"] == code
    assert diagnostic["comparisons"][0]["signals"]["no_cuda_filter"] is None
    assert sum(p == "/catalog/gpus" for p, _ in calls) == 2


def test_primary_failure_skips_diagnostics(task):
    def fail(*_):
        raise ProviderError("permission_denied")

    _, _, diagnostic, calls = observe(task, fail)
    assert diagnostic["status"] == "skipped_primary_query_failed"
    assert len(calls) == 1


def test_invalid_diagnostic_response_does_not_disclose_input(task):
    def get(path, params):
        value = response(task, path, params)
        if path == "/catalog/gpus" and "minCudaVersion" not in params:
            value["gpus"][0]["price"] = {"secure": "DO_NOT_LEAK"}
        return value

    candidates, primary, diagnostic, _ = observe(task, get)
    assert diagnostic["queries"][0]["status"] == "catalog_schema_invalid"
    assert "DO_NOT_LEAK" not in json.dumps(diagnostic)
    assert primary["query_status"] == "ok" and plan(task, candidates)["selected"] is not None


def test_strict_task_stays_blocked_even_if_broader_queries_have_stock():
    task = Task.model_validate_json(Path("examples/spleeter-strict.json").read_text())
    old = task.model_dump()
    candidates, _, diagnostic, _ = observe(task)
    assert diagnostic["comparisons"][0]["inference"] == "cuda_filter_effect_possible"
    assert task.model_dump() == old
    assert plan(task, candidates)["selected"] is None
    replacement = task.model_dump()
    replacement["compatibility"]["cuda_versions"] = []
    replacement["compatibility"]["min_cuda_version"] = "12.8"
    with pytest.raises(ValidationError):
        Task.model_validate(replacement)


@pytest.mark.parametrize(
    "versions,code",
    [
        (["12.7"], "cuda_incompatible"),
        (["12.9"], None),
        (["12.10"], None),
        (["13.0"], None),
        ([], "cuda_unknown"),
    ],
)
def test_numeric_floor_is_catalog_admission_only(task, candidates, versions, code):
    candidate = candidates[0].model_copy(update={"cuda_versions": versions})
    assert reasons(task, candidate) == ([code] if code else [])
    assert plan(task, [candidate])["gpu_verified"] is False


def test_cuda_filter_inputs_mutually_exclusive():
    with pytest.raises(ValidationError):
        Compatibility(cuda_versions=["12.8"], min_cuda_version="12.8")


def test_legacy_fingerprint_remains_recoverable(task):
    task.compatibility = Compatibility(cuda_versions=["12.8"])
    task.limits.max_gpu_hourly_usd = 1.0
    assert task.fingerprint() == "2852b3442739162bf4408c95c9ddc9cce9b12e12dfda9e63451f5c94694d4142"


def test_5090_within_new_unit_cap_still_requires_total_budget_and_authority(task, candidates):
    gpu = next(c for c in candidates if "5090" in c.gpu_id)
    gpu.hourly_usd = 1.19
    assert task.limits.max_gpu_hourly_usd == 3.0 and "price_exceeded" not in reasons(task, gpu)
    with pytest.raises(ValueError, match="authorization_missing"):
        authorize(task, None)
    task.limits.total_budget_usd = None
    assert plan(task, [gpu])["selected"] is None


def test_cli_diagnostics_preserve_blocked_plan_and_render_comparison(task, tmp_path, monkeypatch):
    task.compatibility = Compatibility(cuda_versions=["12.8"])
    config = tmp_path / "task.json"
    config.write_text(task.model_dump_json())
    monkeypatch.setenv("RUNPOD_API_KEY", "TEST_SECRET")
    monkeypatch.setattr(Transport, "get", lambda self, p, q: response(task, p, q))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "broker",
            "--config",
            str(config),
            "--action",
            "preflight",
            "--diagnose-cuda",
            "--output",
            str(tmp_path / "out"),
        ],
    )
    assert main() == 2
    report = json.loads((tmp_path / "out/report.json").read_text())
    markdown = (tmp_path / "out/summary.md").read_text()
    assert report["selected"] is None and "no_eligible_candidate" in report["blockers"]
    assert "CUDA filter comparison" in markdown and "12.9" in markdown
    assert "TEST_SECRET" not in json.dumps(report) + markdown


def test_diagnostic_flag_cannot_turn_dry_run_into_cloud_read(task, tmp_path, monkeypatch):
    monkeypatch.setattr(Transport, "get", lambda *_: pytest.fail("unexpected cloud read"))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "broker",
            "--config",
            "examples/simplexlora-flexible.json",
            "--diagnose-cuda",
            "--output",
            str(tmp_path),
        ],
    )
    assert main() == 2
