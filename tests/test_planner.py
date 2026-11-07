import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from runpod_ci_broker.models import Task
from runpod_ci_broker.planner import authorize, estimate, plan


def change(task, **updates):
    return Task.model_validate({**task.model_dump(), **updates})


def test_deterministic_preference_before_price(task, candidates):
    a = plan(task, candidates)
    assert a == plan(task, list(reversed(candidates)))
    assert a["selected"]["gpu_id"] == task.gpu_preference[0]
    assert a["selected"]["hourly_usd"] > min(c.hourly_usd for c in candidates)
    assert (
        task.fingerprint()
        == Task.model_validate_json(json.dumps(task.model_dump(), sort_keys=True)).fingerprint()
    )


def test_flexible_fallback(task, candidates):
    candidates[0].capacity = "none"
    assert plan(task, candidates)["selected"]["gpu_id"] == task.gpu_preference[1]


def test_strict_never_falls_back(candidates):
    task = Task.model_validate_json(Path("examples/spleeter-strict.json").read_text())
    candidates[0].capacity = "none"
    p = plan(task, candidates)
    assert p["selected"] is None
    assert all(
        "gpu_not_allowed" in d["reasons"]
        for d in p["decisions"]
        if d["candidate"]["gpu_id"] != task.gpu_allowlist[0]
    )


def test_hardware_is_independent_group(task):
    with pytest.raises(ValidationError):
        change(task, strategy="hardware_benchmark", benchmark_group="any")
    a = Task.model_validate_json(Path("examples/flashattention-hardware.json").read_text())
    assert len(a.gpu_allowlist) == 1 and a.benchmark_group == "rtx3090-sm86"


@pytest.mark.parametrize(
    "field,value",
    [
        ("total_budget_usd", None),
        ("runtime_seconds", None),
        ("max_gpu_hourly_usd", None),
        ("storage_hourly_usd", None),
        ("transfer_reserve_usd", None),
        ("total_budget_usd", 0.01),
    ],
)
def test_budget_refuses_missing_or_insufficient(task, auth, field, value):
    altered = change(task, limits={**task.limits.model_dump(), field: value})
    assert not estimate(altered)["within_budget"]
    fresh = auth.model_copy(update={"config_fingerprint": altered.fingerprint()})
    with pytest.raises(ValueError):
        authorize(altered, fresh)


@pytest.mark.parametrize(
    "field,value",
    [
        ("total_budget_usd", float("nan")),
        ("max_gpu_hourly_usd", float("inf")),
        ("max_start_attempts", 0),
        ("runtime_seconds", True),
    ],
)
def test_numeric_validation(task, field, value):
    with pytest.raises(ValidationError):
        change(task, limits={**task.limits.model_dump(), field: value})


def test_budget_counts_full_lifetimes_retries_storage(task):
    a = estimate(task)
    seconds = (180 + 120 + 60 + 60) * 2
    expected = seconds / 3600 * (1.0 + 0.02) + 0.1 + 0.01 + 0.2
    assert a["estimated_total_usd"] >= expected
    assert a["supplier_bill_usd"] is None and a["provider_hard_cap"] is False


@pytest.mark.parametrize(
    "updates,code",
    [
        ({"memory_gb": 12.0}, "insufficient_vram"),
        ({"hourly_usd": 5.0}, "price_exceeded"),
        ({"capacity": "unknown"}, "inventory_unknown"),
        ({"capacity": "none"}, "no_capacity"),
        ({"cuda_versions": []}, "cuda_unknown"),
        ({"gpu_count": 2}, "gpu_count_mismatch"),
    ],
)
def test_explanations(task, candidates, updates, code):
    p = plan(task, [candidates[0].model_copy(update=updates)])
    assert (
        code
        in next(d for d in p["decisions"] if d["candidate"]["gpu_id"] == candidates[0].gpu_id)[
            "reasons"
        ]
    )


def test_network_volume_must_match_region(task, candidates):
    task = change(task, volume={"kind": "network", "id": "existing", "data_center": "EU-RO-1"})
    assert plan(task, candidates)["selected"] is None


@pytest.mark.parametrize(
    "updates",
    [
        {"image": "image:latest"},
        {"source_sha": "main"},
        {"entrypoint": ["bash", "-c", "echo"]},
        {"entrypoint": ["python", "x.py", "$(id)"]},
        {"results": {"required_files": ["../bad"]}},
        {"RUNPOD_API_KEY": "sensitive-not-allowed"},
    ],
)
def test_untrusted_or_unpinned_config_rejected(task, updates):
    with pytest.raises(ValidationError):
        change(task, **updates)


def test_authorization_bound_to_exact_task(task, auth):
    authorize(task, auth)
    for invalid in [
        None,
        auth.model_copy(update={"explicitly_approved": False}),
        auth.model_copy(update={"approved_budget_usd": 0.01}),
        auth.model_copy(update={"gpu_allowlist": []}),
        auth.model_copy(update={"runtime_seconds": 1}),
        auth.model_copy(update={"config_fingerprint": "f" * 64}),
    ]:
        with pytest.raises(ValueError):
            authorize(task, invalid)
