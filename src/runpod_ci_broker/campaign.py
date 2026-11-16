"""One approved campaign; reruns retain intent, elapsed time and spend estimates."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any

from .models import Task
from .planner import estimate
from .worker import CONSUMER, MIG

REPOSITORY = "jasonjiang8866/runpod-ci-broker"
CAMPAIGN = "broker-smoke-20261107"
DIAGNOSTIC_RETRY = CAMPAIGN + "-diagnostic-2"
TRANSPORT_RETEST = CAMPAIGN + "-transport-3"
REPAIR = CAMPAIGN + "-repair-4"
REPAIR_SOURCE = "567edcc32529ad017d13edbc154971aea4d9d624"
REPAIR_IMAGE = (
    "ghcr.io/jasonjiang8866/runpod-ci-broker-worker@sha256:"
    "9226c6ae489e22f89f014cec7f2dfd80ebe1b9d661fcfe318807035c75f3ae03"
)
REPAIR_HISTORY = {
    CAMPAIGN: "46bda3979ff7559b393bfab767341a44fbc25596a2b15736ea582fff5e4c0511",
    DIAGNOSTIC_RETRY: "9e66483324cd361af71486d7459b8d2adb22d3e88a7addaa3ce44e3a5ba71916",
    TRANSPORT_RETEST: "f757328fd8b8215c84321b301f3fffd3069e16a1f6ed2310b3dcd717497ce16e",
}
PRIOR_RESERVATION = 1.000167
SECOND_RESERVATION = 2.000334
SCOPE = "broker-central-smoke"
SOURCE = "2e6fe6930c02fc2469079a1d8e64025562aff07f"
IMAGE = (
    "ghcr.io/jasonjiang8866/runpod-ci-broker-worker@sha256:"
    "d1a450f816d71169c29eb53217d869baa88d9eeaa156911f06b83b8a93f9ad11"
)


def validate_task(task: Task) -> dict[str, Any]:
    b = estimate(task)
    if (
        task.experiment_id not in {CAMPAIGN, DIAGNOSTIC_RETRY, TRANSPORT_RETEST, REPAIR}
        or task.scope != SCOPE
        or task.repository != REPOSITORY
        or task.source_sha != (REPAIR_SOURCE if task.experiment_id == REPAIR else SOURCE)
        or task.image != (REPAIR_IMAGE if task.experiment_id == REPAIR else IMAGE)
        or task.strategy != "flexible_smoke"
        or task.entrypoint != ["broker-cuda-memory-smoke"]
        or set(task.gpu_allowlist) != CONSUMER | {MIG}
        or task.gpu_count != 1
        or task.data_centers
        or task.volume.kind != "none"
        or task.registry_credential_id is not None
        or task.cloud != "SECURE"
        or task.min_vram_gb != 24
        or task.compatibility.min_cuda_version != "12.8"
        or task.compatibility.cuda_versions
        or task.compatibility.min_driver
        or task.compatibility.exact_driver
        or task.compatibility.architectures
        or not b["complete"]
        or not b["within_budget"]
        or task.limits.total_budget_usd != 20
        or task.limits.max_gpu_hourly_usd != 3
        or task.limits.runtime_seconds != 60
        or task.limits.max_start_attempts != 1
        or task.limits.startup_seconds != 300
        or task.limits.collect_seconds != 120
        or task.limits.cleanup_seconds != 120
        or task.limits.max_wait_seconds > 60
        or task.limits.max_queries > 4
        or task.limits.max_collect_attempts > 10
        or task.limits.prior_spend_usd
        != {
            CAMPAIGN: 0,
            DIAGNOSTIC_RETRY: PRIOR_RESERVATION,
            TRANSPORT_RETEST: SECOND_RESERVATION,
            REPAIR: 3.000501,
        }.get(task.experiment_id)
        or task.results.required_files != ["metrics.json", "worker.log", "environment.json"]
        or task.results.metric_min != {"gpu_memory_smoke_passed": 1.0}
        or task.results.metric_max != {"worker_exit_code": 0.0}
        or task.results.max_bytes > 1048576
        or task.results.path != "/workspace/results"
    ):
        raise ValueError("approved_campaign_contract_required")
    return b


def retry_parent(prior: dict[str, Any] | None, task: Task) -> dict[str, Any]:
    """One known predecessor, never a reset or an arbitrary new allowance."""
    if prior is None:
        raise ValueError("retry_requires_original_record")
    original = Task.model_validate(prior["request"])
    validate_task(original)
    c = prior["campaign"]
    if (
        original.experiment_id != CAMPAIGN
        or prior["config_fingerprint"] != original.fingerprint()
        or prior["mode"] != "live"
        or prior["attempts"] != 1
        or prior["state"] != "REJECTED"
        or prior["facts"]["pod_deleted_confirmed"] is not True
        or prior["facts"]["task_completed"] is not True
        or prior["cleanup_verification"] != "direct_404_and_complete_inventory_twice"
        or c["reserved_usd"] != PRIOR_RESERVATION
        or c["reserved_seconds"] != 600
        or not 0 <= c["aggregate_resource_seconds"] <= 600
        or not 0 <= c["estimated_elapsed_cost_usd"] <= PRIOR_RESERVATION
        or task.limits.prior_spend_usd != PRIOR_RESERVATION
    ):
        raise ValueError("retry_parent_not_eligible")
    return {
        "experiment_id": CAMPAIGN,
        "record_sha256": hashlib.sha256(
            json.dumps(prior, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest(),
        "reserved_usd": c["reserved_usd"],
        "reserved_seconds": c["reserved_seconds"],
        "aggregate_resource_seconds": c["aggregate_resource_seconds"],
        "original_wall_deadline": c["wall_deadline"],
    }


def retest_parent(
    prior: dict[str, Any] | None, original: dict[str, Any] | None, task: Task
) -> dict[str, Any]:
    """One newly authorized retest after the known, definitive second create refusal."""
    if prior is None:
        raise ValueError("retest_requires_second_record")
    second = Task.model_validate(prior["request"])
    validate_task(second)
    first = retry_parent(original, second)
    c = prior["campaign"]
    if (
        second.experiment_id != DIAGNOSTIC_RETRY
        or prior["config_fingerprint"] != second.fingerprint()
        or prior["mode"] != "live"
        or prior["attempts"] != 1
        or prior["state"] != "PREFLIGHT_BLOCKED"
        or prior["create_intent"] is not False
        or prior["pod_id"] is not None
        or prior["errors"] != ["invalid_request"]
        or prior["collect_attempts"] != 0
        or prior["cleanup_verification"] != "not_applicable"
        or c["attempt_number"] != 2
        or c["predecessor"] != first
        or c["reserved_usd"] != SECOND_RESERVATION
        or c["reserved_seconds"] != 1200
        or c["aggregate_resource_seconds"] != first["aggregate_resource_seconds"]
        or c["estimated_elapsed_cost_usd"] != PRIOR_RESERVATION
        or task.experiment_id != TRANSPORT_RETEST
        or task.limits.prior_spend_usd != SECOND_RESERVATION
    ):
        raise ValueError("retest_parent_not_eligible")
    return {
        "experiment_id": DIAGNOSTIC_RETRY,
        "record_sha256": hashlib.sha256(
            json.dumps(prior, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest(),
        "reserved_usd": c["reserved_usd"],
        "reserved_seconds": c["reserved_seconds"],
        "aggregate_resource_seconds": c["aggregate_resource_seconds"],
        "original_wall_deadline": c["wall_deadline"],
    }


def record_hash(record: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def repair_parent(history: dict[str, Any] | None) -> dict[str, Any]:
    """One reviewed immutable history, not a general retry or a new budget."""
    if history is None or any(
        name not in history or record_hash(history[name]) != digest
        for name, digest in REPAIR_HISTORY.items()
    ):
        raise ValueError("repair_history_not_verified")
    prior = history[TRANSPORT_RETEST]
    c = prior["campaign"]
    if (
        prior["state"] != "PREFLIGHT_BLOCKED"
        or prior["create_intent"] is not False
        or prior["pod_id"] is not None
        or prior["collect_attempts"] != 0
        or c["reserved_usd"] != 3.000501
        or c["reserved_seconds"] != 1800
    ):
        raise ValueError("repair_parent_not_eligible")
    return {
        "experiment_id": TRANSPORT_RETEST,
        "record_sha256": record_hash(prior),
        "reserved_usd": c["reserved_usd"],
        "reserved_seconds": c["reserved_seconds"],
        "aggregate_resource_seconds": c["aggregate_resource_seconds"],
        "original_wall_deadline": c["wall_deadline"],
    }


def reserve(
    record: dict[str, Any],
    task: Task,
    now: float,
    *,
    prior: dict[str, Any] | None = None,
    original: dict[str, Any] | None = None,
    window_end: float | None = None,
    history: dict[str, Any] | None = None,
) -> None:
    b = validate_task(task)
    if record.get("campaign") is not None or record.get("attempts", 0) != 0:
        raise ValueError("campaign_already_attempted")
    parent = None
    if task.experiment_id in {DIAGNOSTIC_RETRY, TRANSPORT_RETEST, REPAIR}:
        parent = (
            repair_parent(history)
            if task.experiment_id == REPAIR
            else retest_parent(prior, original, task)
            if task.experiment_id == TRANSPORT_RETEST
            else retry_parent(prior, task)
        )
        if (
            window_end is None
            or not math.isfinite(window_end)
            or not now + 600 <= window_end <= now + 3600
        ):
            raise ValueError("explicit_retry_execution_window_required")
    # Reserve the entire attempt BEFORE POST. Never refund unknown create results.
    record["campaign"] = {
        "id": CAMPAIGN,
        "first_intent_at": now,
        "last_observed_at": now,
        "wall_deadline": window_end if parent else now + 18000,
        "attempt_deadline": now + 600,
        "collection_deadline": now + 420,  # leaves 60s upload + 120s cleanup
        "reserved_usd": b["estimated_total_usd"],
        "reserved_seconds": parent["reserved_seconds"] + 600 if parent else 600,
        "aggregate_resource_seconds": parent["aggregate_resource_seconds"] if parent else 0.0,
        "estimated_elapsed_cost_usd": task.limits.prior_spend_usd,
        "observed_max_hourly_compute_usd": None,
        "supplier_bill_usd": None,
    }
    if parent:
        record["campaign"]["attempt_number"] = {
            DIAGNOSTIC_RETRY: 2,
            TRANSPORT_RETEST: 3,
            REPAIR: 4,
        }[task.experiment_id]
        record["campaign"]["predecessor"] = parent
    record["attempts"] = 1


def observe(record: dict[str, Any], task: Task, now: float, rate: float | None = None) -> None:
    c = record["campaign"]
    if not math.isfinite(now) or now < c["last_observed_at"]:
        raise ValueError("campaign_clock_regressed")
    c["last_observed_at"] = now
    if rate is not None:
        if not math.isfinite(rate) or rate < 0:
            raise ValueError("observed_price_invalid")
        c["observed_max_hourly_compute_usd"] = max(c["observed_max_hourly_compute_usd"] or 0, rate)
    until = record.get("deleted_confirmed_at", now)
    elapsed = max(0.0, until - c["first_intent_at"])
    previous_seconds = c.get("predecessor", {}).get("aggregate_resource_seconds", 0.0)
    c["aggregate_resource_seconds"] = max(
        c["aggregate_resource_seconds"], previous_seconds + elapsed
    )
    # Conservative estimate includes all unresolved lifetime, including stopped periods.
    # It is neither metered usage nor an invoice. Never replaces the original reserve.
    hourly = max(3.0, c["observed_max_hourly_compute_usd"] or 0)
    b = estimate(task)
    c["estimated_elapsed_cost_usd"] = round(
        elapsed / 3600 * (hourly + float(task.limits.storage_hourly_usd or 0)) + b["reserves_usd"],
        6,
    )


def may_collect(record: dict[str, Any], task: Task, now: float) -> bool:
    observe(record, task, now)
    c = record["campaign"]
    return bool(
        now < min(c["collection_deadline"], c["wall_deadline"])
        and c["aggregate_resource_seconds"] < 18000
        and c["estimated_elapsed_cost_usd"] < 20
        and (c["observed_max_hourly_compute_usd"] or 0) <= 3
    )
