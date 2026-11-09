"""One approved campaign; reruns retain intent, elapsed time and spend estimates."""

from __future__ import annotations

import math
from typing import Any

from .models import Task
from .planner import estimate
from .worker import CONSUMER, MIG

REPOSITORY = "jasonjiang8866/runpod-ci-broker"
CAMPAIGN = "broker-smoke-20261107"
SCOPE = "broker-central-smoke"
SOURCE = "2e6fe6930c02fc2469079a1d8e64025562aff07f"
IMAGE = (
    "ghcr.io/jasonjiang8866/runpod-ci-broker-worker@sha256:"
    "d1a450f816d71169c29eb53217d869baa88d9eeaa156911f06b83b8a93f9ad11"
)


def validate_task(task: Task) -> dict[str, Any]:
    b = estimate(task)
    if (
        task.experiment_id != CAMPAIGN
        or task.scope != SCOPE
        or task.repository != REPOSITORY
        or task.source_sha != SOURCE
        or task.image != IMAGE
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
        or task.limits.prior_spend_usd != 0
        or task.results.required_files != ["metrics.json", "worker.log", "environment.json"]
        or task.results.metric_min != {"gpu_memory_smoke_passed": 1.0}
        or task.results.metric_max != {"worker_exit_code": 0.0}
        or task.results.max_bytes > 1048576
        or task.results.path != "/workspace/results"
    ):
        raise ValueError("approved_campaign_contract_required")
    return b


def reserve(record: dict[str, Any], task: Task, now: float) -> None:
    b = validate_task(task)
    if record.get("campaign") is not None or record.get("attempts", 0) != 0:
        raise ValueError("campaign_already_attempted")
    # Reserve the entire attempt BEFORE POST. Never refund unknown create results.
    record["campaign"] = {
        "id": CAMPAIGN,
        "first_intent_at": now,
        "last_observed_at": now,
        "wall_deadline": now + 18000,
        "attempt_deadline": now + 600,
        "collection_deadline": now + 420,  # leaves 60s upload + 120s cleanup
        "reserved_usd": b["estimated_total_usd"],
        "reserved_seconds": 600,
        "aggregate_resource_seconds": 0.0,
        "estimated_elapsed_cost_usd": 0.0,
        "observed_max_hourly_compute_usd": None,
        "supplier_bill_usd": None,
    }
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
    c["aggregate_resource_seconds"] = max(c["aggregate_resource_seconds"], elapsed)
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
