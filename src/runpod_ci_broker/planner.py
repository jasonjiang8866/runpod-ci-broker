"""Pure deterministic admission, ranking and conservative cost estimates."""

from __future__ import annotations

from decimal import ROUND_CEILING, Decimal
from typing import Any

from .models import Authorization, Candidate, Task


def estimate(task: Task) -> dict[str, Any]:
    b = task.limits
    required = [
        "total_budget_usd",
        "max_gpu_hourly_usd",
        "runtime_seconds",
        "storage_hourly_usd",
        "transfer_reserve_usd",
        "request_reserve_usd",
        "cleanup_reserve_usd",
    ]
    missing = [name for name in required if getattr(b, name) is None]
    if missing:
        return {"complete": False, "missing": missing, "within_budget": False}
    assert b.runtime_seconds is not None and b.max_gpu_hourly_usd is not None
    assert b.storage_hourly_usd is not None and b.total_budget_usd is not None
    seconds = b.startup_seconds + b.runtime_seconds + b.collect_seconds + b.cleanup_seconds
    hours = Decimal(seconds * b.max_start_attempts) / Decimal(3600)
    gpu = hours * Decimal(str(b.max_gpu_hourly_usd)) * task.gpu_count
    storage = hours * Decimal(str(b.storage_hourly_usd))
    extras = sum(
        Decimal(str(v))
        for v in [
            b.transfer_reserve_usd,
            b.request_reserve_usd,
            b.cleanup_reserve_usd,
            b.prior_spend_usd,
        ]
    )
    total = (gpu + storage + extras).quantize(Decimal("0.000001"), rounding=ROUND_CEILING)
    return {
        "complete": True,
        "within_budget": total <= Decimal(str(b.total_budget_usd)),
        "gpu_usd": float(gpu),
        "storage_usd": float(storage),
        "reserves_usd": float(extras),
        "estimated_total_usd": float(total),
        "seconds_per_attempt": seconds,
        "reserved_attempts": b.max_start_attempts,
        "basis": b.cost_basis,
        "observed_runtime_cost_usd": None,
        "supplier_bill_usd": None,
        "provider_hard_cap": False,
        "risk": "Software deadline; runner loss/API outage can extend billable lifetime.",
    }


def version(value: str) -> tuple[int, ...]:
    return tuple(int(x) for x in value.split("."))


def reasons(task: Task, c: Candidate) -> list[str]:
    out: list[str] = []
    if c.gpu_id not in task.gpu_allowlist:
        out.append("gpu_not_allowed")
    if c.error:
        out.append(c.error)
    if c.cloud != task.cloud:
        out.append("cloud_mismatch")
    if c.memory_gb is None:
        out.append("vram_unknown")
    elif c.memory_gb < task.min_vram_gb:
        out.append("insufficient_vram")
    if c.gpu_count != task.gpu_count:
        out.append("gpu_count_mismatch")
    if c.data_center is None:
        out.append("region_unknown")
    elif task.data_centers and c.data_center not in task.data_centers:
        out.append("region_incompatible")
    if task.volume.kind != "none":
        if c.volume_compatible is not True:
            out.append("volume_incompatible" if c.volume_compatible is False else "volume_unknown")
        if task.volume.kind == "network" and c.data_center != task.volume.data_center:
            out.append("volume_region_incompatible")
    if c.hourly_usd is None:
        out.append("price_unknown")
    elif task.limits.max_gpu_hourly_usd is None:
        out.append("price_cap_missing")
    elif c.hourly_usd > task.limits.max_gpu_hourly_usd:
        out.append("price_exceeded")
    if c.capacity != "available":
        out.append("no_capacity" if c.capacity == "none" else "inventory_unknown")
    comp = task.compatibility
    if comp.cuda_versions and not set(c.cuda_versions) & set(comp.cuda_versions):
        out.append("cuda_incompatible" if c.cuda_versions else "cuda_unknown")
    if comp.min_cuda_version and not any(
        version(v) >= version(comp.min_cuda_version) for v in c.cuda_versions
    ):
        out.append("cuda_incompatible" if c.cuda_versions else "cuda_unknown")
    if comp.exact_driver or comp.min_driver:
        if c.driver is None:
            out.append("driver_unknown")
        elif (
            comp.exact_driver
            and c.driver != comp.exact_driver
            or comp.min_driver
            and version(c.driver) < version(comp.min_driver)
        ):
            out.append("driver_incompatible")
    if comp.architectures and c.architecture not in comp.architectures:
        out.append("architecture_incompatible" if c.architecture else "architecture_unknown")
    return out


def plan(task: Task, candidates: list[Candidate], *, mode: str = "mock") -> dict[str, Any]:
    # Explicit absent entries ensure a missing catalog row never means out of stock.
    present = {c.gpu_id for c in candidates}
    candidates = candidates + [
        Candidate(
            gpu_id=g,
            error="catalog_entry_missing",
            evidence="read_only" if mode == "read_only" else "mock",
        )
        for g in task.gpu_allowlist
        if g not in present
    ]
    decisions: list[dict[str, Any]] = [
        {"candidate": c.model_dump(), "reasons": reasons(task, c)} for c in candidates
    ]
    decisions.sort(key=lambda d: (d["candidate"]["gpu_id"], d["candidate"]["data_center"] or ""))
    prefs = task.gpu_preference + [g for g in task.gpu_allowlist if g not in task.gpu_preference]
    eligible = [d for d in decisions if not d["reasons"]]
    eligible.sort(
        key=lambda d: (
            prefs.index(d["candidate"]["gpu_id"]),
            d["candidate"]["hourly_usd"],
            d["candidate"]["data_center"],
        )
    )
    cost = estimate(task)
    blocked = []
    if not task.gpu_allowlist:
        blocked.append("gpu_allowlist_missing")
    if not cost["complete"]:
        blocked.append("budget_inputs_missing")
    elif not cost["within_budget"]:
        blocked.append("budget_insufficient")
    if not eligible:
        blocked.append("no_eligible_candidate")
    return {
        "schema_version": 1,
        "mode": mode,
        "request": task.model_dump(),
        "config_fingerprint": task.fingerprint(),
        "decisions": decisions,
        "selected": eligible[0]["candidate"] if eligible and not blocked else None,
        "ranking": "hard constraints, explicit GPU preference, per-GPU price, data-center ID",
        "budget": cost,
        "blockers": blocked,
        "paid_authorized": False,
        "actual_environment": None,
        "gpu_verified": False,
        "facts": {
            "task_completed": False,
            "experiment_passed": None,
            "results_saved": False,
            "pod_deleted_confirmed": False,
        },
        "notice": "Catalog capacity is advisory, never a reservation. No GPU was executed.",
    }


def authorize(task: Task, auth: Authorization | None) -> None:
    if auth is None or not auth.explicitly_approved:
        raise ValueError("authorization_missing")
    if auth.experiment_id != task.experiment_id or auth.config_fingerprint != task.fingerprint():
        raise ValueError("authorization_identity_mismatch")
    if not task.gpu_allowlist or not set(task.gpu_allowlist) <= set(auth.gpu_allowlist):
        raise ValueError("authorization_gpu_mismatch")
    b = estimate(task)
    if not b["complete"] or not b["within_budget"]:
        raise ValueError("budget_insufficient")
    if (
        task.limits.total_budget_usd is None
        or task.limits.total_budget_usd > auth.approved_budget_usd
    ):
        raise ValueError("authorization_budget_mismatch")
    if task.limits.runtime_seconds is None or task.limits.runtime_seconds > auth.runtime_seconds:
        raise ValueError("authorization_runtime_mismatch")
