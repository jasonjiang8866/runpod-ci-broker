"""Bounded GET-only resource search and explainable placement feedback.

Catalog signals are ordinal hints, not estimated allocation probabilities. This
module cannot allocate a Pod, extend a budget or resolve an ambiguous create.
"""

from __future__ import annotations

import math
import random
import time
from collections import Counter
from collections.abc import Callable
from datetime import datetime
from typing import Annotated, Any, Literal, Self

from pydantic import Field, model_validator

from .models import Candidate, Identifier, Model, Nonnegative, Task
from .planner import plan
from .provider import ProviderError, ReadOnlyRunPod, Transport


class SearchPolicy(Model):
    objective: Literal["availability", "preference", "price"] = "availability"
    initial_interval_seconds: Annotated[int, Field(ge=1, le=60)] = 5
    max_interval_seconds: Annotated[int, Field(ge=1, le=120)] = 30
    cooldown_seconds: Annotated[int, Field(ge=1, le=3600)] = 60
    max_observation_age_seconds: Annotated[int, Field(ge=1, le=300)] = 30
    max_http_requests: Annotated[int, Field(ge=1, le=200)] = 64

    @model_validator(mode="after")
    def interval_order(self) -> Self:
        if self.max_interval_seconds < self.initial_interval_seconds:
            raise ValueError("search_interval_order")
        return self


class CapacityRefusal(Model):
    gpu_id: Annotated[str, Field(min_length=1, max_length=200)]
    data_center: Identifier
    observed_at: Nonnegative
    reason: Literal["definitive_capacity_refusal"] = "definitive_capacity_refusal"


class SearchHistory(Model):
    config_fingerprint: Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
    refusals: Annotated[list[CapacityRefusal], Field(max_length=128)] = Field(default_factory=list)

    def validate_for(self, task: Task, now: float) -> None:
        if not math.isfinite(now) or now < 0:
            raise ValueError("search_clock_invalid")
        if self.config_fingerprint != task.fingerprint():
            raise ValueError("search_history_task_mismatch")
        if any(r.observed_at > now or r.gpu_id not in task.gpu_allowlist for r in self.refusals):
            raise ValueError("search_history_invalid")


def capacity_feedback(
    task: Task,
    selected: Candidate,
    history: SearchHistory,
    error: ProviderError,
    now: float,
    *,
    mode: Literal["read_only", "mock"] = "read_only",
) -> SearchHistory:
    """Record ONLY an explicit rejected create; never interpret unknown/auth as capacity."""
    # Import the optional create adapter only when processing its failure evidence.
    from .live import CreateHTTPError

    history.validate_for(task, now)
    definitive = (
        isinstance(error, CreateHTTPError)
        and error.diagnostic.get("http_status") in {400, 422}
        and error.diagnostic.get("reason_class") == "capacity"
    ) or (mode == "mock" and error.code == "no_capacity")
    if error.ambiguous or not definitive or selected.data_center is None:
        raise ValueError("capacity_feedback_requires_definitive_refusal")
    refusal = CapacityRefusal(
        gpu_id=selected.gpu_id, data_center=selected.data_center, observed_at=now
    )
    result = SearchHistory(
        config_fingerprint=history.config_fingerprint,
        refusals=[*history.refusals[-127:], refusal],
    )
    result.validate_for(task, now)
    return result


def rank_candidates(
    task: Task,
    candidates: list[Candidate],
    policy: SearchPolicy,
    history: SearchHistory,
    now: float,
    *,
    observations: dict[str, Any] | None = None,
    mode: Literal["read_only", "mock"] = "read_only",
) -> dict[str, Any]:
    history.validate_for(task, now)
    report = plan(task, candidates, mode=mode)
    # Explicit preferences win ties. Without one, prefer 4090 among equally stocked cards.
    preferred = task.gpu_preference or [g for g in task.gpu_allowlist if "RTX 4090" in g]
    preferences = preferred + [g for g in task.gpu_allowlist if g not in preferred]
    stock = {
        (gpu["id"], dc["id"]): dc.get("availability")
        for gpu in (observations or {}).get("catalog_models", [])
        for dc in gpu.get("data_centers", [])
    }
    counts = Counter((c.gpu_id, c.data_center) for c in candidates)
    cooldowns: dict[tuple[str, str], float] = {}
    for refusal in history.refusals:
        key = (refusal.gpu_id, refusal.data_center)
        cooldowns[key] = max(cooldowns.get(key, 0), refusal.observed_at + policy.cooldown_seconds)
    known_centers = {dc["id"] for dc in (observations or {}).get("data_centers", [])}
    queue = []
    for decision in report["decisions"]:
        c = decision["candidate"]
        key = (c["gpu_id"], c["data_center"])
        if counts[key] > 1:
            decision["reasons"].append("duplicate_catalog_placement")
        if mode == "read_only" and c["data_center"] not in known_centers:
            decision["reasons"].append("datacenter_unverified")
        if mode == "read_only" and c["evidence"] != "read_only":
            decision["reasons"].append("mock_evidence_not_live")
        until = cooldowns.get(key, 0)
        decision["cooldown_remaining_seconds"] = max(0.0, until - now)
        if until > now:
            decision["reasons"].append("placement_cooling_down")
        signal = stock.get(key) or ("AVAILABLE" if c["capacity"] == "available" else "UNKNOWN")
        decision["availability_signal"] = signal
        if decision["reasons"]:
            continue
        factors = {
            "stock_order": {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "AVAILABLE": 3}.get(signal, 4),
            "gpu_preference_order": preferences.index(c["gpu_id"]),
            "per_gpu_hourly_usd": c["hourly_usd"],
        }
        queue.append({"candidate": c, "ranking_factors": factors, "availability_signal": signal})
    fields = {
        "availability": ("stock_order", "gpu_preference_order", "per_gpu_hourly_usd"),
        "preference": ("gpu_preference_order", "stock_order", "per_gpu_hourly_usd"),
        "price": ("per_gpu_hourly_usd", "stock_order", "gpu_preference_order"),
    }[policy.objective]
    queue.sort(
        key=lambda item: (
            tuple(item["ranking_factors"][f] for f in fields)
            + (
                item["candidate"]["data_center"],
                item["candidate"]["gpu_id"],
            )
        )
    )
    # Budget and all hard constraints remain authoritative after re-ranking.
    report["blockers"] = [b for b in report["blockers"] if b != "no_eligible_candidate"]
    if not queue:
        report["blockers"].append("no_eligible_candidate")
    report["candidate_queue"] = queue if not report["blockers"] else []
    report["selected"] = (
        report["candidate_queue"][0]["candidate"] if report["candidate_queue"] else None
    )
    report["ranking"] = (
        "hard constraints, temporary capacity cooldown, " + ", ".join(fields) + ", data-center ID"
    )
    report["selection_reason"] = (
        {"objective": policy.objective, **report["candidate_queue"][0]["ranking_factors"]}
        if report["selected"]
        else None
    )
    return report


class ResourceFinder:
    """One bounded search session. No result survives a stale/incomplete new snapshot."""

    def __init__(
        self,
        task: Task,
        policy: SearchPolicy | None = None,
        history: SearchHistory | None = None,
        *,
        get: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
        now: Callable[[], float] = time.monotonic,
        wall_now: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = random.random,
    ):
        self.task, self.policy = task, policy or SearchPolicy()
        self.history = history or SearchHistory(config_fingerprint=task.fingerprint())
        self.get, self.now, self.wall_now, self.sleep, self.jitter = (
            get,
            now,
            wall_now,
            sleep,
            jitter,
        )

    def run(self) -> dict[str, Any]:
        started = self.now()
        deadline = started + self.task.limits.max_wait_seconds
        self.history.validate_for(self.task, self.wall_now())
        policy = self.policy
        report = rank_candidates(self.task, [], policy, self.history, self.wall_now())
        rounds: list[dict[str, Any]] = []
        calls = 0
        stop_reason = "query_limit"
        terminal: str | None = None
        source = self.get

        def checked_get(path: str, params: dict[str, Any]) -> dict[str, Any]:
            nonlocal calls, terminal, source
            if terminal:
                raise ProviderError(terminal)
            if self.now() >= deadline:
                terminal = "search_deadline_reached"
                raise ProviderError(terminal)
            if calls >= policy.max_http_requests:
                terminal = "search_request_limit"
                raise ProviderError(terminal)
            try:
                if source is None:
                    source = Transport(deadline=deadline).get
                calls += 1
                result = source(path, params)
                if self.now() >= deadline:
                    terminal = "search_deadline_reached"
                    raise ProviderError(terminal)
                return result
            except ProviderError as exc:
                if not exc.retryable or exc.ambiguous:
                    terminal = exc.code
                raise

        # Disable nested catalog retries; one loop owns all request/time accounting.
        query_task = self.task.model_copy(
            update={
                "limits": self.task.limits.model_copy(
                    update={"max_queries": 1, "max_wait_seconds": 0}
                )
            }
        )
        provider = ReadOnlyRunPod(get=checked_get, now=self.now, sleep=self.sleep)
        initial_blockers = [b for b in report["blockers"] if b != "no_eligible_candidate"]
        if self.task.registry_credential_id:
            initial_blockers.append("registry_access_unverified")
        for attempt in range(1, self.task.limits.max_queries + 1):
            if initial_blockers:
                report["blockers"] = initial_blockers
                stop_reason = "admission_blocked"
                break
            if self.now() >= deadline:
                stop_reason = "search_deadline_reached"
                break
            candidates, observations = provider.snapshot(query_task)
            report = rank_candidates(
                self.task,
                candidates,
                policy,
                self.history,
                self.wall_now(),
                observations=observations,
            )
            report["provider_observations"] = observations
            for field in ("query_status", "datacenter_status"):
                if observations.get(field) != "ok":
                    report["blockers"].append(field + "_unverified")
            if observations.get("inventory") != "complete":
                report["blockers"].append("inventory_unverified")
            if observations.get("scope_active_pod_count", 0):
                report["blockers"].append("scope_active_pod")
            quote_valid_until = None
            try:
                observed = datetime.fromisoformat(observations.get("observed_at", ""))
                if observed.tzinfo is None:
                    raise ValueError("timestamp_timezone_missing")
                quote_valid_until = observed.timestamp() + policy.max_observation_age_seconds
                age = self.wall_now() - observed.timestamp()
                if not 0 <= age <= policy.max_observation_age_seconds:
                    report["blockers"].append("catalog_observation_stale")
            except (ValueError, TypeError):
                report["blockers"].append("catalog_timestamp_unverified")
            if self.now() >= deadline:
                terminal = "search_deadline_reached"
            if terminal:
                report["blockers"].append(terminal)
            if report["blockers"]:
                report["selected"], report["candidate_queue"] = None, []
                report["selection_reason"] = None
            report["candidate_valid_until"] = quote_valid_until if report["selected"] else None
            item = {
                "round": attempt,
                "http_requests": calls,
                "elapsed_seconds": round(self.now() - started, 3),
                "decisions": report["decisions"],
                "blockers": list(report["blockers"]),
                "selected": report["selected"],
                "next_query_in_seconds": None,
            }
            rounds.append(item)
            if report["selected"]:
                stop_reason = "candidates_found"
                break
            statuses = {
                observations.get(k) for k in ("query_status", "inventory", "datacenter_status")
            }
            if (
                terminal
                or "scope_active_pod" in report["blockers"]
                or any(
                    isinstance(s, str) and ("invalid" in s or "incomplete" in s or "duplicate" in s)
                    for s in statuses
                )
            ):
                stop_reason = terminal or "admission_blocked"
                break
            wait = min(
                policy.initial_interval_seconds * 2 ** (attempt - 1), policy.max_interval_seconds
            )
            wait += max(0.0, min(1.0, self.jitter()))
            cooling = [
                d["cooldown_remaining_seconds"]
                for d in report["decisions"]
                if d["reasons"] == ["placement_cooling_down"]
            ]
            if cooling:
                wait = min(wait, min(cooling))
            if attempt >= self.task.limits.max_queries:
                break
            if self.now() + wait >= deadline:
                stop_reason = "search_deadline_reached"
                break
            item["next_query_in_seconds"] = wait
            self.sleep(wait)
        report["state"] = (
            "CANDIDATES_FOUND"
            if report["selected"]
            else "SEARCH_EXHAUSTED"
            if stop_reason in {"query_limit", "search_request_limit", "search_deadline_reached"}
            else "SEARCH_BLOCKED"
        )
        report["resource_search"] = {
            "policy": policy.model_dump(),
            "history": self.history.model_dump(),
            "rounds": rounds,
            "snapshot_count": len(rounds),
            "http_requests": calls,
            "elapsed_seconds": round(self.now() - started, 3),
            "stop_reason": stop_reason,
            "max_wait_seconds": self.task.limits.max_wait_seconds,
            "max_snapshots": self.task.limits.max_queries,
            "paid_resources_created": False,
            "requires_fresh_precreate_inventory": True,
            "allocation_probability": "unmeasured; stock grades are ordinal hints only",
        }
        return report
