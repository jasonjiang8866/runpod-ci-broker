"""Offline, replayable multi-task placement on a single catalog observation."""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import asdict, replace
from typing import Annotated, Any, Literal

from pydantic import Field

from .discovery import SearchHistory, SearchPolicy, rank_candidates
from .models import Candidate, Model
from .optimization import (
    Assignment,
    PlacementOption,
    PlanningJob,
    PlanningProblem,
    SolverResult,
    digest,
    microusd,
    validate_solution,
)
from .queue import Amount, Entry, QueueState


class Offer(Candidate):
    stock: Literal["HIGH", "MEDIUM", "LOW"] | None = None


class BatchCatalog(Model):
    observed_at: Amount
    complete: bool
    offers: list[Offer]


class BatchReplay(Model):
    schema_version: Literal[1] = 1
    queue: QueueState
    catalog: BatchCatalog
    planned_at: Amount
    aging_seconds: Annotated[int, Field(gt=0)] = 60


class BatchSolver:
    """Consume jobs in supplied order. One proposal per model/site in this wave."""

    def __init__(self, *, protect_scarce: bool):
        self.protect_scarce = protect_scarce
        self.name = "scarcity-aware-v1" if protect_scarce else "fifo-v1"

    def solve(self, problem: PlanningProblem, *, time_limit_ms: int) -> SolverResult:
        deadline = time.monotonic() + time_limit_ms / 1000
        demand = Counter(option.key for job in problem.jobs for option in job.options)
        remaining = problem.budget_microusd
        used: set[str] = set()
        assignments: list[Assignment] = []
        for job in problem.jobs:
            if time.monotonic() >= deadline:
                raise TimeoutError("batch_solver_timeout")
            if len(assignments) >= problem.max_assignments:
                break
            demand.subtract(option.key for option in job.options)
            choices = [
                o for o in job.options if o.key not in used and o.reservation_microusd <= remaining
            ]
            if not choices:
                continue
            # Stable ties retain the per-task availability/preference/price ordering.
            selected = (
                min(choices, key=lambda o: demand[o.key]) if self.protect_scarce else choices[0]
            )
            assignments.append(Assignment(job.key, selected.key))
            remaining -= selected.reservation_microusd
            used.add(selected.key)
        if time.monotonic() >= deadline:
            raise TimeoutError("batch_solver_timeout")
        return SolverResult(
            problem.snapshot_id, "feasible" if assignments else "deferred", tuple(assignments)
        )


def _options(
    entry: Entry, offers: list[Offer], now: int
) -> tuple[tuple[PlacementOption, ...], list[str], list[dict[str, Any]]]:
    task = entry.task
    # Volume reachability is specific to a task's volume. Shared GPU quotes cannot prove it.
    if task.volume.kind != "none" or task.registry_credential_id:
        return (), ["task_access_unverified"], []
    matching = [o for o in offers if o.cloud == task.cloud and o.gpu_count == task.gpu_count]
    observations = {
        "catalog_models": [
            {"id": o.gpu_id, "data_centers": [{"id": o.data_center, "availability": o.stock}]}
            for o in matching
        ]
    }
    ranking = rank_candidates(
        task,
        [Candidate.model_validate(o.model_dump(exclude={"stock"})) for o in matching],
        SearchPolicy(),
        SearchHistory(config_fingerprint=task.fingerprint()),
        now,
        observations=observations,
        mode="mock",
    )
    options = []
    for row in ranking["candidate_queue"]:
        candidate = row["candidate"]
        gpu, site = candidate["gpu_id"], candidate["data_center"]
        options.append(
            PlacementOption(
                digest([gpu, site]),
                gpu,
                site,
                microusd(candidate["hourly_usd"]),
                entry.incremental_microusd,
                row["availability_signal"],
            )
        )
    excluded = [
        {
            "gpu_id": d["candidate"]["gpu_id"],
            "data_center": d["candidate"]["data_center"],
            "reasons": d["reasons"],
        }
        for d in ranking["decisions"]
        if d["reasons"] and d["candidate"]["gpu_id"] in task.gpu_allowlist
    ]
    return tuple(options), ranking["blockers"], excluded


def _decisions(
    problem: PlanningProblem, solver: BatchSolver, details: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    try:
        result = solver.solve(problem, time_limit_ms=1000)
    except TimeoutError:
        return {
            "solver": solver.name,
            "proposed_count": 0,
            "blocker": "batch_solver_timeout",
            "tasks": [
                {
                    "job_key": job.key,
                    "experiment_id": details[job.key]["experiment_id"],
                    "selected": None,
                    "reason": "batch_solver_timeout",
                }
                for job in problem.jobs
            ],
        }

    validate_solution(problem, result)
    chosen = {a.job_key: a.option_key for a in result.assignments}
    if len(set(chosen.values())) != len(chosen):
        raise ValueError("duplicate_wave_placement")
    used: set[str] = set()
    remaining = problem.budget_microusd
    rows = []
    demand = Counter(o.key for job in problem.jobs for o in job.options)
    for job in problem.jobs:
        demand.subtract(o.key for o in job.options)
        reasons = details[job.key]["blockers"]
        selected = next((o for o in job.options if o.key == chosen.get(job.key)), None)
        if selected is not None:
            remaining -= selected.reservation_microusd
            used.add(selected.key)
            reason = (
                "preserve_contended_placement"
                if solver.protect_scarce
                and any(
                    o.key not in used and demand[o.key] > demand[selected.key]
                    for o in job.options[: job.options.index(selected)]
                )
                else "first_available_ranked_option"
            )
        elif reasons:
            reason = reasons[0]
        elif len(used) >= problem.max_assignments:
            reason = "slots_full"
        elif all(o.reservation_microusd > remaining for o in job.options):
            reason = "budget_insufficient"
        else:
            reason = "placement_already_proposed"
        rows.append(
            {
                "job_key": job.key,
                "experiment_id": details[job.key]["experiment_id"],
                "remaining_competing_tasks": demand[selected.key] if selected else None,
                "reason": reason,
                "selected": asdict(selected) if selected else None,
            }
        )
    return {"solver": solver.name, "proposed_count": len(chosen), "tasks": rows}


def batch_plan(replay: BatchReplay) -> dict[str, Any]:
    state, catalog, now = replay.queue, replay.catalog, replay.planned_at
    snapshot_id = digest(replay.model_dump())
    snapshot_blocker = None
    if not catalog.complete:
        snapshot_blocker = "catalog_incomplete"
    elif not 0 <= now - catalog.observed_at <= SearchPolicy().max_observation_age_seconds:
        snapshot_blocker = "catalog_observation_stale"

    jobs: list[PlanningJob] = []
    details: dict[str, dict[str, Any]] = {}
    active_targets = {
        (e.dispatch.gpu_id, e.dispatch.data_center)
        for e in state.entries.values()
        if e.dispatch and not e.release
    }
    for key, entry in sorted(state.entries.items()):
        eligibility = state.reason(entry, now)
        exclusions: list[dict[str, Any]] = []
        options: tuple[PlacementOption, ...] = ()
        if eligibility != "eligible":
            blockers = [eligibility]
        elif entry.enqueued_at > now:
            blockers = ["not_yet_enqueued"]
        elif snapshot_blocker:
            blockers = [snapshot_blocker]
        else:
            options, blockers, exclusions = _options(entry, catalog.offers, now)
            available = []
            for option in options:
                if (option.gpu_id, option.data_center) in active_targets:
                    exclusions.append(
                        {
                            "gpu_id": option.gpu_id,
                            "data_center": option.data_center,
                            "reasons": ["placement_claimed"],
                        }
                    )
                else:
                    available.append(option)
            if options and not available:
                blockers = ["placement_claimed"]
            options = tuple(available)
        jobs.append(PlanningJob(key, options))
        details[key] = {
            "experiment_id": entry.task.experiment_id,
            "scope": entry.task.scope,
            "effective_priority": entry.priority
            + max(0, now - entry.enqueued_at) // replay.aging_seconds,
            "enqueued_at": entry.enqueued_at,
            "blockers": blockers,
            "excluded": exclusions,
            "ranked_options": [asdict(o) for o in options],
        }

    fifo = sorted(jobs, key=lambda j: (details[j.key]["enqueued_at"], j.key))
    ordered = sorted(fifo, key=lambda j: (-details[j.key]["effective_priority"], len(j.options)))
    problem = PlanningProblem(
        snapshot_id,
        tuple(ordered),
        state.policy.slots - state.slots_used,
        state.policy.budget_microusd - state.reserved_microusd,
    )
    return {
        "mode": "offline",
        "snapshot_id": snapshot_id,
        "planned_at": now,
        "observed_at": catalog.observed_at,
        "snapshot_blocker": snapshot_blocker,
        "remaining_slots": problem.max_assignments,
        "remaining_microusd": problem.budget_microusd,
        "candidates_by_task": details,
        "plan": _decisions(problem, BatchSolver(protect_scarce=True), details),
        "fifo": _decisions(
            replace(problem, jobs=tuple(fifo)), BatchSolver(protect_scarce=False), details
        ),
    }
