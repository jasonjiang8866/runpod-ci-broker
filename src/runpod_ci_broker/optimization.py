"""Pure planning seam. Solvers propose IDs; admission and allocation stay outside."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from decimal import ROUND_CEILING, Decimal
from typing import Any, Literal, Protocol


@dataclass(frozen=True)
class PlacementOption:
    key: str
    gpu_id: str
    data_center: str
    hourly_microusd: int
    reservation_microusd: int
    stock_signal: str


@dataclass(frozen=True)
class PlanningJob:
    key: str
    options: tuple[PlacementOption, ...]


@dataclass(frozen=True)
class PlanningProblem:
    snapshot_id: str
    jobs: tuple[PlanningJob, ...]
    max_assignments: int
    budget_microusd: int


@dataclass(frozen=True)
class Assignment:
    job_key: str
    option_key: str


@dataclass(frozen=True)
class SolverResult:
    snapshot_id: str
    status: Literal["feasible", "deferred"]
    assignments: tuple[Assignment, ...]


class PlacementSolver(Protocol):
    name: str

    def solve(self, problem: PlanningProblem, *, time_limit_ms: int) -> SolverResult: ...


class RankedFirstSolver:
    """Baseline only: consume one already-filtered, ordered job; no optimality claim."""

    name = "ranked-first-v1"

    def solve(self, problem: PlanningProblem, *, time_limit_ms: int) -> SolverResult:
        if len(problem.jobs) != 1:
            raise ValueError("baseline_single_job_only")
        job = problem.jobs[0]
        assignments = (Assignment(job.key, job.options[0].key),) if job.options else ()
        return SolverResult(
            problem.snapshot_id, "feasible" if assignments else "deferred", assignments
        )


def microusd(value: Any) -> int:
    amount = Decimal(str(value))
    if not amount.is_finite() or amount < 0:
        raise ValueError("planning_cost_invalid")
    return int((amount * 1_000_000).to_integral_value(rounding=ROUND_CEILING))


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def validate_solution(problem: PlanningProblem, result: SolverResult) -> None:
    """Independently enforce membership, one assignment/job, slots and total reserve."""
    if (
        not isinstance(result, SolverResult)
        or result.snapshot_id != problem.snapshot_id
        or result.status not in {"feasible", "deferred"}
        or not isinstance(result.assignments, tuple)
        or (result.status == "feasible") != bool(result.assignments)
        or len(result.assignments) > problem.max_assignments
    ):
        raise ValueError("solver_result_invalid")
    jobs = {job.key: {option.key: option for option in job.options} for job in problem.jobs}
    selected: set[str] = set()
    reserved = 0
    for assignment in result.assignments:
        if (
            not isinstance(assignment, Assignment)
            or assignment.job_key in selected
            or assignment.option_key not in jobs.get(assignment.job_key, {})
        ):
            raise ValueError("solver_assignment_invalid")
        selected.add(assignment.job_key)
        reserved += jobs[assignment.job_key][assignment.option_key].reservation_microusd
    if reserved > problem.budget_microusd:
        raise ValueError("solver_budget_exceeded")


def propose(
    report: dict[str, Any],
    solver: PlacementSolver | None = None,
    *,
    now: Callable[[], float] = time.monotonic,
    time_limit_ms: int = 100,
) -> dict[str, Any]:
    """Apply a proposal only to admitted options; exceptions/late/invalid output block.

    This seam currently runs trusted, immediate in-process code. The elapsed-time
    check rejects late results but cannot interrupt a hanging third-party solver.
    A bounded subprocess adapter is a prerequisite for enabling such a solver.
    """
    if report["blockers"]:
        return report
    solver = solver or RankedFirstSolver()
    queue = report["candidate_queue"]
    options = tuple(
        PlacementOption(
            key=digest(item["candidate"]),
            gpu_id=item["candidate"]["gpu_id"],
            data_center=item["candidate"]["data_center"],
            hourly_microusd=microusd(item["candidate"]["hourly_usd"]),
            # Reserve all permitted attempts at the cap, not just the selected quote.
            reservation_microusd=microusd(report["budget"]["estimated_total_usd"]),
            stock_signal=item["availability_signal"],
        )
        for item in queue
    )
    job = PlanningJob(report["config_fingerprint"], options)
    identity = digest({"job": asdict(job), "observed_at": report.get("planning_observed_at")})
    problem = PlanningProblem(
        identity, (job,), 1, microusd(report["request"]["limits"]["total_budget_usd"])
    )
    started = now()
    evidence: dict[str, Any] = {
        "interface_version": 1,
        "snapshot_id": identity,
        "solver": "unverified",
        "time_limit_ms": time_limit_ms,
        "optimality": "not_claimed",
        "status": "blocked",
        "assignments": [],
    }
    failure = "solver_failed"
    try:
        if not re.fullmatch(r"[a-z0-9_-]{1,64}", solver.name):
            raise ValueError("solver_name_invalid")
        evidence["solver"] = solver.name
        result = solver.solve(problem, time_limit_ms=time_limit_ms)
        if now() - started > time_limit_ms / 1000:
            failure = "solver_deadline_exceeded"
            raise ValueError(failure)
        validate_solution(problem, result)
        evidence.update(status=result.status, assignments=[asdict(a) for a in result.assignments])
        if result.assignments:
            key = result.assignments[0].option_key
            chosen = next(item for option, item in zip(options, queue) if option.key == key)
            report["selected"] = dict(chosen["candidate"])
            report["selection_reason"] = {
                "objective": report["selection_reason"]["objective"],
                **chosen["ranking_factors"],
                "solver": solver.name,
            }
        else:
            failure = "solver_deferred"
            report["blockers"].append(failure)
    except Exception:  # noqa: BLE001 -- all adapter failures must block, without leaking inputs
        # Never serialize an adapter exception, which may contain private inputs.
        report["blockers"].append(failure)
    evidence["elapsed_ms"] = round(max(0.0, now() - started) * 1000, 3)
    report["placement_plan"] = evidence
    if report["blockers"]:
        report["selected"], report["candidate_queue"], report["selection_reason"] = None, [], None
    return report
