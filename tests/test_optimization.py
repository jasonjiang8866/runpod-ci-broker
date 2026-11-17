"""Planner contract tests; all resources and solver results are offline fixtures."""

import json
from dataclasses import FrozenInstanceError, replace

import pytest

from runpod_ci_broker.discovery import SearchHistory, SearchPolicy, rank_candidates
from runpod_ci_broker.optimization import (
    Assignment,
    PlacementOption,
    PlanningJob,
    PlanningProblem,
    RankedFirstSolver,
    SolverResult,
    microusd,
    propose,
    validate_solution,
)


def report(task, candidates):
    return rank_candidates(
        task,
        candidates,
        SearchPolicy(),
        SearchHistory(config_fingerprint=task.fingerprint()),
        100.0,
        mode="mock",
    )


def test_default_solver_preserves_ranked_selection_and_records_no_optimality(task, candidates):
    r = report(task, candidates)
    selected = r["selected"].copy()
    propose(r)
    assert r["selected"] == selected
    assert r["placement_plan"]["status"] == "feasible"
    assert r["placement_plan"]["optimality"] == "not_claimed"
    assert not r["paid_authorized"] and not any(r["facts"].values())


def test_solver_can_select_alternative_but_receives_only_immutable_admitted_options(
    task, candidates
):
    blocked = candidates[0].model_copy(update={"data_center": "EXPENSIVE", "hourly_usd": 99})

    class Alternative:
        name = "alternative-test"

        def solve(self, problem, *, time_limit_ms):
            with pytest.raises(FrozenInstanceError):
                problem.max_assignments = 99
            job = problem.jobs[0]
            assert all(o.data_center != "EXPENSIVE" for o in job.options)
            assert "entrypoint" not in repr(problem) and "authorization" not in repr(problem)
            chosen = job.options[-1]
            self.chosen = chosen
            return SolverResult(problem.snapshot_id, "feasible", (Assignment(job.key, chosen.key),))

    solver = Alternative()
    r = propose(report(task, candidates + [blocked]), solver)
    assert r["selected"]["gpu_id"] == solver.chosen.gpu_id
    assert r["selected"]["data_center"] == solver.chosen.data_center


@pytest.mark.parametrize(
    "fault", ["stale", "foreign_job", "foreign_option", "duplicate", "crash", "mutate"]
)
def test_invalid_solver_never_produces_selection_or_leaks_exception(task, candidates, fault):
    class Broken:
        name = "broken-test"

        def solve(self, problem, *, time_limit_ms):
            job = problem.jobs[0]
            assignment = Assignment(job.key, job.options[0].key)
            if fault == "crash":
                raise RuntimeError("PRIVATE_SENTINEL")
            if fault == "mutate":
                job.options[0].hourly_microusd = 0
            return SolverResult(
                "stale" if fault == "stale" else problem.snapshot_id,
                "feasible",
                (assignment, assignment)
                if fault == "duplicate"
                else (
                    Assignment("unknown", assignment.option_key)
                    if fault == "foreign_job"
                    else Assignment(job.key, "unknown")
                    if fault == "foreign_option"
                    else assignment,
                ),
            )

    r = propose(report(task, candidates), Broken())
    assert r["selected"] is None and r["candidate_queue"] == []
    assert r["blockers"] == ["solver_failed"]
    assert "PRIVATE_SENTINEL" not in json.dumps(r)


def test_late_solution_is_rejected_and_not_replaced_silently(task, candidates):
    clock = [0.0]

    class Late(RankedFirstSolver):
        def solve(self, problem, *, time_limit_ms):
            clock[0] = 1.0
            return super().solve(problem, time_limit_ms=time_limit_ms)

    r = propose(report(task, candidates), Late(), now=lambda: clock[0], time_limit_ms=100)
    assert r["selected"] is None
    assert r["blockers"] == ["solver_deadline_exceeded"]


def test_shared_budget_slots_and_one_assignment_per_job_are_independently_enforced():
    option = PlacementOption("o", "gpu", "dc", 500_000, 700_000, "HIGH")
    problem = PlanningProblem(
        "snapshot", (PlanningJob("a", (option,)), PlanningJob("b", (option,))), 2, 1_000_000
    )
    two = SolverResult("snapshot", "feasible", (Assignment("a", "o"), Assignment("b", "o")))
    with pytest.raises(ValueError, match="budget_exceeded"):
        validate_solution(problem, two)
    with pytest.raises(ValueError, match="result_invalid"):
        validate_solution(replace(problem, max_assignments=1, budget_microusd=2_000_000), two)
    with pytest.raises(ValueError, match="assignment_invalid"):
        validate_solution(
            replace(problem, budget_microusd=2_000_000),
            replace(two, assignments=(Assignment("a", "o"), Assignment("a", "o"))),
        )
    validate_solution(problem, replace(two, assignments=(Assignment("a", "o"),)))
    with pytest.raises(ValueError, match="single_job"):
        RankedFirstSolver().solve(problem, time_limit_ms=100)


def test_microdollar_conversion_never_rounds_cost_down():
    assert microusd("0.0000001") == 1
    assert microusd("1.000167") == 1_000_167
    for value in ["NaN", "Infinity", "-1"]:
        with pytest.raises(ValueError, match="cost_invalid"):
            microusd(value)
