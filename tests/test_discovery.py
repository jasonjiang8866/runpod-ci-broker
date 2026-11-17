"""Search behavior against documented response fixtures; no live GPU claims."""

import io
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError

import pytest

from runpod_ci_broker.discovery import (
    ResourceFinder,
    SearchHistory,
    SearchPolicy,
    capacity_feedback,
    rank_candidates,
)
from runpod_ci_broker.fake import FakeProvider
from runpod_ci_broker.lifecycle import Engine
from runpod_ci_broker.live import CreateHTTPError
from runpod_ci_broker.models import Task
from runpod_ci_broker.provider import ProviderError, Transport
from runpod_ci_broker.storage import Journal


class Clock:
    def __init__(self):
        self.elapsed = 0.0
        self.waits = []

    def now(self):
        return self.elapsed

    def wall(self):
        return 1800000000.0 + self.elapsed

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.elapsed += seconds


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()

    class Dates:
        @staticmethod
        def now(tz):
            return datetime.fromtimestamp(clock.wall(), UTC)

    monkeypatch.setattr("runpod_ci_broker.provider.datetime", Dates)
    return clock


class Catalog:
    def __init__(self, task, frames=None):
        self.task, self.frames = task, frames or [["LOW", "HIGH"]]
        self.calls = []
        self.round = 0

    def get(self, path, params):
        self.calls.append((path, params.copy()))
        if path == "/catalog/gpus":
            grades = self.frames[min(self.round, len(self.frames) - 1)]
            self.round += 1
            return {
                "gpus": [
                    {
                        "id": gpu,
                        "memory": 24,
                        "price": {"secure": 0.5 + i / 10},
                        "availability": grade,
                        "dataCenters": [{"id": f"DC-{i}", "availability": grade}],
                        "cudaVersions": [{"version": "13.2", "available": True}],
                    }
                    for i, (gpu, grade) in enumerate(zip(self.task.gpu_allowlist, grades))
                ]
            }
        if path == "/pods":
            return {"pods": [], "pagination": {"hasNextPage": False}}
        if path == "/catalog/datacenters":
            return {"dataCenters": [{"id": f"DC-{i}"} for i in range(4)]}
        raise AssertionError(path)


def finder(task, clock, catalog, policy=None, history=None):
    return ResourceFinder(
        task,
        policy,
        history,
        get=catalog.get,
        now=clock.now,
        wall_now=clock.wall,
        sleep=clock.sleep,
        jitter=lambda: 0,
    )


def test_availability_ranks_all_models_and_centers_without_relaxing_constraints(task, clock):
    c = Catalog(task)
    r = finder(task, clock, c).run()
    assert r["state"] == "CANDIDATES_FOUND"
    assert r["selected"]["gpu_id"] == task.gpu_allowlist[1]
    assert [q["availability_signal"] for q in r["candidate_queue"]] == ["HIGH", "LOW"]
    assert r["selection_reason"]["objective"] == "availability"
    assert r["resource_search"]["http_requests"] == 3
    assert not r["paid_authorized"] and not any(r["facts"].values())
    assert all(
        params.get("minCudaVersion") == "12.8" and "cudaVersions" not in params
        for path, params in c.calls
        if path == "/catalog/gpus"
    )


@pytest.mark.parametrize("objective", ["preference", "price"])
def test_explicit_objectives_change_only_ranking(task, clock, objective):
    r = finder(task, clock, Catalog(task), SearchPolicy(objective=objective)).run()
    assert r["selected"]["gpu_id"] == task.gpu_allowlist[0]
    assert len(r["candidate_queue"]) == 2


def test_live_resource_search_rechecks_until_stock_returns(task, clock):
    c = Catalog(task, [["NONE", "NONE"], ["NONE", "NONE"], ["NONE", "HIGH"]])
    r = finder(task, clock, c).run()
    assert r["state"] == "CANDIDATES_FOUND"
    assert clock.waits == [5, 10]
    assert r["resource_search"]["snapshot_count"] == 3
    assert r["resource_search"]["http_requests"] == 9
    assert [x["selected"] is None for x in r["resource_search"]["rounds"]] == [True, True, False]
    assert r["resource_search"]["rounds"][0]["next_query_in_seconds"] == 5


def test_no_capacity_uses_bounded_wait_and_request_budget(task, clock):
    c = Catalog(task, [["NONE", "NONE"]])
    r = finder(task, clock, c).run()
    assert r["state"] == "SEARCH_EXHAUSTED"
    assert clock.now() <= task.limits.max_wait_seconds
    assert c.round <= task.limits.max_queries
    assert r["resource_search"]["stop_reason"] == "search_deadline_reached"
    assert not r["candidate_queue"]


@pytest.mark.parametrize("endpoint", ["/catalog/gpus", "/pods", "/catalog/datacenters"])
@pytest.mark.parametrize(
    "code", ["authentication_failed", "permission_denied", "account_restricted"]
)
def test_terminal_api_errors_never_retry_or_advance_to_more_requests(task, clock, endpoint, code):
    c = Catalog(task)
    original = c.get

    def get(path, params):
        if path == endpoint:
            c.calls.append((path, params))
            raise ProviderError(code)
        return original(path, params)

    c.get = get
    r = finder(task, clock, c).run()
    assert r["state"] == "SEARCH_BLOCKED" and r["selected"] is None
    assert not clock.waits and c.calls[-1][0] == endpoint
    assert r["resource_search"]["stop_reason"] == code


def test_rate_limit_retries_with_backoff_but_no_stale_candidates(task, clock):
    c = Catalog(task)
    original = c.get

    def get(path, params):
        if not c.calls:
            c.calls.append((path, params))
            raise ProviderError("rate_limited", retryable=True)
        return original(path, params)

    c.get = get
    r = finder(task, clock, c).run()
    assert r["state"] == "CANDIDATES_FOUND" and clock.waits == [5]
    assert r["resource_search"]["http_requests"] == 4
    assert r["resource_search"]["rounds"][0]["selected"] is None


def test_partial_paginated_inventory_cannot_exceed_total_request_budget(task, clock):
    c = Catalog(task)
    original = c.get

    def get(path, params):
        if path == "/pods":
            c.calls.append((path, params))
            return {
                "pods": [{"id": str(len(c.calls)), "status": "RUNNING", "env": {}}],
                "pagination": {"hasNextPage": True, "nextCursor": str(len(c.calls))},
            }
        return original(path, params)

    c.get = get
    r = finder(task, clock, c, SearchPolicy(max_http_requests=4)).run()
    assert len(c.calls) == 4 and r["selected"] is None
    assert r["resource_search"]["stop_reason"] == "search_request_limit"
    assert r["state"] == "SEARCH_EXHAUSTED" and not clock.waits


def test_response_arriving_after_deadline_never_becomes_a_quote(task, clock):
    c = Catalog(task)
    original = c.get

    def slow(path, params):
        result = original(path, params)
        clock.sleep(25)
        return result

    c.get = slow
    r = finder(task, clock, c).run()
    assert r["selected"] is None and not r["candidate_queue"]
    assert len(c.calls) == 1
    assert r["resource_search"]["stop_reason"] == "search_deadline_reached"


def test_capacity_feedback_rotates_placement_then_expires(task, candidates):
    history = SearchHistory(config_fingerprint=task.fingerprint())
    first = candidates[0]
    other_center = first.model_copy(update={"data_center": "ANOTHER-DC"})
    options = [first, other_center, candidates[1]]
    history = capacity_feedback(
        task, first, history, ProviderError("no_capacity"), 100.0, mode="mock"
    )
    r = rank_candidates(task, options, SearchPolicy(), history, 101.0, mode="mock")
    assert r["selected"]["gpu_id"] == first.gpu_id
    assert r["selected"]["data_center"] == "ANOTHER-DC"
    assert (
        next(d for d in r["decisions"] if d["candidate"] == first.model_dump())[
            "cooldown_remaining_seconds"
        ]
        == 59
    )
    r = rank_candidates(task, [first, candidates[1]], SearchPolicy(), history, 160.0, mode="mock")
    assert r["selected"] == first.model_dump()


def test_feedback_is_bound_to_task_and_rejects_future_events(task, candidates):
    h = SearchHistory(config_fingerprint="f" * 64)
    with pytest.raises(ValueError, match="task_mismatch"):
        rank_candidates(task, candidates, SearchPolicy(), h, 100.0, mode="mock")
    h = SearchHistory(
        config_fingerprint=task.fingerprint(),
        refusals=[
            {
                "gpu_id": candidates[0].gpu_id,
                "data_center": candidates[0].data_center,
                "observed_at": 101.0,
            }
        ],
    )
    with pytest.raises(ValueError, match="history_invalid"):
        rank_candidates(task, candidates, SearchPolicy(), h, 100.0, mode="mock")


@pytest.mark.parametrize("status", [400, 403, 500])
def test_only_documented_definitive_capacity_refusals_enter_history(task, candidates, status):
    error = CreateHTTPError(
        HTTPError(
            "https://invalid",
            status,
            "error",
            {},
            io.BytesIO(
                b'{"detail":"There are no longer any instances available with the requested specifications. Please refresh and try again."}'
            ),
        )
    )
    h = SearchHistory(config_fingerprint=task.fingerprint())
    if status == 400:
        assert len(capacity_feedback(task, candidates[0], h, error, 100.0).refusals) == 1
    else:
        with pytest.raises(ValueError, match="definitive_refusal"):
            capacity_feedback(task, candidates[0], h, error, 100.0)
        assert not h.refusals


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"hourly_usd": 99.0}, "price_exceeded"),
        ({"memory_gb": 1.0}, "insufficient_vram"),
        ({"cuda_versions": ["12.7"]}, "cuda_incompatible"),
        ({"cloud": "COMMUNITY"}, "cloud_mismatch"),
        ({"gpu_count": 2}, "gpu_count_mismatch"),
    ],
)
def test_cooling_never_relaxes_hard_requirements(task, candidates, changes, reason):
    h = capacity_feedback(
        task,
        candidates[0],
        SearchHistory(config_fingerprint=task.fingerprint()),
        ProviderError("no_capacity"),
        100.0,
        mode="mock",
    )
    remaining = candidates[1].model_copy(update=changes)
    r = rank_candidates(task, [candidates[0], remaining], SearchPolicy(), h, 101.0, mode="mock")
    assert not r["candidate_queue"] and r["selected"] is None
    assert any(reason in d["reasons"] for d in r["decisions"])


def test_locked_benchmark_does_not_borrow_another_gpu(candidates):
    task = Task.model_validate_json(Path("examples/flashattention-hardware.json").read_text())
    target = next(c for c in candidates if c.gpu_id in task.gpu_allowlist)
    h = capacity_feedback(
        task,
        target,
        SearchHistory(config_fingerprint=task.fingerprint()),
        ProviderError("no_capacity"),
        100.0,
        mode="mock",
    )
    r = rank_candidates(task, candidates, SearchPolicy(), h, 101.0, mode="mock")
    assert r["selected"] is None


def test_simulator_capacity_race_switches_gpu_preserving_intent_and_budget(
    tmp_path, task, candidates, auth
):
    p = FakeProvider(candidates, faults={"capacity_race": 1})
    j = Journal(tmp_path, task.scope, task.experiment_id)
    e = Engine(task, p, j, tmp_path / "results", search_policy=SearchPolicy())
    r = e.run(auth)
    assert r["state"] == "ACCEPTED" and r["attempts"] == 2
    assert r["planning_history"][0]["selected"]["gpu_id"] != r["selected"]["gpu_id"]
    assert len(r["resource_search"]["history"]["refusals"]) == 1
    assert r["budget"]["reserved_attempts"] == 2
    e.run(auth)
    assert p.data["create_calls"] == 2
    with pytest.raises(ValueError, match="search_policy_changed"):
        Engine(task, p, j, tmp_path / "results", search_policy=SearchPolicy(objective="price"))


@pytest.mark.parametrize(
    "fault", ["create_lost", "auth_error", "permission_error", "account_error"]
)
def test_simulator_noncapacity_failure_never_switches_or_creates_twice(
    tmp_path, task, candidates, auth, fault
):
    p = FakeProvider(candidates, faults={fault: 1})
    e = Engine(
        task,
        p,
        Journal(tmp_path, task.scope, task.experiment_id),
        tmp_path / "results",
        search_policy=SearchPolicy(),
    )
    r = e.run(auth)
    assert p.data["create_calls"] == 1
    assert r["resource_search"]["history"]["refusals"] == []


def test_discovery_cli_uses_get_only_and_writes_queue(tmp_path, task, monkeypatch, capsys):
    from runpod_ci_broker.cli import main

    catalog = Catalog(task)
    monkeypatch.setenv("RUNPOD_API_KEY", "PRIVATE_SENTINEL")
    monkeypatch.setattr(Transport, "get", lambda self, path, params: catalog.get(path, params))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "broker",
            "--config",
            "examples/simplexlora-flexible.json",
            "--action",
            "discover",
            "--output",
            str(tmp_path),
        ],
    )
    assert main() == 0
    r = json.loads((tmp_path / "report.json").read_text())
    assert r["state"] == "CANDIDATES_FOUND" and len(r["candidate_queue"]) == 2
    assert not r["resource_search"]["paid_resources_created"]
    assert (
        "PRIVATE_SENTINEL" not in capsys.readouterr().out + (tmp_path / "report.json").read_text()
    )


def test_discovery_missing_credentials_still_reports(tmp_path, monkeypatch):
    from runpod_ci_broker.cli import main

    monkeypatch.delenv("RUNPOD_API_KEY", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "broker",
            "--config",
            "examples/simplexlora-flexible.json",
            "--action",
            "discover",
            "--output",
            str(tmp_path),
        ],
    )
    assert main() == 2
    r = json.loads((tmp_path / "report.json").read_text())
    assert r["state"] == "SEARCH_BLOCKED" and r["selected"] is None
    assert r["resource_search"]["stop_reason"] == "credentials_missing"


def test_transport_cannot_start_after_search_deadline(monkeypatch):
    monkeypatch.setenv("RUNPOD_API_KEY", "fake-secret")
    monkeypatch.setattr("runpod_ci_broker.provider.time.monotonic", lambda: 10.0)

    def unexpected(*args, **kwargs):
        pytest.fail("expired search must not open a connection")

    monkeypatch.setattr("runpod_ci_broker.provider.build_opener", unexpected)
    with pytest.raises(ProviderError, match="deadline"):
        Transport(deadline=9.0).get("/catalog/gpus", {})


def test_solver_failure_stops_search_without_extra_api_reads(task, clock):
    class Broken:
        name = "broken-test"

        def solve(self, problem, *, time_limit_ms):
            raise RuntimeError("PRIVATE_SENTINEL")

    c = Catalog(task)
    search = finder(task, clock, c)
    search.solver = Broken()
    r = search.run()
    assert r["state"] == "SEARCH_BLOCKED" and not r["candidate_queue"]
    assert len(c.calls) == 3 and not clock.waits
    assert "PRIVATE_SENTINEL" not in json.dumps(r)


def test_default_preference_selects_4090_only_when_stock_is_equal(task, clock):
    task.gpu_preference = []
    equal = finder(task, clock, Catalog(task, [["HIGH", "HIGH"]])).run()
    assert "4090" in equal["selected"]["gpu_id"]
    unequal = finder(task, clock, Catalog(task, [["HIGH", "LOW"]])).run()
    assert unequal["selected"]["gpu_id"] == task.gpu_allowlist[0]


def test_all_placements_cooling_waits_for_expiry_and_refreshes(task, clock):
    h = SearchHistory(
        config_fingerprint=task.fingerprint(),
        refusals=[
            {"gpu_id": gpu, "data_center": f"DC-{i}", "observed_at": clock.wall()}
            for i, gpu in enumerate(task.gpu_allowlist[:2])
        ],
    )
    c = Catalog(task)
    r = finder(task, clock, c, SearchPolicy(cooldown_seconds=2), h).run()
    assert clock.waits == [2]
    assert c.round == 2 and len(c.calls) == 6
    assert r["state"] == "CANDIDATES_FOUND"
    assert r["resource_search"]["rounds"][0]["selected"] is None
    assert r["candidate_valid_until"] == clock.wall() + 30


@pytest.mark.parametrize("blocked_by", ["budget", "deadline"])
def test_no_network_when_local_admission_prevents_search(task, clock, blocked_by):
    if blocked_by == "budget":
        task.limits.total_budget_usd = None
    else:
        task.limits.max_wait_seconds = 0
    c = Catalog(task)
    r = finder(task, clock, c).run()
    assert not c.calls and r["selected"] is None
    assert r["resource_search"]["http_requests"] == 0


def test_stale_snapshot_then_failed_refresh_never_reuses_old_candidates(task, clock, monkeypatch):
    from runpod_ci_broker.provider import ReadOnlyRunPod

    c = Catalog(task)
    snapshot = ReadOnlyRunPod.snapshot

    def expire(self, query_task):
        candidates, observed = snapshot(self, query_task)
        observed["observed_at"] = datetime.fromtimestamp(clock.wall() - 31, UTC).isoformat()
        return candidates, observed

    monkeypatch.setattr(ReadOnlyRunPod, "snapshot", expire)
    original = c.get

    def lose_permission(path, params):
        if c.round >= 1 and path == "/catalog/gpus":
            c.calls.append((path, params))
            raise ProviderError("permission_denied")
        return original(path, params)

    c.get = lose_permission
    r = finder(task, clock, c).run()
    assert r["state"] == "SEARCH_BLOCKED"
    assert r["selected"] is None and not r["candidate_queue"]
    assert r["candidate_valid_until"] is None
    assert clock.waits == [5]
    assert "catalog_observation_stale" in r["resource_search"]["rounds"][0]["blockers"]
    assert r["resource_search"]["stop_reason"] == "permission_denied"


def test_optional_diversification_explores_other_models_without_excluding_valid_fallback(
    task, candidates
):
    first = candidates[0]
    same_gpu = first.model_copy(update={"data_center": "NEW-DC"})
    other = candidates[1].model_copy(update={"data_center": "OTHER-DC"})
    history = capacity_feedback(
        task,
        first,
        SearchHistory(config_fingerprint=task.fingerprint()),
        ProviderError("no_capacity"),
        100.0,
        mode="mock",
    )
    policy = SearchPolicy(diversify_after_refusal=True)
    r = rank_candidates(task, [first, same_gpu, other], policy, history, 101.0, mode="mock")
    assert r["selected"] == other.model_dump()
    assert same_gpu.model_dump() in [x["candidate"] for x in r["candidate_queue"]]
    assert r["selection_reason"]["recent_gpu_refusals"] == 0
    expensive = other.model_copy(update={"hourly_usd": 100.0})
    r = rank_candidates(task, [first, same_gpu, expensive], policy, history, 101.0, mode="mock")
    assert r["selected"] == same_gpu.model_dump()
    r = rank_candidates(task, [first, other], policy, history, 160.0, mode="mock")
    assert r["selected"] == first.model_dump()
