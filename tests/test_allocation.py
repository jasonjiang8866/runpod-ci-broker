"""Exercise the task-bound controller on an injected cloud. No paid resources."""

import copy
import hashlib
import io
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError

import pytest

from runpod_ci_broker.allocation import BoundedController, ExecutionPermit
from runpod_ci_broker.discovery import SearchPolicy
from runpod_ci_broker.export import ResultHTTPError
from runpod_ci_broker.handover import ControllerJournal, GitHubEvidence
from runpod_ci_broker.ledger import LedgerError, SQLiteStore
from runpod_ci_broker.live import CreateHTTPError
from runpod_ci_broker.models import Authorization, Task
from runpod_ci_broker.optimization import microusd
from runpod_ci_broker.planner import estimate
from runpod_ci_broker.provider import ProviderError
from runpod_ci_broker.report import write_report
from runpod_ci_broker.results import save_bundle

CAPACITY = "There are no longer any instances available with the requested specifications. Please refresh and try again."
IDENTITY = {
    "run_id": "2468",
    "attempt": 1,
    "sha": "c" * 40,
    "path": ".github/workflows/gpu-smoke.yml",
    "event": "workflow_dispatch",
}


class Cloud:
    def __init__(self, task):
        self.task, self.clock, self.pods = task, 1800000000.0, {}
        self.outcomes, self.creates, self.reads, self.deletes = [], [], [], []
        self.stock = "HIGH"

    def now(self):
        return self.clock

    def sleep(self, seconds):
        self.clock += seconds

    def read(self, path, params):
        self.reads.append((path, params))
        if path == "/catalog/gpus":
            return {
                "gpus": [
                    {
                        "id": gpu,
                        "memory": 32,
                        "price": {"secure": 0.5},
                        "availability": self.stock,
                        "dataCenters": [{"id": f"DC-{i}", "availability": self.stock}],
                        "cudaVersions": [{"version": "13.2", "available": True}],
                    }
                    for i, gpu in enumerate(self.task.gpu_allowlist)
                ]
            }
        if path == "/pods":
            return {"pods": self.inventory(), "pagination": {"hasNextPage": False}}
        if path == "/catalog/datacenters":
            return {"dataCenters": [{"id": f"DC-{i}"} for i in range(len(self.task.gpu_allowlist))]}
        raise AssertionError(path)

    def create(self, body):
        self.creates.append(copy.deepcopy(body))
        outcome = self.outcomes.pop(0) if self.outcomes else "ok"
        if outcome in {"capacity", "403", "500"}:
            status = 400 if outcome == "capacity" else int(outcome)
            raise CreateHTTPError(
                HTTPError(
                    "https://PRIVATE_SENTINEL",
                    status,
                    "PRIVATE_SENTINEL",
                    {},
                    io.BytesIO(json.dumps({"detail": CAPACITY}).encode()),
                )
            )
        if outcome == "unclassified":
            raise ProviderError("no_capacity", retryable=True)
        if outcome == "unknown":
            raise ProviderError("create_response_unknown", ambiguous=True)
        pod_id = "pod" + str(len(self.creates))
        self.pods[pod_id] = {
            **copy.deepcopy(body),
            "id": pod_id,
            "status": "RUNNING",
            "dataCenterId": body["dataCenterIds"][0],
            "cost": 0.5,
        }
        if outcome == "lost":
            raise ProviderError("create_response_unknown", ambiguous=True)
        if outcome == "crash":
            raise RuntimeError("simulated_process_loss")
        return pod_id

    def inventory(self):
        return copy.deepcopy(list(self.pods.values()))

    def get(self, pod_id, timeout):
        return copy.deepcopy(self.pods.get(pod_id))

    def delete(self, pod_id, timeout):
        self.deletes.append(pod_id)
        self.pods.pop(pod_id, None)

    def active_ids(self, deadline):
        return set(self.pods)

    def startup_logs(self, pod_id, timeout):
        return {}


@pytest.fixture
def allocation(tmp_path, monkeypatch):
    task = Task.model_validate_json(Path("examples/repair-smoke.json").read_text())
    task.experiment_id, task.scope = "allocation-test", "bounded-test-scope"
    task.limits.prior_spend_usd = 0.0
    task.limits.max_start_attempts = 2
    task.limits.total_budget_usd = 2.0
    task.limits.max_wait_seconds = 60
    task.limits.startup_seconds = 30
    task.limits.collect_seconds = 30
    task.limits.cleanup_seconds = 30
    permit = ExecutionPermit(
        authorization=Authorization(
            experiment_id=task.experiment_id,
            config_fingerprint=task.fingerprint(),
            approved_budget_usd=2.0,
            gpu_allowlist=task.gpu_allowlist,
            runtime_seconds=60,
            explicitly_approved=True,
        ),
        controller_sha=IDENTITY["sha"],
        window_start=1799999999.0,
        window_end=1800001000.0,
    )
    cloud = Cloud(task)

    class Dates:
        @staticmethod
        def now(tz):
            return datetime.fromtimestamp(cloud.now(), UTC)

    monkeypatch.setattr("runpod_ci_broker.provider.datetime", Dates)
    journal = ControllerJournal(
        SQLiteStore(tmp_path / "ledger.db"),
        task.scope,
        task.experiment_id,
        IDENTITY,
        lambda _: True,
    )
    c = BoundedController(
        task,
        permit,
        journal,
        cloud,
        cloud.read,
        tmp_path / "output",
        now=cloud.now,
        sleep=cloud.sleep,
    )
    return c, cloud


def recreate(c, cloud, **kwargs):
    return BoundedController(
        c.task,
        c.permit,
        c.j,
        cloud,
        cloud.read,
        c.output,
        policy=c.policy,
        now=cloud.now,
        sleep=cloud.sleep,
        **kwargs,
    )


def test_capacity_refusal_switches_gpu_inside_one_original_reservation(allocation):
    c, cloud = allocation
    cloud.outcomes = ["capacity", "ok"]
    r = c.start(confirmed=True)
    assert r["state"] == "STARTING" and len(cloud.creates) == 2
    assert [a["outcome"] for a in r["attempt_log"]] == ["capacity_refused", "acknowledged"]
    assert cloud.creates[0]["gpu"]["id"] != cloud.creates[1]["gpu"]["id"]
    assert cloud.creates[0]["env"]["RCB_ATTEMPT_ID"] != cloud.creates[1]["env"]["RCB_ATTEMPT_ID"]
    assert r["allocation"]["reserved_total_microusd"] == microusd(
        estimate(c.task)["estimated_total_usd"]
    )
    assert len(r["resource_search"]["history"]["refusals"]) == 1
    original = copy.deepcopy(r["attempt_log"])
    recreate(c, cloud).start(confirmed=True)
    assert c.j.read()["attempt_log"] == original and len(cloud.creates) == 2
    assert "PRIVATE_SENTINEL" not in json.dumps(r)


def test_attempt_exhaustion_does_not_reset_on_resume(allocation):
    c, cloud = allocation
    cloud.outcomes = ["capacity"] * 3
    r = c.start(confirmed=True)
    assert r["state"] == "NO_CAPACITY" and r["attempts"] == 2 and not r["create_intent"]
    for _ in range(2):
        recreate(c, cloud).start(confirmed=True)
    assert len(cloud.creates) == 2


@pytest.mark.parametrize("outcome", ["403", "500", "unclassified", "unknown", "lost"])
def test_noncapacity_failure_never_falls_back(allocation, outcome):
    c, cloud = allocation
    cloud.outcomes = [outcome]
    r = c.start(confirmed=True)
    recreate(c, cloud).start(confirmed=True)
    assert len(cloud.creates) == 1 and not r["resource_search"]["history"]["refusals"]
    assert r["state"] == ("PREFLIGHT_BLOCKED" if outcome == "403" else "CREATE_UNKNOWN")
    assert r["create_intent"] is (outcome != "403")


@pytest.mark.parametrize("outcome", ["lost", "crash"])
def test_lost_ack_or_process_crash_adopts_and_cleans_without_second_post(allocation, outcome):
    c, cloud = allocation
    cloud.outcomes = [outcome]
    if outcome == "crash":
        with pytest.raises(RuntimeError, match="process_loss"):
            c.start(confirmed=True)
    else:
        c.start(confirmed=True)
    c = recreate(c, cloud)
    c.start(confirmed=True)
    r = c.reconcile()
    assert r["pod_id"] == "pod1" and r["attempt_log"][0]["allocation_confirmed_at"] == cloud.now()
    r = c.cleanup()
    assert r["state"] == "REJECTED" and r["facts"]["pod_deleted_confirmed"]
    assert cloud.deletes == ["pod1"] and len(cloud.creates) == 1


def test_empty_inventory_after_unknown_is_never_permission_to_retry(allocation):
    c, cloud = allocation
    cloud.outcomes = ["unknown"]
    c.start(confirmed=True)
    c.reconcile()
    c.cleanup()
    r = recreate(c, cloud).start(confirmed=True)
    assert r["create_intent"] and not r["facts"]["pod_deleted_confirmed"]
    assert len(cloud.creates) == 1


def test_intent_and_all_attempt_budget_are_confirmed_before_post(allocation):
    c, cloud = allocation
    original = cloud.create

    def create(body):
        saved = c.j.read()
        assert saved["state"] == "CREATING" and saved["create_intent"]
        assert saved["allocation"]["reserved_total_microusd"] == microusd(
            c.cost["estimated_total_usd"]
        )
        assert saved["attempt_log"][-1]["intent_id"] == body["env"]["RCB_ATTEMPT_ID"]
        assert body["env"]["RCB_RESULT_TOKEN"] not in json.dumps(saved)
        return original(body)

    cloud.create = create
    c.start(confirmed=True)


@pytest.mark.parametrize("fault", ["write_failure", "slow_write"])
def test_failed_or_stale_intent_checkpoint_never_posts(allocation, fault):
    c, cloud = allocation
    original = c.save

    def save(r, state=None):
        original(r, state)
        if state == "CREATING":
            if fault == "write_failure":
                raise LedgerError("ledger_write_unconfirmed")
            cloud.sleep(31)

    c.save = save
    if fault == "write_failure":
        with pytest.raises(LedgerError):
            c.start(confirmed=True)
    else:
        assert c.start(confirmed=True)["state"] == "TIMED_OUT"
    recreate(c, cloud).start(confirmed=True)
    assert not cloud.creates and c.j.read()["attempts"] == 1


def test_original_window_blocks_new_creation_but_allows_cleanup(allocation):
    c, cloud = allocation
    c.start(confirmed=True)
    cloud.clock = c.permit.window_end + 1
    r = recreate(c, cloud).cleanup()
    assert r["facts"]["pod_deleted_confirmed"] and len(cloud.creates) == 1


def test_no_capacity_search_and_pagination_obey_persisted_bounds(allocation):
    c, cloud = allocation
    cloud.stock = "NONE"
    r = c.start(confirmed=True)
    assert not cloud.creates and r["query_count"] == c.task.limits.max_queries
    assert r["allocation"]["http_requests"] == len(cloud.reads)
    assert r["allocation"]["reserved_total_microusd"] == 0
    before = len(cloud.reads)
    recreate(c, cloud).start(confirmed=True)
    assert len(cloud.reads) == before


def test_http_budget_includes_paginated_inventory_and_precreate_read(allocation):
    c, cloud = allocation
    c.policy = SearchPolicy(max_http_requests=3)
    r = c.start(confirmed=True)
    assert not cloud.creates and len(cloud.reads) == 3
    assert r["state"] == "PREFLIGHT_BLOCKED"


def test_permit_and_task_budget_cannot_change_on_resume(allocation):
    c, cloud = allocation
    c.start(confirmed=True)
    c.permit.window_end += 100
    with pytest.raises(ValueError, match="record_mismatch"):
        recreate(c, cloud).start(confirmed=True)
    assert len(cloud.creates) == 1


def test_old_attempt_marker_cannot_be_adopted_or_deleted(allocation):
    c, cloud = allocation
    cloud.outcomes = ["capacity", "lost"]
    r = c.start(confirmed=True)
    cloud.pods["pod2"]["env"]["RCB_ATTEMPT_ID"] = r["attempt_log"][0]["intent_id"]
    assert c.cleanup()["state"] == "CLEANUP_PENDING"
    assert not cloud.deletes


def export_fixture(c, cloud, monkeypatch):
    class Reader:
        def __init__(self, *args):
            pass

        def readiness(self, *args):
            return "ready"

    def collect(task, pod_id, reader, destination, **kwargs):
        gpu = cloud.pods[pod_id]["gpu"]["id"]
        architectures = {"NVIDIA GeForce RTX 4090": "sm_89", "NVIDIA GeForce RTX 3090": "sm_86"}
        observation = {
            "device_name": gpu,
            "device_uuid": "a" * 36,
            "device_count": 1,
            "memory_bytes": 32 * 1024**3,
            "driver_cuda_version": 13000,
            "architecture": architectures.get(gpu, "sm_120"),
            "memory_roundtrip_bytes": 4096,
            "memory_roundtrip_passed": True,
            "probe": "cuda_driver_memory_roundtrip_v1",
            "framework_runtime": "not_tested",
        }
        metrics = {"gpu_memory_smoke_passed": 1, "worker_exit_code": 0}
        files = {
            "metrics.json": json.dumps(metrics).encode(),
            "worker.log": b"offline fixture",
            "environment.json": json.dumps({"observed": observation}).encode(),
        }
        manifest = {
            "config_fingerprint": task.fingerprint(),
            "source_sha": task.source_sha,
            "image": task.image,
            "pod_id": pod_id,
            "metrics": metrics,
            "files": {
                k: {"bytes": len(v), "sha256": hashlib.sha256(v).hexdigest()}
                for k, v in files.items()
            },
        }
        return save_bundle(task, pod_id, manifest, files, destination)

    monkeypatch.setattr("runpod_ci_broker.allocation.RunPodResultReader", Reader)
    monkeypatch.setattr("runpod_ci_broker.allocation.collect_https", collect)
    github = GitHubEvidence()
    monkeypatch.setattr(
        github,
        "get",
        lambda _: {
            "id": 123,
            "expired": False,
            "digest": "sha256:" + "d" * 64,
            "name": "allocation-bundle-2468-1"
            + (
                "-" + c.task.experiment_id
                if (c.j.read() or {}).get("allocation", {}).get("queue_dispatch")
                else ""
            ),
            "workflow_run": {"id": 2468, "head_sha": IDENTITY["sha"]},
        },
    )
    return github


def test_full_fallback_smoke_export_receipt_and_cleanup(allocation, monkeypatch):
    c, cloud = allocation
    cloud.outcomes = ["capacity", "ok"]
    github = export_fixture(c, cloud, monkeypatch)
    c.start(confirmed=True)
    r = c.collect()
    assert r["state"] == "COLLECTED" and r["gpu_verified"]
    assert not r["facts"]["results_saved"]
    c.receipt("123", "d" * 64, github)
    r = c.cleanup()
    assert r["state"] == "ACCEPTED" and all(r["facts"].values())
    assert not cloud.pods and len(cloud.creates) == 2 and cloud.deletes == ["pod2"]
    assert recreate(c, cloud).start(confirmed=True)["state"] == "ACCEPTED"
    assert len(r["resource_search"]["rounds"]) == 2
    assert r["allocation"]["estimated_compute_usd"] > 0
    write_report(r, c.output)
    summary = (c.output / "summary.md").read_text()
    assert "Bounded allocation" in summary and "capacity_refused" in summary
    assert "acknowledged" in summary and "supplier bill (USD): None" in summary
    for request in cloud.creates:
        assert request["env"]["RCB_RESULT_TOKEN"] not in (c.output / "report.json").read_text()


def test_collection_failure_terminates_and_does_not_retry_allocation(allocation, monkeypatch):
    c, cloud = allocation

    class Reader:
        def __init__(self, *args):
            pass

        def readiness(self, *args):
            raise ProviderError("result_permission_denied")

    monkeypatch.setattr("runpod_ci_broker.allocation.RunPodResultReader", Reader)
    c.start(confirmed=True)
    r = c.collect()
    assert r["state"] == "REJECTED" and r["facts"]["pod_deleted_confirmed"]
    assert not r["facts"]["results_saved"]
    c.start(confirmed=True)
    assert len(cloud.creates) == 1


def test_cancelled_waiting_task_cannot_reopen_its_search(allocation):
    c, cloud = allocation
    with c.j.lock():
        c.save(c.new_record(), "WAITING_CAPACITY")
    assert c.cancel()["state"] == "CANCELLED"
    assert recreate(c, cloud).start(confirmed=True)["state"] == "CANCELLED"
    assert not cloud.creates and not cloud.reads


def test_retryable_readiness_errors_obey_startup_deadline(allocation, monkeypatch):
    c, cloud = allocation

    calls = []

    def failing_logs(pod_id, timeout):
        calls.append((pod_id, timeout))
        raise OSError("PRIVATE_LOG")

    monkeypatch.setattr(cloud, "startup_logs", failing_logs)

    class Reader:
        def __init__(self, *args):
            pass

        def readiness(self, *args):
            raise ResultHTTPError(404, "readiness")

    monkeypatch.setattr("runpod_ci_broker.allocation.RunPodResultReader", Reader)
    c.start(confirmed=True)
    started = cloud.now()
    r = c.collect()
    assert r["facts"]["pod_deleted_confirmed"] and not r["facts"]["results_saved"]
    assert cloud.now() - started <= c.task.limits.startup_seconds + 3
    assert len(cloud.creates) == 1 and calls == [("pod1", 3)]
    assert r["startup_logs"]["status"] == "diagnostic_unavailable"
    assert r["result_http_failures"][-1]["http_status"] == 404
    assert r["startup_diagnostics"]["samples"][0]["pod_status"] == "RUNNING"
    assert r["attempt_log"][-1]["allocation_confirmed_at"] == started
    assert "PRIVATE_LOG" not in json.dumps(r)


def test_collection_preserves_ledger_failure_for_independent_recovery(allocation, monkeypatch):
    c, cloud = allocation
    c.start(confirmed=True)

    def fail():
        raise LedgerError("ledger_write_unconfirmed")

    def must_not_reacquire():
        raise AssertionError("must not mask the original ledger error")

    monkeypatch.setattr(c, "_collect", fail)
    monkeypatch.setattr(c.j, "lock", must_not_reacquire)
    with pytest.raises(LedgerError, match="^ledger_write_unconfirmed$"):
        c.collect()
    assert len(cloud.creates) == 1 and not cloud.deletes


def test_cancelled_active_task_cleans_without_restarting(allocation):
    c, cloud = allocation
    c.start(confirmed=True)
    r = c.cancel()
    assert r["cancelled"] and r["facts"]["pod_deleted_confirmed"]
    assert r["state"] == "REJECTED"
    recreate(c, cloud).start(confirmed=True)
    assert len(cloud.creates) == 1 and not cloud.pods


def test_cleanup_failure_keeps_scope_blocked_for_another_task(allocation):
    c, cloud = allocation
    c.start(confirmed=True)
    original = cloud.delete

    def unavailable(*args):
        raise ProviderError("provider_unavailable", retryable=True)

    cloud.delete = unavailable
    r = c.cleanup()
    assert r["state"] == "CLEANUP_PENDING" and r["create_intent"]
    assert not r["facts"]["pod_deleted_confirmed"]
    task = c.task.model_copy(update={"experiment_id": "second-task"})
    permit = c.permit.model_copy(deep=True)
    permit.authorization = permit.authorization.model_copy(
        update={"experiment_id": task.experiment_id, "config_fingerprint": task.fingerprint()}
    )
    journal = ControllerJournal(c.j.store, task.scope, task.experiment_id, IDENTITY, lambda _: True)
    second = BoundedController(
        task, permit, journal, cloud, cloud.read, c.output, now=cloud.now, sleep=cloud.sleep
    )
    with pytest.raises(ProviderError, match="unresolved"):
        second.start(confirmed=True)
    assert len(cloud.creates) == 1 and journal.read() is None
    cloud.delete = original
    assert recreate(c, cloud).cleanup()["facts"]["pod_deleted_confirmed"]


def test_manual_recovery_requires_previous_controller_completion(allocation):
    c, cloud = allocation
    c.start(confirmed=True)
    version, scope = c.j.load()
    scope["holder"] = "d" * 32
    c.j.store.swap(c.j.key, version, scope)
    identity = {
        **IDENTITY,
        "run_id": "2469",
        "path": ".github/workflows/recover-smoke.yml",
        "event": "workflow_run",
    }
    journal = ControllerJournal(
        c.j.store, c.task.scope, c.task.experiment_id, identity, lambda _: False, recovery=True
    )
    recovery = BoundedController(
        c.task, c.permit, journal, cloud, cloud.read, c.output, now=cloud.now, sleep=cloud.sleep
    )
    with pytest.raises(LedgerError, match="not_confirmed_completed"):
        recovery.cleanup()
    with pytest.raises(ValueError, match="manual_allocation"):
        recovery.start(confirmed=True)
    journal.completed = lambda _: True
    assert recovery.cleanup()["facts"]["pod_deleted_confirmed"]
    assert len(cloud.creates) == 1


@pytest.mark.parametrize(
    "boundary", ["confirmation", "window", "budget", "scope", "location", "identity"]
)
def test_invalid_admission_never_queries_or_allocates(allocation, boundary):
    c, cloud = allocation
    with pytest.raises(ValueError):
        if boundary == "confirmation":
            c.start()
        elif boundary == "window":
            cloud.clock = c.permit.window_end + 1
            c.start(confirmed=True)
        elif boundary == "budget":
            c.permit.authorization.approved_budget_usd = 0.01
            recreate(c, cloud)
        elif boundary == "scope":
            c.task.scope = "broker-central-smoke"
            recreate(c, cloud)
        elif boundary == "location":
            c.task.data_centers = ["DC-0"]
            recreate(c, cloud)
        else:
            c.j.identity = {**IDENTITY, "run_id": "invalid"}
            recreate(c, cloud)
    assert not cloud.creates and not cloud.reads


def test_permission_denial_during_inventory_does_not_query_more_endpoints(allocation):
    c, cloud = allocation
    original = cloud.read

    def denied(path, params):
        if path == "/pods":
            cloud.reads.append((path, params))
            raise ProviderError("permission_denied")
        return original(path, params)

    c.get = denied
    assert c.start(confirmed=True)["state"] == "PREFLIGHT_BLOCKED"
    assert [path for path, _ in cloud.reads] == ["/catalog/gpus", "/pods"]
    assert not cloud.creates


def test_late_upload_receipt_can_complete_verified_cleaned_task(allocation, monkeypatch):
    c, cloud = allocation
    github = export_fixture(c, cloud, monkeypatch)
    c.start(confirmed=True)
    c.collect()
    assert c.cleanup()["state"] == "REJECTED"
    assert c.receipt("123", "d" * 64, github)["state"] == "ACCEPTED"
    assert len(cloud.creates) == 1


def test_wrong_upload_receipt_cannot_claim_results_saved(allocation, monkeypatch):
    c, cloud = allocation
    github = export_fixture(c, cloud, monkeypatch)
    c.start(confirmed=True)
    c.collect()
    with pytest.raises(ValueError, match="receipt_unverified"):
        c.receipt("123", "f" * 64, github)
    assert not c.j.read()["facts"]["results_saved"]
    assert c.cleanup()["facts"]["pod_deleted_confirmed"]


def test_recovery_upload_receipt_binds_new_exporter_without_changing_intent(
    allocation, monkeypatch
):
    c, cloud = allocation
    c.start(confirmed=True)
    github = export_fixture(c, cloud, monkeypatch)
    identity = {
        **IDENTITY,
        "run_id": "2469",
        "sha": "e" * 40,
        "path": ".github/workflows/recover-allocation.yml",
        "event": "workflow_run",
    }
    journal = ControllerJournal(
        c.j.store, c.task.scope, c.task.experiment_id, identity, lambda _: True, recovery=True
    )
    recovery = BoundedController(
        c.task, c.permit, journal, cloud, cloud.read, c.output, now=cloud.now, sleep=cloud.sleep
    )
    recovery.collect()
    with pytest.raises(ValueError, match="receipt_unverified"):
        recovery.receipt("123", "d" * 64, github)
    monkeypatch.setattr(
        github,
        "get",
        lambda _: {
            "id": 123,
            "expired": False,
            "digest": "sha256:" + "d" * 64,
            "name": "allocation-bundle-2469-1",
            "workflow_run": {"id": 2469, "head_sha": "e" * 40},
        },
    )
    recovery.receipt("123", "d" * 64, github)
    r = recovery.cleanup()
    assert r["state"] == "ACCEPTED" and r["actions_run_id"] == "2468"
    assert r["artifact_receipt"]["run_id"] == "2469" and len(cloud.creates) == 1


def test_cleanup_inventory_receives_only_remaining_monotonic_budget(allocation, monkeypatch):
    import time

    c, cloud = allocation
    c.start(confirmed=True)
    budgets = []

    def inventory(deadline):
        budgets.append(deadline - time.monotonic())
        return set(cloud.pods)

    monkeypatch.setattr(cloud, "active_ids", inventory)
    assert c.cleanup()["facts"]["pod_deleted_confirmed"]
    assert len(budgets) == 2
    assert all(0 < remaining <= c.task.limits.cleanup_seconds for remaining in budgets)


def queue_run(c, cloud, *, latest=None, slots=1):
    from runpod_ci_broker.batch import BatchCatalog, Offer
    from runpod_ci_broker.queue import QueuePolicy
    from runpod_ci_broker.queue_execution import QueuedExecution

    run = QueuedExecution("offline-queue", c)
    run.queue.initialize(
        QueuePolicy(budget_microusd=5_000_000, opening_reserved_microusd=0, slots=slots)
    )
    run.queue.submit(c.task, latest_start=latest)
    # Choose a different GPU from the single-task preference order.
    catalog = BatchCatalog(
        observed_at=int(cloud.now()),
        complete=True,
        offers=[
            Offer(
                gpu_id=c.task.gpu_allowlist[0],
                data_center="DC-0",
                memory_gb=32,
                hourly_usd=0.5,
                capacity="available",
                cuda_versions=["13.2"],
            )
        ],
    )
    return run, catalog


def recovery_for(run, cloud, completed=True):
    from runpod_ci_broker.queue_execution import QueuedExecution

    original = run.controller
    identity = {
        **IDENTITY,
        "run_id": "9999",
        "path": ".github/workflows/recover-allocation.yml",
        "event": "workflow_run",
    }
    journal = ControllerJournal(
        original.j.store,
        original.task.scope,
        original.task.experiment_id,
        identity,
        lambda previous: completed and previous == IDENTITY,
        recovery=True,
    )
    controller = BoundedController(
        original.task,
        original.permit,
        journal,
        cloud,
        cloud.read,
        original.output,
        now=cloud.now,
        sleep=cloud.sleep,
    )
    return QueuedExecution(run.queue.name, controller)


def test_queue_handoff_uses_fresh_selected_quote_and_preserves_reservation(allocation):
    c, cloud = allocation
    run, catalog = queue_run(c, cloud)
    next_task = c.task.model_copy(update={"experiment_id": "next-smoke"})
    run.queue.submit(next_task, priority=-1)
    assert run.claim(catalog) == "claimed"
    assert run.queue.claim(next_task.scope, next_task.experiment_id, "waiting") == "slots_full"
    before = run.queue.status().reserved_microusd
    assert run.start(confirmed=True)["state"] == "STARTING"
    assert cloud.creates[0]["gpu"]["id"] == catalog.offers[0].gpu_id
    assert cloud.creates[0]["dataCenterIds"] == ["DC-0"]
    assert run.settle() == "cleanup_pending"
    c.cleanup()
    assert run.settle() == run.settle() == "verified_deleted"
    assert run.queue.status().reserved_microusd == before
    assert run.queue.claim(next_task.scope, next_task.experiment_id, "waiting") == "claimed"
    with pytest.raises(LedgerError, match="queue_claim_closed"):
        run.start(confirmed=True)
    assert len(cloud.creates) == 1


@pytest.mark.parametrize("failure", ["lost", "crash", "unknown", "delete_failed"])
def test_queue_recovery_after_post_keeps_unknown_owned(allocation, failure):
    c, cloud = allocation
    run, catalog = queue_run(c, cloud)
    run.claim(catalog)
    cloud.outcomes = [failure]
    if failure == "crash":
        with pytest.raises(RuntimeError):
            run.start(confirmed=True)
    else:
        run.start(confirmed=True)
    if failure == "delete_failed":
        cloud.delete = lambda *_: None
    recover = recovery_for(run, cloud)
    expected = "cleanup_pending" if failure in {"unknown", "delete_failed"} else "verified_deleted"
    assert recover.recover() == expected
    assert run.queue.status().slots_used == int(expected == "cleanup_pending")
    assert len(cloud.creates) == 1


def test_queue_claim_crash_before_journal_and_recovery_handover(allocation):
    c, cloud = allocation
    run, catalog = queue_run(c, cloud)
    assert run.claim(catalog) == "claimed"
    with pytest.raises(LedgerError, match="queue_launch_not_completed"):
        recovery_for(run, cloud, completed=False).recover()
    assert run.settle() == "awaiting_controller"
    assert recovery_for(run, cloud).recover() == "no_create"
    assert run.queue.status().slots_used == 0
    with pytest.raises(LedgerError, match="queue_claim_closed"):
        run.start(confirmed=True)
    assert not cloud.creates


@pytest.mark.parametrize("change", ["price", "site", "deadline", "stale_claim"])
def test_queue_quote_change_and_expired_claim(allocation, change):
    c, cloud = allocation
    run, catalog = queue_run(c, cloud, latest=int(cloud.now()) + 2)
    if change == "stale_claim":
        catalog.observed_at -= 31
        assert run.claim(catalog) == "catalog_observation_stale"
        assert run.queue.status().slots_used == 0
        return
    if change == "price":
        catalog.offers[0].hourly_usd = 0.4
    elif change == "site":
        catalog.offers[0].data_center = "other-site"
    run.claim(catalog)
    if change == "deadline":
        cloud.sleep(2)
    r = run.start(confirmed=True)
    assert r["state"] in {"TIMED_OUT", "PREFLIGHT_BLOCKED"}
    assert run.settle() == "no_create"
    assert not cloud.creates


def test_queue_classified_capacity_refusal_can_choose_another_gpu(allocation):
    c, cloud = allocation
    run, catalog = queue_run(c, cloud)
    run.claim(catalog)
    cloud.outcomes = ["capacity", "ok"]
    r = run.start(confirmed=True)
    assert r["state"] == "STARTING"
    assert [a["outcome"] for a in r["attempt_log"]] == ["capacity_refused", "acknowledged"]
    assert len({p["gpu"]["id"] for p in cloud.creates}) == 2


def test_two_queued_jobs_accept_and_hand_off_one_slot(allocation, monkeypatch):
    from runpod_ci_broker.queue_execution import QueuedExecution

    c, cloud = allocation
    first, catalog = queue_run(c, cloud)
    task = c.task.model_copy(update={"experiment_id": "second-smoke"})
    auth = c.permit.authorization.model_copy(
        update={"experiment_id": task.experiment_id, "config_fingerprint": task.fingerprint()}
    )
    journal = ControllerJournal(c.j.store, task.scope, task.experiment_id, IDENTITY, lambda _: True)
    second = QueuedExecution(
        first.queue.name,
        BoundedController(
            task,
            c.permit.model_copy(update={"authorization": auth}),
            journal,
            cloud,
            cloud.read,
            c.output.parent / "second",
            now=cloud.now,
            sleep=cloud.sleep,
        ),
    )
    first.queue.submit(task, priority=-1)
    assert first.claim(catalog) == "claimed"
    assert second.claim(catalog) == "slots_full"
    for run in [first, second]:
        if run is second:
            catalog.observed_at = int(cloud.now())
            assert run.claim(catalog) == "claimed"
        assert run.start(confirmed=True)["state"] == "STARTING"
        controller = run.controller
        github = export_fixture(controller, cloud, monkeypatch)
        assert controller.collect()["state"] == "COLLECTED"
        controller.receipt("123", "d" * 64, github)
        assert controller.cleanup()["state"] == "ACCEPTED"
        assert run.settle() == "verified_deleted"
        status = run.queue.status().entries[run.key].execution_status
        assert status["state"] == "ACCEPTED" and status["worker_readiness"] == "ready"
    assert not cloud.pods and cloud.deletes == ["pod1", "pod2"]


def test_queue_recovers_crash_after_durable_absence(allocation, monkeypatch):
    c, cloud = allocation
    run, catalog = queue_run(c, cloud)
    run.claim(catalog)
    run.start(confirmed=True)

    def crash(_):
        raise RuntimeError("runner lost before finalization")

    monkeypatch.setattr(c, "_finish_cleanup", crash)
    with pytest.raises(RuntimeError):
        c.cleanup()
    assert c.j.read()["state"] == "ACCEPTING" and run.queue.status().slots_used == 1
    confirmed_at = c.j.read()["deleted_confirmed_at"]
    cloud.sleep(300)
    assert recovery_for(run, cloud).recover() == "verified_deleted"
    assert c.j.read()["deleted_confirmed_at"] == confirmed_at
    assert cloud.deletes == ["pod1"] and c.j.read()["state"] == "REJECTED"


def test_paid_queue_job_entrypoints_and_independent_recovery(allocation, monkeypatch, tmp_path):
    from types import SimpleNamespace

    from runpod_ci_broker import queue_workflow as workflow
    from runpod_ci_broker.ledger import SQLiteStore
    from runpod_ci_broker.queue import TaskQueue
    from runpod_ci_broker.sealed import SealedStore, key_file

    template, cloud = allocation
    identity = {**IDENTITY, "path": workflow.LAUNCH}
    raw = SQLiteStore(tmp_path / "pair.db")
    store = SealedStore(raw)
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    key_file().write_bytes(b"x" * 32)
    monkeypatch.setenv("RCB_CONFIRM_PAID", "true")
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary"))
    monkeypatch.setattr(workflow, "MANIFEST", Path("examples/allocation-smoke.json").resolve())
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(workflow, "verify_parent", lambda *args: None)
    monkeypatch.setattr(workflow, "time", SimpleNamespace(time=cloud.now, monotonic=lambda: 0))
    monkeypatch.setattr(workflow, "LiveAPI", lambda **kwargs: cloud)
    monkeypatch.setattr(workflow, "Transport", lambda **kwargs: SimpleNamespace(get=cloud.read))
    monkeypatch.setattr(workflow, "TaskQueue", lambda *a: TaskQueue(*a, clock=cloud.now))
    monkeypatch.setattr(
        workflow,
        "BoundedController",
        lambda *a, **kw: BoundedController(*a, **kw, now=cloud.now, sleep=cloud.sleep),
    )
    # Exercise the real CUDA result bundle verifier with a synthetic worker and artifact service.
    github = export_fixture(template, cloud, monkeypatch)
    monkeypatch.setattr(github, "completed", lambda previous: previous == identity)
    cloud.task = workflow.tasks()["first"]

    def step(action, label="first", who=identity):
        return workflow.run(action, label, who, raw, store, github)

    assert step("prepare") == 0
    with pytest.raises(ValueError, match="first_task_not_accepted"):
        step("start", "second")
    for index, label in enumerate(workflow.LABELS):
        assert step("prepare", label) == step("start", label) == step("collect", label) == 0
        task = workflow.tasks()[label]
        artifact = {
            "id": index + 1,
            "expired": False,
            "digest": "sha256:" + "d" * 64,
            "name": f"allocation-bundle-2468-1-{task.experiment_id}",
            "workflow_run": {"id": 2468, "head_sha": IDENTITY["sha"]},
        }
        monkeypatch.setenv("RCB_ARTIFACT_ID", str(index + 1))
        monkeypatch.setenv("RCB_ARTIFACT_DIGEST", "d" * 64)
        monkeypatch.setattr(github, "get", lambda _, artifact=artifact: artifact)
        if label == "second":
            artifact["name"] = artifact["name"].replace("second", "first")
            with pytest.raises(ValueError, match="artifact_receipt_unverified"):
                step("receipt", label)
            artifact["name"] = artifact["name"].replace("first", "second")
        assert step("receipt", label) == step("cleanup", label) == step("report", label) == 0
    state = TaskQueue(store, workflow.QUEUE).status()
    assert state.reserved_microusd == 16_003_502 and state.slots_used == 0
    with pytest.raises(ValueError, match="campaign_already_bound"):
        step("prepare", who={**identity, "run_id": "5678"})
    monkeypatch.setenv("RCB_CONFIRM_CLEANUP", "true")
    assert step("recover", who={**identity, "run_id": "5678", "path": workflow.RECOVERY}) == 0
    assert len(cloud.creates) == 2 and not cloud.pods
