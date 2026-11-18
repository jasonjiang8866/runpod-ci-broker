import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

from runpod_ci_broker.ledger import LedgerError, SQLiteStore
from runpod_ci_broker.models import Task
from runpod_ci_broker.queue import QueuePolicy, TaskQueue, task_key


def job(name="first"):
    task = Task.model_validate_json(Path("examples/simplexlora-flexible.json").read_text())
    task.limits.startup_seconds = 300
    task.limits.runtime_seconds = 60
    task.limits.collect_seconds = task.limits.cleanup_seconds = 120
    task.limits.max_start_attempts = 3
    task.limits.storage_hourly_usd = 0.001
    task.limits.transfer_reserve_usd = task.limits.request_reserve_usd = 0.05
    task.limits.cleanup_reserve_usd = 0.4
    task.limits.prior_spend_usd = 10.002002
    task.limits.total_budget_usd = 20
    task.experiment_id = name
    return task


def queue_at(path, budget=20_000_000, slots=1):
    q = TaskQueue(SQLiteStore(path), "test", clock=lambda: 1000)
    q.initialize(
        QueuePolicy(budget_microusd=budget, opening_reserved_microusd=10_002_002, slots=slots)
    )
    return q


def claim(q, task, owner="owner"):
    return q.claim(task.scope, task.experiment_id, owner)


def test_retry_restart_and_incremental_accounting(tmp_path):
    path = tmp_path / "queue.db"
    q = queue_at(path, slots=2)
    first, second = job(), job("second")
    for task in (first, second):
        assert q.submit(task) == "submitted"
        assert q.submit(task) == "already_submitted"
        assert claim(q, task) == "claimed"
    reopened = queue_at(path, slots=2)
    assert reopened.submit(first) == "already_submitted"
    assert claim(reopened, first) == "already_claimed"
    state = reopened.status()
    assert state.reserved_microusd == 10_002_002 + 2 * 2_000_500
    assert state.slots_used == 2
    assert state.entries[task_key(first.scope, first.experiment_id)].enqueued_at == 1000
    with pytest.raises(LedgerError, match="queue_task_conflict"):
        reopened.submit(first, priority=1)
    with pytest.raises(LedgerError, match="queue_policy_conflict"):
        queue_at(path, slots=3)


@pytest.mark.parametrize(
    "slots,budget,same_job,other",
    [
        (1, 20_000_000, False, "slots_full"),
        (2, 12_002_502, False, "budget_insufficient"),
        (2, 20_000_000, True, "already_claimed"),
    ],
)
def test_race_two_controllers_on_same_version(tmp_path, slots, budget, same_job, other):
    path = tmp_path / "queue.db"
    q = queue_at(path, budget, slots)
    jobs = [job(), job("second")]
    for task in jobs:
        q.submit(task)
    barrier = Barrier(2)

    class RacingStore(SQLiteStore):
        def swap(self, key, version, value):
            barrier.wait(timeout=5)
            super().swap(key, version, value)

    def compete(task):
        competitor = TaskQueue(RacingStore(path), "test")
        return claim(competitor, jobs[0] if same_job else task, task.experiment_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(compete, jobs)) == sorted(["claimed", other])
    state = q.status()
    assert state.slots_used == 1
    assert state.reserved_microusd == 12_002_502
    assert sorted(state.reason(e, 1001) for e in state.entries.values()) == sorted(
        ["already_claimed", "eligible" if same_job else other]
    )


def test_committed_claim_with_lost_ack_is_not_granted_twice(tmp_path):
    path = tmp_path / "queue.db"
    q = queue_at(path, slots=2)
    first = job()
    q.submit(first)

    class LostAckStore(SQLiteStore):
        def swap(self, key, version, value):
            super().swap(key, version, value)
            raise LedgerError("ledger_write_unknown")

    with pytest.raises(LedgerError, match="ledger_write_unknown"):
        claim(TaskQueue(LostAckStore(path), "test"), first)
    assert claim(q, first, "another") == "already_claimed"
    assert q.status().reserved_microusd == 12_002_502


def test_expired_conflicting_and_invalid_submissions(tmp_path):
    q = queue_at(tmp_path / "queue.db")
    task = job()
    assert q.submit(task, latest_start=1000) == "submitted"
    assert claim(q, task) == "start_deadline_elapsed"
    task.gpu_preference = []
    with pytest.raises(LedgerError, match="queue_task_conflict"):
        q.submit(task, latest_start=1000)
    task.limits.prior_spend_usd += 1
    with pytest.raises(LedgerError, match="prior_spend_outside_opening_balance"):
        q.submit(task)
    task.limits.runtime_seconds = None
    with pytest.raises(LedgerError, match="task_budget_invalid"):
        q.submit(task)
    assert q.status().slots_used == 0


def test_missing_or_truncated_state_cannot_reset_budget(tmp_path):
    store = SQLiteStore(tmp_path / "queue.db")
    q = TaskQueue(store, "test")
    with pytest.raises(LedgerError, match="queue_missing"):
        q.submit(job())
    q = queue_at(tmp_path / "queue.db")
    q.submit(job())
    version, raw = store.read(q.key)
    del raw["entries"]
    store.swap(q.key, version, raw)
    with pytest.raises(LedgerError, match="queue_state_invalid"):
        q.submit(job("second"))


def test_submicro_history_and_increment_round_separately(tmp_path):
    q = queue_at(tmp_path / "queue.db")
    task = job()
    task.limits.prior_spend_usd = 0.0000001
    task.limits.storage_hourly_usd = 0.0000002
    q.submit(task)
    assert claim(q, task) == "claimed"
    assert q.status().reserved_microusd == 10_002_002 + 2_000_001


def test_cli_processes_reopen_same_file(tmp_path):
    cmd = [
        sys.executable,
        "-m",
        "runpod_ci_broker.queue_cli",
        "--db",
        str(tmp_path / "queue.db"),
        "--queue",
        "test",
    ]

    def run(*args, expected=0):
        result = subprocess.run([*cmd, *args], check=False, capture_output=True, text=True)
        assert result.returncode == expected, result.stderr
        return json.loads(result.stdout)

    run("init", "--budget-microusd", "2000000", "--opening-reserved-microusd", "0", "--slots", "1")
    run("submit", "--config", "examples/simplexlora-flexible.json")
    args = [
        "claim",
        "--scope",
        "broker-demo",
        "--experiment-id",
        "simplexlora-cuda-int8-smoke",
        "--owner",
        "worker",
    ]
    assert run(*args)["outcome"] == "claimed"
    assert run(*args, expected=1)["outcome"] == "already_claimed"
    state = run("status")
    assert state["slots_used"] == 1 and state["remaining_microusd"] > 0
