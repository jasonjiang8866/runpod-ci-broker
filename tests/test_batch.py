import json
import sys
from pathlib import Path

import pytest

from runpod_ci_broker.batch import BatchCatalog, BatchReplay, Offer, batch_plan
from runpod_ci_broker.ledger import SQLiteStore
from runpod_ci_broker.models import Task
from runpod_ci_broker.queue import QueuePolicy, TaskQueue
from runpod_ci_broker.queue_cli import main


def setup_replay(tmp_path, *, slots=3, budget=9_000_000, now=1060, priority=0):
    clock = [1057]
    queue = TaskQueue(SQLiteStore(tmp_path / "queue.db"), "demo", clock=lambda: clock[0])
    queue.initialize(QueuePolicy(slots=slots, budget_microusd=budget, opening_reserved_microusd=0))
    for name, gpus in [("flex", ["5090", "4090"]), ("locked", ["5090"]), ("third", ["3090"])]:
        config = json.loads(Path("examples/simplexlora-flexible.json").read_text())
        config.update(experiment_id=name, gpu_allowlist=gpus, gpu_preference=gpus)
        queue.submit(Task.model_validate(config), priority=priority if name == "flex" else 0)
        clock[0] += 1
    offers = [
        Offer(
            gpu_id=gpu,
            data_center=center,
            memory_gb=24,
            hourly_usd=1.0,
            stock="HIGH",
            capacity="available",
            cuda_versions=["12.8"],
        )
        for gpu, center in [("4090", "EU"), ("5090", "US"), ("3090", "ASIA")]
    ]
    return queue, BatchReplay(
        queue=queue.status(),
        catalog=BatchCatalog(observed_at=now, complete=True, offers=offers),
        planned_at=now,
    )


def rows(report, policy="plan"):
    return {row["experiment_id"]: row for row in report[policy]["tasks"]}


def test_scarcity_and_fifo_same_snapshot(tmp_path):
    queue, replay = setup_replay(tmp_path)
    report = batch_plan(replay)
    chosen = rows(report)
    assert [
        (chosen[t]["selected"]["gpu_id"], chosen[t]["selected"]["data_center"])
        for t in ("flex", "locked", "third")
    ] == [("4090", "EU"), ("5090", "US"), ("3090", "ASIA")]
    assert report["plan"]["proposed_count"] == 3
    assert report["fifo"]["proposed_count"] == 2
    assert rows(report, "fifo")["locked"]["reason"] == "placement_already_proposed"
    assert queue.status().slots_used == 0
    assert batch_plan(BatchReplay.model_validate_json(replay.model_dump_json())) == report


def test_high_priority_flexible_job_spares_only_choice(tmp_path):
    _, replay = setup_replay(tmp_path, slots=1, priority=1)
    report = batch_plan(replay)
    assert report["plan"]["tasks"][0]["experiment_id"] == "flex"
    assert rows(report)["flex"]["reason"] == "preserve_contended_placement"
    assert rows(report)["flex"]["selected"]["gpu_id"] == "4090"
    assert rows(report)["locked"]["reason"] == "slots_full"


def test_aging_and_changed_global_inventory(tmp_path):
    _, replay = setup_replay(tmp_path, slots=1)
    flex = next(e for e in replay.queue.entries.values() if e.task.experiment_id == "flex")
    flex.priority = -2
    flex.enqueued_at = replay.planned_at - 180
    a = batch_plan(replay)
    assert a["plan"]["tasks"][0]["experiment_id"] == "flex"
    replay.catalog.offers[0].capacity = "none"
    b = batch_plan(replay)
    assert a["snapshot_id"] != b["snapshot_id"]
    assert rows(b)["flex"]["selected"]["gpu_id"] == "5090"


@pytest.mark.parametrize(
    "complete,offset,reason",
    [
        (False, 0, "catalog_incomplete"),
        (True, -31, "catalog_observation_stale"),
        (True, 1, "catalog_observation_stale"),
    ],
)
def test_incomplete_expired_or_future_quote(tmp_path, complete, offset, reason):
    _, replay = setup_replay(tmp_path)
    replay.catalog.complete = complete
    replay.catalog.observed_at += offset
    report = batch_plan(replay)
    assert report["plan"]["proposed_count"] == report["fifo"]["proposed_count"] == 0
    assert rows(report)["flex"]["reason"] == reason


def test_claimed_reservations_and_stock_are_not_supply(tmp_path):
    q, replay = setup_replay(tmp_path, slots=3, budget=2_500_000)
    q.claim("broker-demo", "third", "owner")
    replay.queue = q.status()
    replay.catalog.offers = [replay.catalog.offers[1]]
    report = batch_plan(replay)
    assert report["plan"]["proposed_count"] == 1
    assert rows(report)["third"]["reason"] == "already_claimed"
    assert rows(report)["locked"]["reason"] == "budget_insufficient"
    assert report["remaining_microusd"] == 2_500_000 - q.status().reserved_microusd


def test_duplicate_placements_and_price_cap(tmp_path):
    _, replay = setup_replay(tmp_path)
    replay.catalog.offers.append(replay.catalog.offers[1].model_copy())
    replay.catalog.offers[0].hourly_usd = 4
    report = batch_plan(replay)
    assert rows(report)["flex"]["reason"] == "no_eligible_candidate"
    assert rows(report)["locked"]["selected"] is None


def test_cli_saves_replay_independent_of_database(tmp_path, monkeypatch, capsys):
    q, replay = setup_replay(tmp_path)
    monkeypatch.setattr("runpod_ci_broker.queue_cli.time.time", lambda: replay.planned_at)
    catalog, saved = tmp_path / "catalog.json", tmp_path / "saved.json"
    catalog.write_text(replay.catalog.model_dump_json())
    args = ["queue", "--queue", "demo", "--db", str(tmp_path / "queue.db")]
    monkeypatch.setattr(
        sys, "argv", [*args, "plan", "--catalog", str(catalog), "--save-replay", str(saved)]
    )
    assert main() == 0
    report = json.loads(capsys.readouterr().out)
    q.claim("broker-demo", "flex", "other")
    monkeypatch.setattr(sys, "argv", [*args, "replay", "--input", str(saved)])
    assert main() == 0
    assert json.loads(capsys.readouterr().out) == report


def test_high_stock_one_site_and_second_site_changes_proposals(tmp_path):
    _, replay = setup_replay(tmp_path)
    only = replay.catalog.offers[1]
    replay.catalog.offers = [only]
    first = batch_plan(replay)
    assert first["plan"]["proposed_count"] == 1
    assert rows(first)["locked"]["reason"] == "placement_already_proposed"
    replay.catalog.offers.append(only.model_copy(update={"data_center": "EU"}))
    second = batch_plan(replay)
    assert second["plan"]["proposed_count"] == 2
    assert (
        rows(second)["locked"]["selected"]["data_center"]
        != rows(second)["flex"]["selected"]["data_center"]
    )
