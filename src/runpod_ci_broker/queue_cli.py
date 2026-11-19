"""Offline task admission on a shared SQLite file."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path

from .batch import BatchCatalog, BatchReplay, batch_plan
from .ledger import LedgerError, SQLiteStore
from .models import Task
from .queue import QueuePolicy, TaskQueue


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--queue", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("--budget-microusd", type=int, required=True)
    init.add_argument("--opening-reserved-microusd", type=int, required=True)
    init.add_argument("--slots", type=int, required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("--config", type=Path, required=True)
    submit.add_argument("--priority", type=int, default=0)
    submit.add_argument("--latest-start", type=int, help="exclusive UTC Unix timestamp")
    claim = commands.add_parser("claim")
    claim.add_argument("--scope", required=True)
    claim.add_argument("--experiment-id", required=True)
    claim.add_argument("--owner", required=True)
    commands.add_parser("status")
    plan = commands.add_parser("plan")
    plan.add_argument("--catalog", type=Path, required=True)
    plan.add_argument(
        "--save-replay", type=Path, help="write the queue, quote and decision time for replay"
    )
    replay = commands.add_parser("replay")
    replay.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "replay":
            saved = BatchReplay.model_validate_json(args.input.read_text())
            if saved.queue.name != args.queue:
                raise LedgerError("queue_replay_name_mismatch")
            print(json.dumps(batch_plan(saved)))
            return 0
        queue = TaskQueue(SQLiteStore(args.db), args.queue)
        if args.command == "init":
            queue.initialize(
                QueuePolicy(
                    budget_microusd=args.budget_microusd,
                    opening_reserved_microusd=args.opening_reserved_microusd,
                    slots=args.slots,
                )
            )
            outcome = "initialized"
        elif args.command == "submit":
            task = Task.model_validate_json(args.config.read_text())
            outcome = queue.submit(task, priority=args.priority, latest_start=args.latest_start)
        elif args.command == "claim":
            outcome = queue.claim(args.scope, args.experiment_id, args.owner)
        elif args.command == "plan":
            catalog = BatchCatalog.model_validate_json(args.catalog.read_text())
            saved = BatchReplay(queue=queue.status(), catalog=catalog, planned_at=int(time.time()))
            report = batch_plan(saved)
            if args.save_replay:
                with args.save_replay.open("x") as output:
                    output.write(saved.model_dump_json())
            print(json.dumps(report))
            return 0
        else:
            state = queue.status()
            now = int(time.time())
            print(
                json.dumps(
                    {
                        "policy": state.policy.model_dump(),
                        "slots_used": state.slots_used,
                        "reserved_microusd": state.reserved_microusd,
                        "remaining_microusd": state.policy.budget_microusd
                        - state.reserved_microusd,
                        "tasks": [
                            {
                                "scope": e.task.scope,
                                "experiment_id": e.task.experiment_id,
                                "outcome": state.reason(e, now),
                                "owner": e.owner,
                                "incremental_microusd": e.incremental_microusd,
                            }
                            for e in state.entries.values()
                        ],
                    }
                )
            )
            return 0
        print(json.dumps({"outcome": outcome}))
        return int(args.command == "claim" and outcome != "claimed")
    except LedgerError as exc:
        print(json.dumps({"error": str(exc)}))
    except (OSError, sqlite3.Error, ValueError):
        print(json.dumps({"error": "queue_input_or_storage_invalid"}))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
