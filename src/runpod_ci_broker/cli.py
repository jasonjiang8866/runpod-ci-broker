"""Default dry-run; only explicit preflight reads the cloud; simulation is network-free."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from .diagnostics import diagnose_cuda
from .fake import FakeProvider
from .lifecycle import Engine
from .models import Authorization, Candidate, Task
from .planner import authorize, plan
from .provider import ProviderError, ReadOnlyRunPod
from .report import write_report
from .storage import Journal


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--action",
        default="dry-run",
        choices=["dry-run", "preflight", "simulate", "run", "reconcile", "cleanup"],
    )
    parser.add_argument(
        "--diagnose-cuda",
        action="store_true",
        help="preflight only: compare exact, unfiltered and minimum CUDA reads; never admit from diagnostics",
    )
    parser.add_argument("--mock-catalog", type=Path)
    parser.add_argument("--faults", type=Path, help="fake fault counts, simulation only")
    parser.add_argument("--state-dir", type=Path, default=Path(".broker"))
    parser.add_argument("--output", type=Path, default=Path("out"))
    parser.add_argument(
        "--authorization", type=Path, help="reviewed execution context, NOT human approval"
    )
    args = parser.parse_args()
    try:
        if args.diagnose_cuda and args.action != "preflight":
            raise ValueError("cuda_diagnostics_require_read_only_preflight")
        task = Task.model_validate_json(args.config.read_text())
        candidates = (
            [Candidate.model_validate(c) for c in json.loads(args.mock_catalog.read_text())]
            if args.mock_catalog
            else []
        )
        record: dict[str, Any]
        if args.action == "run":
            auth = (
                Authorization.model_validate_json(args.authorization.read_text())
                if args.authorization
                else None
            )
            record = plan(task, candidates, mode="offline")
            record["state"] = "EXECUTION_REFUSED"
            try:
                authorize(task, auth)
            except ValueError as exc:
                record["blockers"].append(str(exc))  # fixed internal admission codes only
            record["blockers"].append("live_execution_not_implemented")
            write_report(record, args.output)
            print(
                json.dumps(
                    {
                        "state": "EXECUTION_REFUSED",
                        "blockers": record["blockers"],
                        "paid_resources_created": False,
                    }
                )
            )
            return 2
        if args.action == "preflight":
            if args.mock_catalog or args.faults:
                raise ValueError("cannot_mix_mock_and_read_only_evidence")
            try:
                readonly_provider = ReadOnlyRunPod()
                deadline = readonly_provider.now() + task.limits.max_wait_seconds
                candidates, inventory = readonly_provider.snapshot(task)
                if args.diagnose_cuda:
                    inventory["cuda_diagnostics"] = diagnose_cuda(
                        task, readonly_provider, inventory, deadline
                    )
            except ProviderError as exc:
                candidates = [
                    Candidate(gpu_id=g, error=exc.code, evidence="read_only")
                    for g in task.gpu_allowlist
                ]
                inventory = {"inventory": "unverified", "query_status": exc.code}
            record = plan(task, candidates, mode="read_only")
            record["provider_observations"] = inventory
            for field in ("query_status", "datacenter_status"):
                if inventory.get(field) != "ok":
                    record["blockers"].append(field + "_unverified")
                    record["selected"] = None
            if inventory.get("inventory") != "complete":
                record["blockers"].append("inventory_unverified")
                record["selected"] = None
            if task.registry_credential_id:
                record["blockers"].append("registry_access_unverified")
                record["selected"] = None
            if inventory.get("scope_active_pod_count", 0):
                record["blockers"].append("scope_active_pod")
                record["selected"] = None
        elif args.action == "dry-run":
            record = plan(task, candidates, mode="mock" if args.mock_catalog else "offline")
        else:
            if not args.mock_catalog:
                raise ValueError("phase_one_recovery_is_mock_only_supply_mock_catalog")
            faults = json.loads(args.faults.read_text()) if args.faults else {}
            if not isinstance(faults, dict) or any(
                type(v) is not int or v < 0 for v in faults.values()
            ):
                raise ValueError("invalid_fault_counts")
            provider = FakeProvider(candidates, args.state_dir / "fake-provider.json", faults)
            journal = Journal(args.state_dir, task.scope, task.experiment_id)
            result_dir = journal.scope_dir / (task.fingerprint() + "-results")
            engine = Engine(
                task,
                provider,
                journal,
                result_dir,
                run_id=os.environ.get("GITHUB_RUN_ID", "local-mock"),
            )
            if args.action == "simulate":
                # Explicitly simulation-only. Cannot be passed to a live adapter.
                auth = Authorization(
                    experiment_id=task.experiment_id,
                    config_fingerprint=task.fingerprint(),
                    approved_budget_usd=task.limits.total_budget_usd or 0.01,
                    gpu_allowlist=task.gpu_allowlist,
                    runtime_seconds=task.limits.runtime_seconds or 0,
                    explicitly_approved=True,
                )
                record = engine.run(auth)
            else:
                record = engine.reconcile(cleanup_only=args.action == "cleanup")
            record["simulation_authorization_only"] = True
        write_report(record, args.output)
        print(
            json.dumps(
                {
                    "mode": record["mode"],
                    "state": record.get("state", "PLANNED"),
                    "blockers": record.get("blockers", []),
                    "output": str(args.output),
                    "paid_resources_created": False,
                }
            )
        )
        if args.action == "dry-run":
            return 0
        if args.action == "preflight":
            return 2 if record["blockers"] else 0
        return 0 if record.get("state") == "ACCEPTED" else 2
    except ValidationError as exc:
        # Pydantic normally echoes input values, which could contain secrets.
        print(
            json.dumps(
                {
                    "error": "invalid_configuration",
                    "fields": [
                        ".".join(str(x) for x in e["loc"]) for e in exc.errors(include_input=False)
                    ],
                }
            )
        )
        return 2
    except ProviderError as exc:
        print(json.dumps({"error": exc.code}))
        return 2
    except (ValueError, OSError, KeyError, TypeError):
        print(
            json.dumps(
                {
                    "error": "request_refused_or_invalid_local_state",
                    "live_execution": "disabled; no paid operations in this release",
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
