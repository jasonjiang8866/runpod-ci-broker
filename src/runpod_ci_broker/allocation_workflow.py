"""Trusted manual allocation and independent recovery for one reviewed acceptance task."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any, cast

from .allocation import AllocationClient, BoundedController, ExecutionPermit
from .campaign import REGRESSION, REPOSITORY, SCOPE, record_hash, regression_parent
from .discovery import CapacityRefusal, SearchHistory, SearchPolicy
from .handover import ControllerJournal, GitHubEvidence, context
from .ledger import GitHubContentsStore, LedgerError, Store
from .live import LiveAPI
from .models import Authorization, Model, Task
from .planner import estimate
from .provider import ProviderError, Transport
from .report import write_report
from .sealed import SealedStore, seal_bundle
from .worker import SUPPORTED_GPUS

MANIFEST = Path("examples/allocation-smoke.json")
OUTPUT = Path("out/allocation")
PARENT_SCOPE = "broker-bounded-smoke"
ALLOCATION_SCOPE = "broker-sealed-smoke-v1"
EXPERIMENT = "broker-allocation-20261118-recheck"
CLEANED_PARENT = "broker-allocation-20261117-diverse"
CLEANED_HASH = "4be27b9070e931f7b1bd1f528eb4eb3484ffaeb5413ded1045c3af7f4f0d3acc"
ALLOCATION_PARENT = "broker-allocation-20261117"
ALLOCATION_PARENT_HASH = "2efab13abb225be7b25fae1f76e5e690dc78c5c116f3b03228c6b641fcdd7f01"
PRIOR_HASH = "d61758ff6a2c3b11076cf3995d9345ac85bf0cabb1bccbf2244ad91f73e68aa5"
LAUNCH = ".github/workflows/allocation-smoke.yml"
RECOVERY = ".github/workflows/recover-allocation.yml"


class Manifest(Model):
    enabled: bool
    task: Task
    policy: SearchPolicy

    def check(self) -> None:
        task = self.task
        cost = estimate(task)
        if (
            task.scope != ALLOCATION_SCOPE
            or task.experiment_id != EXPERIMENT
            or task.repository != REPOSITORY
            or set(task.gpu_allowlist) != SUPPORTED_GPUS
            or task.data_centers
            or task.limits.prior_spend_usd != 10.002002
            or task.limits.total_budget_usd != 20.0
            or task.limits.max_gpu_hourly_usd != 3.0
            or task.limits.max_start_attempts != 3
            or not cost["complete"]
            or not cost["within_budget"]
            or cost["seconds_per_attempt"] != 600
            or cost["estimated_total_usd"] > 12.01
            or task.limits.max_wait_seconds > 600
        ):
            raise ValueError("reviewed_allocation_manifest_required")


def predecessor(store: Store, github: GitHubEvidence) -> None:
    """Carry all six old reservations forward; never write or reset the old scope."""
    _, scope = store.read(hashlib.sha256(SCOPE.encode()).hexdigest())
    if not scope or scope.get("scope") != SCOPE or scope.get("holder") is not None:
        raise ValueError("historical_scope_unresolved")
    records = scope["records"]
    regression_parent(records)
    r = records[REGRESSION]
    if (
        len(records) != 6
        or record_hash(r) != PRIOR_HASH
        or r["state"] != "ACCEPTED"
        or not all(r["facts"].values())
        or r["campaign"]["reserved_usd"] != 6.001002
        or r["campaign"]["reserved_seconds"] != 3600
        or not github.completed(scope["controller"])
    ):
        raise ValueError("historical_acceptance_unverified")


def permit_for(task: Task, identity: dict[str, Any], end: float, now: float) -> ExecutionPermit:
    if not 600 <= end - now <= 1800:
        raise ValueError("allocation_window_must_leave_600_to_1800_seconds")
    return ExecutionPermit(
        authorization=Authorization(
            experiment_id=task.experiment_id,
            config_fingerprint=task.fingerprint(),
            approved_budget_usd=20.0,
            gpu_allowlist=task.gpu_allowlist,
            runtime_seconds=60,
            explicitly_approved=True,
        ),
        controller_sha=identity["sha"],
        window_start=now,
        window_end=end,
        worker_source_sha=task.source_sha,
        worker_image=task.image,
    )


def allocation_predecessor(store: Store, task: Task, github: GitHubEvidence) -> SearchHistory:
    _, scope = store.read(hashlib.sha256(PARENT_SCOPE.encode()).hexdigest())
    if not scope or scope.get("scope") != PARENT_SCOPE or scope["holder"] is not None:
        raise ValueError("prior_scope_unresolved")
    records = scope["records"]
    if set(records) != {ALLOCATION_PARENT, CLEANED_PARENT}:
        raise ValueError("unexpected_predecessors")
    r = records[CLEANED_PARENT]
    if (
        record_hash(records[ALLOCATION_PARENT]) != ALLOCATION_PARENT_HASH
        or record_hash(r) != CLEANED_HASH
        or r["state"] != "REJECTED"
        or r["facts"]["pod_deleted_confirmed"] is not True
        or r["cleanup_verification"] != "direct_404_and_complete_inventory_twice"
        or r["allocation"]["reserved_total_microusd"] != 10002002
        or not github.completed(scope["controller"])
    ):
        raise ValueError("prior_cleanup_unverified")
    return SearchHistory(
        config_fingerprint=task.fingerprint(),
        refusals=[
            CapacityRefusal.model_validate(x) for x in r["resource_search"]["history"]["refusals"]
        ],
    )


def verify_recovery(
    identity: dict[str, Any], record: dict[str, Any], github: GitHubEvidence
) -> None:
    original = {
        "run_id": record["actions_run_id"],
        "attempt": record["actions_run_attempt"],
        "sha": record["allocation"]["permit"]["controller_sha"],
        "path": LAUNCH,
        "event": "workflow_dispatch",
    }
    if not github.completed(original):
        raise ValueError("launch_not_completed")
    if identity["event"] == "workflow_run":
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())["workflow_run"]
        if (
            str(event["id"]) != original["run_id"]
            or event["run_attempt"] != original["attempt"]
            or event["head_sha"] != original["sha"]
            or event["path"] != LAUNCH
            or event["event"] != "workflow_dispatch"
            or event["head_repository"]["full_name"] != REPOSITORY
            or event["head_branch"] != "main"
        ):
            raise ValueError("recovery_source_mismatch")
    elif os.environ.get("RCB_CONFIRM_CLEANUP") != "true":
        raise ValueError("manual_cleanup_confirmation_required")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=["prepare", "start", "collect", "receipt", "cleanup", "report"]
    )
    parser.add_argument("--confirm-paid", action="store_true")
    parser.add_argument("--window-end", type=float)
    args = parser.parse_args()
    journal = None
    try:
        identity = context()
        if identity["path"] not in {LAUNCH, RECOVERY}:
            raise ValueError("allocation_workflow_required")
        recovery = identity["path"] == RECOVERY
        if args.action in {"prepare", "start"} and (recovery or not args.confirm_paid):
            raise ValueError("manual_paid_confirmation_required")
        spec = Manifest.model_validate_json(MANIFEST.read_text())
        spec.check()
        github = GitHubEvidence()
        store = GitHubContentsStore(REPOSITORY)
        journal = ControllerJournal(
            SealedStore(store),
            ALLOCATION_SCOPE,
            EXPERIMENT,
            identity,
            github.completed,
            recovery=recovery,
        )
        saved = journal.read()
        initial_history = None
        if not saved:
            if args.action not in {"prepare", "start"}:
                print('{"state":"NO_OWNED_INTENT","runpod_operations":0}')
                return 0
            if not spec.enabled:
                raise ValueError("allocation_release_not_activated")
            predecessor(store, github)
            initial_history = allocation_predecessor(store, spec.task, github)
            permit = permit_for(spec.task, identity, args.window_end or 0, time.time())
            task, policy = spec.task, spec.policy
        else:
            # Recovery uses the exact original request/permit even if main's manifest evolves.
            permit = ExecutionPermit.model_validate(saved["allocation"]["permit"])
            task = Task.model_validate(saved["request"])
            policy = SearchPolicy.model_validate(saved["allocation"]["policy"])
            if recovery:
                verify_recovery(identity, saved, github)
            elif args.action in {"prepare", "start"} and (
                spec.task != task or args.window_end != permit.window_end
            ):
                raise ValueError("original_task_and_window_required")
        if args.action == "prepare":
            # Validated fixed registry/source values only; no provider credentials needed.
            with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
                output.write(f"image={task.image}\nsource_sha={task.source_sha}\n")
            return 0
        if args.action == "report":
            return (
                0
                if saved
                and (
                    saved["state"] == "ACCEPTED"
                    or (
                        recovery
                        and (not saved["create_intent"] or saved["facts"]["pod_deleted_confirmed"])
                    )
                )
                else 2
            )
        api = (
            None
            if args.action == "receipt"
            else LiveAPI(
                allow_create=args.action == "start" and args.confirm_paid,
                allow_termination=args.action in {"collect", "cleanup"},
            )
        )

        # Construct GET transport only inside start. Receipt requires no RunPod key.
        def get(path: str, params: dict[str, Any]) -> dict[str, Any]:
            return Transport().get(path, params)

        controller = BoundedController(
            task,
            permit,
            journal,
            cast(AllocationClient, api),
            get,
            OUTPUT,
            policy=policy,
            initial_history=initial_history,
        )
        if args.action == "start":
            record = controller.start(confirmed=args.confirm_paid)
        elif args.action == "collect":
            record = controller.collect()
            if record["state"] == "COLLECTED":
                seal_bundle(OUTPUT / "bundle", task.results.required_files, task.fingerprint())
        elif args.action == "receipt":
            record = controller.receipt(
                os.environ.get("RCB_ARTIFACT_ID", ""),
                os.environ.get("RCB_ARTIFACT_DIGEST", ""),
                github,
            )
        else:
            record = controller.cleanup()
        print(json.dumps({"state": record["state"], "facts": record["facts"]}))
        if args.action == "cleanup" and record["facts"]["pod_deleted_confirmed"]:
            return 0  # Cleanup success does not assert experiment acceptance.
        return 0 if record["state"] in {"STARTING", "COLLECTED", "ACCEPTED"} else 2
    except (ProviderError, LedgerError, ValueError, OSError, KeyError, TypeError) as exc:
        # These two exception types carry fixed codes, never response bodies or inputs.
        code = exc.code if isinstance(exc, ProviderError) else str(exc)
        safe = isinstance(exc, (ProviderError, LedgerError)) and re.fullmatch(
            r"[a-z_]{1,100}", code
        )
        print(
            json.dumps(
                {
                    "error": code if safe else "allocation_workflow_refused_or_failed",
                    "see_trusted_journal": True,
                }
            )
        )
        return 2
    finally:
        if journal:
            try:
                final_record = journal.read()
                if final_record:
                    public: dict[str, Any] = {
                        k: final_record[k]
                        for k in (
                            "state",
                            "facts",
                            "mode",
                            "config_fingerprint",
                            "gpu_verified",
                            "budget",
                            "artifact_status",
                            "cleanup_verification",
                            "startup_logs",
                            "startup_diagnostics",
                            "result_http_failures",
                            "worker_readiness",
                            "errors",
                            "metrics",
                        )
                        if k in final_record
                    }
                    public["allocation_summary"] = {
                        "gpu": (final_record.get("selected") or {}).get("gpu_id"),
                        "estimated_compute_usd": final_record["allocation"][
                            "estimated_compute_usd"
                        ],
                        "attempts": final_record["attempts"],
                    }
                    write_report(public, OUTPUT)
            except (LedgerError, ValueError, OSError):
                pass


if __name__ == "__main__":
    raise SystemExit(main())
