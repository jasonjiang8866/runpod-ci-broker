"""Fixed two-task acceptance campaign with one sticky slot and independent cleanup."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from .allocation import AllocationClient, BoundedController, ExecutionPermit
from .allocation_workflow import (
    ALLOCATION_SCOPE,
    EXPERIMENT,
    MANIFEST,
    Manifest,
    allocation_predecessor,
    permit_for,
    predecessor,
)
from .batch import BatchCatalog, Offer
from .campaign import REPOSITORY
from .discovery import SearchPolicy
from .handover import ControllerJournal, GitHubEvidence, context
from .ledger import GitHubContentsStore, LedgerError, Store
from .live import LiveAPI
from .models import Task
from .optimization import digest
from .provider import ProviderError, ReadOnlyRunPod, Transport
from .queue import QueuePolicy, TaskQueue, task_key
from .queue_execution import QueuedExecution
from .sealed import SealedStore, seal_bundle

QUEUE = "broker-queue-pair-20261119"
HEADER = digest(["queue-campaign", QUEUE])
LAUNCH = ".github/workflows/queue-smoke.yml"
RECOVERY = ".github/workflows/recover-queue.yml"
OPENING = 12_002_502
LABELS = ("first", "second")
IMAGE = "ghcr.io/jasonjiang8866/runpod-ci-broker-worker@sha256:968681add1078ad7ac9cd39d2887bd5b2948a9356c7eb1f2347af548d2d22291"
SOURCE = "615cf130685fea1ba02173c8b153b4f66388a4c3"
POLICY = QueuePolicy(budget_microusd=20_000_000, opening_reserved_microusd=OPENING, slots=1)


def tasks() -> dict[str, Task]:
    spec = Manifest.model_validate_json(MANIFEST.read_text())
    spec.check()
    if spec.enabled or spec.task.image != IMAGE or spec.task.source_sha != SOURCE:
        raise ValueError("unexpected_predecessor_manifest")
    result = {}
    for label in LABELS:
        task = spec.task.model_copy(deep=True)
        task.scope = QUEUE
        task.experiment_id = f"{QUEUE}-{label}"
        task.limits.prior_spend_usd = OPENING / 1_000_000
        result[label] = task
    return result


def verify_parent(raw: Store, store: Store, github: GitHubEvidence, task: Task) -> None:
    predecessor(raw, github)
    allocation_predecessor(raw, task, github)
    _, parent = store.read(hashlib.sha256(ALLOCATION_SCOPE.encode()).hexdigest())
    if (
        not parent
        or parent.get("scope") != ALLOCATION_SCOPE
        or parent["holder"] is not None
        or set(parent["records"]) != {EXPERIMENT}
    ):
        raise ValueError("parent_scope_unverified")
    record = parent["records"][EXPERIMENT]
    if (
        record["state"] != "ACCEPTED"
        or not all(
            record["facts"].get(k) is True
            for k in (
                "experiment_passed",
                "results_saved",
                "pod_deleted_confirmed",
                "task_completed",
            )
        )
        or record["cleanup_verification"] != "direct_404_and_complete_inventory_twice"
        or record["allocation"]["reserved_total_microusd"] != OPENING
        or not github.completed(parent["controller"])
    ):
        raise ValueError("parent_acceptance_unverified")


def catalog_for(task: Task) -> BatchCatalog:
    reader = ReadOnlyRunPod(get=Transport(deadline=time.monotonic() + 60).get)
    candidates, obs = reader.snapshot(task)
    if (
        obs.get("query_status") != "ok"
        or obs.get("datacenter_status") != "ok"
        or obs.get("inventory") != "complete"
        or obs.get("scope_active_pod_count") != 0
    ):
        raise ValueError("live_catalog_or_inventory_unverified")
    centers = {d["id"] for d in obs["data_centers"]}
    stock = {
        (g["id"], d["id"]): d["availability"]
        for g in obs["catalog_models"]
        for d in g["data_centers"]
    }
    offers = []
    for candidate in candidates:
        if candidate.data_center not in centers or candidate.evidence != "read_only":
            continue
        level = stock.get((candidate.gpu_id, candidate.data_center))
        offers.append(
            Offer(
                **candidate.model_dump(),
                stock=level if level in {"HIGH", "MEDIUM", "LOW"} else None,
            )
        )
    return BatchCatalog(
        observed_at=int(datetime.fromisoformat(obs["observed_at"]).timestamp()),
        complete=True,
        offers=offers,
    )


def run(
    action: str,
    label: str,
    identity: dict[str, Any],
    raw: Store,
    store: Store,
    github: GitHubEvidence,
) -> int:
    recovery = identity["path"] == RECOVERY
    if identity["path"] not in {LAUNCH, RECOVERY} or recovery != (action == "recover"):
        raise ValueError("queue_workflow_required")
    if not recovery and (
        identity["event"] != "workflow_dispatch" or os.environ.get("RCB_CONFIRM_PAID") != "true"
    ):
        raise ValueError("manual_paid_input_required")
    if (
        recovery
        and identity["event"] == "workflow_dispatch"
        and os.environ.get("RCB_CONFIRM_CLEANUP") != "true"
    ):
        raise ValueError("manual_cleanup_input_required")
    version, header = store.read(HEADER)
    if header is None:
        if recovery:
            # No claim can precede a durable header. Refuse to treat orphaned queue state as empty.
            if store.read(TaskQueue(store, QUEUE).key)[1] is not None:
                raise ValueError("campaign_header_missing")
            print("No queue campaign initialized")
            return 0
        if action != "prepare" or label != "first":
            raise ValueError("campaign_not_initialized")
        specs = tasks()
        verify_parent(raw, store, github, specs[label])
        now = time.time()
        header = {
            "launch": identity,
            "tasks": {k: t.model_dump() for k, t in specs.items()},
            "policy": Manifest.model_validate_json(MANIFEST.read_text()).policy.model_dump(),
            "permits": {
                k: permit_for(t, identity, now + 1800, now).model_dump() for k, t in specs.items()
            },
            "waiting_observed": False,
        }
        store.swap(HEADER, version, header)
        version, observed = store.read(HEADER)
        if observed != header:
            raise LedgerError("queue_header_unconfirmed")
    specs = {k: Task.model_validate(header["tasks"][k]) for k in LABELS}
    original = header["launch"]
    if recovery:
        if not github.completed(original):
            raise ValueError("original_run_not_completed")
        if identity["event"] == "workflow_run":
            event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())["workflow_run"]
            if (
                str(event["id"]),
                event["run_attempt"],
                event["head_sha"],
                event["path"],
                event["head_branch"],
                event["head_repository"]["full_name"],
                event["event"],
            ) != (
                original["run_id"],
                original["attempt"],
                original["sha"],
                LAUNCH,
                "main",
                REPOSITORY,
                "workflow_dispatch",
            ):
                raise ValueError("recovery_source_mismatch")
    elif identity != original:
        raise ValueError("campaign_already_bound_to_original_run")

    queue = TaskQueue(store, QUEUE)
    if action == "prepare":
        queue.initialize(POLICY)
        for k, t in specs.items():
            queue.submit(t, priority=int(k == "first"))
        return 0

    def execution(k: str) -> QueuedExecution:
        t = specs[k]
        journal = ControllerJournal(
            store, QUEUE, t.experiment_id, identity, github.completed, recovery=recovery
        )
        api = (
            None
            if action in {"receipt", "report"}
            else LiveAPI(
                allow_create=action == "start",
                allow_termination=action in {"collect", "cleanup", "recover"},
            )
        )
        permit = ExecutionPermit.model_validate(header["permits"][k])
        return QueuedExecution(
            QUEUE,
            BoundedController(
                t,
                permit,
                journal,
                cast(AllocationClient, api),
                lambda p, q: Transport().get(p, q),
                Path("out/queue") / k,
                policy=SearchPolicy.model_validate(header["policy"]),
            ),
        )

    if recovery:
        version, queued = store.read(queue.key)
        if queued is None:
            if store.read(hashlib.sha256(QUEUE.encode()).hexdigest())[1] is not None:
                raise ValueError("queue_state_missing")
            return 0
        outcomes = []
        for k, t in specs.items():
            entry = queue.status().entries.get(task_key(QUEUE, t.experiment_id))
            if entry and entry.dispatch:
                outcomes.append(execution(k).recover())
        leftovers = any(p.get("env", {}).get("RCB_SCOPE") == QUEUE for p in LiveAPI().inventory())
        print(
            json.dumps(
                {
                    "cleanup": outcomes,
                    "slots_used": queue.status().slots_used,
                    "owned_inventory_empty": not leftovers,
                }
            )
        )
        return int(
            leftovers
            or queue.status().slots_used != 0
            or any(o not in {"verified_deleted", "no_create"} for o in outcomes)
        )

    job = execution(label)
    c = job.controller
    if action == "start":
        if label == "second":
            first = queue.status().entries[task_key(QUEUE, specs["first"].experiment_id)]
            if (
                first.release != "verified_deleted"
                or (first.execution_status or {}).get("state") != "ACCEPTED"
                or not header["waiting_observed"]
            ):
                raise ValueError("first_task_not_accepted")
        quote = catalog_for(c.task)
        outcome = job.claim(quote)
        if outcome != "claimed":
            raise LedgerError(outcome)
        if label == "first":
            if execution("second").claim(quote) != "slots_full":
                raise ValueError("waiting_not_observed")
            header["waiting_observed"] = True
            store.swap(HEADER, version, header)
        record = job.start(confirmed=True)
        return int(record["state"] != "STARTING")
    if action == "collect":
        record = c.collect()
        if record["state"] != "COLLECTED":
            return 1
        seal_bundle(c.output / "bundle", c.task.results.required_files, c.task.fingerprint())
        return 0
    if action == "receipt":
        c.receipt(os.environ["RCB_ARTIFACT_ID"], os.environ["RCB_ARTIFACT_DIGEST"], github)
        return 0
    if action == "cleanup":
        if c.j.read() is not None:
            c.cleanup()
        entry = queue.status().entries[job.key]
        if entry.dispatch:
            print(json.dumps({"settlement": job.settle()}))
        return 0
    record = c.record()
    entry = queue.status().entries[job.key]
    summary = {
        "task": label,
        "state": record["state"],
        "facts": record["facts"],
        "gpu": (record.get("selected") or {}).get("gpu_id"),
        "estimated_compute_usd": record["allocation"]["estimated_compute_usd"],
        "cleanup": entry.release,
        "attempts": record["attempts"],
        "waiting_observed": header["waiting_observed"],
        "slots_used": queue.status().slots_used,
    }
    print(json.dumps(summary))
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as f:
        f.write("```json\n" + json.dumps(summary, indent=2) + "\n```\n")
    return int(
        record["state"] != "ACCEPTED"
        or entry.release != "verified_deleted"
        or not header["waiting_observed"]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=["prepare", "start", "collect", "receipt", "cleanup", "report", "recover"]
    )
    parser.add_argument("--task", choices=LABELS, default="first")
    args = parser.parse_args()
    try:
        raw = GitHubContentsStore(REPOSITORY)
        return run(args.action, args.task, context(), raw, SealedStore(raw), GitHubEvidence())
    except (ValueError, KeyError, TypeError, OSError, ProviderError) as error:
        code = str(error) if isinstance(error, LedgerError) else "queue_campaign_failed"
        print(
            json.dumps(
                {
                    "error": code if code.isidentifier() else "queue_campaign_failed",
                    "action": args.action,
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
