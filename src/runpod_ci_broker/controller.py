"""Fixed approved smoke controller; normal CLI remains dry-run by default."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import secrets
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from .campaign import CAMPAIGN, REPOSITORY, SCOPE, may_collect, observe, reserve, validate_task
from .cleanup import CleanupClient, cleanup_owned
from .export import RunPodResultReader, capability_valid, collect_https
from .handover import ControllerJournal, GitHubEvidence, context
from .ledger import GitHubContentsStore, LedgerError
from .lifecycle import owner
from .live import LiveAPI
from .models import Candidate, Task
from .planner import plan
from .provider import ProviderError, ReadOnlyRunPod
from .report import write_report
from .results import validate_manifest, verify_saved
from .worker import Observation, admit


class LiveClient(CleanupClient, Protocol):
    def inventory(self) -> list[dict[str, Any]]: ...
    def create(self, body: dict[str, Any]) -> str: ...


class Controller:
    def __init__(
        self,
        task: Task,
        journal: ControllerJournal,
        api: LiveClient,
        snapshot: Callable[[Task], tuple[list[Candidate], dict[str, Any]]],
        output: Path,
        *,
        now: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ):
        validate_task(task)
        self.task, self.j, self.api, self.snapshot, self.output = (
            task,
            journal,
            api,
            snapshot,
            output,
        )
        self.now, self.sleep = now, sleep

    def record(self) -> dict[str, Any]:
        r = self.j.read()
        if r is None:
            raise ValueError("trusted_live_record_required")
        if r["mode"] != "live" or r["config_fingerprint"] != self.task.fingerprint():
            raise ValueError("campaign_identity_mismatch")
        return r

    def save(self, r: dict[str, Any], state: str | None = None) -> None:
        if state is not None and state != r.get("state"):
            r["events"].append({"from": r.get("state"), "to": state, "at": self.now()})
            r["state"] = state
        self.j.write(r)

    def owns(self, r: dict[str, Any], p: dict[str, Any]) -> bool:
        return bool(
            p.get("name") == r["name"]
            and p.get("image") == self.task.image
            and p.get("mounts") == {}
            and isinstance(p.get("env"), dict)
            and all(p["env"].get(k) == v for k, v in owner(self.task, r["actions_run_id"]).items())
            and isinstance(p.get("gpu"), dict)
            and p["gpu"].get("id") == r["selected"]["gpu_id"]
            and p["gpu"].get("count") == 1
            and p.get("cloud") == self.task.cloud
            and isinstance(p.get("dataCenterId"), str)
            and re.fullmatch(r"[A-Za-z0-9_-]{1,100}", p["dataCenterId"])
        )

    def adopt(self, r: dict[str, Any]) -> dict[str, Any] | None:
        if r["pod_id"]:
            p = self.api.get(r["pod_id"], 5)
            if p is None:
                return None
            matches = [p]
        else:
            rows = self.api.inventory()
            markers = owner(self.task, r["actions_run_id"])
            matches = [
                p
                for p in rows
                if p.get("name") == r["name"]
                or all(p.get("env", {}).get(k) == v for k, v in markers.items())
            ]
        if not matches:
            self.save(r, "CREATE_UNKNOWN" if not r["pod_id"] else "CLEANUP_PENDING")
            return None
        if len(matches) != 1 or not self.owns(r, matches[0]):
            raise ProviderError("ownership_ambiguous")
        p = matches[0]
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", str(p.get("id", ""))):
            raise ProviderError("pod_identity_invalid")
        if r["pod_id"] and r["pod_id"] != p["id"]:
            raise ProviderError("ownership_ambiguous")
        r["pod_id"] = p["id"]
        # dataCenterIds is a placement preference, not a hard-region reservation.
        # This campaign permits any catalog region; retain requested vs actual DC.
        r["selected"]["data_center"] = p["dataCenterId"]
        self.save(r)
        return p

    def start(self, *, confirmed: bool = False) -> dict[str, Any]:
        if not confirmed:
            raise ValueError("explicit_paid_confirmation_required")
        with self.j.lock():
            if self.j.read() is not None:
                return self.record()  # reruns NEVER issue a second POST, even after deletion
            r = {
                **plan(self.task, [], mode="live"),
                "state": "PREFLIGHT",
                "events": [],
                "actions_run_id": self.j.identity["run_id"],
                "create_intent": False,
                "pod_id": None,
                "attempts": 0,
                "errors": [],
                "campaign": None,
                "collect_attempts": 0,
                "artifact_status": "not_collected",
                "cleanup_verification": "not_applicable",
                "metrics": {},
                "exit_code": None,
                "name": "rcb-" + hashlib.sha256((SCOPE + ":" + CAMPAIGN).encode()).hexdigest()[:24],
            }
            self.save(r)
            if self.j.other_unresolved():
                raise ProviderError("scope_has_unresolved_intent")
            candidates, inventory = self.snapshot(self.task)
            r.update(plan(self.task, candidates, mode="live"))
            r["provider_observations"] = inventory  # sanitized ReadOnlyRunPod projection
            if (
                inventory.get("query_status") != "ok"
                or inventory.get("inventory") != "complete"
                or inventory.get("scope_active_pod_count") != 0
                or inventory.get("datacenter_status") != "ok"
            ):
                r["blockers"].append("preflight_inventory_or_query_unknown")
            if r["blockers"]:
                capacities = [d["candidate"]["capacity"] for d in r["decisions"]]
                state = (
                    "NO_CAPACITY"
                    if capacities
                    and set(capacities) == {"none"}
                    and r["blockers"] == ["no_eligible_candidate"]
                    else "PREFLIGHT_BLOCKED"
                )
                if not r["budget"]["within_budget"]:
                    state = "BUDGET_INSUFFICIENT"
                self.save(r, state)
                return r
            # A second complete inventory immediately before reserving intent catches old
            # same-name or scope resources even when their config fingerprint changed.
            for p in self.api.inventory():
                env = p.get("env", {})
                if p.get("name") == r["name"] or (
                    env.get("RCB_TOOL") == "runpod-ci-broker/v1" and env.get("RCB_SCOPE") == SCOPE
                ):
                    raise ProviderError("existing_scope_resource")
            selected = r["selected"]
            r["requested_allocation"] = dict(selected)
            reserve(r, self.task, self.now())
            token = secrets.token_hex(32)
            r["result_capability_sha256"] = hashlib.sha256(token.encode()).hexdigest()
            r["create_intent"], r["paid_authorized"] = True, True
            body = {
                "name": r["name"],
                "image": self.task.image,
                "gpu": {"id": selected["gpu_id"], "count": 1, "minCudaVersion": "12.8"},
                "cloud": "SECURE",
                "dataCenterIds": [selected["data_center"]],
                "disk": 5,
                "mounts": {},
                "ports": ["8000/http"],
                "startSsh": False,
                "startJupyter": False,
                "globalNetworking": False,
                "env": {
                    **owner(self.task, r["actions_run_id"]),
                    "RCB_SELECTED_GPU": selected["gpu_id"],
                    "RCB_TASK_JSON": self.task.model_dump_json(),
                    "RCB_RESULT_TOKEN": token,
                },
            }
            self.save(r, "CREATING")  # durable/read-back BEFORE POST; token stays private
            try:
                r["pod_id"] = self.api.create(body)
                self.save(r, "STARTING")
            except ProviderError as exc:
                r["errors"].append(exc.code)
                if not exc.ambiguous:
                    r["create_intent"] = False
                    self.save(r, "PREFLIGHT_BLOCKED")
                else:
                    self.save(r, "CREATE_UNKNOWN")
            return r

    def collected(self, r: dict[str, Any], destination: Path) -> None:
        if not verify_saved(destination, self.task, r["pod_id"]):
            raise ProviderError("result_integrity_failed")
        manifest = json.loads((destination / "manifest.json").read_text())
        passed = validate_manifest(self.task, r["pod_id"], manifest)
        environment = json.loads((destination / "environment.json").read_text())
        if passed:
            observed = Observation.model_validate(environment["observed"]).model_dump()
            admit(self.task, r["selected"]["gpu_id"], observed)
            r["actual_environment"] = observed
        r["gpu_verified"] = passed
        r["notice"] = (
            "Fixed CUDA memory smoke only; framework and numerical reproduction not tested."
        )
        r["metrics"] = manifest["metrics"]
        r["exit_code"] = manifest["metrics"]["worker_exit_code"]
        r["facts"]["experiment_passed"] = passed
        r["artifact_status"] = "runner_verified_not_durable"
        r["manifest_sha256"] = hashlib.sha256(
            (destination / "manifest.json").read_bytes()
        ).hexdigest()
        self.save(r, "COLLECTED")

    def collect(self) -> dict[str, Any]:
        try:
            r = self._collect()
        except (ProviderError, ValueError, OSError, KeyError, TypeError):
            with self.j.lock():
                r = self.record()
                r["errors"].append("collection_failed_closed")
                self.save(r, "COLLECTION_FAILED")
        if r["create_intent"] and r["state"] != "COLLECTED" and not r["facts"]["results_saved"]:
            # Deadlines/failure take precedence over upload. Cleanup remains owner-checked.
            return self.cleanup()
        return r

    def _collect(self) -> dict[str, Any]:
        with self.j.lock():
            r = self.record()
            if not r["create_intent"] or r["facts"]["pod_deleted_confirmed"]:
                return r
            if r["facts"]["results_saved"]:
                return r
            destination = self.output / "bundle"
            if verify_saved(destination, self.task, r["pod_id"]):
                self.collected(r, destination)
                return r
            for _ in range(90):  # finite status/collection queries, never an unbounded loop
                if not may_collect(r, self.task, self.now()):
                    self.save(r, "TIMED_OUT")
                    break
                try:
                    p = self.adopt(r)
                    if p is None:
                        break  # unknown create stays unresolved, never retry POST
                    status = p.get("status")
                    rate = p.get("cost")
                    if (
                        isinstance(rate, bool)
                        or not isinstance(rate, (float, int))
                        or not math.isfinite(rate)
                        or rate < 0
                    ):
                        raise ProviderError("observed_price_unknown")
                    observe(r, self.task, self.now(), float(rate))
                    if not may_collect(r, self.task, self.now()):
                        self.save(r, "TIMED_OUT")
                        break
                    if status in {"ERROR", "EXITED", "TERMINATED"}:
                        self.save(r, "RUN_FAILED")
                        break
                    if status != "RUNNING":
                        if self.now() >= r["campaign"]["first_intent_at"] + 300:
                            self.save(r, "TIMED_OUT")
                            break
                        self.save(r, "STARTING")
                        self.sleep(5)
                        continue
                    self.save(r, "RUNNING")
                    token = p["env"].get("RCB_RESULT_TOKEN", "")
                    if (
                        not capability_valid(token)
                        or hashlib.sha256(token.encode()).hexdigest()
                        != r["result_capability_sha256"]
                    ):
                        raise ProviderError("result_capability_unrecoverable")
                    if r["collect_attempts"] >= self.task.limits.max_collect_attempts:
                        self.save(r, "COLLECTION_FAILED")
                        break
                    r["collect_attempts"] += 1
                    self.save(r, "COLLECTING")
                    left = r["campaign"]["collection_deadline"] - self.now()
                    collect_https(
                        self.task,
                        r["pod_id"],
                        RunPodResultReader(r["pod_id"], 8000, token),
                        destination,
                        deadline=time.monotonic() + min(15, left),
                    )
                    self.collected(r, destination)
                    break
                except ProviderError as exc:
                    r["errors"].append(exc.code)
                    self.save(r, "COLLECTION_FAILED")
                    if not exc.retryable:
                        break
                    self.sleep(min(10, max(0, r["campaign"]["collection_deadline"] - self.now())))
            return r

    def receipt(self, artifact_id: str, digest: str, github: GitHubEvidence) -> dict[str, Any]:
        if not re.fullmatch(r"[1-9][0-9]*", artifact_id) or not re.fullmatch(
            r"[a-f0-9]{64}", digest
        ):
            raise ValueError("artifact_receipt_required")
        with self.j.lock():
            r = self.record()
            destination = self.output / "bundle"
            if not verify_saved(destination, self.task, r["pod_id"]):
                raise ValueError("verified_bundle_required")
            if hashlib.sha256((destination / "manifest.json").read_bytes()).hexdigest() != r.get(
                "manifest_sha256"
            ):
                raise ValueError("artifact_manifest_mismatch")
            meta = github.get("/actions/artifacts/" + artifact_id)
            identity = self.j.identity
            if (
                meta.get("id") != int(artifact_id)
                or meta.get("expired") is not False
                or meta.get("digest") != "sha256:" + digest
                or meta.get("name") != f"smoke-bundle-{identity['run_id']}-{identity['attempt']}"
                or meta.get("workflow_run", {}).get("id") != int(identity["run_id"])
                or meta.get("workflow_run", {}).get("head_sha") != identity["sha"]
            ):
                raise ValueError("artifact_receipt_unverified")
            r["artifact_receipt"] = {
                "id": int(artifact_id),
                "sha256": digest,
                "run_id": identity["run_id"],
                "attempt": identity["attempt"],
            }
            r["artifact_status"] = "github_upload_receipt_verified"
            r["facts"]["results_saved"] = True
            self.save(r)
            return r

    def cleanup(self) -> dict[str, Any]:
        with self.j.lock():
            r = self.record()
            if not r["create_intent"] or r["facts"]["pod_deleted_confirmed"]:
                return r
            try:
                self.adopt(r)
            except ProviderError as exc:
                r["errors"].append(exc.code)
                self.save(r, "CLEANUP_PENDING")
                return r  # ownership/inventory ambiguity never permits deletion
        r = cleanup_owned(self.task, self.j, self.api, allow_termination=True)
        with self.j.lock():
            r = self.record()
            if r["facts"]["pod_deleted_confirmed"]:
                r.setdefault("deleted_confirmed_at", self.now())
                r["facts"]["task_completed"] = True
                r["state"] = (
                    "ACCEPTED"
                    if (r["facts"]["results_saved"] and r["facts"]["experiment_passed"] is True)
                    else "REJECTED"
                )
            observe(r, self.task, self.now())
            self.save(r)
        return r


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["start", "collect", "receipt", "cleanup", "report"])
    parser.add_argument("--confirm-paid", action="store_true")
    args = parser.parse_args()
    output = Path("out/live")
    journal = None
    try:
        identity = context()
        recovery = identity["path"].endswith("recover-smoke.yml")
        if args.action == "start" and recovery:
            raise ValueError("recovery_never_creates")
        task = Task.model_validate_json(Path("examples/approved-broker-smoke.json").read_text())
        validate_task(task)
        github = GitHubEvidence()
        journal = ControllerJournal(
            GitHubContentsStore(REPOSITORY),
            SCOPE,
            CAMPAIGN,
            identity,
            github.completed,
            recovery=recovery,
        )
        if recovery and journal.read() is None:
            print('{"mode":"live","state":"NO_OWNED_INTENT","runpod_operations":0}')
            return 0
        # report/receipt never need the RunPod management key.
        if args.action == "report":
            r = journal.read()
            if r:
                write_report(r, output)
            return 0 if r and r.get("state") == "ACCEPTED" else 2
        if args.action == "receipt":
            api = None
        else:
            api = LiveAPI(
                allow_create=args.action == "start" and args.confirm_paid,
                allow_termination=args.action in {"collect", "cleanup"},
            )
        controller = (
            Controller(task, journal, api, ReadOnlyRunPod().snapshot, output) if api else None
        )
        if args.action == "receipt":
            # Protocol object unused by receipt; no cloud credentials instantiated.
            controller = Controller(task, journal, None, lambda _: ([], {}), output)  # type: ignore[arg-type]
            r = controller.receipt(
                os.environ.get("RCB_ARTIFACT_ID", ""),
                os.environ.get("RCB_ARTIFACT_DIGEST", ""),
                github,
            )
        elif controller is not None and args.action == "start":
            r = controller.start(confirmed=args.confirm_paid)
        elif controller is not None and args.action == "collect":
            r = controller.collect()
        elif controller is not None:
            r = controller.cleanup()
        else:
            raise ValueError("controller_not_initialized")
        write_report(r, output)
        print(json.dumps({"mode": "live", "state": r["state"], "facts": r["facts"]}))
        return 0 if r["state"] in {"STARTING", "COLLECTED", "ACCEPTED"} else 2
    except (ProviderError, LedgerError, ValueError, OSError, KeyError, TypeError):
        # Exception messages may include validation input or upstream body; never print.
        print('{"error":"controller_refused_or_failed","see_trusted_journal":true}')
        return 2
    finally:
        if journal is not None:
            try:
                record = journal.read()
                if record:
                    write_report(record, output)
            except (LedgerError, ValueError, OSError):
                pass  # workflow_run recovery is independent of this process/finally


if __name__ == "__main__":
    raise SystemExit(main())
