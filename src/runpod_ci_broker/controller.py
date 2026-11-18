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

from .campaign import (
    CAMPAIGN,
    CAPACITY_FALLBACK,
    DIAGNOSTIC_RETRY,
    REGRESSION,
    REPAIR,
    REPOSITORY,
    SCOPE,
    TRANSPORT_RETEST,
    may_collect,
    observe,
    reserve,
    validate_task,
)
from .cleanup import CleanupClient, cleanup_owned
from .discovery import SearchHistory, SearchPolicy, admit_snapshot, capacity_feedback
from .export import ResultHTTPError, RunPodResultReader, capability_valid, collect_https
from .handover import ControllerJournal, GitHubEvidence, context
from .ledger import GitHubContentsStore, LedgerError
from .lifecycle import owner
from .live import CreateHTTPError, LiveAPI
from .models import Candidate, Task
from .optimization import PlacementSolver, RankedFirstSolver
from .planner import plan, reasons
from .provider import ProviderError, ReadOnlyRunPod
from .report import write_report
from .results import validate_manifest, verify_saved
from .startup import capture_startup_logs
from .transport_diagnostics import proxy_environment, record_startup
from .worker import Observation, admit


class LiveClient(CleanupClient, Protocol):
    def inventory(self) -> list[dict[str, Any]]: ...
    def create(self, body: dict[str, Any]) -> str: ...
    def startup_logs(self, pod_id: str, timeout: float) -> dict[str, Any]: ...


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
        search_policy: SearchPolicy | None = None,
        solver: PlacementSolver | None = None,
    ):
        validate_task(task)
        if solver is not None and search_policy is None:
            raise ValueError("solver_requires_search_policy")
        self.search_policy, self.solver = search_policy, solver
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
        if "allocation_observation" in r:
            r["allocation_observation"]["actual_data_center"] = p["dataCenterId"]
        self.save(r)
        return p

    def start(self, *, confirmed: bool = False, window_end: float | None = None) -> dict[str, Any]:
        if not confirmed:
            raise ValueError("explicit_paid_confirmation_required")
        with self.j.lock():
            existing = self.j.read()
            preflight_history = []
            if existing is not None:
                stored_search = existing.get("resource_search")
                if self.search_policy is None and stored_search:
                    self.search_policy = SearchPolicy.model_validate(stored_search["policy"])
                if (
                    self.search_policy is not None
                    and existing.get("resource_search", {}).get("policy")
                    != self.search_policy.model_dump()
                ):
                    raise ValueError("search_policy_changed_on_resume")
                stored_solver = existing.get("placement_plan", {}).get("solver")
                if stored_solver and stored_solver != (self.solver or RankedFirstSolver()).name:
                    raise ValueError("solver_changed_on_resume")
                # A catalog-only refusal has not consumed the one paid attempt.
                # Permit two manual rechecks, retaining every earlier observation.
                if not (
                    self.task.experiment_id == REGRESSION
                    and not self.j.recovery
                    and self.j.identity["event"] == "workflow_dispatch"
                    and existing["config_fingerprint"] == self.task.fingerprint()
                    and existing["state"] in {"PREFLIGHT_BLOCKED", "NO_CAPACITY"}
                    and existing["blockers"] == ["no_eligible_candidate"]
                    and existing["attempts"] == 0
                    and existing["create_intent"] is False
                    and existing["pod_id"] is None
                    and existing["campaign"] is None
                    and existing["selected"] is None
                    and existing["collect_attempts"] == 0
                    and len(existing.get("preflight_history", [])) < 2
                ):
                    return self.record()  # NEVER repeat a reserved or attempted create
                preflight_history = [
                    *existing.get("preflight_history", []),
                    {k: v for k, v in existing.items() if k != "preflight_history"},
                ]
            prior = original = history = None
            if self.task.experiment_id in {
                DIAGNOSTIC_RETRY,
                TRANSPORT_RETEST,
                REPAIR,
                CAPACITY_FALLBACK,
                REGRESSION,
            }:
                if self.j.recovery or self.j.identity["event"] != "workflow_dispatch":
                    raise ValueError("retry_manual_launch_required")
                records = self.j.owned()[1]["records"]
                history = records
                original = records.get(CAMPAIGN)
                prior = (
                    records.get(DIAGNOSTIC_RETRY)
                    if self.task.experiment_id == TRANSPORT_RETEST
                    else original
                )
                previous = self.j.previous_controller
                if previous is None or not self.j.completed(previous):
                    raise ValueError("previous_controller_not_confirmed_completed")
                # Validate the explicit window before persisting any successor record.
                reserve(
                    {},
                    self.task,
                    self.now(),
                    prior=prior,
                    original=original,
                    window_end=window_end,
                    history=history,
                )
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
                "name": "rcb-"
                + hashlib.sha256((SCOPE + ":" + self.task.experiment_id).encode()).hexdigest()[:24],
            }
            if preflight_history:
                r["preflight_history"] = preflight_history
            if self.search_policy:
                r["resource_search"] = {
                    "policy": self.search_policy.model_dump(),
                    "history": SearchHistory(
                        config_fingerprint=self.task.fingerprint()
                    ).model_dump(),
                    "mode": "live_controller",
                    "search_started_at": self.now(),
                    "snapshot_count": 0,
                    "http_requests": None,
                }
            self.save(r)
            if self.j.other_unresolved():
                raise ProviderError("scope_has_unresolved_intent")
            candidates, inventory = self.snapshot(self.task)
            if self.task.experiment_id in {CAPACITY_FALLBACK, REGRESSION}:
                assert history is not None
                refused = history[REPAIR]["requested_allocation"]
                for candidate in candidates:
                    if (candidate.gpu_id, candidate.data_center) == (
                        refused["gpu_id"],
                        refused["data_center"],
                    ):
                        candidate.error = "prior_capacity_refusal"
            if self.search_policy:
                planning = admit_snapshot(
                    self.task,
                    candidates,
                    inventory,
                    self.search_policy,
                    SearchHistory.model_validate(r["resource_search"]["history"]),
                    self.now(),
                    solver=self.solver,
                )
                planning["mode"] = "live"
                r.update(planning)
                r["resource_search"]["snapshot_count"] = 1
            else:
                r.update(plan(self.task, candidates, mode="live"))
            r["notice"] = (
                "Live controller: Pod allocation, GPU verification, saved results and deletion "
                "are separate facts. Catalog capacity is not a reservation."
            )
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
            if self.task.experiment_id in {CAPACITY_FALLBACK, REGRESSION}:
                self.sleep(5)
            for p in self.api.inventory():
                env = p.get("env", {})
                if p.get("name") == r["name"] or (
                    env.get("RCB_TOOL") == "runpod-ci-broker/v1" and env.get("RCB_SCOPE") == SCOPE
                ):
                    raise ProviderError("existing_scope_resource")
            if self.task.experiment_id in {CAPACITY_FALLBACK, REGRESSION}:
                r["predecessor_absence"] = "complete_scope_inventory_twice_before_create"
            selected = r["selected"]
            if self.search_policy and (
                self.now() >= r["candidate_valid_until"]
                or reasons(self.task, Candidate.model_validate(selected))
            ):
                # The inventory check or solver may consume the quote's remaining life.
                # No reservation or create is made from this expired proposal.
                r["blockers"].append("precreate_proposal_expired_or_invalid")
                r["selected"], r["candidate_queue"] = None, []
                r["candidate_valid_until"], r["selection_reason"] = None, None
                self.save(r, "PREFLIGHT_BLOCKED")
                return r
            r["requested_allocation"] = dict(selected)
            reserve(
                r,
                self.task,
                self.now(),
                prior=prior,
                original=original,
                window_end=window_end,
                history=history,
            )
            preferred_centers = [selected["data_center"]]
            if self.task.experiment_id in {REPAIR, CAPACITY_FALLBACK, REGRESSION}:
                # Fresh eligible rows only; requested placement remains advisory.
                preferred_centers += sorted(
                    {
                        d["candidate"]["data_center"]
                        for d in r["decisions"]
                        if not d["reasons"]
                        and d["candidate"]["gpu_id"] == selected["gpu_id"]
                        and d["candidate"]["data_center"] != selected["data_center"]
                    }
                )
                r["requested_data_centers"] = preferred_centers
            token = secrets.token_hex(32)
            r["result_capability_sha256"] = hashlib.sha256(token.encode()).hexdigest()
            r["create_intent"], r["paid_authorized"] = True, True
            if self.search_policy:
                r["allocation_observation"] = {
                    "schema_version": 1,
                    "requested_gpu": selected["gpu_id"],
                    "requested_data_centers": preferred_centers,
                    "quoted_gpu_hourly_usd": selected["hourly_usd"],
                    "intent_at": self.now(),
                    "request_started_at": None,
                    "response_at": None,
                    "request_elapsed_seconds": None,
                    "search_to_acknowledgement_seconds": None,
                    "outcome": "intent_recorded",
                    "ready_after_intent_seconds": None,
                    "actual_data_center": None,
                    "supplier_bill_usd": None,
                }
            body = {
                "name": r["name"],
                "image": self.task.image,
                "gpu": {"id": selected["gpu_id"], "count": 1, "minCudaVersion": "12.8"},
                "cloud": "SECURE",
                "dataCenterIds": preferred_centers,
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
            if self.search_policy and self.now() >= min(
                r["candidate_valid_until"], r["campaign"]["attempt_deadline"]
            ):
                # The original reservation remains consumed, but POST is known not sent.
                r["create_intent"] = False
                r["errors"].append("proposal_expired_before_post")
                self.allocation_outcome(r, "not_sent")
                self.save(r, "PREFLIGHT_BLOCKED")
                return r
            if self.search_policy:
                r["allocation_observation"]["request_started_at"] = self.now()
            try:
                r["pod_id"] = self.api.create(body)
                self.allocation_outcome(r, "acknowledged")
                self.save(r, "STARTING")
            except ProviderError as exc:
                r["errors"].append(exc.code)
                if isinstance(exc, CreateHTTPError):
                    r["create_http_failure"] = exc.diagnostic
                capacity = (
                    isinstance(exc, CreateHTTPError)
                    and not exc.ambiguous
                    and exc.diagnostic.get("http_status") in {400, 422}
                    and exc.diagnostic.get("reason_class") == "capacity"
                )
                if self.search_policy and capacity:
                    r["resource_search"]["history"] = capacity_feedback(
                        self.task,
                        Candidate.model_validate(selected),
                        SearchHistory.model_validate(r["resource_search"]["history"]),
                        exc,
                        self.now(),
                    ).model_dump()
                self.allocation_outcome(
                    r,
                    "unknown" if exc.ambiguous else "capacity_refused" if capacity else "rejected",
                )
                if not exc.ambiguous:
                    r["create_intent"] = False
                    self.save(r, "PREFLIGHT_BLOCKED")
                else:
                    self.save(r, "CREATE_UNKNOWN")
            return r

    def allocation_outcome(self, r: dict[str, Any], outcome: str) -> None:
        observation = r.get("allocation_observation")
        if observation is not None:
            at = self.now()
            started = observation["request_started_at"]
            observation.update(
                outcome=outcome,
                response_at=at,
                request_elapsed_seconds=max(0.0, at - started) if started is not None else None,
                search_to_acknowledgement_seconds=(
                    max(0.0, at - r["resource_search"]["search_started_at"])
                    if outcome == "acknowledged"
                    else None
                ),
            )

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

    def diagnose_startup(self, r: dict[str, Any]) -> None:
        deadline = min(r["campaign"]["collection_deadline"], r["campaign"]["wall_deadline"])
        capture_startup_logs(r, deadline, self.now, self.save, self.api.startup_logs)

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
                    record_startup(r, p, self.now())
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
                        self.diagnose_startup(r)
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
                    if self.task.experiment_id in {REPAIR, CAPACITY_FALLBACK, REGRESSION}:
                        left = r["campaign"]["collection_deadline"] - self.now()
                        phase = RunPodResultReader(r["pod_id"], 8000, token).readiness(
                            self.task, r["pod_id"], min(5, left)
                        )
                        r["worker_readiness"] = phase
                        if phase == "ready" and "allocation_observation" in r:
                            measurement = r["allocation_observation"]
                            if measurement["ready_after_intent_seconds"] is None:
                                measurement["ready_after_intent_seconds"] = max(
                                    0.0, self.now() - measurement["intent_at"]
                                )
                        self.save(r)
                        print("Worker phase: " + phase, flush=True)
                        if phase == "failed":
                            raise ProviderError("worker_refused")
                        if phase != "ready":
                            if self.now() >= r["campaign"]["first_intent_at"] + 300:
                                self.save(r, "TIMED_OUT")
                                break
                            self.sleep(5)
                            continue
                    if r["collect_attempts"] >= self.task.limits.max_collect_attempts:
                        self.save(r, "COLLECTION_FAILED")
                        break
                    r["collect_attempts"] += 1
                    r["collector_proxy_environment"] = proxy_environment(os.environ)
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
                    if isinstance(exc, ResultHTTPError):
                        r.setdefault("result_http_failures", []).append(exc.diagnostic)
                    self.save(r, "COLLECTION_FAILED")
                    if not exc.retryable:
                        if isinstance(exc, ResultHTTPError):
                            self.diagnose_startup(r)
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
    parser.add_argument("--search-policy", type=Path, help="start only: opt-in adaptive planning")
    attempt = parser.add_mutually_exclusive_group()
    attempt.add_argument("--diagnostic-retry", action="store_true")
    attempt.add_argument("--transport-retest", action="store_true")
    attempt.add_argument("--repair", action="store_true")
    attempt.add_argument("--capacity-fallback", action="store_true")
    attempt.add_argument("--regression", action="store_true")
    parser.add_argument("--retry-window-end", type=float)
    args = parser.parse_args()
    output = Path("out/live")
    journal = None
    try:
        identity = context()
        search_policy = None
        if args.search_policy:
            if args.action != "start":
                raise ValueError("search_policy_start_only")
            search_policy = SearchPolicy.model_validate_json(args.search_policy.read_text())
        recovery = identity["path"].endswith("recover-smoke.yml")
        if args.action == "start" and recovery:
            raise ValueError("recovery_never_creates")
        if (
            args.diagnostic_retry
            or args.transport_retest
            or args.repair
            or args.capacity_fallback
            or args.regression
        ) and (args.action != "start" or recovery):
            raise ValueError("retry_manual_launch_required")
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
        # Prior records are immutable. All attempts share the same scope lock.
        # Recovery follows the successor once it exists, never creates one itself.
        _, scope = journal.load()
        if args.regression or REGRESSION in scope["records"]:
            task = Task.model_validate_json(Path("examples/regression-smoke.json").read_text())
            validate_task(task)
            journal.experiment_id = REGRESSION
        elif args.capacity_fallback or CAPACITY_FALLBACK in scope["records"]:
            task = Task.model_validate_json(
                Path("examples/capacity-fallback-smoke.json").read_text()
            )
            validate_task(task)
            journal.experiment_id = CAPACITY_FALLBACK
        elif args.repair or REPAIR in scope["records"]:
            task = Task.model_validate_json(Path("examples/repair-smoke.json").read_text())
            validate_task(task)
            journal.experiment_id = REPAIR
        elif args.transport_retest or TRANSPORT_RETEST in scope["records"]:
            task = Task.model_validate_json(
                Path("examples/transport-retest-smoke.json").read_text()
            )
            validate_task(task)
            journal.experiment_id = TRANSPORT_RETEST
        elif args.diagnostic_retry or DIAGNOSTIC_RETRY in scope["records"]:
            task = Task.model_validate_json(
                Path("examples/diagnostic-retry-smoke.json").read_text()
            )
            validate_task(task)
            journal.experiment_id = DIAGNOSTIC_RETRY
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
        controller = None
        if api:
            if search_policy:
                controller = Controller(
                    task,
                    journal,
                    api,
                    ReadOnlyRunPod().snapshot,
                    output,
                    search_policy=search_policy,
                )
            else:
                controller = Controller(task, journal, api, ReadOnlyRunPod().snapshot, output)
        if args.action == "receipt":
            # Protocol object unused by receipt; no cloud credentials instantiated.
            controller = Controller(task, journal, None, lambda _: ([], {}), output)  # type: ignore[arg-type]
            r = controller.receipt(
                os.environ.get("RCB_ARTIFACT_ID", ""),
                os.environ.get("RCB_ARTIFACT_DIGEST", ""),
                github,
            )
        elif controller is not None and args.action == "start":
            r = controller.start(confirmed=args.confirm_paid, window_end=args.retry_window_end)
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
