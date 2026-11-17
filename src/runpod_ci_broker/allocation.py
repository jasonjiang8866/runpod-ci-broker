"""Task-bound allocation and smoke lifecycle, with one reservation across retries.

Prepared controller API only: no credential-bearing CLI or workflow is enabled.
The existing historical campaigns are deliberately kept on their old controller.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
import time
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any, Protocol, Self

from pydantic import Field, model_validator

from .campaign import REPAIR_IMAGE, REPAIR_SOURCE, REPOSITORY, SCOPE
from .cleanup import CleanupClient, cleanup_owned
from .discovery import SearchHistory, SearchPolicy, admit_snapshot, capacity_feedback
from .export import RunPodResultReader, capability_valid, collect_https
from .handover import ControllerJournal, GitHubEvidence
from .ledger import LedgerError
from .lifecycle import owner
from .live import CreateHTTPError
from .models import Authorization, Candidate, Model, Nonnegative, Positive, Sha, Task
from .optimization import PlacementSolver, RankedFirstSolver, digest, microusd
from .planner import authorize, estimate, plan, reasons
from .provider import ProviderError, ReadOnlyRunPod
from .results import validate_manifest, verify_saved
from .worker import SUPPORTED_GPUS, Observation, admit


class ExecutionPermit(Model):
    """Explicit execution parameters, not proof of a human's authorization."""

    authorization: Authorization
    controller_sha: Sha
    window_start: Nonnegative
    window_end: Positive
    worker_source_sha: Sha = REPAIR_SOURCE
    worker_image: Annotated[
        str,
        Field(pattern=r"^ghcr.io/jasonjiang8866/runpod-ci-broker-worker@sha256:[a-f0-9]{64}$"),
    ] = REPAIR_IMAGE

    @model_validator(mode="after")
    def bounded_window(self) -> Self:
        if not 0 < self.window_end - self.window_start <= 21600:
            raise ValueError("execution_window_invalid")
        return self


class AllocationClient(CleanupClient, Protocol):
    def inventory(self) -> list[dict[str, Any]]: ...
    def create(self, body: dict[str, Any]) -> str: ...


def validate_smoke(task: Task, permit: ExecutionPermit) -> None:
    """General task IDs/budgets, but still the previously verified fixed worker."""
    if (
        task.scope == SCOPE
        or task.repository != REPOSITORY
        or task.data_centers
        or task.image != permit.worker_image
        or task.source_sha != permit.worker_source_sha
        or task.entrypoint != ["broker-cuda-memory-smoke"]
        or task.strategy != "flexible_smoke"
        or task.gpu_count != 1
        or not set(task.gpu_allowlist) <= SUPPORTED_GPUS
        or task.volume.kind != "none"
        or task.registry_credential_id is not None
        or task.compatibility.cuda_versions
        or task.compatibility.exact_driver
        or task.compatibility.min_driver
        or task.compatibility.min_cuda_version is None
        or tuple(map(int, task.compatibility.min_cuda_version.split("."))) < (12, 8)
        or task.limits.runtime_seconds is None
        or task.limits.runtime_seconds > 60
        or set(task.results.required_files) != {"metrics.json", "worker.log", "environment.json"}
        or task.results.path != "/workspace/results"
        or task.results.metric_min != {"gpu_memory_smoke_passed": 1.0}
        or task.results.metric_max != {"worker_exit_code": 0.0}
        or task.results.max_bytes > 1048576
    ):
        raise ValueError("bounded_smoke_contract_required")


class AttemptCleanup(CleanupClient):
    """Add exact attempt-marker checks to the existing deletion verifier."""

    def __init__(self, controller: BoundedController, record: dict[str, Any]):
        self.c, self.record = controller, record

    def get(self, pod_id: str, timeout: float) -> dict[str, Any] | None:
        pod = self.c.api.get(pod_id, timeout)
        if pod is not None and not self.c.owns(self.record, pod):
            raise ProviderError("allocation_attempt_ownership_ambiguous")
        return pod

    def delete(self, pod_id: str, timeout: float) -> None:
        self.c.api.delete(pod_id, timeout)

    def active_ids(self, deadline: float) -> set[str]:
        return self.c.api.active_ids(deadline)


class BoundedController:
    def __init__(
        self,
        task: Task,
        permit: ExecutionPermit,
        journal: ControllerJournal,
        api: AllocationClient,
        get: Callable[[str, dict[str, Any]], dict[str, Any]],
        output: Path,
        *,
        policy: SearchPolicy | None = None,
        solver: PlacementSolver | None = None,
        initial_history: SearchHistory | None = None,
        now: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ):
        validate_smoke(task, permit)
        authorize(task, permit.authorization)
        if (journal.scope, journal.experiment_id) != (task.scope, task.experiment_id):
            raise ValueError("allocation_journal_mismatch")
        if not journal.recovery and journal.identity["sha"] != permit.controller_sha:
            raise ValueError("allocation_controller_revision_mismatch")
        if (
            not re.fullmatch(r"[1-9][0-9]*", str(journal.identity["run_id"]))
            or type(journal.identity["attempt"]) is not int
            or journal.identity["attempt"] < 1
        ):
            raise ValueError("allocation_controller_identity_invalid")
        self.task, self.permit = task.model_copy(deep=True), permit.model_copy(deep=True)
        self.j, self.api, self.get, self.output = journal, api, get, output
        self.policy = (policy or SearchPolicy()).model_copy(deep=True)
        self.solver, self.now, self.sleep = solver or RankedFirstSolver(), now, sleep
        self.cost = estimate(self.task)
        self.initial_history = initial_history or SearchHistory(
            config_fingerprint=task.fingerprint()
        )
        self.initial_history.validate_for(self.task, self.now())

    def save(self, r: dict[str, Any], state: str | None = None) -> None:
        if state and state != r["state"]:
            r["events"].append({"from": r["state"], "to": state, "at": self.now()})
            r["state"] = state
        self.j.write(r)

    def clock(self, r: dict[str, Any]) -> float:
        value = self.now()
        if not math.isfinite(value) or value < r["allocation"]["last_observed_at"]:
            raise ValueError("allocation_clock_regressed")
        r["allocation"]["last_observed_at"] = value
        return value

    def intent_id(self, number: int) -> str:
        return digest([self.task.fingerprint(), self.permit.model_dump(), number])

    def record(self) -> dict[str, Any]:
        r = self.j.read()
        if (
            not r
            or r["mode"] != "live"
            or r["config_fingerprint"] != self.task.fingerprint()
            or r["request"] != self.task.model_dump()
            or r.get("allocation", {}).get("permit") != self.permit.model_dump()
            or r["allocation"]["policy"] != self.policy.model_dump()
            or r["allocation"]["solver"] != self.solver.name
            or r["attempts"] != len(r["attempt_log"])
            or not 0 <= r["attempts"] <= self.task.limits.max_start_attempts
            or r["allocation"]["reserved_total_microusd"]
            != (microusd(self.cost["estimated_total_usd"]) if r["attempts"] else 0)
            or not 0 <= r["query_count"] <= self.task.limits.max_queries
            or not 0 <= r["allocation"]["http_requests"] <= self.policy.max_http_requests
            or (
                r["attempts"] > 0
                and (
                    r["attempt_log"][-1]["outcome"]
                    not in {
                        "intent_recorded",
                        "unknown",
                        "acknowledged",
                        "capacity_refused",
                        "rejected",
                        "not_sent",
                    }
                    or r["create_intent"]
                    != (
                        r["attempt_log"][-1]["outcome"]
                        in {"intent_recorded", "unknown", "acknowledged"}
                    )
                    or (
                        r["state"] in {"PREFLIGHT", "WAITING_CAPACITY"}
                        and r["attempt_log"][-1]["outcome"] != "capacity_refused"
                    )
                )
            )
            or any(
                a["number"] != i
                or a["intent_id"] != self.intent_id(i)
                or (i < r["attempts"] and a["outcome"] != "capacity_refused")
                for i, a in enumerate(r["attempt_log"], 1)
            )
        ):
            raise ValueError("allocation_record_mismatch")
        return r

    def new_record(self) -> dict[str, Any]:
        at = self.now()
        if not math.isfinite(at) or not self.permit.window_start <= at < self.permit.window_end:
            raise ValueError("allocation_window_closed")
        return {
            **plan(self.task, [], mode="live"),
            "state": "PREFLIGHT",
            "events": [],
            "errors": [],
            "name": "rcb-"
            + hashlib.sha256(
                (self.task.scope + ":" + self.task.experiment_id).encode()
            ).hexdigest()[:24],
            "actions_run_id": self.j.identity["run_id"],
            "actions_run_attempt": self.j.identity["attempt"],
            "create_intent": False,
            "pod_id": None,
            "attempts": 0,
            "attempt_log": [],
            "query_count": 0,
            "collect_attempts": 0,
            "status_checks": 0,
            "artifact_status": "not_collected",
            "cleanup_verification": "not_applicable",
            "allocation": {
                "schema_version": 1,
                "permit": self.permit.model_dump(),
                "policy": self.policy.model_dump(),
                "solver": self.solver.name,
                "started_at": at,
                "last_observed_at": at,
                "search_deadline": min(
                    at + self.task.limits.max_wait_seconds, self.permit.window_end
                ),
                "http_requests": 0,
                "terminal_read_error": None,
                "reserved_total_microusd": 0,
                "estimated_compute_usd": None,
                "observed_max_hourly_usd": None,
                "supplier_bill_usd": None,
            },
            "resource_search": {
                "policy": self.policy.model_dump(),
                "mode": "live_controller",
                "history": self.initial_history.model_dump(),
                "rounds": [],
            },
        }

    def read_get(self, r: dict[str, Any], path: str, params: dict[str, Any]) -> dict[str, Any]:
        if r["allocation"]["terminal_read_error"]:
            raise ProviderError(r["allocation"]["terminal_read_error"])
        if self.clock(r) >= r["allocation"]["search_deadline"]:
            raise ProviderError("allocation_search_deadline")
        if r["allocation"]["http_requests"] >= self.policy.max_http_requests:
            raise ProviderError("allocation_http_limit")
        r["allocation"]["http_requests"] += 1
        self.save(r)  # Request budgets survive crashes, including pagination.
        try:
            result = self.get(path, params)
        except ProviderError as exc:
            if not exc.retryable or exc.ambiguous:
                r["allocation"]["terminal_read_error"] = exc.code
                self.save(r)
            raise
        if self.clock(r) >= r["allocation"]["search_deadline"]:
            raise ProviderError("allocation_search_deadline")
        return result

    def start(self, *, confirmed: bool = False) -> dict[str, Any]:
        if not confirmed or self.j.recovery or self.j.identity["event"] != "workflow_dispatch":
            raise ValueError("explicit_manual_allocation_required")
        with self.j.lock():
            r = self.record() if self.j.read() is not None else self.new_record()
            if r.get("cancelled"):
                return r
            if r["create_intent"]:
                if r["state"] == "CREATING":
                    r["attempt_log"][-1]["outcome"] = "unknown"
                    self.save(r, "CREATE_UNKNOWN")
                return r  # No repeated POST, including after process loss before/after POST.
            if r["state"] not in {"PREFLIGHT", "WAITING_CAPACITY"}:
                return r
            if self.j.other_unresolved():
                raise ProviderError("scope_has_unresolved_intent")
            self.save(r)
            reader = ReadOnlyRunPod(get=lambda p, q: self.read_get(r, p, q))
            query_task = self.task.model_copy(
                update={
                    "limits": self.task.limits.model_copy(
                        update={"max_queries": 1, "max_wait_seconds": 0}
                    )
                }
            )
            while (
                r["attempts"] < self.task.limits.max_start_attempts
                and r["query_count"] < self.task.limits.max_queries
            ):
                if self.clock(r) >= r["allocation"]["search_deadline"]:
                    break
                r["query_count"] += 1
                self.save(r)
                candidates, observations = reader.snapshot(query_task)
                history = SearchHistory.model_validate(r["resource_search"]["history"])
                report = admit_snapshot(
                    self.task,
                    candidates,
                    observations,
                    self.policy,
                    history,
                    self.clock(r),
                    solver=self.solver,
                )
                report["mode"] = "live"
                r.update(report)
                r["paid_authorized"] = bool(r["attempts"])
                r["resource_search"]["rounds"].append(
                    {
                        "round": r["query_count"],
                        "http_requests": r["allocation"]["http_requests"],
                        "elapsed_seconds": self.clock(r) - r["allocation"]["started_at"],
                        "selected": r["selected"],
                        "blockers": list(r["blockers"]),
                        "next_query_in_seconds": None,
                    }
                )
                if r["blockers"]:
                    # Only complete no-capacity snapshots or transient read failures can wait.
                    statuses = [
                        observations.get(k)
                        for k in ("query_status", "inventory", "datacenter_status")
                    ]
                    terminal = {
                        "authentication_failed",
                        "permission_denied",
                        "account_restricted",
                        "allocation_search_deadline",
                        "allocation_http_limit",
                        "invalid_request",
                    }
                    if (
                        r["allocation"]["terminal_read_error"]
                        or any(s in terminal for s in statuses)
                        or any(
                            b.startswith("solver_")
                            or b
                            in {
                                "scope_inventory_unverified_or_active",
                                "registry_access_unverified",
                            }
                            for b in r["blockers"]
                        )
                        or any(
                            isinstance(s, str)
                            and any(
                                word in s
                                for word in (
                                    "invalid",
                                    "incomplete",
                                    "duplicate",
                                    "unknown",
                                    "not_found",
                                )
                            )
                            for s in statuses
                        )
                    ):
                        self.save(r, "PREFLIGHT_BLOCKED")
                        return r
                    delay = min(
                        self.policy.initial_interval_seconds * 2 ** (r["query_count"] - 1),
                        self.policy.max_interval_seconds,
                    )
                    remaining = r["allocation"]["search_deadline"] - self.clock(r)
                    r["resource_search"]["rounds"][-1]["next_query_in_seconds"] = (
                        delay
                        if delay < remaining and r["query_count"] < self.task.limits.max_queries
                        else None
                    )
                    self.save(r, "WAITING_CAPACITY")
                    if delay >= remaining or r["query_count"] >= self.task.limits.max_queries:
                        break
                    self.sleep(delay)
                    continue
                # Complete inventory again just before the durable create intent.
                try:
                    rows = reader.listing("/pods", "pods")
                    if any(
                        p.get("name") == r["name"]
                        or (
                            p.get("env", {}).get("RCB_TOOL") == "runpod-ci-broker/v1"
                            and p.get("env", {}).get("RCB_SCOPE") == self.task.scope
                        )
                        for p in rows
                    ):
                        raise ProviderError("existing_scope_resource")
                except ProviderError as exc:
                    r["errors"].append(exc.code)
                    self.save(r, "PREFLIGHT_BLOCKED")
                    return r
                if not self.fresh(r):
                    self.save(r, "TIMED_OUT")
                    return r
                self.create(r)
                if r["attempt_log"][-1]["outcome"] != "capacity_refused":
                    return r
            self.save(r, "NO_CAPACITY")
            return r

    def fresh(self, r: dict[str, Any]) -> bool:
        at = self.clock(r)
        return bool(
            self.permit.window_start
            <= at
            < min(r["candidate_valid_until"], r["allocation"]["search_deadline"])
            and at + self.cost["seconds_per_attempt"] <= self.permit.window_end
            and not reasons(self.task, Candidate.model_validate(r["selected"]))
        )

    def create(self, r: dict[str, Any]) -> None:
        selected = Candidate.model_validate(r["selected"])
        number = r["attempts"] + 1
        token = secrets.token_hex(32)
        intent = {
            "number": number,
            "intent_id": self.intent_id(number),
            "at": self.clock(r),
            "requested": selected.model_dump(),
            "outcome": "intent_recorded",
            "response_at": None,
            "pod_id": None,
            "actual_data_center": None,
        }
        r["attempts"], r["create_intent"], r["paid_authorized"] = number, True, True
        r["attempt_log"].append(intent)
        r["allocation"]["reserved_total_microusd"] = microusd(self.cost["estimated_total_usd"])
        r["collection_deadline"] = (
            intent["at"] + self.cost["seconds_per_attempt"] - self.task.limits.cleanup_seconds
        )
        r["result_capability_sha256"] = hashlib.sha256(token.encode()).hexdigest()
        body = {
            "name": r["name"],
            "image": self.task.image,
            "gpu": {
                "id": selected.gpu_id,
                "count": 1,
                "minCudaVersion": self.task.compatibility.min_cuda_version,
            },
            "cloud": self.task.cloud,
            "dataCenterIds": [selected.data_center],
            "disk": 5,
            "mounts": {},
            "ports": ["8000/http"],
            "startSsh": False,
            "startJupyter": False,
            "globalNetworking": False,
            "env": {
                **owner(self.task, r["actions_run_id"]),
                "RCB_ATTEMPT_ID": intent["intent_id"],
                "RCB_SELECTED_GPU": selected.gpu_id,
                "RCB_TASK_JSON": self.task.model_dump_json(),
                "RCB_RESULT_TOKEN": token,
            },
        }
        self.save(r, "CREATING")
        if not self.fresh(r):
            intent["outcome"], r["create_intent"] = "not_sent", False
            self.save(r, "TIMED_OUT")
            return
        try:
            pod_id = self.api.create(body)
            if not isinstance(pod_id, str) or not re.fullmatch(r"[a-z0-9]{1,64}", pod_id):
                raise ProviderError("create_response_unknown", ambiguous=True)
            intent.update(outcome="acknowledged", response_at=self.clock(r), pod_id=pod_id)
            r["pod_id"] = pod_id
            self.save(r, "STARTING")
        except ProviderError as exc:
            intent["response_at"] = self.clock(r)
            r["errors"].append(exc.code)
            if isinstance(exc, CreateHTTPError) and not exc.ambiguous:
                intent["http_failure"] = exc.diagnostic
                if (
                    exc.diagnostic.get("http_status") in {400, 422}
                    and exc.diagnostic.get("reason_class") == "capacity"
                ):
                    history = capacity_feedback(
                        self.task,
                        selected,
                        SearchHistory.model_validate(r["resource_search"]["history"]),
                        exc,
                        self.clock(r),
                    )
                    r["resource_search"]["history"] = history.model_dump()
                    intent["outcome"], r["create_intent"] = "capacity_refused", False
                    self.save(r, "WAITING_CAPACITY")
                    return
                intent["outcome"], r["create_intent"] = "rejected", False
                self.save(r, "PREFLIGHT_BLOCKED")
            else:
                # An unclassified adapter failure is ambiguous even if its retryable flag says otherwise.
                intent["outcome"] = "unknown"
                self.save(r, "CREATE_UNKNOWN")

    def owns(self, r: dict[str, Any], pod: dict[str, Any]) -> bool:
        return bool(
            pod.get("name") == r["name"]
            and pod.get("image") == self.task.image
            and pod.get("mounts") == {}
            and pod.get("cloud") == self.task.cloud
            and pod.get("gpu", {}).get("id") == r["selected"]["gpu_id"]
            and pod.get("gpu", {}).get("count") == 1
            and isinstance(pod.get("dataCenterId"), str)
            and re.fullmatch(r"[A-Za-z0-9_-]{1,100}", pod["dataCenterId"])
            and isinstance(pod.get("env"), dict)
            and all(
                pod["env"].get(k) == v for k, v in owner(self.task, r["actions_run_id"]).items()
            )
            and pod["env"].get("RCB_ATTEMPT_ID") == r["attempt_log"][-1]["intent_id"]
        )

    def adopt(self, r: dict[str, Any]) -> dict[str, Any] | None:
        if r["pod_id"]:
            pod = self.api.get(r["pod_id"], 5)
            matches = [pod] if pod else []
        else:
            matches = [
                p
                for p in self.api.inventory()
                if p.get("name") == r["name"]
                or p.get("env", {}).get("RCB_ATTEMPT_ID") == r["attempt_log"][-1]["intent_id"]
            ]
        if not matches:
            self.save(r, "CREATE_UNKNOWN" if not r["pod_id"] else "CLEANUP_PENDING")
            return None
        if len(matches) != 1 or not self.owns(r, matches[0]):
            raise ProviderError("allocation_attempt_ownership_ambiguous")
        pod = matches[0]
        if (
            not isinstance(pod.get("id"), str)
            or not re.fullmatch(r"[a-z0-9]{1,64}", pod["id"])
            or (r["pod_id"] and pod["id"] != r["pod_id"])
        ):
            raise ProviderError("pod_identity_invalid")
        r["pod_id"] = pod["id"]
        r["selected"]["data_center"] = pod["dataCenterId"]
        r["attempt_log"][-1].update(
            pod_id=pod["id"],
            actual_data_center=pod["dataCenterId"],
            allocation_confirmed_at=self.now(),
        )
        self.save(r)
        return pod

    def reconcile(self) -> dict[str, Any]:
        with self.j.lock():
            r = self.record()
            if r["create_intent"] and not r["facts"]["pod_deleted_confirmed"]:
                self.adopt(r)
            return r

    def cancel(self) -> dict[str, Any]:
        with self.j.lock():
            r = self.record()
            if r["state"] == "ACCEPTED":
                return r
            r["cancelled"] = True
            self.save(r, "CLEANUP_PENDING" if r["create_intent"] else "CANCELLED")
        return self.cleanup()

    def collect(self) -> dict[str, Any]:
        try:
            r = self._collect()
        except LedgerError:
            # Preserve the original fixed code. Reacquiring a sticky lock here can
            # replace the useful failure with scope_busy; independent recovery owns it.
            raise
        except (ProviderError, ValueError, OSError, KeyError, TypeError):
            with self.j.lock():
                r = self.record()
                r["errors"].append("allocation_collection_failed")
                self.save(r, "COLLECTION_FAILED")
        if r["create_intent"] and r["state"] != "COLLECTED" and not r["facts"]["results_saved"]:
            return self.cleanup()
        return r

    def _collect(self) -> dict[str, Any]:
        with self.j.lock():
            r = self.record()
            if (
                not r["create_intent"]
                or r["facts"]["pod_deleted_confirmed"]
                or r["facts"]["results_saved"]
                or r.get("cancelled")
            ):
                return r
            destination = self.output / "bundle"
            while r["status_checks"] < 120:
                if self.now() >= r["collection_deadline"]:
                    break
                r["status_checks"] += 1
                self.save(r)
                pod = self.adopt(r)
                if pod is None:
                    return r
                rate = pod.get("cost")
                if (
                    isinstance(rate, bool)
                    or not isinstance(rate, (int, float))
                    or not math.isfinite(rate)
                    or not 0 <= rate <= float(self.task.limits.max_gpu_hourly_usd or 0)
                ):
                    raise ProviderError("observed_price_invalid_or_exceeded")
                if self.task.data_centers and pod["dataCenterId"] not in self.task.data_centers:
                    raise ProviderError("allocated_region_incompatible")
                r["allocation"]["observed_max_hourly_usd"] = max(
                    r["allocation"]["observed_max_hourly_usd"] or 0, rate
                )
                r["allocation"]["estimated_compute_usd"] = (
                    max(0, self.now() - r["attempt_log"][-1]["at"])
                    / 3600
                    * r["allocation"]["observed_max_hourly_usd"]
                )
                if pod.get("status") in {"ERROR", "EXITED", "TERMINATED"}:
                    raise ProviderError("worker_not_running")
                token = pod["env"].get("RCB_RESULT_TOKEN", "")
                if (
                    not capability_valid(token)
                    or hashlib.sha256(token.encode()).hexdigest() != r["result_capability_sha256"]
                ):
                    raise ProviderError("result_capability_unrecoverable")
                try:
                    reader = RunPodResultReader(r["pod_id"], 8000, token)
                    phase = (
                        reader.readiness(
                            self.task, r["pod_id"], min(5, r["collection_deadline"] - self.now())
                        )
                        if pod.get("status") == "RUNNING"
                        else "starting"
                    )
                    r["worker_readiness"] = phase
                    self.save(r)
                    if phase == "failed":
                        raise ProviderError("worker_refused")
                    if phase != "ready":
                        if (
                            self.now()
                            >= r["attempt_log"][-1]["at"] + self.task.limits.startup_seconds
                        ):
                            break
                        self.sleep(min(5, max(0, r["collection_deadline"] - self.now())))
                        continue
                    r.setdefault("worker_ready_at", self.now())
                    if not verify_saved(destination, self.task, r["pod_id"]):
                        if r["collect_attempts"] >= self.task.limits.max_collect_attempts:
                            break
                        r["collect_attempts"] += 1
                        self.save(r, "COLLECTING")
                        collect_https(
                            self.task,
                            r["pod_id"],
                            reader,
                            destination,
                            deadline=time.monotonic()
                            + min(15, r["collection_deadline"] - self.now()),
                        )
                    if not verify_saved(destination, self.task, r["pod_id"]):
                        raise ProviderError("result_integrity_failed")
                    manifest = json.loads((destination / "manifest.json").read_text())
                    passed = validate_manifest(self.task, r["pod_id"], manifest)
                    if passed:
                        observed = Observation.model_validate(
                            json.loads((destination / "environment.json").read_text())["observed"]
                        ).model_dump()
                        admit(self.task, r["selected"]["gpu_id"], observed)
                        r["actual_environment"] = observed
                    r["gpu_verified"], r["facts"]["experiment_passed"] = passed, passed
                    r["metrics"], r["artifact_status"] = (
                        manifest["metrics"],
                        "runner_verified_not_durable",
                    )
                    r["manifest_sha256"] = hashlib.sha256(
                        (destination / "manifest.json").read_bytes()
                    ).hexdigest()
                    self.save(r, "COLLECTED")
                    return r
                except ProviderError as exc:
                    r["errors"].append(exc.code)
                    self.save(r)
                    if not exc.retryable:
                        raise
                    if (
                        "worker_ready_at" not in r
                        and self.now()
                        >= r["attempt_log"][-1]["at"] + self.task.limits.startup_seconds
                    ):
                        break
                    self.sleep(min(5, max(0, r["collection_deadline"] - self.now())))
            self.save(r, "TIMED_OUT")
            return r

    def receipt(self, artifact_id: str, sha256: str, github: GitHubEvidence) -> dict[str, Any]:
        if not re.fullmatch(r"[1-9][0-9]*", artifact_id) or not re.fullmatch(
            r"[a-f0-9]{64}", sha256
        ):
            raise ValueError("artifact_receipt_required")
        with self.j.lock():
            r = self.record()
            path = self.output / "bundle"
            if not verify_saved(path, self.task, r["pod_id"]) or hashlib.sha256(
                (path / "manifest.json").read_bytes()
            ).hexdigest() != r.get("manifest_sha256"):
                raise ValueError("verified_bundle_required")
            meta = github.get("/actions/artifacts/" + artifact_id)
            if (
                meta.get("id") != int(artifact_id)
                or meta.get("expired") is not False
                or meta.get("digest") != "sha256:" + sha256
                or meta.get("name")
                != f"allocation-bundle-{self.j.identity['run_id']}-{self.j.identity['attempt']}"
                or meta.get("workflow_run", {}).get("id") != int(self.j.identity["run_id"])
                or meta.get("workflow_run", {}).get("head_sha") != self.j.identity["sha"]
            ):
                raise ValueError("artifact_receipt_unverified")
            r["artifact_receipt"] = {
                "id": int(artifact_id),
                "sha256": sha256,
                "run_id": self.j.identity["run_id"],
            }
            r["artifact_status"], r["facts"]["results_saved"] = (
                "github_upload_receipt_verified",
                True,
            )
            self.save(
                r,
                "ACCEPTED"
                if r["facts"]["pod_deleted_confirmed"]
                and r["facts"]["experiment_passed"]
                and not r.get("cancelled")
                else None,
            )
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
                return r
        cleanup_owned(
            self.task,
            self.j,
            AttemptCleanup(self, r),
            allow_termination=True,
            now=self.now,
            sleep=self.sleep,
        )
        with self.j.lock():
            r = self.record()
            if r["facts"]["pod_deleted_confirmed"]:
                r["deleted_confirmed_at"] = self.now()
                if r["allocation"]["observed_max_hourly_usd"] is not None:
                    r["allocation"]["estimated_compute_usd"] = (
                        max(0, self.now() - r["attempt_log"][-1]["at"])
                        / 3600
                        * r["allocation"]["observed_max_hourly_usd"]
                    )
                r["facts"]["task_completed"] = True
                self.save(
                    r,
                    "ACCEPTED"
                    if r["facts"]["results_saved"]
                    and r["facts"]["experiment_passed"]
                    and not r.get("cancelled")
                    else "REJECTED",
                )
            return r
