"""Crash-resumable lifecycle exercised ONLY against FakeProvider in this release."""

from __future__ import annotations

import hashlib
import random
from pathlib import Path
from typing import Any

from .fake import FakeProvider
from .models import Authorization, Candidate, Task
from .planner import authorize, plan, reasons
from .provider import ProviderError, delay
from .results import save_bundle, verify_saved
from .storage import TaskJournal

TERMINAL = {"ACCEPTED", "REJECTED", "NO_CAPACITY", "BUDGET_INSUFFICIENT", "PREFLIGHT_BLOCKED"}
EDGES = {
    "ACCEPTED": {"REJECTED"},
    "PREFLIGHT": {"WAITING_CAPACITY", "CREATING", "PREFLIGHT_BLOCKED", "BUDGET_INSUFFICIENT"},
    "WAITING_CAPACITY": {"PREFLIGHT", "NO_CAPACITY", "PREFLIGHT_BLOCKED"},
    "CREATING": {"STARTING", "CREATE_UNKNOWN", "WAITING_CAPACITY", "PREFLIGHT_BLOCKED"},
    "CREATE_UNKNOWN": {"STARTING", "CLEANING", "CLEANUP_PENDING"},
    "STARTING": {"RUNNING", "COLLECTING", "RUN_FAILED", "TIMED_OUT", "CLEANING"},
    "RUNNING": {"COLLECTING", "RUN_FAILED", "TIMED_OUT", "CLEANING"},
    "RUN_FAILED": {"COLLECTING", "CLEANING"},
    "TIMED_OUT": {"COLLECTING", "CLEANING"},
    "COLLECTING": {"CLEANING", "COLLECTION_FAILED"},
    "COLLECTION_FAILED": {"COLLECTING", "CLEANING"},
    "CLEANING": {"CLEANUP_PENDING", "ACCEPTING"},
    "CLEANUP_PENDING": {"CLEANING"},
    "ACCEPTING": {"ACCEPTED", "REJECTED"},
}


def owner(task: Task, run_id: str) -> dict[str, str]:
    return dict(
        zip(
            ("RCB_TOOL", "RCB_SCOPE", "RCB_EXPERIMENT", "RCB_CONFIG", "RCB_RUN", "RCB_SOURCE"),
            (
                "runpod-ci-broker/v1",
                task.scope,
                task.experiment_id,
                task.fingerprint(),
                run_id,
                task.source_sha,
            ),
            strict=True,
        )
    )


class Engine:
    def __init__(
        self,
        task: Task,
        provider: FakeProvider,
        journal: TaskJournal,
        results_dir: Path,
        run_id: str = "local-mock",
    ):
        if type(provider) is not FakeProvider:
            raise ValueError("live_execution_not_implemented")
        self.task, self.p, self.j, self.results_dir = task, provider, journal, results_dir
        self.run_id = run_id
        previous = journal.read()
        if previous:
            if previous["config_fingerprint"] != task.fingerprint():
                raise ValueError("experiment_id_reused_with_different_config")
            if previous["mode"] != "mock":
                raise ValueError("journal_mode_mismatch")
            self.r = previous
            if run_id not in self.r["resume_run_ids"]:
                self.r["resume_run_ids"].append(run_id)
        else:
            name = (
                "rcb-"
                + hashlib.sha256((task.scope + ":" + task.experiment_id).encode()).hexdigest()[:24]
            )
            self.r = {
                **plan(task, [], mode="mock"),
                "state": "PREFLIGHT",
                "name": name,
                "actions_run_id": run_id,
                "resume_run_ids": [run_id],
                "pod_id": None,
                "create_intent": False,
                "attempts": 0,
                "query_count": 0,
                "collect_attempts": 0,
                "events": [],
                "errors": [],
                "started_at": provider.now(),
                "intent_at": None,
                "exit_code": None,
                "cleanup_verification": "not_applicable",
                "artifact_status": "not_collected",
                "metrics": {},
                "planning_history": [],
            }
        self.markers = owner(task, self.r["actions_run_id"])

    def save(self) -> None:
        self.j.write(self.r)

    def transition(self, state: str) -> None:
        before = self.r["state"]
        if state not in EDGES.get(before, set()):
            raise ValueError(f"invalid_transition:{before}:{state}")
        self.r["state"] = state
        self.r["events"].append({"from": before, "to": state, "at": self.p.now()})
        self.save()

    def owns(self, pod: dict[str, Any]) -> bool:
        return (
            pod.get("name") == self.r["name"]
            and pod.get("image") == self.task.image
            and all(pod.get("env", {}).get(k) == v for k, v in self.markers.items())
            and (self.r["pod_id"] is None or pod.get("id") == self.r["pod_id"])
            and pod.get("mounts", {}) == self.mounts()
        )

    def mounts(self) -> dict[str, Any]:
        v = self.task.volume
        return {} if v.kind == "none" else {v.kind: [{"volumeId": v.id, "path": v.mount_path}]}

    def matches(self) -> list[dict[str, Any]]:
        pods = self.p.list_pods()
        matches = [
            p
            for p in pods
            if p.get("name") == self.r["name"]
            or (self.r["pod_id"] is not None and p.get("id") == self.r["pod_id"])
        ]
        if len(matches) > 1 or any(not self.owns(p) for p in matches):
            raise ProviderError("ownership_ambiguous")
        return matches

    def recover_create(self) -> bool:
        try:
            pods = self.matches()
            if not pods:
                # Even repeated empty inventory cannot prove an ambiguous POST failed.
                self.r["cleanup_verification"] = "creation_unresolved"
                self.save()
                return False
            self.r["pod_id"] = pods[0]["id"]
            self.transition("STARTING")
            return True
        except ProviderError as exc:
            self.r["errors"].append(exc.code)
            self.save()
            return False

    def cleanup(self) -> None:
        if self.r["state"] == "CLEANUP_PENDING":
            self.transition("CLEANING")
        if self.r["state"] != "CLEANING":
            self.transition("CLEANING")
        self.r["cleanup_verification"] = "pending"
        self.r["facts"]["pod_deleted_confirmed"] = False
        self.save()
        absent = 0
        cleanup_start = self.p.now()
        for i in range(3):
            if i and self.p.now() - cleanup_start >= self.task.limits.cleanup_seconds:
                break
            try:
                pods = self.matches()
                if not pods:
                    if not self.r["pod_id"]:
                        break  # unknown create cannot be "confirmed deleted"
                    absent += 1
                    if absent >= 2:
                        self.r["facts"]["pod_deleted_confirmed"] = True
                        self.r["cleanup_verification"] = "verified_absent_mock"
                        self.r["cleanup_checked_at"] = self.p.now()
                        self.transition("ACCEPTING")
                        return
                else:
                    absent = 0
                    self.r["pod_id"] = pods[0]["id"]
                    self.save()  # durable ownership evidence BEFORE destructive call
                    self.p.delete(self.r["pod_id"])
            except ProviderError as exc:
                self.r["errors"].append(exc.code)
                if not exc.retryable:
                    break
            if i < 2:
                self.p.sleep(1)
        self.transition("CLEANUP_PENDING")

    def accept(self) -> None:
        self.r["facts"]["results_saved"] = verify_saved(
            self.results_dir, self.task, self.r["pod_id"]
        )
        passed = (
            self.r["facts"]["experiment_passed"] is True
            and self.r["facts"]["results_saved"]
            and self.r["facts"]["pod_deleted_confirmed"]
        )
        self.r["facts"]["task_completed"] = True
        self.transition("ACCEPTED" if passed else "REJECTED")

    def work_step(self) -> None:
        now = self.p.now()
        b = self.task.limits
        assert b.runtime_seconds is not None
        deadline = self.r["intent_at"] + b.startup_seconds + b.runtime_seconds
        if now >= deadline or (
            self.r["state"] == "STARTING" and now >= self.r["intent_at"] + b.startup_seconds
        ):
            self.r["facts"]["experiment_passed"] = False
            self.transition("TIMED_OUT")
            return
        try:
            if not self.matches():
                raise ProviderError("pod_missing")
            obs = self.p.observe(self.r["pod_id"])
        except ProviderError as exc:
            self.r["errors"].append(exc.code)
            if exc.retryable:
                self.p.sleep(1)
                self.save()
                return
            self.r["facts"]["experiment_passed"] = False
            self.transition("RUN_FAILED")
            return
        if "environment" in obs:
            c = Candidate.model_validate(obs["environment"])
            # Second admission on observed runtime identity, never accept a CPU fallback.
            mismatch = reasons(self.task, c)
            selected = self.r["selected"]
            if selected and (
                c.gpu_id != selected["gpu_id"] or c.data_center != selected["data_center"]
            ):
                mismatch.append("allocated_environment_mismatch")
            self.r["actual_environment"] = c.model_dump()
            if mismatch:
                self.r["errors"].extend(mismatch)
                self.r["facts"]["experiment_passed"] = False
                self.transition("RUN_FAILED")
                return
        if obs["state"] == "failed":
            self.r["errors"].append(obs["error"])
            self.r["facts"]["experiment_passed"] = False
            self.transition("RUN_FAILED")
        elif obs["state"] == "finished":
            self.r["exit_code"] = obs["exit_code"]
            self.r["facts"]["experiment_passed"] = obs["exit_code"] == 0
            self.r["collection_started_at"] = now
            self.transition("COLLECTING")
        elif obs["state"] == "running" and self.r["state"] == "STARTING":
            self.transition("RUNNING")
        self.p.sleep(1)

    def collect(self) -> None:
        b = self.task.limits
        deadline = self.r["collection_started_at"] + b.collect_seconds
        if self.p.now() >= deadline or self.r["collect_attempts"] >= b.max_collect_attempts:
            self.r["artifact_status"] = "collection_failed_deadline_or_attempt_limit"
            self.transition("CLEANING")
            return
        self.r["collect_attempts"] += 1
        self.save()
        try:
            manifest, files = self.p.collect(self.r["pod_id"], self.task)
            passed = save_bundle(self.task, self.r["pod_id"], manifest, files, self.results_dir)
            self.r["metrics"] = manifest["metrics"]
            self.r["artifact_manifest"] = manifest
            self.r["artifact_location"] = str(self.results_dir)
            self.r["logs_status"] = "verified_mock_worker_log"
            self.r["facts"]["results_saved"] = True
            self.r["facts"]["experiment_passed"] = (
                self.r["facts"]["experiment_passed"] is True and passed
            )
            self.r["artifact_status"] = "verified_on_runner_mock"
            self.transition("CLEANING")
        except (ProviderError, ValueError, OSError) as exc:
            self.r["errors"].append(
                exc.code if isinstance(exc, ProviderError) else "result_validation_or_io_failed"
            )
            self.r["artifact_status"] = "collection_failed"
            self.transition("COLLECTION_FAILED")

    def preflight(self) -> None:
        b = self.task.limits
        self.r["query_count"] += 1
        try:
            candidates = self.p.snapshot(self.task)
        except ProviderError as exc:
            self.r["errors"].append(exc.code)
            self.r["last_query_error"] = exc.code
            if exc.retryable:
                self.transition("WAITING_CAPACITY")
            else:
                self.transition("PREFLIGHT_BLOCKED")
            return
        self.r["last_query_error"] = None
        report = plan(self.task, candidates)
        self.r.setdefault("planning_history", []).append(
            {
                "at": self.p.now(),
                "query": self.r["query_count"],
                "decisions": report["decisions"],
                "selected": report["selected"],
            }
        )
        for key in ("selected", "decisions", "budget", "blockers"):
            self.r[key] = report[key]
        if report["blockers"]:
            if (
                "budget_insufficient" in report["blockers"]
                or "budget_inputs_missing" in report["blockers"]
            ):
                self.transition("BUDGET_INSUFFICIENT")
            else:
                self.transition("WAITING_CAPACITY")
            return
        try:
            if self.j.other_unresolved():
                raise ProviderError("scope_unresolved_task")
            pods = self.p.list_pods()
            if any(
                p.get("name") == self.r["name"]
                or (
                    p.get("env", {}).get("RCB_SCOPE") == self.task.scope
                    and p.get("env", {}).get("RCB_TOOL") == "runpod-ci-broker/v1"
                )
                for p in pods
            ):
                raise ProviderError("scope_active_or_unjournaled_pod")
        except ProviderError as exc:
            self.r["errors"].append(exc.code)
            self.transition("PREFLIGHT_BLOCKED")
            return
        if self.r["attempts"] >= b.max_start_attempts:
            self.transition("PREFLIGHT_BLOCKED")
            return
        c = Candidate.model_validate(report["selected"])
        self.r["attempts"] += 1
        self.r["create_intent"] = True
        self.r["intent_at"] = self.p.now()
        self.transition("CREATING")  # fsync intent BEFORE create
        payload = {
            "name": self.r["name"],
            "image": self.task.image,
            "env": self.markers,
            "mounts": self.mounts(),
        }
        try:
            self.r["pod_id"] = self.p.create(payload, c)
            self.transition("STARTING")
        except ProviderError as exc:
            self.r["errors"].append(exc.code)
            if exc.ambiguous:
                self.transition("CREATE_UNKNOWN")
            else:
                self.r["create_intent"] = False  # only an explicit rejected create
                if exc.retryable and self.r["attempts"] < b.max_start_attempts:
                    self.transition("WAITING_CAPACITY")
                else:
                    self.transition("PREFLIGHT_BLOCKED")

    def run(self, authorization: Authorization | None, *, max_steps: int = 30000) -> dict[str, Any]:
        with self.j.lock():
            authorize(self.task, authorization)
            # Reload under lock; another process may have advanced since construction.
            latest = self.j.read()
            if latest:
                if latest["config_fingerprint"] != self.task.fingerprint():
                    raise ValueError("experiment_id_reused_with_different_config")
                self.r = latest
                self.markers = owner(self.task, self.r["actions_run_id"])
            if self.run_id not in self.r["resume_run_ids"]:
                self.r["resume_run_ids"].append(self.run_id)
            if self.r["state"] == "CREATING":
                self.transition("CREATE_UNKNOWN")
            self.save()
            for _ in range(max_steps):
                state = self.r["state"]
                if state in TERMINAL:
                    if state == "ACCEPTED" and not verify_saved(
                        self.results_dir, self.task, self.r["pod_id"]
                    ):
                        self.r["facts"]["results_saved"] = False
                        self.r["artifact_status"] = "previous_export_missing_or_corrupt"
                        self.transition("REJECTED")
                    break
                if state == "PREFLIGHT":
                    self.preflight()
                elif state == "WAITING_CAPACITY":
                    wait = delay(self.r["query_count"], random.random())
                    if (
                        self.r["query_count"] >= self.task.limits.max_queries
                        or self.p.now() - self.r["started_at"] + wait
                        > self.task.limits.max_wait_seconds
                    ):
                        all_none = bool(self.r["decisions"]) and all(
                            d["candidate"]["capacity"] == "none"
                            for d in self.r["decisions"]
                            if d["candidate"]["gpu_id"] in self.task.gpu_allowlist
                        )
                        self.transition(
                            "NO_CAPACITY"
                            if all_none and not self.r.get("last_query_error")
                            else "PREFLIGHT_BLOCKED"
                        )
                    else:
                        self.p.sleep(wait)
                        self.transition("PREFLIGHT")
                elif state == "CREATE_UNKNOWN":
                    if not self.recover_create():
                        break
                elif state in {"STARTING", "RUNNING"}:
                    self.work_step()
                elif state in {"RUN_FAILED", "TIMED_OUT"}:
                    self.r["collection_started_at"] = self.p.now()
                    self.transition("COLLECTING")
                elif state == "COLLECTING":
                    self.collect()
                elif state == "COLLECTION_FAILED":
                    self.p.sleep(1)
                    self.transition("COLLECTING")
                elif state in {"CLEANING", "CLEANUP_PENDING"}:
                    self.cleanup()
                    if self.r["state"] == "CLEANUP_PENDING":
                        break
                elif state == "ACCEPTING":
                    self.accept()
            self.save()
            return self.r

    def reconcile(self, *, cleanup_only: bool = False) -> dict[str, Any]:
        """Independent bounded recovery; no new create, even with lost journal response."""
        with self.j.lock():
            latest = self.j.read()
            if not latest:
                raise ValueError("trusted_journal_required")
            if latest["config_fingerprint"] != self.task.fingerprint():
                raise ValueError("journal_identity_mismatch")
            self.r = latest
            self.markers = owner(self.task, self.r["actions_run_id"])
            if self.run_id not in self.r["resume_run_ids"]:
                self.r["resume_run_ids"].append(self.run_id)
            if self.r["state"] == "CREATING":
                self.transition("CREATE_UNKNOWN")
            if self.r["state"] == "CREATE_UNKNOWN" and not self.recover_create():
                return self.r
            if self.r["state"] in TERMINAL:
                return self.r
            if self.r["state"] == "ACCEPTING":
                self.accept()
                return self.r
            if self.r["create_intent"]:
                if cleanup_only:
                    self.r["facts"]["experiment_passed"] = False
                    self.r["errors"].append("cancelled_or_cleanup_requested")
                self.cleanup()
                if self.r["state"] == "ACCEPTING":
                    self.accept()
            self.save()
            return self.r
