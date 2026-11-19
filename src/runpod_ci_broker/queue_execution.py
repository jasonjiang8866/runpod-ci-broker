"""Bind a batch recommendation, controller identity and cleanup evidence to one claim."""

from __future__ import annotations

from typing import Any, Literal

from .allocation import BoundedController
from .batch import BatchCatalog, BatchReplay, batch_plan
from .ledger import LedgerError
from .optimization import digest
from .queue import Dispatch, Entry, QueueState, TaskQueue, task_key


class QueuedExecution:
    def __init__(self, queue_name: str, controller: BoundedController):
        self.controller = controller
        self.queue = TaskQueue(controller.j.store, queue_name, clock=controller.now)
        self.key = task_key(controller.task.scope, controller.task.experiment_id)

    def _entry(self, state: QueueState) -> Entry:
        entry = state.entries.get(self.key)
        if entry is None or entry.task != self.controller.task:
            raise LedgerError("queue_task_mismatch")
        return entry

    def claim(self, catalog: BatchCatalog) -> str:
        c = self.controller
        if c.j.recovery or c.j.identity["event"] != "workflow_dispatch":
            raise LedgerError("queue_launch_identity_required")

        def select(state: QueueState) -> tuple[str, bool]:
            now = int(self.queue.clock())
            entry = self._entry(state)
            reason = state.reason(entry, now)
            if reason != "eligible":
                return reason, False
            if not c.permit.window_start <= c.now() < c.permit.window_end:
                return "allocation_window_closed", False
            replay = BatchReplay(queue=state, catalog=catalog, planned_at=now)
            report = batch_plan(replay)
            row = next(row for row in report["plan"]["tasks"] if row["job_key"] == self.key)
            if not row["selected"]:
                return row["reason"], False
            choice = row["selected"]
            entry.dispatch = Dispatch(
                queue_name=state.name,
                snapshot_id=report["snapshot_id"],
                gpu_id=choice["gpu_id"],
                data_center=choice["data_center"],
                hourly_microusd=choice["hourly_microusd"],
                latest_start=entry.latest_start,
                launch_identity=dict(c.j.identity),
                permit_digest=digest(c.permit.model_dump()),
            )
            entry.owner = digest(c.j.identity)
            return "claimed", True

        return self.queue._update(select)

    def _bound(self, state: QueueState) -> Dispatch:
        entry = self._entry(state)
        c = self.controller
        binding = entry.dispatch
        if binding is None or binding.permit_digest != digest(c.permit.model_dump()):
            raise LedgerError("queue_dispatch_mismatch")
        if c.j.recovery:
            if not c.j.completed(dict(binding.launch_identity)):
                raise LedgerError("queue_launch_not_completed")
        elif c.j.identity != binding.launch_identity:
            raise LedgerError("queue_controller_mismatch")
        return binding

    def start(self, *, confirmed: bool = False) -> dict[str, Any]:
        def load_active() -> Dispatch:
            # Even a stale read must not reopen a retired pre-intent claim. Verify
            # its version with CAS while settlement is excluded by the journal lock.
            version, state = self.queue._load()
            dispatch = self._bound(state)
            if self._entry(state).release:
                raise LedgerError("queue_claim_closed")
            self.queue.store.swap(self.queue.key, version, state.model_dump())
            return dispatch

        record = self.controller.start(confirmed=confirmed, queue_dispatch=load_active)
        self.settle()
        return record

    def _locked_record(self, binding: Dispatch) -> dict[str, Any] | None:
        c = self.controller
        _, scope = c.j.owned()
        stored = scope["records"].get(c.task.experiment_id)
        if stored is None:
            return None
        record = c.record()
        if (
            record != stored
            or record["allocation"].get("queue_dispatch") != binding.model_dump()
            or record["actions_run_id"] != binding.launch_identity["run_id"]
            or record["actions_run_attempt"] != binding.launch_identity["attempt"]
        ):
            raise LedgerError("queue_journal_mismatch")
        return record

    def settle(self) -> str:
        """Take evidence only from the locked, validated original allocation journal."""
        c = self.controller
        with c.j.lock():
            state = self.queue.status()
            binding = self._bound(state)
            entry = self._entry(state)
            if entry.release == "no_create":
                return entry.release
            r = self._locked_record(binding)
            if entry.release and (not r or not r["facts"]["pod_deleted_confirmed"]):
                raise LedgerError("queue_journal_mismatch")
            release: Literal["no_create", "verified_deleted"] | None = None
            status: dict[str, str | bool] = {}
            pending = "awaiting_controller"
            if r is None:
                # Only the independent recovery runner with a completed original run
                # may retire an abandoned claim before its first durable intent.
                if not c.j.recovery:
                    return "awaiting_controller"
                release = "no_create"
            else:
                status = {
                    "state": r["state"],
                    "worker_readiness": r.get("worker_readiness", "unobserved"),
                    "startup_diagnostics": (r.get("startup_logs") or {}).get(
                        "status", "unobserved"
                    ),
                    "cleanup_verification": r["cleanup_verification"],
                    "results_saved": r["facts"]["results_saved"],
                }
                if (
                    r["create_intent"]
                    and r["facts"]["pod_deleted_confirmed"]
                    and r["cleanup_verification"] == "direct_404_and_complete_inventory_twice"
                ):
                    release = "verified_deleted"
                elif not r["create_intent"] and (
                    r["state"] in {"NO_CAPACITY", "TIMED_OUT", "PREFLIGHT_BLOCKED", "CANCELLED"}
                    or c.j.recovery
                ):
                    if any(
                        a["outcome"] not in {"not_sent", "capacity_refused", "rejected"}
                        for a in r["attempt_log"]
                    ):
                        raise LedgerError("queue_attempt_unresolved")
                    release = "no_create"
                else:
                    pending = "cleanup_pending" if r["create_intent"] else "awaiting_controller"

            def close(current: QueueState) -> tuple[str, bool]:
                target = self._entry(current)
                if target.dispatch != binding:
                    raise LedgerError("queue_dispatch_mismatch")
                changed = target.release != release or target.execution_status != status
                target.release, target.execution_status = release, status
                return release or pending, changed

            return self.queue._update(close)

    def recover(self) -> str:
        """Recover an owned resource and settle. This entry point has no create path."""
        c = self.controller
        if not c.j.recovery:
            raise LedgerError("queue_recovery_identity_required")
        binding = self._bound(self.queue.status())
        with c.j.lock():
            has_record = self._locked_record(binding) is not None
        if has_record:
            c.cleanup()
        return self.settle()
