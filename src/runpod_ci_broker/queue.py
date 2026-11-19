"""Persistent admission queue. Claims reserve a slot and money in one store mutation."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Annotated, Literal, Self

from pydantic import Field, TypeAdapter, ValidationError, model_validator

from .ledger import LedgerError, Store
from .models import Identifier, Model, Task
from .optimization import digest, microusd
from .planner import estimate

Amount = Annotated[int, Field(ge=0)]
PositiveInt = Annotated[int, Field(gt=0)]
IDENTIFIER = TypeAdapter(Identifier)


class QueuePolicy(Model):
    budget_microusd: PositiveInt
    opening_reserved_microusd: Amount
    slots: PositiveInt

    @model_validator(mode="after")
    def check_opening_balance(self) -> Self:
        if self.opening_reserved_microusd > self.budget_microusd:
            raise ValueError("opening_reservation_exceeds_budget")
        return self


class Dispatch(Model):
    queue_name: Identifier
    snapshot_id: str
    gpu_id: str
    data_center: Identifier
    hourly_microusd: Amount
    permit_digest: str
    launch_identity: dict[str, str | int]
    latest_start: Amount | None


class Entry(Model):
    task: Task
    priority: int
    enqueued_at: Amount
    latest_start: Amount | None
    incremental_microusd: Amount
    owner: Identifier | None
    dispatch: Dispatch | None = Field(default=None, exclude_if=lambda v: v is None)
    release: Literal["verified_deleted", "no_create"] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    execution_status: dict[str, str | bool] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )


def task_key(scope: str, experiment_id: str) -> str:
    return digest([scope, experiment_id])


def reservation(task: Task, policy: QueuePolicy) -> int:
    cost = estimate(task)
    if not cost["complete"] or not cost["within_budget"]:
        raise LedgerError("task_budget_invalid")
    if microusd(task.limits.prior_spend_usd) > policy.opening_reserved_microusd:
        raise LedgerError("prior_spend_outside_opening_balance")
    return int(cost["incremental_reservation_microusd"])


class QueueState(Model):
    schema_version: Literal[1]
    name: Identifier
    policy: QueuePolicy
    entries: dict[str, Entry]

    @property
    def slots_used(self) -> int:
        return sum(e.owner is not None and e.release is None for e in self.entries.values())

    @property
    def reserved_microusd(self) -> int:
        return self.policy.opening_reserved_microusd + sum(
            e.incremental_microusd for e in self.entries.values() if e.owner is not None
        )

    @model_validator(mode="after")
    def verify(self) -> Self:
        for key, entry in self.entries.items():
            if entry.dispatch and (
                entry.owner != digest(entry.dispatch.launch_identity)
                or entry.dispatch.queue_name != self.name
                or entry.dispatch.latest_start != entry.latest_start
            ):
                raise ValueError("queue_dispatch_invalid")
            if entry.release and not entry.dispatch:
                raise ValueError("queue_release_invalid")
            if key != task_key(entry.task.scope, entry.task.experiment_id) or (
                entry.incremental_microusd != reservation(entry.task, self.policy)
            ):
                raise ValueError("queue_entry_invalid")
        if (
            self.slots_used > self.policy.slots
            or self.reserved_microusd > self.policy.budget_microusd
        ):
            raise ValueError("queue_capacity_exceeded")
        return self

    def reason(self, entry: Entry, now: int) -> str:
        if entry.release:
            return entry.release
        if entry.owner is not None:
            return "already_claimed"
        if entry.latest_start is not None and now >= entry.latest_start:
            return "start_deadline_elapsed"
        if self.reserved_microusd + entry.incremental_microusd > self.policy.budget_microusd:
            return "budget_insufficient"
        if self.slots_used >= self.policy.slots:
            return "slots_full"
        return "eligible"


class TaskQueue:
    def __init__(self, store: Store, name: str, *, clock: Callable[[], float] = time.time):
        self.name = IDENTIFIER.validate_python(name, strict=True)
        self.key = digest(["broker-admission-queue-v1", self.name])
        self.store = store
        self.clock = clock

    def _read(self) -> tuple[str | None, QueueState | None]:
        version, raw = self.store.read(self.key)
        if version is None and raw is None:
            return None, None
        if version is None or raw is None:
            raise LedgerError("queue_state_invalid")
        try:
            state = QueueState.model_validate(raw)
        except ValidationError:
            raise LedgerError("queue_state_invalid") from None
        if state.name != self.name:
            raise LedgerError("queue_state_invalid")
        return version, state

    def initialize(self, policy: QueuePolicy) -> None:
        # Initialization alone may create a document. A missing read during
        # submit/claim can never silently create an empty budget.
        version, state = self._read()
        if state is None:
            try:
                state = QueueState(schema_version=1, name=self.name, policy=policy, entries={})
                self.store.swap(self.key, version, state.model_dump())
                return
            except LedgerError as exc:
                if str(exc) != "ledger_conflict":
                    raise
            _, state = self._read()
        if state is None or state.policy != policy:
            raise LedgerError("queue_policy_conflict")

    def _load(self) -> tuple[str, QueueState]:
        version, state = self._read()
        if version is None or state is None:
            raise LedgerError("queue_missing")
        return version, state

    def _update(self, apply: Callable[[QueueState], tuple[str, bool]]) -> str:
        # Only a definitive CAS conflict permits another mutation attempt.
        for _ in range(8):
            version, state = self._load()
            outcome, changed = apply(state)
            if not changed:
                return outcome
            try:
                self.store.swap(self.key, version, state.model_dump())
                return outcome
            except LedgerError as exc:
                if str(exc) != "ledger_conflict":
                    raise
        raise LedgerError("queue_contention")

    def submit(self, task: Task, *, priority: int = 0, latest_start: int | None = None) -> str:
        key = task_key(task.scope, task.experiment_id)
        submitted_at = int(self.clock())

        def insert(state: QueueState) -> tuple[str, bool]:
            entry = Entry(
                task=task,
                priority=priority,
                latest_start=latest_start,
                enqueued_at=submitted_at,
                incremental_microusd=reservation(task, state.policy),
                owner=None,
            )
            existing = state.entries.get(key)
            if existing is not None:
                if (existing.task, existing.priority, existing.latest_start) != (
                    task,
                    priority,
                    latest_start,
                ):
                    raise LedgerError("queue_task_conflict")
                return "already_submitted", False
            state.entries[key] = entry
            return "submitted", True

        return self._update(insert)

    def claim(self, scope: str, experiment_id: str, owner: str) -> str:
        """Claim a specific entry proposed by a caller. Only 'claimed' is a new grant."""
        owner = IDENTIFIER.validate_python(owner, strict=True)

        def assign(state: QueueState) -> tuple[str, bool]:
            entry = state.entries.get(task_key(scope, experiment_id))
            if entry is None:
                raise LedgerError("queue_task_missing")
            reason = state.reason(entry, int(self.clock()))
            if reason != "eligible":
                return reason, False
            entry.owner = owner
            return "claimed", True

        return self._update(assign)

    def status(self) -> QueueState:
        return self._load()[1]
