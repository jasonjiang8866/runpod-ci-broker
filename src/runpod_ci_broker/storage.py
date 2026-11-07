"""Atomic local journal and per-scope flock. NOT a distributed/account lock."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any, Protocol


class TaskJournal(Protocol):
    def lock(self) -> AbstractContextManager[None]: ...
    def read(self) -> dict[str, Any] | None: ...
    def write(self, record: dict[str, Any]) -> None: ...
    def other_unresolved(self) -> bool: ...


def atomic_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".writing-")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, sort_keys=True, allow_nan=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        directory_fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


class Journal:
    def __init__(self, root: Path, scope: str, experiment_id: str):
        # Paths derive from hashes, never untrusted filesystem fragments.
        self.scope_dir = root / hashlib.sha256(scope.encode()).hexdigest()[:24]
        self.path = self.scope_dir / (hashlib.sha256(experiment_id.encode()).hexdigest() + ".json")

    @contextmanager
    def lock(self) -> Iterator[None]:
        self.scope_dir.mkdir(parents=True, exist_ok=True)
        with (self.scope_dir / "scope.lock").open("a") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError("scope_busy") from None
            try:
                yield
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def read(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        value: dict[str, Any] = json.loads(self.path.read_text())
        return value

    def write(self, record: dict[str, Any]) -> None:
        atomic_json(self.path, record)

    def other_unresolved(self) -> bool:
        for p in self.scope_dir.glob("*.json"):
            if p != self.path:
                record = json.loads(p.read_text())
                if record.get("create_intent") and not record["facts"]["pod_deleted_confirmed"]:
                    return True
        return False
