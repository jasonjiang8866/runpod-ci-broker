"""Conditional shared journal. Sticky locks never expire into a second controller.

SQLite is the offline test backend. GitHub's prepared Contents backend is not wired
into credential-bearing workflows. No backend performs RunPod operations.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import re
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, build_opener

from .models import Task
from .provider import NoRedirect

MAX_DOCUMENT = 500_000  # Below GitHub Contents' inline base64 response limit.


class LedgerError(ValueError):
    """Fixed diagnostic codes only; never retain an HTTP response or credential."""


class Store(Protocol):
    def read(self, key: str) -> tuple[str | None, dict[str, Any] | None]: ...
    def swap(self, key: str, version: str | None, value: dict[str, Any]) -> None: ...


def encode(value: dict[str, Any]) -> bytes:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > MAX_DOCUMENT:
        raise LedgerError("ledger_size_limit")
    return raw


def check_key(key: str) -> None:
    if not re.fullmatch(r"[a-f0-9]{64}", key):
        raise LedgerError("ledger_key_invalid")


class SQLiteStore:
    """For offline drills on a shared local disk, NOT a cross-runner cloud store."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self.connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS scopes (key TEXT PRIMARY KEY, body BLOB NOT NULL)"
            )

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=5)

    def read(self, key: str) -> tuple[str | None, dict[str, Any] | None]:
        check_key(key)
        with self.connect() as db:
            row = db.execute("SELECT body FROM scopes WHERE key=?", (key,)).fetchone()
        if row is None:
            return None, None
        raw = bytes(row[0])
        return hashlib.sha256(raw).hexdigest(), json.loads(raw)

    def swap(self, key: str, version: str | None, value: dict[str, Any]) -> None:
        check_key(key)
        raw = encode(value)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT body FROM scopes WHERE key=?", (key,)).fetchone()
            current = hashlib.sha256(bytes(row[0])).hexdigest() if row else None
            if current != version:
                raise LedgerError("ledger_conflict")
            db.execute("INSERT OR REPLACE INTO scopes VALUES (?, ?)", (key, raw))


class GitHubContentsStore:
    """Prepared remote CAS adapter; only an explicit existing broker-state branch.

    Requires a separately approved repository-scoped contents-write capability.
    Reads no RUNPOD_API_KEY; caller must keep RCB_LEDGER_TOKEN on the controller.
    No automatic retries of writes, redirects, branch creation or force updates.
    """

    def __init__(self, repository: str, branch: str = "broker-state"):
        if not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+", repository):
            raise LedgerError("ledger_repository_invalid")
        if branch != "broker-state":
            raise LedgerError("dedicated_ledger_branch_required")
        self.repository, self.branch = repository, branch
        self._token = os.environ.get("RCB_LEDGER_TOKEN")
        if not self._token:
            raise LedgerError("ledger_credentials_missing")

    def request(self, key: str, body: dict[str, Any] | None = None) -> dict[str, Any] | None:
        check_key(key)
        url = f"https://api.github.com/repos/{self.repository}/contents/scopes/{key}.json"
        if body is None:
            url += "?ref=" + quote(self.branch, safe="")
        req = Request(
            url,
            data=json.dumps(body).encode() if body is not None else None,
            method="PUT" if body is not None else "GET",
            headers={
                "Authorization": "Bearer " + str(self._token),
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "runpod-ci-broker",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with build_opener(NoRedirect).open(req, timeout=15) as response:
                raw = response.read(1_000_001)
            if len(raw) > 1_000_000:
                raise LedgerError("ledger_response_too_large")
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise LedgerError("ledger_response_invalid")
            return value
        except HTTPError as exc:
            if exc.code == 404 and body is None:
                # This is NOT sufficient to claim the branch/repository is empty;
                # a following conditional PUT must succeed before any operation.
                return None
            code = {
                401: "ledger_authentication_failed",
                403: "ledger_permission_denied",
                409: "ledger_conflict",
                422: "ledger_conflict",
            }.get(exc.code)
            raise LedgerError(code or "ledger_request_failed") from None
        except (URLError, OSError, TimeoutError):
            raise LedgerError("ledger_write_unknown" if body else "ledger_read_failed") from None
        except (ValueError, UnicodeError):
            raise LedgerError("ledger_response_invalid") from None

    def read(self, key: str) -> tuple[str | None, dict[str, Any] | None]:
        value = self.request(key)
        if value is None:
            return None, None
        try:
            if value["type"] != "file" or value["encoding"] != "base64":
                raise ValueError()
            sha = value["sha"]
            if not isinstance(sha, str) or not re.fullmatch(r"[a-f0-9]{40}", sha):
                raise ValueError()
            raw = base64.b64decode("".join(value["content"].split()), validate=True)
            if len(raw) > MAX_DOCUMENT:
                raise ValueError()
            if hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest() != sha:
                raise ValueError()
            document = json.loads(raw)
            if not isinstance(document, dict):
                raise TypeError()
            return sha, document
        except (ValueError, KeyError, TypeError, AttributeError):
            raise LedgerError("ledger_response_invalid") from None

    def swap(self, key: str, version: str | None, value: dict[str, Any]) -> None:
        if version is not None and not re.fullmatch(r"[a-f0-9]{40}", version):
            raise LedgerError("ledger_version_invalid")
        body: dict[str, Any] = {
            "branch": self.branch,
            "message": "broker: checkpoint scope",
            "content": base64.b64encode(encode(value)).decode(),
        }
        if version is not None:
            body["sha"] = version
        self.request(key, body)


class SharedJournal:
    """One CAS document per scope holds BOTH the lock and every task's intent.

    Lock ID is a process nonce, never a reusable run ID. A dead owner's lock stays
    held. Recover it only after external confirmation that the controller stopped;
    this release deliberately has no automatic takeover/reset method.
    """

    def __init__(self, store: Store, scope: str, experiment_id: str):
        self.store, self.scope, self.experiment_id = store, scope, experiment_id
        self.key = hashlib.sha256(scope.encode()).hexdigest()
        self.holder: str | None = None

    def load(self) -> tuple[str | None, dict[str, Any]]:
        version, value = self.store.read(self.key)
        if value is None:
            if version is not None:
                raise LedgerError("ledger_invalid")
            return None, {
                "schema": 1,
                "scope": self.scope,
                "revision": 0,
                "holder": None,
                "records": {},
            }
        try:
            if (
                value["schema"] != 1
                or value["scope"] != self.scope
                or type(value["revision"]) is not int
                or value["revision"] < 0
                or (
                    value["holder"] is not None
                    and not re.fullmatch(r"[a-f0-9]{32}", value["holder"])
                )
                or not isinstance(value["records"], dict)
            ):
                raise ValueError()
            for experiment, record in value["records"].items():
                task = Task.model_validate(record["request"])
                if (
                    task.scope != self.scope
                    or task.experiment_id != experiment
                    or task.fingerprint() != record["config_fingerprint"]
                    or type(record["create_intent"]) is not bool
                    or type(record["facts"]["pod_deleted_confirmed"]) is not bool
                ):
                    raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise LedgerError("ledger_invalid") from None
        return version, value

    def checkpoint(self, version: str | None, value: dict[str, Any]) -> None:
        value["revision"] += 1  # prevent normal ABA; administrative resets are forbidden
        try:
            self.store.swap(self.key, version, value)
        except LedgerError as exc:
            if str(exc) != "ledger_write_unknown":
                raise
            # A lost write response is reconciled by exact read-back, never resubmitted.
        _, observed = self.load()
        if observed != value:
            raise LedgerError("ledger_write_unconfirmed")

    @contextmanager
    def lock(self) -> Iterator[None]:
        if self.holder is not None:
            raise LedgerError("scope_busy")
        version, value = self.load()
        if value["holder"] is not None:
            raise LedgerError("scope_busy_manual_recovery_required")
        holder = uuid.uuid4().hex
        value["holder"] = holder
        self.checkpoint(version, value)
        self.holder = holder
        try:
            yield
        finally:
            try:
                version, value = self.owned()
                value["holder"] = None
                self.checkpoint(version, value)
            finally:
                self.holder = None

    def owned(self) -> tuple[str | None, dict[str, Any]]:
        version, value = self.load()
        if self.holder is None or value["holder"] != self.holder:
            raise LedgerError("ledger_lock_lost")
        return version, value

    def read(self) -> dict[str, Any] | None:
        _, value = self.load()
        record: dict[str, Any] | None = value["records"].get(self.experiment_id)
        return copy.deepcopy(record)

    def write(self, record: dict[str, Any]) -> None:
        version, value = self.owned()
        task = Task.model_validate(record["request"])
        if (
            task.scope != self.scope
            or task.experiment_id != self.experiment_id
            or task.fingerprint() != record["config_fingerprint"]
        ):
            raise LedgerError("ledger_identity_mismatch")
        value["records"][self.experiment_id] = copy.deepcopy(record)
        self.checkpoint(version, value)

    def other_unresolved(self) -> bool:
        _, value = self.owned()
        return any(
            r["create_intent"] and not r["facts"]["pod_deleted_confirmed"]
            for key, r in value["records"].items()
            if key != self.experiment_id
        )
