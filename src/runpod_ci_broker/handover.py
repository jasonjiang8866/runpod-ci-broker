"""Sticky lock takeover only after the exact GitHub run attempt has completed."""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener

from .campaign import REPOSITORY
from .ledger import LedgerError, SharedJournal, Store
from .provider import NoRedirect

WORKFLOWS = {".github/workflows/gpu-smoke.yml", ".github/workflows/recover-smoke.yml"}


def context() -> dict[str, Any]:
    run = os.environ.get("GITHUB_RUN_ID", "")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "")
    sha = os.environ.get("GITHUB_SHA", "")
    workflow = os.environ.get("GITHUB_WORKFLOW_REF", "")
    paths = [p for p in WORKFLOWS if workflow == f"{REPOSITORY}/{p}@refs/heads/main"]
    if (
        os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
        or os.environ.get("GITHUB_REF") != "refs/heads/main"
        or not re.fullmatch(r"[1-9][0-9]*", run)
        or not re.fullmatch(r"[1-9][0-9]*", attempt)
        or not re.fullmatch(r"[a-f0-9]{40}", sha)
        or len(paths) != 1
    ):
        raise LedgerError("trusted_main_controller_required")
    event = os.environ.get("GITHUB_EVENT_NAME")
    if event not in {"workflow_dispatch", "workflow_run"}:
        raise LedgerError("trusted_controller_event_required")
    if paths[0].endswith("gpu-smoke.yml") and event != "workflow_dispatch":
        raise LedgerError("manual_launch_required")
    return {"run_id": run, "attempt": int(attempt), "sha": sha, "path": paths[0], "event": event}


class GitHubEvidence:
    def get(self, path: str) -> dict[str, Any]:
        if not re.fullmatch(r"/actions/(runs/[0-9]+/attempts/[0-9]+|artifacts/[0-9]+)", path):
            raise LedgerError("github_evidence_path_invalid")
        token = os.environ.get("RCB_LEDGER_TOKEN")
        if not token:
            raise LedgerError("ledger_credentials_missing")
        req = Request(
            "https://api.github.com/repos/" + REPOSITORY + path,
            headers={
                "Authorization": "Bearer " + token,
                "Accept": "application/vnd.github+json",
                "User-Agent": "runpod-ci-broker",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with build_opener(NoRedirect).open(req, timeout=10) as response:
                raw = response.read(1_000_001)
            if len(raw) > 1_000_000:
                raise ValueError()
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise TypeError()
            return value
        except (HTTPError, URLError, OSError, ValueError, TypeError):
            raise LedgerError("github_evidence_unavailable") from None

    def completed(self, identity: dict[str, Any]) -> bool:
        try:
            if (
                not re.fullmatch(r"[1-9][0-9]*", identity["run_id"])
                or type(identity["attempt"]) is not int
                or identity["attempt"] < 1
                or identity["path"] not in WORKFLOWS
            ):
                return False
            value = self.get(f"/actions/runs/{identity['run_id']}/attempts/{identity['attempt']}")
            return bool(
                value["status"] == "completed"
                and value["id"] == int(identity["run_id"])
                and value["run_attempt"] == identity["attempt"]
                and value["head_sha"] == identity["sha"]
                and value["head_branch"] == "main"
                and value["event"] == identity["event"]
                and value["path"] == identity["path"]
                and value["repository"]["full_name"] == REPOSITORY
                and value["head_repository"]["full_name"] == REPOSITORY
            )
        except (KeyError, TypeError, ValueError):
            return False


class ControllerJournal(SharedJournal):
    def __init__(
        self,
        store: Store,
        scope: str,
        experiment: str,
        identity: dict[str, Any],
        completed: Callable[[dict[str, Any]], bool],
        *,
        recovery: bool = False,
    ):
        super().__init__(store, scope, experiment)
        self.identity, self.completed, self.recovery = identity, completed, recovery

    @contextmanager
    def lock(self) -> Iterator[None]:
        if self.holder is not None:
            raise LedgerError("scope_busy")
        version, value = self.load()
        if value["holder"] is not None:
            previous = value.get("controller")
            if not self.recovery or not isinstance(previous, dict):
                raise LedgerError("scope_busy_manual_recovery_required")
            # Never steal a lock based on TTL or a user-supplied 'stopped' flag.
            if not self.completed(previous):
                raise LedgerError("previous_controller_not_confirmed_completed")
            value.setdefault("handovers", []).append({"from": previous, "to": self.identity})
        holder = uuid.uuid4().hex
        value["holder"], value["controller"] = holder, self.identity
        self.checkpoint(version, value)  # CAS preserves every unresolved record
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
