"""Prepared bounded real-Pod cleanup, never create/stop or manage persistent volumes.

Not wired into the public CLI/workflows yet. Activation needs the trusted shared
journal handover. Tests use an injected API; no live deletion has been exercised.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import Callable
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, build_opener

from pydantic import ValidationError

from .lifecycle import owner
from .models import Task
from .provider import NoRedirect, ProviderError, http_error
from .storage import TaskJournal
from .wire import Pod


class CleanupClient(Protocol):
    def get(self, pod_id: str, timeout: float) -> dict[str, Any] | None: ...
    def delete(self, pod_id: str, timeout: float) -> None: ...
    def active_ids(self, deadline: float) -> set[str]: ...


class CleanupAPI:
    def __init__(
        self, *, allow_termination: bool = False, now: Callable[[], float] = time.monotonic
    ):
        self._key = os.environ.get("RUNPOD_API_KEY")
        if not self._key:
            raise ProviderError("credentials_missing")
        self.allow_termination, self.now = allow_termination, now

    def request(self, path: str, timeout: float, *, delete: bool = False) -> dict[str, Any] | None:
        if timeout <= 0:
            raise ProviderError("cleanup_deadline")
        if not re.fullmatch(
            r"/pods(?:/[a-z0-9]{1,64}|\?limit=100(?:&cursor=[A-Za-z0-9%._~-]+)?)", path
        ):
            raise ProviderError("cleanup_endpoint_invalid")
        if delete and (
            not self.allow_termination or not re.fullmatch(r"/pods/[a-z0-9]{1,64}", path)
        ):
            raise ProviderError("termination_not_enabled")
        req = Request(
            "https://api.runpod.io/v2" + path,
            method="DELETE" if delete else "GET",
            headers={"Authorization": "Bearer " + str(self._key), "Accept": "application/json"},
        )
        try:
            with build_opener(NoRedirect).open(req, timeout=min(timeout, 5)) as response:
                if delete:
                    if response.status != 204:
                        raise ProviderError("termination_response_unknown", ambiguous=True)
                    return None  # receipt alone never sets the deletion fact
                raw = response.read(1_000_001)
            if len(raw) > 1_000_000:
                raise ProviderError("cleanup_response_too_large")
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ProviderError("cleanup_response_invalid")
            return value
        except HTTPError as exc:
            if exc.code == 404 and not delete and re.fullmatch(r"/pods/[a-z0-9]+", path):
                return None
            raise http_error(exc.code) from None
        except (URLError, TimeoutError, OSError):
            raise ProviderError(
                "cleanup_request_unknown", retryable=True, ambiguous=delete
            ) from None
        except (ValueError, UnicodeError):
            raise ProviderError("cleanup_response_invalid") from None

    def get(self, pod_id: str, timeout: float) -> dict[str, Any] | None:
        return self.request("/pods/" + pod_id, timeout)

    def delete(self, pod_id: str, timeout: float) -> None:
        self.request("/pods/" + pod_id, timeout, delete=True)

    def active_ids(self, deadline: float) -> set[str]:
        seen_ids: set[str] = set()
        active: set[str] = set()
        seen_cursors: set[str] = set()
        params: dict[str, str | int] = {"limit": 100}
        for _ in range(5):
            response = self.request("/pods?" + urlencode(params), deadline - self.now())
            if response is None:
                raise ProviderError("cleanup_inventory_unknown")
            try:
                rows = response["pods"]
                pagination = response["pagination"]
                if not isinstance(rows, list) or type(pagination["hasNextPage"]) is not bool:
                    raise ValueError()
                for raw in rows:
                    p = Pod.model_validate(raw)
                    if p.id in seen_ids:
                        raise ValueError()
                    seen_ids.add(p.id)
                    if p.status != "TERMINATED":
                        active.add(p.id)
                if not pagination["hasNextPage"]:
                    return active
                cursor = pagination["nextCursor"]
                if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
                    raise ValueError()
                seen_cursors.add(cursor)
                params["cursor"] = cursor
            except (ValueError, KeyError, TypeError, ValidationError):
                raise ProviderError("cleanup_inventory_unknown") from None
        raise ProviderError("cleanup_inventory_incomplete")


def cleanup_owned(
    task: Task,
    journal: TaskJournal,
    api: CleanupClient,
    *,
    allow_termination: bool = False,
    now: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Direct GET plus complete inventory twice; DELETE success is insufficient."""
    with journal.lock():
        r = journal.read()
        if (
            not r
            or r.get("mode") != "live"
            or r.get("config_fingerprint") != task.fingerprint()
            or r.get("request") != task.model_dump()
            or r.get("create_intent") is not True
        ):
            raise ValueError("trusted_live_intent_required")
        name = (
            "rcb-"
            + hashlib.sha256((task.scope + ":" + task.experiment_id).encode()).hexdigest()[:24]
        )
        if r.get("name") != name or not re.fullmatch(r"[0-9]+", str(r.get("actions_run_id", ""))):
            raise ValueError("trusted_live_identity_required")
        pod_id = r.get("pod_id")
        r["facts"]["pod_deleted_confirmed"] = False
        r["state"], r["cleanup_verification"] = "CLEANUP_PENDING", "pending"
        if not isinstance(pod_id, str) or not re.fullmatch(r"[a-z0-9]{1,64}", pod_id):
            r["cleanup_verification"] = "creation_unresolved"
            journal.write(r)
            return r
        selected = r.get("selected")
        if (
            not isinstance(selected, dict)
            or selected.get("gpu_id") not in task.gpu_allowlist
            or not isinstance(selected.get("data_center"), str)
            or not selected["data_center"]
        ):
            raise ValueError("trusted_allocation_required")
        markers = owner(task, r["actions_run_id"])
        expected_mounts = (
            {}
            if task.volume.kind == "none"
            else {task.volume.kind: [{"volumeId": task.volume.id, "path": task.volume.mount_path}]}
        )
        deadline, absent = now() + task.limits.cleanup_seconds, 0
        journal.write(r)
        for attempt in range(3):
            if now() >= deadline:
                break
            try:
                pod = api.get(pod_id, deadline - now())
                if pod is None:
                    ids = api.active_ids(deadline)
                    absent = absent + 1 if pod_id not in ids else 0
                    if absent >= 2:
                        r["facts"]["pod_deleted_confirmed"] = True
                        r["state"] = "ACCEPTING"
                        r["cleanup_verification"] = "direct_404_and_complete_inventory_twice"
                        break
                else:
                    absent = 0
                    if (
                        pod.get("id") != pod_id
                        or pod.get("name") != name
                        or pod.get("image") != task.image
                        or pod.get("mounts") != expected_mounts
                        or not isinstance(pod.get("gpu"), dict)
                        or pod["gpu"].get("id") != selected["gpu_id"]
                        or pod["gpu"].get("count") != task.gpu_count
                        or pod.get("cloud") != task.cloud
                        or pod.get("dataCenterId") != selected["data_center"]
                        or not isinstance(pod.get("env"), dict)
                        or any(pod["env"].get(k) != v for k, v in markers.items())
                    ):
                        raise ProviderError("ownership_ambiguous")
                    if not allow_termination:
                        r["cleanup_verification"] = "owned_termination_not_enabled"
                        break
                    r["cleanup_verification"] = "termination_requested_unconfirmed"
                    journal.write(r)  # verify durable ownership before destructive request
                    api.delete(pod_id, deadline - now())
            except ProviderError as exc:
                r.setdefault("errors", []).append(exc.code)
                if not exc.retryable:
                    break
            journal.write(r)
            if attempt < 2 and now() < deadline:
                sleep(min(1, deadline - now()))
        journal.write(r)
        return r
