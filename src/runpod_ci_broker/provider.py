"""RunPod REST v2 GET-only adapter. There is no live create/stop/delete method."""

from __future__ import annotations

import json
import os
import random
import time
from collections.abc import Callable
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .models import Candidate, Task

API = "https://api.runpod.io/v2"
OWNER_KEYS = ("RCB_TOOL", "RCB_SCOPE", "RCB_EXPERIMENT", "RCB_CONFIG", "RCB_RUN", "RCB_SOURCE")


class ProviderError(Exception):
    def __init__(self, code: str, *, retryable: bool = False, ambiguous: bool = False):
        super().__init__(code)
        self.code, self.retryable, self.ambiguous = code, retryable, ambiguous


def http_error(status: int) -> ProviderError:
    code = {
        401: "authentication_failed",
        402: "account_restricted",
        403: "permission_denied",
        404: "not_found",
        429: "rate_limited",
    }.get(status)
    return ProviderError(
        code or ("provider_unavailable" if status >= 500 else "invalid_request"),
        retryable=status == 429 or status >= 500,
    )


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


class Transport:
    def __init__(self) -> None:
        self._key = os.environ.get("RUNPOD_API_KEY")
        if not self._key:
            raise ProviderError("credentials_missing")

    def get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        # No arbitrary host or method can be supplied by task config.
        if path not in {
            "/catalog/gpus",
            "/catalog/datacenters",
            "/pods",
            "/network-volumes",
            "/global-volumes",
        }:
            raise ProviderError("endpoint_not_allowed")
        url = API + path + ("?" + urlencode(params) if params else "")
        req = Request(
            url,
            method="GET",
            headers={
                "Authorization": "Bearer " + str(self._key),
                "User-Agent": "runpod-ci-broker/0.1",
                "Accept": "application/json",
            },
        )
        try:
            with build_opener(NoRedirect).open(req, timeout=15) as response:
                raw = response.read(8_000_001)
            if len(raw) > 8_000_000:
                raise ProviderError("response_too_large")
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ProviderError("invalid_response")
            return value
        except HTTPError as exc:
            # Do not serialize body, URL, request, headers or provider exception text.
            raise http_error(exc.code) from None
        except (URLError, TimeoutError, OSError):
            raise ProviderError("query_failed", retryable=True) from None
        except (ValueError, UnicodeError):
            raise ProviderError("invalid_response") from None


def delay(attempt: int, jitter: float) -> float:
    return float(min(2 ** max(0, attempt - 1), 16) + max(0.0, min(jitter, 1.0)))


class ReadOnlyRunPod:
    mode = "read_only"

    def __init__(
        self,
        get: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], float] = time.monotonic,
    ):
        self.get = get if get is not None else Transport().get
        self.sleep, self.now = sleep, now

    def listing(self, path: str, key: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        params: dict[str, Any] = {"limit": 1000}
        seen: set[str] = set()
        for _ in range(50):
            response = self.get(path, params)
            items = response.get(key)
            if not isinstance(items, list) or any(not isinstance(x, dict) for x in items):
                raise ProviderError("inventory_invalid")
            rows.extend(items)
            pagination = response.get("pagination", {})
            if pagination.get("hasNextPage") is False:
                return rows
            cursor = pagination.get("nextCursor")
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                raise ProviderError("inventory_incomplete")
            seen.add(cursor)
            params = {**params, "cursor": cursor}
        raise ProviderError("inventory_page_limit")

    def snapshot(self, task: Task) -> tuple[list[Candidate], dict[str, Any]]:
        query: dict[str, Any] = {
            "include": "AVAILABILITY",
            "product": "POD",
            "count": task.gpu_count,
            "cloud": task.cloud,
        }
        if task.compatibility.cuda_versions:
            query["cudaVersions"] = ",".join(task.compatibility.cuda_versions)
        started = self.now()
        catalog: dict[str, Any] = {}
        attempts = 0
        while attempts < task.limits.max_queries:
            attempts += 1
            try:
                catalog = self.get("/catalog/gpus", query)
                break
            except ProviderError as exc:
                wait = delay(attempts, random.random())
                if (
                    not exc.retryable
                    or attempts >= task.limits.max_queries
                    or self.now() - started + wait > task.limits.max_wait_seconds
                ):
                    return (
                        [
                            Candidate(gpu_id=g, error=exc.code, evidence="read_only")
                            for g in task.gpu_allowlist
                        ],
                        {
                            "query_status": exc.code,
                            "query_attempts": attempts,
                            "inventory": "unverified",
                        },
                    )
                self.sleep(wait)
        inventory: dict[str, Any] = {
            "query_status": "ok",
            "query_attempts": attempts,
            "image_pull_verified": False,
            "registry_status": "unverified" if task.registry_credential_id else "not_requested",
        }
        volume_ok: bool | None = None
        try:
            pods = self.listing("/pods", "pods")
            # Account env never leaves this boundary. Only an explicit ownership subset is inspected.
            inventory["active_pod_count"] = sum(p.get("status") != "TERMINATED" for p in pods)
            inventory["scope_active_pod_count"] = sum(
                p.get("status") != "TERMINATED"
                and p.get("env", {}).get("RCB_TOOL") == "runpod-ci-broker/v1"
                and p.get("env", {}).get("RCB_SCOPE") == task.scope
                for p in pods
            )
            inventory["inventory"] = "complete"
        except ProviderError as exc:
            inventory["inventory"] = exc.code
        if task.volume.kind != "none":
            try:
                kind = task.volume.kind
                rows = self.listing("/" + kind + "-volumes", kind + "Volumes")
                matches = [v for v in rows if v.get("id") == task.volume.id]
                volume_ok = len(matches) == 1
                if volume_ok and kind == "network":
                    volume_ok = matches[0].get("dataCenter") == task.volume.data_center
                inventory["volume_status"] = "compatible" if volume_ok else "incompatible"
            except ProviderError as exc:
                inventory["volume_status"] = exc.code
        try:
            dcs = self.get("/catalog/datacenters", {}).get("dataCenters")
            if not isinstance(dcs, list):
                raise ProviderError("datacenter_invalid")
            inventory["data_centers"] = [
                {"id": d["id"], "region": d.get("region")}
                for d in dcs
                if isinstance(d, dict) and "id" in d
            ]
        except ProviderError as exc:
            inventory["datacenter_status"] = exc.code
        gpu_rows = catalog.get("gpus")
        if not isinstance(gpu_rows, list):
            raise ProviderError("catalog_invalid")
        candidates: list[Candidate] = []
        for gpu in gpu_rows:
            if not isinstance(gpu, dict) or gpu.get("id") not in task.gpu_allowlist:
                continue
            regions = gpu.get("dataCenters") or [{}]
            for dc in regions:
                availability = dc.get("availability", gpu.get("availability"))
                cap: Literal["available", "none", "unknown"] = (
                    "available"
                    if availability in {"LOW", "MEDIUM", "HIGH"} and dc.get("id")
                    else "none"
                    if availability == "NONE"
                    else "unknown"
                )
                versions = [
                    c["version"] for c in gpu.get("cudaVersions", []) if c.get("available") is True
                ]
                candidates.append(
                    Candidate(
                        gpu_id=gpu["id"],
                        data_center=dc.get("id"),
                        memory_gb=gpu.get("memory"),
                        gpu_count=task.gpu_count,
                        hourly_usd=gpu.get("price", {}).get(task.cloud.lower()),
                        capacity=cap,
                        cloud=task.cloud,
                        cuda_versions=versions,
                        volume_compatible=volume_ok,
                        evidence="read_only",
                    )
                )
        # Driver and compute capability deliberately remain unknown; no invented API fields.
        return candidates, inventory
