"""RunPod REST v2 GET-only adapter. There is no live create/stop/delete method."""

from __future__ import annotations

import json
import os
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from pydantic import ValidationError

from .models import Candidate, Task
from .wire import GPU, DataCenter, GlobalVolume, NetworkVolume, Pod

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
            ids = [item.get("id") for item in items]
            if any(not isinstance(i, str) or not i for i in ids):
                raise ProviderError("inventory_invalid")
            if len(set(ids)) != len(ids) or set(ids) & {row["id"] for row in rows}:
                raise ProviderError("inventory_duplicate_id")
            rows.extend(items)
            pagination = response.get("pagination", {})
            if not isinstance(pagination, dict) or type(pagination.get("hasNextPage")) is not bool:
                raise ProviderError("inventory_incomplete")
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
            "observed_at": datetime.now(UTC).isoformat(),
            "catalog_query": query,
            "capacity_is_reservation": False,
            "image_pull_verified": False,
            "registry_status": "unverified" if task.registry_credential_id else "not_requested",
        }
        volume_ok: bool | None = None
        try:
            pods = [Pod.model_validate(p) for p in self.listing("/pods", "pods")]
            # Even EXITED Pods can retain billable storage; include them in scope blocking.
            inventory["active_pod_count"] = sum(p.status != "TERMINATED" for p in pods)
            inventory["scope_active_pod_count"] = sum(
                p.status != "TERMINATED"
                and p.env.get("RCB_TOOL") == "runpod-ci-broker/v1"
                and p.env.get("RCB_SCOPE") == task.scope
                for p in pods
            )
            inventory["inventory"] = "complete"
        except ProviderError as exc:
            inventory["inventory"] = exc.code
        except ValidationError:
            inventory["inventory"] = "inventory_schema_invalid"
        if task.volume.kind != "none":
            try:
                kind = task.volume.kind
                rows = self.listing("/" + kind + "-volumes", kind + "Volumes")
                if kind == "network":
                    network = [NetworkVolume.model_validate(v) for v in rows]
                    matches = [v for v in network if v.id == task.volume.id]
                    volume_ok = (
                        len(matches) == 1 and matches[0].dataCenter == task.volume.data_center
                    )
                else:
                    global_volumes = [GlobalVolume.model_validate(v) for v in rows]
                    volume_ok = sum(v.id == task.volume.id for v in global_volumes) == 1
                inventory["volume_status"] = "compatible" if volume_ok else "incompatible"
            except ProviderError as exc:
                inventory["volume_status"] = exc.code
            except ValidationError:
                inventory["volume_status"] = "volume_schema_invalid"
        try:
            dcs = self.get("/catalog/datacenters", {}).get("dataCenters")
            if not isinstance(dcs, list):
                raise ProviderError("datacenter_invalid")
            validated_dcs = [DataCenter.model_validate(d) for d in dcs]
            inventory["data_centers"] = [d.model_dump() for d in validated_dcs]
            inventory["datacenter_status"] = "ok"
        except ProviderError as exc:
            inventory["datacenter_status"] = exc.code
        except ValidationError:
            inventory["datacenter_status"] = "datacenter_schema_invalid"
        gpu_rows = catalog.get("gpus")
        if not isinstance(gpu_rows, list):
            inventory["query_status"] = "catalog_schema_invalid"
            return self.unknown(task, "catalog_schema_invalid"), inventory
        candidates: list[Candidate] = []
        catalog_seen: set[str] = set()
        inventory["catalog_models"] = []
        for raw in gpu_rows:
            if not isinstance(raw, dict):
                inventory["query_status"] = "catalog_schema_invalid"
                continue
            if raw.get("id") not in task.gpu_allowlist:
                continue
            try:
                gpu = GPU.model_validate(raw)
                if gpu.id in catalog_seen:
                    inventory["query_status"] = "catalog_schema_invalid"
                    continue
                catalog_seen.add(gpu.id)
            except ValidationError:
                inventory["query_status"] = "catalog_schema_invalid"
                candidates.append(
                    Candidate(
                        gpu_id=raw["id"], error="catalog_schema_invalid", evidence="read_only"
                    )
                )
                continue
            inventory["catalog_models"].append(
                {
                    "id": gpu.id,
                    "memory_gb": gpu.memory,
                    "per_gpu_hourly_usd": gpu.price.get(task.cloud.lower()),
                    "machine_gpu_count_ceiling": gpu.maxCount.get(task.cloud.lower()),
                    "overall_capacity_signal": gpu.availability,
                }
            )
            versions = [c.version for c in gpu.cudaVersions if c.available]
            # Overall availability never proves capacity in a particular data center.
            locations: list[tuple[str | None, str | None]] = [
                (dc.id, dc.availability) for dc in gpu.dataCenters
            ]
            if not locations:
                locations = [(None, "NONE" if gpu.availability == "NONE" else None)]
            for dc_id, availability in locations:
                cap: Literal["available", "none", "unknown"] = (
                    "available"
                    if availability in {"LOW", "MEDIUM", "HIGH"} and dc_id
                    else "none"
                    if availability == "NONE"
                    else "unknown"
                )
                candidates.append(
                    Candidate(
                        gpu_id=gpu.id,
                        data_center=dc_id,
                        memory_gb=gpu.memory,
                        gpu_count=task.gpu_count,
                        hourly_usd=gpu.price.get(task.cloud.lower()),
                        capacity=cap,
                        cloud=task.cloud,
                        cuda_versions=versions,
                        volume_compatible=volume_ok,
                        evidence="read_only",
                    )
                )
        if inventory["query_status"] != "ok":
            # Preserve the observations but do not admit a partial/malformed catalog.
            for c in candidates:
                c.error = "catalog_schema_invalid"
        return candidates, inventory

    @staticmethod
    def unknown(task: Task, code: str) -> list[Candidate]:
        return [Candidate(gpu_id=g, error=code, evidence="read_only") for g in task.gpu_allowlist]
