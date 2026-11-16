"""Narrow live adapter: one create request, no retries or automatic idempotency claim."""

from __future__ import annotations

import json
import re
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, build_opener

from .cleanup import CleanupAPI
from .provider import NoRedirect, ProviderError, bounded_body, http_error
from .startup import summarize_logs
from .transport_diagnostics import create_reason, header_hints
from .wire import Pod


class CreateHTTPError(ProviderError):
    """Preserve fixed HTTP observations without retaining the request or error body."""

    def __init__(self, error: HTTPError):
        definitive = error.code in {400, 401, 402, 403, 404, 422}
        super().__init__(
            http_error(error.code).code if definitive else "create_response_unknown",
            ambiguous=not definitive,
        )
        self.diagnostic = {
            "stage": "create",
            "http_status": error.code,
            "code": self.code,
            "ambiguous": self.ambiguous,
            "header_hints": header_hints(error.headers),
            "response_origin": "unverified",
        }
        if error.code in {400, 422}:
            try:
                raw = bounded_body(error, 16_384, time.monotonic() + 1)
                self.diagnostic.update(create_reason(raw))
            except (OSError, ValueError):
                self.diagnostic.update(
                    reason_class="unclassified", reason_hints=["body_unavailable"]
                )


class LiveAPI(CleanupAPI):
    def __init__(self, *, allow_create: bool = False, allow_termination: bool = False):
        super().__init__(allow_termination=allow_termination)
        self.allow_create = allow_create

    def startup_logs(self, pod_id: str, timeout: float) -> dict[str, Any]:
        """One read-only SSE snapshot, <=64 KiB and <=3s plus a <=1s socket wait.

        The caller must verify ownership first. Never reconnect, emit raw log text,
        follow redirects or send the management key to a workload proxy.
        """
        if not re.fullmatch(r"[a-z0-9]{1,64}", pod_id) or not 0 < timeout <= 3:
            raise ValueError("startup_diagnostic_bounds")
        req = Request(
            f"https://api.runpod.io/v2/pods/{pod_id}/logs?tail=100",
            headers={
                "Authorization": "Bearer " + str(self._key),
                "Accept": "text/event-stream",
                "User-Agent": "runpod-ci-broker/0.1",
            },
        )
        deadline = time.monotonic() + timeout
        raw = bytearray()
        stop = "eof"
        status: int | None = None
        try:
            with build_opener(NoRedirect).open(req, timeout=min(1, timeout)) as response:
                if response.status != 200:
                    return {"status": "unexpected_status", "root_cause": "unverified"}
                read = getattr(response, "read1", response.read)
                while time.monotonic() < deadline and len(raw) < 65_536:
                    block = read(min(4096, 65_536 - len(raw)))
                    if not block:
                        break
                    raw.extend(block)
                else:
                    stop = "byte_limit" if len(raw) >= 65_536 else "deadline"
        except HTTPError as exc:
            status, stop = exc.code, "http_error"
            exc.close()
        except (TimeoutError, URLError, OSError):
            stop = "timeout_or_transport"
        return {"status": stop, "http_status": status, **summarize_logs(bytes(raw))}

    def inventory(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        ids: set[str] = set()
        cursors: set[str] = set()
        params: dict[str, str | int] = {"limit": 100}
        for _ in range(5):
            value = self.request("/pods?" + urlencode(params), 5)
            try:
                if value is None or not isinstance(value["pods"], list):
                    raise ValueError()
                for raw in value["pods"]:
                    p = Pod.model_validate(raw)
                    if p.id in ids:
                        raise ValueError()
                    ids.add(p.id)
                    rows.append(raw)  # private memory only; includes capability, never serialize
                pagination = value["pagination"]
                if type(pagination["hasNextPage"]) is not bool:
                    raise ValueError()
                if not pagination["hasNextPage"]:
                    return rows
                cursor = pagination["nextCursor"]
                if not isinstance(cursor, str) or not cursor or cursor in cursors:
                    raise ValueError()
                cursors.add(cursor)
                params["cursor"] = cursor
            except (KeyError, TypeError, ValueError):
                raise ProviderError("inventory_unknown") from None
        raise ProviderError("inventory_incomplete")

    def create(self, body: dict[str, Any]) -> str:
        if not self.allow_create:
            raise ProviderError("paid_create_not_enabled")
        req = Request(
            "https://api.runpod.io/v2/pods",
            method="POST",
            data=json.dumps(body, allow_nan=False).encode(),
            headers={
                "Authorization": "Bearer " + str(self._key),
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "runpod-ci-broker/0.1",
            },
        )
        try:
            deadline = time.monotonic() + 10
            with build_opener(NoRedirect).open(req, timeout=5) as response:
                if response.status != 201:
                    raise ValueError()
                raw = bounded_body(response, 1_000_000, deadline)
            pod_id = json.loads(raw)["id"]
            if not isinstance(pod_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", pod_id):
                raise ValueError()
            return pod_id
        except HTTPError as exc:
            # 5xx/timeout/429 may be a lost acknowledgement. Never POST again.
            try:
                failure = CreateHTTPError(exc)
            finally:
                exc.close()  # Only fixed reason categories leave the bounded error reader.
            raise failure from None
        except (URLError, OSError, ValueError, KeyError, TypeError):
            raise ProviderError("create_response_unknown", ambiguous=True) from None
