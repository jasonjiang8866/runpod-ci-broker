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
from .wire import Pod


class LiveAPI(CleanupAPI):
    def __init__(self, *, allow_create: bool = False, allow_termination: bool = False):
        super().__init__(allow_termination=allow_termination)
        self.allow_create = allow_create

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
            if exc.code in {400, 401, 402, 403, 404, 422}:
                raise http_error(exc.code) from None
            raise ProviderError("create_response_unknown", ambiguous=True) from None
        except (URLError, OSError, ValueError, KeyError, TypeError):
            raise ProviderError("create_response_unknown", ambiguous=True) from None
