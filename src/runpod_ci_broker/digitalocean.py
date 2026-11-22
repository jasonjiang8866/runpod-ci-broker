"""DigitalOcean v2 GPU catalog and individually addressed Droplets."""

from __future__ import annotations

import json
import os
import re
import time
from decimal import Decimal
from typing import Any, Literal
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener

from .provider import NoRedirect, ProviderError, bounded_body, http_error


class CreateRejected(ProviderError):
    """A definitive API rejection. Only explicit capacity refusals allow another placement."""


class DigitalOcean:
    def __init__(self, *, mode: Literal["read_only", "allocate", "cleanup"] = "read_only"):
        self.token = os.environ.get("DIGITALOCEAN_ACCESS_TOKEN")
        if not self.token:
            raise ProviderError("credentials_missing")
        self.mode = mode

    def request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        individual = re.fullmatch(r"/droplets/[1-9][0-9]*", path) is not None
        if not (
            (
                method == "GET"
                and (individual or path in {"/sizes", "/droplets", "/account", "/account/keys"})
            )
            or (self.mode == "allocate" and method == "POST" and path == "/droplets")
            or (self.mode == "cleanup" and method == "DELETE" and individual)
        ):
            raise ProviderError("endpoint_not_allowed")
        url = "https://api.digitalocean.com/v2" + path
        if params:
            url += "?" + urlencode(params)
        req = Request(
            url,
            method=method,
            data=json.dumps(body).encode() if body else None,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
        )
        try:
            deadline = time.monotonic() + 15
            with build_opener(NoRedirect).open(req, timeout=15) as response:
                if method == "DELETE" and response.status == 204:
                    return {}
                if response.status != (202 if method == "POST" else 200):
                    raise ProviderError("unexpected_status")
                data = json.loads(bounded_body(response, 4_000_000, deadline), parse_float=Decimal)
                if not isinstance(data, dict):
                    raise ProviderError("response_unverified")
                return data
        except HTTPError as exc:
            if method == "POST" and exc.code in {400, 401, 403, 422}:
                code = http_error(exc.code).code
                if exc.code in {400, 422}:
                    try:
                        message = json.loads(bounded_body(exc, 16_384, deadline)).get("message", "")
                    except (OSError, ValueError, AttributeError):
                        message = ""
                    if isinstance(message, str) and any(
                        phrase in message.lower()
                        for phrase in (
                            "insufficient capacity",
                            "out of capacity",
                            "insufficient resources",
                        )
                    ):
                        code = "capacity_refused"
                    elif isinstance(message, str):
                        detail = message.lower()
                        if "ssh key" in detail or "ssh_keys" in detail:
                            code = "provider_rejected_ssh_configuration"
                        elif "limit" in detail or "quota" in detail:
                            code = "provider_limit_reached"
                        elif "image" in detail:
                            code = "provider_rejected_image"
                raise CreateRejected(code) from None
            raise http_error(exc.code) from None
        except (ValueError, OSError):
            raise ProviderError("response_unverified") from None

    def listing(self, name: str, **filters: Any) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[str | int] = set()
        for page in range(1, 21):
            data = self.request("GET", "/" + name, {"page": page, "per_page": 200, **filters})
            batch = data.get(name)
            meta = data.get("meta")
            if (
                not isinstance(batch, list)
                or not isinstance(meta, dict)
                or type(meta.get("total")) is not int
            ):
                raise ProviderError("inventory_incomplete")
            for row in batch:
                if not isinstance(row, dict):
                    raise ProviderError("inventory_invalid")
                key = row.get("slug" if name == "sizes" else "id")
                if type(key) not in (str, int) or not key or key in seen:
                    raise ProviderError("inventory_duplicate_or_invalid")
                seen.add(key)
                rows.append(row)
            if page == 1:
                total = meta["total"]
            if meta["total"] != total or len(rows) > total:
                raise ProviderError("inventory_changed")
            if len(rows) == total:
                return rows
            if not batch:
                break
        raise ProviderError("inventory_incomplete")
