"""Small allowlisted observations, never proof of an HTTP response's origin.

Do not copy remote strings, bodies, addresses, proxy URLs or Pod environments.
These helpers do no I/O and must not delay cleanup with diagnostic requests.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from email.message import Message
from typing import Any

MAX_STARTUP_SAMPLES = 16
POD_STATES = {"PROVISIONING", "STARTING", "RUNNING", "EXITED", "ERROR", "TERMINATED"}


def header_hints(headers: Mapping[str, str] | Message | None) -> dict[str, str | bool]:
    headers = headers if headers is not None else {}

    # Only look up these named fields; do not retain the raw header map.
    def value(name: str) -> str:
        raw = headers.get(name, "")
        return raw.strip().lower() if isinstance(raw, str) and len(raw) <= 256 else ""

    server = value("Server")
    media = value("Content-Type").split(";", 1)[0].strip()
    return {
        "server_family": server
        if server in {"cloudflare", "nginx", "envoy"}
        else "other_or_missing",
        "media_type": media
        if media
        in {
            "text/html",
            "text/plain",
            "application/json",
            "application/problem+json",
            "application/octet-stream",
        }
        else "other_or_missing",
        "cf_ray_present": "CF-Ray" in headers,
        "cf_challenge_hint": value("cf-mitigated") == "challenge",
        "via_present": "Via" in headers,
        "location_present": "Location" in headers,
        "www_authenticate_present": "WWW-Authenticate" in headers,
    }


def create_reason(raw: bytes) -> dict[str, Any]:
    """Only fixed categories escape; never persist a provider message or echoed env."""
    if len(raw) > 16_384:
        return {"reason_class": "unclassified", "reason_hints": ["body_too_large"]}
    try:
        body = json.loads(raw)
        detail = body.get("detail", "") if isinstance(body, dict) else ""
    except (ValueError, UnicodeError):
        detail = ""
    if not isinstance(detail, str):
        detail = ""
    detail = " ".join(detail.lower().split())
    capacity = detail == (
        "there are no longer any instances available with the requested specifications. "
        "please refresh and try again."
    )
    # Hints aid diagnosis only; substring matches NEVER authorize a retry.
    hints = sorted(
        {
            label
            for word, label in (
                ("disk", "disk"),
                ("image", "image"),
                ("cuda", "cuda"),
                ("entrypoint", "entrypoint"),
                ("command", "command"),
                ("mount", "mount"),
                ("port", "port"),
                ("balance", "balance"),
                ("minimum", "minimum"),
                ("maximum", "maximum"),
                ("invalid", "invalid"),
                ("mutually exclusive", "conflicting_fields"),
            )
            if word in detail
        }
    )
    return {"reason_class": "capacity" if capacity else "unclassified", "reason_hints": hints}


def proxy_environment(environ: Mapping[str, str]) -> dict[str, bool]:
    # Presence only. urllib's effective route depends on OS/bypass settings too.
    # Do not disable proxies or alter NO_PROXY while diagnosing an HTTP denial.
    return {
        name.lower() + "_configured": bool(environ.get(name) or environ.get(name.lower()))
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY")
    }


def record_startup(record: dict[str, Any], pod: dict[str, Any], now: float) -> None:
    """Project an already owner-checked GET; no extra API call or readiness claim."""
    status = pod.get("status")
    ports = pod.get("ports")
    runtime = pod.get("runtime")
    sample = {
        "seconds_since_intent": round(max(0.0, now - record["campaign"]["first_intent_at"]), 3),
        "pod_status": status if isinstance(status, str) and status in POD_STATES else "unknown",
        "result_port_configured": ("8000/http" in ports)
        if isinstance(ports, list) and all(isinstance(p, str) for p in ports)
        else None,
        "runtime_metadata": "present" if isinstance(runtime, dict) else "null_or_missing",
        "worker_readiness": "unverified",
    }
    diagnostic = record.setdefault("startup_diagnostics", {"observations": 0, "samples": []})
    diagnostic["observations"] += 1
    diagnostic["samples"].append(sample)
    # Keep the first observation plus the latest 15 for a bounded audit trail.
    if len(diagnostic["samples"]) > MAX_STARTUP_SAMPLES:
        del diagnostic["samples"][1]
