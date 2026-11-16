"""Lossy, bounded startup diagnostics. Never persist raw provider logs or environments."""

from __future__ import annotations

import json
from typing import Any


def summarize_logs(raw: bytes) -> dict[str, Any]:
    """Interpret complete SSE data lines as hints, not authoritative failure attribution."""
    signals: set[str] = set()
    counts = {"container": 0, "system": 0}
    for line in raw.split(b"\n")[:-1]:  # discard an incomplete final event
        if not line.startswith(b"data:"):
            continue
        try:
            event = json.loads(line[5:])
        except (ValueError, UnicodeError):
            continue
        if not isinstance(event, dict):
            continue
        source, message = event.get("source"), event.get("line")
        if not isinstance(source, str) or source not in counts or not isinstance(message, str):
            continue
        counts[source] += 1
        lowered = message.lower()
        if '"error":"worker_refused"' in lowered.replace(" ", ""):
            signals.add("worker_refusal_hint")
        if "traceback (most recent call last)" in lowered:
            signals.add("python_traceback_hint")
        if any(
            x in lowered
            for x in (
                "pull access denied",
                "manifest unknown",
                "unauthorized: authentication required",
            )
        ):
            signals.add("image_pull_failure_hint")
        if any(
            x in lowered
            for x in ("cuda_error", "libcuda.so", "nvidia-container-cli: initialization error")
        ):
            signals.add("cuda_initialization_hint")
        if "address already in use" in lowered:
            signals.add("port_bind_failure_hint")
    return {"observed_events": counts, "signals": sorted(signals), "root_cause": "unverified"}
