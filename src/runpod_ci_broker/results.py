"""Verify an allowlisted result bundle before recording the export as saved."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any

from .models import Task
from .storage import atomic_json


def save_bundle(
    task: Task, pod_id: str, manifest: dict[str, Any], files: dict[str, bytes], destination: Path
) -> bool:
    if (
        manifest.get("config_fingerprint") != task.fingerprint()
        or manifest.get("source_sha") != task.source_sha
        or manifest.get("image") != task.image
        or manifest.get("pod_id") != pod_id
    ):
        raise ValueError("result_provenance_mismatch")
    if set(files) != set(task.results.required_files) or set(manifest["files"]) != set(files):
        raise ValueError("result_file_set_mismatch")
    if sum(len(x) for x in files.values()) > task.results.max_bytes:
        raise ValueError("result_size_exceeded")
    for name, payload in files.items():
        if manifest["files"][name] != {
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }:
            raise ValueError("result_checksum_mismatch")
    metrics = manifest.get("metrics", {})
    if not isinstance(metrics, dict) or any(
        isinstance(v, bool) or not isinstance(v, (float, int)) or not math.isfinite(v)
        for v in metrics.values()
    ):
        raise ValueError("result_metrics_invalid")
    # metrics.json and the acceptance manifest must agree.
    if "metrics.json" in files and json.loads(files["metrics.json"]) != metrics:
        raise ValueError("result_metrics_mismatch")
    passed = all(
        k in metrics and metrics[k] <= v for k, v in task.results.metric_max.items()
    ) and all(k in metrics and metrics[k] >= v for k, v in task.results.metric_min.items())
    destination.mkdir(parents=True, exist_ok=True)
    for name, payload in files.items():
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if any(p.is_symlink() for p in [path, *path.parents]):
            raise ValueError("result_symlink_refused")
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".result-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
    atomic_json(destination / "manifest.json", manifest)  # completion marker written last
    return passed


def verify_saved(destination: Path, task: Task) -> bool:
    try:
        manifest = json.loads((destination / "manifest.json").read_text())
        if manifest["config_fingerprint"] != task.fingerprint():
            return False
        return all(
            hashlib.sha256((destination / name).read_bytes()).hexdigest()
            == manifest["files"][name]["sha256"]
            for name in task.results.required_files
        )
    except (OSError, KeyError, ValueError):
        return False
