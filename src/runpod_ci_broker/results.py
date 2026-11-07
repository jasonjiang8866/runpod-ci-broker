"""Verify an allowlisted result bundle before recording the export as saved."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from .models import Task
from .storage import atomic_json


def save_bundle(
    task: Task, pod_id: str, manifest: dict[str, Any], files: dict[str, bytes], destination: Path
) -> bool:
    passed = validate_manifest(task, pod_id, manifest)
    if set(files) != set(task.results.required_files):
        raise ValueError("result_file_set_mismatch")
    for name, payload in files.items():
        if manifest["files"][name] != {
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }:
            raise ValueError("result_checksum_mismatch")
    if "metrics.json" in files and json.loads(files["metrics.json"]) != manifest["metrics"]:
        raise ValueError("result_metrics_mismatch")
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


def validate_manifest(task: Task, pod_id: str, manifest: dict[str, Any]) -> bool:
    if not isinstance(manifest, dict) or set(manifest) != {
        "config_fingerprint",
        "source_sha",
        "image",
        "pod_id",
        "metrics",
        "files",
    }:
        raise ValueError("result_manifest_fields")
    if (
        manifest["config_fingerprint"] != task.fingerprint()
        or manifest["source_sha"] != task.source_sha
        or manifest["image"] != task.image
        or manifest["pod_id"] != pod_id
        or not isinstance(pod_id, str)
        or not pod_id
    ):
        raise ValueError("result_provenance_mismatch")
    entries = manifest["files"]
    if not isinstance(entries, dict) or set(entries) != set(task.results.required_files):
        raise ValueError("result_file_set_mismatch")
    for entry in entries.values():
        if (
            not isinstance(entry, dict)
            or set(entry) != {"bytes", "sha256"}
            or type(entry["bytes"]) is not int
            or entry["bytes"] < 0
            or not isinstance(entry["sha256"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", entry["sha256"])
        ):
            raise ValueError("result_digest_invalid")
    if sum(x["bytes"] for x in entries.values()) > task.results.max_bytes:
        raise ValueError("result_size_exceeded")
    metrics = manifest["metrics"]
    if not isinstance(metrics, dict) or any(
        not isinstance(k, str)
        or isinstance(v, bool)
        or not isinstance(v, (float, int))
        or not math.isfinite(v)
        for k, v in metrics.items()
    ):
        raise ValueError("result_metrics_invalid")
    return all(
        k in metrics and metrics[k] <= v for k, v in task.results.metric_max.items()
    ) and all(k in metrics and metrics[k] >= v for k, v in task.results.metric_min.items())


def verify_saved(destination: Path, task: Task, pod_id: str | None = None) -> bool:
    try:
        manifest_path = destination / "manifest.json"
        if any(p.is_symlink() for p in [manifest_path, *manifest_path.parents]):
            return False
        if manifest_path.stat().st_size > 65_536:
            return False
        manifest = json.loads(manifest_path.read_text())
        validate_manifest(task, pod_id if pod_id is not None else manifest["pod_id"], manifest)
        for name in task.results.required_files:
            path = destination / name
            if any(p.is_symlink() for p in [path, *path.parents]) or not path.is_file():
                return False
            if path.stat().st_size != manifest["files"][name]["bytes"]:
                return False
            with path.open("rb") as f:
                if (
                    hashlib.file_digest(f, "sha256").hexdigest()
                    != manifest["files"][name]["sha256"]
                ):
                    return False
        if "metrics.json" in task.results.required_files:
            metrics_path = destination / "metrics.json"
            if metrics_path.stat().st_size > 65_536:
                return False
            if json.loads(metrics_path.read_text()) != manifest["metrics"]:
                return False
        return True
    except (OSError, KeyError, ValueError, TypeError):
        return False
