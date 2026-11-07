"""Fixed short CUDA memory smoke worker; generic business argv is not enabled."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import Field

from .export import capability_valid, result_server
from .models import Model, Task
from .storage import atomic_json

MIG = "NVIDIA RTX PRO 6000 Blackwell Server Edition MIG 1g.24gb"
CONSUMER = {"NVIDIA GeForce RTX " + model for model in ("3090", "4090", "5090")}


class Observation(Model):
    device_name: Annotated[str, Field(pattern=r"^[A-Za-z0-9 ._-]{1,200}$")]
    device_uuid: Annotated[str, Field(pattern=r"^[a-f0-9-]{36}$")]
    device_count: Annotated[int, Field(ge=1, le=8)]
    memory_bytes: Annotated[int, Field(ge=1)]
    driver_cuda_version: Annotated[int, Field(ge=1)]
    architecture: Annotated[str, Field(pattern=r"^sm_[0-9]+$")]
    memory_roundtrip_bytes: Literal[4096]
    memory_roundtrip_passed: bool
    probe: Literal["cuda_driver_memory_roundtrip_v1"]
    framework_runtime: Literal["not_tested"]


def admit(task: Task, selected_gpu: str, observation: dict[str, Any]) -> None:
    """Host driver CUDA support is a floor, never mislabeled as toolkit version."""
    if selected_gpu not in task.gpu_allowlist or selected_gpu not in CONSUMER | {MIG}:
        raise ValueError("gpu_not_allowed")
    if task.strategy != "flexible_smoke" or task.gpu_count != 1:
        raise ValueError("worker_supports_single_flexible_smoke_only")
    if observation.get("device_name") != selected_gpu:
        # A parent GPU name or 24GB size alone is not proof of the exact MIG slice.
        raise ValueError("runtime_gpu_identity_unverified")
    if (
        observation.get("device_count") != 1
        or type(observation.get("memory_bytes")) is not int
        or observation["memory_bytes"] < task.min_vram_gb * 1_000_000_000
        or observation.get("memory_roundtrip_passed") is not True
    ):
        raise ValueError("cuda_admission_failed")
    value = observation.get("driver_cuda_version")
    if type(value) is not int or value < 12080:
        raise ValueError("cuda_driver_incompatible")
    floor = task.compatibility.min_cuda_version
    if floor:
        major, minor = map(int, floor.split("."))
        if value < major * 1000 + minor * 10:
            raise ValueError("cuda_driver_incompatible")
    # This image contains no framework runtime. It must not satisfy a toolkit/driver pin.
    if (
        task.compatibility.cuda_versions
        or task.compatibility.exact_driver
        or task.compatibility.min_driver
    ):
        raise ValueError("exact_software_environment_not_verified")
    if (
        task.compatibility.architectures
        and observation.get("architecture") not in task.compatibility.architectures
    ):
        raise ValueError("gpu_architecture_incompatible")


def sanitized_environment() -> dict[str, str]:
    return {
        k: os.environ[k]
        for k in (
            "PATH",
            "LD_LIBRARY_PATH",
            "CUDA_VISIBLE_DEVICES",
            "NVIDIA_VISIBLE_DEVICES",
            "NVIDIA_DRIVER_CAPABILITIES",
            "LANG",
        )
        if k in os.environ
    }


def run_smoke(
    task: Task, selected_gpu: str, pod_id: str, directory: Path, build_sha: str
) -> tuple[dict[str, Any], dict[str, bytes]]:
    if task.source_sha != build_sha:
        raise ValueError("worker_source_revision_mismatch")
    if any(
        os.environ.get(k)
        for k in ("RUNPOD_API_KEY", "GITHUB_TOKEN", "GH_TOKEN", "RCB_LEDGER_TOKEN")
    ):
        raise ValueError("management_credential_present_on_worker")
    if task.entrypoint != ["broker-cuda-memory-smoke"] or task.limits.runtime_seconds is None:
        raise ValueError("fixed_smoke_entrypoint_required")
    if not re.fullmatch(r"[a-z0-9]{1,64}", pod_id):
        raise ValueError("pod_identity_required")
    if set(task.results.required_files) != {"metrics.json", "worker.log", "environment.json"}:
        raise ValueError("smoke_result_contract_required")
    if not 1 <= task.limits.runtime_seconds <= 60:
        raise ValueError("smoke_runtime_must_not_exceed_60_seconds")
    directory.mkdir(parents=True, exist_ok=False)
    observation: dict[str, Any] = {}
    exit_code, error = 2, "probe_failed"
    try:
        # Fixed child, no shell. Result capability and all account keys excluded.
        result = subprocess.run(
            [sys.executable, "-m", "runpod_ci_broker.cuda_probe"],
            env=sanitized_environment(),
            capture_output=True,
            timeout=task.limits.runtime_seconds,
            check=False,
        )
        if len(result.stdout) > 65_536 or result.returncode != 0:
            raise ValueError("probe_failed")
        observation = Observation.model_validate_json(result.stdout).model_dump()
        admit(task, selected_gpu, observation)
        exit_code, error = 0, "none"
    except subprocess.TimeoutExpired:
        error = "probe_timed_out"
    except (ValueError, TypeError, OSError):
        # Do not echo arbitrary subprocess output, exception text or environment.
        error = "probe_or_admission_failed"
    metrics = {"gpu_memory_smoke_passed": int(exit_code == 0), "worker_exit_code": exit_code}
    environment = {
        "observed": observation,
        "admission_passed": exit_code == 0,
        "failure": error,
        "source_sha": task.source_sha,
        "image": task.image,
        "selected_gpu_id": selected_gpu,
        "pod_id": pod_id,
        "framework_runtime_verified": False,
    }
    files = {
        "metrics.json": json.dumps(metrics).encode(),
        "environment.json": json.dumps(environment, allow_nan=False).encode(),
        "worker.log": (f"CUDA memory smoke exit={exit_code}; status={error}\n").encode(),
    }
    manifest = {
        "config_fingerprint": task.fingerprint(),
        "source_sha": task.source_sha,
        "image": task.image,
        "pod_id": pod_id,
        "metrics": metrics,
        "files": {
            name: {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            for name, data in files.items()
        },
    }
    for name, data in files.items():
        (directory / name).write_bytes(data)
    atomic_json(directory / "manifest.json", manifest)
    return manifest, files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", type=Path, help="or controller-supplied RCB_TASK_JSON")
    parser.add_argument("--selected-gpu", default=os.environ.get("RCB_SELECTED_GPU", ""))
    parser.add_argument("--pod-id", default=os.environ.get("RUNPOD_POD_ID", ""))
    parser.add_argument("--output", type=Path, default=Path("/workspace/results"))
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    try:
        raw_task = args.task.read_text() if args.task else os.environ.pop("RCB_TASK_JSON", "")
        if len(raw_task) > 65_536:
            raise ValueError("task_too_large")
        task = Task.model_validate_json(raw_task)
        # RunPod documents an automatically injected Pod-scoped key with this name.
        # Discard it unused; the controller must NEVER supply its account key.
        os.environ.pop("RUNPOD_API_KEY", None)
        token = os.environ.pop("RCB_RESULT_TOKEN", "")
        if not capability_valid(token) or not 1024 <= args.port <= 65535:
            raise ValueError("result_capability_or_port_invalid")
        manifest, files = run_smoke(
            task,
            args.selected_gpu,
            args.pod_id,
            args.output,
            Path("/opt/broker/SOURCE_SHA").read_text().strip(),
        )
        server = result_server(task, args.pod_id, manifest, files, token, ("0.0.0.0", args.port))
        server.timeout = 1
        deadline = time.monotonic() + task.limits.collect_seconds
        with server:
            while time.monotonic() < deadline:
                server.handle_request()
        # Container exit is NOT a Pod deletion receipt. External controller must terminate.
        return int(manifest["metrics"]["worker_exit_code"])
    except (ValueError, TypeError, OSError):
        print('{"error":"worker_refused","gpu_verified":false}')
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
