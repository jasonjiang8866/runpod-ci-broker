"""Bounded GET-only comparisons, never inputs to task admission or execution."""

from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from .models import Task
from .provider import ProviderError, ReadOnlyRunPod
from .wire import GPU


def projection(task: Task, response: dict[str, Any]) -> list[dict[str, Any]]:
    rows = response.get("gpus")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ProviderError("catalog_schema_invalid")
    seen: set[str] = set()
    models = []
    for raw in rows:
        if raw.get("id") not in task.gpu_allowlist:
            continue
        try:
            gpu = GPU.model_validate(raw)
        except ValidationError:
            raise ProviderError("catalog_schema_invalid") from None
        if gpu.id in seen:
            raise ProviderError("catalog_schema_invalid")
        seen.add(gpu.id)
        models.append(
            {
                "id": gpu.id,
                "memory_gb": gpu.memory,
                "per_gpu_hourly_usd": gpu.price.get(task.cloud.lower()),
                "overall_capacity_signal": gpu.availability,
                "reported_cuda_versions": [v.model_dump() for v in gpu.cudaVersions],
                "data_centers": [dc.model_dump() for dc in gpu.dataCenters],
            }
        )
    return models


def diagnose_cuda(
    task: Task, provider: ReadOnlyRunPod, primary: dict[str, Any], deadline: float
) -> dict[str, Any]:
    """At most two extra reads, sharing the original catalog attempt/time caps.

    A successful broader read never replaces or re-plans the original task. Reads
    are sequential snapshots, so differing signals are evidence, not causal proof.
    """
    result: dict[str, Any] = {
        "diagnostic_only": True,
        "admission_unchanged": True,
        "notice": "Sequential snapshots; inventory can change between reads. A host catalog CUDA label is not the image runtime.",
        "queries": [],
        "comparisons": [],
    }
    if not task.compatibility.cuda_versions and not task.compatibility.min_cuda_version:
        return {**result, "status": "skipped_no_cuda_filter"}
    if primary.get("query_status") != "ok":
        return {**result, "status": "skipped_primary_query_failed"}
    attempts = int(primary["query_attempts"])
    exact = task.compatibility.cuda_versions
    floor = task.compatibility.min_cuda_version or min(
        exact, key=lambda v: tuple(map(int, v.split(".")))
    )
    requested_name = "exact_cuda" if exact else "minimum_cuda"
    query = {
        "include": "AVAILABILITY",
        "product": "POD",
        "count": task.gpu_count,
        "cloud": task.cloud,
    }
    result["status"] = "complete"
    for name, params in [
        ("no_cuda_filter", query),
        (
            ("minimum_cuda", {**query, "minCudaVersion": floor})
            if exact
            else ("exact_cuda", {**query, "cudaVersions": floor})
        ),
    ]:
        if attempts >= task.limits.max_queries:
            result["status"] = "query_limit_reached"
            break
        if provider.now() >= deadline:
            result["status"] = "deadline_reached"
            break
        attempts += 1
        observation: dict[str, Any] = {
            "name": name,
            "query": params,
            "started_at": datetime.now(UTC).isoformat(),
            "status": "ok",
        }
        try:
            observation["catalog_models"] = projection(task, provider.get("/catalog/gpus", params))
        except ProviderError as exc:
            observation["status"] = exc.code
            result["status"] = "incomplete"
        observation["finished_at"] = datetime.now(UTC).isoformat()
        result["queries"].append(observation)
        if observation["status"] != "ok":
            # No extra probe after authentication/permission failure, nor blind diagnostic retries.
            break
    result["catalog_attempts_including_primary"] = attempts
    result["requested_filter"] = requested_name
    snapshots = {requested_name: primary.get("catalog_models", [])}
    snapshots.update({q["name"]: q.get("catalog_models", []) for q in result["queries"]})
    for gpu in task.gpu_allowlist:
        signals = {
            name: next((m["overall_capacity_signal"] for m in models if m["id"] == gpu), None)
            for name, models in snapshots.items()
        }
        broader_signal = any(
            signals.get(name) in {"LOW", "MEDIUM", "HIGH"}
            for name in ("no_cuda_filter", "minimum_cuda")
        )
        conclusion = (
            "cuda_filter_effect_possible"
            if signals.get("exact_cuda") == "NONE" and broader_signal
            else "no_filter_effect_established"
        )
        result["comparisons"].append({"gpu_id": gpu, "signals": signals, "inference": conclusion})
    return result
