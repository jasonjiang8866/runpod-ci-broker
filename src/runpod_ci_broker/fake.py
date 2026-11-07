"""Persistent deterministic fake provider. Never performs network calls or executes argv."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from .models import Candidate, Task
from .provider import ProviderError
from .storage import atomic_json


class FakeProvider:
    mode = "mock"

    def __init__(
        self,
        candidates: list[Candidate],
        path: Path | None = None,
        faults: dict[str, int] | None = None,
    ):
        self.path = path
        self.candidates = candidates
        self.data: dict[str, Any] = (
            json.loads(path.read_text())
            if path and path.exists()
            else {
                "pods": {},
                "faults": faults or {},
                "create_calls": 0,
                "deletions": [],
                "clock": 0.0,
            }
        )
        self.persist()

    def persist(self) -> None:
        if self.path:
            atomic_json(self.path, self.data)

    def now(self) -> float:
        return float(self.data["clock"])

    def sleep(self, seconds: float) -> None:
        self.data["clock"] += seconds
        self.persist()

    def fault(self, name: str) -> bool:
        if self.data["faults"].get(name, 0) > 0:
            self.data["faults"][name] -= 1
            self.persist()
            return True
        return False

    def snapshot(self, task: Task) -> list[Candidate]:
        if self.fault("query_error"):
            raise ProviderError("query_failed", retryable=True)
        if self.fault("capacity_none"):
            return [c.model_copy(update={"capacity": "none"}) for c in self.candidates]
        return self.candidates

    def list_pods(self) -> list[dict[str, Any]]:
        if self.fault("inventory_error"):
            raise ProviderError("inventory_incomplete", retryable=True)
        if self.fault("inventory_hidden"):
            return []  # model eventual consistency
        return copy.deepcopy(list(self.data["pods"].values()))

    def create(self, payload: dict[str, Any], candidate: Candidate) -> str:
        self.data["create_calls"] += 1
        self.persist()
        if self.fault("capacity_race"):
            raise ProviderError("no_capacity", retryable=True)
        if self.fault("auth_error"):
            raise ProviderError("authentication_failed")
        if self.fault("permission_error"):
            raise ProviderError("permission_denied")
        if self.fault("account_error"):
            raise ProviderError("account_restricted")
        pod_id = "fake-" + str(self.data["create_calls"])
        self.data["pods"][pod_id] = {
            **copy.deepcopy(payload),
            "id": pod_id,
            "status": "PROVISIONING",
            "candidate": candidate.model_dump(),
            "polls": 0,
        }
        self.persist()
        if self.fault("create_lost"):
            raise ProviderError("create_outcome_unknown", ambiguous=True)
        return pod_id

    def observe(self, pod_id: str) -> dict[str, Any]:
        if self.fault("poll_error"):
            raise ProviderError("query_failed", retryable=True)
        pod = self.data["pods"].get(pod_id)
        if pod is None:
            raise ProviderError("pod_missing")
        if self.fault("image_pull_error"):
            return {"state": "failed", "error": "image_pull_failed", "exit_code": None}
        if self.fault("execution_error"):
            return {"state": "finished", "exit_code": 2, "environment": pod["candidate"]}
        if self.fault("hang"):
            return {"state": "starting"}
        pod["polls"] += 1
        pod["status"] = "RUNNING"
        self.persist()
        return {
            "state": "running" if pod["polls"] == 1 else "finished",
            "exit_code": None if pod["polls"] == 1 else 0,
            "environment": pod["candidate"],
        }

    def collect(self, pod_id: str, task: Task) -> tuple[dict[str, Any], dict[str, bytes]]:
        if self.fault("collect_error"):
            raise ProviderError("collection_unavailable", retryable=True)
        metrics = {"max_error": 0.000001, "macro_f1": 0.88, "throughput": 100.0}
        files = {
            "metrics.json": json.dumps(metrics).encode(),
            "worker.log": b"MOCK GPU execution\n",
        }
        for name in task.results.required_files:
            files.setdefault(name, b"MOCK fixture artifact\n")
        manifest = {
            "config_fingerprint": task.fingerprint(),
            "source_sha": task.source_sha,
            "image": task.image,
            "pod_id": pod_id,
            "metrics": metrics,
            "files": {
                k: {"bytes": len(v), "sha256": hashlib.sha256(v).hexdigest()}
                for k, v in files.items()
            },
        }
        if self.fault("corrupt_result"):
            files["worker.log"] += b"corruption"
        return manifest, files

    def delete(self, pod_id: str) -> None:
        if self.fault("delete_error"):
            raise ProviderError("provider_unavailable", retryable=True)
        if self.fault("delete_noop"):
            return  # a sent request is not a confirmed deletion
        self.data["pods"].pop(pod_id, None)
        self.data["deletions"].append(pod_id)
        self.persist()
