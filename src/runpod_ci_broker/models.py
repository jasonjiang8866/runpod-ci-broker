"""Strict secret-free contracts. Configuration is data, never shell code."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Positive = Annotated[float, Field(gt=0, allow_inf_nan=False)]
Nonnegative = Annotated[float, Field(ge=0, allow_inf_nan=False)]
Identifier = Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,100}$")]
Sha = Annotated[str, Field(pattern=r"^[a-f0-9]{40}$")]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, validate_assignment=True)


class Limits(Model):
    max_gpu_hourly_usd: Positive | None = None  # per GPU
    total_budget_usd: Positive | None = None
    runtime_seconds: Annotated[int, Field(ge=1, le=14400)] | None = None
    startup_seconds: Annotated[int, Field(ge=1, le=1800)] = 300
    collect_seconds: Annotated[int, Field(ge=1, le=900)] = 120
    cleanup_seconds: Annotated[int, Field(ge=1, le=900)] = 120
    max_start_attempts: Annotated[int, Field(ge=1, le=5)] = 2
    max_wait_seconds: Annotated[int, Field(ge=0, le=3600)] = 60
    max_queries: Annotated[int, Field(ge=1, le=30)] = 4
    max_collect_attempts: Annotated[int, Field(ge=1, le=10)] = 3
    storage_hourly_usd: Nonnegative | None = None
    transfer_reserve_usd: Nonnegative | None = None
    request_reserve_usd: Nonnegative | None = None
    cleanup_reserve_usd: Nonnegative | None = None
    prior_spend_usd: Nonnegative = 0.0
    cost_basis: str = "unverified; supply storage/transfer/request estimates explicitly"


class Compatibility(Model):
    cuda_versions: list[Annotated[str, Field(pattern=r"^\d+\.\d+$")]] = []
    min_driver: Annotated[str, Field(pattern=r"^\d+(\.\d+)+$")] | None = None
    exact_driver: Annotated[str, Field(pattern=r"^\d+(\.\d+)+$")] | None = None
    architectures: list[Annotated[str, Field(pattern=r"^sm_\d+$")]] = []


class Volume(Model):
    kind: Literal["none", "network", "global"] = "none"
    id: Identifier | None = None
    data_center: Identifier | None = None
    mount_path: str = "/workspace-data"

    @model_validator(mode="after")
    def valid(self) -> Self:
        if self.kind != "none" and not self.id:
            raise ValueError("existing volume ID required; Broker never creates storage")
        if self.kind == "network" and not self.data_center:
            raise ValueError("network volume needs an exact data center")
        if not self.mount_path.startswith("/") or ".." in self.mount_path.split("/"):
            raise ValueError("invalid mount path")
        return self


class Results(Model):
    path: str = "/workspace/results"
    required_files: list[str] = ["metrics.json", "worker.log"]
    metric_max: dict[Identifier, Nonnegative] = {}
    metric_min: dict[Identifier, Nonnegative] = {}
    max_bytes: Annotated[int, Field(ge=1, le=1073741824)] = 10485760
    export: Literal["runner_verified_bundle"] = "runner_verified_bundle"
    collection_failure: Literal["retry_until_deadline_then_terminate"] = (
        "retry_until_deadline_then_terminate"
    )

    @field_validator("required_files")
    @classmethod
    def paths(cls, value: list[str]) -> list[str]:
        if not value or len(set(value)) != len(value):
            raise ValueError("unique required files required")
        for p in value:
            if not re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", p):
                raise ValueError("relative artifact path required")
            if any(x in {".", ".."} for x in p.split("/")):
                raise ValueError("path traversal")
        return value

    @field_validator("path")
    @classmethod
    def result_path(cls, value: str) -> str:
        if not value.startswith("/workspace/") or ".." in value.split("/"):
            raise ValueError("results must be below /workspace")
        return value


class Task(Model):
    schema_version: Literal[1] = 1
    experiment_id: Identifier
    scope: Identifier = "default"
    repository: Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")]
    source_sha: Sha
    image: Annotated[str, Field(pattern=r"^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$")]
    entrypoint: Annotated[list[str], Field(min_length=1, max_length=32)]
    strategy: Literal["flexible_smoke", "strict_repro", "hardware_benchmark"]
    gpu_allowlist: Annotated[list[str], Field(max_length=16)] = []
    gpu_preference: list[str] = []
    min_vram_gb: Annotated[int, Field(ge=1, le=1024)] = 24
    gpu_count: Annotated[int, Field(ge=1, le=8)] = 1
    cloud: Literal["SECURE", "COMMUNITY"] = "SECURE"
    data_centers: list[Identifier] = []
    benchmark_group: Identifier | None = None
    compatibility: Compatibility = Field(default_factory=Compatibility)
    volume: Volume = Field(default_factory=Volume)
    limits: Limits = Field(default_factory=Limits)
    results: Results = Field(default_factory=Results)
    cleanup_policy: Literal["terminate_owned_and_verify"] = "terminate_owned_and_verify"
    registry_credential_id: Identifier | None = None
    input_sha256: dict[Identifier, Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]] = {}

    @field_validator("entrypoint")
    @classmethod
    def argv(cls, value: list[str]) -> list[str]:
        if any(not re.fullmatch(r"[A-Za-z0-9_./:=,+@%-]{1,512}", s) for s in value):
            raise ValueError("entrypoint must be safe literal argv tokens")
        if value[0].rsplit("/", 1)[-1] in {"sh", "bash", "dash", "zsh", "eval", "env"}:
            raise ValueError("shell dispatch is not an entrypoint contract")
        if "-c" in value:
            raise ValueError("inline code is not permitted")
        return value

    @model_validator(mode="after")
    def strategy_rules(self) -> Self:
        if len(set(self.gpu_allowlist)) != len(self.gpu_allowlist):
            raise ValueError("GPU allowlist must be unique")
        if not set(self.gpu_preference) <= set(self.gpu_allowlist):
            raise ValueError("preferences must be in allowlist")
        if self.strategy != "flexible_smoke" and len(self.gpu_allowlist) != 1:
            raise ValueError("locked strategies need one GPU; one task per hardware group")
        if self.strategy == "hardware_benchmark" and not self.benchmark_group:
            raise ValueError("hardware benchmark requires an independent group")
        if self.strategy == "strict_repro":
            c = self.compatibility
            if (
                len(self.data_centers) != 1
                or not c.exact_driver
                or len(c.cuda_versions) != 1
                or len(c.architectures) != 1
            ):
                raise ValueError("strict_repro locks data center, driver, CUDA and architecture")
        return self

    def fingerprint(self) -> str:
        return hashlib.sha256(
            json.dumps(self.model_dump(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


class Candidate(Model):
    gpu_id: str
    data_center: str | None = None
    memory_gb: Nonnegative | None = None
    gpu_count: int = 1
    hourly_usd: Nonnegative | None = None
    capacity: Literal["available", "none", "unknown"] = "unknown"
    cloud: Literal["SECURE", "COMMUNITY"] = "SECURE"
    cuda_versions: list[str] = []
    driver: str | None = None
    architecture: str | None = None
    volume_compatible: bool | None = None
    error: str | None = None
    evidence: Literal["mock", "read_only"] = "mock"


class Authorization(Model):
    """Execution context, never proof that a human granted permission."""

    experiment_id: Identifier
    config_fingerprint: Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
    approved_budget_usd: Positive
    gpu_allowlist: list[str]
    runtime_seconds: int
    explicitly_approved: bool = False
