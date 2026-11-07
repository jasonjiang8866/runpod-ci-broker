import ctypes as c
import json
import subprocess

import pytest

from runpod_ci_broker.cuda_probe import GPUProbeError, probe
from runpod_ci_broker.models import Task
from runpod_ci_broker.worker import MIG, admit, run_smoke


class FakeDriver:
    def __init__(self, fail=None, corrupt=False, count=1):
        self.fail, self.corrupt, self.count = fail, corrupt, count
        self.calls = []

    def call(self, name, types, *args):
        self.calls.append(name)
        if name == self.fail:
            raise GPUProbeError("cuda_driver_call_failed")
        if name == "cuDeviceGetCount":
            c.cast(args[0], c.POINTER(c.c_int))[0] = self.count
        if name == "cuDriverGetVersion":
            c.cast(args[0], c.POINTER(c.c_int))[0] = 13000
        if name == "cuDeviceGetName":
            args[0].value = b"NVIDIA GeForce RTX 4090"
        if name == "cuDeviceTotalMem_v2":
            c.cast(args[0], c.POINTER(c.c_size_t))[0] = 24 * 1024**3
        if name == "cuDeviceGetAttribute":
            c.cast(args[0], c.POINTER(c.c_int))[0] = 8 if args[1] == 75 else 9
        if name == "cuMemcpyDtoH_v2":
            for i in range(1024):
                args[0][i] = 0 if self.corrupt else 0x13579BDF


@pytest.fixture
def smoke_task(task):
    return Task.model_validate(
        {
            **task.model_dump(),
            "entrypoint": ["broker-cuda-memory-smoke"],
            "limits": {**task.limits.model_dump(), "runtime_seconds": 30},
            "results": {
                "required_files": ["metrics.json", "worker.log", "environment.json"],
                "metric_min": {"gpu_memory_smoke_passed": 1},
                "metric_max": {"worker_exit_code": 0},
            },
        }
    )


def test_driver_memory_roundtrip_checks_data_and_releases():
    d = FakeDriver()
    r = probe(d)
    assert r["memory_roundtrip_bytes"] == 4096 and r["memory_roundtrip_passed"]
    assert r["driver_cuda_version"] == 13000
    assert d.calls[-2:] == ["cuMemFree_v2", "cuDevicePrimaryCtxRelease_v2"]


@pytest.mark.parametrize("fail", ["cuMemsetD32_v2", "cuMemcpyDtoH_v2", "cuCtxSynchronize"])
def test_device_error_frees_owned_memory(fail):
    d = FakeDriver(fail=fail)
    with pytest.raises(GPUProbeError):
        probe(d)
    assert d.calls[-2:] == ["cuMemFree_v2", "cuDevicePrimaryCtxRelease_v2"]


def test_corrupt_device_readback_is_not_success():
    with pytest.raises(GPUProbeError, match="mismatch"):
        probe(FakeDriver(corrupt=True))


@pytest.mark.parametrize("count", [0, 2])
def test_no_gpu_or_extra_gpu_refused(count):
    d = FakeDriver(count=count)
    with pytest.raises(GPUProbeError):
        probe(d)
    assert "cuMemAlloc_v2" not in d.calls


def test_driver_floor_accepts_cuda13(smoke_task):
    admit(smoke_task, "NVIDIA GeForce RTX 4090", probe(FakeDriver()))


@pytest.mark.parametrize(
    "field,value",
    [
        ("device_name", "CPU"),
        ("device_count", 0),
        ("driver_cuda_version", 12070),
        ("memory_bytes", 1),
        ("memory_roundtrip_passed", False),
    ],
)
def test_bad_runtime_never_admitted(smoke_task, field, value):
    obs = probe(FakeDriver())
    obs[field] = value
    with pytest.raises(ValueError):
        admit(smoke_task, "NVIDIA GeForce RTX 4090", obs)


def test_parent_gpu_name_does_not_prove_mig_profile(smoke_task):
    obs = probe(FakeDriver())
    obs["device_name"] = MIG.split(" MIG ")[0]
    with pytest.raises(ValueError, match="identity"):
        admit(smoke_task, MIG, obs)


def test_wrong_worker_source_rejected_before_subprocess(tmp_path, smoke_task, monkeypatch):
    monkeypatch.setattr(
        "runpod_ci_broker.worker.subprocess.run", lambda *a, **k: pytest.fail("must not run")
    )
    with pytest.raises(ValueError, match="source_revision"):
        run_smoke(smoke_task, "NVIDIA GeForce RTX 4090", "abc123", tmp_path / "out", "0" * 40)


@pytest.mark.parametrize("key", ["RUNPOD_API_KEY", "GITHUB_TOKEN", "GH_TOKEN", "RCB_LEDGER_TOKEN"])
def test_management_credential_never_reaches_worker_child(tmp_path, smoke_task, monkeypatch, key):
    monkeypatch.setenv(key, "DO-NOT-LEAK")
    with pytest.raises(ValueError, match="credential"):
        run_smoke(
            smoke_task, "NVIDIA GeForce RTX 4090", "abc123", tmp_path / "out", smoke_task.source_sha
        )
    assert not (tmp_path / "out").exists()


def test_worker_child_has_no_result_token_or_unrelated_env(tmp_path, smoke_task, monkeypatch):
    monkeypatch.setenv("RCB_RESULT_TOKEN", "a" * 64)
    monkeypatch.setenv("UNRELATED_SECRET", "do-not-leak")

    def run(argv, **kwargs):
        assert "RCB_RESULT_TOKEN" not in kwargs["env"] and "UNRELATED_SECRET" not in kwargs["env"]
        assert kwargs["timeout"] == 30 and not kwargs.get("shell")
        return subprocess.CompletedProcess(argv, 0, json.dumps(probe(FakeDriver())).encode(), b"")

    monkeypatch.setattr("runpod_ci_broker.worker.subprocess.run", run)
    m, files = run_smoke(
        smoke_task, "NVIDIA GeForce RTX 4090", "abc123", tmp_path / "out", smoke_task.source_sha
    )
    assert m["metrics"]["gpu_memory_smoke_passed"] == 1
    assert all(b"do-not-leak" not in data for data in files.values())


def test_timeout_retains_failure_artifacts(tmp_path, smoke_task, monkeypatch):
    def run(*a, **kw):
        raise subprocess.TimeoutExpired("secret-command", 30, output=b"secret-output")

    monkeypatch.setattr("runpod_ci_broker.worker.subprocess.run", run)
    m, files = run_smoke(
        smoke_task, "NVIDIA GeForce RTX 4090", "abc123", tmp_path / "out", smoke_task.source_sha
    )
    assert m["metrics"]["worker_exit_code"] == 2 and b"probe_timed_out" in files["worker.log"]
    assert all(b"secret" not in data for data in files.values())


def test_malformed_probe_never_echoes_arbitrary_output(tmp_path, smoke_task, monkeypatch):
    monkeypatch.setattr(
        "runpod_ci_broker.worker.subprocess.run",
        lambda *a, **k: subprocess.CompletedProcess([], 0, b'{"secret":"do-not-leak"}', b""),
    )
    m, files = run_smoke(
        smoke_task, "NVIDIA GeForce RTX 4090", "abc123", tmp_path / "out", smoke_task.source_sha
    )
    assert m["metrics"]["gpu_memory_smoke_passed"] == 0
    assert all(b"do-not-leak" not in data for data in files.values())
