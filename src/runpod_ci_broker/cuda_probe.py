"""Tiny CUDA Driver API memory smoke. No torch dependency or CPU fallback."""

from __future__ import annotations

import ctypes as c
import json
import uuid
from typing import Any


class GPUProbeError(ValueError):
    pass


class Driver:
    def __init__(self) -> None:
        try:
            self.lib = c.CDLL("libcuda.so.1")
        except OSError:
            raise GPUProbeError("cuda_driver_unavailable") from None

    def call(self, name: str, types: list[Any], *args: Any) -> None:
        try:
            fn = getattr(self.lib, name)
            fn.argtypes, fn.restype = types, c.c_int
            result = fn(*args)
        except (AttributeError, OSError):
            raise GPUProbeError("cuda_driver_abi_unavailable") from None
        if result != 0:
            raise GPUProbeError("cuda_driver_call_failed")


def probe(driver: Driver | None = None) -> dict[str, Any]:
    d = driver if driver is not None else Driver()
    d.call("cuInit", [c.c_uint], 0)
    count, version, device = c.c_int(), c.c_int(), c.c_int()
    d.call("cuDeviceGetCount", [c.POINTER(c.c_int)], c.byref(count))
    if count.value != 1:
        raise GPUProbeError("exactly_one_cuda_device_required")
    d.call("cuDriverGetVersion", [c.POINTER(c.c_int)], c.byref(version))
    d.call("cuDeviceGet", [c.POINTER(c.c_int), c.c_int], c.byref(device), 0)
    name = c.create_string_buffer(256)
    d.call("cuDeviceGetName", [c.c_char_p, c.c_int, c.c_int], name, 256, device)
    memory = c.c_size_t()
    d.call("cuDeviceTotalMem_v2", [c.POINTER(c.c_size_t), c.c_int], c.byref(memory), device)
    major, minor = c.c_int(), c.c_int()
    for attribute, output in [(75, major), (76, minor)]:
        d.call(
            "cuDeviceGetAttribute",
            [c.POINTER(c.c_int), c.c_int, c.c_int],
            c.byref(output),
            attribute,
            device,
        )
    device_uuid = (c.c_ubyte * 16)()
    d.call("cuDeviceGetUuid_v2", [c.c_void_p, c.c_int], device_uuid, device)
    ctx = c.c_void_p()
    d.call("cuDevicePrimaryCtxRetain", [c.POINTER(c.c_void_p), c.c_int], c.byref(ctx), device)
    pointer = c.c_uint64()
    try:
        d.call("cuCtxSetCurrent", [c.c_void_p], ctx)
        d.call("cuMemAlloc_v2", [c.POINTER(c.c_uint64), c.c_size_t], c.byref(pointer), 4096)
        try:
            d.call("cuMemsetD32_v2", [c.c_uint64, c.c_uint, c.c_size_t], pointer, 0x13579BDF, 1024)
            host = (c.c_uint32 * 1024)()
            d.call("cuMemcpyDtoH_v2", [c.c_void_p, c.c_uint64, c.c_size_t], host, pointer, 4096)
            d.call(
                "cuCtxSynchronize",
                [],
            )
            if any(value != 0x13579BDF for value in host):
                raise GPUProbeError("cuda_roundtrip_mismatch")
        finally:
            d.call("cuMemFree_v2", [c.c_uint64], pointer)
    finally:
        d.call("cuDevicePrimaryCtxRelease_v2", [c.c_int], device)
    return {
        "device_name": name.value.decode("utf-8", errors="strict"),
        "device_uuid": str(uuid.UUID(bytes=bytes(device_uuid))),
        "device_count": count.value,
        "memory_bytes": memory.value,
        "driver_cuda_version": version.value,
        "architecture": f"sm_{major.value}{minor.value}",
        "memory_roundtrip_bytes": 4096,
        "memory_roundtrip_passed": True,
        "probe": "cuda_driver_memory_roundtrip_v1",
        "framework_runtime": "not_tested",
    }


def main() -> int:
    try:
        print(json.dumps(probe(), allow_nan=False))
        return 0
    except (GPUProbeError, ValueError, OSError):
        print(json.dumps({"error": "cuda_probe_failed", "gpu_verified": False}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
