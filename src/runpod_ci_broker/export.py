"""Bounded result-only HTTP endpoint and HTTPS collector; no compute operations.

The worker receives a per-task result capability, never a RunPod or GitHub key.
This transport is tested locally; no real RunPod proxy or GPU execution is claimed.
"""

from __future__ import annotations

import hashlib
import hmac
import io
import json
import os
import re
import shutil
import tempfile
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, BinaryIO, Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, build_opener

from .models import Task
from .provider import NoRedirect, ProviderError
from .results import validate_manifest, verify_saved
from .storage import atomic_json

MAX_MANIFEST = 65_536
MAX_EXPORT = 64 * 1024 * 1024  # Small-experiment MVP, no unbounded bundle in RAM.


def capability_valid(token: str) -> bool:
    return re.fullmatch(r"[a-f0-9]{64}", token) is not None


class Reader(Protocol):
    def open(self, path: str, timeout: float) -> AbstractContextManager[BinaryIO]: ...


class RunPodResultReader:
    def __init__(self, pod_id: str, port: int, token: str):
        if not re.fullmatch(r"[a-z0-9]{1,64}", pod_id) or not 1 <= port <= 65535:
            raise ValueError("result_endpoint_invalid")
        if not capability_valid(token):
            raise ValueError("result_capability_invalid")
        self.base = f"https://{pod_id}-{port}.proxy.runpod.net"
        self._token = token

    def open(self, path: str, timeout: float) -> AbstractContextManager[BinaryIO]:
        if not re.fullmatch(r"/v1/(manifest|files/[A-Za-z0-9_./-]+)", path):
            raise ValueError("result_path_invalid")
        if any(p in {".", ".."} for p in path.split("/")):
            raise ValueError("result_path_invalid")
        request = Request(
            self.base + quote(path, safe="/"),
            headers={
                "Authorization": "Bearer " + self._token,
                "Accept-Encoding": "identity",
            },
        )
        return cast(
            AbstractContextManager[BinaryIO],
            build_opener(NoRedirect).open(request, timeout=timeout),
        )


def collect_https(
    task: Task,
    pod_id: str,
    reader: Reader,
    destination: Path,
    *,
    deadline: float,
    now: Callable[[], float] = time.monotonic,
) -> bool:
    """Verify in a sibling staging directory; publish once, after every check.

    deadline is absolute monotonic time from the controller's collection budget.
    A socket operation has at most 5s idle timeout; no redirect or retry here.
    This exports to the runner, NOT to durable GitHub artifacts by itself.
    """

    def remaining() -> float:
        left = deadline - now()
        if left <= 0:
            raise ProviderError("collection_deadline")
        return min(left, 5.0)

    def read(path: str, sink: BinaryIO, limit: int) -> None:
        with reader.open(path, remaining()) as source:
            received = 0
            while True:
                remaining()
                # read1 avoids waiting to fill 64KiB on a slow streaming response.
                method = getattr(source, "read1", source.read)
                chunk = method(min(65_536, limit - received + 1))
                remaining()
                if not chunk:
                    break
                received += len(chunk)
                if received > limit:
                    raise ProviderError("result_size_exceeded")
                sink.write(chunk)

    if task.results.max_bytes > MAX_EXPORT:
        raise ValueError("network_export_limit_64mib")
    if destination.exists() or destination.is_symlink():
        # Never overwrite a previous verified export or follow a hostile symlink.
        raise ValueError("result_destination_exists")
    if any(p.is_symlink() for p in destination.parents):
        raise ValueError("result_symlink_refused")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".collect-", dir=destination.parent))
    try:
        manifest_buffer = io.BytesIO()
        read("/v1/manifest", manifest_buffer, MAX_MANIFEST)
        manifest = json.loads(manifest_buffer.getvalue())
        passed = validate_manifest(task, pod_id, manifest)
        for name in task.results.required_files:
            target = staging / name
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as f:
                read("/v1/files/" + name, f, manifest["files"][name]["bytes"])
                f.flush()
                os.fsync(f.fileno())
        # Persist nested directory entries as well as file contents before publication.
        directories = {staging, *(p.parent for p in staging.rglob("*") if p.is_file())}
        for directory in sorted(directories, key=lambda p: len(p.parts), reverse=True):
            directory_fd = os.open(directory, os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        atomic_json(staging / "manifest.json", manifest)
        if not verify_saved(staging, task, pod_id):
            raise ProviderError("result_integrity_failed")
        remaining()
        staging.rename(destination)
        directory_fd = os.open(destination.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return passed
    except HTTPError as exc:
        code = {
            401: "result_authentication_failed",
            403: "result_permission_denied",
            404: "result_not_ready",
        }.get(exc.code, "result_http_failed")
        raise ProviderError(code, retryable=exc.code in {404, 429} or exc.code >= 500) from None
    except (URLError, TimeoutError, OSError):
        raise ProviderError("result_transport_failed", retryable=True) from None
    except (ValueError, KeyError, TypeError):
        raise ProviderError("result_manifest_invalid") from None
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def result_server(
    task: Task,
    pod_id: str,
    manifest: dict[str, Any],
    files: dict[str, bytes],
    token: str,
    address: tuple[str, int] = ("127.0.0.1", 0),
) -> HTTPServer:
    """Serve an immutable small bundle; bind 0.0.0.0 only behind reviewed TLS proxy.

    Result auth only. Never offers a shell, arbitrary path, or management endpoint.
    Callers own server lifetime; it does not keep a Pod alive or terminate it.
    """
    validate_manifest(task, pod_id, manifest)
    if not capability_valid(token) or sum(map(len, files.values())) > MAX_EXPORT:
        raise ValueError("result_export_invalid")
    if set(files) != set(task.results.required_files):
        raise ValueError("result_file_set_mismatch")
    payloads = {"/v1/manifest": json.dumps(manifest, allow_nan=False).encode()}
    if len(payloads["/v1/manifest"]) > MAX_MANIFEST:
        raise ValueError("result_manifest_too_large")
    for name, data in files.items():
        if manifest["files"][name] != {
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }:
            raise ValueError("result_checksum_mismatch")
        payloads["/v1/files/" + name] = bytes(data)

    class Handler(BaseHTTPRequestHandler):
        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(5)

        def log_message(self, format: str, *args: Any) -> None:
            pass  # no tokens, request paths, headers or arbitrary workload logs

        def do_GET(self) -> None:
            supplied = self.headers.get("Authorization", "")
            if not hmac.compare_digest(supplied.encode(), ("Bearer " + token).encode()):
                self.send_error(401, "Unauthorized")
                return
            payload = payloads.get(self.path)
            if payload is None:
                self.send_error(404, "Not found")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, TimeoutError, ConnectionResetError):
                pass

    return HTTPServer(address, Handler)
