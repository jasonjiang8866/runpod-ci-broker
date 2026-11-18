"""Authenticated encryption for the November regression's journal and result export."""

from __future__ import annotations

import base64
import hashlib
import hmac
import io
import json
import os
import zipfile
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .ledger import LedgerError, Store


def key_file() -> Path:
    return Path(os.environ["RUNNER_TEMP"]) / "broker-evidence.key"


def setup_key() -> None:
    secret = os.environ["RUNPOD_API_KEY"]
    if not secret:
        raise ValueError("credentials_missing")
    key = hmac.digest(secret.encode(), b"runpod-ci-broker/allocation-evidence/v1", hashlib.sha256)
    with os.fdopen(os.open(key_file(), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "wb") as f:
        f.write(key)


def seal(data: bytes, purpose: str) -> bytes:
    nonce = os.urandom(12)
    return nonce + AESGCM(key_file().read_bytes()).encrypt(nonce, data, purpose.encode())


def unseal(data: bytes, purpose: str) -> bytes:
    try:
        return AESGCM(key_file().read_bytes()).decrypt(data[:12], data[12:], purpose.encode())
    except (InvalidTag, ValueError):
        raise LedgerError("sealed_evidence_invalid") from None


class SealedStore:
    def __init__(self, store: Store):
        self.store = store

    def read(self, key: str) -> tuple[str | None, dict[str, Any] | None]:
        version, envelope = self.store.read(key)
        if envelope is None:
            return version, None
        data = base64.b64decode(envelope["sealed_v1"], validate=True)
        value = json.loads(unseal(data, "scope/" + key))
        if not isinstance(value, dict):
            raise LedgerError("sealed_document_invalid")
        return version, value

    def swap(self, key: str, version: str | None, value: dict[str, Any]) -> None:
        data = json.dumps(value, separators=(",", ":"), allow_nan=False).encode()
        self.store.swap(
            key, version, {"sealed_v1": base64.b64encode(seal(data, "scope/" + key)).decode()}
        )


def seal_bundle(directory: Path, files: list[str], fingerprint: str) -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in ["manifest.json", *files]:
            archive.writestr(name, (directory / name).read_bytes())
    path = directory.parent / "bundle.enc"
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(seal(buffer.getvalue(), "bundle/" + fingerprint))
    temporary.replace(path)


if __name__ == "__main__":
    setup_key()
