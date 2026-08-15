from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from qdl.security.policy import redact_secrets


@dataclass(frozen=True, slots=True)
class AuditRecord:
    sequence: int
    timestamp_ns: int
    actor: str
    action: str
    environment: str
    request_id: str
    result: str
    details: Mapping[str, Any]
    previous_hash: str
    record_hash: str


class AuditChain:
    """Append-only, fsync-backed hash chain for control-plane mutations."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._last_sequence, self._last_hash = self.verify(self.path)

    @staticmethod
    def _canonical(payload: Mapping[str, Any]) -> bytes:
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def append(
        self,
        *,
        actor: str,
        action: str,
        environment: str,
        request_id: str,
        result: str,
        details: Mapping[str, Any],
    ) -> AuditRecord:
        with self._lock:
            base = {
                "sequence": self._last_sequence + 1,
                "timestamp_ns": time.time_ns(),
                "actor": actor,
                "action": action,
                "environment": environment,
                "request_id": request_id,
                "result": result,
                "details": redact_secrets(dict(details)),
                "previous_hash": self._last_hash,
            }
            digest = hashlib.sha256(self._canonical(base)).hexdigest()
            record = AuditRecord(**base, record_hash=digest)
            line = self._canonical(asdict(record)) + b"\n"
            descriptor = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
            try:
                os.write(descriptor, line)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self._last_sequence = record.sequence
            self._last_hash = record.record_hash
            return record

    @classmethod
    def verify(cls, path: str | Path) -> tuple[int, str]:
        file_path = Path(path)
        if not file_path.exists():
            return 0, "0" * 64
        previous = "0" * 64
        sequence = 0
        for raw_line in file_path.read_bytes().splitlines():
            payload = json.loads(raw_line)
            record_hash = str(payload.pop("record_hash"))
            if payload["sequence"] != sequence + 1 or payload["previous_hash"] != previous:
                raise ValueError("audit chain sequence or previous hash is invalid")
            expected = hashlib.sha256(cls._canonical(payload)).hexdigest()
            if expected != record_hash:
                raise ValueError("audit chain hash mismatch")
            sequence = int(payload["sequence"])
            previous = record_hash
        return sequence, previous
