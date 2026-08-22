from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class CursorCheckpoint:
    token: str
    offset: int

    def __post_init__(self) -> None:
        if not self.token or self.offset < 0:
            raise ValueError("cursor checkpoint token/offset is invalid")


class CursorStore(Protocol):
    def load(self, key: str) -> CursorCheckpoint | None: ...
    def save(self, key: str, checkpoint: CursorCheckpoint) -> None: ...
    def replace(self, key: str, checkpoint: CursorCheckpoint) -> None: ...


class MemoryCursorStore:
    def __init__(self) -> None:
        self._items: dict[str, CursorCheckpoint] = {}

    def load(self, key: str) -> CursorCheckpoint | None:
        return self._items.get(key)

    def save(self, key: str, checkpoint: CursorCheckpoint) -> None:
        current = self._items.get(key)
        if current is not None and checkpoint.offset < current.offset:
            raise ValueError("cursor checkpoint cannot move backwards")
        self._items[key] = checkpoint

    def replace(self, key: str, checkpoint: CursorCheckpoint) -> None:
        """Begin a new snapshot generation, whose offsets may restart lower."""
        self._items[key] = checkpoint


class FileCursorStore:
    """Atomic single-process cursor store for research/paper consumers."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _read(self) -> dict[str, dict[str, str | int]]:
        if not self.path.exists():
            return {}
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if payload.get("schema") != "qdl.sdk-cursors.v2" or not isinstance(payload.get("items"), dict):
            raise ValueError("cursor store schema is invalid")
        return payload["items"]

    def load(self, key: str) -> CursorCheckpoint | None:
        value = self._read().get(key)
        return CursorCheckpoint(**value) if value else None

    def save(self, key: str, checkpoint: CursorCheckpoint) -> None:
        items = self._read()
        current = items.get(key)
        if current is not None and checkpoint.offset < int(current["offset"]):
            raise ValueError("cursor checkpoint cannot move backwards")
        items[key] = asdict(checkpoint)
        self._write(items)

    def replace(self, key: str, checkpoint: CursorCheckpoint) -> None:
        """Atomically establish a fresh snapshot as the new offset baseline."""
        items = self._read()
        items[key] = asdict(checkpoint)
        self._write(items)

    def _write(self, items: dict[str, dict[str, str | int]]) -> None:
        payload = json.dumps(
            {"schema": "qdl.sdk-cursors.v2", "items": items},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        fd, temporary = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=self.path.parent)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            directory_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
