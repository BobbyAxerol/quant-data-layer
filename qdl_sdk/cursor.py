from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import threading
import time
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


async def _await_durable(awaitable):
    """Drain accepted I/O even on repeated cancellation; never orphan a writer."""
    task = asyncio.ensure_future(awaitable)
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
        except BaseException as error:
            if cancelled:
                raise asyncio.CancelledError() from error
            raise
    if cancelled:
        raise asyncio.CancelledError()
    return result


@dataclass
class _CursorWrite:
    key: str
    checkpoint: CursorCheckpoint
    replace: bool
    done: asyncio.Future


class FileCursorStore:
    """Atomic single-process cursor store for research/paper consumers."""

    def __init__(self, path: str | Path, *, max_pending: int = 64) -> None:
        if not 1 <= max_pending <= 1024:
            raise ValueError("cursor max_pending must be between 1 and 1024")
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._slots = asyncio.Semaphore(max_pending)
        self._io_lock = asyncio.Lock()
        self._queue: asyncio.Queue[_CursorWrite] = asyncio.Queue(max_pending)
        self._worker: asyncio.Task | None = None
        self._closed = False
        self._last_report = time.monotonic()
        self.metrics = {"pending": 0, "pending_peak": 0, "commits": 0,
                        "acknowledged": 0, "errors": 0, "io_seconds": 0.0,
                        "max_io_seconds": 0.0}

    def _read(self) -> dict[str, dict[str, str | int]]:
        if not self.path.exists():
            return {}
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if payload.get("schema") != "qdl.sdk-cursors.v2" or not isinstance(payload.get("items"), dict):
            raise ValueError("cursor store schema is invalid")
        return payload["items"]

    def load(self, key: str) -> CursorCheckpoint | None:
        with self._lock:
            value = self._read().get(key)
            return CursorCheckpoint(**value) if value else None

    def save(self, key: str, checkpoint: CursorCheckpoint) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("cursor store is closed")
            items = self._read()
            current = items.get(key)
            if current is not None and checkpoint.offset < int(current["offset"]):
                raise ValueError("cursor checkpoint cannot move backwards")
            items[key] = asdict(checkpoint)
            self._write(items)

    def replace(self, key: str, checkpoint: CursorCheckpoint) -> None:
        """Atomically establish a fresh snapshot as the new offset baseline."""
        with self._lock:
            if self._closed:
                raise RuntimeError("cursor store is closed")
            items = self._read()
            items[key] = asdict(checkpoint)
            self._write(items)

    async def aload(self, key: str) -> CursorCheckpoint | None:
        async with self._slots:
            if self._closed:
                raise RuntimeError("cursor store is closed")
            async with self._io_lock:
                return await _await_durable(asyncio.to_thread(self.load, key))

    async def asave(self, key: str, checkpoint: CursorCheckpoint) -> None:
        await self._submit(key, checkpoint, replace=False)

    async def areplace(self, key: str, checkpoint: CursorCheckpoint) -> None:
        await self._submit(key, checkpoint, replace=True)

    async def _submit(self, key: str, checkpoint: CursorCheckpoint, *, replace: bool) -> None:
        await self._slots.acquire()
        if self._closed:
            self._slots.release()
            raise RuntimeError("cursor store is closed")
        done = asyncio.get_running_loop().create_future()
        self._queue.put_nowait(_CursorWrite(key, checkpoint, replace, done))
        self.metrics["pending"] += 1
        self.metrics["pending_peak"] = max(self.metrics["pending_peak"], self.metrics["pending"])
        if self._worker is None:
            self._worker = asyncio.create_task(self._drain(), name="qdl-cursor-writer")
        await _await_durable(done)

    def _commit_batch(self, batch: list[_CursorWrite]) -> list[Exception | None]:
        with self._lock:
            items = self._read()
            errors: list[Exception | None] = []
            accepted = False
            for request in batch:
                previous = items.get(request.key)
                value = asdict(request.checkpoint)
                if (not request.replace and previous is not None
                        and request.checkpoint.offset < int(previous["offset"])):
                    errors.append(ValueError("cursor checkpoint cannot move backwards"))
                    continue
                errors.append(None)
                accepted = True
                items[request.key] = value
            # Equal retries still need fsync: a prior directory fsync may have failed.
            if accepted:
                self._write(items)
            return errors

    async def _drain(self) -> None:
        try:
            while not self._queue.empty():
                batch = []
                while not self._queue.empty():
                    batch.append(self._queue.get_nowait())
                started = time.monotonic()
                try:
                    async with self._io_lock:
                        errors = await asyncio.to_thread(self._commit_batch, batch)
                except Exception as error:
                    errors = [error] * len(batch)
                elapsed = time.monotonic() - started
                self.metrics["commits"] += 1
                self.metrics["io_seconds"] += elapsed
                self.metrics["max_io_seconds"] = max(self.metrics["max_io_seconds"], elapsed)
                for request, error in zip(batch, errors):
                    if error is None:
                        self.metrics["acknowledged"] += 1
                        request.done.set_result(None)
                    else:
                        self.metrics["errors"] += 1
                        request.done.set_exception(error)
                    self.metrics["pending"] -= 1
                    self._slots.release()
                if time.monotonic() - self._last_report >= 30:
                    logging.getLogger(__name__).info("qdl_cursor_persistence %s", json.dumps(self.metrics))
                    self._last_report = time.monotonic()
        finally:
            self._worker = None

    async def aclose(self) -> None:
        self._closed = True
        worker = self._worker
        if worker is not None:
            await _await_durable(worker)
        # Drain an admitted read as well; no new operation can pass _closed.
        async with self._io_lock:
            pass

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
