from __future__ import annotations

import hashlib
import io
import json
import os
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from qdl.history.bars import BarRecord, select_revisions


class SnapshotConflict(RuntimeError):
    """The dataset head changed between planning and atomic metadata commit."""


class ObjectStore(Protocol):
    def put_immutable(self, key: str, payload: bytes) -> str: ...
    def get(self, key: str) -> bytes: ...
    def get_optional(self, key: str) -> bytes | None: ...
    def compare_and_swap(self, key: str, expected_sha256: str | None, payload: bytes) -> str: ...
    def list(self, prefix: str) -> list[str]: ...
    def delete(self, key: str) -> None: ...


class LocalObjectStore:
    """Filesystem object-store test adapter with atomic pointer replacement."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def _path(self, key: str) -> Path:
        path = (self.root / key.strip("/")).resolve()
        if self.root not in path.parents:
            raise ValueError("object key escapes configured root")
        return path

    def put_immutable(self, key: str, payload: bytes) -> str:
        path = self._path(key)
        digest = hashlib.sha256(payload).hexdigest()
        with self._lock:
            if path.exists():
                if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                    raise SnapshotConflict("immutable object key already contains different bytes")
                return digest
            path.parent.mkdir(parents=True, exist_ok=True)
            temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
            with temp.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
        return digest

    def get(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def get_optional(self, key: str) -> bytes | None:
        path = self._path(key)
        return path.read_bytes() if path.exists() else None

    def compare_and_swap(self, key: str, expected_sha256: str | None, payload: bytes) -> str:
        path = self._path(key)
        with self._lock:
            current = path.read_bytes() if path.exists() else None
            current_digest = hashlib.sha256(current).hexdigest() if current is not None else None
            if current_digest != expected_sha256:
                raise SnapshotConflict("dataset head changed before metadata commit")
            path.parent.mkdir(parents=True, exist_ok=True)
            temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
            with temp.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
        return hashlib.sha256(payload).hexdigest()

    def list(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        return sorted(str(path.relative_to(self.root)) for path in base.rglob("*") if path.is_file())

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)


class S3CompatibleObjectStore:
    """S3-compatible adapter using conditional writes; client is injected."""

    def __init__(self, client: Any, *, bucket: str, prefix: str = "qdl"):
        if not bucket.strip():
            raise ValueError("S3 bucket is required")
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def _key(self, key: str) -> str:
        return "/".join(part for part in (self.prefix, key.strip("/")) if part)

    def put_immutable(self, key: str, payload: bytes) -> str:
        digest = hashlib.sha256(payload).hexdigest()
        try:
            self.client.put_object(
                Bucket=self.bucket, Key=self._key(key), Body=payload,
                IfNoneMatch="*", Metadata={"sha256": digest},
            )
        except Exception as error:
            existing = self.get_optional(key)
            if existing is None or hashlib.sha256(existing).hexdigest() != digest:
                raise SnapshotConflict("immutable S3 object conflict") from error
        return digest

    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=self._key(key))["Body"].read()

    def get_optional(self, key: str) -> bytes | None:
        try:
            return self.get(key)
        except Exception:
            return None

    def compare_and_swap(self, key: str, expected_sha256: str | None, payload: bytes) -> str:
        current = self.get_optional(key)
        current_digest = hashlib.sha256(current).hexdigest() if current is not None else None
        if current_digest != expected_sha256:
            raise SnapshotConflict("S3 dataset head changed before metadata commit")
        arguments = {
            "Bucket": self.bucket, "Key": self._key(key), "Body": payload,
            "Metadata": {"sha256": hashlib.sha256(payload).hexdigest()},
        }
        if current is None:
            arguments["IfNoneMatch"] = "*"
        else:
            head = self.client.head_object(Bucket=self.bucket, Key=self._key(key))
            arguments["IfMatch"] = head["ETag"]
        try:
            self.client.put_object(**arguments)
        except Exception as error:
            raise SnapshotConflict("conditional S3 metadata commit failed") from error
        return hashlib.sha256(payload).hexdigest()

    def list(self, prefix: str) -> list[str]:
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=self._key(prefix))
        root = f"{self.prefix}/" if self.prefix else ""
        return sorted(item["Key"].removeprefix(root) for item in response.get("Contents", []))

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self._key(key))


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: str
    dataset_id: str
    parent_snapshot_id: str | None
    data_key: str
    data_sha256: str
    row_count: int
    source_cursor_start: str
    source_cursor_end: str
    normalizer_version: str
    config_revision: int
    schema: tuple[tuple[str, str], ...]


class AtomicParquetCatalog:
    """Immutable Parquet and atomic metadata pointer used for shadow certification."""

    def __init__(self, store: ObjectStore):
        self.store = store

    def current(self, dataset_id: str) -> Snapshot | None:
        pointer = self.store.get_optional(self._pointer_key(dataset_id))
        if pointer is None:
            return None
        pointer_data = json.loads(pointer)
        manifest = self.store.get(pointer_data["manifest_key"])
        if hashlib.sha256(manifest).hexdigest() != pointer_data["manifest_sha256"]:
            raise ValueError("snapshot manifest checksum mismatch")
        return Snapshot(**self._snapshot_values(json.loads(manifest)))

    def commit(
        self,
        dataset_id: str,
        records: list[BarRecord],
        *,
        source_cursor_start: str,
        source_cursor_end: str,
        normalizer_version: str,
        config_revision: int,
        expected_parent_snapshot_id: str | None,
        crash_at: str | None = None,
    ) -> Snapshot:
        selected = select_revisions(records)
        if not selected:
            raise ValueError("cannot commit an empty historical snapshot")
        current_pointer = self.store.get_optional(self._pointer_key(dataset_id))
        current = self.current(dataset_id)
        current_id = current.snapshot_id if current else None
        if current_id != expected_parent_snapshot_id:
            raise SnapshotConflict("expected parent is not the current dataset head")
        table = pa.Table.from_pylist([record.as_dict() for record in selected])
        sink = io.BytesIO()
        pq.write_table(table, sink, compression="zstd", use_dictionary=True)
        parquet_bytes = sink.getvalue()
        data_sha = hashlib.sha256(parquet_bytes).hexdigest()
        snapshot_id = str(uuid.uuid4())
        data_key = f"{dataset_id}/data/{snapshot_id}-{data_sha[:16]}.parquet"
        self.store.put_immutable(data_key, parquet_bytes)
        if crash_at == "after_data":
            raise RuntimeError("injected crash after immutable data upload")

        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            dataset_id=dataset_id,
            parent_snapshot_id=current_id,
            data_key=data_key,
            data_sha256=data_sha,
            row_count=len(selected),
            source_cursor_start=source_cursor_start,
            source_cursor_end=source_cursor_end,
            normalizer_version=normalizer_version,
            config_revision=config_revision,
            schema=tuple((field.name, str(field.type)) for field in table.schema),
        )
        manifest_bytes = json.dumps(
            {**snapshot.__dict__, "schema": [list(item) for item in snapshot.schema]},
            sort_keys=True, separators=(",", ":"),
        ).encode()
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
        manifest_key = f"{dataset_id}/metadata/{snapshot_id}.json"
        self.store.put_immutable(manifest_key, manifest_bytes)
        if crash_at == "after_manifest":
            raise RuntimeError("injected crash after manifest upload")

        pointer_bytes = json.dumps(
            {"snapshot_id": snapshot_id, "manifest_key": manifest_key,
             "manifest_sha256": manifest_sha},
            sort_keys=True, separators=(",", ":"),
        ).encode()
        expected_pointer_sha = (
            hashlib.sha256(current_pointer).hexdigest() if current_pointer is not None else None
        )
        self.store.compare_and_swap(
            self._pointer_key(dataset_id), expected_pointer_sha, pointer_bytes
        )
        return snapshot

    def read(self, snapshot: Snapshot | None = None, *, dataset_id: str | None = None) -> list[BarRecord]:
        selected = snapshot or (self.current(dataset_id or "") if dataset_id else None)
        if selected is None:
            return []
        payload = self.store.get(selected.data_key)
        if hashlib.sha256(payload).hexdigest() != selected.data_sha256:
            raise ValueError("Parquet data checksum mismatch")
        table = pq.read_table(io.BytesIO(payload))
        return [BarRecord(**row) for row in table.to_pylist()]

    @staticmethod
    def _pointer_key(dataset_id: str) -> str:
        if not dataset_id.strip() or ".." in dataset_id:
            raise ValueError("invalid dataset_id")
        return f"{dataset_id}/metadata/current.json"

    @staticmethod
    def _snapshot_values(payload: dict) -> dict:
        return {**payload, "schema": tuple(tuple(item) for item in payload["schema"])}


class PyIcebergTableAppender:
    """Thin injected boundary; provisioning/configuration remains an operator decision."""

    def __init__(self, table: Any):
        self.table = table

    def append(self, records: list[BarRecord]) -> None:
        if not records:
            raise ValueError("cannot append empty Iceberg batch")
        self.table.append(pa.Table.from_pylist([row.as_dict() for row in select_revisions(records)]))
