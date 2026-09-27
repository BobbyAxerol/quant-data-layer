"""Fail-closed provider-session liveness state for the stable V2 edge.

The durable canonical event records the market event time.  For sparse feeds
such as TRADE, that timestamp is not a connection-health signal.  Existing
Rust ingestors therefore publish a tiny atomic state record per active
connection in the already shared stable-state volume.  Query readers match it
to the event's session and generation; they never infer liveness from an old
trade or from a newer, different session.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


SESSION_LIVENESS_SCHEMA = "qdl.provider-session-liveness.v1"
_SESSION_STATES = frozenset({"LIVE", "DISCONNECTED"})
_SAFE_FILE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,119}\.json$")
_MAX_STATE_FILES = 256
_MAX_SESSION_ID_LENGTH = 256


@dataclass(frozen=True, slots=True)
class ProviderSessionLiveness:
    """Validated control-plane evidence for one provider connection."""

    source_session_id: str
    connection_generation: int
    state: str
    last_transport_at_ns: int
    updated_at_ns: int
    config_revision: int

    def __post_init__(self) -> None:
        if (
            not self.source_session_id
            or len(self.source_session_id) > _MAX_SESSION_ID_LENGTH
            or self.connection_generation < 1
            or self.state not in _SESSION_STATES
            or self.last_transport_at_ns < 1
            or self.updated_at_ns < self.last_transport_at_ns
            or self.config_revision < 1
        ):
            raise ValueError("provider session liveness record is invalid")

    @classmethod
    def from_mapping(cls, value: object) -> "ProviderSessionLiveness":
        if not isinstance(value, dict) or set(value) != {
            "schema",
            "source_session_id",
            "connection_generation",
            "state",
            "last_transport_at_ns",
            "updated_at_ns",
            "config_revision",
        }:
            raise ValueError("provider session liveness schema is invalid")
        if value["schema"] != SESSION_LIVENESS_SCHEMA:
            raise ValueError("provider session liveness schema is unsupported")
        fields = (
            "connection_generation",
            "last_transport_at_ns",
            "updated_at_ns",
            "config_revision",
        )
        if (
            not isinstance(value["source_session_id"], str)
            or not isinstance(value["state"], str)
            or any(
                isinstance(value[field], bool) or not isinstance(value[field], int)
                for field in fields
            )
        ):
            raise ValueError("provider session liveness numeric fields are invalid")
        return cls(
            source_session_id=str(value["source_session_id"]),
            connection_generation=int(value["connection_generation"]),
            state=str(value["state"]).upper(),
            last_transport_at_ns=int(value["last_transport_at_ns"]),
            updated_at_ns=int(value["updated_at_ns"]),
            config_revision=int(value["config_revision"]),
        )


@dataclass(frozen=True, slots=True)
class ProviderSessionStatus:
    """Lookup result deliberately carries no raw provider payload."""

    state: str
    liveness_ms: int | None
    flags: tuple[str, ...] = ()


class StableSessionLivenessReader:
    """Read bounded, session-scoped liveness records from stable_state.

    ``now_ns`` is the caller's clock, sampled before this read. With
    ``clock_ns`` (the caller's own clock source) every record is judged at a
    clock sampled after the read (never earlier than ``now_ns``): an old
    heartbeat is aged by the full read, and one the ingestor rewrote during
    the read is not mistaken for a skewed clock; a transport time still in the
    future after the read is a real skew and fails closed. Without
    ``clock_ns`` the caller's sample is the only clock.
    """

    def __init__(self, root: str | Path, *, clock_ns: Callable[[], int] | None = None) -> None:
        self.root = Path(root)
        self._clock_ns = clock_ns
        # Parsed file contents by directory and name, keyed by the file's
        # (inode, mtime, size). Only content is kept - never a verdict: age,
        # skew and the session match are judged on every call.
        self._parsed: dict[str, dict[str, tuple[tuple[int, int, int], ProviderSessionLiveness | None]]] = {}

    def _records(
        self, directory: Path, names: list[str]
    ) -> list[ProviderSessionLiveness | None]:
        """The records of ``names``, each re-read only when its file changed.

        The ingestor replaces a record by rename (a new inode), so the stat key
        names one content exactly; a file that changes between the stat and the
        read is stored under the older key and re-read on the next call. The
        cache holds only the files listed now (at most ``_MAX_STATE_FILES``).
        """

        previous = self._parsed.get(str(directory), {})
        current: dict[str, tuple[tuple[int, int, int], ProviderSessionLiveness | None]] = {}
        records: list[ProviderSessionLiveness | None] = []
        for name in names:
            path = directory / name
            try:
                stat = os.stat(path)
            except OSError:
                records.append(None)
                continue
            key = (stat.st_ino, stat.st_mtime_ns, stat.st_size)
            cached = previous.get(name)
            if cached is not None and cached[0] == key:
                record = cached[1]
            else:
                try:
                    record = ProviderSessionLiveness.from_mapping(
                        json.loads(path.read_text(encoding="utf-8"))
                    )
                except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
                    record = None
            current[name] = (key, record)
            records.append(record)
        self._parsed[str(directory)] = current
        return records

    @staticmethod
    def _directory(*, venue: str, market: str) -> str:
        values = (venue.strip().lower(), market.strip().lower())
        if not all(re.fullmatch(r"[a-z0-9]+", value) for value in values):
            raise ValueError("provider session identity is invalid")
        return "-".join(values)

    def status(
        self,
        *,
        venue: str,
        market: str,
        source_session_id: str,
        connection_generation: int,
        config_revision: int,
        now_ns: int,
    ) -> ProviderSessionStatus:
        if (
            not source_session_id
            or len(source_session_id) > _MAX_SESSION_ID_LENGTH
            or connection_generation < 1
            or config_revision < 1
            or now_ns < 1
        ):
            return ProviderSessionStatus("UNKNOWN", None, ("SOURCE_SESSION_INVALID",))
        try:
            directory = self.root / self._directory(venue=venue, market=market)
        except ValueError:
            return ProviderSessionStatus("UNKNOWN", None, ("SOURCE_SESSION_INVALID",))
        try:
            candidates = []
            with os.scandir(directory) as entries:
                for entry in entries:
                    if entry.is_file() and _SAFE_FILE.fullmatch(entry.name):
                        candidates.append(entry.name)
                        if len(candidates) > _MAX_STATE_FILES:
                            return ProviderSessionStatus(
                                "UNKNOWN", None, ("SOURCE_SESSION_UNAVAILABLE",)
                            )
            candidates.sort()
        except FileNotFoundError:
            return ProviderSessionStatus("UNKNOWN", None, ("SOURCE_SESSION_UNAVAILABLE",))
        except OSError:
            return ProviderSessionStatus("UNKNOWN", None, ("SOURCE_SESSION_UNREADABLE",))
        if not candidates:
            self._parsed.pop(str(directory), None)
            return ProviderSessionStatus("UNKNOWN", None, ("SOURCE_SESSION_UNAVAILABLE",))
        match: ProviderSessionLiveness | None = None
        malformed = False
        for record in self._records(directory, candidates):
            if record is None:
                malformed = True
                continue
            if (
                record.source_session_id == source_session_id
                and record.connection_generation == connection_generation
            ):
                if match is not None:
                    return ProviderSessionStatus("UNKNOWN", None, ("SOURCE_SESSION_AMBIGUOUS",))
                match = record
        if match is None:
            flag = "SOURCE_SESSION_MALFORMED" if malformed else "SOURCE_SESSION_UNAVAILABLE"
            return ProviderSessionStatus("UNKNOWN", None, (flag,))
        if match.config_revision != config_revision:
            return ProviderSessionStatus("UNKNOWN", None, ("SOURCE_SESSION_CONFIG_MISMATCH",))
        # The record is judged at the end of this read, never at the caller's
        # earlier sample: that sample would under-state the age of an old
        # heartbeat (1,999 ms at the sample, 2,019 ms after the read passed a
        # 2,000 ms SLA) and over-state skew for one rewritten meanwhile.
        evaluated_ns = now_ns
        if self._clock_ns is not None:
            evaluated_ns = max(now_ns, int(self._clock_ns()))
        if match.last_transport_at_ns > evaluated_ns:
            return ProviderSessionStatus("UNKNOWN", None, ("SOURCE_SESSION_CLOCK_SKEW",))
        age_ms = (evaluated_ns - match.last_transport_at_ns) // 1_000_000
        return ProviderSessionStatus(match.state, int(age_ms))
