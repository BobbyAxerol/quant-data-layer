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
import re
from dataclasses import dataclass
from pathlib import Path


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
    """Read bounded, session-scoped liveness records from stable_state."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

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
            for path in directory.iterdir():
                if path.is_file() and _SAFE_FILE.fullmatch(path.name):
                    candidates.append(path)
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
            return ProviderSessionStatus("UNKNOWN", None, ("SOURCE_SESSION_UNAVAILABLE",))
        match: ProviderSessionLiveness | None = None
        malformed = False
        for path in candidates:
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                record = ProviderSessionLiveness.from_mapping(raw)
            except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
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
        if match.last_transport_at_ns > now_ns:
            return ProviderSessionStatus("UNKNOWN", None, ("SOURCE_SESSION_CLOCK_SKEW",))
        age_ms = (now_ns - match.last_transport_at_ns) // 1_000_000
        return ProviderSessionStatus(match.state, int(age_ms))
