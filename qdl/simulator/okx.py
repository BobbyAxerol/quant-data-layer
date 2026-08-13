from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping


class BookState(str, Enum):
    SYNCING = "SYNCING"
    LIVE = "LIVE"
    GAPPED = "GAPPED"
    DEGRADED = "DEGRADED"


@dataclass(frozen=True)
class FrameResult:
    state: BookState
    accepted: bool
    response: str | None = None


class OkxBookSimulator:
    """Protocol oracle only; the production order-book core belongs to Phase 3."""

    def __init__(self):
        self.generation = 0
        self.last_sequence: int | None = None
        self.state = BookState.SYNCING

    def apply(self, frame: Mapping[str, Any]) -> FrameResult:
        kind = str(frame.get("kind", ""))
        generation = int(frame.get("generation", 0))
        response = None
        if kind == "connect":
            self.generation = generation
            self.last_sequence = None
            self.state = BookState.SYNCING
            accepted = True
        elif kind == "keepalive_ping":
            response = "pong"
            accepted = True
        elif kind in {"keepalive_pong", "rest_envelope", "subscribe_ack"}:
            accepted = True
        elif kind == "maintenance":
            self.last_sequence = None
            self.state = BookState.DEGRADED
            accepted = True
        elif kind == "book" and generation < self.generation:
            accepted = False
        elif kind == "book" and frame.get("action") == "snapshot":
            self.last_sequence = int(frame["seq_id"])
            self.state = BookState.LIVE
            accepted = True
        elif kind == "book" and frame.get("action") == "update":
            if self.state is not BookState.LIVE or self.last_sequence != int(frame["prev_seq_id"]):
                self.last_sequence = None
                self.state = BookState.GAPPED
                accepted = False
            else:
                self.last_sequence = int(frame["seq_id"])
                accepted = True
        else:
            accepted = False
        return FrameResult(state=self.state, accepted=accepted, response=response)
