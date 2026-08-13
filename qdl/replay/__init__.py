"""Deterministic replay and gap-free historical/live handoff."""

from qdl.replay.deterministic import DeterministicReplayEngine, ReplayReport
from qdl.replay.handoff import (
    GapFreeHandoff,
    HandoffStore,
    HandoffGrant,
    HistoricalSnapshotCatalog,
    ReplayGapError,
    SignedHandoffCursorCodec,
    SigningKeyProvider,
    SigningKeySet,
    SnapshotHandoffBundle,
    SnapshotHandoffCoordinator,
    SnapshotWatermarkMismatch,
    StaticSigningKeyProvider,
)

__all__ = [
    "DeterministicReplayEngine",
    "GapFreeHandoff",
    "HandoffStore",
    "HandoffGrant",
    "HistoricalSnapshotCatalog",
    "ReplayGapError",
    "ReplayReport",
    "SignedHandoffCursorCodec",
    "SigningKeyProvider",
    "SigningKeySet",
    "SnapshotHandoffBundle",
    "SnapshotHandoffCoordinator",
    "SnapshotWatermarkMismatch",
    "StaticSigningKeyProvider",
]
