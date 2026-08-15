from __future__ import annotations

import hashlib
import json
import os
import tempfile
import asyncio
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from qdl.consumer import ConsumerManifest
from qdl_sdk import (
    AsyncDataLayerClient,
    BarRevisionPolicy,
    DataRequirement,
    Feed,
    GapPolicy,
    Grade,
    RecoveryPolicy,
    StalePolicy,
    StreamEvent,
)


def sdk_requirement(manifest: ConsumerManifest, index: int = 0) -> DataRequirement:
    try:
        value = manifest.requirements[index]
    except IndexError as error:
        raise ValueError("consumer manifest requirement index is invalid") from error
    return DataRequirement(
        instrument_uid=value.instrument_uid,
        feed=Feed(value.feed.value),
        consumer_grade=Grade(value.consumer_grade.value),
        source_policy_id=value.source_policy_id,
        interval=value.interval,
        warmup_limit=value.warmup_limit,
        max_freshness_ms=value.max_freshness_ms,
        require_full_coverage=value.require_full_coverage,
        require_final_bars=value.require_final_bars,
        stale_policy=StalePolicy(value.stale_policy.value),
        gap_policy=GapPolicy(value.gap_policy.value),
        recovery=RecoveryPolicy(value.recovery.value),
        bar_revision_policy=BarRevisionPolicy(value.bar_revision_policy.value),
    )


@dataclass(frozen=True, slots=True)
class CanaryRunResult:
    consumer_id: str
    applied_events: int
    checkpointed_offset: int
    signal_state_sha256: str
    signal: int
    execution_dependency: str


class DeterministicPaperSignalState:
    """Small deterministic canary state; it observes data and never submits orders."""

    def __init__(self, *, max_bars: int = 30) -> None:
        if max_bars < 2:
            raise ValueError("paper canary state requires at least two bars")
        self.max_bars = max_bars
        self._bars: dict[int, tuple[int, str]] = {}

    def apply_view(self, item) -> None:
        payload = item.payload
        self.apply_bar(
            open_time_ns=int(payload.open_time_ns),
            revision=int(payload.revision),
            close=payload.close.source_text,
        )

    def apply_event(self, envelope) -> None:
        if envelope.WhichOneof("payload") != "bar":
            raise ValueError("paper canary accepts BAR events only")
        bar = envelope.bar
        if not bar.is_final:
            raise ValueError("paper canary cannot apply an in-progress bar")
        self.apply_bar(
            open_time_ns=int(bar.open_time_ns),
            revision=int(bar.revision),
            close=bar.close.source_text,
        )

    def apply_bar(self, *, open_time_ns: int, revision: int, close: str) -> None:
        if open_time_ns <= 0 or revision < 0:
            raise ValueError("paper canary bar identity is invalid")
        parsed = Decimal(close)
        if not parsed.is_finite():
            raise ValueError("paper canary close must be finite")
        current = self._bars.get(open_time_ns)
        if current is not None and revision < current[0]:
            raise ValueError("paper canary bar revision regressed")
        self._bars[open_time_ns] = revision, close
        while len(self._bars) > self.max_bars:
            del self._bars[min(self._bars)]

    @property
    def signal(self) -> int:
        closes = [Decimal(value[1]) for _, value in sorted(self._bars.items())]
        if len(closes) < 2:
            return 0
        mean = sum(closes, Decimal(0)) / Decimal(len(closes))
        return 1 if closes[-1] > mean else -1 if closes[-1] < mean else 0

    @property
    def digest(self) -> str:
        payload = json.dumps(
            [
                {"open_time_ns": key, "revision": value[0], "close": value[1]}
                for key, value in sorted(self._bars.items())
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(payload).hexdigest()

    def export(self, path: str | Path, *, checkpointed_offset: int) -> None:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {
                "schema": "qdl.phase7.2.paper-signal-state.v1",
                "checkpointed_offset": checkpointed_offset,
                "signal": self.signal,
                "sha256": self.digest,
                "bars": [
                    {"open_time_ns": key, "revision": value[0], "close": value[1]}
                    for key, value in sorted(self._bars.items())
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        fd, temporary = tempfile.mkstemp(
            prefix=f".{destination.name}.", dir=destination.parent
        )
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


class PaperAlphaCanary:
    def __init__(
        self,
        *,
        manifest: ConsumerManifest,
        client: AsyncDataLayerClient,
        state_path: str | Path,
    ) -> None:
        if manifest.execution_dependency != "FORBIDDEN":
            raise ValueError("Phase 7.2 paper canary must forbid execution dependency")
        if client.consumer_id != manifest.consumer_id:
            raise ValueError("paper canary SDK/manifest identity mismatch")
        self.manifest = manifest
        self.client = client
        self.state_path = Path(state_path)

    async def run(
        self,
        *,
        event_count: int,
        timeout_seconds: float,
        resume_restored_state: bool = False,
    ) -> CanaryRunResult:
        if event_count < 0 or timeout_seconds <= 0:
            raise ValueError("paper canary run bounds are invalid")
        requirement = sdk_requirement(self.manifest)
        state = DeterministicPaperSignalState(
            max_bars=max(2, requirement.warmup_limit)
        )
        applied = 0
        checkpointed = 0
        async with self.client.warmup_then_stream(
            requirement,
            resume_restored_state=resume_restored_state,
        ) as session:
            for item in session.warmup.data:
                state.apply_view(item)
            while applied < event_count:
                event = await asyncio.wait_for(
                    session.__anext__(), timeout=timeout_seconds
                )
                if not isinstance(event, StreamEvent):
                    continue
                state.apply_event(event.event)
                session.acknowledge(event)
                applied += 1
                checkpointed = event.logical_offset
                state.export(self.state_path, checkpointed_offset=checkpointed)
        return CanaryRunResult(
            consumer_id=self.manifest.consumer_id,
            applied_events=applied,
            checkpointed_offset=checkpointed,
            signal_state_sha256=state.digest,
            signal=state.signal,
            execution_dependency=self.manifest.execution_dependency,
        )
