from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
import signal
import sqlite3
import threading
import time
from pathlib import Path

from qdl.adapters.intervals import (
    canonical_interval_ms,
    is_valid_bar_open_ms,
    latest_closed_boundary_ms,
)
from qdl.adapters.binance import (
    BinanceBarRawBinding,
    fetch_closed_bar_history_raw_envelopes as fetch_binance_history,
    fetch_latest_closed_bar_raw_envelope,
)
from qdl.adapters.okx.bar_edge import (
    OkxBarRawBinding,
    fetch_closed_bar_history_raw_envelopes as fetch_okx_history,
    fetch_latest_closed_bar_raw_envelope as fetch_okx_latest,
)
from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.runtime.stable_catalog import StableSourceBinding, StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionBinding,
    StableAcquisitionPlan,
    validate_shared_authority_record,
)
from qdl.transport.kafka_raw import KafkaRawPublisher, KafkaRawPublisherConfig


logger = logging.getLogger(__name__)

_MAX_DURABLE_BAR_ROWS = 10_000


_STATE_SCHEMA_V2 = "qdl.stable-bar-edge-state.v2"
_STATE_SCHEMA_V3 = "qdl.stable-bar-edge-state.v3"
_STATE_SCHEMA_V4 = "qdl.stable-bar-edge-state.v4"
_MAX_CONNECTION_GENERATION = (1 << 64) - 1
# A durable BAR binding is not a promise that every venue has unlimited
# history.  Three years keeps the default bootstrap useful for daily/weekly
# research while remaining below the proven BTC/ETH history on both providers.
# Consumers requiring more retain the bounded provider-history path, which
# reports real coverage rather than inventing missing rows.
_BOOTSTRAP_HISTORY_LOOKBACK_DAYS = 1_095
_BOOTSTRAP_HISTORY_LOOKBACK_MS = _BOOTSTRAP_HISTORY_LOOKBACK_DAYS * 86_400_000


def _canonical_cache_id(path: str | Path) -> str:
    """Read the durable cache generation without initializing or mutating it."""
    database = Path(path).expanduser().resolve()
    try:
        connection = sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True)
        try:
            row = connection.execute(
                "SELECT cache_id FROM cache_identity WHERE singleton = 1"
            ).fetchone()
        finally:
            connection.close()
    except (OSError, sqlite3.Error) as error:
        raise RuntimeError("stable BAR canonical cache identity is unavailable") from error
    value = str(row[0]) if row is not None else ""
    if len(value) != 32 or any(character not in "0123456789abcdef" for character in value):
        raise RuntimeError("stable BAR canonical cache identity is invalid")
    return value


def _bar_interval_ms(interval: str) -> int:
    # `canonical_interval_ms` is the one fixed-duration interval authority.
    # Keeping a private suffix list here previously rejected a valid weekly
    # demand after the catalog had admitted it.
    try:
        return canonical_interval_ms(interval)
    except ValueError as error:
        raise ValueError(f"stable BAR interval is unsupported: {interval}") from error


def _source_provider(source: StableSourceBinding) -> str:
    """Return the catalog venue used for provider BAR calendar alignment."""
    instrument = getattr(source, "instrument", None)
    identity = getattr(instrument, "identity", None)
    venue = getattr(identity, "venue", None)
    # StableSourceBinding always has identity in runtime.  Keeping the generic
    # anchor for lightweight scheduling test doubles preserves their existing
    # contract without weakening real catalog validation.
    return str(venue) if venue is not None else ""


def _valid_source_bar_open_ms(source: StableSourceBinding, open_ms: int) -> bool:
    return is_valid_bar_open_ms(
        source.interval or "",
        open_ms,
        provider=_source_provider(source),
    )


def _latest_source_closed_boundary_ms(
    source: StableSourceBinding,
    observed_ms: int,
) -> int:
    return latest_closed_boundary_ms(
        source.interval or "",
        observed_ms,
        provider=_source_provider(source),
    )


class StableBinanceBarEdge:
    """Bounded real-provider BAR bootstrap plus Binance closed-bar polling.

    The class name is retained for the existing runtime entrypoint. Historical
    Binance and OKX rows always enter raw Kafka and the Rust canonical core;
    this edge never writes query cache or Redis state directly.
    """

    def __init__(
        self,
        *,
        catalog: StableSourceCatalog,
        acquisition: StableAcquisitionPlan,
        authority: dict,
        publisher: KafkaRawPublisher,
        warmup_rows: int = 500,
        max_catchup_rows: int = 1000,
        settlement_delay_seconds: float = 0.10,
        final_retry_initial_seconds: float = 0.10,
        final_retry_max_seconds: float = 1.0,
        max_concurrent_requests: int = 32,
        state_path: str | Path | None = None,
        canonical_cache_id: str | None = None,
        canonical_cache_path: str | Path | None = None,
        clock=time.time,
        generation_clock_ns=time.time_ns,
    ) -> None:
        if not 1 <= warmup_rows <= _MAX_DURABLE_BAR_ROWS:
            raise ValueError(
                "stable BAR warmup rows must be between 1 and 10000"
            )
        if not 1 <= max_catchup_rows <= _MAX_DURABLE_BAR_ROWS:
            raise ValueError(
                "stable BAR catch-up rows must be between 1 and 10000"
            )
        if not 0.01 <= settlement_delay_seconds <= 2.0:
            raise ValueError("stable BAR initial poll delay must be between 0.01 and 2 seconds")
        if not 0.01 <= final_retry_initial_seconds <= final_retry_max_seconds <= 5.0:
            raise ValueError("stable BAR retry bounds are invalid")
        if not 1 <= max_concurrent_requests <= 64:
            raise ValueError("stable BAR request concurrency must be between 1 and 64")
        self.catalog = catalog
        self.acquisition = acquisition
        self.authority = authority
        self.publisher = publisher
        self.warmup_rows = warmup_rows
        self.max_catchup_rows = max_catchup_rows
        self.settlement_delay_seconds = settlement_delay_seconds
        self.final_retry_initial_seconds = final_retry_initial_seconds
        self.final_retry_max_seconds = final_retry_max_seconds
        self.max_concurrent_requests = max_concurrent_requests
        self.state_path = Path(state_path) if state_path is not None else None
        self.canonical_cache_id = (
            str(canonical_cache_id).strip().lower()
            if canonical_cache_id is not None
            else None
        )
        self.canonical_cache_path = (
            Path(canonical_cache_path).expanduser().resolve()
            if canonical_cache_path is not None
            else None
        )
        if self.canonical_cache_id is not None and (
            len(self.canonical_cache_id) != 32
            or any(character not in "0123456789abcdef" for character in self.canonical_cache_id)
        ):
            raise ValueError("stable BAR canonical cache identity is invalid")
        if self.canonical_cache_path is not None and self.canonical_cache_id is None:
            raise ValueError("stable BAR canonical cache path requires its identity")
        self.clock = clock
        self.generation_clock_ns = generation_clock_ns
        authority_revision = int(authority.get("revision", 0))
        self._authority_revision = authority_revision
        self.connection_generation = 0
        self.binance_session_id = ""
        self.okx_session_id = ""
        self._last_open_ms: dict[str, int] = {}
        self._retry_attempts: dict[str, int] = {}
        self._next_retry_at: dict[str, float] = {}
        self._last_retry_log: dict[str, float] = {}
        self._history_bootstrapped = False
        self._stopped = threading.Event()

        source_by_id = {item.binding_id: item for item in catalog.bindings}
        pairs = tuple(
            (source_by_id[item.binding_id], item)
            for item in acquisition.bindings
        )
        # Every enabled Binance/OKX final-BAR demand needs a bounded history
        # bootstrap before it can satisfy a durable warmup. Recurring polling
        # is limited to explicit PYTHON_REST bindings; the execution-grade
        # crypto policy assigns every enabled final BAR to that recovery owner.
        # A future native BAR mode must not bypass this edge without separate
        # finality and reconciliation certification.
        self.history_bindings = tuple(
            pair
            for pair in pairs
            if (
                pair[1].enabled
                and pair[1].runtime == "BINANCE"
                and pair[0].feed.value == "BAR"
            )
        )
        self.history_okx_bindings = tuple(
            pair
            for pair in pairs
            if (
                pair[1].enabled
                and pair[1].runtime == "OKX"
                and pair[0].feed.value == "BAR"
            )
        )
        # Only explicitly configured REST bindings are polled after bootstrap.
        # Native websocket BARs stay owned by their Rust acquisition lane.
        self.bindings = tuple(
            pair for pair in self.history_bindings if pair[1].mode == "PYTHON_REST"
        )
        self.okx_bindings = tuple(
            pair for pair in self.history_okx_bindings if pair[1].mode == "PYTHON_REST"
        )
        # Demand decides which venues and markets exist, so this edge must not
        # assert a fixed market-family set. It owns every enabled crypto BAR
        # bootstrap and only the explicit REST subset for recurring polling.
        expected_history = {
            source.binding_id
            for source, acquisition in pairs
            if acquisition.enabled
            and acquisition.runtime in {"BINANCE", "OKX"}
            and source.feed.value == "BAR"
        }
        history_owned = {
            source.binding_id
            for source, _acquisition in self.history_bindings + self.history_okx_bindings
        }
        if history_owned != expected_history:
            raise ValueError(
                "stable crypto BAR edge does not bootstrap every configured BAR "
                "binding: " + ",".join(sorted(expected_history - history_owned))
            )
        validate_shared_authority_record(authority)
        self._history_bootstrap_active = bool(expected_history)
        self._rest_fallback_active = bool(self.bindings or self.okx_bindings)
        if self._history_bootstrap_active:
            self._restore_state()
            self._issue_connection_generation()
            # Persist the new generation before any provider row can be sent to
            # Kafka. A crash after issuance must never reuse an older source
            # generation on restart.
            self._persist_state()
        else:
            self._history_bootstrapped = True

    @property
    def _binding_ids(self) -> tuple[str, ...]:
        return tuple(sorted(
            source.binding_id
            for source, _acquisition in self.history_bindings + self.history_okx_bindings
        ))

    def _state_identity_payload(self, *, schema: str) -> dict:
        payload = {
            "schema": schema,
            "slice_id": str(self.authority.get("slice_id", "")),
            "authority_revision": int(self.authority["revision"]),
            "catalog_revision": int(self.catalog.catalog_revision),
            "acquisition_revision": int(self.acquisition.revision),
            "warmup_rows": self.warmup_rows,
            "binding_ids": list(self._binding_ids),
        }
        if schema == _STATE_SCHEMA_V4:
            if self.canonical_cache_id is None:
                raise RuntimeError("stable BAR V4 checkpoint requires canonical cache identity")
            payload["canonical_cache_id"] = self.canonical_cache_id
        return payload

    def _state_payload(self) -> dict:
        if not 1 <= self.connection_generation <= _MAX_CONNECTION_GENERATION:
            raise RuntimeError("stable BAR connection generation is invalid")
        schema = _STATE_SCHEMA_V4 if self.canonical_cache_id is not None else _STATE_SCHEMA_V3
        return {
            **self._state_identity_payload(schema=schema),
            "connection_generation": self.connection_generation,
            "last_open_ms": {
                key: self._last_open_ms[key] for key in sorted(self._last_open_ms)
            },
        }

    def _issue_connection_generation(self) -> None:
        floor = self.generation_clock_ns()
        if isinstance(floor, bool) or not isinstance(floor, int) or floor <= 0:
            raise RuntimeError("stable BAR generation clock is invalid")
        previous = self.connection_generation
        if previous < 0 or previous >= _MAX_CONNECTION_GENERATION:
            raise RuntimeError("stable BAR connection generation is exhausted")
        self.connection_generation = max(previous + 1, floor)
        if self.connection_generation > _MAX_CONNECTION_GENERATION:
            raise RuntimeError("stable BAR connection generation is exhausted")
        suffix = f"r{self._authority_revision}-g{self.connection_generation}"
        self.binance_session_id = f"qdl-v2-stable-binance-rest-{suffix}"
        self.okx_session_id = f"qdl-v2-stable-okx-rest-{suffix}"

    def _restore_state(self) -> None:
        if self.state_path is None or not self.state_path.exists():
            return
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise RuntimeError("stable BAR checkpoint is unreadable") from error
        if not isinstance(payload, dict):
            raise RuntimeError("stable BAR checkpoint fields are invalid")
        schema = payload.get("schema")
        cache_continuity_confirmed = True
        if schema == _STATE_SCHEMA_V2:
            expected = self._state_identity_payload(schema=_STATE_SCHEMA_V2)
            expected_fields = set(expected) | {"last_open_ms"}
            previous_generation = 0
            cache_continuity_confirmed = self.canonical_cache_id is None
        elif schema == _STATE_SCHEMA_V3:
            expected = self._state_identity_payload(schema=_STATE_SCHEMA_V3)
            expected_fields = set(expected) | {"connection_generation", "last_open_ms"}
            previous_generation = payload.get("connection_generation")
            if (
                isinstance(previous_generation, bool)
                or not isinstance(previous_generation, int)
                or not 1 <= previous_generation <= _MAX_CONNECTION_GENERATION
            ):
                raise RuntimeError("stable BAR checkpoint generation is invalid")
            cache_continuity_confirmed = self.canonical_cache_id is None
        elif schema == _STATE_SCHEMA_V4:
            if self.canonical_cache_id is None:
                raise RuntimeError("stable BAR checkpoint requires canonical cache identity")
            expected = self._state_identity_payload(schema=_STATE_SCHEMA_V4)
            expected_fields = set(expected) | {"connection_generation", "last_open_ms"}
            previous_generation = payload.get("connection_generation")
            if (
                isinstance(previous_generation, bool)
                or not isinstance(previous_generation, int)
                or not 1 <= previous_generation <= _MAX_CONNECTION_GENERATION
            ):
                raise RuntimeError("stable BAR checkpoint generation is invalid")
            cache_continuity_confirmed = (
                payload.get("canonical_cache_id") == self.canonical_cache_id
            )
        else:
            raise RuntimeError("stable BAR checkpoint fields are invalid")
        if set(payload) != expected_fields:
            raise RuntimeError("stable BAR checkpoint fields are invalid")
        for field in (
            "schema", "slice_id", "authority_revision", "catalog_revision",
            "acquisition_revision", "warmup_rows", "binding_ids",
        ):
            if payload[field] != expected[field]:
                raise RuntimeError(
                    f"stable BAR checkpoint {field} differs from runtime authority"
                )
        last_open_ms = payload["last_open_ms"]
        if not isinstance(last_open_ms, dict) or not set(last_open_ms).issubset(
            self._binding_ids
        ):
            raise RuntimeError("stable BAR checkpoint binding watermarks are invalid")
        restored: dict[str, int] = {}
        sources = {
            source.binding_id: source
            for source, _acquisition in self.history_bindings + self.history_okx_bindings
        }
        for binding_id, value in last_open_ms.items():
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
                or not _valid_source_bar_open_ms(sources[binding_id], value)
            ):
                raise RuntimeError("stable BAR checkpoint watermark is invalid")
            restored[binding_id] = value
        self.connection_generation = previous_generation
        if not cache_continuity_confirmed:
            self._last_open_ms = {}
            self._history_bootstrapped = False
            logger.warning(
                "stable BAR checkpoint cache generation changed; bounded bootstrap required"
            )
            return
        self._last_open_ms = restored
        self._history_bootstrapped = set(restored) == set(self._binding_ids)
        logger.info(
            "stable BAR checkpoint restored bindings=%s complete=%s generation=%s",
            len(restored),
            self._history_bootstrapped,
            self.connection_generation or "legacy",
        )

    def _persist_state(self) -> None:
        if self.state_path is None:
            return
        parent = self.state_path.parent
        parent.mkdir(parents=True, exist_ok=True)
        temporary = parent / f".{self.state_path.name}.{os.getpid()}.tmp"
        encoded = (
            json.dumps(
                self._state_payload(),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            )
            + "\n"
        ).encode("ascii")
        descriptor = None
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                0o600,
            )
            pending = memoryview(encoded)
            while pending:
                written = os.write(descriptor, pending)
                if written <= 0:
                    raise OSError("stable BAR checkpoint write made no progress")
                pending = pending[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            os.replace(temporary, self.state_path)
            directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)

    def _binance_binding(
        self,
        source: StableSourceBinding,
    ) -> BinanceBarRawBinding:
        identity = source.instrument.identity
        return BinanceBarRawBinding(
            market=identity.market,
            product_type=identity.product_type.value,
            native_symbol=source.instrument.native_symbol,
            interval=source.interval or "",
            subscription_id=source.source_id,
            source_session_id=self.binance_session_id,
            connection_generation=self.connection_generation,
            lease_epoch=1,
            authority_revision=int(self.authority["revision"]),
            partition_plan_epoch=1,
            adapter_version=source.adapter_version,
            config_revision=self.acquisition.revision,
            instrument_catalog_revision=self.catalog.catalog_revision,
        )

    def _okx_binding(
        self,
        source: StableSourceBinding,
    ) -> OkxBarRawBinding:
        identity = source.instrument.identity
        return OkxBarRawBinding(
            market=identity.market,
            product_type=identity.product_type.value,
            native_symbol=source.instrument.native_symbol,
            interval=source.interval or "",
            subscription_id=source.source_id,
            source_session_id=self.okx_session_id,
            connection_generation=self.connection_generation,
            lease_epoch=1,
            authority_revision=int(self.authority["revision"]),
            partition_plan_epoch=1,
            adapter_version=source.adapter_version,
            config_revision=self.acquisition.revision,
            instrument_catalog_revision=self.catalog.catalog_revision,
        )

    def _publish_history(
        self,
        source: StableSourceBinding,
        acquisition: StableAcquisitionBinding,
        envelopes,
        *,
        expected_rows: int,
    ) -> int:
        values = tuple(envelopes)
        if len(values) != expected_rows:
            raise RuntimeError(
                f"stable BAR bootstrap coverage mismatch binding={source.binding_id} "
                f"expected={expected_rows} actual={len(values)}"
            )
        opens = tuple(self._open_time_ms(acquisition, item) for item in values)
        expected_opens = frozenset(opens)
        if len(expected_opens) != expected_rows:
            raise RuntimeError(
                f"stable BAR bootstrap contains duplicate opens binding={source.binding_id}"
            )
        existing_opens = self._durable_final_bar_opens(source, expected_opens)
        missing = tuple(
            item for item, open_ms in zip(values, opens, strict=True)
            if open_ms not in existing_opens
        )
        acknowledgements = self.publisher.publish_many(missing) if missing else ()
        if len(acknowledgements) != len(missing):
            raise RuntimeError("stable BAR bootstrap did not receive every Kafka ACK")
        published_opens = {
            self._open_time_ms(acquisition, item) for item in missing
        }
        if existing_opens | published_opens != expected_opens:
            raise RuntimeError(
                f"stable BAR bootstrap did not cover every durable open binding={source.binding_id}"
            )
        self._assert_canonical_cache_identity()
        self._last_open_ms[source.binding_id] = max(expected_opens)
        self._persist_state()
        logger.info(
            "stable real-provider BAR bootstrap ACK binding=%s venue=%s expected_rows=%s "
            "published_rows=%s existing_durable_rows=%s first_open_ms=%s last_open_ms=%s",
            source.binding_id,
            acquisition.runtime,
            len(values),
            len(missing),
            len(existing_opens),
            min(expected_opens),
            max(expected_opens),
        )
        return len(acknowledgements)

    def _assert_canonical_cache_identity(self) -> None:
        """Refuse to certify a bootstrap if its durable generation changed."""
        if self.canonical_cache_path is None:
            return
        assert self.canonical_cache_id is not None
        if _canonical_cache_id(self.canonical_cache_path) != self.canonical_cache_id:
            raise RuntimeError("stable BAR canonical cache generation changed during bootstrap")

    def _durable_final_bar_opens(
        self,
        source: StableSourceBinding,
        expected_opens: frozenset[int],
    ) -> frozenset[int]:
        """Read only already-durable final BAR coverage for one exact binding.

        A cache rebuild may replay a recent real-time window before the BAR
        edge replenishes older history. Re-publishing that overlap from a later
        REST snapshot would use the same revision-zero event identity while a
        venue may have revised its displayed OHLCV. Recovery must retain the
        captured durable event and fill only missing opens; reconciliation is a
        distinct, explicitly revisioned product.
        """
        if not expected_opens or self.canonical_cache_path is None:
            return frozenset()
        self._assert_canonical_cache_identity()
        try:
            connection = sqlite3.connect(
                f"{self.canonical_cache_path.as_uri()}?mode=ro", uri=True
            )
            try:
                rows = connection.execute(
                    """
                    SELECT payload FROM events
                    WHERE stream = ? AND partition_key = ?
                    ORDER BY logical_offset DESC
                    LIMIT 10000
                    """,
                    (source.canonical_stream, source.partition_key),
                ).fetchall()
            finally:
                connection.close()
        except (OSError, sqlite3.Error) as error:
            raise RuntimeError("stable BAR durable coverage is unavailable") from error

        covered: set[int] = set()
        final_lifecycles = {
            market_data_pb2.BAR_LIFECYCLE_FINAL,
            market_data_pb2.BAR_LIFECYCLE_REVISED,
        }
        expected_role = getattr(common_pb2, f"SOURCE_ROLE_{source.source_role}")
        for (payload,) in rows:
            try:
                envelope = market_data_pb2.EventEnvelope.FromString(payload)
            except Exception as error:
                raise RuntimeError("stable BAR durable payload is unreadable") from error
            if envelope.WhichOneof("payload") != "bar":
                raise RuntimeError("stable BAR durable partition contains a non-BAR payload")
            if (
                envelope.instrument_uid != source.instrument.instrument_uid
                or envelope.instrument_id != source.instrument.instrument_id
                or not source.accepts_instrument_revision(envelope.instrument_revision)
                or envelope.venue != source.instrument.identity.venue
                or envelope.market != source.instrument.identity.market
                or envelope.product_type != source.instrument.identity.product_type.value
                or envelope.native_symbol != source.instrument.native_symbol
                or envelope.provider != source.provider
                or envelope.source_id != source.source_id
                or envelope.source_role != expected_role
                or envelope.bar.interval != source.interval
            ):
                raise RuntimeError("stable BAR durable partition differs from its binding")
            open_ms = int(envelope.bar.open_time_ns) // 1_000_000
            if (
                open_ms in expected_opens
                and envelope.bar.is_final
                and envelope.bar.lifecycle in final_lifecycles
            ):
                covered.add(open_ms)
        self._assert_canonical_cache_identity()
        return frozenset(covered)

    def _bootstrap_rows_for(self, source: StableSourceBinding) -> int:
        """Return the real-history bound for one fixed-duration BAR.

        `warmup_rows` stays a global upper bound, but a weekly provider request
        for 10,000 rows would require roughly 192 years that neither
        Binance nor OKX can truthfully supply.  The interval-aware cap makes
        a long BAR bootstrap bounded and honest while keeping minute/hour
        warmups at the configured maximum.
        """
        interval_ms = _bar_interval_ms(source.interval or "")
        return min(
            self.warmup_rows,
            max(1, _BOOTSTRAP_HISTORY_LOOKBACK_MS // interval_ms),
        )

    def _settled_observed_ms(self) -> int:
        """Return the real observation clock for provider finality checks.

        The initial poll offset is enforced by `_expected_closed_open_ms`; it
        must not be subtracted from the provider cutoff.  Subtracting it made
        every new close look unavailable until the next scheduler pass and
        turned a small grace into a fixed multi-second latency floor.
        """
        return int(self.clock() * 1000)

    def _expected_closed_open_ms(
        self,
        source: StableSourceBinding,
        *,
        observed_ms: int,
    ) -> int | None:
        """Return the exact final BAR target eligible for the first read.

        The provider remains the finality authority.  This method only adds a
        minimal scheduler offset after the documented boundary; callers keep
        the same target pinned until an authentic final provider row is
        accepted and durably acknowledged.
        """
        boundary_ms = _latest_source_closed_boundary_ms(source, observed_ms)
        ready_at_ms = boundary_ms + int(self.settlement_delay_seconds * 1000)
        if observed_ms < ready_at_ms:
            return None
        return boundary_ms - _bar_interval_ms(source.interval or "")

    def _retry_delay_seconds(self, binding_id: str) -> float:
        attempts = getattr(self, "_retry_attempts", {}).get(binding_id, 0)
        initial = float(getattr(self, "final_retry_initial_seconds", 0.10))
        maximum = float(getattr(self, "final_retry_max_seconds", 1.0))
        return min(initial * (2 ** min(attempts, 8)), maximum)

    def _schedule_retry(
        self,
        binding_id: str,
        *,
        now: float,
        error: Exception | None = None,
    ) -> None:
        retry_attempts = getattr(self, "_retry_attempts", None)
        if retry_attempts is None:
            retry_attempts = self._retry_attempts = {}
        next_retry_at = getattr(self, "_next_retry_at", None)
        if next_retry_at is None:
            next_retry_at = self._next_retry_at = {}
        delay = self._retry_delay_seconds(binding_id)
        retry_attempts[binding_id] = retry_attempts.get(binding_id, 0) + 1
        next_retry_at[binding_id] = now + delay

        # A provider may publish a final row a few hundred milliseconds after
        # its clock boundary.  That normal race is retried silently. Actual
        # errors are bounded in logs as well as in request rate.
        if error is not None:
            retry_logs = getattr(self, "_last_retry_log", None)
            if retry_logs is None:
                retry_logs = self._last_retry_log = {}
            if now - retry_logs.get(binding_id, 0.0) >= 30.0:
                logger.warning(
                    "stable final BAR provider read failed binding=%s retry_in_seconds=%.2f error=%s",
                    binding_id,
                    delay,
                    error,
                )
                retry_logs[binding_id] = now

    def _clear_retry(self, binding_id: str) -> None:
        getattr(self, "_retry_attempts", {}).pop(binding_id, None)
        getattr(self, "_next_retry_at", {}).pop(binding_id, None)
        getattr(self, "_last_retry_log", {}).pop(binding_id, None)

    def _retry_is_due(self, binding_id: str, *, now: float) -> bool:
        return getattr(self, "_next_retry_at", {}).get(binding_id, 0.0) <= now

    def bootstrap_history(self) -> int:
        if not self._history_bootstrap_active:
            return 0
        if self._history_bootstrapped:
            return 0
        observed_ms = self._settled_observed_ms()
        published = 0
        for source, acquisition in self.history_bindings:
            if source.binding_id in self._last_open_ms:
                continue
            bootstrap_rows = self._bootstrap_rows_for(source)
            published += self._publish_history(
                source,
                acquisition,
                fetch_binance_history(
                    self._binance_binding(source),
                    limit=bootstrap_rows,
                    now_ms=observed_ms,
                    attempts=4,
                    test_provenance=False,
                ),
                expected_rows=bootstrap_rows,
            )
        for source, acquisition in self.history_okx_bindings:
            if source.binding_id in self._last_open_ms:
                continue
            bootstrap_rows = self._bootstrap_rows_for(source)
            published += self._publish_history(
                source,
                acquisition,
                asyncio.run(fetch_okx_history(
                    self._okx_binding(source),
                    limit=bootstrap_rows,
                    now_ms=observed_ms,
                    test_provenance=False,
                )),
                expected_rows=bootstrap_rows,
            )
        self._history_bootstrapped = (
            set(self._last_open_ms) == set(self._binding_ids)
        )
        if not self._history_bootstrapped:
            raise RuntimeError("stable BAR bootstrap did not checkpoint every binding")
        logger.info(
            "stable multi-venue BAR bootstrap complete bindings=%s rows=%s",
            len(self.history_bindings) + len(self.history_okx_bindings),
            published,
        )
        return published

    @staticmethod
    def _open_time_ms(
        acquisition: StableAcquisitionBinding,
        envelope,
    ) -> int:
        payload = json.loads(envelope.raw_frame_bytes)
        if acquisition.runtime == "BINANCE":
            return int(payload["row"][0])
        if acquisition.runtime == "OKX":
            return int(payload["data"][0][0])
        raise ValueError("stable crypto BAR runtime is unsupported")

    def _pending_for_binding(
        self,
        source: StableSourceBinding,
        acquisition: StableAcquisitionBinding,
        latest,
        *,
        observed_ms: int,
    ) -> tuple[tuple[object, int], ...]:
        latest_open_ms = self._open_time_ms(acquisition, latest)
        previous_open_ms = self._last_open_ms.get(source.binding_id)
        if previous_open_ms is None:
            return ((latest, latest_open_ms),)
        if latest_open_ms < previous_open_ms:
            raise RuntimeError(
                f"stable BAR provider latest precedes durable watermark "
                f"binding={source.binding_id} previous={previous_open_ms} "
                f"latest={latest_open_ms}"
            )
        if latest_open_ms == previous_open_ms:
            return ()

        interval_ms = _bar_interval_ms(source.interval or "")
        elapsed_ms = latest_open_ms - previous_open_ms
        if elapsed_ms % interval_ms:
            raise RuntimeError(
                f"stable BAR provider boundary mismatch binding={source.binding_id} "
                f"previous={previous_open_ms} latest={latest_open_ms}"
            )
        pending_rows = elapsed_ms // interval_ms
        if pending_rows > self.max_catchup_rows:
            raise RuntimeError(
                f"stable BAR catch-up exceeds bound binding={source.binding_id} "
                f"required={pending_rows} max={self.max_catchup_rows}"
            )
        if pending_rows == 1:
            values = (latest,)
        elif acquisition.runtime == "BINANCE":
            values = fetch_binance_history(
                self._binance_binding(source),
                limit=pending_rows,
                now_ms=observed_ms,
                attempts=4,
                test_provenance=False,
            )
        else:
            values = asyncio.run(fetch_okx_history(
                self._okx_binding(source),
                limit=pending_rows,
                now_ms=observed_ms,
                test_provenance=False,
            ))

        opens = tuple(self._open_time_ms(acquisition, item) for item in values)
        expected = tuple(
            range(previous_open_ms + interval_ms, latest_open_ms + 1, interval_ms)
        )
        if opens != expected:
            raise RuntimeError(
                f"stable BAR catch-up is not contiguous binding={source.binding_id} "
                f"expected_rows={len(expected)} observed_rows={len(opens)}"
            )
        return tuple(zip(values, opens, strict=True))

    def _binding_is_due(self, source: StableSourceBinding, *, observed_ms: int) -> bool:
        """Return whether the exact currently closed BAR is still unacked."""
        newest_closed_open = self._expected_closed_open_ms(
            source, observed_ms=observed_ms
        )
        if newest_closed_open is None:
            return False
        previous_open = self._last_open_ms.get(source.binding_id)
        return previous_open is None or previous_open < newest_closed_open

    def _next_ready_at(self, now: float) -> float:
        """Wake for a due target retry or the next provider BAR boundary."""
        candidates = []
        observed_ms = int(now * 1000)
        for source, _acquisition in self.bindings + self.okx_bindings:
            interval_ms = _bar_interval_ms(source.interval or "")
            boundary_ms = _latest_source_closed_boundary_ms(
                source,
                observed_ms,
            )
            expected_open_ms = self._expected_closed_open_ms(
                source, observed_ms=observed_ms
            )
            previous_open_ms = self._last_open_ms.get(source.binding_id)
            if (
                expected_open_ms is not None
                and (previous_open_ms is None or previous_open_ms < expected_open_ms)
            ):
                retry_at = getattr(self, "_next_retry_at", {}).get(
                    source.binding_id, now
                )
                candidates.append(max(now, retry_at))
                continue
            if expected_open_ms is None:
                candidates.append(
                    boundary_ms / 1000 + self.settlement_delay_seconds
                )
            else:
                candidates.append(
                    (boundary_ms + interval_ms) / 1000
                    + self.settlement_delay_seconds
                )
        return min(candidates) if candidates else now + 60.0

    def _fetch_latest(
        self,
        source: StableSourceBinding,
        acquisition: StableAcquisitionBinding,
        *,
        observed_ms: int,
    ):
        """Read one provider-final BAR without hiding scheduler retry timing."""
        if acquisition.runtime == "BINANCE":
            return fetch_latest_closed_bar_raw_envelope(
                self._binance_binding(source),
                now_ms=observed_ms,
                attempts=1,
                test_provenance=False,
            )
        if acquisition.runtime == "OKX":
            return asyncio.run(fetch_okx_latest(
                self._okx_binding(source),
                now_ms=observed_ms,
                attempts=1,
                test_provenance=False,
            ))
        raise ValueError("stable crypto BAR runtime is unsupported")

    def run_cycle(self) -> int:
        if not self._rest_fallback_active:
            return 0
        observed_ms = self._settled_observed_ms()
        now = observed_ms / 1000
        due = tuple(
            (source, acquisition)
            for source, acquisition in self.bindings + self.okx_bindings
            if self._binding_is_due(source, observed_ms=observed_ms)
            and self._retry_is_due(source.binding_id, now=now)
        )
        if not due:
            return 0

        latest: dict[str, object] = {}
        workers = min(
            len(due), int(getattr(self, "max_concurrent_requests", 32))
        )
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="qdl-final-bar",
        ) as executor:
            futures = {
                executor.submit(
                    self._fetch_latest,
                    source,
                    acquisition,
                    observed_ms=observed_ms,
                ): (source, acquisition)
                for source, acquisition in due
            }
            for future in as_completed(futures):
                source, _acquisition = futures[future]
                try:
                    latest[source.binding_id] = future.result()
                except Exception as error:
                    self._schedule_retry(
                        source.binding_id,
                        now=now,
                        error=error,
                    )

        pending = []
        for source, acquisition in due:
            envelope = latest.get(source.binding_id)
            if envelope is None:
                continue
            try:
                values = self._pending_for_binding(
                    source, acquisition, envelope, observed_ms=observed_ms
                )
            except Exception as error:
                self._schedule_retry(
                    source.binding_id,
                    now=now,
                    error=error,
                )
                continue
            if values:
                pending.extend(
                    (source, item, open_time_ms)
                    for item, open_time_ms in values
                )
                continue
            expected_open_ms = self._expected_closed_open_ms(
                source, observed_ms=observed_ms
            )
            previous_open_ms = self._last_open_ms.get(source.binding_id)
            if (
                expected_open_ms is not None
                and (previous_open_ms is None or previous_open_ms < expected_open_ms)
            ):
                self._schedule_retry(source.binding_id, now=now)
        if not pending:
            return 0

        acknowledgements = self.publisher.publish_many(
            item for _source, item, _open_time_ms in pending
        )
        if len(acknowledgements) != len(pending):
            raise RuntimeError("stable latest-closed BAR cycle missed a Kafka ACK")

        acknowledged_opens: dict[str, int] = {}
        for source, _item, open_time_ms in pending:
            acknowledged_opens[source.binding_id] = max(
                open_time_ms, acknowledged_opens.get(source.binding_id, -1)
            )
        self._last_open_ms.update(acknowledged_opens)
        for binding_id in acknowledged_opens:
            self._clear_retry(binding_id)
        self._persist_state()
        logger.info(
            "stable multi-venue closed BAR ACK count=%s bindings=%s",
            len(acknowledgements),
            ",".join(sorted(acknowledged_opens)),
        )
        return len(acknowledgements)

    def _loop_sleep_seconds(self, now: float) -> float:
        if not self._rest_fallback_active:
            return 60.0
        # The configured 100ms first poll/retry cannot work when the outer
        # scheduler imposes a larger arbitrary sleep floor.
        return max(0.01, self._next_ready_at(now) - now)

    def run_forever(self) -> None:
        if not self._history_bootstrap_active:
            logger.info("stable crypto BAR edge idle; no enabled crypto BAR demand")
            while not self._stopped.wait(60.0):
                pass
            return
        failures = 0
        while not self._stopped.is_set():
            try:
                # Bootstrap is a bounded latest-closed history read.  It must
                # run immediately after process start; `_next_ready_at()` is
                # deliberately a recurring-poll scheduler and moves a settled
                # boundary to the next interval.  Using it here would defer an
                # empty checkpoint forever at every boundary.
                self.bootstrap_history()
                if self._rest_fallback_active:
                    self.run_cycle()
                failures = 0
            except Exception:
                failures += 1
                logger.exception(
                    "stable crypto BAR cycle failed consecutive_failures=%s", failures
                )
            delay = self._loop_sleep_seconds(self.clock())
            if failures:
                delay = min(delay, min(2 ** min(failures, 6), 30))
            self._stopped.wait(delay)

    def stop(self, *_args) -> None:
        self._stopped.set()
        self.publisher.close()


def build_from_environment() -> StableBinanceBarEdge:
    catalog = StableSourceCatalog.load(os.environ["QDL_STABLE_SOURCE_BINDINGS"])
    acquisition = StableAcquisitionPlan.load(
        os.environ["QDL_STABLE_ACQUISITION_BINDINGS"], catalog=catalog
    )
    runtime_dir = Path(os.environ["QDL_STABLE_RUNTIME_DIR"])
    authority = json.loads((runtime_dir / "authority.json").read_text(encoding="utf-8"))
    cert_root = Path(os.environ["QDL_KAFKA_CERT_ROOT"])
    publisher = KafkaRawPublisher(KafkaRawPublisherConfig(
        bootstrap_servers=os.environ["QDL_KAFKA_BOOTSTRAP_SERVERS"],
        client_id=os.environ["QDL_KAFKA_CLIENT_ID"],
        topic=acquisition.raw_topic,
        ca_path=cert_root / "ca.crt",
        certificate_path=cert_root / "client.crt",
        key_path=cert_root / "client.key",
    ))
    canonical_cache_path = Path(os.environ.get(
        "QDL_STABLE_CANONICAL_CACHE_PATH",
        str(
            Path(
                os.environ.get(
                    "QDL_STABLE_DURABLE_STATE_DIR",
                    "/var/lib/qdl-stable/shared",
                )
            )
            / "canonical-cache.sqlite3"
        ),
    ))
    return StableBinanceBarEdge(
        catalog=catalog,
        acquisition=acquisition,
        authority=authority,
        publisher=publisher,
        warmup_rows=int(os.environ.get("QDL_STABLE_BAR_WARMUP_ROWS", "500")),
        max_catchup_rows=int(
            os.environ.get("QDL_STABLE_BAR_MAX_CATCHUP_ROWS", "1000")
        ),
        settlement_delay_seconds=float(
            os.environ.get("QDL_STABLE_BAR_SETTLEMENT_DELAY_SECONDS", "0.10")
        ),
        final_retry_initial_seconds=float(
            os.environ.get("QDL_STABLE_BAR_RETRY_INITIAL_SECONDS", "0.10")
        ),
        final_retry_max_seconds=float(
            os.environ.get("QDL_STABLE_BAR_RETRY_MAX_SECONDS", "1.0")
        ),
        max_concurrent_requests=int(
            os.environ.get("QDL_STABLE_BAR_MAX_CONCURRENT_REQUESTS", "32")
        ),
        state_path=os.environ.get(
            "QDL_STABLE_BAR_STATE_PATH",
            str(
                Path(
                    os.environ.get(
                        "QDL_STABLE_STATE_DIR",
                        "/var/lib/qdl-stable/runtime",
                    )
                )
                / "stable-crypto-bar-edge.json"
            ),
        ),
        canonical_cache_id=_canonical_cache_id(canonical_cache_path),
        canonical_cache_path=canonical_cache_path,
    )


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    edge = build_from_environment()
    signal.signal(signal.SIGTERM, edge.stop)
    signal.signal(signal.SIGINT, edge.stop)
    try:
        edge.run_forever()
    finally:
        if not edge._stopped.is_set():
            edge.stop()
    return 0
