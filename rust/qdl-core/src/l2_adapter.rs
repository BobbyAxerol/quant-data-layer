//! Venue-edge parsers for the provider-neutral L2 state core.
//!
//! This module owns documented wire semantics only.  It opens no socket,
//! performs no REST request, writes no durable record and makes no execution
//! decision.  A runtime supplies scoped raw bytes, a connection generation and
//! its receipt time; the adapter returns a fail-closed transition for the
//! shared `L2BookCore`.

use std::collections::VecDeque;

use serde_json::Value;

use crate::l2_book::{
    BookConfig, BookDelta, BookError, BookIdentity, BookLevelInput, BookOutcome, BookSide,
    BookSnapshot, BookStatus, BookView, ChecksumEvidence, ChecksumPolicy, L2BookCore,
    SequencePolicy, SnapshotAdmissionPolicy, SnapshotOrigin,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderBookProtocol {
    BinanceUsdmDiffDepth,
    OkxPublicBooks,
}

/// Decides whether a transition becomes a durable, consumer-visible canonical
/// event.  Bootstrap, duplicate and failed transitions remain observable to
/// quality telemetry only; a consumer cannot reconstruct a book from them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookPublication {
    None,
    Snapshot,
    Delta,
}

/// A single transition is safe to expose only when `view` is present.  A
/// caller must retain the matching snapshot plus lossless deltas to replay a
/// complete book; this object never converts an unverified bootstrap into an
/// execution-grade view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookTransition {
    pub kind: BookTransitionKind,
    pub publication: BookPublication,
    pub outcome: BookOutcome,
    pub status: BookStatus,
    pub generation: u64,
    pub snapshot_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub observed_at_ms: Option<i64>,
    pub native_sequence_start: Option<u64>,
    pub previous_sequence: Option<u64>,
    pub updates: Vec<BookLevelInput>,
    pub view: Option<BookView>,
    /// Set only when the runtime emits a periodic materialized snapshot from
    /// an already verified current view. It keeps native sequence semantics
    /// intact while making the periodic observation idempotency-distinct.
    pub materialized_snapshot_at_ms: Option<i64>,
}

impl BookTransition {
    pub fn sequence_verified(&self) -> bool {
        self.view.is_some() && self.status == BookStatus::Ready
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookTransitionKind {
    Snapshot,
    Delta,
    Lifecycle,
}

/// Provider-specific protocol normalizer around one provider-neutral core
/// identity.  It is intentionally one instance per demanded subscription,
/// never a process/container per symbol.
#[derive(Debug)]
pub struct L2BookAdapter {
    protocol: ProviderBookProtocol,
    native_symbol: String,
    identity: BookIdentity,
    core: L2BookCore,
    bootstrap_buffer_capacity: usize,
    buffered_binance_deltas: VecDeque<BufferedBinanceDelta>,
}

const DEFAULT_BINANCE_BOOTSTRAP_BUFFER_CAPACITY: usize = 2_048;

#[derive(Clone, Debug)]
struct BufferedBinanceDelta {
    generation: u64,
    observed_at_ms: Option<i64>,
    sequence_start: u64,
    previous_sequence: Option<u64>,
    sequence_end: u64,
    updates: Vec<BookLevelInput>,
}

impl L2BookAdapter {
    pub fn binance_diff_depth(
        identity: BookIdentity,
        native_symbol: impl Into<String>,
        view_depth_per_side: usize,
    ) -> Result<Self, BookError> {
        let config = BookConfig::new(
            identity,
            SequencePolicy::RangeBridgeThenPrevious,
            ChecksumPolicy::Ignore,
            view_depth_per_side,
        )?
        .with_snapshot_admission_policy(SnapshotAdmissionPolicy::RestBootstrapAllowed)?;
        Self::new(
            ProviderBookProtocol::BinanceUsdmDiffDepth,
            native_symbol,
            config,
        )
    }

    /// Backward-compatible spelling for the active USD-M provider profile.
    /// The protocol itself is shared with any documented Binance diff-depth
    /// venue; identity keeps the provider/market distinct.
    pub fn binance_usdm_diff_depth(
        identity: BookIdentity,
        native_symbol: impl Into<String>,
        view_depth_per_side: usize,
    ) -> Result<Self, BookError> {
        Self::binance_diff_depth(identity, native_symbol, view_depth_per_side)
    }

    pub fn okx_public_books(
        identity: BookIdentity,
        native_symbol: impl Into<String>,
        view_depth_per_side: usize,
    ) -> Result<Self, BookError> {
        let config = BookConfig::new(
            identity,
            SequencePolicy::PreviousSequence,
            // The normal public `books` checksum is currently documented as
            // deprecated/fixed.  Sequence continuity is the integrity proof.
            ChecksumPolicy::Ignore,
            view_depth_per_side,
        )?;
        Self::new(ProviderBookProtocol::OkxPublicBooks, native_symbol, config)
    }

    fn new(
        protocol: ProviderBookProtocol,
        native_symbol: impl Into<String>,
        config: BookConfig,
    ) -> Result<Self, BookError> {
        let native_symbol = native_symbol.into();
        if native_symbol.trim().is_empty() {
            return Err(BookError::InvalidIdentity("native_symbol"));
        }
        Ok(Self {
            protocol,
            native_symbol,
            identity: config.identity.clone(),
            core: L2BookCore::new(config),
            bootstrap_buffer_capacity: DEFAULT_BINANCE_BOOTSTRAP_BUFFER_CAPACITY,
            buffered_binance_deltas: VecDeque::new(),
        })
    }

    /// Bound only the short Binance race window between opening a diff-depth
    /// socket and receiving its REST snapshot. Overflow fails closed and
    /// forces a resync rather than silently dropping a delta.
    pub fn with_binance_bootstrap_buffer_capacity(
        mut self,
        capacity: usize,
    ) -> Result<Self, BookError> {
        if capacity == 0 {
            return Err(BookError::InvalidIdentity("bootstrap_buffer_capacity"));
        }
        self.bootstrap_buffer_capacity = capacity;
        Ok(self)
    }

    pub fn protocol(&self) -> ProviderBookProtocol {
        self.protocol
    }

    pub fn core(&self) -> &L2BookCore {
        &self.core
    }

    /// Apply the response to the already scoped Binance depth request. Deltas
    /// received before it are kept in a bounded in-memory race buffer and are
    /// replayed in provider arrival order. A verified bridge publishes one
    /// complete snapshot, never a delta without its matching snapshot anchor.
    pub fn apply_binance_rest_snapshot(
        &mut self,
        raw: &Value,
        generation: u64,
        received_at_ms: i64,
    ) -> Result<BookTransition, String> {
        self.require_protocol(ProviderBookProtocol::BinanceUsdmDiffDepth)?;
        positive_time(received_at_ms, "received_at_ms")?;
        validate_optional_symbol(raw, "symbol", &self.native_symbol)?;
        let sequence_end = unsigned(raw, "lastUpdateId")?;
        let levels = side_levels(raw, "bids", BookSide::Bid, None)?
            .into_iter()
            .chain(side_levels(raw, "asks", BookSide::Ask, None)?)
            .collect();
        let observed_at_ms = optional_time(raw, &["E", "T"])?.or(Some(received_at_ms));

        // A periodic REST read is a recovery anchor, not an instruction to
        // overwrite a continuous websocket book. Resetting a READY book with
        // a snapshot captured while deltas are still arriving loses the bridge
        // window and can manufacture a false sequence gap. We still parse the
        // snapshot strictly so malformed provider data is never hidden.
        if generation == self.core.generation() && self.core.status() == BookStatus::Ready {
            return Ok(self.transition(
                BookTransitionKind::Snapshot,
                BookPublication::None,
                BookOutcome::Keepalive,
                observed_at_ms,
                None,
                None,
                vec![],
            ));
        }
        let outcome = self.core.apply_snapshot(&BookSnapshot {
            identity: self.core_identity(),
            generation,
            sequence_end,
            checksum: ChecksumEvidence::NotProvided,
            origin: SnapshotOrigin::Rest,
            levels,
        });
        if outcome != BookOutcome::BootstrapApplied {
            return Ok(self.transition(
                BookTransitionKind::Snapshot,
                BookPublication::None,
                outcome,
                observed_at_ms,
                None,
                None,
                vec![],
            ));
        }
        self.replay_binance_bootstrap(generation, observed_at_ms)
    }

    pub fn apply_binance_ws_delta(
        &mut self,
        raw: &Value,
        generation: u64,
    ) -> Result<BookTransition, String> {
        self.require_protocol(ProviderBookProtocol::BinanceUsdmDiffDepth)?;
        validate_symbol(raw, "s", &self.native_symbol)?;
        let delta = self.parse_binance_ws_delta(raw, generation)?;
        if generation > self.core.generation()
            || matches!(
                self.core.status(),
                BookStatus::AwaitingSnapshot | BookStatus::Resyncing
            )
        {
            self.buffer_binance_delta(delta)?;
            return Ok(self.transition(
                BookTransitionKind::Delta,
                BookPublication::None,
                BookOutcome::BufferedAwaitingBootstrap,
                optional_time(raw, &["E", "T"])?,
                None,
                None,
                vec![],
            ));
        }
        self.apply_binance_delta(delta)
    }

    /// Apply one complete OKX public `books` websocket payload.  REST `/books`
    /// is intentionally not accepted on this path because it cannot establish
    /// continuity for a stateful update stream.
    pub fn apply_okx_ws_frame(
        &mut self,
        raw: &Value,
        generation: u64,
    ) -> Result<BookTransition, String> {
        self.require_protocol(ProviderBookProtocol::OkxPublicBooks)?;
        let argument = object(raw, "arg")?;
        if text(argument, "channel")? != "books" {
            return Err("OKX book channel must be books".into());
        }
        validate_symbol_object(argument, "instId", &self.native_symbol)?;
        let action = text_value(raw.get("action"), "action")?;
        let rows = array(raw, "data")?;
        if rows.len() != 1 {
            return Err("OKX books payload must contain exactly one data row".into());
        }
        let row = rows[0]
            .as_object()
            .ok_or_else(|| "OKX books data row must be an object".to_owned())?;
        let sequence_end = unsigned_object(row, "seqId")?;
        let observed_at_ms = optional_time_object(row, &["ts"])?;
        let levels = side_levels_object(row, "bids", BookSide::Bid, Some(3))?
            .into_iter()
            .chain(side_levels_object(row, "asks", BookSide::Ask, Some(3))?)
            .collect();
        let (kind, publication, outcome, native_sequence_start, previous_sequence, updates) =
            match action.as_str() {
                "snapshot" => (
                    BookTransitionKind::Snapshot,
                    BookPublication::None,
                    self.core.apply_snapshot(&BookSnapshot {
                        identity: self.core_identity(),
                        generation,
                        sequence_end,
                        checksum: ChecksumEvidence::NotProvided,
                        origin: SnapshotOrigin::WebSocket,
                        levels,
                    }),
                    None,
                    None,
                    vec![],
                ),
                "update" => {
                    let previous_sequence = unsigned_object(row, "prevSeqId")?;
                    let was_ready = self.core.status() == BookStatus::Ready;
                    let outcome = self.core.apply_delta(&BookDelta {
                        identity: self.core_identity(),
                        generation,
                        sequence_start: None,
                        previous_sequence: Some(previous_sequence),
                        sequence_end,
                        checksum: ChecksumEvidence::NotProvided,
                        updates: levels.clone(),
                    });
                    (
                        BookTransitionKind::Delta,
                        if outcome == BookOutcome::DeltaApplied && !was_ready {
                            BookPublication::Snapshot
                        } else if outcome == BookOutcome::DeltaApplied {
                            BookPublication::Delta
                        } else {
                            BookPublication::None
                        },
                        outcome,
                        Some(previous_sequence),
                        Some(previous_sequence),
                        levels,
                    )
                }
                _ => return Err("OKX books action must be snapshot or update".into()),
            };
        Ok(self.transition(
            kind,
            if outcome == BookOutcome::SnapshotApplied {
                BookPublication::Snapshot
            } else {
                publication
            },
            outcome,
            observed_at_ms,
            native_sequence_start,
            previous_sequence,
            updates,
        ))
    }

    pub fn request_resync(&mut self, generation: u64) -> BookTransition {
        let outcome = self.core.request_resync(generation);
        self.buffered_binance_deltas.clear();
        self.transition(
            BookTransitionKind::Lifecycle,
            BookPublication::None,
            outcome,
            None,
            None,
            None,
            vec![],
        )
    }

    pub fn disconnect(&mut self) -> BookTransition {
        let outcome = self.core.disconnect();
        self.buffered_binance_deltas.clear();
        self.transition(
            BookTransitionKind::Lifecycle,
            BookPublication::None,
            outcome,
            None,
            None,
            None,
            vec![],
        )
    }

    fn require_protocol(&self, expected: ProviderBookProtocol) -> Result<(), String> {
        if self.protocol != expected {
            return Err("provider book adapter protocol mismatch".into());
        }
        Ok(())
    }

    fn core_identity(&self) -> BookIdentity {
        // The adapter retains the same immutable validated identity passed to
        // the core. Raw frames are checked against `native_symbol` before this
        // identity can be used, preventing a mutable symbol lookup from
        // cross-mixing books.
        self.identity.clone()
    }

    fn transition(
        &self,
        kind: BookTransitionKind,
        publication: BookPublication,
        outcome: BookOutcome,
        observed_at_ms: Option<i64>,
        native_sequence_start: Option<u64>,
        previous_sequence: Option<u64>,
        updates: Vec<BookLevelInput>,
    ) -> BookTransition {
        BookTransition {
            kind,
            publication,
            outcome,
            status: self.core.status(),
            generation: self.core.generation(),
            snapshot_sequence: self.core.snapshot_sequence(),
            last_sequence: self.core.last_sequence(),
            observed_at_ms,
            native_sequence_start,
            previous_sequence,
            updates,
            view: self.core.view(),
            materialized_snapshot_at_ms: None,
        }
    }

    fn parse_binance_ws_delta(
        &self,
        raw: &Value,
        generation: u64,
    ) -> Result<BufferedBinanceDelta, String> {
        validate_symbol(raw, "s", &self.native_symbol)?;
        let sequence_start = unsigned(raw, "U")?;
        let sequence_end = unsigned(raw, "u")?;
        if sequence_start > sequence_end {
            return Err("Binance depth update sequence range is invalid".into());
        }
        Ok(BufferedBinanceDelta {
            generation,
            observed_at_ms: optional_time(raw, &["E", "T"])?,
            sequence_start,
            previous_sequence: optional_unsigned(raw, "pu")?,
            sequence_end,
            updates: side_levels(raw, "b", BookSide::Bid, None)?
                .into_iter()
                .chain(side_levels(raw, "a", BookSide::Ask, None)?)
                .collect(),
        })
    }

    fn buffer_binance_delta(&mut self, delta: BufferedBinanceDelta) -> Result<(), String> {
        if delta.generation < self.core.generation() {
            return Ok(());
        }
        if delta.generation > self.core.generation() {
            self.core.request_resync(delta.generation);
            self.buffered_binance_deltas.clear();
        }
        if self.buffered_binance_deltas.len() >= self.bootstrap_buffer_capacity {
            self.core.request_resync(delta.generation);
            self.buffered_binance_deltas.clear();
            return Err("Binance depth bootstrap buffer overflow; resync required".into());
        }
        self.buffered_binance_deltas.push_back(delta);
        Ok(())
    }

    fn replay_binance_bootstrap(
        &mut self,
        generation: u64,
        bootstrap_observed_at_ms: Option<i64>,
    ) -> Result<BookTransition, String> {
        let snapshot_sequence = self
            .core
            .snapshot_sequence()
            .ok_or_else(|| "Binance bootstrap snapshot has no sequence".to_owned())?;
        let mut final_observed_at_ms = bootstrap_observed_at_ms;
        let mut became_ready = false;
        while let Some(delta) = self.buffered_binance_deltas.pop_front() {
            if delta.generation != generation || delta.sequence_end <= snapshot_sequence {
                continue;
            }
            let was_ready = self.core.status() == BookStatus::Ready;
            let outcome = self.apply_delta_to_core(&delta);
            final_observed_at_ms = delta.observed_at_ms.or(final_observed_at_ms);
            match outcome {
                BookOutcome::DeltaApplied => {
                    became_ready =
                        became_ready || (!was_ready && self.core.status() == BookStatus::Ready);
                }
                BookOutcome::Duplicate | BookOutcome::Keepalive => {}
                _ => {
                    self.buffered_binance_deltas.clear();
                    return Ok(self.transition(
                        BookTransitionKind::Snapshot,
                        BookPublication::None,
                        outcome,
                        final_observed_at_ms,
                        None,
                        None,
                        vec![],
                    ));
                }
            }
        }
        Ok(self.transition(
            BookTransitionKind::Snapshot,
            if became_ready {
                BookPublication::Snapshot
            } else {
                BookPublication::None
            },
            if became_ready {
                BookOutcome::SnapshotApplied
            } else {
                BookOutcome::BootstrapApplied
            },
            final_observed_at_ms,
            None,
            None,
            vec![],
        ))
    }

    fn apply_binance_delta(
        &mut self,
        delta: BufferedBinanceDelta,
    ) -> Result<BookTransition, String> {
        let was_ready = self.core.status() == BookStatus::Ready;
        let outcome = self.apply_delta_to_core(&delta);
        let publication = if outcome == BookOutcome::DeltaApplied && !was_ready {
            BookPublication::Snapshot
        } else if outcome == BookOutcome::DeltaApplied {
            BookPublication::Delta
        } else {
            BookPublication::None
        };
        Ok(self.transition(
            BookTransitionKind::Delta,
            publication,
            outcome,
            delta.observed_at_ms,
            Some(delta.sequence_start),
            delta.previous_sequence,
            delta.updates,
        ))
    }

    fn apply_delta_to_core(&mut self, delta: &BufferedBinanceDelta) -> BookOutcome {
        self.core.apply_delta(&BookDelta {
            identity: self.core_identity(),
            generation: delta.generation,
            sequence_start: Some(delta.sequence_start),
            previous_sequence: delta.previous_sequence,
            sequence_end: delta.sequence_end,
            checksum: ChecksumEvidence::NotProvided,
            updates: delta.updates.clone(),
        })
    }
}

fn positive_time(value: i64, field: &str) -> Result<(), String> {
    if value <= 0 {
        return Err(format!("{field} must be positive"));
    }
    Ok(())
}

fn object<'a>(raw: &'a Value, field: &str) -> Result<&'a serde_json::Map<String, Value>, String> {
    raw.get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| format!("book frame object field is missing: {field}"))
}

fn array<'a>(raw: &'a Value, field: &str) -> Result<&'a Vec<Value>, String> {
    raw.get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("book frame array field is missing: {field}"))
}

fn text(raw: &serde_json::Map<String, Value>, field: &str) -> Result<String, String> {
    text_value(raw.get(field), field)
}

fn text_value(value: Option<&Value>, field: &str) -> Result<String, String> {
    match value {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        _ => Err(format!("book frame scalar field is missing: {field}")),
    }
}

fn unsigned(raw: &Value, field: &str) -> Result<u64, String> {
    text_value(raw.get(field), field)?
        .parse::<u64>()
        .map_err(|_| format!("book frame unsigned field is invalid: {field}"))
}

fn unsigned_object(raw: &serde_json::Map<String, Value>, field: &str) -> Result<u64, String> {
    text(raw, field)?
        .parse::<u64>()
        .map_err(|_| format!("book frame unsigned field is invalid: {field}"))
}

fn optional_unsigned(raw: &Value, field: &str) -> Result<Option<u64>, String> {
    match raw.get(field) {
        None | Some(Value::Null) => Ok(None),
        value => text_value(value, field)?
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("book frame unsigned field is invalid: {field}")),
    }
}

fn optional_time(raw: &Value, fields: &[&str]) -> Result<Option<i64>, String> {
    for field in fields {
        if raw.get(*field).is_some() {
            return text_value(raw.get(*field), field)?
                .parse::<i64>()
                .map(Some)
                .map_err(|_| format!("book frame timestamp is invalid: {field}"));
        }
    }
    Ok(None)
}

fn optional_time_object(
    raw: &serde_json::Map<String, Value>,
    fields: &[&str],
) -> Result<Option<i64>, String> {
    for field in fields {
        if raw.get(*field).is_some() {
            return text(raw, field)?
                .parse::<i64>()
                .map(Some)
                .map_err(|_| format!("book frame timestamp is invalid: {field}"));
        }
    }
    Ok(None)
}

fn validate_symbol(raw: &Value, field: &str, expected: &str) -> Result<(), String> {
    if text_value(raw.get(field), field)? != expected {
        return Err("book frame native symbol mismatch".into());
    }
    Ok(())
}

fn validate_symbol_object(
    raw: &serde_json::Map<String, Value>,
    field: &str,
    expected: &str,
) -> Result<(), String> {
    if text(raw, field)? != expected {
        return Err("book frame native symbol mismatch".into());
    }
    Ok(())
}

fn validate_optional_symbol(raw: &Value, field: &str, expected: &str) -> Result<(), String> {
    match raw.get(field) {
        None | Some(Value::Null) => Ok(()),
        _ => validate_symbol(raw, field, expected),
    }
}

fn side_levels(
    raw: &Value,
    field: &str,
    side: BookSide,
    order_count_index: Option<usize>,
) -> Result<Vec<BookLevelInput>, String> {
    side_levels_values(array(raw, field)?, side, order_count_index, field)
}

fn side_levels_object(
    raw: &serde_json::Map<String, Value>,
    field: &str,
    side: BookSide,
    order_count_index: Option<usize>,
) -> Result<Vec<BookLevelInput>, String> {
    let values = raw
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("book frame array field is missing: {field}"))?;
    side_levels_values(values, side, order_count_index, field)
}

fn side_levels_values(
    values: &[Value],
    side: BookSide,
    order_count_index: Option<usize>,
    field: &str,
) -> Result<Vec<BookLevelInput>, String> {
    values
        .iter()
        .map(|row| {
            let values = row
                .as_array()
                .filter(|values| values.len() >= 2)
                .ok_or_else(|| format!("book level requires price and quantity: {field}"))?;
            let order_count = order_count_index
                .and_then(|index| values.get(index))
                .map(|value| text_value(Some(value), "order_count"))
                .transpose()?
                .map(|value| {
                    value
                        .parse::<u64>()
                        .map_err(|_| "book level order count is invalid".to_owned())
                })
                .transpose()?;
            Ok(BookLevelInput {
                side,
                price: text_value(values.first(), "price")?,
                quantity: text_value(values.get(1), "quantity")?,
                order_count,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{BookIdentity, BookOutcome, BookPublication, BookStatus, L2BookAdapter};

    fn identity(provider: &str, symbol: &str, channel: &str) -> BookIdentity {
        BookIdentity::new(provider, format!("uid-{symbol}"), channel).unwrap()
    }

    #[test]
    fn binance_rest_bootstrap_is_unreadable_until_ws_range_bridges() {
        let mut adapter = L2BookAdapter::binance_usdm_diff_depth(
            identity("BINANCE_USDM_DIFF_DEPTH", "BTCUSDT", "depth"),
            "BTCUSDT",
            2,
        )
        .unwrap();
        let bootstrap = adapter
            .apply_binance_rest_snapshot(
                &json!({"lastUpdateId": 100, "bids": [["10", "2"]], "asks": [["11", "3"]]}),
                4,
                1_000,
            )
            .unwrap();
        assert_eq!(bootstrap.outcome, BookOutcome::BootstrapApplied);
        assert_eq!(bootstrap.status, BookStatus::Bootstrapping);
        assert!(!bootstrap.sequence_verified());
        assert!(bootstrap.view.is_none());
        assert_eq!(bootstrap.publication, BookPublication::None);

        let ready = adapter
            .apply_binance_ws_delta(
                &json!({"s":"BTCUSDT","U":99,"u":101,"pu":98,"E":1001,"b":[["10","4"]],"a":[["11","0"]]}),
                4,
            )
            .unwrap();
        assert_eq!(ready.outcome, BookOutcome::DeltaApplied);
        assert!(ready.sequence_verified());
        assert_eq!(ready.snapshot_sequence, Some(100));
        assert_eq!(ready.last_sequence, Some(101));
        assert_eq!(ready.publication, BookPublication::Snapshot);
        let view = ready.view.unwrap();
        assert_eq!(view.bids[0].quantity.canonical_text(), "4");
        assert!(view.asks.is_empty());
    }

    #[test]
    fn binance_gap_and_cross_symbol_fail_closed() {
        let mut adapter = L2BookAdapter::binance_usdm_diff_depth(
            identity("BINANCE_USDM_DIFF_DEPTH", "ETHUSDT", "depth"),
            "ETHUSDT",
            5,
        )
        .unwrap();
        adapter
            .apply_binance_rest_snapshot(
                &json!({"lastUpdateId": 20, "bids": [["10", "1"]], "asks": [["11", "1"]]}),
                1,
                1,
            )
            .unwrap();
        let gap = adapter
            .apply_binance_ws_delta(
                &json!({"s":"ETHUSDT","U":22,"u":22,"pu":20,"E":2,"b":[],"a":[]}),
                1,
            )
            .unwrap();
        assert_eq!(gap.outcome, BookOutcome::SequenceGap);
        assert_eq!(gap.status, BookStatus::Gapped);
        assert!(adapter
            .apply_binance_ws_delta(&json!({"s":"BTCUSDT","U":21,"u":21,"E":3,"b":[],"a":[]}), 1,)
            .is_err());
    }

    #[test]
    fn binance_ready_refresh_keeps_verified_book_and_post_gap_snapshot_recovers() {
        let mut adapter = L2BookAdapter::binance_usdm_diff_depth(
            identity("BINANCE_USDM_DIFF_DEPTH", "BTCUSDT", "depth"),
            "BTCUSDT",
            2,
        )
        .unwrap();
        adapter
            .apply_binance_rest_snapshot(
                &json!({"lastUpdateId": 100, "bids": [["10", "1"]], "asks": [["11", "1"]]}),
                1,
                1,
            )
            .unwrap();
        let ready = adapter
            .apply_binance_ws_delta(
                &json!({"s":"BTCUSDT","U":100,"u":101,"pu":99,"E":2,"b":[["10","2"]],"a":[]}),
                1,
            )
            .unwrap();
        assert_eq!(ready.outcome, BookOutcome::DeltaApplied);
        let refresh = adapter
            .apply_binance_rest_snapshot(
                &json!({"lastUpdateId": 99, "bids": [["10", "99"]], "asks": [["11", "99"]]}),
                1,
                3,
            )
            .unwrap();
        assert_eq!(refresh.outcome, BookOutcome::Keepalive);
        assert_eq!(refresh.status, BookStatus::Ready);
        assert_eq!(refresh.snapshot_sequence, Some(100));
        assert_eq!(refresh.last_sequence, Some(101));
        assert_eq!(refresh.view.unwrap().bids[0].quantity.canonical_text(), "2");
        assert!(adapter
            .apply_binance_rest_snapshot(&json!({"bids": [], "asks": []}), 1, 4)
            .is_err());

        let gap = adapter
            .apply_binance_ws_delta(
                &json!({"s":"BTCUSDT","U":103,"u":103,"pu":102,"E":5,"b":[],"a":[]}),
                1,
            )
            .unwrap();
        assert_eq!(gap.outcome, BookOutcome::SequenceGap);
        adapter.request_resync(1);
        let buffered = adapter
            .apply_binance_ws_delta(
                &json!({"s":"BTCUSDT","U":104,"u":104,"pu":103,"E":6,"b":[["10","3"]],"a":[]}),
                1,
            )
            .unwrap();
        assert_eq!(buffered.outcome, BookOutcome::BufferedAwaitingBootstrap);
        let recovered = adapter
            .apply_binance_rest_snapshot(
                &json!({"lastUpdateId": 103, "bids": [["10", "2"]], "asks": [["11", "1"]]}),
                1,
                7,
            )
            .unwrap();
        assert_eq!(recovered.outcome, BookOutcome::SnapshotApplied);
        assert!(recovered.sequence_verified());
        assert_eq!(recovered.last_sequence, Some(104));
    }

    #[test]
    fn okx_accepts_ws_snapshot_chains_update_and_rejects_bad_action() {
        let mut adapter = L2BookAdapter::okx_public_books(
            identity("OKX_PUBLIC_V5_SWAP", "BTC-USDT-SWAP", "books"),
            "BTC-USDT-SWAP",
            2,
        )
        .unwrap();
        let snapshot = adapter
            .apply_okx_ws_frame(
                &json!({"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[{"seqId":"30","prevSeqId":"-1","ts":"10","bids":[["100","1","0","2"]],"asks":[["101","2","0","3"]]}]}),
                7,
            )
            .unwrap();
        assert!(snapshot.sequence_verified());
        let update = adapter
            .apply_okx_ws_frame(
                &json!({"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update","data":[{"seqId":"31","prevSeqId":"30","ts":"11","bids":[["100","2","0","4"]],"asks":[]}]}),
                7,
            )
            .unwrap();
        assert_eq!(update.outcome, BookOutcome::DeltaApplied);
        assert_eq!(update.publication, BookPublication::Delta);
        assert_eq!(update.view.unwrap().bids[0].quantity.canonical_text(), "2");
        assert!(adapter
            .apply_okx_ws_frame(
                &json!({"arg":{"channel":"books5","instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[]}),
                7,
            )
            .is_err());
    }

    #[test]
    fn binance_pre_snapshot_buffer_is_bounded_and_resyncs_without_lossy_publish() {
        let mut adapter = L2BookAdapter::binance_diff_depth(
            identity("BINANCE_USDM_DIFF_DEPTH", "BTCUSDT", "depth"),
            "BTCUSDT",
            2,
        )
        .unwrap()
        .with_binance_bootstrap_buffer_capacity(1)
        .unwrap();
        let first = adapter
            .apply_binance_ws_delta(&json!({"s":"BTCUSDT","U":11,"u":11,"E":1,"b":[],"a":[]}), 2)
            .unwrap();
        assert_eq!(first.outcome, BookOutcome::BufferedAwaitingBootstrap);
        assert_eq!(first.publication, BookPublication::None);
        let error = adapter
            .apply_binance_ws_delta(&json!({"s":"BTCUSDT","U":12,"u":12,"E":2,"b":[],"a":[]}), 2)
            .expect_err("overflow must force an explicit resync");
        assert!(error.contains("overflow"));
        assert_eq!(adapter.core().status(), BookStatus::Resyncing);
        assert!(adapter.core().view().is_none());
    }
}
