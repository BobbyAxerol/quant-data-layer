//! Shared committed reader and indexed fan-out (KN-2 K2.2, D2/D3).
//!
//! One reader per replica reads every partition of the canonical topic
//! (`read_committed`, manual assignment, never commits) and dispatches each
//! record under **that partition's** lock only: there is no global writer
//! lock. Subscribers are indexed by physical key; a record is decoded once for
//! all of them and shared (`Arc`). Each partition keeps a ring of recent
//! records bounded by bytes and age.
//!
//! Replay-to-live (D3): [`Hub::register`] runs under the partition lock, so
//! the returned barrier (the next undispatched offset) splits the log exactly:
//! every record below it was dispatched before the subscriber existed and is
//! replayed (from the ring when it still covers the range, else by a separate
//! bounded reader), every record at or above it goes to the subscriber's
//! queue. The live reader is never sought.

use crate::generated::marketdata_v2::EventEnvelope;
use crate::subscription::Subscription;
use prost::Message as _;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Accounting overhead per queued/ringed record beyond key and payload.
pub const RECORD_OVERHEAD: usize = 96;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawRecord {
    pub partition: i32,
    pub offset: i64,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    /// Kafka record timestamp (producer create time), 0 when unknown.
    pub timestamp_ms: i64,
}

impl RawRecord {
    pub fn weight(&self) -> usize {
        self.key.len() + self.payload.len() + RECORD_OVERHEAD
    }
}

pub type SharedRecord = Arc<RawRecord>;

#[derive(Debug)]
pub struct HotRecord {
    pub raw: SharedRecord,
    /// Captured under the same partition lock as the selected row. The cursor
    /// may cover later records of OTHER products and transaction markers.
    pub boundary_offset: i64,
}

/// A record decoded once and shared by every subscriber of its key.
#[derive(Debug)]
pub struct LiveRecord {
    pub raw: SharedRecord,
    pub envelope: EventEnvelope,
}

/// Empirical decoded-size allowance per payload byte: measured
/// 2.25 (BAR) to 4.8 (BOOK_SNAPSHOT) on average and 7.28 at most over 54,970
/// real canonical records (K2-T08 capture, counting allocator). Anything
/// that holds a decoded record is charged `raw + DECODED_FACTOR x payload`.
/// This is not a bound for arbitrary protobuf shapes; new product/depth
/// profiles need RSS/decode sizing before promotion (KN contract section 7).
pub const DECODED_FACTOR: usize = 8;

/// [`LiveRecord::weight`] of a raw record, before it is decoded.
pub fn live_weight(raw: &RawRecord) -> usize {
    raw.weight() + DECODED_FACTOR * raw.payload.len()
}

impl LiveRecord {
    /// Bytes charged for holding this record (raw + decoded).
    pub fn weight(&self) -> usize {
        live_weight(&self.raw)
    }

    pub fn decode(raw: SharedRecord) -> Result<Arc<Self>, String> {
        let envelope = EventEnvelope::decode(raw.payload.as_slice())
            .map_err(|error| format!("canonical record failed to decode: {error}"))?;
        Ok(Arc::new(Self { raw, envelope }))
    }
}

/// The committed log as the live reader sees it (Kafka in production, an
/// in-memory log in tests). Blocking; driven by the hub's reader thread.
pub trait LogSource: Send {
    /// The next committed record, or `None` when nothing arrived in time.
    fn poll(&mut self, timeout: Duration) -> Result<Option<RawRecord>, String>;
    /// `(partition, next offset the source will read)` per assigned partition.
    fn positions(&self) -> Vec<(i32, i64)>;
}

#[derive(Clone, Debug)]
pub struct HubConfig {
    pub ring_max_bytes: usize,
    pub ring_max_age: Duration,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            ring_max_bytes: 32 << 20,
            ring_max_age: Duration::from_secs(120),
        }
    }
}

/// Upper bounds (ms) of the commit -> dispatch lag buckets; one more bucket
/// counts everything above the last bound.
pub const LAG_BOUNDS_MS: [i64; 12] = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000];

/// Commit -> dispatch lag of the live reader (K2-T08 lag per replica):
/// how long after the producer's record timestamp the hub dispatched it.
#[derive(Default)]
pub struct LagHistogram {
    buckets: [AtomicU64; LAG_BOUNDS_MS.len() + 1],
}

impl LagHistogram {
    pub fn record(&self, lag_ms: i64) {
        let index = LAG_BOUNDS_MS
            .iter()
            .position(|bound| lag_ms <= *bound)
            .unwrap_or(LAG_BOUNDS_MS.len());
        self.buckets[index].fetch_add(1, Ordering::Relaxed);
    }

    /// `{n, p50_le, p95_le, p99_le}`: the bucket upper bound each quantile
    /// falls in (`-1` = above the last bound).
    pub fn summary(&self) -> serde_json::Value {
        let counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        let total: u64 = counts.iter().sum();
        let quantile = |q: f64| {
            let wanted = (total as f64 * q).ceil() as u64;
            let mut seen = 0;
            for (index, count) in counts.iter().enumerate() {
                seen += count;
                if seen >= wanted.max(1) {
                    return LAG_BOUNDS_MS.get(index).copied().unwrap_or(-1);
                }
            }
            -1
        };
        serde_json::json!({"n": total, "p50_le": quantile(0.5), "p95_le": quantile(0.95),
            "p99_le": quantile(0.99), "buckets": counts})
    }
}

#[derive(Default)]
pub struct HubMetrics {
    pub dispatch_lag: LagHistogram,
    pub records: AtomicU64,
    pub bytes: AtomicU64,
    pub duplicates: AtomicU64,
    pub decode_failures: AtomicU64,
    pub offers: AtomicU64,
    pub ring_evictions: AtomicU64,
    pub reader_errors: AtomicU64,
}

/// What a new subscriber must replay before its queue: records of its key in
/// `(after, barrier)`. `ring` is `Some` when the ring still covered the whole
/// range at registration; `None` means a bounded replay reader is needed.
#[derive(Debug)]
pub struct Registration {
    pub barrier: i64,
    pub ring: Option<Vec<SharedRecord>>,
}

struct PartitionState {
    /// Every record below this offset has been dispatched.
    next_offset: i64,
    ring: VecDeque<(Instant, SharedRecord)>,
    ring_bytes: usize,
    /// The ring holds every dispatched record in `[ring_start, next_offset)`.
    ring_start: i64,
    subscribers: HashMap<Vec<u8>, Vec<Arc<Subscription>>>,
}

pub struct Hub {
    partitions: HashMap<i32, Mutex<PartitionState>>,
    config: HubConfig,
    pub metrics: HubMetrics,
    stop: AtomicBool,
    reader_failed: AtomicBool,
    reader_progress: Mutex<Option<Instant>>,
    /// Records produced before this replica started (the warm range, a
    /// restart backlog) are not live-path lag and stay out of the histogram.
    started_ms: i64,
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_nanos() as i64)
        .unwrap_or_default()
}

impl Hub {
    /// `starts`: the first offset the reader will deliver per partition.
    pub fn new(starts: &[(i32, i64)], config: HubConfig) -> Self {
        let partitions = starts
            .iter()
            .map(|&(partition, start)| {
                (
                    partition,
                    Mutex::new(PartitionState {
                        next_offset: start,
                        ring: VecDeque::new(),
                        ring_bytes: 0,
                        ring_start: start,
                        subscribers: HashMap::new(),
                    }),
                )
            })
            .collect();
        Self {
            partitions,
            config,
            metrics: HubMetrics::default(),
            stop: AtomicBool::new(false),
            reader_failed: AtomicBool::new(true),
            reader_progress: Mutex::new(None),
            started_ms: now_ns() / 1_000_000,
        }
    }

    pub fn partitions(&self) -> Vec<i32> {
        let mut ids: Vec<i32> = self.partitions.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn next_offset(&self, partition: i32) -> Option<i64> {
        self.partitions
            .get(&partition)
            .and_then(|state| state.lock().ok().map(|state| state.next_offset))
    }

    fn evict(&self, state: &mut PartitionState, now: Instant) {
        while let Some((at, front)) = state.ring.front() {
            let aged = now.duration_since(*at) > self.config.ring_max_age;
            if state.ring_bytes <= self.config.ring_max_bytes && !aged {
                break;
            }
            let weight = front.weight();
            state.ring_start = front.offset + 1;
            state.ring_bytes -= weight;
            state.ring.pop_front();
            self.metrics.ring_evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Dispatch one committed record. A record at or below an already
    /// dispatched offset is a duplicate transport delivery and is ignored.
    pub fn dispatch(&self, record: RawRecord) {
        let Some(partition) = self.partitions.get(&record.partition) else {
            return;
        };
        let record = Arc::new(record);
        let Ok(mut state) = partition.lock() else {
            return;
        };
        if record.offset < state.next_offset {
            self.metrics.duplicates.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.metrics.records.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .bytes
            .fetch_add(record.payload.len() as u64, Ordering::Relaxed);
        if record.timestamp_ms >= self.started_ms {
            self.metrics
                .dispatch_lag
                .record(now_ns() / 1_000_000 - record.timestamp_ms);
        }
        let now = Instant::now();
        state.ring_bytes += record.weight();
        state.ring.push_back((now, record.clone()));
        state.next_offset = record.offset + 1;
        self.evict(&mut state, now);
        let Some(subscribers) = state.subscribers.get(&record.key) else {
            return;
        };
        match LiveRecord::decode(record) {
            Ok(live) => {
                let at = now_ns();
                for subscriber in subscribers {
                    self.metrics.offers.fetch_add(1, Ordering::Relaxed);
                    subscriber.offer(&live, at);
                }
            }
            Err(error) => {
                self.metrics.decode_failures.fetch_add(1, Ordering::Relaxed);
                for subscriber in subscribers {
                    subscriber.fail(&error);
                }
            }
        }
    }

    /// The reader moved past records it never delivered (transaction markers,
    /// aborted batches): every offset below `position` is dispatched.
    pub fn advance(&self, partition: i32, position: i64) {
        if let Some(state) = self.partitions.get(&partition) {
            if let Ok(mut state) = state.lock() {
                if position > state.next_offset {
                    state.next_offset = position;
                    if state.ring.is_empty() {
                        state.ring_start = position;
                    }
                }
                self.evict(&mut state, Instant::now());
            }
        }
    }

    /// A bounded read of committed canonical bytes, not an execution-quality verdict.
    /// No Redis, Query, new Kafka reader, offset commit, or retained decoded cache.
    pub fn latest_hot(
        &self,
        key: &[u8],
        feed: &str,
        deadline: Instant,
    ) -> Result<HotRecord, String> {
        use crate::requirement::is_product;
        use qdl_contracts::qdl::common::v1::QualityFlag;
        if !matches!(
            feed,
            "TRADE" | "QUOTE" | "MARK_INDEX_PRICE" | "BOOK_SNAPSHOT" | "BOOK_DELTA"
        ) {
            return Err("HOT_FEED_UNSUPPORTED".into());
        }
        self.check_hot_reader()?;
        let mut selected = None;
        let mut key_partition = None;
        for (partition, state) in &self.partitions {
            let state = state.try_lock().map_err(|_| "HOT_READER_BUSY")?;
            let mut newest_scope = None;
            for (at, raw) in state.ring.iter().rev() {
                let now = Instant::now();
                if now >= deadline {
                    return Err("HOT_READ_DEADLINE".into());
                }
                if now.duration_since(*at) > self.config.ring_max_age {
                    break;
                }
                if raw.key != key {
                    continue;
                }
                if key_partition.is_some_and(|prior| prior != *partition) {
                    return Err("HOT_PARTITION_MISMATCH".into());
                }
                key_partition = Some(*partition);
                if raw.offset < 0 || raw.payload.len() > 256 * 1024 {
                    return Err("HOT_RECORD_INVALID".into());
                }
                let envelope = EventEnvelope::decode(raw.payload.as_slice())
                    .map_err(|_| "HOT_RECORD_INVALID")?;
                // A newer unsafe sibling (snapshot/delta share a physical key)
                // must not be hidden by returning an older healthy payload.
                if envelope.quality_flags.iter().any(|flag| {
                    matches!(
                        QualityFlag::try_from(*flag),
                        Ok(QualityFlag::SequenceGapBefore
                            | QualityFlag::OutOfOrder
                            | QualityFlag::ResyncRequired)
                    )
                }) {
                    return Err("HOT_RECORD_GAPPED".into());
                }
                let book_generation = match &envelope.payload {
                    Some(
                        crate::generated::marketdata_v2::event_envelope::Payload::BookSnapshot(
                            book,
                        ),
                    ) => {
                        if !book.sequence_verified || book.book_generation == 0 {
                            return Err("HOT_BOOK_UNVERIFIED".into());
                        }
                        book.book_generation
                    }
                    Some(crate::generated::marketdata_v2::event_envelope::Payload::BookDelta(
                        book,
                    )) => {
                        if !book.sequence_verified || book.book_generation == 0 || book.reset {
                            return Err("HOT_BOOK_UNVERIFIED".into());
                        }
                        book.book_generation
                    }
                    _ => 0,
                };
                let scope = (
                    envelope.source_session_id.clone(),
                    envelope.connection_generation,
                    envelope.authority_revision,
                    envelope.config_revision,
                    envelope.lease_epoch,
                    book_generation,
                );
                if newest_scope.as_ref().is_some_and(|newest| newest != &scope) {
                    return Err("HOT_RECORD_GENERATION_MISMATCH".into());
                }
                newest_scope = Some(scope);
                if is_product(feed, None, &envelope) {
                    selected = Some(HotRecord { raw: raw.clone(), boundary_offset: state.next_offset - 1 });
                    break;
                }
                if !(matches!(feed, "BOOK_SNAPSHOT" | "BOOK_DELTA")
                    && (is_product("BOOK_SNAPSHOT", None, &envelope)
                        || is_product("BOOK_DELTA", None, &envelope)))
                {
                    return Err("HOT_RECORD_PRODUCT_MISMATCH".into());
                }
            }
        }
        self.check_hot_reader()?;
        selected.ok_or_else(|| "HOT_RECORD_MISSING".into())
    }

    fn check_hot_reader(&self) -> Result<(), String> {
        if self.stop.load(Ordering::Acquire) || self.reader_failed.load(Ordering::Acquire) {
            return Err("HOT_READER_UNAVAILABLE".into());
        }
        let progress = self
            .reader_progress
            .try_lock()
            .map_err(|_| "HOT_READER_BUSY")?;
        if progress.is_none_or(|at| at.elapsed() > Duration::from_secs(1)) {
            return Err("HOT_READER_STALLED".into());
        }
        Ok(())
    }

    /// Register `subscriber` for `key` on `partition`, resuming after
    /// `after`. Atomic with dispatch on that partition (D3).
    pub fn register(
        &self,
        partition: i32,
        key: &[u8],
        subscriber: Arc<Subscription>,
        after: i64,
    ) -> Result<Registration, String> {
        let state = self
            .partitions
            .get(&partition)
            .ok_or_else(|| format!("partition {partition} is not served by this replica"))?;
        let mut state = state
            .lock()
            .map_err(|_| "partition state poisoned".to_owned())?;
        let barrier = state.next_offset;
        let ring = if after + 1 >= barrier {
            Some(Vec::new())
        } else if after + 1 >= state.ring_start {
            Some(
                state
                    .ring
                    .iter()
                    .map(|(_, record)| record)
                    .filter(|record| record.offset > after && record.key == key)
                    .cloned()
                    .collect(),
            )
        } else {
            None
        };
        state
            .subscribers
            .entry(key.to_vec())
            .or_default()
            .push(subscriber);
        Ok(Registration { barrier, ring })
    }

    pub fn unregister(&self, partition: i32, key: &[u8], id: u64) {
        if let Some(state) = self.partitions.get(&partition) {
            if let Ok(mut state) = state.lock() {
                if let Some(list) = state.subscribers.get_mut(key) {
                    list.retain(|subscriber| subscriber.id != id);
                    if list.is_empty() {
                        state.subscribers.remove(key);
                    }
                }
            }
        }
    }

    pub fn subscriber_count(&self) -> usize {
        self.partitions
            .values()
            .filter_map(|state| state.lock().ok())
            .map(|state| state.subscribers.values().map(Vec::len).sum::<usize>())
            .sum()
    }

    pub fn ring_bytes(&self) -> usize {
        self.partitions
            .values()
            .filter_map(|state| state.lock().ok())
            .map(|state| state.ring_bytes)
            .sum()
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Drive `source` on a dedicated thread until [`Hub::stop`].
    pub fn run(self: &Arc<Self>, mut source: Box<dyn LogSource>) -> std::thread::JoinHandle<()> {
        let hub = self.clone();
        std::thread::Builder::new()
            .name("qdl-kn-live-reader".into())
            .spawn(move || {
                while !hub.stop.load(Ordering::Relaxed) {
                    match source.poll(Duration::from_millis(100)) {
                        Ok(Some(record)) => {
                            hub.dispatch(record);
                            hub.reader_failed.store(false, Ordering::Release);
                            if let Ok(mut progress) = hub.reader_progress.lock() {
                                *progress = Some(Instant::now());
                            }
                        }
                        Ok(None) => {
                            if let Ok(mut progress) = hub.reader_progress.lock() {
                                *progress = Some(Instant::now());
                            }
                            for (partition, position) in source.positions() {
                                hub.advance(partition, position);
                            }
                        }
                        Err(_) => {
                            hub.reader_failed.store(true, Ordering::Release);
                            hub.metrics.reader_errors.fetch_add(1, Ordering::Relaxed);
                            std::thread::sleep(Duration::from_millis(100));
                        }
                    }
                }
            })
            .expect("spawn live reader thread")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(offset: i64, timestamp_ms: i64) -> RawRecord {
        RawRecord {
            partition: 0,
            offset,
            key: b"k".to_vec(),
            payload: Vec::new(),
            timestamp_ms,
        }
    }

    #[test]
    fn records_produced_before_the_replica_started_are_not_dispatch_lag() {
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        let started = hub.started_ms;
        hub.dispatch(record(0, started - 60_000));
        hub.dispatch(record(1, 0));
        hub.dispatch(record(2, started));
        let summary = hub.metrics.dispatch_lag.summary();
        assert_eq!(summary["n"], 1);
        assert_eq!(hub.metrics.records.load(Ordering::Relaxed), 3);
    }
    fn healthy(hub: &Hub) {
        hub.reader_failed.store(false, Ordering::Release);
        *hub.reader_progress.lock().unwrap() = Some(Instant::now());
    }

    fn hot_record(
        partition: i32,
        offset: i64,
        feed: &str,
        generation: u64,
        flags: Vec<i32>,
    ) -> RawRecord {
        use crate::generated::marketdata_v2::{
            event_envelope::Payload, OrderBookDelta, OrderBookSnapshot, Quote, Trade,
        };
        let payload = match feed {
            "TRADE" => Payload::Trade(Trade::default()),
            "QUOTE" => Payload::Quote(Quote::default()),
            "BOOK_SNAPSHOT" => Payload::BookSnapshot(OrderBookSnapshot {
                book_generation: 1,
                sequence_verified: true,
                ..Default::default()
            }),
            "BOOK_DELTA" => Payload::BookDelta(OrderBookDelta {
                book_generation: 1,
                sequence_verified: true,
                ..Default::default()
            }),
            _ => panic!("test feed"),
        };
        RawRecord {
            partition,
            offset,
            key: b"hot".to_vec(),
            timestamp_ms: 1,
            payload: EventEnvelope {
                payload: Some(payload),
                connection_generation: generation,
                quality_flags: flags,
                ..Default::default()
            }
            .encode_to_vec(),
        }
    }

    fn latest(hub: &Hub, feed: &str) -> Result<HotRecord, String> {
        hub.latest_hot(b"hot", feed, Instant::now() + Duration::from_secs(1))
    }

    #[test]
    fn hot_read_preserves_zero_offset_and_does_not_join_or_seek() {
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        hub.dispatch(hot_record(0, 0, "QUOTE", 1, vec![]));
        healthy(&hub);
        assert_eq!(latest(&hub, "QUOTE").unwrap().raw.offset, 0);
        assert_eq!(hub.next_offset(0), Some(1));
        hub.advance(0, 9);
        let view = latest(&hub, "QUOTE").unwrap();
        assert_eq!(view.raw.offset, 0);
        assert_eq!(view.boundary_offset, 8);
        assert_eq!(hub.subscriber_count(), 0);
        assert_eq!(latest(&hub, "BAR").unwrap_err(), "HOT_FEED_UNSUPPORTED");
    }

    #[test]
    fn hot_read_selects_exact_book_payload_and_never_hides_newer_gap_or_generation() {
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        hub.dispatch(hot_record(0, 0, "BOOK_SNAPSHOT", 1, vec![]));
        hub.dispatch(hot_record(0, 1, "BOOK_DELTA", 1, vec![]));
        healthy(&hub);
        assert_eq!(latest(&hub, "BOOK_SNAPSHOT").unwrap().raw.offset, 0);
        assert_eq!(latest(&hub, "BOOK_DELTA").unwrap().raw.offset, 1);
        hub.dispatch(hot_record(0, 2, "BOOK_DELTA", 2, vec![]));
        assert_eq!(
            latest(&hub, "BOOK_SNAPSHOT").unwrap_err(),
            "HOT_RECORD_GENERATION_MISMATCH"
        );
        hub.dispatch(hot_record(0, 3, "BOOK_DELTA", 2, vec![16]));
        assert_eq!(
            latest(&hub, "BOOK_SNAPSHOT").unwrap_err(),
            "HOT_RECORD_GAPPED"
        );
        assert_eq!(latest(&hub, "BOOK_DELTA").unwrap_err(), "HOT_RECORD_GAPPED");
    }

    #[test]
    fn hot_read_refuses_malformed_latest_and_cross_partition_identity() {
        let hub = Hub::new(&[(0, 0), (1, 0)], HubConfig::default());
        healthy(&hub);
        hub.dispatch(hot_record(0, 0, "QUOTE", 1, vec![]));
        hub.dispatch(hot_record(1, 0, "QUOTE", 1, vec![]));
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_PARTITION_MISMATCH");
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        healthy(&hub);
        hub.dispatch(hot_record(0, 0, "QUOTE", 1, vec![]));
        let mut bad = hot_record(0, 1, "QUOTE", 1, vec![]);
        bad.payload = vec![255];
        hub.dispatch(bad);
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_RECORD_INVALID");
    }

    #[test]
    fn hot_read_refuses_failed_stopped_stalled_or_expired_reader() {
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        hub.dispatch(hot_record(0, 0, "QUOTE", 1, vec![]));
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_READER_UNAVAILABLE");
        healthy(&hub);
        *hub.reader_progress.lock().unwrap() = Some(Instant::now() - Duration::from_secs(2));
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_READER_STALLED");
        healthy(&hub);
        assert_eq!(
            hub.latest_hot(b"hot", "QUOTE", Instant::now()).unwrap_err(),
            "HOT_READ_DEADLINE"
        );
        hub.stop();
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_READER_UNAVAILABLE");
        let hub = Hub::new(
            &[(0, 0)],
            HubConfig {
                ring_max_age: Duration::ZERO,
                ..Default::default()
            },
        );
        hub.dispatch(hot_record(0, 0, "QUOTE", 1, vec![]));
        healthy(&hub);
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_RECORD_MISSING");
    }

    #[test]
    fn hot_read_is_bounded_by_existing_ring_and_refuses_busy_partition() {
        let hub = Hub::new(
            &[(0, 0)],
            HubConfig {
                ring_max_bytes: 1,
                ..Default::default()
            },
        );
        hub.dispatch(hot_record(0, 0, "QUOTE", 1, vec![]));
        healthy(&hub);
        assert_eq!(hub.ring_bytes(), 0);
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_RECORD_MISSING");
        let _lock = hub.partitions[&0].lock().unwrap();
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_READER_BUSY");
    }
}
