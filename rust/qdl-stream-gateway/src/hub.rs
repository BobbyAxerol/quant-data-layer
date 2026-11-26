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
//!
//! Hot read (R2, Astra advisory 2026-10-10): a canonical hot lookup touches
//! only the partition(s) that hold its key. Dispatch keeps, under the same
//! partition lock, a per-key view of the ring (`by_key`) and a directory of
//! the partitions whose ring holds each key (`key_home`). A lookup copies the
//! key's `Arc` references, the dispatch boundary and the broker head proof in
//! one short critical section and decodes/validates outside it. Locks are
//! acquired with a bounded wait against the request deadline instead of a
//! single `try_lock`, and reader progress is an atomic, so concurrent lookups
//! of different (or the same) products no longer refuse each other.
//!
//! Quiet keys (DL-DEF-2026-11-26-M): the ring is bounded by bytes, so a key
//! whose product does not change for longer than its partition's ring
//! coverage loses its last record to eviction and the lookup could no longer
//! prove that no newer record exists (`HOT_RECORD_MISSING`). Dispatch also
//! keeps, under the same partition lock, a last-value entry per key (the
//! newest dispatched record of an indexable hot product). Ring eviction never
//! removes it; it is bounded by a number of keys and a per-record size, not
//! by ring bytes. A lookup whose ring view is empty is answered from it only
//! when it is the key's newest record, the partition boundary and broker head
//! proof are current, the provider session's connection generation and lease
//! fence still match the newest seen for that session, and the key has been
//! quiet for at most `max_quiet` (else `HOT_QUIET_BEYOND_MAX`). A reader
//! error clears every entry (continuity is no longer proven).

use crate::generated::marketdata_v2::EventEnvelope;
use crate::subscription::{ByteBudget, Charge, Subscription};
use prost::Message as _;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, TryLockError};
use std::time::{Duration, Instant};

/// Accounting overhead per queued/ringed record beyond key and payload.
pub const RECORD_OVERHEAD: usize = 96;

/// TEST_ONLY partition-lock wait/hold histograms for the OPT-01 lock
/// disposition. Compiled only with `--features lock-profile`; default and
/// image builds contain no timing calls. Process-wide, fixed log2 buckets,
/// no per-event logging. Hold is recorded when the timer drops, right after
/// the guard (it includes the unlock).
#[cfg(feature = "lock-profile")]
pub mod lock_profile {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    pub const OPS: [&str; 7] = [
        "dispatch",
        "register",
        "register_near",
        "retained_replay",
        "hot_snapshot",
        "advance",
        "unregister",
    ];

    #[derive(Clone, Copy)]
    pub enum Op {
        Dispatch,
        Register,
        RegisterNear,
        RetainedReplay,
        HotSnapshot,
        Advance,
        Unregister,
    }

    /// Bucket i holds samples whose bit length is i: ns < 2^i.
    const BUCKETS: usize = 48;

    pub struct Histogram {
        buckets: [AtomicU64; BUCKETS],
        max_ns: AtomicU64,
    }

    impl Histogram {
        const fn new() -> Self {
            Self {
                buckets: [const { AtomicU64::new(0) }; BUCKETS],
                max_ns: AtomicU64::new(0),
            }
        }

        fn record(&self, elapsed: Duration) {
            let ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;
            let index = ((u64::BITS - ns.leading_zeros()) as usize).min(BUCKETS - 1);
            self.buckets[index].fetch_add(1, Ordering::Relaxed);
            self.max_ns.fetch_max(ns, Ordering::Relaxed);
        }

        fn reset(&self) {
            for bucket in &self.buckets {
                bucket.store(0, Ordering::Relaxed);
            }
            self.max_ns.store(0, Ordering::Relaxed);
        }

        /// Quantiles are bucket upper bounds (`_le_ns`); p99 only for n >= 100.
        fn summary(&self) -> serde_json::Value {
            let counts: Vec<u64> = self
                .buckets
                .iter()
                .map(|b| b.load(Ordering::Relaxed))
                .collect();
            let n: u64 = counts.iter().sum();
            let le = |q: f64| -> Option<u64> {
                let wanted = ((n as f64) * q).ceil().max(1.0) as u64;
                let mut seen = 0;
                for (index, count) in counts.iter().enumerate() {
                    seen += count;
                    if seen >= wanted {
                        return Some(1u64 << index);
                    }
                }
                None
            };
            serde_json::json!({
                "n": n,
                "p50_le_ns": (n > 0).then(|| le(0.5)).flatten(),
                "p95_le_ns": (n > 0).then(|| le(0.95)).flatten(),
                "p99_le_ns": (n >= 100).then(|| le(0.99)).flatten(),
                "max_ns": self.max_ns.load(Ordering::Relaxed),
                "buckets": counts,
            })
        }
    }

    struct Profile {
        wait: [Histogram; OPS.len()],
        hold: [Histogram; OPS.len()],
        dispatch_fanout: Histogram,
    }

    static PROFILE: Profile = Profile {
        wait: [const { Histogram::new() }; OPS.len()],
        hold: [const { Histogram::new() }; OPS.len()],
        dispatch_fanout: Histogram::new(),
    };

    /// Started immediately before the lock call.
    pub struct Wait(Op, Instant);

    pub fn waiting(op: Op) -> Wait {
        Wait(op, Instant::now())
    }

    impl Wait {
        /// Record the wait; the returned timer records the hold on drop.
        pub fn acquired(self) -> Hold {
            let now = Instant::now();
            PROFILE.wait[self.0 as usize].record(now.duration_since(self.1));
            Hold(self.0, now)
        }
    }

    pub struct Hold(Op, Instant);

    impl Drop for Hold {
        fn drop(&mut self) {
            PROFILE.hold[self.0 as usize].record(self.1.elapsed());
        }
    }

    /// Dispatch decode + fan-out span, a subset of the dispatch hold.
    pub struct Fanout(Instant);

    pub fn fanout() -> Fanout {
        Fanout(Instant::now())
    }

    impl Drop for Fanout {
        fn drop(&mut self) {
            PROFILE.dispatch_fanout.record(self.0.elapsed());
        }
    }

    pub fn reset() {
        for histogram in PROFILE.wait.iter().chain(&PROFILE.hold) {
            histogram.reset();
        }
        PROFILE.dispatch_fanout.reset();
    }

    pub fn summary() -> serde_json::Value {
        let ops: serde_json::Map<String, serde_json::Value> = OPS
            .iter()
            .enumerate()
            .map(|(index, name)| {
                (
                    (*name).to_owned(),
                    serde_json::json!({
                        "wait": PROFILE.wait[index].summary(),
                        "hold": PROFILE.hold[index].summary(),
                    }),
                )
            })
            .collect();
        serde_json::json!({
            "unit": "ns", "buckets": "log2; quantiles are bucket upper bounds",
            "ops": ops, "dispatch_decode_fanout_hold": PROFILE.dispatch_fanout.summary(),
            "overhead": "two Instant::now plus atomics per lock; compare with feature-off inclusive timings",
        })
    }
}

/// Acquire a partition lock, timed under `lock-profile` (no-op otherwise).
/// Returns `(lock result, hold timer)`; bind the timer for the guard's scope.
macro_rules! profiled_lock {
    ($op:ident, $lock:expr) => {{
        #[cfg(feature = "lock-profile")]
        let wait = $crate::hub::lock_profile::waiting($crate::hub::lock_profile::Op::$op);
        let guard = $lock;
        #[cfg(feature = "lock-profile")]
        let hold = wait.acquired();
        #[cfg(not(feature = "lock-profile"))]
        let hold = ();
        (guard, hold)
    }};
}

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
    /// The live budget charge for this one decoded copy, taken once at
    /// dispatch and released exactly once, when the last `Arc` (a queue
    /// entry or a subscription task) drops. Subscribers then charge the
    /// replica budget only their queue entry overhead. `None` for replay/ring decodes (charged per record to the
    /// replay budget by their owner) and when the dispatch-time charge was
    /// refused: each subscriber then charges the full weight itself.
    pub shared_charge: Option<Charge>,
    /// Gateway wall clock (Unix ns) when the live hub dispatched this record
    /// to its subscriptions; 0 for replay/ring decodes (diagnostic only,
    /// `StreamRecord.gateway_dispatch_wall_ns`, SDK 2.4.0).
    pub dispatched_wall_ns: u64,
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

    /// Decode without a shared charge (the caller accounts for it).
    pub fn decode(raw: SharedRecord) -> Result<Arc<Self>, String> {
        Self::decode_charged(raw, None, 0)
    }

    /// Decode once and charge the decoded copy to `budget` once, for every
    /// holder together. A refused charge leaves `shared_charge` empty.
    /// `dispatched_wall_ns` is the live dispatch wall clock (0 = none).
    pub fn decode_charged(
        raw: SharedRecord,
        budget: Option<&Arc<ByteBudget>>,
        dispatched_wall_ns: u64,
    ) -> Result<Arc<Self>, String> {
        let envelope = decode_canonical(&raw)?;
        Ok(Self::from_envelope(
            raw,
            envelope,
            budget,
            dispatched_wall_ns,
        ))
    }

    /// [`LiveRecord::decode_charged`] for an envelope already decoded once.
    pub fn from_envelope(
        raw: SharedRecord,
        envelope: EventEnvelope,
        budget: Option<&Arc<ByteBudget>>,
        dispatched_wall_ns: u64,
    ) -> Arc<Self> {
        let shared_charge = budget.and_then(|budget| budget.charge(live_weight(&raw)));
        Arc::new(Self {
            raw,
            envelope,
            shared_charge,
            dispatched_wall_ns,
        })
    }
}

fn decode_canonical(raw: &RawRecord) -> Result<EventEnvelope, String> {
    EventEnvelope::decode(raw.payload.as_slice())
        .map_err(|error| format!("canonical record failed to decode: {error}"))
}

/// The committed log as the live reader sees it (Kafka in production, an
/// in-memory log in tests). Blocking; driven by the hub's reader thread.
pub trait LogSource: Send {
    /// The next committed record, or `None` when nothing arrived in time.
    fn poll(&mut self, timeout: Duration) -> Result<Option<RawRecord>, String>;
    /// `(partition, next offset the source will read)` per assigned partition.
    fn positions(&self) -> Vec<(i32, i64)>;
    /// Positive broker responses only; an empty poll is not a liveness proof.
    fn broker_heads(&self) -> Vec<(i32, i64, Instant)> {
        Vec::new()
    }
}

#[derive(Clone, Debug)]
pub struct HubConfig {
    pub ring_max_bytes: usize,
    pub ring_max_age: Duration,
    /// Last-value entries across all partitions (split evenly); 0 disables
    /// the quiet-key index and lookups behave as ring-only.
    pub last_value_max_keys: usize,
    /// A key quiet for longer than this is refused `HOT_QUIET_BEYOND_MAX`
    /// instead of being served from its last-value entry.
    pub max_quiet: Duration,
}

/// Default `QDL_KN_LAST_VALUE_MAX_KEYS`. The v2.4.2-realm-ts16 bundle has 220
/// hot non-book bindings (104 QUOTE, 102 MARK_INDEX_PRICE, 14 TRADE) among
/// 1,577 bindings on 1,560 physical keys.
pub const DEFAULT_LAST_VALUE_MAX_KEYS: usize = 2048;
/// Default `QDL_KN_MAX_QUIET_SECONDS`.
pub const DEFAULT_MAX_QUIET: Duration = Duration::from_secs(900);
/// A record heavier than this ([`RawRecord::weight`]) is not kept as a last
/// value (its older entry is dropped, so it never answers for the key).
/// Memory bound: `last_value_max_keys x LAST_VALUE_MAX_RECORD_BYTES`.
pub const LAST_VALUE_MAX_RECORD_BYTES: usize = 4096;
/// Provider sessions whose newest connection generation/lease fence is
/// tracked; a session beyond it has no fence proof and is refused.
pub const SESSION_FENCE_MAX: usize = 4096;

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            ring_max_bytes: 32 << 20,
            ring_max_age: Duration::from_secs(120),
            last_value_max_keys: DEFAULT_LAST_VALUE_MAX_KEYS,
            max_quiet: DEFAULT_MAX_QUIET,
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
    /// Dispatches whose shared live-budget charge was refused (the replica
    /// live budget is really full); their subscribers charged in full.
    pub live_budget_refused: AtomicU64,
    pub ring_evictions: AtomicU64,
    pub reader_errors: AtomicU64,
    /// Lookups answered from a last-value entry (ring view empty).
    pub last_value_hits: AtomicU64,
    /// Lookups refused `HOT_QUIET_BEYOND_MAX`.
    pub last_value_quiet_refused: AtomicU64,
    /// Entries dropped to stay within the per-partition key bound.
    pub last_value_evictions: AtomicU64,
    /// Records not kept as a last value (not a hot non-book product, not
    /// decodable, or heavier than [`LAST_VALUE_MAX_RECORD_BYTES`]).
    pub last_value_unindexed: AtomicU64,
    /// Reader errors that cleared every last-value entry.
    pub last_value_continuity_breaks: AtomicU64,
}

/// What a new subscriber must replay before its queue: records of its key in
/// `(after, barrier)`. `ring` is `Some` when the ring still covered the whole
/// range at registration; `None` means a bounded replay reader is needed.
#[derive(Debug)]
pub struct Registration {
    pub barrier: i64,
    pub ring: Option<Vec<SharedRecord>>,
}

/// Bulk replay remains unregistered until the retained bridge fits both budgets.
#[derive(Debug)]
pub enum NearRegistration {
    NeedReplay {
        barrier: i64,
    },
    Registered {
        barrier: i64,
        bridge: Vec<SharedRecord>,
        charge: Charge,
    },
}

struct PartitionState {
    /// Every record below this offset has been dispatched.
    next_offset: i64,
    ring: VecDeque<(Instant, SharedRecord)>,
    ring_bytes: usize,
    /// The ring holds every dispatched record in `[ring_start, next_offset)`.
    ring_start: i64,
    /// Copy-on-write per key: dispatch clones the `Arc` under the partition
    /// lock and offers outside it; register/unregister (rare) replace the list.
    subscribers: HashMap<Vec<u8>, Arc<[Arc<Subscription>]>>,
    /// The ring's records per key, in ring (offset) order. Same entries as
    /// `ring`, so it is bounded by the ring and evicted with it.
    by_key: HashMap<Vec<u8>, VecDeque<(Instant, SharedRecord)>>,
    /// Last broker head this partition had caught up to, and when the broker
    /// confirmed it. Read under the same lock as `next_offset`.
    broker_head: Option<(i64, Instant)>,
    /// DL-DEF-2026-11-26-M: the newest dispatched record per key, kept past
    /// ring eviction; bounded by keys ([`Hub::last_value_cap`]).
    last_value: HashMap<Vec<u8>, LastValue>,
    last_value_bytes: usize,
}

/// A provider session's connection generation and lease fence.
type SessionFence = (String, u64, u64);

/// A last value copied for a lookup: the record with its boundary, the
/// partition's broker head proof and the record's session fence.
type LastValueView = (HotRecord, Option<(i64, Instant)>, SessionFence);

/// The newest dispatched record of one key on one partition.
struct LastValue {
    at: Instant,
    raw: SharedRecord,
    fence: SessionFence,
}

/// Ring entries a hot lookup copies per critical section. Non-book keys
/// resolve on the newest entry; a BOOK_SNAPSHOT lookup behind many deltas
/// continues with further chunks, each one short lock.
const HOT_SCAN_CHUNK: usize = 256;

/// What a hot lookup copies out of one partition in one critical section.
struct KeySnapshot {
    partition: i32,
    /// The key's newest ring entries (at most [`HOT_SCAN_CHUNK`]), newest first.
    entries: Vec<(Instant, SharedRecord)>,
    boundary_offset: i64,
    broker_head: Option<(i64, Instant)>,
}

/// Acquire `mutex` without blocking past `deadline`. A lock still held at the
/// deadline is `HOT_READER_BUSY`; a poisoned one means a reader-side panic.
fn lock_until<T>(mutex: &Mutex<T>, deadline: Instant) -> Result<MutexGuard<'_, T>, String> {
    let mut spins = 0u32;
    loop {
        match mutex.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(TryLockError::Poisoned(_)) => return Err("HOT_READER_UNAVAILABLE".into()),
            Err(TryLockError::WouldBlock) => {
                if Instant::now() >= deadline {
                    return Err("HOT_READER_BUSY".into());
                }
                // Holders keep these locks for O(record) work; spin briefly,
                // then back off so a waiter never burns a core.
                if spins < 64 {
                    std::hint::spin_loop();
                } else if spins < 128 {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(Duration::from_micros(20));
                }
                spins = spins.saturating_add(1);
            }
        }
    }
}

pub struct Hub {
    partitions: HashMap<i32, Mutex<PartitionState>>,
    /// Dispatcher-only gate per partition (lock order: gate, then partition).
    /// Held across decode/offer so every subscriber sees one partition's
    /// records in offset order even with concurrent dispatch callers, while
    /// the partition lock covers only ring/boundary/list-snapshot work.
    dispatch_gates: HashMap<i32, Mutex<()>>,
    config: HubConfig,
    pub metrics: HubMetrics,
    stop: AtomicBool,
    reader_failed: AtomicBool,
    /// Last reader poll, in ns after `origin` plus one; 0 = never.
    reader_progress: AtomicU64,
    origin: Instant,
    /// Partitions whose ring currently holds each key. Changed only when a
    /// key enters or leaves a partition's ring (lock order: partition, then
    /// this); lookups hold it alone, for a map read.
    key_home: Mutex<HashMap<Vec<u8>, Vec<i32>>>,
    /// Records produced before this replica started (the warm range, a
    /// restart backlog) are not live-path lag and stay out of the histogram.
    started_ms: i64,
    /// The replica live budget a dispatched record is charged to once
    /// ([`Hub::attach_live_budget`]); unset, subscribers charge in full.
    live_budget: OnceLock<Arc<ByteBudget>>,
    data_clock: crate::replay_clock::DataClock,
    /// Last-value entries per partition; 0 = index disabled.
    last_value_cap: usize,
    /// Partitions whose last-value index holds each key (lock order:
    /// partition, then this), like `key_home` for the ring.
    value_home: Mutex<HashMap<Vec<u8>, Vec<i32>>>,
    /// Newest `(connection_generation, lease_epoch)` dispatched per provider
    /// session; a last value of an older generation/fence is refused.
    session_fence: Mutex<HashMap<String, (u64, u64)>>,
}

/// The session fence of a record that may be kept as a last value: a hot
/// non-book product (QUOTE, TRADE, MARK_INDEX_PRICE). Books need their
/// snapshot/delta chain and stay ring-only; anything else on a key (a feed
/// state or quality event) drops the key's entry.
fn last_value_fence(envelope: &EventEnvelope) -> Option<SessionFence> {
    use crate::generated::marketdata_v2::event_envelope::Payload;
    matches!(
        envelope.payload,
        Some(Payload::Quote(_) | Payload::Trade(_) | Payload::MarkIndexPrice(_))
    )
    .then(|| {
        (
            envelope.source_session_id.clone(),
            envelope.connection_generation,
            envelope.lease_epoch,
        )
    })
}

/// Copy-on-write append of `subscriber` to `key`'s list (rare: registration).
fn attach(
    lists: &mut HashMap<Vec<u8>, Arc<[Arc<Subscription>]>>,
    key: &[u8],
    subscriber: Arc<Subscription>,
) {
    let mut next: Vec<Arc<Subscription>> =
        lists.get(key).map(|list| list.to_vec()).unwrap_or_default();
    next.push(subscriber);
    lists.insert(key.to_vec(), next.into());
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_nanos() as i64)
        .unwrap_or_default()
}

impl Hub {
    /// `starts`: the first offset the reader will deliver per partition.
    /// Panics before serving if configured TEST data time is invalid or unsupported.
    pub fn new(starts: &[(i32, i64)], config: HubConfig) -> Self {
        // Fail before the reader/server starts; never ignore configured TEST time.
        let data_clock = crate::replay_clock::DataClock::from_env()
            .unwrap_or_else(|error| panic!("stream gateway startup refused: {error}"));
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
                        by_key: HashMap::new(),
                        broker_head: None,
                        last_value: HashMap::new(),
                        last_value_bytes: 0,
                    }),
                )
            })
            .collect();
        let dispatch_gates = starts
            .iter()
            .map(|&(partition, _)| (partition, Mutex::new(())))
            .collect();
        Self {
            partitions,
            dispatch_gates,
            metrics: HubMetrics::default(),
            stop: AtomicBool::new(false),
            reader_failed: AtomicBool::new(true),
            reader_progress: AtomicU64::new(0),
            // Before any reader instant, so an old progress mark is representable.
            origin: Instant::now()
                .checked_sub(Duration::from_secs(3_600))
                .unwrap_or_else(Instant::now),
            key_home: Mutex::new(HashMap::new()),
            started_ms: now_ns() / 1_000_000,
            live_budget: OnceLock::new(),
            data_clock,
            last_value_cap: if config.last_value_max_keys == 0 {
                0
            } else {
                (config.last_value_max_keys / starts.len().max(1)).max(1)
            },
            value_home: Mutex::new(HashMap::new()),
            session_fence: Mutex::new(HashMap::new()),
            config,
        }
    }

    /// Charge each decoded live record to `budget` once, at dispatch,
    /// instead of once per subscriber. The first attachment wins; returns
    /// whether this one was attached.
    pub fn attach_live_budget(&self, budget: Arc<ByteBudget>) -> bool {
        self.live_budget.set(budget).is_ok()
    }

    pub(crate) fn freshness_now_ns(&self, real_ns: i64) -> Result<i64, &'static str> {
        self.data_clock.now_ns(real_ns)
    }

    #[cfg(feature = "isolated-replay-clock")]
    pub(crate) fn test_clock_enabled(&self) -> bool {
        self.data_clock.is_replay()
    }

    #[cfg(feature = "isolated-replay-clock")]
    pub(crate) fn check_test_clock(&self) -> Result<(), &'static str> {
        self.data_clock.now_ns(0).map(|_| ())
    }

    #[cfg(feature = "isolated-replay-clock")]
    pub(crate) async fn test_clock_invalidated(&self) {
        loop {
            if self.check_test_clock().is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[cfg(all(test, feature = "isolated-replay-clock"))]
    pub(crate) fn with_test_data_clock(mut self, clock: crate::replay_clock::DataClock) -> Self {
        self.data_clock = clock;
        self
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

    fn evict(&self, partition: i32, state: &mut PartitionState, now: Instant) {
        while let Some((at, front)) = state.ring.front() {
            let aged = now.duration_since(*at) > self.config.ring_max_age;
            if state.ring_bytes <= self.config.ring_max_bytes && !aged {
                break;
            }
            let weight = front.weight();
            state.ring_start = front.offset + 1;
            state.ring_bytes -= weight;
            let Some((_, front)) = state.ring.pop_front() else {
                break;
            };
            // The ring and every per-key view are in offset order, so the
            // evicted record is the front of its key's view.
            let emptied = state.by_key.get_mut(&front.key).is_some_and(|entries| {
                entries.pop_front();
                entries.is_empty()
            });
            if emptied {
                state.by_key.remove(&front.key);
                self.leave_home(&front.key, partition);
            }
            self.metrics.ring_evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn enter_home(&self, key: &[u8], partition: i32) {
        if let Ok(mut homes) = self.key_home.lock() {
            let entry = homes.entry(key.to_vec()).or_default();
            if !entry.contains(&partition) {
                entry.push(partition);
            }
        }
    }

    fn leave_home(&self, key: &[u8], partition: i32) {
        if let Ok(mut homes) = self.key_home.lock() {
            if let Some(entry) = homes.get_mut(key) {
                entry.retain(|home| *home != partition);
                if entry.is_empty() {
                    homes.remove(key);
                }
            }
        }
    }

    fn enter_value_home(&self, key: &[u8], partition: i32) {
        if let Ok(mut homes) = self.value_home.lock() {
            let entry = homes.entry(key.to_vec()).or_default();
            if !entry.contains(&partition) {
                entry.push(partition);
            }
        }
    }

    fn leave_value_home(&self, key: &[u8], partition: i32) {
        if let Ok(mut homes) = self.value_home.lock() {
            if let Some(entry) = homes.get_mut(key) {
                entry.retain(|home| *home != partition);
                if entry.is_empty() {
                    homes.remove(key);
                }
            }
        }
    }

    /// Record the newest `(generation, lease)` dispatched for `session`.
    /// Generations and fences only move forward; an older one never
    /// replaces a newer one. Bounded by [`SESSION_FENCE_MAX`] sessions.
    fn advance_session_fence(&self, session: &str, generation: u64, lease: u64) {
        if session.is_empty() {
            return;
        }
        let Ok(mut fences) = self.session_fence.lock() else {
            return;
        };
        let full = fences.len() >= SESSION_FENCE_MAX;
        match fences.get_mut(session) {
            Some(newest) => {
                if (generation, lease) > *newest {
                    *newest = (generation, lease);
                }
            }
            None if !full => {
                fences.insert(session.to_owned(), (generation, lease));
            }
            None => {}
        }
    }

    /// Keep `record` as its key's last value on this partition, under the
    /// dispatch lock. A record that cannot be kept drops the key's older
    /// entry: an entry is only ever the key's newest dispatched record.
    fn remember_last_value(
        &self,
        state: &mut PartitionState,
        record: &SharedRecord,
        now: Instant,
        fence: Option<SessionFence>,
    ) {
        if self.last_value_cap == 0 {
            return;
        }
        let weight = record.weight();
        let fence = match fence {
            Some(fence) if weight <= LAST_VALUE_MAX_RECORD_BYTES => fence,
            _ => {
                self.metrics
                    .last_value_unindexed
                    .fetch_add(1, Ordering::Relaxed);
                if let Some(old) = state.last_value.remove(&record.key) {
                    state.last_value_bytes -= old.raw.weight();
                    self.leave_value_home(&record.key, record.partition);
                }
                return;
            }
        };
        let entry = LastValue {
            at: now,
            raw: record.clone(),
            fence,
        };
        if let Some(old) = state.last_value.insert(record.key.clone(), entry) {
            state.last_value_bytes = state.last_value_bytes - old.raw.weight() + weight;
            return;
        }
        state.last_value_bytes += weight;
        self.enter_value_home(&record.key, record.partition);
        if state.last_value.len() > self.last_value_cap {
            self.make_last_value_room(state, &record.key, now);
        }
    }

    /// Over the key bound: drop entries quiet past `max_quiet` (they can no
    /// longer answer), then the least recently dispatched other key.
    fn make_last_value_room(&self, state: &mut PartitionState, keep: &[u8], now: Instant) {
        let max_quiet = self.config.max_quiet;
        let mut dropped: Vec<Vec<u8>> = state
            .last_value
            .iter()
            .filter(|(key, entry)| {
                key.as_slice() != keep && now.duration_since(entry.at) > max_quiet
            })
            .map(|(key, _)| key.clone())
            .collect();
        if dropped.is_empty() {
            dropped.extend(
                state
                    .last_value
                    .iter()
                    .filter(|(key, _)| key.as_slice() != keep)
                    .min_by_key(|(_, entry)| entry.at)
                    .map(|(key, _)| key.clone()),
            );
        }
        for key in dropped {
            if let Some(old) = state.last_value.remove(&key) {
                state.last_value_bytes -= old.raw.weight();
                self.leave_value_home(&key, old.raw.partition);
                self.metrics
                    .last_value_evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// The reader failed: continuity since every last value is no longer
    /// proven, so all entries and session fences are dropped. Quiet keys
    /// then refuse (as before DL-DEF-M) until their next record.
    fn break_last_value_continuity(&self) {
        if self.last_value_cap == 0 {
            return;
        }
        for state in self.partitions.values() {
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.last_value.clear();
            state.last_value_bytes = 0;
        }
        if let Ok(mut homes) = self.value_home.lock() {
            homes.clear();
        }
        if let Ok(mut fences) = self.session_fence.lock() {
            fences.clear();
        }
        self.metrics
            .last_value_continuity_breaks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// How long `record` (dispatched at `at`) has been its key's newest
    /// record: the larger of the time since this replica dispatched it and
    /// the time since the producer created it (a warm-start replica
    /// dispatches old records late; 0 = producer time unknown).
    fn quiet_age(at: Instant, record: &RawRecord) -> Duration {
        let dispatched = at.elapsed();
        if record.timestamp_ms <= 0 {
            return dispatched;
        }
        let produced_ms = (now_ns() / 1_000_000).saturating_sub(record.timestamp_ms);
        dispatched.max(Duration::from_millis(produced_ms.max(0) as u64))
    }

    /// The key's last-value entry when its ring view is empty, with the
    /// boundary and broker head proof copied under the same partition lock.
    /// `Ok(None)` = no entry (`HOT_RECORD_MISSING` as before).
    fn last_value_snapshot(
        &self,
        key: &[u8],
        deadline: Instant,
    ) -> Result<Option<LastValueView>, String> {
        if self.last_value_cap == 0 {
            return Ok(None);
        }
        let homes = lock_until(&self.value_home, deadline)?
            .get(key)
            .cloned()
            .unwrap_or_default();
        let mut found = None;
        let mut beyond_max = false;
        for partition in homes {
            let Some(state) = self.partitions.get(&partition) else {
                continue;
            };
            let (state, _hold) = profiled_lock!(HotSnapshot, lock_until(state, deadline));
            let state = state?;
            let Some(entry) = state.last_value.get(key) else {
                continue;
            };
            let copy = (
                entry.at,
                HotRecord {
                    raw: entry.raw.clone(),
                    boundary_offset: state.next_offset - 1,
                },
                state.broker_head,
                entry.fence.clone(),
            );
            drop(state);
            if Self::quiet_age(copy.0, &copy.1.raw) > self.config.max_quiet {
                beyond_max = true;
                continue;
            }
            // Two partitions with a current last value for one key break
            // the partition plan, exactly as two live ring views do.
            if found.is_some() {
                return Err("HOT_PARTITION_MISMATCH".into());
            }
            found = Some((copy.1, copy.2, copy.3));
        }
        if found.is_none() && beyond_max {
            self.metrics
                .last_value_quiet_refused
                .fetch_add(1, Ordering::Relaxed);
            return Err("HOT_QUIET_BEYOND_MAX".into());
        }
        Ok(found)
    }

    /// Validate a last value as the ring scan validates its newest entry:
    /// decodable, no gap/resync flag, the requested product, and still the
    /// newest connection generation and lease fence of its session.
    fn check_last_value(
        &self,
        hot: &HotRecord,
        feed: &str,
        fence: &SessionFence,
    ) -> Result<(), String> {
        use crate::requirement::is_product;
        use qdl_contracts::qdl::common::v1::QualityFlag;
        let raw = &hot.raw;
        if raw.offset < 0 || raw.offset > hot.boundary_offset {
            return Err("HOT_RECORD_INVALID".into());
        }
        let envelope =
            EventEnvelope::decode(raw.payload.as_slice()).map_err(|_| "HOT_RECORD_INVALID")?;
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
        if !is_product(feed, None, &envelope) {
            return Err("HOT_RECORD_PRODUCT_MISMATCH".into());
        }
        let (session, generation, lease) = fence;
        if !session.is_empty() {
            let newest = lock_until(
                &self.session_fence,
                Instant::now() + Duration::from_millis(5),
            )?
            .get(session)
            .copied();
            if newest != Some((*generation, *lease)) {
                return Err("HOT_RECORD_GENERATION_MISMATCH".into());
            }
        }
        Ok(())
    }

    /// Last-value entries and their raw bytes across partitions (metrics).
    pub fn last_value_usage(&self) -> (usize, usize) {
        self.partitions
            .values()
            .filter_map(|state| state.lock().ok())
            .fold((0, 0), |(keys, bytes), state| {
                (
                    keys + state.last_value.len(),
                    bytes + state.last_value_bytes,
                )
            })
    }

    /// Dispatch one committed record. A record at or below an already
    /// dispatched offset is a duplicate transport delivery and is ignored.
    pub fn dispatch(&self, record: RawRecord) {
        let (Some(partition), Some(gate)) = (
            self.partitions.get(&record.partition),
            self.dispatch_gates.get(&record.partition),
        ) else {
            return;
        };
        // A poisoned gate guards no data; keep ordering, never stop dispatch.
        let _gate = gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = Arc::new(record);
        // DL-DEF-2026-11-26-M: with the last-value index on, every record is
        // decoded once here (gate held, partition lock not yet taken) for its
        // session fence; subscribers then share this decode.
        let decoded = (self.last_value_cap > 0).then(|| decode_canonical(&record));
        let fence = match &decoded {
            Some(Ok(envelope)) => last_value_fence(envelope),
            _ => None,
        };
        if let Some((session, generation, lease)) = &fence {
            self.advance_session_fence(session, *generation, *lease);
        }
        let subscribers = {
            let (state, _hold) = profiled_lock!(Dispatch, partition.lock());
            let Ok(mut state) = state else {
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
            if let Some(entries) = state.by_key.get_mut(&record.key) {
                entries.push_back((now, record.clone()));
            } else {
                state
                    .by_key
                    .insert(record.key.clone(), VecDeque::from([(now, record.clone())]));
                self.enter_home(&record.key, record.partition);
            }
            state.next_offset = record.offset + 1;
            self.remember_last_value(&mut state, &record, now, fence);
            self.evict(record.partition, &mut state, now);
            // Registration is atomic with this boundary: a subscriber attached
            // at barrier <= offset is in this snapshot, one attached later has
            // barrier > offset and gets the record from the ring instead.
            let Some(subscribers) = state.subscribers.get(&record.key) else {
                return;
            };
            subscribers.clone()
        };
        #[cfg(feature = "lock-profile")]
        let _fanout = lock_profile::fanout();
        let budget = self.live_budget.get();
        let at = now_ns();
        let live = match decoded {
            Some(Ok(envelope)) => Ok(LiveRecord::from_envelope(
                record,
                envelope,
                budget,
                at.max(0) as u64,
            )),
            Some(Err(error)) => Err(error),
            None => LiveRecord::decode_charged(record, budget, at.max(0) as u64),
        };
        match live {
            Ok(live) => {
                if budget.is_some() && live.shared_charge.is_none() {
                    self.metrics
                        .live_budget_refused
                        .fetch_add(1, Ordering::Relaxed);
                }
                for subscriber in subscribers.iter() {
                    self.metrics.offers.fetch_add(1, Ordering::Relaxed);
                    match self.freshness_now_ns(at) {
                        Ok(data_at) => subscriber.offer(&live, data_at),
                        Err(error) => {
                            subscriber.fail(error);
                            subscriber.close();
                        }
                    }
                }
            }
            Err(error) => {
                self.metrics.decode_failures.fetch_add(1, Ordering::Relaxed);
                for subscriber in subscribers.iter() {
                    subscriber.fail(&error);
                }
            }
        }
    }

    /// The reader moved past records it never delivered (transaction markers,
    /// aborted batches): every offset below `position` is dispatched.
    pub fn advance(&self, partition: i32, position: i64) {
        if let Some(state) = self.partitions.get(&partition) {
            let (state, _hold) = profiled_lock!(Advance, state.lock());
            if let Ok(mut state) = state {
                if position > state.next_offset {
                    state.next_offset = position;
                    if state.ring.is_empty() {
                        state.ring_start = position;
                    }
                }
                self.evict(partition, &mut state, Instant::now());
            }
        }
    }

    /// Copy `key`'s newest ring entries, the dispatch boundary and the broker
    /// head proof of one partition in a single critical section.
    fn key_snapshot(
        &self,
        partition: i32,
        key: &[u8],
        deadline: Instant,
    ) -> Result<Option<KeySnapshot>, String> {
        let Some(state) = self.partitions.get(&partition) else {
            return Ok(None);
        };
        let (state, _hold) = profiled_lock!(HotSnapshot, lock_until(state, deadline));
        let state = state?;
        let Some(entries) = state.by_key.get(key) else {
            return Ok(None);
        };
        Ok(Some(KeySnapshot {
            partition,
            entries: entries.iter().rev().take(HOT_SCAN_CHUNK).cloned().collect(),
            boundary_offset: state.next_offset - 1,
            broker_head: state.broker_head,
        }))
    }

    /// The next chunk of `key`'s ring entries strictly below `before`, newest
    /// first. Records are immutable and only appended at the back or evicted
    /// at the front, so this continues the first snapshot exactly; an
    /// entry evicted meanwhile is simply absent (the scan then finds less).
    fn key_entries_before(
        &self,
        partition: i32,
        key: &[u8],
        before: i64,
        deadline: Instant,
    ) -> Result<Vec<(Instant, SharedRecord)>, String> {
        let Some(state) = self.partitions.get(&partition) else {
            return Ok(Vec::new());
        };
        let (state, _hold) = profiled_lock!(HotSnapshot, lock_until(state, deadline));
        let state = state?;
        let Some(entries) = state.by_key.get(key) else {
            return Ok(Vec::new());
        };
        let end = entries.partition_point(|(_, record)| record.offset < before);
        Ok(entries
            .range(end.saturating_sub(HOT_SCAN_CHUNK)..end)
            .rev()
            .cloned()
            .collect())
    }

    /// A bounded read of committed canonical bytes, not an execution-quality verdict.
    /// No Redis, Query, new Kafka reader, offset commit, or retained decoded cache.
    ///
    /// Touches only the partition(s) whose ring holds `key`; the critical
    /// section copies references, decoding and validation run outside it.
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
        let homes = lock_until(&self.key_home, deadline)?
            .get(key)
            .cloned()
            .unwrap_or_default();
        // A key with unexpired records in two partitions breaks the
        // partition plan; records past the ring age do not count.
        let mut live = None;
        for partition in homes {
            let Some(snapshot) = self.key_snapshot(partition, key, deadline)? else {
                continue;
            };
            let now = Instant::now();
            if snapshot
                .entries
                .first()
                .is_none_or(|(at, _)| now.duration_since(*at) > self.config.ring_max_age)
            {
                continue;
            }
            if live.is_some() {
                return Err("HOT_PARTITION_MISMATCH".into());
            }
            live = Some(snapshot);
        }
        let mut selected = None;
        let ring_view_empty = live.is_none();
        let mut from_last_value = false;
        if let Some(snapshot) = live {
            let mut newest_scope = None;
            let mut chunk = snapshot.entries;
            'scan: loop {
                for (at, raw) in &chunk {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err("HOT_READ_DEADLINE".into());
                    }
                    if now.duration_since(*at) > self.config.ring_max_age {
                        break 'scan;
                    }
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
                        Some(
                            crate::generated::marketdata_v2::event_envelope::Payload::BookDelta(
                                book,
                            ),
                        ) => {
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
                        selected = Some((
                            HotRecord {
                                raw: raw.clone(),
                                boundary_offset: snapshot.boundary_offset,
                            },
                            snapshot.broker_head,
                        ));
                        break 'scan;
                    }
                    if !(matches!(feed, "BOOK_SNAPSHOT" | "BOOK_DELTA")
                        && (is_product("BOOK_SNAPSHOT", None, &envelope)
                            || is_product("BOOK_DELTA", None, &envelope)))
                    {
                        return Err("HOT_RECORD_PRODUCT_MISMATCH".into());
                    }
                }
                let Some((_, oldest)) = chunk.last() else {
                    break;
                };
                if chunk.len() < HOT_SCAN_CHUNK {
                    break;
                }
                let before = oldest.offset;
                chunk = self.key_entries_before(snapshot.partition, key, before, deadline)?;
            }
        }
        // DL-DEF-2026-11-26-M: a quiet key evicted from the ring is answered
        // from its last value, under the same head/catch-up proof below.
        if selected.is_none() && ring_view_empty && !matches!(feed, "BOOK_SNAPSHOT" | "BOOK_DELTA")
        {
            if let Some((hot, head, fence)) = self.last_value_snapshot(key, deadline)? {
                if Instant::now() >= deadline {
                    return Err("HOT_READ_DEADLINE".into());
                }
                self.check_last_value(&hot, feed, &fence)?;
                selected = Some((hot, head));
                from_last_value = true;
            }
        }
        self.check_hot_reader()?;
        let (selected, head) = selected.ok_or_else(|| "HOT_RECORD_MISSING".to_owned())?;
        // The head proof was copied with the boundary, under one lock.
        let (head, confirmed) = head.ok_or("HOT_BROKER_UNCONFIRMED")?;
        if confirmed.elapsed() > Duration::from_secs(1) {
            return Err("HOT_BROKER_UNCONFIRMED".into());
        }
        if selected.boundary_offset + 1 < head {
            return Err("HOT_READER_CATCHING_UP".into());
        }
        if from_last_value {
            self.metrics.last_value_hits.fetch_add(1, Ordering::Relaxed);
        }
        Ok(selected)
    }

    /// Whether this replica's committed reader is caught up on `partition`
    /// now: it made progress within 1 s, and a broker head the broker
    /// confirmed within 1 s is not ahead of what it dispatched (the hot-read
    /// rules). A session-alive frame (DL v2.3.2) vouches only for a stream
    /// whose reader has seen everything committed up to that head.
    pub fn reader_caught_up(&self, partition: i32) -> Result<(), String> {
        self.check_hot_reader()?;
        let state = self
            .partitions
            .get(&partition)
            .ok_or("HOT_PARTITION_UNKNOWN")?
            .lock()
            .map_err(|_| "HOT_READER_UNAVAILABLE")?;
        let (head, confirmed) = state.broker_head.ok_or("HOT_BROKER_UNCONFIRMED")?;
        if confirmed.elapsed() > Duration::from_secs(1) {
            return Err("HOT_BROKER_UNCONFIRMED".into());
        }
        if state.next_offset < head {
            return Err("HOT_READER_CATCHING_UP".into());
        }
        Ok(())
    }

    fn mark_progress(&self, at: Instant) {
        let since = at.saturating_duration_since(self.origin).as_nanos();
        self.reader_progress.store(
            u64::try_from(since).unwrap_or(u64::MAX - 1) + 1,
            Ordering::Release,
        );
    }

    fn check_hot_reader(&self) -> Result<(), String> {
        if self.stop.load(Ordering::Acquire) || self.reader_failed.load(Ordering::Acquire) {
            return Err("HOT_READER_UNAVAILABLE".into());
        }
        let progress = self.reader_progress.load(Ordering::Acquire);
        let stalled = progress == 0
            || (self.origin + Duration::from_nanos(progress - 1)).elapsed()
                > Duration::from_secs(1);
        if stalled {
            return Err("HOT_READER_STALLED".into());
        }
        Ok(())
    }

    /// Record a positive broker head for `partition` if the reader has
    /// dispatched up to it; returns whether it was recorded. A head the
    /// reader has not reached yet never replaces the last caught-up proof.
    fn confirm_broker_head(&self, partition: i32, head: i64, confirmed: Instant) -> bool {
        let Some(state) = self.partitions.get(&partition) else {
            return false;
        };
        let Ok(mut state) = state.lock() else {
            return false;
        };
        if state.next_offset < head {
            return false;
        }
        state.broker_head = Some((head, confirmed));
        true
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
        let (state, _hold) = profiled_lock!(Register, state.lock());
        let mut state = state.map_err(|_| "partition state poisoned".to_owned())?;
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
        attach(&mut state.subscribers, key, subscriber);
        Ok(Registration { barrier, ring })
    }

    /// Retained replay without attaching a live subscriber. Coverage and the
    /// selected references are captured under the same partition lock.
    pub fn retained_replay(
        &self,
        partition: i32,
        key: &[u8],
        after: i64,
        barrier: i64,
        max_records: usize,
        budget: &Arc<ByteBudget>,
    ) -> Option<(Vec<SharedRecord>, Charge)> {
        let partition_state = self.partitions.get(&partition)?;
        let (state, _hold) = profiled_lock!(RetainedReplay, partition_state.lock());
        let mut state = state.ok()?;
        self.evict(partition, &mut state, Instant::now());
        if barrier > state.next_offset || after.checked_add(1)? < state.ring_start {
            return None;
        }
        let entries = state.by_key.get(key);
        let begin = entries.map_or(0, |rows| rows.partition_point(|(_, r)| r.offset <= after));
        let end = entries.map_or(0, |rows| rows.partition_point(|(_, r)| r.offset < barrier));
        if end.saturating_sub(begin) > max_records {
            return None;
        }
        let selected = || {
            entries
                .into_iter()
                .flat_map(|rows| rows.range(begin..end))
                .map(|(_, record)| record)
        };
        let mut count = 0usize;
        let mut bytes = 0usize;
        for record in selected() {
            count += 1;
            if count > max_records {
                return None;
            }
            bytes = bytes.checked_add(live_weight(record))?;
        }
        let charge = budget.charge(bytes)?;
        Some((selected().cloned().collect(), charge))
    }

    /// Capture a charged, bounded bridge and attach live atomically. The caller
    /// must retain the charge until the bridge is drained and preserve one
    /// cumulative replay deadline/budget across unsuccessful attempts.
    pub fn try_register_near(
        &self,
        partition: i32,
        key: &[u8],
        subscriber: Arc<Subscription>,
        after: i64,
        max_bridge_records: usize,
        replay_budget: &Arc<ByteBudget>,
    ) -> Result<NearRegistration, String> {
        let state = self
            .partitions
            .get(&partition)
            .ok_or_else(|| format!("partition {partition} is not served by this replica"))?;
        let (state, _hold) = profiled_lock!(RegisterNear, state.lock());
        let mut state = state.map_err(|_| "partition state poisoned".to_owned())?;
        self.evict(partition, &mut state, Instant::now());
        let barrier = state.next_offset;
        let start = after.checked_add(1).ok_or("replay offset overflow")?;
        // A cursor ahead of this replica requires catch-up, never registration
        // against a lower barrier. Missing key alone cannot establish coverage.
        if start > barrier || (start < barrier && start < state.ring_start) {
            return Ok(NearRegistration::NeedReplay { barrier });
        }
        let entries = state.by_key.get(key);
        let begin = entries.map_or(0, |rows| rows.partition_point(|(_, r)| r.offset <= after));
        let end = entries.map_or(0, VecDeque::len);
        if end.saturating_sub(begin) > max_bridge_records {
            return Ok(NearRegistration::NeedReplay { barrier });
        }
        let selected = || {
            entries
                .into_iter()
                .flat_map(|rows| rows.range(begin..end))
                .map(|(_, record)| record)
        };
        let mut count = 0usize;
        let mut bytes = 0usize;
        for record in selected() {
            count += 1;
            if count > max_bridge_records {
                return Ok(NearRegistration::NeedReplay { barrier });
            }
            bytes = bytes
                .checked_add(live_weight(record))
                .ok_or("replay bridge size overflow")?;
        }
        let Some(charge) = replay_budget.charge(bytes) else {
            return Ok(NearRegistration::NeedReplay { barrier });
        };
        let bridge = selected().cloned().collect();
        attach(&mut state.subscribers, key, subscriber);
        Ok(NearRegistration::Registered {
            barrier,
            bridge,
            charge,
        })
    }

    pub fn unregister(&self, partition: i32, key: &[u8], id: u64) {
        if let Some(state) = self.partitions.get(&partition) {
            let (state, _hold) = profiled_lock!(Unregister, state.lock());
            if let Ok(mut state) = state {
                if let Some(list) = state.subscribers.get(key) {
                    let kept: Vec<_> = list
                        .iter()
                        .filter(|subscriber| subscriber.id != id)
                        .cloned()
                        .collect();
                    if kept.is_empty() {
                        state.subscribers.remove(key);
                    } else if kept.len() != list.len() {
                        state.subscribers.insert(key.to_vec(), kept.into());
                    }
                }
            }
        }
    }

    pub fn subscriber_count(&self) -> usize {
        self.partitions
            .values()
            .filter_map(|state| state.lock().ok())
            .map(|state| {
                state
                    .subscribers
                    .values()
                    .map(|list| list.len())
                    .sum::<usize>()
            })
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
                let mut failed_at: Option<Instant> = None;
                while !hub.stop.load(Ordering::Relaxed) {
                    let polled = source.poll(Duration::from_millis(100));
                    match polled {
                        Ok(Some(record)) => {
                            hub.dispatch(record);
                            hub.reader_failed.store(false, Ordering::Release);
                            hub.mark_progress(Instant::now());
                        }
                        Ok(None) => {
                            hub.mark_progress(Instant::now());
                            for (partition, position) in source.positions() {
                                hub.advance(partition, position);
                            }
                        }
                        Err(_) => {
                            failed_at = Some(Instant::now());
                            hub.reader_failed.store(true, Ordering::Release);
                            hub.break_last_value_continuity();
                            hub.metrics.reader_errors.fetch_add(1, Ordering::Relaxed);
                            std::thread::sleep(Duration::from_millis(100));
                        }
                    }
                    // Keep the last caught-up proof until its original expiry.
                    // A newer broker head still in flight must not erase it.
                    let proofs: Vec<_> = source
                        .broker_heads()
                        .into_iter()
                        .filter(|(partition, head, confirmed)| {
                            hub.confirm_broker_head(*partition, *head, *confirmed)
                        })
                        .collect();
                    if failed_at.is_some_and(|failed| proofs.iter().any(|(_, _, at)| *at > failed))
                    {
                        hub.reader_failed.store(false, Ordering::Release);
                        failed_at = None;
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
        hub.mark_progress(Instant::now());
        for partition in hub.partitions.keys() {
            set_head(hub, *partition, Some((0, Instant::now())));
        }
    }

    /// Install a head proof directly, bypassing the caught-up filter.
    fn set_head(hub: &Hub, partition: i32, head: Option<(i64, Instant)>) {
        hub.partitions[&partition].lock().unwrap().broker_head = head;
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
    fn hot_read_requires_positive_broker_proof_and_catchup_not_poll_activity() {
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        hub.dispatch(hot_record(0, 0, "QUOTE", 1, vec![]));
        healthy(&hub);
        set_head(&hub, 0, None);
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_BROKER_UNCONFIRMED");
        set_head(&hub, 0, Some((1, Instant::now() - Duration::from_secs(2))));
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_BROKER_UNCONFIRMED");
        set_head(&hub, 0, Some((2, Instant::now())));
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_READER_CATCHING_UP");
        // The reader path records only a head it has reached.
        assert!(!hub.confirm_broker_head(0, 3, Instant::now()));
        assert!(hub.confirm_broker_head(0, 1, Instant::now()));
        hub.advance(0, 2);
        assert_eq!(latest(&hub, "QUOTE").unwrap().raw.offset, 0);
    }

    #[test]
    fn quiet_reader_recovers_only_after_broker_confirmation_newer_than_error() {
        struct Source {
            failed: bool,
            proof: Arc<Mutex<Instant>>,
        }
        impl LogSource for Source {
            fn poll(&mut self, _: Duration) -> Result<Option<RawRecord>, String> {
                if !self.failed {
                    self.failed = true;
                    return Err("disconnected".into());
                }
                std::thread::sleep(Duration::from_millis(5));
                Ok(None)
            }
            fn positions(&self) -> Vec<(i32, i64)> {
                vec![(0, 1)]
            }
            fn broker_heads(&self) -> Vec<(i32, i64, Instant)> {
                vec![(0, 1, *self.proof.lock().unwrap())]
            }
        }
        let hub = Arc::new(Hub::new(&[(0, 0)], HubConfig::default()));
        hub.dispatch(hot_record(0, 0, "QUOTE", 1, vec![]));
        healthy(&hub);
        let proof = Arc::new(Mutex::new(Instant::now()));
        let reader = hub.run(Box::new(Source {
            failed: false,
            proof: proof.clone(),
        }));
        let deadline = Instant::now() + Duration::from_secs(10); // load headroom (DL v2.4.0 CI6)
        while !hub.reader_failed.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_READER_UNAVAILABLE");
        *proof.lock().unwrap() = Instant::now();
        while hub.reader_failed.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(latest(&hub, "QUOTE").unwrap().raw.offset, 0);
        hub.stop();
        reader.join().unwrap();
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
        hub.mark_progress(Instant::now() - Duration::from_secs(2));
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_READER_STALLED");
        healthy(&hub);
        assert_eq!(
            hub.latest_hot(b"hot", "QUOTE", Instant::now()).unwrap_err(),
            "HOT_READ_DEADLINE"
        );
        hub.stop();
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_READER_UNAVAILABLE");
        // Ring-only semantics (index disabled): an expired ring view is missing.
        let hub = Hub::new(
            &[(0, 0)],
            HubConfig {
                ring_max_age: Duration::ZERO,
                last_value_max_keys: 0,
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
                last_value_max_keys: 0,
                ..Default::default()
            },
        );
        hub.dispatch(hot_record(0, 0, "QUOTE", 1, vec![]));
        healthy(&hub);
        assert_eq!(hub.ring_bytes(), 0);
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_RECORD_MISSING");
        // The per-key index and the directory are evicted with the ring.
        assert!(hub.partitions[&0].lock().unwrap().by_key.is_empty());
        assert!(hub.key_home.lock().unwrap().is_empty());
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        hub.dispatch(hot_record(0, 0, "QUOTE", 1, vec![]));
        healthy(&hub);
        // The key's own partition held past the deadline: BUSY at the deadline.
        let _lock = hub.partitions[&0].lock().unwrap();
        let started = Instant::now();
        let budget = Duration::from_millis(30);
        assert_eq!(
            hub.latest_hot(b"hot", "QUOTE", started + budget)
                .unwrap_err(),
            "HOT_READER_BUSY"
        );
        let waited = started.elapsed();
        assert!(waited >= budget, "{waited:?}");
        assert!(waited < budget + Duration::from_millis(50), "{waited:?}");
    }

    fn keyed(partition: i32, offset: i64, key: &[u8]) -> RawRecord {
        RawRecord {
            key: key.to_vec(),
            ..hot_record(partition, offset, "QUOTE", 1, vec![])
        }
    }

    #[test]
    fn hot_read_ignores_unrelated_partition_lock_and_quiet_product_skips_busy_ones() {
        let hub = Hub::new(&[(0, 0), (1, 0)], HubConfig::default());
        hub.dispatch(keyed(0, 0, b"quiet"));
        for offset in 1..5_001 {
            hub.dispatch(keyed(0, offset, b"busy"));
        }
        hub.dispatch(keyed(1, 0, b"other"));
        healthy(&hub);
        let quiet = hub
            .latest_hot(b"quiet", "QUOTE", Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert_eq!((quiet.raw.offset, quiet.boundary_offset), (0, 5_000));
        // An unrelated partition held by someone else does not refuse.
        let _held = hub.partitions[&1].lock().unwrap();
        let busy = hub
            .latest_hot(b"busy", "QUOTE", Instant::now() + Duration::from_millis(50))
            .unwrap();
        assert_eq!(busy.raw.offset, 5_000);
    }

    #[test]
    fn book_snapshot_behind_many_deltas_is_found_across_chunks_and_gaps_still_refuse() {
        for deltas in [
            HOT_SCAN_CHUNK - 1,
            HOT_SCAN_CHUNK,
            HOT_SCAN_CHUNK + 1,
            3 * HOT_SCAN_CHUNK + 7,
        ] {
            let hub = Hub::new(&[(0, 0)], HubConfig::default());
            hub.dispatch(hot_record(0, 0, "BOOK_SNAPSHOT", 1, vec![]));
            for offset in 1..=deltas as i64 {
                hub.dispatch(hot_record(0, offset, "BOOK_DELTA", 1, vec![]));
            }
            healthy(&hub);
            let view = latest(&hub, "BOOK_SNAPSHOT").unwrap();
            assert_eq!((view.raw.offset, view.boundary_offset), (0, deltas as i64));
            assert_eq!(
                latest(&hub, "BOOK_DELTA").unwrap().raw.offset,
                deltas as i64
            );
        }
        // An unsafe sibling in a later chunk still refuses the old snapshot.
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        hub.dispatch(hot_record(0, 0, "BOOK_SNAPSHOT", 1, vec![]));
        hub.dispatch(hot_record(0, 1, "BOOK_DELTA", 1, vec![16]));
        for offset in 2..(2 * HOT_SCAN_CHUNK as i64) {
            hub.dispatch(hot_record(0, offset, "BOOK_DELTA", 1, vec![]));
        }
        healthy(&hub);
        assert_eq!(
            latest(&hub, "BOOK_SNAPSHOT").unwrap_err(),
            "HOT_RECORD_GAPPED"
        );
    }

    #[test]
    fn key_moving_partition_after_ring_expiry_is_not_a_mismatch() {
        let hub = Hub::new(
            &[(0, 0), (1, 0)],
            HubConfig {
                ring_max_age: Duration::from_millis(30),
                ..Default::default()
            },
        );
        hub.dispatch(keyed(0, 0, b"hot"));
        healthy(&hub);
        std::thread::sleep(Duration::from_millis(40));
        // Partition 0 still holds the aged record (no eviction ran there).
        hub.dispatch(keyed(1, 0, b"hot"));
        assert_eq!(hub.key_home.lock().unwrap()[&b"hot".to_vec()].len(), 2);
        assert_eq!(latest(&hub, "QUOTE").unwrap().raw.partition, 1);
        hub.advance(0, 1);
        assert_eq!(hub.key_home.lock().unwrap()[&b"hot".to_vec()], vec![1]);
        hub.dispatch(keyed(0, 1, b"hot"));
        assert_eq!(latest(&hub, "QUOTE").unwrap_err(), "HOT_PARTITION_MISMATCH");
    }

    #[test]
    fn hot_read_snapshot_is_one_critical_section_under_concurrent_dispatch() {
        // Lookups race a dispatcher that confirms the broker head right after
        // each record. Every reply must be a dispatched record of the key at
        // or below its boundary, the boundary must cover the head proof, and
        // per-thread results never go backwards. No reply may be BUSY.
        let hub = Arc::new(Hub::new(&[(0, 0), (1, 0)], HubConfig::default()));
        hub.dispatch(keyed(0, 0, b"k0"));
        hub.dispatch(keyed(1, 0, b"k1"));
        healthy(&hub);
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (hub, stop) = (hub.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut offset = 1;
                while !stop.load(Ordering::Relaxed) {
                    for partition in [0, 1] {
                        let key = if partition == 0 { b"k0" } else { b"k1" };
                        hub.dispatch(keyed(partition, offset, key));
                        assert!(hub.confirm_broker_head(partition, offset + 1, Instant::now()));
                    }
                    hub.mark_progress(Instant::now());
                    offset += 1;
                }
                offset
            })
        };
        let readers: Vec<_> = (0..8)
            .map(|index| {
                let hub = hub.clone();
                std::thread::spawn(move || {
                    let key: &[u8] = if index % 2 == 0 { b"k0" } else { b"k1" };
                    let (mut ok, mut last) = (0, -1);
                    let until = Instant::now() + Duration::from_millis(300);
                    while Instant::now() < until {
                        let deadline = Instant::now() + Duration::from_millis(100);
                        let view = hub.latest_hot(key, "QUOTE", deadline).unwrap();
                        assert_eq!(view.raw.key, key);
                        assert!(view.raw.offset <= view.boundary_offset);
                        assert!(view.raw.offset >= last, "went backwards");
                        assert!(
                            view.boundary_offset < hub.next_offset(view.raw.partition).unwrap()
                        );
                        last = view.raw.offset;
                        ok += 1;
                    }
                    ok
                })
            })
            .collect();
        let total: usize = readers.into_iter().map(|r| r.join().unwrap()).sum();
        stop.store(true, Ordering::Relaxed);
        assert!(writer.join().unwrap() > 1);
        assert!(total > 0);
    }

    // ------------------------------- DL-DEF-2026-11-26-M quiet-key last value

    fn now_ms() -> i64 {
        now_ns() / 1_000_000
    }

    /// A QUOTE of `key` produced now, in `session` at `(generation, lease)`.
    fn quote(
        partition: i32,
        offset: i64,
        key: &[u8],
        session: &str,
        generation: u64,
        lease: u64,
        flags: Vec<i32>,
    ) -> RawRecord {
        use crate::generated::marketdata_v2::{event_envelope::Payload, Quote};
        RawRecord {
            partition,
            offset,
            key: key.to_vec(),
            timestamp_ms: now_ms(),
            payload: EventEnvelope {
                payload: Some(Payload::Quote(Quote::default())),
                source_session_id: session.into(),
                connection_generation: generation,
                lease_epoch: lease,
                quality_flags: flags,
                ..Default::default()
            }
            .encode_to_vec(),
        }
    }

    /// Every record leaves the ring at once (the ~30 s busy-partition
    /// coverage of the 2026-11-26 RCA, shrunk to zero).
    fn tiny_ring(partitions: &[(i32, i64)], config: HubConfig) -> Hub {
        Hub::new(
            partitions,
            HubConfig {
                ring_max_bytes: 1,
                ..config
            },
        )
    }

    fn lookup(hub: &Hub, key: &[u8]) -> Result<HotRecord, String> {
        hub.latest_hot(key, "QUOTE", Instant::now() + Duration::from_secs(1))
    }

    #[test]
    fn an_evicted_quiet_key_is_answered_from_its_last_value() {
        let hub = tiny_ring(&[(0, 0)], HubConfig::default());
        hub.dispatch(quote(0, 0, b"ont", "okx-1", 7, 0, vec![]));
        // Busy keys of other products keep the partition moving.
        for offset in 1..50 {
            hub.dispatch(quote(0, offset, b"busy", "okx-1", 7, 0, vec![]));
        }
        healthy(&hub);
        set_head(&hub, 0, Some((50, Instant::now())));
        assert_eq!(hub.ring_bytes(), 0, "the ring holds nothing");
        assert!(hub.partitions[&0].lock().unwrap().by_key.is_empty());
        let view = lookup(&hub, b"ont").unwrap();
        assert_eq!((view.raw.offset, view.boundary_offset), (0, 49));
        // Transaction markers after it move the boundary, not the record.
        hub.advance(0, 60);
        set_head(&hub, 0, Some((60, Instant::now())));
        let view = lookup(&hub, b"ont").unwrap();
        assert_eq!((view.raw.offset, view.boundary_offset), (0, 59));
        assert_eq!(hub.metrics.last_value_hits.load(Ordering::Relaxed), 2);
        let (keys, bytes) = hub.last_value_usage();
        assert_eq!(keys, 2);
        assert!(bytes <= 2 * LAST_VALUE_MAX_RECORD_BYTES);
        // The same lookup without the index is today's refusal.
        let ring_only = tiny_ring(
            &[(0, 0)],
            HubConfig {
                last_value_max_keys: 0,
                ..Default::default()
            },
        );
        ring_only.dispatch(quote(0, 0, b"ont", "okx-1", 7, 0, vec![]));
        healthy(&ring_only);
        assert_eq!(
            lookup(&ring_only, b"ont").unwrap_err(),
            "HOT_RECORD_MISSING"
        );
    }

    #[test]
    fn a_last_value_is_only_ever_the_newest_record_of_its_key() {
        let hub = tiny_ring(&[(0, 0)], HubConfig::default());
        hub.dispatch(quote(0, 0, b"ont", "okx-1", 7, 0, vec![]));
        hub.dispatch(quote(0, 1, b"ont", "okx-1", 7, 0, vec![]));
        healthy(&hub);
        set_head(&hub, 0, Some((2, Instant::now())));
        assert_eq!(
            lookup(&hub, b"ont").unwrap().raw.offset,
            1,
            "newer offset wins"
        );
        // A newer gapped record refuses; the older healthy one is never served.
        hub.dispatch(quote(0, 2, b"ont", "okx-1", 7, 0, vec![16]));
        set_head(&hub, 0, Some((3, Instant::now())));
        assert_eq!(lookup(&hub, b"ont").unwrap_err(), "HOT_RECORD_GAPPED");
        // A newer record that cannot be kept (oversize, another product)
        // drops the entry instead of leaving an older one in place.
        let mut oversize = quote(0, 3, b"ont", "okx-1", 7, 0, vec![]);
        let big = {
            use crate::generated::marketdata_v2::{event_envelope::Payload, Quote};
            EventEnvelope {
                payload: Some(Payload::Quote(Quote::default())),
                source_session_id: "x".repeat(LAST_VALUE_MAX_RECORD_BYTES),
                ..Default::default()
            }
            .encode_to_vec()
        };
        oversize.payload = big;
        hub.dispatch(oversize);
        set_head(&hub, 0, Some((4, Instant::now())));
        assert_eq!(lookup(&hub, b"ont").unwrap_err(), "HOT_RECORD_MISSING");
        hub.dispatch(quote(0, 4, b"ont", "okx-1", 7, 0, vec![]));
        let mut state_event = hot_record(0, 5, "TRADE", 7, vec![]);
        state_event.key = b"ont".to_vec();
        state_event.timestamp_ms = now_ms();
        hub.dispatch(state_event);
        set_head(&hub, 0, Some((6, Instant::now())));
        // TRADE on the QUOTE key is kept, and is the wrong product.
        assert_eq!(
            lookup(&hub, b"ont").unwrap_err(),
            "HOT_RECORD_PRODUCT_MISMATCH"
        );
        let mut book = hot_record(0, 6, "BOOK_DELTA", 7, vec![]);
        book.key = b"ont".to_vec();
        hub.dispatch(book);
        set_head(&hub, 0, Some((7, Instant::now())));
        assert_eq!(lookup(&hub, b"ont").unwrap_err(), "HOT_RECORD_MISSING");
        assert!(hub.metrics.last_value_unindexed.load(Ordering::Relaxed) >= 2);
    }

    #[test]
    fn a_last_value_refuses_without_current_head_generation_fence_or_continuity() {
        let hub = tiny_ring(&[(0, 0), (1, 0)], HubConfig::default());
        hub.dispatch(quote(0, 0, b"ont", "okx-1", 7, 0, vec![]));
        healthy(&hub);
        // High-watermark proof: missing, stale, then ahead of dispatch.
        set_head(&hub, 0, None);
        assert_eq!(lookup(&hub, b"ont").unwrap_err(), "HOT_BROKER_UNCONFIRMED");
        set_head(&hub, 0, Some((1, Instant::now() - Duration::from_secs(2))));
        assert_eq!(lookup(&hub, b"ont").unwrap_err(), "HOT_BROKER_UNCONFIRMED");
        set_head(&hub, 0, Some((5, Instant::now())));
        assert_eq!(lookup(&hub, b"ont").unwrap_err(), "HOT_READER_CATCHING_UP");
        set_head(&hub, 0, Some((1, Instant::now())));
        assert_eq!(lookup(&hub, b"ont").unwrap().raw.offset, 0);
        hub.mark_progress(Instant::now() - Duration::from_secs(2));
        assert_eq!(lookup(&hub, b"ont").unwrap_err(), "HOT_READER_STALLED");
        healthy(&hub);
        set_head(&hub, 0, Some((1, Instant::now())));
        // The session reconnected (generation 8) on another key and partition.
        hub.dispatch(quote(1, 0, b"band", "okx-1", 8, 0, vec![]));
        assert_eq!(
            lookup(&hub, b"ont").unwrap_err(),
            "HOT_RECORD_GENERATION_MISMATCH"
        );
        // An older generation arriving late never moves the fence back.
        hub.dispatch(quote(1, 1, b"neo", "okx-1", 7, 0, vec![]));
        assert_eq!(
            lookup(&hub, b"ont").unwrap_err(),
            "HOT_RECORD_GENERATION_MISMATCH"
        );
        // ONT's own next BBO in generation 8 is served; a new lease fence refuses it.
        hub.dispatch(quote(0, 1, b"ont", "okx-1", 8, 0, vec![]));
        set_head(&hub, 0, Some((2, Instant::now())));
        assert_eq!(lookup(&hub, b"ont").unwrap().raw.offset, 1);
        hub.dispatch(quote(1, 2, b"band", "okx-1", 8, 3, vec![]));
        assert_eq!(
            lookup(&hub, b"ont").unwrap_err(),
            "HOT_RECORD_GENERATION_MISMATCH"
        );
        hub.dispatch(quote(0, 2, b"ont", "okx-1", 8, 3, vec![]));
        set_head(&hub, 0, Some((3, Instant::now())));
        assert_eq!(lookup(&hub, b"ont").unwrap().raw.offset, 2);
        // A reader error breaks continuity: every last value is dropped.
        hub.break_last_value_continuity();
        assert_eq!(hub.last_value_usage(), (0, 0));
        assert_eq!(lookup(&hub, b"ont").unwrap_err(), "HOT_RECORD_MISSING");
        assert_eq!(
            hub.metrics
                .last_value_continuity_breaks
                .load(Ordering::Relaxed),
            1
        );
        // A key holding a current last value on two partitions is refused.
        hub.dispatch(quote(0, 3, b"ont", "okx-1", 8, 3, vec![]));
        hub.dispatch(quote(1, 3, b"ont", "okx-1", 8, 3, vec![]));
        set_head(&hub, 0, Some((4, Instant::now())));
        set_head(&hub, 1, Some((4, Instant::now())));
        assert_eq!(lookup(&hub, b"ont").unwrap_err(), "HOT_PARTITION_MISMATCH");
    }

    #[test]
    fn the_reader_error_path_clears_last_values() {
        struct Failing(bool);
        impl LogSource for Failing {
            fn poll(&mut self, _: Duration) -> Result<Option<RawRecord>, String> {
                if !self.0 {
                    self.0 = true;
                    return Err("disconnected".into());
                }
                std::thread::sleep(Duration::from_millis(5));
                Ok(None)
            }
            fn positions(&self) -> Vec<(i32, i64)> {
                Vec::new()
            }
        }
        let hub = Arc::new(tiny_ring(&[(0, 0)], HubConfig::default()));
        hub.dispatch(quote(0, 0, b"ont", "okx-1", 7, 0, vec![]));
        assert_eq!(hub.last_value_usage().0, 1);
        let reader = hub.run(Box::new(Failing(false)));
        let deadline = Instant::now() + Duration::from_secs(10);
        while hub
            .metrics
            .last_value_continuity_breaks
            .load(Ordering::Relaxed)
            == 0
        {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        hub.stop();
        reader.join().unwrap();
        assert_eq!(hub.last_value_usage(), (0, 0));
    }

    #[test]
    fn a_key_quiet_beyond_the_maximum_is_refused_typed() {
        let config = HubConfig {
            max_quiet: Duration::from_millis(300),
            ..Default::default()
        };
        let hub = tiny_ring(&[(0, 0)], config.clone());
        hub.dispatch(quote(0, 0, b"ont", "okx-1", 7, 0, vec![]));
        healthy(&hub);
        set_head(&hub, 0, Some((1, Instant::now())));
        assert_eq!(lookup(&hub, b"ont").unwrap().raw.offset, 0);
        std::thread::sleep(Duration::from_millis(400));
        set_head(&hub, 0, Some((1, Instant::now())));
        hub.mark_progress(Instant::now());
        assert_eq!(lookup(&hub, b"ont").unwrap_err(), "HOT_QUIET_BEYOND_MAX");
        assert_eq!(
            hub.metrics.last_value_quiet_refused.load(Ordering::Relaxed),
            1
        );
        // A warm-start replica dispatches an old record now: the producer
        // time still counts, so it is refused at once.
        let hub = tiny_ring(&[(0, 0)], config);
        let mut old = quote(0, 0, b"ont", "okx-1", 7, 0, vec![]);
        old.timestamp_ms = now_ms() - 1_000;
        hub.dispatch(old);
        healthy(&hub);
        set_head(&hub, 0, Some((1, Instant::now())));
        assert_eq!(lookup(&hub, b"ont").unwrap_err(), "HOT_QUIET_BEYOND_MAX");
    }

    #[test]
    fn the_last_value_index_is_bounded_by_keys_not_ring_bytes() {
        let hub = tiny_ring(
            &[(0, 0), (1, 0)],
            HubConfig {
                last_value_max_keys: 8,
                ..Default::default()
            },
        );
        assert_eq!(hub.last_value_cap, 4, "split evenly over partitions");
        for offset in 0..100 {
            let key = format!("k{offset}");
            hub.dispatch(quote(0, offset, key.as_bytes(), "okx-1", 7, 0, vec![]));
            hub.dispatch(quote(1, offset, key.as_bytes(), "okx-1", 7, 0, vec![]));
            let (keys, bytes) = hub.last_value_usage();
            assert!(keys <= 8, "{keys}");
            assert!(bytes <= 8 * LAST_VALUE_MAX_RECORD_BYTES, "{bytes}");
        }
        assert_eq!(hub.last_value_usage().0, 8);
        assert!(hub.value_home.lock().unwrap().len() <= 8);
        assert_eq!(
            hub.metrics.last_value_evictions.load(Ordering::Relaxed),
            2 * 96
        );
        healthy(&hub);
        set_head(&hub, 0, Some((100, Instant::now())));
        set_head(&hub, 1, Some((100, Instant::now())));
        // The least recently dispatched keys left first; the newest stay.
        assert_eq!(lookup(&hub, b"k0").unwrap_err(), "HOT_RECORD_MISSING");
        assert_eq!(lookup(&hub, b"k99").unwrap_err(), "HOT_PARTITION_MISMATCH");
        // Entries quiet past the maximum are dropped before any current one.
        let hub = tiny_ring(
            &[(0, 0)],
            HubConfig {
                last_value_max_keys: 2,
                max_quiet: Duration::from_millis(300),
                ..Default::default()
            },
        );
        hub.dispatch(quote(0, 0, b"old", "okx-1", 7, 0, vec![]));
        std::thread::sleep(Duration::from_millis(400));
        hub.dispatch(quote(0, 1, b"fresh", "okx-1", 7, 0, vec![]));
        hub.dispatch(quote(0, 2, b"new", "okx-1", 7, 0, vec![]));
        healthy(&hub);
        set_head(&hub, 0, Some((3, Instant::now())));
        assert_eq!(lookup(&hub, b"fresh").unwrap().raw.offset, 1);
        assert_eq!(lookup(&hub, b"new").unwrap().raw.offset, 2);
        assert_eq!(lookup(&hub, b"old").unwrap_err(), "HOT_RECORD_MISSING");
    }

    #[test]
    fn book_feeds_never_answer_from_a_last_value() {
        let hub = tiny_ring(&[(0, 0)], HubConfig::default());
        hub.dispatch(hot_record(0, 0, "BOOK_SNAPSHOT", 1, vec![]));
        healthy(&hub);
        set_head(&hub, 0, Some((1, Instant::now())));
        assert_eq!(
            latest(&hub, "BOOK_SNAPSHOT").unwrap_err(),
            "HOT_RECORD_MISSING"
        );
        assert_eq!(hub.last_value_usage().0, 0);
    }

    // ----------------------------------------------- live budget charge-once

    use crate::generated::query_v2 as query;
    use crate::requirement::StreamRequirement;
    use crate::subscription::{Next, QUEUE_ENTRY_OVERHEAD};

    fn trade_requirement() -> StreamRequirement {
        StreamRequirement::from_proto(&query::DataRequirement {
            instrument_uid: "u".into(),
            source_policy_id: "p".into(),
            require_full_coverage: true,
            require_final_bars: true,
            feed_type: query::FeedType::Trade as i32,
            grade: query::ConsumerGrade::Alpha as i32,
            stale_policy_type: query::StalePolicy::Block as i32,
            gap_policy_type: query::GapPolicy::Block as i32,
            recovery_policy: query::RecoveryPolicy::SnapshotAndReplay as i32,
            revision_policy: query::BarRevisionPolicy::Latest as i32,
            ..Default::default()
        })
        .expect("requirement")
    }

    /// A lossless TRADE whose decoded weight dwarfs the queue entry overhead.
    fn big_trade(key: &[u8], offset: i64) -> RawRecord {
        use crate::generated::marketdata_v2::{event_envelope::Payload, Trade};
        RawRecord {
            partition: 0,
            offset,
            key: key.to_vec(),
            timestamp_ms: 1,
            payload: EventEnvelope {
                payload: Some(Payload::Trade(Trade {
                    native_trade_id: "x".repeat(4_000),
                    ..Default::default()
                })),
                ..Default::default()
            }
            .encode_to_vec(),
        }
    }

    fn big_weight() -> usize {
        live_weight(&big_trade(b"fan", 0))
    }

    fn subscribe(
        hub: &Hub,
        key: &[u8],
        id: u64,
        depth: usize,
        max_bytes: usize,
        budget: &Arc<ByteBudget>,
    ) -> Arc<Subscription> {
        let subscription = Arc::new(Subscription::new(
            id,
            "c".into(),
            trade_requirement(),
            depth,
            max_bytes,
            budget.clone(),
            -1,
        ));
        hub.register(0, key, subscription.clone(), -1).unwrap();
        subscription
    }

    #[tokio::test]
    async fn a_record_fanned_out_to_four_subscribers_is_charged_once() {
        let weight = big_weight();
        assert!(weight > 8 * QUEUE_ENTRY_OVERHEAD, "{weight}");
        let budget = ByteBudget::new(1 << 20);
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        assert!(hub.attach_live_budget(budget.clone()));
        assert!(!hub.attach_live_budget(ByteBudget::new(1)), "first wins");
        let subscribers: Vec<_> = (0..4)
            .map(|id| subscribe(&hub, b"fan", id, 16, 1 << 20, &budget))
            .collect();
        hub.dispatch(big_trade(b"fan", 0));
        assert_eq!(budget.used(), weight + 4 * QUEUE_ENTRY_OVERHEAD);
        for subscriber in &subscribers {
            // The per-subscription cap still counts the full weight.
            assert_eq!(subscriber.queued(), (1, weight));
        }
        assert_eq!(hub.metrics.live_budget_refused.load(Ordering::Relaxed), 0);
        // Three subscribers hand their copy to the transport and finish.
        for subscriber in &subscribers[..3] {
            match subscriber.next().await {
                Next::Record(record, _, charge) => {
                    assert_eq!(charge.bytes(), QUEUE_ENTRY_OVERHEAD);
                    assert!(record.shared_charge.is_some());
                }
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(budget.used(), weight + QUEUE_ENTRY_OVERHEAD);
        // The last holder keeps the shared charge until it drops the record.
        let (record, charge) = match subscribers[3].next().await {
            Next::Record(record, _, charge) => (record, charge),
            other => panic!("{other:?}"),
        };
        drop(charge);
        assert_eq!(budget.used(), weight);
        drop(record);
        assert_eq!(budget.used(), 0, "released exactly once");
    }

    #[tokio::test]
    async fn a_budget_for_one_decoded_record_delivers_it_to_all_four() {
        let weight = big_weight();
        let total = weight + 4 * QUEUE_ENTRY_OVERHEAD;
        assert!(total < 2 * weight, "fits one decoded copy, not four");
        let budget = ByteBudget::new(total);
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        hub.attach_live_budget(budget.clone());
        let subscribers: Vec<_> = (0..4)
            .map(|id| subscribe(&hub, b"fan", id, 16, 1 << 20, &budget))
            .collect();
        hub.dispatch(big_trade(b"fan", 0));
        assert_eq!(budget.used(), total);
        for subscriber in &subscribers {
            assert!(matches!(subscriber.next().await, Next::Record(..)));
        }
        assert_eq!(budget.used(), 0);

        // Before: every subscriber charged the full weight; three of four
        // overflowed with an empty queue (the 2026-10-10 17:22:48Z shape).
        let budget = ByteBudget::new(total);
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        let subscribers: Vec<_> = (0..4)
            .map(|id| subscribe(&hub, b"fan", id, 16, 1 << 20, &budget))
            .collect();
        hub.dispatch(big_trade(b"fan", 0));
        let mut overflowed = 0;
        for subscriber in &subscribers {
            if matches!(subscriber.next().await, Next::Overflow) {
                overflowed += 1;
                assert_eq!(subscriber.queued(), (0, 0));
            }
        }
        assert_eq!(overflowed, 3);
    }

    #[tokio::test]
    async fn a_refused_dispatch_charge_falls_back_to_full_per_subscriber_charges() {
        // Real exhaustion: the decoded copy itself does not fit. Lossless
        // subscribers overflow typed, as before; nothing leaks.
        let budget = ByteBudget::new(4 * QUEUE_ENTRY_OVERHEAD);
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        hub.attach_live_budget(budget.clone());
        let subscribers: Vec<_> = (0..2)
            .map(|id| subscribe(&hub, b"fan", id, 16, 1 << 20, &budget))
            .collect();
        hub.dispatch(big_trade(b"fan", 0));
        assert_eq!(hub.metrics.live_budget_refused.load(Ordering::Relaxed), 1);
        for subscriber in &subscribers {
            assert!(matches!(subscriber.next().await, Next::Overflow));
        }
        assert_eq!(budget.used(), 0);
        // A fitting fallback record carries its full weight per subscriber.
        let budget = ByteBudget::new(4 * big_weight());
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        hub.attach_live_budget(ByteBudget::new(1));
        let subscriber = subscribe(&hub, b"fan", 0, 16, 1 << 20, &budget);
        hub.dispatch(big_trade(b"fan", 0));
        assert_eq!(budget.used(), big_weight());
        subscriber.close();
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn a_slow_lossless_subscriber_at_its_cap_cannot_overflow_an_idle_one() {
        let weight = big_weight();
        let total = 16 * weight;
        // `idle` is offered one record after `slow` filled up and stalled.
        async fn run(total: usize, max_bytes: usize) -> (bool, bool) {
            let budget = ByteBudget::new(total);
            let hub = Hub::new(&[(0, 0)], HubConfig::default());
            hub.attach_live_budget(budget.clone());
            let slow = subscribe(&hub, b"slow", 0, 1_000, max_bytes, &budget);
            let idle = subscribe(&hub, b"idle", 1, 1_000, max_bytes, &budget);
            for offset in 0..64 {
                hub.dispatch(big_trade(b"slow", offset));
            }
            hub.dispatch(big_trade(b"idle", 64));
            assert!(budget.used() <= total);
            let slow_overflowed = matches!(slow.next().await, Next::Overflow);
            let idle_delivered = matches!(idle.next().await, Next::Record(..));
            (slow_overflowed, idle_delivered)
        }
        // A fair share (the new default, queue_total / 2): the slow
        // subscriber overflows on its own cap; the idle one is untouched.
        let share = crate::memory::default_queue_bytes_per_subscription(total as u64) as usize;
        assert_eq!(run(total, share).await, (true, true));
        // A per-subscription cap equal to the whole budget (the old default)
        // lets the slow subscriber take it all and overflow the idle one.
        assert_eq!(run(total, total).await, (true, false));
    }

    /// 2026-10-15 stage-100 shape: deep lossless subscriptions on different
    /// keys, each under its own fair-share cap, together exhaust the live
    /// budget; idle readers must keep flowing. Returns (idle overflows, deep
    /// overflows, the budget after the run).
    async fn deep_and_idle(budget: Arc<ByteBudget>) -> (usize, usize, Arc<ByteBudget>) {
        let total = budget.limit();
        let share = crate::memory::default_queue_bytes_per_subscription(total as u64) as usize;
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        hub.attach_live_budget(budget.clone());
        let deep_keys: Vec<Vec<u8>> = (0..3).map(|i| format!("deep{i}").into_bytes()).collect();
        let idle_keys: Vec<Vec<u8>> = (0..30).map(|i| format!("idle{i}").into_bytes()).collect();
        let mut deep: Vec<_> = deep_keys
            .iter()
            .enumerate()
            .map(|(id, key)| subscribe(&hub, key, id as u64, 2_000, share, &budget))
            .collect();
        let idle: Vec<_> = idle_keys
            .iter()
            .enumerate()
            .map(|(id, key)| subscribe(&hub, key, 100 + id as u64, 1_000, share, &budget))
            .collect();
        let (mut offset, mut deep_overflows, mut idle_overflows) = (0i64, 0, 0);
        for _round in 0..200 {
            for key in &deep_keys {
                hub.dispatch(big_trade(key, offset));
                offset += 1;
                assert!(budget.used() <= total, "{} > {total}", budget.used());
            }
            // A deep subscription that overflowed is ended by its stream task
            // (BACKPRESSURE + resume token) and resubscribes empty from its
            // cursor, as a client does.
            for (index, subscription) in deep.iter_mut().enumerate() {
                if matches!(subscription.terminal(), Some(Next::Overflow)) {
                    deep_overflows += 1;
                    subscription.close();
                    hub.unregister(0, &deep_keys[index], subscription.id);
                    *subscription = subscribe(
                        &hub,
                        &deep_keys[index],
                        1_000 + offset as u64,
                        2_000,
                        share,
                        &budget,
                    );
                }
            }
            for key in &idle_keys {
                hub.dispatch(big_trade(key, offset));
                offset += 1;
            }
            // Every idle reader takes its record to the transport at once
            // (entry charge topped up to a private copy), then sends it.
            let mut in_flight = Vec::new();
            for subscription in &idle {
                match subscription.next().await {
                    Next::Record(record, _, mut charge) => {
                        let (weight, entry) = (record.weight(), charge.bytes());
                        let missing = weight.saturating_sub(entry);
                        if missing > 0 {
                            match subscription.charge_handoff(missing, weight, &charge) {
                                Ok(extra) => charge.absorb(extra),
                                Err(_) => idle_overflows += 1,
                            }
                        }
                        in_flight.push((record, charge));
                    }
                    Next::Overflow => {
                        idle_overflows += 1;
                        subscription.close();
                    }
                    other => panic!("{other:?}"),
                }
                assert!(budget.used() <= total, "{} > {total}", budget.used());
            }
            drop(in_flight);
            if idle_overflows > 0 {
                break;
            }
        }
        for subscription in deep.iter().chain(&idle) {
            subscription.close();
        }
        (idle_overflows, deep_overflows, budget)
    }

    #[tokio::test]
    async fn deep_lossless_subscriptions_cannot_overflow_thirty_idle_ones() {
        let total = 512 * big_weight();
        let budget = ByteBudget::live(total);
        let (reserve, floor) = budget.reserve_and_floor();
        assert_eq!((reserve, floor), (total / 8, total / 128));
        let (idle, deep, budget) = deep_and_idle(budget).await;
        assert_eq!(idle, 0, "no idle reader overflowed or stalled at handoff");
        assert!(deep >= 1, "the deep subscription over its floor was closed");
        let counters = &budget.counters;
        assert!(counters.over_floor_refused.load(Ordering::Relaxed) >= 1);
        assert!(
            counters.reserve_admitted.load(Ordering::Relaxed) >= 1,
            "the reserve was used"
        );
        assert_eq!(counters.reserve_exhausted.load(Ordering::Relaxed), 0);
        assert_eq!(budget.used(), 0, "every byte released");

        // The same run without the reserve (before this fix): the deep
        // subscriptions exhaust the budget and idle readers overflow.
        let (idle, _, budget) = deep_and_idle(ByteBudget::new(total)).await;
        assert!(idle >= 1);
        assert_eq!(
            budget.counters.over_floor_refused.load(Ordering::Relaxed),
            0
        );
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn a_single_deep_subscription_over_its_floor_alone_overflows() {
        let weight = big_weight();
        let total = 128 * weight;
        let budget = ByteBudget::live(total);
        let hub = Hub::new(&[(0, 0)], HubConfig::default());
        hub.attach_live_budget(budget.clone());
        // Its own cap is the whole budget: only the reserve can stop it.
        let deep = subscribe(&hub, b"deep", 0, 10_000, total, &budget);
        let idle = subscribe(&hub, b"idle", 1, 10_000, total, &budget);
        let mut offset = 0;
        while deep.terminal().is_none() {
            hub.dispatch(big_trade(b"deep", offset));
            offset += 1;
            assert!(offset < 1_000);
        }
        let (reserve, _) = budget.reserve_and_floor();
        assert!(
            budget.used() <= total - reserve,
            "deep never entered the reserve"
        );
        assert!(deep.queued().1 > total / 2);
        hub.dispatch(big_trade(b"idle", offset));
        assert!(matches!(deep.next().await, Next::Overflow));
        assert!(matches!(idle.next().await, Next::Record(..)));
        assert_eq!(
            budget.counters.over_floor_refused.load(Ordering::Relaxed),
            1
        );
        deep.close();
        assert!(budget.used() < weight + QUEUE_ENTRY_OVERHEAD);
    }
}
