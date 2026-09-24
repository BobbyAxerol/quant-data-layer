//! Bounded replay (KN-2 K2.3, D3/D4).
//!
//! A replay reads the committed range `(after, barrier)` of one partition with
//! its **own** reader, never the live one. Readers come from a pool bounded
//! globally and per consumer; each replay is bounded by records scanned,
//! bytes scanned, wall time and matched records, and stops within one poll
//! when it is cancelled (client gone). Every limit ends in a typed outcome.

use crate::hub::RawRecord;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RangeError {
    /// The requested offset is below the partition's retention floor.
    Retention,
    Other(String),
}

/// One bounded range reader over the committed log.
pub trait RangeCursor: Send {
    fn next(&mut self, timeout: Duration) -> Result<Option<RawRecord>, RangeError>;
    /// Next offset this reader will return (committed position).
    fn position(&self) -> Option<i64>;
}

pub trait RangeSource: Send + Sync {
    fn open(&self, partition: i32, from: i64) -> Result<Box<dyn RangeCursor>, RangeError>;
}

#[derive(Clone, Debug)]
pub struct ReplayLimits {
    pub max_scanned_records: u64,
    pub max_scanned_bytes: u64,
    pub max_duration: Duration,
    /// `max_replay_events` of the Python gateway: more matching records than
    /// this means the backlog exceeds the bounded window (resnapshot).
    pub max_matched: u64,
}

impl Default for ReplayLimits {
    fn default() -> Self {
        Self {
            max_scanned_records: 2_000_000,
            max_scanned_bytes: 1 << 30,
            max_duration: Duration::from_secs(20),
            max_matched: 10_000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayEnd {
    /// The range is exhausted: every record below `barrier` was scanned.
    Complete,
    /// `limit` matched records were emitted (Replay RPC page).
    PageFull,
    ScanLimit(&'static str),
    Backlog,
    Retention,
    Cancelled,
    Error(String),
}

/// What the consumer of a scanned key match did with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Emit {
    /// Another product on the same physical key (BOOK snapshot/delta share
    /// one): not counted against the page or the backlog.
    Skip,
    Sent,
    /// The client is gone.
    Stop,
}

#[derive(Default)]
pub struct ReplayMetrics {
    pub ring_hits: AtomicU64,
    pub reader_replays: AtomicU64,
    pub scanned: AtomicU64,
    pub matched: AtomicU64,
    pub active: AtomicU64,
    pub refused_capacity: AtomicU64,
    pub scan_limited: AtomicU64,
}

/// Scan `(after, barrier)` of `partition` for `key`. `page`: stop after this
/// many sent records (Replay RPC).
#[allow(clippy::too_many_arguments)]
pub fn scan_range(
    source: &dyn RangeSource,
    partition: i32,
    after: i64,
    barrier: i64,
    key: &[u8],
    limits: &ReplayLimits,
    page: Option<u64>,
    cancel: &AtomicBool,
    metrics: &ReplayMetrics,
    emit: &mut dyn FnMut(RawRecord) -> Emit,
) -> ReplayEnd {
    if after + 1 >= barrier {
        return ReplayEnd::Complete;
    }
    let mut cursor = match source.open(partition, after + 1) {
        Ok(cursor) => cursor,
        Err(RangeError::Retention) => return ReplayEnd::Retention,
        Err(RangeError::Other(error)) => return ReplayEnd::Error(error),
    };
    let started = Instant::now();
    let (mut scanned, mut bytes, mut matched) = (0u64, 0u64, 0u64);
    loop {
        if cancel.load(Ordering::Relaxed) {
            return ReplayEnd::Cancelled;
        }
        if started.elapsed() > limits.max_duration {
            return ReplayEnd::ScanLimit("REPLAY_TIME_LIMIT");
        }
        if cursor.position().is_some_and(|next| next >= barrier) {
            return ReplayEnd::Complete;
        }
        let record = match cursor.next(Duration::from_millis(100)) {
            Ok(Some(record)) => record,
            Ok(None) => continue,
            Err(RangeError::Retention) => return ReplayEnd::Retention,
            Err(RangeError::Other(error)) => return ReplayEnd::Error(error),
        };
        if record.offset >= barrier {
            return ReplayEnd::Complete;
        }
        scanned += 1;
        bytes += record.payload.len() as u64;
        metrics.scanned.fetch_add(1, Ordering::Relaxed);
        if scanned > limits.max_scanned_records {
            return ReplayEnd::ScanLimit("REPLAY_SCAN_LIMIT");
        }
        if bytes > limits.max_scanned_bytes {
            return ReplayEnd::ScanLimit("REPLAY_BYTE_LIMIT");
        }
        if record.key != key {
            continue;
        }
        if matched >= limits.max_matched {
            return ReplayEnd::Backlog;
        }
        match emit(record) {
            Emit::Skip => continue,
            Emit::Stop => return ReplayEnd::Cancelled,
            Emit::Sent => {}
        }
        matched += 1;
        metrics.matched.fetch_add(1, Ordering::Relaxed);
        if page.is_some_and(|page| matched >= page) {
            return ReplayEnd::PageFull;
        }
    }
}

/// Replay readers bounded globally and per consumer.
pub struct ReplayPool {
    global: Arc<Semaphore>,
    per_consumer: Arc<Mutex<HashMap<String, usize>>>,
    per_consumer_max: usize,
    wait: Duration,
    pub metrics: ReplayMetrics,
}

/// Held for the lifetime of one replay; releases both limits when dropped.
pub struct ReplayPermit {
    _global: OwnedSemaphorePermit,
    consumer: String,
    per_consumer: Arc<Mutex<HashMap<String, usize>>>,
}

impl Drop for ReplayPermit {
    fn drop(&mut self) {
        if let Ok(mut counts) = self.per_consumer.lock() {
            if let Some(count) = counts.get_mut(&self.consumer) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(&self.consumer);
                }
            }
        }
    }
}

impl ReplayPool {
    pub fn new(global: usize, per_consumer_max: usize, wait: Duration) -> Self {
        Self {
            global: Arc::new(Semaphore::new(global.max(1))),
            per_consumer: Arc::new(Mutex::new(HashMap::new())),
            per_consumer_max: per_consumer_max.max(1),
            wait,
            metrics: ReplayMetrics::default(),
        }
    }

    /// A permit, or `None` when the consumer's share or the pool stays
    /// exhausted for the admission wait (typed RATE_LIMITED by the caller).
    pub async fn acquire(&self, consumer: &str) -> Option<ReplayPermit> {
        {
            let mut counts = self.per_consumer.lock().ok()?;
            let count = counts.entry(consumer.to_owned()).or_insert(0);
            if *count >= self.per_consumer_max {
                self.metrics
                    .refused_capacity
                    .fetch_add(1, Ordering::Relaxed);
                return None;
            }
            *count += 1;
        }
        let release = |pool: &Self| {
            if let Ok(mut counts) = pool.per_consumer.lock() {
                if let Some(count) = counts.get_mut(consumer) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        counts.remove(consumer);
                    }
                }
            }
        };
        match tokio::time::timeout(self.wait, self.global.clone().acquire_owned()).await {
            Ok(Ok(permit)) => Some(ReplayPermit {
                _global: permit,
                consumer: consumer.to_owned(),
                per_consumer: self.per_consumer.clone(),
            }),
            _ => {
                release(self);
                self.metrics
                    .refused_capacity
                    .fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn available(&self) -> usize {
        self.global.available_permits()
    }

    pub fn consumers_in_replay(&self) -> usize {
        self.per_consumer.lock().map(|map| map.len()).unwrap_or(0)
    }
}
