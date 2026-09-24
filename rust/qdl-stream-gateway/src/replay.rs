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
use tokio::sync::Semaphore;

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
    pub detached: AtomicU64,
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

/// Records handed to one replay request at a time; a request that cannot
/// take the next record within [`DETACH_AFTER`] is detached from the shared
/// pass and continues from its own offset in the next pass.
pub const REPLAY_CHANNEL: usize = 256;
pub const DETACH_AFTER: Duration = Duration::from_millis(200);

/// One replay: the committed range `(after, barrier)` of one partition for
/// one key and product, delivered to `sink` in offset order.
pub struct ReplayRequest {
    pub after: i64,
    pub barrier: i64,
    pub key: Vec<u8>,
    pub feed: String,
    pub interval: Option<String>,
    pub page: Option<u64>,
    pub sink: tokio::sync::mpsc::Sender<std::sync::Arc<crate::hub::LiveRecord>>,
    pub done: Option<tokio::sync::oneshot::Sender<ReplayEnd>>,
    pub cancel: Arc<AtomicBool>,
    pub deadline: Instant,
    matched: u64,
}

impl ReplayRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        after: i64,
        barrier: i64,
        key: Vec<u8>,
        product: (String, Option<String>),
        page: Option<u64>,
        sink: tokio::sync::mpsc::Sender<std::sync::Arc<crate::hub::LiveRecord>>,
        done: tokio::sync::oneshot::Sender<ReplayEnd>,
        cancel: Arc<AtomicBool>,
        deadline: Instant,
    ) -> Self {
        Self {
            after,
            barrier,
            key,
            feed: product.0,
            interval: product.1,
            page,
            sink,
            done: Some(done),
            cancel,
            deadline,
            matched: 0,
        }
    }

    fn finish(&mut self, end: ReplayEnd) {
        if let Some(done) = self.done.take() {
            let _ = done.send(end);
        }
    }
}

#[derive(Default)]
struct PartitionReplays {
    pending: Vec<ReplayRequest>,
    joiners: Vec<ReplayRequest>,
    scanning: bool,
    /// Next offset the running pass will read (joiners at or after it fit).
    position: i64,
}

/// Coalesced replay (KN-2 D4 amendment after the K2.5/K2-T08 runs): all
/// pending requests of a partition are served by **one** pass of one reader,
/// instead of one full-range scan per request (measured: 1.09 M records
/// scanned to replay 5,946 in a cold reconnect storm). At most `readers`
/// passes run at once across partitions.
pub struct ReplayCoordinator {
    source: Arc<dyn RangeSource>,
    readers: Arc<Semaphore>,
    reader_count: usize,
    limits: ReplayLimits,
    partitions: Mutex<HashMap<i32, PartitionReplays>>,
    pub metrics: ReplayMetrics,
}

impl ReplayCoordinator {
    pub fn new(source: Arc<dyn RangeSource>, readers: usize, limits: ReplayLimits) -> Arc<Self> {
        Arc::new(Self {
            source,
            readers: Arc::new(Semaphore::new(readers.max(1))),
            reader_count: readers.max(1),
            limits,
            partitions: Mutex::new(HashMap::new()),
            metrics: ReplayMetrics::default(),
        })
    }

    pub fn available(&self) -> usize {
        self.readers.available_permits()
    }

    pub fn readers(&self) -> usize {
        self.reader_count
    }

    /// Requests waiting for or inside a pass.
    pub fn in_flight(&self) -> usize {
        self.partitions
            .lock()
            .map(|map| {
                map.values()
                    .map(|entry| entry.pending.len() + entry.joiners.len())
                    .sum::<usize>()
            })
            .unwrap_or(0)
            + self.metrics.active.load(Ordering::Relaxed) as usize
    }

    pub fn submit(self: &Arc<Self>, partition: i32, mut request: ReplayRequest) {
        if request.after + 1 >= request.barrier {
            request.finish(ReplayEnd::Complete);
            return;
        }
        // Offsets bound the records to scan (markers only make it smaller):
        // a range that cannot fit the scan budget is refused without a scan.
        if (request.barrier - request.after - 1) as u64 > self.limits.max_scanned_records {
            self.metrics.scan_limited.fetch_add(1, Ordering::Relaxed);
            request.finish(ReplayEnd::ScanLimit("REPLAY_SCAN_LIMIT"));
            return;
        }
        let start = {
            let Ok(mut map) = self.partitions.lock() else {
                request.finish(ReplayEnd::Error("replay coordinator poisoned".into()));
                return;
            };
            let entry = map.entry(partition).or_default();
            if entry.scanning && entry.position <= request.after + 1 {
                entry.joiners.push(request);
                false
            } else {
                entry.pending.push(request);
                !std::mem::replace(&mut entry.scanning, true)
            }
        };
        if start {
            let coordinator = self.clone();
            tokio::spawn(async move {
                let Ok(permit) = coordinator.readers.clone().acquire_owned().await else {
                    return;
                };
                let _ = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    coordinator.run_partition(partition);
                })
                .await;
            });
        }
    }

    fn take_batch(&self, partition: i32) -> Vec<ReplayRequest> {
        let Ok(mut map) = self.partitions.lock() else {
            return Vec::new();
        };
        let entry = map.entry(partition).or_default();
        let batch = std::mem::take(&mut entry.pending);
        if batch.is_empty() {
            entry.scanning = false;
        }
        batch
    }

    fn set_position(&self, partition: i32, position: i64) -> Vec<ReplayRequest> {
        let Ok(mut map) = self.partitions.lock() else {
            return Vec::new();
        };
        let entry = map.entry(partition).or_default();
        entry.position = position;
        std::mem::take(&mut entry.joiners)
    }

    fn requeue(&self, partition: i32, request: ReplayRequest) {
        if let Ok(mut map) = self.partitions.lock() {
            map.entry(partition).or_default().pending.push(request);
        }
    }

    /// Serve every pending request of `partition`, pass after pass, until
    /// none is left. Runs on a blocking thread holding one reader permit.
    fn run_partition(&self, partition: i32) {
        loop {
            let mut batch = self.take_batch(partition);
            if batch.is_empty() {
                return;
            }
            self.metrics.active.fetch_add(1, Ordering::Relaxed);
            self.metrics.reader_replays.fetch_add(1, Ordering::Relaxed);
            self.pass(partition, &mut batch);
            self.metrics.active.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn pass(&self, partition: i32, batch: &mut Vec<ReplayRequest>) {
        // Open at the earliest start; a request below the retention floor is
        // answered and the pass reopens at the next start.
        batch.sort_by_key(|request| request.after);
        let mut cursor = loop {
            let Some(first) = batch.first() else {
                return;
            };
            match self.source.open(partition, first.after + 1) {
                Ok(cursor) => break cursor,
                Err(RangeError::Retention) => {
                    let mut expired = batch.remove(0);
                    expired.finish(ReplayEnd::Retention);
                }
                Err(RangeError::Other(error)) => {
                    for request in batch.iter_mut() {
                        request.finish(ReplayEnd::Error(error.clone()));
                    }
                    batch.clear();
                    return;
                }
            }
        };
        let mut position = batch[0].after + 1;
        let mut active: Vec<ReplayRequest> = std::mem::take(batch);
        loop {
            for joiner in self.set_position(partition, position) {
                if joiner.after + 1 >= position {
                    active.push(joiner);
                } else {
                    self.requeue(partition, joiner);
                }
            }
            // Finish what is done, expired or abandoned.
            let now = Instant::now();
            let mut index = 0;
            while index < active.len() {
                let request = &mut active[index];
                let end = if request.cancel.load(Ordering::Relaxed) || request.sink.is_closed() {
                    Some(ReplayEnd::Cancelled)
                } else if position >= request.barrier {
                    Some(ReplayEnd::Complete)
                } else if now > request.deadline {
                    Some(ReplayEnd::ScanLimit("REPLAY_TIME_LIMIT"))
                } else {
                    None
                };
                match end {
                    Some(end) => {
                        let mut done = active.swap_remove(index);
                        done.finish(end);
                    }
                    None => index += 1,
                }
            }
            if active.is_empty() {
                return;
            }
            let record = match cursor.next(Duration::from_millis(100)) {
                Ok(Some(record)) => record,
                Ok(None) => {
                    if let Some(next) = cursor.position() {
                        position = position.max(next);
                    }
                    continue;
                }
                Err(RangeError::Retention) => {
                    for mut request in active.drain(..) {
                        request.finish(ReplayEnd::Retention);
                    }
                    return;
                }
                Err(RangeError::Other(error)) => {
                    for mut request in active.drain(..) {
                        request.finish(ReplayEnd::Error(error.clone()));
                    }
                    return;
                }
            };
            position = record.offset + 1;
            self.metrics.scanned.fetch_add(1, Ordering::Relaxed);
            if !active.iter().any(|request| request.key == record.key) {
                continue;
            }
            let shared = Arc::new(record);
            let Ok(live) = crate::hub::LiveRecord::decode(shared) else {
                continue;
            };
            let mut index = 0;
            while index < active.len() {
                let offset = live.raw.offset;
                let request = &mut active[index];
                let wanted = request.key == live.raw.key
                    && offset > request.after
                    && offset < request.barrier
                    && crate::requirement::is_product(
                        &request.feed,
                        request.interval.as_deref(),
                        &live.envelope,
                    );
                if !wanted {
                    index += 1;
                    continue;
                }
                if request.matched >= self.limits.max_matched {
                    let mut done = active.swap_remove(index);
                    done.finish(ReplayEnd::Backlog);
                    continue;
                }
                match send_or_detach(&request.sink, live.clone()) {
                    Sent::Delivered => {
                        request.after = offset;
                        request.matched += 1;
                        self.metrics.matched.fetch_add(1, Ordering::Relaxed);
                        if request.page.is_some_and(|page| request.matched >= page) {
                            let mut done = active.swap_remove(index);
                            done.finish(ReplayEnd::PageFull);
                            continue;
                        }
                        index += 1;
                    }
                    Sent::Closed => {
                        let mut done = active.swap_remove(index);
                        done.finish(ReplayEnd::Cancelled);
                    }
                    Sent::Detach => {
                        // Too slow for the shared pass: continue from its own
                        // offset (the record was not sent) in the next pass.
                        self.metrics.detached.fetch_add(1, Ordering::Relaxed);
                        let detached = active.swap_remove(index);
                        self.requeue(partition, detached);
                    }
                }
            }
        }
    }
}

enum Sent {
    Delivered,
    Closed,
    Detach,
}

fn send_or_detach(
    sink: &tokio::sync::mpsc::Sender<std::sync::Arc<crate::hub::LiveRecord>>,
    record: std::sync::Arc<crate::hub::LiveRecord>,
) -> Sent {
    let deadline = Instant::now() + DETACH_AFTER;
    let mut record = record;
    loop {
        match sink.try_send(record) {
            Ok(()) => return Sent::Delivered,
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return Sent::Closed,
            Err(tokio::sync::mpsc::error::TrySendError::Full(back)) => {
                if Instant::now() >= deadline {
                    return Sent::Detach;
                }
                record = back;
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }
}
