//! Bounded replay (KN-2 K2.3, D3/D4).
//!
//! A replay reads the committed range `(after, barrier)` of one partition with
//! its **own** reader, never the live one. [`ReplayCoordinator`] serves every
//! pending request of a partition with one coalesced pass per reader permit;
//! each request is bounded by records scanned, bytes scanned, wall time and
//! matched records, stops within one poll when cancelled (client gone), and
//! always ends with exactly one typed [`ReplayEnd`] (Astra KN-2 R1 F2-F4).

use crate::hub::{LiveRecord, RawRecord};
use crate::subscription::{ByteBudget, Charge};
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
    /// Maximum matching records per bounded replay. Exhaustion is retryable
    /// REPLAY_MATCHED_LIMIT, not retention loss; resume from the last token.
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
    /// A committed record of the requested key in the range failed to
    /// decode: the product cannot be replayed past it (never skipped).
    Corrupt {
        partition: i32,
        offset: i64,
    },
    Error(String),
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
    /// Detached because the replay budget was exhausted.
    pub budget_detached: AtomicU64,
    /// Full-channel slot waits and cumulative elapsed time (saturating).
    pub slot_waits: AtomicU64,
    pub slot_wait_ns: AtomicU64,
    /// Replay-only source calls, including failures and empty cursor polls.
    pub open_calls: AtomicU64,
    pub open_elapsed_ns: AtomicU64,
    pub cursor_next_calls: AtomicU64,
    pub cursor_next_elapsed_ns: AtomicU64,
}

/// Records handed to one replay request at a time; a request that cannot
/// take the next record within [`DETACH_AFTER`] (its channel is full) is
/// detached from the shared pass and continues from its own offset in a later
/// pass. Replay has its **own** byte budget, separate from the live queues, so
/// a reconnect storm can never starve live delivery (K2.5 R1 rerun: one shared
/// budget let 185 replay prefetches overflow live lossless queues); when it is
/// exhausted a request detaches at once instead of stalling the shared pass.
pub const REPLAY_CHANNEL: usize = 32;
pub const DETACH_AFTER: Duration = Duration::from_millis(200);

/// A replayed record and the budget bytes it holds until the receiver drops
/// it (F4: replay channels are inside the replica byte budget).
pub struct Replayed {
    pub record: Arc<LiveRecord>,
    charge: Charge,
}

impl Replayed {
    /// The record and its charge, which the caller hands on to the transport.
    pub fn into_parts(self) -> (Arc<LiveRecord>, Charge) {
        (self.record, self.charge)
    }
}

/// One replay: the committed range `(after, barrier)` of one partition for
/// one key and product, delivered to `sink` in offset order.
pub struct ReplayRequest {
    pub after: i64,
    pub barrier: i64,
    pub key: Vec<u8>,
    pub feed: String,
    pub interval: Option<String>,
    pub page: Option<u64>,
    pub sink: tokio::sync::mpsc::Sender<Replayed>,
    pub done: Option<tokio::sync::oneshot::Sender<ReplayEnd>>,
    pub cancel: Arc<AtomicBool>,
    pub deadline: Instant,
    matched: u64,
    scanned: u64,
    scanned_bytes: u64,
}

impl ReplayRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        after: i64,
        barrier: i64,
        key: Vec<u8>,
        product: (String, Option<String>),
        page: Option<u64>,
        sink: tokio::sync::mpsc::Sender<Replayed>,
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
            scanned: 0,
            scanned_bytes: 0,
        }
    }

    fn finish(&mut self, end: ReplayEnd) {
        if let Some(done) = self.done.take() {
            let _ = done.send(end);
        }
    }

    /// Why this request must end now, before the next record is read.
    fn ended(&self, position: i64, now: Instant) -> Option<ReplayEnd> {
        if self.cancel.load(Ordering::Relaxed) || self.sink.is_closed() {
            Some(ReplayEnd::Cancelled)
        } else if position >= self.barrier {
            Some(ReplayEnd::Complete)
        } else if now > self.deadline {
            Some(ReplayEnd::ScanLimit("REPLAY_TIME_LIMIT"))
        } else {
            None
        }
    }
}

/// A request dropped without an answer still answers (every exit path, F3).
impl Drop for ReplayRequest {
    fn drop(&mut self) {
        self.finish(ReplayEnd::Error("replay request dropped".into()));
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

/// Coalesced replay (KN-2 D4 amendment after the K2.5/K2-T08 runs): the
/// pending requests of a partition are served by **one** pass of one reader,
/// instead of one full-range scan per request (measured: 1.09 M records
/// scanned to replay 5,946 in a cold reconnect storm). At most `readers`
/// passes run at once; each pass takes a reader permit and gives it back
/// when it ends, so a busy partition cannot hold a reader forever.
pub struct ReplayCoordinator {
    source: Arc<dyn RangeSource>,
    readers: Arc<Semaphore>,
    reader_count: usize,
    limits: ReplayLimits,
    budget: Arc<ByteBudget>,
    partitions: Mutex<HashMap<i32, PartitionReplays>>,
    pub metrics: ReplayMetrics,
}

impl ReplayCoordinator {
    pub fn new(
        source: Arc<dyn RangeSource>,
        readers: usize,
        limits: ReplayLimits,
        budget: Arc<ByteBudget>,
    ) -> Arc<Self> {
        Arc::new(Self {
            source,
            readers: Arc::new(Semaphore::new(readers.max(1))),
            reader_count: readers.max(1),
            limits,
            budget,
            partitions: Mutex::new(HashMap::new()),
            metrics: ReplayMetrics::default(),
        })
    }

    pub fn available(&self) -> usize {
        self.readers.available_permits()
    }

    /// The replay byte budget (channels and ring replays).
    pub fn budget(&self) -> &Arc<ByteBudget> {
        &self.budget
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
                // One pass per permit; the partition's driver ends when a
                // pass finds nothing left (and has cleared `scanning`).
                loop {
                    let Ok(permit) = coordinator.readers.clone().acquire_owned().await else {
                        coordinator.abandon(partition);
                        return;
                    };
                    let worker = coordinator.clone();
                    let outcome = tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        worker.run_pass(partition)
                    })
                    .await
                    .unwrap_or(Pass::Done);
                    match outcome {
                        Pass::Done => return,
                        Pass::Progress => {}
                        // Nothing delivered, budget exhausted: back off
                        // instead of re-reading the range in a tight loop.
                        Pass::Starved => tokio::time::sleep(STARVED_BACKOFF).await,
                    }
                }
            });
        }
    }

    /// Everything waiting on `partition` (pending and joiners). Clears
    /// `scanning` when nothing is left, atomically with the check, so a new
    /// submit starts a new driver.
    fn take_batch(&self, partition: i32) -> Vec<ReplayRequest> {
        let Ok(mut map) = self.partitions.lock() else {
            return Vec::new();
        };
        let entry = map.entry(partition).or_default();
        let mut batch = std::mem::take(&mut entry.pending);
        batch.append(&mut entry.joiners);
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

    /// The driver could not get a reader (shutdown): answer everyone.
    fn abandon(&self, partition: i32) {
        for mut request in self.take_batch(partition) {
            request.finish(ReplayEnd::Error("replay readers closed".into()));
        }
    }

    /// One pass over everything waiting.
    fn run_pass(&self, partition: i32) -> Pass {
        let mut batch = self.take_batch(partition);
        if batch.is_empty() {
            return Pass::Done;
        }
        self.metrics.active.fetch_add(1, Ordering::Relaxed);
        self.metrics.reader_replays.fetch_add(1, Ordering::Relaxed);
        let starved = self.pass(partition, &mut batch);
        // Every request of the batch was answered, requeued or is still in
        // `batch` only if the pass returned early: answer those too.
        for mut request in batch.drain(..) {
            request.finish(ReplayEnd::Error("replay pass ended early".into()));
        }
        self.metrics.active.fetch_sub(1, Ordering::Relaxed);
        if starved {
            Pass::Starved
        } else {
            Pass::Progress
        }
    }

    /// Serve `batch` in one pass; `true` when it delivered nothing and some
    /// request detached for lack of replay budget.
    fn pass(&self, partition: i32, batch: &mut Vec<ReplayRequest>) -> bool {
        let mut delivered = false;
        let mut starved = false;
        self.pass_inner(partition, batch, &mut delivered, &mut starved);
        starved && !delivered
    }

    fn pass_inner(
        &self,
        partition: i32,
        batch: &mut Vec<ReplayRequest>,
        delivered: &mut bool,
        starved: &mut bool,
    ) {
        let started = Instant::now();
        // Requests already over their deadline or abandoned while waiting
        // for a reader are answered before any read.
        let now = Instant::now();
        batch.retain_mut(|request| match request.ended(i64::MIN, now) {
            Some(end) => {
                request.finish(end);
                false
            }
            None => true,
        });
        // Open at the earliest start; a request below the retention floor is
        // answered and the pass reopens at the next start.
        batch.sort_by_key(|request| request.after);
        let mut cursor = loop {
            let Some(first) = batch.first() else {
                return;
            };
            let from = first.after + 1;
            self.set_position_only(partition, from);
            let started = Instant::now();
            saturating_add(&self.metrics.open_calls, 1);
            let opened = self.source.open(partition, from);
            saturating_add(&self.metrics.open_elapsed_ns, elapsed_ns(started));
            match opened {
                Ok(cursor) => break cursor,
                Err(RangeError::Retention) => {
                    let mut expired = batch.remove(0);
                    expired.finish(ReplayEnd::Retention);
                }
                Err(RangeError::Other(error)) => {
                    for mut request in batch.drain(..) {
                        request.finish(ReplayEnd::Error(error.clone()));
                    }
                    // Joiners that arrived while the reader was opening are
                    // served by the next pass (the driver loops), never lost.
                    return;
                }
            }
        };
        let mut position = batch[0].after + 1;
        let mut active: Vec<ReplayRequest> = std::mem::take(batch);
        loop {
            // A pass takes joiners for at most `max_duration`, so one busy
            // partition cannot keep its reader forever.
            let joining = started.elapsed() < self.limits.max_duration;
            for joiner in self.set_position(partition, position) {
                if joining && joiner.after + 1 >= position {
                    active.push(joiner);
                } else {
                    self.requeue(partition, joiner);
                }
            }
            let now = Instant::now();
            let mut index = 0;
            while index < active.len() {
                match active[index].ended(position, now) {
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
            let next_started = Instant::now();
            saturating_add(&self.metrics.cursor_next_calls, 1);
            let next = cursor.next(Duration::from_millis(100));
            saturating_add(&self.metrics.cursor_next_elapsed_ns, elapsed_ns(next_started));
            let record = match next {
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
            // Scan limits are per request, over the records of its own range
            // (F4): checked before anything of this record is emitted.
            let size = record.payload.len() as u64;
            let offset = record.offset;
            let mut index = 0;
            while index < active.len() {
                let request = &mut active[index];
                if offset <= request.after || offset >= request.barrier {
                    index += 1;
                    continue;
                }
                request.scanned += 1;
                request.scanned_bytes += size;
                let limit = if request.scanned > self.limits.max_scanned_records {
                    Some("REPLAY_SCAN_LIMIT")
                } else if request.scanned_bytes > self.limits.max_scanned_bytes {
                    Some("REPLAY_BYTE_LIMIT")
                } else {
                    None
                };
                match limit {
                    Some(reason) => {
                        self.metrics.scan_limited.fetch_add(1, Ordering::Relaxed);
                        let mut done = active.swap_remove(index);
                        done.finish(ReplayEnd::ScanLimit(reason));
                    }
                    None => index += 1,
                }
            }
            if !active.iter().any(|request| request.key == record.key) {
                continue;
            }
            let record_key = record.key.clone();
            let live = match LiveRecord::decode(Arc::new(record)) {
                Ok(live) => live,
                Err(_) => {
                    // F2: a record of a requested key that cannot be decoded
                    // may be any of that key's products; every request whose
                    // range holds it ends typed, the others continue.
                    let mut index = 0;
                    while index < active.len() {
                        let request = &active[index];
                        let holds = request.key == record_key
                            && offset > request.after
                            && offset < request.barrier;
                        if holds {
                            let mut done = active.swap_remove(index);
                            done.finish(ReplayEnd::Corrupt { partition, offset });
                        } else {
                            index += 1;
                        }
                    }
                    continue;
                }
            };
            let weight = live.weight();
            let mut index = 0;
            while index < active.len() {
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
                match self.send_or_detach(&request.sink, &live, weight) {
                    Sent::Delivered => {
                        *delivered = true;
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
                        // offset later.
                        self.metrics.detached.fetch_add(1, Ordering::Relaxed);
                        let detached = active.swap_remove(index);
                        self.requeue(partition, detached);
                    }
                    Sent::Starved => {
                        *starved = true;
                        self.metrics.budget_detached.fetch_add(1, Ordering::Relaxed);
                        let detached = active.swap_remove(index);
                        self.requeue(partition, detached);
                    }
                }
            }
        }
    }

    fn set_position_only(&self, partition: i32, position: i64) {
        if let Ok(mut map) = self.partitions.lock() {
            map.entry(partition).or_default().position = position;
        }
    }

    fn send_or_detach(
        &self,
        sink: &tokio::sync::mpsc::Sender<Replayed>,
        record: &Arc<LiveRecord>,
        weight: usize,
    ) -> Sent {
        let deadline = Instant::now() + DETACH_AFTER;
        let permit = match sink.try_reserve() {
            Ok(permit) => permit,
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return Sent::Closed,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                let started = Instant::now();
                saturating_add(&self.metrics.slot_waits, 1);
                // Only called by the spawn_blocking replay reader. Construct
                // the timer inside its runtime, retaining the original bound.
                let reserved = tokio::runtime::Handle::current().block_on(async {
                    tokio::time::timeout(
                        deadline.saturating_duration_since(Instant::now()),
                        sink.reserve(),
                    )
                    .await
                });
                saturating_add(&self.metrics.slot_wait_ns, elapsed_ns(started));
                match reserved {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) => return Sent::Closed,
                    Err(_) => return Sent::Detach,
                }
            }
        };
        if sink.is_closed() {
            return Sent::Closed;
        }
        // Never hold replay bytes while waiting for a slot. A failed charge
        // drops the permit and leaves starvation/backoff to the shared pass.
        let Some(charge) = self.budget.charge(weight) else {
            return Sent::Starved;
        };
        permit.send(Replayed {
            record: record.clone(),
            charge,
        });
        Sent::Delivered
    }
}

fn saturating_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

/// How a pass ended, for the partition's driver.
enum Pass {
    Done,
    Progress,
    Starved,
}

const STARVED_BACKOFF: Duration = Duration::from_millis(50);

#[derive(Debug, PartialEq, Eq)]
enum Sent {
    Delivered,
    Closed,
    Detach,
    Starved,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    struct UnusedSource;

    impl RangeSource for UnusedSource {
        fn open(&self, _: i32, _: i64) -> Result<Box<dyn RangeCursor>, RangeError> {
            panic!("slot tests must not read the source");
        }
    }

    fn record(offset: i64) -> Arc<LiveRecord> {
        Arc::new(LiveRecord {
            raw: Arc::new(RawRecord {
                partition: 0,
                offset,
                key: b"test-only".to_vec(),
                payload: Vec::new(),
                timestamp_ms: 0,
            }),
            envelope: Default::default(),
            shared_charge: None,
            dispatched_wall_ns: 0,
        })
    }

    fn coordinator(bytes: usize) -> Arc<ReplayCoordinator> {
        ReplayCoordinator::new(
            Arc::new(UnusedSource),
            1,
            ReplayLimits::default(),
            ByteBudget::new(bytes),
        )
    }

    fn fill(coordinator: &ReplayCoordinator, sink: &mpsc::Sender<Replayed>) {
        for offset in 0..REPLAY_CHANNEL {
            let record = record(offset as i64);
            sink.try_reserve().unwrap().send(Replayed {
                charge: coordinator.budget.charge(record.weight()).unwrap(),
                record,
            });
        }
        assert_eq!(sink.capacity(), 0);
    }

    fn send(
        coordinator: &Arc<ReplayCoordinator>,
        sink: &mpsc::Sender<Replayed>,
    ) -> tokio::task::JoinHandle<Sent> {
        let coordinator = coordinator.clone();
        let sink = sink.clone();
        tokio::task::spawn_blocking(move || {
            let record = record(REPLAY_CHANNEL as i64);
            coordinator.send_or_detach(&sink, &record, record.weight())
        })
    }

    async fn outcome(task: tokio::task::JoinHandle<Sent>) -> Sent {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("bounded slot operation")
            .expect("blocking reader")
    }

    async fn waiting(coordinator: &ReplayCoordinator) {
        // Synchronize on entry into the slow path, not a guessed sleep.
        tokio::time::timeout(Duration::from_secs(5), async {
            while coordinator.metrics.slot_waits.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reader entered the full-channel wait");
    }

    #[tokio::test]
    async fn available_slot_delivers_without_waiting_and_releases_charge() {
        let coordinator = coordinator(1 << 20);
        let (sink, mut receiver) = mpsc::channel(REPLAY_CHANNEL);
        assert_eq!(outcome(send(&coordinator, &sink)).await, Sent::Delivered);
        assert_eq!(coordinator.metrics.slot_waits.load(Ordering::Relaxed), 0);
        assert_eq!(coordinator.metrics.slot_wait_ns.load(Ordering::Relaxed), 0);
        let delivered = receiver.try_recv().unwrap();
        assert_eq!(delivered.record.raw.offset, REPLAY_CHANNEL as i64);
        assert_eq!(coordinator.budget.used(), delivered.record.weight());
        drop(delivered);
        assert_eq!(coordinator.budget.used(), 0);
        assert_eq!(sink.capacity(), REPLAY_CHANNEL);
    }

    #[tokio::test]
    async fn full_channel_wakes_on_a_slot_and_preserves_order_and_budget() {
        let coordinator = coordinator(1 << 20);
        let (sink, mut receiver) = mpsc::channel(REPLAY_CHANNEL);
        fill(&coordinator, &sink);
        let used = coordinator.budget.used();
        let task = send(&coordinator, &sink);
        waiting(&coordinator).await;
        assert_eq!(coordinator.budget.used(), used, "no charge while waiting");
        let first = receiver.try_recv().unwrap();
        assert_eq!(first.record.raw.offset, 0);
        drop(first);
        assert_eq!(outcome(task).await, Sent::Delivered);
        assert_eq!(coordinator.budget.used(), used);
        assert_eq!(coordinator.metrics.slot_waits.load(Ordering::Relaxed), 1);
        assert!(coordinator.metrics.slot_wait_ns.load(Ordering::Relaxed) > 0);
        for offset in 1..=REPLAY_CHANNEL {
            let delivered = receiver.try_recv().unwrap();
            assert_eq!(delivered.record.raw.offset, offset as i64);
            drop(delivered);
        }
        assert!(receiver.try_recv().is_err(), "no duplicate or extra record");
        assert_eq!(coordinator.budget.used(), 0);
        assert_eq!(sink.capacity(), REPLAY_CHANNEL);
    }

    #[tokio::test]
    async fn closed_channel_is_cancelled_before_or_during_slot_wait() {
        for during_wait in [false, true] {
            let coordinator = coordinator(1 << 20);
            let (sink, receiver) = mpsc::channel(REPLAY_CHANNEL);
            let task = if during_wait {
                fill(&coordinator, &sink);
                let task = send(&coordinator, &sink);
                waiting(&coordinator).await;
                drop(receiver);
                task
            } else {
                drop(receiver);
                send(&coordinator, &sink)
            };
            assert_eq!(outcome(task).await, Sent::Closed);
            assert_eq!(coordinator.budget.used(), 0);
            assert_eq!(
                coordinator.metrics.slot_waits.load(Ordering::Relaxed),
                u64::from(during_wait)
            );
        }
    }

    #[tokio::test]
    async fn full_channel_detaches_at_the_existing_deadline_without_leaking() {
        let coordinator = coordinator(1 << 20);
        let (sink, mut receiver) = mpsc::channel(REPLAY_CHANNEL);
        fill(&coordinator, &sink);
        let used = coordinator.budget.used();
        let started = Instant::now();
        assert_eq!(outcome(send(&coordinator, &sink)).await, Sent::Detach);
        assert!(started.elapsed() >= DETACH_AFTER);
        assert_eq!(coordinator.metrics.slot_waits.load(Ordering::Relaxed), 1);
        assert!(coordinator.metrics.slot_wait_ns.load(Ordering::Relaxed) > 0);
        assert_eq!(coordinator.budget.used(), used);
        for offset in 0..REPLAY_CHANNEL {
            let delivered = receiver.try_recv().unwrap();
            assert_eq!(delivered.record.raw.offset, offset as i64);
            drop(delivered);
        }
        assert!(receiver.try_recv().is_err(), "timed-out record was not sent");
        assert_eq!(sink.capacity(), REPLAY_CHANNEL, "no leaked permit");
        assert_eq!(coordinator.budget.used(), 0);
        assert_eq!(outcome(send(&coordinator, &sink)).await, Sent::Delivered);
        drop(receiver);
        assert_eq!(coordinator.budget.used(), 0);
    }

    #[tokio::test]
    async fn budget_starvation_releases_the_slot_on_both_paths() {
        let weight = record(0).weight();
        for full in [false, true] {
            let coordinator = coordinator(REPLAY_CHANNEL * weight);
            let (sink, mut receiver) = mpsc::channel(REPLAY_CHANNEL);
            let held = if full {
                fill(&coordinator, &sink);
                None
            } else {
                Some(coordinator.budget.charge(REPLAY_CHANNEL * weight).unwrap())
            };
            let task = send(&coordinator, &sink);
            let received = if full {
                waiting(&coordinator).await;
                // Free capacity but retain its charge, like a transport handoff.
                Some(receiver.try_recv().unwrap())
            } else {
                None
            };
            assert_eq!(outcome(task).await, Sent::Starved);
            assert_eq!(coordinator.budget.used(), REPLAY_CHANNEL * weight);
            assert_eq!(sink.capacity(), if full { 1 } else { REPLAY_CHANNEL });
            assert_eq!(
                coordinator.metrics.slot_waits.load(Ordering::Relaxed),
                u64::from(full)
            );
            drop((held, received, receiver));
            assert_eq!(coordinator.budget.used(), 0);
        }
    }

    #[test]
    fn diagnostic_counters_saturate_instead_of_wrapping() {
        let counter = AtomicU64::new(u64::MAX - 1);
        saturating_add(&counter, 2);
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
        saturating_add(&counter, 1);
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
    }
}
