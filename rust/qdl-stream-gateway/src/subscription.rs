//! One subscriber's bounded queue (KN-2 K2.2/K2.4, D1/D5).
//!
//! The live reader offers records without ever blocking on a subscriber: the
//! queue is bounded by items and by the full weight of what it references,
//! and a replica-wide budget bounds what is really held, so total queued
//! memory is bounded whatever the number of slow clients. A record decoded
//! once by the hub carries one shared budget charge for that copy
//! ([`LiveRecord::shared_charge`]); a queue entry then charges the budget
//! only [`QUEUE_ENTRY_OVERHEAD`], never the decoded weight once per
//! subscriber. Without a shared charge it charges the full weight, as
//! before. When the queue is full, a record is dropped only if
//! the record **right after it** has the same lifecycle key and signature
//! (`qdl_contracts::delivery`): a coalesced run keeps its latest record and
//! every transition between runs survives (A B A B stays four records).
//! Otherwise the subscription is marked overflowed and the stream ends with
//! the public BACKPRESSURE contract. A lossless record is never dropped to
//! stay healthy. The same budget also bounds replay channels, ring-replay
//! references and outbound handoffs ([`Charge`]).

use crate::hub::LiveRecord;
use crate::requirement::{delivery_decision, Delivery, StreamRequirement};
use qdl_contracts::delivery::{DeliveryPolicy, RecordLifecycle};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Notify;

/// A latest-state subscription holds at most this many records (DL-V2 R1.8,
/// `LATEST_STATE_BUFFER_EVENTS`): a deep FIFO of quotes only ages them out.
pub const LATEST_STATE_BUFFER_EVENTS: usize = 8;

/// Replica budget bytes of one queue entry beyond the shared record: the
/// `Queued` slot (~200 B) plus the lifecycle signature's strings.
pub const QUEUE_ENTRY_OVERHEAD: usize = 512;

/// Replica-wide byte budget shared by every subscriber queue.
#[derive(Debug)]
pub struct ByteBudget {
    limit: usize,
    used: AtomicUsize,
}

impl ByteBudget {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
        })
    }

    fn try_charge(&self, bytes: usize) -> bool {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                (used + bytes <= self.limit).then_some(used + bytes)
            })
            .is_ok()
    }

    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// Reserve `bytes` until the returned [`Charge`] is dropped; `None` when
    /// the budget is exhausted.
    pub fn charge(self: &Arc<Self>, bytes: usize) -> Option<Charge> {
        self.try_charge(bytes).then(|| Charge {
            budget: self.clone(),
            bytes,
        })
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

/// Bytes held against a [`ByteBudget`], released on drop. A record is
/// charged **once**, when it enters a queue, a replay channel or a ring
/// replay, and the charge travels with it to the transport: nothing waits
/// for a second charge while holding the first (no budget self-deadlock).
#[derive(Debug)]
pub struct Charge {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

impl Charge {
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Fold `other` into this charge (one release later, no budget change).
    /// Both must hold the same budget.
    pub fn absorb(&mut self, mut other: Charge) {
        assert!(
            Arc::ptr_eq(&self.budget, &other.budget),
            "charges of different budgets cannot be merged"
        );
        self.bytes += std::mem::take(&mut other.bytes);
    }

    /// Move up to `bytes` of this charge into a new one (no budget change).
    pub fn split(&mut self, bytes: usize) -> Charge {
        let moved = bytes.min(self.bytes);
        self.bytes -= moved;
        Charge {
            budget: self.budget.clone(),
            bytes: moved,
        }
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

#[derive(Debug)]
pub struct Queued {
    pub record: Arc<LiveRecord>,
    lifecycle: RecordLifecycle,
    weight: usize,
    enqueued: Instant,
    charge: Charge,
}

#[derive(Debug)]
pub enum Next {
    /// A record, when it was queued (the send-path queue wait) and its
    /// budget charge, which the caller hands on to the transport. The charge
    /// is only the entry overhead when the record carries a shared charge;
    /// the caller tops it up before it copies the record for the transport.
    Record(Arc<LiveRecord>, Instant, Charge),
    Overflow,
    Failed(String),
    Closed,
}

#[derive(Default, Debug)]
pub struct SubscriptionCounters {
    pub other_product: AtomicU64,
    pub rejected_at_push: AtomicU64,
    pub coalesced: AtomicU64,
    /// Eligible live enqueue attempts, including the attempt that overflows.
    pub offered: AtomicU64,
    /// Live queue pops, before read-time filtering or transport handoff; not ACKs.
    pub dequeued: AtomicU64,
}

#[derive(Debug)]
struct State {
    queue: VecDeque<Queued>,
    bytes: usize,
    /// Offsets at or below this were delivered or skipped already (dedup).
    floor: i64,
    overflowed: bool,
    failed: Option<String>,
    closed: bool,
}

#[derive(Debug)]
pub struct Subscription {
    pub id: u64,
    pub consumer_id: String,
    pub requirement: StreamRequirement,
    pub depth: usize,
    max_bytes: usize,
    budget: Arc<ByteBudget>,
    state: Mutex<State>,
    notify: Notify,
    pub counters: SubscriptionCounters,
}

impl Subscription {
    /// `after`: the cursor offset; nothing at or below it is ever queued.
    pub fn new(
        id: u64,
        consumer_id: String,
        requirement: StreamRequirement,
        buffer_events: usize,
        max_bytes: usize,
        budget: Arc<ByteBudget>,
        after: i64,
    ) -> Self {
        let feed_policy = qdl_contracts::delivery::delivery_policy(
            &requirement.delivery.feed,
            Some("IN_PROGRESS"),
        );
        let depth = if requirement.delivery.feed != "BAR"
            && feed_policy == Ok(DeliveryPolicy::LatestState)
        {
            buffer_events.clamp(1, LATEST_STATE_BUFFER_EVENTS)
        } else {
            buffer_events.max(1)
        };
        Self {
            id,
            consumer_id,
            requirement,
            depth,
            max_bytes,
            budget,
            state: Mutex::new(State {
                queue: VecDeque::new(),
                bytes: 0,
                floor: after,
                overflowed: false,
                failed: None,
                closed: false,
            }),
            notify: Notify::new(),
            counters: SubscriptionCounters::default(),
        }
    }

    /// Called by the live reader under the partition lock; never blocks on
    /// the client.
    pub fn offer(&self, record: &Arc<LiveRecord>, now_ns: i64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.closed || state.overflowed || state.failed.is_some() {
            return;
        }
        if record.raw.offset <= state.floor {
            return;
        }
        match delivery_decision(&self.requirement, &record.envelope, now_ns) {
            Delivery::OtherProduct => {
                self.counters.other_product.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Delivery::TooOld => {
                self.counters
                    .rejected_at_push
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            Delivery::Deliver => {}
        }
        self.counters.offered.fetch_add(1, Ordering::Relaxed);
        let lifecycle = RecordLifecycle::of(&self.requirement.delivery.feed, &record.envelope);
        // The per-subscription cap counts the full weight this queue keeps
        // alive; the replica budget only what this entry adds to memory.
        let weight = record.weight();
        let held = if record.shared_charge.is_some() {
            QUEUE_ENTRY_OVERHEAD
        } else {
            weight
        };
        let charge = loop {
            let fits = state.queue.len() < self.depth && state.bytes + weight <= self.max_bytes;
            if fits {
                if let Some(charge) = self.budget.charge(held) {
                    break charge;
                }
            }
            // Drop the oldest record that the record right after it
            // supersedes (Astra KN-2 R1 F5): coalescing stays inside one
            // contiguous run, so A B A B never loses a transition; a
            // lossless record is never dropped.
            let queued = state.queue.len();
            let droppable = (0..queued).find(|&index| {
                let next = if index + 1 < queued {
                    &state.queue[index + 1].lifecycle
                } else {
                    &lifecycle
                };
                state.queue[index].lifecycle.superseded_by(next)
            });
            match droppable {
                Some(index) => {
                    if let Some(dropped) = state.queue.remove(index) {
                        state.bytes -= dropped.weight;
                        self.counters.coalesced.fetch_add(1, Ordering::Relaxed);
                    }
                }
                None => {
                    state.overflowed = true;
                    drop(state);
                    self.notify.notify_one();
                    return;
                }
            }
        };
        state.bytes += weight;
        state.floor = record.raw.offset;
        state.queue.push_back(Queued {
            record: record.clone(),
            lifecycle,
            weight,
            enqueued: Instant::now(),
            charge,
        });
        drop(state);
        self.notify.notify_one();
    }

    pub fn fail(&self, reason: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.failed.get_or_insert_with(|| reason.to_owned());
        }
        self.notify.notify_one();
    }

    /// The next queued record, waiting for one. Overflow and failure win over
    /// queued records: the stream must end with a typed signal.
    pub async fn next(&self) -> Next {
        loop {
            let notified = self.notify.notified();
            {
                let Ok(mut state) = self.state.lock() else {
                    return Next::Failed("subscription state poisoned".into());
                };
                if let Some(reason) = &state.failed {
                    return Next::Failed(reason.clone());
                }
                if state.overflowed {
                    return Next::Overflow;
                }
                if let Some(item) = state.queue.pop_front() {
                    state.bytes -= item.weight;
                    self.counters.dequeued.fetch_add(1, Ordering::Relaxed);
                    return Next::Record(item.record, item.enqueued, item.charge);
                }
                if state.closed {
                    return Next::Closed;
                }
            }
            notified.await;
        }
    }

    pub fn queued(&self) -> (usize, usize) {
        self.state
            .lock()
            .map(|state| (state.queue.len(), state.bytes))
            .unwrap_or_default()
    }

    /// Release everything still queued (the stream ended).
    pub fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            state.queue.clear();
            state.bytes = 0;
        }
        self.notify.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::marketdata_v2::{
        event_envelope, Bar, BarLifecycle, EventEnvelope, OrderBookDelta, Quote, Trade,
    };
    use crate::generated::query_v2 as query;
    use crate::hub::RawRecord;
    use prost::Message as _;

    fn requirement(feed: query::FeedType) -> StreamRequirement {
        StreamRequirement::from_proto(&query::DataRequirement {
            instrument_uid: "u".into(),
            interval: if feed == query::FeedType::Bar {
                "1m".into()
            } else {
                String::new()
            },
            source_policy_id: "p".into(),
            require_full_coverage: true,
            require_final_bars: true,
            feed_type: feed as i32,
            grade: query::ConsumerGrade::Alpha as i32,
            stale_policy_type: query::StalePolicy::Block as i32,
            gap_policy_type: query::GapPolicy::Block as i32,
            recovery_policy: query::RecoveryPolicy::SnapshotAndReplay as i32,
            revision_policy: query::BarRevisionPolicy::Latest as i32,
            ..Default::default()
        })
        .expect("requirement")
    }

    fn record(offset: i64, payload: event_envelope::Payload, flags: Vec<i32>) -> Arc<LiveRecord> {
        let envelope = EventEnvelope {
            quality_flags: flags,
            payload: Some(payload),
            ..Default::default()
        };
        Arc::new(LiveRecord {
            raw: Arc::new(RawRecord {
                partition: 0,
                offset,
                key: b"k".to_vec(),
                payload: envelope.encode_to_vec(),
                timestamp_ms: 0,
            }),
            envelope,
            shared_charge: None,
        })
    }

    fn quote(offset: i64, flags: Vec<i32>) -> Arc<LiveRecord> {
        record(
            offset,
            event_envelope::Payload::Quote(Quote::default()),
            flags,
        )
    }

    async fn drain(subscription: &Subscription) -> Vec<i64> {
        let mut offsets = Vec::new();
        while subscription.queued().0 > 0 {
            match subscription.next().await {
                Next::Record(record, _, _) => offsets.push(record.raw.offset),
                other => panic!("{other:?}"),
            }
        }
        offsets
    }

    fn counts(subscription: &Subscription) -> (u64, u64) {
        (
            subscription.counters.offered.load(Ordering::Relaxed),
            subscription.counters.dequeued.load(Ordering::Relaxed),
        )
    }

    #[test]
    fn the_queue_entry_overhead_covers_an_entry() {
        // The slot itself; the lifecycle signature's short ids live in the rest.
        assert!(
            std::mem::size_of::<Queued>() * 2 <= QUEUE_ENTRY_OVERHEAD,
            "{}",
            std::mem::size_of::<Queued>()
        );
    }

    #[tokio::test]
    async fn counters_count_eligible_attempts_and_local_pops_only() {
        let mut subscription = queue(query::FeedType::Trade, 2);
        subscription.requirement.delivery.max_freshness_ms = Some(1);
        let trade = |offset| {
            record(
                offset,
                event_envelope::Payload::Trade(Trade::default()),
                vec![],
            )
        };
        assert_eq!(counts(&subscription), (0, 0));
        subscription.offer(&trade(0), 0); // At cursor floor.
        subscription.offer(&quote(1, vec![]), 0); // Other product.
        subscription.offer(&trade(1), 2_000_000); // Too old at push.
        assert_eq!(counts(&subscription), (0, 0));
        assert_eq!(
            subscription.counters.other_product.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            subscription
                .counters
                .rejected_at_push
                .load(Ordering::Relaxed),
            1
        );
        subscription.offer(&trade(1), 0);
        subscription.offer(&trade(1), 0); // Duplicate.
        assert_eq!(counts(&subscription), (1, 0));
        let charge = match subscription.next().await {
            Next::Record(record, _, charge) => {
                // This record is already counted even if a later read rejects it.
                assert_eq!(
                    delivery_decision(&subscription.requirement, &record.envelope, 2_000_000),
                    Delivery::TooOld
                );
                charge
            }
            other => panic!("{other:?}"),
        };
        assert_eq!(counts(&subscription), (1, 1));
        assert!(
            subscription.budget.used() > 0,
            "handoff still owns its bytes"
        );
        drop(charge);
        assert_eq!(subscription.budget.used(), 0);
        subscription.offer(&trade(2), 0);
        subscription.close(); // Discard is not a dequeue.
        subscription.offer(&trade(3), 0);
        assert!(matches!(subscription.next().await, Next::Closed));
        assert_eq!(counts(&subscription), (2, 1));
    }

    #[tokio::test]
    async fn failure_and_budget_overflow_do_not_count_as_dequeues() {
        let failed = queue(query::FeedType::Trade, 2);
        let trade = record(1, event_envelope::Payload::Trade(Trade::default()), vec![]);
        failed.offer(&trade, 0);
        failed.fail("test-only failure");
        failed.offer(
            &record(2, event_envelope::Payload::Trade(Trade::default()), vec![]),
            0,
        );
        assert!(matches!(failed.next().await, Next::Failed(_)));
        assert_eq!(counts(&failed), (1, 0));
        failed.close();
        assert_eq!(failed.budget.used(), 0);

        let mut overflow = queue(query::FeedType::Trade, 2);
        overflow.budget = ByteBudget::new(0);
        overflow.offer(&trade, 0);
        assert!(matches!(overflow.next().await, Next::Overflow));
        assert_eq!(counts(&overflow), (1, 0));
        assert_eq!(overflow.queued(), (0, 0));
        assert_eq!(overflow.budget.used(), 0);
        assert_eq!(counts(&failed), (1, 0), "counters are subscription-local");
    }

    #[tokio::test]
    async fn a_latest_state_queue_keeps_the_newest_and_every_transition() {
        let budget = ByteBudget::new(1 << 20);
        let subscription = Subscription::new(
            1,
            "c".into(),
            requirement(query::FeedType::Quote),
            1_000,
            1 << 20,
            budget.clone(),
            0,
        );
        assert_eq!(subscription.depth, LATEST_STATE_BUFFER_EVENTS);
        // 1..=10 in quality state A, 11..=20 in state B.
        for offset in 1..=20 {
            let flags = if offset > 10 { vec![9] } else { vec![] };
            subscription.offer(&quote(offset, flags), 0);
        }
        let delivered = drain(&subscription).await;
        assert!(
            delivered.len() <= LATEST_STATE_BUFFER_EVENTS,
            "{delivered:?}"
        );
        assert_eq!(*delivered.last().unwrap(), 20, "newest kept");
        assert!(
            delivered.contains(&10),
            "the last record of state A survives: the transition is not coalesced away {delivered:?}"
        );
        assert!(delivered.windows(2).all(|pair| pair[1] > pair[0]));
        assert_eq!(budget.used(), 0, "every byte released after draining");
        assert_eq!(counts(&subscription), (20, delivered.len() as u64));
        assert_eq!(
            subscription.counters.coalesced.load(Ordering::Relaxed),
            20 - delivered.len() as u64
        );
    }

    #[tokio::test]
    async fn a_lossless_queue_overflows_instead_of_dropping() {
        let budget = ByteBudget::new(1 << 20);
        let subscription = Subscription::new(
            2,
            "c".into(),
            requirement(query::FeedType::Trade),
            4,
            1 << 20,
            budget.clone(),
            0,
        );
        for offset in 1..=5 {
            subscription.offer(
                &record(
                    offset,
                    event_envelope::Payload::Trade(Trade::default()),
                    vec![],
                ),
                0,
            );
        }
        assert!(matches!(subscription.next().await, Next::Overflow));
        assert_eq!(subscription.counters.coalesced.load(Ordering::Relaxed), 0);
        assert_eq!(counts(&subscription), (5, 0));
        subscription.offer(
            &record(6, event_envelope::Payload::Trade(Trade::default()), vec![]),
            0,
        );
        assert_eq!(counts(&subscription), (5, 0));
        subscription.close();
        assert_eq!(counts(&subscription), (5, 0));
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn records_at_or_below_the_cursor_are_never_queued() {
        let subscription = Subscription::new(
            3,
            "c".into(),
            requirement(query::FeedType::Trade),
            10,
            1 << 20,
            ByteBudget::new(1 << 20),
            5,
        );
        for offset in [3, 5, 6, 6, 7] {
            subscription.offer(
                &record(
                    offset,
                    event_envelope::Payload::Trade(Trade::default()),
                    vec![],
                ),
                0,
            );
        }
        assert_eq!(drain(&subscription).await, vec![6, 7]);
    }

    fn queue(feed: query::FeedType, depth: usize) -> Subscription {
        Subscription::new(
            9,
            "c".into(),
            requirement(feed),
            depth,
            1 << 20,
            ByteBudget::new(1 << 20),
            0,
        )
    }

    fn bar(offset: i64, open_minute: i64, lifecycle: BarLifecycle) -> Arc<LiveRecord> {
        record(
            offset,
            event_envelope::Payload::Bar(Bar {
                interval: "1m".into(),
                open_time_ns: open_minute * 60_000_000_000,
                lifecycle: lifecycle as i32,
                ..Default::default()
            }),
            vec![],
        )
    }

    /// Everything queued, or `Err(delivered so far)` on a typed overflow.
    async fn outcome(subscription: &Subscription) -> Result<Vec<i64>, Vec<i64>> {
        let mut offsets = Vec::new();
        loop {
            if subscription.queued().0 == 0 && !subscription.state.lock().unwrap().overflowed {
                return Ok(offsets);
            }
            match subscription.next().await {
                Next::Record(record, _, _) => offsets.push(record.raw.offset),
                Next::Overflow => return Err(offsets),
                other => panic!("{other:?}"),
            }
        }
    }

    // Astra KN-2 R1 F5: coalescing stays inside a contiguous run.
    #[tokio::test]
    async fn f5_a_b_a_b_never_loses_a_transition() {
        let subscription = queue(query::FeedType::Quote, 2);
        for (offset, flags) in [(1, vec![]), (2, vec![9]), (3, vec![]), (4, vec![9])] {
            subscription.offer(&quote(offset, flags), 0);
        }
        // Nothing may be coalesced: every record is a transition; the queue
        // overflows (typed backpressure) and nothing was dropped silently.
        assert_eq!(outcome(&subscription).await, Err(vec![]));
        assert_eq!(subscription.counters.coalesced.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn f5_a_b_a_keeps_both_transitions_or_overflows() {
        let subscription = queue(query::FeedType::Quote, 2);
        for (offset, flags) in [(1, vec![]), (2, vec![9]), (3, vec![])] {
            subscription.offer(&quote(offset, flags), 0);
        }
        assert_eq!(outcome(&subscription).await, Err(vec![]));
        // With room for all three, all three arrive.
        let roomy = queue(query::FeedType::Quote, 3);
        for (offset, flags) in [(1, vec![]), (2, vec![9]), (3, vec![])] {
            roomy.offer(&quote(offset, flags), 0);
        }
        assert_eq!(outcome(&roomy).await, Ok(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn f5_same_state_bursts_coalesce_to_the_last_of_each_run() {
        // Runs A(1..=5) B(6..=9) A(10..=12): the last of every run survives.
        let subscription = queue(query::FeedType::Quote, 3);
        for offset in 1..=12 {
            let flags = if (6..=9).contains(&offset) {
                vec![9]
            } else {
                vec![]
            };
            subscription.offer(&quote(offset, flags), 0);
        }
        assert_eq!(outcome(&subscription).await, Ok(vec![5, 9, 12]));
    }

    #[tokio::test]
    async fn f5_bar_in_progress_coalesces_only_within_one_open_time() {
        let subscription = queue(query::FeedType::Bar, 3);
        // minute 1: three updates then final; minute 2: two updates.
        subscription.offer(&bar(1, 1, BarLifecycle::InProgress), 0);
        subscription.offer(&bar(2, 1, BarLifecycle::InProgress), 0);
        subscription.offer(&bar(3, 1, BarLifecycle::InProgress), 0);
        subscription.offer(&bar(4, 1, BarLifecycle::Final), 0);
        subscription.offer(&bar(5, 2, BarLifecycle::InProgress), 0);
        // Updates of minute 1 coalesce into its final; the final is kept.
        assert_eq!(outcome(&subscription).await, Ok(vec![3, 4, 5]));
        // A revision after the final is lossless: final -> revised overflows
        // a full queue rather than replacing the final.
        let revised = queue(query::FeedType::Bar, 1);
        revised.offer(&bar(1, 1, BarLifecycle::Final), 0);
        revised.offer(&bar(2, 1, BarLifecycle::Revised), 0);
        assert_eq!(outcome(&revised).await, Err(vec![]));
    }

    #[tokio::test]
    async fn f5_book_records_and_other_products_never_coalesce() {
        let book = queue(query::FeedType::BookDelta, 2);
        for offset in 1..=3 {
            book.offer(
                &record(
                    offset,
                    event_envelope::Payload::BookDelta(OrderBookDelta {
                        reset: offset == 2,
                        ..Default::default()
                    }),
                    vec![],
                ),
                0,
            );
        }
        assert_eq!(outcome(&book).await, Err(vec![]));
        // A trade offered to a quote subscription is another product: never
        // queued, never a supersessor.
        let quotes = queue(query::FeedType::Quote, 2);
        quotes.offer(&quote(1, vec![]), 0);
        quotes.offer(
            &record(2, event_envelope::Payload::Trade(Trade::default()), vec![]),
            0,
        );
        quotes.offer(&quote(3, vec![9]), 0);
        assert_eq!(outcome(&quotes).await, Ok(vec![1, 3]));
    }
}
