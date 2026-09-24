//! One subscriber's bounded queue (KN-2 K2.2/K2.4, D1/D5).
//!
//! The live reader offers records without ever blocking on a subscriber: the
//! queue is bounded by items and bytes, and bytes are also charged to a
//! replica-wide budget, so total queued memory is bounded whatever the
//! number of slow clients. When the queue is full, a record is dropped only if
//! a later record of the same lifecycle key and signature supersedes it
//! (`qdl_contracts::delivery`); otherwise the subscription is marked
//! overflowed and the stream ends with the public BACKPRESSURE contract. A
//! lossless record is never dropped to stay healthy.

use crate::hub::LiveRecord;
use crate::requirement::{delivery_decision, Delivery, StreamRequirement};
use qdl_contracts::delivery::{DeliveryPolicy, RecordLifecycle};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// A latest-state subscription holds at most this many records (DL-V2 R1.8,
/// `LATEST_STATE_BUFFER_EVENTS`): a deep FIFO of quotes only ages them out.
pub const LATEST_STATE_BUFFER_EVENTS: usize = 8;

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

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub struct Queued {
    pub record: Arc<LiveRecord>,
    lifecycle: RecordLifecycle,
    weight: usize,
}

#[derive(Debug)]
pub enum Next {
    Record(Arc<LiveRecord>),
    Overflow,
    Failed(String),
    Closed,
}

#[derive(Default, Debug)]
pub struct SubscriptionCounters {
    pub other_product: AtomicU64,
    pub rejected_at_push: AtomicU64,
    pub coalesced: AtomicU64,
    pub offered: AtomicU64,
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
        let weight = record.raw.weight();
        loop {
            let fits = state.queue.len() < self.depth && state.bytes + weight <= self.max_bytes;
            if fits && self.budget.try_charge(weight) {
                break;
            }
            // Drop the oldest record some later record supersedes; a
            // lossless record, or a transition, is never dropped.
            let droppable = (0..state.queue.len()).find(|&index| {
                let candidate = &state.queue[index].lifecycle;
                candidate.superseded_by(&lifecycle)
                    || state
                        .queue
                        .iter()
                        .skip(index + 1)
                        .any(|later| candidate.superseded_by(&later.lifecycle))
            });
            match droppable {
                Some(index) => {
                    if let Some(dropped) = state.queue.remove(index) {
                        state.bytes -= dropped.weight;
                        self.budget.release(dropped.weight);
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
        }
        state.bytes += weight;
        state.floor = record.raw.offset;
        state.queue.push_back(Queued {
            record: record.clone(),
            lifecycle,
            weight,
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
                    self.budget.release(item.weight);
                    return Next::Record(item.record);
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
            for item in state.queue.drain(..) {
                self.budget.release(item.weight);
            }
            state.bytes = 0;
        }
        self.notify.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::marketdata_v2::{event_envelope, EventEnvelope, Quote, Trade};
    use crate::generated::query_v2 as query;
    use crate::hub::RawRecord;
    use prost::Message as _;

    fn requirement(feed: query::FeedType) -> StreamRequirement {
        StreamRequirement::from_proto(&query::DataRequirement {
            instrument_uid: "u".into(),
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
            }),
            envelope,
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
                Next::Record(record) => offsets.push(record.raw.offset),
                other => panic!("{other:?}"),
            }
        }
        offsets
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
        subscription.close();
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
}
