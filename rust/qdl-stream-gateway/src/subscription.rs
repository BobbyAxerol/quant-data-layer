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

/// A subscription holding at most this many other records (queued, or
/// dequeued and not yet released by the transport) is at its floor whatever
/// their size: the previous record of an idle reader is often still held
/// by the transport when the next one arrives.
pub const LIVE_FLOOR_ENTRIES: usize = 1;

/// Replica-wide byte budget shared by every subscriber queue.
///
/// A live budget ([`ByteBudget::live`]) keeps `reserve` bytes that only a
/// subscription at or under its floor may use: [`ByteBudget::charge`] admits
/// up to `limit - reserve`, [`ByteBudget::charge_within_floor`] up to
/// `limit`. Deep subscriptions therefore cannot exhaust what an idle one
/// needs, and the total never exceeds `limit`. [`ByteBudget::new`] has no
/// reserve (replay, tests): both admit up to `limit`.
#[derive(Debug)]
pub struct ByteBudget {
    limit: usize,
    reserve: usize,
    floor: usize,
    used: AtomicUsize,
    pub counters: BudgetCounters,
}

/// Which isolation branch fired (all zero without a reserve).
#[derive(Default, Debug)]
pub struct BudgetCounters {
    /// Charges of a subscription at or under its floor admitted only
    /// because of the reserve (they did not fit `limit - reserve`).
    pub reserve_admitted: AtomicU64,
    /// Lossless pushes of a subscription over its floor refused because the
    /// shared part was exhausted: that subscription alone overflows.
    pub over_floor_refused: AtomicU64,
    /// Pushes of a subscription at or under its floor refused even with the
    /// reserve: the reserve itself was exhausted (should stay 0).
    pub reserve_exhausted: AtomicU64,
    /// Transport top-ups ([`Subscription::charge_handoff`]) refused at least
    /// once, by the floor state at the first refusal (one per record, not
    /// per retry). The enqueue counters above never see these.
    pub handoff_refused_within_floor: AtomicU64,
    pub handoff_refused_over_floor: AtomicU64,
    /// Top-ups that never fitted before the slow-consumer deadline: the
    /// stream was closed with BACKPRESSURE at handoff, by the floor state at
    /// the last attempt.
    pub handoff_timeout_within_floor: AtomicU64,
    pub handoff_timeout_over_floor: AtomicU64,
}

impl ByteBudget {
    pub fn new(limit: usize) -> Arc<Self> {
        Self::with_reserve(limit, 0, 0)
    }

    /// The replica live budget: reserve and floor derived from `limit`
    /// ([`crate::memory::live_reserve_and_floor`]).
    pub fn live(limit: usize) -> Arc<Self> {
        let (reserve, floor) = crate::memory::live_reserve_and_floor(limit as u64);
        Self::with_reserve(limit, reserve as usize, floor as usize)
    }

    pub fn with_reserve(limit: usize, reserve: usize, floor: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            reserve: reserve.min(limit),
            floor,
            used: AtomicUsize::new(0),
            counters: BudgetCounters::default(),
        })
    }

    fn try_charge(&self, bytes: usize, ceiling: usize) -> bool {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                (used + bytes <= ceiling).then_some(used + bytes)
            })
            .is_ok()
    }

    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
    }

    fn charge_up_to(self: &Arc<Self>, bytes: usize, ceiling: usize) -> Option<Charge> {
        self.try_charge(bytes, ceiling).then(|| Charge {
            budget: self.clone(),
            bytes,
            records: 0,
            owner: None,
        })
    }

    /// Reserve `bytes` of the shared part (`limit - reserve`) until the
    /// returned [`Charge`] is dropped; `None` when it is exhausted.
    pub fn charge(self: &Arc<Self>, bytes: usize) -> Option<Charge> {
        self.charge_up_to(bytes, self.limit - self.reserve)
    }

    /// [`Self::charge`] for a holder at or under its floor: may also use the
    /// reserve, never beyond `limit`.
    pub fn charge_within_floor(self: &Arc<Self>, bytes: usize) -> Option<Charge> {
        if let Some(charge) = self.charge(bytes) {
            return Some(charge);
        }
        let charge = self.charge_up_to(bytes, self.limit)?;
        self.counters
            .reserve_admitted
            .fetch_add(1, Ordering::Relaxed);
        Some(charge)
    }

    /// Charge for a subscription holding `held` full-weight bytes in
    /// `queued` entries that wants `bytes` more of `weight` full weight.
    fn charge_for(
        self: &Arc<Self>,
        bytes: usize,
        queued: usize,
        held: usize,
        weight: usize,
    ) -> Option<Charge> {
        if self.within_floor(queued, held, weight) {
            self.charge_within_floor(bytes)
        } else {
            self.charge(bytes)
        }
    }

    pub(crate) fn within_floor(&self, queued: usize, held: usize, weight: usize) -> bool {
        self.reserve > 0 && (queued <= LIVE_FLOOR_ENTRIES || held + weight <= self.floor)
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    /// `(reserve, floor)` bytes; `(0, 0)` without a reserve.
    pub fn reserve_and_floor(&self) -> (usize, usize) {
        (self.reserve, self.floor)
    }
}

/// Bytes held against a [`ByteBudget`], released on drop. A record is
/// charged **once**, when it enters a queue, a replay channel or a ring
/// replay, and the charge travels with it to the transport: nothing waits
/// for a second charge while holding the first (no budget self-deadlock).
///
/// Once a live record leaves its subscription's queue, its charge is
/// attributed to that subscription (`owner`) until the transport drops it,
/// so the floor test sees what a stalled stream holds in its outbound
/// handoff and transport, not only in its queue: its bytes AND the number of
/// records it stands for (`records`: 1 for a queue entry, 0 for a top-up or
/// a non-record charge, the sum for a batch), so two handed-off records are
/// two entries, never one.
#[derive(Debug)]
pub struct Charge {
    budget: Arc<ByteBudget>,
    bytes: usize,
    records: usize,
    owner: Option<Arc<Outstanding>>,
}

/// A subscription's live records dequeued but not yet released by the
/// transport (handoff channel, batch, tonic): exact record count and bytes.
#[derive(Debug, Default)]
pub struct Outstanding {
    records: AtomicUsize,
    bytes: AtomicUsize,
}

impl Outstanding {
    fn add(&self, records: usize, bytes: usize) {
        self.records.fetch_add(records, Ordering::AcqRel);
        self.bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    fn sub(&self, records: usize, bytes: usize) {
        self.records.fetch_sub(records, Ordering::AcqRel);
        self.bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// `(records, bytes)` now.
    pub fn load(&self) -> (usize, usize) {
        (
            self.records.load(Ordering::Acquire),
            self.bytes.load(Ordering::Acquire),
        )
    }
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
        let bytes = std::mem::take(&mut other.bytes);
        let records = std::mem::take(&mut other.records);
        if let Some(owner) = &other.owner {
            owner.sub(records, bytes);
        }
        if let Some(owner) = &self.owner {
            owner.add(records, bytes);
        }
        self.bytes += bytes;
        self.records += records;
    }

    /// Records this charge stands for (see [`Charge`]).
    pub fn records(&self) -> usize {
        self.records
    }

    /// Attribute this charge to a subscription's outstanding records until
    /// it is dropped (or absorbed into another charge).
    fn attribute(&mut self, owner: &Arc<Outstanding>) {
        if let Some(previous) = self.owner.replace(owner.clone()) {
            previous.sub(self.records, self.bytes);
        }
        owner.add(self.records, self.bytes);
    }

    /// Move up to `bytes` of this charge into a new one (no budget change).
    /// Bytes only: the record count stays with `self`.
    pub fn split(&mut self, bytes: usize) -> Charge {
        let moved = bytes.min(self.bytes);
        self.bytes -= moved;
        Charge {
            budget: self.budget.clone(),
            bytes: moved,
            records: 0,
            owner: self.owner.clone(),
        }
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        if let Some(owner) = &self.owner {
            owner.sub(self.records, self.bytes);
        }
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
    /// Records whose transport top-up was refused at least once.
    pub handoff_refused: AtomicU64,
    /// Top-ups that timed out: the stream ended BACKPRESSURE at handoff.
    pub handoff_timeouts: AtomicU64,
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
    /// Live records (exact count) and budget bytes dequeued but not yet
    /// released by the transport (handoff channel, batches, tonic): counted
    /// with the queue when deciding whether this subscription is at or under
    /// its floor.
    out_of_queue: Arc<Outstanding>,
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
            out_of_queue: Arc::new(Outstanding::default()),
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
        // Refused for want of replica budget (not this queue's own bounds),
        // and whether this subscription was at or under its floor then.
        let mut budget_refused: Option<bool>;
        let mut charge = loop {
            let fits = state.queue.len() < self.depth && state.bytes + weight <= self.max_bytes;
            budget_refused = None;
            if fits {
                let (queued, bytes) = self.floor_load(&state, None);
                if let Some(charge) = self.budget.charge_for(held, queued, bytes, weight) {
                    break charge;
                }
                budget_refused = Some(self.budget.within_floor(queued, bytes, weight));
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
                    let counter = match budget_refused {
                        Some(true) => Some(&self.budget.counters.reserve_exhausted),
                        Some(false) if self.budget.reserve > 0 => {
                            Some(&self.budget.counters.over_floor_refused)
                        }
                        _ => None,
                    };
                    if let Some(counter) = counter {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    state.overflowed = true;
                    drop(state);
                    self.notify.notify_one();
                    return;
                }
            }
        };
        // One queue entry is one record wherever its charge travels next.
        charge.records = 1;
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

    /// Charge `bytes` of the live budget for this subscription's transport
    /// copy of a dequeued record of `weight` whose entry charge (`entry`,
    /// already out of the queue) the caller holds: a subscription at or
    /// under its floor (queue plus its other outstanding records) may use
    /// the reserve, so an idle reader is never stalled at handoff by deep
    /// ones. `Err(within_floor)` when refused: whether this subscription was
    /// at or under its floor then (attribute it with
    /// [`Self::note_handoff_refused`] / [`Self::note_handoff_timeout`]).
    pub fn charge_handoff(&self, bytes: usize, weight: usize, entry: &Charge) -> Result<Charge, bool> {
        let (queued, held) = match self.state.lock() {
            Ok(state) => self.floor_load(&state, Some(entry)),
            Err(_) => return Err(false),
        };
        match self.budget.charge_for(bytes, queued, held, weight) {
            Some(mut charge) => {
                charge.attribute(&self.out_of_queue);
                Ok(charge)
            }
            None => Err(self.budget.within_floor(queued, held, weight)),
        }
    }

    /// The first refused top-up of one record (not every retry).
    pub fn note_handoff_refused(&self, within_floor: bool) {
        self.counters.handoff_refused.fetch_add(1, Ordering::Relaxed);
        let counters = &self.budget.counters;
        let counter = if within_floor {
            &counters.handoff_refused_within_floor
        } else {
            &counters.handoff_refused_over_floor
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// A top-up that timed out and ends the stream; `within_floor` at the
    /// last attempt.
    pub fn note_handoff_timeout(&self, within_floor: bool) {
        self.counters.handoff_timeouts.fetch_add(1, Ordering::Relaxed);
        let counters = &self.budget.counters;
        let counter = if within_floor {
            &counters.handoff_timeout_within_floor
        } else {
            &counters.handoff_timeout_over_floor
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// `(entries, bytes)` the floor test sees: the queue plus every record
    /// dequeued and not yet released, each counted as one entry, except
    /// exactly `own` (the record being topped up, when it is attributed to
    /// this subscription).
    fn floor_load(&self, state: &State, own: Option<&Charge>) -> (usize, usize) {
        let (mut records, mut bytes) = self.out_of_queue.load();
        if let Some(own) = own.filter(|own| {
            own.owner
                .as_ref()
                .is_some_and(|owner| Arc::ptr_eq(owner, &self.out_of_queue))
        }) {
            records = records.saturating_sub(own.records);
            bytes = bytes.saturating_sub(own.bytes);
        }
        (state.queue.len() + records, state.bytes + bytes)
    }

    /// `(records, bytes)` dequeued and not yet released by the transport.
    pub fn outstanding(&self) -> (usize, usize) {
        self.out_of_queue.load()
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
                if let Some(mut item) = state.queue.pop_front() {
                    state.bytes -= item.weight;
                    item.charge.attribute(&self.out_of_queue);
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

    /// The next queued record only if one is already available: never waits
    /// (batched delivery packs what is queued, it does not wait to fill).
    /// `None` also when the stream must end; [`Self::next`] then reports the
    /// overflow, failure or close, so a batch never crosses that boundary.
    pub fn try_next(&self) -> Option<(Arc<LiveRecord>, Instant, Charge)> {
        let mut state = self.state.lock().ok()?;
        if state.failed.is_some() || state.overflowed || state.closed {
            return None;
        }
        let mut item = state.queue.pop_front()?;
        state.bytes -= item.weight;
        item.charge.attribute(&self.out_of_queue);
        self.counters.dequeued.fetch_add(1, Ordering::Relaxed);
        Some((item.record, item.enqueued, item.charge))
    }

    /// Linearization check before announcing LIVE. A later failure remains a
    /// normal live terminal event; an already latched failure cannot be hidden.
    pub fn terminal(&self) -> Option<Next> {
        let Ok(state) = self.state.lock() else {
            return Some(Next::Failed("subscription state poisoned".into()));
        };
        if let Some(reason) = &state.failed { Some(Next::Failed(reason.clone())) }
        else if state.overflowed { Some(Next::Overflow) }
        else if state.closed { Some(Next::Closed) }
        else { None }
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
            dispatched_wall_ns: 0,
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
    async fn try_next_takes_only_queued_records_and_never_crosses_an_end() {
        let trade = |offset| {
            record(
                offset,
                event_envelope::Payload::Trade(Trade::default()),
                vec![],
            )
        };
        let subscription = queue(query::FeedType::Trade, 8);
        assert!(subscription.try_next().is_none(), "never waits");
        for offset in 1..=3 {
            subscription.offer(&trade(offset), 0);
        }
        let (first, _, charge) = subscription.try_next().expect("queued");
        assert_eq!(first.raw.offset, 1);
        assert_eq!(counts(&subscription), (3, 1));
        assert_eq!(subscription.queued().0, 2);
        drop(charge);
        // A failure ends batching even with records queued; next() reports it.
        subscription.fail("test-only failure");
        assert!(subscription.try_next().is_none());
        assert!(matches!(subscription.next().await, Next::Failed(_)));
        subscription.close();
        assert_eq!(subscription.budget.used(), 0);

        let overflow = queue(query::FeedType::Trade, 1);
        overflow.offer(&trade(1), 0);
        overflow.offer(&trade(2), 0); // Lossless: overflows, nothing dropped.
        assert!(overflow.try_next().is_none());
        assert!(matches!(overflow.next().await, Next::Overflow));
        overflow.close();
        assert_eq!(overflow.budget.used(), 0);
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

    // ------------------------------------------- SR1 exact outstanding count

    /// A lossless record of about `kib` KiB full weight, no shared charge
    /// (its queue entry charges the full weight).
    fn heavy(offset: i64, kib: usize) -> Arc<LiveRecord> {
        let id = "x".repeat(kib * 1024 / (crate::hub::DECODED_FACTOR + 1));
        record(
            offset,
            event_envelope::Payload::Trade(Trade {
                native_trade_id: id,
                ..Default::default()
            }),
            vec![],
        )
    }

    /// A reserve budget (floor 256 KiB, as production) whose shared part
    /// holds `shared` bytes, and a lossless subscription on it.
    fn isolated(shared: usize, reserve: usize) -> (Arc<ByteBudget>, Subscription) {
        let budget = ByteBudget::with_reserve(shared + reserve, reserve, 256 * 1024);
        let subscription = Subscription::new(
            11,
            "c".into(),
            requirement(query::FeedType::Trade),
            100,
            usize::MAX,
            budget.clone(),
            0,
        );
        (budget, subscription)
    }

    /// Take the rest of the shared part, as deep subscriptions do.
    fn fill_shared(budget: &Arc<ByteBudget>) -> Charge {
        let (reserve, _) = budget.reserve_and_floor();
        let rest = budget.limit() - reserve - budget.used();
        budget.charge(rest).expect("shared rest")
    }

    async fn take(subscription: &Subscription) -> Charge {
        match subscription.next().await {
            Next::Record(_, _, charge) => charge,
            other => panic!("{other:?}"),
        }
    }

    fn admitted(budget: &ByteBudget) -> (u64, u64, u64) {
        let counters = &budget.counters;
        (
            counters.reserve_admitted.load(Ordering::Relaxed),
            counters.over_floor_refused.load(Ordering::Relaxed),
            counters.reserve_exhausted.load(Ordering::Relaxed),
        )
    }

    #[tokio::test]
    async fn sr1_two_large_outbound_records_and_another_offer_stay_out_of_the_reserve() {
        // Review SR1 counterexample: empty queue, two unreleased 200 KiB
        // records at the transport, shared part full. The old floor saw ONE
        // entry and let the third offer into the reserve beyond 256 KiB.
        let weight = heavy(1, 200).weight();
        assert!(weight > 128 * 1024 && weight < 256 * 1024, "{weight}");
        let (budget, subscription) = isolated(8 * weight, 4 * weight);
        subscription.offer(&heavy(1, 200), 0);
        subscription.offer(&heavy(2, 200), 0);
        let first = take(&subscription).await;
        let second = take(&subscription).await;
        assert_eq!(subscription.outstanding(), (2, 2 * weight));
        let hog = fill_shared(&budget);
        subscription.offer(&heavy(3, 200), 0);
        assert!(matches!(subscription.next().await, Next::Overflow));
        assert_eq!(admitted(&budget), (0, 1, 0), "refused over its floor");
        drop((first, second, hog));
        subscription.close();
        assert_eq!(subscription.outstanding(), (0, 0));
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn sr1_batched_outbound_records_count_one_entry_each() {
        let weight = heavy(1, 200).weight();
        let (budget, subscription) = isolated(8 * weight, 4 * weight);
        for offset in 1..=3 {
            subscription.offer(&heavy(offset, 200), 0);
        }
        // A batch merges its records' charges into one (Batch::push).
        let mut batch = take(&subscription).await;
        let (_, _, second) = subscription.try_next().expect("queued");
        batch.absorb(second);
        assert_eq!(batch.records(), 2);
        assert_eq!(subscription.outstanding(), (2, 2 * weight));
        // One record still queued plus two in the batch: three entries.
        let hog = fill_shared(&budget);
        subscription.offer(&heavy(4, 200), 0);
        assert!(matches!(subscription.next().await, Next::Overflow));
        assert_eq!(admitted(&budget), (0, 1, 0));
        drop(batch);
        assert_eq!(subscription.outstanding(), (0, 0));
        drop(hog);
        subscription.close();
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn sr1_cancellation_releases_exactly_what_was_outstanding() {
        let weight = heavy(1, 200).weight();
        let (budget, subscription) = isolated(8 * weight, 4 * weight);
        for offset in 1..=3 {
            subscription.offer(&heavy(offset, 200), 0);
        }
        // Two records in the bounded handoff, one queued; the client goes.
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        sender.send(take(&subscription).await).await.unwrap();
        sender.send(take(&subscription).await).await.unwrap();
        assert_eq!(subscription.outstanding(), (2, 2 * weight));
        drop((sender, receiver));
        assert_eq!(subscription.outstanding(), (0, 0));
        assert_eq!(budget.used(), weight, "only the queued entry remains");
        subscription.close();
        assert_eq!(budget.used(), 0);
        assert_eq!(subscription.outstanding(), (0, 0));
    }

    #[tokio::test]
    async fn sr1_one_oversized_outbound_record_is_still_within_the_floor() {
        // The policy's exception: at most one other record, of any size.
        let weight = heavy(1, 600).weight();
        assert!(weight > 512 * 1024);
        let (budget, subscription) = isolated(4 * weight, 2 * weight);
        subscription.offer(&heavy(1, 600), 0);
        let outbound = take(&subscription).await;
        assert_eq!(subscription.outstanding(), (1, weight));
        let hog = fill_shared(&budget);
        subscription.offer(&heavy(2, 600), 0);
        assert_eq!(subscription.queued().0, 1, "admitted from the reserve");
        assert_eq!(admitted(&budget), (1, 0, 0));
        drop((outbound, hog));
        subscription.close();
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn sr1_a_top_up_excludes_exactly_its_own_record_and_never_double_counts() {
        let budget = ByteBudget::with_reserve(8 << 20, 1 << 20, 256 * 1024);
        let subscription = Subscription::new(
            12,
            "c".into(),
            requirement(query::FeedType::Trade),
            100,
            usize::MAX,
            budget.clone(),
            0,
        );
        // Shared-charged records: the queue entry is QUEUE_ENTRY_OVERHEAD,
        // the transport copy is topped up to the full weight.
        let shared = |offset| {
            let mut record = Arc::try_unwrap(heavy(offset, 200)).unwrap();
            record.shared_charge = budget.charge(record.weight());
            Arc::new(record)
        };
        let records: Vec<_> = (1..=4).map(shared).collect();
        assert!(records.iter().all(|record| record.shared_charge.is_some()));
        let weight = records[0].weight();
        for record in &records[..3] {
            subscription.offer(record, 0);
        }
        let mut first = take(&subscription).await;
        assert_eq!(subscription.outstanding(), (1, QUEUE_ENTRY_OVERHEAD));
        let hog = fill_shared(&budget);
        // Its own entry is excluded; two queued records are its load, so
        // the top-up of the FIRST record (count 2, bytes over the floor)
        // stays out of the reserve.
        assert_eq!(
            subscription
                .charge_handoff(weight - QUEUE_ENTRY_OVERHEAD, weight, &first)
                .err(),
            Some(false)
        );
        let second = take(&subscription).await;
        let third = take(&subscription).await;
        drop((second, third));
        let hog2 = fill_shared(&budget);
        // Now nothing else is held: it may use the reserve.
        let extra = subscription
            .charge_handoff(weight - QUEUE_ENTRY_OVERHEAD, weight, &first)
            .expect("at its floor");
        assert_eq!(subscription.outstanding(), (1, weight), "entry + top-up");
        first.absorb(extra);
        assert_eq!(first.records(), 1);
        assert_eq!(subscription.outstanding(), (1, weight), "absorb moves, never adds");
        // A second record's top-up sees exactly one other record.
        subscription.offer(&records[3], 0);
        let fourth = take(&subscription).await;
        assert_eq!(subscription.outstanding(), (2, weight + QUEUE_ENTRY_OVERHEAD));
        assert!(subscription
            .charge_handoff(weight - QUEUE_ENTRY_OVERHEAD, weight, &fourth)
            .is_ok());
        // The first top-up, the fourth entry and the fourth top-up.
        assert_eq!(budget.counters.reserve_admitted.load(Ordering::Relaxed), 3);
        drop((first, fourth, hog, hog2, records));
        subscription.close();
        assert_eq!(subscription.outstanding(), (0, 0));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn handoff_refusals_and_timeouts_are_counted_by_floor_state() {
        let subscription = queue(query::FeedType::Trade, 2);
        subscription.note_handoff_refused(true);
        subscription.note_handoff_refused(false);
        subscription.note_handoff_timeout(false);
        let budget = &subscription.budget.counters;
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        assert_eq!(
            (
                load(&budget.handoff_refused_within_floor),
                load(&budget.handoff_refused_over_floor),
                load(&budget.handoff_timeout_within_floor),
                load(&budget.handoff_timeout_over_floor),
            ),
            (1, 1, 0, 1)
        );
        let own = &subscription.counters;
        assert_eq!((load(&own.handoff_refused), load(&own.handoff_timeouts)), (2, 1));
        assert_eq!(load(&budget.reserve_exhausted) + load(&budget.over_floor_refused), 0);
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
