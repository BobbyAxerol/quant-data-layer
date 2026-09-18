use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SequencePolicy {
    None,
    Monotonic,
    Contiguous,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SequenceDecision {
    Accepted,
    SessionStarted,
    Duplicate,
    OutOfOrder,
    Gap { expected: u64, actual: u64 },
    StaleSession,
}

#[derive(Clone, Debug, Default)]
struct PartitionSequence {
    session_id: String,
    generation: u64,
    last_sequence: Option<u64>,
    recent_event_ids: BTreeSet<Vec<u8>>,
}

#[derive(Clone, Debug, Default)]
pub struct OrderingTracker {
    partitions: BTreeMap<String, PartitionSequence>,
    max_recent_ids: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderingStage {
    partition_key: String,
    session_id: String,
    generation: u64,
    last_sequence: Option<u64>,
    pending_event_ids: BTreeSet<Vec<u8>>,
    reset_recent: bool,
}

/// The connection lane a provider session id belongs to, when it declares one.
///
/// A session id is `<runtime>-<shard>-<generation>-<nanos>`, so the lane is
/// everything before the generation. Connection generations are counted per
/// lane - the ingestor opens one socket per feed class and each keeps its own
/// counter - so a number from one lane says nothing about another. Returns
/// `None` for an id that does not carry the two trailing numeric segments,
/// which the caller must treat as "cannot prove a different lane".
pub fn session_lane(session_id: &str) -> Option<&str> {
    let mut segments = session_id.rsplitn(3, '-');
    let nanos = segments.next()?;
    let generation = segments.next()?;
    let lane = segments.next()?;
    let numeric = |value: &str| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit());
    (!lane.is_empty() && numeric(generation) && numeric(nanos)).then_some(lane)
}

/// Whether two sessions' generation counters are comparable at all.
///
/// Public because there is more than one generation fence in this workspace.
/// The L2 book core keeps its own (`qdl_core::l2_book::L2BookCore`), book
/// frames never reach `OrderingTracker`, and on 2026-09-18 that second fence
/// silently dropped every Binance book frame when a routed lane restarted its
/// counter at 1 against a remembered 96. One rule, one implementation: a
/// caller that needs to know whether two generations are comparable calls
/// this, and never writes a second parser.
///
/// Fencing a lower generation is how a superseded connection's late frames are
/// rejected, and every reconnect of one lane does produce a higher number. That
/// only means anything inside one lane. Two examples from 2026-09-17, both of
/// which quarantined every closed OKX candle for hours: the business bar lane
/// sat on generation 23 while the public book lane was on 53,986, and the bar
/// edge writes REST repair rows under `qdl-v2-stable-okx-rest-r1-g<nanos>` with
/// a *nanosecond timestamp* where a connection counter belongs - 1.79e18 against
/// the ingestor's 25.
///
/// So an identity this cannot parse is not a lane that might match: it is a
/// different producer whose numbering means something else entirely, and
/// comparing the two is meaningless rather than conservative. Only two
/// positively identified identities in the same lane can fence each other, and
/// a superseded connection always carries one, because the ingestor is what
/// writes them.
pub fn same_session_lane(session_id: &str, stage_session_id: &str) -> bool {
    match (session_lane(session_id), session_lane(stage_session_id)) {
        (Some(lane), Some(stage_lane)) => lane == stage_lane,
        _ => false,
    }
}

impl OrderingTracker {
    pub fn new(max_recent_ids: usize) -> Self {
        Self {
            partitions: BTreeMap::new(),
            max_recent_ids: max_recent_ids.max(1),
        }
    }

    pub fn observe(
        &mut self,
        partition_key: &str,
        session_id: &str,
        generation: u64,
        sequence: u64,
        event_id: Vec<u8>,
    ) -> SequenceDecision {
        self.observe_with_policy(
            partition_key,
            session_id,
            generation,
            sequence,
            event_id,
            SequencePolicy::Contiguous,
        )
    }

    pub fn observe_with_policy(
        &mut self,
        partition_key: &str,
        session_id: &str,
        generation: u64,
        sequence: u64,
        event_id: Vec<u8>,
        policy: SequencePolicy,
    ) -> SequenceDecision {
        let mut stage = self.stage(partition_key);
        let decision = self.observe_staged(
            &mut stage, session_id, generation, sequence, event_id, policy,
        );
        if matches!(
            decision,
            SequenceDecision::Accepted | SequenceDecision::SessionStarted
        ) {
            self.commit_stage(stage);
        }
        decision
    }

    /// What the tracker currently believes about this partition.
    ///
    /// A stale-generation rejection is unreadable without the other side of the
    /// comparison: on 2026-09-17 the question "stale against what?" could not be
    /// answered from the quarantine record, the raw stream or the logs.
    pub fn observed_session(&self, partition_key: &str) -> (String, u64) {
        self.partitions
            .get(partition_key)
            .map(|state| (state.session_id.clone(), state.generation))
            .unwrap_or_default()
    }

    pub fn stage(&self, partition_key: &str) -> OrderingStage {
        let (session_id, generation, last_sequence) = self
            .partitions
            .get(partition_key)
            .map(|state| {
                (
                    state.session_id.clone(),
                    state.generation,
                    state.last_sequence,
                )
            })
            .unwrap_or_default();
        OrderingStage {
            partition_key: partition_key.into(),
            session_id,
            generation,
            last_sequence,
            pending_event_ids: BTreeSet::new(),
            reset_recent: false,
        }
    }

    pub fn observe_staged(
        &self,
        stage: &mut OrderingStage,
        session_id: &str,
        generation: u64,
        sequence: u64,
        event_id: Vec<u8>,
        policy: SequencePolicy,
    ) -> SequenceDecision {
        if same_session_lane(session_id, &stage.session_id) && generation < stage.generation {
            return SequenceDecision::StaleSession;
        }
        if generation > stage.generation || stage.session_id != session_id {
            stage.session_id = session_id.into();
            stage.generation = generation;
            stage.last_sequence = Some(sequence);
            stage.pending_event_ids.clear();
            stage.pending_event_ids.insert(event_id);
            stage.reset_recent = true;
            return SequenceDecision::SessionStarted;
        }
        let committed_duplicate = !stage.reset_recent
            && self
                .partitions
                .get(&stage.partition_key)
                .is_some_and(|state| state.recent_event_ids.contains(&event_id));
        if committed_duplicate || stage.pending_event_ids.contains(&event_id) {
            return SequenceDecision::Duplicate;
        }
        let decision = match (policy, stage.last_sequence) {
            (SequencePolicy::None, _) => SequenceDecision::Accepted,
            (_, Some(last)) if sequence <= last => SequenceDecision::OutOfOrder,
            (SequencePolicy::Contiguous, Some(last)) if sequence > last.saturating_add(1) => {
                SequenceDecision::Gap {
                    expected: last.saturating_add(1),
                    actual: sequence,
                }
            }
            _ => SequenceDecision::Accepted,
        };
        if matches!(decision, SequenceDecision::Accepted) {
            stage.last_sequence = Some(sequence);
            stage.pending_event_ids.insert(event_id);
        }
        decision
    }

    pub fn commit_stage(&mut self, stage: OrderingStage) {
        let state = self.partitions.entry(stage.partition_key).or_default();
        state.session_id = stage.session_id;
        state.generation = stage.generation;
        state.last_sequence = stage.last_sequence;
        if stage.reset_recent {
            state.recent_event_ids.clear();
        }
        state.recent_event_ids.extend(stage.pending_event_ids);
        while state.recent_event_ids.len() > self.max_recent_ids {
            if let Some(first) = state.recent_event_ids.iter().next().cloned() {
                state.recent_event_ids.remove(&first);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OrderingTracker, SequenceDecision, SequencePolicy};

    #[test]
    fn session_lane_reads_the_runtime_session_identity() {
        assert_eq!(
            super::session_lane("okx-business-001-23-1789644569811393373"),
            Some("okx-business-001")
        );
        assert_eq!(
            super::session_lane("okx-public-001-53986-1789646281539900904"),
            Some("okx-public-001")
        );
        assert_eq!(
            super::session_lane("binance-USDM-004-11-1789631081490511152"),
            Some("binance-USDM-004")
        );
    }

    #[test]
    fn an_unidentifiable_producer_cannot_fence_anything() {
        assert_eq!(super::session_lane("session-7"), None);
        assert_eq!(super::session_lane(""), None);
        assert_eq!(super::session_lane("okx-business-001-x-123"), None);
        assert_eq!(
            super::session_lane("qdl-v2-stable-okx-rest-r1-g1789653808007759187"),
            None
        );
        assert!(!super::same_session_lane("session-7", "session-9"));
        assert!(!super::same_session_lane("", ""));
    }

    #[test]
    fn a_rest_repair_writer_does_not_fence_the_live_ingestor() {
        // The bar edge publishes REST repair rows with a nanosecond timestamp
        // where a connection generation belongs. One of those on a bar key used
        // to fence every subsequent native candle - 25 against 1.79e18 - for as
        // long as the core process lived.
        let mut tracker = OrderingTracker::new(8);
        assert_eq!(
            tracker.observe_with_policy(
                "btc/okx_bar/okx-swap-btcusdt-bar-1m",
                "qdl-v2-stable-okx-rest-r1-g1789653808007759187",
                1_789_653_808_007_759_187,
                1,
                vec![1],
                SequencePolicy::None,
            ),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            tracker.observe_with_policy(
                "btc/okx_bar/okx-swap-btcusdt-bar-1m",
                "okx-business-001-25-1789651475122920508",
                25,
                2,
                vec![2],
                SequencePolicy::None,
            ),
            SequenceDecision::SessionStarted
        );
    }

    #[test]
    fn a_second_lane_is_not_stale_behind_a_higher_counter() {
        // OKX bars arrive on the business lane (generation 23) while books,
        // trades and quotes run on public lanes whose counters are in the tens
        // of thousands. The lower number is a different socket, not a
        // superseded one, and its closed candles must still publish.
        let mut tracker = OrderingTracker::new(8);
        assert_eq!(
            tracker.observe_with_policy(
                "btc/okx_bar/src",
                "okx-public-001-53986-1789646281539900904",
                53_986,
                1,
                vec![1],
                SequencePolicy::None,
            ),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            tracker.observe_with_policy(
                "btc/okx_bar/src",
                "okx-business-001-23-1789644569811393373",
                23,
                2,
                vec![2],
                SequencePolicy::None,
            ),
            SequenceDecision::SessionStarted
        );
    }

    #[test]
    fn the_same_lane_still_fences_a_superseded_generation() {
        let mut tracker = OrderingTracker::new(8);
        assert_eq!(
            tracker.observe(
                "btc/okx_bbo/src",
                "okx-public-003-82-1789645262695907911",
                82,
                10,
                vec![1],
            ),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            tracker.observe(
                "btc/okx_bbo/src",
                "okx-public-003-81-1789645262695907000",
                81,
                11,
                vec![2],
            ),
            SequenceDecision::StaleSession
        );
    }

    #[test]
    fn a_reconnect_of_the_same_lane_still_starts_a_session() {
        let mut tracker = OrderingTracker::new(8);
        tracker.observe(
            "btc/okx_trade/src",
            "okx-public-002-70-1789645429051830300",
            70,
            5,
            vec![1],
        );
        assert_eq!(
            tracker.observe(
                "btc/okx_trade/src",
                "okx-public-002-71-1789645429051830999",
                71,
                6,
                vec![2],
            ),
            SequenceDecision::SessionStarted
        );
    }

    #[test]
    fn duplicate_gap_out_of_order_and_session_reset_are_distinct() {
        let mut tracker = OrderingTracker::new(8);
        assert_eq!(
            tracker.observe(
                "btc",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                10,
                vec![1]
            ),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            tracker.observe(
                "btc",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                11,
                vec![2]
            ),
            SequenceDecision::Accepted
        );
        assert_eq!(
            tracker.observe(
                "btc",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                11,
                vec![2]
            ),
            SequenceDecision::Duplicate
        );
        assert_eq!(
            tracker.observe(
                "btc",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                13,
                vec![3]
            ),
            SequenceDecision::Gap {
                expected: 12,
                actual: 13
            }
        );
        assert_eq!(
            tracker.observe(
                "btc",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                9,
                vec![4]
            ),
            SequenceDecision::OutOfOrder
        );
        assert_eq!(
            tracker.observe(
                "btc",
                "qdl-test-lane-001-2-1700000000000000001",
                2,
                1,
                vec![5]
            ),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            tracker.observe(
                "btc",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                14,
                vec![6]
            ),
            SequenceDecision::StaleSession
        );
    }
    #[test]
    fn monotonic_allows_native_leaps_but_contiguous_detects_them() {
        let mut monotonic = OrderingTracker::new(8);
        assert_eq!(
            monotonic.observe_with_policy(
                "trade",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                10,
                vec![1],
                SequencePolicy::Monotonic,
            ),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            monotonic.observe_with_policy(
                "trade",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                15,
                vec![2],
                SequencePolicy::Monotonic,
            ),
            SequenceDecision::Accepted
        );
        let mut none = OrderingTracker::new(8);
        none.observe_with_policy(
            "bar",
            "qdl-test-lane-001-1-1700000000000000000",
            1,
            60,
            vec![1],
            SequencePolicy::None,
        );
        assert_eq!(
            none.observe_with_policy(
                "bar",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                1,
                vec![2],
                SequencePolicy::None
            ),
            SequenceDecision::Accepted
        );
    }

    #[test]
    fn discarded_stage_does_not_mutate_committed_ordering_state() {
        let tracker = OrderingTracker::new(8);
        let mut discarded = tracker.stage("btc");
        assert_eq!(
            tracker.observe_staged(
                &mut discarded,
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                10,
                vec![1],
                SequencePolicy::Contiguous,
            ),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            tracker.observe_staged(
                &mut discarded,
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                11,
                vec![2],
                SequencePolicy::Contiguous,
            ),
            SequenceDecision::Accepted
        );

        let mut retry = tracker.stage("btc");
        assert_eq!(
            tracker.observe_staged(
                &mut retry,
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                10,
                vec![1],
                SequencePolicy::Contiguous,
            ),
            SequenceDecision::SessionStarted
        );
    }

    #[test]
    fn committed_stage_preserves_batch_duplicate_and_gap_semantics() {
        let mut tracker = OrderingTracker::new(8);
        let mut stage = tracker.stage("btc");
        assert_eq!(
            tracker.observe_staged(
                &mut stage,
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                10,
                vec![1],
                SequencePolicy::Contiguous,
            ),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            tracker.observe_staged(
                &mut stage,
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                11,
                vec![2],
                SequencePolicy::Contiguous,
            ),
            SequenceDecision::Accepted
        );
        assert_eq!(
            tracker.observe_staged(
                &mut stage,
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                11,
                vec![2],
                SequencePolicy::Contiguous,
            ),
            SequenceDecision::Duplicate
        );
        tracker.commit_stage(stage);

        assert_eq!(
            tracker.observe(
                "btc",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                11,
                vec![2]
            ),
            SequenceDecision::Duplicate
        );
        assert_eq!(
            tracker.observe(
                "btc",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                13,
                vec![3]
            ),
            SequenceDecision::Gap {
                expected: 12,
                actual: 13,
            }
        );
        assert_eq!(
            tracker.observe(
                "btc",
                "qdl-test-lane-001-1-1700000000000000000",
                1,
                12,
                vec![4]
            ),
            SequenceDecision::Accepted
        );
    }
}
