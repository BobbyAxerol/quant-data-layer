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
fn session_lane(session_id: &str) -> Option<&str> {
    let mut segments = session_id.rsplitn(3, '-');
    let nanos = segments.next()?;
    let generation = segments.next()?;
    let lane = segments.next()?;
    let numeric = |value: &str| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit());
    (!lane.is_empty() && numeric(generation) && numeric(nanos)).then_some(lane)
}

/// Whether two sessions' generation counters are comparable at all.
///
/// Fencing a lower generation is how a superseded connection's late frames are
/// rejected, and every reconnect of one lane does produce a higher number. Two
/// *different* lanes carry unrelated counters: OKX's business bar lane was on
/// generation 23 while its public book lane was on 53,986, and comparing those
/// as bare integers quarantined every closed candle for hours on 2026-09-17.
/// Only a positively identified different lane lifts the fence; anything this
/// cannot parse keeps the original comparison.
fn same_session_lane(session_id: &str, stage_session_id: &str) -> bool {
    match (session_lane(session_id), session_lane(stage_session_id)) {
        (Some(lane), Some(stage_lane)) => lane == stage_lane,
        _ => true,
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
    fn an_unparsable_session_identity_keeps_the_original_fence() {
        assert_eq!(super::session_lane("session-7"), None);
        assert_eq!(super::session_lane(""), None);
        assert_eq!(super::session_lane("okx-business-001-x-123"), None);
        // Neither side is identifiable, so the bare generation comparison stands.
        assert!(super::same_session_lane("session-7", "session-9"));
        assert!(super::same_session_lane("", ""));
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
            tracker.observe("btc", "s1", 1, 10, vec![1]),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            tracker.observe("btc", "s1", 1, 11, vec![2]),
            SequenceDecision::Accepted
        );
        assert_eq!(
            tracker.observe("btc", "s1", 1, 11, vec![2]),
            SequenceDecision::Duplicate
        );
        assert_eq!(
            tracker.observe("btc", "s1", 1, 13, vec![3]),
            SequenceDecision::Gap {
                expected: 12,
                actual: 13
            }
        );
        assert_eq!(
            tracker.observe("btc", "s1", 1, 9, vec![4]),
            SequenceDecision::OutOfOrder
        );
        assert_eq!(
            tracker.observe("btc", "s2", 2, 1, vec![5]),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            tracker.observe("btc", "s1", 1, 14, vec![6]),
            SequenceDecision::StaleSession
        );
    }
    #[test]
    fn monotonic_allows_native_leaps_but_contiguous_detects_them() {
        let mut monotonic = OrderingTracker::new(8);
        assert_eq!(
            monotonic
                .observe_with_policy("trade", "s1", 1, 10, vec![1], SequencePolicy::Monotonic,),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            monotonic
                .observe_with_policy("trade", "s1", 1, 15, vec![2], SequencePolicy::Monotonic,),
            SequenceDecision::Accepted
        );
        let mut none = OrderingTracker::new(8);
        none.observe_with_policy("bar", "s1", 1, 60, vec![1], SequencePolicy::None);
        assert_eq!(
            none.observe_with_policy("bar", "s1", 1, 1, vec![2], SequencePolicy::None),
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
                "s1",
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
                "s1",
                1,
                11,
                vec![2],
                SequencePolicy::Contiguous,
            ),
            SequenceDecision::Accepted
        );

        let mut retry = tracker.stage("btc");
        assert_eq!(
            tracker.observe_staged(&mut retry, "s1", 1, 10, vec![1], SequencePolicy::Contiguous,),
            SequenceDecision::SessionStarted
        );
    }

    #[test]
    fn committed_stage_preserves_batch_duplicate_and_gap_semantics() {
        let mut tracker = OrderingTracker::new(8);
        let mut stage = tracker.stage("btc");
        assert_eq!(
            tracker.observe_staged(&mut stage, "s1", 1, 10, vec![1], SequencePolicy::Contiguous,),
            SequenceDecision::SessionStarted
        );
        assert_eq!(
            tracker.observe_staged(&mut stage, "s1", 1, 11, vec![2], SequencePolicy::Contiguous,),
            SequenceDecision::Accepted
        );
        assert_eq!(
            tracker.observe_staged(&mut stage, "s1", 1, 11, vec![2], SequencePolicy::Contiguous,),
            SequenceDecision::Duplicate
        );
        tracker.commit_stage(stage);

        assert_eq!(
            tracker.observe("btc", "s1", 1, 11, vec![2]),
            SequenceDecision::Duplicate
        );
        assert_eq!(
            tracker.observe("btc", "s1", 1, 13, vec![3]),
            SequenceDecision::Gap {
                expected: 12,
                actual: 13,
            }
        );
        assert_eq!(
            tracker.observe("btc", "s1", 1, 12, vec![4]),
            SequenceDecision::Accepted
        );
    }
}
