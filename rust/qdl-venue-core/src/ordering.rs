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
        if generation < stage.generation {
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
