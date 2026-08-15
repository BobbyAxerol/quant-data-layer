use std::collections::{BTreeMap, BTreeSet};

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
        let state = self.partitions.entry(partition_key.into()).or_default();
        if generation < state.generation {
            return SequenceDecision::StaleSession;
        }
        if generation > state.generation || state.session_id != session_id {
            state.session_id = session_id.into();
            state.generation = generation;
            state.last_sequence = Some(sequence);
            state.recent_event_ids.clear();
            state.recent_event_ids.insert(event_id);
            return SequenceDecision::SessionStarted;
        }
        if state.recent_event_ids.contains(&event_id) {
            return SequenceDecision::Duplicate;
        }
        let decision = match state.last_sequence {
            Some(last) if sequence <= last => SequenceDecision::OutOfOrder,
            Some(last) if sequence > last.saturating_add(1) => SequenceDecision::Gap {
                expected: last.saturating_add(1),
                actual: sequence,
            },
            _ => SequenceDecision::Accepted,
        };
        if matches!(decision, SequenceDecision::Accepted) {
            state.last_sequence = Some(sequence);
            state.recent_event_ids.insert(event_id);
            while state.recent_event_ids.len() > self.max_recent_ids {
                if let Some(first) = state.recent_event_ids.iter().next().cloned() {
                    state.recent_event_ids.remove(&first);
                }
            }
        }
        decision
    }
}

#[cfg(test)]
mod tests {
    use super::{OrderingTracker, SequenceDecision};

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
}
