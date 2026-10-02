//! Low-frequency ownership evidence; Kafka's default rebalance behavior is unchanged.
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct HandoffContext {
    pub stage: &'static str,
    pub client_id: String,
    assignment: AtomicU64,
}

impl HandoffContext {
    pub fn new(stage: &'static str, client_id: String) -> Self {
        Self {
            stage,
            client_id,
            assignment: AtomicU64::new(0),
        }
    }
    pub fn generation(&self) -> u64 {
        self.assignment.load(Ordering::Acquire)
    }
    fn record(&self, phase: &str, rebalance: &Rebalance<'_>) {
        let (kind, partitions) = match rebalance {
            Rebalance::Assign(p) => (
                "assign",
                p.elements()
                    .iter()
                    .map(|x| format!("{}/{}", x.topic(), x.partition()))
                    .collect::<Vec<_>>(),
            ),
            Rebalance::Revoke(p) => (
                "revoke",
                p.elements()
                    .iter()
                    .map(|x| format!("{}/{}", x.topic(), x.partition()))
                    .collect::<Vec<_>>(),
            ),
            Rebalance::Error(_) => ("error", Vec::new()),
        };
        let at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        println!(
            "{}",
            json!({"event":"projector_membership", "at_ms":at_ms,
            "stage":self.stage, "client_id":self.client_id, "phase":phase,
            "kind":kind, "partitions":partitions, "assignment_sequence":self.generation()})
        );
    }
}
impl ClientContext for HandoffContext {}
impl ConsumerContext for HandoffContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        self.record("before", rebalance);
    }
    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if matches!(rebalance, Rebalance::Assign(_)) {
            self.assignment.fetch_add(1, Ordering::AcqRel);
        }
        self.record("after", rebalance);
    }
}
