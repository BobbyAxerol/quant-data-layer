#![forbid(unsafe_code)]

use qdl_venue_core::backpressure::{
    BoundedLifecycleQueue, DeliveryClass, EnqueueDecision, QueueItem,
};
use qdl_venue_core::ordering::{OrderingTracker, SequenceDecision};
use qdl_venue_core::session::{SessionError, SessionPolicy, SessionState, VenueSession};
use qdl_venue_core::sharding::{changed_assignments, rendezvous_plan};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut session = VenueSession::new(SessionPolicy {
        heartbeat_timeout_ns: 100,
        read_timeout_ns: 100,
        reconnect_budget: 3,
    });
    session.set_desired(["btc@trade".into(), "eth@trade".into()]);
    let generation = session.connect("source-session-1".into(), 9, 10_000)?;
    let requested = session.transport_ready()?.len();
    session.subscription_ack("btc@trade", generation)?;
    session.subscription_ack("eth@trade", generation)?;
    let stale_generation_rejected =
        session.permits_frame(generation - 1, 9, 10) == Err(SessionError::StaleGeneration);
    session.heartbeat(generation, 10)?;
    session.permits_frame(generation, 9, 11)?;
    let timeout_degraded = session.tick(200) == Err(SessionError::HeartbeatExpired)
        && session.state() == SessionState::Degraded;

    let mut ordering = OrderingTracker::new(128);
    let ordering_results = [
        ordering.observe("btc", "s1", 1, 10, vec![1]),
        ordering.observe("btc", "s1", 1, 11, vec![2]),
        ordering.observe("btc", "s1", 1, 11, vec![2]),
        ordering.observe("btc", "s1", 1, 13, vec![3]),
        ordering.observe("btc", "s2", 2, 1, vec![4]),
        ordering.observe("btc", "s1", 1, 14, vec![5]),
    ];
    let ordering_pass = ordering_results
        == [
            SequenceDecision::SessionStarted,
            SequenceDecision::Accepted,
            SequenceDecision::Duplicate,
            SequenceDecision::Gap {
                expected: 12,
                actual: 13,
            },
            SequenceDecision::SessionStarted,
            SequenceDecision::StaleSession,
        ];

    let mut queue = BoundedLifecycleQueue::new(2, true)?;
    let enqueue = |key: &str, value: u8, class| QueueItem {
        key: key.into(),
        payload: vec![value],
        class,
    };
    let queue_results = [
        queue.enqueue(enqueue("bbo", 1, DeliveryClass::LatestState)),
        queue.enqueue(enqueue("bbo", 2, DeliveryClass::LatestState)),
        queue.enqueue(enqueue("trade", 3, DeliveryClass::Lossless)),
        queue.enqueue(enqueue("trade", 4, DeliveryClass::Lossless)),
    ];
    queue.set_spool_full(true);
    let spool_full_disconnect =
        queue.enqueue(enqueue("trade", 5, DeliveryClass::Lossless)) == EnqueueDecision::Disconnect;

    let instruments = (0..10_000)
        .map(|index| format!("i-{index}"))
        .collect::<Vec<_>>();
    let two = vec!["owner-a".into(), "owner-b".into()];
    let three = vec!["owner-a".into(), "owner-b".into(), "owner-c".into()];
    let plan_two = rendezvous_plan(1, &instruments, &two)?;
    let plan_three = rendezvous_plan(2, &instruments, &three)?;
    let changed = changed_assignments(&plan_two, &plan_three);
    let churn_ratio = changed as f64 / instruments.len() as f64;

    let status = stale_generation_rejected
        && timeout_degraded
        && ordering_pass
        && queue_results
            == [
                EnqueueDecision::Enqueued,
                EnqueueDecision::Coalesced,
                EnqueueDecision::Enqueued,
                EnqueueDecision::SpoolRequired,
            ]
        && spool_full_disconnect
        && (0.25..0.40).contains(&churn_ratio);
    println!(
        "{}",
        serde_json::to_string(&json!({
            "status": if status { "PASS" } else { "FAIL" },
            "session": {
                "subscription_count": requested,
                "stale_generation_rejected": stale_generation_rejected,
                "timeout_degraded": timeout_degraded,
            },
            "ordering": {
                "duplicate_gap_session_reset_pass": ordering_pass,
            },
            "backpressure": {
                "latest_state_coalesced": queue_results[1] == EnqueueDecision::Coalesced,
                "lossless_spooled": queue_results[3] == EnqueueDecision::SpoolRequired,
                "spool_full_disconnect": spool_full_disconnect,
            },
            "sharding": {
                "algorithm": plan_two.algorithm,
                "instruments": instruments.len(),
                "owners_before": two.len(),
                "owners_after": three.len(),
                "changed": changed,
                "churn_ratio": churn_ratio,
            },
        }))?
    );
    if status {
        Ok(())
    } else {
        Err("venue-core certification failed".into())
    }
}
