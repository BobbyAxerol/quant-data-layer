//! Stream compatibility boundary for the shared, default-off TEST data clock.

pub(crate) struct DataClock(qdl_test_data_clock::DataClock);

impl DataClock {
    pub(crate) fn from_env() -> Result<Self, &'static str> {
        qdl_test_data_clock::DataClock::from_env(cfg!(feature = "isolated-replay-clock")).map(Self)
    }

    pub(crate) fn now_ns(&self, real_ns: i64) -> Result<i64, &'static str> {
        self.0.now_ns(real_ns)
    }

    #[cfg(feature = "isolated-replay-clock")]
    pub(crate) fn is_replay(&self) -> bool {
        self.0.is_replay()
    }

    #[cfg(all(test, feature = "isolated-replay-clock"))]
    pub(crate) fn test_fixture() -> (Self, impl Fn(bool)) {
        let (clock, expire) = qdl_test_data_clock::DataClock::test_fixture();
        (Self(clock), expire)
    }

    #[cfg(all(test, feature = "isolated-replay-clock"))]
    fn test_fixture_at(source_ns: i64) -> (Self, impl Fn(bool)) {
        let (clock, expire) = qdl_test_data_clock::DataClock::test_fixture_at(source_ns);
        (Self(clock), expire)
    }
}

#[cfg(feature = "isolated-replay-clock")]
pub(crate) use qdl_test_data_clock::INVALID;

#[cfg(all(test, feature = "isolated-replay-clock"))]
mod tests {
    use super::*;
    use std::sync::Arc;

    const NS: &str = "p184-g12-dl-0123456789abcdef";
    const WALL: u64 = 10_000_000_000;

    #[test]
    fn old_record_delivers_at_historical_time_but_not_realtime() {
        use crate::generated::{
            marketdata_v2::{event_envelope, EventEnvelope, Trade},
            query_v2 as query,
        };
        use crate::hub::{LiveRecord, RawRecord};
        use crate::requirement::{delivery_decision, Delivery, StreamRequirement};
        use crate::subscription::{ByteBudget, Subscription};
        use prost::Message as _;

        let mut requirement = StreamRequirement::from_proto(&query::DataRequirement {
            instrument_uid: "u".into(),
            source_policy_id: "p".into(),
            feed_type: query::FeedType::Trade as i32,
            grade: query::ConsumerGrade::Alpha as i32,
            stale_policy_type: query::StalePolicy::Block as i32,
            gap_policy_type: query::GapPolicy::Block as i32,
            recovery_policy: query::RecoveryPolicy::SnapshotAndReplay as i32,
            revision_policy: query::BarRevisionPolicy::Latest as i32,
            ..Default::default()
        })
        .unwrap();
        requirement.delivery.max_freshness_ms = Some(1);
        let envelope = EventEnvelope {
            source_event_time_ns: 1_000_000_000,
            payload: Some(event_envelope::Payload::Trade(Trade::default())),
            ..Default::default()
        };
        let (clock, expired) = DataClock::test_fixture_at(1_000_000_000);
        let data_now = clock.now_ns(WALL as i64).unwrap();
        assert_eq!(
            delivery_decision(&requirement, &envelope, data_now),
            Delivery::Deliver
        );
        assert_eq!(
            delivery_decision(&requirement, &envelope, WALL as i64),
            Delivery::TooOld
        );
        let record = LiveRecord::decode_charged(
            Arc::new(RawRecord {
                partition: 0,
                offset: 1,
                key: b"k".to_vec(),
                payload: envelope.encode_to_vec(),
                timestamp_ms: 1000,
            }),
            None,
            WALL,
        )
        .unwrap();
        // The data clock must not rewrite envelope or dispatch/trace wall metadata.
        assert_eq!(record.dispatched_wall_ns, WALL);
        assert_eq!(record.envelope.source_event_time_ns, 1_000_000_000);
        let queue = |id| {
            Subscription::new(
                id,
                format!("{NS}.stream"),
                requirement.clone(),
                2,
                1_000_000,
                ByteBudget::new(1_000_000),
                0,
            )
        };
        let historical = queue(1);
        historical.offer(&record, data_now);
        assert_eq!(historical.queued().0, 1);
        let realtime = queue(2);
        realtime.offer(&record, WALL as i64);
        assert_eq!(realtime.queued().0, 0);
        expired(true);
        assert_eq!(clock.now_ns(WALL as i64), Err(INVALID));
    }
}
