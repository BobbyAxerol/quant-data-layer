//! Requirement handling shared by Subscribe: the proto mapping of
//! `requirement_from_proto`, the entitlement checks of `DataPlaneAccess` and
//! the delivery predicate of `GrpcMarketDataService._matches_requirement`.

use crate::auth::AccessError;
use crate::bundle::Manifest;
use crate::generated::marketdata_v2::{event_envelope, EventEnvelope};
use crate::generated::query_v2 as query;
use qdl_contracts::cursor_v3::DeliveryRequirement;
use qdl_contracts::requirement::{ValidatedRequirement, WarmupHorizon};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamRequirement {
    pub delivery: DeliveryRequirement,
    pub warmup_rows: u64,
    pub time_range: bool,
}

impl StreamRequirement {
    /// Validated exactly as the Python server validates
    /// (`qdl_contracts::requirement`); any refusal is INVALID_ARGUMENT.
    pub fn from_proto(value: &query::DataRequirement) -> Result<Self, String> {
        let validated = ValidatedRequirement::from_proto(value).map_err(|error| error.message)?;
        let (warmup_rows, time_range) = match validated.warmup.as_ref().map(|w| &w.horizon) {
            Some(WarmupHorizon::Rows(rows)) => (u64::from(*rows), false),
            Some(WarmupHorizon::TimeRange { .. }) => (0, true),
            None => (u64::from(validated.warmup_limit), false),
        };
        Ok(Self {
            delivery: validated.delivery,
            warmup_rows,
            time_range,
        })
    }

    /// `effective_event_recency_policy`: the declared recency policy, else the
    /// stale policy.
    pub fn effective_event_recency_policy(&self) -> &str {
        self.delivery
            .event_recency_policy
            .as_deref()
            .unwrap_or(&self.delivery.stale_policy)
    }

    /// Payload oneof name for this feed, as `envelope.WhichOneof("payload")`.
    pub fn payload_name(&self) -> String {
        self.delivery.feed.to_ascii_lowercase()
    }
}

/// `ConsumerManifest.requirement_allowed` + the warmup-row quota of
/// `DataPlaneAccess.require_requirement`. Every failure is a
/// `DataPlaneAccessError`, which Subscribe answers with PERMISSION_DENIED.
pub fn require_requirement(
    manifest: &Manifest,
    requirement: &StreamRequirement,
) -> Result<(), AccessError> {
    let wanted = &requirement.delivery;
    let allowed = manifest.requirements.iter().any(|configured| {
        configured.instrument_uid == wanted.instrument_uid
            && configured.feed == wanted.feed
            && configured.interval == wanted.interval
            && configured.consumer_grade == wanted.consumer_grade
            && configured.source_policy_id == wanted.source_policy_id
            && configured.event_recency_policy == wanted.event_recency_policy
            && configured.max_session_liveness_ms == wanted.max_session_liveness_ms
    });
    if !allowed {
        return Err(AccessError::PermissionDenied(
            "data requirement is outside the registered consumer manifest".into(),
        ));
    }
    if requirement.time_range {
        // Time-range warmups are sized from interval arithmetic that KN-2
        // ports with the full service; the slice refuses them explicitly.
        return Err(AccessError::PermissionDenied(
            "time-range warmup is not served by the KN-1 prototype".into(),
        ));
    }
    if requirement.warmup_rows > manifest.quotas.max_warmup_rows {
        return Err(AccessError::PermissionDenied(
            "warmup limit exceeds the registered consumer quota".into(),
        ));
    }
    Ok(())
}

/// Interval carried by the canonical payload (`canonical_payload_interval`):
/// only BAR carries one among the feeds this slice serves.
fn payload_interval(envelope: &EventEnvelope) -> Option<String> {
    match &envelope.payload {
        Some(event_envelope::Payload::Bar(bar)) if !bar.interval.is_empty() => {
            Some(bar.interval.clone())
        }
        _ => None,
    }
}

fn payload_name(envelope: &EventEnvelope) -> Option<&'static str> {
    Some(match envelope.payload.as_ref()? {
        event_envelope::Payload::Trade(_) => "trade",
        event_envelope::Payload::Bar(_) => "bar",
        event_envelope::Payload::Quote(_) => "quote",
        event_envelope::Payload::BookSnapshot(_) => "book_snapshot",
        event_envelope::Payload::BookDelta(_) => "book_delta",
        event_envelope::Payload::FundingRate(_) => "funding_rate",
        event_envelope::Payload::OpenInterest(_) => "open_interest",
        event_envelope::Payload::MarkIndexPrice(_) => "mark_index_price",
        event_envelope::Payload::Ticker(_) => "ticker",
        event_envelope::Payload::FeedState(_) => "feed_state",
        event_envelope::Payload::QualityEvent(_) => "quality_event",
        event_envelope::Payload::LongShortRatio(_) => "long_short_ratio",
        event_envelope::Payload::TakerFlow(_) => "taker_flow",
        event_envelope::Payload::Basis(_) => "basis",
        event_envelope::Payload::ContractMetadata(_) => "contract_metadata",
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    Deliver,
    OtherProduct,
    TooOld,
}

/// `_matches_requirement`: exact product identity, then strict freshness when
/// a bound is declared and the effective recency policy is BLOCK or PAUSE.
pub fn delivery_decision(
    requirement: &StreamRequirement,
    envelope: &EventEnvelope,
    now_ns: i64,
) -> Delivery {
    if payload_name(envelope) != Some(requirement.payload_name().as_str())
        || payload_interval(envelope) != requirement.delivery.interval
    {
        return Delivery::OtherProduct;
    }
    let Some(max_freshness_ms) = requirement.delivery.max_freshness_ms else {
        return Delivery::Deliver;
    };
    if !matches!(
        requirement.effective_event_recency_policy(),
        "BLOCK" | "PAUSE"
    ) {
        return Delivery::Deliver;
    }
    let observed_ns = match &envelope.payload {
        Some(event_envelope::Payload::Bar(bar)) if requirement.delivery.feed == "BAR" => {
            bar.close_time_ns
        }
        _ => envelope.source_event_time_ns,
    };
    // Python integers do not overflow: compare in i128, where neither a huge
    // client bound nor an extreme event time can wrap.
    if i128::from(now_ns) - i128::from(observed_ns) <= i128::from(max_freshness_ms) * 1_000_000 {
        Delivery::Deliver
    } else {
        Delivery::TooOld
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::marketdata_v2::Trade;

    fn trade_requirement() -> query::DataRequirement {
        query::DataRequirement {
            instrument_uid: "fb26214c-7b9b-5961-95b2-55154755af0f".into(),
            source_policy_id: "crypto_primary_v2".into(),
            max_freshness_ms: 15_000,
            require_full_coverage: true,
            require_final_bars: true,
            feed_type: query::FeedType::Trade as i32,
            grade: query::ConsumerGrade::Alpha as i32,
            stale_policy_type: query::StalePolicy::Block as i32,
            gap_policy_type: query::GapPolicy::Block as i32,
            recovery_policy: query::RecoveryPolicy::SnapshotAndReplay as i32,
            revision_policy: query::BarRevisionPolicy::Latest as i32,
            ..Default::default()
        }
    }

    #[test]
    fn subscribe_refuses_what_the_python_server_refuses() {
        assert!(StreamRequirement::from_proto(&trade_requirement()).is_ok());
        let partial_execution = query::DataRequirement {
            grade: query::ConsumerGrade::Execution as i32,
            require_full_coverage: false,
            ..trade_requirement()
        };
        assert_eq!(
            StreamRequirement::from_proto(&partial_execution),
            Err("execution-grade requirements need full coverage".into())
        );
        let python_blank_uid = query::DataRequirement {
            instrument_uid: "\u{1f}".into(),
            ..trade_requirement()
        };
        assert_eq!(
            StreamRequirement::from_proto(&python_blank_uid),
            Err("instrument_uid is required".into())
        );
        let trade_with_interval = query::DataRequirement {
            interval: "1m".into(),
            ..trade_requirement()
        };
        assert!(StreamRequirement::from_proto(&trade_with_interval).is_err());
    }

    #[test]
    fn a_huge_freshness_bound_never_wraps_into_too_old() {
        let requirement = StreamRequirement::from_proto(&query::DataRequirement {
            max_freshness_ms: u64::MAX,
            ..trade_requirement()
        })
        .expect("valid");
        let envelope = EventEnvelope {
            source_event_time_ns: i64::MIN,
            payload: Some(event_envelope::Payload::Trade(Trade::default())),
            ..Default::default()
        };
        assert_eq!(
            delivery_decision(&requirement, &envelope, i64::MAX),
            Delivery::Deliver
        );
        let bounded = StreamRequirement::from_proto(&trade_requirement()).expect("valid");
        assert_eq!(
            delivery_decision(&bounded, &envelope, i64::MAX),
            Delivery::TooOld
        );
    }
}
