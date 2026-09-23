//! Requirement handling shared by Subscribe: the proto mapping of
//! `requirement_from_proto`, the entitlement checks of `DataPlaneAccess` and
//! the delivery predicate of `GrpcMarketDataService._matches_requirement`.

use crate::auth::AccessError;
use crate::bundle::Manifest;
use crate::generated::marketdata_v2::{event_envelope, EventEnvelope};
use crate::generated::query_v2 as query;
use qdl_contracts::cursor_v3::DeliveryRequirement;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamRequirement {
    pub delivery: DeliveryRequirement,
    pub warmup_rows: u64,
    pub time_range: bool,
}

impl StreamRequirement {
    pub fn from_proto(value: &query::DataRequirement) -> Result<Self, String> {
        let delivery = DeliveryRequirement::from_proto(value)?;
        let (warmup_rows, time_range) = match value.warmup.as_ref().and_then(|w| w.horizon.as_ref())
        {
            Some(query::warmup_specification::Horizon::Rows(rows)) => (u64::from(*rows), false),
            Some(query::warmup_specification::Horizon::TimeRange(_)) => (0, true),
            None => (u64::from(value.warmup_limit), false),
        };
        if value.warmup.is_some()
            && value
                .warmup
                .as_ref()
                .and_then(|w| w.horizon.as_ref())
                .is_none()
        {
            return Err("warmup horizon is required".into());
        }
        Ok(Self {
            delivery,
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
    if now_ns - observed_ns <= (max_freshness_ms as i64) * 1_000_000 {
        Delivery::Deliver
    } else {
        Delivery::TooOld
    }
}
