//! Stream delivery policy (KN-2 K2.4, invariant 27), the native side of
//! `qdl.ingestion.contracts.delivery_policy`.
//!
//! A record is either lossless, latest-state, or a lifecycle-coalescible BAR
//! update. Only a coalescible record may ever be dropped, and only when a
//! later record with the same lifecycle key and the same lifecycle signature
//! supersedes it: a quality-state or source-authority transition changes the
//! signature and is therefore never coalesced away. The old Python stream
//! coalesced BOOK_SNAPSHOT; invariant 27 makes every book record lossless, so
//! that is not ported. `contracts/golden/kn_v220/delivery_policy.json`
//! (produced from the Python domain function) is the shared oracle.

use crate::qdl::marketdata::v2::{event_envelope, BarLifecycle, EventEnvelope};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryPolicy {
    Lossless,
    LatestState,
    LifecycleCoalesce,
}

impl DeliveryPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            DeliveryPolicy::Lossless => "LOSSLESS",
            DeliveryPolicy::LatestState => "LATEST_STATE",
            DeliveryPolicy::LifecycleCoalesce => "LIFECYCLE_COALESCE",
        }
    }
}

/// Domain policy for a public feed name. `Err` is the domain's refusal of a
/// BAR without an explicit lifecycle.
pub fn delivery_policy(
    feed: &str,
    bar_lifecycle: Option<&str>,
) -> Result<DeliveryPolicy, &'static str> {
    match feed {
        "TRADE" | "BOOK_SNAPSHOT" | "BOOK_DELTA" => Ok(DeliveryPolicy::Lossless),
        "BAR" => match bar_lifecycle {
            None | Some("UNSPECIFIED") => Err("bar delivery requires an explicit lifecycle"),
            Some("IN_PROGRESS") => Ok(DeliveryPolicy::LifecycleCoalesce),
            Some(_) => Ok(DeliveryPolicy::Lossless),
        },
        _ => Ok(DeliveryPolicy::LatestState),
    }
}

/// What must stay equal for one record to supersede another: quality state
/// and source authority/session. A change is a transition and is lossless.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleSignature {
    pub quality_flags: Vec<i32>,
    pub authority_revision: u64,
    pub source_id: String,
    pub source_role: i32,
    pub provider: String,
    pub source_session_id: String,
    pub connection_generation: u64,
    pub lease_epoch: u64,
}

/// The delivery facts of one canonical record for a subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordLifecycle {
    pub policy: DeliveryPolicy,
    /// Records supersede only within one key: the BAR open time, or the
    /// product itself for latest-state feeds.
    pub coalesce_key: Option<i64>,
    pub signature: LifecycleSignature,
}

impl RecordLifecycle {
    pub fn of(feed: &str, envelope: &EventEnvelope) -> Self {
        let bar = match &envelope.payload {
            Some(event_envelope::Payload::Bar(bar)) => Some(bar),
            _ => None,
        };
        let lifecycle = bar.map(|bar| {
            BarLifecycle::try_from(bar.lifecycle)
                .map(|value| value.as_str_name().trim_start_matches("BAR_LIFECYCLE_"))
                .unwrap_or("UNSPECIFIED")
        });
        // Fail safe: a record the domain would refuse is never coalesced.
        let policy = delivery_policy(feed, lifecycle).unwrap_or(DeliveryPolicy::Lossless);
        let mut quality_flags = envelope.quality_flags.clone();
        quality_flags.sort_unstable();
        Self {
            policy,
            coalesce_key: bar.map(|bar| bar.open_time_ns),
            signature: LifecycleSignature {
                quality_flags,
                authority_revision: envelope.authority_revision,
                source_id: envelope.source_id.clone(),
                source_role: envelope.source_role,
                provider: envelope.provider.clone(),
                source_session_id: envelope.source_session_id.clone(),
                connection_generation: envelope.connection_generation,
                lease_epoch: envelope.lease_epoch,
            },
        }
    }

    pub fn coalescible(&self) -> bool {
        self.policy != DeliveryPolicy::Lossless
    }

    /// Whether `later` may replace `self` without losing a lossless fact:
    /// both coalescible, same key and the same lifecycle signature.
    pub fn superseded_by(&self, later: &RecordLifecycle) -> bool {
        self.coalescible()
            && self.coalesce_key == later.coalesce_key
            && self.signature == later.signature
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qdl::marketdata::v2::{Bar, Quote};
    use serde_json::Value;

    fn golden() -> Value {
        serde_json::from_str(include_str!(
            "../../../contracts/golden/kn_v220/delivery_policy.json"
        ))
        .expect("delivery policy golden")
    }

    #[test]
    fn every_public_feed_matches_the_domain_policy() {
        let doc = golden();
        let cases = doc["cases"].as_array().expect("cases");
        assert!(cases.len() >= 17);
        for case in cases {
            let feed = case["feed"].as_str().expect("feed");
            let lifecycle = case["bar_lifecycle"].as_str();
            let expected = case["policy"].as_str().expect("policy");
            let got = delivery_policy(feed, lifecycle)
                .map(DeliveryPolicy::as_str)
                .unwrap_or("ERROR");
            assert_eq!(got, expected, "{feed} {lifecycle:?}");
        }
    }

    fn quote(flags: Vec<i32>, authority: u64) -> EventEnvelope {
        EventEnvelope {
            quality_flags: flags,
            authority_revision: authority,
            payload: Some(event_envelope::Payload::Quote(Quote::default())),
            ..Default::default()
        }
    }

    fn bar(open_ms: i64, lifecycle: BarLifecycle) -> EventEnvelope {
        EventEnvelope {
            payload: Some(event_envelope::Payload::Bar(Bar {
                open_time_ns: open_ms * 1_000_000,
                lifecycle: lifecycle as i32,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    fn a_quote_supersedes_a_quote_only_without_a_transition() {
        let first = RecordLifecycle::of("QUOTE", &quote(vec![1], 1));
        assert!(first.superseded_by(&RecordLifecycle::of("QUOTE", &quote(vec![1], 1))));
        // Quality-state transition: lossless (invariant 27).
        assert!(!first.superseded_by(&RecordLifecycle::of("QUOTE", &quote(vec![1, 9], 1))));
        // Source-authority transition: lossless.
        assert!(!first.superseded_by(&RecordLifecycle::of("QUOTE", &quote(vec![1], 2))));
        // Flag order is not a transition.
        let unordered = RecordLifecycle::of("QUOTE", &quote(vec![9, 1], 1));
        assert!(RecordLifecycle::of("QUOTE", &quote(vec![1, 9], 1)).superseded_by(&unordered));
    }

    #[test]
    fn bars_coalesce_only_in_progress_updates_of_one_open_time() {
        let progress = RecordLifecycle::of("BAR", &bar(60_000, BarLifecycle::InProgress));
        assert!(progress.superseded_by(&RecordLifecycle::of(
            "BAR",
            &bar(60_000, BarLifecycle::InProgress)
        )));
        assert!(progress.superseded_by(&RecordLifecycle::of(
            "BAR",
            &bar(60_000, BarLifecycle::Final)
        )));
        // Another open time never supersedes.
        assert!(!progress.superseded_by(&RecordLifecycle::of(
            "BAR",
            &bar(120_000, BarLifecycle::InProgress)
        )));
        // A final, revised or cancelled bar is never dropped.
        for lifecycle in [
            BarLifecycle::Final,
            BarLifecycle::Revised,
            BarLifecycle::Cancelled,
            BarLifecycle::Unspecified,
        ] {
            let record = RecordLifecycle::of("BAR", &bar(60_000, lifecycle));
            assert!(!record.coalescible(), "{lifecycle:?}");
            assert!(!record.superseded_by(&RecordLifecycle::of(
                "BAR",
                &bar(60_000, BarLifecycle::Final)
            )));
        }
    }

    #[test]
    fn book_and_trade_records_are_never_coalescible() {
        for feed in ["TRADE", "BOOK_SNAPSHOT", "BOOK_DELTA"] {
            let record = RecordLifecycle::of(feed, &EventEnvelope::default());
            assert!(!record.coalescible(), "{feed}");
        }
        assert!(RecordLifecycle::of("MARK_INDEX_PRICE", &EventEnvelope::default()).coalescible());
    }
}
