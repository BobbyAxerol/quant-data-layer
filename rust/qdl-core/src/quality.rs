//! Pure binding-quality policy shared with Python through golden fixtures.
//!
//! Rust owns raw session, generation, gap, component-receipt and watermark
//! facts.  Query and audit may map those facts to their public surfaces, but
//! this evaluator pins the policy vocabulary so neither layer can silently
//! invent a second freshness rule.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ComponentEvidence {
    pub name: String,
    pub receipt_age_ms: u64,
    pub quiet_after_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BindingQualityInput {
    pub binding_id: String,
    pub instrument_uid: String,
    pub feed: String,
    pub source_role: String,
    pub authoritative: bool,
    pub acquisition_enabled: bool,
    pub acquisition_mode: String,
    pub market_open: bool,
    pub event_present: bool,
    pub event_age_ms: Option<u64>,
    pub event_limit_ms: u64,
    pub event_recency_policy: String,
    pub session_state: String,
    pub session_liveness_ms: Option<u64>,
    pub session_limit_ms: Option<u64>,
    pub components: Vec<ComponentEvidence>,
    pub generation_matches: bool,
    pub config_matches: bool,
    pub gap_open: bool,
    pub book_verified: bool,
    pub final_bar: bool,
    pub require_final_bar: bool,
    pub watermark_offset: u64,
    pub allow_quiet_execution: bool,
    pub flags: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BindingQualityDecision {
    pub binding_id: String,
    pub instrument_uid: String,
    pub feed: String,
    pub semantics: String,
    pub availability: String,
    pub state: String,
    pub event_recency_state: String,
    pub provider_session_state: String,
    pub provider_session_liveness_ms: Option<u64>,
    pub complete: bool,
    pub execution_eligible: bool,
    pub watermark_offset: u64,
    pub reason_codes: Vec<String>,
}

fn add_reason(reasons: &mut Vec<String>, value: impl Into<String>) {
    let value = value.into();
    if !value.is_empty() && !reasons.contains(&value) {
        reasons.push(value);
    }
}

fn semantics(input: &BindingQualityInput) -> &'static str {
    let feed = input.feed.to_ascii_uppercase();
    if feed == "BAR" || input.require_final_bar {
        "FINAL_SCHEDULED"
    } else if input.event_recency_policy == "OBSERVE"
        && matches!(feed.as_str(), "TRADE" | "BOOK_DELTA" | "MARK_INDEX_PRICE")
    {
        "QUIET_SESSION"
    } else {
        "STRICT_EVENT"
    }
}

fn availability(input: &BindingQualityInput) -> &'static str {
    if input.acquisition_mode == "PYTHON_VENDOR_SDK" {
        "EXPECTED_V1_PRIMARY"
    } else if !input.acquisition_enabled {
        "EXPECTED_DARK"
    } else if !input.market_open {
        "OUT_OF_SESSION"
    } else {
        "ACTIVE"
    }
}

/// Evaluate a declared binding strictly from facts supplied by the owning
/// runtime.  It performs no I/O, retry, fallback or timestamp rewriting.
pub fn evaluate_binding_quality(input: &BindingQualityInput) -> BindingQualityDecision {
    let semantics = semantics(input).to_owned();
    let availability = availability(input).to_owned();
    let mut reasons = input.flags.clone();
    let event_recency_state = match input.event_age_ms {
        Some(age) if age > input.event_limit_ms => {
            add_reason(&mut reasons, "LAST_EVENT_STALE");
            "STALE"
        }
        Some(_) => "LIVE",
        None => "NOT_APPLICABLE",
    }
    .to_owned();

    let session_ok = match input.session_limit_ms {
        Some(limit) => {
            input.session_state == "LIVE"
                && input.session_liveness_ms.is_some_and(|age| age <= limit)
        }
        None => !matches!(
            input.session_state.as_str(),
            "STALE" | "DISCONNECTED" | "UNKNOWN"
        ),
    };
    let mut component_ok = true;
    for component in &input.components {
        if component.receipt_age_ms > component.quiet_after_ms {
            component_ok = false;
            add_reason(
                &mut reasons,
                format!("COMPONENT_{}_STALE", component.name.to_uppercase()),
            );
        }
    }
    if !input.generation_matches {
        add_reason(&mut reasons, "GENERATION_MISMATCH");
    }
    if !input.config_matches {
        add_reason(&mut reasons, "CONFIG_REVISION_MISMATCH");
    }
    if input.gap_open {
        add_reason(&mut reasons, "OPEN_SEQUENCE_GAP");
    }
    if !input.book_verified {
        add_reason(&mut reasons, "BOOK_SEQUENCE_UNVERIFIED");
    }
    if input.require_final_bar && !input.final_bar {
        add_reason(&mut reasons, "BAR_NOT_FINAL");
    }
    if !session_ok {
        match input.session_state.as_str() {
            "STALE" | "DISCONNECTED" | "UNKNOWN" => add_reason(
                &mut reasons,
                format!("SOURCE_SESSION_{}", input.session_state),
            ),
            _ => add_reason(&mut reasons, "SOURCE_SESSION_HEARTBEAT_EXPIRED"),
        }
    }

    let state = if availability == "EXPECTED_V1_PRIMARY" {
        add_reason(&mut reasons, "EXPECTED_V1_PRIMARY");
        "DISABLED"
    } else if availability == "EXPECTED_DARK" {
        add_reason(&mut reasons, "EXPECTED_DARK");
        "DISABLED"
    } else if availability == "OUT_OF_SESSION" {
        add_reason(&mut reasons, "OUT_OF_SESSION");
        "MARKET_CLOSED"
    } else if !input.event_present {
        add_reason(&mut reasons, "NO_DURABLE_EVENT");
        "NOT_READY"
    } else if input.gap_open {
        "GAPPED"
    } else if !input.book_verified || (input.require_final_bar && !input.final_bar) {
        "SYNCING"
    } else if !input.generation_matches
        || !input.config_matches
        || !session_ok
        || !component_ok
        || (semantics != "QUIET_SESSION" && event_recency_state == "STALE")
    {
        "STALE"
    } else {
        "LIVE"
    }
    .to_owned();

    let complete = input.event_present
        && !input.gap_open
        && input.book_verified
        && (!input.require_final_bar || input.final_bar);
    let event_ok = matches!(event_recency_state.as_str(), "LIVE" | "NOT_APPLICABLE");
    let quiet_execution_ok =
        semantics == "QUIET_SESSION" && input.allow_quiet_execution && session_ok && component_ok;
    let execution_eligible = availability == "ACTIVE"
        && input.authoritative
        && input.source_role == "PRIMARY"
        && state == "LIVE"
        && complete
        && (event_ok || quiet_execution_ok);

    BindingQualityDecision {
        binding_id: input.binding_id.clone(),
        instrument_uid: input.instrument_uid.clone(),
        feed: input.feed.clone(),
        semantics,
        availability,
        state,
        event_recency_state,
        provider_session_state: input.session_state.clone(),
        provider_session_liveness_ms: input.session_liveness_ms,
        complete,
        execution_eligible,
        watermark_offset: input.watermark_offset,
        reason_codes: reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::{evaluate_binding_quality, BindingQualityInput};
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Fixture {
        schema: String,
        cases: Vec<Case>,
    }

    #[derive(Deserialize)]
    struct Case {
        name: String,
        input: BindingQualityInput,
        expected: Expected,
    }

    #[derive(Deserialize)]
    struct Expected {
        semantics: String,
        availability: String,
        state: String,
        event_recency_state: String,
        complete: bool,
        execution_eligible: bool,
        reason_codes: Vec<String>,
    }

    #[test]
    fn rust_matches_shared_binding_quality_golden_corpus() {
        let fixture: Fixture = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../contracts/golden/quality/binding-quality-decision-v1.json"
        )))
        .expect("quality golden JSON must parse");
        assert_eq!(fixture.schema, "qdl.binding-quality-decision.v1");
        assert!(!fixture.cases.is_empty());
        for case in fixture.cases {
            let actual = evaluate_binding_quality(&case.input);
            assert_eq!(actual.semantics, case.expected.semantics, "{}", case.name);
            assert_eq!(
                actual.availability, case.expected.availability,
                "{}",
                case.name
            );
            assert_eq!(actual.state, case.expected.state, "{}", case.name);
            assert_eq!(
                actual.event_recency_state, case.expected.event_recency_state,
                "{}",
                case.name
            );
            assert_eq!(actual.complete, case.expected.complete, "{}", case.name);
            assert_eq!(
                actual.execution_eligible, case.expected.execution_eligible,
                "{}",
                case.name
            );
            assert_eq!(
                actual.reason_codes, case.expected.reason_codes,
                "{}",
                case.name
            );
        }
    }
}
