//! Native validation of a public `DataRequirement` (KN-1 finding F1), the
//! Rust side of `qdl.stream.grpc_service.requirement_from_proto` followed by
//! `WarmupSpecification` and `DataRequirement.__post_init__`.
//!
//! A requirement is validated before it is digested or served, so the native
//! gateway refuses exactly what the Python server refuses, in the same order
//! and with the same message. Every refusal carries a stable rule code;
//! `contracts/golden/kn_v220/requirement_validation.json` (produced by the
//! Python server path) is the shared oracle for both languages.

use crate::cursor_v3::DeliveryRequirement;
use crate::qdl::query::v2 as query;

const METRIC_INTERVAL_FEEDS: [&str; 3] = ["LONG_SHORT_RATIO", "TAKER_FLOW", "BASIS"];
const OPTIONAL_INTERVAL_FEEDS: [&str; 1] = ["OPEN_INTEREST"];
const EXECUTION_PRICE_VALIDATION_FEEDS: [&str; 6] = [
    "TRADE",
    "QUOTE",
    "BAR",
    "BOOK_SNAPSHOT",
    "BOOK_DELTA",
    "MARK_INDEX_PRICE",
];
const MAX_PUBLIC_WARMUP_ROWS: u32 = 10_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequirementError {
    pub rule: &'static str,
    pub message: String,
}

impl RequirementError {
    fn new(rule: &'static str, message: impl Into<String>) -> Self {
        Self {
            rule,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RequirementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WarmupHorizon {
    Rows(u32),
    TimeRange {
        start_time_ns: i64,
        end_time_ns: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Warmup {
    pub horizon: WarmupHorizon,
    pub interval_source_policy: String,
    pub max_cache_age_ms: u32,
    pub deadline_ms: u32,
}

/// A requirement the Python server would accept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedRequirement {
    pub delivery: DeliveryRequirement,
    pub warmup_limit: u32,
    pub warmup: Option<Warmup>,
}

/// `str.isspace` semantics: Unicode White_Space plus the four ASCII
/// separators U+001C..U+001F that Python also strips.
fn is_python_space(character: char) -> bool {
    character.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&character)
}

fn is_blank(value: &str) -> bool {
    value.chars().all(is_python_space)
}

/// `enum_value` of `requirement_from_proto`: an unknown wire number is refused
/// as `Enum.Name` refuses it, then UNSPECIFIED is refused.
fn enum_name(
    name: Option<&'static str>,
    number: i32,
    wrapper: &str,
    prefix: &str,
) -> Result<String, RequirementError> {
    let name = name.ok_or_else(|| {
        RequirementError::new(
            "ENUM_UNKNOWN",
            format!("Enum {wrapper} has no name defined for value {number}"),
        )
    })?;
    if name.ends_with("_UNSPECIFIED") {
        let rule = match prefix {
            "FEED_TYPE_" => "FEED_UNSPECIFIED",
            "CONSUMER_GRADE_" => "GRADE_UNSPECIFIED",
            "STALE_POLICY_" => "STALE_UNSPECIFIED",
            "GAP_POLICY_" => "GAP_UNSPECIFIED",
            "RECOVERY_POLICY_" => "RECOVERY_UNSPECIFIED",
            _ => "REVISION_UNSPECIFIED",
        };
        return Err(RequirementError::new(
            rule,
            format!("{} cannot be UNSPECIFIED", prefix.to_ascii_lowercase()),
        ));
    }
    Ok(name.strip_prefix(prefix).unwrap_or(name).to_owned())
}

macro_rules! proto_enum {
    ($type:ident, $number:expr, $prefix:literal) => {
        enum_name(
            query::$type::try_from($number)
                .ok()
                .map(|value| value.as_str_name()),
            $number,
            stringify!($type),
            $prefix,
        )
    };
}

fn warmup_from_proto(value: &query::WarmupSpecification) -> Result<Warmup, RequirementError> {
    let Some(horizon) = value.horizon.as_ref() else {
        return Err(RequirementError::new(
            "WARMUP_HORIZON",
            "warmup horizon is required",
        ));
    };
    if value.interval_source_policy == query::IntervalSourcePolicy::Unspecified as i32 {
        return Err(RequirementError::new(
            "WARMUP_INTERVAL_SOURCE",
            "warmup interval source policy is required",
        ));
    }
    let interval_source_policy =
        query::IntervalSourcePolicy::try_from(value.interval_source_policy)
            .map_err(|_| {
                RequirementError::new(
                    "ENUM_UNKNOWN",
                    format!(
                        "Enum IntervalSourcePolicy has no name defined for value {}",
                        value.interval_source_policy
                    ),
                )
            })?
            .as_str_name()
            .trim_start_matches("INTERVAL_SOURCE_POLICY_")
            .to_owned();
    let horizon = match horizon {
        query::warmup_specification::Horizon::Rows(rows) => WarmupHorizon::Rows(*rows),
        query::warmup_specification::Horizon::TimeRange(range) => {
            if range.start_time_ns <= 0 || range.end_time_ns <= range.start_time_ns {
                return Err(RequirementError::new(
                    "WARMUP_TIME_RANGE",
                    "warmup time range must be positive and increasing",
                ));
            }
            WarmupHorizon::TimeRange {
                start_time_ns: range.start_time_ns,
                end_time_ns: range.end_time_ns,
            }
        }
    };
    if let WarmupHorizon::Rows(rows) = horizon {
        if !(1..=100_000).contains(&rows) {
            return Err(RequirementError::new(
                "WARMUP_ROWS_RANGE",
                "warmup rows must be between 1 and 100000",
            ));
        }
    }
    if value.max_cache_age_ms > 86_400_000 {
        return Err(RequirementError::new(
            "WARMUP_CACHE_AGE",
            "warmup max_cache_age_ms is outside bounds",
        ));
    }
    if !(100..=120_000).contains(&value.deadline_ms) {
        return Err(RequirementError::new(
            "WARMUP_DEADLINE",
            "warmup deadline_ms must be between 100 and 120000",
        ));
    }
    Ok(Warmup {
        horizon,
        interval_source_policy,
        max_cache_age_ms: value.max_cache_age_ms,
        deadline_ms: value.deadline_ms,
    })
}

impl ValidatedRequirement {
    pub fn from_proto(value: &query::DataRequirement) -> Result<Self, RequirementError> {
        let feed = proto_enum!(FeedType, value.feed_type, "FEED_TYPE_")?;
        let consumer_grade = proto_enum!(ConsumerGrade, value.grade, "CONSUMER_GRADE_")?;
        let event_recency_policy =
            if value.event_recency_policy == query::StalePolicy::Unspecified as i32 {
                None
            } else {
                Some(proto_enum!(
                    StalePolicy,
                    value.event_recency_policy,
                    "STALE_POLICY_"
                )?)
            };
        let stale_policy = proto_enum!(StalePolicy, value.stale_policy_type, "STALE_POLICY_")?;
        let gap_policy = proto_enum!(GapPolicy, value.gap_policy_type, "GAP_POLICY_")?;
        let recovery = proto_enum!(RecoveryPolicy, value.recovery_policy, "RECOVERY_POLICY_")?;
        let bar_revision_policy = proto_enum!(
            BarRevisionPolicy,
            value.revision_policy,
            "BAR_REVISION_POLICY_"
        )?;
        let warmup = value.warmup.as_ref().map(warmup_from_proto).transpose()?;
        let delivery = DeliveryRequirement {
            instrument_uid: value.instrument_uid.clone(),
            feed,
            interval: (!value.interval.is_empty()).then(|| value.interval.clone()),
            consumer_grade,
            source_policy_id: value.source_policy_id.clone(),
            max_freshness_ms: (value.max_freshness_ms != 0).then_some(value.max_freshness_ms),
            event_recency_policy,
            max_session_liveness_ms: (value.max_session_liveness_ms != 0)
                .then_some(value.max_session_liveness_ms),
            require_full_coverage: value.require_full_coverage,
            require_final_bars: value.require_final_bars,
            stale_policy,
            gap_policy,
            recovery,
            bar_revision_policy,
        };
        let requirement = Self {
            delivery,
            warmup_limit: value.warmup_limit,
            warmup,
        };
        requirement.check()?;
        Ok(requirement)
    }

    /// `DataRequirement.__post_init__`, in its order. The positivity checks
    /// on `max_freshness_ms` / `max_session_liveness_ms` and the enum
    /// UNSPECIFIED check cannot fail here: the proto mapping already turned
    /// zero into `None` and refused UNSPECIFIED.
    fn check(&self) -> Result<(), RequirementError> {
        let delivery = &self.delivery;
        if is_blank(&delivery.instrument_uid) {
            return Err(RequirementError::new(
                "INSTRUMENT_UID",
                "instrument_uid is required",
            ));
        }
        if is_blank(&delivery.source_policy_id) {
            return Err(RequirementError::new(
                "SOURCE_POLICY_ID",
                "source_policy_id is required",
            ));
        }
        if self.warmup_limit > MAX_PUBLIC_WARMUP_ROWS {
            return Err(RequirementError::new(
                "WARMUP_LIMIT",
                "warmup_limit must be between 0 and 10000",
            ));
        }
        match self.warmup.as_ref().map(|warmup| &warmup.horizon) {
            Some(WarmupHorizon::Rows(rows)) => {
                if *rows > MAX_PUBLIC_WARMUP_ROWS {
                    return Err(RequirementError::new(
                        "WARMUP_ROWS_PUBLIC",
                        "public V2 warmup rows cannot exceed 10000",
                    ));
                }
                if self.warmup_limit != 0 && self.warmup_limit != *rows {
                    return Err(RequirementError::new(
                        "WARMUP_LIMIT_CONFLICT",
                        "warmup_limit conflicts with warmup.rows",
                    ));
                }
            }
            Some(WarmupHorizon::TimeRange { .. }) if self.warmup_limit != 0 => {
                return Err(RequirementError::new(
                    "WARMUP_TIME_RANGE_LIMIT",
                    "time-range warmup cannot also declare warmup_limit",
                ));
            }
            _ => {}
        }
        if delivery.event_recency_policy.as_deref() == Some("OBSERVE")
            && delivery.max_session_liveness_ms.is_none()
        {
            return Err(RequirementError::new(
                "RECENCY_OBSERVE_SLA",
                "observed event recency requires an explicit provider session SLA",
            ));
        }
        let feed = delivery.feed.as_str();
        let interval_blank = delivery.interval.as_deref().is_none_or(is_blank);
        if feed == "BAR" {
            if interval_blank {
                return Err(RequirementError::new(
                    "BAR_INTERVAL",
                    "bar requirements need an interval",
                ));
            }
        } else if METRIC_INTERVAL_FEEDS.contains(&feed) {
            if interval_blank {
                return Err(RequirementError::new(
                    "METRIC_INTERVAL",
                    "metric-series requirements need a sampling interval",
                ));
            }
        } else if OPTIONAL_INTERVAL_FEEDS.contains(&feed) {
            if delivery.interval.as_deref().is_some_and(is_blank) {
                return Err(RequirementError::new(
                    "OI_INTERVAL_BLANK",
                    "open-interest sampling interval cannot be blank",
                ));
            }
        } else if delivery.interval.is_some() {
            return Err(RequirementError::new(
                "INTERVAL_NOT_ALLOWED",
                "interval is valid only for bar or metric-series requirements",
            ));
        }
        if delivery.consumer_grade == "EXECUTION" {
            if !EXECUTION_PRICE_VALIDATION_FEEDS.contains(&feed) {
                return Err(RequirementError::new(
                    "EXECUTION_FEED",
                    "execution-grade requirements need an execution-price validation feed",
                ));
            }
            if delivery.stale_policy != "BLOCK" {
                return Err(RequirementError::new(
                    "EXECUTION_STALE",
                    "execution-grade stale policy must BLOCK",
                ));
            }
            if delivery.gap_policy != "BLOCK" {
                return Err(RequirementError::new(
                    "EXECUTION_GAP",
                    "execution-grade gap policy must BLOCK",
                ));
            }
            if !delivery.require_full_coverage {
                return Err(RequirementError::new(
                    "EXECUTION_COVERAGE",
                    "execution-grade requirements need full coverage",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn golden() -> Value {
        serde_json::from_str(include_str!(
            "../../../contracts/golden/kn_v220/requirement_validation.json"
        ))
        .expect("requirement validation golden")
    }

    fn enum_number(value: &Value, prefix: &str, from_name: fn(&str) -> Option<i32>) -> i32 {
        match value {
            Value::Null => 0,
            Value::Number(number) => number.as_i64().expect("enum number") as i32,
            Value::String(name) => from_name(&format!("{prefix}{name}")).expect("enum name"),
            other => panic!("enum value {other}"),
        }
    }

    fn proto(value: &Value) -> query::DataRequirement {
        let text = |field: &str| value[field].as_str().expect(field).to_owned();
        let number = |field: &str| value[field].as_u64().expect(field);
        let flag = |field: &str| value[field].as_bool().expect(field);
        let stale = |field: &str| {
            enum_number(&value[field], "STALE_POLICY_", |name| {
                query::StalePolicy::from_str_name(name).map(|v| v as i32)
            })
        };
        let warmup = value["warmup"].as_object().map(|warmup| {
            let horizon = match warmup["horizon"].as_str().expect("horizon") {
                "rows" => Some(query::warmup_specification::Horizon::Rows(
                    warmup["rows"].as_u64().expect("rows") as u32,
                )),
                "time_range" => Some(query::warmup_specification::Horizon::TimeRange(
                    query::WarmupTimeRange {
                        start_time_ns: warmup["start_time_ns"].as_i64().expect("start"),
                        end_time_ns: warmup["end_time_ns"].as_i64().expect("end"),
                    },
                )),
                _ => None,
            };
            query::WarmupSpecification {
                horizon,
                interval_source_policy: enum_number(
                    &warmup["interval_source_policy"],
                    "INTERVAL_SOURCE_POLICY_",
                    |name| query::IntervalSourcePolicy::from_str_name(name).map(|v| v as i32),
                ),
                max_cache_age_ms: warmup["max_cache_age_ms"].as_u64().expect("age") as u32,
                deadline_ms: warmup["deadline_ms"].as_u64().expect("deadline") as u32,
            }
        });
        #[allow(deprecated)]
        query::DataRequirement {
            instrument_uid: text("instrument_uid"),
            interval: text("interval"),
            source_policy_id: text("source_policy_id"),
            warmup_limit: number("warmup_limit") as u32,
            max_freshness_ms: number("max_freshness_ms"),
            require_full_coverage: flag("require_full_coverage"),
            require_final_bars: flag("require_final_bars"),
            feed_type: enum_number(&value["feed"], "FEED_TYPE_", |name| {
                query::FeedType::from_str_name(name).map(|v| v as i32)
            }),
            grade: enum_number(&value["consumer_grade"], "CONSUMER_GRADE_", |name| {
                query::ConsumerGrade::from_str_name(name).map(|v| v as i32)
            }),
            stale_policy_type: stale("stale_policy"),
            gap_policy_type: enum_number(&value["gap_policy"], "GAP_POLICY_", |name| {
                query::GapPolicy::from_str_name(name).map(|v| v as i32)
            }),
            recovery_policy: enum_number(&value["recovery"], "RECOVERY_POLICY_", |name| {
                query::RecoveryPolicy::from_str_name(name).map(|v| v as i32)
            }),
            revision_policy: enum_number(
                &value["bar_revision_policy"],
                "BAR_REVISION_POLICY_",
                |name| query::BarRevisionPolicy::from_str_name(name).map(|v| v as i32),
            ),
            event_recency_policy: stale("event_recency_policy"),
            max_session_liveness_ms: number("max_session_liveness_ms"),
            warmup,
            ..Default::default()
        }
    }

    #[test]
    fn every_golden_case_gets_the_python_outcome() {
        let golden = golden();
        let cases = golden["cases"].as_array().expect("cases");
        assert!(cases.len() >= 40);
        for case in cases {
            let name = case["name"].as_str().expect("name");
            let outcome = ValidatedRequirement::from_proto(&proto(&case["requirement"]));
            match case["rule"].as_str() {
                None => assert!(outcome.is_ok(), "{name}: {outcome:?}"),
                Some(rule) => {
                    let error = outcome.expect_err(name);
                    assert_eq!(error.rule, rule, "{name}");
                    // Same message as the Python server (INVALID_ARGUMENT detail).
                    let prefix = golden["rule_messages"][rule].as_str().expect("message");
                    assert!(
                        error.message.starts_with(prefix),
                        "{name}: {}",
                        error.message
                    );
                }
            }
        }
    }

    #[test]
    fn every_rule_code_is_exercised_by_the_golden() {
        let golden = golden();
        let mut seen: Vec<&str> = golden["cases"]
            .as_array()
            .expect("cases")
            .iter()
            .filter_map(|case| case["rule"].as_str())
            .collect();
        seen.sort_unstable();
        seen.dedup();
        let rules: Vec<&str> = golden["rules"]
            .as_array()
            .expect("rules")
            .iter()
            .map(|rule| rule.as_str().expect("rule"))
            .collect();
        assert_eq!(seen, rules);
    }

    #[test]
    fn a_digest_is_never_produced_for_an_invalid_requirement() {
        let golden = golden();
        for case in golden["cases"].as_array().expect("cases") {
            let digest = DeliveryRequirement::from_proto(&proto(&case["requirement"]));
            assert_eq!(digest.is_ok(), case["rule"].is_null(), "{}", case["name"]);
        }
    }
}
