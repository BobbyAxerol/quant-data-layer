//! Kafka-native state contract (KN-1 K1.3), the native side of
//! `qdl/projection/state_contract.py`: logical product keys, source versus
//! changelog coordinates, stage-B apply, append-only BAR revisions and cache
//! read state. `contracts/golden/kn_v220/state_contract.json` is the shared
//! oracle; the KN-3 projector implements these rules, it does not invent them.

use serde_json::{Map, Value};

pub const LPK_VERSION: &str = "lpk1";
pub const MAX_OFFSET: u64 = i64::MAX as u64;
const MAX_PARTITION: u64 = (1 << 31) - 1;

/// Strict JSON decoding shared by the state records (KN-1 F4): exactly the
/// named fields; integers are JSON integers (never `true`, never `1.0`),
/// strings are strings. `qdl/projection/state_contract.py` refuses the same
/// shapes.
fn object<'a>(
    value: &'a Value,
    names: &[&str],
    what: &str,
) -> Result<&'a Map<String, Value>, String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{what} must be an object"))?;
    if object.len() != names.len() || names.iter().any(|name| !object.contains_key(*name)) {
        return Err(format!("{what} fields are incomplete or unknown"));
    }
    Ok(object)
}

fn integer(
    object: &Map<String, Value>,
    name: &str,
    maximum: u64,
    what: &str,
) -> Result<u64, String> {
    object[name]
        .as_u64()
        .filter(|value| *value <= maximum)
        .ok_or_else(|| format!("{what} field is invalid: {name}"))
}

fn text(object: &Map<String, Value>, name: &str, what: &str) -> Result<String, String> {
    object[name]
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("{what} field is invalid: {name}"))
}

fn is_hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn is_lpk_field(value: &str) -> bool {
    (1..=96).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalProductKey {
    pub environment: String,
    pub venue: String,
    pub market: String,
    pub instrument_uid: String,
    pub feed: String,
    pub qualifier: String,
}

impl LogicalProductKey {
    pub fn new(
        environment: &str,
        venue: &str,
        market: &str,
        instrument_uid: &str,
        feed: &str,
        interval: Option<&str>,
    ) -> Result<Self, String> {
        let key = Self {
            environment: environment.to_owned(),
            venue: venue.to_owned(),
            market: market.to_owned(),
            instrument_uid: instrument_uid.to_owned(),
            feed: feed.to_owned(),
            qualifier: interval.unwrap_or("-").to_owned(),
        };
        key.validate()?;
        Ok(key)
    }

    fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("environment", &self.environment),
            ("venue", &self.venue),
            ("market", &self.market),
            ("instrument_uid", &self.instrument_uid),
            ("feed", &self.feed),
            ("qualifier", &self.qualifier),
        ] {
            if !is_lpk_field(value) {
                return Err(format!("logical product key field is invalid: {name}"));
            }
        }
        for value in [&self.venue, &self.market, &self.feed] {
            if value.to_ascii_uppercase() != *value {
                return Err("venue, market and feed are upper-case enum names".into());
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> String {
        [
            LPK_VERSION,
            &self.environment,
            &self.venue,
            &self.market,
            &self.instrument_uid,
            &self.feed,
            &self.qualifier,
        ]
        .join("|")
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        let parts: Vec<&str> = value.split('|').collect();
        if parts.len() != 7 || parts[0] != LPK_VERSION {
            return Err("unsupported logical product key".into());
        }
        let key = Self {
            environment: parts[1].to_owned(),
            venue: parts[2].to_owned(),
            market: parts[3].to_owned(),
            instrument_uid: parts[4].to_owned(),
            feed: parts[5].to_owned(),
            qualifier: parts[6].to_owned(),
        };
        key.validate()?;
        if key.encode() != value {
            return Err("logical product key is not canonical".into());
        }
        Ok(key)
    }
}

/// The committed canonical record a state was derived from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceCoordinate {
    pub topic_id: String,
    pub partition: u32,
    pub offset: u64,
}

/// Where a derived state record sits. Delivery metadata only; public cursors
/// never carry it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangelogCoordinate {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub materializer_epoch: u64,
}

impl SourceCoordinate {
    pub fn from_value(value: &Value) -> Result<Self, String> {
        const WHAT: &str = "source coordinate";
        let object = object(value, &["topic_id", "partition", "offset"], WHAT)?;
        Ok(Self {
            topic_id: text(object, "topic_id", WHAT)?,
            partition: integer(object, "partition", MAX_PARTITION, WHAT)? as u32,
            offset: integer(object, "offset", MAX_OFFSET, WHAT)?,
        })
    }
}

impl ChangelogCoordinate {
    pub fn from_value(value: &Value) -> Result<Self, String> {
        const WHAT: &str = "changelog coordinate";
        let object = object(
            value,
            &["topic", "partition", "offset", "materializer_epoch"],
            WHAT,
        )?;
        let materializer_epoch = integer(object, "materializer_epoch", MAX_OFFSET, WHAT)?;
        if materializer_epoch < 1 {
            return Err("changelog coordinate is out of range".into());
        }
        Ok(Self {
            topic: text(object, "topic", WHAT)?,
            partition: integer(object, "partition", MAX_PARTITION, WHAT)? as u32,
            offset: integer(object, "offset", MAX_OFFSET, WHAT)?,
            materializer_epoch,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyDecision {
    Apply,
    Duplicate,
    Stale,
    NotComparable,
    StaleInProgress,
    StaleRevision,
    Conflict,
}

impl ApplyDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            ApplyDecision::Apply => "APPLY",
            ApplyDecision::Duplicate => "DUPLICATE",
            ApplyDecision::Stale => "STALE",
            ApplyDecision::NotComparable => "NOT_COMPARABLE",
            ApplyDecision::StaleInProgress => "STALE_IN_PROGRESS",
            ApplyDecision::StaleRevision => "STALE_REVISION",
            ApplyDecision::Conflict => "CONFLICT",
        }
    }
}

/// Offsets compare only inside one topic identity and physical partition.
pub fn latest_apply_decision(
    current: Option<&SourceCoordinate>,
    incoming: &SourceCoordinate,
) -> ApplyDecision {
    let Some(current) = current else {
        return ApplyDecision::Apply;
    };
    if current.topic_id != incoming.topic_id || current.partition != incoming.partition {
        return ApplyDecision::NotComparable;
    }
    match incoming.offset.cmp(&current.offset) {
        std::cmp::Ordering::Greater => ApplyDecision::Apply,
        std::cmp::Ordering::Equal => ApplyDecision::Duplicate,
        std::cmp::Ordering::Less => ApplyDecision::Stale,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BarState {
    pub is_final: bool,
    pub revision: u32,
    pub content_sha256: String,
    pub source: SourceCoordinate,
}

impl BarState {
    pub fn from_value(value: &Value) -> Result<Self, String> {
        const WHAT: &str = "bar state";
        let object = object(
            value,
            &["is_final", "revision", "content_sha256", "source"],
            WHAT,
        )?;
        let content_sha256 = text(object, "content_sha256", WHAT)?;
        if !is_hex64(&content_sha256) {
            return Err("bar content hash must be lowercase SHA-256".into());
        }
        Ok(Self {
            is_final: object["is_final"]
                .as_bool()
                .ok_or("bar state field is invalid: is_final")?,
            revision: integer(object, "revision", u64::from(u32::MAX), WHAT)? as u32,
            content_sha256,
            source: SourceCoordinate::from_value(&object["source"])?,
        })
    }
}

/// A final bar is never replaced by an in-progress update; an equal revision
/// with different content is a conflict, never last-write-wins.
pub fn bar_revision_decision(current: Option<&BarState>, incoming: &BarState) -> ApplyDecision {
    let Some(current) = current else {
        return ApplyDecision::Apply;
    };
    match (current.is_final, incoming.is_final) {
        (true, false) => ApplyDecision::StaleInProgress,
        (false, true) => ApplyDecision::Apply,
        (false, false) => latest_apply_decision(Some(&current.source), &incoming.source),
        (true, true) => match incoming.revision.cmp(&current.revision) {
            std::cmp::Ordering::Greater => ApplyDecision::Apply,
            std::cmp::Ordering::Less => ApplyDecision::StaleRevision,
            std::cmp::Ordering::Equal if incoming.content_sha256 == current.content_sha256 => {
                ApplyDecision::Duplicate
            }
            std::cmp::Ordering::Equal => ApplyDecision::Conflict,
        },
    }
}

/// A product is served only from the published ready cache generation.
pub fn cache_read_state(
    ready_generation: Option<u64>,
    entry_generation: Option<u64>,
) -> &'static str {
    match (ready_generation, entry_generation) {
        (None, _) => "NOT_READY_NO_GENERATION",
        (Some(_), None) => "NOT_READY_MISSING",
        (Some(ready), Some(entry)) if ready != entry => "NOT_READY_OTHER_GENERATION",
        _ => "READY",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn golden() -> Value {
        let path = format!(
            "{}/../../contracts/golden/kn_v220/state_contract.json",
            env!("CARGO_MANIFEST_DIR")
        );
        serde_json::from_str(&std::fs::read_to_string(path).expect("golden")).expect("json")
    }

    fn coordinate(value: &Value) -> SourceCoordinate {
        SourceCoordinate::from_value(value).expect("coordinate")
    }

    fn bar(value: &Value) -> Option<BarState> {
        (!value.is_null()).then(|| BarState::from_value(value).expect("bar"))
    }

    #[test]
    fn malformed_state_records_are_refused_like_python() {
        let doc = golden();
        let list = |name: &str| doc[name].as_array().expect(name).clone();
        for value in list("source_coordinates_invalid") {
            assert!(SourceCoordinate::from_value(&value).is_err(), "{value}");
        }
        for value in list("changelog_coordinates_invalid") {
            assert!(ChangelogCoordinate::from_value(&value).is_err(), "{value}");
        }
        for value in list("changelog_coordinates_valid") {
            assert!(ChangelogCoordinate::from_value(&value).is_ok(), "{value}");
        }
        for value in list("bar_states_invalid") {
            assert!(BarState::from_value(&value).is_err(), "{value}");
        }
        assert!(list("source_coordinates_invalid").len() >= 10);
        assert!(list("bar_states_invalid").len() >= 10);
    }

    #[test]
    fn logical_product_keys() {
        let doc = golden();
        for vector in doc["logical_product_keys"].as_array().expect("keys") {
            let product = &vector["product"];
            let text = |name: &str| product[name].as_str().expect("field");
            let key = LogicalProductKey::new(
                text("environment"),
                text("venue"),
                text("market"),
                text("instrument_uid"),
                text("feed"),
                product["interval"].as_str(),
            )
            .expect("valid key");
            assert_eq!(key.encode(), vector["key"].as_str().expect("key"));
            assert_eq!(LogicalProductKey::parse(&key.encode()).expect("parse"), key);
        }
        for value in doc["logical_product_keys_invalid"]
            .as_array()
            .expect("invalid")
        {
            assert!(
                LogicalProductKey::parse(value.as_str().expect("text")).is_err(),
                "{value}"
            );
        }
    }

    #[test]
    fn latest_apply() {
        for case in golden()["latest_apply"].as_array().expect("cases") {
            let current = (!case["current"].is_null()).then(|| coordinate(&case["current"]));
            assert_eq!(
                latest_apply_decision(current.as_ref(), &coordinate(&case["incoming"])).as_str(),
                case["decision"].as_str().expect("decision"),
                "{}",
                case["name"]
            );
        }
    }

    #[test]
    fn bar_revision() {
        for case in golden()["bar_revision"].as_array().expect("cases") {
            let incoming = bar(&case["incoming"]).expect("incoming");
            assert_eq!(
                bar_revision_decision(bar(&case["current"]).as_ref(), &incoming).as_str(),
                case["decision"].as_str().expect("decision"),
                "{}",
                case["name"]
            );
        }
    }

    #[test]
    fn cache_read_states() {
        for case in golden()["cache_read_state"].as_array().expect("cases") {
            assert_eq!(
                cache_read_state(
                    case["ready_generation"].as_u64(),
                    case["entry_generation"].as_u64()
                ),
                case["state"].as_str().expect("state"),
                "{}",
                case["name"]
            );
        }
    }
}
