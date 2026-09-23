//! Signed handoff cursor v3 (KN-1 K1.3), the native side of
//! `qdl/replay/cursor_v3.py`.
//!
//! Query (Python) issues the token with a snapshot; the native Stream verifies
//! it and signs one per delivered record, so both implementations must agree
//! on every byte and on every rejection reason. The rules are deliberately
//! small: a canonical JSON object with byte-sorted keys, no whitespace,
//! strings from a charset that never needs escaping, unsigned decimal
//! integers below 2^63, base64url without padding, HMAC-SHA256 over the
//! received body bytes. `contracts/golden/kn_v220/cursor_v3.json` is the
//! shared oracle.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ring::{digest, hmac};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::qdl::query::v2 as query;
use crate::requirement::ValidatedRequirement;

pub const SCHEMA_V3: &str = "qdl.handoff-cursor.v3";
pub const LEGACY_SCHEMAS: [&str; 2] = ["qdl.handoff-cursor.v1", "qdl.handoff-cursor.v2"];
pub const REQUIREMENT_DIGEST_SCHEMA: &str = "qdl.requirement-digest.v1";
pub const MAX_INTEGER: u64 = i64::MAX as u64;
const MAX_TOKEN_BYTES: usize = 4096;

/// Why a token was refused. `Expired` maps to gRPC OUT_OF_RANGE, which the
/// SDK answers with a fresh snapshot; `Invalid` maps to INVALID_ARGUMENT,
/// which the SDK does not recover from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CursorError {
    Invalid(&'static str),
    Expired(&'static str),
}

impl CursorError {
    pub fn reason(&self) -> &'static str {
        match self {
            CursorError::Invalid(reason) | CursorError::Expired(reason) => reason,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorV3Claims {
    pub key_id: String,
    pub environment: String,
    pub consumer_id: String,
    pub requirement_digest: String,
    pub schema_major: u64,
    pub stream: String,
    pub product_key: String,
    pub snapshot_id: String,
    pub source_topic_id: String,
    pub source_partition: u64,
    pub source_offset: u64,
    pub partition_plan_epoch: u64,
    pub source_policy_revision: u64,
    pub catalog_revision: u64,
    pub route_generation: String,
    pub issued_at_ns: u64,
    pub expires_at_ns: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorV3Expectation {
    pub environment: String,
    pub stream: String,
    pub source_topic_id: String,
    pub partition_plan_epoch: u64,
    pub source_policy_revision: u64,
    pub catalog_revision: u64,
    pub route_generation: String,
    pub schema_major: u64,
}

enum Field {
    Text(String),
    Integer(u64),
}

/// Field order is the Python dataclass order: validation reports the first
/// failing field in this order on both sides.
const FIELDS: [(&str, bool); 17] = [
    ("key_id", false),
    ("environment", false),
    ("consumer_id", false),
    ("requirement_digest", false),
    ("schema_major", true),
    ("stream", false),
    ("product_key", false),
    ("snapshot_id", false),
    ("source_topic_id", false),
    ("source_partition", true),
    ("source_offset", true),
    ("partition_plan_epoch", true),
    ("source_policy_revision", true),
    ("catalog_revision", true),
    ("route_generation", false),
    ("issued_at_ns", true),
    ("expires_at_ns", true),
];

pub fn is_token_text(value: &str) -> bool {
    (1..=256).contains(&value.len())
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'.' | b'_' | b':' | b'/' | b'@' | b'|' | b'+' | b'=' | b'-'
                )
        })
}

fn is_hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl CursorV3Claims {
    fn fields(&self) -> BTreeMap<&'static str, Field> {
        let text = |value: &String| Field::Text(value.clone());
        BTreeMap::from([
            ("key_id", text(&self.key_id)),
            ("environment", text(&self.environment)),
            ("consumer_id", text(&self.consumer_id)),
            ("requirement_digest", text(&self.requirement_digest)),
            ("schema_major", Field::Integer(self.schema_major)),
            ("stream", text(&self.stream)),
            ("product_key", text(&self.product_key)),
            ("snapshot_id", text(&self.snapshot_id)),
            ("source_topic_id", text(&self.source_topic_id)),
            ("source_partition", Field::Integer(self.source_partition)),
            ("source_offset", Field::Integer(self.source_offset)),
            (
                "partition_plan_epoch",
                Field::Integer(self.partition_plan_epoch),
            ),
            (
                "source_policy_revision",
                Field::Integer(self.source_policy_revision),
            ),
            ("catalog_revision", Field::Integer(self.catalog_revision)),
            ("route_generation", text(&self.route_generation)),
            ("issued_at_ns", Field::Integer(self.issued_at_ns)),
            ("expires_at_ns", Field::Integer(self.expires_at_ns)),
            ("schema", Field::Text(SCHEMA_V3.to_owned())),
        ])
    }

    pub fn validate(&self) -> Result<(), CursorError> {
        let fields = self.fields();
        for (name, integer) in FIELDS {
            match (&fields[name], integer) {
                (Field::Integer(value), true) if *value <= MAX_INTEGER => {}
                (Field::Text(value), false) if is_token_text(value) => {}
                (_, true) => return Err(CursorError::Invalid("FIELD_RANGE")),
                (_, false) => return Err(CursorError::Invalid("FIELD_CHARSET")),
            }
        }
        if !is_hex64(&self.requirement_digest) {
            return Err(CursorError::Invalid("FIELD_CHARSET"));
        }
        if self.expires_at_ns <= self.issued_at_ns {
            return Err(CursorError::Invalid("FIELD_RANGE"));
        }
        Ok(())
    }

    /// Canonical body bytes. `BTreeMap<&str, _>` orders keys by byte value,
    /// which is the Python `sorted(key=str.encode)` order.
    pub fn body(&self) -> Result<Vec<u8>, CursorError> {
        self.validate()?;
        let mut body = String::from("{");
        for (index, (name, field)) in self.fields().into_iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            body.push('"');
            body.push_str(name);
            body.push_str("\":");
            match field {
                Field::Integer(value) => body.push_str(&value.to_string()),
                Field::Text(value) => {
                    body.push('"');
                    body.push_str(&value);
                    body.push('"');
                }
            }
        }
        body.push('}');
        Ok(body.into_bytes())
    }
}

fn decode_part(value: &str) -> Result<Vec<u8>, CursorError> {
    // The default engine rejects padding and non-canonical trailing bits,
    // which is the rule the Python codec enforces by re-encoding.
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(CursorError::Invalid("ENCODING"));
    }
    URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| CursorError::Invalid("ENCODING"))
}

fn claims_from_object(raw: &Map<String, Value>) -> Result<CursorV3Claims, CursorError> {
    for (name, integer) in FIELDS {
        let value = &raw[name];
        if integer {
            match value.as_u64() {
                Some(number) if number <= MAX_INTEGER => {}
                _ => return Err(CursorError::Invalid("FIELD_RANGE")),
            }
        } else {
            match value.as_str() {
                Some(text) if is_token_text(text) => {}
                _ => return Err(CursorError::Invalid("FIELD_CHARSET")),
            }
        }
    }
    let text = |name: &str| raw[name].as_str().unwrap_or_default().to_owned();
    let integer = |name: &str| raw[name].as_u64().unwrap_or_default();
    let claims = CursorV3Claims {
        key_id: text("key_id"),
        environment: text("environment"),
        consumer_id: text("consumer_id"),
        requirement_digest: text("requirement_digest"),
        schema_major: integer("schema_major"),
        stream: text("stream"),
        product_key: text("product_key"),
        snapshot_id: text("snapshot_id"),
        source_topic_id: text("source_topic_id"),
        source_partition: integer("source_partition"),
        source_offset: integer("source_offset"),
        partition_plan_epoch: integer("partition_plan_epoch"),
        source_policy_revision: integer("source_policy_revision"),
        catalog_revision: integer("catalog_revision"),
        route_generation: text("route_generation"),
        issued_at_ns: integer("issued_at_ns"),
        expires_at_ns: integer("expires_at_ns"),
    };
    claims.validate()?;
    Ok(claims)
}

pub struct CursorV3Codec {
    keys: BTreeMap<String, hmac::Key>,
    active_key_id: String,
}

impl CursorV3Codec {
    pub fn new(keys: &BTreeMap<String, Vec<u8>>, active_key_id: &str) -> Result<Self, String> {
        if !keys.contains_key(active_key_id) {
            return Err("active cursor-signing key is unavailable".into());
        }
        let mut prepared = BTreeMap::new();
        for (key_id, secret) in keys {
            if secret.len() < 32 {
                return Err("cursor-signing secrets must contain at least 256 bits".into());
            }
            if !is_token_text(key_id) {
                return Err("cursor key id has characters outside the token charset".into());
            }
            prepared.insert(key_id.clone(), hmac::Key::new(hmac::HMAC_SHA256, secret));
        }
        Ok(Self {
            keys: prepared,
            active_key_id: active_key_id.to_owned(),
        })
    }

    pub fn active_key_id(&self) -> &str {
        &self.active_key_id
    }

    pub fn encode(&self, claims: &CursorV3Claims) -> Result<String, CursorError> {
        if claims.key_id != self.active_key_id {
            return Err(CursorError::Invalid("INACTIVE_KEY"));
        }
        let body = claims.body()?;
        let signature = hmac::sign(&self.keys[&claims.key_id], &body);
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(&body),
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        ))
    }

    /// Verify in the exact order of the Python codec so both report the same
    /// reason for the same token.
    pub fn verify(
        &self,
        token: &str,
        consumer_id: &str,
        environment: &str,
        requirement_digest: &str,
        expected: &CursorV3Expectation,
        now_ns: u64,
    ) -> Result<CursorV3Claims, CursorError> {
        if token.len() > MAX_TOKEN_BYTES || token.matches('.').count() != 1 {
            return Err(CursorError::Invalid("ENCODING"));
        }
        let (encoded_body, encoded_signature) = token
            .split_once('.')
            .ok_or(CursorError::Invalid("ENCODING"))?;
        let body = decode_part(encoded_body)?;
        let signature = decode_part(encoded_signature)?;
        let raw: Value =
            serde_json::from_slice(&body).map_err(|_| CursorError::Invalid("ENCODING"))?;
        let object = raw.as_object().ok_or(CursorError::Invalid("ENCODING"))?;
        match object.get("schema").and_then(Value::as_str) {
            Some(schema) if LEGACY_SCHEMAS.contains(&schema) => {
                return Err(CursorError::Expired("LEGACY_SCHEMA"))
            }
            Some(SCHEMA_V3) => {}
            _ => return Err(CursorError::Invalid("SCHEMA")),
        }
        let key = object
            .get("key_id")
            .and_then(Value::as_str)
            .and_then(|key_id| self.keys.get(key_id))
            .ok_or(CursorError::Invalid("UNKNOWN_KEY"))?;
        hmac::verify(key, &body, &signature).map_err(|_| CursorError::Invalid("SIGNATURE"))?;
        if object.len() != FIELDS.len() + 1
            || !FIELDS.iter().all(|(name, _)| object.contains_key(*name))
        {
            return Err(CursorError::Invalid("FIELDS"));
        }
        let claims = claims_from_object(object)?;
        if claims.body()? != body {
            return Err(CursorError::Invalid("NON_CANONICAL"));
        }
        if claims.consumer_id != consumer_id {
            return Err(CursorError::Invalid("CONSUMER"));
        }
        if claims.environment != environment || environment != expected.environment {
            return Err(CursorError::Invalid("ENVIRONMENT"));
        }
        if claims.requirement_digest != requirement_digest {
            return Err(CursorError::Invalid("REQUIREMENT"));
        }
        let checks: [(&'static str, bool); 7] = [
            ("SCHEMA_MAJOR", claims.schema_major == expected.schema_major),
            ("STREAM", claims.stream == expected.stream),
            (
                "TOPIC_GENERATION",
                claims.source_topic_id == expected.source_topic_id,
            ),
            (
                "PARTITION_PLAN",
                claims.partition_plan_epoch == expected.partition_plan_epoch,
            ),
            (
                "SOURCE_POLICY",
                claims.source_policy_revision == expected.source_policy_revision,
            ),
            (
                "CATALOG",
                claims.catalog_revision == expected.catalog_revision,
            ),
            (
                "ROUTE_GENERATION",
                claims.route_generation == expected.route_generation,
            ),
        ];
        if let Some((reason, _)) = checks.iter().find(|(_, same)| !same) {
            return Err(CursorError::Expired(reason));
        }
        if now_ns >= claims.expires_at_ns {
            return Err(CursorError::Expired("EXPIRED"));
        }
        Ok(claims)
    }
}

/// The normalized delivery requirement both sides digest. The historical
/// horizon (`warmup_limit`, `warmup`) is deliberately absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryRequirement {
    pub instrument_uid: String,
    pub feed: String,
    pub interval: Option<String>,
    pub consumer_grade: String,
    pub source_policy_id: String,
    pub max_freshness_ms: Option<u64>,
    pub event_recency_policy: Option<String>,
    pub max_session_liveness_ms: Option<u64>,
    pub require_full_coverage: bool,
    pub require_final_bars: bool,
    pub stale_policy: String,
    pub gap_policy: String,
    pub recovery: String,
    pub bar_revision_policy: String,
}

impl DeliveryRequirement {
    /// Same mapping as `qdl.stream.grpc_service.requirement_from_proto`,
    /// including the full `DataRequirement` validation: a requirement the
    /// Python server refuses is never digested (KN-1 F1).
    pub fn from_proto(value: &query::DataRequirement) -> Result<Self, String> {
        ValidatedRequirement::from_proto(value)
            .map(|requirement| requirement.delivery)
            .map_err(|error| error.message)
    }

    pub fn digest(&self) -> String {
        let optional = |value: &Option<String>| value.clone().unwrap_or_default();
        let number = |value: Option<u64>| value.map(|v| v.to_string()).unwrap_or_default();
        let lines = [
            REQUIREMENT_DIGEST_SCHEMA.to_owned(),
            format!("instrument_uid={}", self.instrument_uid),
            format!("feed={}", self.feed),
            format!("interval={}", optional(&self.interval)),
            format!("consumer_grade={}", self.consumer_grade),
            format!("source_policy_id={}", self.source_policy_id),
            format!("max_freshness_ms={}", number(self.max_freshness_ms)),
            format!(
                "event_recency_policy={}",
                optional(&self.event_recency_policy)
            ),
            format!(
                "max_session_liveness_ms={}",
                number(self.max_session_liveness_ms)
            ),
            format!("require_full_coverage={}", self.require_full_coverage),
            format!("require_final_bars={}", self.require_final_bars),
            format!("stale_policy={}", self.stale_policy),
            format!("gap_policy={}", self.gap_policy),
            format!("recovery={}", self.recovery),
            format!("bar_revision_policy={}", self.bar_revision_policy),
        ];
        let hashed = digest::digest(&digest::SHA256, lines.join("\n").as_bytes());
        let mut hex = String::with_capacity(64);
        for byte in hashed.as_ref() {
            let _ = write!(hex, "{byte:02x}");
        }
        hex
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn golden(name: &str) -> Value {
        let path = format!(
            "{}/../../contracts/golden/kn_v220/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        serde_json::from_str(&std::fs::read_to_string(path).expect("golden file")).expect("json")
    }

    fn codec(doc: &Value) -> CursorV3Codec {
        let keys = doc["keys"]
            .as_object()
            .expect("keys")
            .iter()
            .map(|(id, secret)| {
                (
                    id.clone(),
                    secret.as_str().expect("secret").as_bytes().to_vec(),
                )
            })
            .collect();
        CursorV3Codec::new(&keys, doc["active_key_id"].as_str().expect("active")).expect("codec")
    }

    fn claims(value: &Value) -> CursorV3Claims {
        claims_from_object(value.as_object().expect("claims")).expect("valid claims")
    }

    fn expectation(value: &Value) -> CursorV3Expectation {
        let text = |name: &str| value[name].as_str().expect("text").to_owned();
        let integer = |name: &str| value[name].as_u64().expect("integer");
        CursorV3Expectation {
            environment: text("environment"),
            stream: text("stream"),
            source_topic_id: text("source_topic_id"),
            partition_plan_epoch: integer("partition_plan_epoch"),
            source_policy_revision: integer("source_policy_revision"),
            catalog_revision: integer("catalog_revision"),
            route_generation: text("route_generation"),
            schema_major: integer("schema_major"),
        }
    }

    #[test]
    fn canonical_body_and_token_match_python_bytes() {
        let doc = golden("cursor_v3.json");
        let canonical = &doc["canonical"];
        let claims = claims(&canonical["claims"]);
        assert_eq!(
            String::from_utf8(claims.body().expect("body")).expect("utf8"),
            canonical["body"].as_str().expect("body")
        );
        assert_eq!(
            codec(&doc).encode(&claims).expect("token"),
            canonical["token"].as_str().expect("token")
        );
        assert_eq!(claims.source_offset, 9_223_372_036_854_775_806);
    }

    #[test]
    fn every_golden_verification_case_has_the_same_outcome() {
        let doc = golden("cursor_v3.json");
        let codec = codec(&doc);
        let cases = doc["cases"].as_array().expect("cases");
        assert!(cases.len() >= 25);
        for case in cases {
            let name = case["name"].as_str().expect("name");
            let result = codec.verify(
                case["token"].as_str().expect("token"),
                case["consumer_id"].as_str().expect("consumer"),
                case["environment"].as_str().expect("environment"),
                case["requirement_digest"].as_str().expect("digest"),
                &expectation(&case["expectation"]),
                case["now_ns"].as_u64().expect("now"),
            );
            let (outcome, reason) = match &result {
                Ok(_) => ("OK", None),
                Err(CursorError::Expired(reason)) => ("EXPIRED", Some(*reason)),
                Err(CursorError::Invalid(reason)) => ("INVALID", Some(*reason)),
            };
            assert_eq!(
                outcome,
                case["outcome"].as_str().expect("outcome"),
                "{name}"
            );
            assert_eq!(reason, case["reason"].as_str(), "{name}");
        }
    }

    #[test]
    fn malformed_claims_are_refused_with_the_python_reason() {
        let doc = golden("cursor_v3.json");
        let cases = doc["claims_invalid"].as_array().expect("claims_invalid");
        assert!(cases.len() >= 4);
        for case in cases {
            let refused = claims_from_object(case["claims"].as_object().expect("claims"))
                .expect_err(case["name"].as_str().expect("name"));
            assert_eq!(
                Some(refused.reason()),
                case["reason"].as_str(),
                "{}",
                case["name"]
            );
        }
    }

    #[test]
    fn only_the_active_key_signs() {
        let doc = golden("cursor_v3.json");
        let mut rotated = claims(&doc["canonical"]["claims"]);
        rotated.key_id = "kn1-test-k1".into();
        assert_eq!(
            codec(&doc).encode(&rotated),
            Err(CursorError::Invalid("INACTIVE_KEY"))
        );
    }

    fn proto_requirement(value: &Value) -> query::DataRequirement {
        let text = |name: &str| value[name].as_str().unwrap_or_default().to_owned();
        let named = |name: &str, prefix: &str| format!("{prefix}{}", text(name));
        query::DataRequirement {
            instrument_uid: text("instrument_uid"),
            interval: text("interval"),
            source_policy_id: text("source_policy_id"),
            warmup_limit: value["warmup_limit"].as_u64().unwrap_or_default() as u32,
            max_freshness_ms: value["max_freshness_ms"].as_u64().unwrap_or_default(),
            max_session_liveness_ms: value["max_session_liveness_ms"]
                .as_u64()
                .unwrap_or_default(),
            require_full_coverage: value["require_full_coverage"].as_bool().expect("coverage"),
            require_final_bars: value["require_final_bars"].as_bool().expect("final"),
            feed_type: query::FeedType::from_str_name(&named("feed", "FEED_TYPE_")).expect("feed")
                as i32,
            grade: query::ConsumerGrade::from_str_name(&named("consumer_grade", "CONSUMER_GRADE_"))
                .expect("grade") as i32,
            stale_policy_type: query::StalePolicy::from_str_name(&named(
                "stale_policy",
                "STALE_POLICY_",
            ))
            .expect("stale") as i32,
            gap_policy_type: query::GapPolicy::from_str_name(&named("gap_policy", "GAP_POLICY_"))
                .expect("gap") as i32,
            recovery_policy: query::RecoveryPolicy::from_str_name(&named(
                "recovery",
                "RECOVERY_POLICY_",
            ))
            .expect("recovery") as i32,
            revision_policy: query::BarRevisionPolicy::from_str_name(&named(
                "bar_revision_policy",
                "BAR_REVISION_POLICY_",
            ))
            .expect("revision") as i32,
            event_recency_policy: if value["event_recency_policy"].is_string() {
                query::StalePolicy::from_str_name(&named("event_recency_policy", "STALE_POLICY_"))
                    .expect("recency") as i32
            } else {
                0
            },
            ..Default::default()
        }
    }

    #[test]
    fn requirement_digest_from_the_proto_matches_python() {
        let doc = golden("requirement_digest.json");
        for vector in doc["vectors"].as_array().expect("vectors") {
            let requirement =
                DeliveryRequirement::from_proto(&proto_requirement(&vector["requirement"]))
                    .expect("requirement");
            assert_eq!(
                requirement.digest(),
                vector["digest"].as_str().expect("digest"),
                "{}",
                vector["name"]
            );
        }
    }

    #[test]
    fn unspecified_enums_are_refused_like_python() {
        let requirement = query::DataRequirement::default();
        assert!(DeliveryRequirement::from_proto(&requirement).is_err());
    }
}
