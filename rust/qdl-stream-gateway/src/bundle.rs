//! The authorization bundle compiled by `scripts/kn_gateway_bundle.py` from
//! the Python manifest and catalog loaders. The gateway never reparses YAML;
//! it refuses a bundle whose canonical SHA-256 does not match.

use ring::digest;
use serde_json::{Map, Value};

pub const SCHEMA: &str = "qdl.kn.v220.gateway-bundle.v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    pub binding_id: String,
    pub instrument_uid: String,
    pub venue: String,
    pub market: String,
    pub feed: String,
    pub interval: Option<String>,
    pub source_policy_id: String,
    pub physical_key: String,
    pub product_key: String,
    pub stale_after_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestRequirement {
    pub instrument_uid: String,
    pub feed: String,
    pub interval: Option<String>,
    pub consumer_grade: String,
    pub source_policy_id: String,
    pub event_recency_policy: Option<String>,
    pub max_session_liveness_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Quotas {
    pub requests_per_minute: u64,
    pub max_batch_items: u64,
    pub max_warmup_rows: u64,
    pub max_streams: u64,
    pub max_buffer_events: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub consumer_id: String,
    pub subject: String,
    pub environment: String,
    pub manifest_revision: u64,
    pub allowed_purposes: Vec<String>,
    pub allowed_permissions: Vec<String>,
    pub quotas: Quotas,
    pub requirements: Vec<ManifestRequirement>,
}

#[derive(Clone, Debug)]
pub struct Bundle {
    pub sha256: String,
    pub environment: String,
    pub catalog_revision: u64,
    pub source_policy_revision: u64,
    pub canonical_stream: String,
    pub bindings: Vec<Binding>,
    pub manifests: Vec<Manifest>,
}

fn text(value: &Value, name: &str) -> Result<String, String> {
    value[name]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("bundle field {name} must be a string"))
}

fn optional_text(value: &Value, name: &str) -> Result<Option<String>, String> {
    match &value[name] {
        Value::Null => Ok(None),
        Value::String(text) => Ok(Some(text.clone())),
        _ => Err(format!("bundle field {name} must be a string or null")),
    }
}

fn integer(value: &Value, name: &str) -> Result<u64, String> {
    value[name]
        .as_u64()
        .ok_or_else(|| format!("bundle field {name} must be an unsigned integer"))
}

fn strings(value: &Value, name: &str) -> Result<Vec<String>, String> {
    value[name]
        .as_array()
        .ok_or_else(|| format!("bundle field {name} must be an array"))?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("bundle {name} entries must be strings"))
        })
        .collect()
}

/// SHA-256 of the canonical JSON (sorted keys, compact) without `sha256`,
/// the same bytes `scripts/kn_gateway_bundle.py` hashes.
pub fn canonical_sha256(document: &Map<String, Value>) -> String {
    let mut body = document.clone();
    body.remove("sha256");
    let encoded = serde_json::to_string(&Value::Object(body)).expect("JSON value serializes");
    digest::digest(&digest::SHA256, encoded.as_bytes())
        .as_ref()
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

impl Bundle {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let document: Value = serde_json::from_str(raw).map_err(|error| error.to_string())?;
        let object = document.as_object().ok_or("bundle must be a JSON object")?;
        if document["schema"].as_str() != Some(SCHEMA) {
            return Err("unsupported gateway bundle schema".into());
        }
        let declared = text(&document, "sha256")?;
        let computed = canonical_sha256(object);
        if declared != computed {
            return Err(format!(
                "gateway bundle hash mismatch: declared {declared}, computed {computed}"
            ));
        }
        let catalog = &document["catalog"];
        let bindings = catalog["bindings"]
            .as_array()
            .ok_or("bundle catalog bindings must be an array")?
            .iter()
            .map(|item| {
                Ok(Binding {
                    binding_id: text(item, "binding_id")?,
                    instrument_uid: text(item, "instrument_uid")?,
                    venue: text(item, "venue")?,
                    market: text(item, "market")?,
                    feed: text(item, "feed")?,
                    interval: optional_text(item, "interval")?,
                    source_policy_id: text(item, "source_policy_id")?,
                    physical_key: text(item, "physical_key")?,
                    product_key: text(item, "product_key")?,
                    stale_after_ms: integer(item, "stale_after_ms")?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let manifests = document["manifests"]
            .as_array()
            .ok_or("bundle manifests must be an array")?
            .iter()
            .map(|item| {
                let quotas = &item["quotas"];
                Ok(Manifest {
                    consumer_id: text(item, "consumer_id")?,
                    subject: text(item, "subject")?,
                    environment: text(item, "environment")?,
                    manifest_revision: integer(item, "manifest_revision")?,
                    allowed_purposes: strings(item, "allowed_purposes")?,
                    allowed_permissions: strings(item, "allowed_permissions")?,
                    quotas: Quotas {
                        requests_per_minute: integer(quotas, "requests_per_minute")?,
                        max_batch_items: integer(quotas, "max_batch_items")?,
                        max_warmup_rows: integer(quotas, "max_warmup_rows")?,
                        max_streams: integer(quotas, "max_streams")?,
                        max_buffer_events: integer(quotas, "max_buffer_events")?,
                    },
                    requirements: item["requirements"]
                        .as_array()
                        .ok_or("manifest requirements must be an array")?
                        .iter()
                        .map(|requirement| {
                            Ok(ManifestRequirement {
                                instrument_uid: text(requirement, "instrument_uid")?,
                                feed: text(requirement, "feed")?,
                                interval: optional_text(requirement, "interval")?,
                                consumer_grade: text(requirement, "consumer_grade")?,
                                source_policy_id: text(requirement, "source_policy_id")?,
                                event_recency_policy: optional_text(
                                    requirement,
                                    "event_recency_policy",
                                )?,
                                max_session_liveness_ms: requirement["max_session_liveness_ms"]
                                    .as_u64(),
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            sha256: declared,
            environment: text(&document, "environment")?,
            catalog_revision: integer(catalog, "catalog_revision")?,
            source_policy_revision: integer(catalog, "source_policy_revision")?,
            canonical_stream: text(catalog, "canonical_stream")?,
            bindings,
            manifests,
        })
    }

    pub fn manifest_by_subject(&self, environment: &str, subject: &str) -> Option<&Manifest> {
        self.manifests
            .iter()
            .find(|item| item.environment == environment && item.subject == subject)
    }

    /// `StableSourceCatalog.binding_for`: (uid, feed, interval), then policy.
    pub fn binding_for(
        &self,
        instrument_uid: &str,
        feed: &str,
        interval: Option<&str>,
        source_policy_id: &str,
    ) -> Option<&Binding> {
        self.bindings
            .iter()
            .find(|item| {
                item.instrument_uid == instrument_uid
                    && item.feed == feed
                    && item.interval.as_deref() == interval
            })
            .filter(|item| item.source_policy_id == source_policy_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Value {
        serde_json::json!({
            "schema": SCHEMA,
            "environment": "paper",
            "catalog": {"catalog_revision": 9, "source_policy_revision": 1,
                "canonical_stream": "md.canonical.v2", "bindings": [{
                    "binding_id": "okx-swap-btc-usdt-swap-trade",
                    "instrument_uid": "fb26214c-7b9b-5961-95b2-55154755af0f",
                    "venue": "OKX", "market": "SWAP", "feed": "TRADE", "interval": null,
                    "source_policy_id": "crypto_primary_v2",
                    "physical_key": "fb26214c-7b9b-5961-95b2-55154755af0f/trade/okx-swap",
                    "source_id": "okx-swap",
                    "stale_after_ms": 2000,
                    "product_key": "lpk1|paper|OKX|SWAP|fb26214c-7b9b-5961-95b2-55154755af0f|TRADE|-"}]},
            "manifests": []
        })
    }

    #[test]
    fn a_bundle_with_the_right_hash_parses_and_a_tampered_one_is_refused() {
        let mut document = sample();
        let hash = canonical_sha256(document.as_object().expect("object"));
        document["sha256"] = Value::String(hash);
        let bundle = Bundle::parse(&document.to_string()).expect("valid bundle");
        assert_eq!(bundle.catalog_revision, 9);
        assert!(bundle
            .binding_for(
                "fb26214c-7b9b-5961-95b2-55154755af0f",
                "TRADE",
                None,
                "crypto_primary_v2"
            )
            .is_some());
        assert!(bundle
            .binding_for(
                "fb26214c-7b9b-5961-95b2-55154755af0f",
                "TRADE",
                None,
                "other"
            )
            .is_none());
        document["catalog"]["catalog_revision"] = Value::from(10);
        assert!(Bundle::parse(&document.to_string())
            .unwrap_err()
            .contains("hash mismatch"));
    }
}
