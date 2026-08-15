use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EdgeRuntime {
    Rust,
    PythonSdk,
    FixtureOnly,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityManifest {
    pub schema_version: u32,
    pub venue: String,
    pub market: String,
    pub product_type: String,
    pub feed: String,
    pub edge_runtime: EdgeRuntime,
    pub adapter_version: String,
    pub native_sequence_field: String,
    pub sequence_scope: String,
    pub source_timestamp_precision: String,
    pub heartbeat: String,
    pub subscription_ack: String,
    pub duplicate_identity: String,
    pub reconnect_sequence_continuity: String,
    pub rate_limit_profile: String,
    pub supports_raw_exact_frame: bool,
    pub authority_eligible: bool,
    pub certification: String,
}

impl CapabilityManifest {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err("unsupported capability schema".into());
        }
        for (name, value) in [
            ("venue", &self.venue),
            ("market", &self.market),
            ("product_type", &self.product_type),
            ("feed", &self.feed),
            ("adapter_version", &self.adapter_version),
            ("sequence_scope", &self.sequence_scope),
            ("heartbeat", &self.heartbeat),
            ("subscription_ack", &self.subscription_ack),
            ("duplicate_identity", &self.duplicate_identity),
            ("rate_limit_profile", &self.rate_limit_profile),
            ("certification", &self.certification),
        ] {
            if value.trim().is_empty() {
                return Err(format!("capability {name} must not be empty"));
            }
        }
        if self.certification != "FIXTURE_ONLY" && !self.supports_raw_exact_frame {
            return Err("live/shadow capability requires exact raw-frame support".into());
        }
        Ok(())
    }
}
