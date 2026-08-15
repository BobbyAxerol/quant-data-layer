use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuthorityMode {
    RustShadow,
    RustCanary,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SinkTarget {
    ShadowRaw,
    ShadowCanonical,
    CanaryCanonical,
    PublicV2,
    LegacyV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityRecord {
    pub schema: String,
    pub slice_id: String,
    pub revision: u64,
    pub mode: AuthorityMode,
    pub candidate_image_digest: String,
    pub capability_manifest_digest: String,
    pub contract_digest: String,
    pub partition_plan_digest: String,
    pub public_write_allowed: bool,
    pub legacy_write_allowed: bool,
    pub approved_by: String,
    pub effective_at_ns: i64,
}

impl AuthorityRecord {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema != "qdl.authority-record.v1"
            || self.slice_id.trim().is_empty()
            || self.revision == 0
            || self.approved_by.trim().is_empty()
            || self.effective_at_ns <= 0
        {
            return Err("authority record identity/revision is invalid".into());
        }
        if !valid_digest(&self.candidate_image_digest, true)
            || !valid_digest(&self.capability_manifest_digest, false)
            || !valid_digest(&self.contract_digest, false)
            || !valid_digest(&self.partition_plan_digest, false)
        {
            return Err("authority record digest is invalid".into());
        }
        if self.public_write_allowed || self.legacy_write_allowed {
            return Err("Phase 8 authority record cannot enable public or legacy writes".into());
        }
        Ok(())
    }
}

fn valid_digest(value: &str, prefixed: bool) -> bool {
    let text = if prefixed {
        value.strip_prefix("sha256:")
    } else {
        Some(value)
    };
    text.is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationContext {
    pub slice_id: String,
    pub authority_revision: u64,
    pub shard_id: String,
    pub lease_epoch: u64,
    pub target: SinkTarget,
}

#[derive(Default)]
pub struct AuthorityFence {
    current: Option<AuthorityRecord>,
    lease_epochs: HashMap<String, u64>,
}

impl AuthorityFence {
    pub fn apply(&mut self, record: AuthorityRecord) -> Result<(), String> {
        record.validate()?;
        if let Some(current) = &self.current {
            if record.slice_id != current.slice_id {
                return Err("authority slice cannot change inside one fence".into());
            }
            if record.revision < current.revision {
                return Err("stale authority revision".into());
            }
            if record.revision == current.revision {
                return if record == *current {
                    Ok(())
                } else {
                    Err("conflicting authority record at the same revision".into())
                };
            }
        }
        self.current = Some(record);
        Ok(())
    }

    pub fn permits(&mut self, context: &PublicationContext) -> Result<(), String> {
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| "authority record is not loaded".to_owned())?;
        if context.slice_id != current.slice_id
            || context.authority_revision != current.revision
            || context.lease_epoch == 0
            || context.shard_id.trim().is_empty()
        {
            return Err("publication identity does not match current authority".into());
        }
        let latest = self
            .lease_epochs
            .get(&context.shard_id)
            .copied()
            .unwrap_or(0);
        if context.lease_epoch < latest {
            return Err("stale publication lease epoch".into());
        }
        self.lease_epochs
            .insert(context.shard_id.clone(), context.lease_epoch);
        let allowed = match current.mode {
            AuthorityMode::RustShadow => matches!(
                context.target,
                SinkTarget::ShadowRaw | SinkTarget::ShadowCanonical
            ),
            AuthorityMode::RustCanary => matches!(
                context.target,
                SinkTarget::ShadowRaw | SinkTarget::ShadowCanonical | SinkTarget::CanaryCanonical
            ),
        };
        if !allowed {
            return Err("sink target is not permitted by current authority".into());
        }
        Ok(())
    }

    pub fn current(&self) -> Option<&AuthorityRecord> {
        self.current.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthorityFence, AuthorityMode, AuthorityRecord, PublicationContext, SinkTarget};

    fn record(revision: u64, mode: AuthorityMode) -> AuthorityRecord {
        AuthorityRecord {
            schema: "qdl.authority-record.v1".into(),
            slice_id: "BINANCE:USDM:TRADE:BTCUSDT".into(),
            revision,
            mode,
            candidate_image_digest: format!("sha256:{}", "1".repeat(64)),
            capability_manifest_digest: "2".repeat(64),
            contract_digest: "3".repeat(64),
            partition_plan_digest: "4".repeat(64),
            public_write_allowed: false,
            legacy_write_allowed: false,
            approved_by: "phase8-certification".into(),
            effective_at_ns: 1,
        }
    }

    fn publication(revision: u64, lease_epoch: u64, target: SinkTarget) -> PublicationContext {
        PublicationContext {
            slice_id: "BINANCE:USDM:TRADE:BTCUSDT".into(),
            authority_revision: revision,
            shard_id: "binance-usdm-trade-0".into(),
            lease_epoch,
            target,
        }
    }

    #[test]
    fn shadow_canary_shadow_never_grants_public_or_legacy_target() {
        let mut fence = AuthorityFence::default();
        fence.apply(record(1, AuthorityMode::RustShadow)).unwrap();
        assert!(fence
            .permits(&publication(1, 1, SinkTarget::ShadowCanonical))
            .is_ok());
        assert!(fence
            .permits(&publication(1, 1, SinkTarget::PublicV2))
            .is_err());
        fence.apply(record(2, AuthorityMode::RustCanary)).unwrap();
        assert!(fence
            .permits(&publication(2, 2, SinkTarget::CanaryCanonical))
            .is_ok());
        assert!(fence
            .permits(&publication(2, 2, SinkTarget::LegacyV1))
            .is_err());
        fence.apply(record(3, AuthorityMode::RustShadow)).unwrap();
        assert!(fence
            .permits(&publication(3, 3, SinkTarget::CanaryCanonical))
            .is_err());
    }

    #[test]
    fn stale_revision_lease_and_conflicting_record_fail_closed() {
        let mut fence = AuthorityFence::default();
        fence.apply(record(2, AuthorityMode::RustCanary)).unwrap();
        assert!(fence.apply(record(1, AuthorityMode::RustShadow)).is_err());
        let mut conflict = record(2, AuthorityMode::RustCanary);
        conflict.approved_by = "other".into();
        assert!(fence.apply(conflict).is_err());
        fence
            .permits(&publication(2, 4, SinkTarget::CanaryCanonical))
            .unwrap();
        assert!(fence
            .permits(&publication(2, 3, SinkTarget::CanaryCanonical))
            .is_err());
    }
}
