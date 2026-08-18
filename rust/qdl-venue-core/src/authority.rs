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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Phase9AuthorityState {
    RustShadow,
    RustCanary,
    Blocked,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Phase9AuthorityRecord {
    pub schema: String,
    pub slice_id: String,
    pub state: Phase9AuthorityState,
    pub owner_id: String,
    pub authority_revision: u64,
    pub lease_epoch: u64,
    pub partition_plan_epoch: u64,
    pub candidate_digest: String,
    pub prerequisite_bundle_id: Option<String>,
    pub start_watermark: u64,
    pub approved_by: Option<String>,
    pub approved_at_ns: Option<i64>,
    pub hold_until_ns: Option<i64>,
    pub public_write_allowed: bool,
    pub legacy_write_allowed: bool,
}

impl Phase9AuthorityRecord {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema != "qdl.authority-record.v2"
            || self.slice_id.trim().is_empty()
            || self.owner_id.trim().is_empty()
            || self.authority_revision == 0
            || self.lease_epoch == 0
            || self.partition_plan_epoch == 0
            || !valid_digest(&self.candidate_digest, false)
        {
            return Err("Phase 9 authority identity/epoch is invalid".into());
        }
        if self.public_write_allowed || self.legacy_write_allowed {
            return Err("Phase 9.1 cannot enable public or legacy writes".into());
        }
        match self.state {
            Phase9AuthorityState::RustCanary => {
                let bundle = self
                    .prerequisite_bundle_id
                    .as_deref()
                    .ok_or_else(|| "canary prerequisite bundle is required".to_owned())?;
                let approved_by = self
                    .approved_by
                    .as_deref()
                    .ok_or_else(|| "canary approver is required".to_owned())?;
                let approved_at = self
                    .approved_at_ns
                    .ok_or_else(|| "canary approval time is required".to_owned())?;
                let hold_until = self
                    .hold_until_ns
                    .ok_or_else(|| "canary hold time is required".to_owned())?;
                if !valid_uuid(bundle)
                    || approved_by.trim().is_empty()
                    || approved_at <= 0
                    || hold_until <= approved_at
                {
                    return Err("canary approval/bundle/hold is invalid".into());
                }
            }
            Phase9AuthorityState::RustShadow | Phase9AuthorityState::Blocked => {
                if self.prerequisite_bundle_id.is_some()
                    || self.approved_by.is_some()
                    || self.approved_at_ns.is_some()
                    || self.hold_until_ns.is_some()
                {
                    return Err("non-canary authority cannot carry an approval bundle".into());
                }
            }
        }
        Ok(())
    }
}

fn valid_uuid(value: &str) -> bool {
    let widths = [8, 4, 4, 4, 12];
    let parts: Vec<&str> = value.split('-').collect();
    parts.len() == widths.len()
        && parts.iter().zip(widths).all(|(part, width)| {
            part.len() == width && part.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Phase9PublicationContext {
    pub slice_id: String,
    pub owner_id: String,
    pub authority_revision: u64,
    pub shard_id: String,
    pub lease_epoch: u64,
    pub partition_plan_epoch: u64,
    pub source_watermark: u64,
    pub target: SinkTarget,
}

#[derive(Default)]
pub struct Phase9AuthorityFence {
    current: Option<Phase9AuthorityRecord>,
    committed_watermarks: HashMap<String, u64>,
}

impl Phase9AuthorityFence {
    pub fn apply(&mut self, record: Phase9AuthorityRecord) -> Result<(), String> {
        record.validate()?;
        if let Some(current) = &self.current {
            if record.slice_id != current.slice_id
                || record.candidate_digest != current.candidate_digest
                || record.partition_plan_epoch != current.partition_plan_epoch
            {
                return Err(
                    "Phase 9 authority scope/candidate/plan cannot change inside one fence".into(),
                );
            }
            if record.authority_revision < current.authority_revision {
                return Err("stale Phase 9 authority revision".into());
            }
            if record.authority_revision == current.authority_revision {
                return if record == *current {
                    Ok(())
                } else {
                    Err("conflicting Phase 9 authority record at the same revision".into())
                };
            }
            if record.lease_epoch < current.lease_epoch
                || (record.owner_id != current.owner_id
                    && record.lease_epoch <= current.lease_epoch)
            {
                return Err("stale or conflicting Phase 9 owner lease".into());
            }
            let transition_allowed = match current.state {
                Phase9AuthorityState::RustShadow => matches!(
                    record.state,
                    Phase9AuthorityState::RustShadow
                        | Phase9AuthorityState::RustCanary
                        | Phase9AuthorityState::Blocked
                ),
                Phase9AuthorityState::RustCanary => matches!(
                    record.state,
                    Phase9AuthorityState::RustCanary
                        | Phase9AuthorityState::RustShadow
                        | Phase9AuthorityState::Blocked
                ),
                Phase9AuthorityState::Blocked => matches!(
                    record.state,
                    Phase9AuthorityState::Blocked | Phase9AuthorityState::RustShadow
                ),
            };
            if !transition_allowed {
                return Err("Phase 9 authority transition is not permitted".into());
            }
        }
        self.current = Some(record);
        Ok(())
    }

    pub fn permits(&self, context: &Phase9PublicationContext, now_ns: i64) -> Result<(), String> {
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| "Phase 9 authority record is not loaded".to_owned())?;
        if now_ns <= 0
            || context.slice_id != current.slice_id
            || context.owner_id != current.owner_id
            || context.authority_revision != current.authority_revision
            || context.lease_epoch != current.lease_epoch
            || context.partition_plan_epoch != current.partition_plan_epoch
            || context.shard_id.trim().is_empty()
        {
            return Err("publication identity does not match current Phase 9 authority".into());
        }
        if matches!(context.target, SinkTarget::PublicV2 | SinkTarget::LegacyV1) {
            return Err("Phase 9.1 public and legacy targets are fenced".into());
        }
        let target_allowed = match current.state {
            Phase9AuthorityState::RustShadow => matches!(
                context.target,
                SinkTarget::ShadowRaw | SinkTarget::ShadowCanonical
            ),
            Phase9AuthorityState::RustCanary => {
                let approved_at = current.approved_at_ns.expect("validated canary approval");
                let hold_until = current.hold_until_ns.expect("validated canary hold");
                if now_ns < approved_at || now_ns >= hold_until {
                    return Err("Phase 9 canary approval window is not active".into());
                }
                matches!(
                    context.target,
                    SinkTarget::ShadowRaw
                        | SinkTarget::ShadowCanonical
                        | SinkTarget::CanaryCanonical
                )
            }
            Phase9AuthorityState::Blocked => false,
        };
        if !target_allowed {
            return Err("sink target is not permitted by current Phase 9 authority".into());
        }
        let committed = self
            .committed_watermarks
            .get(&context.shard_id)
            .copied()
            .unwrap_or(current.start_watermark);
        if context.source_watermark <= committed {
            return Err("source watermark is stale or already committed".into());
        }
        Ok(())
    }

    pub fn commit(&mut self, context: &Phase9PublicationContext) -> Result<(), String> {
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| "Phase 9 authority record is not loaded".to_owned())?;
        if context.slice_id != current.slice_id
            || context.owner_id != current.owner_id
            || context.authority_revision != current.authority_revision
            || context.lease_epoch != current.lease_epoch
            || context.partition_plan_epoch != current.partition_plan_epoch
        {
            return Err("authority changed before publication commit".into());
        }
        let committed = self
            .committed_watermarks
            .get(&context.shard_id)
            .copied()
            .unwrap_or(current.start_watermark);
        if context.source_watermark <= committed {
            return Err("publication watermark commit regressed".into());
        }
        self.committed_watermarks
            .insert(context.shard_id.clone(), context.source_watermark);
        Ok(())
    }

    pub fn current(&self) -> Option<&Phase9AuthorityRecord> {
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

#[cfg(test)]
mod phase9_tests {
    use super::{
        Phase9AuthorityFence, Phase9AuthorityRecord, Phase9AuthorityState,
        Phase9PublicationContext, SinkTarget,
    };

    const SLICE: &str = "production/binance/usdm/perpetual/trade/plan-1/btcusdt";
    const OWNER: &str = "rust-ingestor-binance-usdm-shard-0";
    const BUNDLE: &str = "558042db-a766-5a55-b5b3-4b508d649df9";

    fn record(
        revision: u64,
        lease_epoch: u64,
        state: Phase9AuthorityState,
    ) -> Phase9AuthorityRecord {
        let canary = state == Phase9AuthorityState::RustCanary;
        Phase9AuthorityRecord {
            schema: "qdl.authority-record.v2".into(),
            slice_id: SLICE.into(),
            state,
            owner_id: OWNER.into(),
            authority_revision: revision,
            lease_epoch,
            partition_plan_epoch: 1,
            candidate_digest: "1".repeat(64),
            prerequisite_bundle_id: canary.then(|| BUNDLE.into()),
            start_watermark: 100,
            approved_by: canary.then(|| "phase91-operator".into()),
            approved_at_ns: canary.then_some(1_000),
            hold_until_ns: canary.then_some(10_000),
            public_write_allowed: false,
            legacy_write_allowed: false,
        }
    }

    fn publication(target: SinkTarget, watermark: u64) -> Phase9PublicationContext {
        Phase9PublicationContext {
            slice_id: SLICE.into(),
            owner_id: OWNER.into(),
            authority_revision: 2,
            shard_id: "binance-usdm-trade-0".into(),
            lease_epoch: 2,
            partition_plan_epoch: 1,
            source_watermark: watermark,
            target,
        }
    }

    #[test]
    fn canary_allows_only_isolated_target_inside_approval_window() {
        let mut fence = Phase9AuthorityFence::default();
        fence
            .apply(record(1, 1, Phase9AuthorityState::RustShadow))
            .unwrap();
        fence
            .apply(record(2, 2, Phase9AuthorityState::RustCanary))
            .unwrap();
        assert!(fence
            .permits(&publication(SinkTarget::CanaryCanonical, 101), 2_000)
            .is_ok());
        assert!(fence
            .permits(&publication(SinkTarget::PublicV2, 101), 2_000)
            .is_err());
        assert!(fence
            .permits(&publication(SinkTarget::LegacyV1, 101), 2_000)
            .is_err());
        assert!(fence
            .permits(&publication(SinkTarget::CanaryCanonical, 101), 999)
            .is_err());
        assert!(fence
            .permits(&publication(SinkTarget::CanaryCanonical, 101), 10_000)
            .is_err());
    }

    #[test]
    fn publication_binds_every_identity_and_epoch() {
        let mut fence = Phase9AuthorityFence::default();
        fence
            .apply(record(2, 2, Phase9AuthorityState::RustCanary))
            .unwrap();
        let base = publication(SinkTarget::CanaryCanonical, 101);
        let variants = [
            Phase9PublicationContext {
                slice_id: "other".into(),
                ..base.clone()
            },
            Phase9PublicationContext {
                owner_id: "stale-owner".into(),
                ..base.clone()
            },
            Phase9PublicationContext {
                authority_revision: 1,
                ..base.clone()
            },
            Phase9PublicationContext {
                lease_epoch: 1,
                ..base.clone()
            },
            Phase9PublicationContext {
                partition_plan_epoch: 2,
                ..base.clone()
            },
            Phase9PublicationContext {
                shard_id: String::new(),
                ..base.clone()
            },
        ];
        for context in variants {
            assert!(fence.permits(&context, 2_000).is_err());
        }
    }

    #[test]
    fn watermark_advances_only_after_explicit_durable_commit() {
        let mut fence = Phase9AuthorityFence::default();
        fence
            .apply(record(2, 2, Phase9AuthorityState::RustCanary))
            .unwrap();
        let first = publication(SinkTarget::CanaryCanonical, 101);
        assert!(fence.permits(&first, 2_000).is_ok());
        assert!(fence.permits(&first, 2_000).is_ok());
        fence.commit(&first).unwrap();
        assert!(fence.permits(&first, 2_000).is_err());
        assert!(fence
            .permits(&publication(SinkTarget::CanaryCanonical, 100), 2_000)
            .is_err());
        let second = publication(SinkTarget::CanaryCanonical, 102);
        assert!(fence.permits(&second, 2_000).is_ok());
        fence.commit(&second).unwrap();
    }

    #[test]
    fn blocked_state_fences_every_target_and_requires_shadow_before_canary() {
        let mut fence = Phase9AuthorityFence::default();
        fence
            .apply(record(1, 1, Phase9AuthorityState::RustShadow))
            .unwrap();
        fence
            .apply(record(2, 1, Phase9AuthorityState::Blocked))
            .unwrap();
        let mut context = publication(SinkTarget::ShadowCanonical, 101);
        context.owner_id = OWNER.into();
        context.authority_revision = 2;
        context.lease_epoch = 1;
        assert!(fence.permits(&context, 2_000).is_err());
        assert!(fence
            .apply(record(3, 2, Phase9AuthorityState::RustCanary))
            .is_err());
        fence
            .apply(record(3, 2, Phase9AuthorityState::RustShadow))
            .unwrap();
    }

    #[test]
    fn malformed_records_and_stale_transitions_fail_closed() {
        let mut invalid = record(1, 1, Phase9AuthorityState::RustCanary);
        invalid.public_write_allowed = true;
        assert!(invalid.validate().is_err());
        let mut invalid = record(1, 1, Phase9AuthorityState::RustCanary);
        invalid.prerequisite_bundle_id = None;
        assert!(invalid.validate().is_err());
        let mut invalid = record(1, 1, Phase9AuthorityState::RustShadow);
        invalid.approved_by = Some("unexpected".into());
        assert!(invalid.validate().is_err());

        let mut fence = Phase9AuthorityFence::default();
        fence
            .apply(record(2, 2, Phase9AuthorityState::RustCanary))
            .unwrap();
        assert!(fence
            .apply(record(1, 2, Phase9AuthorityState::RustShadow))
            .is_err());
        let mut conflict = record(2, 2, Phase9AuthorityState::RustCanary);
        conflict.owner_id = "conflicting-owner".into();
        assert!(fence.apply(conflict).is_err());
        let mut stale_owner = record(3, 2, Phase9AuthorityState::RustShadow);
        stale_owner.owner_id = "next-owner".into();
        assert!(fence.apply(stale_owner).is_err());
        let mut wrong_candidate = record(3, 3, Phase9AuthorityState::RustShadow);
        wrong_candidate.candidate_digest = "2".repeat(64);
        assert!(fence.apply(wrong_candidate).is_err());
    }
}
