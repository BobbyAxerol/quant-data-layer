use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuthorityMode {
    RustShadow,
    RustCanary,
    RustPrimary,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SinkTarget {
    ShadowRaw,
    ShadowCanonical,
    ShadowQuarantine,
    CanaryCanonical,
    PrimaryRaw,
    PrimaryCanonical,
    PrimaryQuarantine,
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
                SinkTarget::ShadowRaw | SinkTarget::ShadowCanonical | SinkTarget::ShadowQuarantine
            ),
            AuthorityMode::RustCanary => matches!(
                context.target,
                SinkTarget::ShadowRaw
                    | SinkTarget::ShadowCanonical
                    | SinkTarget::ShadowQuarantine
                    | SinkTarget::CanaryCanonical
            ),
            // Rust primary owns the private canonical execution plane. Query
            // and stream project from it; V1 is a separate fallback route.
            AuthorityMode::RustPrimary => matches!(
                context.target,
                SinkTarget::PrimaryRaw
                    | SinkTarget::PrimaryCanonical
                    | SinkTarget::PrimaryQuarantine
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
                SinkTarget::ShadowRaw | SinkTarget::ShadowCanonical | SinkTarget::ShadowQuarantine
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Phase92AuthorityState {
    RustCanary,
    RustPrimary,
    Blocked,
    RollbackPending,
    PythonPrimary,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Phase92HandoffDirection {
    PythonToRust,
    RustToPython,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Phase92TerminalCheckpoint {
    pub schema: String,
    pub checkpoint_id: String,
    pub slice_id: String,
    pub owner_id: String,
    pub authority_revision: u64,
    pub lease_epoch: u64,
    pub partition_plan_epoch: u64,
    pub source_session_id: String,
    pub connection_generation: u64,
    pub terminal_watermark: u64,
    pub terminal_event_id: String,
    pub terminal_payload_sha256: String,
    pub candidate_digest: String,
    pub committed_at_ns: i64,
}

impl Phase92TerminalCheckpoint {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema != "qdl.terminal-owner-checkpoint.v1"
            || !valid_uuid(&self.checkpoint_id)
            || self.slice_id.trim().is_empty()
            || self.owner_id.trim().is_empty()
            || self.source_session_id.trim().is_empty()
            || self.terminal_event_id.trim().is_empty()
            || self.authority_revision == 0
            || self.lease_epoch == 0
            || self.partition_plan_epoch == 0
            || self.connection_generation == 0
            || self.committed_at_ns <= 0
            || !valid_digest(&self.terminal_payload_sha256, false)
            || !valid_digest(&self.candidate_digest, false)
        {
            return Err("Phase 9.2 terminal checkpoint is invalid".into());
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<String, String> {
        self.validate()?;
        let payload = serde_json::to_vec(self)
            .map_err(|error| format!("terminal checkpoint encoding failed: {error}"))?;
        use sha2::Digest;
        Ok(hex::encode(sha2::Sha256::digest(payload)))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Phase92AcceptedHandoff {
    pub schema: String,
    pub handoff_id: String,
    pub direction: Phase92HandoffDirection,
    pub checkpoint_digest: String,
    pub slice_id: String,
    pub old_owner_id: String,
    pub new_owner_id: String,
    pub expected_state: Phase92AuthorityState,
    pub new_state: Phase92AuthorityState,
    pub expected_authority_revision: u64,
    pub new_authority_revision: u64,
    pub expected_lease_epoch: u64,
    pub new_lease_epoch: u64,
    pub partition_plan_epoch: u64,
    pub terminal_watermark: u64,
    pub first_new_watermark: u64,
    pub overlap_start_watermark: u64,
    pub overlap_end_watermark: u64,
    pub old_event_count: u64,
    pub new_event_count: u64,
    pub semantic_mismatches: u64,
    pub open_gaps: u64,
    pub candidate_digest: String,
    pub prerequisite_bundle_id: String,
    pub approved_by: String,
    pub approved_at_ns: i64,
    pub expires_at_ns: i64,
}

impl Phase92AcceptedHandoff {
    pub fn validate(&self, checkpoint: &Phase92TerminalCheckpoint) -> Result<(), String> {
        checkpoint.validate()?;
        if self.schema != "qdl.accepted-authority-handoff.v1"
            || !valid_uuid(&self.handoff_id)
            || !valid_uuid(&self.prerequisite_bundle_id)
            || self.slice_id.trim().is_empty()
            || self.old_owner_id.trim().is_empty()
            || self.new_owner_id.trim().is_empty()
            || self.old_owner_id == self.new_owner_id
            || self.approved_by.trim().is_empty()
            || self.partition_plan_epoch == 0
            || !valid_digest(&self.checkpoint_digest, false)
            || !valid_digest(&self.candidate_digest, false)
            || self.approved_at_ns <= 0
            || self.expires_at_ns <= self.approved_at_ns
        {
            return Err("Phase 9.2 handoff identity/approval is invalid".into());
        }
        let states_match = match self.direction {
            Phase92HandoffDirection::PythonToRust => {
                self.expected_state == Phase92AuthorityState::RustCanary
                    && self.new_state == Phase92AuthorityState::RustPrimary
            }
            Phase92HandoffDirection::RustToPython => {
                self.expected_state == Phase92AuthorityState::RollbackPending
                    && self.new_state == Phase92AuthorityState::PythonPrimary
            }
        };
        if !states_match
            || self.new_authority_revision != self.expected_authority_revision + 1
            || self.new_lease_epoch <= self.expected_lease_epoch
            || self.first_new_watermark != self.terminal_watermark + 1
            || self.overlap_start_watermark > self.overlap_end_watermark
            || self.overlap_end_watermark != self.terminal_watermark
            || self.old_event_count == 0
            || self.old_event_count != self.new_event_count
            || self.semantic_mismatches != 0
            || self.open_gaps != 0
        {
            return Err("Phase 9.2 handoff boundary/reconciliation is invalid".into());
        }
        if self.checkpoint_digest != checkpoint.digest()?
            || self.slice_id != checkpoint.slice_id
            || self.old_owner_id != checkpoint.owner_id
            || self.expected_authority_revision != checkpoint.authority_revision
            || self.expected_lease_epoch != checkpoint.lease_epoch
            || self.partition_plan_epoch != checkpoint.partition_plan_epoch
            || self.terminal_watermark != checkpoint.terminal_watermark
            || self.candidate_digest != checkpoint.candidate_digest
        {
            return Err("Phase 9.2 handoff does not bind the terminal checkpoint".into());
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<String, String> {
        let payload = serde_json::to_vec(self)
            .map_err(|error| format!("accepted handoff encoding failed: {error}"))?;
        use sha2::Digest;
        Ok(hex::encode(sha2::Sha256::digest(payload)))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Phase92AuthorityRecord {
    pub schema: String,
    pub slice_id: String,
    pub state: Phase92AuthorityState,
    pub owner_id: String,
    pub authority_revision: u64,
    pub lease_epoch: u64,
    pub partition_plan_epoch: u64,
    pub candidate_digest: String,
    pub prerequisite_bundle_id: Option<String>,
    pub start_watermark: u64,
    pub terminal_watermark: Option<u64>,
    pub previous_owner_id: Option<String>,
    pub handoff_digest: Option<String>,
    pub approved_by: Option<String>,
    pub approved_at_ns: Option<i64>,
    pub hold_until_ns: Option<i64>,
    pub public_write_allowed: bool,
    pub legacy_write_allowed: bool,
}

impl Phase92AuthorityRecord {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema != "qdl.authority-record.v3"
            || self.slice_id.trim().is_empty()
            || self.owner_id.trim().is_empty()
            || self.authority_revision == 0
            || self.lease_epoch == 0
            || self.partition_plan_epoch == 0
            || !valid_digest(&self.candidate_digest, false)
        {
            return Err("Phase 9.2 authority identity/epoch is invalid".into());
        }
        let approval_valid = || {
            self.approved_by
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
                && self.approved_at_ns.is_some_and(|value| value > 0)
                && self
                    .hold_until_ns
                    .is_some_and(|hold| self.approved_at_ns.is_some_and(|approved| hold > approved))
        };
        match self.state {
            Phase92AuthorityState::RustCanary => {
                if self.public_write_allowed
                    || self.legacy_write_allowed
                    || self.terminal_watermark.is_some()
                    || self.previous_owner_id.is_some()
                    || self.handoff_digest.is_some()
                    || !self
                        .prerequisite_bundle_id
                        .as_deref()
                        .is_some_and(valid_uuid)
                    || !approval_valid()
                {
                    return Err("Phase 9.2 canary authority is invalid".into());
                }
            }
            Phase92AuthorityState::RustPrimary | Phase92AuthorityState::PythonPrimary => {
                if !self.public_write_allowed
                    || !self.legacy_write_allowed
                    || self
                        .previous_owner_id
                        .as_deref()
                        .is_none_or(|value| value.trim().is_empty() || value == self.owner_id)
                    || self.terminal_watermark != Some(self.start_watermark)
                    || !self
                        .handoff_digest
                        .as_deref()
                        .is_some_and(|value| valid_digest(value, false))
                    || !approval_valid()
                {
                    return Err("Phase 9.2 primary authority/handoff is invalid".into());
                }
                if self.state == Phase92AuthorityState::RustPrimary
                    && !self
                        .prerequisite_bundle_id
                        .as_deref()
                        .is_some_and(valid_uuid)
                {
                    return Err("Rust primary requires a prerequisite bundle".into());
                }
            }
            Phase92AuthorityState::Blocked | Phase92AuthorityState::RollbackPending => {
                if self.public_write_allowed || self.legacy_write_allowed {
                    return Err("blocked/rollback authority cannot write".into());
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Phase92AuthorityControlEvent {
    pub schema: String,
    pub event_id: String,
    pub slice_id: String,
    pub authority_revision: u64,
    pub database_state: String,
    pub authority: Option<Phase92AuthorityRecord>,
    pub checkpoint: Option<Phase92TerminalCheckpoint>,
    pub handoff: Option<Phase92AcceptedHandoff>,
}

impl Phase92AuthorityControlEvent {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema != "qdl.authority-control-event.v1"
            || !valid_uuid(&self.event_id)
            || self.slice_id.trim().is_empty()
            || self.authority_revision == 0
            || self.database_state.trim().is_empty()
        {
            return Err("Phase 9.2 authority control event identity is invalid".into());
        }
        let Some(authority) = &self.authority else {
            if self.checkpoint.is_some() || self.handoff.is_some() {
                return Err("non-writable authority event cannot carry handoff evidence".into());
            }
            return Ok(());
        };
        authority.validate()?;
        if authority.slice_id != self.slice_id
            || authority.authority_revision != self.authority_revision
        {
            return Err("authority control event and authority record differ".into());
        }
        let primary = matches!(
            authority.state,
            Phase92AuthorityState::RustPrimary | Phase92AuthorityState::PythonPrimary
        );
        if primary {
            let checkpoint = self
                .checkpoint
                .as_ref()
                .ok_or_else(|| "primary authority control event needs checkpoint".to_owned())?;
            let handoff = self
                .handoff
                .as_ref()
                .ok_or_else(|| "primary authority control event needs handoff".to_owned())?;
            handoff.validate(checkpoint)?;
            let handoff_digest = handoff.digest()?;
            if authority.owner_id != handoff.new_owner_id
                || authority.previous_owner_id.as_deref() != Some(handoff.old_owner_id.as_str())
                || authority.authority_revision != handoff.new_authority_revision
                || authority.lease_epoch != handoff.new_lease_epoch
                || authority.partition_plan_epoch != handoff.partition_plan_epoch
                || authority.start_watermark != handoff.terminal_watermark
                || authority.terminal_watermark != Some(handoff.terminal_watermark)
                || authority.handoff_digest.as_deref() != Some(handoff_digest.as_str())
                || authority.approved_by.as_deref() != Some(handoff.approved_by.as_str())
                || authority.approved_at_ns != Some(handoff.approved_at_ns)
                || authority.hold_until_ns != Some(handoff.expires_at_ns)
            {
                return Err("primary authority record does not bind accepted handoff".into());
            }
        } else if self.checkpoint.is_some() || self.handoff.is_some() {
            return Err("non-primary authority event cannot carry handoff evidence".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Phase92PublicationContext {
    pub slice_id: String,
    pub owner_id: String,
    pub authority_revision: u64,
    pub shard_id: String,
    pub lease_epoch: u64,
    pub partition_plan_epoch: u64,
    pub source_watermark: u64,
    pub target: SinkTarget,
}

#[derive(Clone, Default)]
pub struct Phase92AuthorityFence {
    current: Option<Phase92AuthorityRecord>,
    committed_watermarks: HashMap<(String, SinkTarget), u64>,
    recovery_required: bool,
}

impl Phase92AuthorityFence {
    pub fn apply(&mut self, record: Phase92AuthorityRecord) -> Result<(), String> {
        record.validate()?;
        if let Some(current) = &self.current {
            if record.authority_revision == current.authority_revision {
                return if record == *current {
                    Ok(())
                } else {
                    Err("conflicting Phase 9.2 authority at the same revision".into())
                };
            }
        } else {
            self.recovery_required = matches!(
                record.state,
                Phase92AuthorityState::RustPrimary | Phase92AuthorityState::PythonPrimary
            );
            self.current = Some(record);
            return Ok(());
        }
        if matches!(
            record.state,
            Phase92AuthorityState::RustPrimary | Phase92AuthorityState::PythonPrimary
        ) {
            return Err("primary ownership transition requires accepted handoff".into());
        }
        self.apply_transition(record)
    }

    pub fn apply_control_event(
        &mut self,
        event: &Phase92AuthorityControlEvent,
        now_ns: i64,
    ) -> Result<(), String> {
        event.validate()?;
        let Some(record) = event.authority.clone() else {
            return Ok(());
        };
        if self
            .current
            .as_ref()
            .is_some_and(|current| current == &record)
        {
            return Ok(());
        }
        let primary = matches!(
            record.state,
            Phase92AuthorityState::RustPrimary | Phase92AuthorityState::PythonPrimary
        );
        if primary && self.current.is_some() {
            let checkpoint = event
                .checkpoint
                .as_ref()
                .ok_or_else(|| "primary authority event needs checkpoint".to_owned())?;
            let handoff = event
                .handoff
                .as_ref()
                .ok_or_else(|| "primary authority event needs handoff".to_owned())?;
            self.apply_handoff(checkpoint, handoff, record, now_ns)
        } else {
            self.apply(record)
        }
    }

    pub fn apply_handoff(
        &mut self,
        checkpoint: &Phase92TerminalCheckpoint,
        handoff: &Phase92AcceptedHandoff,
        record: Phase92AuthorityRecord,
        now_ns: i64,
    ) -> Result<(), String> {
        handoff.validate(checkpoint)?;
        record.validate()?;
        if now_ns <= 0 || now_ns < handoff.approved_at_ns || now_ns >= handoff.expires_at_ns {
            return Err("Phase 9.2 handoff approval window is not active".into());
        }
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| "Phase 9.2 authority record is not loaded".to_owned())?;
        if current.slice_id != handoff.slice_id
            || current.owner_id != handoff.old_owner_id
            || current.state != handoff.expected_state
            || current.authority_revision != handoff.expected_authority_revision
            || current.lease_epoch != handoff.expected_lease_epoch
            || current.partition_plan_epoch != handoff.partition_plan_epoch
            || current.candidate_digest != handoff.candidate_digest
            || record.slice_id != handoff.slice_id
            || record.owner_id != handoff.new_owner_id
            || record.state != handoff.new_state
            || record.authority_revision != handoff.new_authority_revision
            || record.lease_epoch != handoff.new_lease_epoch
            || record.partition_plan_epoch != handoff.partition_plan_epoch
            || record.candidate_digest != handoff.candidate_digest
            || record.start_watermark != handoff.terminal_watermark
            || record.terminal_watermark != Some(handoff.terminal_watermark)
            || record.previous_owner_id.as_deref() != Some(handoff.old_owner_id.as_str())
            || record.handoff_digest.as_deref() != Some(handoff.digest()?.as_str())
            || record.approved_by.as_deref() != Some(handoff.approved_by.as_str())
            || record.approved_at_ns != Some(handoff.approved_at_ns)
            || record.hold_until_ns != Some(handoff.expires_at_ns)
        {
            return Err("Phase 9.2 authority CAS/handoff binding failed".into());
        }
        self.apply_transition(record)
    }

    fn apply_transition(&mut self, record: Phase92AuthorityRecord) -> Result<(), String> {
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| "Phase 9.2 authority record is not loaded".to_owned())?;
        if record.slice_id != current.slice_id
            || record.candidate_digest != current.candidate_digest
            || record.partition_plan_epoch != current.partition_plan_epoch
            || record.authority_revision != current.authority_revision + 1
            || record.lease_epoch < current.lease_epoch
            || (record.owner_id != current.owner_id && record.lease_epoch <= current.lease_epoch)
        {
            return Err("Phase 9.2 authority compare-and-swap failed".into());
        }
        let allowed = match current.state {
            Phase92AuthorityState::RustCanary => matches!(
                record.state,
                Phase92AuthorityState::RustPrimary | Phase92AuthorityState::Blocked
            ),
            Phase92AuthorityState::RustPrimary => matches!(
                record.state,
                Phase92AuthorityState::Blocked | Phase92AuthorityState::RollbackPending
            ),
            Phase92AuthorityState::Blocked => {
                record.state == Phase92AuthorityState::RollbackPending
            }
            Phase92AuthorityState::RollbackPending => {
                record.state == Phase92AuthorityState::PythonPrimary
            }
            Phase92AuthorityState::PythonPrimary => record.state == Phase92AuthorityState::Blocked,
        };
        if !allowed {
            return Err("Phase 9.2 authority transition is not permitted".into());
        }
        self.current = Some(record);
        self.recovery_required = false;
        Ok(())
    }

    pub fn restore_committed_watermark(
        &mut self,
        context: &Phase92PublicationContext,
    ) -> Result<(), String> {
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| "Phase 9.2 authority record is not loaded".to_owned())?;
        let restoring_canary = current.state == Phase92AuthorityState::RustCanary
            && context.target == SinkTarget::CanaryCanonical;
        let restoring_primary = matches!(
            current.state,
            Phase92AuthorityState::RustPrimary | Phase92AuthorityState::PythonPrimary
        ) && matches!(
            context.target,
            SinkTarget::PrimaryCanonical | SinkTarget::PublicV2 | SinkTarget::LegacyV1
        );
        if !(restoring_canary || (self.recovery_required && restoring_primary))
            || context.slice_id != current.slice_id
            || context.owner_id != current.owner_id
            || context.authority_revision != current.authority_revision
            || context.lease_epoch != current.lease_epoch
            || context.partition_plan_epoch != current.partition_plan_epoch
            || context.shard_id.trim().is_empty()
            || context.source_watermark < current.start_watermark
        {
            return Err("Phase 9.2 recovered watermark identity is invalid".into());
        }
        let key = (context.shard_id.clone(), context.target);
        if self
            .committed_watermarks
            .get(&key)
            .is_some_and(|value| context.source_watermark < *value)
        {
            return Err("Phase 9.2 recovered watermark regressed".into());
        }
        self.committed_watermarks
            .insert(key, context.source_watermark);
        Ok(())
    }

    pub fn permits(&self, context: &Phase92PublicationContext, now_ns: i64) -> Result<(), String> {
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| "Phase 9.2 authority record is not loaded".to_owned())?;
        if now_ns <= 0
            || context.slice_id != current.slice_id
            || context.owner_id != current.owner_id
            || context.authority_revision != current.authority_revision
            || context.lease_epoch != current.lease_epoch
            || context.partition_plan_epoch != current.partition_plan_epoch
            || context.shard_id.trim().is_empty()
        {
            return Err("publication identity does not match Phase 9.2 authority".into());
        }
        let target_allowed = match current.state {
            Phase92AuthorityState::RustCanary => context.target == SinkTarget::CanaryCanonical,
            Phase92AuthorityState::RustPrimary | Phase92AuthorityState::PythonPrimary => matches!(
                context.target,
                SinkTarget::PrimaryCanonical | SinkTarget::PublicV2 | SinkTarget::LegacyV1
            ),
            Phase92AuthorityState::Blocked | Phase92AuthorityState::RollbackPending => false,
        };
        if !target_allowed {
            return Err("sink target is not permitted by Phase 9.2 authority".into());
        }
        if current.state == Phase92AuthorityState::RustCanary {
            let approved_at = current
                .approved_at_ns
                .ok_or_else(|| "authority approval is missing".to_owned())?;
            let hold_until = current
                .hold_until_ns
                .ok_or_else(|| "authority hold window is missing".to_owned())?;
            if now_ns < approved_at || now_ns >= hold_until {
                return Err("Phase 9.2 authority approval window is not active".into());
            }
        }
        let key = (context.shard_id.clone(), context.target);
        if self.recovery_required && !self.committed_watermarks.contains_key(&key) {
            return Err("Phase 9.2 durable target watermark recovery is required".into());
        }
        let committed = self
            .committed_watermarks
            .get(&key)
            .copied()
            .unwrap_or(current.start_watermark);
        if context.source_watermark != committed + 1 {
            return Err("Phase 9.2 source watermark is duplicate, stale or gapped".into());
        }
        Ok(())
    }

    pub fn commit(&mut self, context: &Phase92PublicationContext) -> Result<(), String> {
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| "Phase 9.2 authority record is not loaded".to_owned())?;
        if context.slice_id != current.slice_id
            || context.owner_id != current.owner_id
            || context.authority_revision != current.authority_revision
            || context.lease_epoch != current.lease_epoch
            || context.partition_plan_epoch != current.partition_plan_epoch
        {
            return Err("Phase 9.2 authority changed before commit".into());
        }
        let key = (context.shard_id.clone(), context.target);
        let committed = self
            .committed_watermarks
            .get(&key)
            .copied()
            .unwrap_or(current.start_watermark);
        if context.source_watermark != committed + 1 {
            return Err("Phase 9.2 watermark commit is not contiguous".into());
        }
        self.committed_watermarks
            .insert(key, context.source_watermark);
        Ok(())
    }

    pub fn next_watermark(&self, shard_id: &str, target: SinkTarget) -> Result<u64, String> {
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| "Phase 9.2 authority record is not loaded".to_owned())?;
        if shard_id.trim().is_empty() {
            return Err("Phase 9.2 shard identity is empty".into());
        }
        self.committed_watermarks
            .get(&(shard_id.to_owned(), target))
            .copied()
            .unwrap_or(current.start_watermark)
            .checked_add(1)
            .ok_or_else(|| "Phase 9.2 watermark overflow".to_owned())
    }

    pub fn current(&self) -> Option<&Phase92AuthorityRecord> {
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
    fn authority_modes_bind_exact_private_targets() {
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
        fence.apply(record(4, AuthorityMode::RustPrimary)).unwrap();
        for target in [
            SinkTarget::PrimaryRaw,
            SinkTarget::PrimaryCanonical,
            SinkTarget::PrimaryQuarantine,
        ] {
            assert!(fence.permits(&publication(4, 4, target)).is_ok());
        }
        assert!(fence
            .permits(&publication(4, 4, SinkTarget::ShadowCanonical))
            .is_err());
        assert!(fence
            .permits(&publication(4, 4, SinkTarget::PublicV2))
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

#[cfg(test)]
mod phase92_tests {
    use super::{
        Phase92AcceptedHandoff, Phase92AuthorityFence, Phase92AuthorityRecord,
        Phase92AuthorityState, Phase92HandoffDirection, Phase92PublicationContext,
        Phase92TerminalCheckpoint, SinkTarget,
    };

    const SLICE: &str = "production/binance/usdm/perpetual/trade/plan-1/btcusdt";
    const BUNDLE: &str = "558042db-a766-5a55-b5b3-4b508d649df9";

    fn canary() -> Phase92AuthorityRecord {
        Phase92AuthorityRecord {
            schema: "qdl.authority-record.v3".into(),
            slice_id: SLICE.into(),
            state: Phase92AuthorityState::RustCanary,
            owner_id: "python-primary".into(),
            authority_revision: 7,
            lease_epoch: 11,
            partition_plan_epoch: 1,
            candidate_digest: "1".repeat(64),
            prerequisite_bundle_id: Some(BUNDLE.into()),
            start_watermark: 89,
            terminal_watermark: None,
            previous_owner_id: None,
            handoff_digest: None,
            approved_by: Some("phase92-operator".into()),
            approved_at_ns: Some(1),
            hold_until_ns: Some(10_000),
            public_write_allowed: false,
            legacy_write_allowed: false,
        }
    }

    fn checkpoint(
        owner: &str,
        revision: u64,
        lease: u64,
        watermark: u64,
    ) -> Phase92TerminalCheckpoint {
        Phase92TerminalCheckpoint {
            schema: "qdl.terminal-owner-checkpoint.v1".into(),
            checkpoint_id: "11111111-1111-4111-8111-111111111111".into(),
            slice_id: SLICE.into(),
            owner_id: owner.into(),
            authority_revision: revision,
            lease_epoch: lease,
            partition_plan_epoch: 1,
            source_session_id: "session-1".into(),
            connection_generation: 1,
            terminal_watermark: watermark,
            terminal_event_id: format!("event-{watermark}"),
            terminal_payload_sha256: "2".repeat(64),
            candidate_digest: "1".repeat(64),
            committed_at_ns: 1,
        }
    }

    fn handoff(
        checkpoint: &Phase92TerminalCheckpoint,
        direction: Phase92HandoffDirection,
        new_owner: &str,
        new_state: Phase92AuthorityState,
    ) -> Phase92AcceptedHandoff {
        Phase92AcceptedHandoff {
            schema: "qdl.accepted-authority-handoff.v1".into(),
            handoff_id: "22222222-2222-4222-8222-222222222222".into(),
            direction,
            checkpoint_digest: checkpoint.digest().unwrap(),
            slice_id: SLICE.into(),
            old_owner_id: checkpoint.owner_id.clone(),
            new_owner_id: new_owner.into(),
            expected_state: if direction == Phase92HandoffDirection::PythonToRust {
                Phase92AuthorityState::RustCanary
            } else {
                Phase92AuthorityState::RollbackPending
            },
            new_state,
            expected_authority_revision: checkpoint.authority_revision,
            new_authority_revision: checkpoint.authority_revision + 1,
            expected_lease_epoch: checkpoint.lease_epoch,
            new_lease_epoch: checkpoint.lease_epoch + 1,
            partition_plan_epoch: 1,
            terminal_watermark: checkpoint.terminal_watermark,
            first_new_watermark: checkpoint.terminal_watermark + 1,
            overlap_start_watermark: checkpoint.terminal_watermark - 10,
            overlap_end_watermark: checkpoint.terminal_watermark,
            old_event_count: 11,
            new_event_count: 11,
            semantic_mismatches: 0,
            open_gaps: 0,
            candidate_digest: "1".repeat(64),
            prerequisite_bundle_id: BUNDLE.into(),
            approved_by: "phase92-operator".into(),
            approved_at_ns: 1,
            expires_at_ns: 10_000,
        }
    }

    fn primary(
        checkpoint: &Phase92TerminalCheckpoint,
        handoff: &Phase92AcceptedHandoff,
    ) -> Phase92AuthorityRecord {
        Phase92AuthorityRecord {
            schema: "qdl.authority-record.v3".into(),
            slice_id: SLICE.into(),
            state: handoff.new_state,
            owner_id: handoff.new_owner_id.clone(),
            authority_revision: handoff.new_authority_revision,
            lease_epoch: handoff.new_lease_epoch,
            partition_plan_epoch: 1,
            candidate_digest: "1".repeat(64),
            prerequisite_bundle_id: (handoff.new_state == Phase92AuthorityState::RustPrimary)
                .then(|| BUNDLE.into()),
            start_watermark: checkpoint.terminal_watermark,
            terminal_watermark: Some(checkpoint.terminal_watermark),
            previous_owner_id: Some(checkpoint.owner_id.clone()),
            handoff_digest: Some(handoff.digest().unwrap()),
            approved_by: Some("phase92-operator".into()),
            approved_at_ns: Some(1),
            hold_until_ns: Some(10_000),
            public_write_allowed: true,
            legacy_write_allowed: true,
        }
    }

    fn publication(
        record: &Phase92AuthorityRecord,
        target: SinkTarget,
        watermark: u64,
    ) -> Phase92PublicationContext {
        Phase92PublicationContext {
            slice_id: SLICE.into(),
            owner_id: record.owner_id.clone(),
            authority_revision: record.authority_revision,
            shard_id: "binance-usdm-trade-0".into(),
            lease_epoch: record.lease_epoch,
            partition_plan_epoch: 1,
            source_watermark: watermark,
            target,
        }
    }

    #[test]
    fn accepted_handoff_is_required_and_first_primary_watermark_is_terminal_plus_one() {
        let current = canary();
        let checkpoint = checkpoint("python-primary", 7, 11, 100);
        let handoff = handoff(
            &checkpoint,
            Phase92HandoffDirection::PythonToRust,
            "rust-primary",
            Phase92AuthorityState::RustPrimary,
        );
        let primary = primary(&checkpoint, &handoff);
        let mut fence = Phase92AuthorityFence::default();
        fence.apply(current).unwrap();
        assert!(fence.apply(primary.clone()).is_err());
        fence
            .apply_handoff(&checkpoint, &handoff, primary.clone(), 2)
            .unwrap();
        assert!(fence
            .restore_committed_watermark(&publication(&primary, SinkTarget::PrimaryCanonical, 120,))
            .is_err());

        for target in [
            SinkTarget::PrimaryCanonical,
            SinkTarget::PublicV2,
            SinkTarget::LegacyV1,
        ] {
            assert!(fence
                .permits(&publication(&primary, target, 100), 2)
                .is_err());
            assert!(fence
                .permits(&publication(&primary, target, 102), 2)
                .is_err());
            let first = publication(&primary, target, 101);
            fence.permits(&first, 2).unwrap();
            fence.commit(&first).unwrap();
            assert!(fence.permits(&first, 2).is_err());
        }
    }

    #[test]
    fn final_sink_and_projector_watermarks_are_independent_and_gap_free() {
        let checkpoint = checkpoint("python-primary", 7, 11, 100);
        let handoff = handoff(
            &checkpoint,
            Phase92HandoffDirection::PythonToRust,
            "rust-primary",
            Phase92AuthorityState::RustPrimary,
        );
        let primary = primary(&checkpoint, &handoff);
        let mut fence = Phase92AuthorityFence::default();
        fence.apply(canary()).unwrap();
        fence
            .apply_handoff(&checkpoint, &handoff, primary.clone(), 2)
            .unwrap();

        let canonical = publication(&primary, SinkTarget::PrimaryCanonical, 101);
        fence.permits(&canonical, 2).unwrap();
        fence.commit(&canonical).unwrap();
        assert!(fence
            .permits(&publication(&primary, SinkTarget::PrimaryCanonical, 102), 2)
            .is_ok());
        assert!(fence
            .permits(&publication(&primary, SinkTarget::PublicV2, 101), 2)
            .is_ok());
        assert!(fence
            .permits(&publication(&primary, SinkTarget::LegacyV1, 101), 2)
            .is_ok());
    }

    #[test]
    fn stale_owner_revision_lease_plan_and_wrong_target_fail_closed() {
        let checkpoint = checkpoint("python-primary", 7, 11, 100);
        let handoff = handoff(
            &checkpoint,
            Phase92HandoffDirection::PythonToRust,
            "rust-primary",
            Phase92AuthorityState::RustPrimary,
        );
        let primary = primary(&checkpoint, &handoff);
        let mut fence = Phase92AuthorityFence::default();
        fence.apply(canary()).unwrap();
        fence
            .apply_handoff(&checkpoint, &handoff, primary.clone(), 2)
            .unwrap();
        let base = publication(&primary, SinkTarget::PrimaryCanonical, 101);
        let variants = [
            Phase92PublicationContext {
                owner_id: "python-primary".into(),
                ..base.clone()
            },
            Phase92PublicationContext {
                authority_revision: 7,
                ..base.clone()
            },
            Phase92PublicationContext {
                lease_epoch: 11,
                ..base.clone()
            },
            Phase92PublicationContext {
                partition_plan_epoch: 2,
                ..base.clone()
            },
            Phase92PublicationContext {
                target: SinkTarget::CanaryCanonical,
                ..base
            },
        ];
        for value in variants {
            assert!(fence.permits(&value, 2).is_err());
        }
    }

    #[test]
    fn formal_rollback_fences_rust_and_hands_off_to_python_with_new_epoch() {
        let initial_checkpoint = checkpoint("python-primary", 7, 11, 100);
        let to_rust = handoff(
            &initial_checkpoint,
            Phase92HandoffDirection::PythonToRust,
            "rust-primary",
            Phase92AuthorityState::RustPrimary,
        );
        let rust_primary = primary(&initial_checkpoint, &to_rust);
        let mut fence = Phase92AuthorityFence::default();
        fence.apply(canary()).unwrap();
        fence
            .apply_handoff(&initial_checkpoint, &to_rust, rust_primary.clone(), 2)
            .unwrap();

        let mut blocked = rust_primary.clone();
        blocked.state = Phase92AuthorityState::Blocked;
        blocked.authority_revision += 1;
        blocked.public_write_allowed = false;
        blocked.legacy_write_allowed = false;
        fence.apply(blocked.clone()).unwrap();
        let mut pending = blocked;
        pending.state = Phase92AuthorityState::RollbackPending;
        pending.authority_revision += 1;
        fence.apply(pending.clone()).unwrap();

        let rollback_checkpoint = checkpoint(
            "rust-primary",
            pending.authority_revision,
            pending.lease_epoch,
            120,
        );
        let to_python = handoff(
            &rollback_checkpoint,
            Phase92HandoffDirection::RustToPython,
            "python-rollback",
            Phase92AuthorityState::PythonPrimary,
        );
        let python_primary = primary(&rollback_checkpoint, &to_python);
        fence
            .apply_handoff(&rollback_checkpoint, &to_python, python_primary.clone(), 2)
            .unwrap();
        assert!(fence
            .permits(
                &publication(&rust_primary, SinkTarget::PrimaryCanonical, 101),
                2,
            )
            .is_err());
        assert!(fence
            .permits(
                &publication(&python_primary, SinkTarget::PrimaryCanonical, 121),
                2,
            )
            .is_ok());
    }

    #[test]
    fn crash_before_cas_reconstructs_canary_and_accepts_only_exact_handoff() {
        let persisted_canary = canary();
        let terminal = checkpoint("python-primary", 7, 11, 100);
        let accepted = handoff(
            &terminal,
            Phase92HandoffDirection::PythonToRust,
            "rust-primary",
            Phase92AuthorityState::RustPrimary,
        );
        let rust_primary = primary(&terminal, &accepted);

        let mut recovered = Phase92AuthorityFence::default();
        recovered.apply(persisted_canary).unwrap();
        recovered
            .apply_handoff(&terminal, &accepted, rust_primary.clone(), 2)
            .unwrap();
        assert_eq!(recovered.current(), Some(&rust_primary));
        assert!(recovered
            .apply_handoff(&terminal, &accepted, rust_primary, 2)
            .is_err());
    }

    #[test]
    fn restarted_primary_fails_closed_until_each_target_watermark_is_restored() {
        let initial_checkpoint = checkpoint("python-primary", 7, 11, 100);
        let to_rust = handoff(
            &initial_checkpoint,
            Phase92HandoffDirection::PythonToRust,
            "rust-primary",
            Phase92AuthorityState::RustPrimary,
        );
        let rust_primary = primary(&initial_checkpoint, &to_rust);
        let mut recovered = Phase92AuthorityFence::default();
        recovered.apply(rust_primary.clone()).unwrap();
        let duplicate = publication(&rust_primary, SinkTarget::PrimaryCanonical, 120);
        assert!(recovered.permits(&duplicate, 2).is_err());
        recovered.restore_committed_watermark(&duplicate).unwrap();
        assert!(recovered.permits(&duplicate, 2).is_err());
        assert!(recovered
            .permits(
                &publication(&rust_primary, SinkTarget::PrimaryCanonical, 121),
                2,
            )
            .is_ok());
        assert!(recovered
            .permits(&publication(&rust_primary, SinkTarget::PublicV2, 121), 2)
            .is_err());
    }

    #[test]
    fn canary_and_handoff_windows_expire_but_accepted_primary_persists() {
        let canary_record = canary();
        let canary_publication = publication(&canary_record, SinkTarget::CanaryCanonical, 90);
        let mut canary_fence = Phase92AuthorityFence::default();
        canary_fence.apply(canary_record.clone()).unwrap();
        assert!(canary_fence.permits(&canary_publication, 9_999).is_ok());
        assert!(canary_fence
            .permits(&canary_publication, 10_000)
            .is_err_and(|error| error.contains("approval window is not active")));

        let terminal = checkpoint("python-primary", 7, 11, 100);
        let accepted = handoff(
            &terminal,
            Phase92HandoffDirection::PythonToRust,
            "rust-primary",
            Phase92AuthorityState::RustPrimary,
        );
        let rust_primary = primary(&terminal, &accepted);
        assert!(canary_fence
            .apply_handoff(&terminal, &accepted, rust_primary.clone(), 10_000)
            .is_err_and(|error| error.contains("handoff approval window is not active")));

        let mut accepted_fence = Phase92AuthorityFence::default();
        accepted_fence.apply(canary_record).unwrap();
        accepted_fence
            .apply_handoff(&terminal, &accepted, rust_primary.clone(), 2)
            .unwrap();
        assert!(accepted_fence
            .permits(
                &publication(&rust_primary, SinkTarget::PrimaryCanonical, 101),
                20_000,
            )
            .is_ok());
    }

    #[test]
    fn restarted_primary_recovers_after_historical_hold_expiry_and_newer_revision_fences_it() {
        let terminal = checkpoint("python-primary", 7, 11, 100);
        let accepted = handoff(
            &terminal,
            Phase92HandoffDirection::PythonToRust,
            "rust-primary",
            Phase92AuthorityState::RustPrimary,
        );
        let rust_primary = primary(&terminal, &accepted);
        let mut recovered = Phase92AuthorityFence::default();
        recovered.apply(rust_primary.clone()).unwrap();

        for target in [
            SinkTarget::PrimaryCanonical,
            SinkTarget::PublicV2,
            SinkTarget::LegacyV1,
        ] {
            let restored = publication(&rust_primary, target, 120);
            assert!(recovered
                .permits(&restored, 20_000)
                .is_err_and(|error| error.contains("recovery is required")));
            recovered.restore_committed_watermark(&restored).unwrap();
            assert!(recovered
                .permits(&publication(&rust_primary, target, 121), 20_000)
                .is_ok());
        }

        let stale_primary_context = publication(&rust_primary, SinkTarget::PrimaryCanonical, 121);
        let mut blocked = rust_primary.clone();
        blocked.state = Phase92AuthorityState::Blocked;
        blocked.authority_revision += 1;
        blocked.public_write_allowed = false;
        blocked.legacy_write_allowed = false;
        recovered.apply(blocked.clone()).unwrap();
        assert!(recovered.permits(&stale_primary_context, 20_000).is_err());

        let mut rollback_pending = blocked;
        rollback_pending.state = Phase92AuthorityState::RollbackPending;
        rollback_pending.authority_revision += 1;
        recovered.apply(rollback_pending.clone()).unwrap();
        assert!(recovered
            .permits(
                &publication(
                    &rollback_pending,
                    SinkTarget::PrimaryCanonical,
                    rollback_pending.start_watermark + 1,
                ),
                20_000,
            )
            .is_err_and(|error| error.contains("sink target is not permitted")));
    }

    #[test]
    fn python_rollback_primary_persists_after_historical_hold_expiry() {
        let initial_checkpoint = checkpoint("python-primary", 7, 11, 100);
        let to_rust = handoff(
            &initial_checkpoint,
            Phase92HandoffDirection::PythonToRust,
            "rust-primary",
            Phase92AuthorityState::RustPrimary,
        );
        let rust_primary = primary(&initial_checkpoint, &to_rust);
        let mut fence = Phase92AuthorityFence::default();
        fence.apply(canary()).unwrap();
        fence
            .apply_handoff(&initial_checkpoint, &to_rust, rust_primary.clone(), 2)
            .unwrap();

        let mut blocked = rust_primary.clone();
        blocked.state = Phase92AuthorityState::Blocked;
        blocked.authority_revision += 1;
        blocked.public_write_allowed = false;
        blocked.legacy_write_allowed = false;
        fence.apply(blocked.clone()).unwrap();
        let mut pending = blocked;
        pending.state = Phase92AuthorityState::RollbackPending;
        pending.authority_revision += 1;
        fence.apply(pending.clone()).unwrap();

        let rollback_checkpoint = checkpoint(
            "rust-primary",
            pending.authority_revision,
            pending.lease_epoch,
            120,
        );
        let to_python = handoff(
            &rollback_checkpoint,
            Phase92HandoffDirection::RustToPython,
            "python-rollback",
            Phase92AuthorityState::PythonPrimary,
        );
        let python_primary = primary(&rollback_checkpoint, &to_python);
        fence
            .apply_handoff(&rollback_checkpoint, &to_python, python_primary.clone(), 2)
            .unwrap();
        assert!(fence
            .permits(
                &publication(&python_primary, SinkTarget::PrimaryCanonical, 121),
                20_000,
            )
            .is_ok());
    }

    #[test]
    fn primary_authority_must_bind_the_exact_handoff_approval() {
        let terminal = checkpoint("python-primary", 7, 11, 100);
        let accepted = handoff(
            &terminal,
            Phase92HandoffDirection::PythonToRust,
            "rust-primary",
            Phase92AuthorityState::RustPrimary,
        );
        let mut mismatched_primary = primary(&terminal, &accepted);
        mismatched_primary.hold_until_ns = Some(9_999);
        let mut fence = Phase92AuthorityFence::default();
        fence.apply(canary()).unwrap();
        assert!(fence
            .apply_handoff(&terminal, &accepted, mismatched_primary, 2)
            .is_err_and(|error| error.contains("CAS/handoff binding failed")));
    }

    #[test]
    fn dirty_or_off_by_one_handoff_is_rejected() {
        let checkpoint = checkpoint("python-primary", 7, 11, 100);
        let mut dirty = handoff(
            &checkpoint,
            Phase92HandoffDirection::PythonToRust,
            "rust-primary",
            Phase92AuthorityState::RustPrimary,
        );
        dirty.semantic_mismatches = 1;
        assert!(dirty.validate(&checkpoint).is_err());
        dirty.semantic_mismatches = 0;
        dirty.first_new_watermark = 102;
        assert!(dirty.validate(&checkpoint).is_err());
    }
}

#[cfg(test)]
mod phase92_control_event_tests {
    use super::{
        Phase92AuthorityControlEvent, Phase92AuthorityFence, Phase92AuthorityState,
        Phase92PublicationContext, SinkTarget,
    };

    const EVENT_JSON: &str =
        include_str!("../../../tests/fixtures/phase9/authority-control-primary.json");
    const ACTIVE_NOW_NS: i64 = 1_787_218_200_000_000_000;

    fn publication(
        event: &Phase92AuthorityControlEvent,
        target: SinkTarget,
        watermark: u64,
    ) -> Phase92PublicationContext {
        let authority = event.authority.as_ref().expect("fixture authority");
        Phase92PublicationContext {
            slice_id: authority.slice_id.clone(),
            owner_id: authority.owner_id.clone(),
            authority_revision: authority.authority_revision,
            shard_id: "binance-usdm-trade-ethusdt-0".into(),
            lease_epoch: authority.lease_epoch,
            partition_plan_epoch: authority.partition_plan_epoch,
            source_watermark: watermark,
            target,
        }
    }

    #[test]
    fn python_control_event_decodes_and_restart_stays_fenced_until_target_restore() {
        let event: Phase92AuthorityControlEvent =
            serde_json::from_str(EVENT_JSON).expect("control fixture decodes");
        event.validate().expect("control fixture validates");
        let mut fence = Phase92AuthorityFence::default();
        fence
            .apply_control_event(&event, ACTIVE_NOW_NS)
            .expect("primary snapshot loads in recovery mode");
        let first = publication(&event, SinkTarget::PrimaryCanonical, 501);
        assert!(fence
            .permits(&first, ACTIVE_NOW_NS)
            .is_err_and(|error| error.contains("recovery is required")));
        for target in [
            SinkTarget::PrimaryCanonical,
            SinkTarget::PublicV2,
            SinkTarget::LegacyV1,
        ] {
            fence
                .restore_committed_watermark(&publication(&event, target, 500))
                .expect("independent target watermark restores");
        }
        fence
            .permits(&first, ACTIVE_NOW_NS)
            .expect("first post-handoff canonical watermark is W+1");
        fence.commit(&first).expect("W+1 commits");
        assert!(fence
            .permits(
                &publication(&event, SinkTarget::PrimaryCanonical, 501),
                ACTIVE_NOW_NS,
            )
            .is_err_and(|error| error.contains("duplicate, stale or gapped")));
        fence
            .apply_control_event(&event, ACTIVE_NOW_NS)
            .expect("identical compacted authority replay is idempotent");
    }

    #[test]
    fn canary_to_primary_requires_and_applies_exact_handoff() {
        let event: Phase92AuthorityControlEvent =
            serde_json::from_str(EVENT_JSON).expect("control fixture decodes");
        let primary = event.authority.as_ref().expect("fixture authority");
        let handoff = event.handoff.as_ref().expect("fixture handoff");
        let mut canary = primary.clone();
        canary.state = Phase92AuthorityState::RustCanary;
        canary.owner_id = handoff.old_owner_id.clone();
        canary.authority_revision = handoff.expected_authority_revision;
        canary.lease_epoch = handoff.expected_lease_epoch;
        canary.start_watermark = handoff.terminal_watermark;
        canary.terminal_watermark = None;
        canary.previous_owner_id = None;
        canary.handoff_digest = None;
        canary.public_write_allowed = false;
        canary.legacy_write_allowed = false;
        let mut fence = Phase92AuthorityFence::default();
        fence.apply(canary).expect("canary authority loads");
        fence
            .apply_control_event(&event, ACTIVE_NOW_NS)
            .expect("exact handoff promotes primary");
        assert_eq!(
            fence.current().expect("primary authority").state,
            Phase92AuthorityState::RustPrimary
        );
    }

    #[test]
    fn altered_outer_identity_fails_closed() {
        let mut event: Phase92AuthorityControlEvent =
            serde_json::from_str(EVENT_JSON).expect("control fixture decodes");
        event.authority_revision += 1;
        assert!(event
            .validate()
            .is_err_and(|error| error.contains("differ")));
    }
}
