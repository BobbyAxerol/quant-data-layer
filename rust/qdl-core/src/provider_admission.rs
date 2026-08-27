//! Provider-neutral admission state machine for bounded external market-data
//! lanes. It is deliberately pure: durable coordination is owned by the
//! realtime core, while Python provider adapters consume its wire decision.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::rate_limit::TokenBucket;

pub const PROVIDER_ADMISSION_SCHEMA: &str = "qdl.provider_admission.v1";
pub const PROVIDER_ADMISSION_AUTHORITY: &str = "RUST_QDL_CORE_V1";

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderLaneKey {
    pub provider: String,
    pub market: String,
    pub endpoint_family: String,
}

impl ProviderLaneKey {
    pub fn new(
        provider: impl Into<String>,
        market: impl Into<String>,
        endpoint_family: impl Into<String>,
    ) -> Result<Self, ProviderAdmissionError> {
        let lane = Self {
            provider: provider.into(),
            market: market.into(),
            endpoint_family: endpoint_family.into(),
        };
        lane.validate()?;
        Ok(lane)
    }

    pub fn validate(&self) -> Result<(), ProviderAdmissionError> {
        for (field, value) in [
            ("provider", self.provider.as_str()),
            ("market", self.market.as_str()),
            ("endpoint_family", self.endpoint_family.as_str()),
        ] {
            if !is_lane_segment(value) {
                return Err(ProviderAdmissionError::InvalidLane(format!(
                    "{field} must be 1..64 ASCII uppercase/digit/_/- characters"
                )));
            }
        }
        Ok(())
    }

    pub fn redis_suffix(&self) -> String {
        format!("{}:{}:{}", self.provider, self.market, self.endpoint_family)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AdmissionPriority {
    Realtime,
    Batch,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AdmissionDisposition {
    Granted,
    Deferred,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AdmissionDeferReason {
    Cooldown,
    TokenBudget,
    InflightCapacity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderLanePolicy {
    pub token_capacity: u64,
    pub refill_tokens: u64,
    pub refill_interval_ns: i64,
    pub max_inflight: u32,
    pub reserved_realtime_inflight: u32,
    pub max_lease_ns: i64,
    pub default_cooldown_ns: i64,
    pub idle_ttl_ns: i64,
}

impl ProviderLanePolicy {
    pub fn validate(&self) -> Result<(), ProviderAdmissionError> {
        if self.token_capacity == 0
            || self.refill_tokens == 0
            || self.refill_interval_ns <= 0
            || self.max_inflight == 0
            || self.reserved_realtime_inflight > self.max_inflight
            || self.max_lease_ns <= 0
            || self.default_cooldown_ns <= 0
            || self.idle_ttl_ns <= 0
        {
            return Err(ProviderAdmissionError::InvalidPolicy(
                "provider lane policy bounds are invalid".into(),
            ));
        }
        Ok(())
    }

    pub fn sha256(&self) -> String {
        let encoded = serde_json::to_vec(self)
            .expect("ProviderLanePolicy only contains serializable primitive fields");
        hex::encode(Sha256::digest(encoded))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionRequest {
    pub request_id: String,
    pub priority: AdmissionPriority,
    pub token_cost: u64,
}

impl ProviderAdmissionRequest {
    pub fn validate(&self, policy: &ProviderLanePolicy) -> Result<(), ProviderAdmissionError> {
        if !is_request_id(&self.request_id) {
            return Err(ProviderAdmissionError::InvalidRequest(
                "request_id must be 1..128 ASCII alphanumeric/._:- characters".into(),
            ));
        }
        if self.token_cost == 0 || self.token_cost > policy.token_capacity {
            return Err(ProviderAdmissionError::InvalidRequest(
                "token_cost must fit the configured provider lane".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionLease {
    pub request_id: String,
    pub priority: AdmissionPriority,
    pub token_cost: u64,
    pub expires_at_ns: i64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionMetrics {
    pub admitted: u64,
    pub deferred: u64,
    pub coalesced: u64,
    pub cooldowns: u64,
    pub wait_ms: u64,
    pub expired_leases: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionDecision {
    pub schema: String,
    pub authority: String,
    pub lane: ProviderLaneKey,
    pub request_id: String,
    pub disposition: AdmissionDisposition,
    pub defer_reason: Option<AdmissionDeferReason>,
    pub retry_after_ms: Option<u64>,
    pub lease_expires_at_ns: Option<i64>,
    pub coalesced: bool,
}

impl ProviderAdmissionDecision {
    fn granted(lane: ProviderLaneKey, lease: &ProviderAdmissionLease, coalesced: bool) -> Self {
        Self {
            schema: PROVIDER_ADMISSION_SCHEMA.into(),
            authority: PROVIDER_ADMISSION_AUTHORITY.into(),
            lane,
            request_id: lease.request_id.clone(),
            disposition: AdmissionDisposition::Granted,
            defer_reason: None,
            retry_after_ms: None,
            lease_expires_at_ns: Some(lease.expires_at_ns),
            coalesced,
        }
    }

    fn deferred(
        lane: ProviderLaneKey,
        request_id: String,
        reason: AdmissionDeferReason,
        retry_after_ns: i64,
    ) -> Self {
        Self {
            schema: PROVIDER_ADMISSION_SCHEMA.into(),
            authority: PROVIDER_ADMISSION_AUTHORITY.into(),
            lane,
            request_id,
            disposition: AdmissionDisposition::Deferred,
            defer_reason: Some(reason),
            retry_after_ms: Some(ns_to_ms(retry_after_ns)),
            lease_expires_at_ns: None,
            coalesced: false,
        }
    }

    pub fn validate_for(
        &self,
        expected_lane: &ProviderLaneKey,
        expected_request_id: &str,
    ) -> Result<(), ProviderAdmissionError> {
        if self.schema != PROVIDER_ADMISSION_SCHEMA
            || self.authority != PROVIDER_ADMISSION_AUTHORITY
            || &self.lane != expected_lane
            || self.request_id != expected_request_id
        {
            return Err(ProviderAdmissionError::InvalidDecision(
                "decision identity or Rust authority is invalid".into(),
            ));
        }
        match self.disposition {
            AdmissionDisposition::Granted
                if self.defer_reason.is_none()
                    && self.retry_after_ms.is_none()
                    && self.lease_expires_at_ns.is_some() =>
            {
                Ok(())
            }
            AdmissionDisposition::Deferred
                if self.defer_reason.is_some()
                    && self.retry_after_ms.unwrap_or(0) > 0
                    && self.lease_expires_at_ns.is_none()
                    && !self.coalesced =>
            {
                Ok(())
            }
            _ => Err(ProviderAdmissionError::InvalidDecision(
                "decision fields do not match its disposition".into(),
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitSignal {
    pub http_status: Option<u16>,
    pub provider_code: Option<i64>,
    pub retry_after_ns: Option<i64>,
}

impl RateLimitSignal {
    pub fn validate(&self) -> Result<(), ProviderAdmissionError> {
        let recognized = matches!(self.http_status, Some(418 | 429))
            || matches!(self.provider_code, Some(-1003));
        if !recognized {
            return Err(ProviderAdmissionError::InvalidRateLimitSignal(
                "only HTTP 418/429 or provider code -1003 can open cooldown".into(),
            ));
        }
        if self.retry_after_ns.is_some_and(|value| value <= 0) {
            return Err(ProviderAdmissionError::InvalidRateLimitSignal(
                "retry_after_ns must be positive when supplied".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionState {
    pub schema: String,
    pub lane: ProviderLaneKey,
    pub policy: ProviderLanePolicy,
    pub policy_sha256: String,
    pub revision: u64,
    /// Last shared coordinator clock observed by this lane. The state machine
    /// clamps a caller clock that moves backwards so a new worker cannot
    /// shorten an existing lease or cooldown.
    pub last_transition_ns: i64,
    pub bucket: TokenBucket,
    pub cooldown_until_ns: i64,
    pub leases: BTreeMap<String, ProviderAdmissionLease>,
    pub metrics: ProviderAdmissionMetrics,
}

impl ProviderAdmissionState {
    pub fn new(
        lane: ProviderLaneKey,
        policy: ProviderLanePolicy,
        now_ns: i64,
    ) -> Result<Self, ProviderAdmissionError> {
        lane.validate()?;
        policy.validate()?;
        if now_ns <= 0 {
            return Err(ProviderAdmissionError::InvalidClock);
        }
        Ok(Self {
            schema: PROVIDER_ADMISSION_SCHEMA.into(),
            lane,
            policy_sha256: policy.sha256(),
            bucket: TokenBucket::new(
                policy.token_capacity,
                policy.refill_tokens,
                policy.refill_interval_ns,
                now_ns,
            )
            .map_err(ProviderAdmissionError::InvalidPolicy)?,
            policy,
            revision: 1,
            last_transition_ns: now_ns,
            cooldown_until_ns: 0,
            leases: BTreeMap::new(),
            metrics: ProviderAdmissionMetrics::default(),
        })
    }

    pub fn validate(&self) -> Result<(), ProviderAdmissionError> {
        if self.schema != PROVIDER_ADMISSION_SCHEMA
            || self.revision == 0
            || self.last_transition_ns <= 0
        {
            return Err(ProviderAdmissionError::InvalidState(
                "admission state schema or revision is invalid".into(),
            ));
        }
        self.lane.validate()?;
        self.policy.validate()?;
        self.bucket
            .validate()
            .map_err(ProviderAdmissionError::InvalidState)?;
        if self.bucket.capacity() != self.policy.token_capacity
            || self.bucket.refill_tokens() != self.policy.refill_tokens
            || self.bucket.refill_interval_ns() != self.policy.refill_interval_ns
            || self.policy_sha256 != self.policy.sha256()
        {
            return Err(ProviderAdmissionError::InvalidState(
                "admission state policy does not match its bucket or digest".into(),
            ));
        }
        if self.cooldown_until_ns < 0 || self.leases.len() > self.policy.max_inflight as usize {
            return Err(ProviderAdmissionError::InvalidState(
                "admission state cooldown or lease bounds are invalid".into(),
            ));
        }
        for (request_id, lease) in &self.leases {
            if request_id != &lease.request_id
                || lease.token_cost == 0
                || lease.token_cost > self.policy.token_capacity
                || lease.expires_at_ns <= 0
            {
                return Err(ProviderAdmissionError::InvalidState(
                    "admission lease state is invalid".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn admit(
        &mut self,
        request: ProviderAdmissionRequest,
        now_ns: i64,
    ) -> Result<ProviderAdmissionDecision, ProviderAdmissionError> {
        self.validate()?;
        let now_ns = self.observe_clock(now_ns)?;
        request.validate(&self.policy)?;
        self.reap_expired(now_ns);
        if let Some(lease) = self.leases.get(&request.request_id).cloned() {
            self.metrics.coalesced = self.metrics.coalesced.saturating_add(1);
            self.bump_revision();
            return Ok(ProviderAdmissionDecision::granted(
                self.lane.clone(),
                &lease,
                true,
            ));
        }
        if self.cooldown_until_ns > now_ns {
            let retry_after_ns = self.cooldown_until_ns.saturating_sub(now_ns);
            self.record_defer(retry_after_ns);
            return Ok(ProviderAdmissionDecision::deferred(
                self.lane.clone(),
                request.request_id,
                AdmissionDeferReason::Cooldown,
                retry_after_ns,
            ));
        }
        if !self.has_inflight_capacity(request.priority) {
            let retry_after_ns = self.next_lease_release_ns(now_ns);
            self.record_defer(retry_after_ns);
            return Ok(ProviderAdmissionDecision::deferred(
                self.lane.clone(),
                request.request_id,
                AdmissionDeferReason::InflightCapacity,
                retry_after_ns,
            ));
        }
        let retry_after_ns = self
            .bucket
            .retry_after_ns(request.token_cost, now_ns)
            .ok_or_else(|| {
                ProviderAdmissionError::InvalidRequest(
                    "token_cost cannot fit configured lane capacity".into(),
                )
            })?;
        if retry_after_ns > 0 {
            self.record_defer(retry_after_ns);
            return Ok(ProviderAdmissionDecision::deferred(
                self.lane.clone(),
                request.request_id,
                AdmissionDeferReason::TokenBudget,
                retry_after_ns,
            ));
        }
        if !self.bucket.try_acquire(request.token_cost, now_ns) {
            return Err(ProviderAdmissionError::InvalidState(
                "token bucket admitted then rejected the same request".into(),
            ));
        }
        let lease = ProviderAdmissionLease {
            request_id: request.request_id.clone(),
            priority: request.priority,
            token_cost: request.token_cost,
            expires_at_ns: now_ns.saturating_add(self.policy.max_lease_ns),
        };
        self.leases.insert(request.request_id, lease.clone());
        self.metrics.admitted = self.metrics.admitted.saturating_add(1);
        self.bump_revision();
        Ok(ProviderAdmissionDecision::granted(
            self.lane.clone(),
            &lease,
            false,
        ))
    }

    pub fn complete(
        &mut self,
        request_id: &str,
        now_ns: i64,
    ) -> Result<bool, ProviderAdmissionError> {
        self.validate()?;
        if !is_request_id(request_id) {
            return Err(ProviderAdmissionError::InvalidRequest(
                "request_id or clock is invalid".into(),
            ));
        }
        let now_ns = self.observe_clock(now_ns)?;
        self.reap_expired(now_ns);
        let removed = self.leases.remove(request_id).is_some();
        if removed {
            self.bump_revision();
        }
        Ok(removed)
    }

    pub fn record_rate_limit(
        &mut self,
        request_id: Option<&str>,
        signal: RateLimitSignal,
        now_ns: i64,
    ) -> Result<ProviderAdmissionDecision, ProviderAdmissionError> {
        self.validate()?;
        let now_ns = self.observe_clock(now_ns)?;
        signal.validate()?;
        self.reap_expired(now_ns);
        let request_id = request_id.unwrap_or("provider-cooldown");
        if !is_request_id(request_id) {
            return Err(ProviderAdmissionError::InvalidRequest(
                "rate-limit request_id is invalid".into(),
            ));
        }
        self.leases.remove(request_id);
        let cooldown_ns = signal
            .retry_after_ns
            .unwrap_or(self.policy.default_cooldown_ns);
        self.cooldown_until_ns = self
            .cooldown_until_ns
            .max(now_ns.saturating_add(cooldown_ns));
        let retry_after_ns = self.cooldown_until_ns.saturating_sub(now_ns);
        self.metrics.cooldowns = self.metrics.cooldowns.saturating_add(1);
        self.record_defer(retry_after_ns);
        Ok(ProviderAdmissionDecision::deferred(
            self.lane.clone(),
            request_id.into(),
            AdmissionDeferReason::Cooldown,
            retry_after_ns,
        ))
    }

    pub fn storage_ttl_ms(&mut self, now_ns: i64) -> Result<u64, ProviderAdmissionError> {
        self.validate()?;
        let now_ns = self.observe_clock(now_ns)?;
        self.reap_expired(now_ns);
        let lease_remaining = self
            .leases
            .values()
            .map(|lease| lease.expires_at_ns.saturating_sub(now_ns))
            .max()
            .unwrap_or(0);
        let remaining = self
            .policy
            .idle_ttl_ns
            .max(self.cooldown_until_ns.saturating_sub(now_ns))
            .max(lease_remaining);
        Ok(ns_to_ms(remaining))
    }

    fn has_inflight_capacity(&self, priority: AdmissionPriority) -> bool {
        let active = self.leases.len() as u32;
        if active >= self.policy.max_inflight {
            return false;
        }
        match priority {
            AdmissionPriority::Realtime => true,
            AdmissionPriority::Batch => {
                active < self.policy.max_inflight - self.policy.reserved_realtime_inflight
            }
        }
    }

    fn next_lease_release_ns(&self, now_ns: i64) -> i64 {
        self.leases
            .values()
            .map(|lease| lease.expires_at_ns.saturating_sub(now_ns).max(1))
            .min()
            .unwrap_or(1)
    }

    fn reap_expired(&mut self, now_ns: i64) {
        let expired = self
            .leases
            .iter()
            .filter_map(|(key, lease)| (lease.expires_at_ns <= now_ns).then_some(key.clone()))
            .collect::<Vec<_>>();
        if !expired.is_empty() {
            for key in expired {
                self.leases.remove(&key);
                self.metrics.expired_leases = self.metrics.expired_leases.saturating_add(1);
            }
            self.bump_revision();
        }
    }

    fn record_defer(&mut self, retry_after_ns: i64) {
        self.metrics.deferred = self.metrics.deferred.saturating_add(1);
        self.metrics.wait_ms = self
            .metrics
            .wait_ms
            .saturating_add(ns_to_ms(retry_after_ns));
        self.bump_revision();
    }

    fn bump_revision(&mut self) {
        self.revision = self.revision.saturating_add(1);
    }

    fn observe_clock(&mut self, now_ns: i64) -> Result<i64, ProviderAdmissionError> {
        if now_ns <= 0 {
            return Err(ProviderAdmissionError::InvalidClock);
        }
        let effective_now_ns = now_ns.max(self.last_transition_ns);
        self.last_transition_ns = effective_now_ns;
        Ok(effective_now_ns)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderAdmissionError {
    InvalidLane(String),
    InvalidPolicy(String),
    InvalidRequest(String),
    InvalidRateLimitSignal(String),
    InvalidDecision(String),
    InvalidState(String),
    InvalidClock,
}

impl Display for ProviderAdmissionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLane(value) => {
                write!(formatter, "provider admission lane is invalid: {value}")
            }
            Self::InvalidPolicy(value) => {
                write!(formatter, "provider admission policy is invalid: {value}")
            }
            Self::InvalidRequest(value) => {
                write!(formatter, "provider admission request is invalid: {value}")
            }
            Self::InvalidRateLimitSignal(value) => write!(
                formatter,
                "provider admission rate-limit signal is invalid: {value}"
            ),
            Self::InvalidDecision(value) => {
                write!(formatter, "provider admission decision is invalid: {value}")
            }
            Self::InvalidState(value) => {
                write!(formatter, "provider admission state is invalid: {value}")
            }
            Self::InvalidClock => write!(formatter, "provider admission clock must be positive"),
        }
    }
}

impl std::error::Error for ProviderAdmissionError {}

fn is_lane_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_uppercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn is_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn ns_to_ms(value_ns: i64) -> u64 {
    value_ns
        .max(1)
        .saturating_add(999_999)
        .saturating_div(1_000_000) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lane() -> ProviderLaneKey {
        ProviderLaneKey::new("BINANCE", "USDM", "REFERENCE_NATIVE_BASIS").unwrap()
    }

    fn policy() -> ProviderLanePolicy {
        ProviderLanePolicy {
            token_capacity: 4,
            refill_tokens: 1,
            refill_interval_ns: 1_000,
            max_inflight: 3,
            reserved_realtime_inflight: 1,
            max_lease_ns: 5_000,
            default_cooldown_ns: 60_000,
            idle_ttl_ns: 10_000,
        }
    }

    fn request(id: &str, priority: AdmissionPriority) -> ProviderAdmissionRequest {
        ProviderAdmissionRequest {
            request_id: id.into(),
            priority,
            token_cost: 1,
        }
    }

    #[test]
    fn coalesces_exact_request_and_keeps_symbols_out_of_lane_identity() {
        let mut state = ProviderAdmissionState::new(lane(), policy(), 100).unwrap();
        let first = state
            .admit(request("basis:BTCUSDT", AdmissionPriority::Batch), 100)
            .unwrap();
        let repeat = state
            .admit(request("basis:BTCUSDT", AdmissionPriority::Batch), 101)
            .unwrap();
        let other = state
            .admit(request("basis:ETHUSDT", AdmissionPriority::Batch), 102)
            .unwrap();
        assert_eq!(first.disposition, AdmissionDisposition::Granted);
        assert!(repeat.coalesced);
        assert_eq!(other.disposition, AdmissionDisposition::Granted);
        assert_eq!(state.leases.len(), 2);
        assert_eq!(
            state.lane.redis_suffix(),
            "BINANCE:USDM:REFERENCE_NATIVE_BASIS"
        );
    }

    #[test]
    fn reserves_inflight_capacity_for_realtime() {
        let mut state = ProviderAdmissionState::new(lane(), policy(), 100).unwrap();
        state
            .admit(request("batch:one", AdmissionPriority::Batch), 100)
            .unwrap();
        state
            .admit(request("batch:two", AdmissionPriority::Batch), 100)
            .unwrap();
        let blocked = state
            .admit(request("batch:three", AdmissionPriority::Batch), 100)
            .unwrap();
        let realtime = state
            .admit(request("bar:BTCUSDT", AdmissionPriority::Realtime), 100)
            .unwrap();
        assert_eq!(blocked.disposition, AdmissionDisposition::Deferred);
        assert_eq!(
            blocked.defer_reason,
            Some(AdmissionDeferReason::InflightCapacity)
        );
        assert_eq!(realtime.disposition, AdmissionDisposition::Granted);
    }

    #[test]
    fn allows_a_realtime_only_lane_without_admitting_batch_work() {
        let mut realtime_only = policy();
        realtime_only.max_inflight = 1;
        realtime_only.reserved_realtime_inflight = 1;
        let mut state = ProviderAdmissionState::new(lane(), realtime_only, 100).unwrap();
        assert_eq!(
            state
                .admit(request("basis:BTCUSDT", AdmissionPriority::Batch), 100)
                .unwrap()
                .defer_reason,
            Some(AdmissionDeferReason::InflightCapacity)
        );
        assert_eq!(
            state
                .admit(request("bar:BTCUSDT", AdmissionPriority::Realtime), 100)
                .unwrap()
                .disposition,
            AdmissionDisposition::Granted
        );
    }

    #[test]
    fn opens_a_shared_cooldown_without_hot_retry() {
        let mut state = ProviderAdmissionState::new(lane(), policy(), 100).unwrap();
        state
            .admit(request("basis:SOLUSDT", AdmissionPriority::Batch), 100)
            .unwrap();
        let deferred = state
            .record_rate_limit(
                Some("basis:SOLUSDT"),
                RateLimitSignal {
                    http_status: Some(418),
                    provider_code: None,
                    retry_after_ns: None,
                },
                101,
            )
            .unwrap();
        let next = state
            .admit(request("basis:BNBUSDT", AdmissionPriority::Batch), 102)
            .unwrap();
        assert_eq!(deferred.retry_after_ms, Some(1));
        assert_eq!(next.defer_reason, Some(AdmissionDeferReason::Cooldown));
        assert_eq!(next.retry_after_ms, Some(1));
        assert_eq!(state.leases.len(), 0);
        assert_eq!(state.metrics.cooldowns, 1);
    }

    #[test]
    fn honors_explicit_cooldown_and_recovers_after_expiry() {
        let mut state = ProviderAdmissionState::new(lane(), policy(), 100).unwrap();
        state
            .record_rate_limit(
                None,
                RateLimitSignal {
                    http_status: Some(429),
                    provider_code: None,
                    retry_after_ns: Some(2_000),
                },
                100,
            )
            .unwrap();
        assert_eq!(
            state
                .admit(request("basis:DOGEUSDT", AdmissionPriority::Batch), 101)
                .unwrap()
                .retry_after_ms,
            Some(1)
        );
        assert_eq!(
            state
                .admit(request("basis:DOGEUSDT", AdmissionPriority::Batch), 2_100)
                .unwrap()
                .disposition,
            AdmissionDisposition::Granted
        );
    }

    #[test]
    fn expires_leases_and_clamps_a_backward_worker_clock() {
        let mut state = ProviderAdmissionState::new(lane(), policy(), 100).unwrap();
        state
            .admit(request("batch:one", AdmissionPriority::Batch), 100)
            .unwrap();
        state
            .admit(request("batch:two", AdmissionPriority::Batch), 100)
            .unwrap();
        assert_eq!(
            state
                .admit(request("batch:three", AdmissionPriority::Batch), 101)
                .unwrap()
                .defer_reason,
            Some(AdmissionDeferReason::InflightCapacity)
        );
        assert_eq!(
            state
                .admit(request("batch:three", AdmissionPriority::Batch), 5_101)
                .unwrap()
                .disposition,
            AdmissionDisposition::Granted
        );
        assert_eq!(state.metrics.expired_leases, 2);

        state
            .record_rate_limit(
                None,
                RateLimitSignal {
                    http_status: Some(429),
                    provider_code: None,
                    retry_after_ns: Some(10_000),
                },
                5_102,
            )
            .unwrap();
        let deferred = state
            .admit(request("basis:ETHUSDT", AdmissionPriority::Batch), 5_000)
            .unwrap();
        assert_eq!(deferred.defer_reason, Some(AdmissionDeferReason::Cooldown));
        assert_eq!(state.last_transition_ns, 5_102);
    }

    #[test]
    fn token_wait_is_deterministic_and_invalid_provider_signals_fail_closed() {
        let mut limited_policy = policy();
        limited_policy.token_capacity = 2;
        let mut state = ProviderAdmissionState::new(lane(), limited_policy, 100).unwrap();
        state
            .admit(request("basis:one", AdmissionPriority::Batch), 100)
            .unwrap();
        state
            .admit(request("basis:two", AdmissionPriority::Batch), 100)
            .unwrap();
        let blocked = state
            .admit(request("basis:three", AdmissionPriority::Realtime), 100)
            .unwrap();
        assert_eq!(
            blocked.defer_reason,
            Some(AdmissionDeferReason::TokenBudget)
        );
        assert_eq!(blocked.retry_after_ms, Some(1));
        assert!(matches!(
            state.record_rate_limit(
                None,
                RateLimitSignal {
                    http_status: Some(500),
                    provider_code: None,
                    retry_after_ns: None
                },
                100,
            ),
            Err(ProviderAdmissionError::InvalidRateLimitSignal(_))
        ));
    }

    #[test]
    fn binance_provider_code_minus_1003_opens_the_same_shared_cooldown() {
        let mut state = ProviderAdmissionState::new(lane(), policy(), 100).unwrap();
        let outcome = state
            .record_rate_limit(
                None,
                RateLimitSignal {
                    http_status: None,
                    provider_code: Some(-1003),
                    retry_after_ns: None,
                },
                100,
            )
            .unwrap();
        assert_eq!(outcome.defer_reason, Some(AdmissionDeferReason::Cooldown));
        assert_eq!(
            state
                .admit(request("basis:BTCUSDT", AdmissionPriority::Batch), 101)
                .unwrap()
                .defer_reason,
            Some(AdmissionDeferReason::Cooldown)
        );
    }

    #[test]
    fn wire_decision_refuses_identity_or_shape_rewrites() {
        let mut state = ProviderAdmissionState::new(lane(), policy(), 100).unwrap();
        let decision = state
            .admit(request("basis:BTCUSDT", AdmissionPriority::Batch), 100)
            .unwrap();
        decision.validate_for(&lane(), "basis:BTCUSDT").unwrap();
        let wire = serde_json::to_value(&decision).unwrap();
        assert_eq!(wire["schema"], PROVIDER_ADMISSION_SCHEMA);
        assert_eq!(wire["authority"], PROVIDER_ADMISSION_AUTHORITY);
        assert_eq!(wire["disposition"], "GRANTED");
        assert!(decision.validate_for(&lane(), "basis:ETHUSDT").is_err());
        let mut invalid = decision.clone();
        invalid.retry_after_ms = Some(1);
        assert!(invalid.validate_for(&lane(), "basis:BTCUSDT").is_err());
    }
}
