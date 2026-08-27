//! Runtime-owned atomic coordination for the pure `qdl-core` provider
//! admission state machine. The Redis EVAL contract stores only serialized
//! admission state; all policy transitions happen in Rust before the CAS.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};

use qdl_core::provider_admission::{
    ProviderAdmissionDecision, ProviderAdmissionError, ProviderAdmissionRequest,
    ProviderAdmissionState, ProviderLaneKey, ProviderLanePolicy, RateLimitSignal,
};
use redis::{cmd, Client, Script};

pub const REDIS_PROVIDER_ADMISSION_CAS_LUA: &str = r#"
local current = redis.call('GET', KEYS[1])
if ARGV[1] == '__QDL_ADMISSION_MISSING__' then
  if current then return 0 end
elseif (not current) or current ~= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
"#;

const MISSING_STATE_SENTINEL: &str = "__QDL_ADMISSION_MISSING__";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisAdmissionCas {
    pub key: String,
    pub expected_state_json: Option<String>,
    pub next_state_json: String,
    pub ttl_ms: u64,
}

impl RedisAdmissionCas {
    pub fn eval_args(&self) -> [String; 3] {
        [
            self.expected_state_json
                .clone()
                .unwrap_or_else(|| MISSING_STATE_SENTINEL.into()),
            self.next_state_json.clone(),
            self.ttl_ms.max(1).to_string(),
        ]
    }
}

pub trait AtomicAdmissionStore: Send + Sync {
    fn load(&self, key: &str) -> Result<Option<String>, AdmissionStoreError>;
    fn compare_and_swap(&self, operation: &RedisAdmissionCas) -> Result<bool, AdmissionStoreError>;
}

/// Narrow adapter boundary for the existing stable Redis EVAL capability.
/// Runtime wiring is intentionally deferred to C3.6-C.2; C3.6-C.1 tests this
/// contract through an isolated executor and never names a live Redis key.
pub trait RedisAdmissionExecutor: Send + Sync {
    fn get(&self, key: &str) -> Result<Option<String>, AdmissionStoreError>;
    fn eval(&self, script: &str, key: &str, args: [String; 3]) -> Result<i64, AdmissionStoreError>;
}

#[derive(Clone)]
pub struct RedisAdmissionStore<E> {
    executor: E,
}

impl<E> RedisAdmissionStore<E> {
    pub fn new(executor: E) -> Self {
        Self { executor }
    }
}

impl<E: RedisAdmissionExecutor> AtomicAdmissionStore for RedisAdmissionStore<E> {
    fn load(&self, key: &str) -> Result<Option<String>, AdmissionStoreError> {
        self.executor.get(key)
    }

    fn compare_and_swap(&self, operation: &RedisAdmissionCas) -> Result<bool, AdmissionStoreError> {
        let outcome = self.executor.eval(
            REDIS_PROVIDER_ADMISSION_CAS_LUA,
            &operation.key,
            operation.eval_args(),
        )?;
        match outcome {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(AdmissionStoreError::Protocol(format!(
                "Redis admission CAS returned invalid result {value}"
            ))),
        }
    }
}

/// Synchronous client deliberately used only by the narrow coordinator adapter.
/// Each operation uses one Redis command/EVAL and no provider payload is ever
/// stored here. Runtime construction and TLS credentials remain C3.6-C.2 work.
#[derive(Clone)]
pub struct RedisClientAdmissionExecutor {
    client: Client,
}

impl RedisClientAdmissionExecutor {
    pub fn new(redis_url: &str) -> Result<Self, AdmissionStoreError> {
        if redis_url.trim().is_empty() {
            return Err(AdmissionStoreError::Configuration(
                "provider admission Redis URL is required".into(),
            ));
        }
        Client::open(redis_url)
            .map(|client| Self { client })
            .map_err(|error| {
                AdmissionStoreError::Protocol(format!(
                    "provider admission Redis URL is invalid: {error}"
                ))
            })
    }

    pub fn delete(&self, key: &str) -> Result<(), AdmissionStoreError> {
        let mut connection = self.connection()?;
        cmd("DEL")
            .arg(key)
            .query::<i64>(&mut connection)
            .map(|_| ())
            .map_err(redis_error)
    }

    /// Return the shared Redis wall clock in nanoseconds.  Runtime admission
    /// transitions must never compare separate worker monotonic epochs.
    pub fn time_ns(&self) -> Result<i64, AdmissionStoreError> {
        let mut connection = self.connection()?;
        let (seconds, microseconds): (i64, i64) =
            cmd("TIME").query(&mut connection).map_err(redis_error)?;
        if seconds <= 0 || !(0..1_000_000).contains(&microseconds) {
            return Err(AdmissionStoreError::Protocol(
                "Redis TIME returned an invalid wall-clock value".into(),
            ));
        }
        seconds
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_add(microseconds.saturating_mul(1_000)))
            .ok_or_else(|| {
                AdmissionStoreError::Protocol("Redis TIME overflowed nanoseconds".into())
            })
    }

    fn connection(&self) -> Result<redis::Connection, AdmissionStoreError> {
        self.client.get_connection().map_err(redis_error)
    }
}

impl RedisAdmissionExecutor for RedisClientAdmissionExecutor {
    fn get(&self, key: &str) -> Result<Option<String>, AdmissionStoreError> {
        let mut connection = self.connection()?;
        cmd("GET")
            .arg(key)
            .query::<Option<String>>(&mut connection)
            .map_err(redis_error)
    }

    fn eval(&self, script: &str, key: &str, args: [String; 3]) -> Result<i64, AdmissionStoreError> {
        let mut connection = self.connection()?;
        Script::new(script)
            .key(key)
            .arg(&args[0])
            .arg(&args[1])
            .arg(&args[2])
            .invoke::<i64>(&mut connection)
            .map_err(redis_error)
    }
}

#[derive(Clone, Debug)]
pub struct ProviderAdmissionCoordinator<S> {
    store: S,
    key_prefix: String,
    max_cas_attempts: usize,
}

impl<S> ProviderAdmissionCoordinator<S> {
    pub fn new(store: S, key_prefix: impl Into<String>) -> Result<Self, AdmissionStoreError> {
        let key_prefix = key_prefix.into();
        if !is_prefix(&key_prefix) {
            return Err(AdmissionStoreError::Configuration(
                "provider admission Redis prefix is invalid".into(),
            ));
        }
        Ok(Self {
            store,
            key_prefix,
            max_cas_attempts: 8,
        })
    }

    pub fn with_max_cas_attempts(
        mut self,
        max_cas_attempts: usize,
    ) -> Result<Self, AdmissionStoreError> {
        if !(1..=32).contains(&max_cas_attempts) {
            return Err(AdmissionStoreError::Configuration(
                "provider admission CAS attempts must be 1..32".into(),
            ));
        }
        self.max_cas_attempts = max_cas_attempts;
        Ok(self)
    }

    pub fn key_for(&self, lane: &ProviderLaneKey) -> Result<String, AdmissionStoreError> {
        lane.validate().map_err(AdmissionStoreError::Core)?;
        Ok(format!("{}:{}", self.key_prefix, lane.redis_suffix()))
    }
}

impl<S: AtomicAdmissionStore> ProviderAdmissionCoordinator<S> {
    pub fn admit(
        &self,
        lane: ProviderLaneKey,
        policy: ProviderLanePolicy,
        request: ProviderAdmissionRequest,
        now_ns: i64,
    ) -> Result<ProviderAdmissionDecision, AdmissionStoreError> {
        self.transition(lane, policy, now_ns, move |state| {
            state.admit(request.clone(), now_ns)
        })
    }

    pub fn complete(
        &self,
        lane: ProviderLaneKey,
        policy: ProviderLanePolicy,
        request_id: String,
        now_ns: i64,
    ) -> Result<bool, AdmissionStoreError> {
        self.transition(lane, policy, now_ns, move |state| {
            state.complete(&request_id, now_ns)
        })
    }

    pub fn record_rate_limit(
        &self,
        lane: ProviderLaneKey,
        policy: ProviderLanePolicy,
        request_id: Option<String>,
        signal: RateLimitSignal,
        now_ns: i64,
    ) -> Result<ProviderAdmissionDecision, AdmissionStoreError> {
        self.transition(lane, policy, now_ns, move |state| {
            state.record_rate_limit(request_id.as_deref(), signal.clone(), now_ns)
        })
    }

    fn transition<F, T>(
        &self,
        lane: ProviderLaneKey,
        policy: ProviderLanePolicy,
        now_ns: i64,
        mut mutate: F,
    ) -> Result<T, AdmissionStoreError>
    where
        F: FnMut(&mut ProviderAdmissionState) -> Result<T, ProviderAdmissionError>,
    {
        let key = self.key_for(&lane)?;
        policy.validate().map_err(AdmissionStoreError::Core)?;
        for _ in 0..self.max_cas_attempts {
            let expected_state_json = self.store.load(&key)?;
            let mut state = match &expected_state_json {
                Some(encoded) => serde_json::from_str::<ProviderAdmissionState>(encoded)
                    .map_err(|error| AdmissionStoreError::Protocol(error.to_string()))?,
                None => ProviderAdmissionState::new(lane.clone(), policy.clone(), now_ns)
                    .map_err(AdmissionStoreError::Core)?,
            };
            if state.lane != lane || state.policy_sha256 != policy.sha256() {
                return Err(AdmissionStoreError::PolicyMismatch);
            }
            let decision = mutate(&mut state).map_err(AdmissionStoreError::Core)?;
            let ttl_ms = state
                .storage_ttl_ms(now_ns)
                .map_err(AdmissionStoreError::Core)?;
            let next_state_json = serde_json::to_string(&state)
                .map_err(|error| AdmissionStoreError::Protocol(error.to_string()))?;
            let operation = RedisAdmissionCas {
                key: key.clone(),
                expected_state_json,
                next_state_json,
                ttl_ms,
            };
            if self.store.compare_and_swap(&operation)? {
                return Ok(decision);
            }
        }
        Err(AdmissionStoreError::ConflictExhausted)
    }
}

#[derive(Clone, Default)]
pub struct InMemoryAdmissionStore {
    entries: Arc<Mutex<BTreeMap<String, String>>>,
}

impl InMemoryAdmissionStore {
    pub fn entry_count(&self) -> usize {
        self.entries
            .lock()
            .expect("in-memory store lock poisoned")
            .len()
    }
}

impl AtomicAdmissionStore for InMemoryAdmissionStore {
    fn load(&self, key: &str) -> Result<Option<String>, AdmissionStoreError> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| AdmissionStoreError::Protocol("in-memory store lock poisoned".into()))?
            .get(key)
            .cloned())
    }

    fn compare_and_swap(&self, operation: &RedisAdmissionCas) -> Result<bool, AdmissionStoreError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| AdmissionStoreError::Protocol("in-memory store lock poisoned".into()))?;
        if entries.get(&operation.key) != operation.expected_state_json.as_ref() {
            return Ok(false);
        }
        entries.insert(operation.key.clone(), operation.next_state_json.clone());
        Ok(true)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionStoreError {
    Configuration(String),
    Core(ProviderAdmissionError),
    Protocol(String),
    PolicyMismatch,
    ConflictExhausted,
}

impl Display for AdmissionStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(value) => {
                write!(formatter, "provider admission store config error: {value}")
            }
            Self::Core(value) => Display::fmt(value, formatter),
            Self::Protocol(value) => write!(
                formatter,
                "provider admission store protocol error: {value}"
            ),
            Self::PolicyMismatch => write!(
                formatter,
                "provider admission stored policy does not match caller policy"
            ),
            Self::ConflictExhausted => {
                write!(formatter, "provider admission CAS conflict limit exhausted")
            }
        }
    }
}

impl std::error::Error for AdmissionStoreError {}

fn redis_error(error: redis::RedisError) -> AdmissionStoreError {
    AdmissionStoreError::Protocol(format!(
        "provider admission Redis operation failed: {error}"
    ))
}

fn is_prefix(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use qdl_core::provider_admission::{
        AdmissionDeferReason, AdmissionDisposition, AdmissionPriority,
    };
    use std::sync::atomic::{AtomicBool, Ordering};

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

    fn request(request_id: &str, priority: AdmissionPriority) -> ProviderAdmissionRequest {
        ProviderAdmissionRequest {
            request_id: request_id.into(),
            priority,
            token_cost: 1,
        }
    }

    #[derive(Clone, Default)]
    struct ConflictOnceStore {
        inner: InMemoryAdmissionStore,
        fail_first_compare: Arc<AtomicBool>,
    }

    impl AtomicAdmissionStore for ConflictOnceStore {
        fn load(&self, key: &str) -> Result<Option<String>, AdmissionStoreError> {
            self.inner.load(key)
        }

        fn compare_and_swap(
            &self,
            operation: &RedisAdmissionCas,
        ) -> Result<bool, AdmissionStoreError> {
            if self.fail_first_compare.swap(false, Ordering::SeqCst) {
                return Ok(false);
            }
            self.inner.compare_and_swap(operation)
        }
    }

    #[test]
    fn shared_store_coordinates_independent_workers_without_symbol_keys() {
        let store = InMemoryAdmissionStore::default();
        let left =
            ProviderAdmissionCoordinator::new(store.clone(), "qdl:test:admission:v1").unwrap();
        let right =
            ProviderAdmissionCoordinator::new(store.clone(), "qdl:test:admission:v1").unwrap();
        left.admit(
            lane(),
            policy(),
            request("basis:BTCUSDT", AdmissionPriority::Batch),
            100,
        )
        .unwrap();
        right
            .admit(
                lane(),
                policy(),
                request("basis:ETHUSDT", AdmissionPriority::Batch),
                101,
            )
            .unwrap();
        let deferred = left
            .admit(
                lane(),
                policy(),
                request("basis:SOLUSDT", AdmissionPriority::Batch),
                102,
            )
            .unwrap();
        let realtime = right
            .admit(
                lane(),
                policy(),
                request("bar:BTCUSDT", AdmissionPriority::Realtime),
                103,
            )
            .unwrap();
        assert_eq!(
            deferred.defer_reason,
            Some(AdmissionDeferReason::InflightCapacity)
        );
        assert_eq!(realtime.disposition, AdmissionDisposition::Granted);
        assert_eq!(store.entry_count(), 1);
    }

    #[test]
    fn rate_limit_from_one_worker_defers_another_worker() {
        let store = InMemoryAdmissionStore::default();
        let left =
            ProviderAdmissionCoordinator::new(store.clone(), "qdl:test:admission:v1").unwrap();
        let right = ProviderAdmissionCoordinator::new(store, "qdl:test:admission:v1").unwrap();
        left.admit(
            lane(),
            policy(),
            request("basis:DOGEUSDT", AdmissionPriority::Batch),
            100,
        )
        .unwrap();
        let outcome = left
            .record_rate_limit(
                lane(),
                policy(),
                Some("basis:DOGEUSDT".into()),
                RateLimitSignal {
                    http_status: Some(418),
                    provider_code: None,
                    retry_after_ns: Some(50_000),
                },
                101,
            )
            .unwrap();
        let deferred = right
            .admit(
                lane(),
                policy(),
                request("basis:BNBUSDT", AdmissionPriority::Batch),
                102,
            )
            .unwrap();
        assert_eq!(outcome.defer_reason, Some(AdmissionDeferReason::Cooldown));
        assert_eq!(deferred.defer_reason, Some(AdmissionDeferReason::Cooldown));
        assert_eq!(deferred.retry_after_ms, Some(1));
    }

    #[test]
    fn retries_an_atomic_conflict_without_duplicate_admission() {
        let store = ConflictOnceStore {
            inner: InMemoryAdmissionStore::default(),
            fail_first_compare: Arc::new(AtomicBool::new(true)),
        };
        let coordinator =
            ProviderAdmissionCoordinator::new(store.clone(), "qdl:test:admission:v1").unwrap();
        assert_eq!(
            coordinator
                .admit(
                    lane(),
                    policy(),
                    request("basis:BTCUSDT", AdmissionPriority::Batch),
                    100,
                )
                .unwrap()
                .disposition,
            AdmissionDisposition::Granted
        );
        assert_eq!(store.inner.entry_count(), 1);
    }

    #[test]
    fn completion_releases_a_reserved_slot_and_policy_mismatch_fails_closed() {
        let store = InMemoryAdmissionStore::default();
        let coordinator =
            ProviderAdmissionCoordinator::new(store, "qdl:test:admission:v1").unwrap();
        coordinator
            .admit(
                lane(),
                policy(),
                request("basis:BTCUSDT", AdmissionPriority::Batch),
                100,
            )
            .unwrap();
        coordinator
            .admit(
                lane(),
                policy(),
                request("basis:ETHUSDT", AdmissionPriority::Batch),
                100,
            )
            .unwrap();
        assert_eq!(
            coordinator
                .admit(
                    lane(),
                    policy(),
                    request("basis:SOLUSDT", AdmissionPriority::Batch),
                    101,
                )
                .unwrap()
                .defer_reason,
            Some(AdmissionDeferReason::InflightCapacity)
        );
        assert!(coordinator
            .complete(lane(), policy(), "basis:BTCUSDT".into(), 102,)
            .unwrap());
        assert_eq!(
            coordinator
                .admit(
                    lane(),
                    policy(),
                    request("basis:SOLUSDT", AdmissionPriority::Batch),
                    103,
                )
                .unwrap()
                .disposition,
            AdmissionDisposition::Granted
        );
        let mut changed_policy = policy();
        changed_policy.token_capacity = 5;
        assert!(matches!(
            coordinator.admit(
                lane(),
                changed_policy,
                request("basis:BNBUSDT", AdmissionPriority::Batch),
                104,
            ),
            Err(AdmissionStoreError::PolicyMismatch)
        ));
    }

    #[test]
    fn endpoint_families_and_venues_have_independent_atomic_lanes() {
        let store = InMemoryAdmissionStore::default();
        let coordinator =
            ProviderAdmissionCoordinator::new(store.clone(), "qdl:test:admission:v1").unwrap();
        let history = lane();
        let book = ProviderLaneKey::new("BINANCE", "USDM", "BOOK_SNAPSHOT").unwrap();
        let okx_history = ProviderLaneKey::new("OKX", "SWAP", "REFERENCE_NATIVE_BASIS").unwrap();
        coordinator
            .admit(
                history,
                policy(),
                request("basis:BTCUSDT", AdmissionPriority::Batch),
                100,
            )
            .unwrap();
        coordinator
            .admit(
                book,
                policy(),
                request("book:BTCUSDT", AdmissionPriority::Batch),
                100,
            )
            .unwrap();
        coordinator
            .admit(
                okx_history,
                policy(),
                request("basis:BTC-USDT-SWAP", AdmissionPriority::Batch),
                100,
            )
            .unwrap();
        assert_eq!(store.entry_count(), 3);
    }

    #[test]
    fn redis_cas_contract_is_exact_and_namespaced() {
        let store = InMemoryAdmissionStore::default();
        let coordinator =
            ProviderAdmissionCoordinator::new(store.clone(), "qdl:test:admission:v1").unwrap();
        coordinator
            .admit(
                lane(),
                policy(),
                request("basis:BTCUSDT", AdmissionPriority::Batch),
                100,
            )
            .unwrap();
        let key = coordinator.key_for(&lane()).unwrap();
        assert_eq!(
            key,
            "qdl:test:admission:v1:BINANCE:USDM:REFERENCE_NATIVE_BASIS"
        );
        let current = store.load(&key).unwrap().unwrap();
        let operation = RedisAdmissionCas {
            key: key.clone(),
            expected_state_json: Some("stale".into()),
            next_state_json: current.clone(),
            ttl_ms: 1,
        };
        assert!(!store.compare_and_swap(&operation).unwrap());
        let args = RedisAdmissionCas {
            expected_state_json: Some(current),
            ..operation
        }
        .eval_args();
        assert_eq!(args[2], "1");
        assert!(REDIS_PROVIDER_ADMISSION_CAS_LUA.contains("redis.call('SET'"));
    }

    #[test]
    #[ignore = "requires an isolated Redis URL in QDL_TEST_REDIS_URL"]
    fn isolated_redis_cas_coordinates_two_workers_and_cleans_its_key() {
        let redis_url = std::env::var("QDL_TEST_REDIS_URL")
            .expect("QDL_TEST_REDIS_URL is required for isolated Redis integration test");
        let executor = RedisClientAdmissionExecutor::new(&redis_url).unwrap();
        let store = RedisAdmissionStore::new(executor.clone());
        let prefix = format!("qdl:test:c36-c1:{}", std::process::id());
        let left = ProviderAdmissionCoordinator::new(store, &prefix).unwrap();
        let right =
            ProviderAdmissionCoordinator::new(RedisAdmissionStore::new(executor.clone()), &prefix)
                .unwrap();
        let key = left.key_for(&lane()).unwrap();
        executor.delete(&key).unwrap();
        left.admit(
            lane(),
            policy(),
            request("basis:BTCUSDT", AdmissionPriority::Batch),
            100,
        )
        .unwrap();
        let deferred = right
            .record_rate_limit(
                lane(),
                policy(),
                Some("basis:BTCUSDT".into()),
                RateLimitSignal {
                    http_status: Some(418),
                    provider_code: None,
                    retry_after_ns: Some(50_000),
                },
                101,
            )
            .unwrap();
        assert_eq!(deferred.defer_reason, Some(AdmissionDeferReason::Cooldown));
        assert_eq!(
            left.admit(
                lane(),
                policy(),
                request("basis:ETHUSDT", AdmissionPriority::Batch),
                102
            )
            .unwrap()
            .defer_reason,
            Some(AdmissionDeferReason::Cooldown)
        );
        executor.delete(&key).unwrap();
        assert!(executor.get(&key).unwrap().is_none());
    }
}
