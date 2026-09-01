//! Private Rust-authoritative admission endpoint hosted by the existing core.
//!
//! Query roles use this small HTTP/1.1 relay only on `stable_internal` before
//! their Python vendor adapter starts the fragile Binance native-basis call.
//! The protocol deliberately has no public port, no vendor payloads and no
//! Python policy.  Rust plus Redis owns every admission transition.

use std::collections::BTreeMap;
use std::env;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use crate::provider_admission::{
    AtomicAdmissionStore, ProviderAdmissionCoordinator, RedisAdmissionStore,
    RedisClientAdmissionExecutor,
};
use qdl_core::provider_admission::{
    ProviderAdmissionRequest, ProviderLaneKey, ProviderLanePolicy, RateLimitSignal,
    PROVIDER_ADMISSION_AUTHORITY, PROVIDER_ADMISSION_SCHEMA,
};
use ring::hmac;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const POLICY_SCHEMA: &str = "qdl.provider_admission.policy.v1";
const INTERNAL_PATH: &str = "/internal/provider-admission/v1";
const SIGNATURE_HEADER: &str = "x-qdl-stable-signature";
const MAX_HEADER_BYTES: usize = 8 * 1024;
const MAX_BODY_BYTES: usize = 16 * 1024;

#[derive(Clone)]
pub struct ProviderAdmissionService<S> {
    coordinator: ProviderAdmissionCoordinator<S>,
    policies: BTreeMap<ProviderLaneKey, ProviderLanePolicy>,
}

impl<S> ProviderAdmissionService<S> {
    pub fn new(
        coordinator: ProviderAdmissionCoordinator<S>,
        policies: BTreeMap<ProviderLaneKey, ProviderLanePolicy>,
    ) -> Result<Self, String> {
        if policies.is_empty() {
            return Err("provider admission policy declares no lanes".into());
        }
        for (lane, policy) in &policies {
            lane.validate().map_err(|error| error.to_string())?;
            policy.validate().map_err(|error| error.to_string())?;
        }
        Ok(Self {
            coordinator,
            policies,
        })
    }

    fn policy(&self, lane: &ProviderLaneKey) -> Result<ProviderLanePolicy, String> {
        self.policies
            .get(lane)
            .cloned()
            .ok_or_else(|| "provider admission lane is not declared by immutable policy".into())
    }
}

impl<S: AtomicAdmissionStore> ProviderAdmissionService<S> {
    pub fn execute_at(&self, request: AdmissionWireRequest, now_ns: i64) -> Result<Value, String> {
        request.validate_schema()?;
        match request {
            AdmissionWireRequest::Admit { lane, request, .. } => {
                let policy = self.policy(&lane)?;
                let decision = self
                    .coordinator
                    .admit(lane, policy, request, now_ns)
                    .map_err(|error| error.to_string())?;
                serde_json::to_value(decision).map_err(|error| error.to_string())
            }
            AdmissionWireRequest::Complete {
                lane, request_id, ..
            } => {
                let policy = self.policy(&lane)?;
                let completed = self
                    .coordinator
                    .complete(lane.clone(), policy, request_id.clone(), now_ns)
                    .map_err(|error| error.to_string())?;
                Ok(json!({
                    "schema": PROVIDER_ADMISSION_SCHEMA,
                    "authority": PROVIDER_ADMISSION_AUTHORITY,
                    "operation": "COMPLETE",
                    "lane": lane,
                    "request_id": request_id,
                    "completed": completed,
                }))
            }
            AdmissionWireRequest::RateLimit {
                lane,
                request_id,
                signal,
                ..
            } => {
                let policy = self.policy(&lane)?;
                let decision = self
                    .coordinator
                    .record_rate_limit(lane, policy, request_id, signal, now_ns)
                    .map_err(|error| error.to_string())?;
                serde_json::to_value(decision).map_err(|error| error.to_string())
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(
    tag = "operation",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub enum AdmissionWireRequest {
    Admit {
        schema: String,
        lane: ProviderLaneKey,
        request: ProviderAdmissionRequest,
    },
    Complete {
        schema: String,
        lane: ProviderLaneKey,
        request_id: String,
    },
    RateLimit {
        schema: String,
        lane: ProviderLaneKey,
        #[serde(default)]
        request_id: Option<String>,
        signal: RateLimitSignal,
    },
}

impl AdmissionWireRequest {
    fn validate_schema(&self) -> Result<(), String> {
        let schema = match self {
            Self::Admit { schema, .. }
            | Self::Complete { schema, .. }
            | Self::RateLimit { schema, .. } => schema,
        };
        if schema != PROVIDER_ADMISSION_SCHEMA {
            return Err("provider admission wire schema is unsupported".into());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyDocument {
    schema: String,
    revision: u64,
    lanes: Vec<PolicyLane>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyLane {
    lane: ProviderLaneKey,
    policy: ProviderLanePolicy,
}

impl PolicyDocument {
    fn load(
        path: &Path,
        expected_sha256: &str,
    ) -> Result<(BTreeMap<ProviderLaneKey, ProviderLanePolicy>, String), String> {
        let bytes = std::fs::read(path)
            .map_err(|error| format!("provider admission policy is unreadable: {error}"))?;
        let actual_sha256 = hex::encode(Sha256::digest(&bytes));
        if actual_sha256 != expected_sha256 {
            return Err("provider admission policy SHA-256 does not match sealed config".into());
        }
        let document: Self = serde_json::from_slice(&bytes)
            .map_err(|error| format!("provider admission policy is invalid: {error}"))?;
        if document.schema != POLICY_SCHEMA || document.revision == 0 || document.lanes.is_empty() {
            return Err("provider admission policy schema/revision/lanes are invalid".into());
        }
        let mut lanes = BTreeMap::new();
        for item in document.lanes {
            item.lane.validate().map_err(|error| error.to_string())?;
            item.policy.validate().map_err(|error| error.to_string())?;
            if lanes.insert(item.lane, item.policy).is_some() {
                return Err("provider admission policy contains a duplicate lane".into());
            }
        }
        Ok((lanes, actual_sha256))
    }
}

#[derive(Clone)]
struct AdmissionHttpState {
    service: ProviderAdmissionService<RedisAdmissionStore<RedisClientAdmissionExecutor>>,
    executor: RedisClientAdmissionExecutor,
    secret: Arc<Vec<u8>>,
}

pub struct ProviderAdmissionServer {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), String>>,
}

impl ProviderAdmissionServer {
    pub async fn start_from_environment() -> Result<Option<Self>, String> {
        if !enabled_from_environment()? {
            return Ok(None);
        }
        let secret = required("QDL_PROVIDER_ADMISSION_SECRET")?.into_bytes();
        if secret.len() < 32 {
            return Err("provider admission secret must contain at least 256 bits".into());
        }
        let policy_path = required("QDL_PROVIDER_ADMISSION_POLICY_PATH")?;
        let policy_sha256 = required_sha256("QDL_PROVIDER_ADMISSION_POLICY_SHA256")?;
        let prefix = required("QDL_PROVIDER_ADMISSION_REDIS_PREFIX")?;
        if !prefix.starts_with("qdl:stable:v2:") {
            return Err(
                "provider admission Redis prefix must stay inside stable V2 namespace".into(),
            );
        }
        let listen_addr: SocketAddr = required("QDL_PROVIDER_ADMISSION_LISTEN_ADDR")?
            .parse()
            .map_err(|_| "provider admission listen address is invalid".to_owned())?;
        if listen_addr.port() == 0 {
            return Err("provider admission listen port must be non-zero".into());
        }
        let executor =
            RedisClientAdmissionExecutor::new(&required("QDL_PROVIDER_ADMISSION_REDIS_URL")?)
                .map_err(|error| error.to_string())?;
        let (policies, actual_sha256) =
            PolicyDocument::load(Path::new(&policy_path), &policy_sha256)?;
        let coordinator = ProviderAdmissionCoordinator::new(
            RedisAdmissionStore::new(executor.clone()),
            prefix.clone(),
        )
        .map_err(|error| error.to_string())?;
        let state = AdmissionHttpState {
            service: ProviderAdmissionService::new(coordinator, policies)?,
            executor,
            secret: Arc::new(secret),
        };
        let listener = TcpListener::bind(listen_addr)
            .await
            .map_err(|error| format!("provider admission bind failed: {error}"))?;
        let (shutdown, receiver) = oneshot::channel();
        println!(
            "{}",
            json!({
                "event": "qdl_provider_admission_started",
                "authority": PROVIDER_ADMISSION_AUTHORITY,
                "lane_count": state.service.policies.len(),
                "policy_sha256": actual_sha256,
                "redis_prefix": prefix,
                "private_path": INTERNAL_PATH,
            })
        );
        let task = tokio::spawn(serve(listener, state, receiver));
        Ok(Some(Self {
            shutdown: Some(shutdown),
            task,
        }))
    }

    pub async fn stop(mut self) -> Result<(), String> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task
            .await
            .map_err(|error| format!("provider admission server join failed: {error}"))?
    }
}

async fn serve(
    listener: TcpListener,
    state: AdmissionHttpState,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<(), String> {
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _peer) = accepted.map_err(|error| format!("provider admission accept failed: {error}"))?;
                let connection_state = state.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, connection_state).await;
                });
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, state: AdmissionHttpState) -> Result<(), String> {
    let response = match read_request(&mut stream).await {
        Ok(request) => match execute_request(request, state).await {
            Ok(value) => (200, value),
            Err(error) => (error.status, json!({"error": error.code})),
        },
        Err(error) => (error.status, json!({"error": error.code})),
    };
    write_response(&mut stream, response.0, &response.1).await
}

async fn execute_request(
    request: RawAdmissionRequest,
    state: AdmissionHttpState,
) -> Result<Value, AdmissionHttpError> {
    verify_signature(&request.signature, &request.body, &state.secret)?;
    let request: AdmissionWireRequest =
        serde_json::from_slice(&request.body).map_err(|_| AdmissionHttpError::bad_request())?;
    let now_ns = {
        let executor = state.executor.clone();
        tokio::task::spawn_blocking(move || executor.time_ns())
            .await
            .map_err(|_| AdmissionHttpError::internal())?
            .map_err(|_| AdmissionHttpError::unavailable())?
    };
    let service = state.service.clone();
    tokio::task::spawn_blocking(move || service.execute_at(request, now_ns))
        .await
        .map_err(|_| AdmissionHttpError::internal())?
        .map_err(|_| AdmissionHttpError::unavailable())
}

struct RawAdmissionRequest {
    signature: String,
    body: Vec<u8>,
}

async fn read_request(stream: &mut TcpStream) -> Result<RawAdmissionRequest, AdmissionHttpError> {
    let mut bytes = Vec::with_capacity(MAX_HEADER_BYTES);
    let mut chunk = [0_u8; 2048];
    let header_end = loop {
        if let Some(offset) = find_header_end(&bytes) {
            break offset;
        }
        if bytes.len() >= MAX_HEADER_BYTES {
            return Err(AdmissionHttpError::too_large());
        }
        let count = stream
            .read(&mut chunk)
            .await
            .map_err(|_| AdmissionHttpError::bad_request())?;
        if count == 0 {
            return Err(AdmissionHttpError::bad_request());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > MAX_HEADER_BYTES + MAX_BODY_BYTES {
            return Err(AdmissionHttpError::too_large());
        }
    };
    let (signature, content_length) = parse_request_head(&bytes[..header_end])?;
    if content_length > MAX_BODY_BYTES {
        return Err(AdmissionHttpError::too_large());
    }
    let body_start = header_end + 4;
    while bytes.len() < body_start + content_length {
        let count = stream
            .read(&mut chunk)
            .await
            .map_err(|_| AdmissionHttpError::bad_request())?;
        if count == 0 {
            return Err(AdmissionHttpError::bad_request());
        }
        if bytes.len().saturating_add(count) > body_start + content_length {
            return Err(AdmissionHttpError::bad_request());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    if bytes.len() != body_start + content_length {
        return Err(AdmissionHttpError::bad_request());
    }
    Ok(RawAdmissionRequest {
        signature,
        body: bytes[body_start..].to_vec(),
    })
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|value| value == b"\r\n\r\n")
}

fn parse_request_head(head: &[u8]) -> Result<(String, usize), AdmissionHttpError> {
    let text = std::str::from_utf8(head).map_err(|_| AdmissionHttpError::bad_request())?;
    let mut lines = text.split("\r\n");
    if lines.next() != Some("POST /internal/provider-admission/v1 HTTP/1.1") {
        return Err(AdmissionHttpError::not_found());
    }
    let mut signature = None;
    let mut content_length = None;
    let mut content_type = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            return Err(AdmissionHttpError::bad_request());
        };
        let value = value.trim();
        match name.to_ascii_lowercase().as_str() {
            SIGNATURE_HEADER => {
                if signature.replace(value.to_owned()).is_some() {
                    return Err(AdmissionHttpError::bad_request());
                }
            }
            "content-length" => {
                if content_length.replace(value.to_owned()).is_some() {
                    return Err(AdmissionHttpError::bad_request());
                }
            }
            "content-type" => {
                if content_type.replace(value.to_owned()).is_some() {
                    return Err(AdmissionHttpError::bad_request());
                }
            }
            "transfer-encoding" => return Err(AdmissionHttpError::bad_request()),
            _ => {}
        }
    }
    let signature = signature.ok_or_else(AdmissionHttpError::unauthorized)?;
    if content_type.as_deref() != Some("application/json") {
        return Err(AdmissionHttpError::bad_request());
    }
    let content_length = content_length
        .ok_or_else(AdmissionHttpError::bad_request)?
        .parse::<usize>()
        .map_err(|_| AdmissionHttpError::bad_request())?;
    Ok((signature, content_length))
}

async fn write_response(stream: &mut TcpStream, status: u16, value: &Value) -> Result<(), String> {
    let body = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(|error| format!("provider admission response header write failed: {error}"))?;
    stream
        .write_all(&body)
        .await
        .map_err(|error| format!("provider admission response body write failed: {error}"))?;
    stream
        .shutdown()
        .await
        .map_err(|error| format!("provider admission response shutdown failed: {error}"))
}

fn verify_signature(signature: &str, body: &[u8], secret: &[u8]) -> Result<(), AdmissionHttpError> {
    let encoded = signature
        .strip_prefix("sha256=")
        .ok_or_else(AdmissionHttpError::unauthorized)?;
    let provided = hex::decode(encoded).map_err(|_| AdmissionHttpError::unauthorized())?;
    if provided.len() != 32
        || hmac::verify(&hmac::Key::new(hmac::HMAC_SHA256, secret), body, &provided).is_err()
    {
        return Err(AdmissionHttpError::unauthorized());
    }
    Ok(())
}

#[derive(Debug)]
struct AdmissionHttpError {
    status: u16,
    code: &'static str,
}

impl AdmissionHttpError {
    fn unauthorized() -> Self {
        Self {
            status: 401,
            code: "UNAUTHORIZED",
        }
    }

    fn bad_request() -> Self {
        Self {
            status: 400,
            code: "BAD_REQUEST",
        }
    }

    fn not_found() -> Self {
        Self {
            status: 404,
            code: "NOT_FOUND",
        }
    }

    fn too_large() -> Self {
        Self {
            status: 413,
            code: "PAYLOAD_TOO_LARGE",
        }
    }

    fn unavailable() -> Self {
        Self {
            status: 503,
            code: "UNAVAILABLE",
        }
    }

    fn internal() -> Self {
        Self {
            status: 500,
            code: "INTERNAL_ERROR",
        }
    }
}

fn enabled_from_environment() -> Result<bool, String> {
    match env::var("QDL_PROVIDER_ADMISSION_ENABLED") {
        Err(env::VarError::NotPresent) => Ok(false),
        Ok(value) if value == "true" => Ok(true),
        Ok(value) if value == "false" => Ok(false),
        Ok(_) => Err("QDL_PROVIDER_ADMISSION_ENABLED must be true or false".into()),
        Err(env::VarError::NotUnicode(_)) => {
            Err("QDL_PROVIDER_ADMISSION_ENABLED is not UTF-8".into())
        }
    }
}

fn required(name: &str) -> Result<String, String> {
    let value =
        env::var(name).map_err(|_| format!("required environment variable is missing: {name}"))?;
    if value.trim().is_empty() {
        return Err(format!("required environment variable is empty: {name}"));
    }
    Ok(value)
}

fn required_sha256(name: &str) -> Result<String, String> {
    let value = required(name)?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(format!("{name} must be a lowercase SHA-256"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider_admission::InMemoryAdmissionStore;
    use qdl_core::provider_admission::{
        AdmissionDisposition, AdmissionPriority, ProviderAdmissionDecision,
    };

    fn lane() -> ProviderLaneKey {
        ProviderLaneKey::new("BINANCE", "USDM", "REFERENCE_NATIVE_BASIS").unwrap()
    }

    fn policy() -> ProviderLanePolicy {
        ProviderLanePolicy {
            token_capacity: 2,
            refill_tokens: 1,
            refill_interval_ns: 1_000,
            max_inflight: 1,
            reserved_realtime_inflight: 0,
            max_lease_ns: 5_000,
            default_cooldown_ns: 10_000,
            idle_ttl_ns: 10_000,
        }
    }

    fn service() -> ProviderAdmissionService<InMemoryAdmissionStore> {
        let coordinator =
            ProviderAdmissionCoordinator::new(InMemoryAdmissionStore::default(), "qdl:test:c36-c2")
                .unwrap();
        ProviderAdmissionService::new(coordinator, BTreeMap::from([(lane(), policy())])).unwrap()
    }

    fn admit(request_id: &str) -> AdmissionWireRequest {
        AdmissionWireRequest::Admit {
            schema: PROVIDER_ADMISSION_SCHEMA.into(),
            lane: lane(),
            request: ProviderAdmissionRequest {
                request_id: request_id.into(),
                priority: AdmissionPriority::Batch,
                token_cost: 1,
            },
        }
    }

    #[test]
    fn private_wire_runs_admit_complete_and_rate_limit_in_rust() {
        let service = service();
        let granted: ProviderAdmissionDecision =
            serde_json::from_value(service.execute_at(admit("basis:BTCUSDT"), 100).unwrap())
                .unwrap();
        assert_eq!(granted.disposition, AdmissionDisposition::Granted);
        let complete = service
            .execute_at(
                AdmissionWireRequest::Complete {
                    schema: PROVIDER_ADMISSION_SCHEMA.into(),
                    lane: lane(),
                    request_id: "basis:BTCUSDT".into(),
                },
                101,
            )
            .unwrap();
        assert_eq!(complete["completed"], true);
        let rate_limited: ProviderAdmissionDecision = serde_json::from_value(
            service
                .execute_at(
                    AdmissionWireRequest::RateLimit {
                        schema: PROVIDER_ADMISSION_SCHEMA.into(),
                        lane: lane(),
                        request_id: None,
                        signal: RateLimitSignal {
                            http_status: Some(418),
                            provider_code: None,
                            retry_after_ns: Some(2_000),
                        },
                    },
                    102,
                )
                .unwrap(),
        )
        .unwrap();
        assert_eq!(rate_limited.disposition, AdmissionDisposition::Deferred);
        let deferred: ProviderAdmissionDecision =
            serde_json::from_value(service.execute_at(admit("basis:ETHUSDT"), 103).unwrap())
                .unwrap();
        assert_eq!(deferred.disposition, AdmissionDisposition::Deferred);
    }

    #[test]
    fn wire_schema_unknown_lane_and_request_head_fail_closed() {
        let service = service();
        assert!(service
            .execute_at(
                AdmissionWireRequest::Admit {
                    schema: "wrong".into(),
                    lane: lane(),
                    request: ProviderAdmissionRequest {
                        request_id: "basis:BTCUSDT".into(),
                        priority: AdmissionPriority::Batch,
                        token_cost: 1,
                    },
                },
                100,
            )
            .is_err());
        assert!(service
            .execute_at(
                AdmissionWireRequest::Admit {
                    schema: PROVIDER_ADMISSION_SCHEMA.into(),
                    lane: ProviderLaneKey::new("OKX", "SWAP", "REFERENCE_NATIVE_BASIS").unwrap(),
                    request: ProviderAdmissionRequest {
                        request_id: "basis:BTC-USDT-SWAP".into(),
                        priority: AdmissionPriority::Batch,
                        token_cost: 1,
                    },
                },
                100,
            )
            .is_err());
        assert!(
            parse_request_head(b"GET /internal/provider-admission/v1 HTTP/1.1\r\n\r\n").is_err()
        );
        assert!(parse_request_head(b"POST /internal/provider-admission/v1 HTTP/1.1\r\ncontent-type: application/json\r\ncontent-length: 2\r\nx-qdl-stable-signature: sha256=00\r\ntransfer-encoding: chunked\r\n").is_err());
    }

    #[test]
    fn hmac_rejects_missing_and_invalid_values() {
        let secret = b"01234567890123456789012345678901";
        assert!(verify_signature("", b"{}", secret).is_err());
        assert!(verify_signature("sha256=00", b"{}", secret).is_err());
    }
}
