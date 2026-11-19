//! Data-plane identity, the native twin of `qdl/security/data_plane.py`,
//! `qdl/security/policy.py:ServiceTokenVerifier` and `qdl/security/grpc.py`.
//!
//! The order of checks and the resulting status codes follow the Python
//! implementation, so a consumer sees the same outcome from either Stream.
//! Timestamps follow PyJWT 2.13 (the version in the running image): zero
//! leeway, `iat` must not be in the future, `nbf` is checked when present and
//! a token is expired when `exp <= now`. The library only verifies the
//! signature, issuer and audience; the time rules are applied here.

use crate::bundle::{Bundle, Manifest};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use ring::digest;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

pub const STREAM_READ: &str = "stream:read";
pub const SNAPSHOT_READ: &str = "snapshot:read";
pub const HISTORY_READ: &str = "history:read";
pub const STATUS_READ: &str = "status:read";

/// Python `DataPlaneAccessError` status classes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccessError {
    Unauthenticated(String),
    PermissionDenied(String),
    RateLimited(String),
    Unavailable(String),
    /// A `ValueError` raised inside an access check (the handler prefixes it).
    Invalid(String),
}

impl AccessError {
    pub fn to_status(&self) -> tonic::Status {
        match self {
            AccessError::Unauthenticated(detail) => tonic::Status::unauthenticated(detail),
            AccessError::PermissionDenied(detail) => tonic::Status::permission_denied(detail),
            AccessError::RateLimited(detail) => tonic::Status::resource_exhausted(detail),
            AccessError::Unavailable(detail) => tonic::Status::unavailable(detail),
            AccessError::Invalid(detail) => tonic::Status::invalid_argument(detail),
        }
    }
}

pub struct JwtConfig {
    pub environment: String,
    pub issuer: String,
    pub audience: String,
    pub keys: BTreeMap<String, (DecodingKey, Vec<Algorithm>)>,
    pub algorithms: Vec<Algorithm>,
    pub max_lifetime_seconds: i64,
    pub subjects_by_key_id: BTreeMap<String, String>,
    pub environments_by_key_id: BTreeMap<String, String>,
}

const OID_RSA_ENCRYPTION: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01];
const OID_EC_PUBLIC_KEY: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01];
const OID_P256: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07];

/// One DER TLV: returns (tag, contents, rest).
fn der_tlv(input: &[u8]) -> Result<(u8, &[u8], &[u8]), String> {
    let (&tag, rest) = input.split_first().ok_or("truncated DER")?;
    let (&first, rest) = rest.split_first().ok_or("truncated DER length")?;
    let (length, rest) = if first & 0x80 == 0 {
        (usize::from(first), rest)
    } else {
        let count = usize::from(first & 0x7f);
        if count == 0 || count > 4 || rest.len() < count {
            return Err("unsupported DER length".into());
        }
        let length = rest[..count]
            .iter()
            .fold(0usize, |value, byte| (value << 8) | usize::from(*byte));
        (length, &rest[count..])
    };
    if rest.len() < length {
        return Err("truncated DER value".into());
    }
    Ok((tag, &rest[..length], &rest[length..]))
}

/// Public key from a PEM SubjectPublicKeyInfo, without the `use_pem` feature
/// of jsonwebtoken (it pulls `time` >= 0.3.47, which needs a newer toolchain
/// than the pinned 1.82, while older `time` carries RUSTSEC-2026-0009).
/// RSA yields the PKCS#1 key for `from_rsa_der`; P-256 yields the point for
/// `from_ec_der`.
pub fn public_key_from_pem(pem: &str) -> Result<(Algorithm, DecodingKey), String> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::SubjectPublicKeyInfoDer;

    let spki = SubjectPublicKeyInfoDer::from_pem_slice(pem.as_bytes())
        .map_err(|error| format!("PEM: {error}"))?;
    let (tag, body, _) = der_tlv(spki.as_ref())?;
    if tag != 0x30 {
        return Err("SPKI is not a SEQUENCE".into());
    }
    let (tag, algorithm, rest) = der_tlv(body)?;
    if tag != 0x30 {
        return Err("SPKI algorithm is not a SEQUENCE".into());
    }
    let (tag, key_bits, _) = der_tlv(rest)?;
    if tag != 0x03 || key_bits.first() != Some(&0) {
        return Err("SPKI key is not a whole-byte BIT STRING".into());
    }
    let key = &key_bits[1..];
    let (tag, oid, parameters) = der_tlv(algorithm)?;
    if tag != 0x06 {
        return Err("SPKI algorithm has no OID".into());
    }
    if oid == OID_RSA_ENCRYPTION {
        return Ok((Algorithm::RS256, DecodingKey::from_rsa_der(key)));
    }
    if oid == OID_EC_PUBLIC_KEY {
        let (tag, curve, _) = der_tlv(parameters)?;
        if tag != 0x06 || curve != OID_P256 {
            return Err("only P-256 EC keys are allowed".into());
        }
        return Ok((Algorithm::ES256, DecodingKey::from_ec_der(key)));
    }
    Err("unsupported public key algorithm".into())
}

impl JwtConfig {
    /// Explicit key realms share public market storage, never consumer identity.
    /// Missing mapping retains the single-realm compatibility behavior.
    pub fn with_key_environments_json(mut self, raw: &str) -> Result<Self, String> {
        let mapping: Option<BTreeMap<String, String>> =
            serde_json::from_str(raw).map_err(|error| error.to_string())?;
        if let Some(mapping) = mapping {
            if mapping.keys().ne(self.keys.keys())
                || mapping
                    .values()
                    .any(|value| !matches!(value.as_str(), "paper" | "sandbox" | "live"))
            {
                return Err("JWT key-realm bindings must cover exactly the keyring".into());
            }
            self.environments_by_key_id = mapping;
        }
        Ok(self)
    }

    pub fn key_environment(&self, key_id: &str) -> &str {
        self.environments_by_key_id
            .get(key_id)
            .map(String::as_str)
            .unwrap_or(&self.environment)
    }

    /// Same inputs as `DataPlaneSecurityConfig.from_environment`: a kid ->
    /// PEM public key map and a kid -> subject map that must cover it.
    pub fn from_json(
        environment: &str,
        issuer: &str,
        audience: &str,
        keys_json: &str,
        subjects_json: &str,
        algorithms: &str,
        max_lifetime_seconds: i64,
    ) -> Result<Self, String> {
        let algorithms = algorithms
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|name| match name {
                "RS256" => Ok(Algorithm::RS256),
                "ES256" => Ok(Algorithm::ES256),
                other => Err(format!("unsupported JWT algorithm {other}")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if algorithms.is_empty() {
            return Err("an explicit signed JWT algorithm allowlist is required".into());
        }
        let keys_raw: BTreeMap<String, String> =
            serde_json::from_str(keys_json).map_err(|error| error.to_string())?;
        let subjects: BTreeMap<String, String> =
            serde_json::from_str(subjects_json).map_err(|error| error.to_string())?;
        if keys_raw.is_empty() || keys_raw.keys().ne(subjects.keys()) {
            return Err(
                "data-plane JWT key-subject bindings must cover exactly the keyring".into(),
            );
        }
        let mut keys = BTreeMap::new();
        for (kid, pem) in keys_raw {
            let (algorithm, key) = public_key_from_pem(&pem)
                .map_err(|error| format!("JWT key {kid} is not a usable public key: {error}"))?;
            if !algorithms.contains(&algorithm) {
                return Err(format!("JWT key {kid} algorithm is outside the allowlist"));
            }
            let usable = vec![algorithm];
            keys.insert(kid, (key, usable));
        }
        Ok(Self {
            environment: environment.to_lowercase(),
            issuer: issuer.to_owned(),
            audience: audience.to_owned(),
            keys,
            algorithms,
            max_lifetime_seconds,
            subjects_by_key_id: subjects,
            environments_by_key_id: BTreeMap::new(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub subject: String,
    pub environment: String,
    pub roles: BTreeSet<String>,
    pub key_id: String,
    pub consumer_manifest_revision: Option<u64>,
}

fn now_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs_f64())
        .unwrap_or_default()
}

/// `int(claim)` for a JSON number. Python integers are unbounded, so the
/// value is widened to i128 (a float is truncated, saturating beyond the i128
/// range) and every later comparison or difference is exact or checked: an
/// extreme `iat`/`exp` can never wrap into an acceptable lifetime (KN-1 F2).
/// Strings and booleans, which `int()` would also coerce, are refused: the
/// native side fails closed there.
fn claim_integer(claims: &Value, name: &str) -> Result<i128, String> {
    let invalid = || format!("{name} must be an integer");
    match &claims[name] {
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                return Ok(i128::from(value));
            }
            if let Some(value) = number.as_u64() {
                return Ok(i128::from(value));
            }
            number
                .as_f64()
                .filter(|value| value.is_finite())
                .map(|value| value.trunc() as i128)
                .ok_or_else(invalid)
        }
        _ => Err(invalid()),
    }
}

/// `expires_at <= issued_at or expires_at - issued_at > max_lifetime`, with
/// the difference checked: an i128 overflow is a lifetime beyond any policy.
fn lifetime_exceeds_policy(issued_at: i128, expires_at: i128, max_lifetime_seconds: i64) -> bool {
    expires_at <= issued_at
        || expires_at
            .checked_sub(issued_at)
            .is_none_or(|lifetime| lifetime > i128::from(max_lifetime_seconds))
}

/// `ServiceTokenVerifier.verify` + the PyJWT claim rules it relies on.
pub fn verify_token(config: &JwtConfig, token: &str, now: f64) -> Result<Principal, String> {
    let header = decode_header(token).map_err(|_| "workload token verification failed")?;
    let key_id = header.kid.clone().unwrap_or_default();
    let (key, usable) = config
        .keys
        .get(&key_id)
        .filter(|(_, usable)| {
            config.algorithms.contains(&header.alg) && usable.contains(&header.alg)
        })
        .ok_or("untrusted workload token key or algorithm")?;
    let _ = usable;
    let mut validation = Validation::new(header.alg);
    validation.leeway = 0;
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.set_issuer(&[config.issuer.as_str()]);
    validation.set_audience(&[config.audience.as_str()]);
    validation.set_required_spec_claims(&["sub", "iss", "aud", "exp"]);
    let claims = decode::<Value>(token, key, &validation)
        .map_err(|_| "workload token verification failed")?
        .claims;
    for name in ["sub", "iss", "aud", "exp", "iat", "jti", "environment"] {
        if claims.get(name).is_none() {
            return Err("workload token verification failed".into());
        }
    }
    if !claims["jti"].is_string() || !claims["sub"].is_string() {
        return Err("workload token verification failed".into());
    }
    let issued_at = claim_integer(&claims, "iat")?;
    let expires_at = claim_integer(&claims, "exp")?;
    if issued_at as f64 > now {
        return Err("workload token verification failed".into());
    }
    if claims.get("nbf").is_some() && claim_integer(&claims, "nbf")? as f64 > now {
        return Err("workload token verification failed".into());
    }
    if expires_at as f64 <= now {
        return Err("workload token verification failed".into());
    }
    if lifetime_exceeds_policy(issued_at, expires_at, config.max_lifetime_seconds) {
        return Err("workload token lifetime exceeds policy".into());
    }
    let environment = claims["environment"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| claims["environment"].to_string());
    if environment != config.key_environment(&key_id) {
        return Err("workload token environment mismatch".into());
    }
    let roles: BTreeSet<String> = claims["roles"]
        .as_array()
        .map(|values| {
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string())
                })
                .collect()
        })
        .unwrap_or_default();
    // DL v2.4.0 S8: role claims are accepted and ignored. The signing key is
    // bound to the manifest subject and the manifest's permissions are the
    // only gate (`Access::require_permission`), as in the Python service.
    let consumer_manifest_revision = match claims.get("consumer_manifest_revision") {
        None | Some(Value::Null) => None,
        Some(value) => {
            let revision = value
                .as_u64()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
                .ok_or("workload token manifest revision is invalid")?;
            if revision < 1 {
                return Err("workload token manifest revision is invalid".into());
            }
            Some(revision)
        }
    };
    Ok(Principal {
        subject: claims["sub"].as_str().unwrap_or_default().to_owned(),
        environment,
        roles,
        key_id,
        consumer_manifest_revision,
    })
}

/// Request quota. DL v2.4.0 S7: a coarse per-replica guard; `refund` returns
/// a unit a later refusal must not keep (default: nothing to return).
pub trait RequestQuota: Send + Sync {
    fn consume(&self, manifest: &Manifest) -> Result<(), AccessError>;

    fn refund(&self, _manifest: &Manifest) {}
}

/// Per-replica in-memory token bucket per consumer identity (DL v2.4.0 S7).
///
/// Rate: `requests_per_minute * share / 60` per second, with a burst of
/// `burst_seconds` of that rate (at least 10 units). With `share` = 1/replicas
/// the fleet total equals the manifest's minute quota. No network call: a
/// Redis outage can no longer refuse every stream open. Identities named by
/// `full_share_prefixes` (default `trading-system.`) get their full rate on
/// every replica: the Trading System writer's traffic is not balanced across
/// replicas (same rule as the Python Query service).
pub struct LocalTokenBucketQuota {
    share: f64,
    burst_seconds: f64,
    full_share_prefixes: Vec<String>,
    buckets: std::sync::Mutex<BTreeMap<String, (f64, std::time::Instant)>>,
}

impl LocalTokenBucketQuota {
    pub fn new(share: f64, burst_seconds: f64) -> Result<Self, String> {
        Self::with_full_share(share, burst_seconds, vec!["trading-system.".into()])
    }

    pub fn with_full_share(
        share: f64,
        burst_seconds: f64,
        full_share_prefixes: Vec<String>,
    ) -> Result<Self, String> {
        if !(share > 0.0 && share <= 1.0 && (1.0..=600.0).contains(&burst_seconds)) {
            return Err("local quota share must be in (0, 1] and burst within 1..600 s".into());
        }
        Ok(Self {
            share,
            burst_seconds,
            full_share_prefixes: full_share_prefixes
                .into_iter()
                .filter(|item| !item.is_empty())
                .collect(),
            buckets: std::sync::Mutex::new(BTreeMap::new()),
        })
    }

    fn shape(&self, manifest: &Manifest) -> (f64, f64) {
        let share = if self
            .full_share_prefixes
            .iter()
            .any(|prefix| manifest.consumer_id.starts_with(prefix.as_str()))
        {
            1.0
        } else {
            self.share
        };
        let rate = manifest.quotas.requests_per_minute as f64 * share / 60.0;
        (rate, (rate * self.burst_seconds).max(10.0))
    }

    fn take(&self, manifest: &Manifest, now: std::time::Instant) -> bool {
        let (rate, capacity) = self.shape(manifest);
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let entry = buckets
            .entry(manifest.consumer_id.clone())
            .or_insert((capacity, now));
        let elapsed = now.saturating_duration_since(entry.1).as_secs_f64();
        entry.0 = (entry.0 + elapsed * rate).min(capacity);
        entry.1 = now;
        if entry.0 >= 1.0 {
            entry.0 -= 1.0;
            true
        } else {
            false
        }
    }
}

impl RequestQuota for LocalTokenBucketQuota {
    fn consume(&self, manifest: &Manifest) -> Result<(), AccessError> {
        if self.take(manifest, std::time::Instant::now()) {
            Ok(())
        } else {
            Err(AccessError::RateLimited(
                "consumer request quota is exhausted".into(),
            ))
        }
    }

    fn refund(&self, manifest: &Manifest) {
        let (_, capacity) = self.shape(manifest);
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(entry) = buckets.get_mut(&manifest.consumer_id) {
            entry.0 = (entry.0 + 1.0).min(capacity);
        }
    }
}

/// The local guard plus an optional shared Redis minute counter. A Redis
/// failure is not a refusal (the local guard already admitted the request);
/// only a Redis "exhausted" answer refuses, and returns the local unit.
pub struct LayeredQuota {
    pub local: LocalTokenBucketQuota,
    pub shared: Option<RedisMinuteQuota>,
}

impl RequestQuota for LayeredQuota {
    fn consume(&self, manifest: &Manifest) -> Result<(), AccessError> {
        self.local.consume(manifest)?;
        if let Some(shared) = &self.shared {
            match shared.consume(manifest) {
                Ok(()) | Err(AccessError::Unavailable(_)) => {}
                Err(error) => {
                    self.local.refund(manifest);
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    fn refund(&self, manifest: &Manifest) {
        self.local.refund(manifest);
    }
}

const MINUTE_QUOTA_LUA: &str = r"
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
if count > tonumber(ARGV[1]) then
  return {0, count}
end
return {1, count}
";

pub struct RedisMinuteQuota {
    client: redis::Client,
    prefix: String,
}

impl RedisMinuteQuota {
    pub fn new(url: &str, prefix: &str) -> Result<Self, String> {
        let prefix = prefix.trim_matches(|c| c == ':' || c == ' ').to_owned();
        if !prefix.starts_with("qdl:beta:v2:") && !prefix.starts_with("qdl:stable:v2:") {
            return Err("shared quota requires a dedicated beta or stable Redis prefix".into());
        }
        Ok(Self {
            client: redis::Client::open(url).map_err(|error| error.to_string())?,
            prefix,
        })
    }

    pub fn key(&self, consumer_id: &str, minute: u64) -> String {
        let hashed = digest::digest(&digest::SHA256, consumer_id.as_bytes());
        let identity: String = hashed.as_ref()[..12]
            .iter()
            .fold(String::new(), |mut hex, byte| {
                use std::fmt::Write as _;
                let _ = write!(hex, "{byte:02x}");
                hex
            });
        format!("{}:quota:minute:{identity}:{minute}", self.prefix)
    }
}

impl RequestQuota for RedisMinuteQuota {
    fn consume(&self, manifest: &Manifest) -> Result<(), AccessError> {
        let minute = (now_seconds() / 60.0) as u64;
        let unavailable = || AccessError::Unavailable("shared request quota is unavailable".into());
        let mut connection = self
            .client
            .get_connection_with_timeout(std::time::Duration::from_millis(500))
            .map_err(|_| unavailable())?;
        let _ = connection.set_read_timeout(Some(std::time::Duration::from_millis(500)));
        let result: (i64, i64) = redis::Script::new(MINUTE_QUOTA_LUA)
            .key(self.key(&manifest.consumer_id, minute))
            .arg(manifest.quotas.requests_per_minute)
            .arg(120_000)
            .invoke(&mut connection)
            .map_err(|_| unavailable())?;
        if result.0 != 1 {
            return Err(AccessError::RateLimited(
                "consumer request quota is exhausted".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Access {
    pub principal: Principal,
    pub manifest: Manifest,
    pub purpose: String,
}

impl Access {
    /// `DataPlaneAccess.require_permission(STREAM_READ)`.
    pub fn require_stream_read(&self) -> Result<(), AccessError> {
        self.require_permission(STREAM_READ)
    }

    /// `DataPlaneAccess.require_permission`: the manifest must list the
    /// permission (DL v2.4.0 S8: token role claims are not consulted).
    pub fn require_permission(&self, permission: &str) -> Result<(), AccessError> {
        if !self
            .manifest
            .allowed_permissions
            .iter()
            .any(|item| item == permission)
        {
            return Err(AccessError::PermissionDenied(format!(
                "consumer is not entitled to {permission}"
            )));
        }
        Ok(())
    }

    /// `DataPlaneAccess.require_consumer`.
    pub fn require_consumer(&self, consumer_id: &str) -> Result<(), AccessError> {
        if consumer_id != self.manifest.consumer_id {
            return Err(AccessError::PermissionDenied(
                "authenticated workload is not bound to the requested consumer".into(),
            ));
        }
        Ok(())
    }

    /// `DataPlaneAccess.require_stream_buffer`.
    pub fn require_stream_buffer(&self, size: u64) -> Result<(), AccessError> {
        if size < 1 || size > self.manifest.quotas.max_buffer_events {
            return Err(AccessError::PermissionDenied(
                "stream buffer exceeds the registered consumer quota".into(),
            ));
        }
        Ok(())
    }

    /// `DataPlaneAccess.require_feed_scope` (Replay: the cursor's product).
    pub fn require_feed_scope(&self, instrument_uid: &str, feed: &str) -> Result<(), AccessError> {
        if !self
            .manifest
            .requirements
            .iter()
            .any(|item| item.instrument_uid == instrument_uid && item.feed == feed)
        {
            return Err(AccessError::PermissionDenied(
                "stream cursor scope is outside the registered consumer manifest".into(),
            ));
        }
        Ok(())
    }
}

/// `DataPlaneIdentityService.authenticate` + the gRPC interceptor purpose rule.
pub fn authenticate(
    config: &JwtConfig,
    bundle: &Bundle,
    quota: &dyn RequestQuota,
    authorization: &str,
    consumer_id: &str,
    purpose: &str,
) -> Result<Access, AccessError> {
    let bearer = authorization
        .strip_prefix("Bearer ")
        .map(str::trim)
        .unwrap_or_default();
    if bearer.is_empty() {
        return Err(AccessError::Unauthenticated(
            "workload bearer token is required".into(),
        ));
    }
    let principal =
        verify_token(config, bearer, now_seconds()).map_err(AccessError::Unauthenticated)?;
    let manifest = bundle
        .manifest_by_subject(&principal.environment, &principal.subject)
        .ok_or_else(|| AccessError::Unauthenticated("consumer manifest is not registered".into()))?
        .clone();
    if config.subjects_by_key_id.get(&principal.key_id) != Some(&principal.subject) {
        return Err(AccessError::Unauthenticated(
            "workload token signing key is not bound to the manifest subject".into(),
        ));
    }
    // Exact revision unless the bundle carries a purely additive accept-set
    // (DL v2.3.0 `min_token_revision`), the same rule as the Python service.
    if !manifest.accepts_token_revision(principal.consumer_manifest_revision) {
        return Err(AccessError::Unauthenticated(
            "workload token is not bound to the active consumer manifest revision".into(),
        ));
    }
    if consumer_id != manifest.consumer_id {
        return Err(AccessError::PermissionDenied(
            "authenticated workload is not bound to the requested consumer".into(),
        ));
    }
    let purpose = purpose.to_ascii_uppercase();
    if purpose.is_empty() || purpose == "UNSPECIFIED" {
        return Err(AccessError::PermissionDenied(
            "purpose cannot be UNSPECIFIED".into(),
        ));
    }
    if !manifest.allowed_purposes.contains(&purpose) {
        return Err(AccessError::PermissionDenied(
            "consumer manifest does not allow the requested data purpose".into(),
        ));
    }
    // DL v2.4.0 S3: the quota is charged only after authentication, the
    // manifest binding and the purpose rule admitted the request; a refusal
    // above never spends a unit.
    quota.consume(&manifest)?;
    Ok(Access {
        principal,
        manifest,
        purpose,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use ring::rand::SystemRandom;
    use ring::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};

    /// ES256 keys generated per test run: no private key lives in Git.
    fn keypair() -> (EncodingKey, DecodingKey) {
        let rng = SystemRandom::new();
        let pkcs8 =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).expect("generate");
        let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
            .expect("parse");
        (
            EncodingKey::from_ec_der(pkcs8.as_ref()),
            DecodingKey::from_ec_der(pair.public_key().as_ref()),
        )
    }

    fn config(decoding: DecodingKey) -> JwtConfig {
        JwtConfig {
            environments_by_key_id: BTreeMap::new(),
            environment: "paper".into(),
            issuer: "https://identity.test".into(),
            audience: "qdl-v2-stable".into(),
            keys: BTreeMap::from([("k1".into(), (decoding, vec![Algorithm::ES256]))]),
            algorithms: vec![Algorithm::ES256],
            max_lifetime_seconds: 900,
            subjects_by_key_id: BTreeMap::from([("k1".into(), "spiffe://test/alpha".into())]),
        }
    }

    fn token(encoding: &EncodingKey, kid: &str, claims: Value) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(kid.into());
        encode(&header, &claims, encoding).expect("encode")
    }

    fn claims(now: i64) -> Value {
        serde_json::json!({
            "sub": "spiffe://test/alpha", "iss": "https://identity.test", "aud": "qdl-v2-stable",
            "iat": now, "exp": now + 300, "jti": "j1", "environment": "paper",
            "roles": ["stream_consumer"], "consumer_manifest_revision": 3
        })
    }

    #[test]
    fn pinned_key_realms_do_not_cross_paper_sandbox_live() {
        for realm in ["paper", "sandbox", "live"] {
            let (encoding, decoding) = keypair();
            let config = config(decoding)
                .with_key_environments_json(&serde_json::json!({"k1": realm}).to_string())
                .unwrap();
            let now = now_seconds().floor() as i64;
            for claimed in ["paper", "sandbox", "live"] {
                let mut value = claims(now);
                value["environment"] = claimed.into();
                let result = verify_token(&config, &token(&encoding, "k1", value), now as f64);
                assert_eq!(result.is_ok(), realm == claimed);
            }
        }
    }

    #[test]
    fn key_realm_mapping_requires_exact_keys_and_known_realms() {
        for raw in ["{}", r#"{"other":"live"}"#, r#"{"k1":"*"}"#, "[]"] {
            let (_, decoding) = keypair();
            assert!(config(decoding).with_key_environments_json(raw).is_err());
        }
        let (_, decoding) = keypair();
        assert!(config(decoding).with_key_environments_json("null").is_ok());
    }

    #[test]
    fn a_valid_token_verifies_and_every_claim_rule_refuses() {
        let (encoding, decoding) = keypair();
        let config = config(decoding);
        let now = now_seconds().floor() as i64;
        let ok =
            verify_token(&config, &token(&encoding, "k1", claims(now)), now as f64).expect("valid");
        assert_eq!(ok.consumer_manifest_revision, Some(3));
        let mutate = |f: &dyn Fn(&mut Value)| {
            let mut value = claims(now);
            f(&mut value);
            verify_token(&config, &token(&encoding, "k1", value), now as f64)
        };
        assert!(mutate(&|c| c["aud"] = "other".into()).is_err(), "audience");
        assert!(
            mutate(&|c| c["iss"] = "https://evil".into()).is_err(),
            "issuer"
        );
        assert!(
            mutate(&|c| c["exp"] = now.into()).is_err(),
            "exp == now is expired"
        );
        assert!(
            mutate(&|c| c["iat"] = (now + 5).into()).is_err(),
            "iat in the future"
        );
        assert!(
            mutate(&|c| c["nbf"] = (now + 5).into()).is_err(),
            "nbf in the future"
        );
        assert!(
            mutate(&|c| c["exp"] = (now + 901).into()).is_err(),
            "lifetime"
        );
        assert!(
            mutate(&|c| c["environment"] = "live".into()).is_err(),
            "environment"
        );
        // DL v2.4.0 S8: role claims are accepted and ignored.
        assert!(
            mutate(&|c| c["roles"] = serde_json::json!(["root"])).is_ok(),
            "unknown role is ignored"
        );
        assert!(
            mutate(&|c| c["roles"] = serde_json::json!([])).is_ok(),
            "empty roles are ignored"
        );
        assert!(
            mutate(&|c| {
                c.as_object_mut().expect("object").remove("roles");
            })
            .is_ok(),
            "absent roles are ignored"
        );
        assert!(
            mutate(&|c| {
                c.as_object_mut().expect("object").remove("jti");
            })
            .is_err(),
            "jti required"
        );
        assert!(
            mutate(&|c| c["consumer_manifest_revision"] = 0.into()).is_err(),
            "revision"
        );
        assert!(
            verify_token(&config, &token(&encoding, "k9", claims(now)), now as f64).is_err(),
            "unknown kid"
        );
        let (other, _) = keypair();
        assert!(
            verify_token(&config, &token(&other, "k1", claims(now)), now as f64).is_err(),
            "wrong signing key"
        );
    }

    #[test]
    fn extreme_issued_at_or_expiry_is_a_lifetime_refusal_never_an_overflow() {
        let (encoding, decoding) = keypair();
        let config = config(decoding);
        let now = now_seconds().floor() as i64;
        let lifetime = Err("workload token lifetime exceeds policy".to_owned());
        let verify = |iat: Value, exp: Value| {
            let mut value = claims(now);
            value["iat"] = iat;
            value["exp"] = exp;
            verify_token(&config, &token(&encoding, "k1", value), now as f64)
                .map(|principal| principal.subject)
        };
        // exp - iat overflowed i64 and wrapped into an accepted lifetime.
        assert_eq!(verify(i64::MIN.into(), (now + 100).into()), lifetime);
        assert_eq!(
            verify(serde_json::json!(-1e300), (now + 100).into()),
            lifetime
        );
        assert_eq!(verify((-1_i64).into(), (now + 100).into()), lifetime);
        // A far-future exp is a lifetime refusal too (int() is unbounded).
        // A non-integer exp is refused one step earlier here: jsonwebtoken
        // parses a required `exp` as u64. Python refuses the same token at
        // the lifetime rule; both are an authentication failure.
        assert_eq!(
            verify(now.into(), serde_json::json!(1e300)),
            Err("workload token verification failed".to_owned())
        );
        assert_eq!(verify(now.into(), u64::MAX.into()), lifetime);
        assert_eq!(verify(i64::MIN.into(), u64::MAX.into()), lifetime);
        // A float within policy truncates exactly like int().
        assert!(verify(serde_json::json!(now as f64 + 0.9), (now + 300).into()).is_ok());
    }

    #[test]
    fn extreme_not_before_never_panics_and_only_a_past_value_passes() {
        let (encoding, decoding) = keypair();
        let config = config(decoding);
        let now = now_seconds().floor() as i64;
        let verify = |nbf: Value| {
            let mut value = claims(now);
            value["nbf"] = nbf;
            verify_token(&config, &token(&encoding, "k1", value), now as f64)
        };
        // Far past (as int() sees it) is accepted; far future is refused.
        assert!(verify(i64::MIN.into()).is_ok());
        assert!(verify(serde_json::json!(-1e300)).is_ok());
        assert!(verify(i64::MAX.into()).is_err());
        assert!(verify(u64::MAX.into()).is_err());
        assert!(verify(serde_json::json!(1e300)).is_err());
        assert!(verify((now + 1).into()).is_err());
        assert!(verify(now.into()).is_ok());
    }

    #[test]
    fn lifetime_arithmetic_is_checked_at_the_i128_edges() {
        assert!(lifetime_exceeds_policy(i128::MIN, i128::MAX, 900));
        assert!(lifetime_exceeds_policy(i128::MAX, i128::MIN, 900));
        assert!(lifetime_exceeds_policy(0, 901, 900));
        assert!(!lifetime_exceeds_policy(0, 900, 900));
        assert!(lifetime_exceeds_policy(5, 5, 900));
    }

    fn spki_pem(point: &[u8]) -> String {
        use base64::Engine as _;
        let mut algorithm = vec![0x06, OID_EC_PUBLIC_KEY.len() as u8];
        algorithm.extend_from_slice(OID_EC_PUBLIC_KEY);
        algorithm.extend_from_slice(&[0x06, OID_P256.len() as u8]);
        algorithm.extend_from_slice(OID_P256);
        let mut bits = vec![0x03, (point.len() + 1) as u8, 0x00];
        bits.extend_from_slice(point);
        let mut body = vec![0x30, algorithm.len() as u8];
        body.extend_from_slice(&algorithm);
        body.extend_from_slice(&bits);
        let mut spki = vec![0x30, body.len() as u8];
        spki.extend_from_slice(&body);
        format!(
            "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----\n",
            base64::engine::general_purpose::STANDARD.encode(spki)
        )
    }

    #[test]
    fn a_pem_spki_key_from_config_verifies_a_real_signature() {
        let rng = SystemRandom::new();
        let pkcs8 =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).expect("generate");
        let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
            .expect("parse");
        let keys = serde_json::json!({"k1": spki_pem(pair.public_key().as_ref())}).to_string();
        let subjects = serde_json::json!({"k1": "spiffe://test/alpha"}).to_string();
        let mut config = JwtConfig::from_json(
            "paper",
            "https://identity.test",
            "qdl-v2-stable",
            &keys,
            &subjects,
            "ES256",
            900,
        )
        .expect("config from PEM");
        config.subjects_by_key_id = BTreeMap::from([("k1".into(), "spiffe://test/alpha".into())]);
        let now = now_seconds().floor() as i64;
        let encoding = EncodingKey::from_ec_der(pkcs8.as_ref());
        assert!(verify_token(&config, &token(&encoding, "k1", claims(now)), now as f64).is_ok());
        assert!(
            JwtConfig::from_json("paper", "i", "a", &keys, &subjects, "RS256", 900).is_err(),
            "an EC key is refused when only RS256 is allowed"
        );
        assert!(public_key_from_pem(
            "-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n"
        )
        .is_err());
    }

    #[test]
    fn a_disallowed_algorithm_is_refused_before_the_signature() {
        let (_, decoding) = keypair();
        let config = config(decoding);
        let now = now_seconds().floor() as i64;
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("k1".into());
        let hs = encode(&header, &claims(now), &EncodingKey::from_secret(b"guess")).expect("hs");
        assert_eq!(
            verify_token(&config, &hs, now as f64).unwrap_err(),
            "untrusted workload token key or algorithm"
        );
    }

    fn quota_manifest(rpm: u64) -> Manifest {
        Manifest {
            consumer_id: "alpha.okx.paper.stable".into(),
            subject: "spiffe://test/alpha".into(),
            environment: "paper".into(),
            manifest_revision: 3,
            min_token_revision: 1,
            allowed_purposes: vec!["INTERNAL_ALPHA".into()],
            allowed_permissions: vec![STREAM_READ.into()],
            quotas: crate::bundle::Quotas {
                requests_per_minute: rpm,
                max_batch_items: 10,
                max_warmup_rows: 100,
                max_streams: 4,
                max_buffer_events: 100,
            },
            requirements: Vec::new(),
        }
    }

    #[test]
    fn the_local_bucket_admits_its_burst_refills_and_refunds() {
        // 1,200 rpm at share 0.5 = 10/s per replica, burst 2 s = 20 units.
        let quota = LocalTokenBucketQuota::new(0.5, 2.0).expect("quota");
        let manifest = quota_manifest(1_200);
        let start = std::time::Instant::now();
        for _ in 0..20 {
            assert!(quota.take(&manifest, start));
        }
        assert!(!quota.take(&manifest, start), "burst exhausted");
        quota.refund(&manifest);
        assert!(quota.take(&manifest, start), "a refunded unit is usable");
        assert!(!quota.take(&manifest, start));
        let later = start + std::time::Duration::from_millis(500);
        for _ in 0..5 {
            assert!(quota.take(&manifest, later), "10/s refills 5 in 500 ms");
        }
        assert!(!quota.take(&manifest, later));
        assert!(LocalTokenBucketQuota::new(0.0, 2.0).is_err());
        assert!(LocalTokenBucketQuota::new(0.5, 0.5).is_err());
    }

    #[test]
    fn the_trading_system_identity_gets_its_full_rate_per_replica() {
        let quota = LocalTokenBucketQuota::new(0.5, 2.0).expect("quota");
        let mut ts = quota_manifest(1_200);
        ts.consumer_id = "trading-system.paper.stable".into();
        let now = std::time::Instant::now();
        let taken = (0..100).take_while(|_| quota.take(&ts, now)).count();
        assert_eq!(taken, 40, "20/s x 2 s burst, not the alpha 10/s x 2 s");
    }

    #[test]
    fn a_redis_outage_does_not_refuse_and_the_local_guard_still_holds() {
        // Port 1 refuses connections: the shared layer is "unavailable".
        let quota = LayeredQuota {
            local: LocalTokenBucketQuota::new(1.0, 1.0).expect("local"),
            shared: Some(
                RedisMinuteQuota::new("redis://127.0.0.1:1/0", "qdl:stable:v2:test")
                    .expect("redis"),
            ),
        };
        let manifest = quota_manifest(600); // 10/s, burst max(10, 10) = 10
        for _ in 0..10 {
            assert_eq!(quota.consume(&manifest), Ok(()));
        }
        assert!(matches!(
            quota.consume(&manifest),
            Err(AccessError::RateLimited(_))
        ));
    }

    #[test]
    fn a_purpose_refusal_spends_no_quota() {
        struct Counting(std::sync::atomic::AtomicUsize);
        impl RequestQuota for Counting {
            fn consume(&self, _manifest: &Manifest) -> Result<(), AccessError> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        }
        let (encoding, decoding) = keypair();
        let config = config(decoding);
        let now = now_seconds().floor() as i64;
        let mut manifest = quota_manifest(600);
        manifest.subject = "spiffe://test/alpha".into();
        let bundle = Bundle {
            sha256: String::new(),
            environment: "paper".into(),
            catalog_revision: 1,
            source_policy_revision: 1,
            canonical_stream: "canonical".into(),
            bindings: Vec::new(),
            manifests: vec![manifest.clone()],
        };
        let counting = Counting(std::sync::atomic::AtomicUsize::new(0));
        let bearer = format!("Bearer {}", token(&encoding, "k1", claims(now)));
        let refused = authenticate(
            &config,
            &bundle,
            &counting,
            &bearer,
            &manifest.consumer_id,
            "INTERNAL_EXECUTION",
        );
        assert!(matches!(refused, Err(AccessError::PermissionDenied(_))));
        assert_eq!(counting.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        let admitted = authenticate(
            &config,
            &bundle,
            &counting,
            &bearer,
            &manifest.consumer_id,
            "INTERNAL_ALPHA",
        );
        assert!(admitted.is_ok(), "{admitted:?}");
        assert_eq!(counting.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn the_quota_key_matches_the_python_key() {
        let quota = RedisMinuteQuota::new("redis://127.0.0.1:1/0", "qdl:stable:v2:paper:candidate")
            .expect("quota");
        // Literal from Python: hashlib.sha256(b"alpha.okx.paper.stable").hexdigest()[:24]
        assert_eq!(
            quota.key("alpha.okx.paper.stable", 29836320),
            "qdl:stable:v2:paper:candidate:quota:minute:e5bd8020a90433f281beda1e:29836320"
        );
    }
}
