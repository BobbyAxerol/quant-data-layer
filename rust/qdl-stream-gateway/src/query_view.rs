//! KN-4 D29: GetSnapshot/GetFeedStatus answered by the paired Query replica.
//!
//! The gateway runs its own access sequence first (service.rs), then asks the
//! Query replica's private read view (`qdl/runtime/kn_read_view.py`), which
//! runs the unchanged Python oracle over the market cache and signs the
//! cursor v3. One semantic owner for quality, warmup enforcement and cursor
//! issuance; Subscribe and Replay never depend on it.
//!
//! Protocol: `POST <url>/internal/v2/kn/read-view`, JSON
//! `{"schema","kind","consumer_id","requirement": base64(DataRequirement)}`,
//! header `X-QDL-Stable-Signature: sha256=<hex HMAC-SHA256(secret, body)>`,
//! mutual TLS with the gateway's own certificate (serverAuth + clientAuth) and
//! the configured CA. Replies: 200 protobuf message; 409 `{"code","detail"}`
//! (typed refusal -> FAILED_PRECONDITION "{code}:{detail}", RATE_LIMITED ->
//! RESOURCE_EXHAUSTED); 400 `{"detail"}` -> INVALID_ARGUMENT; anything else,
//! or every target unreachable -> UNAVAILABLE `DEPENDENCY_UNAVAILABLE:...`
//! (the SDK retries it). Targets are tried in rotating order.
//!
//! Configuration (unset `QDL_KN_READ_VIEW_URLS` keeps `NotReadyReadView`):
//! `QDL_KN_READ_VIEW_URLS` (comma-separated `https://host:port`),
//! `QDL_KN_READ_VIEW_SECRET_FILE` (hex, >= 32 bytes), `QDL_KN_READ_VIEW_CA_FILE`,
//! the client identity `QDL_KN_TLS_CERT_FILE`/`QDL_KN_TLS_KEY_FILE`,
//! `QDL_KN_READ_VIEW_TIMEOUT_MS` (default 30000).

use crate::generated::query_v2 as query;
use crate::readview::{ReadView, ReadViewError, ReadViewStatus};
use crate::requirement::StreamRequirement;
use base64::Engine as _;
use prost::Message;
use ring::hmac;
use rustls::crypto::ring::default_provider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub const READ_VIEW_PATH: &str = "/internal/v2/kn/read-view";
pub const READ_VIEW_SCHEMA: &str = "qdl.v2.kn-read-view.v1";

pub struct QueryReadView {
    targets: Vec<String>,
    client: reqwest::Client,
    key: hmac::Key,
    next: AtomicUsize,
}

/// `sha256=<lowercase hex>` of HMAC-SHA256 over the exact body
/// (`qdl.runtime.internal_auth.stable_hmac_signature`).
pub fn signature(key: &hmac::Key, body: &[u8]) -> String {
    let tag = hmac::sign(key, body);
    let mut text = String::with_capacity(7 + 64);
    text.push_str("sha256=");
    for byte in tag.as_ref() {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// The request body; field order is irrelevant to the server, the bytes
/// sent are the bytes signed.
pub fn request_body(kind: &str, consumer_id: &str, proto: &query::DataRequirement) -> Vec<u8> {
    serde_json::json!({
        "schema": READ_VIEW_SCHEMA,
        "kind": kind,
        "consumer_id": consumer_id,
        "requirement": base64::engine::general_purpose::STANDARD.encode(proto.encode_to_vec()),
    })
    .to_string()
    .into_bytes()
}

/// Map one reply of the read view to a message or a typed refusal.
pub fn map_reply<M: Message + Default>(status: u16, body: &[u8]) -> Result<M, ReadViewError> {
    match status {
        200 => M::decode(body).map_err(|error| {
            ReadViewError::unavailable(format!("read view reply does not decode: {error}"))
        }),
        409 => {
            let value: serde_json::Value = serde_json::from_slice(body).map_err(|_| {
                ReadViewError::unavailable("read view refusal is not JSON".to_owned())
            })?;
            let code = value["code"].as_str().unwrap_or("").to_owned();
            let detail = value["detail"].as_str().unwrap_or("").to_owned();
            if code.is_empty() {
                return Err(ReadViewError::unavailable(
                    "read view refusal has no code".to_owned(),
                ));
            }
            Err(if code == "RATE_LIMITED" {
                ReadViewError {
                    code,
                    detail,
                    status: ReadViewStatus::ResourceExhausted,
                }
            } else {
                ReadViewError::precondition(&code, &detail)
            })
        }
        400 => {
            let value: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
            Err(ReadViewError {
                code: "INVALID_ARGUMENT".into(),
                detail: value["detail"]
                    .as_str()
                    .unwrap_or("INVALID_ARGUMENT:read view refused the request")
                    .to_owned(),
                status: ReadViewStatus::InvalidArgument,
            })
        }
        other => Err(ReadViewError::unavailable(format!(
            "read view answered HTTP {other}"
        ))),
    }
}

fn client_config(cert_pem: &str, key_pem: &str, ca_pem: &str) -> Result<ClientConfig, String> {
    let certs = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read view client certificate: {error}"))?;
    if certs.is_empty() {
        return Err("read view client certificate file holds no certificate".into());
    }
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
        .map_err(|error| format!("read view client key: {error}"))?;
    let mut roots = RootCertStore::empty();
    for ca in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        roots
            .add(ca.map_err(|error| format!("read view CA: {error}"))?)
            .map_err(|error| format!("read view CA: {error}"))?;
    }
    if roots.is_empty() {
        return Err("read view CA file holds no certificate".into());
    }
    let mut config = ClientConfig::builder_with_provider(Arc::new(default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|error| error.to_string())?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|error| format!("read view client identity: {error}"))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

impl QueryReadView {
    pub fn new(
        targets: Vec<String>,
        secret: &[u8],
        cert_pem: &str,
        key_pem: &str,
        ca_pem: &str,
        timeout: Duration,
    ) -> Result<Self, String> {
        if targets.is_empty() {
            return Err("QDL_KN_READ_VIEW_URLS names no target".into());
        }
        for target in &targets {
            if !target.starts_with("https://") || target.ends_with('/') {
                return Err(format!(
                    "read view target must be https://host:port: {target}"
                ));
            }
        }
        if secret.len() < 32 {
            return Err("read view secret must contain at least 256 bits".into());
        }
        let client = reqwest::Client::builder()
            .use_preconfigured_tls(client_config(cert_pem, key_pem, ca_pem)?)
            .timeout(timeout)
            .connect_timeout(Duration::from_secs(2))
            .no_proxy()
            .build()
            .map_err(|error| format!("read view client: {error}"))?;
        Ok(Self {
            targets,
            client,
            key: hmac::Key::new(hmac::HMAC_SHA256, secret),
            next: AtomicUsize::new(0),
        })
    }

    async fn ask<M: Message + Default>(
        &self,
        kind: &str,
        consumer_id: &str,
        proto: &query::DataRequirement,
    ) -> Result<M, ReadViewError> {
        let body = request_body(kind, consumer_id, proto);
        let signed = signature(&self.key, &body);
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let mut last = String::new();
        for index in 0..self.targets.len() {
            let target = &self.targets[(start + index) % self.targets.len()];
            let reply = self
                .client
                .post(format!("{target}{READ_VIEW_PATH}"))
                .header("content-type", "application/json")
                .header("X-QDL-Stable-Signature", &signed)
                .body(body.clone())
                .send()
                .await;
            match reply {
                Ok(response) if response.status().is_server_error() => {
                    last = format!("{target}: HTTP {}", response.status().as_u16());
                }
                Ok(response) => {
                    let status = response.status().as_u16();
                    let bytes = response.bytes().await.map_err(|error| {
                        ReadViewError::unavailable(format!("read view reply: {error}"))
                    })?;
                    return map_reply(status, &bytes);
                }
                Err(error) => last = format!("{target}: {error}"),
            }
        }
        Err(ReadViewError::unavailable(format!(
            "no Query read view answered ({last})"
        )))
    }
}

#[tonic::async_trait]
impl ReadView for QueryReadView {
    async fn snapshot(
        &self,
        _requirement: &StreamRequirement,
        proto: &query::DataRequirement,
        consumer_id: &str,
    ) -> Result<query::GetSnapshotResponse, ReadViewError> {
        self.ask("SNAPSHOT", consumer_id, proto).await
    }

    async fn status(
        &self,
        _requirement: &StreamRequirement,
        proto: &query::DataRequirement,
        consumer_id: &str,
    ) -> Result<query::GetFeedStatusResponse, ReadViewError> {
        self.ask("STATUS", consumer_id, proto).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_signature_is_the_python_stable_signature() {
        // python3: stable_hmac_signature(bytes(range(32)), b'{"a":1}')
        let key = hmac::Key::new(hmac::HMAC_SHA256, &(0u8..32).collect::<Vec<_>>());
        assert_eq!(
            signature(&key, br#"{"a":1}"#),
            "sha256=b4cebde30982443ec36c18fa99ad10a70e705bba779f57c37e84decb64ae50fb"
        );
    }

    #[test]
    fn replies_map_to_the_python_status_codes() {
        let ok = query::GetFeedStatusResponse {
            state: "LIVE".into(),
            ..Default::default()
        };
        let decoded: query::GetFeedStatusResponse = map_reply(200, &ok.encode_to_vec()).unwrap();
        assert_eq!(decoded, ok);
        let refused = map_reply::<query::GetFeedStatusResponse>(
            409,
            br#"{"code":"DATA_NOT_READY","detail":"feed status is unavailable"}"#,
        )
        .unwrap_err();
        let status = refused.to_status();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            status.message(),
            "DATA_NOT_READY:feed status is unavailable"
        );
        let limited = map_reply::<query::GetFeedStatusResponse>(
            409,
            br#"{"code":"RATE_LIMITED","detail":"lane"}"#,
        )
        .unwrap_err()
        .to_status();
        assert_eq!(limited.code(), tonic::Code::ResourceExhausted);
        assert_eq!(limited.message(), "RATE_LIMITED:lane");
        let invalid =
            map_reply::<query::GetFeedStatusResponse>(400, br#"{"detail":"INVALID_ARGUMENT:bad"}"#)
                .unwrap_err()
                .to_status();
        assert_eq!(invalid.code(), tonic::Code::InvalidArgument);
        assert_eq!(invalid.message(), "INVALID_ARGUMENT:bad");
        for status in [401u16, 500, 503] {
            let error = map_reply::<query::GetFeedStatusResponse>(status, b"").unwrap_err();
            assert_eq!(error.to_status().code(), tonic::Code::Unavailable);
            assert!(error
                .to_status()
                .message()
                .starts_with("DEPENDENCY_UNAVAILABLE:"));
        }
    }

    #[test]
    fn the_body_carries_the_exact_proto_requirement() {
        let proto = query::DataRequirement {
            instrument_uid: "u".into(),
            ..Default::default()
        };
        let body: serde_json::Value =
            serde_json::from_slice(&request_body("STATUS", "c", &proto)).unwrap();
        assert_eq!(body["schema"], READ_VIEW_SCHEMA);
        assert_eq!(body["kind"], "STATUS");
        assert_eq!(body["consumer_id"], "c");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(body["requirement"].as_str().unwrap())
            .unwrap();
        assert_eq!(
            query::DataRequirement::decode(bytes.as_slice()).unwrap(),
            proto
        );
    }

    #[test]
    fn service_hostnames_with_underscores_are_valid_tls_names() {
        // The stable SANs name replicas `query_v2_1`/`query_v2_2`.
        for name in ["query_v2_1", "query_v2_2", "qdl-v2-query"] {
            assert!(
                rustls::pki_types::ServerName::try_from(name).is_ok(),
                "{name}"
            );
        }
    }
}
