//! `qdl-stream-gateway` binary (KN-1 slice).
//!
//! `qdl-stream-gateway serve` runs the authenticated gRPC stream on mTLS;
//! `qdl-stream-gateway probe-latest <physical-key>` prints the latest
//! committed record for a key (partition, offset, event id) so the harness
//! can issue a real cursor. Configuration is environment-only; secrets are
//! read from mounted files named by the environment, never logged.

use prost::Message as _;
use qdl_contracts::cursor_v3::{CursorV3Codec, CursorV3Expectation};
use qdl_stream_gateway::auth::{JwtConfig, RedisMinuteQuota};
use qdl_stream_gateway::bundle::Bundle;
use qdl_stream_gateway::generated::marketdata_v2::EventEnvelope;
use qdl_stream_gateway::generated::query_v2::market_data_stream_service_server::MarketDataStreamServiceServer;
use qdl_stream_gateway::reader::{latest_for_key, KafkaSettings};
use qdl_stream_gateway::service::{Gateway, GatewayState};
use qdl_stream_gateway::tls;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

fn env(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is required"))
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn read(path_variable: &str) -> Result<String, String> {
    let path = env(path_variable)?;
    std::fs::read_to_string(&path).map_err(|error| format!("{path_variable}={path}: {error}"))
}

fn kafka_settings() -> Result<KafkaSettings, String> {
    let tls = match std::env::var("QDL_KN_KAFKA_CA") {
        Ok(ca) => Some((ca, env("QDL_KN_KAFKA_CERT")?, env("QDL_KN_KAFKA_KEY")?)),
        Err(_) => None,
    };
    Ok(KafkaSettings {
        bootstrap: env("QDL_KN_KAFKA_BOOTSTRAP")?,
        topic: env_or("QDL_KN_KAFKA_TOPIC", "md.canonical.v2"),
        group_id: env_or("QDL_KN_KAFKA_GROUP", "kn-stream-reader"),
        client_id: env_or("QDL_KN_KAFKA_CLIENT_ID", "qdl-kn1-stream-gateway"),
        tls,
        fetch_wait_ms: env_or("QDL_KN_KAFKA_FETCH_WAIT_MS", "10")
            .parse()
            .map_err(|_| "QDL_KN_KAFKA_FETCH_WAIT_MS must be an integer")?,
    })
}

fn cursor_codec() -> Result<CursorV3Codec, String> {
    let raw: BTreeMap<String, String> = serde_json::from_str(&read("QDL_KN_CURSOR_KEYS_FILE")?)
        .map_err(|error| error.to_string())?;
    let keys = raw
        .into_iter()
        .map(|(id, hex)| {
            let bytes = (0..hex.len())
                .step_by(2)
                .map(|index| u8::from_str_radix(hex.get(index..index + 2).unwrap_or("zz"), 16))
                .collect::<Result<Vec<u8>, _>>()
                .map_err(|_| format!("cursor key {id} is not hex"))?;
            Ok((id, bytes))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    CursorV3Codec::new(&keys, &env("QDL_KN_CURSOR_ACTIVE_KEY_ID")?)
}

async fn serve() -> Result<(), String> {
    let bundle = Bundle::parse(&read("QDL_KN_BUNDLE_FILE")?)?;
    let environment = env_or("QDL_ENVIRONMENT", "paper").to_lowercase();
    if bundle.environment != environment {
        return Err("gateway bundle environment differs from QDL_ENVIRONMENT".into());
    }
    let jwt = JwtConfig::from_json(
        &environment,
        &env("QDL_DATA_JWT_ISSUER")?,
        &env("QDL_DATA_JWT_AUDIENCE")?,
        &env("QDL_DATA_JWT_KEYS_JSON")?,
        &env("QDL_DATA_JWT_KEY_SUBJECTS_JSON")?,
        &env_or("QDL_DATA_JWT_ALGORITHMS", "RS256,ES256"),
        env_or("QDL_DATA_JWT_MAX_LIFETIME_SECONDS", "900")
            .parse()
            .map_err(|_| "QDL_DATA_JWT_MAX_LIFETIME_SECONDS must be an integer")?,
    )?;
    let quota = RedisMinuteQuota::new(
        &env("QDL_KN_QUOTA_REDIS_URL")?,
        &env("QDL_KN_QUOTA_PREFIX")?,
    )?;
    let expectation = CursorV3Expectation {
        environment: environment.clone(),
        stream: bundle.canonical_stream.clone(),
        source_topic_id: env("QDL_KN_TOPIC_ID")?,
        partition_plan_epoch: env_or("QDL_KN_PARTITION_PLAN_EPOCH", "1")
            .parse()
            .map_err(|_| "QDL_KN_PARTITION_PLAN_EPOCH must be an integer")?,
        source_policy_revision: bundle.source_policy_revision,
        catalog_revision: bundle.catalog_revision,
        route_generation: env("QDL_KN_ROUTE_GENERATION")?,
        schema_major: 2,
    };
    let state = Arc::new(GatewayState::new(
        jwt,
        bundle,
        Arc::new(quota),
        cursor_codec()?,
        expectation,
        kafka_settings()?,
        env_or("QDL_KN_CURSOR_TTL_SECONDS", "3600")
            .parse()
            .map_err(|_| "QDL_KN_CURSOR_TTL_SECONDS must be an integer")?,
    ));
    let tls_config = tls::server_config(
        &read("QDL_KN_TLS_CERT_FILE")?,
        &read("QDL_KN_TLS_KEY_FILE")?,
        &read("QDL_KN_TLS_CLIENT_CA_FILE")?,
    )?;
    let reporter = state.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            let metrics = &reporter.metrics;
            println!(
                "{}",
                serde_json::json!({
                    "event": "qdl_kn_gateway_metrics",
                    "bundle_sha256": reporter.bundle.sha256,
                    "subscriptions_opened": metrics.subscriptions_opened.load(Ordering::Relaxed),
                    "subscriptions_active": metrics.subscriptions_active.load(Ordering::Relaxed),
                    "delivered": metrics.delivered.load(Ordering::Relaxed),
                    "other_product": metrics.other_product.load(Ordering::Relaxed),
                    "too_old": metrics.too_old.load(Ordering::Relaxed),
                    "overflow": metrics.overflow.load(Ordering::Relaxed),
                    "refused": metrics.refused.load(Ordering::Relaxed),
                })
            );
        }
    });
    let address = env_or("QDL_KN_LISTEN", "0.0.0.0:8210")
        .parse()
        .map_err(|_| "QDL_KN_LISTEN must be host:port")?;
    println!(
        "{}",
        serde_json::json!({"event": "qdl_kn_gateway_start", "listen": env_or("QDL_KN_LISTEN", "0.0.0.0:8210"),
            "bundle_sha256": state.bundle.sha256, "route_generation": state.expectation.route_generation})
    );
    let incoming = tls::incoming(address, tls_config)
        .await
        .map_err(|error| format!("listen: {error}"))?;
    Server::builder()
        .add_service(MarketDataStreamServiceServer::new(Gateway { state }))
        .serve_with_incoming_shutdown(incoming, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|error| error.to_string())
}

fn probe_latest(physical_key: &str) -> Result<(), String> {
    let settings = kafka_settings()?;
    let latest = latest_for_key(
        &settings,
        physical_key.as_bytes(),
        20_000,
        Duration::from_secs(30),
    )?
    .ok_or("no committed record for the key in the scanned window")?;
    let envelope =
        EventEnvelope::decode(latest.payload.as_slice()).map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::json!({
            "physical_key": physical_key,
            "partition": latest.partition,
            "offset": latest.offset,
            "event_id": envelope.event_id.iter().fold(String::new(), |mut hex, byte| {
                use std::fmt::Write as _;
                let _ = write!(hex, "{byte:02x}");
                hex
            }),
            "source_event_time_ns": envelope.source_event_time_ns,
        })
    );
    Ok(())
}

/// Least-privilege check: a reader must not be able to commit offsets. The
/// principal holds DESCRIBE (not READ) on `kn-` groups, so the broker must
/// refuse this commit; if it ever succeeded it would only mark a throwaway
/// `kn-` group, never a production one (guarded in `KafkaSettings::validate`).
fn acl_probe() -> Result<(), String> {
    use rdkafka::consumer::{CommitMode, Consumer};
    let settings = kafka_settings()?;
    let consumer = settings.base_consumer()?;
    let mut list = rdkafka::TopicPartitionList::new();
    list.add_partition_offset(&settings.topic, 0, rdkafka::Offset::Offset(0))
        .map_err(|error| error.to_string())?;
    let result = consumer.commit(&list, CommitMode::Sync);
    println!(
        "{}",
        serde_json::json!({
            "group": settings.group_id,
            "commit_refused": result.is_err(),
            "error": result.err().map(|error| error.to_string()),
        })
    );
    Ok(())
}

#[tokio::main]
async fn main() {
    let arguments: Vec<String> = std::env::args().collect();
    let result = match arguments.get(1).map(String::as_str) {
        Some("serve") | None => serve().await,
        Some("probe-latest") => arguments
            .get(2)
            .ok_or_else(|| "probe-latest needs a physical key".to_owned())
            .and_then(|key| probe_latest(key)),
        Some("acl-probe") => acl_probe(),
        Some(other) => Err(format!("unknown command {other}")),
    };
    if let Err(error) = result {
        eprintln!(
            "{}",
            serde_json::json!({"event": "qdl_kn_gateway_error", "error": error})
        );
        std::process::exit(2);
    }
}
