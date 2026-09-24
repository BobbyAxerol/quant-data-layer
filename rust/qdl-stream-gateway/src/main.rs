//! `qdl-stream-gateway` binary (KN-2 native Stream).
//!
//! `qdl-stream-gateway serve` runs the authenticated gRPC stream on mTLS;
//! `qdl-stream-gateway probe-latest <physical-key>` prints the latest
//! committed record for a key (partition, offset, event id) so the harness
//! can issue a real cursor. Configuration is environment-only; secrets are
//! read from mounted files named by the environment, never logged.

use prost::Message as _;
use qdl_contracts::cursor_v3::{CursorV3Codec, CursorV3Expectation};
use qdl_stream_gateway::auth::{JwtConfig, RedisMinuteQuota};
use qdl_stream_gateway::authority::{Authority, AuthorityHandle};
use qdl_stream_gateway::bundle::Bundle;
use qdl_stream_gateway::generated::marketdata_v2::EventEnvelope;
use qdl_stream_gateway::generated::query_v2::market_data_stream_service_server::MarketDataStreamServiceServer;
use qdl_stream_gateway::hub::{Hub, HubConfig};
use qdl_stream_gateway::memory::{container_memory_limit, MemoryPlan};
use qdl_stream_gateway::reader::{latest_for_key, KafkaLogSource, KafkaRangeSource, KafkaSettings};
use qdl_stream_gateway::readview::NotReadyReadView;
use qdl_stream_gateway::replay::{ReplayCoordinator, ReplayLimits};
use qdl_stream_gateway::service::{Gateway, GatewayState, StreamLimits};
use qdl_stream_gateway::subscription::ByteBudget;
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

fn parsed<T: std::str::FromStr>(name: &str, default: &str) -> Result<T, String> {
    env_or(name, default)
        .parse()
        .map_err(|_| format!("{name} must be a number"))
}

fn jwt_config(environment: &str) -> Result<JwtConfig, String> {
    // A JWT config file makes key rotation/revocation reloadable (D7); the
    // environment variables of the Python service remain the fallback.
    if let Ok(path) = std::env::var("QDL_KN_JWT_CONFIG_FILE") {
        let raw = std::fs::read_to_string(&path)
            .map_err(|error| format!("QDL_KN_JWT_CONFIG_FILE={path}: {error}"))?;
        let value: serde_json::Value =
            serde_json::from_str(&raw).map_err(|error| format!("jwt config: {error}"))?;
        let text = |name: &str| {
            value[name]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("jwt config field {name} must be a string"))
        };
        return JwtConfig::from_json(
            environment,
            &text("issuer")?,
            &text("audience")?,
            &value["keys"].to_string(),
            &value["subjects"].to_string(),
            &text("algorithms")?,
            value["max_lifetime_seconds"]
                .as_i64()
                .ok_or("jwt config max_lifetime_seconds must be an integer")?,
        );
    }
    JwtConfig::from_json(
        environment,
        &env("QDL_DATA_JWT_ISSUER")?,
        &env("QDL_DATA_JWT_AUDIENCE")?,
        &env("QDL_DATA_JWT_KEYS_JSON")?,
        &env("QDL_DATA_JWT_KEY_SUBJECTS_JSON")?,
        &env_or("QDL_DATA_JWT_ALGORITHMS", "RS256,ES256"),
        parsed("QDL_DATA_JWT_MAX_LIFETIME_SECONDS", "900")?,
    )
}

fn load_authority() -> Result<Authority, String> {
    let bundle = Bundle::parse(&read("QDL_KN_BUNDLE_FILE")?)?;
    let environment = env_or("QDL_ENVIRONMENT", "paper").to_lowercase();
    if bundle.environment != environment {
        return Err("gateway bundle environment differs from QDL_ENVIRONMENT".into());
    }
    let expectation = CursorV3Expectation {
        environment: environment.clone(),
        stream: bundle.canonical_stream.clone(),
        source_topic_id: env("QDL_KN_TOPIC_ID")?,
        partition_plan_epoch: parsed("QDL_KN_PARTITION_PLAN_EPOCH", "1")?,
        source_policy_revision: bundle.source_policy_revision,
        catalog_revision: bundle.catalog_revision,
        route_generation: env("QDL_KN_ROUTE_GENERATION")?,
        schema_major: 2,
    };
    Ok(Authority {
        jwt: jwt_config(&environment)?,
        bundle,
        expectation,
    })
}

/// Bytes of every file the authority is built from, to detect a change.
fn authority_sources() -> Vec<u8> {
    ["QDL_KN_BUNDLE_FILE", "QDL_KN_JWT_CONFIG_FILE"]
        .iter()
        .filter_map(|name| std::env::var(name).ok())
        .flat_map(|path| std::fs::read(path).unwrap_or_default())
        .collect()
}

/// Resident set and CPU seconds of this process, for per-replica evidence.
fn process_usage() -> (u64, f64) {
    let rss = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|text| text.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map_or(0, |pages| pages * 4096);
    let cpu = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|text| {
            let fields: Vec<&str> = text.rsplit_once(')')?.1.split_whitespace().collect();
            let user: f64 = fields.get(11)?.parse().ok()?;
            let system: f64 = fields.get(12)?.parse().ok()?;
            Some((user + system) / 100.0)
        })
        .unwrap_or(0.0);
    (rss, cpu)
}

fn limits() -> Result<StreamLimits, String> {
    Ok(StreamLimits {
        cursor_ttl_seconds: parsed("QDL_KN_CURSOR_TTL_SECONDS", "3600")?,
        replay: ReplayLimits {
            max_scanned_records: parsed("QDL_KN_REPLAY_MAX_SCAN_RECORDS", "2000000")?,
            max_scanned_bytes: parsed("QDL_KN_REPLAY_MAX_SCAN_BYTES", "1073741824")?,
            max_duration: Duration::from_millis(parsed("QDL_KN_REPLAY_MAX_MS", "30000")?),
            max_matched: parsed("QDL_KN_MAX_REPLAY_EVENTS", "10000")?,
        },
        catchup_deadline: Duration::from_millis(parsed("QDL_KN_CATCHUP_DEADLINE_MS", "10000")?),
        slow_consumer_after: Duration::from_millis(parsed("QDL_KN_SLOW_CONSUMER_MS", "10000")?),
        queue_bytes_per_subscription: parsed("QDL_KN_QUEUE_BYTES_PER_SUBSCRIPTION", "33554432")?,
        max_subscriptions: parsed("QDL_KN_MAX_SUBSCRIPTIONS", "384")?,
        max_replay_rpcs: parsed("QDL_KN_MAX_REPLAY_RPCS", "32")?,
        replay_page_default: 1_000,
    })
}

async fn serve() -> Result<(), String> {
    let authority = AuthorityHandle::new(load_authority()?);
    let quota = RedisMinuteQuota::new(
        &env("QDL_KN_QUOTA_REDIS_URL")?,
        &env("QDL_KN_QUOTA_PREFIX")?,
    )?;
    let readers: usize = parsed("QDL_KN_REPLAY_READERS", "4")?;
    let limits = limits()?;
    // Defaults fit a 256 MiB replica (K2-T08 sizing); checked, fail closed.
    let memory = MemoryPlan {
        ring_total: parsed("QDL_KN_RING_BYTES_TOTAL", "50331648")?,
        queue_total: parsed("QDL_KN_QUEUE_BYTES_TOTAL", "33554432")?,
        replay_total: parsed("QDL_KN_REPLAY_BYTES_TOTAL", "33554432")?,
        replay_readers: readers as u64,
        streams: (limits.max_subscriptions + limits.max_replay_rpcs) as u64,
        reserve: parsed("QDL_KN_MEMORY_RESERVE_BYTES", "67108864")?,
        limit: container_memory_limit(),
    };
    memory.check()?;
    let settings = kafka_settings()?;
    let (source, starts) =
        KafkaLogSource::open_warm(&settings, parsed("QDL_KN_RING_WARM_RECORDS", "20000")?)?;
    let hub = Arc::new(Hub::new(
        &starts,
        HubConfig {
            ring_max_bytes: memory.ring_per_partition(starts.len()),
            ring_max_age: Duration::from_secs(parsed("QDL_KN_RING_MAX_AGE_SECONDS", "120")?),
        },
    ));
    let reader = hub.run(Box::new(source));
    let budget = ByteBudget::new(memory.queue_total as usize);
    let replay = ReplayCoordinator::new(
        Arc::new(KafkaRangeSource::new(settings, readers)),
        readers,
        limits.replay.clone(),
        ByteBudget::new(memory.replay_total as usize),
    );
    let state = Arc::new(GatewayState::new(
        authority.clone(),
        Arc::new(quota),
        cursor_codec()?,
        hub.clone(),
        replay,
        Arc::new(NotReadyReadView),
        budget,
        limits,
    ));
    let tls_config = tls::server_config(
        &read("QDL_KN_TLS_CERT_FILE")?,
        &read("QDL_KN_TLS_KEY_FILE")?,
        &read("QDL_KN_TLS_CLIENT_CA_FILE")?,
    )?;
    let reload_every = Duration::from_secs(parsed("QDL_KN_AUTHORITY_RELOAD_SECONDS", "5")?);
    let watched = authority.clone();
    tokio::spawn(async move {
        let mut known = authority_sources();
        let mut tick = tokio::time::interval(reload_every);
        loop {
            tick.tick().await;
            let current = authority_sources();
            if current == known {
                continue;
            }
            known = current;
            match load_authority() {
                Ok(next) => {
                    let sha = next.bundle.sha256.clone();
                    watched.replace(next);
                    println!(
                        "{}",
                        serde_json::json!({"event": "qdl_kn_authority_reloaded",
                            "generation": watched.generation(), "bundle_sha256": sha})
                    );
                }
                Err(error) => println!(
                    "{}",
                    serde_json::json!({"event": "qdl_kn_authority_reload_refused", "error": error})
                ),
            }
        }
    });
    let reporter = state.clone();
    let report_every = Duration::from_secs(parsed("QDL_KN_METRICS_SECONDS", "10")?);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(report_every);
        loop {
            tick.tick().await;
            let metrics = &reporter.metrics;
            let hub = &reporter.hub.metrics;
            let replay = &reporter.replay.metrics;
            let load = |value: &std::sync::atomic::AtomicU64| value.load(Ordering::Relaxed);
            let (rss, cpu) = process_usage();
            println!(
                "{}",
                serde_json::json!({
                    "event": "qdl_kn_gateway_metrics",
                    "bundle_sha256": reporter.authority.current().bundle.sha256,
                    "authority_generation": reporter.authority.generation(),
                    "subscriptions_opened": load(&metrics.subscriptions_opened),
                    "subscriptions_active": load(&metrics.subscriptions_active),
                    "hub_subscribers": reporter.hub.subscriber_count(),
                    "delivered": load(&metrics.delivered),
                    "replayed": load(&metrics.replayed),
                    "aged_out_at_read": load(&metrics.aged_out_at_read),
                    "overflow": load(&metrics.overflow),
                    "refused": load(&metrics.refused),
                    "revoked": load(&metrics.revoked),
                    "lagging": load(&metrics.lagging),
                    "expired": load(&metrics.expired),
                    "replay_rpcs": load(&metrics.replay_rpcs),
                    "hub_records": load(&hub.records),
                    "dispatch_lag_ms": hub.dispatch_lag.summary(),
                    "queue_wait_ms": metrics.queue_wait.summary(),
                    "handoff_wait_ms": metrics.handoff_wait.summary(),
                    "hub_bytes": load(&hub.bytes),
                    "hub_duplicates": load(&hub.duplicates),
                    "hub_decode_failures": load(&hub.decode_failures),
                    "hub_offers": load(&hub.offers),
                    "hub_reader_errors": load(&hub.reader_errors),
                    "ring_bytes": reporter.hub.ring_bytes(),
                    "ring_evictions": load(&hub.ring_evictions),
                    "replay_ring_hits": load(&replay.ring_hits),
                    "replay_readers": load(&replay.reader_replays),
                    "replay_active": load(&replay.active),
                    "replay_scanned": load(&replay.scanned),
                    "replay_refused_capacity": load(&replay.refused_capacity),
                    "replay_scan_limited": load(&replay.scan_limited),
                    "replay_readers_free": reporter.replay.available(),
                    "replay_in_flight": reporter.replay.in_flight(),
                    "replay_detached": load(&replay.detached),
                    "queue_bytes": reporter.budget.used(),
                    "rss_bytes": rss,
                    "cpu_seconds": cpu,
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
            "bundle_sha256": state.authority.current().bundle.sha256,
            "route_generation": state.authority.current().expectation.route_generation,
            "partitions": starts, "memory": memory.summary()})
    );
    let incoming = tls::incoming(address, tls_config)
        .await
        .map_err(|error| format!("listen: {error}"))?;
    let stopping = state.clone();
    let served = Server::builder()
        .add_service(MarketDataStreamServiceServer::new(Gateway { state }))
        .serve_with_incoming_shutdown(incoming, async move {
            // SIGTERM (`docker stop`) or SIGINT: end every stream typed and
            // retryable, give the tasks a moment to send it, then stop.
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            println!(
                "{}",
                serde_json::json!({"event": "qdl_kn_gateway_stopping"})
            );
            stopping.shut_down();
            tokio::time::sleep(Duration::from_secs(2)).await;
        })
        .await
        .map_err(|error| error.to_string());
    hub.stop();
    let _ = reader.join();
    served
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
