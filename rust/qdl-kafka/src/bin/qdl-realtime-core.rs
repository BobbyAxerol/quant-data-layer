#![forbid(unsafe_code)]

use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;
use qdl_contracts::qdl::provider::v1::RawProviderEnvelope;
use qdl_kafka::{
    KafkaTlsConfig, KafkaTransportConfig, TransactionalKafkaBridge, TransactionalKafkaOutput,
    TransactionalShadowTopics,
};
use qdl_realtime_core::{RealtimeCore, RealtimeCoreConfig};
use qdl_venue_core::authority::{AuthorityMode, AuthorityRecord, PublicationContext, SinkTarget};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeConfig {
    core: RealtimeCoreConfig,
    raw_topics: Vec<String>,
    authority: AuthorityRecord,
    shard_id: String,
    transactional_id: String,
    batch_size: usize,
    batch_wait_ms: u64,
    max_events: u64,
    metrics_every_batches: u64,
}

impl RuntimeConfig {
    fn validate(&self) -> Result<(), String> {
        self.core.validate().map_err(|error| error.to_string())?;
        self.authority.validate()?;
        if self.authority.mode != AuthorityMode::RustShadow
            || self.raw_topics.is_empty()
            || self.raw_topics.iter().any(|topic| topic.trim().is_empty())
            || self.shard_id.trim().is_empty()
            || self.transactional_id.trim().is_empty()
            || self.batch_size == 0
            || self.batch_size > 10_000
            || self.batch_wait_ms == 0
            || self.batch_wait_ms > 1_000
            || self.metrics_every_batches == 0
        {
            return Err("realtime-core runtime config is invalid or not RUST_SHADOW".into());
        }
        Ok(())
    }
}

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("required environment variable is missing: {name}"))
}

fn kafka_config() -> Result<KafkaTransportConfig, String> {
    let cert_root = required("QDL_KAFKA_CERT_ROOT")?;
    let timeout_seconds = env::var("QDL_KAFKA_REQUEST_TIMEOUT_SECONDS")
        .unwrap_or_else(|_| "30".into())
        .parse::<u64>()
        .map_err(|_| "QDL_KAFKA_REQUEST_TIMEOUT_SECONDS must be positive")?;
    if timeout_seconds == 0 {
        return Err("QDL_KAFKA_REQUEST_TIMEOUT_SECONDS must be positive".into());
    }
    Ok(KafkaTransportConfig {
        bootstrap_servers: required("QDL_KAFKA_BOOTSTRAP_SERVERS")?,
        client_id: required("QDL_KAFKA_CLIENT_ID")?,
        group_id: required("QDL_KAFKA_GROUP_ID")?,
        request_timeout: Duration::from_secs(timeout_seconds),
        tls: KafkaTlsConfig {
            ca_location: format!("{cert_root}/ca.crt"),
            certificate_location: format!("{cert_root}/client.crt"),
            key_location: format!("{cert_root}/client.key"),
            key_password: None,
        },
    })
}

fn now_ns() -> Result<i64, Box<dyn std::error::Error>> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .try_into()?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = env::args()
        .nth(1)
        .ok_or("usage: qdl-realtime-core CONFIG.json")?;
    let config: RuntimeConfig = serde_json::from_slice(&tokio::fs::read(config_path).await?)?;
    config.validate()?;
    let mut core = RealtimeCore::new(config.core.clone())?;
    let bridge = TransactionalKafkaBridge::new(
        &kafka_config()?,
        TransactionalShadowTopics {
            raw_inputs: config.raw_topics.clone(),
            canonical: config.core.canonical_stream.clone(),
            quarantine: config.core.quarantine_stream.clone(),
        },
        &config.transactional_id,
    )?;
    bridge.apply_authority(config.authority.clone()).await?;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "qdl_realtime_core_started",
            "authority": "RUST_SHADOW",
            "bindings": config.core.bindings.len(),
            "batch_size": config.batch_size,
            "production_public_writes": 0,
            "production_legacy_writes": 0,
        }))?
    );

    let mut processed = 0_u64;
    let mut canonical = 0_u64;
    let mut quarantines = 0_u64;
    let mut duplicates = 0_u64;
    let mut filtered = 0_u64;
    let mut batches = 0_u64;
    'service: loop {
        if config.max_events > 0 && processed >= config.max_events {
            break;
        }
        let first = tokio::select! {
            result = bridge.next() => result?,
            _ = tokio::signal::ctrl_c() => break 'service,
        };
        let mut inputs = vec![first];
        while inputs.len() < config.batch_size
            && (config.max_events == 0 || processed + (inputs.len() as u64) < config.max_events)
        {
            match tokio::time::timeout(Duration::from_millis(config.batch_wait_ms), bridge.next())
                .await
            {
                Ok(Ok(input)) => inputs.push(input),
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => break,
            }
        }

        let normalized_at_ns = now_ns()?;
        let mut outputs = vec![];
        for input in &inputs {
            let raw = RawProviderEnvelope::decode(input.record.payload.as_slice())?;
            if raw.authority_revision != config.authority.revision {
                return Err("raw authority revision does not match runtime authority".into());
            }
            let result = core.process(raw.clone(), normalized_at_ns)?;
            canonical += result.canonical.len() as u64;
            quarantines += result.quarantines.len() as u64;
            duplicates += result.duplicates as u64;
            filtered += result.filtered as u64;
            for record in result.canonical {
                outputs.push(TransactionalKafkaOutput {
                    record,
                    publication: PublicationContext {
                        slice_id: config.authority.slice_id.clone(),
                        authority_revision: raw.authority_revision,
                        shard_id: config.shard_id.clone(),
                        lease_epoch: raw.lease_epoch,
                        target: SinkTarget::ShadowCanonical,
                    },
                });
            }
            for record in result.quarantines {
                outputs.push(TransactionalKafkaOutput {
                    record,
                    publication: PublicationContext {
                        slice_id: config.authority.slice_id.clone(),
                        authority_revision: raw.authority_revision,
                        shard_id: config.shard_id.clone(),
                        lease_epoch: raw.lease_epoch,
                        target: SinkTarget::ShadowQuarantine,
                    },
                });
            }
        }
        bridge.commit(&inputs, &outputs).await?;
        processed += inputs.len() as u64;
        batches += 1;
        if batches % config.metrics_every_batches == 0 {
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "event": "qdl_realtime_core_progress",
                    "processed": processed,
                    "canonical": canonical,
                    "quarantines": quarantines,
                    "duplicates": duplicates,
                    "filtered": filtered,
                    "batches": batches,
                }))?
            );
        }
    }
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "qdl_realtime_core_stopped",
            "processed": processed,
            "canonical": canonical,
            "quarantines": quarantines,
            "duplicates": duplicates,
            "filtered": filtered,
            "batches": batches,
        }))?
    );
    Ok(())
}
