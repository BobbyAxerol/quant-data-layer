#![forbid(unsafe_code)]

use std::collections::HashSet;
use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;
use qdl_contracts::qdl::provider::v1::{QuarantineReason, RawProviderEnvelope};
use qdl_core::backoff::BackoffPolicy;
use qdl_core::transport::RetryClass;
use qdl_kafka::{
    shutdown_signal, KafkaTlsConfig, KafkaTransportConfig, KafkaTransportError,
    TransactionalKafkaBridge, TransactionalKafkaOutput, TransactionalShadowTopics,
};
use qdl_realtime_core::{
    provider_admission_server::ProviderAdmissionServer, CoreError, RealtimeCore, RealtimeCoreConfig,
};
use qdl_venue_core::authority::{AuthorityMode, AuthorityRecord, PublicationContext, SinkTarget};
use serde::Deserialize;
use serde_json::json;

const MAX_IN_GENERATION_RECEIVE_RETRIES: u32 = 3;

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
    #[serde(default)]
    strict_subscription_scope: bool,
}

impl RuntimeConfig {
    fn validate(&self) -> Result<(), String> {
        self.core.validate().map_err(|error| error.to_string())?;
        self.authority.validate()?;
        if !matches!(
            self.authority.mode,
            AuthorityMode::RustShadow | AuthorityMode::RustPrimary
        ) || self.raw_topics.is_empty()
            || self.raw_topics.iter().any(|topic| topic.trim().is_empty())
            || self.shard_id.trim().is_empty()
            || self.transactional_id.trim().is_empty()
            || self.batch_size == 0
            || self.batch_size > 10_000
            || self.batch_wait_ms == 0
            || self.batch_wait_ms > 1_000
            || self.metrics_every_batches == 0
        {
            return Err(
                "realtime-core runtime config is invalid or not a shared Rust authority".into(),
            );
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

type RuntimeError = Box<dyn std::error::Error + Send + Sync>;

fn now_ns() -> Result<i64, RuntimeError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .try_into()?)
}

fn should_retry_transport(class: RetryClass) -> bool {
    class != RetryClass::NonRetryable
}

fn retryable_runtime_error(error: &RuntimeError) -> bool {
    error
        .downcast_ref::<KafkaTransportError>()
        .is_some_and(|value| should_retry_transport(value.retry_class()))
}

fn should_retry_receive_in_generation(error: &KafkaTransportError, failures: u32) -> bool {
    should_retry_transport(error.retry_class()) && failures < MAX_IN_GENERATION_RECEIVE_RETRIES
}

fn approved_subscription_scope(config: &RuntimeConfig) -> HashSet<String> {
    config
        .core
        .bindings
        .iter()
        .map(|binding| binding.source_id.clone())
        .collect()
}

fn is_approved_subscription(
    raw: &RawProviderEnvelope,
    approved_subscriptions: &HashSet<String>,
) -> bool {
    approved_subscriptions.contains(&raw.subscription_id)
}

fn strict_quarantine_reason(error: &CoreError) -> Option<(QuarantineReason, &'static str)> {
    match error {
        CoreError::UnknownBinding => Some((
            QuarantineReason::FencingRejected,
            "raw identity does not match declared strict scope",
        )),
        CoreError::ProvenanceRejected => Some((
            QuarantineReason::FencingRejected,
            "test provenance is forbidden in strict scope",
        )),
        CoreError::RawEnvelope(_) => Some((
            QuarantineReason::Malformed,
            "raw envelope validation failed in strict scope",
        )),
        CoreError::Decode(_) => Some((
            QuarantineReason::SemanticInvalid,
            "provider frame cannot be canonicalized in strict scope",
        )),
        CoreError::Configuration(_) => None,
    }
}

fn output_targets(mode: AuthorityMode) -> Result<(SinkTarget, SinkTarget), String> {
    match mode {
        AuthorityMode::RustShadow => {
            Ok((SinkTarget::ShadowCanonical, SinkTarget::ShadowQuarantine))
        }
        AuthorityMode::RustPrimary => {
            Ok((SinkTarget::PrimaryCanonical, SinkTarget::PrimaryQuarantine))
        }
        AuthorityMode::RustCanary => Err("shared realtime core cannot run RUST_CANARY".into()),
    }
}

fn authority_mode_name(mode: AuthorityMode) -> &'static str {
    match mode {
        AuthorityMode::RustShadow => "RUST_SHADOW",
        AuthorityMode::RustCanary => "RUST_CANARY",
        AuthorityMode::RustPrimary => "RUST_PRIMARY",
    }
}

async fn run_generation(
    config: &RuntimeConfig,
    generation: u64,
    backoff: &BackoffPolicy,
) -> Result<(), RuntimeError> {
    let mut core = RealtimeCore::new(config.core.clone())?;
    let approved_subscriptions = approved_subscription_scope(config);
    let (canonical_target, quarantine_target) = output_targets(config.authority.mode)?;
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
            "authority": authority_mode_name(config.authority.mode),
            "generation": generation,
            "bindings": config.core.bindings.len(),
            "approved_subscriptions": approved_subscriptions.len(),
            "batch_size": config.batch_size,
            "public_writes": 0,
            "legacy_writes": 0,
        }))?
    );

    let mut processed = 0_u64;
    let mut canonical = 0_u64;
    let mut quarantines = 0_u64;
    let mut duplicates = 0_u64;
    let mut filtered = 0_u64;
    let mut ignored_out_of_scope = 0_u64;
    let mut scope_quarantines = 0_u64;
    let mut batches = 0_u64;
    let mut receive_failures = 0_u32;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let stop_reason = 'service: loop {
        if config.max_events > 0 && processed >= config.max_events {
            break 'service "MAX_EVENTS";
        }
        let first = loop {
            let received = tokio::select! {
                result = bridge.next() => result,
                result = &mut shutdown => break 'service result?.as_str(),
            };
            match received {
                Ok(input) => {
                    receive_failures = 0;
                    break input;
                }
                Err(error) if should_retry_receive_in_generation(&error, receive_failures) => {
                    receive_failures = receive_failures.saturating_add(1);
                    eprintln!(
                        "{}",
                        serde_json::to_string(&json!({
                            "event": "qdl_realtime_core_receive_retry",
                            "generation": generation,
                            "attempt": receive_failures,
                            "max_attempts": MAX_IN_GENERATION_RECEIVE_RETRIES,
                            "error": error.to_string(),
                        }))?
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(
                            backoff.delay_ms(
                                receive_failures.saturating_sub(1),
                                receive_failures as u16,
                            ),
                        )) => {}
                        result = &mut shutdown => break 'service result?.as_str(),
                    }
                }
                Err(error) => return Err(error.into()),
            }
        };
        let mut inputs = vec![first];
        while inputs.len() < config.batch_size
            && (config.max_events == 0 || processed + (inputs.len() as u64) < config.max_events)
        {
            match tokio::time::timeout(Duration::from_millis(config.batch_wait_ms), bridge.next())
                .await
            {
                Ok(Ok(input)) => {
                    receive_failures = 0;
                    inputs.push(input);
                }
                Ok(Err(error)) if should_retry_receive_in_generation(&error, receive_failures) => {
                    receive_failures = receive_failures.saturating_add(1);
                    eprintln!(
                        "{}",
                        serde_json::to_string(&json!({
                            "event": "qdl_realtime_core_receive_deferred",
                            "generation": generation,
                            "attempt": receive_failures,
                            "max_attempts": MAX_IN_GENERATION_RECEIVE_RETRIES,
                            "error": error.to_string(),
                        }))?
                    );
                    break;
                }
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => break,
            }
        }

        let normalized_at_ns = now_ns()?;
        let mut outputs = vec![];
        for input in &inputs {
            let raw = RawProviderEnvelope::decode(input.record.payload.as_slice())?;
            let result = if !is_approved_subscription(&raw, &approved_subscriptions) {
                if config.strict_subscription_scope {
                    scope_quarantines = scope_quarantines.saturating_add(1);
                    core.quarantine_raw(
                        &raw,
                        QuarantineReason::FencingRejected,
                        "undeclared raw subscription in strict scope",
                        normalized_at_ns,
                    )
                } else {
                    ignored_out_of_scope = ignored_out_of_scope.saturating_add(1);
                    continue;
                }
            } else if raw.authority_revision != config.authority.revision {
                if config.strict_subscription_scope {
                    scope_quarantines = scope_quarantines.saturating_add(1);
                    core.quarantine_raw(
                        &raw,
                        QuarantineReason::FencingRejected,
                        "raw authority revision does not match strict runtime authority",
                        normalized_at_ns,
                    )
                } else {
                    return Err("raw authority revision does not match runtime authority".into());
                }
            } else {
                match core.process_at_transport_offset(
                    raw.clone(),
                    normalized_at_ns,
                    input.cursor.offset,
                ) {
                    Ok(result) => result,
                    Err(error) if config.strict_subscription_scope => {
                        let Some((reason, summary)) = strict_quarantine_reason(&error) else {
                            return Err(error.into());
                        };
                        scope_quarantines = scope_quarantines.saturating_add(1);
                        core.quarantine_raw(&raw, reason, summary, normalized_at_ns)
                    }
                    Err(error) => return Err(error.into()),
                }
            };
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
                        target: canonical_target,
                    },
                    raw_provider_envelope: Some(input.record.payload.clone()),
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
                        target: quarantine_target,
                    },
                    raw_provider_envelope: None,
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
                    "generation": generation,
                    "processed": processed,
                    "canonical": canonical,
                    "quarantines": quarantines,
                    "duplicates": duplicates,
                    "filtered": filtered,
                    "ignored_out_of_scope": ignored_out_of_scope,
                    "scope_quarantines": scope_quarantines,
                    "batches": batches,
                }))?
            );
        }
    };
    bridge.unsubscribe();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "qdl_realtime_core_stopped",
            "generation": generation,
            "processed": processed,
            "canonical": canonical,
            "quarantines": quarantines,
            "duplicates": duplicates,
            "filtered": filtered,
            "ignored_out_of_scope": ignored_out_of_scope,
            "scope_quarantines": scope_quarantines,
            "batches": batches,
            "reason": stop_reason,
        }))?
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), RuntimeError> {
    let config_path = env::args()
        .nth(1)
        .ok_or("usage: qdl-realtime-core CONFIG.json")?;
    let config: RuntimeConfig = serde_json::from_slice(&tokio::fs::read(config_path).await?)?;
    config.validate()?;
    // This listener is private to the existing stable-internal network and is
    // absent unless the sealed C3.6 runtime binding enables it.  It survives
    // Kafka generation retries so all query roles share one Rust coordinator.
    let admission_server = ProviderAdmissionServer::start_from_environment().await?;
    let backoff = BackoffPolicy {
        initial_ms: 500,
        maximum_ms: 30_000,
        multiplier: 2,
        jitter_bps: 2_000,
    }
    .validate()?;
    let mut generation = 0_u64;
    let mut failures = 0_u32;
    let result = loop {
        generation = generation.saturating_add(1);
        match run_generation(&config, generation, &backoff).await {
            Ok(()) => break Ok(()),
            Err(error) if retryable_runtime_error(&error) => {
                failures = failures.saturating_add(1);
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_realtime_core_retry",
                        "generation": generation,
                        "attempt": failures,
                        "error": error.to_string(),
                    }))?
                );
                tokio::time::sleep(Duration::from_millis(
                    backoff.delay_ms(failures, failures.min(10_000) as u16),
                ))
                .await;
            }
            Err(error) => break Err(error),
        }
    };
    if let Some(server) = admission_server {
        server.stop().await?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_retries_only_retryable_or_capacity_transport_errors() {
        assert!(should_retry_transport(RetryClass::Retryable));
        assert!(should_retry_transport(RetryClass::Capacity));
        assert!(!should_retry_transport(RetryClass::NonRetryable));
    }

    #[test]
    fn retryable_receive_errors_preserve_generation_only_within_bound() {
        let retryable = KafkaTransportError::SnapshotTimeout("transient receive".into());
        assert!(should_retry_receive_in_generation(&retryable, 0));
        assert!(should_retry_receive_in_generation(
            &retryable,
            MAX_IN_GENERATION_RECEIVE_RETRIES - 1,
        ));
        assert!(!should_retry_receive_in_generation(
            &retryable,
            MAX_IN_GENERATION_RECEIVE_RETRIES,
        ));

        let fatal = KafkaTransportError::Configuration("bad config".into());
        assert!(!should_retry_receive_in_generation(&fatal, 0));
    }

    #[test]
    fn shared_raw_scope_accepts_only_exact_subscription_ids() {
        let approved_subscriptions = HashSet::from(["dnse-vn30f1m-trade-stable-001".to_owned()]);
        let approved = RawProviderEnvelope {
            subscription_id: "dnse-vn30f1m-trade-stable-001".into(),
            ..Default::default()
        };
        let foreign = RawProviderEnvelope {
            subscription_id: "binance-usdm-btcusdt-trade-stable-001".into(),
            ..Default::default()
        };

        assert!(is_approved_subscription(&approved, &approved_subscriptions));
        assert!(!is_approved_subscription(&foreign, &approved_subscriptions));
    }

    #[test]
    fn strict_scope_errors_map_to_durable_quarantine_reasons() {
        assert_eq!(
            strict_quarantine_reason(&CoreError::UnknownBinding),
            Some((
                QuarantineReason::FencingRejected,
                "raw identity does not match declared strict scope",
            )),
        );
        assert_eq!(
            strict_quarantine_reason(&CoreError::Decode("bad frame".into())),
            Some((
                QuarantineReason::SemanticInvalid,
                "provider frame cannot be canonicalized in strict scope",
            )),
        );
        assert_eq!(
            strict_quarantine_reason(&CoreError::Configuration("bad config".into())),
            None,
        );
    }

    #[test]
    fn shared_core_binds_private_outputs_to_authority_mode() {
        assert_eq!(
            output_targets(AuthorityMode::RustShadow).unwrap(),
            (SinkTarget::ShadowCanonical, SinkTarget::ShadowQuarantine),
        );
        assert_eq!(
            output_targets(AuthorityMode::RustPrimary).unwrap(),
            (SinkTarget::PrimaryCanonical, SinkTarget::PrimaryQuarantine),
        );
        assert!(output_targets(AuthorityMode::RustCanary).is_err());
    }
}
