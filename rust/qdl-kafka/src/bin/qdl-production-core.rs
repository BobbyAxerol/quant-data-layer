#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;
use qdl_contracts::qdl::provider::v1::RawProviderEnvelope;
use qdl_core::backoff::BackoffPolicy;
use qdl_core::transport::RetryClass;
use qdl_kafka::phase92_bootstrap::{
    Phase92BootstrapPayload, Phase92BootstrapScope, Phase92SignedBootstrapCursor,
};
use qdl_kafka::phase92_runtime::{
    KafkaCompactedSnapshotReader, Phase92Decision, Phase92Progress, Phase92TargetCheckpoint,
    Phase92TransactionalKafkaBridge, Phase92TransactionalOutput, Phase92TransactionalTopics,
};
use qdl_kafka::{
    shutdown_signal, KafkaEventSource, KafkaTlsConfig, KafkaTransportConfig, KafkaTransportError,
};
use qdl_realtime_core::{ProcessBatch, RealtimeCore, RealtimeCoreConfig};
use qdl_venue_core::authority::{
    Phase92AuthorityControlEvent, Phase92AuthorityState, Phase92PublicationContext, SinkTarget,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeSliceBinding {
    subscription_id: String,
    slice_id: String,
    shard_id: String,
    raw_authority_revision: u64,
    raw_lease_epoch: u64,
    raw_partition_plan_epoch: u64,
}

impl RuntimeSliceBinding {
    fn validate(&self) -> Result<(), String> {
        if self.subscription_id.trim().is_empty()
            || self.slice_id.trim().is_empty()
            || self.shard_id.trim().is_empty()
            || self.raw_authority_revision == 0
            || self.raw_lease_epoch == 0
            || self.raw_partition_plan_epoch == 0
        {
            return Err("production runtime slice binding is invalid".into());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProductionRuntimeConfig {
    core: RealtimeCoreConfig,
    topics: ProductionTopicConfig,
    slices: Vec<RuntimeSliceBinding>,
    transactional_id: String,
    promotion_scope_digest: String,
    partition_plan_epoch: u64,
    bootstrap_cursor_path: String,
    batch_size: usize,
    batch_wait_ms: u64,
    max_events: u64,
    metrics_every_batches: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProductionTopicConfig {
    raw_inputs: Vec<String>,
    authority_control: String,
    target_checkpoints: String,
    canary_canonical: String,
    primary_canonical: String,
    public_v2: String,
    legacy_v1: String,
    quarantine: String,
}

impl ProductionRuntimeConfig {
    fn topics(&self) -> Phase92TransactionalTopics {
        Phase92TransactionalTopics {
            raw_inputs: self.topics.raw_inputs.clone(),
            canary_canonical: self.topics.canary_canonical.clone(),
            primary_canonical: self.topics.primary_canonical.clone(),
            public_v2: self.topics.public_v2.clone(),
            legacy_v1: self.topics.legacy_v1.clone(),
            quarantine: self.topics.quarantine.clone(),
            target_checkpoints: self.topics.target_checkpoints.clone(),
            authority_control: self.topics.authority_control.clone(),
        }
    }

    fn validate(&self) -> Result<(), String> {
        self.core.validate().map_err(|error| error.to_string())?;
        self.topics()
            .validate()
            .map_err(|error| error.to_string())?;
        if self.transactional_id.trim().is_empty()
            || !lower_sha256(&self.promotion_scope_digest)
            || self.partition_plan_epoch == 0
            || !self.bootstrap_cursor_path.starts_with("/runtime/")
            || self.bootstrap_cursor_path.len() <= "/runtime/".len()
            || self.slices.is_empty()
            || self.batch_size == 0
            || self.batch_size > 1_000
            || self.batch_wait_ms == 0
            || self.batch_wait_ms > 1_000
            || self.metrics_every_batches == 0
        {
            return Err("production realtime core config bounds are invalid".into());
        }
        let mut subscriptions = HashSet::new();
        let mut slices = HashSet::new();
        for binding in &self.slices {
            binding.validate()?;
            if binding.raw_partition_plan_epoch != self.partition_plan_epoch {
                return Err("production runtime raw/bundle partition plan epochs differ".into());
            }
            if !subscriptions.insert(binding.subscription_id.clone())
                || !slices.insert(binding.slice_id.clone())
            {
                return Err(
                    "production runtime subscription/slice identities must be unique".into(),
                );
            }
        }
        Ok(())
    }

    fn bindings(&self) -> HashMap<String, RuntimeSliceBinding> {
        self.slices
            .iter()
            .cloned()
            .map(|binding| (binding.subscription_id.clone(), binding))
            .collect()
    }

    fn bootstrap_scope(&self, kafka: &KafkaTransportConfig) -> Phase92BootstrapScope {
        Phase92BootstrapScope {
            consumer_group_id: kafka.group_id.clone(),
            raw_topics: self.topics.raw_inputs.clone(),
            promotion_scope_digest: self.promotion_scope_digest.clone(),
            // The token's candidate is cross-checked against reconstructed
            // authority below. It cannot be baked into this bundle without a
            // circular dependency on the authority packet/manifest.
            candidate_digest: None,
            partition_plan_epoch: self.partition_plan_epoch,
        }
    }

    fn load_signed_bootstrap(
        &self,
        kafka: &KafkaTransportConfig,
    ) -> Result<Phase92BootstrapPayload, RuntimeError> {
        let keyring = required("QDL_PHASE92_BOOTSTRAP_CURSOR_KEYS_JSON")?;
        Ok(Phase92SignedBootstrapCursor::load_and_verify(
            &self.bootstrap_cursor_path,
            &keyring,
            &self.bootstrap_scope(kafka),
        )?)
    }

    async fn validate_bootstrap_authority(
        &self,
        bridge: &Phase92TransactionalKafkaBridge,
        bootstrap: &Phase92BootstrapPayload,
    ) -> Result<(), RuntimeError> {
        for binding in &self.slices {
            let authority = bridge
                .current_authority(&binding.slice_id)
                .await
                .ok_or("production bootstrap authority disappeared")?;
            if authority.slice_id != binding.slice_id
                || authority.partition_plan_epoch != self.partition_plan_epoch
                || authority.candidate_digest != bootstrap.candidate_digest
            {
                return Err("signed bootstrap cursor differs from restored authority".into());
            }
        }
        Ok(())
    }
}

fn lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| {
            byte.is_ascii_digit() || (byte.is_ascii_lowercase() && byte.is_ascii_hexdigit())
        })
}

type RuntimeError = Box<dyn std::error::Error + Send + Sync>;

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("required environment variable is missing: {name}"))
}

fn kafka_config(group_suffix: &str) -> Result<KafkaTransportConfig, String> {
    let cert_root = required("QDL_KAFKA_CERT_ROOT")?;
    let timeout_seconds = env::var("QDL_KAFKA_REQUEST_TIMEOUT_SECONDS")
        .unwrap_or_else(|_| "30".into())
        .parse::<u64>()
        .map_err(|_| "QDL_KAFKA_REQUEST_TIMEOUT_SECONDS must be positive")?;
    if timeout_seconds == 0 || group_suffix.trim().is_empty() {
        return Err("Kafka timeout/group suffix must be positive and non-empty".into());
    }
    Ok(KafkaTransportConfig {
        bootstrap_servers: required("QDL_KAFKA_BOOTSTRAP_SERVERS")?,
        client_id: format!("{}-{group_suffix}", required("QDL_KAFKA_CLIENT_ID")?),
        group_id: format!("{}-{group_suffix}", required("QDL_KAFKA_GROUP_ID")?),
        request_timeout: Duration::from_secs(timeout_seconds),
        tls: KafkaTlsConfig {
            ca_location: format!("{cert_root}/ca.crt"),
            certificate_location: format!("{cert_root}/client.crt"),
            key_location: format!("{cert_root}/client.key"),
            key_password: None,
        },
    })
}

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

fn expected_targets(state: Phase92AuthorityState) -> Result<Vec<SinkTarget>, String> {
    match state {
        Phase92AuthorityState::RustCanary => Ok(vec![SinkTarget::CanaryCanonical]),
        Phase92AuthorityState::RustPrimary => Ok(vec![
            SinkTarget::PrimaryCanonical,
            SinkTarget::PublicV2,
            SinkTarget::LegacyV1,
        ]),
        Phase92AuthorityState::PythonPrimary
        | Phase92AuthorityState::Blocked
        | Phase92AuthorityState::RollbackPending => {
            Err("Rust production core does not own this authority state".into())
        }
    }
}

async fn restore_authority(
    bridge: &Phase92TransactionalKafkaBridge,
    config: &ProductionRuntimeConfig,
) -> Result<(), RuntimeError> {
    let snapshot_config = kafka_config(&format!("phase92-recovery-{}", config.transactional_id))?;
    let records = KafkaCompactedSnapshotReader::new(snapshot_config)?.read(&[
        &config.topics.authority_control,
        &config.topics.target_checkpoints,
    ])?;
    let required_slices: HashSet<_> = config
        .slices
        .iter()
        .map(|binding| binding.slice_id.as_str())
        .collect();
    let mut events = HashMap::new();
    let mut checkpoints = Vec::new();
    for record in records {
        if record.topic == config.topics.authority_control {
            let event: Phase92AuthorityControlEvent = serde_json::from_slice(&record.payload)?;
            event.validate()?;
            if record.key != event.slice_id {
                return Err("authority compacted key differs from event slice".into());
            }
            if required_slices.contains(event.slice_id.as_str()) {
                if event.authority.is_none() {
                    return Err("production slice has no Phase 9.2 authority record".into());
                }
                events.insert(event.slice_id.clone(), event);
            }
        } else if record.topic == config.topics.target_checkpoints {
            let checkpoint: Phase92TargetCheckpoint = serde_json::from_slice(&record.payload)?;
            checkpoint.validate()?;
            if record.key != checkpoint.key() {
                return Err("target checkpoint compacted key differs from payload".into());
            }
            if required_slices.contains(checkpoint.slice_id.as_str()) {
                checkpoints.push(checkpoint);
            }
        }
    }
    if events.len() != required_slices.len() {
        return Err("one or more production slices have no authority event".into());
    }
    let restore_time = now_ns()?;
    for binding in &config.slices {
        let event = events
            .get(&binding.slice_id)
            .ok_or("production authority event is missing")?;
        bridge.apply_authority_event(event, restore_time).await?;
        let authority = event
            .authority
            .as_ref()
            .ok_or("production authority record is missing")?;
        let targets = expected_targets(authority.state)?;
        let exact: Vec<_> = checkpoints
            .iter()
            .filter(|checkpoint| {
                checkpoint.slice_id == binding.slice_id
                    && checkpoint.shard_id == binding.shard_id
                    && checkpoint.owner_id == authority.owner_id
                    && checkpoint.authority_revision == authority.authority_revision
                    && checkpoint.lease_epoch == authority.lease_epoch
                    && checkpoint.partition_plan_epoch == authority.partition_plan_epoch
                    && checkpoint.candidate_digest == authority.candidate_digest
                    && targets.contains(&checkpoint.target)
            })
            .collect();
        if exact.len() == targets.len() {
            for checkpoint in exact {
                bridge.restore_checkpoint(checkpoint).await?;
            }
        } else if exact.is_empty() && authority.state == Phase92AuthorityState::RustPrimary {
            let handoff = event
                .handoff
                .as_ref()
                .ok_or("fresh Rust primary bootstrap requires accepted handoff")?;
            let terminal = event
                .checkpoint
                .as_ref()
                .ok_or("fresh Rust primary bootstrap requires terminal checkpoint")?;
            if handoff.terminal_watermark != authority.start_watermark
                || terminal.terminal_watermark != authority.start_watermark
            {
                return Err("fresh Rust primary bootstrap W differs from handoff evidence".into());
            }
            for target in targets {
                let checkpoint = Phase92TargetCheckpoint {
                    schema: "qdl.target-watermark-checkpoint.v1".into(),
                    slice_id: authority.slice_id.clone(),
                    owner_id: authority.owner_id.clone(),
                    authority_revision: authority.authority_revision,
                    lease_epoch: authority.lease_epoch,
                    partition_plan_epoch: authority.partition_plan_epoch,
                    shard_id: binding.shard_id.clone(),
                    target,
                    source_watermark: authority.start_watermark,
                    source_event_id: format!("handoff-{}", handoff.handoff_id),
                    decision: Phase92Decision::Filtered,
                    output_payload_sha256: "0".repeat(64),
                    candidate_digest: authority.candidate_digest.clone(),
                    committed_at_ns: restore_time,
                };
                bridge.restore_checkpoint(&checkpoint).await?;
            }
        } else if !exact.is_empty() || authority.state != Phase92AuthorityState::RustCanary {
            return Err("target checkpoint recovery is partial or inconsistent".into());
        }
    }
    Ok(())
}

async fn watch_authority(
    bridge: Arc<Phase92TransactionalKafkaBridge>,
    config: KafkaTransportConfig,
    topic: String,
    allowed_slices: HashSet<String>,
) -> Result<(), RuntimeError> {
    let source = KafkaEventSource::new(&config, &[&topic])?;
    loop {
        let (record, _) = source.next().await?;
        let event: Phase92AuthorityControlEvent = serde_json::from_slice(&record.payload)?;
        event.validate()?;
        if record.partition_key != event.slice_id {
            return Err("authority stream key differs from event slice".into());
        }
        if !allowed_slices.contains(&event.slice_id) {
            source.checkpoint()?;
            continue;
        }
        let current = bridge.current_authority(&event.slice_id).await;
        if current
            .as_ref()
            .is_some_and(|value| event.authority_revision < value.authority_revision)
        {
            source.checkpoint()?;
            continue;
        }
        if event.authority.is_none() {
            return Err("active production slice received non-Phase92 authority event".into());
        }
        bridge.apply_authority_event(&event, now_ns()?).await?;
        source.checkpoint()?;
    }
}

fn approved_binding<'a>(
    raw: &RawProviderEnvelope,
    bindings: &'a HashMap<String, RuntimeSliceBinding>,
) -> Option<&'a RuntimeSliceBinding> {
    bindings.get(&raw.subscription_id)
}

fn validate_raw_authority(
    raw: &RawProviderEnvelope,
    binding: &RuntimeSliceBinding,
) -> Result<(), RuntimeError> {
    if raw.subscription_id != binding.subscription_id
        || raw.authority_revision != binding.raw_authority_revision
        || raw.lease_epoch != binding.raw_lease_epoch
        || raw.partition_plan_epoch != binding.raw_partition_plan_epoch
    {
        return Err("raw acquisition identity/lease differs from approved binding".into());
    }
    Ok(())
}

fn decision(result: &ProcessBatch) -> Result<Phase92Decision, RuntimeError> {
    let active = [
        !result.canonical.is_empty(),
        !result.quarantines.is_empty(),
        result.duplicates > 0,
        result.filtered > 0,
    ];
    if active.into_iter().filter(|value| *value).count() != 1 {
        return Err("normalizer produced an ambiguous decision for one raw event".into());
    }
    if !result.canonical.is_empty() {
        Ok(Phase92Decision::Canonical)
    } else if !result.quarantines.is_empty() {
        Ok(Phase92Decision::Quarantine)
    } else if result.duplicates > 0 {
        Ok(Phase92Decision::Duplicate)
    } else {
        Ok(Phase92Decision::Filtered)
    }
}

fn output_stream(topics: &ProductionTopicConfig, target: SinkTarget) -> Result<&str, RuntimeError> {
    match target {
        SinkTarget::CanaryCanonical => Ok(&topics.canary_canonical),
        SinkTarget::PrimaryCanonical => Ok(&topics.primary_canonical),
        SinkTarget::PublicV2 => Ok(&topics.public_v2),
        SinkTarget::LegacyV1 => Ok(&topics.legacy_v1),
        _ => Err("production runtime received a non-production target".into()),
    }
}

async fn run_generation(
    config: &ProductionRuntimeConfig,
    generation: u64,
) -> Result<(), RuntimeError> {
    let mut core = RealtimeCore::new(config.core.clone())?;
    let raw_kafka = kafka_config("phase92-raw")?;
    let bootstrap = config.load_signed_bootstrap(&raw_kafka)?;
    let bridge = Arc::new(Phase92TransactionalKafkaBridge::new_with_signed_bootstrap(
        &raw_kafka,
        config.topics(),
        &config.transactional_id,
        bootstrap.clone(),
    )?);
    restore_authority(&bridge, config).await?;
    config
        .validate_bootstrap_authority(&bridge, &bootstrap)
        .await?;
    let binding_by_subscription = config.bindings();
    let allowed_slices: HashSet<_> = config
        .slices
        .iter()
        .map(|binding| binding.slice_id.clone())
        .collect();
    let mut authority_task = tokio::spawn(watch_authority(
        Arc::clone(&bridge),
        kafka_config(&format!("phase92-authority-{}", config.transactional_id))?,
        config.topics.authority_control.clone(),
        allowed_slices,
    ));
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "qdl_production_core_started",
            "generation": generation,
            "slices": config.slices.len(),
            "bindings": config.core.bindings.len(),
            "authority_reconstructed": true,
            "target_watermarks_reconstructed": true,
            "bootstrap_cursor_id": bootstrap.cursor_id,
            "bootstrap_generation": bootstrap.generation,
            "bootstrap_status": bridge.bootstrap_status(),
        }))?
    );

    let mut processed = 0_u64;
    let mut canonical = 0_u64;
    let mut quarantines = 0_u64;
    let mut duplicates = 0_u64;
    let mut filtered = 0_u64;
    let mut ignored_out_of_scope = 0_u64;
    let mut batches = 0_u64;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let stop_reason = 'service: loop {
        if config.max_events > 0 && processed >= config.max_events {
            break 'service "MAX_EVENTS";
        }
        let first = tokio::select! {
            result = bridge.next() => result?,
            result = &mut shutdown => {
                break 'service result?.as_str();
            }
            result = &mut authority_task => {
                return match result {
                    Ok(Ok(())) => Err("authority watcher stopped unexpectedly".into()),
                    Ok(Err(error)) => Err(error),
                    Err(error) => Err(error.into()),
                };
            }
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
        let mut outputs = Vec::new();
        let mut progress = Vec::new();
        let mut local_next: HashMap<(String, String, SinkTarget), u64> = HashMap::new();
        for input in &inputs {
            let raw = RawProviderEnvelope::decode(input.record.payload.as_slice())?;
            let Some(binding) = approved_binding(&raw, &binding_by_subscription) else {
                ignored_out_of_scope = ignored_out_of_scope.saturating_add(1);
                continue;
            };
            validate_raw_authority(&raw, binding)?;
            let authority = bridge
                .current_authority(&binding.slice_id)
                .await
                .ok_or("production slice authority disappeared")?;
            let targets = expected_targets(authority.state)?;
            let result =
                core.process_at_transport_offset(raw, normalized_at_ns, input.cursor.offset)?;
            let item_decision = decision(&result)?;
            canonical += result.canonical.len() as u64;
            quarantines += result.quarantines.len() as u64;
            duplicates += result.duplicates as u64;
            filtered += result.filtered as u64;

            for target in targets {
                let key = (binding.slice_id.clone(), binding.shard_id.clone(), target);
                let source_watermark = if let Some(next) = local_next.get_mut(&key) {
                    let value = *next;
                    *next = next.checked_add(1).ok_or("logical watermark overflow")?;
                    value
                } else {
                    let value = bridge
                        .next_watermark(&binding.slice_id, &binding.shard_id, target)
                        .await?;
                    local_next.insert(
                        key,
                        value.checked_add(1).ok_or("logical watermark overflow")?,
                    );
                    value
                };
                let publication = Phase92PublicationContext {
                    slice_id: authority.slice_id.clone(),
                    owner_id: authority.owner_id.clone(),
                    authority_revision: authority.authority_revision,
                    shard_id: binding.shard_id.clone(),
                    lease_epoch: authority.lease_epoch,
                    partition_plan_epoch: authority.partition_plan_epoch,
                    source_watermark,
                    target,
                };
                progress.push(Phase92Progress {
                    publication: publication.clone(),
                    decision: item_decision,
                    source_cursor: input.cursor.clone(),
                    source_event_id: input.record.event_id.clone(),
                });
                if item_decision == Phase92Decision::Canonical {
                    for record in &result.canonical {
                        let mut projected = record.clone();
                        projected.stream = output_stream(&config.topics, target)?.into();
                        outputs.push(Phase92TransactionalOutput {
                            record: projected,
                            publication: publication.clone(),
                            raw_provider_envelope: Some(input.record.payload.clone()),
                        });
                    }
                } else if item_decision == Phase92Decision::Quarantine
                    && target == SinkTarget::PrimaryCanonical
                {
                    for record in &result.quarantines {
                        let mut quarantined = record.clone();
                        quarantined.stream = config.topics.quarantine.clone();
                        outputs.push(Phase92TransactionalOutput {
                            record: quarantined,
                            publication: publication.clone(),
                            raw_provider_envelope: Some(input.record.payload.clone()),
                        });
                    }
                }
            }
        }
        bridge
            .commit(&inputs, &outputs, &progress, normalized_at_ns)
            .await?;
        processed += inputs.len() as u64;
        batches += 1;
        if batches % config.metrics_every_batches == 0 {
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "event": "qdl_production_core_progress",
                    "generation": generation,
                    "processed": processed,
                    "canonical": canonical,
                    "quarantines": quarantines,
                    "duplicates": duplicates,
                    "filtered": filtered,
                    "ignored_out_of_scope": ignored_out_of_scope,
                    "batches": batches,
                    "bootstrap_status": bridge.bootstrap_status(),
                }))?
            );
        }
    };
    bridge.unsubscribe();
    authority_task.abort();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "qdl_production_core_stopped",
            "generation": generation,
            "processed": processed,
            "canonical": canonical,
            "quarantines": quarantines,
            "duplicates": duplicates,
            "filtered": filtered,
            "ignored_out_of_scope": ignored_out_of_scope,
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
        .ok_or("usage: qdl-production-core CONFIG.json")?;
    let config: ProductionRuntimeConfig =
        serde_json::from_slice(&tokio::fs::read(config_path).await?)?;
    config.validate()?;
    let backoff = BackoffPolicy {
        initial_ms: 500,
        maximum_ms: 30_000,
        multiplier: 2,
        jitter_bps: 2_000,
    }
    .validate()?;
    let mut generation = 0_u64;
    let mut failures = 0_u32;
    loop {
        generation = generation.saturating_add(1);
        match run_generation(&config, generation).await {
            Ok(()) => return Ok(()),
            Err(error) if retryable_runtime_error(&error) => {
                failures = failures.saturating_add(1);
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_production_core_retry",
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
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_is_exclusive_and_fail_closed() {
        let canonical = ProcessBatch {
            canonical: vec![qdl_core::transport::DurableRecord {
                stream: "canonical".into(),
                partition_key: "key".into(),
                event_id: vec![1],
                payload: vec![2],
                accepted_at_ns: 1,
            }],
            quarantines: vec![],
            duplicates: 0,
            filtered: 0,
        };
        assert_eq!(decision(&canonical).unwrap(), Phase92Decision::Canonical);
        let ambiguous = ProcessBatch {
            filtered: 1,
            ..canonical
        };
        assert!(decision(&ambiguous).is_err());
    }

    #[test]
    fn shared_raw_scope_accepts_exact_binding_and_ignores_valid_unbound_envelope() {
        let binding = RuntimeSliceBinding {
            subscription_id: "binance-btc-trade".into(),
            slice_id: "production/binance/usdm/perpetual/trade/plan-1/btcusdt".into(),
            shard_id: "btcusdt-trade".into(),
            raw_authority_revision: 1,
            raw_lease_epoch: 2,
            raw_partition_plan_epoch: 3,
        };
        let bindings = HashMap::from([(binding.subscription_id.clone(), binding)]);
        let approved = RawProviderEnvelope {
            subscription_id: "binance-btc-trade".into(),
            ..Default::default()
        };
        let out_of_scope = RawProviderEnvelope {
            subscription_id: "binance-spot-sol-trade".into(),
            ..Default::default()
        };

        assert_eq!(
            approved_binding(&approved, &bindings).map(|value| value.slice_id.as_str()),
            Some("production/binance/usdm/perpetual/trade/plan-1/btcusdt")
        );
        assert!(approved_binding(&out_of_scope, &bindings).is_none());
    }

    #[test]
    fn raw_and_publication_authorities_are_explicitly_separate() {
        let raw = RawProviderEnvelope {
            subscription_id: "binance-btc-trade".into(),
            authority_revision: 1,
            lease_epoch: 2,
            partition_plan_epoch: 3,
            ..Default::default()
        };
        let binding = RuntimeSliceBinding {
            subscription_id: "binance-btc-trade".into(),
            slice_id: "production/binance/usdm/perpetual/trade/plan-1/btcusdt".into(),
            shard_id: "btcusdt-trade".into(),
            raw_authority_revision: 1,
            raw_lease_epoch: 2,
            raw_partition_plan_epoch: 3,
        };
        validate_raw_authority(&raw, &binding).unwrap();
        let mut stale = raw;
        stale.lease_epoch = 1;
        assert!(validate_raw_authority(&stale, &binding).is_err());
    }
}
