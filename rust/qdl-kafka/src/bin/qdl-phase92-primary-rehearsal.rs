#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use qdl_core::transport::DurableRecord;
use qdl_kafka::{
    KafkaDurableSink, KafkaEventSource, KafkaTlsConfig, KafkaTransportConfig,
    Phase92FencedKafkaSink, Phase92SinkTopics,
};
use qdl_venue_core::authority::{
    Phase92AcceptedHandoff, Phase92AuthorityRecord, Phase92AuthorityState, Phase92HandoffDirection,
    Phase92PublicationContext, Phase92TerminalCheckpoint, SinkTarget,
};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("required environment variable is missing: {name}"))
}

fn now_ns() -> Result<i64, Box<dyn std::error::Error>> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .try_into()?)
}

fn transport_config(identity: &str, group_id: &str) -> Result<KafkaTransportConfig, String> {
    let cert_root = required("QDL_KAFKA_CERT_ROOT")?;
    Ok(KafkaTransportConfig {
        bootstrap_servers: required("QDL_KAFKA_BOOTSTRAP_SERVERS")?,
        client_id: format!("phase92-primary-{identity}"),
        group_id: group_id.to_owned(),
        request_timeout: Duration::from_secs(15),
        tls: KafkaTlsConfig {
            ca_location: format!("{cert_root}/ca.crt"),
            certificate_location: format!("{cert_root}/phase8-{identity}.crt"),
            key_location: format!("{cert_root}/phase8-{identity}.key"),
            key_password: None,
        },
    })
}

fn durable_record<T: Serialize>(
    stream: &str,
    key: &str,
    value: &T,
    nonce: &str,
) -> Result<DurableRecord, Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(value)?;
    let mut event_id = Sha256::new();
    event_id.update(stream.as_bytes());
    event_id.update(key.as_bytes());
    event_id.update(nonce.as_bytes());
    event_id.update(&payload);
    Ok(DurableRecord {
        stream: stream.to_owned(),
        partition_key: key.to_owned(),
        event_id: event_id.finalize().to_vec(),
        payload,
        accepted_at_ns: now_ns()?,
    })
}

async fn receive_authority(
    source: &KafkaEventSource,
    slice_id: &str,
    revision: u64,
) -> Result<Phase92AuthorityRecord, Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let (record, _) = source.next().await?;
            let authority: Phase92AuthorityRecord = serde_json::from_slice(&record.payload)
                .map_err(|error| qdl_kafka::KafkaTransportError::Fencing(error.to_string()))?;
            if authority.slice_id == slice_id && authority.authority_revision == revision {
                source.checkpoint()?;
                return Ok::<Phase92AuthorityRecord, qdl_kafka::KafkaTransportError>(authority);
            }
        }
    })
    .await
    .map_err(|_| "timed out reading persistent Phase 9.2 authority record")?
    .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
}

async fn receive_projection_range(
    source: &KafkaEventSource,
    slice_id: &str,
    owner_id: &str,
    authority_revision: u64,
    first_watermark: u64,
    last_watermark: u64,
) -> Result<u64, Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut observed = BTreeSet::new();
        loop {
            let (record, _) = source.next().await?;
            let payload: serde_json::Value = serde_json::from_slice(&record.payload)
                .map_err(|error| qdl_kafka::KafkaTransportError::Fencing(error.to_string()))?;
            let watermark = payload
                .get("source_watermark")
                .and_then(serde_json::Value::as_u64)
                .ok_or(qdl_kafka::KafkaTransportError::MissingField(
                    "source_watermark",
                ))?;
            let terminal_identity_matches = watermark != last_watermark
                || (payload.get("owner_id").and_then(serde_json::Value::as_str) == Some(owner_id)
                    && payload
                        .get("authority_revision")
                        .and_then(serde_json::Value::as_u64)
                        == Some(authority_revision));
            if payload.get("slice_id").and_then(serde_json::Value::as_str) != Some(slice_id)
                || !terminal_identity_matches
                || watermark < first_watermark
                || watermark > last_watermark
                || !observed.insert(watermark)
            {
                return Err(qdl_kafka::KafkaTransportError::Fencing(
                    "Phase 9.2 durable projection recovery diverged".into(),
                ));
            }
            if watermark == last_watermark {
                let expected: BTreeSet<u64> = (first_watermark..=last_watermark).collect();
                if observed != expected {
                    return Err(qdl_kafka::KafkaTransportError::Fencing(
                        "Phase 9.2 durable projection recovery has a gap".into(),
                    ));
                }
                source.checkpoint()?;
                return Ok(last_watermark);
            }
        }
    })
    .await
    .map_err(|_| "timed out reading Phase 9.2 durable projection")?
    .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
}

#[allow(clippy::too_many_arguments)]
fn authority(
    slice_id: &str,
    state: Phase92AuthorityState,
    owner_id: &str,
    revision: u64,
    lease_epoch: u64,
    candidate_digest: &str,
    bundle_id: Option<&str>,
    start_watermark: u64,
    terminal_watermark: Option<u64>,
    previous_owner_id: Option<&str>,
    handoff_digest: Option<String>,
    approved_at_ns: i64,
    hold_until_ns: i64,
) -> Phase92AuthorityRecord {
    let primary = matches!(
        state,
        Phase92AuthorityState::RustPrimary | Phase92AuthorityState::PythonPrimary
    );
    let active = matches!(
        state,
        Phase92AuthorityState::RustCanary
            | Phase92AuthorityState::RustPrimary
            | Phase92AuthorityState::PythonPrimary
    );
    Phase92AuthorityRecord {
        schema: "qdl.authority-record.v3".into(),
        slice_id: slice_id.into(),
        state,
        owner_id: owner_id.into(),
        authority_revision: revision,
        lease_epoch,
        partition_plan_epoch: 1,
        candidate_digest: candidate_digest.into(),
        prerequisite_bundle_id: bundle_id.map(str::to_owned),
        start_watermark,
        terminal_watermark,
        previous_owner_id: previous_owner_id.map(str::to_owned),
        handoff_digest,
        approved_by: active.then(|| "phase92-isolated-rehearsal".into()),
        approved_at_ns: active.then_some(approved_at_ns),
        hold_until_ns: active.then_some(hold_until_ns),
        public_write_allowed: primary,
        legacy_write_allowed: primary,
    }
}

#[allow(clippy::too_many_arguments)]
fn checkpoint(
    checkpoint_id: &str,
    slice_id: &str,
    owner_id: &str,
    revision: u64,
    lease_epoch: u64,
    watermark: u64,
    candidate_digest: &str,
    nonce: &str,
) -> Result<Phase92TerminalCheckpoint, Box<dyn std::error::Error>> {
    let terminal_payload_sha256 = hex::encode(Sha256::digest(
        format!("{slice_id}:{owner_id}:{revision}:{watermark}:{nonce}").as_bytes(),
    ));
    Ok(Phase92TerminalCheckpoint {
        schema: "qdl.terminal-owner-checkpoint.v1".into(),
        checkpoint_id: checkpoint_id.into(),
        slice_id: slice_id.into(),
        owner_id: owner_id.into(),
        authority_revision: revision,
        lease_epoch,
        partition_plan_epoch: 1,
        source_session_id: format!("phase92-{owner_id}-{nonce}"),
        connection_generation: 1,
        terminal_watermark: watermark,
        terminal_event_id: format!("phase92-event-{watermark}"),
        terminal_payload_sha256,
        candidate_digest: candidate_digest.into(),
        committed_at_ns: now_ns()?,
    })
}

#[allow(clippy::too_many_arguments)]
fn handoff(
    handoff_id: &str,
    direction: Phase92HandoffDirection,
    checkpoint: &Phase92TerminalCheckpoint,
    new_owner_id: &str,
    new_state: Phase92AuthorityState,
    bundle_id: &str,
    approved_at_ns: i64,
    expires_at_ns: i64,
) -> Result<Phase92AcceptedHandoff, Box<dyn std::error::Error>> {
    let expected_state = match direction {
        Phase92HandoffDirection::PythonToRust => Phase92AuthorityState::RustCanary,
        Phase92HandoffDirection::RustToPython => Phase92AuthorityState::RollbackPending,
    };
    let result = Phase92AcceptedHandoff {
        schema: "qdl.accepted-authority-handoff.v1".into(),
        handoff_id: handoff_id.into(),
        direction,
        checkpoint_digest: checkpoint.digest()?,
        slice_id: checkpoint.slice_id.clone(),
        old_owner_id: checkpoint.owner_id.clone(),
        new_owner_id: new_owner_id.into(),
        expected_state,
        new_state,
        expected_authority_revision: checkpoint.authority_revision,
        new_authority_revision: checkpoint.authority_revision + 1,
        expected_lease_epoch: checkpoint.lease_epoch,
        new_lease_epoch: checkpoint.lease_epoch + 1,
        partition_plan_epoch: 1,
        terminal_watermark: checkpoint.terminal_watermark,
        first_new_watermark: checkpoint.terminal_watermark + 1,
        overlap_start_watermark: checkpoint.terminal_watermark.saturating_sub(10),
        overlap_end_watermark: checkpoint.terminal_watermark,
        old_event_count: 11,
        new_event_count: 11,
        semantic_mismatches: 0,
        open_gaps: 0,
        candidate_digest: checkpoint.candidate_digest.clone(),
        prerequisite_bundle_id: bundle_id.into(),
        approved_by: "phase92-isolated-rehearsal".into(),
        approved_at_ns,
        expires_at_ns,
    };
    result.validate(checkpoint)?;
    Ok(result)
}

fn publication(
    record: &Phase92AuthorityRecord,
    watermark: u64,
    target: SinkTarget,
) -> Phase92PublicationContext {
    Phase92PublicationContext {
        slice_id: record.slice_id.clone(),
        owner_id: record.owner_id.clone(),
        authority_revision: record.authority_revision,
        shard_id: "binance-usdm-trade-0".into(),
        lease_epoch: record.lease_epoch,
        partition_plan_epoch: record.partition_plan_epoch,
        source_watermark: watermark,
        target,
    }
}

async fn persist_authority(
    authority_sink: &KafkaDurableSink,
    audit_sink: &KafkaDurableSink,
    authority_source: &KafkaEventSource,
    authority_topic: &str,
    audit_topic: &str,
    nonce: &str,
    record: &Phase92AuthorityRecord,
) -> Result<(u64, u64, Phase92AuthorityRecord), Box<dyn std::error::Error>> {
    let revision = record.authority_revision;
    let authority_offset = authority_sink
        .append(&durable_record(
            authority_topic,
            &record.slice_id,
            record,
            &format!("{nonce}:authority:{revision}"),
        )?)
        .await?
        .cursor
        .offset;
    let audit_offset = audit_sink
        .append(&durable_record(
            audit_topic,
            &format!("{}:{revision}", record.slice_id),
            record,
            &format!("{nonce}:audit:{revision}"),
        )?)
        .await?
        .cursor
        .offset;
    let persisted = receive_authority(authority_source, &record.slice_id, revision).await?;
    Ok((authority_offset, audit_offset, persisted))
}

async fn publish_range(
    sink: &Phase92FencedKafkaSink,
    topics: &Phase92SinkTopics,
    record: &Phase92AuthorityRecord,
    first: u64,
    last: u64,
    nonce: &str,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let mut offsets = Vec::new();
    for watermark in first..=last {
        let payload = json!({
            "schema": "qdl.phase92.isolated-projection.v1",
            "slice_id": record.slice_id,
            "owner_id": record.owner_id,
            "authority_revision": record.authority_revision,
            "lease_epoch": record.lease_epoch,
            "source_watermark": watermark,
            "provider_provenance": "REAL_PROVIDER_READ_ONLY_CAPTURE",
        });
        for (target, topic) in [
            (
                SinkTarget::PrimaryCanonical,
                topics.primary_canonical.as_str(),
            ),
            (SinkTarget::PublicV2, topics.public_v2.as_str()),
            (SinkTarget::LegacyV1, topics.legacy_v1.as_str()),
        ] {
            offsets.push(
                sink.append(
                    &durable_record(
                        topic,
                        "btc-usdt",
                        &payload,
                        &format!("{nonce}:{watermark}:{target:?}"),
                    )?,
                    &publication(record, watermark, target),
                    now_ns()?,
                )
                .await?
                .cursor
                .offset,
            );
        }
    }
    Ok(offsets)
}

#[allow(clippy::too_many_arguments)]
async fn run_recovery_verify(
    authority_topic: &str,
    primary_topic: &str,
    public_topic: &str,
    legacy_topic: &str,
    slice_id: &str,
    nonce: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected_revision: u64 = required("QDL_RECOVERY_AUTHORITY_REVISION")?.parse()?;
    let first_watermark: u64 = required("QDL_RECOVERY_FIRST_WATERMARK")?.parse()?;
    let last_watermark: u64 = required("QDL_RECOVERY_LAST_WATERMARK")?.parse()?;
    if first_watermark == 0 || last_watermark < first_watermark {
        return Err("Phase 9.2 recovery watermark range is invalid".into());
    }

    let producer_group = format!("phase8-phase92-recovery-producer-{nonce}");
    let producer_config = transport_config("producer", &producer_group)?;
    let authority_group = format!("phase8-phase92-recovery-authority-{nonce}");
    let authority_config = transport_config("consumer", &authority_group)?;
    let authority_source = KafkaEventSource::new(&authority_config, &[authority_topic])?;
    let latest = receive_authority(&authority_source, slice_id, expected_revision).await?;
    if latest.state != Phase92AuthorityState::PythonPrimary {
        return Err("Phase 9.2 recovery expected terminal Python primary authority".into());
    }

    let topics = Phase92SinkTopics {
        primary_canonical: primary_topic.to_owned(),
        public_v2: public_topic.to_owned(),
        legacy_v1: legacy_topic.to_owned(),
    };
    let fenced_sink = Phase92FencedKafkaSink::new(&producer_config, topics.clone())?;
    fenced_sink.apply_authority(latest.clone()).await?;
    let next_watermark = last_watermark
        .checked_add(1)
        .ok_or("Phase 9.2 recovery watermark overflow")?;
    let pre_restore = fenced_sink
        .append(
            &durable_record(
                primary_topic,
                "pre-restore",
                &json!({"must_not_publish": "pre-restore"}),
                &format!("{nonce}:pre-restore"),
            )?,
            &publication(&latest, next_watermark, SinkTarget::PrimaryCanonical),
            now_ns()?,
        )
        .await;

    let mut observed = serde_json::Map::new();
    let mut target_pre_restore_rejected = true;
    let mut duplicate_after_restore_rejected = true;
    for (name, target, topic) in [
        ("primary", SinkTarget::PrimaryCanonical, primary_topic),
        ("public", SinkTarget::PublicV2, public_topic),
        ("legacy", SinkTarget::LegacyV1, legacy_topic),
    ] {
        let group = format!("phase8-phase92-recovery-{name}-{nonce}");
        let config = transport_config("consumer", &group)?;
        let source = KafkaEventSource::new(&config, &[topic])?;
        let durable_watermark = receive_projection_range(
            &source,
            slice_id,
            &latest.owner_id,
            latest.authority_revision,
            first_watermark,
            last_watermark,
        )
        .await?;
        observed.insert(name.into(), json!(durable_watermark));

        target_pre_restore_rejected &= fenced_sink
            .append(
                &durable_record(
                    topic,
                    "target-pre-restore",
                    &json!({"must_not_publish": name}),
                    &format!("{nonce}:{name}:pre-restore"),
                )?,
                &publication(&latest, next_watermark, target),
                now_ns()?,
            )
            .await
            .is_err();
        let restored = publication(&latest, durable_watermark, target);
        fenced_sink.restore_committed_watermark(&restored).await?;
        duplicate_after_restore_rejected &= fenced_sink
            .append(
                &durable_record(
                    topic,
                    "duplicate-after-restore",
                    &json!({"must_not_publish": name}),
                    &format!("{nonce}:{name}:duplicate"),
                )?,
                &restored,
                now_ns()?,
            )
            .await
            .is_err();
    }

    let projection_offsets = publish_range(
        &fenced_sink,
        &topics,
        &latest,
        next_watermark,
        next_watermark,
        nonce,
    )
    .await?;
    let checks = json!({
        "restart_pre_restore_failed_closed": pre_restore.is_err(),
        "each_target_pre_restore_failed_closed": target_pre_restore_rejected,
        "durable_target_watermarks_restored": observed.values().all(|value| value == &json!(last_watermark)),
        "duplicate_after_restore_rejected": duplicate_after_restore_rejected,
        "resumed_at_exact_next_watermark": projection_offsets.len() == 3,
    });
    let passed = checks
        .as_object()
        .is_some_and(|values| values.values().all(|value| value == &json!(true)));
    println!(
        "{}",
        serde_json::to_string(&json!({
            "schema": "qdl.phase92.process-restart-recovery.v1",
            "status": if passed { "PASS" } else { "FAIL" },
            "mode": "RECOVERY_VERIFY",
            "production_authorized": false,
            "authority_revision": latest.authority_revision,
            "owner_id": latest.owner_id,
            "restored_target_watermarks": observed,
            "resumed_watermark": next_watermark,
            "projection_offsets": projection_offsets,
            "checks": checks,
            "production_public_writes": 0,
            "production_legacy_writes": 0,
        }))?
    );
    if !passed {
        return Err("Phase 9.2 process restart recovery failed".into());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let authority_topic = required("QDL_AUTHORITY_TOPIC")?;
    let audit_topic = required("QDL_AUDIT_TOPIC")?;
    let checkpoint_topic = required("QDL_CHECKPOINT_TOPIC")?;
    let handoff_topic = required("QDL_HANDOFF_TOPIC")?;
    let primary_topic = required("QDL_PRIMARY_CANONICAL_TOPIC")?;
    let public_topic = required("QDL_ISOLATED_PUBLIC_TOPIC")?;
    let legacy_topic = required("QDL_ISOLATED_LEGACY_TOPIC")?;
    let production_public = required("QDL_PRODUCTION_PUBLIC_TOPIC")?;
    let production_legacy = required("QDL_PRODUCTION_LEGACY_TOPIC")?;
    let required_topics = [
        &authority_topic,
        &audit_topic,
        &checkpoint_topic,
        &handoff_topic,
        &primary_topic,
        &public_topic,
        &legacy_topic,
    ];
    if required_topics
        .iter()
        .any(|topic| !topic.contains(".phase92."))
        || required_topics
            .iter()
            .any(|topic| topic.as_str() == production_public || topic.as_str() == production_legacy)
    {
        return Err("Phase 9.2 rehearsal topic is not isolated".into());
    }

    let nonce = required("QDL_AUTHORITY_NONCE")?;
    let candidate_digest = required("QDL_CANDIDATE_DIGEST")?;
    let bundle_id = required("QDL_PREREQUISITE_BUNDLE_ID")?;
    let slice_id = required("QDL_SLICE_ID")?;
    let python_owner = required("QDL_PYTHON_OWNER_ID")?;
    let rust_owner = required("QDL_RUST_OWNER_ID")?;
    let rollback_owner = required("QDL_ROLLBACK_OWNER_ID")?;
    let mode = env::var("QDL_REHEARSAL_MODE").unwrap_or_else(|_| "FULL".into());
    if mode == "RECOVERY_VERIFY" {
        return run_recovery_verify(
            &authority_topic,
            &primary_topic,
            &public_topic,
            &legacy_topic,
            &slice_id,
            &nonce,
        )
        .await;
    }
    if mode != "FULL" {
        return Err(format!("unsupported Phase 9.2 rehearsal mode: {mode}").into());
    }
    let group = format!("phase8-phase92-authority-{nonce}");
    let producer_config = transport_config("producer", &group)?;
    let consumer_config = transport_config("consumer", &group)?;
    let authority_sink = KafkaDurableSink::new(&producer_config)?;
    let audit_sink = KafkaDurableSink::new(&producer_config)?;
    let evidence_sink = KafkaDurableSink::new(&producer_config)?;
    let authority_source = KafkaEventSource::new(&consumer_config, &[&authority_topic])?;
    let topics = Phase92SinkTopics {
        primary_canonical: primary_topic,
        public_v2: public_topic,
        legacy_v1: legacy_topic,
    };
    let fenced_sink = Phase92FencedKafkaSink::new(&producer_config, topics.clone())?;
    let started_at = now_ns()?;
    let approved_at = started_at - 1_000_000_000;
    let hold_until = started_at + 300_000_000_000;
    let mut authority_offsets = Vec::new();
    let mut audit_offsets = Vec::new();
    let mut checkpoint_offsets = Vec::new();
    let mut handoff_offsets = Vec::new();
    let mut projection_offsets = Vec::new();
    let mut checks = serde_json::Map::new();

    let initial = authority(
        &slice_id,
        Phase92AuthorityState::RustCanary,
        &python_owner,
        7,
        11,
        &candidate_digest,
        Some(&bundle_id),
        89,
        None,
        None,
        None,
        approved_at,
        hold_until,
    );
    let (authority_offset, audit_offset, persisted) = persist_authority(
        &authority_sink,
        &audit_sink,
        &authority_source,
        &authority_topic,
        &audit_topic,
        &nonce,
        &initial,
    )
    .await?;
    authority_offsets.push(authority_offset);
    audit_offsets.push(audit_offset);
    fenced_sink.apply_authority(persisted).await?;

    let terminal = checkpoint(
        "11111111-1111-4111-8111-111111111192",
        &slice_id,
        &python_owner,
        7,
        11,
        100,
        &candidate_digest,
        &nonce,
    )?;
    let to_rust = handoff(
        "22222222-2222-4222-8222-222222222192",
        Phase92HandoffDirection::PythonToRust,
        &terminal,
        &rust_owner,
        Phase92AuthorityState::RustPrimary,
        &bundle_id,
        approved_at,
        hold_until,
    )?;
    checkpoint_offsets.push(
        evidence_sink
            .append(&durable_record(
                &checkpoint_topic,
                &slice_id,
                &terminal,
                &format!("{nonce}:checkpoint:python"),
            )?)
            .await?
            .cursor
            .offset,
    );
    handoff_offsets.push(
        evidence_sink
            .append(&durable_record(
                &handoff_topic,
                &slice_id,
                &to_rust,
                &format!("{nonce}:handoff:rust"),
            )?)
            .await?
            .cursor
            .offset,
    );

    let rust_primary = authority(
        &slice_id,
        Phase92AuthorityState::RustPrimary,
        &rust_owner,
        8,
        12,
        &candidate_digest,
        Some(&bundle_id),
        100,
        Some(100),
        Some(&python_owner),
        Some(to_rust.digest()?),
        approved_at,
        hold_until,
    );
    checks.insert(
        "direct_primary_without_handoff_rejected".into(),
        json!(fenced_sink
            .apply_authority(rust_primary.clone())
            .await
            .is_err()),
    );
    let cutover_started = Instant::now();
    let (authority_offset, audit_offset, persisted) = persist_authority(
        &authority_sink,
        &audit_sink,
        &authority_source,
        &authority_topic,
        &audit_topic,
        &nonce,
        &rust_primary,
    )
    .await?;
    authority_offsets.push(authority_offset);
    audit_offsets.push(audit_offset);
    fenced_sink
        .apply_handoff(&terminal, &to_rust, persisted, now_ns()?)
        .await?;
    let cutover_ms = cutover_started.elapsed().as_secs_f64() * 1_000.0;

    for (name, context) in [
        (
            "terminal_watermark_rejected",
            publication(&rust_primary, 100, SinkTarget::PrimaryCanonical),
        ),
        (
            "gap_watermark_rejected",
            publication(&rust_primary, 102, SinkTarget::PrimaryCanonical),
        ),
        (
            "stale_owner_rejected",
            Phase92PublicationContext {
                owner_id: python_owner.clone(),
                ..publication(&rust_primary, 101, SinkTarget::PrimaryCanonical)
            },
        ),
        (
            "stale_revision_rejected",
            Phase92PublicationContext {
                authority_revision: 7,
                ..publication(&rust_primary, 101, SinkTarget::PrimaryCanonical)
            },
        ),
        (
            "stale_lease_rejected",
            Phase92PublicationContext {
                lease_epoch: 11,
                ..publication(&rust_primary, 101, SinkTarget::PrimaryCanonical)
            },
        ),
        (
            "wrong_plan_rejected",
            Phase92PublicationContext {
                partition_plan_epoch: 2,
                ..publication(&rust_primary, 101, SinkTarget::PrimaryCanonical)
            },
        ),
    ] {
        checks.insert(
            name.into(),
            json!(fenced_sink
                .append(
                    &durable_record(
                        &topics.primary_canonical,
                        "rejected",
                        &json!({"must_not_publish": name}),
                        &format!("{nonce}:rejected:{name}"),
                    )?,
                    &context,
                    now_ns()?,
                )
                .await
                .is_err()),
        );
    }

    projection_offsets
        .extend(publish_range(&fenced_sink, &topics, &rust_primary, 101, 164, &nonce).await?);
    checks.insert(
        "duplicate_after_ack_rejected".into(),
        json!(fenced_sink
            .append(
                &durable_record(
                    &topics.primary_canonical,
                    "duplicate",
                    &json!({"must_not_publish": "duplicate"}),
                    &format!("{nonce}:duplicate"),
                )?,
                &publication(&rust_primary, 164, SinkTarget::PrimaryCanonical),
                now_ns()?,
            )
            .await
            .is_err()),
    );

    let mut blocked = rust_primary.clone();
    blocked.state = Phase92AuthorityState::Blocked;
    blocked.authority_revision = 9;
    blocked.public_write_allowed = false;
    blocked.legacy_write_allowed = false;
    let (authority_offset, audit_offset, persisted) = persist_authority(
        &authority_sink,
        &audit_sink,
        &authority_source,
        &authority_topic,
        &audit_topic,
        &nonce,
        &blocked,
    )
    .await?;
    authority_offsets.push(authority_offset);
    audit_offsets.push(audit_offset);
    fenced_sink.apply_authority(persisted).await?;

    let mut pending = blocked.clone();
    pending.state = Phase92AuthorityState::RollbackPending;
    pending.authority_revision = 10;
    let (authority_offset, audit_offset, persisted) = persist_authority(
        &authority_sink,
        &audit_sink,
        &authority_source,
        &authority_topic,
        &audit_topic,
        &nonce,
        &pending,
    )
    .await?;
    authority_offsets.push(authority_offset);
    audit_offsets.push(audit_offset);
    fenced_sink.apply_authority(persisted).await?;

    let rust_terminal = checkpoint(
        "33333333-3333-4333-8333-333333333192",
        &slice_id,
        &rust_owner,
        10,
        12,
        164,
        &candidate_digest,
        &nonce,
    )?;
    let to_python = handoff(
        "44444444-4444-4444-8444-444444444192",
        Phase92HandoffDirection::RustToPython,
        &rust_terminal,
        &rollback_owner,
        Phase92AuthorityState::PythonPrimary,
        &bundle_id,
        approved_at,
        hold_until,
    )?;
    checkpoint_offsets.push(
        evidence_sink
            .append(&durable_record(
                &checkpoint_topic,
                &slice_id,
                &rust_terminal,
                &format!("{nonce}:checkpoint:rust"),
            )?)
            .await?
            .cursor
            .offset,
    );
    handoff_offsets.push(
        evidence_sink
            .append(&durable_record(
                &handoff_topic,
                &slice_id,
                &to_python,
                &format!("{nonce}:handoff:python"),
            )?)
            .await?
            .cursor
            .offset,
    );
    let python_primary = authority(
        &slice_id,
        Phase92AuthorityState::PythonPrimary,
        &rollback_owner,
        11,
        13,
        &candidate_digest,
        None,
        164,
        Some(164),
        Some(&rust_owner),
        Some(to_python.digest()?),
        approved_at,
        hold_until,
    );
    let rollback_started = Instant::now();
    let (authority_offset, audit_offset, persisted) = persist_authority(
        &authority_sink,
        &audit_sink,
        &authority_source,
        &authority_topic,
        &audit_topic,
        &nonce,
        &python_primary,
    )
    .await?;
    authority_offsets.push(authority_offset);
    audit_offsets.push(audit_offset);
    fenced_sink
        .apply_handoff(&rust_terminal, &to_python, persisted, now_ns()?)
        .await?;
    projection_offsets
        .extend(publish_range(&fenced_sink, &topics, &python_primary, 165, 180, &nonce).await?);
    let rollback_ms = rollback_started.elapsed().as_secs_f64() * 1_000.0;
    checks.insert(
        "rust_after_rollback_rejected".into(),
        json!(fenced_sink
            .append(
                &durable_record(
                    &topics.primary_canonical,
                    "stale-rust",
                    &json!({"must_not_publish": "stale-rust"}),
                    &format!("{nonce}:stale-rust"),
                )?,
                &publication(&rust_primary, 165, SinkTarget::PrimaryCanonical),
                now_ns()?,
            )
            .await
            .is_err()),
    );

    let passed = checks.values().all(|value| value == &json!(true))
        && authority_offsets.len() == 5
        && audit_offsets.len() == 5
        && checkpoint_offsets.len() == 2
        && handoff_offsets.len() == 2
        && projection_offsets.len() == 240;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "schema": "qdl.phase92.isolated-primary-runtime.v1",
            "status": if passed { "PASS" } else { "FAIL" },
            "mode": "ISOLATED_REHEARSAL",
            "production_authorized": false,
            "authority_transitions": [
                "RUST_CANARY", "RUST_PRIMARY", "BLOCKED",
                "ROLLBACK_PENDING", "PYTHON_PRIMARY"
            ],
            "authority_offsets": authority_offsets,
            "audit_offsets": audit_offsets,
            "checkpoint_offsets": checkpoint_offsets,
            "handoff_offsets": handoff_offsets,
            "projection_offsets": projection_offsets,
            "checks": checks,
            "cutover_ms": cutover_ms,
            "rollback_ms": rollback_ms,
            "first_rust_watermark": 101,
            "last_rust_watermark": 164,
            "first_python_rollback_watermark": 165,
            "last_watermark": 180,
            "isolated_primary_writes": 80,
            "isolated_public_writes": 80,
            "isolated_legacy_writes": 80,
            "production_public_writes": 0,
            "production_legacy_writes": 0,
            "final_authority": "PYTHON_PRIMARY",
        }))?
    );
    if !passed {
        return Err("Phase 9.2 isolated primary rehearsal failed".into());
    }
    Ok(())
}
