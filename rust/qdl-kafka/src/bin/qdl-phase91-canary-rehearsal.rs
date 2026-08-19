#![forbid(unsafe_code)]

use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use qdl_core::transport::DurableRecord;
use qdl_kafka::{
    KafkaDurableSink, KafkaEventSource, KafkaTlsConfig, KafkaTransportConfig,
    Phase9FencedKafkaSink, Phase9SinkTopics,
};
use qdl_venue_core::authority::{
    Phase9AuthorityRecord, Phase9AuthorityState, Phase9PublicationContext, SinkTarget,
};
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
        client_id: format!("phase91-canary-{identity}"),
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

fn durable_record(
    stream: &str,
    partition_key: &str,
    payload: Vec<u8>,
    nonce: &str,
) -> Result<DurableRecord, Box<dyn std::error::Error>> {
    let mut event_id = Sha256::new();
    event_id.update(stream.as_bytes());
    event_id.update(partition_key.as_bytes());
    event_id.update(nonce.as_bytes());
    event_id.update(&payload);
    Ok(DurableRecord {
        stream: stream.to_owned(),
        partition_key: partition_key.to_owned(),
        event_id: event_id.finalize().to_vec(),
        payload,
        accepted_at_ns: now_ns()?,
    })
}

async fn receive_authority(
    source: &KafkaEventSource,
    slice_id: &str,
    revision: u64,
) -> Result<Phase9AuthorityRecord, Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let (record, _) = source.next().await?;
            let authority: Phase9AuthorityRecord = serde_json::from_slice(&record.payload)
                .map_err(|error| qdl_kafka::KafkaTransportError::Fencing(error.to_string()))?;
            if authority.slice_id == slice_id && authority.authority_revision == revision {
                source.checkpoint()?;
                return Ok::<Phase9AuthorityRecord, qdl_kafka::KafkaTransportError>(authority);
            }
        }
    })
    .await
    .map_err(|_| "timed out reading persistent Phase 9 authority record")?
    .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
}

#[allow(clippy::too_many_arguments)]
fn authority_record(
    slice_id: &str,
    owner_id: &str,
    candidate_digest: &str,
    bundle_id: &str,
    revision: u64,
    lease_epoch: u64,
    state: Phase9AuthorityState,
    start_watermark: u64,
    approved_at_ns: i64,
    hold_until_ns: i64,
) -> Phase9AuthorityRecord {
    let canary = state == Phase9AuthorityState::RustCanary;
    Phase9AuthorityRecord {
        schema: "qdl.authority-record.v2".into(),
        slice_id: slice_id.into(),
        state,
        owner_id: owner_id.into(),
        authority_revision: revision,
        lease_epoch,
        partition_plan_epoch: 1,
        candidate_digest: candidate_digest.into(),
        prerequisite_bundle_id: canary.then(|| bundle_id.into()),
        start_watermark,
        approved_by: canary.then(|| "phase91-isolated-rehearsal".into()),
        approved_at_ns: canary.then_some(approved_at_ns),
        hold_until_ns: canary.then_some(hold_until_ns),
        public_write_allowed: false,
        legacy_write_allowed: false,
    }
}

#[allow(clippy::too_many_arguments)]
fn publication(
    slice_id: &str,
    owner_id: &str,
    revision: u64,
    lease_epoch: u64,
    partition_plan_epoch: u64,
    source_watermark: u64,
    target: SinkTarget,
) -> Phase9PublicationContext {
    Phase9PublicationContext {
        slice_id: slice_id.into(),
        owner_id: owner_id.into(),
        authority_revision: revision,
        shard_id: "binance-usdm-trade-0".into(),
        lease_epoch,
        partition_plan_epoch,
        source_watermark,
        target,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let authority_topic = required("QDL_AUTHORITY_TOPIC")?;
    let audit_topic = required("QDL_AUDIT_TOPIC")?;
    let shadow_raw_topic = required("QDL_SHADOW_RAW_TOPIC")?;
    let shadow_topic = required("QDL_SHADOW_CANONICAL_TOPIC")?;
    let canary_topic = required("QDL_CANARY_CANONICAL_TOPIC")?;
    let public_topic = required("QDL_PUBLIC_TOPIC")?;
    let legacy_topic = required("QDL_LEGACY_TOPIC")?;
    let nonce = required("QDL_AUTHORITY_NONCE")?;
    let candidate_digest = required("QDL_CANDIDATE_DIGEST")?;
    let bundle_id = required("QDL_PREREQUISITE_BUNDLE_ID")?;
    let slice_id = required("QDL_SLICE_ID")?;
    let shadow_owner = required("QDL_SHADOW_OWNER_ID")?;
    let canary_owner = required("QDL_CANARY_OWNER_ID")?;
    let group = format!("phase8-phase91-authority-{nonce}");
    let producer_config = transport_config("producer", &group)?;
    let consumer_config = transport_config("consumer", &group)?;
    let authority_sink = KafkaDurableSink::new(&producer_config)?;
    let audit_sink = KafkaDurableSink::new(&producer_config)?;
    let authority_source = KafkaEventSource::new(&consumer_config, &[&authority_topic])?;
    let fenced_sink = Phase9FencedKafkaSink::new(
        &producer_config,
        Phase9SinkTopics {
            shadow_raw: shadow_raw_topic.clone(),
            shadow_canonical: shadow_topic.clone(),
            shadow_quarantine: format!("{shadow_topic}.quarantine"),
            canary_canonical: canary_topic.clone(),
        },
    )?;
    let started_at = now_ns()?;
    let approved_at = started_at - 1_000_000_000;
    let hold_until = started_at + 300_000_000_000;
    let transitions = [
        authority_record(
            &slice_id,
            &shadow_owner,
            &candidate_digest,
            &bundle_id,
            1,
            1,
            Phase9AuthorityState::RustShadow,
            100,
            approved_at,
            hold_until,
        ),
        authority_record(
            &slice_id,
            &canary_owner,
            &candidate_digest,
            &bundle_id,
            2,
            2,
            Phase9AuthorityState::RustCanary,
            100,
            approved_at,
            hold_until,
        ),
        authority_record(
            &slice_id,
            &canary_owner,
            &candidate_digest,
            &bundle_id,
            3,
            2,
            Phase9AuthorityState::Blocked,
            100,
            approved_at,
            hold_until,
        ),
        authority_record(
            &slice_id,
            &shadow_owner,
            &candidate_digest,
            &bundle_id,
            4,
            3,
            Phase9AuthorityState::RustShadow,
            100,
            approved_at,
            hold_until,
        ),
    ];
    let mut authority_offsets = Vec::new();
    let mut audit_offsets = Vec::new();
    let mut shadow_offsets = Vec::new();
    let mut canary_offsets = Vec::new();
    let mut checks = serde_json::Map::new();

    for authority in transitions {
        let revision = authority.authority_revision;
        let state = authority.state;
        let durable = durable_record(
            &authority_topic,
            &slice_id,
            serde_json::to_vec(&authority)?,
            &format!("{nonce}:authority:{revision}"),
        )?;
        authority_offsets.push(authority_sink.append(&durable).await?.cursor.offset);
        let audit = durable_record(
            &audit_topic,
            &format!("{slice_id}:{revision}"),
            serde_json::to_vec(&authority)?,
            &format!("{nonce}:audit:{revision}"),
        )?;
        audit_offsets.push(audit_sink.append(&audit).await?.cursor.offset);
        let persisted = receive_authority(&authority_source, &slice_id, revision).await?;
        fenced_sink.apply_authority(persisted).await?;

        match state {
            Phase9AuthorityState::RustShadow if revision == 1 => {
                let event = durable_record(
                    &shadow_topic,
                    "btc-usdt",
                    serde_json::to_vec(&json!({"kind": "phase91-shadow", "revision": revision}))?,
                    &format!("{nonce}:shadow:{revision}"),
                )?;
                shadow_offsets.push(
                    fenced_sink
                        .append(
                            &event,
                            &publication(
                                &slice_id,
                                &shadow_owner,
                                revision,
                                1,
                                1,
                                101,
                                SinkTarget::ShadowCanonical,
                            ),
                            now_ns()?,
                        )
                        .await?
                        .cursor
                        .offset,
                );
            }
            Phase9AuthorityState::RustCanary => {
                for source_watermark in 102..166 {
                    let event = durable_record(
                        &canary_topic,
                        "btc-usdt",
                        serde_json::to_vec(&json!({
                            "kind": "phase91-canary",
                            "revision": revision,
                            "source_watermark": source_watermark,
                        }))?,
                        &format!("{nonce}:canary:{source_watermark}"),
                    )?;
                    canary_offsets.push(
                        fenced_sink
                            .append(
                                &event,
                                &publication(
                                    &slice_id,
                                    &canary_owner,
                                    revision,
                                    2,
                                    1,
                                    source_watermark,
                                    SinkTarget::CanaryCanonical,
                                ),
                                now_ns()?,
                            )
                            .await?
                            .cursor
                            .offset,
                    );
                }
                let rejected_record = |stream: &str, label: &str| {
                    durable_record(
                        stream,
                        label,
                        b"must-not-publish".to_vec(),
                        &format!("{nonce}:rejected:{label}"),
                    )
                };
                let cases = [
                    (
                        "public_target_rejected",
                        rejected_record(&public_topic, "public")?,
                        publication(&slice_id, &canary_owner, 2, 2, 1, 166, SinkTarget::PublicV2),
                    ),
                    (
                        "legacy_target_rejected",
                        rejected_record(&legacy_topic, "legacy")?,
                        publication(&slice_id, &canary_owner, 2, 2, 1, 166, SinkTarget::LegacyV1),
                    ),
                    (
                        "topic_masquerade_rejected",
                        rejected_record(&public_topic, "masquerade")?,
                        publication(
                            &slice_id,
                            &canary_owner,
                            2,
                            2,
                            1,
                            166,
                            SinkTarget::CanaryCanonical,
                        ),
                    ),
                    (
                        "stale_owner_rejected",
                        rejected_record(&canary_topic, "owner")?,
                        publication(
                            &slice_id,
                            &shadow_owner,
                            2,
                            2,
                            1,
                            166,
                            SinkTarget::CanaryCanonical,
                        ),
                    ),
                    (
                        "stale_revision_rejected",
                        rejected_record(&canary_topic, "revision")?,
                        publication(
                            &slice_id,
                            &canary_owner,
                            1,
                            2,
                            1,
                            166,
                            SinkTarget::CanaryCanonical,
                        ),
                    ),
                    (
                        "stale_lease_rejected",
                        rejected_record(&canary_topic, "lease")?,
                        publication(
                            &slice_id,
                            &canary_owner,
                            2,
                            1,
                            1,
                            166,
                            SinkTarget::CanaryCanonical,
                        ),
                    ),
                    (
                        "wrong_plan_rejected",
                        rejected_record(&canary_topic, "plan")?,
                        publication(
                            &slice_id,
                            &canary_owner,
                            2,
                            2,
                            2,
                            166,
                            SinkTarget::CanaryCanonical,
                        ),
                    ),
                    (
                        "duplicate_watermark_rejected",
                        rejected_record(&canary_topic, "watermark")?,
                        publication(
                            &slice_id,
                            &canary_owner,
                            2,
                            2,
                            1,
                            165,
                            SinkTarget::CanaryCanonical,
                        ),
                    ),
                ];
                for (name, event, context) in cases {
                    checks.insert(
                        name.into(),
                        json!(fenced_sink
                            .append(&event, &context, now_ns()?)
                            .await
                            .is_err()),
                    );
                }
            }
            Phase9AuthorityState::Blocked => {
                let event = durable_record(
                    &canary_topic,
                    "blocked",
                    b"must-not-publish".to_vec(),
                    &format!("{nonce}:blocked"),
                )?;
                checks.insert(
                    "blocked_state_rejected".into(),
                    json!(fenced_sink
                        .append(
                            &event,
                            &publication(
                                &slice_id,
                                &canary_owner,
                                3,
                                2,
                                1,
                                166,
                                SinkTarget::CanaryCanonical,
                            ),
                            now_ns()?,
                        )
                        .await
                        .is_err()),
                );
            }
            Phase9AuthorityState::RustShadow => {
                let event = durable_record(
                    &shadow_topic,
                    "btc-usdt",
                    serde_json::to_vec(&json!({"kind": "phase91-shadow", "revision": revision}))?,
                    &format!("{nonce}:shadow:{revision}"),
                )?;
                shadow_offsets.push(
                    fenced_sink
                        .append(
                            &event,
                            &publication(
                                &slice_id,
                                &shadow_owner,
                                revision,
                                3,
                                1,
                                166,
                                SinkTarget::ShadowCanonical,
                            ),
                            now_ns()?,
                        )
                        .await?
                        .cursor
                        .offset,
                );
                let rejected = durable_record(
                    &canary_topic,
                    "after-rollback",
                    b"must-not-publish".to_vec(),
                    &format!("{nonce}:after-rollback"),
                )?;
                checks.insert(
                    "canary_after_rollback_rejected".into(),
                    json!(fenced_sink
                        .append(
                            &rejected,
                            &publication(
                                &slice_id,
                                &shadow_owner,
                                4,
                                3,
                                1,
                                167,
                                SinkTarget::CanaryCanonical,
                            ),
                            now_ns()?,
                        )
                        .await
                        .is_err()),
                );
            }
        }
    }

    let all_rejected = checks.values().all(|value| value == &json!(true));
    let passed = authority_offsets.len() == 4
        && audit_offsets.len() == 4
        && shadow_offsets.len() == 2
        && canary_offsets.len() == 64
        && all_rejected;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "status": if passed { "PASS" } else { "FAIL" },
            "schema": "qdl.phase91.isolated-canary-runtime.v1",
            "mode": "ISOLATED_REHEARSAL",
            "production_authorized": false,
            "transitions": ["RUST_SHADOW", "RUST_CANARY", "BLOCKED", "RUST_SHADOW"],
            "authority_offsets": authority_offsets,
            "audit_offsets": audit_offsets,
            "shadow_offsets": shadow_offsets,
            "canary_offsets": canary_offsets,
            "checks": checks,
            "public_writes": 0,
            "legacy_writes": 0,
            "final_authority": "RUST_SHADOW",
        }))?
    );
    if passed {
        Ok(())
    } else {
        Err("Phase 9.1 isolated canary rehearsal failed".into())
    }
}
