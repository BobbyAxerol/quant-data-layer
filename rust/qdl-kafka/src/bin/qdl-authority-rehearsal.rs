#![forbid(unsafe_code)]

use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use qdl_core::transport::DurableRecord;
use qdl_kafka::{
    FencedKafkaSink, KafkaDurableSink, KafkaEventSource, KafkaTlsConfig, KafkaTransportConfig,
};
use qdl_venue_core::authority::{AuthorityMode, AuthorityRecord, PublicationContext, SinkTarget};
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
        client_id: format!("phase8-authority-{identity}"),
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
) -> Result<AuthorityRecord, Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let (record, _) = source.next().await?;
            let authority: AuthorityRecord = serde_json::from_slice(&record.payload)
                .map_err(|error| qdl_kafka::KafkaTransportError::Fencing(error.to_string()))?;
            if authority.slice_id == slice_id && authority.revision == revision {
                source.checkpoint()?;
                return Ok::<AuthorityRecord, qdl_kafka::KafkaTransportError>(authority);
            }
        }
    })
    .await
    .map_err(|_| "timed out reading persistent authority record")?
    .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
}

fn authority_record(
    slice_id: &str,
    revision: u64,
    mode: AuthorityMode,
    image_digest: &str,
    capability_digest: &str,
    contract_digest: &str,
    partition_plan_digest: &str,
) -> Result<AuthorityRecord, Box<dyn std::error::Error>> {
    Ok(AuthorityRecord {
        schema: "qdl.authority-record.v1".into(),
        slice_id: slice_id.into(),
        revision,
        mode,
        candidate_image_digest: image_digest.into(),
        capability_manifest_digest: capability_digest.into(),
        contract_digest: contract_digest.into(),
        partition_plan_digest: partition_plan_digest.into(),
        public_write_allowed: false,
        legacy_write_allowed: false,
        approved_by: "phase8-authority-rehearsal".into(),
        effective_at_ns: now_ns()?,
    })
}

fn publication(revision: u64, lease_epoch: u64, target: SinkTarget) -> PublicationContext {
    PublicationContext {
        slice_id: "BINANCE:USDM:TRADE:BTCUSDT".into(),
        authority_revision: revision,
        shard_id: "binance-usdm-trade-0".into(),
        lease_epoch,
        target,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let authority_topic = required("QDL_AUTHORITY_TOPIC")?;
    let canonical_topic = required("QDL_CANONICAL_TOPIC")?;
    let nonce = required("QDL_AUTHORITY_NONCE")?;
    let image_digest = required("QDL_CANDIDATE_IMAGE_DIGEST")?;
    let capability_digest = required("QDL_CAPABILITY_DIGEST")?;
    let contract_digest = required("QDL_CONTRACT_DIGEST")?;
    let partition_plan_digest = required("QDL_PARTITION_PLAN_DIGEST")?;
    let slice_id = "BINANCE:USDM:TRADE:BTCUSDT";
    let group = format!("phase8-authority-{nonce}");
    let producer_config = transport_config("producer", &group)?;
    let consumer_config = transport_config("consumer", &group)?;
    let authority_sink = KafkaDurableSink::new(&producer_config)?;
    let authority_source = KafkaEventSource::new(&consumer_config, &[&authority_topic])?;
    let fenced_sink = FencedKafkaSink::new(&producer_config)?;
    let mut authority_offsets = Vec::new();
    let mut canonical_offsets = Vec::new();

    for (revision, mode) in [
        (1, AuthorityMode::RustShadow),
        (2, AuthorityMode::RustCanary),
        (3, AuthorityMode::RustShadow),
    ] {
        let authority = authority_record(
            slice_id,
            revision,
            mode,
            &image_digest,
            &capability_digest,
            &contract_digest,
            &partition_plan_digest,
        )?;
        let durable = durable_record(
            &authority_topic,
            slice_id,
            serde_json::to_vec(&authority)?,
            &format!("{nonce}:{revision}"),
        )?;
        let append = authority_sink.append(&durable).await?;
        authority_offsets.push(append.cursor.offset);
        let persisted = receive_authority(&authority_source, slice_id, revision).await?;
        fenced_sink.apply_authority(persisted)?;

        let target = if mode == AuthorityMode::RustCanary {
            SinkTarget::CanaryCanonical
        } else {
            SinkTarget::ShadowCanonical
        };
        let event = durable_record(
            &canonical_topic,
            "btc-usdt",
            serde_json::to_vec(&json!({
                "kind": "phase8-authority-rehearsal",
                "revision": revision,
                "mode": mode,
                "nonce": nonce,
            }))?,
            &format!("{nonce}:canonical:{revision}"),
        )?;
        let append = fenced_sink
            .append(&event, &publication(revision, revision, target))
            .await?;
        canonical_offsets.push(append.cursor.offset);
    }

    let public_rejected = fenced_sink
        .append(
            &durable_record(
                &canonical_topic,
                "public",
                b"must-not-publish".to_vec(),
                &format!("{nonce}:public"),
            )?,
            &publication(3, 3, SinkTarget::PublicV2),
        )
        .await
        .is_err();
    let legacy_rejected = fenced_sink
        .append(
            &durable_record(
                &canonical_topic,
                "legacy",
                b"must-not-publish".to_vec(),
                &format!("{nonce}:legacy"),
            )?,
            &publication(3, 3, SinkTarget::LegacyV1),
        )
        .await
        .is_err();
    let stale_revision_rejected = fenced_sink
        .append(
            &durable_record(
                &canonical_topic,
                "stale",
                b"must-not-publish".to_vec(),
                &format!("{nonce}:stale"),
            )?,
            &publication(2, 2, SinkTarget::ShadowCanonical),
        )
        .await
        .is_err();
    let canary_after_rollback_rejected = fenced_sink
        .append(
            &durable_record(
                &canonical_topic,
                "canary",
                b"must-not-publish".to_vec(),
                &format!("{nonce}:canary-after-rollback"),
            )?,
            &publication(3, 3, SinkTarget::CanaryCanonical),
        )
        .await
        .is_err();

    let status = public_rejected
        && legacy_rejected
        && stale_revision_rejected
        && canary_after_rollback_rejected
        && authority_offsets.len() == 3
        && canonical_offsets.len() == 3;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "status": if status { "PASS" } else { "FAIL" },
            "transitions": ["RUST_SHADOW", "RUST_CANARY", "RUST_SHADOW"],
            "authority_offsets": authority_offsets,
            "canonical_shadow_offsets": canonical_offsets,
            "persistent_authority_records": 3,
            "public_write_attempts": 1,
            "public_writes": 0,
            "legacy_write_attempts": 1,
            "legacy_writes": 0,
            "public_rejected": public_rejected,
            "legacy_rejected": legacy_rejected,
            "stale_revision_rejected": stale_revision_rejected,
            "canary_after_rollback_rejected": canary_after_rollback_rejected,
            "final_authority": "RUST_SHADOW",
        }))?
    );
    if status {
        Ok(())
    } else {
        Err("authority rehearsal failed closed gate".into())
    }
}
