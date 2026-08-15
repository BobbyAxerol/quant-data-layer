#![forbid(unsafe_code)]

use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use qdl_core::transport::DurableRecord;
use qdl_kafka::{KafkaDurableSink, KafkaEventSource, KafkaTlsConfig, KafkaTransportConfig};
use serde_json::json;
use sha2::{Digest, Sha256};

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("required environment variable is missing: {name}"))
}

fn transport_config(identity: &str, group_id: &str) -> Result<KafkaTransportConfig, String> {
    let cert_root = required("QDL_KAFKA_CERT_ROOT")?;
    Ok(KafkaTransportConfig {
        bootstrap_servers: required("QDL_KAFKA_BOOTSTRAP_SERVERS")?,
        client_id: format!("phase8-rust-{identity}"),
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topic = required("QDL_KAFKA_SMOKE_TOPIC")?;
    let nonce = required("QDL_KAFKA_SMOKE_NONCE")?;
    let event_id = Sha256::digest(format!("phase8-rust-smoke:{nonce}")).to_vec();
    let group_id = format!("phase8-rust-smoke-{nonce}");
    let sink = KafkaDurableSink::new(&transport_config("producer", &group_id)?)?;
    let source = KafkaEventSource::new(&transport_config("consumer", &group_id)?, &[&topic])?;
    let payload = serde_json::to_vec(&json!({
        "kind": "phase8-rust-transport-smoke",
        "nonce": nonce,
    }))?;
    let accepted_at_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .try_into()?;
    let record = DurableRecord {
        stream: topic.clone(),
        partition_key: format!("smoke:{nonce}"),
        event_id: event_id.clone(),
        payload: payload.clone(),
        accepted_at_ns,
    };
    let append = sink.append(&record).await?;

    let received = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let value = source.next().await?;
            if value.0.event_id == event_id {
                return Ok::<_, qdl_kafka::KafkaTransportError>(value);
            }
        }
    })
    .await
    .map_err(|_| "timed out waiting for Rust Kafka transport record")??;
    source.checkpoint()?;

    if received.0.payload != payload || received.0.partition_key != record.partition_key {
        return Err("Rust transport changed key or payload".into());
    }
    if append.cursor.transport_partition != received.1.transport_partition
        || append.cursor.offset != received.1.offset
    {
        return Err("ACK-derived producer cursor differs from consumed cursor".into());
    }

    println!(
        "{}",
        serde_json::to_string(&json!({
            "status": "PASS",
            "topic": topic,
            "partition": append.cursor.transport_partition,
            "offset": append.cursor.offset,
            "event_id_sha256": hex::encode(event_id),
            "payload_bytes": payload.len(),
            "checkpointed": true,
        }))?
    );
    Ok(())
}
