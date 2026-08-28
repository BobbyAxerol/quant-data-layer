#![forbid(unsafe_code)]

use std::time::Instant;

use prost::Message;
use qdl_contracts::qdl::provider::v1::{
    CaptureBoundary, RawProviderEnvelope, TransportCompression, TransportProtocol,
};
use qdl_realtime_core::{CoreBinding, RealtimeCore, RealtimeCoreConfig};
use qdl_venue_core::ordering::SequencePolicy;
use serde_json::json;
use sha2::{Digest, Sha256};

fn percentile(values: &mut [u128], percentile: f64) -> u128 {
    values.sort_unstable();
    let index = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[index]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let events = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "100000".into())
        .parse::<u64>()?;
    let minimum_events_per_second = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "50000".into())
        .parse::<f64>()?;
    if events < 1000 || minimum_events_per_second <= 0.0 {
        return Err("benchmark bounds are invalid".into());
    }
    let binding = CoreBinding {
        provider: "BINANCE_DIRECT".into(),
        venue: "BINANCE".into(),
        market: "USDM".into(),
        product_type: "PERPETUAL".into(),
        native_symbol: "BTCUSDT".into(),
        native_channel: "trade".into(),
        provider_kind: "binance_usdm_trade".into(),
        instrument_uid: "benchmark-btcusdt".into(),
        instrument_id: "BINANCE.USDM.PERPETUAL.BTC-USDT".into(),
        instrument_revision: 1,
        instrument_catalog_revision: 1,
        source_id: "benchmark-binance-trade".into(),
        source_role: "PRIMARY".into(),
        normalizer_version: "qdl-rust-core/2.0.0".into(),
        require_final_bar: false,
        sequence_policy: SequencePolicy::Monotonic,
        l2: None,
    };
    let mut core = RealtimeCore::new(RealtimeCoreConfig {
        canonical_stream: "benchmark.canonical".into(),
        quarantine_stream: "benchmark.quarantine".into(),
        allow_test_provenance: true,
        dedup_capacity: events as usize,
        bindings: vec![binding],
    })?;
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(events as usize);
    let mut output_count = 0_u64;
    let mut output_bytes = 0_u64;
    for sequence in 1..=events {
        let raw_frame = serde_json::to_vec(&json!({
            "s": "BTCUSDT", "t": sequence, "p": "60000.10",
            "q": "0.001", "T": 1_786_352_400_000_u64 + sequence, "m": false,
        }))?;
        let capture_id = Sha256::digest(sequence.to_be_bytes())[..16].to_vec();
        let envelope = RawProviderEnvelope {
            raw_schema_name: "qdl.provider.raw".into(),
            raw_schema_major: 1,
            raw_schema_minor: 0,
            capture_id,
            provider: "BINANCE_DIRECT".into(),
            venue: "BINANCE".into(),
            market: "USDM".into(),
            product_type: "PERPETUAL".into(),
            native_symbol: "BTCUSDT".into(),
            native_channel: "trade".into(),
            subscription_id: "benchmark".into(),
            source_session_id: "benchmark-session".into(),
            connection_generation: 1,
            lease_epoch: 1,
            authority_revision: 1,
            partition_plan_epoch: 1,
            received_at_ns: 1_786_352_400_000_000_000 + sequence as i64,
            transport_protocol: TransportProtocol::FileReplay as i32,
            transport_compression: TransportCompression::None as i32,
            capture_boundary: CaptureBoundary::ReplayBytes as i32,
            raw_frame_sha256: Sha256::digest(&raw_frame).to_vec(),
            raw_frame_bytes: raw_frame,
            adapter_version: "benchmark/1".into(),
            config_revision: 1,
            instrument_catalog_revision: 1,
            correlation_id: format!("benchmark-{sequence}"),
            test_provenance: true,
        };
        let encoded = envelope.encode_to_vec();
        let event_started = Instant::now();
        let batch = core.process_bytes(&encoded, 1_786_352_400_100_000_000 + sequence as i64)?;
        latencies.push(event_started.elapsed().as_nanos());
        if batch.canonical.len() != 1 || !batch.quarantines.is_empty() || batch.duplicates != 0 {
            return Err("benchmark core produced a non-canonical decision".into());
        }
        output_count += 1;
        output_bytes += batch.canonical[0].payload.len() as u64;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let throughput = events as f64 / elapsed;
    let p50_ns = percentile(&mut latencies, 0.50);
    let p99_ns = percentile(&mut latencies, 0.99);
    let status = if throughput >= minimum_events_per_second {
        "PASS"
    } else {
        "FAIL"
    };
    println!(
        "{}",
        serde_json::to_string(&json!({
            "status": status,
            "events": events,
            "canonical": output_count,
            "quarantines": 0,
            "duplicates": 0,
            "elapsed_seconds": elapsed,
            "events_per_second": throughput,
            "minimum_events_per_second": minimum_events_per_second,
            "p50_ns": p50_ns,
            "p99_ns": p99_ns,
            "output_bytes": output_bytes,
        }))?
    );
    if status != "PASS" {
        return Err("realtime core throughput gate failed".into());
    }
    Ok(())
}
