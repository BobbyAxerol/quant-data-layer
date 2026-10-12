//! Offline preparation only. Usage: n1_capture_inventory CORE.json CAPTURE.jsonl
//! CORE.json is a RealtimeCoreConfig (not the runtime config wrapper).
//! First JSONL line: {"schema":"n1_capture_inventory_input_v1",
//! "core_config_sha256":"<SHA256 of exact CORE.json bytes>","raw_stream":"<topic>",
//! "source_provenance":"captured_provider","processing_clock_policy":"<declared policy>",
//! "limits":{"max_rows":1000000,"max_row_bytes":1048576,
//! "max_output_row_bytes":8388608,"max_output_bytes":1073741824}}
//! Subsequent lines: {"raw_envelope_hex":"<original protobuf bytes>",
//! "partition":0,"offset":42,"processing_at_ns":1786352400123456000}
//! One stream/core owner per invocation; file order is the declared cross-partition
//! order. No sorting, clock/offset rewriting, authority admission or unique count.
//! A completion record is required: earlier output is partial after ANY error.
//! Provenance is an input declaration plus TEST-flag rejection, not authentication.
//! Aggregate input (config + header + rows) and output each have a fixed 8 GiB cap.
//! Header quotas may lower limits, never raise these caps; row-count ceiling is 12M.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};

use prost::Message;
use qdl_contracts::qdl::marketdata::v2::EventEnvelope;
use qdl_contracts::qdl::provider::v1::{QuarantineRecord, RawProviderEnvelope};
use qdl_core::transport::DurableRecord;
use qdl_realtime_core::{RealtimeCore, RealtimeCoreConfig};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const MAX_CONFIG_BYTES: usize = 4 * 1024 * 1024;
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_PARTITIONS: usize = 4096;
const MAX_OPERATION_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const SCOPE: &str = "offline_normalizer_inventory_only";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Limits {
    max_rows: u64,
    max_row_bytes: usize,
    max_output_row_bytes: usize,
    max_output_bytes: u64,
}

impl Limits {
    fn validate(&self) -> Result<()> {
        if !(1..=12_000_000).contains(&self.max_rows)
            || !(1..=16 * 1024 * 1024).contains(&self.max_row_bytes)
            || !(1..=64 * 1024 * 1024).contains(&self.max_output_row_bytes)
            || !(1..=MAX_OPERATION_BYTES).contains(&self.max_output_bytes)
        {
            return Err("limits outside hard bounds".into());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    schema: String,
    core_config_sha256: String,
    raw_stream: String,
    source_provenance: String,
    processing_clock_policy: String,
    limits: Limits,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InputRow {
    raw_envelope_hex: String,
    partition: i32,
    offset: u64,
    processing_at_ns: i64,
}

fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

// Take bounds the allocation before read_until sees a potentially unbounded line.
fn line(reader: &mut impl BufRead, limit: usize) -> Result<Option<Vec<u8>>> {
    let mut bytes = Vec::new();
    reader
        .take(limit as u64 + 1)
        .read_until(b'\n', &mut bytes)?;
    if bytes.len() > limit {
        return Err("input line exceeds byte limit (including newline)".into());
    }
    if bytes.is_empty() {
        Ok(None)
    } else {
        Ok(Some(bytes))
    }
}

fn budgeted_line(
    reader: &mut impl BufRead,
    row_limit: usize,
    remaining: &mut u64,
) -> Result<Option<Vec<u8>>> {
    if *remaining == 0 {
        return if reader.fill_buf()?.is_empty() {
            Ok(None)
        } else {
            Err("total input byte limit exceeded".into())
        };
    }
    let limit = row_limit.min(usize::try_from(*remaining).unwrap_or(usize::MAX));
    let bytes = line(reader, limit)?;
    if let Some(bytes) = &bytes {
        *remaining -= bytes.len() as u64;
    }
    Ok(bytes)
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("output row byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn emit(writer: &mut impl Write, value: &Value, limits: &Limits, total: &mut u64) -> Result<()> {
    let mut buffer = BoundedBuffer {
        bytes: Vec::new(),
        limit: limits.max_output_row_bytes,
    };
    serde_json::to_writer(&mut buffer, value)?;
    buffer.write_all(b"\n")?;
    let next = total
        .checked_add(buffer.bytes.len() as u64)
        .ok_or("output size overflow")?;
    if next > limits.max_output_bytes {
        return Err("total output byte limit exceeded".into());
    }
    writer.write_all(&buffer.bytes)?;
    *total = next;
    Ok(())
}

fn record_json(record: &DurableRecord) -> Value {
    json!({
        "stream": record.stream, "partition_key": record.partition_key,
        "event_id_hex": hex::encode(&record.event_id),
        "payload_sha256": hash(&record.payload),
        "payload_protobuf_hex": hex::encode(&record.payload),
        "accepted_at_ns": record.accepted_at_ns,
    })
}

fn inventory(
    config_bytes: &[u8],
    reader: &mut impl BufRead,
    writer: &mut impl Write,
) -> Result<()> {
    if config_bytes.len() > MAX_CONFIG_BYTES {
        return Err("config exceeds byte limit".into());
    }
    let mut input_remaining = MAX_OPERATION_BYTES - config_bytes.len() as u64;
    let header: Header = serde_json::from_slice(
        &budgeted_line(reader, MAX_HEADER_BYTES, &mut input_remaining)?.ok_or("missing header")?,
    )?;
    header.limits.validate()?;
    if header.schema != "n1_capture_inventory_input_v1"
        || header.source_provenance != "captured_provider"
        || header.raw_stream.trim().is_empty()
        || header.raw_stream.len() > 256
        || header.processing_clock_policy.trim().is_empty()
        || header.processing_clock_policy.len() > 1024
        || header.core_config_sha256 != hash(config_bytes)
    {
        return Err("invalid schema/provenance/stream/clock policy or config hash mismatch".into());
    }
    let config: RealtimeCoreConfig = serde_json::from_slice(config_bytes)?;
    if config.allow_test_provenance
        || config.dedup_capacity > 1_000_000
        || config.bindings.len() > 10_000
    {
        return Err("TEST-enabled config or config resource bound exceeded".into());
    }
    let mut core = RealtimeCore::new(config)?;
    let mut offsets = BTreeMap::new();
    let mut count = 0_u64;
    let mut output_bytes = 0_u64;
    let mut input_digest = Sha256::new();
    emit(
        writer,
        &json!({
            "kind": "header", "schema": "n1_capture_inventory_output_v1", "scope": SCOPE,
            "core_config_sha256": header.core_config_sha256, "raw_stream": header.raw_stream,
            "processing_clock_policy": header.processing_clock_policy,
            "source_provenance": header.source_provenance,
            "authority_admission_verified": false, "provider_authenticity_verified": false,
            "historical_production_equivalence": false, "capacity_evidence": false,
            "unique_count_claimed": false, "initial_state": "fresh_core",
            "ordering": "input_file_order_with_strict_per_partition_offsets",
            "max_total_input_bytes": MAX_OPERATION_BYTES,
            "max_total_output_bytes": header.limits.max_output_bytes,
        }),
        &header.limits,
        &mut output_bytes,
    )?;
    loop {
        // At the row limit, look ahead without allocating or processing another row.
        if count == header.limits.max_rows {
            if !reader.fill_buf()?.is_empty() {
                return Err("input row count limit exceeded".into());
            }
            break;
        }
        let Some(bytes) = budgeted_line(reader, header.limits.max_row_bytes, &mut input_remaining)?
        else {
            break;
        };
        let row: InputRow = serde_json::from_slice(&bytes)?;
        if row.partition < 0 || row.processing_at_ns <= 0 {
            return Err("negative partition or nonpositive processing time".into());
        }
        if offsets
            .get(&row.partition)
            .is_some_and(|previous| row.offset <= *previous)
        {
            return Err("partition offsets must strictly increase".into());
        }
        if !offsets.contains_key(&row.partition) && offsets.len() == MAX_PARTITIONS {
            return Err("partition count limit exceeded".into());
        }
        let raw_bytes = hex::decode(&row.raw_envelope_hex)?;
        let raw = RawProviderEnvelope::decode(raw_bytes.as_slice())?;
        if raw.test_provenance {
            return Err("TEST raw provenance is forbidden".into());
        }
        let capture_id = hex::encode(&raw.capture_id);
        let frame_hash = hex::encode(&raw.raw_frame_sha256);
        let received_at_ns = raw.received_at_ns;
        let batch = core.process_at_transport_offset(raw, row.processing_at_ns, row.offset)?;
        // Reject before constructing hex/JSON copies of a large expanded batch.
        let mut minimum_bytes = 0_usize;
        for record in batch.canonical.iter().chain(&batch.quarantines) {
            minimum_bytes = minimum_bytes
                .checked_add(
                    record
                        .payload
                        .len()
                        .checked_mul(2)
                        .ok_or("output size overflow")?,
                )
                .ok_or("output size overflow")?;
            if minimum_bytes > header.limits.max_output_row_bytes {
                return Err("output payloads exceed row byte limit".into());
            }
        }
        let mut canonical = Vec::new();
        for record in &batch.canonical {
            let event = EventEnvelope::decode(record.payload.as_slice())?;
            let mut value = record_json(record);
            value["identity"] = json!({
                "schema_name": event.schema_name, "instrument_uid": event.instrument_uid,
                "instrument_id": event.instrument_id, "venue": event.venue,
                "market": event.market, "source_id": event.source_id,
                "source_sequence": event.source_sequence,
                "partition_sequence": event.partition_sequence,
                "raw_capture_id_hex": hex::encode(&event.raw_capture_id),
                "raw_payload_hash": hex::encode(&event.raw_payload_hash),
                "source_event_time_ns": event.source_event_time_ns,
                "received_at_ns": event.received_at_ns,
                "normalized_at_ns": event.normalized_at_ns,
                "published_at_ns": event.published_at_ns,
            });
            canonical.push(value);
        }
        let mut quarantines = Vec::new();
        for record in &batch.quarantines {
            let quarantine = QuarantineRecord::decode(record.payload.as_slice())?;
            let mut value = record_json(record);
            value["reason"] = json!(quarantine.reason);
            quarantines.push(value);
        }
        count += 1;
        emit(
            writer,
            &json!({
                "kind": "row", "row": count, "partition": row.partition, "offset": row.offset,
                "processing_at_ns": row.processing_at_ns, "received_at_ns": received_at_ns,
                "raw_envelope_sha256": hash(&raw_bytes), "capture_id_hex": capture_id,
                "raw_frame_sha256": frame_hash, "canonical": canonical, "quarantines": quarantines,
                "duplicates": batch.duplicates, "filtered": batch.filtered,
                "filtered_outcome": batch.filtered_outcome,
            }),
            &header.limits,
            &mut output_bytes,
        )?;
        offsets.insert(row.partition, row.offset);
        input_digest.update(&bytes);
    }
    emit(
        writer,
        &json!({
            "kind": "complete", "scope": SCOPE, "rows": count,
            "input_rows_sha256": hex::encode(input_digest.finalize()),
            "total_input_bytes": MAX_OPERATION_BYTES - input_remaining,
            "unique_count_claimed": false, "capacity_evidence": false,
        }),
        &header.limits,
        &mut output_bytes,
    )?;
    writer.flush()?;
    Ok(())
}

fn main() {
    // Parser/core errors may contain input text. Never print those at this CLI boundary.
    if run_cli().is_err() {
        eprintln!("offline inventory failed; output is incomplete");
        std::process::exit(1);
    }
}

fn run_cli() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: n1_capture_inventory CORE.json CAPTURE.jsonl".into());
    }
    let mut config = Vec::new();
    File::open(&args[0])?
        .take(MAX_CONFIG_BYTES as u64 + 1)
        .read_to_end(&mut config)?;
    inventory(
        &config,
        &mut BufReader::new(File::open(&args[1])?),
        &mut io::stdout().lock(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use qdl_contracts::qdl::provider::v1::{
        CaptureBoundary, TransportCompression, TransportProtocol,
    };

    // Payloads reused verbatim from the production core's transport replay tests.
    // These are unit fixtures, never evidence of authenticated captured provider data.
    const TRADE: &[u8] = br#"{"s":"BTCUSDT","t":10,"p":"60000.1","q":"0.01","T":3,"m":false}"#;
    const MANY: &[u8] = br#"{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"1","px":"1","sz":"2","side":"buy","ts":"3"},{"instId":"BTC-USDT-SWAP","tradeId":"2","px":"2","sz":"2","side":"buy","ts":"4"}]}"#;

    fn config(okx: bool) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "canonical_stream":"test.canonical", "quarantine_stream":"test.quarantine",
            "allow_test_provenance":false, "dedup_capacity":16,
            "bindings":[{
                "provider":if okx {"OKX_DIRECT"} else {"BINANCE_DIRECT"},
                "venue":if okx {"OKX"} else {"BINANCE"},
                "market":if okx {"SWAP"} else {"USDM"}, "product_type":"PERPETUAL",
                "native_symbol":if okx {"BTC-USDT-SWAP"} else {"BTCUSDT"},
                "native_channel":if okx {"trades"} else {"trade"},
                "provider_kind":if okx {"okx_trade"} else {"binance_usdm_trade"},
                "instrument_uid":"fixture-uid", "instrument_id":"fixture-instrument",
                "instrument_revision":1, "instrument_catalog_revision":3,
                "source_id":"fixture-source", "source_role":"PRIMARY",
                "normalizer_version":"qdl-rust-core/2.0.0", "sequence_policy":"MONOTONIC"
            }]
        }))
        .unwrap()
    }

    fn raw(config: &[u8], frame: &[u8]) -> RawProviderEnvelope {
        let config: RealtimeCoreConfig = serde_json::from_slice(config).unwrap();
        let b = &config.bindings[0];
        RawProviderEnvelope {
            raw_schema_name: "qdl.provider.raw".into(),
            raw_schema_major: 1,
            raw_schema_minor: 0,
            capture_id: Sha256::digest(frame)[..16].to_vec(),
            provider: b.provider.clone(),
            venue: b.venue.clone(),
            market: b.market.clone(),
            product_type: b.product_type.clone(),
            native_symbol: b.native_symbol.clone(),
            native_channel: b.native_channel.clone(),
            subscription_id: "fixture-subscription".into(),
            source_session_id: "qdl-test-lane-001-1-1700000000000000000".into(),
            connection_generation: 1,
            lease_epoch: 7,
            authority_revision: 1,
            partition_plan_epoch: 1,
            received_at_ns: 1_786_352_400_123_456_000,
            transport_protocol: TransportProtocol::Websocket as i32,
            transport_compression: TransportCompression::None as i32,
            capture_boundary: CaptureBoundary::PostDecompression as i32,
            raw_frame_bytes: frame.to_vec(),
            raw_frame_sha256: Sha256::digest(frame).to_vec(),
            adapter_version: "fixture/2.0.0".into(),
            config_revision: 1,
            instrument_catalog_revision: 3,
            correlation_id: "unit-fixture-only".into(),
            test_provenance: false,
        }
    }

    fn header(config: &[u8]) -> Value {
        json!({"schema":"n1_capture_inventory_input_v1", "core_config_sha256":hash(config),
            "raw_stream":"test.raw", "source_provenance":"captured_provider",
            "processing_clock_policy":"explicit unit fixture time",
            "limits":{"max_rows":10,"max_row_bytes":65536,
                "max_output_row_bytes":65536,"max_output_bytes":1048576}})
    }

    fn row(raw: &RawProviderEnvelope, offset: u64) -> Value {
        json!({"raw_envelope_hex":hex::encode(raw.encode_to_vec()), "partition":0,
            "offset":offset, "processing_at_ns":raw.received_at_ns})
    }

    fn run(config: &[u8], header: Value, rows: &[Value]) -> (Result<()>, Vec<Value>) {
        let mut input = serde_json::to_vec(&header).unwrap();
        input.push(b'\n');
        for row in rows {
            serde_json::to_writer(&mut input, row).unwrap();
            input.push(b'\n');
        }
        let mut output = Vec::new();
        let result = inventory(config, &mut input.as_slice(), &mut output);
        let values = output
            .split(|b| *b == b'\n')
            .filter(|b| !b.is_empty())
            .map(|b| serde_json::from_slice(b).unwrap())
            .collect();
        (result, values)
    }

    #[test]
    fn production_trade_fixture_matches_direct_core_and_duplicate_is_zero_output() {
        let config = config(false);
        let raw = raw(&config, TRADE);
        let mut core = RealtimeCore::new(serde_json::from_slice(&config).unwrap()).unwrap();
        let expected = core
            .process_at_transport_offset(raw.clone(), raw.received_at_ns, 42)
            .unwrap();
        let (result, output) = run(&config, header(&config), &[row(&raw, 42), row(&raw, 43)]);
        result.unwrap();
        assert_eq!(
            output[1]["canonical"][0]["payload_protobuf_hex"],
            hex::encode(&expected.canonical[0].payload)
        );
        assert_eq!(
            output[1]["canonical"][0]["payload_sha256"],
            hash(&expected.canonical[0].payload)
        );
        assert_eq!(
            output[1]["canonical"][0]["event_id_hex"],
            hex::encode(&expected.canonical[0].event_id)
        );
        assert_eq!(
            output[1]["canonical"][0]["identity"]["partition_sequence"],
            42_000_001_u64
        );
        assert_eq!(output[2]["canonical"], json!([]));
        assert_eq!(output[2]["duplicates"], 1);
        assert_eq!(output.last().unwrap()["kind"], "complete");
    }

    #[test]
    fn expanded_okx_frame_has_many_outputs() {
        let config = config(true);
        let (result, output) = run(&config, header(&config), &[row(&raw(&config, MANY), 42)]);
        result.unwrap();
        assert_eq!(output[1]["canonical"].as_array().unwrap().len(), 2);
        assert_eq!(
            output[1]["canonical"][1]["identity"]["partition_sequence"],
            42_000_002_u64
        );
    }

    #[test]
    fn malformed_schema_hex_protobuf_and_pin_are_rejected() {
        let config = config(false);
        let valid = row(&raw(&config, TRADE), 42);
        for change in [
            json!({}),
            json!({"raw_envelope_hex":"zz","partition":0,"offset":42,"processing_at_ns":1}),
            json!({"raw_envelope_hex":"ff","partition":0,"offset":42,"processing_at_ns":1}),
        ] {
            assert!(run(&config, header(&config), &[change]).0.is_err());
        }
        let mut h = header(&config);
        h["core_config_sha256"] = json!("00");
        assert!(run(&config, h, &[valid.clone()]).0.is_err());
        let mut h = header(&config);
        h["schema"] = json!("unknown");
        assert!(run(&config, h, &[valid]).0.is_err());
        assert!(inventory(&config, &mut b"{\n".as_slice(), &mut Vec::new()).is_err());
    }

    #[test]
    fn offsets_strict_per_partition_and_times_are_not_rewritten() {
        let config = config(false);
        let r = row(&raw(&config, TRADE), 42);
        for offset in [41, 42] {
            let mut bad = r.clone();
            bad["offset"] = json!(offset);
            let (result, output) = run(&config, header(&config), &[r.clone(), bad]);
            assert!(result.is_err());
            assert_ne!(output.last().unwrap()["kind"], "complete");
        }
        let mut independent = r.clone();
        independent["partition"] = json!(1);
        independent["offset"] = json!(0);
        independent["processing_at_ns"] = json!(7);
        let (result, output) = run(&config, header(&config), &[r, independent]);
        result.unwrap();
        assert_eq!(output[2]["processing_at_ns"], 7);
        assert_eq!(output[2]["offset"], 0);
    }

    #[test]
    fn test_provenance_and_permissive_config_are_rejected() {
        let config = config(false);
        let mut raw = raw(&config, TRADE);
        raw.test_provenance = true;
        assert!(run(&config, header(&config), &[row(&raw, 0)]).0.is_err());
        let mut permissive: Value = serde_json::from_slice(&config).unwrap();
        permissive["allow_test_provenance"] = json!(true);
        let bytes = serde_json::to_vec(&permissive).unwrap();
        assert!(run(&bytes, header(&bytes), &[]).0.is_err());
    }

    #[test]
    fn quarantine_and_filtered_dispositions_are_preserved() {
        let config = config(false);
        let invalid = br#"{"s":"BTCUSDT","t":10,"p":"60000.1","q":"0","T":3,"m":false}"#;
        let (result, output) = run(&config, header(&config), &[row(&raw(&config, invalid), 42)]);
        result.unwrap();
        assert_eq!(output[1]["canonical"], json!([]));
        assert_eq!(output[1]["quarantines"].as_array().unwrap().len(), 1);
        let status = br#"{"e":"trade","s":"BTCUSDT","t":10,"p":"0","q":"0","T":3,"m":false,"X":"NA","st":1}"#;
        let (result, output) = run(&config, header(&config), &[row(&raw(&config, status), 43)]);
        result.unwrap();
        assert_eq!(output[1]["filtered"], 1);
        assert_eq!(output[1]["canonical"], json!([]));
    }

    #[test]
    fn historical_mark_pair_exposes_wall_clock_expiry_boundary() {
        // Payloads/cadences from okx_mark_index_quiet_component_expiry_blocks_re_materialization.
        // A deterministic later-wall-time scenario, not a live-clock measurement.
        let mark_frame = br#"{"arg":{"channel":"mark-price","instId":"DOGE-USDT-SWAP"},"data":[{"instId":"DOGE-USDT-SWAP","markPx":"0.14525","ts":"1786352400000"}]}"#;
        let index_frame = br#"{"arg":{"channel":"index-tickers","instId":"DOGE-USDT"},"data":[{"instId":"DOGE-USDT","idxPx":"0.14510","ts":"1786352400100"}]}"#;
        let mut cfg: Value = serde_json::from_slice(&config(true)).unwrap();
        let mut mark = cfg["bindings"][0].clone();
        mark["native_symbol"] = json!("DOGE-USDT-SWAP");
        mark["native_channel"] = json!("mark-price");
        mark["physical_native_symbol"] = json!("DOGE-USDT-SWAP");
        mark["physical_native_channel"] = json!("mark-price");
        mark["provider_kind"] = json!("okx_mark_price");
        mark["sequence_policy"] = json!("NONE");
        mark["mark_index"] = json!({"component":"MARK","quiet_after_ms":250});
        let mut index = mark.clone();
        index["physical_native_symbol"] = json!("DOGE-USDT");
        index["physical_native_channel"] = json!("index-tickers");
        index["provider_kind"] = json!("okx_index_price");
        index["mark_index"] = json!({"component":"INDEX","quiet_after_ms":500});
        cfg["bindings"] = json!([mark, index]);
        let cfg = serde_json::to_vec(&cfg).unwrap();
        let origin = 1_786_352_400_000_000_000_i64;
        let mut mark_raw = raw(&cfg, mark_frame);
        mark_raw.received_at_ns = origin;
        let mut index_raw = raw(&cfg, index_frame);
        index_raw.native_symbol = "DOGE-USDT".into();
        index_raw.native_channel = "index-tickers".into();
        index_raw.received_at_ns = origin + 100_000_000;
        let rows = vec![row(&mark_raw, 42), row(&index_raw, 43)];
        let (historical_result, historical) = run(&cfg, header(&cfg), &rows);
        historical_result.unwrap();
        assert_eq!(historical[1]["canonical"], json!([]));
        assert_eq!(historical[1]["filtered"], 0);
        assert_eq!(historical[2]["canonical"].as_array().unwrap().len(), 1);
        let bytes = hex::decode(
            historical[2]["canonical"][0]["payload_protobuf_hex"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        let event = EventEnvelope::decode(bytes.as_slice()).unwrap();
        assert_eq!(event.source_event_time_ns, origin);
        assert_eq!(event.received_at_ns, origin);
        assert_eq!(event.normalized_at_ns, origin + 100_000_000);
        let mut wall_rows = rows.clone();
        for r in &mut wall_rows {
            r["processing_at_ns"] = json!(origin + 86_400_000_000_000_i64);
        }
        let mut wall_header = header(&cfg);
        wall_header["processing_clock_policy"] = json!("fixed later real-wall-time scenario");
        let (wall_result, wall) = run(&cfg, wall_header, &wall_rows);
        wall_result.unwrap();
        assert_eq!(wall[2]["canonical"], json!([]));
        assert_eq!(wall[2]["filtered"], 1);
        assert_eq!(wall[2]["filtered_outcome"], "MARK_INDEX_COMPONENT_EXPIRED");
        for i in 1..=2 {
            assert_eq!(
                historical[i]["raw_envelope_sha256"],
                wall[i]["raw_envelope_sha256"]
            );
            assert_eq!(historical[i]["offset"], wall[i]["offset"]);
        }
    }

    #[test]
    fn operation_caps_and_aggregate_input_are_enforced() {
        let config = config(false);
        let mut h = header(&config);
        h["limits"]["max_rows"] = json!(12_000_000);
        h["limits"]["max_output_bytes"] = json!(MAX_OPERATION_BYTES);
        assert!(run(&config, h.clone(), &[]).0.is_ok());
        h["limits"]["max_rows"] = json!(12_000_001);
        assert!(run(&config, h.clone(), &[]).0.is_err());
        h["limits"]["max_rows"] = json!(12_000_000);
        h["limits"]["max_output_bytes"] = json!(MAX_OPERATION_BYTES + 1);
        assert!(run(&config, h, &[]).0.is_err());
        let mut remaining = 4;
        let mut input = b"a\nb\nc\n".as_slice();
        assert_eq!(
            budgeted_line(&mut input, 16, &mut remaining)
                .unwrap()
                .unwrap(),
            b"a\n"
        );
        assert_eq!(
            budgeted_line(&mut input, 16, &mut remaining)
                .unwrap()
                .unwrap(),
            b"b\n"
        );
        assert_eq!(remaining, 0);
        assert!(budgeted_line(&mut input, 16, &mut remaining).is_err());
        assert!(budgeted_line(&mut b"".as_slice(), 16, &mut remaining)
            .unwrap()
            .is_none());
        let mut remaining = 3;
        assert!(budgeted_line(&mut b"abcd".as_slice(), 16, &mut remaining).is_err());
    }

    #[test]
    fn bounded_input_output_and_empty_capture() {
        let config = config(false);
        let r = row(&raw(&config, TRADE), 42);
        for (field, value) in [
            ("max_row_bytes", 8),
            ("max_output_row_bytes", 8),
            ("max_output_bytes", 8),
            ("max_rows", 0),
        ] {
            let mut h = header(&config);
            h["limits"][field] = json!(value);
            assert!(run(&config, h, &[r.clone()]).0.is_err());
        }
        let mut h = header(&config);
        h["limits"]["max_rows"] = json!(1);
        assert!(run(&config, h, &[r.clone(), r]).0.is_err());
        let (result, output) = run(&config, header(&config), &[]);
        result.unwrap();
        assert_eq!(output[1]["rows"], 0);
        assert_eq!(output[1]["unique_count_claimed"], false);
        assert!(line(&mut b"12345".as_slice(), 4).is_err());
    }
}
