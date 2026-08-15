use prost::Message;
use qdl_contracts::qdl::common::v1::{AggressorSide, BarOrigin, BookSide, QualityFlag, SourceRole};
use qdl_contracts::qdl::marketdata::v2::{
    event_envelope, Bar, BarLifecycle, BookLevel, EventEnvelope, OrderBookSnapshot, Quote, Trade,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::decimal::parse_decimal;
use crate::event_id::deterministic_event_id;

#[derive(Clone, Debug, Deserialize)]
pub struct TradeContext {
    pub instrument_uid: String,
    pub instrument_id: String,
    pub instrument_revision: u64,
    pub venue: String,
    pub market: String,
    pub product_type: String,
    pub native_symbol: String,
    pub provider: String,
    pub source_id: String,
    pub lease_epoch: u64,
    pub received_at_ns: i64,
    pub normalized_at_ns: i64,
    pub published_at_ns: i64,
    pub partition_sequence: u64,
    pub normalizer_version: String,
    pub adapter_version: String,
    pub config_revision: u64,
    #[serde(default)]
    pub correlation_id: String,
    #[serde(default)]
    pub source_session_id: String,
    #[serde(default)]
    pub connection_generation: u64,
    #[serde(default)]
    pub authority_revision: u64,
    #[serde(default)]
    pub partition_plan_epoch: u64,
    #[serde(default)]
    pub raw_capture_id: Vec<u8>,
    #[serde(default)]
    pub raw_frame_sha256: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TradeFixture {
    pub provider_kind: String,
    pub context: TradeContext,
    pub raw: Value,
}

fn text(raw: &Value, field: &str) -> Result<String, String> {
    match raw.get(field) {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        _ => Err(format!("required provider field is missing: {field}")),
    }
}

fn integer(raw: &Value, field: &str) -> Result<i64, String> {
    text(raw, field)?
        .parse::<i64>()
        .map_err(|_| format!("invalid integer provider field: {field}"))
}

fn unsigned(raw: &Value, field: &str) -> Result<u64, String> {
    text(raw, field)?
        .parse::<u64>()
        .map_err(|_| format!("invalid unsigned provider field: {field}"))
}

fn boolean(raw: &Value, field: &str) -> Result<bool, String> {
    raw.get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("required provider boolean is missing or invalid: {field}"))
}

fn set_payload_hash(envelope: &mut EventEnvelope) -> Result<(), String> {
    if envelope.source_session_id.is_empty() {
        return Ok(());
    }
    let payload = envelope
        .payload
        .as_ref()
        .ok_or_else(|| "canonical payload is required before hashing".to_owned())?;
    let bytes = match payload {
        event_envelope::Payload::Trade(value) => value.encode_to_vec(),
        event_envelope::Payload::Quote(value) => value.encode_to_vec(),
        event_envelope::Payload::Bar(value) => value.encode_to_vec(),
        event_envelope::Payload::BookSnapshot(value) => value.encode_to_vec(),
        event_envelope::Payload::BookDelta(value) => value.encode_to_vec(),
        event_envelope::Payload::FundingRate(value) => value.encode_to_vec(),
        event_envelope::Payload::OpenInterest(value) => value.encode_to_vec(),
        event_envelope::Payload::MarkIndexPrice(value) => value.encode_to_vec(),
        event_envelope::Payload::Ticker(value) => value.encode_to_vec(),
        event_envelope::Payload::FeedState(value) => value.encode_to_vec(),
        event_envelope::Payload::QualityEvent(value) => value.encode_to_vec(),
    };
    envelope.canonical_payload_hash = Sha256::digest(bytes).to_vec();
    Ok(())
}

fn canonical_json(raw: &Value) -> Result<Vec<u8>, String> {
    serde_json::to_vec(raw).map_err(|error| error.to_string())
}

pub fn canonicalize_trade(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    match fixture.provider_kind.as_str() {
        "binance_usdm_trade" | "binance_usdm_agg_trade" => canonicalize_binance(fixture),
        "binance_usdm_bbo" => canonicalize_binance_bbo(fixture),
        "binance_usdm_bar" => canonicalize_binance_bar(fixture),
        "okx_trade" => canonicalize_okx(fixture),
        "dnse_bar" => canonicalize_dnse_bar(fixture),
        "deribit_option_book_fixture" => canonicalize_deribit_fixture(fixture),
        other => Err(format!("unsupported provider fixture: {other}")),
    }
}

fn base_envelope(
    fixture: &TradeFixture,
    feed: &str,
    source_sequence: String,
    source_event_time_ms: i64,
) -> Result<EventEnvelope, String> {
    let context = &fixture.context;
    validate_shadow_context(context)?;
    let raw_bytes = canonical_json(&fixture.raw)?;
    let event_id = deterministic_event_id(
        &[
            b"2",
            context.venue.as_bytes(),
            context.market.as_bytes(),
            context.instrument_uid.as_bytes(),
            feed.as_bytes(),
            context.source_id.as_bytes(),
            source_sequence.as_bytes(),
        ],
        16,
    )?;
    Ok(EventEnvelope {
        schema_name: format!("qdl.marketdata.{feed}"),
        schema_major: 2,
        schema_minor: 0,
        event_id,
        instrument_uid: context.instrument_uid.clone(),
        instrument_id: context.instrument_id.clone(),
        instrument_revision: context.instrument_revision,
        venue: context.venue.clone(),
        market: context.market.clone(),
        product_type: context.product_type.clone(),
        native_symbol: context.native_symbol.clone(),
        provider: context.provider.clone(),
        source_id: context.source_id.clone(),
        source_role: SourceRole::Primary as i32,
        lease_epoch: context.lease_epoch,
        source_event_time_ns: source_event_time_ms * 1_000_000,
        received_at_ns: context.received_at_ns,
        normalized_at_ns: context.normalized_at_ns,
        published_at_ns: context.published_at_ns,
        source_sequence,
        partition_sequence: context.partition_sequence,
        normalizer_version: context.normalizer_version.clone(),
        adapter_version: context.adapter_version.clone(),
        quality_flags: vec![],
        raw_payload_hash: if context.raw_frame_sha256.is_empty() {
            Sha256::digest(raw_bytes).to_vec()
        } else {
            context.raw_frame_sha256.clone()
        },
        correlation_id: context.correlation_id.clone(),
        config_revision: context.config_revision,
        source_session_id: context.source_session_id.clone(),
        connection_generation: context.connection_generation,
        authority_revision: context.authority_revision,
        partition_plan_epoch: context.partition_plan_epoch,
        canonical_payload_hash: vec![],
        raw_capture_id: context.raw_capture_id.clone(),
        payload: None,
    })
}

fn validate_shadow_context(context: &TradeContext) -> Result<(), String> {
    if !context.source_session_id.is_empty() && context.raw_capture_id.len() != 16 {
        return Err("exact-frame shadow context requires a 16-byte raw_capture_id".to_owned());
    }
    if !context.source_session_id.is_empty() && context.raw_frame_sha256.len() != 32 {
        return Err("exact-frame shadow context requires a 32-byte raw_frame_sha256".to_owned());
    }
    Ok(())
}

fn verify_binance_symbol(fixture: &TradeFixture) -> Result<(), String> {
    if text(&fixture.raw, "s")?.to_uppercase() != fixture.context.native_symbol.to_uppercase() {
        return Err("provider symbol does not match resolved instrument".into());
    }
    Ok(())
}

fn canonicalize_binance_bbo(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    verify_binance_symbol(fixture)?;
    let sequence = text(&fixture.raw, "u")?;
    let source_time = fixture
        .raw
        .get("T")
        .and_then(Value::as_i64)
        .or_else(|| fixture.raw.get("E").and_then(Value::as_i64))
        .ok_or_else(|| "required provider timestamp is missing".to_owned())?;
    let mut envelope = base_envelope(fixture, "quote", sequence, source_time)?;
    envelope.payload = Some(event_envelope::Payload::Quote(Quote {
        bid_price: Some(parse_decimal(&text(&fixture.raw, "b")?)?),
        bid_quantity: Some(parse_decimal(&text(&fixture.raw, "B")?)?),
        ask_price: Some(parse_decimal(&text(&fixture.raw, "a")?)?),
        ask_quantity: Some(parse_decimal(&text(&fixture.raw, "A")?)?),
        level: 1,
    }));
    set_payload_hash(&mut envelope)?;
    Ok(envelope)
}

fn canonicalize_binance_bar(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    verify_binance_symbol(fixture)?;
    let kline = fixture
        .raw
        .get("k")
        .ok_or_else(|| "Binance kline frame requires k object".to_owned())?;
    if text(kline, "s")?.to_uppercase() != fixture.context.native_symbol.to_uppercase() {
        return Err("provider kline symbol does not match resolved instrument".into());
    }
    let source_time = integer(&fixture.raw, "E")?;
    let sequence = format!(
        "{}:{}:{}",
        text(kline, "t")?,
        integer(kline, "L")?,
        source_time
    );
    let mut envelope = base_envelope(fixture, "bar", sequence, source_time)?;
    let is_final = boolean(kline, "x")?;
    envelope.payload = Some(event_envelope::Payload::Bar(Bar {
        interval: text(kline, "i")?,
        open_time_ns: integer(kline, "t")? * 1_000_000,
        close_time_ns: integer(kline, "T")? * 1_000_000,
        open: Some(parse_decimal(&text(kline, "o")?)?),
        high: Some(parse_decimal(&text(kline, "h")?)?),
        low: Some(parse_decimal(&text(kline, "l")?)?),
        close: Some(parse_decimal(&text(kline, "c")?)?),
        volume: Some(parse_decimal(&text(kline, "v")?)?),
        trade_count: unsigned(kline, "n")?,
        is_final,
        revision: 0,
        origin: BarOrigin::VenueNative as i32,
        lifecycle: if is_final {
            BarLifecycle::Final as i32
        } else {
            BarLifecycle::InProgress as i32
        },
        supersedes_event_id: None,
    }));
    set_payload_hash(&mut envelope)?;
    Ok(envelope)
}

fn canonicalize_binance(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    if text(&fixture.raw, "s")?.to_uppercase() != fixture.context.native_symbol.to_uppercase() {
        return Err("provider symbol does not match resolved instrument".into());
    }
    let native_trade_id = text(&fixture.raw, "a").or_else(|_| text(&fixture.raw, "t"))?;
    let buyer_maker = boolean(&fixture.raw, "m")?;
    build_trade(
        fixture,
        native_trade_id,
        text(&fixture.raw, "p")?,
        text(&fixture.raw, "q")?,
        if buyer_maker {
            AggressorSide::Sell
        } else {
            AggressorSide::Buy
        },
        fixture
            .raw
            .get("T")
            .and_then(Value::as_i64)
            .or_else(|| fixture.raw.get("E").and_then(Value::as_i64))
            .ok_or_else(|| "required provider timestamp is missing".to_owned())?,
        buyer_maker,
    )
}

fn canonicalize_okx(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    if text(&fixture.raw, "instId")? != fixture.context.native_symbol {
        return Err("provider symbol does not match resolved instrument".into());
    }
    let side = match text(&fixture.raw, "side")?.as_str() {
        "buy" => AggressorSide::Buy,
        "sell" => AggressorSide::Sell,
        _ => return Err("unsupported OKX aggressor side".into()),
    };
    build_trade(
        fixture,
        text(&fixture.raw, "tradeId")?,
        text(&fixture.raw, "px")?,
        text(&fixture.raw, "sz")?,
        side,
        integer(&fixture.raw, "ts")?,
        false,
    )
}

fn canonicalize_dnse_bar(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    if text(&fixture.raw, "symbol")?.to_uppercase() != fixture.context.native_symbol.to_uppercase()
    {
        return Err("DNSE bar symbol does not match resolved instrument".into());
    }
    let open_time_ms = integer(&fixture.raw, "open_time_ms")?;
    let close_time_ms = integer(&fixture.raw, "close_time_ms")?;
    let is_final = boolean(&fixture.raw, "is_final")?;
    let trade_count_available = boolean(&fixture.raw, "trade_count_available")?;
    let trade_count = if trade_count_available {
        unsigned(&fixture.raw, "trade_count")?
    } else {
        0
    };
    let sequence = format!("{open_time_ms}:{close_time_ms}");
    let mut envelope = base_envelope(fixture, "bar", sequence, close_time_ms)?;
    if !trade_count_available {
        envelope
            .quality_flags
            .push(QualityFlag::FieldMissing as i32);
    }
    envelope.payload = Some(event_envelope::Payload::Bar(Bar {
        interval: text(&fixture.raw, "interval")?,
        open_time_ns: open_time_ms * 1_000_000,
        close_time_ns: close_time_ms * 1_000_000,
        open: Some(parse_decimal(&text(&fixture.raw, "o")?)?),
        high: Some(parse_decimal(&text(&fixture.raw, "h")?)?),
        low: Some(parse_decimal(&text(&fixture.raw, "l")?)?),
        close: Some(parse_decimal(&text(&fixture.raw, "c")?)?),
        volume: Some(parse_decimal(&text(&fixture.raw, "v")?)?),
        trade_count,
        is_final,
        revision: unsigned(&fixture.raw, "revision")?
            .try_into()
            .map_err(|_| "DNSE revision exceeds uint32".to_owned())?,
        origin: BarOrigin::VenueNative as i32,
        lifecycle: if is_final {
            BarLifecycle::Final as i32
        } else {
            BarLifecycle::InProgress as i32
        },
        supersedes_event_id: None,
    }));
    set_payload_hash(&mut envelope)?;
    Ok(envelope)
}

fn deribit_levels(raw: &Value, field: &str, side: BookSide) -> Result<Vec<BookLevel>, String> {
    let rows = raw
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("Deribit fixture {field} must be a list"))?;
    rows.iter()
        .map(|row| {
            let values = row
                .as_array()
                .filter(|items| items.len() >= 2)
                .ok_or_else(|| "Deribit fixture level requires price and amount".to_owned())?;
            let price = values[0]
                .as_str()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| values[0].to_string());
            let quantity = values[1]
                .as_str()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| values[1].to_string());
            Ok(BookLevel {
                side: side as i32,
                price: Some(parse_decimal(&price)?),
                quantity: Some(parse_decimal(&quantity)?),
                order_count: 0,
            })
        })
        .collect()
}

fn canonicalize_deribit_fixture(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    if text(&fixture.raw, "provenance")? != "TEST_SYNTHETIC_EXTENSION_FIXTURE" {
        return Err("Deribit fixture parser cannot accept live provenance".into());
    }
    if text(&fixture.raw, "native_symbol")? != fixture.context.native_symbol {
        return Err("Deribit fixture instrument mismatch".into());
    }
    let sequence = text(&fixture.raw, "change_id")?;
    let source_time = integer(&fixture.raw, "timestamp")?;
    let mut envelope = base_envelope(fixture, "book_snapshot", sequence.clone(), source_time)?;
    let bids = deribit_levels(&fixture.raw, "bids", BookSide::Bid)?;
    let asks = deribit_levels(&fixture.raw, "asks", BookSide::Ask)?;
    let depth = bids.len().max(asks.len());
    let mut levels = bids;
    levels.extend(asks);
    envelope
        .quality_flags
        .push(QualityFlag::FieldMissing as i32);
    envelope.payload = Some(event_envelope::Payload::BookSnapshot(OrderBookSnapshot {
        native_sequence: sequence,
        checksum: String::new(),
        levels,
        depth: depth
            .try_into()
            .map_err(|_| "Deribit depth exceeds uint32".to_owned())?,
    }));
    set_payload_hash(&mut envelope)?;
    Ok(envelope)
}

fn build_trade(
    fixture: &TradeFixture,
    native_trade_id: String,
    price: String,
    quantity: String,
    side: AggressorSide,
    source_event_time_ms: i64,
    is_buyer_maker: bool,
) -> Result<EventEnvelope, String> {
    let context = &fixture.context;
    validate_shadow_context(context)?;
    let raw_bytes = canonical_json(&fixture.raw)?;
    let schema_major = b"2";
    let event_id = deterministic_event_id(
        &[
            schema_major,
            context.venue.as_bytes(),
            context.market.as_bytes(),
            context.instrument_uid.as_bytes(),
            b"trade",
            context.source_id.as_bytes(),
            native_trade_id.as_bytes(),
        ],
        16,
    )?;
    let mut envelope = EventEnvelope {
        schema_name: "qdl.marketdata.trade".into(),
        schema_major: 2,
        schema_minor: 0,
        event_id,
        instrument_uid: context.instrument_uid.clone(),
        instrument_id: context.instrument_id.clone(),
        instrument_revision: context.instrument_revision,
        venue: context.venue.clone(),
        market: context.market.clone(),
        product_type: context.product_type.clone(),
        native_symbol: context.native_symbol.clone(),
        provider: context.provider.clone(),
        source_id: context.source_id.clone(),
        source_role: SourceRole::Primary as i32,
        lease_epoch: context.lease_epoch,
        source_event_time_ns: source_event_time_ms * 1_000_000,
        received_at_ns: context.received_at_ns,
        normalized_at_ns: context.normalized_at_ns,
        published_at_ns: context.published_at_ns,
        source_sequence: native_trade_id.clone(),
        partition_sequence: context.partition_sequence,
        normalizer_version: context.normalizer_version.clone(),
        adapter_version: context.adapter_version.clone(),
        quality_flags: vec![],
        raw_payload_hash: if context.raw_frame_sha256.is_empty() {
            Sha256::digest(raw_bytes).to_vec()
        } else {
            context.raw_frame_sha256.clone()
        },
        correlation_id: context.correlation_id.clone(),
        config_revision: context.config_revision,
        source_session_id: context.source_session_id.clone(),
        connection_generation: context.connection_generation,
        authority_revision: context.authority_revision,
        partition_plan_epoch: context.partition_plan_epoch,
        canonical_payload_hash: vec![],
        raw_capture_id: context.raw_capture_id.clone(),
        payload: Some(event_envelope::Payload::Trade(Trade {
            native_trade_id,
            price: Some(parse_decimal(&price)?),
            quantity: Some(parse_decimal(&quantity)?),
            aggressor_side: side as i32,
            is_block_trade: false,
            is_buyer_maker,
        })),
    };
    set_payload_hash(&mut envelope)?;
    Ok(envelope)
}

pub fn canonical_bytes(fixture: &TradeFixture) -> Result<Vec<u8>, String> {
    Ok(canonicalize_trade(fixture)?.encode_to_vec())
}

#[cfg(test)]
mod tests {
    use super::{canonical_bytes, TradeFixture};

    #[test]
    fn provider_fixtures_match_python_golden_bytes() {
        for (fixture_name, golden_name) in [
            ("binance_usdm_trade.json", "binance-usdm-trade.bin"),
            ("okx_trade.json", "okx-swap-trade.bin"),
            ("binance_usdm_bbo.json", "binance-usdm-bbo.bin"),
            ("binance_usdm_bar.json", "binance-usdm-bar.bin"),
        ] {
            let fixture_path = format!(
                "{}/../../tests/fixtures/phase2/{fixture_name}",
                env!("CARGO_MANIFEST_DIR")
            );
            let fixture: TradeFixture =
                serde_json::from_slice(&std::fs::read(fixture_path).expect("read fixture"))
                    .expect("decode fixture");
            let actual = canonical_bytes(&fixture).expect("canonicalize fixture");
            let golden_path = format!(
                "{}/../../contracts/golden/phase2/{golden_name}",
                env!("CARGO_MANIFEST_DIR")
            );
            assert_eq!(actual, std::fs::read(golden_path).expect("read golden"));
        }
    }
}
