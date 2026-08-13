use prost::Message;
use qdl_contracts::qdl::common::v1::{AggressorSide, BarOrigin, SourceRole};
use qdl_contracts::qdl::marketdata::v2::{event_envelope, Bar, EventEnvelope, Quote, Trade};
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

fn canonical_json(raw: &Value) -> Result<Vec<u8>, String> {
    serde_json::to_vec(raw).map_err(|error| error.to_string())
}

pub fn canonicalize_trade(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    match fixture.provider_kind.as_str() {
        "binance_usdm_agg_trade" => canonicalize_binance(fixture),
        "binance_usdm_bbo" => canonicalize_binance_bbo(fixture),
        "binance_usdm_bar" => canonicalize_binance_bar(fixture),
        "okx_trade" => canonicalize_okx(fixture),
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
        raw_payload_hash: Sha256::digest(raw_bytes).to_vec(),
        correlation_id: context.correlation_id.clone(),
        config_revision: context.config_revision,
        payload: None,
    })
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
        kline.get("L").and_then(Value::as_i64).unwrap_or(0),
        source_time
    );
    let mut envelope = base_envelope(fixture, "bar", sequence, source_time)?;
    envelope.payload = Some(event_envelope::Payload::Bar(Bar {
        interval: text(kline, "i")?,
        open_time_ns: integer(kline, "t")? * 1_000_000,
        close_time_ns: integer(kline, "T")? * 1_000_000,
        open: Some(parse_decimal(&text(kline, "o")?)?),
        high: Some(parse_decimal(&text(kline, "h")?)?),
        low: Some(parse_decimal(&text(kline, "l")?)?),
        close: Some(parse_decimal(&text(kline, "c")?)?),
        volume: Some(parse_decimal(&text(kline, "v")?)?),
        trade_count: kline.get("n").and_then(Value::as_u64).unwrap_or(0),
        is_final: kline.get("x").and_then(Value::as_bool).unwrap_or(false),
        revision: 0,
        origin: BarOrigin::VenueNative as i32,
    }));
    Ok(envelope)
}

fn canonicalize_binance(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    if text(&fixture.raw, "s")?.to_uppercase() != fixture.context.native_symbol.to_uppercase() {
        return Err("provider symbol does not match resolved instrument".into());
    }
    let native_trade_id = text(&fixture.raw, "a").or_else(|_| text(&fixture.raw, "t"))?;
    let buyer_maker = fixture
        .raw
        .get("m")
        .and_then(Value::as_bool)
        .unwrap_or(false);
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
    Ok(EventEnvelope {
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
        raw_payload_hash: Sha256::digest(raw_bytes).to_vec(),
        correlation_id: context.correlation_id.clone(),
        config_revision: context.config_revision,
        payload: Some(event_envelope::Payload::Trade(Trade {
            native_trade_id,
            price: Some(parse_decimal(&price)?),
            quantity: Some(parse_decimal(&quantity)?),
            aggressor_side: side as i32,
            is_block_trade: false,
            is_buyer_maker,
        })),
    })
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
