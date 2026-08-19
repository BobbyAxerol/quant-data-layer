use prost::Message;
use qdl_contracts::qdl::common::v1::{
    AggressorSide, BarOrigin, BookSide, QualityFlag, QuantityUnit, SourceRole,
};
use qdl_contracts::qdl::marketdata::v2::{
    event_envelope, Bar, BarLifecycle, BookLevel, EventEnvelope, OrderBookSnapshot, Quote, Trade,
    TradeIdentityKind,
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
    #[serde(default = "default_source_role")]
    pub source_role: String,
}

fn default_source_role() -> String {
    "PRIMARY".into()
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
        "binance_usdm_trade" | "binance_usdm_agg_trade" | "binance_spot_trade" => {
            canonicalize_binance(fixture)
        }
        "binance_usdm_bbo" | "binance_spot_bbo" => canonicalize_binance_bbo(fixture),
        "binance_usdm_bar" | "binance_spot_bar" => canonicalize_binance_bar(fixture),
        "okx_trade" => canonicalize_okx(fixture),
        "okx_bbo" => canonicalize_okx_bbo(fixture),
        "okx_bar" => canonicalize_okx_bar(fixture),
        "dnse_trade" => canonicalize_dnse_trade(fixture),
        "dnse_bar" | "vnstock_bar" => canonicalize_dnse_bar(fixture),
        "deribit_option_book_fixture" => canonicalize_deribit_fixture(fixture),
        other => Err(format!("unsupported provider fixture: {other}")),
    }
}

fn source_role(value: &str) -> Result<SourceRole, String> {
    match value.trim().to_ascii_uppercase().as_str() {
        "PRIMARY" => Ok(SourceRole::Primary),
        "SECONDARY" => Ok(SourceRole::Secondary),
        "REFERENCE" => Ok(SourceRole::Reference),
        "BACKFILL" => Ok(SourceRole::Backfill),
        _ => Err("canonical source role is invalid".into()),
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
        source_role: source_role(&context.source_role)? as i32,
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

fn quantity_unit(context: &TradeContext) -> Result<QuantityUnit, String> {
    let venue = context.venue.trim().to_ascii_uppercase();
    let market = context.market.trim().to_ascii_uppercase();
    let product = context.product_type.trim().to_ascii_uppercase();
    if venue.is_empty() || market.is_empty() || product.is_empty() {
        return Err("quantity-unit identity is incomplete".into());
    }
    if product == "COMMON_STOCK" {
        return if matches!(
            venue.as_str(),
            "DNSE" | "HOSE" | "HNX" | "UPCOM" | "VN_MARKETS"
        ) {
            Ok(QuantityUnit::Share)
        } else {
            Err("COMMON_STOCK quantity unit requires a VN venue identity".into())
        };
    }
    if product == "FUTURE"
        && (matches!(
            venue.as_str(),
            "DNSE" | "HOSE" | "HNX" | "UPCOM" | "VN_MARKETS"
        ) || matches!(market.as_str(), "VN_DERIVATIVES" | "DERIVATIVES"))
    {
        return Ok(QuantityUnit::Contract);
    }
    if venue == "OKX" {
        return match (market.as_str(), product.as_str()) {
            ("SPOT", "SPOT") => Ok(QuantityUnit::BaseAsset),
            ("SWAP", "PERPETUAL") | ("FUTURES", "FUTURE") | ("OPTIONS", "OPTION") => {
                Ok(QuantityUnit::Contract)
            }
            _ => Err("unsupported OKX quantity-unit identity".into()),
        };
    }
    if venue == "DERIBIT" && product == "OPTION" {
        return Ok(QuantityUnit::Contract);
    }
    if venue == "BINANCE" {
        return match (market.as_str(), product.as_str()) {
            ("SPOT", "SPOT") | ("USDM", "PERPETUAL") => Ok(QuantityUnit::BaseAsset),
            _ => Err("unsupported Binance quantity-unit identity".into()),
        };
    }
    Err(format!(
        "quantity unit is undefined for {venue}/{market}/{product}"
    ))
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
        quantity_unit: quantity_unit(&fixture.context)? as i32,
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
        volume_unit: quantity_unit(&fixture.context)? as i32,
        base_volume: Some(parse_decimal(&text(kline, "v")?)?),
        quote_volume: kline
            .get("q")
            .map(scalar_text)
            .transpose()?
            .map(|value| parse_decimal(&value))
            .transpose()?,
        contract_volume: None,
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
        TradeInput {
            native_trade_id,
            price: text(&fixture.raw, "p")?,
            quantity: text(&fixture.raw, "q")?,
            side: if buyer_maker {
                AggressorSide::Sell
            } else {
                AggressorSide::Buy
            },
            source_event_time_ms: fixture
                .raw
                .get("T")
                .and_then(Value::as_i64)
                .or_else(|| fixture.raw.get("E").and_then(Value::as_i64))
                .ok_or_else(|| "required provider timestamp is missing".to_owned())?,
            is_buyer_maker: buyer_maker,
            identity_kind: TradeIdentityKind::Native,
        },
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
        TradeInput {
            native_trade_id: text(&fixture.raw, "tradeId")?,
            price: text(&fixture.raw, "px")?,
            quantity: text(&fixture.raw, "sz")?,
            side,
            source_event_time_ms: integer(&fixture.raw, "ts")?,
            is_buyer_maker: false,
            identity_kind: TradeIdentityKind::Native,
        },
    )
}

fn scalar_text(value: &Value) -> Result<String, String> {
    match value {
        Value::String(value) if !value.is_empty() => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err("provider scalar is missing or invalid".into()),
    }
}

fn okx_frame_row<'a>(fixture: &'a TradeFixture, channel: &str) -> Result<&'a Value, String> {
    let argument = fixture
        .raw
        .get("arg")
        .and_then(Value::as_object)
        .ok_or_else(|| "OKX frame arg must be an object".to_owned())?;
    if argument.get("channel").and_then(Value::as_str) != Some(channel)
        || argument.get("instId").and_then(Value::as_str)
            != Some(fixture.context.native_symbol.as_str())
    {
        return Err("OKX frame channel/instrument mismatch".into());
    }
    let rows = fixture
        .raw
        .get("data")
        .and_then(Value::as_array)
        .filter(|rows| rows.len() == 1)
        .ok_or_else(|| "OKX frame requires one data row".to_owned())?;
    Ok(&rows[0])
}

fn okx_bbo_level<'a>(row: &'a Value, side: &str) -> Result<&'a Vec<Value>, String> {
    row.get(side)
        .and_then(Value::as_array)
        .filter(|levels| levels.len() == 1)
        .and_then(|levels| levels[0].as_array())
        .filter(|level| level.len() >= 2)
        .ok_or_else(|| format!("OKX bbo-tbt requires one {side} level"))
}

fn canonicalize_okx_bbo(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    let row = okx_frame_row(fixture, "bbo-tbt")?;
    let sequence = text(row, "seqId")?;
    let source_time = integer(row, "ts")?;
    let bid = okx_bbo_level(row, "bids")?;
    let ask = okx_bbo_level(row, "asks")?;
    let mut envelope = base_envelope(fixture, "quote", sequence, source_time)?;
    envelope.payload = Some(event_envelope::Payload::Quote(Quote {
        bid_price: Some(parse_decimal(&scalar_text(&bid[0])?)?),
        bid_quantity: Some(parse_decimal(&scalar_text(&bid[1])?)?),
        ask_price: Some(parse_decimal(&scalar_text(&ask[0])?)?),
        ask_quantity: Some(parse_decimal(&scalar_text(&ask[1])?)?),
        level: 1,
        quantity_unit: quantity_unit(&fixture.context)? as i32,
    }));
    set_payload_hash(&mut envelope)?;
    Ok(envelope)
}

fn canonicalize_okx_bar(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    let row = okx_frame_row(fixture, "candle1m")?
        .as_array()
        .filter(|values| values.len() >= 9)
        .ok_or_else(|| "OKX candle1m requires the native nine-field row".to_owned())?;
    let open_time_ms = scalar_text(&row[0])?
        .parse::<i64>()
        .map_err(|_| "OKX candle open timestamp is invalid".to_owned())?;
    let confirm = scalar_text(&row[8])?;
    let is_final = match confirm.as_str() {
        "0" => false,
        "1" => true,
        _ => return Err("OKX candle confirm must be 0 or 1".into()),
    };
    let sequence = format!(
        "{open_time_ms}:{confirm}:{}",
        fixture.context.partition_sequence
    );
    let mut envelope = base_envelope(fixture, "bar", sequence, open_time_ms)?;
    envelope
        .quality_flags
        .push(QualityFlag::FieldMissing as i32);
    envelope.payload = Some(event_envelope::Payload::Bar(Bar {
        interval: "1m".into(),
        open_time_ns: open_time_ms * 1_000_000,
        close_time_ns: (open_time_ms + 60_000 - 1) * 1_000_000,
        open: Some(parse_decimal(&scalar_text(&row[1])?)?),
        high: Some(parse_decimal(&scalar_text(&row[2])?)?),
        low: Some(parse_decimal(&scalar_text(&row[3])?)?),
        close: Some(parse_decimal(&scalar_text(&row[4])?)?),
        volume: Some(parse_decimal(&scalar_text(&row[5])?)?),
        trade_count: 0,
        is_final,
        revision: 0,
        origin: BarOrigin::VenueNative as i32,
        lifecycle: if is_final {
            BarLifecycle::Final as i32
        } else {
            BarLifecycle::InProgress as i32
        },
        supersedes_event_id: None,
        volume_unit: quantity_unit(&fixture.context)? as i32,
        base_volume: Some(parse_decimal(&scalar_text(
            if quantity_unit(&fixture.context)? == QuantityUnit::BaseAsset {
                &row[5]
            } else {
                &row[6]
            },
        )?)?),
        quote_volume: Some(parse_decimal(&scalar_text(&row[7])?)?),
        contract_volume: if quantity_unit(&fixture.context)? == QuantityUnit::Contract {
            Some(parse_decimal(&scalar_text(&row[5])?)?)
        } else {
            None
        },
    }));
    set_payload_hash(&mut envelope)?;
    Ok(envelope)
}

fn canonicalize_dnse_trade(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    if text(&fixture.raw, "symbol")?.to_uppercase() != fixture.context.native_symbol.to_uppercase()
    {
        return Err("DNSE trade symbol does not match resolved instrument".into());
    }
    if fixture.context.raw_capture_id.len() != 16 {
        return Err("DNSE trade identity requires an exact raw capture id".into());
    }
    let native_trade_id = format!("derived:{}", hex::encode(&fixture.context.raw_capture_id));
    let mut envelope = build_trade(
        fixture,
        TradeInput {
            native_trade_id,
            price: text(&fixture.raw, "price")?,
            quantity: text(&fixture.raw, "quantity")?,
            side: AggressorSide::Unspecified,
            source_event_time_ms: fixture.context.received_at_ns / 1_000_000,
            is_buyer_maker: false,
            identity_kind: TradeIdentityKind::DerivedRawCapture,
        },
    )?;
    envelope.quality_flags.extend([
        QualityFlag::SourceTimeMissing as i32,
        QualityFlag::FieldMissing as i32,
    ]);
    Ok(envelope)
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
        volume_unit: quantity_unit(&fixture.context)? as i32,
        base_volume: None,
        quote_volume: None,
        contract_volume: if quantity_unit(&fixture.context)? == QuantityUnit::Contract {
            Some(parse_decimal(&text(&fixture.raw, "v")?)?)
        } else {
            None
        },
    }));
    set_payload_hash(&mut envelope)?;
    Ok(envelope)
}

fn deribit_levels(
    raw: &Value,
    field: &str,
    side: BookSide,
    quantity_unit: QuantityUnit,
) -> Result<Vec<BookLevel>, String> {
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
                quantity_unit: quantity_unit as i32,
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
    let unit = quantity_unit(&fixture.context)?;
    let bids = deribit_levels(&fixture.raw, "bids", BookSide::Bid, unit)?;
    let asks = deribit_levels(&fixture.raw, "asks", BookSide::Ask, unit)?;
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

struct TradeInput {
    native_trade_id: String,
    price: String,
    quantity: String,
    side: AggressorSide,
    source_event_time_ms: i64,
    is_buyer_maker: bool,
    identity_kind: TradeIdentityKind,
}

fn build_trade(fixture: &TradeFixture, trade: TradeInput) -> Result<EventEnvelope, String> {
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
            trade.native_trade_id.as_bytes(),
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
        source_role: source_role(&context.source_role)? as i32,
        lease_epoch: context.lease_epoch,
        source_event_time_ns: trade.source_event_time_ms * 1_000_000,
        received_at_ns: context.received_at_ns,
        normalized_at_ns: context.normalized_at_ns,
        published_at_ns: context.published_at_ns,
        source_sequence: trade.native_trade_id.clone(),
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
            native_trade_id: trade.native_trade_id,
            price: Some(parse_decimal(&trade.price)?),
            quantity: Some(parse_decimal(&trade.quantity)?),
            aggressor_side: trade.side as i32,
            is_block_trade: false,
            is_buyer_maker: trade.is_buyer_maker,
            quantity_unit: quantity_unit(context)? as i32,
            identity_kind: trade.identity_kind as i32,
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
            ("binance_usdm_bbo.json", "binance-usdm-bbo.bin"),
            ("binance_usdm_bar.json", "binance-usdm-bar.bin"),
            ("binance_spot_trade.json", "binance-spot-trade.bin"),
            ("binance_spot_bbo.json", "binance-spot-bbo.bin"),
            ("binance_spot_bar.json", "binance-spot-bar.bin"),
            ("okx_trade.json", "okx-swap-trade.bin"),
            ("okx_bbo.json", "okx-swap-bbo.bin"),
            ("okx_bar.json", "okx-swap-bar.bin"),
            ("okx_spot_trade.json", "okx-spot-trade.bin"),
            ("okx_spot_bbo.json", "okx-spot-bbo.bin"),
            ("okx_spot_bar.json", "okx-spot-bar.bin"),
            ("dnse_derivative_trade.json", "dnse-derivative-trade.bin"),
            ("dnse_derivative_bar.json", "dnse-derivative-bar.bin"),
            ("dnse_equity_trade.json", "dnse-equity-trade.bin"),
            ("dnse_equity_bar.json", "dnse-equity-bar.bin"),
            ("vnstock_equity_bar.json", "vnstock-equity-bar.bin"),
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
