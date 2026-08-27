use prost::Message;
use qdl_contracts::qdl::common::v1::{
    decimal_value, AggressorSide, BarOrigin, BookSide, DecimalValue, QualityFlag, QuantityUnit,
    SourceRole,
};
use qdl_contracts::qdl::marketdata::v2::{
    event_envelope, Bar, BarLifecycle, BookLevel, EventEnvelope, OrderBookDelta, OrderBookSnapshot,
    Quote, Trade, TradeIdentityKind,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::decimal::parse_decimal;
use crate::event_id::deterministic_event_id;
use crate::l2_adapter::{BookPublication, BookTransition, BookTransitionKind};
use crate::l2_book::{BookSide as CoreBookSide, BookViewLevel};
use crate::okx::candle_interval;

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
        event_envelope::Payload::LongShortRatio(value) => value.encode_to_vec(),
        event_envelope::Payload::TakerFlow(value) => value.encode_to_vec(),
        event_envelope::Payload::Basis(value) => value.encode_to_vec(),
        event_envelope::Payload::ContractMetadata(value) => value.encode_to_vec(),
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
        "binance_usdm_rest_bar" | "binance_spot_rest_bar" => canonicalize_binance_rest_bar(fixture),
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
            ("SPOT", "SPOT") | ("USDM", "PERPETUAL" | "FUTURE") => Ok(QuantityUnit::BaseAsset),
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
    let provider_time = fixture
        .raw
        .get("T")
        .and_then(Value::as_i64)
        .or_else(|| fixture.raw.get("E").and_then(Value::as_i64));
    let mut envelope = base_envelope(
        fixture,
        "quote",
        sequence,
        provider_time.unwrap_or(fixture.context.received_at_ns / 1_000_000),
    )?;
    if provider_time.is_none() {
        envelope
            .quality_flags
            .push(QualityFlag::SourceTimeMissing as i32);
    }
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

fn canonicalize_binance_rest_bar(fixture: &TradeFixture) -> Result<EventEnvelope, String> {
    if text(&fixture.raw, "symbol")?.to_uppercase() != fixture.context.native_symbol.to_uppercase()
    {
        return Err("provider symbol does not match resolved instrument".into());
    }
    let row = fixture
        .raw
        .get("row")
        .and_then(Value::as_array)
        .filter(|row| row.len() >= 11)
        .ok_or_else(|| "Binance REST kline requires the unmodified native row".to_owned())?;
    let origin_name = fixture
        .raw
        .get("bar_origin")
        .and_then(Value::as_str)
        .unwrap_or("BACKFILLED")
        .to_ascii_uppercase();
    let origin = match origin_name.as_str() {
        "VENUE_NATIVE" => BarOrigin::VenueNative,
        "BACKFILLED" => BarOrigin::Backfilled,
        "RECONCILED" => BarOrigin::Reconciled,
        _ => return Err("Binance REST bar origin is invalid".into()),
    };
    let open_time_ms = scalar_text(&row[0])?
        .parse::<i64>()
        .map_err(|_| "Binance REST open time is invalid".to_owned())?;
    let close_time_ms = scalar_text(&row[6])?
        .parse::<i64>()
        .map_err(|_| "Binance REST close time is invalid".to_owned())?;
    let mut envelope = base_envelope(
        fixture,
        "bar",
        format!("{open_time_ms}:{close_time_ms}"),
        close_time_ms,
    )?;
    if origin == BarOrigin::Backfilled {
        envelope.quality_flags.push(QualityFlag::Backfilled as i32);
    }
    envelope.payload = Some(event_envelope::Payload::Bar(Bar {
        interval: text(&fixture.raw, "interval")?,
        open_time_ns: open_time_ms * 1_000_000,
        close_time_ns: close_time_ms * 1_000_000,
        open: Some(parse_decimal(&scalar_text(&row[1])?)?),
        high: Some(parse_decimal(&scalar_text(&row[2])?)?),
        low: Some(parse_decimal(&scalar_text(&row[3])?)?),
        close: Some(parse_decimal(&scalar_text(&row[4])?)?),
        volume: Some(parse_decimal(&scalar_text(&row[5])?)?),
        trade_count: scalar_text(&row[8])?
            .parse::<u64>()
            .map_err(|_| "Binance REST trade count is invalid".to_owned())?,
        is_final: true,
        revision: 0,
        origin: origin as i32,
        lifecycle: BarLifecycle::Final as i32,
        supersedes_event_id: None,
        volume_unit: quantity_unit(&fixture.context)? as i32,
        base_volume: Some(parse_decimal(&scalar_text(&row[5])?)?),
        quote_volume: Some(parse_decimal(&scalar_text(&row[7])?)?),
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
    let channel = fixture
        .raw
        .get("arg")
        .and_then(Value::as_object)
        .and_then(|argument| argument.get("channel"))
        .and_then(Value::as_str)
        .ok_or_else(|| "OKX candle channel is missing".to_owned())?;
    let (interval, interval_ms) = candle_interval(channel)?;
    let row = okx_frame_row(fixture, channel)?
        .as_array()
        .filter(|values| values.len() >= 9)
        .ok_or_else(|| "OKX candle requires the native nine-field row".to_owned())?;
    let open_time_ms = scalar_text(&row[0])?
        .parse::<i64>()
        .map_err(|_| "OKX candle open timestamp is invalid".to_owned())?;
    let confirm = scalar_text(&row[8])?;
    let is_final = match confirm.as_str() {
        "0" => false,
        "1" => true,
        _ => return Err("OKX candle confirm must be 0 or 1".into()),
    };
    // Keep one provider event identity across REST bootstrap, WebSocket delivery,
    // and process restart. A same-identity content conflict must fail closed.
    let sequence = format!("{open_time_ms}:{confirm}");
    let mut envelope = base_envelope(fixture, "bar", sequence, open_time_ms)?;
    envelope
        .quality_flags
        .push(QualityFlag::FieldMissing as i32);
    envelope.payload = Some(event_envelope::Payload::Bar(Bar {
        interval: interval.into(),
        open_time_ns: open_time_ms * 1_000_000,
        close_time_ns: (open_time_ms + interval_ms - 1) * 1_000_000,
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
        book_generation: 0,
        sequence_verified: false,
        truncated: false,
    }));
    set_payload_hash(&mut envelope)?;
    Ok(envelope)
}

/// Canonicalize the current top-N view only after the Rust L2 core has proven
/// sequence continuity. A REST bootstrap or gapped/disconnected transition has
/// no readable view and is rejected instead of becoming a stale book snapshot.
pub fn canonicalize_l2_ready_snapshot(
    fixture: &TradeFixture,
    transition: &BookTransition,
) -> Result<EventEnvelope, String> {
    let view = transition
        .view
        .as_ref()
        .filter(|_| transition.sequence_verified())
        .ok_or_else(|| "L2 snapshot requires a verified readable book view".to_owned())?;
    let source_time_ms = l2_source_time_ms(fixture, transition)?;
    let source_sequence = format!("g{}:{}", view.generation, view.last_sequence);
    let mut envelope = base_envelope(fixture, "book_snapshot", source_sequence, source_time_ms)?;
    let unit = quantity_unit(&fixture.context)?;
    let mut levels = l2_levels(&view.bids, unit)?;
    levels.extend(l2_levels(&view.asks, unit)?);
    if levels.is_empty() {
        return Err("verified L2 snapshot has no readable levels".into());
    }
    envelope.payload = Some(event_envelope::Payload::BookSnapshot(OrderBookSnapshot {
        native_sequence: view.last_sequence.to_string(),
        checksum: String::new(),
        levels,
        depth: view
            .bids
            .len()
            .max(view.asks.len())
            .try_into()
            .map_err(|_| "L2 depth exceeds uint32".to_owned())?,
        book_generation: view.generation,
        sequence_verified: true,
        truncated: view.truncated,
    }));
    set_payload_hash(&mut envelope)?;
    Ok(envelope)
}

/// Convert only a publication-approved L2 transition into its canonical V2
/// representation.  A caller can persist raw bootstrap/gap lifecycle records
/// for audit, but those transitions intentionally yield no readable book event.
pub fn canonicalize_l2_publication(
    fixture: &TradeFixture,
    transition: &BookTransition,
) -> Result<Option<EventEnvelope>, String> {
    match transition.publication {
        BookPublication::None => Ok(None),
        BookPublication::Snapshot => canonicalize_l2_ready_snapshot(fixture, transition).map(Some),
        BookPublication::Delta => canonicalize_l2_delta(fixture, transition).map(Some),
    }
}

/// Canonicalize a lossless L2 delta admitted by the Rust core. The output
/// carries the generation and original snapshot anchor required for an SDK to
/// replay it; it is never emitted for a bootstrap, duplicate, gap or lifecycle
/// transition.
pub fn canonicalize_l2_delta(
    fixture: &TradeFixture,
    transition: &BookTransition,
) -> Result<EventEnvelope, String> {
    if transition.kind != BookTransitionKind::Delta || !transition.sequence_verified() {
        return Err("L2 delta requires a verified delta transition".into());
    }
    let view = transition
        .view
        .as_ref()
        .ok_or_else(|| "verified L2 delta lacks a readable view".to_owned())?;
    let native_sequence_start = transition
        .native_sequence_start
        .or(transition.previous_sequence)
        .ok_or_else(|| "L2 delta lacks a sequence start".to_owned())?;
    let source_time_ms = l2_source_time_ms(fixture, transition)?;
    let source_sequence = format!("g{}:{}", view.generation, view.last_sequence);
    let mut envelope = base_envelope(fixture, "book_delta", source_sequence, source_time_ms)?;
    envelope.payload = Some(event_envelope::Payload::BookDelta(OrderBookDelta {
        native_sequence_start: native_sequence_start.to_string(),
        native_sequence_end: view.last_sequence.to_string(),
        snapshot_sequence: view.snapshot_sequence.to_string(),
        checksum: String::new(),
        updates: l2_input_levels(&transition.updates, quantity_unit(&fixture.context)?)?,
        reset: false,
        book_generation: view.generation,
        sequence_verified: true,
    }));
    set_payload_hash(&mut envelope)?;
    Ok(envelope)
}

fn l2_source_time_ms(fixture: &TradeFixture, transition: &BookTransition) -> Result<i64, String> {
    let value = transition
        .observed_at_ms
        .unwrap_or(fixture.context.received_at_ns / 1_000_000);
    if value <= 0 {
        return Err("L2 source/receipt timestamp must be positive".into());
    }
    Ok(value)
}

fn l2_levels(
    levels: &[BookViewLevel],
    quantity_unit: QuantityUnit,
) -> Result<Vec<BookLevel>, String> {
    levels
        .iter()
        .map(|level| {
            Ok(BookLevel {
                side: l2_side(level.side) as i32,
                price: Some(parse_decimal(&level.price.canonical_text())?),
                quantity: Some(parse_decimal(&level.quantity.canonical_text())?),
                order_count: level
                    .order_count
                    .unwrap_or(0)
                    .try_into()
                    .map_err(|_| "L2 order count exceeds uint32".to_owned())?,
                quantity_unit: quantity_unit as i32,
            })
        })
        .collect()
}

fn l2_input_levels(
    levels: &[crate::l2_book::BookLevelInput],
    quantity_unit: QuantityUnit,
) -> Result<Vec<BookLevel>, String> {
    levels
        .iter()
        .map(|level| {
            Ok(BookLevel {
                side: l2_side(level.side) as i32,
                price: Some(parse_decimal(&level.price)?),
                quantity: Some(parse_decimal(&level.quantity)?),
                order_count: level
                    .order_count
                    .unwrap_or(0)
                    .try_into()
                    .map_err(|_| "L2 order count exceeds uint32".to_owned())?,
                quantity_unit: quantity_unit as i32,
            })
        })
        .collect()
}

fn l2_side(side: CoreBookSide) -> BookSide {
    match side {
        CoreBookSide::Bid => BookSide::Bid,
        CoreBookSide::Ask => BookSide::Ask,
    }
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

fn parse_positive_trade_decimal(source: &str, field: &str) -> Result<DecimalValue, String> {
    let value = parse_decimal(source)?;
    let positive = match value.coefficient.as_ref() {
        Some(decimal_value::Coefficient::Mantissa(value)) => *value > 0,
        Some(decimal_value::Coefficient::MantissaText(value)) => {
            value != "0" && !value.starts_with('-')
        }
        None => false,
    };
    if !positive {
        return Err(format!("{field} must be positive"));
    }
    Ok(value)
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
            price: Some(parse_positive_trade_decimal(&trade.price, "trade price")?),
            quantity: Some(parse_positive_trade_decimal(
                &trade.quantity,
                "trade quantity",
            )?),
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
    use prost::Message;
    use qdl_contracts::qdl::common::v1::{decimal_value, DecimalValue, QuantityUnit};
    use qdl_contracts::qdl::marketdata::v2::{
        event_envelope, Basis, BasisKind, ContractMetadata, LongShortRatio,
        LongShortRatioPopulation, MetricUnit, OpenInterest, TakerFlow,
    };
    use sha2::{Digest, Sha256};

    use super::{
        canonical_bytes, canonicalize_l2_publication, canonicalize_trade, set_payload_hash,
        EventEnvelope, TradeFixture,
    };
    use crate::l2_adapter::{BookPublication, L2BookAdapter};
    use crate::l2_book::BookIdentity;
    use serde_json::json;

    fn decimal(value: i64, scale: i32) -> DecimalValue {
        DecimalValue {
            coefficient: Some(decimal_value::Coefficient::Mantissa(value)),
            scale,
            source_text: if scale == 0 {
                value.to_string()
            } else {
                format!("{value}e-{scale}")
            },
        }
    }

    fn envelope(payload: event_envelope::Payload) -> EventEnvelope {
        EventEnvelope {
            source_session_id: "phase104-contract-hash".into(),
            payload: Some(payload),
            ..Default::default()
        }
    }

    #[test]
    fn non_positive_trade_price_and_quantity_fail_closed() {
        let fixture_path = format!(
            "{}/../../tests/fixtures/phase2/binance_usdm_trade.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let template: TradeFixture =
            serde_json::from_slice(&std::fs::read(fixture_path).expect("read fixture"))
                .expect("decode fixture");
        for (field, value, message) in [
            ("p", "0", "trade price must be positive"),
            ("p", "-0.01", "trade price must be positive"),
            ("q", "0", "trade quantity must be positive"),
            ("q", "-0.01", "trade quantity must be positive"),
        ] {
            let mut fixture = template.clone();
            fixture.raw[field] = serde_json::Value::String(value.into());
            let error = canonical_bytes(&fixture).expect_err("non-positive trade must fail");
            assert_eq!(error, message);
        }
    }

    #[test]
    fn provider_fixtures_match_python_golden_bytes() {
        for (fixture_name, golden_name) in [
            ("binance_usdm_trade.json", "binance-usdm-trade.bin"),
            ("binance_usdm_bbo.json", "binance-usdm-bbo.bin"),
            ("binance_usdm_bar.json", "binance-usdm-bar.bin"),
            ("binance_usdm_rest_bar.json", "binance-usdm-rest-bar.bin"),
            ("binance_spot_trade.json", "binance-spot-trade.bin"),
            ("binance_spot_bbo.json", "binance-spot-bbo.bin"),
            ("binance_spot_bar.json", "binance-spot-bar.bin"),
            ("binance_spot_rest_bar.json", "binance-spot-rest-bar.bin"),
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

    #[test]
    fn binance_usdm_dated_future_bar_keeps_base_asset_volume_unit() {
        let fixture_path = format!(
            "{}/../../tests/fixtures/phase2/binance_usdm_rest_bar.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let mut fixture: TradeFixture =
            serde_json::from_slice(&std::fs::read(fixture_path).expect("read fixture"))
                .expect("decode fixture");
        fixture.context.instrument_id = "BINANCE.USDM.FUTURE.BTC-USDT-260925".into();
        fixture.context.native_symbol = "BTCUSDT_260925".into();
        fixture.context.product_type = "FUTURE".into();
        fixture.raw["symbol"] = serde_json::Value::String("BTCUSDT_260925".into());

        let envelope = canonicalize_trade(&fixture).expect("canonicalize dated future bar");
        let event_envelope::Payload::Bar(bar) = envelope.payload.expect("bar payload") else {
            panic!("dated future fixture must remain a bar")
        };
        assert_eq!(bar.volume_unit, QuantityUnit::BaseAsset as i32);
        assert_eq!(bar.volume.expect("volume").source_text, "12.500");
    }

    #[test]
    fn l2_bootstrap_emits_only_verified_snapshot_then_lossless_delta() {
        let fixture_path = format!(
            "{}/../../tests/fixtures/phase2/binance_usdm_trade.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let mut fixture: TradeFixture =
            serde_json::from_slice(&std::fs::read(fixture_path).expect("read fixture"))
                .expect("decode fixture");
        fixture.provider_kind = "binance_usdm_diff_depth".into();
        fixture.context.connection_generation = 4;
        fixture.context.partition_sequence = 51;
        let identity = BookIdentity::new(
            "BINANCE_USDM_DIFF_DEPTH",
            fixture.context.instrument_uid.clone(),
            "depth",
        )
        .expect("book identity");
        let mut adapter =
            L2BookAdapter::binance_diff_depth(identity, "BTCUSDT", 2).expect("binance adapter");

        let buffered = adapter
            .apply_binance_ws_delta(
                &json!({
                    "s":"BTCUSDT", "U":101, "u":101, "pu":100, "E":1001,
                    "b":[["60000.1","2"]], "a":[["60000.2","3"]]
                }),
                4,
            )
            .expect("buffer delta");
        assert_eq!(buffered.publication, BookPublication::None);
        assert!(canonicalize_l2_publication(&fixture, &buffered)
            .expect("bootstrap publication")
            .is_none());

        let bridge = adapter
            .apply_binance_rest_snapshot(
                &json!({
                    "lastUpdateId":100,
                    "bids":[["60000.1","1"]],
                    "asks":[["60000.2","1"]]
                }),
                4,
                1000,
            )
            .expect("bridge snapshot");
        assert_eq!(bridge.publication, BookPublication::Snapshot);
        let snapshot = canonicalize_l2_publication(&fixture, &bridge)
            .expect("snapshot publication")
            .expect("verified snapshot");
        let event_envelope::Payload::BookSnapshot(book) = snapshot.payload.expect("book snapshot")
        else {
            panic!("expected book snapshot");
        };
        assert_eq!(book.native_sequence, "101");
        assert_eq!(book.book_generation, 4);
        assert!(book.sequence_verified);
        assert_eq!(book.depth, 1);

        let delta = adapter
            .apply_binance_ws_delta(
                &json!({
                    "s":"BTCUSDT", "U":102, "u":102, "pu":101, "E":1002,
                    "b":[["60000.1","0"]], "a":[["60000.3","4"]]
                }),
                4,
            )
            .expect("chained delta");
        assert_eq!(delta.publication, BookPublication::Delta);
        let event = canonicalize_l2_publication(&fixture, &delta)
            .expect("delta publication")
            .expect("verified delta");
        let event_envelope::Payload::BookDelta(book) = event.payload.expect("book delta") else {
            panic!("expected book delta");
        };
        assert_eq!(book.snapshot_sequence, "100");
        assert_eq!(book.native_sequence_start, "102");
        assert_eq!(book.native_sequence_end, "102");
        assert_eq!(book.book_generation, 4);
        assert!(book.sequence_verified);
    }

    #[test]
    fn okx_fixed_interval_bar_uses_channel_interval_and_close_boundary() {
        let fixture_path = format!(
            "{}/../../tests/fixtures/phase2/okx_bar.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let mut fixture: TradeFixture =
            serde_json::from_slice(&std::fs::read(fixture_path).expect("read fixture"))
                .expect("decode fixture");
        fixture.raw["arg"]["channel"] = serde_json::Value::String("candle15m".into());
        fixture.raw["data"][0][0] = serde_json::Value::String("900000".into());
        let envelope = canonicalize_trade(&fixture).expect("canonicalize fixed bar");
        let bar = match envelope.payload.expect("bar payload") {
            event_envelope::Payload::Bar(value) => value,
            _ => panic!("expected bar payload"),
        };
        assert_eq!(bar.interval, "15m");
        assert_eq!(bar.open_time_ns, 900_000_000_000);
        assert_eq!(bar.close_time_ns, 1_799_999_000_000);
    }

    #[test]
    fn reference_payload_hashes_are_exhaustive_and_deterministic() {
        let payloads = vec![
            event_envelope::Payload::OpenInterest(OpenInterest {
                quantity: Some(decimal(10, 0)),
                notional: Some(decimal(2500, 2)),
                quantity_unit: QuantityUnit::Contract as i32,
                sampling_interval: "5m".into(),
            }),
            event_envelope::Payload::LongShortRatio(LongShortRatio {
                population: LongShortRatioPopulation::GlobalAccount as i32,
                sampling_interval: "1h".into(),
                long_value: Some(decimal(6, 1)),
                short_value: Some(decimal(4, 1)),
                long_short_ratio: Some(decimal(15, 1)),
                value_unit: MetricUnit::Ratio as i32,
            }),
            event_envelope::Payload::TakerFlow(TakerFlow {
                sampling_interval: "1h".into(),
                buy_volume: Some(decimal(3, 0)),
                sell_volume: Some(decimal(2, 0)),
                buy_sell_ratio: Some(decimal(15, 1)),
                quantity_unit: QuantityUnit::BaseAsset as i32,
            }),
            event_envelope::Payload::Basis(Basis {
                kind: BasisKind::Derived as i32,
                sampling_interval: "1h".into(),
                basis: Some(decimal(12, 2)),
                basis_unit: MetricUnit::Percent as i32,
                annualized_basis: Some(decimal(18, 2)),
                reference_instrument_uid: "BINANCE.USDM.PERPETUAL.BTC-USDT".into(),
                formula_id: "spread-v1".into(),
                input_instrument_uids: vec![
                    "BINANCE.USDM.PERPETUAL.BTC-USDT".into(),
                    "BINANCE.USDM.FUTURE.BTC-USDT-20261225".into(),
                ],
            }),
            event_envelope::Payload::ContractMetadata(ContractMetadata {
                contract_kind: "PERPETUAL".into(),
                settlement_asset: "USDT".into(),
                contract_multiplier: Some(decimal(1, 0)),
                price_tick: Some(decimal(1, 2)),
                quantity_step: Some(decimal(1, 3)),
                expiry_time_ns: None,
                funding_interval_ns: Some(28_800_000_000_000),
                continuous: false,
                underlying_instrument_uid: "BINANCE.USDM.PERPETUAL.BTC-USDT".into(),
            }),
        ];
        for payload in payloads {
            let expected = match &payload {
                event_envelope::Payload::OpenInterest(value) => value.encode_to_vec(),
                event_envelope::Payload::LongShortRatio(value) => value.encode_to_vec(),
                event_envelope::Payload::TakerFlow(value) => value.encode_to_vec(),
                event_envelope::Payload::Basis(value) => value.encode_to_vec(),
                event_envelope::Payload::ContractMetadata(value) => value.encode_to_vec(),
                _ => unreachable!("Phase 10.4 reference payload only"),
            };
            let mut value = envelope(payload);
            set_payload_hash(&mut value).expect("hash reference payload");
            assert_eq!(
                value.canonical_payload_hash,
                Sha256::digest(expected).to_vec()
            );
        }
    }
}
