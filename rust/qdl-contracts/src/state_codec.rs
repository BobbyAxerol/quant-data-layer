//! KN-3 state codecs (contract sections 2-5, decisions D2-D4), the native
//! side of `qdl/projection/kn_state_codec.py`.
//!
//! * State trailer (48 bytes): source offset u64 BE, materializer epoch u64
//!   BE, SHA-256 of the canonical EventEnvelope bytes.
//! * BAR cache row: trailer + the canonical envelope without the LPK-derived
//!   fields (instrument uid, venue, market, bar interval); decoding restores
//!   them from the LPK and proves the bytes against the stored hash.
//! * Latest cache value: trailer + the full canonical bytes.
//! * State-topic frame: `QKS1` + kind u8 + header length u32 BE + canonical
//!   strict JSON header + the canonical envelope bytes unchanged.
//! * Keys and partition: latest key = LPK; BAR keys keep the in-progress row
//!   apart from every final revision; partition = Kafka murmur2 of the LPK.
//!
//! Both languages refuse the same input with the same short reason code
//! (`REASONS`); `contracts/golden/kn_v220/state_codec.json`, built from real
//! canonical records, is the shared oracle.

use prost::Message;
use ring::digest;
use serde_json::{Map, Value};
use std::fmt::Write as _;

use crate::qdl::marketdata::v2::{event_envelope::Payload, Bar, EventEnvelope};
use crate::state_contract::{LogicalProductKey, SourceCoordinate, MAX_OFFSET};

pub const TRAILER_BYTES: usize = 48;
pub const FRAME_MAGIC: &[u8; 4] = b"QKS1";
pub const FRAME_PREFIX_BYTES: usize = 9;
pub const MAX_HEADER_BYTES: usize = 4096;
pub const LEGACY_PROVENANCE: &str = "legacy_import";
pub const KAFKA_MURMUR2_SEED: u32 = 0x9747_b28c;
const MURMUR2_M: u32 = 0x5bd1_e995;
const MAX_PARTITION: u64 = (1 << 31) - 1;
const MAX_REVISION: u64 = u32::MAX as u64;
const MAX_TEXT_BYTES: usize = 1024;
const MAX_EVENT_ID_HEX: usize = 1024;

/// Shared refusal reasons, identical to `REASONS` in the Python codec.
pub const REASONS: [&str; 18] = [
    "TRUNCATED",
    "MAGIC",
    "KIND",
    "HEADER_SIZE",
    "HEADER_JSON",
    "HEADER_FIELDS",
    "FIELD",
    "NON_CANONICAL",
    "BODY",
    "CONTENT_HASH",
    "ENVELOPE",
    "NOT_BAR",
    "PRODUCT_MISMATCH",
    "OPEN_TIME",
    "HEADER_MISMATCH",
    "TRAILER",
    "ENVELOPE_NOT_CANONICAL",
    "PARTITIONS",
];

/// A typed refusal. `reason` is one of `REASONS`; `detail` names the field
/// when the reason concerns one (the same name Python reports).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateCodecError {
    pub reason: &'static str,
    pub detail: String,
}

impl StateCodecError {
    fn new(reason: &'static str) -> Self {
        Self {
            reason,
            detail: String::new(),
        }
    }

    fn at(reason: &'static str, detail: &str) -> Self {
        Self {
            reason,
            detail: detail.to_owned(),
        }
    }
}

impl std::fmt::Display for StateCodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "STATE_CODEC:{}", self.reason)?;
        if !self.detail.is_empty() {
            write!(formatter, ":{}", self.detail)?;
        }
        Ok(())
    }
}

impl std::error::Error for StateCodecError {}

type Result<T> = std::result::Result<T, StateCodecError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FrameKind {
    Latest = 1,
    BarRevision = 2,
    RetentionFloor = 3,
    LegacyBar = 4,
}

impl FrameKind {
    pub fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Latest),
            2 => Some(Self::BarRevision),
            3 => Some(Self::RetentionFloor),
            4 => Some(Self::LegacyBar),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "LATEST",
            Self::BarRevision => "BAR_REVISION",
            Self::RetentionFloor => "RETENTION_FLOOR",
            Self::LegacyBar => "LEGACY_BAR",
        }
    }

    /// The exact header field set, in sorted (validation) order.
    fn fields(self) -> &'static [&'static str] {
        match self {
            Self::Latest => &[
                "content_sha256",
                "event_id",
                "lpk",
                "materializer_epoch",
                "source",
            ],
            Self::BarRevision => &[
                "content_sha256",
                "event_id",
                "is_final",
                "lpk",
                "materializer_epoch",
                "open_time_ms",
                "revision",
                "source",
            ],
            Self::RetentionFloor => &["floor_open_time_ms", "lpk", "materializer_epoch"],
            Self::LegacyBar => &[
                "content_sha256",
                "event_id",
                "is_final",
                "legacy",
                "lpk",
                "materializer_epoch",
                "open_time_ms",
                "provenance",
                "revision",
            ],
        }
    }

    fn is_bar(self) -> bool {
        matches!(self, Self::BarRevision | Self::LegacyBar)
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut out = [0_u8; 32];
    out.copy_from_slice(digest::digest(&digest::SHA256, bytes).as_ref());
    out
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(text, "{byte:02x}");
    }
    text
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn is_hex64(value: &str) -> bool {
    value.len() == 64 && is_lower_hex(value)
}

fn is_event_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() % 2 == 0
        && value.len() <= MAX_EVENT_ID_HEX
        && is_lower_hex(value)
}

/// Header strings never need JSON escaping, so both encoders agree.
fn is_text(value: &str) -> bool {
    (1..=MAX_TEXT_BYTES).contains(&value.len())
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'.' | b'_' | b':' | b'/' | b'@' | b'|' | b'+' | b'=' | b'-'
                )
        })
}

// ------------------------------------------------------------------ trailer

/// A cache value proven against its trailer hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedState {
    pub canonical: Vec<u8>,
    pub source_offset: u64,
    pub materializer_epoch: u64,
}

fn check_trailer_numbers(source_offset: u64, materializer_epoch: u64) -> Result<()> {
    if source_offset > MAX_OFFSET || !(1..=MAX_OFFSET).contains(&materializer_epoch) {
        return Err(StateCodecError::new("TRAILER"));
    }
    Ok(())
}

pub fn state_trailer(
    canonical: &[u8],
    source_offset: u64,
    materializer_epoch: u64,
) -> Result<[u8; TRAILER_BYTES]> {
    check_trailer_numbers(source_offset, materializer_epoch)?;
    let mut trailer = [0_u8; TRAILER_BYTES];
    trailer[..8].copy_from_slice(&source_offset.to_be_bytes());
    trailer[8..16].copy_from_slice(&materializer_epoch.to_be_bytes());
    trailer[16..].copy_from_slice(&sha256(canonical));
    Ok(trailer)
}

fn read_trailer(data: &[u8]) -> Result<(u64, u64, &[u8])> {
    if data.len() < TRAILER_BYTES {
        return Err(StateCodecError::new("TRUNCATED"));
    }
    let number = |range: std::ops::Range<usize>| {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&data[range]);
        u64::from_be_bytes(bytes)
    };
    let source_offset = number(0..8);
    let materializer_epoch = number(8..16);
    check_trailer_numbers(source_offset, materializer_epoch)?;
    Ok((source_offset, materializer_epoch, &data[16..TRAILER_BYTES]))
}

/// Latest cache value: trailer + the full canonical bytes (nothing stripped).
pub fn encode_latest_value(
    canonical: &[u8],
    source_offset: u64,
    materializer_epoch: u64,
) -> Result<Vec<u8>> {
    let trailer = state_trailer(canonical, source_offset, materializer_epoch)?;
    if canonical.is_empty() {
        return Err(StateCodecError::new("BODY"));
    }
    let mut value = Vec::with_capacity(TRAILER_BYTES + canonical.len());
    value.extend_from_slice(&trailer);
    value.extend_from_slice(canonical);
    Ok(value)
}

pub fn decode_latest_value(value: &[u8]) -> Result<DecodedState> {
    let (source_offset, materializer_epoch, stored) = read_trailer(value)?;
    let canonical = &value[TRAILER_BYTES..];
    if canonical.is_empty() {
        return Err(StateCodecError::new("BODY"));
    }
    if sha256(canonical) != stored {
        return Err(StateCodecError::new("CONTENT_HASH"));
    }
    Ok(DecodedState {
        canonical: canonical.to_vec(),
        source_offset,
        materializer_epoch,
    })
}

// ------------------------------------------------------------------ product checks

fn parse_envelope(canonical: &[u8]) -> Result<EventEnvelope> {
    EventEnvelope::decode(canonical).map_err(|_| StateCodecError::new("ENVELOPE"))
}

/// The upper-case feed name of a payload: the proto oneof field name.
pub fn payload_feed(payload: Option<&Payload>) -> &'static str {
    match payload {
        None => "",
        Some(Payload::Trade(_)) => "TRADE",
        Some(Payload::Quote(_)) => "QUOTE",
        Some(Payload::Bar(_)) => "BAR",
        Some(Payload::BookSnapshot(_)) => "BOOK_SNAPSHOT",
        Some(Payload::BookDelta(_)) => "BOOK_DELTA",
        Some(Payload::FundingRate(_)) => "FUNDING_RATE",
        Some(Payload::OpenInterest(_)) => "OPEN_INTEREST",
        Some(Payload::MarkIndexPrice(_)) => "MARK_INDEX_PRICE",
        Some(Payload::Ticker(_)) => "TICKER",
        Some(Payload::FeedState(_)) => "FEED_STATE",
        Some(Payload::QualityEvent(_)) => "QUALITY_EVENT",
        Some(Payload::LongShortRatio(_)) => "LONG_SHORT_RATIO",
        Some(Payload::TakerFlow(_)) => "TAKER_FLOW",
        Some(Payload::Basis(_)) => "BASIS",
        Some(Payload::ContractMetadata(_)) => "CONTRACT_METADATA",
    }
}

fn bar_of(envelope: &EventEnvelope) -> Option<&Bar> {
    match &envelope.payload {
        Some(Payload::Bar(bar)) => Some(bar),
        _ => None,
    }
}

fn bar_mut(envelope: &mut EventEnvelope) -> Option<&mut Bar> {
    match &mut envelope.payload {
        Some(Payload::Bar(bar)) => Some(bar),
        _ => None,
    }
}

/// The envelope is the product `lpk` names: uid, venue, market, feed, qualifier.
fn check_product(envelope: &EventEnvelope, lpk: &LogicalProductKey) -> Result<()> {
    let qualifier = bar_of(envelope).map_or("-", |bar| bar.interval.as_str());
    for (name, actual, expected) in [
        (
            "instrument_uid",
            envelope.instrument_uid.as_str(),
            lpk.instrument_uid.as_str(),
        ),
        ("venue", envelope.venue.as_str(), lpk.venue.as_str()),
        ("market", envelope.market.as_str(), lpk.market.as_str()),
        (
            "feed",
            payload_feed(envelope.payload.as_ref()),
            lpk.feed.as_str(),
        ),
        ("qualifier", qualifier, lpk.qualifier.as_str()),
    ] {
        if actual != expected {
            return Err(StateCodecError::at("PRODUCT_MISMATCH", name));
        }
    }
    Ok(())
}

fn open_time_ms(bar: &Bar) -> Result<u64> {
    if bar.open_time_ns < 0 || bar.open_time_ns % 1_000_000 != 0 {
        return Err(StateCodecError::new("OPEN_TIME"));
    }
    Ok((bar.open_time_ns / 1_000_000) as u64)
}

// ------------------------------------------------------------------ BAR cache row

fn restore_bar(mut envelope: EventEnvelope, lpk: &LogicalProductKey) -> Vec<u8> {
    envelope.instrument_uid = lpk.instrument_uid.clone();
    envelope.venue = lpk.venue.clone();
    envelope.market = lpk.market.clone();
    if let Some(bar) = bar_mut(&mut envelope) {
        bar.interval = lpk.qualifier.clone();
    }
    envelope.encode_to_vec()
}

/// BAR current-index row: trailer + the canonical envelope without the LPK
/// fields. Refuses a non-BAR, a row of another product and canonical bytes
/// that `decode_bar_row` would not reproduce byte for byte.
pub fn encode_bar_row(
    canonical: &[u8],
    lpk: &LogicalProductKey,
    source_offset: u64,
    materializer_epoch: u64,
) -> Result<Vec<u8>> {
    let trailer = state_trailer(canonical, source_offset, materializer_epoch)?;
    let mut envelope = parse_envelope(canonical)?;
    if bar_of(&envelope).is_none() {
        return Err(StateCodecError::new("NOT_BAR"));
    }
    check_product(&envelope, lpk)?;
    envelope.instrument_uid.clear();
    envelope.venue.clear();
    envelope.market.clear();
    if let Some(bar) = bar_mut(&mut envelope) {
        bar.interval.clear();
    }
    let body = envelope.encode_to_vec();
    if restore_bar(envelope, lpk) != canonical {
        return Err(StateCodecError::new("ENVELOPE_NOT_CANONICAL"));
    }
    let mut row = Vec::with_capacity(TRAILER_BYTES + body.len());
    row.extend_from_slice(&trailer);
    row.extend_from_slice(&body);
    Ok(row)
}

pub fn decode_bar_row(row: &[u8], lpk: &LogicalProductKey) -> Result<DecodedState> {
    let (source_offset, materializer_epoch, stored) = read_trailer(row)?;
    let envelope = parse_envelope(&row[TRAILER_BYTES..])?;
    let Some(bar) = bar_of(&envelope) else {
        return Err(StateCodecError::new("NOT_BAR"));
    };
    if !envelope.instrument_uid.is_empty()
        || !envelope.venue.is_empty()
        || !envelope.market.is_empty()
        || !bar.interval.is_empty()
    {
        return Err(StateCodecError::new("NON_CANONICAL"));
    }
    let canonical = restore_bar(envelope, lpk);
    if sha256(&canonical) != stored {
        return Err(StateCodecError::new("CONTENT_HASH"));
    }
    Ok(DecodedState {
        canonical,
        source_offset,
        materializer_epoch,
    })
}

// ------------------------------------------------------------------ canonical JSON header

/// Sorted byte-order keys, `,`/`:`, no whitespace. Only called on validated
/// values (strings from the no-escape charset, u64 integers, bools, objects).
pub fn canonical_json(value: &Map<String, Value>) -> Vec<u8> {
    let mut out = String::from("{");
    let mut keys: Vec<&String> = value.keys().collect();
    keys.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    for (index, key) in keys.into_iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(key);
        out.push_str("\":");
        match &value[key] {
            Value::Bool(flag) => out.push_str(if *flag { "true" } else { "false" }),
            Value::Number(number) => out.push_str(&number.to_string()),
            Value::String(text) => {
                out.push('"');
                out.push_str(text);
                out.push('"');
            }
            Value::Object(object) => {
                out.push_str(&String::from_utf8_lossy(&canonical_json(object)));
            }
            other => out.push_str(&other.to_string()),
        }
    }
    out.push('}');
    out.into_bytes()
}

fn exact(value: &Map<String, Value>, names: &[&str], at: &str) -> Result<()> {
    if value.len() != names.len() || names.iter().any(|name| !value.contains_key(*name)) {
        return Err(StateCodecError::at("HEADER_FIELDS", at));
    }
    Ok(())
}

fn integer(value: &Value, minimum: u64, maximum: u64, name: &str) -> Result<u64> {
    value
        .as_u64()
        .filter(|number| (minimum..=maximum).contains(number))
        .ok_or_else(|| StateCodecError::at("FIELD", name))
}

fn text<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value
        .as_str()
        .filter(|text| is_text(text))
        .ok_or_else(|| StateCodecError::at("FIELD", name))
}

fn object<'a>(value: &'a Value, name: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| StateCodecError::at("FIELD", name))
}

/// Exact field set, then every field in sorted name order (nested too).
fn validate_header(kind: FrameKind, header: &Map<String, Value>) -> Result<LogicalProductKey> {
    let names = kind.fields();
    exact(header, names, "header")?;
    let mut lpk = None;
    for name in names {
        let value = &header[*name];
        match *name {
            "content_sha256" => {
                if !value.as_str().is_some_and(is_hex64) {
                    return Err(StateCodecError::at("FIELD", name));
                }
            }
            "event_id" => {
                if !value.as_str().is_some_and(is_event_id) {
                    return Err(StateCodecError::at("FIELD", name));
                }
            }
            "floor_open_time_ms" | "open_time_ms" => {
                integer(value, 0, MAX_OFFSET, name)?;
            }
            "is_final" => {
                if !value.is_boolean() {
                    return Err(StateCodecError::at("FIELD", name));
                }
            }
            "legacy" => {
                let legacy = object(value, name)?;
                exact(
                    legacy,
                    &[
                        "spool_logical_offset",
                        "spool_partition_key",
                        "spool_stream",
                    ],
                    name,
                )?;
                integer(
                    &legacy["spool_logical_offset"],
                    0,
                    MAX_OFFSET,
                    "legacy.spool_logical_offset",
                )?;
                text(&legacy["spool_partition_key"], "legacy.spool_partition_key")?;
                text(&legacy["spool_stream"], "legacy.spool_stream")?;
            }
            "lpk" => {
                let encoded = text(value, name)?;
                lpk = Some(
                    LogicalProductKey::parse(encoded)
                        .map_err(|_| StateCodecError::at("FIELD", name))?,
                );
            }
            "materializer_epoch" => {
                integer(value, 1, MAX_OFFSET, name)?;
            }
            "provenance" => {
                if value.as_str() != Some(LEGACY_PROVENANCE) {
                    return Err(StateCodecError::at("FIELD", name));
                }
            }
            "revision" => {
                integer(value, 0, MAX_REVISION, name)?;
            }
            "source" => {
                let source = object(value, name)?;
                exact(source, &["offset", "partition", "topic_id"], name)?;
                integer(&source["offset"], 0, MAX_OFFSET, "source.offset")?;
                integer(&source["partition"], 0, MAX_PARTITION, "source.partition")?;
                text(&source["topic_id"], "source.topic_id")?;
            }
            _ => unreachable!("header field sets are static"),
        }
    }
    Ok(lpk.expect("every header carries an lpk"))
}

// ------------------------------------------------------------------ state-topic frame

/// Where a legacy-imported BAR came from; never a canonical Kafka offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyLineage {
    pub spool_stream: String,
    pub spool_partition_key: String,
    pub spool_logical_offset: u64,
}

/// One state-topic record (decision D2). Build it with the constructors,
/// which derive every header fact from the envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateFrame {
    pub kind: FrameKind,
    pub lpk: LogicalProductKey,
    pub materializer_epoch: u64,
    pub envelope: Vec<u8>,
    pub content_sha256: Option<String>,
    pub event_id: Option<String>,
    pub source: Option<SourceCoordinate>,
    pub open_time_ms: Option<u64>,
    pub revision: Option<u32>,
    pub is_final: Option<bool>,
    pub floor_open_time_ms: Option<u64>,
    pub legacy: Option<LegacyLineage>,
}

/// What the envelope itself says; a header must agree with every item.
struct EnvelopeFacts {
    content_sha256: String,
    event_id: String,
    bar: Option<(u64, u32, bool)>,
}

fn envelope_facts(
    kind: FrameKind,
    canonical: &[u8],
    lpk: &LogicalProductKey,
) -> Result<EnvelopeFacts> {
    if canonical.is_empty() {
        return Err(StateCodecError::new("BODY"));
    }
    let envelope = parse_envelope(canonical)?;
    if kind.is_bar() && bar_of(&envelope).is_none() {
        return Err(StateCodecError::new("NOT_BAR"));
    }
    check_product(&envelope, lpk)?;
    let bar = match bar_of(&envelope) {
        Some(bar) if kind.is_bar() => Some((open_time_ms(bar)?, bar.revision, bar.is_final)),
        _ => None,
    };
    Ok(EnvelopeFacts {
        content_sha256: hex(&sha256(canonical)),
        event_id: hex(&envelope.event_id),
        bar,
    })
}

impl StateFrame {
    fn empty(kind: FrameKind, lpk: &LogicalProductKey, materializer_epoch: u64) -> Self {
        Self {
            kind,
            lpk: lpk.clone(),
            materializer_epoch,
            envelope: Vec::new(),
            content_sha256: None,
            event_id: None,
            source: None,
            open_time_ms: None,
            revision: None,
            is_final: None,
            floor_open_time_ms: None,
            legacy: None,
        }
    }

    fn with_facts(mut self, canonical: &[u8], facts: EnvelopeFacts) -> Self {
        self.envelope = canonical.to_vec();
        self.content_sha256 = Some(facts.content_sha256);
        self.event_id = Some(facts.event_id);
        if let Some((open_time_ms, revision, is_final)) = facts.bar {
            self.open_time_ms = Some(open_time_ms);
            self.revision = Some(revision);
            self.is_final = Some(is_final);
        }
        self
    }

    fn from_envelope(
        kind: FrameKind,
        canonical: &[u8],
        lpk: &LogicalProductKey,
        materializer_epoch: u64,
        source: Option<SourceCoordinate>,
        legacy: Option<LegacyLineage>,
    ) -> Result<Self> {
        let facts = envelope_facts(kind, canonical, lpk)?;
        let mut frame = Self::empty(kind, lpk, materializer_epoch).with_facts(canonical, facts);
        frame.source = source;
        frame.legacy = legacy;
        validate_header(kind, &frame.header())?;
        Ok(frame)
    }

    pub fn latest(
        canonical: &[u8],
        lpk: &LogicalProductKey,
        source: SourceCoordinate,
        materializer_epoch: u64,
    ) -> Result<Self> {
        Self::from_envelope(
            FrameKind::Latest,
            canonical,
            lpk,
            materializer_epoch,
            Some(source),
            None,
        )
    }

    pub fn bar_revision(
        canonical: &[u8],
        lpk: &LogicalProductKey,
        source: SourceCoordinate,
        materializer_epoch: u64,
    ) -> Result<Self> {
        Self::from_envelope(
            FrameKind::BarRevision,
            canonical,
            lpk,
            materializer_epoch,
            Some(source),
            None,
        )
    }

    pub fn legacy_bar(
        canonical: &[u8],
        lpk: &LogicalProductKey,
        legacy: LegacyLineage,
        materializer_epoch: u64,
    ) -> Result<Self> {
        Self::from_envelope(
            FrameKind::LegacyBar,
            canonical,
            lpk,
            materializer_epoch,
            None,
            Some(legacy),
        )
    }

    pub fn retention_floor(
        lpk: &LogicalProductKey,
        floor_open_time_ms: u64,
        materializer_epoch: u64,
    ) -> Result<Self> {
        if lpk.feed != "BAR" {
            return Err(StateCodecError::at("PRODUCT_MISMATCH", "feed"));
        }
        let mut frame = Self::empty(FrameKind::RetentionFloor, lpk, materializer_epoch);
        frame.floor_open_time_ms = Some(floor_open_time_ms);
        validate_header(FrameKind::RetentionFloor, &frame.header())?;
        Ok(frame)
    }

    /// The header object for this kind (absent facts are `null`, which
    /// validation refuses).
    pub fn header(&self) -> Map<String, Value> {
        let optional = |value: Option<Value>| value.unwrap_or(Value::Null);
        let mut header = Map::new();
        header.insert("lpk".into(), Value::from(self.lpk.encode()));
        header.insert(
            "materializer_epoch".into(),
            Value::from(self.materializer_epoch),
        );
        if self.kind == FrameKind::RetentionFloor {
            header.insert(
                "floor_open_time_ms".into(),
                optional(self.floor_open_time_ms.map(Value::from)),
            );
            return header;
        }
        header.insert(
            "content_sha256".into(),
            optional(self.content_sha256.clone().map(Value::from)),
        );
        header.insert(
            "event_id".into(),
            optional(self.event_id.clone().map(Value::from)),
        );
        if self.kind == FrameKind::LegacyBar {
            let legacy = self.legacy.as_ref().map(|legacy| {
                let mut object = Map::new();
                object.insert(
                    "spool_logical_offset".into(),
                    Value::from(legacy.spool_logical_offset),
                );
                object.insert(
                    "spool_partition_key".into(),
                    Value::from(legacy.spool_partition_key.clone()),
                );
                object.insert(
                    "spool_stream".into(),
                    Value::from(legacy.spool_stream.clone()),
                );
                Value::Object(object)
            });
            header.insert("legacy".into(), optional(legacy));
            header.insert("provenance".into(), Value::from(LEGACY_PROVENANCE));
        } else {
            let source = self.source.as_ref().map(|source| {
                let mut object = Map::new();
                object.insert("offset".into(), Value::from(source.offset));
                object.insert("partition".into(), Value::from(source.partition));
                object.insert("topic_id".into(), Value::from(source.topic_id.clone()));
                Value::Object(object)
            });
            header.insert("source".into(), optional(source));
        }
        if self.kind.is_bar() {
            header.insert(
                "open_time_ms".into(),
                optional(self.open_time_ms.map(Value::from)),
            );
            header.insert("revision".into(), optional(self.revision.map(Value::from)));
            header.insert("is_final".into(), optional(self.is_final.map(Value::from)));
        }
        header
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let header = self.header();
        validate_header(self.kind, &header)?;
        let encoded = canonical_json(&header);
        let mut frame =
            Vec::with_capacity(FRAME_PREFIX_BYTES + encoded.len() + self.envelope.len());
        frame.extend_from_slice(FRAME_MAGIC);
        frame.push(self.kind as u8);
        frame.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        frame.extend_from_slice(&encoded);
        frame.extend_from_slice(&self.envelope);
        Ok(frame)
    }

    /// Strict decode; the first failing check in the documented order wins.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < FRAME_PREFIX_BYTES {
            return Err(StateCodecError::new("TRUNCATED"));
        }
        if &data[..4] != FRAME_MAGIC {
            return Err(StateCodecError::new("MAGIC"));
        }
        let kind = FrameKind::from_byte(data[4]).ok_or(StateCodecError::new("KIND"))?;
        let header_len = u32::from_be_bytes([data[5], data[6], data[7], data[8]]) as usize;
        if header_len > MAX_HEADER_BYTES {
            return Err(StateCodecError::new("HEADER_SIZE"));
        }
        if FRAME_PREFIX_BYTES + header_len > data.len() {
            return Err(StateCodecError::new("TRUNCATED"));
        }
        let header_bytes = &data[FRAME_PREFIX_BYTES..FRAME_PREFIX_BYTES + header_len];
        let body = &data[FRAME_PREFIX_BYTES + header_len..];
        let header = match serde_json::from_slice::<Value>(header_bytes) {
            Ok(Value::Object(header)) => header,
            _ => return Err(StateCodecError::new("HEADER_JSON")),
        };
        let lpk = validate_header(kind, &header)?;
        if canonical_json(&header) != header_bytes {
            return Err(StateCodecError::new("NON_CANONICAL"));
        }
        let epoch = header["materializer_epoch"].as_u64().unwrap_or_default();
        if kind == FrameKind::RetentionFloor {
            if !body.is_empty() {
                return Err(StateCodecError::new("BODY"));
            }
            if lpk.feed != "BAR" {
                return Err(StateCodecError::at("PRODUCT_MISMATCH", "feed"));
            }
            let mut frame = Self::empty(kind, &lpk, epoch);
            frame.floor_open_time_ms = header["floor_open_time_ms"].as_u64();
            return Ok(frame);
        }
        if body.is_empty() {
            return Err(StateCodecError::new("BODY"));
        }
        if Some(hex(&sha256(body)).as_str()) != header["content_sha256"].as_str() {
            return Err(StateCodecError::new("CONTENT_HASH"));
        }
        let source = header.get("source").map(|source| SourceCoordinate {
            topic_id: source["topic_id"].as_str().unwrap_or_default().to_owned(),
            partition: source["partition"].as_u64().unwrap_or_default() as u32,
            offset: source["offset"].as_u64().unwrap_or_default(),
        });
        let legacy = header.get("legacy").map(|legacy| LegacyLineage {
            spool_stream: legacy["spool_stream"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            spool_partition_key: legacy["spool_partition_key"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            spool_logical_offset: legacy["spool_logical_offset"].as_u64().unwrap_or_default(),
        });
        let facts = envelope_facts(kind, body, &lpk)?;
        if header["event_id"].as_str() != Some(facts.event_id.as_str()) {
            return Err(StateCodecError::at("HEADER_MISMATCH", "event_id"));
        }
        if let Some((open_time_ms, revision, is_final)) = facts.bar {
            for (name, agrees) in [
                (
                    "open_time_ms",
                    header["open_time_ms"].as_u64() == Some(open_time_ms),
                ),
                (
                    "revision",
                    header["revision"].as_u64() == Some(u64::from(revision)),
                ),
                ("is_final", header["is_final"].as_bool() == Some(is_final)),
            ] {
                if !agrees {
                    return Err(StateCodecError::at("HEADER_MISMATCH", name));
                }
            }
        }
        let mut frame = Self::empty(kind, &lpk, epoch).with_facts(body, facts);
        frame.source = source;
        frame.legacy = legacy;
        Ok(frame)
    }

    /// The compaction key of this record in its state topic.
    pub fn key(&self) -> Result<String> {
        match self.kind {
            FrameKind::Latest => Ok(latest_key(&self.lpk)),
            FrameKind::RetentionFloor => Ok(floor_key(&self.lpk)),
            FrameKind::BarRevision | FrameKind::LegacyBar => bar_key(
                &self.lpk,
                self.open_time_ms.unwrap_or(u64::MAX),
                self.is_final.unwrap_or(false),
                self.revision.unwrap_or_default(),
                self.content_sha256.as_deref().unwrap_or_default(),
            ),
        }
    }
}

// ------------------------------------------------------------------ keys and partition

pub fn latest_key(lpk: &LogicalProductKey) -> String {
    lpk.encode()
}

/// `<lpk>|<open_ms>|p` for the one in-progress row of an open time, else
/// `<lpk>|<open_ms>|f<revision>|<first 16 hex of content_sha256>` per final fact.
pub fn bar_key(
    lpk: &LogicalProductKey,
    open_time_ms: u64,
    is_final: bool,
    revision: u32,
    content_sha256: &str,
) -> Result<String> {
    if open_time_ms > MAX_OFFSET {
        return Err(StateCodecError::at("FIELD", "open_time_ms"));
    }
    if !is_final {
        return Ok(format!("{}|{open_time_ms}|p", lpk.encode()));
    }
    if !is_hex64(content_sha256) {
        return Err(StateCodecError::at("FIELD", "content_sha256"));
    }
    Ok(format!(
        "{}|{open_time_ms}|f{revision}|{}",
        lpk.encode(),
        &content_sha256[..16]
    ))
}

pub fn floor_key(lpk: &LogicalProductKey) -> String {
    format!("{}|floor", lpk.encode())
}

/// Kafka `org.apache.kafka.common.utils.Utils.murmur2`, as Java's signed int.
pub fn murmur2(data: &[u8]) -> i32 {
    let mut h = KAFKA_MURMUR2_SEED ^ (data.len() as u32);
    let chunks = data.chunks_exact(4);
    let tail = chunks.remainder();
    for chunk in chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(MURMUR2_M);
        k ^= k >> 24;
        k = k.wrapping_mul(MURMUR2_M);
        h = h.wrapping_mul(MURMUR2_M) ^ k;
    }
    if tail.len() >= 3 {
        h ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        h ^= u32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        h ^= u32::from(tail[0]);
        h = h.wrapping_mul(MURMUR2_M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(MURMUR2_M);
    h ^= h >> 15;
    h as i32
}

/// Kafka's default partitioner over the LPK bytes: one product, one partition.
pub fn state_partition(lpk: &LogicalProductKey, partitions: u32) -> Result<u32> {
    if !(1..=MAX_PARTITION as u32).contains(&partitions) {
        return Err(StateCodecError::new("PARTITIONS"));
    }
    Ok(((murmur2(lpk.encode().as_bytes()) as u32) & 0x7fff_ffff) % partitions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;

    fn golden() -> Value {
        let path = format!(
            "{}/../../contracts/golden/kn_v220/state_codec.json",
            env!("CARGO_MANIFEST_DIR")
        );
        serde_json::from_str(&std::fs::read_to_string(path).expect("golden")).expect("json")
    }

    fn b64(value: &Value) -> Vec<u8> {
        STANDARD
            .decode(value.as_str().expect("base64 text"))
            .expect("base64")
    }

    fn unhex(value: &str) -> Vec<u8> {
        (0..value.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&value[index..index + 2], 16).expect("hex"))
            .collect()
    }

    fn lpk(value: &Value) -> LogicalProductKey {
        LogicalProductKey::parse(value.as_str().expect("lpk")).expect("valid lpk")
    }

    fn u64_of(value: &Value) -> u64 {
        value.as_u64().expect("u64")
    }

    fn source(value: &Value) -> SourceCoordinate {
        SourceCoordinate {
            topic_id: value["topic_id"].as_str().expect("topic").to_owned(),
            partition: u64_of(&value["partition"]) as u32,
            offset: u64_of(&value["offset"]),
        }
    }

    fn legacy(record: &Value) -> LegacyLineage {
        LegacyLineage {
            spool_stream: record["spool_stream"].as_str().expect("stream").to_owned(),
            spool_partition_key: record["spool_partition_key"]
                .as_str()
                .expect("key")
                .to_owned(),
            spool_logical_offset: u64_of(&record["spool_offset"]),
        }
    }

    fn kind_named(name: &str) -> FrameKind {
        (1..=4)
            .filter_map(FrameKind::from_byte)
            .find(|kind| kind.as_str() == name)
            .expect("kind")
    }

    fn expected_frame(kind: FrameKind, header: &str, body: &[u8]) -> Vec<u8> {
        let mut frame = FRAME_MAGIC.to_vec();
        frame.push(kind as u8);
        frame.extend_from_slice(&(header.len() as u32).to_be_bytes());
        frame.extend_from_slice(header.as_bytes());
        frame.extend_from_slice(body);
        frame
    }

    type Bodies = std::collections::BTreeMap<String, Vec<u8>>;

    fn bodies(doc: &Value) -> Bodies {
        doc["records"]
            .as_array()
            .expect("records")
            .iter()
            .map(|record| {
                (
                    record["name"].as_str().expect("name").to_owned(),
                    b64(&record["canonical_b64"]),
                )
            })
            .collect()
    }

    /// Vector bytes: `field`, or `head_b64` + the named record's canonical bytes.
    fn resolve_bytes(case: &Value, field: &str, bodies: &Bodies) -> Vec<u8> {
        match case["body_record"].as_str() {
            Some(name) => {
                let mut bytes = b64(&case["head_b64"]);
                bytes.extend_from_slice(&bodies[name]);
                bytes
            }
            None => b64(&case[field]),
        }
    }

    fn resolve_canonical(case: &Value, bodies: &Bodies) -> Vec<u8> {
        if let Some(name) = case["canonical_record"].as_str() {
            let mut bytes = bodies[name].clone();
            bytes.extend(unhex(
                case["canonical_suffix_hex"].as_str().expect("suffix"),
            ));
            bytes
        } else if case["canonical_b64"].is_string() {
            b64(&case["canonical_b64"])
        } else {
            Vec::new()
        }
    }

    fn reason<T: std::fmt::Debug>(result: Result<T>) -> (String, String) {
        let error = result.expect_err("refused");
        (error.reason.to_owned(), error.detail)
    }

    fn expected_reason(case: &Value) -> (String, String) {
        (
            case["reason"].as_str().expect("reason").to_owned(),
            case["detail"].as_str().unwrap_or_default().to_owned(),
        )
    }

    #[test]
    fn golden_records_encode_and_decode_byte_for_byte() {
        let doc = golden();
        let topic = doc["records"].as_array().expect("records");
        assert!(topic.len() >= 20);
        for record in topic {
            let name = record["name"].as_str().expect("name");
            let canonical = b64(&record["canonical_b64"]);
            let key = lpk(&record["lpk"]);
            let offset = u64_of(&record["source"]["offset"]);
            let epoch = u64_of(&record["materializer_epoch"]);
            let mut value = unhex(
                record["latest_value_trailer_hex"]
                    .as_str()
                    .expect("trailer"),
            );
            value.extend_from_slice(&canonical);
            assert_eq!(
                encode_latest_value(&canonical, offset, epoch).expect("value"),
                value,
                "{name}"
            );
            let decoded = decode_latest_value(&value).expect("decode value");
            assert_eq!(
                (
                    decoded.canonical,
                    decoded.source_offset,
                    decoded.materializer_epoch
                ),
                (canonical.clone(), offset, epoch)
            );
            if !record["bar_row_b64"].is_null() {
                let row = b64(&record["bar_row_b64"]);
                assert_eq!(
                    encode_bar_row(&canonical, &key, offset, epoch).expect("row"),
                    row,
                    "{name}"
                );
                let decoded = decode_bar_row(&row, &key).expect("decode row");
                assert_eq!(decoded.canonical, canonical, "{name}");
                assert_eq!(
                    (decoded.source_offset, decoded.materializer_epoch),
                    (offset, epoch)
                );
            }
            for (kind_name, header) in record["headers"].as_object().expect("headers") {
                let kind = kind_named(kind_name);
                let frame = match kind {
                    FrameKind::Latest => {
                        StateFrame::latest(&canonical, &key, source(&record["source"]), epoch)
                    }
                    FrameKind::BarRevision => {
                        StateFrame::bar_revision(&canonical, &key, source(&record["source"]), epoch)
                    }
                    FrameKind::LegacyBar => {
                        StateFrame::legacy_bar(&canonical, &key, legacy(record), epoch)
                    }
                    FrameKind::RetentionFloor => unreachable!("floors are listed separately"),
                }
                .expect("frame");
                let bytes = frame.encode().expect("encode");
                assert_eq!(
                    bytes,
                    expected_frame(kind, header.as_str().expect("header"), &canonical),
                    "{name} {kind_name}"
                );
                assert_eq!(StateFrame::decode(&bytes).expect("decode"), frame);
                let key_name = if kind == FrameKind::Latest {
                    "latest"
                } else {
                    "bar"
                };
                assert_eq!(
                    frame.key().expect("key"),
                    record["keys"][key_name].as_str().expect("key"),
                    "{name}"
                );
            }
            assert_eq!(
                floor_key(&key),
                record["keys"]["floor"].as_str().expect("floor")
            );
            for (partitions, expected) in record["partitions"].as_object().expect("partitions") {
                assert_eq!(
                    u64::from(state_partition(&key, partitions.parse().expect("n")).expect("p")),
                    u64_of(expected),
                    "{name} {partitions}"
                );
            }
        }
    }

    #[test]
    fn golden_full_frames_and_floors() {
        let doc = golden();
        let records = doc["records"].as_array().expect("records");
        for case in doc["full_frames"].as_array().expect("full frames") {
            let bytes = b64(&case["frame_b64"]);
            let frame = StateFrame::decode(&bytes).expect("decode");
            assert_eq!(frame.kind.as_str(), case["kind"].as_str().expect("kind"));
            let record = records
                .iter()
                .find(|record| record["name"] == case["record"])
                .expect("record");
            assert_eq!(frame.envelope, b64(&record["canonical_b64"]));
            assert_eq!(frame.encode().expect("encode"), bytes);
        }
        for case in doc["floor_frames"].as_array().expect("floors") {
            let frame = StateFrame::retention_floor(
                &lpk(&case["lpk"]),
                u64_of(&case["floor_open_time_ms"]),
                u64_of(&case["materializer_epoch"]),
            )
            .expect("floor");
            let bytes = frame.encode().expect("encode");
            assert_eq!(bytes, b64(&case["frame_b64"]));
            assert_eq!(StateFrame::decode(&bytes).expect("decode"), frame);
            assert_eq!(
                frame.key().expect("key"),
                case["key"].as_str().expect("key")
            );
        }
    }

    #[test]
    fn golden_invalid_vectors_fail_with_the_python_reason() {
        let doc = golden();
        let bodies = bodies(&doc);
        let mut seen = std::collections::BTreeSet::new();
        for case in doc["invalid_frames"].as_array().expect("frames") {
            let got = reason(StateFrame::decode(&resolve_bytes(
                case,
                "frame_b64",
                &bodies,
            )));
            assert_eq!(got, expected_reason(case), "{}", case["name"]);
            seen.insert(got.0);
        }
        for case in doc["invalid_bar_rows"].as_array().expect("rows") {
            let got = reason(decode_bar_row(
                &resolve_bytes(case, "row_b64", &bodies),
                &lpk(&case["lpk"]),
            ));
            assert_eq!(got, expected_reason(case), "{}", case["name"]);
            seen.insert(got.0);
        }
        for case in doc["invalid_latest_values"].as_array().expect("values") {
            let got = reason(decode_latest_value(&b64(&case["value_b64"])));
            assert_eq!(got, expected_reason(case), "{}", case["name"]);
            seen.insert(got.0);
        }
        for case in doc["refused_encodes"].as_array().expect("encodes") {
            let canonical = resolve_canonical(case, &bodies);
            let offset = case["source_offset"].as_u64().unwrap_or_default();
            let epoch = u64_of(&case["materializer_epoch"]);
            let key = || lpk(&case["lpk"]);
            let got = match case["op"].as_str().expect("op") {
                "bar_row" => reason(encode_bar_row(&canonical, &key(), offset, epoch)),
                "latest_value" => reason(encode_latest_value(&canonical, offset, epoch)),
                "LATEST" => reason(StateFrame::latest(
                    &canonical,
                    &key(),
                    source(&case["source"]),
                    epoch,
                )),
                "BAR_REVISION" => reason(StateFrame::bar_revision(
                    &canonical,
                    &key(),
                    source(&case["source"]),
                    epoch,
                )),
                "LEGACY_BAR" => reason(StateFrame::legacy_bar(
                    &canonical,
                    &key(),
                    legacy(case),
                    epoch,
                )),
                "RETENTION_FLOOR" => reason(StateFrame::retention_floor(
                    &key(),
                    u64_of(&case["floor_open_time_ms"]),
                    epoch,
                )),
                "partition" => reason(state_partition(&key(), u64_of(&case["partitions"]) as u32)),
                other => panic!("unknown op {other}"),
            };
            assert_eq!(got, expected_reason(case), "{}", case["name"]);
            seen.insert(got.0);
        }
        let all: std::collections::BTreeSet<String> =
            REASONS.iter().map(|reason| reason.to_string()).collect();
        assert_eq!(seen, all, "every reason has a golden vector");
        let listed: Vec<&str> = doc["reasons"]
            .as_array()
            .expect("reasons")
            .iter()
            .map(|reason| reason.as_str().expect("reason"))
            .collect();
        assert_eq!(listed, REASONS);
    }

    #[test]
    fn kafka_murmur2_vectors() {
        let doc = golden();
        let cases = doc["murmur2"].as_array().expect("murmur2");
        assert!(cases.len() >= 6);
        for case in cases {
            let input = unhex(case["input_hex"].as_str().expect("input"));
            assert_eq!(
                i64::from(murmur2(&input)),
                case["hash"].as_i64().expect("hash"),
                "{case}"
            );
        }
        // Kafka UtilsTest.testMurmur2, kept in code as well as in the golden.
        assert_eq!(murmur2(b"21"), -973_932_308);
        assert_eq!(murmur2(b"foobar"), -790_332_482);
        assert_eq!(murmur2(b"abc"), 479_470_107);
    }

    #[test]
    fn header_numbers_are_strict() {
        let lpk = LogicalProductKey::parse(
            "lpk1|paper|OKX|SWAP|6c7c9256-2905-5c75-a149-fa0ac36bbbc7|BAR|1m",
        )
        .expect("lpk");
        let floor = StateFrame::retention_floor(&lpk, 5, 1).expect("floor");
        let bytes = floor.encode().expect("encode");
        let header_len = bytes.len() - FRAME_PREFIX_BYTES;
        let header = std::str::from_utf8(&bytes[FRAME_PREFIX_BYTES..]).expect("utf8");
        assert_eq!(header_len, header.len());
        for (replacement, expected) in [
            (
                "\"floor_open_time_ms\":5.0",
                ("FIELD", "floor_open_time_ms"),
            ),
            (
                "\"floor_open_time_ms\":true",
                ("FIELD", "floor_open_time_ms"),
            ),
            (
                "\"floor_open_time_ms\":\"5\"",
                ("FIELD", "floor_open_time_ms"),
            ),
            ("\"floor_open_time_ms\":-1", ("FIELD", "floor_open_time_ms")),
            ("\"floor_open_time_ms\":-0", ("FIELD", "floor_open_time_ms")),
            ("\"floor_open_time_ms\":05", ("HEADER_JSON", "")),
        ] {
            let edited = header.replace("\"floor_open_time_ms\":5", replacement);
            let frame = expected_frame(FrameKind::RetentionFloor, &edited, b"");
            let error = StateFrame::decode(&frame).expect_err(replacement);
            assert_eq!(
                (error.reason, error.detail.as_str()),
                expected,
                "{replacement}"
            );
        }
        assert_eq!(
            StateFrame::retention_floor(&lpk, 5, 0)
                .expect_err("epoch 0")
                .detail,
            "materializer_epoch"
        );
    }

    #[test]
    fn keys_partition_and_trailer_edges() {
        let lpk = LogicalProductKey::parse(
            "lpk1|paper|BINANCE|USDM|8aedd349-6999-5874-b0dd-34c6451c0b3a|BAR|1m",
        )
        .expect("lpk");
        let digest = "ab".repeat(32);
        assert_eq!(
            bar_key(&lpk, 60_000, false, 3, "").expect("in-progress"),
            format!("{}|60000|p", lpk.encode())
        );
        assert_eq!(
            bar_key(&lpk, 60_000, true, 3, &digest).expect("final"),
            format!("{}|60000|f3|abababababababab", lpk.encode())
        );
        assert_eq!(
            bar_key(&lpk, 60_000, true, 3, "AB")
                .expect_err("hash")
                .reason,
            "FIELD"
        );
        assert_eq!(
            state_partition(&lpk, 0).expect_err("zero").reason,
            "PARTITIONS"
        );
        assert_eq!(state_partition(&lpk, 1).expect("one"), 0);
        assert_eq!(
            state_trailer(b"x", MAX_OFFSET + 1, 1)
                .expect_err("offset")
                .reason,
            "TRAILER"
        );
        let trailer = state_trailer(b"x", MAX_OFFSET, MAX_OFFSET).expect("widest");
        assert_eq!(&trailer[..8], &MAX_OFFSET.to_be_bytes());
        assert_eq!(
            decode_latest_value(&[0_u8; 47]).expect_err("short").reason,
            "TRUNCATED"
        );
    }

    /// Opt-in full-sample check (KN-3 codec slice): `QDL_KN3_CROSS_IN` is the
    /// Python export of every sample record (`kn_state_codec_golden.py
    /// cross-export`); Rust re-encodes each, compares bytes, decodes the
    /// Python bytes and writes its own encodings to `QDL_KN3_CROSS_OUT` for
    /// `cross-verify` to decode in Python. Without the variables it is a no-op.
    #[test]
    fn full_sample_cross_check_when_exported() {
        let (Ok(input), Ok(output)) = (
            std::env::var("QDL_KN3_CROSS_IN"),
            std::env::var("QDL_KN3_CROSS_OUT"),
        ) else {
            eprintln!("full-sample cross-check not requested (QDL_KN3_CROSS_IN/OUT unset)");
            return;
        };
        let doc: Value =
            serde_json::from_str(&std::fs::read_to_string(input).expect("cross")).expect("json");
        let mut mismatches = Vec::new();
        let mut out = Vec::new();
        let mut compare = |name: &str, what: &str, rust: &[u8], python: &[u8]| {
            if rust != python {
                let first = rust
                    .iter()
                    .zip(python)
                    .position(|(left, right)| left != right)
                    .unwrap_or(rust.len().min(python.len()));
                mismatches.push(format!("{name} {what}: first differing byte {first}"));
            }
        };
        for record in doc["records"].as_array().expect("records") {
            let name = record["name"].as_str().expect("name");
            let canonical = b64(&record["canonical_b64"]);
            let key = lpk(&record["lpk"]);
            let offset = u64_of(&record["source"]["offset"]);
            let epoch = u64_of(&record["materializer_epoch"]);
            let value = encode_latest_value(&canonical, offset, epoch).expect("value");
            compare(
                name,
                "latest_value",
                &value,
                &b64(&record["latest_value_b64"]),
            );
            assert_eq!(
                decode_latest_value(&b64(&record["latest_value_b64"]))
                    .expect("decode python value")
                    .canonical,
                canonical
            );
            let mut row_b64 = Value::Null;
            if !record["bar_row_b64"].is_null() {
                let row = encode_bar_row(&canonical, &key, offset, epoch).expect("row");
                let python = b64(&record["bar_row_b64"]);
                compare(name, "bar_row", &row, &python);
                assert_eq!(
                    decode_bar_row(&python, &key)
                        .expect("decode python row")
                        .canonical,
                    canonical,
                    "{name}"
                );
                row_b64 = Value::from(STANDARD.encode(&row));
            }
            let mut frames = Map::new();
            for (kind_name, python_b64) in record["frames_b64"].as_object().expect("frames") {
                let frame = match kind_named(kind_name) {
                    FrameKind::Latest => {
                        StateFrame::latest(&canonical, &key, source(&record["source"]), epoch)
                    }
                    FrameKind::BarRevision => {
                        StateFrame::bar_revision(&canonical, &key, source(&record["source"]), epoch)
                    }
                    FrameKind::LegacyBar => {
                        StateFrame::legacy_bar(&canonical, &key, legacy(record), epoch)
                    }
                    FrameKind::RetentionFloor => unreachable!("no floor per record"),
                }
                .expect("frame");
                let bytes = frame.encode().expect("encode");
                let python = b64(python_b64);
                compare(name, kind_name, &bytes, &python);
                assert_eq!(
                    StateFrame::decode(&python).expect("decode python frame"),
                    frame
                );
                frames.insert(kind_name.clone(), Value::from(STANDARD.encode(&bytes)));
            }
            let mut item = Map::new();
            item.insert("name".into(), Value::from(name));
            item.insert(
                "latest_value_b64".into(),
                Value::from(STANDARD.encode(&value)),
            );
            item.insert("bar_row_b64".into(), row_b64);
            item.insert("frames_b64".into(), Value::Object(frames));
            out.push(Value::Object(item));
        }
        let mut document = Map::new();
        document.insert("records".into(), Value::Array(out));
        std::fs::write(output, serde_json::to_vec(&document).expect("json")).expect("write");
        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }
}
