//! DL v2.3.2 x TS v2.1.5 isolated e2e gateway (TEST_ONLY; owner GO 2026-11-18). NOT Kafka or live evidence.
//!
//! Usage: v232_quiet_quote_e2e PRIVATE_DIR BUNDLE QUERY_PORT GRPC_PORT
//!
//! Runs the real v2.3.2 Stream service (`Gateway`: hub, replay, access, cursor codec, SessionAlive frames and the
//! opt-in DELIVERY_ON_CHANGE mark) over an in-memory committed log, on plaintext loopback gRPC, next to a minimal
//! Query snapshot stub (plain HTTP/1.1) that answers like Query for the same records. Bindings come from a bundle
//! compiled by `scripts/kn_gateway_bundle.py` (one ON_CHANGE OKX QUOTE binding and one STRICT_EVENT QUOTE binding
//! are picked); the consumer manifest, JWT keys and cursor secret are TEST_ONLY and persisted in PRIVATE_DIR (0600),
//! as is the log, so a restarted gateway serves the same offsets and accepts the writer's durable cursors.
//!
//! Data: one BBO per instrument at first start. The STRICT instrument gets a new BBO every second; the ON_CHANGE one
//! only when the control file `publish` appears (then quiet). A fake provider session record
//! (`session-liveness/<venue>-<market>/lane-1.json`, `qdl.provider-session-liveness.v1`) is rewritten every 250 ms,
//! LIVE unless the control file `disconnect` exists. Evidence lines (counts, no tokens) go to `gateway-events.jsonl`
//! and a metrics snapshot to `gateway-metrics.json` every second. Binds 127.0.0.1 only; SIGTERM stops it.

use base64::{engine::general_purpose::STANDARD, Engine};
use jsonwebtoken::{Algorithm, DecodingKey};
use prost::Message;
use qdl_contracts::cursor_v3::{
    CursorV3Claims, CursorV3Codec, CursorV3Expectation, DeliveryRequirement,
};
use qdl_stream_gateway::{
    auth::{AccessError, JwtConfig, RequestQuota},
    authority::{Authority, AuthorityHandle},
    bundle::{Binding, Bundle, Manifest, ManifestRequirement, Quotas},
    generated::{
        marketdata_v2::{event_envelope, EventEnvelope, Quote},
        query_v2 as query,
    },
    hub::{Hub, HubConfig, LogSource, RawRecord},
    readview::NotReadyReadView,
    replay::{RangeCursor, RangeError, RangeSource, ReplayCoordinator},
    service::{Gateway, GatewayState, StreamLimits},
    session_liveness::SessionLivenessReader,
    subscription::ByteBudget,
};
use query::market_data_stream_service_server::{
    MarketDataStreamService, MarketDataStreamServiceServer,
};
use ring::{
    rand::{SecureRandom, SystemRandom},
    signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING},
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::{Request, Response, Status};

const CONSUMER: &str = "test.v232-e2e.market-data";
const SUBJECT: &str = "spiffe://test/v232-e2e-writer";
const KEY_ID: &str = "v232-e2e";
const ISSUER: &str = "https://v232-e2e.test";
const AUDIENCE: &str = "qdl-v232-e2e";
const ENVIRONMENT: &str = "paper";
const STREAM: &str = "test.v232-e2e.canonical";
const SESSION_ID: &str = "TEST_ONLY-okx-bbo-lane-1";
const GENERATION: u64 = 7;
const CONFIG_REVISION: u64 = 5;
const BUDGET_MS: u64 = 2000;
const MAX_EVENT_LINES: u64 = 20_000;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn private_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temporary = path.with_extension("pending");
    let _ = fs::remove_file(&temporary);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(temporary, path)
}

/// One instrument of the e2e: a binding picked from the compiled bundle and its TEST identity.
#[derive(Clone)]
struct Instrument {
    binding: Binding,
    partition: i32,
    symbol: String,
    requirement: query::DataRequirement,
}

impl Instrument {
    fn instrument_id(&self) -> String {
        format!("OKX:PERPETUAL:{}", self.symbol)
    }
    fn provider(&self) -> String {
        format!(
            "{}-{}",
            self.binding.venue.to_lowercase(),
            self.binding.market.to_lowercase()
        )
    }
    fn source_id(&self) -> String {
        format!("{}-quote-primary", self.provider())
    }
}

fn requirement(binding: &Binding) -> query::DataRequirement {
    query::DataRequirement {
        instrument_uid: binding.instrument_uid.clone(),
        feed_type: query::FeedType::Quote as i32,
        grade: query::ConsumerGrade::Execution as i32,
        source_policy_id: binding.source_policy_id.clone(),
        max_freshness_ms: BUDGET_MS,
        max_session_liveness_ms: BUDGET_MS,
        event_recency_policy: query::StalePolicy::Observe as i32,
        stale_policy_type: query::StalePolicy::Block as i32,
        gap_policy_type: query::GapPolicy::Block as i32,
        recovery_policy: query::RecoveryPolicy::SnapshotAndReplay as i32,
        revision_policy: query::BarRevisionPolicy::EmitRevisions as i32,
        require_full_coverage: true,
        require_final_bars: false,
        ..Default::default()
    }
}

/// The committed log: per partition, encoded envelopes at offsets 1.., persisted so a restart serves them again.
struct Log {
    records: Mutex<Vec<Vec<Vec<u8>>>>,
    directory: PathBuf,
}

impl Log {
    fn open(directory: &Path, partitions: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let mut records = vec![Vec::new(); partitions];
        for (p, rows) in records.iter_mut().enumerate() {
            let path = directory.join(format!("log-{p}.b64"));
            if let Ok(text) = fs::read_to_string(&path) {
                for line in text.lines().filter(|l| !l.is_empty()) {
                    rows.push(STANDARD.decode(line)?);
                }
            }
        }
        Ok(Self {
            records: Mutex::new(records),
            directory: directory.to_path_buf(),
        })
    }
    fn len(&self, partition: i32) -> i64 {
        self.records.lock().unwrap()[partition as usize].len() as i64
    }
    fn append(&self, partition: i32, payload: Vec<u8>) -> i64 {
        let mut records = self.records.lock().unwrap();
        let path = self.directory.join(format!("log-{partition}.b64"));
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path)
            .expect("log file");
        writeln!(file, "{}", STANDARD.encode(&payload)).expect("log append");
        file.sync_all().expect("log sync");
        records[partition as usize].push(payload);
        records[partition as usize].len() as i64
    }
    fn get(&self, partition: i32, offset: i64) -> Option<Vec<u8>> {
        let records = self.records.lock().unwrap();
        let rows = records.get(usize::try_from(partition).ok()?)?;
        (offset >= 1)
            .then(|| rows.get(offset as usize - 1).cloned())
            .flatten()
    }
}

struct Keys(Vec<Vec<u8>>);
impl Keys {
    fn key(&self, partition: i32) -> Vec<u8> {
        self.0[partition as usize].clone()
    }
}

struct Live {
    log: Arc<Log>,
    keys: Arc<Keys>,
    positions: Vec<i64>,
}
impl LogSource for Live {
    fn poll(&mut self, timeout: Duration) -> Result<Option<RawRecord>, String> {
        let deadline = Instant::now() + timeout;
        loop {
            for p in 0..self.positions.len() {
                let offset = self.positions[p];
                if let Some(payload) = self.log.get(p as i32, offset) {
                    self.positions[p] += 1;
                    return Ok(Some(RawRecord {
                        partition: p as i32,
                        offset,
                        key: self.keys.key(p as i32),
                        payload,
                        timestamp_ms: 0,
                    }));
                }
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn positions(&self) -> Vec<(i32, i64)> {
        self.positions
            .iter()
            .enumerate()
            .map(|(p, &o)| (p as i32, o))
            .collect()
    }
    fn broker_heads(&self) -> Vec<(i32, i64, Instant)> {
        (0..self.positions.len())
            .map(|p| (p as i32, self.log.len(p as i32) + 1, Instant::now()))
            .collect()
    }
}

struct Ranges(Arc<Log>, Arc<Keys>);
struct Reader {
    log: Arc<Log>,
    keys: Arc<Keys>,
    partition: i32,
    position: i64,
}
impl RangeSource for Ranges {
    fn open(&self, partition: i32, from: i64) -> Result<Box<dyn RangeCursor>, RangeError> {
        Ok(Box::new(Reader {
            log: self.0.clone(),
            keys: self.1.clone(),
            partition,
            position: from.max(1),
        }))
    }
}
impl RangeCursor for Reader {
    fn next(&mut self, _: Duration) -> Result<Option<RawRecord>, RangeError> {
        let Some(payload) = self.log.get(self.partition, self.position) else {
            return Ok(None);
        };
        let record = RawRecord {
            partition: self.partition,
            offset: self.position,
            key: self.keys.key(self.partition),
            payload,
            timestamp_ms: 0,
        };
        self.position += 1;
        Ok(Some(record))
    }
    fn position(&self) -> Option<i64> {
        Some(self.position)
    }
}

struct FixtureQuota;
impl RequestQuota for FixtureQuota {
    fn consume(&self, _: &Manifest) -> Result<(), AccessError> {
        Ok(())
    }
}

/// Bounded evidence lines (counts only, never tokens or payloads).
struct Events {
    file: Mutex<fs::File>,
    lines: AtomicU64,
}
impl Events {
    fn write(&self, mut value: Value) {
        if self.lines.fetch_add(1, Ordering::Relaxed) >= MAX_EVENT_LINES {
            return;
        }
        value["at_ns"] = json!(now_ns());
        let mut file = self.file.lock().unwrap();
        let _ = writeln!(file, "{value}");
    }
}

fn decimal(text: &str) -> qdl_contracts::qdl::common::v1::DecimalValue {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let digits = format!("{whole}{fraction}");
    qdl_contracts::qdl::common::v1::DecimalValue {
        scale: fraction.len() as i32,
        source_text: text.into(),
        coefficient: Some(
            qdl_contracts::qdl::common::v1::decimal_value::Coefficient::Mantissa(
                digits.parse().unwrap(),
            ),
        ),
    }
}

fn envelope(instrument: &Instrument, sequence: u64) -> EventEnvelope {
    let now = now_ns();
    let bid = format!("{}.{:02}", 100 + sequence % 7, sequence % 100);
    let ask = format!("{}.{:02}", 101 + sequence % 7, sequence % 100);
    EventEnvelope {
        schema_name: "qdl.marketdata.v2.QuoteEvent".into(),
        schema_major: 2,
        event_id: format!("TEST_ONLY-{}-{sequence}", instrument.partition).into_bytes(),
        instrument_uid: instrument.binding.instrument_uid.clone(),
        instrument_id: instrument.instrument_id(),
        instrument_revision: 4,
        venue: instrument.binding.venue.clone(),
        market: instrument.binding.market.clone(),
        product_type: "PERPETUAL".into(),
        native_symbol: instrument.symbol.clone(),
        provider: instrument.provider(),
        source_id: instrument.source_id(),
        source_role: qdl_contracts::qdl::common::v1::SourceRole::Primary as i32,
        source_event_time_ns: now,
        received_at_ns: now,
        normalized_at_ns: now,
        published_at_ns: now,
        normalizer_version: "rust-v2".into(),
        adapter_version: "okx-v2".into(),
        config_revision: CONFIG_REVISION,
        source_session_id: SESSION_ID.into(),
        connection_generation: GENERATION,
        authority_revision: 3,
        payload: Some(event_envelope::Payload::Quote(Quote {
            bid_price: Some(decimal(&bid)),
            bid_quantity: Some(decimal("2")),
            ask_price: Some(decimal(&ask)),
            ask_quantity: Some(decimal("3")),
            level: 1,
            quantity_unit: qdl_contracts::qdl::common::v1::QuantityUnit::BaseAsset as i32,
        })),
        ..Default::default()
    }
}

fn decimal_json(value: &Option<qdl_contracts::qdl::common::v1::DecimalValue>) -> Value {
    let value = value.as_ref().unwrap();
    let coefficient = match &value.coefficient {
        Some(qdl_contracts::qdl::common::v1::decimal_value::Coefficient::Mantissa(m)) => {
            m.to_string()
        }
        Some(qdl_contracts::qdl::common::v1::decimal_value::Coefficient::MantissaText(t)) => {
            t.clone()
        }
        None => "0".into(),
    };
    json!({"coefficient": coefficient, "scale": value.scale, "source_text": value.source_text})
}

/// The fake provider session, as the ingestor lane writes it; `live` false = DISCONNECTED (transport frozen).
struct Session {
    root: PathBuf,
    last_transport_ns: AtomicU64,
}
impl Session {
    fn write(&self, venue: &str, market: &str, live: bool) {
        let now = now_ns();
        if live {
            self.last_transport_ns.store(now as u64, Ordering::Relaxed);
        }
        let record = json!({
            "schema": "qdl.provider-session-liveness.v1", "source_session_id": SESSION_ID,
            "connection_generation": GENERATION, "state": if live { "LIVE" } else { "DISCONNECTED" },
            "last_transport_at_ns": self.last_transport_ns.load(Ordering::Relaxed) as i64,
            "updated_at_ns": now, "config_revision": CONFIG_REVISION,
        });
        let directory = self.root.join(format!(
            "{}-{}",
            venue.to_lowercase(),
            market.to_lowercase()
        ));
        let _ = fs::create_dir_all(&directory);
        let _ = private_write(
            &directory.join("lane-1.json"),
            record.to_string().as_bytes(),
        );
    }
    fn liveness_ms(&self) -> i64 {
        (now_ns() - self.last_transport_ns.load(Ordering::Relaxed) as i64).max(0) / 1_000_000
    }
}

struct QueryStub {
    instruments: Vec<Instrument>,
    log: Arc<Log>,
    codec: CursorV3Codec,
    expectation: CursorV3Expectation,
    session: Arc<Session>,
    disconnect: PathBuf,
    events: Arc<Events>,
}

impl QueryStub {
    /// DL Query's snapshot for the newest record of `uid` (TS `view_from_query` / `validate_snapshot` shape).
    fn answer(&self, uid: &str) -> (u16, Value) {
        let Some(instrument) = self
            .instruments
            .iter()
            .find(|i| i.binding.instrument_uid == uid)
        else {
            return (
                404,
                json!({"code": "INSTRUMENT_NOT_FOUND", "retryable": false}),
            );
        };
        let offset = self.log.len(instrument.partition);
        let Some(payload) = self.log.get(instrument.partition, offset) else {
            return (
                503,
                json!({"code": "DATA_NOT_READY", "retryable": true, "detail": "no record"}),
            );
        };
        let envelope = EventEnvelope::decode(payload.as_slice()).unwrap();
        let Some(event_envelope::Payload::Quote(quote)) = &envelope.payload else {
            unreachable!()
        };
        let now = now_ns();
        let freshness_ms = (now - envelope.source_event_time_ns).max(0) / 1_000_000;
        let live = !self.disconnect.exists();
        let liveness_ms = self.session.liveness_ms();
        let event_stale = freshness_ms > BUDGET_MS as i64;
        let session_ok = live && liveness_ms <= BUDGET_MS as i64;
        let on_change = instrument.binding.delivery_on_change;
        let mut flags = Vec::new();
        if on_change {
            flags.push("DELIVERY_ON_CHANGE");
        }
        if event_stale {
            flags.push("LAST_EVENT_STALE");
        }
        if !live {
            flags.push("SOURCE_SESSION_DISCONNECTED");
        }
        // The quiet-session rule: an old ON_CHANGE BBO stays eligible while its session is proven LIVE.
        let eligible = session_ok && (!event_stale || on_change);
        let delivery = DeliveryRequirement::from_proto(&instrument.requirement).unwrap();
        let cursor = self
            .codec
            .encode(&CursorV3Claims {
                key_id: "e2e".into(),
                environment: ENVIRONMENT.into(),
                consumer_id: CONSUMER.into(),
                requirement_digest: delivery.digest(),
                schema_major: 2,
                stream: STREAM.into(),
                product_key: instrument.binding.product_key.clone(),
                snapshot_id: format!("e2e-{offset}"),
                source_topic_id: self.expectation.source_topic_id.clone(),
                source_partition: instrument.partition as u64,
                source_offset: offset as u64,
                partition_plan_epoch: 1,
                source_policy_revision: 1,
                catalog_revision: 1,
                route_generation: self.expectation.route_generation.clone(),
                issued_at_ns: now as u64,
                expires_at_ns: now as u64 + 900_000_000_000,
            })
            .unwrap();
        let view = json!({
            "contract": {"adapter_version": envelope.adapter_version, "authority_revision": envelope.authority_revision,
                "config_revision": envelope.config_revision, "contract_version": "2.0.0",
                "correlation_id": "TEST_ONLY-e2e-query", "instrument_catalog_revision": 1,
                "normalizer_version": envelope.normalizer_version, "schema_digest": "a".repeat(64),
                "source_policy_revision": 1},
            "cursor": cursor, "event_id": hex(&envelope.event_id), "feed": "QUOTE",
            "instrument_id": envelope.instrument_id, "instrument_revision": envelope.instrument_revision,
            "instrument_uid": envelope.instrument_uid, "interval": null,
            "observed_at_ns": envelope.source_event_time_ns,
            "payload": {"ask_price": decimal_json(&quote.ask_price), "ask_quantity": decimal_json(&quote.ask_quantity),
                "bid_price": decimal_json(&quote.bid_price), "bid_quantity": decimal_json(&quote.bid_quantity),
                "feed": "QUOTE", "level": quote.level, "quantity_unit": "BASE_ASSET"},
            "quality": {"complete": true, "event_recency_state": if event_stale { "STALE" } else { "LIVE" },
                "execution_eligible": eligible, "flags": flags, "freshness_ms": freshness_ms, "gap_open": false,
                "policy_id": instrument.binding.source_policy_id, "provider_session_liveness_ms": liveness_ms,
                "provider_session_state": if live { "LIVE" } else { "DISCONNECTED" },
                "state": if eligible { "LIVE" } else { "STALE" }},
            "received_at_ns": envelope.received_at_ns, "revision": 0, "snapshot_id": format!("e2e-{offset}"),
            "source": {"authoritative": true, "provider": envelope.provider, "source_id": envelope.source_id,
                "source_role": "PRIMARY", "venue": envelope.venue},
            "watermark_offset": offset,
        });
        (
            200,
            json!({"schema": "qdl.marketdata.snapshot.v2", "request_id": format!("e2e-{now}"), "data": view}),
        )
    }

    async fn serve(self: Arc<Self>, listener: tokio::net::TcpListener) {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let stub = self.clone();
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                loop {
                    let mut chunk = [0u8; 8192];
                    let Ok(read) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }
                    buffer.extend_from_slice(&chunk[..read]);
                    while let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buffer[..end]).to_string();
                        let length = head
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(str::to_owned)
                            })
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        if buffer.len() < end + 4 + length {
                            break;
                        }
                        buffer.drain(..end + 4 + length);
                        let path = head
                            .split_whitespace()
                            .nth(1)
                            .unwrap_or("")
                            .split('?')
                            .next()
                            .unwrap_or("");
                        let uid = path
                            .strip_prefix("/v2/market-data/")
                            .and_then(|rest| rest.strip_suffix("/snapshot"))
                            .unwrap_or("");
                        let (status, body) = stub.answer(uid);
                        stub.events
                            .write(json!({"kind": "query", "uid": uid, "status": status}));
                        let body = body.to_string();
                        let reason = if status == 200 { "OK" } else { "Error" };
                        let reply = format!(
                            "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        if socket.write_all(reply.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

/// Logs every Subscribe's opt-in fields (counts only), then delegates to the real service.
struct Logged {
    inner: Gateway,
    events: Arc<Events>,
    uids: BTreeMap<String, String>,
}

#[tonic::async_trait]
impl MarketDataStreamService for Logged {
    type SubscribeStream = <Gateway as MarketDataStreamService>::SubscribeStream;
    type ReplayStream = <Gateway as MarketDataStreamService>::ReplayStream;

    async fn subscribe(
        &self,
        request: Request<query::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let body = request.get_ref();
        let uid = body
            .requirement
            .as_ref()
            .map(|r| r.instrument_uid.clone())
            .unwrap_or_default();
        let (flag, interval) = (body.delivery_on_change_flag, body.session_alive_interval_ms);
        let result = self.inner.subscribe(request).await;
        self.events.write(json!({"kind": "subscribe", "uid": uid,
            "on_change_binding": self.uids.get(&uid),
            "delivery_on_change_flag": flag,
            "session_alive_interval_ms": interval,
            "status": result.as_ref().err().map(|s| s.message().split(':').next().unwrap_or("").to_owned())}));
        result
    }
    async fn replay(
        &self,
        request: Request<query::ReplayRequest>,
    ) -> Result<Response<Self::ReplayStream>, Status> {
        self.inner.replay(request).await
    }
    async fn get_snapshot(
        &self,
        request: Request<query::GetSnapshotRequest>,
    ) -> Result<Response<query::GetSnapshotResponse>, Status> {
        self.inner.get_snapshot(request).await
    }
    async fn get_feed_status(
        &self,
        request: Request<query::GetFeedStatusRequest>,
    ) -> Result<Response<query::GetFeedStatusResponse>, Status> {
        self.inner.get_feed_status(request).await
    }
}

fn persistent_secret(
    path: &Path,
    rng: &SystemRandom,
    len: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if let Ok(bytes) = fs::read(path) {
        return Ok(bytes);
    }
    let mut secret = vec![0; len];
    rng.fill(&mut secret).map_err(|_| "random")?;
    private_write(path, &secret)?;
    Ok(secret)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        return Err("PRIVATE_DIR BUNDLE QUERY_PORT GRPC_PORT".into());
    }
    let directory = PathBuf::from(&args[1]);
    let bundle_text = fs::read_to_string(&args[2])?;
    let query_port: u16 = args[3].parse()?;
    let grpc_port: u16 = args[4].parse()?;
    let compiled = Bundle::parse(&bundle_text)?;
    let pick = |on_change: bool| {
        compiled
            .bindings
            .iter()
            .filter(|b| {
                b.feed == "QUOTE" && b.interval.is_none() && b.delivery_on_change == on_change
            })
            .find(|b| b.venue == "OKX" && b.market == "SWAP")
            .or_else(|| {
                compiled.bindings.iter().find(|b| {
                    b.feed == "QUOTE" && b.interval.is_none() && b.delivery_on_change == on_change
                })
            })
            .cloned()
    };
    let on_change = pick(true).ok_or("bundle has no ON_CHANGE QUOTE binding")?;
    let strict = pick(false).ok_or("bundle has no STRICT_EVENT QUOTE binding")?;
    let instruments: Vec<Instrument> = [on_change, strict]
        .into_iter()
        .enumerate()
        .map(|(p, binding)| Instrument {
            partition: p as i32,
            symbol: if p == 0 {
                "E2E-ONCHANGE-USDT-SWAP".into()
            } else {
                "E2E-STRICT-USDT-SWAP".into()
            },
            requirement: requirement(&binding),
            binding,
        })
        .collect();

    let rng = SystemRandom::new();
    let secret = persistent_secret(&directory.join("cursor-secret.bin"), &rng, 32)?;
    let pkcs8 = match fs::read(directory.join("writer-jwt.der")) {
        Ok(bytes) => bytes,
        Err(_) => {
            let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
                .map_err(|_| "key generation")?;
            private_write(&directory.join("writer-jwt.der"), pkcs8.as_ref())?;
            pkcs8.as_ref().to_vec()
        }
    };
    let pem = format!(
        "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
        STANDARD.encode(&pkcs8)
    );
    private_write(&directory.join("writer-jwt.pem"), pem.as_bytes())?;
    let key = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &pkcs8, &rng)
        .map_err(|_| "key parse")?;
    let codec = CursorV3Codec::new(&BTreeMap::from([("e2e".into(), secret.clone())]), "e2e")?;
    let stub_codec = CursorV3Codec::new(&BTreeMap::from([("e2e".into(), secret)]), "e2e")?;
    let expectation = CursorV3Expectation {
        environment: ENVIRONMENT.into(),
        stream: STREAM.into(),
        source_topic_id: "v232-e2e-not-kafka".into(),
        partition_plan_epoch: 1,
        source_policy_revision: 1,
        catalog_revision: 1,
        route_generation: "v232-e2e".into(),
        schema_major: 2,
        catalog_revision_by_product: Default::default(),
    };
    let bundle = Bundle {
        sha256: "v232-e2e-test-only".into(),
        environment: ENVIRONMENT.into(),
        catalog_revision: 1,
        source_policy_revision: 1,
        canonical_stream: STREAM.into(),
        bindings: instruments
            .iter()
            .map(|i| Binding {
                lineage_revision: 1,
                ..i.binding.clone()
            })
            .collect(),
        manifests: vec![Manifest {
            consumer_id: CONSUMER.into(),
            subject: SUBJECT.into(),
            environment: ENVIRONMENT.into(),
            manifest_revision: 1,
            min_token_revision: 1,
            allowed_purposes: vec!["INTERNAL_EXECUTION".into()],
            allowed_permissions: vec![
                "stream:read".into(),
                "history:read".into(),
                "snapshot:read".into(),
            ],
            requirements: instruments
                .iter()
                .map(|i| ManifestRequirement {
                    instrument_uid: i.binding.instrument_uid.clone(),
                    feed: "QUOTE".into(),
                    interval: None,
                    consumer_grade: "EXECUTION".into(),
                    source_policy_id: i.binding.source_policy_id.clone(),
                    event_recency_policy: Some("OBSERVE".into()),
                    max_session_liveness_ms: Some(BUDGET_MS),
                })
                .collect(),
            quotas: Quotas {
                requests_per_minute: 100_000,
                max_batch_items: 100,
                max_warmup_rows: 5000,
                max_streams: 32,
                max_buffer_events: 1000,
            },
        }],
    };
    let authority = AuthorityHandle::new(Authority {
        bundle,
        expectation: expectation.clone(),
        jwt: JwtConfig {
            environment: ENVIRONMENT.into(),
            issuer: ISSUER.into(),
            audience: AUDIENCE.into(),
            keys: BTreeMap::from([(
                KEY_ID.into(),
                (
                    DecodingKey::from_ec_der(key.public_key().as_ref()),
                    vec![Algorithm::ES256],
                ),
            )]),
            algorithms: vec![Algorithm::ES256],
            max_lifetime_seconds: 900,
            environments_by_key_id: BTreeMap::new(),
            subjects_by_key_id: BTreeMap::from([(KEY_ID.into(), SUBJECT.into())]),
        },
    });

    let log = Arc::new(Log::open(&directory, instruments.len())?);
    let keys = Arc::new(Keys(
        instruments
            .iter()
            .map(|i| i.binding.physical_key.clone().into_bytes())
            .collect(),
    ));
    let sequence = AtomicU64::new(
        instruments
            .iter()
            .map(|i| log.len(i.partition) as u64)
            .sum(),
    );
    for instrument in &instruments {
        if log.len(instrument.partition) == 0 {
            let s = sequence.fetch_add(1, Ordering::Relaxed);
            log.append(
                instrument.partition,
                envelope(instrument, s).encode_to_vec(),
            );
        }
    }
    let session_root = directory.join("session-liveness");
    let session = Arc::new(Session {
        root: session_root.clone(),
        last_transport_ns: AtomicU64::new(now_ns() as u64),
    });
    let disconnect = directory.join("disconnect");
    session.write(
        &instruments[0].binding.venue,
        &instruments[0].binding.market,
        !disconnect.exists(),
    );
    let events = Arc::new(Events {
        file: Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .open(directory.join("gateway-events.jsonl"))?,
        ),
        lines: AtomicU64::new(0),
    });

    let starts: Vec<_> = instruments.iter().map(|i| (i.partition, 1)).collect();
    let hub = Arc::new(Hub::new(
        &starts,
        HubConfig {
            // QDL_E2E_RING_BYTES (DL-DEF-2026-11-26-M): a tiny ring evicts the quiet ON_CHANGE BBO at once,
            // so its hot view is answered from the last-value index (`hot_on_change` in gateway-metrics.json).
            ring_max_bytes: std::env::var("QDL_E2E_RING_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(8 << 20),
            ring_max_age: Duration::from_secs(600),
            // QDL_E2E_LAST_VALUE_MAX_KEYS=0 reproduces the pre-DL-DEF-M ring-only lookup (HOT_RECORD_MISSING).
            last_value_max_keys: std::env::var("QDL_E2E_LAST_VALUE_MAX_KEYS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(qdl_stream_gateway::hub::DEFAULT_LAST_VALUE_MAX_KEYS),
            ..Default::default()
        },
    ));
    let reader = hub.run(Box::new(Live {
        log: log.clone(),
        keys: keys.clone(),
        positions: vec![1; instruments.len()],
    }));
    let limits = StreamLimits::default();
    let state = Arc::new(
        GatewayState::new(
            authority,
            Arc::new(FixtureQuota),
            codec,
            hub.clone(),
            ReplayCoordinator::new(
                Arc::new(Ranges(log.clone(), keys.clone())),
                2,
                limits.replay.clone(),
                ByteBudget::new(32 << 20),
            ),
            Arc::new(NotReadyReadView),
            ByteBudget::live(32 << 20),
            limits,
        )
        .with_session_liveness(Arc::new(SessionLivenessReader::new(session_root))),
    );

    let stub = Arc::new(QueryStub {
        instruments: instruments.clone(),
        log: log.clone(),
        codec: stub_codec,
        expectation,
        session: session.clone(),
        disconnect: disconnect.clone(),
        events: events.clone(),
    });
    let query_listener = tokio::net::TcpListener::bind(("127.0.0.1", query_port)).await?;
    tokio::spawn(stub.serve(query_listener));
    let grpc_listener = tokio::net::TcpListener::bind(("127.0.0.1", grpc_port)).await?;

    // Background: session record (250 ms), STRICT BBOs (1 s), ON_CHANGE BBO on `publish`, metrics (1 s).
    {
        let (instruments, log, session, directory, state, hub) = (
            instruments.clone(),
            log.clone(),
            session.clone(),
            directory.clone(),
            state.clone(),
            hub.clone(),
        );
        tokio::spawn(async move {
            let mut tick = 0u64;
            loop {
                tokio::time::sleep(Duration::from_millis(250)).await;
                tick += 1;
                session.write(
                    &instruments[0].binding.venue,
                    &instruments[0].binding.market,
                    !directory.join("disconnect").exists(),
                );
                if tick % 4 == 0 {
                    let s = sequence.fetch_add(1, Ordering::Relaxed);
                    log.append(
                        instruments[1].partition,
                        envelope(&instruments[1], s).encode_to_vec(),
                    );
                    let m = &state.metrics;
                    let snapshot = json!({"at_ns": now_ns(), "session_alive_sent": m.session_alive_sent.load(Ordering::Relaxed),
                    "session_alive_dropped": m.session_alive_dropped.load(Ordering::Relaxed),
                    "delivered": m.delivered.load(Ordering::Relaxed), "replayed": m.replayed.load(Ordering::Relaxed),
                    "refused": m.refused.load(Ordering::Relaxed),
                    "log": instruments.iter().map(|i| log.len(i.partition)).collect::<Vec<_>>(),
                    "ring_bytes": hub.ring_bytes(), "last_value": hub.last_value_usage(),
                    "last_value_hits": hub.metrics.last_value_hits.load(Ordering::Relaxed),
                    "hot_on_change": match hub.latest_hot(instruments[0].binding.physical_key.as_bytes(), "QUOTE",
                        Instant::now() + Duration::from_millis(50)) {
                        Ok(hot) => json!({"ok": true, "offset": hot.raw.offset, "boundary": hot.boundary_offset}),
                        Err(error) => json!({"ok": false, "error": error}),
                    }});
                    let _ = private_write(
                        &directory.join("gateway-metrics.json"),
                        snapshot.to_string().as_bytes(),
                    );
                }
                let publish = directory.join("publish");
                if publish.exists() {
                    let s = sequence.fetch_add(1, Ordering::Relaxed);
                    log.append(
                        instruments[0].partition,
                        envelope(&instruments[0], s).encode_to_vec(),
                    );
                    let _ = fs::remove_file(publish);
                }
            }
        });
    }

    let ready = json!({
        "schema": "qdl.v232-e2e-gateway.ready.v1", "test_only": true,
        "grpc_target": format!("http://127.0.0.1:{grpc_port}"), "query_url": format!("http://127.0.0.1:{query_port}"),
        "consumer_id": CONSUMER,
        "jwt": {"private_key_file": "writer-jwt.pem", "key_id": KEY_ID, "algorithm": "ES256", "issuer": ISSUER,
            "audience": AUDIENCE, "subject": SUBJECT, "environment": ENVIRONMENT,
            "roles": ["stream_consumer", "historical_reader", "market_data_reader"], "venues": ["OKX"],
            "consumer_manifest_revision": 1},
        "instruments": instruments.iter().map(|i| json!({
            "instrument_uid": i.binding.instrument_uid, "instrument_id": i.instrument_id(), "venue": i.binding.venue,
            "market": i.binding.market, "product_type": "PERPETUAL", "symbol": i.symbol,
            "canonical_symbol": i.symbol.trim_end_matches("-SWAP"), "provider": i.provider(), "source_id": i.source_id(),
            "source_policy_id": i.binding.source_policy_id, "instrument_revision": 4,
            "binding_id": i.binding.binding_id, "delivery_on_change": i.binding.delivery_on_change,
            "requirement_proto_b64": STANDARD.encode(i.requirement.encode_to_vec())})).collect::<Vec<_>>(),
        "session": {"source_session_id": SESSION_ID, "connection_generation": GENERATION, "config_revision": CONFIG_REVISION},
        "bundle_source": args[2],
    });
    private_write(
        &directory.join("ready.json"),
        serde_json::to_vec_pretty(&ready)?.as_slice(),
    )?;

    let uids = instruments
        .iter()
        .map(|i| {
            (
                i.binding.instrument_uid.clone(),
                if i.binding.delivery_on_change {
                    "ON_CHANGE"
                } else {
                    "STRICT_EVENT"
                }
                .to_owned(),
            )
        })
        .collect();
    let service = Logged {
        inner: Gateway {
            state: state.clone(),
        },
        events,
        uids,
    };
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        loop {
            let result = grpc_listener.accept().await.map(|(s, _)| s);
            if tx.send(result).await.is_err() {
                break;
            }
        }
    });
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let stopping = state.clone();
    tonic::transport::Server::builder()
        .add_service(MarketDataStreamServiceServer::new(service))
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::ReceiverStream::new(rx),
            async move {
                term.recv().await;
                stopping.shut_down();
            },
        )
        .await?;
    hub.stop();
    reader.join().map_err(|_| "reader panicked")?;
    Ok(())
}
