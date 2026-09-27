//! KN-2 contract tests of the native Stream service over an in-memory
//! committed log (guide 18.9, K2-T01..T07).
//!
//! The service trait is called in-process (no network): these tests pin the
//! hub/barrier/replay/queue/lifecycle/access semantics exactly and fast. The
//! same binary is exercised over real Kafka, mTLS and the Python SDK by the
//! isolated KN-2 run (`scripts/kn_native_slice_probe.py`), which is where the
//! transport-level evidence lives. The log simulates what `read_committed`
//! Kafka shows a reader: offset gaps from transaction markers and aborted
//! batches, duplicate transport deliveries and a retention floor.

use jsonwebtoken::{encode, Algorithm, DecodingKey, EncodingKey, Header};
use prost::Message as _;
use qdl_contracts::cursor_v3::{
    CursorV3Claims, CursorV3Codec, CursorV3Expectation, DeliveryRequirement,
};
use qdl_stream_gateway::auth::{AccessError, JwtConfig, RequestQuota};
use qdl_stream_gateway::authority::{Authority, AuthorityHandle};
use qdl_stream_gateway::bundle::{Binding, Bundle, Manifest, ManifestRequirement, Quotas};
use qdl_stream_gateway::generated::marketdata_v2::{
    event_envelope, Bar, BarLifecycle, EventEnvelope, OrderBookDelta, OrderBookSnapshot, Quote,
    Trade,
};
use qdl_stream_gateway::generated::query_v2 as query;
use qdl_stream_gateway::generated::query_v2::market_data_stream_service_server::MarketDataStreamService;
use qdl_stream_gateway::hub::{Hub, HubConfig, LogSource, RawRecord};
use qdl_stream_gateway::readview::{NotReadyReadView, ReadView, ReadViewError};
use qdl_stream_gateway::replay::{
    RangeCursor, RangeError, RangeSource, ReplayCoordinator, ReplayEnd, ReplayLimits,
    ReplayRequest, Replayed,
};
use qdl_stream_gateway::requirement::StreamRequirement;
use qdl_stream_gateway::service::{Gateway, GatewayState, StreamLimits};
use qdl_stream_gateway::subscription::ByteBudget;
use ring::rand::SystemRandom;
use ring::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_stream::StreamExt;
use tonic::{Code, Request, Status};

// ------------------------------------------------------------------ the log

#[derive(Default)]
struct LogInner {
    records: BTreeMap<i32, Vec<RawRecord>>,
    next: BTreeMap<i32, i64>,
    low: BTreeMap<i32, i64>,
}

#[derive(Clone, Default)]
struct Log(Arc<Mutex<LogInner>>);

impl Log {
    fn new(partitions: &[i32]) -> Self {
        let log = Log::default();
        {
            let mut inner = log.0.lock().unwrap();
            for &partition in partitions {
                inner.records.insert(partition, Vec::new());
                inner.next.insert(partition, 0);
                inner.low.insert(partition, 0);
            }
        }
        log
    }

    fn append(&self, partition: i32, key: &str, envelope: &EventEnvelope) -> i64 {
        let mut inner = self.0.lock().unwrap();
        let offset = inner.next[&partition];
        inner.next.insert(partition, offset + 1);
        inner.records.get_mut(&partition).unwrap().push(RawRecord {
            partition,
            offset,
            key: key.as_bytes().to_vec(),
            payload: envelope.encode_to_vec(),
            timestamp_ms: 0,
        });
        offset
    }

    /// A committed record whose payload is not a canonical envelope.
    fn append_corrupt(&self, partition: i32, key: &str) -> i64 {
        let mut inner = self.0.lock().unwrap();
        let offset = inner.next[&partition];
        inner.next.insert(partition, offset + 1);
        inner.records.get_mut(&partition).unwrap().push(RawRecord {
            partition,
            offset,
            key: key.as_bytes().to_vec(),
            payload: vec![0xff],
            timestamp_ms: 0,
        });
        offset
    }

    /// A transaction marker or an aborted batch: offsets a committed reader
    /// never sees as records.
    fn gap(&self, partition: i32, width: i64) {
        let mut inner = self.0.lock().unwrap();
        let next = inner.next[&partition] + width;
        inner.next.insert(partition, next);
    }

    fn set_low(&self, partition: i32, low: i64) {
        self.0.lock().unwrap().low.insert(partition, low);
    }

    fn end(&self, partition: i32) -> i64 {
        self.0.lock().unwrap().next[&partition]
    }

    fn records_of(&self, key: &str, after: i64) -> Vec<i64> {
        let inner = self.0.lock().unwrap();
        inner
            .records
            .values()
            .flatten()
            .filter(|record| record.key == key.as_bytes() && record.offset > after)
            .map(|record| record.offset)
            .collect()
    }

    /// The log end if no record at or after `position` exists, read under one
    /// lock: a reader past the last record has also passed trailing markers.
    fn end_if_caught_up(&self, partition: i32, position: i64) -> Option<i64> {
        let inner = self.0.lock().unwrap();
        let pending = inner.records[&partition]
            .iter()
            .any(|record| record.offset >= position);
        (!pending).then(|| inner.next[&partition])
    }

    fn first_at_or_after(&self, partition: i32, from: i64) -> Option<RawRecord> {
        let inner = self.0.lock().unwrap();
        inner.records[&partition]
            .iter()
            .find(|record| record.offset >= from)
            .cloned()
    }
}

/// The live reader: every partition, delivering committed records in
/// offset order; may be paused (a lagging replica) and may re-deliver the
/// previous record (duplicate transport delivery).
struct LiveSource {
    log: Log,
    positions: BTreeMap<i32, i64>,
    paused: Arc<AtomicBool>,
    duplicate_every: Option<u64>,
    polls: u64,
    last: Option<RawRecord>,
}

impl LiveSource {
    fn new(log: &Log, starts: &[(i32, i64)], paused: Arc<AtomicBool>) -> Self {
        Self {
            log: log.clone(),
            positions: starts.iter().copied().collect(),
            paused,
            duplicate_every: None,
            polls: 0,
            last: None,
        }
    }
}

impl LogSource for LiveSource {
    fn poll(&mut self, timeout: Duration) -> Result<Option<RawRecord>, String> {
        let deadline = Instant::now() + timeout;
        loop {
            if !self.paused.load(Ordering::Relaxed) {
                self.polls += 1;
                if let (Some(every), Some(last)) = (self.duplicate_every, &self.last) {
                    if self.polls % every == 0 {
                        return Ok(Some(last.clone()));
                    }
                }
                for (&partition, position) in self.positions.iter_mut() {
                    if let Some(record) = self.log.first_at_or_after(partition, *position) {
                        *position = record.offset + 1;
                        self.last = Some(record.clone());
                        return Ok(Some(record));
                    }
                }
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn positions(&self) -> Vec<(i32, i64)> {
        if self.paused.load(Ordering::Relaxed) {
            return self.positions.iter().map(|(&p, &o)| (p, o)).collect();
        }
        self.positions
            .keys()
            .map(|&partition| {
                let position = self.positions[&partition];
                let end = self.log.end_if_caught_up(partition, position);
                (partition, end.unwrap_or(position))
            })
            .collect()
    }
}

struct Range {
    log: Log,
    /// Per-record read time, so a pass takes time as it does on a broker.
    delay: Duration,
}

struct RangeReader {
    log: Log,
    partition: i32,
    position: i64,
    delay: Duration,
}

impl RangeSource for Range {
    fn open(&self, partition: i32, from: i64) -> Result<Box<dyn RangeCursor>, RangeError> {
        let low = self.log.0.lock().unwrap().low[&partition];
        if from < low {
            return Err(RangeError::Retention);
        }
        Ok(Box::new(RangeReader {
            log: self.log.clone(),
            partition,
            position: from,
            delay: self.delay,
        }))
    }
}

impl RangeCursor for RangeReader {
    fn next(&mut self, _timeout: Duration) -> Result<Option<RawRecord>, RangeError> {
        if !self.delay.is_zero() {
            std::thread::sleep(self.delay);
        }
        match self.log.first_at_or_after(self.partition, self.position) {
            Some(record) => {
                self.position = record.offset + 1;
                Ok(Some(record))
            }
            None => {
                self.position = self.position.max(self.log.end(self.partition));
                Ok(None)
            }
        }
    }

    fn position(&self) -> Option<i64> {
        Some(self.position)
    }
}

// ------------------------------------------------------------ the products

const UID: &str = "fb26214c-7b9b-5961-95b2-55154755af0f";
const CONSUMER: &str = "alpha.okx.paper.test";
const SUBJECT: &str = "spiffe://test/alpha";
const TRADE_KEY: &str = "fb26214c/trade/okx-test";
const QUOTE_KEY: &str = "fb26214c/quote/okx-test";
const BOOK_KEY: &str = "fb26214c/book/okx-test";
const BAR_KEY: &str = "fb26214c/bar/okx-test-1m";

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn envelope(payload: event_envelope::Payload) -> EventEnvelope {
    EventEnvelope {
        instrument_uid: UID.into(),
        source_event_time_ns: now_ns(),
        published_at_ns: now_ns(),
        payload: Some(payload),
        ..Default::default()
    }
}

fn trade(id: u64) -> EventEnvelope {
    envelope(event_envelope::Payload::Trade(Trade {
        native_trade_id: id.to_string(),
        ..Default::default()
    }))
}

fn quote(level: u32, flags: Vec<i32>) -> EventEnvelope {
    let mut value = envelope(event_envelope::Payload::Quote(Quote {
        level,
        ..Default::default()
    }));
    value.quality_flags = flags;
    value
}

fn snapshot(sequence: u64) -> EventEnvelope {
    envelope(event_envelope::Payload::BookSnapshot(OrderBookSnapshot {
        native_sequence: sequence.to_string(),
        ..Default::default()
    }))
}

fn delta(sequence: u64, reset: bool) -> EventEnvelope {
    envelope(event_envelope::Payload::BookDelta(OrderBookDelta {
        native_sequence_end: sequence.to_string(),
        reset,
        ..Default::default()
    }))
}

fn bar(open_minute: i64, lifecycle: BarLifecycle, interval: &str) -> EventEnvelope {
    envelope(event_envelope::Payload::Bar(Bar {
        interval: interval.into(),
        open_time_ns: open_minute * 60_000_000_000,
        close_time_ns: (open_minute + 1) * 60_000_000_000 - 1,
        lifecycle: lifecycle as i32,
        ..Default::default()
    }))
}

fn binding(feed: &str, interval: Option<&str>, key: &str) -> Binding {
    let qualifier = interval.unwrap_or("-");
    Binding {
        binding_id: format!("okx-test-{}", feed.to_lowercase()),
        instrument_uid: UID.into(),
        venue: "OKX".into(),
        market: "SWAP".into(),
        feed: feed.into(),
        interval: interval.map(str::to_owned),
        source_policy_id: "crypto_primary_v2".into(),
        physical_key: key.into(),
        product_key: format!("lpk1|paper|OKX|SWAP|{UID}|{feed}|{qualifier}"),
        stale_after_ms: 5_000,
    }
}

fn manifest_requirement(feed: &str, interval: Option<&str>) -> ManifestRequirement {
    ManifestRequirement {
        instrument_uid: UID.into(),
        feed: feed.into(),
        interval: interval.map(str::to_owned),
        consumer_grade: "ALPHA".into(),
        source_policy_id: "crypto_primary_v2".into(),
        event_recency_policy: None,
        max_session_liveness_ms: None,
    }
}

fn bundle(revision: u64, permissions: &[&str], feeds: &[(&str, Option<&str>)]) -> Bundle {
    bundle_with_streams(revision, permissions, feeds, 64)
}

fn bundle_with_streams(
    revision: u64,
    permissions: &[&str],
    feeds: &[(&str, Option<&str>)],
    max_streams: u64,
) -> Bundle {
    Bundle {
        sha256: format!("test-bundle-{revision}"),
        environment: "paper".into(),
        catalog_revision: 9,
        source_policy_revision: 1,
        canonical_stream: "md.canonical.v2".into(),
        bindings: vec![
            binding("TRADE", None, TRADE_KEY),
            binding("QUOTE", None, QUOTE_KEY),
            binding("BOOK_SNAPSHOT", None, BOOK_KEY),
            binding("BOOK_DELTA", None, BOOK_KEY),
            binding("BAR", Some("1m"), BAR_KEY),
        ],
        manifests: vec![Manifest {
            consumer_id: CONSUMER.into(),
            subject: SUBJECT.into(),
            environment: "paper".into(),
            manifest_revision: revision,
            allowed_purposes: vec!["INTERNAL_ALPHA".into()],
            allowed_permissions: permissions
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            quotas: Quotas {
                requests_per_minute: 1_000_000,
                max_batch_items: 10,
                max_warmup_rows: 5_000,
                max_streams,
                max_buffer_events: 1_000,
            },
            requirements: feeds
                .iter()
                .map(|(feed, interval)| manifest_requirement(feed, *interval))
                .collect(),
        }],
    }
}

const ALL_FEEDS: [(&str, Option<&str>); 5] = [
    ("TRADE", None),
    ("QUOTE", None),
    ("BOOK_SNAPSHOT", None),
    ("BOOK_DELTA", None),
    ("BAR", Some("1m")),
];
const ALL_PERMISSIONS: [&str; 4] = [
    "stream:read",
    "snapshot:read",
    "status:read",
    "history:read",
];

fn requirement(feed: &str, interval: Option<&str>) -> query::DataRequirement {
    query::DataRequirement {
        instrument_uid: UID.into(),
        interval: interval.unwrap_or_default().into(),
        source_policy_id: "crypto_primary_v2".into(),
        require_full_coverage: true,
        require_final_bars: true,
        feed_type: query::FeedType::from_str_name(&format!("FEED_TYPE_{feed}")).unwrap() as i32,
        grade: query::ConsumerGrade::Alpha as i32,
        stale_policy_type: query::StalePolicy::Block as i32,
        gap_policy_type: query::GapPolicy::Block as i32,
        recovery_policy: query::RecoveryPolicy::SnapshotAndReplay as i32,
        revision_policy: query::BarRevisionPolicy::Latest as i32,
        ..Default::default()
    }
}

// --------------------------------------------------------------- identity

struct Keys {
    encoding: EncodingKey,
    decoding: DecodingKey,
}

fn keypair() -> Keys {
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let pair =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    Keys {
        encoding: EncodingKey::from_ec_der(pkcs8.as_ref()),
        decoding: DecodingKey::from_ec_der(pair.public_key().as_ref()),
    }
}

fn jwt_config(keys: &[(&str, &Keys)]) -> JwtConfig {
    JwtConfig {
        environment: "paper".into(),
        issuer: "https://identity.test".into(),
        audience: "qdl-v2-stable".into(),
        keys: keys
            .iter()
            .map(|(kid, key)| {
                (
                    (*kid).to_owned(),
                    (key.decoding.clone(), vec![Algorithm::ES256]),
                )
            })
            .collect(),
        algorithms: vec![Algorithm::ES256],
        max_lifetime_seconds: 900,
        subjects_by_key_id: keys
            .iter()
            .map(|(kid, _)| ((*kid).to_owned(), SUBJECT.to_owned()))
            .collect(),
    }
}

fn expectation() -> CursorV3Expectation {
    CursorV3Expectation {
        environment: "paper".into(),
        stream: "md.canonical.v2".into(),
        source_topic_id: "kn2TestTopicId00000000".into(),
        partition_plan_epoch: 1,
        source_policy_revision: 1,
        catalog_revision: 9,
        route_generation: "kn2-test-r1".into(),
        schema_major: 2,
    }
}

fn bearer(key: &Keys, kid: &str, roles: &[&str], revision: u64) -> String {
    let now = now_ns() / 1_000_000_000;
    let claims = serde_json::json!({
        "sub": SUBJECT, "iss": "https://identity.test", "aud": "qdl-v2-stable",
        "iat": now, "exp": now + 300, "jti": format!("j-{}", now_ns()), "environment": "paper",
        "roles": roles, "consumer_manifest_revision": revision
    });
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(kid.into());
    format!(
        "Bearer {}",
        encode(&header, &claims, &key.encoding).unwrap()
    )
}

struct NoQuota;

impl RequestQuota for NoQuota {
    fn consume(&self, _manifest: &Manifest) -> Result<(), AccessError> {
        Ok(())
    }
}

// ---------------------------------------------------------------- harness

struct Harness {
    log: Log,
    gateway: Gateway,
    hub: Arc<Hub>,
    paused: Arc<AtomicBool>,
    codec: CursorV3Codec,
    keys: Keys,
    reader: Option<std::thread::JoinHandle<()>>,
}

struct Options {
    ring_max_bytes: usize,
    limits: StreamLimits,
    budget: usize,
    replay_readers: usize,
    read_view: Arc<dyn ReadView>,
    permissions: Vec<&'static str>,
    duplicate_every: Option<u64>,
    log: Option<Log>,
    max_streams: u64,
    range_delay: Duration,
    /// Replay budget when it differs from the live one.
    replay_budget: Option<usize>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            ring_max_bytes: 32 << 20,
            limits: StreamLimits {
                catchup_deadline: Duration::from_millis(400),
                slow_consumer_after: Duration::from_secs(30),
                ..StreamLimits::default()
            },
            budget: 64 << 20,
            replay_readers: 4,
            read_view: Arc::new(NotReadyReadView),
            permissions: ALL_PERMISSIONS.to_vec(),
            duplicate_every: None,
            log: None,
            max_streams: 64,
            range_delay: Duration::ZERO,
            replay_budget: None,
        }
    }
}

const PARTITIONS: [i32; 2] = [0, 1];

impl Harness {
    fn new(options: Options) -> Self {
        let log = options.log.clone().unwrap_or_else(|| Log::new(&PARTITIONS));
        // Offset 0 of every partition exists so a cursor can name it.
        if options.log.is_none() {
            for partition in PARTITIONS {
                log.append(partition, "seed", &trade(0));
            }
        }
        let starts: Vec<(i32, i64)> = PARTITIONS.iter().map(|&p| (p, log.end(p))).collect();
        let hub = Arc::new(Hub::new(
            &starts,
            HubConfig {
                ring_max_bytes: options.ring_max_bytes,
                ring_max_age: Duration::from_secs(600),
            },
        ));
        let paused = Arc::new(AtomicBool::new(false));
        let mut source = LiveSource::new(&log, &starts, paused.clone());
        source.duplicate_every = options.duplicate_every;
        let reader = hub.run(Box::new(source));
        let keys = keypair();
        let codec = CursorV3Codec::new(
            &BTreeMap::from([("kn2-test-k1".to_owned(), vec![7u8; 32])]),
            "kn2-test-k1",
        )
        .unwrap();
        let authority = AuthorityHandle::new(Authority {
            jwt: jwt_config(&[("k1", &keys)]),
            bundle: bundle_with_streams(3, &options.permissions, &ALL_FEEDS, options.max_streams),
            expectation: expectation(),
        });
        let budget = ByteBudget::new(options.budget);
        let state = Arc::new(GatewayState::new(
            authority,
            Arc::new(NoQuota),
            CursorV3Codec::new(
                &BTreeMap::from([("kn2-test-k1".to_owned(), vec![7u8; 32])]),
                "kn2-test-k1",
            )
            .unwrap(),
            hub.clone(),
            ReplayCoordinator::new(
                Arc::new(Range {
                    log: log.clone(),
                    delay: options.range_delay,
                }),
                options.replay_readers,
                options.limits.replay.clone(),
                // Replay has its own budget (the live size unless set).
                ByteBudget::new(options.replay_budget.unwrap_or(options.budget)),
            ),
            options.read_view,
            budget,
            options.limits,
        ));
        Self {
            log,
            gateway: Gateway { state },
            hub,
            paused,
            codec,
            keys,
            reader: Some(reader),
        }
    }

    fn state(&self) -> &Arc<GatewayState> {
        &self.gateway.state
    }

    fn cursor(&self, feed: &str, interval: Option<&str>, partition: i32, after: i64) -> String {
        let digest = DeliveryRequirement::from_proto(&requirement(feed, interval))
            .unwrap()
            .digest();
        let qualifier = interval.unwrap_or("-");
        let now = now_ns() as u64;
        let expected = expectation();
        self.codec
            .encode(&CursorV3Claims {
                key_id: "kn2-test-k1".into(),
                environment: "paper".into(),
                consumer_id: CONSUMER.into(),
                requirement_digest: digest,
                schema_major: 2,
                stream: expected.stream,
                product_key: format!("lpk1|paper|OKX|SWAP|{UID}|{feed}|{qualifier}"),
                snapshot_id: "kn2-test-snapshot".into(),
                source_topic_id: expected.source_topic_id,
                source_partition: partition as u64,
                source_offset: after as u64,
                partition_plan_epoch: 1,
                source_policy_revision: 1,
                catalog_revision: 9,
                route_generation: expected.route_generation,
                issued_at_ns: now,
                expires_at_ns: now + 3_600_000_000_000,
            })
            .unwrap()
    }

    fn authorized<T>(&self, body: T) -> Request<T> {
        self.request(
            body,
            &bearer(
                &self.keys,
                "k1",
                &["stream_consumer", "market_data_reader", "historical_reader"],
                3,
            ),
        )
    }

    fn request<T>(&self, body: T, bearer: &str) -> Request<T> {
        let mut request = Request::new(body);
        let metadata = request.metadata_mut();
        metadata.insert("authorization", bearer.parse().unwrap());
        metadata.insert("x-qdl-consumer-id", CONSUMER.parse().unwrap());
        metadata.insert("x-qdl-purpose", "INTERNAL_ALPHA".parse().unwrap());
        request
    }

    async fn subscribe(
        &self,
        feed: &str,
        interval: Option<&str>,
        after: i64,
        buffer: u32,
    ) -> Result<Stream, Status> {
        let partition = 0;
        let request = self.authorized(query::SubscribeRequest {
            consumer_id: CONSUMER.into(),
            requirement: Some(requirement(feed, interval)),
            cursor_token: self.cursor(feed, interval, partition, after),
            max_buffer_events: buffer,
        });
        let response = self.gateway.subscribe(request).await?;
        Ok(Stream(response.into_inner()))
    }

    /// Wait until every stream slot is released (tasks noticed the client left).
    async fn released(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.state().open_streams() != 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(self.state().open_streams(), 0, "stream slots released");
    }

    fn settle(&self) {
        // Let the live reader dispatch what was appended.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let behind = PARTITIONS
                .iter()
                .any(|&p| self.hub.next_offset(p).unwrap_or(0) < self.log.end(p));
            if !behind {
                return;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.hub.stop();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

struct Stream(
    std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<query::SubscribeResponse, Status>> + Send>,
    >,
);

#[derive(Debug)]
enum Item {
    Control(String),
    Event(u64, Box<EventEnvelope>),
    Error(Code, String),
    End,
}

impl Stream {
    async fn next(&mut self) -> Item {
        match tokio::time::timeout(Duration::from_secs(5), self.0.next()).await {
            Err(_) => Item::End,
            Ok(None) => Item::End,
            Ok(Some(Err(status))) => Item::Error(status.code(), status.message().to_owned()),
            Ok(Some(Ok(response))) => {
                let record = response.record.unwrap();
                match record.payload.unwrap() {
                    query::stream_record::Payload::Control(control) => Item::Control(control.code),
                    query::stream_record::Payload::Event(event) => {
                        Item::Event(record.logical_offset, Box::new(event))
                    }
                }
            }
        }
    }

    /// Controls and offsets until `count` events arrived (or the stream ends).
    async fn collect(&mut self, count: usize) -> (Vec<String>, Vec<u64>) {
        let (mut controls, mut offsets) = (Vec::new(), Vec::new());
        while offsets.len() < count {
            match self.next().await {
                Item::Control(code) => controls.push(code),
                Item::Event(offset, _) => offsets.push(offset),
                Item::Error(code, message) => {
                    controls.push(format!("{code:?}:{message}"));
                    break;
                }
                Item::End => break,
            }
        }
        (controls, offsets)
    }

    async fn until_live(&mut self) -> Vec<u64> {
        let mut offsets = Vec::new();
        loop {
            match self.next().await {
                Item::Control(code) if code == "LIVE" => return offsets,
                Item::Control(_) => {}
                Item::Event(offset, _) => offsets.push(offset),
                other => panic!("stream ended before LIVE: {other:?}"),
            }
        }
    }

    async fn error(&mut self) -> (Code, String) {
        loop {
            match self.next().await {
                Item::Error(code, message) => return (code, message),
                Item::End => panic!("stream ended without a status"),
                _ => {}
            }
        }
    }
}

fn as_offsets(values: &[i64]) -> Vec<u64> {
    values.iter().map(|&value| value as u64).collect()
}

/// A request carrying consumer and purpose headers but no bearer token.
fn bare<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("x-qdl-consumer-id", CONSUMER.parse().unwrap());
    request
        .metadata_mut()
        .insert("x-qdl-purpose", "INTERNAL_ALPHA".parse().unwrap());
    request
}

// ------------------------------------------------------------------ K2-T01

#[derive(Default)]
struct BootstrapView(Mutex<Option<query::GetSnapshotResponse>>);

#[tonic::async_trait]
impl ReadView for BootstrapView {
    async fn snapshot(
        &self,
        _requirement: &StreamRequirement,
        _proto: &query::DataRequirement,
        _consumer_id: &str,
    ) -> Result<query::GetSnapshotResponse, ReadViewError> {
        self.0
            .lock()
            .unwrap()
            .clone()
            .ok_or(ReadViewError::precondition(
                "DATA_NOT_READY",
                "test fixture: no applied product state",
            ))
    }

    async fn status(
        &self,
        requirement: &StreamRequirement,
        proto: &query::DataRequirement,
        consumer_id: &str,
    ) -> Result<query::GetFeedStatusResponse, ReadViewError> {
        NotReadyReadView
            .status(requirement, proto, consumer_id)
            .await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t01_offset_zero_is_snapshot_state_not_a_genesis_or_skipped_event() {
    for ring_max_bytes in [0, 32 << 20] {
        let view = Arc::new(BootstrapView::default());
        let harness = Harness::new(Options {
            log: Some(Log::new(&PARTITIONS)), // No seed/genesis record.
            read_view: view.clone(),
            ring_max_bytes,
            ..Options::default()
        });
        let request = || {
            harness.authorized(query::GetSnapshotRequest {
                requirement: Some(requirement("TRADE", None)),
                consumer_id: CONSUMER.into(),
            })
        };
        let empty = harness.gateway.get_snapshot(request()).await.err().unwrap();
        assert_eq!(empty.code(), Code::FailedPrecondition);
        assert!(empty.message().starts_with("DATA_NOT_READY:"));

        let first = trade(100);
        assert_eq!(harness.log.append(0, TRADE_KEY, &first), 0);
        harness.settle();
        // KN-2 ReadView fixture; persisted coverage is tested in KN-3/KN-4.
        *view.0.lock().unwrap() = Some(query::GetSnapshotResponse {
            request_id: "test-bootstrap".into(),
            snapshot_id: "kn2-test-snapshot".into(),
            stream_cursor: harness.cursor("TRADE", None, 0, 0),
            data_as_of_ns: now_ns(),
            watermark_offset: 0,
            events: vec![first.clone()],
        });
        let snapshot = harness
            .gateway
            .get_snapshot(request())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(snapshot.watermark_offset, 0);
        assert_eq!(snapshot.events, vec![first]);
        assert_eq!(harness.log.append(0, TRADE_KEY, &trade(101)), 1);
        harness.settle();
        let response = harness
            .gateway
            .subscribe(harness.authorized(query::SubscribeRequest {
                consumer_id: CONSUMER.into(),
                requirement: Some(requirement("TRADE", None)),
                cursor_token: snapshot.stream_cursor,
                max_buffer_events: 100,
            }))
            .await
            .unwrap();
        let mut stream = Stream(response.into_inner());
        assert_eq!(stream.until_live().await, vec![1]);
        assert_eq!(harness.log.append(0, TRADE_KEY, &trade(102)), 2);
        harness.settle();
        assert_eq!(stream.collect(1).await.1, vec![2]);
        let mut replay = harness.replay_rpc("TRADE", 0, 100).await.unwrap();
        let mut offsets = Vec::new();
        while let Some(result) = replay.next().await {
            offsets.push(result.unwrap().record.unwrap().logical_offset);
        }
        assert_eq!(offsets, vec![1, 2]);
        drop(stream);
        drop(replay);
        harness.baseline().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t01_markers_duplicates_and_a_sparse_key_deliver_exactly_the_committed_records() {
    let harness = Harness::new(Options {
        duplicate_every: Some(3),
        ..Options::default()
    });
    let log = &harness.log;
    // Before the subscriber: its key is sparse among other keys, with
    // transaction markers / aborted batches in between.
    let cursor = log.append(0, TRADE_KEY, &trade(1));
    for index in 0..40u64 {
        log.append(0, QUOTE_KEY, &quote(index as u32, vec![]));
        if index % 7 == 0 {
            log.gap(0, 3);
            log.append(0, TRADE_KEY, &trade(100 + index));
        }
    }
    harness.settle();
    let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
    let replayed = stream.until_live().await;
    // Live phase: more sparse records, gaps and duplicate deliveries.
    for index in 0..30u64 {
        log.append(0, QUOTE_KEY, &quote(index as u32, vec![]));
        if index % 5 == 0 {
            log.gap(0, 2);
            log.append(0, TRADE_KEY, &trade(500 + index));
        }
    }
    let expected = as_offsets(&log.records_of(TRADE_KEY, cursor));
    let (_, live) = stream.collect(expected.len() - replayed.len()).await;
    let delivered: Vec<u64> = replayed.iter().chain(live.iter()).copied().collect();
    assert_eq!(delivered, expected, "every committed record once, in order");
    // The gateway counts a record after the transport accepted it, so the
    // client may read it a moment before the counter moves.
    let metrics = &harness.state().metrics;
    let wanted = (replayed.len() as u64, live.len() as u64);
    let counted = || {
        (
            metrics.replayed.load(Ordering::Relaxed),
            metrics.delivered.load(Ordering::Relaxed),
        )
    };
    let deadline = Instant::now() + Duration::from_secs(2);
    while counted() != wanted && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        counted(),
        wanted,
        "gateway counters equal what the client received"
    );
    assert!(harness.hub.metrics.duplicates.load(Ordering::Relaxed) > 0);
}

// ------------------------------------------------------------------ K2-T02

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t02_ring_hit_and_replay_reader_deliver_the_same_records() {
    // Ring large enough (hit) versus a ring that keeps almost nothing (miss).
    for (ring_max_bytes, expect_hit) in [(32 << 20, true), (256, false)] {
        let harness = Harness::new(Options {
            ring_max_bytes,
            ..Options::default()
        });
        let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
        for index in 0..50u64 {
            harness.log.append(0, TRADE_KEY, &trade(10 + index));
            harness
                .log
                .append(0, QUOTE_KEY, &quote(index as u32, vec![]));
        }
        harness.settle();
        let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
        let replayed = stream.until_live().await;
        assert_eq!(
            replayed,
            as_offsets(&harness.log.records_of(TRADE_KEY, cursor))
        );
        let metrics = &harness.state().replay.metrics;
        assert_eq!(metrics.ring_hits.load(Ordering::Relaxed) == 1, expect_hit);
        assert_eq!(
            metrics.reader_replays.load(Ordering::Relaxed) == 1,
            !expect_hit
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t02_retention_scan_and_backlog_limits_end_typed() {
    // Below the retention floor: typed expiry, a fresh snapshot is required.
    let harness = Harness::new(Options {
        ring_max_bytes: 256,
        ..Options::default()
    });
    for index in 0..20u64 {
        harness.log.append(0, TRADE_KEY, &trade(index));
    }
    harness.settle();
    harness.log.set_low(0, 10);
    let mut stream = harness.subscribe("TRADE", None, 0, 100).await.unwrap();
    let (code, message) = stream.error().await;
    assert_eq!(code, Code::OutOfRange);
    assert!(message.starts_with("CURSOR_EXPIRED:RETENTION"), "{message}");

    // A sparse key beyond the scan budget: typed expiry, never an endless scan.
    let mut limits = Options::default().limits;
    limits.replay = ReplayLimits {
        max_scanned_records: 25,
        ..ReplayLimits::default()
    };
    let harness = Harness::new(Options {
        ring_max_bytes: 256,
        limits,
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    for index in 0..60u64 {
        harness
            .log
            .append(0, QUOTE_KEY, &quote(index as u32, vec![]));
    }
    harness.log.append(0, TRADE_KEY, &trade(2));
    harness.settle();
    let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
    let (code, message) = stream.error().await;
    assert_eq!(code, Code::OutOfRange);
    assert!(message.contains("REPLAY_SCAN_LIMIT"), "{message}");

    // More matching records than the bounded window: typed expiry (parity).
    let mut limits = Options::default().limits;
    limits.replay = ReplayLimits {
        max_matched: 5,
        ..ReplayLimits::default()
    };
    let harness = Harness::new(Options {
        limits,
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    for index in 0..10u64 {
        harness.log.append(0, TRADE_KEY, &trade(10 + index));
    }
    harness.settle();
    let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
    let (code, message) = stream.error().await;
    assert_eq!(code, Code::OutOfRange);
    assert!(message.contains("replay backlog exceeds"), "{message}");
    assert!(harness.state().metrics.expired.load(Ordering::Relaxed) >= 1);
}

/// Deterministic xorshift so the property test is reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t02_property_subscribing_at_any_moment_splits_replay_and_live_exactly() {
    for seed in 1..=12u64 {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ seed);
        let ring = if seed % 2 == 0 { 32 << 20 } else { 512 };
        let harness = Harness::new(Options {
            ring_max_bytes: ring,
            duplicate_every: (seed % 3 == 0).then_some(4),
            ..Options::default()
        });
        let log = harness.log.clone();
        let mut trades = vec![log.append(0, TRADE_KEY, &trade(1))];
        let mut streams = Vec::new();
        for step in 0..120u64 {
            match rng.below(10) {
                0..=4 => trades.push(log.append(0, TRADE_KEY, &trade(step))),
                5..=6 => {
                    log.append(0, QUOTE_KEY, &quote(step as u32, vec![]));
                }
                7 => log.gap(0, 1 + rng.below(3) as i64),
                _ => {
                    // Subscribe while the reader dispatches concurrently, at
                    // a random committed cursor of the key.
                    let after = trades[rng.below(trades.len() as u64) as usize];
                    let stream = harness
                        .subscribe("TRADE", None, after, 1_000)
                        .await
                        .unwrap();
                    streams.push((after, stream));
                }
            }
            if rng.below(4) == 0 {
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
        }
        harness.settle();
        for (after, mut stream) in streams {
            let expected = as_offsets(&log.records_of(TRADE_KEY, after));
            let (controls, offsets) = stream.collect(expected.len()).await;
            assert_eq!(
                offsets, expected,
                "seed {seed} after {after} controls {controls:?}"
            );
            assert_eq!(controls.first().map(String::as_str), Some("REPLAYING"));
            // Every record below the barrier came during replay, so LIVE is
            // the next frame; otherwise it came between replay and live.
            if !controls.contains(&"LIVE".to_owned()) {
                match stream.next().await {
                    Item::Control(code) => assert_eq!(code, "LIVE", "seed {seed}"),
                    other => panic!("seed {seed}: expected LIVE, got {other:?}"),
                }
            }
            assert!(controls.iter().filter(|code| *code == "LIVE").count() <= 1);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t02_cancelled_replays_release_readers_permits_and_memory() {
    let harness = Harness::new(Options {
        ring_max_bytes: 256,
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    for index in 0..400u64 {
        harness.log.append(0, TRADE_KEY, &trade(10 + index));
    }
    harness.settle();
    for _ in 0..20 {
        let mut stream = harness
            .subscribe("TRADE", None, cursor, 1_000)
            .await
            .unwrap();
        let _ = stream.next().await; // REPLAYING, then the client goes away
        drop(stream);
        harness.released().await;
    }
    // One atomic snapshot: readers, requests, both budgets, slots and
    // subscribers all back to baseline.
    harness.baseline().await;
    assert_eq!(
        harness
            .state()
            .metrics
            .subscriptions_active
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t02_a_reconnect_storm_queues_for_replay_readers_instead_of_failing() {
    // One reader in the pool, a ring that covers nothing: every stream needs
    // the reader, all of one consumer, opened at once.
    let harness = Harness::new(Options {
        ring_max_bytes: 256,
        replay_readers: 1,
        range_delay: Duration::from_millis(3),
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    for index in 0..30u64 {
        harness.log.append(0, TRADE_KEY, &trade(10 + index));
        harness
            .log
            .append(0, QUOTE_KEY, &quote(index as u32, vec![]));
    }
    harness.settle();
    let expected = as_offsets(&harness.log.records_of(TRADE_KEY, cursor));
    let mut streams = Vec::new();
    for _ in 0..12 {
        streams.push(harness.subscribe("TRADE", None, cursor, 100).await.unwrap());
    }
    for mut stream in streams {
        assert_eq!(
            stream.until_live().await,
            expected,
            "every stream replayed in turn"
        );
    }
    // Coalesced: one reader served all twelve in a few shared passes.
    let metrics = &harness.state().replay.metrics;
    let passes = metrics.reader_replays.load(Ordering::Relaxed);
    assert!((1..12).contains(&passes), "{passes} passes for 12 replays");
}

// ------------------------------------------------------------------ K2-T03

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t03_two_subscriptions_of_one_identity_progress_independently() {
    let harness = Harness::new(Options {
        max_streams: 4,
        ..Options::default()
    });
    let first = harness.log.append(0, TRADE_KEY, &trade(1));
    let second = harness.log.append(0, TRADE_KEY, &trade(2));
    harness.log.append(0, TRADE_KEY, &trade(3));
    harness.settle();
    let mut a = harness.subscribe("TRADE", None, first, 100).await.unwrap();
    let mut b = harness.subscribe("TRADE", None, second, 100).await.unwrap();
    assert_eq!(
        a.until_live().await,
        as_offsets(&harness.log.records_of(TRADE_KEY, first))
    );
    assert_eq!(
        b.until_live().await,
        as_offsets(&harness.log.records_of(TRADE_KEY, second))
    );
    let next = harness.log.append(0, TRADE_KEY, &trade(4));
    assert_eq!(a.collect(1).await.1, vec![next as u64]);
    assert_eq!(b.collect(1).await.1, vec![next as u64]);
    // Per-consumer stream quota (max_streams 4): two more fit, a fifth does not.
    let _c = harness.subscribe("TRADE", None, next, 100).await.unwrap();
    let _d = harness.subscribe("TRADE", None, next, 100).await.unwrap();
    let refused = harness
        .subscribe("TRADE", None, next, 100)
        .await
        .err()
        .unwrap();
    assert_eq!(refused.code(), Code::ResourceExhausted);
    assert!(refused
        .message()
        .contains("consumer concurrent stream quota"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t03_key_rotation_keeps_streams_and_revocation_closes_them() {
    let harness = Harness::new(Options::default());
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    harness.settle();
    let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
    stream.until_live().await;
    let authority = harness.state().authority.clone();
    // Rotation: a second key is added; the open stream keeps delivering.
    let rotated = keypair();
    authority.replace(Authority {
        jwt: jwt_config(&[("k1", &harness.keys), ("k2", &rotated)]),
        bundle: bundle(3, &ALL_PERMISSIONS, &ALL_FEEDS),
        expectation: expectation(),
    });
    let next = harness.log.append(0, TRADE_KEY, &trade(2));
    assert_eq!(stream.collect(1).await.1, vec![next as u64]);
    // Revocation: the stream's signing key is withdrawn; it ends typed.
    authority.replace(Authority {
        jwt: jwt_config(&[("k2", &rotated)]),
        bundle: bundle(3, &ALL_PERMISSIONS, &ALL_FEEDS),
        expectation: expectation(),
    });
    let (code, message) = stream.error().await;
    assert_eq!(code, Code::Unauthenticated);
    assert!(message.contains("revoked"), "{message}");
    // Entitlement withdrawn by a new manifest revision: ends typed too.
    let mut other = harness.subscribe("TRADE", None, next, 100).await.err();
    assert!(
        other.take().is_some(),
        "the revoked key cannot open a new stream"
    );
    assert!(harness.state().metrics.revoked.load(Ordering::Relaxed) >= 1);
}

// ------------------------------------------------------------------ K2-T04

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t04_a_replica_behind_the_cursor_waits_bounded_then_answers_retryable() {
    // Replica B shares the committed log but its reader is paused (behind).
    let harness = Harness::new(Options::default());
    let log = harness.log.clone();
    harness.paused.store(true, Ordering::Relaxed);
    let ahead = log.append(0, TRADE_KEY, &trade(1));
    // The cursor comes from a replica that already delivered `ahead`.
    let mut stream = harness.subscribe("TRADE", None, ahead, 100).await.unwrap();
    let (code, message) = stream.error().await;
    assert_eq!(code, Code::Unavailable, "retryable, never a false expiry");
    assert!(message.starts_with("REPLICA_LAGGING"), "{message}");
    assert_eq!(harness.state().metrics.lagging.load(Ordering::Relaxed), 1);
    // The replica catches up within the bound: exact next record, no resnapshot.
    let mut stream = harness.subscribe("TRADE", None, ahead, 100).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    harness.paused.store(false, Ordering::Relaxed);
    let next = log.append(0, TRADE_KEY, &trade(2));
    let (controls, offsets) = stream.collect(1).await;
    assert_eq!(offsets, vec![next as u64], "{controls:?}");
    assert!(controls.contains(&"LIVE".to_owned()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t04_a_stopping_replica_ends_streams_retryable_and_they_resume_exactly() {
    let harness = Harness::new(Options::default());
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    harness.settle();
    let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
    stream.until_live().await;
    let first = harness.log.append(0, TRADE_KEY, &trade(2));
    assert_eq!(stream.collect(1).await.1, vec![first as u64]);
    harness.state().shut_down();
    let (code, message) = stream.error().await;
    assert_eq!(code, Code::Unavailable, "retryable: the client fails over");
    assert!(message.starts_with("GATEWAY_SHUTTING_DOWN"), "{message}");
    // Resuming after the last delivered record (on this or another replica)
    // continues exactly.
    let next = harness.log.append(0, TRADE_KEY, &trade(3));
    let replica = Harness::new(Options {
        log: Some(harness.log.clone()),
        ..Options::default()
    });
    replica.settle();
    let mut resumed = replica.subscribe("TRADE", None, first, 100).await.unwrap();
    assert_eq!(resumed.until_live().await, vec![next as u64]);
}

// ------------------------------------------------------------------ K2-T05

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t05_a_slow_lossless_reader_ends_typed_and_resources_return_to_baseline() {
    let harness = Harness::new(Options::default());
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    harness.settle();
    let mut stream = harness.subscribe("TRADE", None, cursor, 10).await.unwrap();
    stream.until_live().await;
    // The client stops reading; lossless records overflow its bounded queue.
    for index in 0..200u64 {
        harness.log.append(0, TRADE_KEY, &trade(10 + index));
    }
    harness.settle();
    let mut saw_backpressure = false;
    let (code, message) = loop {
        match stream.next().await {
            Item::Control(code) if code == "RATE_LIMITED" => saw_backpressure = true,
            Item::Error(code, message) => break (code, message),
            Item::End => panic!("stream ended silently"),
            _ => {}
        }
    };
    assert!(saw_backpressure, "BACKPRESSURE control precedes the status");
    assert_eq!(code, Code::ResourceExhausted);
    assert!(message.starts_with("RATE_LIMITED:"), "{message}");
    drop(stream);
    // Repeated connect/disconnect returns every counter to its baseline.
    harness.released().await;
    for _ in 0..25 {
        let mut stream = harness.subscribe("TRADE", None, cursor, 10).await.unwrap();
        let _ = stream.next().await;
        drop(stream);
        harness.released().await;
    }
    harness.baseline().await;
    assert!(harness.state().metrics.overflow.load(Ordering::Relaxed) >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t05_the_replica_byte_budget_bounds_all_queues_together() {
    // A budget for about three records: a stalled reader cannot hold more.
    let harness = Harness::new(Options {
        budget: live_weight(TRADE_KEY, &trade(1)) * 3 + 10,
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    harness.settle();
    let mut stream = harness
        .subscribe("TRADE", None, cursor, 1_000)
        .await
        .unwrap();
    stream.until_live().await;
    for index in 0..50u64 {
        harness.log.append(0, TRADE_KEY, &trade(10 + index));
    }
    harness.settle();
    assert!(harness.state().budget.used() <= live_weight(TRADE_KEY, &trade(1)) * 3 + 10);
    let (code, _) = stream.error().await;
    assert_eq!(
        code,
        Code::ResourceExhausted,
        "over budget is typed backpressure"
    );
}

// ------------------------------------------------------------------ K2-T06

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t06_book_snapshot_and_delta_on_one_key_never_cross_or_coalesce() {
    let harness = Harness::new(Options::default());
    let cursor = harness.log.append(0, BOOK_KEY, &snapshot(1));
    harness.settle();
    let mut snapshots = harness
        .subscribe("BOOK_SNAPSHOT", None, cursor, 10)
        .await
        .unwrap();
    let mut deltas = harness
        .subscribe("BOOK_DELTA", None, cursor, 1_000)
        .await
        .unwrap();
    snapshots.until_live().await;
    deltas.until_live().await;
    let mut expected_snapshots = Vec::new();
    let mut expected_deltas = Vec::new();
    for sequence in 2..8u64 {
        expected_deltas.push(
            harness
                .log
                .append(0, BOOK_KEY, &delta(sequence, sequence == 4)) as u64,
        );
        expected_snapshots.push(harness.log.append(0, BOOK_KEY, &snapshot(sequence)) as u64);
    }
    assert_eq!(
        snapshots.collect(expected_snapshots.len()).await.1,
        expected_snapshots
    );
    assert_eq!(
        deltas.collect(expected_deltas.len()).await.1,
        expected_deltas
    );
    // Book snapshots are lossless (invariant 27): a stalled reader overflows
    // rather than silently losing a snapshot.
    for sequence in 10..40u64 {
        harness.log.append(0, BOOK_KEY, &snapshot(sequence));
    }
    harness.settle();
    let (code, _) = snapshots.error().await;
    assert_eq!(code, Code::ResourceExhausted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t06_quotes_coalesce_to_the_newest_but_keep_quality_transitions() {
    let harness = Harness::new(Options::default());
    let cursor = harness.log.append(0, QUOTE_KEY, &quote(0, vec![]));
    harness.settle();
    let mut stream = harness
        .subscribe("QUOTE", None, cursor, 1_000)
        .await
        .unwrap();
    stream.until_live().await;
    // Stall the reader: 40 quotes arrive for an 8-deep latest-state queue;
    // one of them changes the quality state.
    let mut transition = 0;
    let mut last = 0;
    for index in 1..=40u32 {
        let flags = if index >= 20 { vec![9] } else { vec![] };
        last = harness
            .log
            .append(0, QUOTE_KEY, &quote(index, flags.clone()));
        if index == 20 {
            transition = last;
        }
    }
    harness.settle();
    let mut delivered = Vec::new();
    loop {
        match stream.next().await {
            Item::Event(offset, _) => {
                delivered.push(offset);
                if offset == last as u64 {
                    break;
                }
            }
            Item::Error(code, message) => panic!("{code:?} {message}"),
            Item::End => panic!("ended after {delivered:?}"),
            Item::Control(_) => {}
        }
    }
    // Depth 8 + the 2-record transport handoff + one in flight.
    assert!(
        delivered.len() <= 11,
        "coalesced to the latest state: {delivered:?}"
    );
    assert!(
        delivered.windows(2).all(|pair| pair[1] > pair[0]),
        "offsets stay increasing"
    );
    assert_eq!(
        *delivered.last().unwrap(),
        last as u64,
        "the newest quote is delivered"
    );
    // The last record before the transition (old quality state) was kept, so
    // the consumer sees the state change rather than a jump into it.
    assert!(delivered.iter().any(|&offset| offset < transition as u64));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t06_bars_coalesce_in_progress_updates_but_never_final_bars_or_another_interval() {
    let harness = Harness::new(Options::default());
    let cursor = harness
        .log
        .append(0, BAR_KEY, &bar(0, BarLifecycle::Final, "1m"));
    harness.settle();
    let mut stream = harness
        .subscribe("BAR", Some("1m"), cursor, 5)
        .await
        .unwrap();
    stream.until_live().await;
    let mut finals = Vec::new();
    for minute in 1..=3i64 {
        for _ in 0..6 {
            harness
                .log
                .append(0, BAR_KEY, &bar(minute, BarLifecycle::InProgress, "1m"));
        }
        // Another interval on the same physical key is another product.
        harness
            .log
            .append(0, BAR_KEY, &bar(minute, BarLifecycle::Final, "5m"));
        finals.push(
            harness
                .log
                .append(0, BAR_KEY, &bar(minute, BarLifecycle::Final, "1m")) as u64,
        );
    }
    harness.settle();
    let mut delivered = Vec::new();
    let mut lifecycles = Vec::new();
    while delivered.last() != finals.last() {
        match stream.next().await {
            Item::Event(offset, event) => {
                if let Some(event_envelope::Payload::Bar(value)) = event.payload {
                    assert_eq!(value.interval, "1m", "no cross-interval delivery");
                    lifecycles.push(value.lifecycle);
                }
                delivered.push(offset);
            }
            Item::Error(code, message) => panic!("{code:?} {message} after {delivered:?}"),
            Item::End => panic!("ended after {delivered:?}"),
            Item::Control(_) => {}
        }
    }
    for offset in &finals {
        assert!(delivered.contains(offset), "final bar {offset} delivered");
    }
    let finals_seen = lifecycles
        .iter()
        .filter(|&&value| value == BarLifecycle::Final as i32)
        .count();
    assert_eq!(finals_seen, 3);
}

// ------------------------------------------------------------------ K2-T07

struct FixtureView;

#[tonic::async_trait]
impl ReadView for FixtureView {
    async fn snapshot(
        &self,
        requirement: &StreamRequirement,
        _proto: &query::DataRequirement,
        consumer_id: &str,
    ) -> Result<query::GetSnapshotResponse, ReadViewError> {
        Ok(query::GetSnapshotResponse {
            request_id: format!("fixture:{consumer_id}"),
            snapshot_id: format!("fixture:{}", requirement.delivery.feed),
            ..Default::default()
        })
    }

    async fn status(
        &self,
        requirement: &StreamRequirement,
        _proto: &query::DataRequirement,
        _consumer_id: &str,
    ) -> Result<query::GetFeedStatusResponse, ReadViewError> {
        Ok(query::GetFeedStatusResponse {
            state: format!("FIXTURE_{}", requirement.delivery.feed),
            ..Default::default()
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t07_snapshot_and_status_run_the_python_checks_then_the_read_view() {
    let harness = Harness::new(Options::default());
    let snapshot_request = |requirement| query::GetSnapshotRequest {
        consumer_id: CONSUMER.into(),
        requirement: Some(requirement),
    };
    // Default view: typed not-ready, never UNIMPLEMENTED.
    let status = harness
        .gateway
        .get_snapshot(harness.authorized(snapshot_request(requirement("TRADE", None))))
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().starts_with("DATA_NOT_READY:"),
        "{}",
        status.message()
    );
    let status = harness
        .gateway
        .get_feed_status(harness.authorized(query::GetFeedStatusRequest {
            requirement: Some(requirement("TRADE", None)),
            consumer_id: CONSUMER.into(),
        }))
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), Code::FailedPrecondition);
    // Invalid requirement: INVALID_ARGUMENT with the handler's prefix.
    let mut bad = requirement("TRADE", None);
    bad.interval = "1m".into();
    let status = harness
        .gateway
        .get_snapshot(harness.authorized(snapshot_request(bad)))
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().starts_with("INVALID_ARGUMENT:"),
        "{}",
        status.message()
    );
    // A warmup needs history:read; the token's role must grant it.
    let mut warm = requirement("BAR", Some("1m"));
    warm.warmup_limit = 10;
    let without_history = harness.request(
        snapshot_request(warm.clone()),
        &bearer(&harness.keys, "k1", &["market_data_reader"], 3),
    );
    let status = harness
        .gateway
        .get_snapshot(without_history)
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), Code::PermissionDenied);
    // Over the manifest warmup quota.
    let mut huge = requirement("BAR", Some("1m"));
    huge.warmup_limit = 6_000;
    let status = harness
        .gateway
        .get_snapshot(harness.authorized(snapshot_request(huge)))
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(status.message().contains("warmup limit exceeds"));
    // A fixture view proves the response mapping (tests only).
    let fixture = Harness::new(Options {
        read_view: Arc::new(FixtureView),
        ..Options::default()
    });
    let response = fixture
        .gateway
        .get_snapshot(fixture.authorized(snapshot_request(requirement("QUOTE", None))))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.snapshot_id, "fixture:QUOTE");
    let response = fixture
        .gateway
        .get_feed_status(fixture.authorized(query::GetFeedStatusRequest {
            requirement: Some(requirement("QUOTE", None)),
            consumer_id: CONSUMER.into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.state, "FIXTURE_QUOTE");
    // status:read withdrawn from the manifest: refused.
    let no_status = Harness::new(Options {
        read_view: Arc::new(FixtureView),
        permissions: vec!["stream:read", "snapshot:read"],
        ..Options::default()
    });
    let status = no_status
        .gateway
        .get_feed_status(no_status.authorized(query::GetFeedStatusRequest {
            requirement: Some(requirement("QUOTE", None)),
            consumer_id: CONSUMER.into(),
        }))
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(status.message().contains("status:read"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t07_every_rpc_refuses_missing_or_mismatched_identity() {
    let harness = Harness::new(Options::default());
    let cursor = harness.cursor("TRADE", None, 0, 0);
    let subscribe = query::SubscribeRequest {
        consumer_id: CONSUMER.into(),
        requirement: Some(requirement("TRADE", None)),
        cursor_token: cursor.clone(),
        max_buffer_events: 10,
    };
    let replay = query::ReplayRequest {
        consumer_id: CONSUMER.into(),
        cursor_token: cursor.clone(),
        limit: 10,
    };
    let snapshot = query::GetSnapshotRequest {
        consumer_id: CONSUMER.into(),
        requirement: Some(requirement("TRADE", None)),
    };
    let status = query::GetFeedStatusRequest {
        requirement: Some(requirement("TRADE", None)),
        consumer_id: CONSUMER.into(),
    };
    let codes = [
        harness
            .gateway
            .subscribe(bare(subscribe.clone()))
            .await
            .err()
            .unwrap()
            .code(),
        harness
            .gateway
            .replay(bare(replay.clone()))
            .await
            .err()
            .unwrap()
            .code(),
        harness
            .gateway
            .get_snapshot(bare(snapshot.clone()))
            .await
            .err()
            .unwrap()
            .code(),
        harness
            .gateway
            .get_feed_status(bare(status.clone()))
            .await
            .err()
            .unwrap()
            .code(),
    ];
    assert!(
        codes.iter().all(|code| *code == Code::Unauthenticated),
        "{codes:?}"
    );
    // The body names another consumer than the authenticated one.
    let mut other = subscribe.clone();
    other.consumer_id = "alpha.binance.paper.test".into();
    let refused = harness
        .gateway
        .subscribe(harness.authorized(other))
        .await
        .err()
        .unwrap();
    assert_eq!(refused.code(), Code::PermissionDenied);
    let mut other = replay.clone();
    other.consumer_id = "alpha.binance.paper.test".into();
    let refused = harness
        .gateway
        .replay(harness.authorized(other))
        .await
        .err()
        .unwrap();
    assert_eq!(refused.code(), Code::PermissionDenied);
    // A token without the stream role cannot Subscribe or Replay.
    let reader_only = bearer(&harness.keys, "k1", &["market_data_reader"], 3);
    let refused = harness
        .gateway
        .subscribe(harness.request(subscribe, &reader_only))
        .await
        .err()
        .unwrap();
    assert_eq!(refused.code(), Code::PermissionDenied);
    let refused = harness
        .gateway
        .replay(harness.request(replay, &reader_only))
        .await
        .err()
        .unwrap();
    assert_eq!(refused.code(), Code::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t07_replay_pages_the_cursor_product_with_signed_tokens() {
    let harness = Harness::new(Options::default());
    let cursor = harness.log.append(0, BOOK_KEY, &snapshot(1));
    let mut snapshots = Vec::new();
    for sequence in 2..30u64 {
        harness.log.append(0, BOOK_KEY, &delta(sequence, false));
        snapshots.push(harness.log.append(0, BOOK_KEY, &snapshot(sequence)) as u64);
    }
    harness.settle();
    let token = harness.cursor("BOOK_SNAPSHOT", None, 0, cursor);
    let response = harness
        .gateway
        .replay(harness.authorized(query::ReplayRequest {
            consumer_id: CONSUMER.into(),
            cursor_token: token,
            limit: 10,
        }))
        .await
        .unwrap();
    let mut stream = response.into_inner();
    let mut offsets = Vec::new();
    let mut last_token = String::new();
    while let Some(item) = stream.next().await {
        let record = item.unwrap().record.unwrap();
        match record.payload.unwrap() {
            query::stream_record::Payload::Event(event) => {
                assert!(matches!(
                    event.payload,
                    Some(event_envelope::Payload::BookSnapshot(_))
                ));
                offsets.push(record.logical_offset);
                last_token = record.resume_token;
            }
            query::stream_record::Payload::Control(_) => panic!("Replay sends records only"),
        }
    }
    assert_eq!(
        offsets,
        snapshots[..10].to_vec(),
        "exactly one page of the product"
    );
    // The next page resumes from the last token.
    let response = harness
        .gateway
        .replay(harness.authorized(query::ReplayRequest {
            consumer_id: CONSUMER.into(),
            cursor_token: last_token,
            limit: 5,
        }))
        .await
        .unwrap();
    let next: Vec<u64> = response
        .into_inner()
        .map(|item| item.unwrap().record.unwrap().logical_offset)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(next, snapshots[10..15].to_vec());
    // A page above the manifest buffer quota, or above the server bound.
    let token = harness.cursor("BOOK_SNAPSHOT", None, 0, cursor);
    let refused = harness
        .gateway
        .replay(harness.authorized(query::ReplayRequest {
            consumer_id: CONSUMER.into(),
            cursor_token: token,
            limit: 2_000,
        }))
        .await
        .err()
        .unwrap();
    assert_eq!(refused.code(), Code::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t07_replay_refuses_a_cursor_outside_the_consumer_feed_scope() {
    // The consumer is entitled to TRADE only; a BOOK_SNAPSHOT cursor is refused.
    let harness = Harness::new(Options::default());
    let authority = harness.state().authority.clone();
    authority.replace(Authority {
        jwt: jwt_config(&[("k1", &harness.keys)]),
        bundle: bundle(3, &ALL_PERMISSIONS, &[("TRADE", None)]),
        expectation: expectation(),
    });
    let token = harness.cursor("BOOK_SNAPSHOT", None, 0, 0);
    let refused = harness
        .gateway
        .replay(harness.authorized(query::ReplayRequest {
            consumer_id: CONSUMER.into(),
            cursor_token: token,
            limit: 10,
        }))
        .await
        .err()
        .unwrap();
    assert_eq!(refused.code(), Code::PermissionDenied);
    assert!(refused
        .message()
        .contains("stream cursor scope is outside the registered consumer manifest"));
}

// ------------------------------------------- Astra KN-2 R1 regressions (F1-F4)
//
// The five counterexamples of `kn2-astra-review-20260924/probes.rs`, kept as
// durable tests, plus the boundary cases the review asked for.

fn revoke_all(harness: &Harness) {
    harness.state().authority.replace(Authority {
        jwt: jwt_config(&[]),
        bundle: bundle(3, &ALL_PERMISSIONS, &ALL_FEEDS),
        expectation: expectation(),
    });
}

type ReplayStream = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<query::ReplayResponse, Status>> + Send>,
>;

impl Harness {
    async fn replay_rpc(&self, feed: &str, after: i64, limit: u32) -> Result<ReplayStream, Status> {
        let token = self.cursor(feed, None, 0, after);
        let response = self
            .gateway
            .replay(self.authorized(query::ReplayRequest {
                consumer_id: CONSUMER.into(),
                cursor_token: token,
                limit,
            }))
            .await?;
        Ok(response.into_inner())
    }

    /// Wait until replay, budget and slots are back to baseline.
    async fn baseline(&self) {
        let state = self.state().clone();
        let snapshot = || {
            (
                state.replay.in_flight(),
                state.replay.available(),
                state.budget.used(),
                state.replay.budget().used(),
                state.open_streams(),
                state.open_replays(),
                self.hub.subscriber_count(),
            )
        };
        let settled = (0, state.replay.readers(), 0, 0, 0, 0, 0);
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut seen = snapshot();
        while seen != settled && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
            seen = snapshot();
        }
        assert_eq!(
            seen, settled,
            "(in_flight, readers free, live bytes, replay bytes, streams, replays, subscribers)"
        );
    }
}

// F1: revocation is never missed, whatever the stream is doing.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f1_revocation_during_reader_replay_ends_before_live() {
    let harness = Harness::new(Options {
        ring_max_bytes: 1,
        range_delay: Duration::from_millis(20),
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    for index in 2..32 {
        harness.log.append(0, TRADE_KEY, &trade(index));
    }
    harness.settle();
    let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
    assert!(matches!(stream.next().await, Item::Control(ref code) if code == "REPLAYING"));
    revoke_all(&harness);
    loop {
        match stream.next().await {
            Item::Error(Code::Unauthenticated, _) => break,
            Item::Control(code) if code == "LIVE" => panic!("revoked stream reached LIVE"),
            Item::End => panic!("revocation not surfaced"),
            _ => {}
        }
    }
    drop(stream);
    harness.baseline().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f1_revocation_before_the_first_record_and_during_ring_replay() {
    let harness = Harness::new(Options::default());
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    for index in 2..40 {
        harness.log.append(0, TRADE_KEY, &trade(index));
    }
    harness.settle();
    // Admitted, then revoked before anything was read: nothing but the error.
    let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
    revoke_all(&harness);
    let mut events = 0;
    loop {
        match stream.next().await {
            Item::Error(Code::Unauthenticated, _) => break,
            Item::Event(..) => events += 1,
            Item::Control(code) if code == "LIVE" => panic!("revoked stream reached LIVE"),
            Item::End => panic!("revocation not surfaced"),
            Item::Control(_) => {}
            Item::Error(code, message) => panic!("{code:?} {message}"),
        }
    }
    // At most the handoff (two responses) was already with the transport.
    assert!(events <= 2, "{events} records after revocation");
    drop(stream);
    harness.baseline().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f1_revocation_during_a_blocked_send_ends_the_stream() {
    let harness = Harness::new(Options::default());
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    harness.settle();
    let mut stream = harness
        .subscribe("TRADE", None, cursor, 1_000)
        .await
        .unwrap();
    stream.until_live().await;
    // The client stops reading: the handoff fills, the task blocks in send.
    for index in 2..30 {
        harness.log.append(0, TRADE_KEY, &trade(index));
    }
    harness.settle();
    tokio::time::sleep(Duration::from_millis(100)).await;
    revoke_all(&harness);
    let mut events = 0;
    loop {
        match stream.next().await {
            Item::Error(Code::Unauthenticated, _) => break,
            Item::Event(..) => events += 1,
            other => panic!("expected the revocation, got {other:?}"),
        }
    }
    assert!(events <= 2, "{events} records reached a revoked client");
    drop(stream);
    harness.baseline().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f1_revocation_ends_a_replay_rpc_and_rotation_keeps_readers() {
    let harness = Harness::new(Options {
        ring_max_bytes: 1,
        range_delay: Duration::from_millis(10),
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    for index in 2..40 {
        harness.log.append(0, TRADE_KEY, &trade(index));
    }
    harness.settle();
    // Additive rotation mid-replay: the authorized reader keeps going.
    let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
    assert!(matches!(stream.next().await, Item::Control(ref code) if code == "REPLAYING"));
    let rotated = keypair();
    harness.state().authority.replace(Authority {
        jwt: jwt_config(&[("k1", &harness.keys), ("k2", &rotated)]),
        bundle: bundle(3, &ALL_PERMISSIONS, &ALL_FEEDS),
        expectation: expectation(),
    });
    assert_eq!(
        stream.until_live().await,
        as_offsets(&harness.log.records_of(TRADE_KEY, cursor))
    );
    drop(stream);
    // Replay RPC: revoked while paging.
    let mut replay = harness.replay_rpc("TRADE", cursor, 100).await.unwrap();
    let first = replay.next().await.unwrap();
    assert!(first.is_ok());
    revoke_all(&harness);
    let mut records = 1;
    let status = loop {
        match tokio::time::timeout(Duration::from_secs(5), replay.next()).await {
            Ok(Some(Ok(_))) => records += 1,
            Ok(Some(Err(status))) => break status,
            other => panic!("replay did not end typed: {other:?}"),
        }
    };
    assert_eq!(status.code(), Code::Unauthenticated);
    assert!(
        records < 38,
        "revocation stopped the page ({records} records)"
    );
    drop(replay);
    harness.baseline().await;
}

// F2: a corrupt committed record is never skipped.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f2_corrupt_records_end_typed_on_reader_and_ring_paths_alike() {
    for (ring_max_bytes, position) in [(1, 0), (1, 1), (1, 2), (32 << 20, 1)] {
        let harness = Harness::new(Options {
            ring_max_bytes,
            ..Options::default()
        });
        // Corrupt as committed (first, middle or last of the range), so the
        // ring holds the same bytes as the log.
        let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
        let mut bad = 0;
        for index in 0..3 {
            if index == position {
                bad = harness.log.append_corrupt(0, TRADE_KEY);
            } else {
                harness.log.append(0, TRADE_KEY, &trade(index as u64 + 2));
            }
        }
        harness.settle();
        // Twice: a retry meets the same record and ends the same way.
        for _ in 0..2 {
            let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
            let mut delivered = Vec::new();
            loop {
                match stream.next().await {
                    Item::Error(Code::DataLoss, message) => {
                        assert!(message.contains("DATA_INTEGRITY"), "{message}");
                        break;
                    }
                    Item::Event(offset, _) => delivered.push(offset as i64),
                    Item::Control(code) if code == "LIVE" => panic!(
                        "corrupt record {bad} skipped (ring {ring_max_bytes}): {delivered:?}"
                    ),
                    Item::Control(_) => {}
                    other => panic!("{other:?}"),
                }
            }
            assert!(
                delivered.iter().all(|&offset| offset < bad),
                "nothing past the corrupt record: {delivered:?}"
            );
            drop(stream);
            harness.baseline().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f2_a_corrupt_record_fails_only_its_key_in_a_shared_pass() {
    let harness = Harness::new(Options {
        ring_max_bytes: 1,
        range_delay: Duration::from_millis(2),
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    let mut quotes = Vec::new();
    let mut bad = 0;
    for index in 2..12u32 {
        if index == 6 {
            bad = harness.log.append_corrupt(0, TRADE_KEY);
        } else {
            harness.log.append(0, TRADE_KEY, &trade(u64::from(index)));
        }
        quotes.push(harness.log.append(0, QUOTE_KEY, &quote(index, vec![])) as u64);
    }
    harness.settle();
    assert!(bad > cursor);
    let mut trades = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
    let mut others = harness.subscribe("QUOTE", None, cursor, 100).await.unwrap();
    assert_eq!(trades.error().await.0, Code::DataLoss);
    assert_eq!(
        others.until_live().await,
        quotes,
        "the other key is unaffected"
    );
    drop(trades);
    drop(others);
    harness.baseline().await;
}

// F3: every request is answered, on every exit.

struct FailingOpen {
    entered: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
}

impl RangeSource for FailingOpen {
    fn open(&self, _partition: i32, _from: i64) -> Result<Box<dyn RangeCursor>, RangeError> {
        self.entered.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(2);
        while !self.release.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(RangeError::Other("injected source-open failure".into()))
    }
}

fn submit(
    coordinator: &Arc<ReplayCoordinator>,
    after: i64,
    barrier: i64,
    key: &str,
    deadline: Duration,
) -> (
    tokio::sync::mpsc::Receiver<Replayed>,
    tokio::sync::oneshot::Receiver<ReplayEnd>,
) {
    let (sink, records) = tokio::sync::mpsc::channel(64);
    let (done, end) = tokio::sync::oneshot::channel();
    coordinator.submit(
        0,
        ReplayRequest::new(
            after,
            barrier,
            key.as_bytes().to_vec(),
            ("TRADE".into(), None),
            None,
            sink,
            done,
            Arc::new(AtomicBool::new(false)),
            Instant::now() + deadline,
        ),
    );
    (records, end)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f3_a_joiner_is_answered_after_a_reader_open_failure() {
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let budget = ByteBudget::new(1 << 20);
    let coordinator = ReplayCoordinator::new(
        Arc::new(FailingOpen {
            entered: entered.clone(),
            release: release.clone(),
        }),
        1,
        ReplayLimits::default(),
        budget.clone(),
    );
    let (_first_records, first) = submit(&coordinator, 0, 3, TRADE_KEY, Duration::from_millis(200));
    tokio::time::timeout(Duration::from_secs(1), async {
        while !entered.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let (_second_records, second) =
        submit(&coordinator, 0, 3, TRADE_KEY, Duration::from_millis(200));
    release.store(true, Ordering::SeqCst);
    for (index, end) in [first, second].into_iter().enumerate() {
        let result = tokio::time::timeout(Duration::from_secs(2), end).await;
        assert!(
            matches!(
                result,
                Ok(Ok(ReplayEnd::Error(_))) | Ok(Ok(ReplayEnd::ScanLimit(_)))
            ),
            "request {index} stranded: {result:?}, in_flight={}",
            coordinator.in_flight()
        );
    }
    coordinator_baseline(&coordinator, &budget).await;
}

/// The coordinator back at rest, checked as one snapshot: the driver re-takes
/// its permit once to see that nothing is left, so fields read one by one
/// can catch that instant.
async fn coordinator_baseline(coordinator: &Arc<ReplayCoordinator>, budget: &Arc<ByteBudget>) {
    let snapshot = || {
        (
            coordinator.in_flight(),
            coordinator.available(),
            budget.used(),
        )
    };
    let settled = (0, coordinator.readers(), 0);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut seen = snapshot();
    while seen != settled && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
        seen = snapshot();
    }
    assert_eq!(seen, settled, "(in_flight, readers free, bytes)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f3_joiners_at_the_last_record_and_empty_ranges_complete_exactly() {
    let log = Log::new(&PARTITIONS);
    for index in 0..40u64 {
        log.append(0, TRADE_KEY, &trade(index));
    }
    log.gap(0, 5); // trailing markers only
    let end = log.end(0);
    let budget = ByteBudget::new(1 << 20);
    let coordinator = ReplayCoordinator::new(
        Arc::new(Range {
            log: log.clone(),
            delay: Duration::from_millis(3),
        }),
        1,
        ReplayLimits::default(),
        budget.clone(),
    );
    let (mut all, all_end) = submit(&coordinator, -1, end, TRADE_KEY, Duration::from_secs(5));
    // Joins while the pass is at its last records.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (mut tail, tail_end) = submit(&coordinator, 37, end, TRADE_KEY, Duration::from_secs(5));
    // A range holding only markers.
    let (mut empty, empty_end) = submit(&coordinator, 39, end, TRADE_KEY, Duration::from_secs(5));
    // A request's sink closes once it is answered: collect until then.
    async fn collect(records: &mut tokio::sync::mpsc::Receiver<Replayed>) -> Vec<i64> {
        let mut offsets = Vec::new();
        while let Ok(Some(replayed)) =
            tokio::time::timeout(Duration::from_secs(5), records.recv()).await
        {
            offsets.push(replayed.record.raw.offset);
        }
        offsets
    }
    assert_eq!(collect(&mut all).await, (0..40).collect::<Vec<i64>>());
    assert_eq!(all_end.await.unwrap(), ReplayEnd::Complete);
    assert_eq!(collect(&mut tail).await, vec![38, 39]);
    assert_eq!(tail_end.await.unwrap(), ReplayEnd::Complete);
    assert!(collect(&mut empty).await.is_empty());
    assert_eq!(empty_end.await.unwrap(), ReplayEnd::Complete);
    drop((all, tail, empty));
    coordinator_baseline(&coordinator, &budget).await;
}

// F4: bounded replay - per-request scan limits, budget-charged channels,
// ring references and handoffs, bounded Replay admission.

fn large_snapshot(sequence: u64, levels: u32) -> EventEnvelope {
    use qdl_stream_gateway::generated::marketdata_v2::BookLevel;
    envelope(event_envelope::Payload::BookSnapshot(OrderBookSnapshot {
        native_sequence: sequence.to_string(),
        levels: (0..levels)
            .map(|level| BookLevel {
                order_count: level + 1,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }))
}

fn live_weight(key: &str, envelope: &EventEnvelope) -> usize {
    qdl_stream_gateway::hub::LiveRecord::decode(Arc::new(RawRecord {
        partition: 0,
        offset: 0,
        key: key.as_bytes().to_vec(),
        payload: envelope.encode_to_vec(),
        timestamp_ms: 0,
    }))
    .unwrap()
    .weight()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f4_the_coordinator_enforces_the_scan_byte_and_record_limits() {
    for (records, bytes, reason) in [
        (2_000_000, 1, "REPLAY_BYTE_LIMIT"),
        (1, 1 << 30, "REPLAY_SCAN_LIMIT"),
    ] {
        let mut limits = StreamLimits::default();
        limits.replay.max_scanned_bytes = bytes;
        limits.replay.max_scanned_records = records;
        let harness = Harness::new(Options {
            ring_max_bytes: 1,
            limits,
            ..Options::default()
        });
        let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
        harness.log.append(0, QUOTE_KEY, &quote(1, vec![]));
        harness.log.append(0, TRADE_KEY, &trade(2));
        harness.settle();
        // Records: the range is 2 offsets, so the up-front span check passes
        // only for the byte case; the record case must stop inside the pass.
        let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
        loop {
            match stream.next().await {
                Item::Error(Code::OutOfRange, message) => {
                    assert!(message.contains(reason), "{message}");
                    break;
                }
                Item::Event(offset, _) => panic!("{reason}: delivered {offset} past the bound"),
                Item::Control(code) if code == "LIVE" => panic!("{reason} ignored"),
                Item::Control(_) => {}
                other => panic!("{other:?}"),
            }
        }
        drop(stream);
        harness.baseline().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f4_large_book_replays_stay_inside_the_budget_and_end_exact_or_typed() {
    let weight = live_weight(BOOK_KEY, &large_snapshot(1, 800));
    let limit = weight * 3;
    let mut limits = StreamLimits::default();
    limits.replay.max_duration = Duration::from_secs(3);
    let harness = Harness::new(Options {
        ring_max_bytes: 1,
        budget: limit,
        limits,
        range_delay: Duration::from_millis(1),
        ..Options::default()
    });
    let cursor = harness.log.append(0, BOOK_KEY, &snapshot(0));
    for sequence in 1..=20 {
        harness
            .log
            .append(0, BOOK_KEY, &large_snapshot(sequence, 800));
    }
    harness.settle();
    let expected = as_offsets(&harness.log.records_of(BOOK_KEY, cursor));
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let watcher = {
        let state = harness.state().clone();
        let peak = peak.clone();
        tokio::spawn(async move {
            for _ in 0..600 {
                peak.fetch_max(state.replay.budget().used(), Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    // Four clients reading at once, as real clients do.
    let mut readers = Vec::new();
    for _ in 0..4 {
        let mut stream = harness
            .subscribe("BOOK_SNAPSHOT", None, cursor, 100)
            .await
            .unwrap();
        let expected = expected.clone();
        readers.push(tokio::spawn(async move {
            let mut offsets = Vec::new();
            loop {
                match stream.next().await {
                    Item::Event(offset, _) => offsets.push(offset),
                    Item::Control(code) if code == "LIVE" => {
                        assert_eq!(offsets, expected, "a completed replay is exact");
                        return true;
                    }
                    Item::Control(_) => {}
                    Item::Error(Code::ResourceExhausted, message) => {
                        assert!(message.contains("RATE_LIMITED"), "{message}");
                        return false;
                    }
                    other => panic!("neither exact nor typed: {other:?}"),
                }
            }
        }));
    }
    let mut exact = 0;
    for reader in readers {
        if reader.await.unwrap() {
            exact += 1;
        }
    }
    watcher.abort();
    assert!(exact >= 1, "the budget still lets replays progress");
    assert!(peak.load(Ordering::Relaxed) <= limit);
    harness.baseline().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f4_ring_references_are_charged_or_fall_back_to_the_reader() {
    let weight = live_weight(TRADE_KEY, &trade(1));
    let harness = Harness::new(Options {
        budget: weight * 6,
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    for index in 2..40 {
        harness.log.append(0, TRADE_KEY, &trade(index));
    }
    harness.settle();
    // The ring covers the range but its 38 records do not fit the budget.
    let mut stream = harness.subscribe("TRADE", None, cursor, 100).await.unwrap();
    assert_eq!(
        stream.until_live().await,
        as_offsets(&harness.log.records_of(TRADE_KEY, cursor))
    );
    let metrics = &harness.state().replay.metrics;
    assert_eq!(metrics.ring_hits.load(Ordering::Relaxed), 0);
    assert!(metrics.reader_replays.load(Ordering::Relaxed) >= 1);
    drop(stream);
    harness.baseline().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f4_replay_rpcs_are_admitted_per_replica_and_per_consumer_and_released() {
    let harness = Harness::new(Options {
        ring_max_bytes: 1,
        range_delay: Duration::from_millis(20),
        limits: StreamLimits {
            max_replay_rpcs: 1,
            ..Options::default().limits
        },
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    for index in 2..60 {
        harness.log.append(0, TRADE_KEY, &trade(index));
    }
    harness.settle();
    let mut first = harness.replay_rpc("TRADE", cursor, 50).await.unwrap();
    assert!(first.next().await.unwrap().is_ok());
    let refused = harness.replay_rpc("TRADE", cursor, 50).await.err().unwrap();
    assert_eq!(refused.code(), Code::ResourceExhausted);
    assert!(
        refused.message().contains("replay capacity"),
        "{}",
        refused.message()
    );
    // A client that goes away mid-page releases its slot, reader and bytes.
    drop(first);
    harness.baseline().await;
    let mut again = harness.replay_rpc("TRADE", cursor, 5).await.unwrap();
    let mut count = 0;
    while let Some(item) = again.next().await {
        item.unwrap();
        count += 1;
    }
    assert_eq!(count, 5);
    drop(again);
    harness.baseline().await;
    // Per consumer: the manifest's max_streams bounds concurrent Replays.
    let harness = Harness::new(Options {
        ring_max_bytes: 1,
        range_delay: Duration::from_millis(20),
        max_streams: 1,
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    for index in 2..30 {
        harness.log.append(0, TRADE_KEY, &trade(index));
    }
    harness.settle();
    let mut held = harness.replay_rpc("TRADE", cursor, 20).await.unwrap();
    assert!(held.next().await.unwrap().is_ok());
    let refused = harness.replay_rpc("TRADE", cursor, 20).await.err().unwrap();
    assert_eq!(refused.code(), Code::ResourceExhausted);
    assert!(refused
        .message()
        .contains("consumer concurrent replay quota"));
    drop(held);
    harness.baseline().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f4_a_starved_replay_storm_never_overflows_live_delivery() {
    // K2.5 R1 rerun finding: with one shared budget, 185 replay prefetches
    // overflowed live lossless queues. Replay now has its own budget.
    let weight = live_weight(TRADE_KEY, &trade(1));
    let mut limits = StreamLimits::default();
    limits.replay.max_duration = Duration::from_secs(2);
    let harness = Harness::new(Options {
        ring_max_bytes: 1,
        range_delay: Duration::from_millis(2),
        replay_budget: Some(weight * 2),
        limits,
        ..Options::default()
    });
    let cursor = harness.log.append(0, TRADE_KEY, &trade(1));
    for index in 2..60 {
        harness.log.append(0, TRADE_KEY, &trade(index));
    }
    harness.settle();
    let end = harness.log.end(0) - 1;
    let mut live = harness.subscribe("TRADE", None, end, 1_000).await.unwrap();
    assert!(live.until_live().await.is_empty());
    let mut storm = Vec::new();
    for _ in 0..8 {
        storm.push(
            harness
                .subscribe("TRADE", None, cursor, 1_000)
                .await
                .unwrap(),
        );
    }
    let mut fresh = Vec::new();
    for index in 100..150 {
        fresh.push(harness.log.append(0, TRADE_KEY, &trade(index)) as u64);
    }
    assert_eq!(
        live.collect(fresh.len()).await,
        (vec![], fresh),
        "live delivery is exact"
    );
    let expected = as_offsets(&harness.log.records_of(TRADE_KEY, cursor));
    for mut stream in storm {
        let mut offsets = Vec::new();
        loop {
            match stream.next().await {
                Item::Event(offset, _) => offsets.push(offset),
                Item::Control(code) if code == "LIVE" => {
                    assert!(
                        expected.starts_with(&offsets),
                        "a replay is exact while it runs"
                    );
                    break;
                }
                Item::Control(_) => {}
                Item::Error(Code::ResourceExhausted, message) => {
                    assert!(message.contains("RATE_LIMITED"), "{message}");
                    break;
                }
                other => panic!("neither exact nor typed: {other:?}"),
            }
        }
    }
    drop(live);
    harness.baseline().await;
}
