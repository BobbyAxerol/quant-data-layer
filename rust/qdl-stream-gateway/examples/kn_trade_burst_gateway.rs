//! Isolated capture-backed gRPC driver, NOT Kafka or live-freshness evidence.
//! Usage: kn_trade_burst_gateway CAPTURE TEMPLATES PRIVATE_DIR RATE REPEAT [PORT]
//! PRIVATE_DIR must exist; publication starts when every product is subscribed.
//! Packet credentials are ephemeral and written mode 0600, never to stdout.
use base64::{engine::general_purpose::STANDARD, Engine};
use jsonwebtoken::{encode, Algorithm, DecodingKey, EncodingKey, Header};
use prost::Message;
use qdl_contracts::cursor_v3::{
    CursorV3Claims, CursorV3Codec, CursorV3Expectation, DeliveryRequirement,
};
use qdl_stream_gateway::{
    auth::{AccessError, JwtConfig, RequestQuota},
    authority::{Authority, AuthorityHandle},
    bundle::{Binding, Bundle, Manifest, ManifestRequirement, Quotas},
    generated::{
        marketdata_v2::{event_envelope, EventEnvelope},
        query_v2 as query,
    },
    hub::{Hub, HubConfig, LogSource, RawRecord},
    readview::NotReadyReadView,
    replay::{RangeCursor, RangeError, RangeSource, ReplayCoordinator},
    service::{Gateway, GatewayState, StreamLimits},
    subscription::ByteBudget,
};
use query::market_data_stream_service_server::MarketDataStreamServiceServer;
use ring::{
    rand::{SecureRandom, SystemRandom},
    signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING},
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CONSUMER: &str = "test.capture.trade-burst";
const SUBJECT: &str = "spiffe://test/capture-trade-burst";
const STREAM: &str = "test.capture.canonical";
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
fn private_json(path: &Path, value: &Value) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = path.with_extension("pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(serde_json::to_string(value)?.as_bytes())?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    fs::File::open(path.parent().ok_or("output parent missing")?)?.sync_all()?;
    Ok(())
}

struct Product {
    uid: String,
    envelope: EventEnvelope,
    payloads: Vec<Vec<u8>>,
    source_offsets: Vec<(i64, i64)>,
    published: AtomicU64,
}
struct Capture {
    products: Vec<Product>,
}
impl Capture {
    fn record(&self, partition: i32, offset: i64) -> Option<RawRecord> {
        let product = self.products.get(usize::try_from(partition).ok()?)?;
        if offset < 1 || offset as u64 > product.published.load(Ordering::Acquire) {
            return None;
        }
        Some(RawRecord {
            partition,
            offset,
            key: product.uid.as_bytes().to_vec(),
            payload: product.payloads[(offset as usize - 1) % product.payloads.len()].clone(),
            timestamp_ms: 0,
        })
    }
}
struct Live {
    capture: Arc<Capture>,
    positions: Vec<i64>,
    next_partition: usize,
}
impl LogSource for Live {
    fn poll(&mut self, timeout: Duration) -> Result<Option<RawRecord>, String> {
        let deadline = Instant::now() + timeout;
        loop {
            for _ in 0..self.positions.len() {
                let p = self.next_partition;
                self.next_partition = (p + 1) % self.positions.len();
                if let Some(row) = self.capture.record(p as i32, self.positions[p]) {
                    self.positions[p] += 1;
                    return Ok(Some(row));
                }
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_micros(250));
        }
    }
    fn positions(&self) -> Vec<(i32, i64)> {
        self.positions
            .iter()
            .enumerate()
            .map(|(p, &o)| (p as i32, o))
            .collect()
    }
}
struct Ranges(Arc<Capture>);
struct Reader {
    capture: Arc<Capture>,
    partition: i32,
    position: i64,
}
impl RangeSource for Ranges {
    fn open(&self, partition: i32, from: i64) -> Result<Box<dyn RangeCursor>, RangeError> {
        Ok(Box::new(Reader {
            capture: self.0.clone(),
            partition,
            position: from.max(1),
        }))
    }
}
impl RangeCursor for Reader {
    fn next(&mut self, _: Duration) -> Result<Option<RawRecord>, RangeError> {
        let record = self.capture.record(self.partition, self.position);
        if record.is_some() {
            self.position += 1;
        }
        Ok(record)
    }
    fn position(&self) -> Option<i64> {
        Some(self.position)
    }
}
// Local fixture only: real JWT/manifest/cursor validation, no shared quota Redis.
struct FixtureQuota;
impl RequestQuota for FixtureQuota {
    fn consume(&self, _: &Manifest) -> Result<(), AccessError> {
        Ok(())
    }
}

fn requirement(uid: &str, template: &Value) -> query::DataRequirement {
    query::DataRequirement {
        instrument_uid: uid.into(),
        source_policy_id: template["requirement"]["source_policy_id"]
            .as_str()
            .unwrap()
            .into(),
        feed_type: query::FeedType::Trade as i32,
        grade: query::ConsumerGrade::Execution as i32,
        stale_policy_type: query::StalePolicy::Block as i32,
        gap_policy_type: query::GapPolicy::Block as i32,
        recovery_policy: query::RecoveryPolicy::SnapshotAndReplay as i32,
        revision_policy: query::BarRevisionPolicy::Latest as i32,
        require_full_coverage: true,
        require_final_bars: true,
        event_recency_policy: query::StalePolicy::Observe as i32,
        max_freshness_ms: template["requirement"]["max_freshness_ms"]
            .as_u64()
            .unwrap(),
        max_session_liveness_ms: template["requirement"]["max_session_liveness_ms"]
            .as_u64()
            .unwrap(),
        // Actual TS OBSERVE delivery policy; only projection uses a test clock.
        ..Default::default()
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 || args.len() > 7 {
        return Err("CAPTURE TEMPLATES PRIVATE_DIR RATE REPEAT [PORT]".into());
    }
    let directory = PathBuf::from(&args[3]);
    let rate: u64 = args[4].parse()?;
    let repeat: u64 = args[5].parse()?;
    if !(100..=4000).contains(&rate) || !(1..=30).contains(&repeat) {
        return Err("bounded rate=100..4000 repeat=1..30 required".into());
    }
    if directory.join("start").exists() || directory.join("ready.json").exists() {
        return Err("fresh private directory required".into());
    }
    let templates: Value = serde_json::from_slice(&fs::read(&args[2])?)?;
    let mut products: Vec<Product> = Vec::new();
    let mut schedule = Vec::new();
    let mut bytes = 0usize;
    for line in BufReader::new(fs::File::open(&args[1])?).lines() {
        let row: Value = serde_json::from_str(&line?)?;
        let payload = STANDARD.decode(row["payload"].as_str().ok_or("payload missing")?)?;
        bytes += payload.len();
        if bytes > 32 << 20 || schedule.len() >= 20000 {
            return Err("capture bound exceeded".into());
        }
        let envelope = EventEnvelope::decode(payload.as_slice())?;
        if !matches!(envelope.payload, Some(event_envelope::Payload::Trade(_)))
            || templates.get(&envelope.instrument_uid).is_none()
        {
            return Err("TRADE and matching template required".into());
        }
        let p = match products
            .iter()
            .position(|p| p.uid == envelope.instrument_uid)
        {
            Some(p) => p,
            None => {
                products.push(Product {
                    uid: envelope.instrument_uid.clone(),
                    envelope,
                    payloads: Vec::new(),
                    source_offsets: Vec::new(),
                    published: AtomicU64::new(0),
                });
                products.len() - 1
            }
        };
        let coordinate = (
            row["partition"].as_i64().ok_or("partition missing")?,
            row["offset"].as_i64().ok_or("offset missing")?,
        );
        if products[p]
            .source_offsets
            .last()
            .is_some_and(|old| old.0 != coordinate.0 || old.1 >= coordinate.1)
        {
            return Err("capture product offsets must increase in one partition".into());
        }
        products[p].payloads.push(payload);
        products[p].source_offsets.push(coordinate);
        schedule.push(p);
    }
    if schedule.is_empty() || products.len() > 10 {
        return Err("expected 1..10 nonempty products".into());
    }
    if schedule.len() as u64 * repeat > rate * 120 {
        return Err("offer window exceeds 120 seconds".into());
    }
    let capture = Arc::new(Capture { products });
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
        .map_err(|_| "key generation")?;
    let key = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
        .map_err(|_| "key parse")?;
    let mut secret = vec![0; 32];
    rng.fill(&mut secret).map_err(|_| "random")?;
    let codec = CursorV3Codec::new(&BTreeMap::from([("burst".into(), secret)]), "burst")?;
    let expectation = CursorV3Expectation {
        environment: "paper".into(),
        stream: STREAM.into(),
        source_topic_id: "capture-test-not-kafka".into(),
        partition_plan_epoch: 1,
        source_policy_revision: 1,
        catalog_revision: 1,
        route_generation: "capture-test-only".into(),
        schema_major: 2,
    };
    let mut bindings = Vec::new();
    let mut requirements = Vec::new();
    let mut routes = Vec::new();
    for (p, product) in capture.products.iter().enumerate() {
        let req = requirement(&product.uid, &templates[&product.uid]);
        let delivery = DeliveryRequirement::from_proto(&req)?;
        let lpk = format!(
            "lpk1|paper|{}|{}|{}|TRADE|-",
            product.envelope.venue, product.envelope.market, product.uid
        );
        bindings.push(Binding {
            binding_id: format!("capture-{p}"),
            instrument_uid: product.uid.clone(),
            venue: product.envelope.venue.clone(),
            market: product.envelope.market.clone(),
            feed: "TRADE".into(),
            interval: None,
            source_policy_id: req.source_policy_id.clone(),
            physical_key: product.uid.clone(),
            product_key: lpk.clone(),
            stale_after_ms: 3000,
        });
        requirements.push(ManifestRequirement {
            instrument_uid: product.uid.clone(),
            feed: "TRADE".into(),
            interval: None,
            consumer_grade: "EXECUTION".into(),
            source_policy_id: req.source_policy_id.clone(),
            event_recency_policy: Some("OBSERVE".into()),
            max_session_liveness_ms: Some(req.max_session_liveness_ms),
        });
        let cursor = codec
            .encode(&CursorV3Claims {
                key_id: "burst".into(),
                environment: "paper".into(),
                consumer_id: CONSUMER.into(),
                requirement_digest: delivery.digest(),
                schema_major: 2,
                stream: STREAM.into(),
                product_key: lpk,
                snapshot_id: "capture-start".into(),
                source_topic_id: expectation.source_topic_id.clone(),
                source_partition: p as u64,
                source_offset: 0,
                partition_plan_epoch: 1,
                source_policy_revision: 1,
                catalog_revision: 1,
                route_generation: expectation.route_generation.clone(),
                issued_at_ns: now_ns(),
                expires_at_ns: now_ns() + 900_000_000_000,
            })
            .map_err(|e| format!("cursor: {e:?}"))?;
        routes.push(json!({"instrument_uid": product.uid, "partition": p, "cursor": cursor,
            "requirement_proto_b64": STANDARD.encode(req.encode_to_vec()), "expected_events": product.payloads.len() as u64 * repeat,
            "source_coordinates": product.source_offsets}));
    }
    let bundle = Bundle {
        sha256: "capture-test-only".into(),
        environment: "paper".into(),
        catalog_revision: 1,
        source_policy_revision: 1,
        canonical_stream: STREAM.into(),
        bindings,
        manifests: vec![Manifest {
            consumer_id: CONSUMER.into(),
            subject: SUBJECT.into(),
            environment: "paper".into(),
            manifest_revision: 1,
            allowed_purposes: vec!["INTERNAL_EXECUTION".into()],
            allowed_permissions: vec!["stream:read".into(), "history:read".into()],
            requirements,
            quotas: Quotas {
                requests_per_minute: 10000,
                max_batch_items: 100,
                max_warmup_rows: 5000,
                max_streams: 32,
                max_buffer_events: 1000,
            },
        }],
    };
    let authority = AuthorityHandle::new(Authority {
        bundle,
        expectation,
        jwt: JwtConfig {
            environment: "paper".into(),
            issuer: "https://capture.test".into(),
            audience: "capture-test".into(),
            keys: BTreeMap::from([(
                "burst".into(),
                (
                    DecodingKey::from_ec_der(key.public_key().as_ref()),
                    vec![Algorithm::ES256],
                ),
            )]),
            algorithms: vec![Algorithm::ES256],
            max_lifetime_seconds: 900,
            environments_by_key_id: BTreeMap::new(),
            subjects_by_key_id: BTreeMap::from([("burst".into(), SUBJECT.into())]),
        },
    });
    let starts: Vec<_> = (0..capture.products.len()).map(|p| (p as i32, 1)).collect();
    let hub = Arc::new(Hub::new(
        &starts,
        HubConfig {
            ring_max_bytes: (48 << 20) / starts.len(),
            ring_max_age: Duration::from_secs(120),
        },
    ));
    let reader = hub.run(Box::new(Live {
        capture: capture.clone(),
        positions: vec![1; starts.len()],
        next_partition: 0,
    }));
    let limits = StreamLimits::default();
    let state = Arc::new(GatewayState::new(
        authority,
        Arc::new(FixtureQuota),
        codec,
        hub.clone(),
        ReplayCoordinator::new(
            Arc::new(Ranges(capture.clone())),
            2,
            limits.replay.clone(),
            ByteBudget::new(32 << 20),
        ),
        Arc::new(NotReadyReadView),
        ByteBudget::new(32 << 20),
        limits,
    ));
    let port: u16 = args.get(6).map(String::as_str).unwrap_or("18219").parse()?;
    let address: std::net::SocketAddr = format!("127.0.0.1:{port}").parse()?;
    let tls_root = directory.join("tls");
    let tls_enabled = tls_root.exists();
    let encrypted = if tls_enabled {
        let config = qdl_stream_gateway::tls::server_config(
            &fs::read_to_string(tls_root.join("server.pem"))?,
            &fs::read_to_string(tls_root.join("server.key"))?,
            &fs::read_to_string(tls_root.join("ca.pem"))?,
        )?;
        Some(qdl_stream_gateway::tls::incoming(address, config).await?)
    } else {
        None
    };
    let plaintext = if tls_enabled {
        None
    } else {
        let listener = tokio::net::TcpListener::bind(address).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            loop {
                let result = listener.accept().await.map(|(s, _)| s);
                if tx.send(result).await.is_err() {
                    break;
                }
            }
        });
        Some(tokio_stream::wrappers::ReceiverStream::new(rx))
    };
    let tls_packet = if tls_enabled {
        json!({"ca_file":"tls/ca.pem", "client_cert_file":"tls/client.pem", "client_key_file":"tls/client.key"})
    } else {
        json!(false)
    };
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some("burst".into());
    let jwt = encode(
        &header,
        &json!({"sub": SUBJECT, "iss":"https://capture.test", "aud":"capture-test",
        "iat": now_ns()/1_000_000_000, "exp": now_ns()/1_000_000_000+900, "jti": format!("capture-{}", now_ns()),
        "environment":"paper", "roles":["stream_consumer","historical_reader","market_data_reader"], "consumer_manifest_revision":1}),
        &EncodingKey::from_ec_der(pkcs8.as_ref()),
    )?;
    let cursors: BTreeMap<_, _> = routes
        .iter()
        .map(|r| (r["instrument_uid"].as_str().unwrap(), &r["cursor"]))
        .collect();
    let totals: BTreeMap<_, _> = routes
        .iter()
        .map(|r| (r["instrument_uid"].as_str().unwrap(), &r["expected_events"]))
        .collect();
    private_json(
        &directory.join("ready.json"),
        &json!({"target":format!("127.0.0.1:{port}"), "tls":tls_packet,
        "test_only":true,"consumer_id":CONSUMER,"purpose":"INTERNAL_EXECUTION","token":jwt,"routes":routes,"cursors":cursors,"total_per_uid":totals,
        "offered_rate":rate,"repeat":repeat,"capture_clock":"max(received_at_ns,source_event_time_ns)+1000000",
        "scope":"test-only capture-backed real gRPC; no Kafka durability or live eligibility"}),
    )?;
    let stopping = state.clone();
    let publishing = state.clone();
    let output = directory.clone();
    let producer = tokio::spawn(async move {
        let wait = Instant::now();
        while publishing.hub.subscriber_count() < capture.products.len() {
            if wait.elapsed() > Duration::from_secs(90) {
                return Err("start timeout".to_owned());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let began = Instant::now();
        let total = schedule.len() as u64 * repeat;
        let mut max_late = 0f64;
        let mut max_lag = 0u64;
        let mut queue_peak = 0usize;
        for index in 0..total {
            if index % 16 == 0 {
                let due = Duration::from_secs_f64(index as f64 / rate as f64);
                tokio::time::sleep(due.saturating_sub(began.elapsed())).await;
                max_late = max_late.max(began.elapsed().saturating_sub(due).as_secs_f64() * 1000.0);
            }
            capture.products[schedule[index as usize % schedule.len()]]
                .published
                .fetch_add(1, Ordering::Release);
            if index % 16 == 0 {
                let lag: u64 = capture
                    .products
                    .iter()
                    .enumerate()
                    .map(|(p, product)| {
                        product.published.load(Ordering::Acquire).saturating_sub(
                            publishing
                                .hub
                                .next_offset(p as i32)
                                .unwrap_or(1)
                                .saturating_sub(1) as u64,
                        )
                    })
                    .sum();
                max_lag = max_lag.max(lag);
                queue_peak = queue_peak.max(publishing.budget.used());
            }
        }
        private_json(&output.join("producer.json"), &json!({"offered":total,"offered_rate":rate,
            "offer_seconds":began.elapsed().as_secs_f64(),"producer_schedule_late_max_ms":max_late,
            "hub_lag_peak_sampled_records":max_lag,"queue_peak_sampled_bytes":queue_peak,
            "pass":false,"note":"producer receipt only; TS apply/durable ACK and drain decide acceptance"})).map_err(|e| e.to_string())?;
        let drain = Instant::now();
        while !output.join("stop").exists() && drain.elapsed() < Duration::from_secs(60) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let load = |v: &AtomicU64| v.load(Ordering::Relaxed);
        private_json(&output.join("gateway.json"), &json!({"delivered":load(&publishing.metrics.delivered),
            "replayed":load(&publishing.metrics.replayed),"overflow":load(&publishing.metrics.overflow),
            "reader_errors":load(&publishing.hub.metrics.reader_errors),"decode_errors":load(&publishing.hub.metrics.decode_failures),
            "queue_end_bytes":publishing.budget.used(),"drain_wait_seconds":drain.elapsed().as_secs_f64(),
            "client_stop_received":output.join("stop").exists()})).map_err(|e| e.to_string())?;
        Ok::<(), String>(())
    });
    let server = tonic::transport::Server::builder()
        .add_service(MarketDataStreamServiceServer::new(Gateway { state }));
    let shutdown = async move {
        match producer.await {
            Ok(Ok(())) => {}
            other => eprintln!("capture producer failed: {other:?}"),
        }
        stopping.shut_down();
    };
    let result = match encrypted {
        Some(incoming) => {
            server
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
        }
        None => {
            server
                .serve_with_incoming_shutdown(plaintext.unwrap(), shutdown)
                .await
        }
    };
    hub.stop();
    reader.join().map_err(|_| "reader panicked")?;
    result?;
    Ok(())
}
