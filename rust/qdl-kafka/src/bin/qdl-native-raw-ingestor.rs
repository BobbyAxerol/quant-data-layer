#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{ErrorKind, Write};
use std::path::{Component, Path};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::stream::FuturesUnordered;
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use qdl_contracts::qdl::provider::v1::{
    CaptureBoundary, RawProviderEnvelope, TransportCompression, TransportProtocol,
};
use qdl_core::backoff::BackoffPolicy;
use qdl_core::binance::decode_combined;
use qdl_core::okx::{
    parse_subscription_ack, subscription_command, ControlRequestBudget, OkxService, OkxSubscription,
};
use qdl_core::transport::{DurableRecord, RetryClass};
use qdl_kafka::{
    FencedKafkaSink, KafkaTlsConfig, KafkaTransportConfig, KafkaTransportError, PendingKafkaAppend,
};
use qdl_venue_core::authority::{AuthorityMode, AuthorityRecord, PublicationContext, SinkTarget};
use qdl_venue_core::backpressure::DeliveryClass;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ProviderRuntime {
    Binance,
    Okx,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum RawFeed {
    Trade,
    Quote,
    Bar,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBinding {
    provider: String,
    venue: String,
    market: String,
    product_type: String,
    native_symbol: String,
    native_channel: String,
    subscription_id: String,
    adapter_version: String,
    instrument_catalog_revision: u64,
    feed: RawFeed,
    delivery_class: DeliveryClass,
}

impl RawBinding {
    fn key(&self) -> String {
        format!("{}|{}", self.native_channel, self.native_symbol)
    }

    fn validate(&self, runtime: ProviderRuntime) -> Result<(), String> {
        for (name, value) in [
            ("provider", self.provider.as_str()),
            ("venue", self.venue.as_str()),
            ("market", self.market.as_str()),
            ("product_type", self.product_type.as_str()),
            ("native_symbol", self.native_symbol.as_str()),
            ("native_channel", self.native_channel.as_str()),
            ("subscription_id", self.subscription_id.as_str()),
            ("adapter_version", self.adapter_version.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("raw binding {name} must not be empty"));
            }
        }
        if self.instrument_catalog_revision == 0 {
            return Err("instrument catalog revision must be positive".into());
        }
        if !matches!(
            (self.feed, self.delivery_class),
            (RawFeed::Quote, DeliveryClass::LatestState)
                | (RawFeed::Trade | RawFeed::Bar, DeliveryClass::Lossless)
        ) {
            return Err("raw binding feed/delivery class is invalid".into());
        }
        match runtime {
            ProviderRuntime::Binance
                if self.venue != "BINANCE" || !matches!(self.market.as_str(), "USDM" | "SPOT") =>
            {
                Err("Binance raw binding identity is invalid".into())
            }
            ProviderRuntime::Okx
                if self.venue != "OKX" || !matches!(self.market.as_str(), "SWAP" | "SPOT") =>
            {
                Err("OKX raw binding identity is invalid".into())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IngestorConfig {
    runtime: ProviderRuntime,
    websocket_url: String,
    business_websocket_url: Option<String>,
    raw_stream: String,
    shard_id: String,
    lease_epoch: u64,
    partition_plan_epoch: u64,
    config_revision: u64,
    heartbeat_seconds: u64,
    max_events: u64,
    max_runtime_seconds: u64,
    metrics_every_events: u64,
    generation_state_path: String,
    max_inflight_publishes: usize,
    latest_state_flush_ms: u64,
    authority: AuthorityRecord,
    bindings: Vec<RawBinding>,
}

impl IngestorConfig {
    fn validate(&self) -> Result<(), String> {
        self.authority.validate()?;
        if self.authority.mode != AuthorityMode::RustShadow
            || self.websocket_url.trim().is_empty()
            || !self.websocket_url.starts_with("wss://")
            || self.raw_stream.trim().is_empty()
            || self.shard_id.trim().is_empty()
            || self.lease_epoch == 0
            || self.partition_plan_epoch == 0
            || self.config_revision == 0
            || self.heartbeat_seconds == 0
            || self.heartbeat_seconds >= 30
            || self.metrics_every_events == 0
            || self.max_inflight_publishes == 0
            || self.max_inflight_publishes > 4_096
            || self.latest_state_flush_ms == 0
            || self.latest_state_flush_ms > 1_000
            || self.bindings.is_empty()
        {
            return Err("native raw ingestor config is invalid or not RUST_SHADOW".into());
        }
        let generation_path = Path::new(&self.generation_state_path);
        if !generation_path.is_absolute()
            || generation_path
                .components()
                .any(|component| matches!(component, Component::ParentDir))
        {
            return Err("generation state path must be absolute without parent traversal".into());
        }
        if self.runtime == ProviderRuntime::Okx {
            let business = self
                .business_websocket_url
                .as_deref()
                .ok_or("OKX business WebSocket URL is required")?;
            if !business.starts_with("wss://") {
                return Err("OKX business WebSocket URL must use wss".into());
            }
        }
        let mut keys = std::collections::HashSet::new();
        for binding in &self.bindings {
            binding.validate(self.runtime)?;
            if !keys.insert(binding.key()) {
                return Err("duplicate native raw binding".into());
            }
        }
        Ok(())
    }
}

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("required environment variable is missing: {name}"))
}

fn kafka_config(identity: &str) -> Result<KafkaTransportConfig, String> {
    let cert_root = required("QDL_KAFKA_CERT_ROOT")?;
    Ok(KafkaTransportConfig {
        bootstrap_servers: required("QDL_KAFKA_BOOTSTRAP_SERVERS")?,
        client_id: format!("{}-{identity}", required("QDL_KAFKA_CLIENT_ID")?),
        group_id: required("QDL_KAFKA_GROUP_ID")?,
        request_timeout: Duration::from_secs(30),
        tls: KafkaTlsConfig {
            ca_location: format!("{cert_root}/ca.crt"),
            certificate_location: format!("{cert_root}/client.crt"),
            key_location: format!("{cert_root}/client.key"),
            key_password: None,
        },
    })
}

fn now_ns() -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .try_into()?)
}

fn next_connection_generation(
    state_path: &Path,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let parent = state_path
        .parent()
        .ok_or("generation state path has no parent")?;
    fs::create_dir_all(parent)?;
    let previous = match fs::read_to_string(state_path) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .map_err(|_| "generation state is corrupt")?,
        Err(error) if error.kind() == ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    let generation = previous
        .checked_add(1)
        .ok_or("connection generation overflow")?;
    let temporary = state_path.with_extension(format!("tmp-{}-{}", std::process::id(), now_ns()?));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    writeln!(file, "{generation}")?;
    file.sync_all()?;
    fs::rename(&temporary, state_path)?;
    File::open(parent)?.sync_all()?;
    Ok(generation)
}

fn capture_id(session: &str, generation: u64, received_at_ns: i64, frame: &[u8]) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(session.as_bytes());
    digest.update(generation.to_be_bytes());
    digest.update(received_at_ns.to_be_bytes());
    digest.update(frame);
    digest.finalize()[..16].to_vec()
}

struct RawPublisher {
    sink: FencedKafkaSink,
    authority: AuthorityRecord,
    raw_stream: String,
    shard_id: String,
    lease_epoch: u64,
    partition_plan_epoch: u64,
    config_revision: u64,
}

impl RawPublisher {
    fn new(
        config: &IngestorConfig,
        identity: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let sink = FencedKafkaSink::new(&kafka_config(identity)?)?;
        sink.apply_authority(config.authority.clone())?;
        Ok(Self {
            sink,
            authority: config.authority.clone(),
            raw_stream: config.raw_stream.clone(),
            shard_id: config.shard_id.clone(),
            lease_epoch: config.lease_epoch,
            partition_plan_epoch: config.partition_plan_epoch,
            config_revision: config.config_revision,
        })
    }

    fn record(
        &self,
        binding: &RawBinding,
        session_id: &str,
        generation: u64,
        raw_frame: &[u8],
        received_at_ns: i64,
    ) -> DurableRecord {
        let capture_id = capture_id(session_id, generation, received_at_ns, raw_frame);
        let raw = RawProviderEnvelope {
            raw_schema_name: "qdl.provider.raw".into(),
            raw_schema_major: 1,
            raw_schema_minor: 0,
            capture_id: capture_id.clone(),
            provider: binding.provider.clone(),
            venue: binding.venue.clone(),
            market: binding.market.clone(),
            product_type: binding.product_type.clone(),
            native_symbol: binding.native_symbol.clone(),
            native_channel: binding.native_channel.clone(),
            subscription_id: binding.subscription_id.clone(),
            source_session_id: session_id.into(),
            connection_generation: generation,
            lease_epoch: self.lease_epoch,
            authority_revision: self.authority.revision,
            partition_plan_epoch: self.partition_plan_epoch,
            received_at_ns,
            transport_protocol: TransportProtocol::Websocket as i32,
            transport_compression: TransportCompression::None as i32,
            capture_boundary: CaptureBoundary::PostDecompression as i32,
            raw_frame_sha256: Sha256::digest(raw_frame).to_vec(),
            raw_frame_bytes: raw_frame.to_vec(),
            adapter_version: binding.adapter_version.clone(),
            config_revision: self.config_revision,
            instrument_catalog_revision: binding.instrument_catalog_revision,
            correlation_id: hex::encode(&capture_id),
            test_provenance: false,
        };
        DurableRecord {
            stream: self.raw_stream.clone(),
            partition_key: format!(
                "{}/{}/{}/{}",
                binding.venue, binding.market, binding.native_symbol, binding.native_channel
            ),
            event_id: capture_id,
            payload: raw.encode_to_vec(),
            accepted_at_ns: received_at_ns,
        }
    }

    fn publication(&self) -> PublicationContext {
        PublicationContext {
            slice_id: self.authority.slice_id.clone(),
            authority_revision: self.authority.revision,
            shard_id: self.shard_id.clone(),
            lease_epoch: self.lease_epoch,
            target: SinkTarget::ShadowRaw,
        }
    }

    async fn enqueue_with_retry(
        &self,
        binding: &RawBinding,
        session_id: &str,
        generation: u64,
        raw_frame: &[u8],
        received_at_ns: i64,
        stopped: &AtomicBool,
    ) -> Result<PendingKafkaAppend, KafkaTransportError> {
        let record = self.record(binding, session_id, generation, raw_frame, received_at_ns);
        let publication = self.publication();
        let backoff = BackoffPolicy {
            initial_ms: 10,
            maximum_ms: 1_000,
            multiplier: 2,
            jitter_bps: 2_000,
        }
        .validate()
        .map_err(KafkaTransportError::Configuration)?;
        let mut failures = 0_u32;
        loop {
            match self.sink.enqueue(&record, &publication) {
                Ok(delivery) => return Ok(delivery),
                Err(error)
                    if error.retry_class() != RetryClass::NonRetryable
                        && !stopped.load(Ordering::Acquire) =>
                {
                    failures = failures.saturating_add(1);
                    eprintln!(
                        "{}",
                        serde_json::to_string(&json!({
                            "event": "qdl_native_raw_enqueue_retry",
                            "runtime": binding.venue.as_str(),
                            "attempt": failures,
                            "retry_class": format!("{:?}", error.retry_class()).to_ascii_uppercase(),
                            "error": error.to_string(),
                        }))
                        .unwrap_or_else(|_| "{\"event\":\"qdl_native_raw_enqueue_retry\"}".into())
                    );
                    tokio::time::sleep(Duration::from_millis(
                        backoff.delay_ms(failures, failures.min(10_000) as u16),
                    ))
                    .await;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

type RawPublishFuture = Pin<Box<dyn Future<Output = Result<(), KafkaTransportError>> + Send>>;

fn raw_publish_future(delivery: PendingKafkaAppend) -> RawPublishFuture {
    Box::pin(async move { delivery.wait().await.map(|_| ()) })
}

#[derive(Clone, Debug)]
struct PendingRawFrame {
    binding: RawBinding,
    session_id: String,
    generation: u64,
    raw_frame: Vec<u8>,
    received_at_ns: i64,
}

#[derive(Default)]
struct LatestStateBuffer {
    frames: BTreeMap<String, PendingRawFrame>,
}

impl LatestStateBuffer {
    fn push(&mut self, frame: PendingRawFrame) -> bool {
        debug_assert_eq!(frame.binding.delivery_class, DeliveryClass::LatestState);
        self.frames.insert(frame.binding.key(), frame).is_some()
    }

    fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    fn drain(&mut self) -> Vec<PendingRawFrame> {
        std::mem::take(&mut self.frames).into_values().collect()
    }
}

async fn enqueue_lossless_frame(
    frame: PendingRawFrame,
    publisher: &RawPublisher,
    inflight: &mut FuturesUnordered<RawPublishFuture>,
    accepted: &AtomicU64,
    max_events: u64,
    max_inflight: usize,
    stopped: &AtomicBool,
) -> Result<bool, KafkaTransportError> {
    while inflight.len() >= max_inflight {
        match inflight.next().await {
            Some(result) => result?,
            None => {
                return Err(KafkaTransportError::Configuration(
                    "lossless publish window became inconsistent".into(),
                ))
            }
        }
    }
    if !reserve(accepted, max_events).await {
        return Ok(false);
    }
    let delivery = publisher
        .enqueue_with_retry(
            &frame.binding,
            &frame.session_id,
            frame.generation,
            &frame.raw_frame,
            frame.received_at_ns,
            stopped,
        )
        .await;
    match delivery {
        Ok(delivery) => {
            inflight.push(raw_publish_future(delivery));
            Ok(true)
        }
        Err(error) => {
            if max_events > 0 {
                accepted.fetch_sub(1, Ordering::AcqRel);
            }
            Err(error)
        }
    }
}

async fn flush_latest_concurrent(
    buffer: &mut LatestStateBuffer,
    publisher: &RawPublisher,
    inflight: &mut FuturesUnordered<RawPublishFuture>,
    accepted: &AtomicU64,
    max_events: u64,
    max_inflight: usize,
    stopped: &AtomicBool,
) -> Result<bool, KafkaTransportError> {
    for frame in buffer.drain() {
        if !enqueue_lossless_frame(
            frame,
            publisher,
            inflight,
            accepted,
            max_events,
            max_inflight,
            stopped,
        )
        .await?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn deadline(seconds: u64) -> Option<tokio::time::Instant> {
    (seconds > 0).then(|| tokio::time::Instant::now() + Duration::from_secs(seconds))
}

fn should_stop(
    stopped: &AtomicBool,
    accepted: &AtomicU64,
    max_events: u64,
    expires: Option<tokio::time::Instant>,
) -> bool {
    stopped.load(Ordering::Acquire)
        || (max_events > 0 && accepted.load(Ordering::Acquire) >= max_events)
        || expires.is_some_and(|value| tokio::time::Instant::now() >= value)
}

async fn reserve(accepted: &AtomicU64, max_events: u64) -> bool {
    if max_events == 0 {
        accepted.fetch_add(1, Ordering::AcqRel);
        return true;
    }
    loop {
        let current = accepted.load(Ordering::Acquire);
        if current >= max_events {
            return false;
        }
        if accepted
            .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }
        tokio::task::yield_now().await;
    }
}

async fn run_binance(
    config: Arc<IngestorConfig>,
    accepted: Arc<AtomicU64>,
    coalesced_latest: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bindings: HashMap<String, RawBinding> = config
        .bindings
        .iter()
        .cloned()
        .map(|binding| (binding.native_channel.clone(), binding))
        .collect();
    let streams = bindings.keys().cloned().collect::<Vec<_>>().join("/");
    let url = format!(
        "{}?streams={streams}",
        config.websocket_url.trim_end_matches('?')
    );
    let publisher = RawPublisher::new(&config, "binance")?;
    let backoff = BackoffPolicy {
        initial_ms: 250,
        maximum_ms: 30_000,
        multiplier: 2,
        jitter_bps: 2_000,
    }
    .validate()?;
    let generation_path = format!("{}.binance", config.generation_state_path);
    let expires = deadline(config.max_runtime_seconds);
    let mut failures = 0_u32;
    while !should_stop(&stopped, &accepted, config.max_events, expires) {
        let generation = next_connection_generation(Path::new(&generation_path))?;
        match connect_async(&url).await {
            Ok((mut socket, _)) => {
                let session_id = format!(
                    "binance-{}-{generation}-{}",
                    config.market_name(),
                    now_ns()?
                );
                let mut inflight = FuturesUnordered::<RawPublishFuture>::new();
                let mut latest = LatestStateBuffer::default();
                let mut latest_tick =
                    tokio::time::interval(Duration::from_millis(config.latest_state_flush_ms));
                latest_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                latest_tick.tick().await;
                let mut disconnected = false;
                let mut publish_error = None;
                while !should_stop(&stopped, &accepted, config.max_events, expires) {
                    if inflight.len() >= config.max_inflight_publishes {
                        match inflight.next().await {
                            Some(Ok(())) => {
                                failures = 0;
                                continue;
                            }
                            Some(Err(error)) => {
                                publish_error = Some(error);
                                break;
                            }
                            None => {
                                return Err("lossless publish window became inconsistent".into())
                            }
                        }
                    }
                    let message = tokio::select! {
                        biased;
                        _ = latest_tick.tick(), if !latest.is_empty() => {
                            if !flush_latest_concurrent(
                                &mut latest,
                                &publisher,
                                &mut inflight,
                                &accepted,
                                config.max_events,
                                config.max_inflight_publishes,
                                &stopped,
                            ).await? {
                                break;
                            }
                            failures = 0;
                            continue;
                        }
                        message = socket.next() => message,
                        completed = inflight.next(), if !inflight.is_empty() => {
                            match completed {
                                Some(Ok(())) => {
                                    failures = 0;
                                    continue;
                                }
                                Some(Err(error)) => {
                                    publish_error = Some(error);
                                    break;
                                }
                                None => {
                                return Err("lossless publish window became inconsistent".into())
                            }
                            }
                        }
                    };
                    let message = match message {
                        Some(Ok(message)) => message,
                        Some(Err(error)) => {
                            failures = failures.saturating_add(1);
                            eprintln!(
                                "{}",
                                serde_json::to_string(&json!({
                                    "event": "qdl_native_session_disconnected",
                                    "runtime": "BINANCE",
                                    "attempt": failures,
                                    "generation": generation,
                                    "error": error.to_string(),
                                }))?
                            );
                            disconnected = true;
                            break;
                        }
                        None => {
                            failures = failures.saturating_add(1);
                            eprintln!(
                                "{}",
                                serde_json::to_string(&json!({
                                    "event": "qdl_native_session_disconnected",
                                    "runtime": "BINANCE",
                                    "attempt": failures,
                                    "generation": generation,
                                    "error": "provider closed the WebSocket",
                                }))?
                            );
                            disconnected = true;
                            break;
                        }
                    };
                    if let Message::Ping(payload) = message {
                        socket.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    if !message.is_text() {
                        continue;
                    }
                    let raw_text = message.into_text()?.to_string();
                    let frame = decode_combined(raw_text.clone())?;
                    let binding = bindings
                        .get(&frame.stream)
                        .ok_or("Binance frame has no approved binding")?
                        .clone();
                    let frame = PendingRawFrame {
                        binding,
                        session_id: session_id.clone(),
                        generation,
                        raw_frame: raw_text.into_bytes(),
                        received_at_ns: now_ns()?,
                    };
                    if frame.binding.delivery_class == DeliveryClass::LatestState {
                        if latest.push(frame) {
                            coalesced_latest.fetch_add(1, Ordering::AcqRel);
                        }
                        continue;
                    }
                    if !enqueue_lossless_frame(
                        frame,
                        &publisher,
                        &mut inflight,
                        &accepted,
                        config.max_events,
                        config.max_inflight_publishes,
                        &stopped,
                    )
                    .await?
                    {
                        break;
                    }
                }
                if publish_error.is_none() {
                    if let Err(error) = flush_latest_concurrent(
                        &mut latest,
                        &publisher,
                        &mut inflight,
                        &accepted,
                        config.max_events,
                        config.max_inflight_publishes,
                        &stopped,
                    )
                    .await
                    {
                        publish_error = Some(error);
                    }
                }
                while let Some(result) = inflight.next().await {
                    match result {
                        Ok(()) => failures = 0,
                        Err(error) if publish_error.is_none() => publish_error = Some(error),
                        Err(_) => {}
                    }
                }
                if let Some(error) = publish_error {
                    return Err(error.into());
                }
                if disconnected && !should_stop(&stopped, &accepted, config.max_events, expires) {
                    tokio::time::sleep(Duration::from_millis(
                        backoff.delay_ms(failures, failures.min(10_000) as u16),
                    ))
                    .await;
                }
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_native_connect_failed",
                        "runtime": "BINANCE",
                        "attempt": failures,
                        "generation": generation,
                        "error": error.to_string(),
                    }))?
                );
                tokio::time::sleep(Duration::from_millis(
                    backoff.delay_ms(failures, failures.min(10_000) as u16),
                ))
                .await;
            }
        }
    }
    Ok(())
}

impl IngestorConfig {
    fn market_name(&self) -> &str {
        self.bindings
            .first()
            .map(|binding| binding.market.as_str())
            .unwrap_or("unknown")
    }
}

async fn run_okx_service(
    service: OkxService,
    url: String,
    bindings: Vec<RawBinding>,
    config: Arc<IngestorConfig>,
    accepted: Arc<AtomicU64>,
    coalesced_latest: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if bindings.is_empty() {
        return Ok(());
    }
    let publisher = RawPublisher::new(
        &config,
        match service {
            OkxService::Public => "okx-public",
            OkxService::Business => "okx-business",
        },
    )?;
    let subscriptions: Vec<OkxSubscription> = bindings
        .iter()
        .map(|binding| OkxSubscription {
            channel: binding.native_channel.clone(),
            inst_id: binding.native_symbol.clone(),
        })
        .collect();
    let binding_map: HashMap<String, RawBinding> = bindings
        .into_iter()
        .map(|binding| (binding.key(), binding))
        .collect();
    let backoff = BackoffPolicy {
        initial_ms: 250,
        maximum_ms: 30_000,
        multiplier: 2,
        jitter_bps: 2_000,
    }
    .validate()?;
    let expires = deadline(config.max_runtime_seconds);
    let service_name = match service {
        OkxService::Public => "public",
        OkxService::Business => "business",
    };
    let generation_path = format!("{}.okx-{service_name}", config.generation_state_path);
    let mut failures = 0_u32;
    let mut budget = ControlRequestBudget::default();
    while !should_stop(&stopped, &accepted, config.max_events, expires) {
        let generation = next_connection_generation(Path::new(&generation_path))?;
        match connect_async(&url).await {
            Ok((socket, _)) => {
                let (mut writer, mut reader) = socket.split();
                let command_id = generation.to_string();
                budget.permit(now_ns()?)?;
                writer
                    .send(Message::Text(subscription_command(
                        &command_id,
                        &subscriptions,
                    )?))
                    .await?;
                let mut pending = subscriptions
                    .iter()
                    .map(|item| (item.channel.clone(), item.inst_id.clone()))
                    .collect::<Vec<_>>();
                while !pending.is_empty() {
                    let message = tokio::time::timeout(Duration::from_secs(10), reader.next())
                        .await
                        .map_err(|_| "OKX subscription ACK timed out")?
                        .ok_or("OKX socket closed before subscription ACK")??;
                    if message.is_text() {
                        let payload: Value = serde_json::from_str(message.to_text()?)?;
                        parse_subscription_ack(&payload, &command_id, &mut pending)?;
                    }
                }
                let session_id = format!(
                    "okx-{}-{generation}-{}",
                    match service {
                        OkxService::Public => "public",
                        OkxService::Business => "business",
                    },
                    now_ns()?
                );
                let mut inflight = FuturesUnordered::<RawPublishFuture>::new();
                let mut latest = LatestStateBuffer::default();
                let mut latest_tick =
                    tokio::time::interval(Duration::from_millis(config.latest_state_flush_ms));
                latest_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                latest_tick.tick().await;
                let mut disconnected = false;
                let mut publish_error = None;
                while !should_stop(&stopped, &accepted, config.max_events, expires) {
                    if inflight.len() >= config.max_inflight_publishes {
                        match inflight.next().await {
                            Some(Ok(())) => {
                                failures = 0;
                                continue;
                            }
                            Some(Err(error)) => {
                                publish_error = Some(error);
                                break;
                            }
                            None => {
                                return Err("lossless publish window became inconsistent".into())
                            }
                        }
                    }
                    let read = tokio::time::timeout(
                        Duration::from_secs(config.heartbeat_seconds),
                        reader.next(),
                    );
                    tokio::pin!(read);
                    let outcome = tokio::select! {
                        biased;
                        _ = latest_tick.tick(), if !latest.is_empty() => {
                            if !flush_latest_concurrent(
                                &mut latest,
                                &publisher,
                                &mut inflight,
                                &accepted,
                                config.max_events,
                                config.max_inflight_publishes,
                                &stopped,
                            ).await? {
                                break;
                            }
                            failures = 0;
                            continue;
                        }
                        result = &mut read => Some(result),
                        completed = inflight.next(), if !inflight.is_empty() => {
                            match completed {
                                Some(Ok(())) => {
                                    failures = 0;
                                    continue;
                                }
                                Some(Err(error)) => {
                                    publish_error = Some(error);
                                    break;
                                }
                                None => {
                                return Err("lossless publish window became inconsistent".into())
                            }
                            }
                        }
                    };
                    let message = match outcome.expect("OKX read outcome is present") {
                        Ok(Some(Ok(message))) => message,
                        Ok(Some(Err(_))) | Ok(None) => {
                            disconnected = true;
                            break;
                        }
                        Err(_) => {
                            writer.send(Message::Text("ping".into())).await?;
                            match tokio::time::timeout(
                                Duration::from_secs(config.heartbeat_seconds),
                                reader.next(),
                            )
                            .await
                            {
                                Ok(Some(Ok(message)))
                                    if message.is_text() && message.to_text()? == "pong" =>
                                {
                                    continue
                                }
                                _ => {
                                    disconnected = true;
                                    break;
                                }
                            }
                        }
                    };
                    if let Message::Ping(payload) = message {
                        writer.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    if !message.is_text() {
                        continue;
                    }
                    let raw_text = message.to_text()?.to_owned();
                    let payload: Value = serde_json::from_str(&raw_text)?;
                    if payload.get("event").and_then(Value::as_str) == Some("notice") {
                        disconnected = true;
                        break;
                    }
                    if payload.get("event").is_some() {
                        return Err("unexpected OKX control event after subscription".into());
                    }
                    let argument = payload
                        .get("arg")
                        .and_then(Value::as_object)
                        .ok_or("OKX data arg is missing")?;
                    let channel = argument
                        .get("channel")
                        .and_then(Value::as_str)
                        .ok_or("OKX data channel is missing")?;
                    let instrument = argument
                        .get("instId")
                        .and_then(Value::as_str)
                        .ok_or("OKX data instrument is missing")?;
                    let binding = binding_map
                        .get(&format!("{channel}|{instrument}"))
                        .ok_or("OKX frame has no approved binding")?;
                    let frame = PendingRawFrame {
                        binding: binding.clone(),
                        session_id: session_id.clone(),
                        generation,
                        raw_frame: raw_text.into_bytes(),
                        received_at_ns: now_ns()?,
                    };
                    if frame.binding.delivery_class == DeliveryClass::LatestState {
                        if latest.push(frame) {
                            coalesced_latest.fetch_add(1, Ordering::AcqRel);
                        }
                        continue;
                    }
                    if !enqueue_lossless_frame(
                        frame,
                        &publisher,
                        &mut inflight,
                        &accepted,
                        config.max_events,
                        config.max_inflight_publishes,
                        &stopped,
                    )
                    .await?
                    {
                        break;
                    }
                }
                if publish_error.is_none() {
                    if let Err(error) = flush_latest_concurrent(
                        &mut latest,
                        &publisher,
                        &mut inflight,
                        &accepted,
                        config.max_events,
                        config.max_inflight_publishes,
                        &stopped,
                    )
                    .await
                    {
                        publish_error = Some(error);
                    }
                }
                while let Some(result) = inflight.next().await {
                    match result {
                        Ok(()) => failures = 0,
                        Err(error) if publish_error.is_none() => publish_error = Some(error),
                        Err(_) => {}
                    }
                }
                if let Some(error) = publish_error {
                    return Err(error.into());
                }
                if disconnected && !should_stop(&stopped, &accepted, config.max_events, expires) {
                    failures = failures.saturating_add(1);
                    eprintln!(
                        "{}",
                        serde_json::to_string(&json!({
                            "event": "qdl_native_session_disconnected",
                            "runtime": "OKX",
                            "service": format!("{:?}", service).to_ascii_uppercase(),
                            "attempt": failures,
                            "generation": generation,
                        }))?
                    );
                    tokio::time::sleep(Duration::from_millis(
                        backoff.delay_ms(failures, failures.min(10_000) as u16),
                    ))
                    .await;
                }
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_native_connect_failed",
                        "runtime": "OKX",
                        "service": format!("{:?}", service).to_ascii_uppercase(),
                        "attempt": failures,
                        "error": error.to_string(),
                    }))?
                );
                tokio::time::sleep(Duration::from_millis(
                    backoff.delay_ms(failures, failures.min(10_000) as u16),
                ))
                .await;
            }
        }
    }
    Ok(())
}

async fn run_okx(
    config: Arc<IngestorConfig>,
    accepted: Arc<AtomicU64>,
    coalesced_latest: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let public = config
        .bindings
        .iter()
        .filter(|binding| {
            OkxSubscription {
                channel: binding.native_channel.clone(),
                inst_id: binding.native_symbol.clone(),
            }
            .service()
                == Ok(OkxService::Public)
        })
        .cloned()
        .collect();
    let business = config
        .bindings
        .iter()
        .filter(|binding| {
            OkxSubscription {
                channel: binding.native_channel.clone(),
                inst_id: binding.native_symbol.clone(),
            }
            .service()
                == Ok(OkxService::Business)
        })
        .cloned()
        .collect();
    tokio::try_join!(
        run_okx_service(
            OkxService::Public,
            config.websocket_url.clone(),
            public,
            config.clone(),
            accepted.clone(),
            coalesced_latest.clone(),
            stopped.clone(),
        ),
        run_okx_service(
            OkxService::Business,
            config
                .business_websocket_url
                .clone()
                .ok_or("OKX business WebSocket URL is required")?,
            business,
            config,
            accepted,
            coalesced_latest,
            stopped,
        ),
    )?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| "failed to install rustls ring crypto provider")?;
    let config_path = env::args()
        .nth(1)
        .ok_or("usage: qdl-native-raw-ingestor CONFIG.json")?;
    let config: IngestorConfig = serde_json::from_slice(&tokio::fs::read(config_path).await?)?;
    config.validate()?;
    let config = Arc::new(config);
    let accepted = Arc::new(AtomicU64::new(0));
    let coalesced_latest = Arc::new(AtomicU64::new(0));
    let stopped = Arc::new(AtomicBool::new(false));
    let stop_signal = stopped.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            stop_signal.store(true, Ordering::Release);
        }
    });
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "qdl_native_raw_ingestor_started",
            "runtime": format!("{:?}", config.runtime).to_ascii_uppercase(),
            "authority": "RUST_SHADOW",
            "bindings": config.bindings.len(),
            "latest_state_flush_ms": config.latest_state_flush_ms,
            "production_public_writes": 0,
            "production_legacy_writes": 0,
        }))?
    );
    let supervisor_backoff = BackoffPolicy {
        initial_ms: 500,
        maximum_ms: 30_000,
        multiplier: 2,
        jitter_bps: 2_000,
    }
    .validate()?;
    let supervisor_deadline = deadline(config.max_runtime_seconds);
    let mut supervisor_failures = 0_u32;
    while !should_stop(&stopped, &accepted, config.max_events, supervisor_deadline) {
        let accepted_before = accepted.load(Ordering::Acquire);
        let result = match config.runtime {
            ProviderRuntime::Binance => {
                run_binance(
                    config.clone(),
                    accepted.clone(),
                    coalesced_latest.clone(),
                    stopped.clone(),
                )
                .await
            }
            ProviderRuntime::Okx => {
                run_okx(
                    config.clone(),
                    accepted.clone(),
                    coalesced_latest.clone(),
                    stopped.clone(),
                )
                .await
            }
        };
        match result {
            Ok(()) => break,
            Err(error) => {
                if error
                    .downcast_ref::<KafkaTransportError>()
                    .is_some_and(|value| value.retry_class() == RetryClass::NonRetryable)
                {
                    return Err(error);
                }
                if should_stop(&stopped, &accepted, config.max_events, supervisor_deadline) {
                    break;
                }
                if accepted.load(Ordering::Acquire) > accepted_before {
                    supervisor_failures = 0;
                }
                supervisor_failures = supervisor_failures.saturating_add(1);
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_native_runtime_retry",
                        "runtime": format!("{:?}", config.runtime).to_ascii_uppercase(),
                        "attempt": supervisor_failures,
                        "error": error.to_string(),
                    }))?
                );
                tokio::time::sleep(Duration::from_millis(
                    supervisor_backoff
                        .delay_ms(supervisor_failures, supervisor_failures.min(10_000) as u16),
                ))
                .await;
            }
        }
    }
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "qdl_native_raw_ingestor_stopped",
            "accepted_raw_frames": accepted.load(Ordering::Acquire),
            "coalesced_latest_state_frames": coalesced_latest.load(Ordering::Acquire),
        }))?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        next_connection_generation, DeliveryClass, LatestStateBuffer, PendingRawFrame,
        ProviderRuntime, RawBinding, RawFeed,
    };
    use std::fs;
    use std::path::PathBuf;

    fn generation_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "qdl-native-generation-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn binding(feed: RawFeed, delivery_class: DeliveryClass) -> RawBinding {
        RawBinding {
            provider: "BINANCE_DIRECT".into(),
            venue: "BINANCE".into(),
            market: "USDM".into(),
            product_type: "PERPETUAL".into(),
            native_symbol: "BTCUSDT".into(),
            native_channel: match feed {
                RawFeed::Trade => "btcusdt@trade",
                RawFeed::Quote => "btcusdt@bookTicker",
                RawFeed::Bar => "candle1m",
            }
            .into(),
            subscription_id: "test-source".into(),
            adapter_version: "test/2.0.0".into(),
            instrument_catalog_revision: 1,
            feed,
            delivery_class,
        }
    }

    #[test]
    fn feed_delivery_contract_allows_only_latest_quote_and_lossless_trade_bar() {
        assert!(binding(RawFeed::Quote, DeliveryClass::LatestState)
            .validate(ProviderRuntime::Binance)
            .is_ok());
        assert!(binding(RawFeed::Trade, DeliveryClass::Lossless)
            .validate(ProviderRuntime::Binance)
            .is_ok());
        assert!(binding(RawFeed::Bar, DeliveryClass::Lossless)
            .validate(ProviderRuntime::Binance)
            .is_ok());
        assert!(binding(RawFeed::Trade, DeliveryClass::LatestState)
            .validate(ProviderRuntime::Binance)
            .is_err());
        assert!(binding(RawFeed::Quote, DeliveryClass::Lossless)
            .validate(ProviderRuntime::Binance)
            .is_err());
    }

    #[test]
    fn lossless_and_latest_state_delivery_contracts_are_provider_neutral() {
        let mut okx_trade = binding(RawFeed::Trade, DeliveryClass::Lossless);
        okx_trade.provider = "OKX_DIRECT".into();
        okx_trade.venue = "OKX".into();
        okx_trade.market = "SWAP".into();
        okx_trade.product_type = "PERPETUAL".into();
        okx_trade.native_symbol = "BTC-USDT-SWAP".into();
        okx_trade.native_channel = "trades".into();
        assert!(okx_trade.validate(ProviderRuntime::Okx).is_ok());

        let mut okx_quote = okx_trade.clone();
        okx_quote.feed = RawFeed::Quote;
        okx_quote.delivery_class = DeliveryClass::LatestState;
        okx_quote.native_channel = "bbo-tbt".into();
        assert!(okx_quote.validate(ProviderRuntime::Okx).is_ok());

        okx_trade.delivery_class = DeliveryClass::LatestState;
        okx_quote.delivery_class = DeliveryClass::Lossless;
        assert!(okx_trade.validate(ProviderRuntime::Okx).is_err());
        assert!(okx_quote.validate(ProviderRuntime::Okx).is_err());
    }

    #[test]
    fn latest_state_buffer_keeps_last_authentic_frame_per_binding() {
        let mut buffer = LatestStateBuffer::default();
        let first = PendingRawFrame {
            binding: binding(RawFeed::Quote, DeliveryClass::LatestState),
            session_id: "session-1".into(),
            generation: 1,
            raw_frame: br#"{"u":1}"#.to_vec(),
            received_at_ns: 10,
        };
        let mut last = first.clone();
        last.raw_frame = br#"{"u":2}"#.to_vec();
        last.received_at_ns = 20;
        assert!(!buffer.push(first));
        assert!(buffer.push(last));
        let values = buffer.drain();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].raw_frame, br#"{"u":2}"#);
        assert_eq!(values[0].received_at_ns, 20);
        assert!(buffer.is_empty());
    }

    #[test]
    fn durable_generation_advances_across_reopens_and_corruption_fails_closed() {
        let directory = generation_path("restart");
        let state = directory.join("binance-usdm");
        assert_eq!(next_connection_generation(&state).unwrap(), 1);
        assert_eq!(next_connection_generation(&state).unwrap(), 2);
        assert_eq!(fs::read_to_string(&state).unwrap(), "2\n");
        fs::write(&state, "not-a-generation\n").unwrap();
        assert!(next_connection_generation(&state).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn venue_service_generation_files_are_independent() {
        let directory = generation_path("services");
        let public = directory.join("okx-swap.okx-public");
        let business = directory.join("okx-swap.okx-business");
        assert_eq!(next_connection_generation(&public).unwrap(), 1);
        assert_eq!(next_connection_generation(&business).unwrap(), 1);
        assert_eq!(next_connection_generation(&public).unwrap(), 2);
        assert_eq!(fs::read_to_string(&business).unwrap(), "1\n");
        fs::remove_dir_all(directory).unwrap();
    }
}
