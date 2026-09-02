#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{ErrorKind, Write};
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::future::try_join_all;
use futures_util::stream::FuturesUnordered;
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use qdl_contracts::qdl::provider::v1::{
    CaptureBoundary, RawProviderEnvelope, TransportCompression, TransportProtocol,
};
use qdl_core::backoff::BackoffPolicy;
use qdl_core::binance::{decode_subscribed, validate_stream as validate_binance_stream};
use qdl_core::binance_session::{
    parse_subscription_reply as parse_binance_subscription_reply,
    subscription_command as binance_subscription_command,
};
use qdl_core::okx::{
    parse_subscription_ack as parse_okx_subscription_ack,
    subscription_command as okx_subscription_command, ControlRequestBudget, OkxService,
    OkxSubscription,
};
use qdl_core::transport::{DurableRecord, RetryClass};
use qdl_kafka::{
    shutdown_signal, FencedKafkaSink, KafkaTlsConfig, KafkaTransportConfig, KafkaTransportError,
    PendingKafkaAppend,
};
use qdl_venue_core::authority::{AuthorityMode, AuthorityRecord, PublicationContext, SinkTarget};
use qdl_venue_core::backpressure::DeliveryClass;
use serde::{Deserialize, Serialize};
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
    Book,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawL2Config {
    provider_protocol: String,
    depth_per_side: usize,
    rest_snapshot_url: Option<String>,
    snapshot_refresh_seconds: Option<u64>,
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
    #[serde(default)]
    l2: Option<RawL2Config>,
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
                | (
                    RawFeed::Trade | RawFeed::Bar | RawFeed::Book,
                    DeliveryClass::Lossless
                )
        ) {
            return Err("raw binding feed/delivery class is invalid".into());
        }
        match runtime {
            ProviderRuntime::Binance => {
                if self.venue != "BINANCE" || !matches!(self.market.as_str(), "USDM" | "SPOT") {
                    return Err("Binance raw binding identity is invalid".into());
                }
                validate_binance_stream(&self.native_channel)?;
                if self.feed == RawFeed::Book {
                    let Some(l2) = &self.l2 else {
                        return Err("Binance BOOK binding requires L2 configuration".into());
                    };
                    if l2.provider_protocol != "BINANCE_DIFF_DEPTH"
                        || !matches!(l2.depth_per_side, 5 | 10 | 20 | 50 | 100 | 500 | 1_000)
                        || l2.rest_snapshot_url.as_deref()
                            != Some(match self.market.as_str() {
                                "USDM" => "https://fapi.binance.com/fapi/v1/depth",
                                "SPOT" => "https://api.binance.com/api/v3/depth",
                                _ => unreachable!("Binance market validated above"),
                            })
                        || !matches!(l2.snapshot_refresh_seconds, Some(5..=300))
                    {
                        return Err("Binance BOOK binding L2 configuration is invalid".into());
                    }
                } else if self.l2.is_some() {
                    return Err("non-BOOK raw binding cannot carry L2 configuration".into());
                }
            }
            ProviderRuntime::Okx => {
                if self.venue != "OKX"
                    || !matches!(self.market.as_str(), "SWAP" | "FUTURES" | "SPOT")
                {
                    return Err("OKX raw binding identity is invalid".into());
                }
                if self.feed == RawFeed::Book {
                    let Some(l2) = &self.l2 else {
                        return Err("OKX BOOK binding requires L2 configuration".into());
                    };
                    if l2.provider_protocol != "OKX_PUBLIC_BOOKS"
                        || !(1..=10_000).contains(&l2.depth_per_side)
                        || l2.rest_snapshot_url.is_some()
                        || !matches!(l2.snapshot_refresh_seconds, Some(5..=300))
                        || self.native_channel != "books"
                    {
                        return Err("OKX BOOK binding L2 configuration is invalid".into());
                    }
                } else if self.l2.is_some() {
                    return Err("non-BOOK raw binding cannot carry L2 configuration".into());
                }
            }
        }
        Ok(())
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
    session_liveness_dir: String,
    session_liveness_write_interval_ms: u64,
    max_inflight_publishes: usize,
    #[serde(default = "default_max_subscriptions_per_connection")]
    max_subscriptions_per_connection: usize,
    latest_state_flush_ms: u64,
    authority: AuthorityRecord,
    bindings: Vec<RawBinding>,
}

fn default_max_subscriptions_per_connection() -> usize {
    100
}

fn partition_bindings(bindings: &[RawBinding], max_subscriptions: usize) -> Vec<Vec<RawBinding>> {
    bindings
        .chunks(max_subscriptions)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn partition_feed_lanes(bindings: &[RawBinding], max_subscriptions: usize) -> Vec<Vec<RawBinding>> {
    // Stateful BOOK sessions need an independent recovery cadence. BAR, TRADE
    // and QUOTE stay isolated as well, so rotating a book snapshot connection
    // never disconnects another feed class. These are lanes inside one shared
    // venue/market role, never symbol workers.
    [RawFeed::Book, RawFeed::Bar, RawFeed::Trade, RawFeed::Quote]
        .into_iter()
        .flat_map(|feed| {
            let lane = bindings
                .iter()
                .filter(|binding| binding.feed == feed)
                .cloned()
                .collect::<Vec<_>>();
            partition_bindings(&lane, max_subscriptions)
        })
        .collect()
}

fn partition_binance_bindings(
    bindings: &[RawBinding],
    max_subscriptions: usize,
) -> Vec<Vec<RawBinding>> {
    partition_feed_lanes(bindings, max_subscriptions)
}

fn partition_okx_bindings(
    bindings: &[RawBinding],
    max_subscriptions: usize,
) -> Vec<Vec<RawBinding>> {
    partition_feed_lanes(bindings, max_subscriptions)
}

impl IngestorConfig {
    fn validate(&self) -> Result<(), String> {
        self.authority.validate()?;
        if !matches!(
            self.authority.mode,
            AuthorityMode::RustShadow | AuthorityMode::RustPrimary
        ) || self.websocket_url.trim().is_empty()
            || !self.websocket_url.starts_with("wss://")
            || self.raw_stream.trim().is_empty()
            || self.shard_id.trim().is_empty()
            || self.lease_epoch == 0
            || self.partition_plan_epoch == 0
            || self.config_revision == 0
            || self.heartbeat_seconds == 0
            || self.heartbeat_seconds >= 30
            || self.metrics_every_events == 0
            || !(250..=5_000).contains(&self.session_liveness_write_interval_ms)
            || self.max_inflight_publishes == 0
            || self.max_inflight_publishes > 4_096
            || self.max_subscriptions_per_connection == 0
            || self.max_subscriptions_per_connection > 1_024
            || self.latest_state_flush_ms == 0
            || self.latest_state_flush_ms > 1_000
            || self.bindings.is_empty()
        {
            return Err(
                "native raw ingestor config is invalid or not a shared Rust authority".into(),
            );
        }
        let generation_path = Path::new(&self.generation_state_path);
        if !generation_path.is_absolute()
            || generation_path
                .components()
                .any(|component| matches!(component, Component::ParentDir))
        {
            return Err("generation state path must be absolute without parent traversal".into());
        }
        let session_liveness_dir = Path::new(&self.session_liveness_dir);
        if !session_liveness_dir.is_absolute()
            || session_liveness_dir
                .components()
                .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(
                "session liveness directory must be absolute without parent traversal".into(),
            );
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
        if self.runtime == ProviderRuntime::Binance
            && !self.websocket_url.trim_end_matches('/').ends_with("/ws")
        {
            return Err("Binance native WebSocket URL must use the control /ws endpoint".into());
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

fn authority_mode_name(mode: &AuthorityMode) -> &'static str {
    match mode {
        AuthorityMode::RustShadow => "RUST_SHADOW",
        AuthorityMode::RustCanary => "RUST_CANARY",
        AuthorityMode::RustPrimary => "RUST_PRIMARY",
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

const SESSION_LIVENESS_SCHEMA: &str = "qdl.provider-session-liveness.v1";

#[derive(Serialize)]
struct ProviderSessionLiveness<'a> {
    schema: &'static str,
    source_session_id: &'a str,
    connection_generation: u64,
    state: &'static str,
    last_transport_at_ns: i64,
    updated_at_ns: i64,
    config_revision: u64,
}

struct SessionLivenessWriter {
    path: PathBuf,
    config_revision: u64,
    write_interval_ns: i64,
    last_written_ns: Option<i64>,
}

impl SessionLivenessWriter {
    fn new(
        config: &IngestorConfig,
        lane: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if lane.is_empty()
            || lane.len() > 120
            || !lane.chars().all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
            })
        {
            return Err("session liveness lane is invalid".into());
        }
        Ok(Self {
            path: Path::new(&config.session_liveness_dir).join(format!("{lane}.json")),
            config_revision: config.config_revision,
            write_interval_ns: i64::try_from(config.session_liveness_write_interval_ms)?
                .checked_mul(1_000_000)
                .ok_or("session liveness write interval overflow")?,
            last_written_ns: None,
        })
    }

    fn live(
        &mut self,
        session_id: &str,
        generation: u64,
        transport_at_ns: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.write(session_id, generation, "LIVE", transport_at_ns, false)
    }

    fn disconnected(
        &mut self,
        session_id: &str,
        generation: u64,
        transport_at_ns: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.write(
            session_id,
            generation,
            "DISCONNECTED",
            transport_at_ns,
            true,
        )
    }

    fn write(
        &mut self,
        session_id: &str,
        generation: u64,
        state: &'static str,
        transport_at_ns: i64,
        force: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if session_id.is_empty()
            || session_id.len() > 256
            || generation == 0
            || transport_at_ns <= 0
        {
            return Err("provider session liveness identity is invalid".into());
        }
        if !force
            && self
                .last_written_ns
                .is_some_and(|last| transport_at_ns.saturating_sub(last) < self.write_interval_ns)
        {
            return Ok(());
        }
        let parent = self
            .path
            .parent()
            .ok_or("session liveness path has no parent")?;
        fs::create_dir_all(parent)?;
        let payload = serde_json::to_vec(&ProviderSessionLiveness {
            schema: SESSION_LIVENESS_SCHEMA,
            source_session_id: session_id,
            connection_generation: generation,
            state,
            last_transport_at_ns: transport_at_ns,
            updated_at_ns: now_ns()?,
            config_revision: self.config_revision,
        })?;
        let temporary =
            self.path
                .with_extension(format!("tmp-{}-{}", std::process::id(), now_ns()?));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&payload)?;
        file.sync_all()?;
        fs::rename(&temporary, &self.path)?;
        File::open(parent)?.sync_all()?;
        self.last_written_ns = Some(transport_at_ns);
        Ok(())
    }
}

fn capture_id(session: &str, generation: u64, received_at_ns: i64, frame: &[u8]) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(session.as_bytes());
    digest.update(generation.to_be_bytes());
    digest.update(received_at_ns.to_be_bytes());
    digest.update(frame);
    digest.finalize()[..16].to_vec()
}

/// One already-validated provider frame awaiting durable publication.
/// Grouping its immutable coordinates keeps retry behavior coupled to the
/// exact record identity without widening the publisher API.
struct RawFrameRef<'a> {
    binding: &'a RawBinding,
    session_id: &'a str,
    generation: u64,
    raw_frame: &'a [u8],
    received_at_ns: i64,
    transport_protocol: TransportProtocol,
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
        transport_protocol: TransportProtocol,
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
            transport_protocol: transport_protocol as i32,
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
        let target = match self.authority.mode {
            AuthorityMode::RustShadow => SinkTarget::ShadowRaw,
            AuthorityMode::RustPrimary => SinkTarget::PrimaryRaw,
            AuthorityMode::RustCanary => {
                unreachable!("validated native raw ingestor cannot run RUST_CANARY")
            }
        };
        PublicationContext {
            slice_id: self.authority.slice_id.clone(),
            authority_revision: self.authority.revision,
            shard_id: self.shard_id.clone(),
            lease_epoch: self.lease_epoch,
            target,
        }
    }

    async fn enqueue_with_retry(
        &self,
        frame: RawFrameRef<'_>,
        stopped: &AtomicBool,
    ) -> Result<PendingKafkaAppend, KafkaTransportError> {
        let record = self.record(
            frame.binding,
            frame.session_id,
            frame.generation,
            frame.raw_frame,
            frame.received_at_ns,
            frame.transport_protocol,
        );
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
                            "runtime": frame.binding.venue.as_str(),
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
    transport_protocol: TransportProtocol,
}

fn pending_binance_frame(
    bindings: &HashMap<String, RawBinding>,
    session_id: &str,
    generation: u64,
    raw_text: String,
    received_at_ns: i64,
) -> Result<PendingRawFrame, std::io::Error> {
    let decoded = decode_subscribed(raw_text.clone())
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
    let binding = bindings.get(&decoded.stream).cloned().ok_or_else(|| {
        std::io::Error::new(
            ErrorKind::InvalidData,
            "Binance frame has no approved binding",
        )
    })?;
    Ok(PendingRawFrame {
        binding,
        session_id: session_id.into(),
        generation,
        raw_frame: raw_text.into_bytes(),
        received_at_ns,
        transport_protocol: TransportProtocol::Websocket,
    })
}

fn pending_okx_frame(
    bindings: &HashMap<String, RawBinding>,
    session_id: &str,
    generation: u64,
    raw_text: String,
    received_at_ns: i64,
) -> Result<PendingRawFrame, std::io::Error> {
    let payload: Value = serde_json::from_str(&raw_text)
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
    if payload.get("event").is_some() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "OKX control frame cannot be published as provider data",
        ));
    }
    let argument = payload
        .get("arg")
        .and_then(Value::as_object)
        .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, "OKX data arg is missing"))?;
    let channel = argument
        .get("channel")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidData, "OKX data channel is missing")
        })?;
    let instrument = argument
        .get("instId")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidData, "OKX data instrument is missing")
        })?;
    let binding = bindings
        .get(&format!("{channel}|{instrument}"))
        .cloned()
        .ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidData, "OKX frame has no approved binding")
        })?;
    Ok(PendingRawFrame {
        binding,
        session_id: session_id.into(),
        generation,
        raw_frame: raw_text.into_bytes(),
        received_at_ns,
        transport_protocol: TransportProtocol::Websocket,
    })
}

const MAX_BOOK_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;

async fn fetch_binance_book_snapshot(
    client: reqwest::Client,
    binding: RawBinding,
    session_id: String,
    generation: u64,
) -> Result<PendingRawFrame, String> {
    let l2 = binding
        .l2
        .as_ref()
        .ok_or_else(|| "Binance BOOK binding is missing L2 configuration".to_owned())?;
    let endpoint = l2
        .rest_snapshot_url
        .as_deref()
        .ok_or_else(|| "Binance BOOK binding is missing REST snapshot URL".to_owned())?;
    let policy = BackoffPolicy {
        initial_ms: 100,
        maximum_ms: 1_000,
        multiplier: 2,
        jitter_bps: 2_000,
    }
    .validate()
    .map_err(|error| error.to_string())?;
    let mut last_error = "Binance depth snapshot did not run".to_owned();
    for attempt in 1..=4_u32 {
        let response = client
            .get(endpoint)
            .query(&[
                ("symbol", binding.native_symbol.as_str()),
                ("limit", &l2.depth_per_side.to_string()),
            ])
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => {
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|error| format!("Binance depth snapshot body failed: {error}"))?;
                if bytes.is_empty() || bytes.len() > MAX_BOOK_SNAPSHOT_BYTES {
                    return Err("Binance depth snapshot body is outside bounded size".into());
                }
                let payload: Value = serde_json::from_slice(&bytes)
                    .map_err(|_| "Binance depth snapshot is not JSON".to_owned())?;
                if payload
                    .get("lastUpdateId")
                    .and_then(Value::as_u64)
                    .is_none()
                    || payload.get("bids").and_then(Value::as_array).is_none()
                    || payload.get("asks").and_then(Value::as_array).is_none()
                {
                    return Err("Binance depth snapshot misses documented fields".into());
                }
                return Ok(PendingRawFrame {
                    binding,
                    session_id,
                    generation,
                    raw_frame: bytes.to_vec(),
                    received_at_ns: now_ns().map_err(|error| error.to_string())?,
                    transport_protocol: TransportProtocol::Http,
                });
            }
            Ok(response) => {
                let status = response.status();
                last_error = format!("Binance depth snapshot returned HTTP {status}");
                if !(status.as_u16() == 429 || status.is_server_error()) {
                    return Err(last_error);
                }
            }
            Err(error) => last_error = format!("Binance depth snapshot transport failed: {error}"),
        }
        if attempt < 4 {
            tokio::time::sleep(Duration::from_millis(
                policy.delay_ms(attempt, attempt as u16),
            ))
            .await;
        }
    }
    Err(last_error)
}

async fn fetch_binance_book_snapshots(
    client: &reqwest::Client,
    bindings: &HashMap<String, RawBinding>,
    session_id: &str,
    generation: u64,
) -> Result<Vec<PendingRawFrame>, String> {
    let requests = bindings
        .values()
        .filter(|binding| binding.feed == RawFeed::Book)
        .cloned()
        .map(|binding| {
            fetch_binance_book_snapshot(client.clone(), binding, session_id.to_owned(), generation)
        });
    let mut frames = try_join_all(requests).await?;
    frames.sort_by(|left, right| {
        left.binding
            .native_channel
            .cmp(&right.binding.native_channel)
    });
    Ok(frames)
}

fn book_snapshot_renewal_period(bindings: &HashMap<String, RawBinding>) -> Option<Duration> {
    if bindings.is_empty()
        || bindings
            .values()
            .any(|binding| binding.feed != RawFeed::Book)
    {
        return None;
    }
    bindings
        .values()
        .filter_map(|binding| {
            binding
                .l2
                .as_ref()
                .and_then(|l2| l2.snapshot_refresh_seconds)
        })
        .min()
        .map(Duration::from_secs)
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
            RawFrameRef {
                binding: &frame.binding,
                session_id: &frame.session_id,
                generation: frame.generation,
                raw_frame: &frame.raw_frame,
                received_at_ns: frame.received_at_ns,
                transport_protocol: frame.transport_protocol,
            },
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

struct PendingPublishWindow<'a> {
    publisher: &'a RawPublisher,
    latest: &'a mut LatestStateBuffer,
    inflight: &'a mut FuturesUnordered<RawPublishFuture>,
    accepted: &'a AtomicU64,
    coalesced_latest: &'a AtomicU64,
    max_events: u64,
    max_inflight: usize,
    stopped: &'a AtomicBool,
}

async fn publish_pending_frame(
    frame: PendingRawFrame,
    window: &mut PendingPublishWindow<'_>,
) -> Result<bool, KafkaTransportError> {
    if frame.binding.delivery_class == DeliveryClass::LatestState {
        if window.latest.push(frame) {
            window.coalesced_latest.fetch_add(1, Ordering::AcqRel);
        }
        return Ok(true);
    }
    enqueue_lossless_frame(
        frame,
        window.publisher,
        window.inflight,
        window.accepted,
        window.max_events,
        window.max_inflight,
        window.stopped,
    )
    .await
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

async fn run_binance_connection(
    config: Arc<IngestorConfig>,
    shard_index: usize,
    shard_bindings: Vec<RawBinding>,
    accepted: Arc<AtomicU64>,
    coalesced_latest: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bindings: HashMap<String, RawBinding> = shard_bindings
        .into_iter()
        .map(|binding| (binding.native_channel.clone(), binding))
        .collect();
    let streams = bindings.keys().cloned().collect::<Vec<_>>();
    let url = config.websocket_url.clone();
    let publisher = RawPublisher::new(&config, &format!("binance-{shard_index:03}"))?;
    let snapshot_client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .build()?;
    let snapshot_period = book_snapshot_renewal_period(&bindings);
    let backoff = BackoffPolicy {
        initial_ms: 250,
        maximum_ms: 30_000,
        multiplier: 2,
        jitter_bps: 2_000,
    }
    .validate()?;
    let generation_path = format!("{}.binance-{shard_index:03}", config.generation_state_path);
    let expires = deadline(config.max_runtime_seconds);
    let mut failures = 0_u32;
    while !should_stop(&stopped, &accepted, config.max_events, expires) {
        let generation = next_connection_generation(Path::new(&generation_path))?;
        match connect_async(&url).await {
            Ok((mut socket, _)) => {
                socket
                    .send(Message::Text(binance_subscription_command(
                        generation, true, &streams,
                    )?))
                    .await?;
                let mut acknowledged = false;
                let mut pre_ack_frames = VecDeque::new();
                while !acknowledged {
                    let message = tokio::time::timeout(Duration::from_secs(10), socket.next())
                        .await
                        .map_err(|_| "Binance subscription ACK timed out")?
                        .ok_or("Binance socket closed before subscription ACK")??;
                    match message {
                        Message::Ping(payload) => {
                            socket.send(Message::Pong(payload)).await?;
                        }
                        Message::Pong(_) => {}
                        Message::Text(payload) => {
                            if parse_binance_subscription_reply(payload.as_ref(), generation)? {
                                acknowledged = true;
                            } else {
                                if pre_ack_frames.len() >= config.max_inflight_publishes {
                                    return Err(
                                        "Binance pre-ACK frame buffer exceeded durable publish bound"
                                            .into(),
                                    );
                                }
                                pre_ack_frames.push_back((payload.to_string(), now_ns()?));
                            }
                        }
                        Message::Close(_) => {
                            return Err("Binance socket closed before subscription ACK".into());
                        }
                        _ => {}
                    }
                }
                let session_id = format!(
                    "binance-{}-{shard_index:03}-{generation}-{}",
                    config.market_name(),
                    now_ns()?
                );
                let mut liveness =
                    SessionLivenessWriter::new(&config, &format!("binance-{shard_index:03}"))?;
                liveness.live(&session_id, generation, now_ns()?)?;
                let mut inflight = FuturesUnordered::<RawPublishFuture>::new();
                let mut latest = LatestStateBuffer::default();
                let mut latest_tick =
                    tokio::time::interval(Duration::from_millis(config.latest_state_flush_ms));
                latest_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                latest_tick.tick().await;
                let mut book_snapshot_tick = snapshot_period.map(tokio::time::interval);
                if let Some(tick) = book_snapshot_tick.as_mut() {
                    // Consume the interval's immediate first tick. The initial
                    // REST snapshot below is explicit and ordered after any
                    // pre-ACK deltas, so the Rust adapter can bridge it.
                    tick.tick().await;
                }
                let mut disconnected = false;
                let mut publish_error = None;
                let mut exhausted = false;
                while let Some((raw_text, received_at_ns)) = pre_ack_frames.pop_front() {
                    let frame = pending_binance_frame(
                        &bindings,
                        &session_id,
                        generation,
                        raw_text,
                        received_at_ns,
                    )?;
                    if !publish_pending_frame(
                        frame,
                        &mut PendingPublishWindow {
                            publisher: &publisher,
                            latest: &mut latest,
                            inflight: &mut inflight,
                            accepted: &accepted,
                            coalesced_latest: &coalesced_latest,
                            max_events: config.max_events,
                            max_inflight: config.max_inflight_publishes,
                            stopped: &stopped,
                        },
                    )
                    .await?
                    {
                        exhausted = true;
                        break;
                    }
                }
                if !exhausted && snapshot_period.is_some() {
                    for frame in fetch_binance_book_snapshots(
                        &snapshot_client,
                        &bindings,
                        &session_id,
                        generation,
                    )
                    .await
                    .map_err(|error| std::io::Error::new(ErrorKind::Other, error))?
                    {
                        if !publish_pending_frame(
                            frame,
                            &mut PendingPublishWindow {
                                publisher: &publisher,
                                latest: &mut latest,
                                inflight: &mut inflight,
                                accepted: &accepted,
                                coalesced_latest: &coalesced_latest,
                                max_events: config.max_events,
                                max_inflight: config.max_inflight_publishes,
                                stopped: &stopped,
                            },
                        )
                        .await?
                        {
                            exhausted = true;
                            break;
                        }
                    }
                }
                while !exhausted && !should_stop(&stopped, &accepted, config.max_events, expires) {
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
                        socket.next(),
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
                        _ = async {
                            match book_snapshot_tick.as_mut() {
                                Some(tick) => {
                                    tick.tick().await;
                                }
                                None => std::future::pending::<()>().await,
                            }
                        }, if snapshot_period.is_some() => {
                            for frame in fetch_binance_book_snapshots(
                                &snapshot_client,
                                &bindings,
                                &session_id,
                                generation,
                            )
                            .await
                            .map_err(|error| std::io::Error::new(ErrorKind::Other, error))?
                            {
                                if !publish_pending_frame(
                                    frame,
                                    &mut PendingPublishWindow {
                                        publisher: &publisher,
                                        latest: &mut latest,
                                        inflight: &mut inflight,
                                        accepted: &accepted,
                                        coalesced_latest: &coalesced_latest,
                                        max_events: config.max_events,
                                        max_inflight: config.max_inflight_publishes,
                                        stopped: &stopped,
                                    },
                                )
                                .await?
                                {
                                    exhausted = true;
                                    break;
                                }
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
                    let message = match outcome.expect("Binance read outcome is present") {
                        Ok(Some(Ok(message))) => {
                            if !matches!(message, Message::Close(_)) {
                                liveness.live(&session_id, generation, now_ns()?)?;
                            }
                            message
                        }
                        Ok(Some(Err(error))) => {
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
                            liveness.disconnected(&session_id, generation, now_ns()?)?;
                            disconnected = true;
                            break;
                        }
                        Ok(None) => {
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
                            liveness.disconnected(&session_id, generation, now_ns()?)?;
                            disconnected = true;
                            break;
                        }
                        Err(_) => {
                            socket.send(Message::Ping(Vec::new())).await?;
                            match tokio::time::timeout(
                                Duration::from_secs(config.heartbeat_seconds),
                                socket.next(),
                            )
                            .await
                            {
                                Ok(Some(Ok(Message::Pong(_)))) => {
                                    liveness.live(&session_id, generation, now_ns()?)?;
                                    continue;
                                }
                                Ok(Some(Ok(message))) => {
                                    if !matches!(message, Message::Close(_)) {
                                        liveness.live(&session_id, generation, now_ns()?)?;
                                    }
                                    message
                                }
                                _ => {
                                    failures = failures.saturating_add(1);
                                    eprintln!(
                                        "{}",
                                        serde_json::to_string(&json!({
                                            "event": "qdl_native_session_heartbeat_failed",
                                            "runtime": "BINANCE",
                                            "attempt": failures,
                                            "generation": generation,
                                        }))?
                                    );
                                    liveness.disconnected(&session_id, generation, now_ns()?)?;
                                    disconnected = true;
                                    break;
                                }
                            }
                        }
                    };
                    if let Message::Ping(payload) = message {
                        socket.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    if matches!(message, Message::Pong(_)) {
                        continue;
                    }
                    if matches!(message, Message::Close(_)) {
                        liveness.disconnected(&session_id, generation, now_ns()?)?;
                        disconnected = true;
                        break;
                    }
                    if !message.is_text() {
                        continue;
                    }
                    let raw_text = message.into_text()?.to_string();
                    let frame = pending_binance_frame(
                        &bindings,
                        &session_id,
                        generation,
                        raw_text,
                        now_ns()?,
                    )?;
                    if !publish_pending_frame(
                        frame,
                        &mut PendingPublishWindow {
                            publisher: &publisher,
                            latest: &mut latest,
                            inflight: &mut inflight,
                            accepted: &accepted,
                            coalesced_latest: &coalesced_latest,
                            max_events: config.max_events,
                            max_inflight: config.max_inflight_publishes,
                            stopped: &stopped,
                        },
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
                liveness.disconnected(&session_id, generation, now_ns()?)?;
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

async fn run_binance(
    config: Arc<IngestorConfig>,
    accepted: Arc<AtomicU64>,
    coalesced_latest: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let shards =
        partition_binance_bindings(&config.bindings, config.max_subscriptions_per_connection);
    let futures = shards.into_iter().enumerate().map(|(index, bindings)| {
        run_binance_connection(
            config.clone(),
            index + 1,
            bindings,
            accepted.clone(),
            coalesced_latest.clone(),
            stopped.clone(),
        )
    });
    try_join_all(futures).await?;
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

struct OkxServiceShard {
    service: OkxService,
    shard_index: usize,
    url: String,
    bindings: Vec<RawBinding>,
}

async fn run_okx_service(
    shard: OkxServiceShard,
    config: Arc<IngestorConfig>,
    accepted: Arc<AtomicU64>,
    coalesced_latest: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let OkxServiceShard {
        service,
        shard_index,
        url,
        bindings,
    } = shard;
    if bindings.is_empty() {
        return Ok(());
    }
    let publisher = RawPublisher::new(
        &config,
        &format!(
            "{}-{shard_index:03}",
            match service {
                OkxService::Public => "okx-public",
                OkxService::Business => "okx-business",
            }
        ),
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
    let generation_path = format!(
        "{}.okx-{service_name}-{shard_index:03}",
        config.generation_state_path
    );
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
                    .send(Message::Text(okx_subscription_command(
                        &command_id,
                        &subscriptions,
                    )?))
                    .await?;
                let session_id = format!(
                    "okx-{}-{shard_index:03}-{generation}-{}",
                    match service {
                        OkxService::Public => "public",
                        OkxService::Business => "business",
                    },
                    now_ns()?
                );
                let mut pending = subscriptions
                    .iter()
                    .map(|item| (item.channel.clone(), item.inst_id.clone()))
                    .collect::<Vec<_>>();
                let mut pre_ack_frames = VecDeque::new();
                while !pending.is_empty() {
                    let message = tokio::time::timeout(Duration::from_secs(10), reader.next())
                        .await
                        .map_err(|_| "OKX subscription ACK timed out")?
                        .ok_or("OKX socket closed before subscription ACK")??;
                    match message {
                        Message::Ping(payload) => {
                            writer.send(Message::Pong(payload)).await?;
                        }
                        Message::Pong(_) => {}
                        Message::Text(payload) => {
                            let raw_text = payload.to_string();
                            let decoded: Value = serde_json::from_str(&raw_text)?;
                            if parse_okx_subscription_ack(&decoded, &command_id, &mut pending)? {
                                continue;
                            }
                            if pre_ack_frames.len() >= config.max_inflight_publishes {
                                return Err(
                                    "OKX pre-ACK frame buffer exceeded durable publish bound"
                                        .into(),
                                );
                            }
                            pre_ack_frames.push_back(pending_okx_frame(
                                &binding_map,
                                &session_id,
                                generation,
                                raw_text,
                                now_ns()?,
                            )?);
                        }
                        Message::Close(_) => {
                            return Err("OKX socket closed before subscription ACK".into());
                        }
                        _ => {}
                    }
                }
                let mut liveness = SessionLivenessWriter::new(
                    &config,
                    &format!("okx-{service_name}-{shard_index:03}"),
                )?;
                liveness.live(&session_id, generation, now_ns()?)?;
                let mut inflight = FuturesUnordered::<RawPublishFuture>::new();
                let mut latest = LatestStateBuffer::default();
                let mut latest_tick =
                    tokio::time::interval(Duration::from_millis(config.latest_state_flush_ms));
                latest_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                latest_tick.tick().await;
                let snapshot_period = book_snapshot_renewal_period(&binding_map);
                let mut snapshot_tick = snapshot_period.map(tokio::time::interval);
                if let Some(tick) = snapshot_tick.as_mut() {
                    // The initial provider subscription already requested the
                    // authoritative snapshot. Consume interval's immediate
                    // tick so renewal only occurs after the declared bound.
                    tick.tick().await;
                }
                let mut disconnected = false;
                let mut snapshot_renewal = false;
                let mut publish_error = None;
                let mut exhausted = false;
                while let Some(frame) = pre_ack_frames.pop_front() {
                    if !publish_pending_frame(
                        frame,
                        &mut PendingPublishWindow {
                            publisher: &publisher,
                            latest: &mut latest,
                            inflight: &mut inflight,
                            accepted: &accepted,
                            coalesced_latest: &coalesced_latest,
                            max_events: config.max_events,
                            max_inflight: config.max_inflight_publishes,
                            stopped: &stopped,
                        },
                    )
                    .await?
                    {
                        exhausted = true;
                        break;
                    }
                }
                while !exhausted && !should_stop(&stopped, &accepted, config.max_events, expires) {
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
                        _ = async {
                            match snapshot_tick.as_mut() {
                                Some(tick) => {
                                    tick.tick().await;
                                }
                                None => std::future::pending::<()>().await,
                            }
                        }, if snapshot_period.is_some() => {
                            // OKX books can only establish a new executable
                            // state from its documented websocket snapshot.
                            // Rotate the isolated BOOK lane so a core restart
                            // obtains that snapshot without touching trade,
                            // quote or bar sessions.
                            snapshot_renewal = true;
                            break;
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
                        Ok(Some(Ok(message))) => {
                            if !matches!(message, Message::Close(_)) {
                                liveness.live(&session_id, generation, now_ns()?)?;
                            }
                            message
                        }
                        Ok(Some(Err(_))) | Ok(None) => {
                            liveness.disconnected(&session_id, generation, now_ns()?)?;
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
                                    liveness.live(&session_id, generation, now_ns()?)?;
                                    continue;
                                }
                                _ => {
                                    liveness.disconnected(&session_id, generation, now_ns()?)?;
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
                        liveness.disconnected(&session_id, generation, now_ns()?)?;
                        disconnected = true;
                        break;
                    }
                    if payload.get("event").is_some() {
                        return Err("unexpected OKX control event after subscription".into());
                    }
                    let frame = pending_okx_frame(
                        &binding_map,
                        &session_id,
                        generation,
                        raw_text,
                        now_ns()?,
                    )?;
                    if !publish_pending_frame(
                        frame,
                        &mut PendingPublishWindow {
                            publisher: &publisher,
                            latest: &mut latest,
                            inflight: &mut inflight,
                            accepted: &accepted,
                            coalesced_latest: &coalesced_latest,
                            max_events: config.max_events,
                            max_inflight: config.max_inflight_publishes,
                            stopped: &stopped,
                        },
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
                liveness.disconnected(&session_id, generation, now_ns()?)?;
                if let Some(error) = publish_error {
                    return Err(error.into());
                }
                if snapshot_renewal && !should_stop(&stopped, &accepted, config.max_events, expires)
                {
                    failures = 0;
                    println!(
                        "{}",
                        serde_json::to_string(&json!({
                            "event": "qdl_native_book_snapshot_renewal",
                            "runtime": "OKX",
                            "service": format!("{:?}", service).to_ascii_uppercase(),
                            "generation": generation,
                            "bindings": binding_map.len(),
                            "snapshot_refresh_seconds": snapshot_period.map(|value| value.as_secs()),
                        }))?
                    );
                    continue;
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
    let public: Vec<RawBinding> = config
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
    let business: Vec<RawBinding> = config
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
    let business_url = config
        .business_websocket_url
        .clone()
        .ok_or("OKX business WebSocket URL is required")?;
    let mut futures = Vec::new();
    for (index, bindings) in
        partition_okx_bindings(&public, config.max_subscriptions_per_connection)
            .into_iter()
            .enumerate()
    {
        futures.push(run_okx_service(
            OkxServiceShard {
                service: OkxService::Public,
                shard_index: index + 1,
                url: config.websocket_url.clone(),
                bindings,
            },
            config.clone(),
            accepted.clone(),
            coalesced_latest.clone(),
            stopped.clone(),
        ));
    }
    for (index, bindings) in
        partition_okx_bindings(&business, config.max_subscriptions_per_connection)
            .into_iter()
            .enumerate()
    {
        futures.push(run_okx_service(
            OkxServiceShard {
                service: OkxService::Business,
                shard_index: index + 1,
                url: business_url.clone(),
                bindings,
            },
            config.clone(),
            accepted.clone(),
            coalesced_latest.clone(),
            stopped.clone(),
        ));
    }
    try_join_all(futures).await?;
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
        match shutdown_signal().await {
            Ok(signal) => {
                println!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_native_raw_ingestor_shutdown_requested",
                        "reason": signal.as_str(),
                    }))
                    .unwrap_or_else(|_| {
                        "{\"event\":\"qdl_native_raw_ingestor_shutdown_requested\"}".into()
                    })
                );
                stop_signal.store(true, Ordering::Release);
            }
            Err(error) => {
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_native_raw_ingestor_signal_error",
                        "error": error.to_string(),
                    }))
                    .unwrap_or_else(|_| {
                        "{\"event\":\"qdl_native_raw_ingestor_signal_error\"}".into()
                    })
                );
            }
        }
    });
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "qdl_native_raw_ingestor_started",
            "runtime": format!("{:?}", config.runtime).to_ascii_uppercase(),
            "authority": authority_mode_name(&config.authority.mode),
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
        authority_mode_name, book_snapshot_renewal_period, next_connection_generation, partition_binance_bindings,
        partition_bindings, partition_okx_bindings, pending_binance_frame, pending_okx_frame,
        AuthorityMode, DeliveryClass, LatestStateBuffer, PendingRawFrame, ProviderRuntime, RawBinding, RawFeed,
        SessionLivenessWriter,
    };
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;

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
                RawFeed::Bar => "btcusdt@kline_1m",
                RawFeed::Book => "btcusdt@depth@100ms",
            }
            .into(),
            subscription_id: "test-source".into(),
            adapter_version: "test/2.0.0".into(),
            instrument_catalog_revision: 1,
            feed,
            delivery_class,
            l2: None,
        }
    }

    #[test]
    fn startup_authority_name_matches_fenced_mode() {
        assert_eq!(authority_mode_name(&AuthorityMode::RustShadow), "RUST_SHADOW");
        assert_eq!(authority_mode_name(&AuthorityMode::RustCanary), "RUST_CANARY");
        assert_eq!(authority_mode_name(&AuthorityMode::RustPrimary), "RUST_PRIMARY");
    }

    #[test]
    fn provider_bindings_are_sharded_without_truncation() {
        let values: Vec<RawBinding> = (0..205)
            .map(|index| {
                let mut value = binding(RawFeed::Trade, DeliveryClass::Lossless);
                value.native_symbol = format!("S{index}USDT");
                value.native_channel = format!("s{index}usdt@trade");
                value.subscription_id = format!("source-{index}");
                value
            })
            .collect();
        let shards = partition_bindings(&values, 100);
        assert_eq!(
            shards.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![100, 100, 5]
        );
        assert_eq!(shards.into_iter().flatten().count(), values.len());
    }

    #[test]
    fn binance_feed_lanes_isolate_lossless_books_bars_trades_from_quotes() {
        let mut trade = binding(RawFeed::Trade, DeliveryClass::Lossless);
        trade.native_symbol = "ETHUSDT".into();
        trade.native_channel = "ethusdt@trade".into();
        let mut quote = binding(RawFeed::Quote, DeliveryClass::LatestState);
        quote.native_symbol = "ETHUSDT".into();
        quote.native_channel = "ethusdt@bookTicker".into();
        let mut bar = binding(RawFeed::Bar, DeliveryClass::Lossless);
        bar.native_symbol = "ETHUSDT".into();
        bar.native_channel = "ethusdt@kline_1m".into();

        let mut book = binding(RawFeed::Book, DeliveryClass::Lossless);
        book.l2 = Some(super::RawL2Config {
            provider_protocol: "BINANCE_DIFF_DEPTH".into(),
            depth_per_side: 100,
            rest_snapshot_url: Some("https://fapi.binance.com/fapi/v1/depth".into()),
            snapshot_refresh_seconds: Some(30),
        });
        let values = vec![
            book,
            trade.clone(),
            quote.clone(),
            bar.clone(),
            binding(RawFeed::Bar, DeliveryClass::Lossless),
        ];
        let lanes = partition_binance_bindings(&values, 100);
        assert_eq!(lanes.len(), 4);
        assert!(lanes[0].iter().all(|item| item.feed == RawFeed::Book));
        assert!(lanes[1].iter().all(|item| item.feed == RawFeed::Bar));
        assert!(lanes[2].iter().all(|item| item.feed == RawFeed::Trade));
        assert!(lanes[3].iter().all(|item| item.feed == RawFeed::Quote));
        assert_eq!(lanes.into_iter().flatten().count(), values.len());
    }

    #[test]
    fn okx_book_lane_renews_without_rotating_trade_or_quote_lanes() {
        let mut book = binding(RawFeed::Book, DeliveryClass::Lossless);
        book.provider = "OKX_DIRECT".into();
        book.venue = "OKX".into();
        book.market = "SWAP".into();
        book.product_type = "PERPETUAL".into();
        book.native_symbol = "BTC-USDT-SWAP".into();
        book.native_channel = "books".into();
        book.l2 = Some(super::RawL2Config {
            provider_protocol: "OKX_PUBLIC_BOOKS".into(),
            depth_per_side: 100,
            rest_snapshot_url: None,
            snapshot_refresh_seconds: Some(30),
        });
        let mut trade = book.clone();
        trade.feed = RawFeed::Trade;
        trade.native_channel = "trades".into();
        trade.l2 = None;
        let mut quote = trade.clone();
        quote.feed = RawFeed::Quote;
        quote.delivery_class = DeliveryClass::LatestState;
        quote.native_channel = "bbo-tbt".into();

        let values = vec![book, trade, quote];
        let lanes = partition_okx_bindings(&values, 100);
        assert_eq!(lanes.len(), 3);
        assert!(lanes[0].iter().all(|item| item.feed == RawFeed::Book));
        assert!(lanes[1].iter().all(|item| item.feed == RawFeed::Trade));
        assert!(lanes[2].iter().all(|item| item.feed == RawFeed::Quote));
        let books = lanes[0]
            .iter()
            .cloned()
            .map(|item| (item.key(), item))
            .collect::<HashMap<_, _>>();
        let mixed = values
            .into_iter()
            .map(|item| (item.key(), item))
            .collect::<HashMap<_, _>>();
        assert_eq!(
            book_snapshot_renewal_period(&books),
            Some(Duration::from_secs(30))
        );
        assert_eq!(book_snapshot_renewal_period(&mixed), None);
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
        let mut book = binding(RawFeed::Book, DeliveryClass::Lossless);
        book.l2 = Some(super::RawL2Config {
            provider_protocol: "BINANCE_DIFF_DEPTH".into(),
            depth_per_side: 100,
            rest_snapshot_url: Some("https://fapi.binance.com/fapi/v1/depth".into()),
            snapshot_refresh_seconds: Some(30),
        });
        assert!(book.validate(ProviderRuntime::Binance).is_ok());
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

        let mut okx_book = binding(RawFeed::Book, DeliveryClass::Lossless);
        okx_book.provider = "OKX_DIRECT".into();
        okx_book.venue = "OKX".into();
        okx_book.market = "SWAP".into();
        okx_book.product_type = "PERPETUAL".into();
        okx_book.native_symbol = "BTC-USDT-SWAP".into();
        okx_book.native_channel = "books".into();
        okx_book.l2 = Some(super::RawL2Config {
            provider_protocol: "OKX_PUBLIC_BOOKS".into(),
            depth_per_side: 100,
            rest_snapshot_url: None,
            snapshot_refresh_seconds: Some(30),
        });
        assert!(okx_book.validate(ProviderRuntime::Okx).is_ok());
        okx_book.l2.as_mut().unwrap().snapshot_refresh_seconds = None;
        assert!(okx_book.validate(ProviderRuntime::Okx).is_err());
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
            transport_protocol: qdl_contracts::qdl::provider::v1::TransportProtocol::Websocket,
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
    fn pre_ack_binance_frame_retains_direct_channel_or_fails_closed() {
        let binding = binding(RawFeed::Trade, DeliveryClass::Lossless);
        let bindings = HashMap::from([(binding.native_channel.clone(), binding)]);
        let frame = pending_binance_frame(
            &bindings,
            "session-1",
            7,
            r#"{"e":"trade","s":"BTCUSDT","t":1,"p":"1","q":"2","T":3,"m":false}"#.into(),
            10,
        )
        .unwrap();
        assert_eq!(frame.binding.native_channel, "btcusdt@trade");
        assert_eq!(frame.session_id, "session-1");
        assert_eq!(frame.generation, 7);

        let error = pending_binance_frame(
            &bindings,
            "session-1",
            7,
            r#"{"e":"trade","s":"ETHUSDT","t":1,"p":"1","q":"2","T":3,"m":false}"#.into(),
            11,
        )
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn binance_depth_update_binds_only_to_the_approved_book_channel() {
        let binding = binding(RawFeed::Book, DeliveryClass::Lossless);
        let bindings = HashMap::from([(binding.native_channel.clone(), binding)]);
        let frame = pending_binance_frame(
            &bindings,
            "session-1",
            7,
            r#"{"e":"depthUpdate","s":"BTCUSDT","U":10,"u":10,"pu":9,"b":[["1","2"]],"a":[["3","4"]]}"#.into(),
            10,
        )
        .unwrap();
        assert_eq!(frame.binding.feed, RawFeed::Book);
        assert_eq!(frame.binding.native_channel, "btcusdt@depth@100ms");

        let error = pending_binance_frame(
            &bindings,
            "session-1",
            7,
            r#"{"e":"depthUpdate","s":"ETHUSDT","U":10,"u":10,"pu":9,"b":[["1","2"]],"a":[["3","4"]]}"#.into(),
            11,
        )
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn pre_ack_okx_frame_retains_approved_binding_or_fails_closed() {
        let mut binding = binding(RawFeed::Trade, DeliveryClass::Lossless);
        binding.provider = "OKX_DIRECT".into();
        binding.venue = "OKX".into();
        binding.market = "SWAP".into();
        binding.native_symbol = "BTC-USDT-SWAP".into();
        binding.native_channel = "trades".into();
        let bindings = HashMap::from([(binding.key(), binding)]);
        let frame = pending_okx_frame(
            &bindings,
            "session-1",
            7,
            r#"{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"1"}]}"#.into(),
            10,
        )
        .unwrap();
        assert_eq!(frame.binding.native_channel, "trades");
        assert_eq!(frame.session_id, "session-1");
        assert_eq!(frame.generation, 7);

        let error = pending_okx_frame(
            &bindings,
            "session-1",
            7,
            r#"{"arg":{"channel":"trades","instId":"ETH-USDT-SWAP"},"data":[{"instId":"ETH-USDT-SWAP","tradeId":"1"}]}"#.into(),
            11,
        )
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
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

    #[test]
    fn session_liveness_writer_is_atomic_bounded_and_disconnects_explicitly() {
        let directory = generation_path("session-liveness");
        let path = directory.join("binance-usdm").join("binance-000.json");
        let mut writer = SessionLivenessWriter {
            path: path.clone(),
            config_revision: 7,
            write_interval_ns: 1_000_000_000,
            last_written_ns: None,
        };

        writer.live("session-1", 3, 1_000_000_000).unwrap();
        let first: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(first["schema"], "qdl.provider-session-liveness.v1");
        assert_eq!(first["source_session_id"], "session-1");
        assert_eq!(first["connection_generation"], 3);
        assert_eq!(first["state"], "LIVE");
        assert_eq!(first["last_transport_at_ns"], 1_000_000_000_i64);
        assert_eq!(first["config_revision"], 7);

        writer.live("session-1", 3, 1_500_000_000).unwrap();
        let bounded: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(bounded["last_transport_at_ns"], 1_000_000_000_i64);

        writer.disconnected("session-1", 3, 1_500_000_000).unwrap();
        let disconnected: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(disconnected["state"], "DISCONNECTED");
        assert_eq!(disconnected["last_transport_at_ns"], 1_500_000_000_i64);
        assert!(directory.join("binance-usdm").is_dir());
        assert_eq!(
            fs::read_dir(directory.join("binance-usdm"))
                .unwrap()
                .count(),
            1
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
