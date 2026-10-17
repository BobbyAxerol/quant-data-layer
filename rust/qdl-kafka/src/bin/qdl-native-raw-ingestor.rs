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
    CaptureBoundary, QuarantineReason, QuarantineRecord, RawProviderEnvelope, TransportCompression,
    TransportProtocol,
};
use qdl_core::backoff::BackoffPolicy;
use qdl_core::binance::{
    self, route_subscribed as route_binance_subscribed, validate_stream as validate_binance_stream,
    BinanceRoute,
};
use qdl_core::binance_session::{
    parse_subscription_reply as parse_binance_subscription_reply,
    subscription_command as binance_subscription_command,
};
use qdl_core::json_probe::{Kind, Skip};
use qdl_core::okx::{
    parse_subscription_ack as parse_okx_subscription_ack,
    subscription_command as okx_subscription_command, ControlRequestBudget, OkxService,
    OkxSubscription,
};
use qdl_core::transport::{DurableRecord, RetryClass};
use qdl_kafka::{
    shutdown_signal, FencedKafkaSink, KafkaEventSource, KafkaTlsConfig, KafkaTransportConfig,
    KafkaTransportError, PendingKafkaAppend, ProducerQueueBounds,
};
use qdl_realtime_core::{
    L2_RESNAPSHOT_REQUIRED_V1, L2_TRANSPORT_LIVENESS_KEY, L2_TRANSPORT_LIVENESS_VERSION,
};
use qdl_venue_core::authority::{AuthorityMode, AuthorityRecord, PublicationContext, SinkTarget};
use qdl_venue_core::backpressure::DeliveryClass;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
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
    MarkIndex,
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
    /// The logical MARK_INDEX_PRICE target.  OKX emits mark and index on
    /// distinct physical subscriptions; this keeps both raw components in
    /// one Kafka partition without turning symbols into worker identities.
    #[serde(default)]
    mark_index_target: Option<String>,
}

/// A lane's bindings by routing key. Values are shared: every frame of a
/// binding refers to the one immutable binding instead of cloning its nine
/// strings (Codex A6).
type BindingMap = HashMap<String, Arc<RawBinding>>;

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
            (
                RawFeed::Quote | RawFeed::MarkIndex,
                DeliveryClass::LatestState
            ) | (
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
                if self.feed == RawFeed::MarkIndex
                    && self.native_channel
                        != format!("{}@markPrice@1s", self.native_symbol.to_ascii_lowercase())
                {
                    return Err("Binance MARK_INDEX binding channel is invalid".into());
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
                if self.feed == RawFeed::MarkIndex
                    && !matches!(self.native_channel.as_str(), "mark-price" | "index-tickers")
                {
                    return Err("OKX MARK_INDEX binding channel is invalid".into());
                }
            }
        }
        if self.feed == RawFeed::MarkIndex {
            if self
                .mark_index_target
                .as_deref()
                .map(|target| target.trim().is_empty())
                .unwrap_or(true)
            {
                return Err("MARK_INDEX raw binding requires a logical target".into());
            }
        } else if self.mark_index_target.is_some() {
            return Err("only MARK_INDEX raw bindings may carry a logical target".into());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BookResyncConfig {
    quarantine_stream: String,
    consumer_group_prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IngestorConfig {
    runtime: ProviderRuntime,
    /// OKX: the `/ws/v5/public` service. Binance: the `/public/ws` routed base.
    websocket_url: String,
    /// OKX only: the `/ws/v5/business` service that carries candle channels.
    business_websocket_url: Option<String>,
    /// Binance only: the `/market/ws` routed base. Binance split its USD-M
    /// WebSocket into `/public`, `/market` and `/private` and decommissioned
    /// the unrouted form on 2026-04-23; `@markPrice` and `@kline_*` live under
    /// `/market` and push nothing on a connection that does not carry it.
    market_websocket_url: Option<String>,
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
    /// IDR-040 fix 2: cadence of the in-band transport liveness marker for
    /// quiet Binance BOOK bindings. Absent or zero (the default) emits none.
    /// The realtime core that reads the marker must be deployed first.
    #[serde(default)]
    book_liveness_proof_ms: u64,
    /// Silent-socket detection at sub-second resolution. Both absent (or
    /// zero), the default, keeps the `heartbeat_seconds` rule: ping after
    /// `heartbeat_seconds` of inbound silence, dead at twice that. Set
    /// together: ping after `liveness_ping_ms` of inbound silence, dead once
    /// `liveness_dead_ms` has passed since the last message read and the ping
    /// has had at least `liveness_dead_ms - liveness_ping_ms` to be answered.
    /// A socket that delivers data never pings. Old binaries reject these
    /// fields (`deny_unknown_fields`), so the binary must ship first.
    #[serde(default)]
    liveness_ping_ms: u64,
    #[serde(default)]
    liveness_dead_ms: u64,
    /// IDR-112: venue-timestamp staleness recycle of OKX latest-state QUOTE
    /// lanes. Absent selects the documented defaults (enabled); fields left
    /// out of a present object keep their defaults. Old binaries reject this
    /// field (`deny_unknown_fields`), so the binary must ship first.
    #[serde(default)]
    latest_state_staleness: LatestStateStalenessConfig,
    authority: AuthorityRecord,
    bindings: Vec<RawBinding>,
    #[serde(default)]
    book_resync: Option<BookResyncConfig>,
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
    [
        RawFeed::Book,
        RawFeed::Bar,
        RawFeed::Trade,
        RawFeed::Quote,
        RawFeed::MarkIndex,
    ]
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

/// Binance lanes for one routed base, in the same feed-lane order as before.
///
/// Every stream must classify, so a binding whose route is unknown fails the
/// whole role rather than being dropped into whichever group happens to be
/// iterated first.
/// The lane-name segment for a feed class.
///
/// Lane identity is `binance-<route>-<feed>-<shard>`, and it is deliberately
/// keyed on the feed rather than on position. Numbering lanes by their order
/// inside a route means adding a feed renumbers every lane after it, which
/// changes the identity of a book that did nothing wrong - and a book whose
/// lane identity changes must re-bootstrap. Phase 3 adds a BAR lane to the
/// `/market` route; with a positional name that would have moved MARK_INDEX.
fn feed_lane_name(feed: RawFeed) -> &'static str {
    match feed {
        RawFeed::Book => "book",
        RawFeed::Bar => "bar",
        RawFeed::Trade => "trade",
        RawFeed::Quote => "quote",
        RawFeed::MarkIndex => "markindex",
    }
}

fn partition_binance_route(
    bindings: &[RawBinding],
    route: BinanceRoute,
    max_subscriptions: usize,
) -> Result<Vec<BinanceLane>, String> {
    let mut selected = Vec::new();
    for binding in bindings {
        if binance::stream_route(&binding.native_channel)? == route {
            selected.push(binding.clone());
        }
    }
    let mut lanes: Vec<BinanceLane> = Vec::new();
    for shard in partition_binance_bindings(&selected, max_subscriptions) {
        let Some(feed) = shard.first().map(|binding| binding.feed) else {
            continue;
        };
        // The index counts inside the feed, never across the route. A lane
        // numbered by its position in the route is renamed whenever a
        // neighbouring feed is added or removed, and a lane whose identity
        // changes must re-bootstrap its book - so adding BAR to `/market`
        // would silently restart MARK_INDEX.
        let shard_index = lanes.iter().filter(|lane| lane.feed == feed).count() + 1;
        lanes.push(BinanceLane {
            feed,
            shard_index,
            bindings: shard,
        });
    }
    Ok(lanes)
}

/// One Binance lane: its feed class, its index inside that feed class, and its
/// bindings. See `partition_binance_route`.
struct BinanceLane {
    feed: RawFeed,
    shard_index: usize,
    bindings: Vec<RawBinding>,
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
        if let Some(resync) = &self.book_resync {
            if resync.quarantine_stream.trim().is_empty()
                || resync.quarantine_stream == self.raw_stream
                || resync.consumer_group_prefix.trim().is_empty()
            {
                return Err(
                    "book resync requires a separate quarantine topic and scoped group".into(),
                );
            }
        }
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
            || (self.book_liveness_proof_ms != 0
                && !(250..=5_000).contains(&self.book_liveness_proof_ms))
            || self.liveness().is_none()
            || self.latest_state_staleness.validate().is_err()
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
            if self.market_websocket_url.is_some() {
                return Err("OKX does not use a Binance /market routed base".into());
            }
        }
        if self.runtime == ProviderRuntime::Binance {
            let market_url = self
                .market_websocket_url
                .as_deref()
                .ok_or("Binance market WebSocket URL is required")?;
            if self.business_websocket_url.is_some() {
                return Err("Binance does not use an OKX business service".into());
            }
            if self.market_name() == "USDM" {
                // USD-M is the market Binance split. Refuse the base it
                // decommissioned on 2026-04-23: the socket accepts it and
                // silently limits the role to the `/public` group, which is how
                // every Binance kline and mark price was lost for five months
                // while the subscription was acknowledged and the connection
                // looked healthy.
                if !binance::is_routed_control_url(&self.websocket_url, BinanceRoute::Public) {
                    return Err(
                        "Binance USD-M public WebSocket URL must be the routed /public/ws \
                         control endpoint"
                            .into(),
                    );
                }
                if !binance::is_routed_control_url(market_url, BinanceRoute::Market) {
                    return Err(
                        "Binance USD-M market WebSocket URL must be the routed /market/ws \
                         control endpoint"
                            .into(),
                    );
                }
            } else {
                // Spot publishes no equivalent route split, so it keeps one
                // control endpoint and both fields name it.
                if !self.websocket_url.trim_end_matches('/').ends_with("/ws")
                    || !market_url.starts_with("wss://")
                    || !market_url.trim_end_matches('/').ends_with("/ws")
                {
                    return Err(
                        "Binance native WebSocket URL must use the control /ws endpoint".into(),
                    );
                }
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

/// Writes the per-lane session liveness file.
///
/// The write is a create + write + fsync + rename + directory fsync. It used to
/// run inline on the lane's async task for the first message after each write
/// interval, and every lane of a process runs inside one `try_join_all` task,
/// so each fsync stalled every lane of the process for as long as the disk
/// took. Now a throttled LIVE write is handed to the blocking pool and not
/// awaited: the read loop keeps going, a write still in flight makes the next
/// due LIVE write wait for the following message, and its error surfaces on
/// that next call. Forced writes (`begin`, `disconnected`) first wait for any
/// write in flight and then complete before returning, so a DISCONNECTED
/// record can never be overtaken by an older LIVE one and keeps failing the
/// lane on error, as before.
struct SessionLivenessWriter {
    path: PathBuf,
    config_revision: u64,
    write_interval_ns: i64,
    last_written_ns: Option<i64>,
    pending: Option<tokio::task::JoinHandle<Result<(), String>>>,
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
            pending: None,
        })
    }

    /// Throttled LIVE record; never blocks the caller on the disk.
    fn live(
        &mut self,
        session_id: &str,
        generation: u64,
        transport_at_ns: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        validate_liveness_identity(session_id, generation, transport_at_ns)?;
        if self
            .last_written_ns
            .is_some_and(|last| transport_at_ns.saturating_sub(last) < self.write_interval_ns)
        {
            return Ok(());
        }
        if let Some(pending) = self.pending.as_ref() {
            if !pending.is_finished() {
                // The previous write is still on the disk; the next message
                // after it finishes writes the newer transport time.
                return Ok(());
            }
        }
        self.reap_finished()?;
        let payload = self.payload(session_id, generation, "LIVE", transport_at_ns)?;
        let path = self.path.clone();
        self.pending = Some(tokio::task::spawn_blocking(move || {
            write_state_atomically(&path, &payload).map_err(|error| error.to_string())
        }));
        self.last_written_ns = Some(transport_at_ns);
        Ok(())
    }

    /// First record of a new session: written at once, never throttled.
    async fn begin(
        &mut self,
        session_id: &str,
        generation: u64,
        transport_at_ns: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.write_now(session_id, generation, "LIVE", transport_at_ns)
            .await
    }

    async fn disconnected(
        &mut self,
        session_id: &str,
        generation: u64,
        transport_at_ns: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.write_now(session_id, generation, "DISCONNECTED", transport_at_ns)
            .await
    }

    /// Wait for a LIVE write in flight, then write this record and wait for it.
    async fn write_now(
        &mut self,
        session_id: &str,
        generation: u64,
        state: &'static str,
        transport_at_ns: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        validate_liveness_identity(session_id, generation, transport_at_ns)?;
        if let Some(pending) = self.pending.take() {
            pending.await??;
        }
        let payload = self.payload(session_id, generation, state, transport_at_ns)?;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            write_state_atomically(&path, &payload).map_err(|error| error.to_string())
        })
        .await??;
        self.last_written_ns = Some(transport_at_ns);
        Ok(())
    }

    /// Surface the result of a finished background write.
    fn reap_finished(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(pending) = self.pending.take() {
            match futures_util::FutureExt::now_or_never(pending) {
                Some(result) => result??,
                None => return Err("session liveness write state is inconsistent".into()),
            }
        }
        Ok(())
    }

    fn payload(
        &self,
        session_id: &str,
        generation: u64,
        state: &'static str,
        transport_at_ns: i64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::to_vec(&ProviderSessionLiveness {
            schema: SESSION_LIVENESS_SCHEMA,
            source_session_id: session_id,
            connection_generation: generation,
            state,
            last_transport_at_ns: transport_at_ns,
            updated_at_ns: now_ns()?,
            config_revision: self.config_revision,
        })?)
    }
}

fn validate_liveness_identity(
    session_id: &str,
    generation: u64,
    transport_at_ns: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if session_id.is_empty() || session_id.len() > 256 || generation == 0 || transport_at_ns <= 0 {
        return Err("provider session liveness identity is invalid".into());
    }
    Ok(())
}

/// `next_connection_generation` (a durable read, write, fsync and rename) on
/// the blocking pool, so a reconnect does not stall the other lanes' task.
async fn connection_generation(
    state_path: &str,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let state_path = PathBuf::from(state_path);
    tokio::task::spawn_blocking(move || {
        next_connection_generation(&state_path).map_err(|error| error.to_string())
    })
    .await?
    .map_err(Into::into)
}

/// Replace a small state file by rename, so a reader sees the old or the new
/// content, never a partial one.
fn write_state_atomically(
    path: &Path,
    payload: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let parent = path.parent().ok_or("state path has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension(format!("tmp-{}-{}", std::process::id(), now_ns()?));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(payload)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

const SESSION_LINEAGE_SCHEMA: &str = "qdl.provider-session-lineage.v1";
const MAX_SESSION_LINEAGE_LINKS: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SessionLink {
    source_session_id: String,
    connection_generation: u64,
}

#[derive(Serialize)]
struct ProviderSessionLineage<'a> {
    schema: &'static str,
    lane: &'a str,
    chain: Vec<SessionLink>,
    updated_at_ns: i64,
    config_revision: u64,
}

/// Per-lane chain of sessions that succeeded each other make-before-break
/// (successor fully ACKed before the predecessor stopped reading) on an OKX
/// service-upgrade notice or an IDR-112 staleness recycle.
///
/// Each link was subscribed and ACKed while its predecessor was still
/// reading, and the predecessor stopped reading only after that. So the last
/// event a predecessor published for a binding stays that binding's latest
/// event for as long as the chain's last session is live: anything newer was
/// published by a later link. A reconnect that left a gap starts a new chain.
/// The file is `lineage/<lane>.lineage` under the liveness directory:
/// liveness readers that predate it list only regular files of that
/// directory, and the container healthcheck (`find -name '*.json'`, which
/// does recurse) does not match the extension.
struct SessionLineageWriter {
    path: PathBuf,
    lane: String,
    config_revision: u64,
    chain: VecDeque<SessionLink>,
}

impl SessionLineageWriter {
    fn new(
        config: &IngestorConfig,
        lane: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::at(
            Path::new(&config.session_liveness_dir),
            lane,
            config.config_revision,
        )
    }

    fn at(
        liveness_dir: &Path,
        lane: &str,
        config_revision: u64,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if lane.is_empty()
            || lane.len() > 120
            || !lane.chars().all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
            })
        {
            return Err("session lineage lane is invalid".into());
        }
        Ok(Self {
            path: liveness_dir.join("lineage").join(format!("{lane}.lineage")),
            lane: lane.into(),
            config_revision,
            chain: VecDeque::new(),
        })
    }

    /// Record a new session; `continued` only when it took over from the
    /// chain's last session with overlap. The durable write runs on the
    /// blocking pool and is awaited, so its order and error are unchanged.
    async fn begin(
        &mut self,
        session_id: &str,
        generation: u64,
        continued: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if session_id.is_empty() || session_id.len() > 256 || generation == 0 {
            return Err("provider session lineage identity is invalid".into());
        }
        if !continued {
            self.chain.clear();
        }
        self.chain.push_back(SessionLink {
            source_session_id: session_id.into(),
            connection_generation: generation,
        });
        while self.chain.len() > MAX_SESSION_LINEAGE_LINKS {
            self.chain.pop_front();
        }
        let payload = serde_json::to_vec(&ProviderSessionLineage {
            schema: SESSION_LINEAGE_SCHEMA,
            lane: &self.lane,
            chain: self.chain.iter().cloned().collect(),
            updated_at_ns: now_ns()?,
            config_revision: self.config_revision,
        })?;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            write_state_atomically(&path, &payload).map_err(|error| error.to_string())
        })
        .await?
        .map_err(Into::into)
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

/// One process-wide raw producer shared by every lane (Codex A6 / step 1).
///
/// Lanes used to own one idempotent producer each: a librdkafka instance with
/// its own broker threads, queue and batches per lane. Every field below is
/// process configuration, identical for all lanes, and lanes already fail the
/// whole role together (`try_join_all`), so sharing one producer changes no
/// failure domain. Per-key order is kept: a key belongs to one binding, a
/// binding to one lane, and one idempotent producer preserves the enqueue
/// order of each partition. The producer is rebuilt on every supervisor retry,
/// as the per-lane producers were.
struct RawPublisher {
    sink: RawSink,
    envelope: RawEnvelopeContext,
}

/// Where raw envelopes go. Production has exactly one variant, the fenced
/// Kafka sink; the test-only in-memory sink lets the unit tests drive the
/// production session loops (`run_okx_service`) against a fake venue without
/// a broker (E7: test the loop that runs, not a copy of it).
// Test builds only: the in-memory variant is small next to the Kafka sink.
#[allow(clippy::large_enum_variant)]
enum RawSink {
    Kafka(FencedKafkaSink),
    #[cfg(test)]
    Memory(Arc<std::sync::Mutex<Vec<DurableRecord>>>),
}

enum RawDelivery {
    Kafka(PendingKafkaAppend),
    #[cfg(test)]
    Memory,
}

impl RawSink {
    fn enqueue(
        &self,
        record: &DurableRecord,
        publication: &PublicationContext,
    ) -> Result<RawDelivery, KafkaTransportError> {
        match self {
            Self::Kafka(sink) => sink.enqueue(record, publication).map(RawDelivery::Kafka),
            #[cfg(test)]
            Self::Memory(records) => {
                records
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(record.clone());
                Ok(RawDelivery::Memory)
            }
        }
    }
}

/// The process configuration every raw envelope carries.
struct RawEnvelopeContext {
    authority: AuthorityRecord,
    raw_stream: String,
    shard_id: String,
    lease_epoch: u64,
    partition_plan_epoch: u64,
    config_revision: u64,
}

impl RawPublisher {
    /// `lanes` sizes the shared queue to the aggregate of the per-lane bounds
    /// it replaces (`ProducerQueueBounds::scaled_default`).
    fn new(
        config: &IngestorConfig,
        identity: &str,
        lanes: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let sink = FencedKafkaSink::with_queue_bounds(
            &kafka_config(identity)?,
            ProducerQueueBounds::scaled_default(lanes)?,
        )?;
        sink.apply_authority(config.authority.clone())?;
        Ok(Self {
            sink: RawSink::Kafka(sink),
            envelope: RawEnvelopeContext::new(config),
        })
    }

    #[cfg(test)]
    fn memory(config: &IngestorConfig) -> (Self, Arc<std::sync::Mutex<Vec<DurableRecord>>>) {
        let records = Arc::new(std::sync::Mutex::new(Vec::new()));
        (
            Self {
                sink: RawSink::Memory(Arc::clone(&records)),
                envelope: RawEnvelopeContext::new(config),
            },
            records,
        )
    }
}

impl RawEnvelopeContext {
    fn new(config: &IngestorConfig) -> Self {
        Self {
            authority: config.authority.clone(),
            raw_stream: config.raw_stream.clone(),
            shard_id: config.shard_id.clone(),
            lease_epoch: config.lease_epoch,
            partition_plan_epoch: config.partition_plan_epoch,
            config_revision: config.config_revision,
        }
    }

    /// The raw envelope record of one frame. The frame bytes move into the
    /// envelope; they are hashed twice because the two digests have different
    /// preimages and purposes: `capture_id` is SHA-256(session, generation,
    /// receipt time, frame)[..16], the event identity of this capture, while
    /// `raw_frame_sha256` is SHA-256(frame), the content proof the core checks
    /// against the bytes. Neither can be derived from the other.
    fn record(&self, frame: PendingRawFrame) -> DurableRecord {
        let PendingRawFrame {
            binding,
            session_id,
            generation,
            raw_frame,
            received_at_ns,
            transport_protocol,
        } = frame;
        let capture_id = capture_id(&session_id, generation, received_at_ns, &raw_frame);
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
            source_session_id: session_id,
            connection_generation: generation,
            lease_epoch: self.lease_epoch,
            authority_revision: self.authority.revision,
            partition_plan_epoch: self.partition_plan_epoch,
            received_at_ns,
            transport_protocol: transport_protocol as i32,
            transport_compression: TransportCompression::None as i32,
            capture_boundary: CaptureBoundary::PostDecompression as i32,
            raw_frame_sha256: Sha256::digest(&raw_frame).to_vec(),
            raw_frame_bytes: raw_frame,
            adapter_version: binding.adapter_version.clone(),
            config_revision: self.config_revision,
            instrument_catalog_revision: binding.instrument_catalog_revision,
            correlation_id: hex::encode(&capture_id),
            test_provenance: false,
        };
        let partition_key = if let Some(target) = &binding.mark_index_target {
            format!("{}/{}/mark-index/{}", binding.venue, binding.market, target)
        } else {
            format!(
                "{}/{}/{}/{}",
                binding.venue, binding.market, binding.native_symbol, binding.native_channel
            )
        };
        DurableRecord {
            stream: self.raw_stream.clone(),
            partition_key,
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
}

impl RawPublisher {
    async fn enqueue_with_retry(
        &self,
        frame: PendingRawFrame,
        stopped: &AtomicBool,
        deadline: Option<Instant>,
    ) -> Result<RawDelivery, KafkaTransportError> {
        let binding = Arc::clone(&frame.binding);
        let record = self.envelope.record(frame);
        let publication = self.envelope.publication();
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
            if deadline.is_some_and(|value| Instant::now() >= value) {
                return Err(KafkaTransportError::SnapshotTimeout(
                    "L2 raw Kafka enqueue exceeded provider renewal bound".into(),
                ));
            }
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
                    let delay = Duration::from_millis(
                        backoff.delay_ms(failures, failures.min(10_000) as u16),
                    );
                    if let Some(deadline) = deadline {
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = tokio::time::sleep_until(deadline) => {
                                return Err(KafkaTransportError::SnapshotTimeout(
                                    "L2 raw Kafka enqueue exceeded provider renewal bound".into(),
                                ));
                            }
                        }
                    } else {
                        tokio::time::sleep(delay).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
}

type RawPublishFuture = Pin<Box<dyn Future<Output = Result<(), KafkaTransportError>> + Send>>;

/// Cadence of `qdl_native_raw_ingestor_progress` (item 3 of the P18.4
/// capacity package). Time-driven on purpose: a line every N frames
/// (`metrics_every_events`, the core's model) stops exactly when a lane
/// stalls, which is what this telemetry must show, and on a hot Binance lane
/// it would print several lines a second. Each line covers its own interval
/// and carries the interval maxima with their wall time, so a stall shorter
/// than the interval is still visible and can be matched to core raw_age.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);

/// min/mean/max of one span over a progress interval, plus when the max was.
#[derive(Clone, Copy, Debug, Default)]
struct WindowSpan {
    count: u64,
    total_ns: u128,
    min_ns: u64,
    max_ns: u64,
    max_at_ns: i64,
}

impl WindowSpan {
    fn observe(&mut self, span: Duration, at_ns: i64) {
        let span_ns = u64::try_from(span.as_nanos()).unwrap_or(u64::MAX);
        if self.count == 0 || span_ns < self.min_ns {
            self.min_ns = span_ns;
        }
        if self.count == 0 || span_ns > self.max_ns {
            self.max_ns = span_ns;
            self.max_at_ns = at_ns;
        }
        self.count += 1;
        self.total_ns += u128::from(span_ns);
    }

    fn report(&self) -> Value {
        if self.count == 0 {
            return Value::Null;
        }
        let mean_ns = (self.total_ns / u128::from(self.count)) as f64;
        json!({
            "n": self.count,
            "min": self.min_ns as f64 / 1e6,
            "mean": mean_ns / 1e6,
            "max": self.max_ns as f64 / 1e6,
            "max_at_ns": self.max_at_ns,
        })
    }
}

/// One lane's counters for the current progress interval.
#[derive(Debug, Default)]
struct LaneWindow {
    frames: u64,
    bytes: u64,
    enqueued: u64,
    acknowledged: u64,
    coalesced: u64,
    inflight_max: usize,
    read_gap: WindowSpan,
    enqueue: WindowSpan,
    produce: WindowSpan,
    /// IDR-112 lanes only: host time at read minus venue `ts` (clamped at 0).
    venue_lag: WindowSpan,
    stale_frames: u64,
    /// Staleness triggers that did not start a recycle (any SUPPRESSED_*).
    staleness_suppressed: u64,
}

#[derive(Debug, Default)]
struct LaneState {
    window: LaneWindow,
    generation: u64,
    frames_total: u64,
    last_frame_at_ns: i64,
    last_read: Option<Instant>,
    inflight: usize,
    staleness_baseline_ms: Option<i64>,
    staleness_recycles_total: u64,
    /// Outcome of the latest staleness trigger (RECYCLE or SUPPRESSED_*).
    staleness_last_action: Option<&'static str>,
    standby_failures_total: u64,
    /// Standby attempts cancelled by `OKX_ESTABLISH_BUDGET` (review F1).
    standby_deadline_total: u64,
}

/// Throughput and latency of one lane, shared by the lane task, its publish
/// futures and the process reporter. One uncontended mutex per update.
#[derive(Debug)]
struct LaneTelemetry {
    lane: String,
    state: std::sync::Mutex<LaneState>,
}

impl LaneTelemetry {
    fn with<T>(&self, update: impl FnOnce(&mut LaneState) -> T) -> T {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        update(&mut state)
    }

    /// A new provider session: socket read gaps restart from here.
    fn session(&self, generation: u64) {
        self.with(|state| {
            state.generation = generation;
            state.last_read = None;
        });
    }

    /// Any WebSocket message read from the provider (data, ping, pong).
    fn read(&self, at: Instant, at_ns: i64) {
        self.with(|state| {
            if let Some(previous) = state.last_read.replace(at) {
                state
                    .window
                    .read_gap
                    .observe(at.saturating_duration_since(previous), at_ns);
            }
        });
    }

    /// A provider data frame routed to a binding.
    fn frame(&self, received_at_ns: i64) {
        self.with(|state| {
            state.window.frames += 1;
            state.frames_total += 1;
            state.last_frame_at_ns = state.last_frame_at_ns.max(received_at_ns);
        });
    }

    fn coalesced(&self) {
        self.with(|state| state.window.coalesced += 1);
    }

    /// IDR-112: one frame measured by the staleness detector.
    fn venue_lag(&self, lag_ms: i64, stale: bool, baseline_ms: Option<i64>) {
        let at_ns = now_ns().unwrap_or_default();
        let lag = Duration::from_millis(u64::try_from(lag_ms).unwrap_or(0));
        self.with(|state| {
            state.window.venue_lag.observe(lag, at_ns);
            state.window.stale_frames += u64::from(stale);
            state.staleness_baseline_ms = baseline_ms;
        });
    }

    fn staleness_recycled(&self) {
        self.with(|state| state.staleness_recycles_total += 1);
    }

    /// What one staleness trigger led to.
    fn staleness_action(&self, action: &'static str) {
        self.with(|state| {
            state.staleness_last_action = Some(action);
            state.window.staleness_suppressed += u64::from(action != "RECYCLE");
        });
    }

    /// A standby session (notice or staleness) that did not become ready.
    fn standby_failed(&self, reason: &'static str) {
        self.with(|state| {
            state.standby_failures_total += 1;
            state.standby_deadline_total += u64::from(reason == "STANDBY_DEADLINE");
        });
    }

    /// A frame handed to the producer: enqueue time includes QueueFull retries.
    fn enqueued(&self, took: Duration, bytes: usize, inflight: usize) {
        let at_ns = now_ns().unwrap_or_default();
        self.with(|state| {
            state.window.enqueued += 1;
            state.window.bytes += bytes as u64;
            state.window.enqueue.observe(took, at_ns);
            state.inflight = inflight;
            state.window.inflight_max = state.window.inflight_max.max(inflight);
        });
    }

    /// Broker acknowledgement of one enqueued frame (produce latency).
    fn acknowledged(&self, took: Duration) {
        let at_ns = now_ns().unwrap_or_default();
        self.with(|state| {
            state.window.acknowledged += 1;
            state.inflight = state.inflight.saturating_sub(1);
            state.window.produce.observe(took, at_ns);
        });
    }

    fn report(&self, interval: Duration, now_ns: i64) -> Value {
        let seconds = interval.as_secs_f64().max(f64::EPSILON);
        self.with(|state| {
            let window = std::mem::take(&mut state.window);
            json!({
                "lane": self.lane,
                "generation": state.generation,
                "frames": window.frames,
                "frames_per_s": (window.frames as f64 / seconds * 10.0).round() / 10.0,
                "frames_total": state.frames_total,
                "bytes": window.bytes,
                "enqueued": window.enqueued,
                "acknowledged": window.acknowledged,
                "coalesced": window.coalesced,
                "inflight": state.inflight,
                "inflight_max": window.inflight_max,
                "last_frame_age_ms": (state.last_frame_at_ns > 0)
                    .then(|| now_ns.saturating_sub(state.last_frame_at_ns) as f64 / 1e6),
                "read_gap_ms": window.read_gap.report(),
                "enqueue_ms": window.enqueue.report(),
                "produce_ms": window.produce.report(),
                "venue_lag_ms": window.venue_lag.report(),
                "stale_frames": window.stale_frames,
                "staleness_baseline_ms": state.staleness_baseline_ms,
                "staleness_recycles_total": state.staleness_recycles_total,
                "staleness_suppressed": window.staleness_suppressed,
                "staleness_last_action": state.staleness_last_action,
                "standby_failures_total": state.standby_failures_total,
                "standby_deadline_total": state.standby_deadline_total,
            })
        })
    }
}

/// Every lane of the process, for the periodic progress line.
#[derive(Debug, Default)]
struct TelemetryRegistry {
    lanes: std::sync::Mutex<Vec<Arc<LaneTelemetry>>>,
}

impl TelemetryRegistry {
    /// The lane's telemetry; a lane restarted by the supervisor keeps its own.
    fn lane(&self, lane: &str) -> Arc<LaneTelemetry> {
        let mut lanes = self
            .lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = lanes.iter().find(|item| item.lane == lane) {
            return Arc::clone(existing);
        }
        let created = Arc::new(LaneTelemetry {
            lane: lane.to_owned(),
            state: std::sync::Mutex::new(LaneState::default()),
        });
        lanes.push(Arc::clone(&created));
        created
    }

    fn progress_line(
        &self,
        runtime: &str,
        interval: Duration,
        now_ns: i64,
        accepted: u64,
        coalesced: u64,
    ) -> Value {
        let lanes = self
            .lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        json!({
            "event": "qdl_native_raw_ingestor_progress",
            "runtime": runtime,
            "interval_s": interval.as_secs_f64(),
            "accepted_raw_frames": accepted,
            "coalesced_latest_state_frames": coalesced,
            "lanes": lanes
                .iter()
                .map(|lane| lane.report(interval, now_ns))
                .collect::<Vec<_>>(),
        })
    }
}

/// Print one progress line per `PROGRESS_INTERVAL` until the process stops.
async fn report_progress(
    registry: Arc<TelemetryRegistry>,
    runtime: &'static str,
    accepted: Arc<AtomicU64>,
    coalesced: Arc<AtomicU64>,
) {
    let mut tick = tokio::time::interval(PROGRESS_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    let mut previous = Instant::now();
    loop {
        tick.tick().await;
        let now = Instant::now();
        let Ok(now_ns) = now_ns() else { continue };
        let line = registry.progress_line(
            runtime,
            now.saturating_duration_since(previous),
            now_ns,
            accepted.load(Ordering::Acquire),
            coalesced.load(Ordering::Acquire),
        );
        previous = now;
        println!("{line}");
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BookSessionBootstrap {
    PeriodicRestAnchor(Duration),
    InitialSnapshotAndGapResync,
}

fn raw_publish_future(
    delivery: RawDelivery,
    deadline: Option<Instant>,
    telemetry: Arc<LaneTelemetry>,
) -> RawPublishFuture {
    let enqueued = Instant::now();
    Box::pin(async move {
        // Production has the Kafka variant only; test builds add the memory one.
        #[allow(clippy::infallible_destructuring_match)]
        let delivery = match delivery {
            RawDelivery::Kafka(delivery) => delivery,
            #[cfg(test)]
            RawDelivery::Memory => {
                telemetry.acknowledged(enqueued.elapsed());
                return Ok(());
            }
        };
        let persisted = if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline, delivery.wait_persisted())
                .await
                .map_err(|_| {
                    KafkaTransportError::SnapshotTimeout(
                        "L2 raw Kafka delivery exceeded provider renewal bound".into(),
                    )
                })??
        } else {
            delivery.wait_persisted().await?
        };
        if let qdl_kafka::PersistedKafkaAppend::OffsetUnavailable {
            stream,
            partition,
            reported_offset,
        } = persisted
        {
            eprintln!(
                "{}",
                json!({"event":"qdl_raw_persisted_without_offset",
                "stream":stream, "partition":partition, "reported_offset":reported_offset,
                "cursor_created":false})
            );
        }
        telemetry.acknowledged(enqueued.elapsed());
        Ok(())
    })
}

fn book_delivery_remaining_ns(
    binding: &RawBinding,
    received_at_ns: i64,
    current_at_ns: i64,
) -> Result<Option<u64>, KafkaTransportError> {
    if binding.feed != RawFeed::Book {
        return Ok(None);
    }
    let refresh_seconds = binding
        .l2
        .as_ref()
        .and_then(|l2| l2.snapshot_refresh_seconds)
        .ok_or_else(|| {
            KafkaTransportError::Configuration(
                "BOOK binding is missing validated snapshot renewal configuration".into(),
            )
        })?;
    let max_age_ns = refresh_seconds
        .checked_mul(1_000_000_000)
        .ok_or_else(|| KafkaTransportError::Configuration("BOOK renewal bound overflow".into()))?;
    let max_age_ns = i64::try_from(max_age_ns)
        .map_err(|_| KafkaTransportError::Configuration("BOOK renewal bound exceeds i64".into()))?;
    let elapsed_ns = current_at_ns.saturating_sub(received_at_ns).max(0);
    let remaining_ns = max_age_ns.saturating_sub(elapsed_ns);
    if remaining_ns <= 0 {
        return Err(KafkaTransportError::SnapshotTimeout(
            "L2 raw frame exceeded provider renewal bound before Kafka delivery".into(),
        ));
    }
    Ok(Some(u64::try_from(remaining_ns).map_err(|_| {
        KafkaTransportError::Configuration("BOOK delivery remaining time is invalid".into())
    })?))
}

#[derive(Clone, Debug)]
struct PendingRawFrame {
    binding: Arc<RawBinding>,
    session_id: String,
    generation: u64,
    raw_frame: Vec<u8>,
    received_at_ns: i64,
    transport_protocol: TransportProtocol,
}

/// Route one Binance frame to its binding. A borrowed routing parse replaces
/// the copy of the frame and the full `Value` DOM (a second parse and a `data`
/// subtree clone for combined frames) that `decode_subscribed` built; the
/// accepted frames, stream names and errors are identical
/// (`binance::route_subscribed`).
fn pending_binance_frame(
    bindings: &BindingMap,
    session_id: &str,
    generation: u64,
    raw_text: String,
    received_at_ns: i64,
) -> Result<PendingRawFrame, std::io::Error> {
    let stream = route_binance_subscribed(&raw_text)
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
    let binding = bindings.get(&stream).cloned().ok_or_else(|| {
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
    bindings: &BindingMap,
    session_id: &str,
    generation: u64,
    raw_text: String,
    received_at_ns: i64,
) -> Result<PendingRawFrame, std::io::Error> {
    let routing = OkxRouting::parse(&raw_text)
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
    let binding = routing.data_binding(bindings)?;
    drop(routing);
    Ok(okx_data_frame(
        binding,
        session_id,
        generation,
        raw_text,
        received_at_ns,
    ))
}

fn okx_data_frame(
    binding: Arc<RawBinding>,
    session_id: &str,
    generation: u64,
    raw_text: String,
    received_at_ns: i64,
) -> PendingRawFrame {
    PendingRawFrame {
        binding,
        session_id: session_id.into(),
        generation,
        raw_frame: raw_text.into_bytes(),
        received_at_ns,
        transport_protocol: TransportProtocol::Websocket,
    }
}

/// The routing view of one OKX frame, from one borrowed parse (Codex A6).
///
/// The data path used to build a full `Value` DOM to look for a notice or a
/// control event and then a second DOM in `pending_okx_frame` to read
/// `arg.channel` / `arg.instId`. This keeps exactly what those reads used:
/// whether `event` is present (any type, `null` included, as `Value::get`
/// sees it), `event` and `code` text for the notice, and the `arg` fields. The
/// parser checks the whole frame as a `Value` parse does (`json_probe`).
#[derive(Debug, Default)]
struct OkxRouting<'a> {
    event: Option<Kind<'a>>,
    code: Option<Kind<'a>>,
    arg: Option<OkxArgument<'a>>,
}

#[derive(Debug)]
enum OkxArgument<'a> {
    NotObject,
    Object {
        channel: Option<Kind<'a>>,
        inst_id: Option<Kind<'a>>,
    },
}

impl<'a> OkxRouting<'a> {
    fn parse(raw: &'a str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(raw)
    }

    /// Same as `okx_notice_code` on the decoded frame.
    fn notice_code(&self) -> Option<String> {
        (self.event.as_ref().and_then(Kind::as_str) == Some("notice")).then(|| {
            self.code
                .as_ref()
                .and_then(Kind::as_str)
                .unwrap_or("missing")
                .to_owned()
        })
    }

    fn has_event(&self) -> bool {
        self.event.is_some()
    }

    /// The binding of a data frame, with `pending_okx_frame`'s checks and errors.
    fn data_binding(&self, bindings: &BindingMap) -> Result<Arc<RawBinding>, std::io::Error> {
        if self.has_event() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "OKX control frame cannot be published as provider data",
            ));
        }
        let Some(OkxArgument::Object { channel, inst_id }) = self.arg.as_ref() else {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "OKX data arg is missing",
            ));
        };
        let channel = channel.as_ref().and_then(Kind::as_str).ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidData, "OKX data channel is missing")
        })?;
        let instrument = inst_id.as_ref().and_then(Kind::as_str).ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidData, "OKX data instrument is missing")
        })?;
        bindings
            .get(&format!("{channel}|{instrument}"))
            .cloned()
            .ok_or_else(|| {
                std::io::Error::new(ErrorKind::InvalidData, "OKX frame has no approved binding")
            })
    }
}

impl<'de> Deserialize<'de> for OkxRouting<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RoutingVisitor;
        impl<'de> serde::de::Visitor<'de> for RoutingVisitor {
            type Value = OkxRouting<'de>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("any JSON value")
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Ok(OkxRouting::default())
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> {
                Ok(OkxRouting::default())
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> {
                Ok(OkxRouting::default())
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> {
                Ok(OkxRouting::default())
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Ok(OkxRouting::default())
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(OkxRouting::default())
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                while seq.next_element::<Skip>()?.is_some() {}
                Ok(OkxRouting::default())
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut routing = OkxRouting::default();
                while let Some(key) = map.next_key::<Kind<'de>>()? {
                    match key.as_str() {
                        Some("event") => routing.event = Some(map.next_value()?),
                        Some("code") => routing.code = Some(map.next_value()?),
                        Some("arg") => routing.arg = Some(map.next_value()?),
                        _ => {
                            map.next_value::<Skip>()?;
                        }
                    }
                }
                Ok(routing)
            }
        }
        deserializer.deserialize_any(RoutingVisitor)
    }
}

impl<'de> Deserialize<'de> for OkxArgument<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ArgumentVisitor;
        impl<'de> serde::de::Visitor<'de> for ArgumentVisitor {
            type Value = OkxArgument<'de>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("any JSON value")
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Ok(OkxArgument::NotObject)
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> {
                Ok(OkxArgument::NotObject)
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> {
                Ok(OkxArgument::NotObject)
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> {
                Ok(OkxArgument::NotObject)
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Ok(OkxArgument::NotObject)
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(OkxArgument::NotObject)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                while seq.next_element::<Skip>()?.is_some() {}
                Ok(OkxArgument::NotObject)
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let (mut channel, mut inst_id) = (None, None);
                while let Some(key) = map.next_key::<Kind<'de>>()? {
                    match key.as_str() {
                        Some("channel") => channel = Some(map.next_value()?),
                        Some("instId") => inst_id = Some(map.next_value()?),
                        _ => {
                            map.next_value::<Skip>()?;
                        }
                    }
                }
                Ok(OkxArgument::Object { channel, inst_id })
            }
        }
        deserializer.deserialize_any(ArgumentVisitor)
    }
}

const MAX_BOOK_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;

async fn fetch_binance_book_snapshot(
    client: reqwest::Client,
    binding: Arc<RawBinding>,
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
    bindings: &BindingMap,
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

fn book_snapshot_renewal_period(bindings: &BindingMap) -> Option<Duration> {
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

fn book_session_bootstrap(
    runtime: ProviderRuntime,
    bindings: &BindingMap,
) -> Option<BookSessionBootstrap> {
    if bindings.is_empty()
        || bindings
            .values()
            .any(|binding| binding.feed != RawFeed::Book)
    {
        return None;
    }
    match runtime {
        // Binance diff-depth requires a periodic REST anchor to maintain the
        // documented snapshot/delta bridge for an otherwise healthy socket.
        ProviderRuntime::Binance => {
            book_snapshot_renewal_period(bindings).map(BookSessionBootstrap::PeriodicRestAnchor)
        }
        // OKX `books` sends an initial WebSocket snapshot followed by ordered
        // deltas. A healthy session stays open; only a real disconnect, notice
        // or sequence-gap path may cause resync/reconnect.
        ProviderRuntime::Okx => Some(BookSessionBootstrap::InitialSnapshotAndGapResync),
    }
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

#[allow(clippy::too_many_arguments)]
async fn enqueue_lossless_frame(
    frame: PendingRawFrame,
    publisher: &RawPublisher,
    inflight: &mut FuturesUnordered<RawPublishFuture>,
    accepted: &AtomicU64,
    max_events: u64,
    max_inflight: usize,
    stopped: &AtomicBool,
    telemetry: &Arc<LaneTelemetry>,
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
    let remaining_ns = match book_delivery_remaining_ns(
        &frame.binding,
        frame.received_at_ns,
        now_ns().map_err(|error| KafkaTransportError::Configuration(error.to_string()))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            if max_events > 0 {
                accepted.fetch_sub(1, Ordering::AcqRel);
            }
            return Err(error);
        }
    };
    let deadline = remaining_ns.map(|value| Instant::now() + Duration::from_nanos(value));
    let bytes = frame.raw_frame.len();
    let enqueue_started = Instant::now();
    let delivery = publisher.enqueue_with_retry(frame, stopped, deadline).await;
    match delivery {
        Ok(delivery) => {
            inflight.push(raw_publish_future(
                delivery,
                deadline,
                Arc::clone(telemetry),
            ));
            telemetry.enqueued(enqueue_started.elapsed(), bytes, inflight.len());
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
    telemetry: &'a Arc<LaneTelemetry>,
}

async fn publish_pending_frame(
    frame: PendingRawFrame,
    window: &mut PendingPublishWindow<'_>,
) -> Result<bool, KafkaTransportError> {
    if frame.binding.delivery_class == DeliveryClass::LatestState {
        if window.latest.push(frame) {
            window.coalesced_latest.fetch_add(1, Ordering::AcqRel);
            window.telemetry.coalesced();
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
        window.telemetry,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn flush_latest_concurrent(
    buffer: &mut LatestStateBuffer,
    publisher: &RawPublisher,
    inflight: &mut FuturesUnordered<RawPublishFuture>,
    accepted: &AtomicU64,
    max_events: u64,
    max_inflight: usize,
    stopped: &AtomicBool,
    telemetry: &Arc<LaneTelemetry>,
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
            telemetry,
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

struct BookResyncListener {
    source: KafkaEventSource,
    retry_after: Instant,
    failures: u32,
    next_checkpoint: Instant,
    published_anchors: HashMap<String, (String, u64, i64)>,
}

fn book_resync_listener(
    config: &IngestorConfig,
    lane: &str,
    bindings: &BindingMap,
) -> Result<Option<BookResyncListener>, KafkaTransportError> {
    let Some(resync) = config.book_resync.as_ref() else {
        return Ok(None);
    };
    if bindings.is_empty() || bindings.values().any(|b| b.feed != RawFeed::Book) {
        return Ok(None);
    }
    let mut transport = kafka_config(lane).map_err(KafkaTransportError::Configuration)?;
    transport.group_id = format!("{}-{lane}", resync.consumer_group_prefix);
    transport.client_id = format!("{}-resync-{lane}", transport.client_id);
    KafkaEventSource::new_live_control(&transport, &[&resync.quarantine_stream]).map(|source| {
        Some(BookResyncListener {
            source,
            retry_after: Instant::now(),
            failures: 0,
            next_checkpoint: Instant::now(),
            published_anchors: HashMap::new(),
        })
    })
}

fn matches_book_resync(
    record: &DurableRecord,
    config: &IngestorConfig,
    bindings: &BindingMap,
    session_id: &str,
    generation: u64,
    now_ns: i64,
) -> bool {
    let Ok(request) = QuarantineRecord::decode(record.payload.as_slice()) else {
        return false;
    };
    if request.safe_summary != L2_RESNAPSHOT_REQUIRED_V1
        || request.reason != QuarantineReason::SequenceGap as i32
    {
        return false;
    }
    let Some(raw) = request.raw.as_ref() else {
        return false;
    };
    let Some(binding) = bindings
        .values()
        .find(|b| b.subscription_id == raw.subscription_id)
    else {
        return false;
    };
    let Some(l2) = binding.l2.as_ref() else {
        return false;
    };
    let max_age_ns = l2
        .snapshot_refresh_seconds
        .unwrap_or(0)
        .saturating_mul(1_000_000_000) as i64;
    if binding.feed != RawFeed::Book
        || raw.source_session_id != session_id
        || raw.connection_generation != generation
        || raw.test_provenance
        || raw.provider != binding.provider
        || raw.venue != binding.venue
        || raw.market != binding.market
        || raw.product_type != binding.product_type
        || raw.native_symbol != binding.native_symbol
        || raw.native_channel != binding.native_channel
        || raw.adapter_version != binding.adapter_version
        || raw.instrument_catalog_revision != binding.instrument_catalog_revision
        || raw.config_revision != config.config_revision
        || raw.lease_epoch != config.lease_epoch
        || raw.partition_plan_epoch != config.partition_plan_epoch
        || raw.authority_revision != config.authority.revision
        || raw.received_at_ns <= 0
        || request.quarantined_at_ns < raw.received_at_ns
        || now_ns < request.quarantined_at_ns
        || now_ns.saturating_sub(raw.received_at_ns) > max_age_ns
    {
        return false;
    }
    let mut evidence = Sha256::new();
    evidence.update(raw.encode_to_vec());
    evidence.update(request.reason.to_be_bytes());
    let mut id = Sha256::new();
    id.update(&raw.capture_id);
    id.update(L2_RESNAPSHOT_REQUIRED_V1.as_bytes());
    request.evidence_sha256 == evidence.finalize().as_slice()
        && record.event_id == id.finalize().as_slice()
        && raw.raw_frame_sha256 == Sha256::digest(&raw.raw_frame_bytes).as_slice()
}

// A request based on a pre-anchor delta is already being repaired. Reconnecting
// here would throw away the just-published REST bootstrap on every generation.
fn snapshot_satisfies_resync(
    record: &DurableRecord,
    anchors: &HashMap<String, (String, u64, i64)>,
) -> bool {
    let Ok(request) = QuarantineRecord::decode(record.payload.as_slice()) else {
        return false;
    };
    let Some(raw) = request.raw else { return false };
    anchors
        .get(&raw.subscription_id)
        .is_some_and(|(session, generation, receipt)| {
            session == &raw.source_session_id
                && *generation == raw.connection_generation
                && *receipt >= raw.received_at_ns
        })
}

// Hints are reissued and session-fenced, not market-data acknowledgements.
// Coalesce offset submissions so a quarantine flood cannot queue one broker
// commit request per rejected frame in librdkafka.
fn claim_hint_checkpoint(next: &mut Instant, now: Instant) -> bool {
    if now < *next {
        return false;
    }
    *next = now + Duration::from_secs(1);
    true
}

async fn book_resync_requested(
    listener: &mut Option<BookResyncListener>,
    config: &IngestorConfig,
    bindings: &BindingMap,
    session: &str,
    generation: u64,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let Some(listener) = listener else {
        return std::future::pending().await;
    };
    tokio::time::sleep_until(listener.retry_after).await;
    let received = listener.source.next().await;
    let (record, _) = match received {
        Ok(record) => record,
        Err(error) => {
            listener.failures = listener.failures.saturating_add(1);
            listener.retry_after =
                Instant::now() + Duration::from_secs(u64::from(listener.failures).min(30));
            eprintln!(
                "{}",
                json!({"event":"qdl_book_resync_receive_failed", "error":error.to_string()})
            );
            return Ok(false);
        }
    };
    let requested = matches_book_resync(&record, config, bindings, session, generation, now_ns()?)
        && !snapshot_satisfies_resync(&record, &listener.published_anchors);
    // A crash before reconnect is recovered by the core's bounded reissue.
    // Checkpoint failure may replay this hint, but not across a new WS session.
    if !claim_hint_checkpoint(&mut listener.next_checkpoint, Instant::now()) {
        return Ok(requested);
    }
    if let Err(error) = listener.source.checkpoint_hint() {
        listener.failures = listener.failures.saturating_add(1);
        listener.retry_after =
            Instant::now() + Duration::from_secs(u64::from(listener.failures).min(30));
        eprintln!(
            "{}",
            json!({"event":"qdl_book_resync_checkpoint_failed", "error":error.to_string()})
        );
    } else {
        listener.failures = 0;
    }
    Ok(requested)
}

/// In-band transport evidence for the books of one Binance connection
/// (IDR-040 fix 2).
///
/// Binance pushes nothing for a book that does not change, so a quiet book is
/// otherwise indistinguishable from a stalled one. TCP delivers a connection's
/// messages in order: when any message (a data frame or the pong to our ping)
/// is read at `t`, every frame the venue sent before it on this connection has
/// been read and enqueued. A marker stamped `t` and enqueued into a book's own
/// raw partition therefore follows all of that book's earlier frames. It is
/// emitted only for a book with no frame for one interval, never for a book
/// that is not on this connection, and never with evidence older than one
/// interval (the connection is pinged instead).
struct BookLivenessProof {
    interval_ns: i64,
    last_transport_at_ns: i64,
    last_frame_at_ns: HashMap<String, i64>,
    last_marker_at_ns: HashMap<String, i64>,
}

impl BookLivenessProof {
    fn new(interval: Duration, bindings: &BindingMap, connected_at_ns: i64) -> Self {
        let interval_ns = i64::try_from(interval.as_nanos()).unwrap_or(i64::MAX);
        let books = bindings
            .values()
            .filter(|binding| binding.feed == RawFeed::Book)
            .map(|binding| (binding.native_channel.clone(), connected_at_ns));
        Self {
            interval_ns,
            last_transport_at_ns: connected_at_ns,
            last_frame_at_ns: books.collect(),
            last_marker_at_ns: HashMap::new(),
        }
    }

    fn transport(&mut self, at_ns: i64) {
        self.last_transport_at_ns = self.last_transport_at_ns.max(at_ns);
    }

    fn frame(&mut self, channel: &str, at_ns: i64) {
        if let Some(last) = self.last_frame_at_ns.get_mut(channel) {
            *last = (*last).max(at_ns);
        }
    }

    /// Markers due now, and whether the connection should be pinged so the
    /// next tick has evidence newer than half an interval.
    fn due(
        &mut self,
        bindings: &BindingMap,
        session_id: &str,
        generation: u64,
        now_ns: i64,
    ) -> (Vec<PendingRawFrame>, bool) {
        let evidence = self.last_transport_at_ns;
        let age = now_ns.saturating_sub(evidence);
        let ping = age >= self.interval_ns / 2;
        if age > self.interval_ns {
            return (vec![], ping);
        }
        let mut channels = self.last_frame_at_ns.keys().cloned().collect::<Vec<_>>();
        channels.sort();
        let mut frames = Vec::new();
        for channel in channels {
            let quiet = self
                .last_frame_at_ns
                .get(&channel)
                .is_some_and(|last| evidence.saturating_sub(*last) >= self.interval_ns);
            let newer = self
                .last_marker_at_ns
                .get(&channel)
                .map_or(true, |last| evidence > *last);
            let Some(binding) = bindings.get(&channel) else {
                continue;
            };
            if !quiet || !newer {
                continue;
            }
            let marker = json!({
                L2_TRANSPORT_LIVENESS_KEY: L2_TRANSPORT_LIVENESS_VERSION,
                "stream": channel,
            });
            self.last_marker_at_ns.insert(channel.clone(), evidence);
            frames.push(PendingRawFrame {
                binding: binding.clone(),
                session_id: session_id.into(),
                generation,
                raw_frame: marker.to_string().into_bytes(),
                received_at_ns: evidence,
                transport_protocol: TransportProtocol::Websocket,
            });
        }
        (frames, ping)
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_binance_connection(
    config: Arc<IngestorConfig>,
    route: BinanceRoute,
    shard_index: usize,
    shard_bindings: Vec<RawBinding>,
    publisher: Arc<RawPublisher>,
    telemetry: Arc<TelemetryRegistry>,
    accepted: Arc<AtomicU64>,
    coalesced_latest: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if shard_bindings.is_empty() {
        return Ok(());
    }
    // The routed base and the feed class are both part of this lane's identity.
    // Two lanes that shared a name would share a transactional producer, a
    // session-liveness file and a connection-generation counter; a shared
    // generation counter across lanes is exactly what quarantined every OKX
    // candle on 2026-09-17, and a lane whose name moves when a neighbour is
    // added is what silently restarts a book that nothing asked to change.
    //
    // A feed lane holds one feed class by construction, so the first binding
    // names the whole lane.
    let route_name = route.segment();
    let feed_name = feed_lane_name(
        shard_bindings
            .first()
            .map(|binding| binding.feed)
            .ok_or("Binance lane has no bindings")?,
    );
    let bindings: BindingMap = shard_bindings
        .into_iter()
        .map(|binding| (binding.native_channel.clone(), Arc::new(binding)))
        .collect();
    let streams = bindings.keys().cloned().collect::<Vec<_>>();
    let lane = format!("binance-{route_name}-{feed_name}-{shard_index:03}");
    let url = match route {
        BinanceRoute::Public => config.websocket_url.clone(),
        BinanceRoute::Market => config
            .market_websocket_url
            .clone()
            .ok_or("Binance market WebSocket URL is required")?,
    };
    let telemetry = telemetry.lane(&lane);
    let mut resync_listener = book_resync_listener(&config, &lane, &bindings)?;
    let snapshot_client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .build()?;
    let snapshot_period = match book_session_bootstrap(ProviderRuntime::Binance, &bindings) {
        Some(BookSessionBootstrap::PeriodicRestAnchor(period)) => Some(period),
        Some(BookSessionBootstrap::InitialSnapshotAndGapResync) => {
            return Err("Binance BOOK lane cannot use an OKX session policy".into())
        }
        None => None,
    };
    let liveness_proof_interval = (snapshot_period.is_some() && config.book_liveness_proof_ms > 0)
        .then(|| Duration::from_millis(config.book_liveness_proof_ms));
    let liveness_policy = config
        .liveness()
        .ok_or("native raw ingestor liveness bounds are invalid")?;
    let backoff = BackoffPolicy {
        initial_ms: 250,
        maximum_ms: 30_000,
        multiplier: 2,
        jitter_bps: 2_000,
    }
    .validate()?;
    let generation_path = format!("{}.{lane}", config.generation_state_path);
    let expires = deadline(config.max_runtime_seconds);
    let mut failures = 0_u32;
    while !should_stop(&stopped, &accepted, config.max_events, expires) {
        let generation = connection_generation(&generation_path).await?;
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
                                pre_ack_frames.push_back((payload, now_ns()?));
                            }
                        }
                        Message::Close(_) => {
                            return Err("Binance socket closed before subscription ACK".into());
                        }
                        _ => {}
                    }
                }
                // `<runtime>-<shard>-<generation>-<nanos>`: the ordering fence
                // reads everything before the generation as the lane, so the
                // route has to sit inside it for two routed lanes to be
                // distinguishable producers.
                let session_id = format!(
                    "binance-{}-{route_name}-{feed_name}-{shard_index:03}-{generation}-{}",
                    config.market_name(),
                    now_ns()?
                );
                let mut liveness = SessionLivenessWriter::new(&config, &lane)?;
                telemetry.session(generation);
                let connected_at_ns = now_ns()?;
                liveness.live(&session_id, generation, connected_at_ns)?;
                let mut book_proof = liveness_proof_interval
                    .map(|interval| BookLivenessProof::new(interval, &bindings, connected_at_ns));
                let mut book_proof_tick = liveness_proof_interval.map(tokio::time::interval);
                if let Some(tick) = book_proof_tick.as_mut() {
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    tick.tick().await;
                }
                let mut ping_due = false;
                let mut inbound = InboundDeadline::new(liveness_policy, Instant::now());
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
                            telemetry: &telemetry,
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
                        let anchor_key = frame.binding.subscription_id.clone();
                        let anchor = (
                            frame.session_id.clone(),
                            frame.generation,
                            frame.received_at_ns,
                        );
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
                                telemetry: &telemetry,
                            },
                        )
                        .await?
                        {
                            exhausted = true;
                            break;
                        }
                        if let Some(listener) = resync_listener.as_mut() {
                            listener.published_anchors.insert(anchor_key, anchor);
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
                    if ping_due {
                        ping_due = false;
                        socket.send(Message::Ping(Vec::new())).await?;
                    }
                    // Absolute deadline: the proof tick or any other branch
                    // winning never extends it (R4).
                    let read = tokio::time::timeout_at(inbound.deadline(), socket.next());
                    tokio::pin!(read);
                    // Control backlog must not take priority over socket and publish progress.
                    let outcome = tokio::select! {
                        _ = async {
                            match book_proof_tick.as_mut() {
                                Some(tick) => {
                                    tick.tick().await;
                                }
                                None => std::future::pending::<()>().await,
                            }
                        }, if book_proof_tick.is_some() => {
                            let Some(proof) = book_proof.as_mut() else {
                                continue;
                            };
                            let (markers, ping) =
                                proof.due(&bindings, &session_id, generation, now_ns()?);
                            ping_due = ping;
                            for marker in markers {
                                if !publish_pending_frame(
                                    marker,
                                    &mut PendingPublishWindow {
                                        publisher: &publisher,
                                        latest: &mut latest,
                                        inflight: &mut inflight,
                                        accepted: &accepted,
                                        coalesced_latest: &coalesced_latest,
                                        max_events: config.max_events,
                                        max_inflight: config.max_inflight_publishes,
                                        stopped: &stopped,
                                        telemetry: &telemetry,
                                    },
                                )
                                .await?
                                {
                                    exhausted = true;
                                    break;
                                }
                            }
                            continue;
                        }
                        requested = book_resync_requested(&mut resync_listener, &config, &bindings, &session_id, generation) => {
                            if requested? {
                                eprintln!("{{\"event\":\"qdl_native_book_resync_requested\",\"session\":\"{}\",\"generation\":{}}}", session_id, generation);
                                disconnected = true;
                                break;
                            }
                            continue;
                        }
                        _ = latest_tick.tick(), if !latest.is_empty() => {
                            if !flush_latest_concurrent(
                                &mut latest,
                                &publisher,
                                &mut inflight,
                                &accepted,
                                config.max_events,
                                config.max_inflight_publishes,
                                &stopped,
                                &telemetry,
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
                                let anchor_key = frame.binding.subscription_id.clone();
                                let anchor = (frame.session_id.clone(), frame.generation, frame.received_at_ns);
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
                                        telemetry: &telemetry,
                                    },
                                )
                                .await?
                                {
                                    exhausted = true;
                                    break;
                                }
                                if let Some(listener) = resync_listener.as_mut() {
                                    listener.published_anchors.insert(anchor_key, anchor);
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
                                let read_at_ns = now_ns()?;
                                let read_at = Instant::now();
                                inbound.inbound(read_at);
                                telemetry.read(read_at, read_at_ns);
                                liveness.live(&session_id, generation, read_at_ns)?;
                                if let Some(proof) = book_proof.as_mut() {
                                    proof.transport(read_at_ns);
                                }
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
                            liveness
                                .disconnected(&session_id, generation, now_ns()?)
                                .await?;
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
                            liveness
                                .disconnected(&session_id, generation, now_ns()?)
                                .await?;
                            disconnected = true;
                            break;
                        }
                        // The pong (or any frame) arrives through the same
                        // select, so other branches keep running meanwhile; the
                        // ping itself never moves the deadline.
                        Err(_) => match inbound.expired(Instant::now()) {
                            Keepalive::SendPing => {
                                socket.send(Message::Ping(Vec::new())).await?;
                                continue;
                            }
                            Keepalive::Dead => {
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
                                liveness
                                    .disconnected(&session_id, generation, now_ns()?)
                                    .await?;
                                disconnected = true;
                                break;
                            }
                        },
                    };
                    if let Message::Ping(payload) = message {
                        socket.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    if matches!(message, Message::Pong(_)) {
                        continue;
                    }
                    if matches!(message, Message::Close(_)) {
                        liveness
                            .disconnected(&session_id, generation, now_ns()?)
                            .await?;
                        disconnected = true;
                        break;
                    }
                    if !message.is_text() {
                        continue;
                    }
                    // The text moves out of the message: no copy of the frame.
                    let raw_text = message.into_text()?;
                    let frame = pending_binance_frame(
                        &bindings,
                        &session_id,
                        generation,
                        raw_text,
                        now_ns()?,
                    )?;
                    if let Some(proof) = book_proof.as_mut() {
                        proof.frame(&frame.binding.native_channel, frame.received_at_ns);
                    }
                    telemetry.frame(frame.received_at_ns);
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
                            telemetry: &telemetry,
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
                        &telemetry,
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
                liveness
                    .disconnected(&session_id, generation, now_ns()?)
                    .await?;
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
    telemetry: Arc<TelemetryRegistry>,
    accepted: Arc<AtomicU64>,
    coalesced_latest: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // One socket group per routed base, feed lanes inside each. A lane is named
    // `binance-<route>-<feed>-<shard>` and the shard counts inside its feed, so
    // adding or removing a feed never renames a lane that did not change.
    let mut lanes = Vec::new();
    for route in [BinanceRoute::Public, BinanceRoute::Market] {
        for lane in partition_binance_route(
            &config.bindings,
            route,
            config.max_subscriptions_per_connection,
        )? {
            lanes.push((route, lane));
        }
    }
    let publisher = Arc::new(RawPublisher::new(&config, "binance-raw", lanes.len())?);
    let mut futures = Vec::new();
    for (route, lane) in lanes {
        futures.push(run_binance_connection(
            config.clone(),
            route,
            lane.shard_index,
            lane.bindings,
            Arc::clone(&publisher),
            Arc::clone(&telemetry),
            accepted.clone(),
            coalesced_latest.clone(),
            stopped.clone(),
        ));
    }
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
    /// Absolute bound on one connect + subscribe (`OKX_ESTABLISH_BUDGET` in
    /// production; tests shorten it).
    establish_budget: Duration,
}

/// IDR-112 review F1: absolute bound on one OKX connect + subscribe, counted
/// from the moment the connect pacer admits the attempt. Progress (ping, pong,
/// notice, a partial ACK set) never extends it, so a peer that keeps a socket
/// busy with control traffic cannot hold a standby pending. Above the 10 s
/// per-read ACK timeout that predates it (a healthy establishment takes well
/// under a second) and well under the 60 s OKX keeps a socket open after a
/// 64008 notice, so a failed standby leaves room for a repeated notice to
/// retry.
const OKX_ESTABLISH_BUDGET: Duration = Duration::from_secs(15);

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type OkxSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type OkxWriter = futures_util::stream::SplitSink<OkxSocket, Message>;
type OkxReader = futures_util::stream::SplitStream<OkxSocket>;
type OkxStandby = Pin<Box<dyn Future<Output = Result<OkxEstablished, OkxEstablishError>> + Send>>;

/// OKX documents at most three WebSocket connection requests per second per
/// IP. Every OKX lane of this process connects through one pacer, so an
/// immediate reconnect of several lanes at once cannot exceed that limit.
const OKX_CONNECT_SPACING: Duration = Duration::from_millis(350);

#[derive(Clone, Default)]
struct OkxConnectPacer {
    next: Arc<tokio::sync::Mutex<Option<Instant>>>,
}

impl OkxConnectPacer {
    async fn admit(&self) {
        let mut next = self.next.lock().await;
        if let Some(at) = *next {
            tokio::time::sleep_until(at).await;
        }
        *next = Some(Instant::now() + OKX_CONNECT_SPACING);
    }
}

/// Why an OKX session ended; logged with every disconnect so the cause of a
/// stale window is evidence, not inference.
#[derive(Clone, Debug, Eq, PartialEq)]
enum OkxSessionEnd {
    /// `event: notice` (64008: service upgrade). The standby session was
    /// subscribed (every ACK) before this one stopped reading; that orders the
    /// generations, it does not prove the successor already delivered a frame
    /// for every binding (see the handover in `run_okx_service`).
    NoticeHandover {
        code: String,
    },
    /// `event: notice`, but no standby session could be established before
    /// the old connection ended.
    Notice {
        code: String,
    },
    /// IDR-112: the lane was stale against its venue timestamps; a standby
    /// session subscribed before this one stopped reading (make-before-break).
    StalenessHandover {
        stale_frames: usize,
        max_excess_ms: i64,
    },
    Closed {
        code: Option<u16>,
        reason: String,
    },
    Eof,
    ReadError(String),
    WriteError(String),
    HeartbeatTimeout,
    BookResync,
}

impl OkxSessionEnd {
    fn cause(&self) -> &'static str {
        match self {
            Self::NoticeHandover { .. } => "NOTICE_HANDOVER",
            Self::StalenessHandover { .. } => "STALENESS_HANDOVER",
            Self::Notice { .. } => "NOTICE",
            Self::Closed { .. } => "CLOSE_FRAME",
            Self::Eof => "EOF",
            Self::ReadError(_) => "READ_ERROR",
            Self::WriteError(_) => "WRITE_ERROR",
            Self::HeartbeatTimeout => "HEARTBEAT_TIMEOUT",
            Self::BookResync => "BOOK_RESYNC",
        }
    }

    fn detail(&self) -> Value {
        match self {
            Self::NoticeHandover { code } | Self::Notice { code } => json!({"code": code}),
            Self::StalenessHandover {
                stale_frames,
                max_excess_ms,
            } => json!({"stale_frames": stale_frames, "max_excess_ms": max_excess_ms}),
            Self::Closed { code, reason } => json!({"code": code, "reason": reason}),
            Self::ReadError(error) | Self::WriteError(error) => json!({"error": error}),
            Self::Eof | Self::HeartbeatTimeout | Self::BookResync => Value::Null,
        }
    }

    fn handed_over(&self) -> bool {
        matches!(
            self,
            Self::NoticeHandover { .. } | Self::StalenessHandover { .. }
        )
    }
}

/// The OKX service-upgrade notice code, or None for any other payload.
///
/// OKX pushes `{"event":"notice","code":"64008",...}` 60 seconds before a
/// service upgrade closes the connection (public, private and, since
/// 2026-06-11, business endpoints).
fn okx_notice_code(payload: &Value) -> Option<String> {
    (payload.get("event").and_then(Value::as_str) == Some("notice")).then(|| {
        payload
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("missing")
            .to_owned()
    })
}

/// Delay before reconnecting after a session ended.
///
/// `failures` counts consecutive session ends and connect failures and is
/// reset by any durable publish. A session that delivered data reconnects at
/// once (`failures == 1`): a fixed backoff there is pure stale window. A
/// session that ended again without delivering anything backs off as before.
fn okx_reconnect_delay_ms(backoff: BackoffPolicy, failures: u32) -> u64 {
    if failures <= 1 {
        0
    } else {
        backoff.delay_ms(failures, failures.min(10_000) as u16)
    }
}

#[derive(Debug, Eq, PartialEq)]
enum Keepalive {
    SendPing,
    Dead,
}

/// Smallest silence before a liveness ping. Venue limits checked 2026-10-10:
/// Binance USD-M allows 10 incoming messages per second per connection and
/// OKX lists no ping limit (only subscribe/unsubscribe/login, 480 per hour).
/// One ping per 500 ms of silence is at most 2 messages per second.
const LIVENESS_PING_MIN_MS: u64 = 500;
/// Smallest time a ping is given to be answered before the socket is dead.
const LIVENESS_PONG_WINDOW_MIN_MS: u64 = 1_000;
/// OKX closes a socket after 30 s without data and asks for a client timer
/// N < 30 s, so no liveness bound may reach it.
const LIVENESS_DEAD_MAX_MS: u64 = 29_000;

/// When a silent connection is pinged and when it is declared dead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LivenessPolicy {
    /// Inbound silence after which one ping is sent.
    ping_after: Duration,
    /// Inbound silence after which the connection is dead, provided the ping
    /// has had at least `dead_after - ping_after` to be answered.
    dead_after: Duration,
}

impl LivenessPolicy {
    /// The pre-knob rule: ping after `heartbeat`, dead at twice that.
    fn heartbeat(heartbeat: Duration) -> Self {
        Self {
            ping_after: heartbeat,
            dead_after: heartbeat * 2,
        }
    }

    /// Validated sub-second policy; both zero selects the heartbeat rule.
    fn from_config(heartbeat_seconds: u64, ping_ms: u64, dead_ms: u64) -> Option<Self> {
        match (ping_ms, dead_ms) {
            (0, 0) => Some(Self::heartbeat(Duration::from_secs(heartbeat_seconds))),
            (ping, dead)
                if ping >= LIVENESS_PING_MIN_MS
                    && dead <= LIVENESS_DEAD_MAX_MS
                    && dead.checked_sub(ping)? >= LIVENESS_PONG_WINDOW_MIN_MS =>
            {
                Some(Self {
                    ping_after: Duration::from_millis(ping),
                    dead_after: Duration::from_millis(dead),
                })
            }
            _ => None,
        }
    }

    fn pong_window(&self) -> Duration {
        self.dead_after.saturating_sub(self.ping_after)
    }
}

impl IngestorConfig {
    /// None when the liveness fields are out of bounds (validation refuses).
    fn liveness(&self) -> Option<LivenessPolicy> {
        LivenessPolicy::from_config(
            self.heartbeat_seconds,
            self.liveness_ping_ms,
            self.liveness_dead_ms,
        )
    }
}

/// Hard bound on detecting a silent socket (Astra R4).
///
/// The read loops also select on timers (book-liveness proof, latest-state
/// flush, resync listener, snapshot renewal, publish completion). A read
/// timeout created afresh on every iteration is discarded whenever one of
/// those wins, so a socket that sends neither data nor pong was never
/// declared dead while any of them kept firing. This deadline is absolute: it
/// moves only on a message actually read from the socket (data, ping, pong or
/// OKX text `pong`). Sending a ping, a timer tick or a publish completion never
/// moves it. Idle for `ping_after` -> one ping; still nothing by `dead_after`
/// after the last inbound message -> dead. With the heartbeat rule
/// (`dead_after = 2 * ping_after`) that matches OKX's keepalive rule (send
/// `ping` after N < 30 idle seconds, reconnect if no `pong` within N).
///
/// The ping is always given its full pong window from the moment it was
/// actually sent: if the loop was held past `ping_after` (a REST snapshot
/// fetch inside a select branch can take up to 5 s), the dead deadline moves
/// to `sent + (dead_after - ping_after)` instead of firing before the venue
/// could have answered. A busy socket never pings: every frame read resets it.
#[derive(Debug)]
struct InboundDeadline {
    policy: LivenessPolicy,
    last_inbound: Instant,
    ping_sent_at: Option<Instant>,
}

impl InboundDeadline {
    fn new(policy: LivenessPolicy, now: Instant) -> Self {
        Self {
            policy,
            last_inbound: now,
            ping_sent_at: None,
        }
    }

    fn inbound(&mut self, now: Instant) {
        self.last_inbound = now;
        self.ping_sent_at = None;
    }

    fn deadline(&self) -> Instant {
        match self.ping_sent_at {
            None => self.last_inbound + self.policy.ping_after,
            Some(sent) => {
                (self.last_inbound + self.policy.dead_after).max(sent + self.policy.pong_window())
            }
        }
    }

    /// Called when `deadline()` has passed with nothing read.
    fn expired(&mut self, now: Instant) -> Keepalive {
        if self.ping_sent_at.is_some() {
            Keepalive::Dead
        } else {
            self.ping_sent_at = Some(now);
            Keepalive::SendPing
        }
    }
}

/// IDR-112: venue-timestamp staleness recycle of an OKX latest-state QUOTE lane.
///
/// Evidence (stage-100 135719Z RCA, DL plan 62a67ee/27c28bd): one OKX public
/// session (okx-public-003, bbo-tbt) delivered bursts of frames whose venue
/// `ts` was 0.45-1.5 s old while a fresh connection to the same edge IP saw a
/// maximum of 60 ms (healthy lag p50 29, p99 35 ms). The socket stayed busy,
/// so the silence-based liveness never pinged, and the session served for
/// hours. The detector measures `lag = host time at read - venue ts` per
/// frame, compares it with the lane's own recent baseline, and when the lane
/// is stale in a sustained way recycles the session through the 64008
/// make-before-break standby path (successor subscribed before the old socket
/// stops being read).
///
/// Scope: only lanes whose every binding is a QUOTE feed with LATEST_STATE
/// delivery. A bbo-tbt frame is the current top of book, superseded by the
/// next one, and is pushed on change with the matching-engine time, so an old
/// `ts` means a late frame. Not TRADE / BAR (lossless: every event matters),
/// not BOOK (stateful: a session change forces a re-snapshot), not MARK_INDEX
/// (OKX re-pushes an unchanged mark/index every 10 s, so its `ts` age is not a
/// delivery delay). Binance lanes have no make-before-break path and are not
/// covered.
///
/// Clock skew: host time is a monotonic clock anchored once to the wall clock
/// (an NTP step moves neither the lag nor the baseline), and staleness is the
/// excess over the baseline, the median of per-bucket minimum lags over the
/// horizon. A constant venue/host offset cancels out; a single frame with a
/// bogus timestamp moves one bucket minimum, not the median.
///
/// Availability trade-offs (review F3, accepted; the detector is relative, not
/// an absolute freshness check, and never makes data eligible): a session that
/// is stale for more than half the horizon, or already delayed when the lane
/// starts, becomes its own baseline and is not detected; with `min_bindings`
/// 2 a delay confined to one instrument never triggers. Unusable data stays
/// the downstream validator's to refuse. The progress line shows the absolute
/// lag (`venue_lag_ms`), whether the baseline is armed
/// (`staleness_baseline_ms` null until it is) and the last trigger outcome
/// (`staleness_last_action`, `staleness_suppressed`).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
struct LatestStateStalenessConfig {
    /// Off keeps the pre-IDR-112 behaviour exactly (no parse, no recycle).
    enabled: bool,
    /// A frame is stale when its lag exceeds the baseline by at least this.
    excess_ms: u64,
    /// Stale frames are counted over this sliding host-time window.
    window_ms: u64,
    /// Stale frames needed inside the window (>= 2: one late frame never triggers).
    min_frames: usize,
    /// Distinct bindings among them (clamped to the lane's binding count).
    min_bindings: usize,
    /// Baseline bucket length; each bucket keeps its minimum lag.
    baseline_bucket_ms: u64,
    /// Buckets kept (horizon = buckets * bucket length).
    baseline_buckets: usize,
    /// Completed buckets needed before the detector arms.
    min_baseline_buckets: usize,
    /// No new recycle within this time after the previous one started.
    cooldown_ms: u64,
    /// Recycles started in any rolling hour, per lane.
    max_recycles_per_hour: usize,
}

impl Default for LatestStateStalenessConfig {
    /// Conservative defaults from the 135719Z evidence: healthy lag p99 35 ms
    /// (max 60 ms on the parallel probe), bad bursts 0.45-1.5 s. The 250 ms
    /// excess is > 4x the healthy maximum and below the smallest bad-window
    /// maximum (446.9 ms); 5 frames from >= 2 instruments inside 2 s is a
    /// lane-wide burst (the bad bursts hit all six quote symbols together),
    /// not one late frame. Baseline: 10 s buckets over 10 min, armed after
    /// 30 s. At most one recycle per minute and six per hour per lane, far
    /// under OKX's 3 connects/s per IP and 480 subscribe requests/h.
    fn default() -> Self {
        Self {
            enabled: true,
            excess_ms: 250,
            window_ms: 2_000,
            min_frames: 5,
            min_bindings: 2,
            baseline_bucket_ms: 10_000,
            baseline_buckets: 60,
            min_baseline_buckets: 3,
            cooldown_ms: 60_000,
            max_recycles_per_hour: 6,
        }
    }
}

impl LatestStateStalenessConfig {
    fn validate(&self) -> Result<(), String> {
        if (100..=10_000).contains(&self.excess_ms)
            && (100..=60_000).contains(&self.window_ms)
            && (2..=10_000).contains(&self.min_frames)
            && (1..=1_024).contains(&self.min_bindings)
            && (1_000..=600_000).contains(&self.baseline_bucket_ms)
            && (3..=1_440).contains(&self.baseline_buckets)
            && (1..=self.baseline_buckets).contains(&self.min_baseline_buckets)
            && (10_000..=3_600_000).contains(&self.cooldown_ms)
            && (1..=60).contains(&self.max_recycles_per_hour)
        {
            Ok(())
        } else {
            Err("latest_state_staleness bounds are invalid".into())
        }
    }
}

/// True when the staleness recycle may act on this lane (see the config).
fn staleness_applies(bindings: &BindingMap) -> bool {
    !bindings.is_empty()
        && bindings.values().all(|binding| {
            binding.feed == RawFeed::Quote && binding.delivery_class == DeliveryClass::LatestState
        })
}

/// The newest venue timestamp (ms) of an OKX data frame: `data[*].ts`.
/// None when absent or not a positive integer string; such a frame is not
/// measured (it can never trigger a recycle).
fn okx_venue_ts_ms(raw: &str) -> Option<i64> {
    #[derive(Deserialize)]
    struct Frame<'a> {
        #[serde(borrow, default)]
        data: Vec<Item<'a>>,
    }
    #[derive(Deserialize)]
    struct Item<'a> {
        #[serde(borrow, default)]
        ts: Option<std::borrow::Cow<'a, str>>,
    }
    let frame: Frame<'_> = serde_json::from_str(raw).ok()?;
    frame
        .data
        .iter()
        .filter_map(|item| item.ts.as_deref()?.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .max()
}

/// What one sustained-staleness detection saw.
#[derive(Clone, Debug, Eq, PartialEq)]
struct StalenessTrigger {
    stale_frames: usize,
    distinct_bindings: usize,
    /// Host-time span from the first to the last stale frame counted.
    span_ms: i64,
    max_lag_ms: i64,
    max_excess_ms: i64,
    baseline_ms: i64,
}

impl StalenessTrigger {
    fn report(&self) -> Value {
        json!({
            "stale_frames": self.stale_frames,
            "distinct_bindings": self.distinct_bindings,
            "span_ms": self.span_ms,
            "max_lag_ms": self.max_lag_ms,
            "max_excess_ms": self.max_excess_ms,
            "baseline_ms": self.baseline_ms,
        })
    }
}

/// One measured frame.
#[derive(Debug)]
struct StalenessObservation {
    lag_ms: i64,
    stale: bool,
    trigger: Option<StalenessTrigger>,
}

#[derive(Debug, Eq, PartialEq)]
enum RecycleDecision {
    Recycle,
    Cooldown { remaining_ms: i64 },
    Cap { recycles_last_hour: usize },
}

const STALENESS_CAP_WINDOW_MS: i64 = 3_600_000;
/// Suppressed triggers are logged at most once per this interval.
const STALENESS_SUPPRESSED_LOG_MS: i64 = 10_000;

/// Per-lane detector; lives across sessions so the baseline, cooldown and cap
/// survive a recycle. Pure: all times are passed in (host ms).
#[derive(Debug)]
struct StalenessDetector {
    config: LatestStateStalenessConfig,
    min_bindings: usize,
    anchor_ns: i64,
    anchor: Instant,
    /// (bucket index, minimum lag) of the bucket being filled.
    current: Option<(i64, i64)>,
    completed: VecDeque<(i64, i64)>,
    baseline_ms: Option<i64>,
    /// (host ms, binding key, lag, excess) of stale frames inside the window.
    stale: VecDeque<(i64, String, i64, i64)>,
    recycles: VecDeque<i64>,
    suppressed: u64,
    suppressed_logged_ms: Option<i64>,
}

impl StalenessDetector {
    fn new(
        config: LatestStateStalenessConfig,
        lane_bindings: usize,
        anchor_ns: i64,
        anchor: Instant,
    ) -> Self {
        Self {
            config,
            min_bindings: config.min_bindings.min(lane_bindings.max(1)),
            anchor_ns,
            anchor,
            current: None,
            completed: VecDeque::new(),
            baseline_ms: None,
            stale: VecDeque::new(),
            recycles: VecDeque::new(),
            suppressed: 0,
            suppressed_logged_ms: None,
        }
    }

    /// Host time (ms) of a monotonic instant, anchored once to the wall clock.
    fn host_ms(&self, at: Instant) -> i64 {
        let elapsed =
            i64::try_from(at.saturating_duration_since(self.anchor).as_nanos()).unwrap_or(i64::MAX);
        self.anchor_ns.saturating_add(elapsed) / 1_000_000
    }

    fn baseline_ms(&self) -> Option<i64> {
        self.baseline_ms
    }

    /// A new session: stale frames of the previous one never count for it.
    fn session_started(&mut self) {
        self.stale.clear();
    }

    fn observe_at(
        &mut self,
        at: Instant,
        venue_ms: i64,
        binding: &RawBinding,
    ) -> StalenessObservation {
        let host_ms = self.host_ms(at);
        self.observe(host_ms, venue_ms, binding)
    }

    fn observe(
        &mut self,
        host_ms: i64,
        venue_ms: i64,
        binding: &RawBinding,
    ) -> StalenessObservation {
        let lag_ms = host_ms.saturating_sub(venue_ms);
        let bucket_ms = i64::try_from(self.config.baseline_bucket_ms).unwrap_or(i64::MAX);
        let bucket = host_ms.div_euclid(bucket_ms);
        match self.current.as_mut() {
            Some((index, minimum)) if *index == bucket => *minimum = (*minimum).min(lag_ms),
            Some(_) | None => {
                if let Some(done) = self.current.replace((bucket, lag_ms)) {
                    self.completed.push_back(done);
                }
                let horizon = i64::try_from(self.config.baseline_buckets).unwrap_or(i64::MAX);
                while self
                    .completed
                    .front()
                    .is_some_and(|(index, _)| *index <= bucket.saturating_sub(horizon))
                {
                    self.completed.pop_front();
                }
                self.baseline_ms =
                    (self.completed.len() >= self.config.min_baseline_buckets).then(|| {
                        let mut minima = self
                            .completed
                            .iter()
                            .map(|(_, minimum)| *minimum)
                            .collect::<Vec<_>>();
                        minima.sort_unstable();
                        minima[(minima.len() - 1) / 2]
                    });
            }
        }
        let Some(baseline_ms) = self.baseline_ms else {
            return StalenessObservation {
                lag_ms,
                stale: false,
                trigger: None,
            };
        };
        let window_ms = i64::try_from(self.config.window_ms).unwrap_or(i64::MAX);
        while self
            .stale
            .front()
            .is_some_and(|(at, ..)| *at < host_ms.saturating_sub(window_ms))
        {
            self.stale.pop_front();
        }
        let excess_ms = lag_ms.saturating_sub(baseline_ms);
        let stale = excess_ms >= i64::try_from(self.config.excess_ms).unwrap_or(i64::MAX);
        if stale {
            self.stale
                .push_back((host_ms, binding.key(), lag_ms, excess_ms));
        }
        let mut trigger = None;
        if stale && self.stale.len() >= self.config.min_frames {
            let mut keys = self
                .stale
                .iter()
                .map(|(_, key, ..)| key.as_str())
                .collect::<Vec<_>>();
            keys.sort_unstable();
            keys.dedup();
            if keys.len() >= self.min_bindings {
                trigger = Some(StalenessTrigger {
                    stale_frames: self.stale.len(),
                    distinct_bindings: keys.len(),
                    span_ms: host_ms - self.stale.front().map(|(at, ..)| *at).unwrap_or(host_ms),
                    max_lag_ms: self
                        .stale
                        .iter()
                        .map(|(_, _, lag, _)| *lag)
                        .max()
                        .unwrap_or(0),
                    max_excess_ms: self
                        .stale
                        .iter()
                        .map(|(.., excess)| *excess)
                        .max()
                        .unwrap_or(0),
                    baseline_ms,
                });
                self.stale.clear();
            }
        }
        StalenessObservation {
            lag_ms,
            stale,
            trigger,
        }
    }

    /// Whether a detected trigger may start a recycle now. Pure check: the
    /// slot is taken by `recycle_started`, once the connection attempt is
    /// actually admitted (review F3), so a refusal further on (OKX control
    /// budget) costs no cooldown or cap.
    fn decide(&mut self, host_ms: i64) -> RecycleDecision {
        while self
            .recycles
            .front()
            .is_some_and(|at| *at <= host_ms.saturating_sub(STALENESS_CAP_WINDOW_MS))
        {
            self.recycles.pop_front();
        }
        let cooldown_ms = i64::try_from(self.config.cooldown_ms).unwrap_or(i64::MAX);
        if let Some(last) = self.recycles.back() {
            let since = host_ms.saturating_sub(*last);
            if since < cooldown_ms {
                return RecycleDecision::Cooldown {
                    remaining_ms: cooldown_ms - since,
                };
            }
        }
        if self.recycles.len() >= self.config.max_recycles_per_hour {
            return RecycleDecision::Cap {
                recycles_last_hour: self.recycles.len(),
            };
        }
        RecycleDecision::Recycle
    }

    /// A recycle attempt started (standby connecting): it counts for the
    /// cooldown and the hourly cap whether or not the standby succeeds.
    fn recycle_started(&mut self, host_ms: i64) {
        self.recycles.push_back(host_ms);
    }

    /// Rate limit for suppressed-trigger log lines: Some(count suppressed
    /// since the previous line) when one is due.
    fn suppressed_log_due(&mut self, host_ms: i64) -> Option<u64> {
        self.suppressed += 1;
        let due = self
            .suppressed_logged_ms
            .is_none_or(|at| host_ms.saturating_sub(at) >= STALENESS_SUPPRESSED_LOG_MS);
        due.then(|| {
            self.suppressed_logged_ms = Some(host_ms);
            std::mem::take(&mut self.suppressed)
        })
    }
}

/// A staleness recycle whose standby session is being established.
#[derive(Debug)]
struct PendingRecycle {
    trigger: StalenessTrigger,
    started: Instant,
    successor_generation: u64,
}

struct OkxEstablished {
    writer: OkxWriter,
    reader: OkxReader,
    generation: u64,
    session_id: String,
    pre_ack_frames: VecDeque<PendingRawFrame>,
    connect_started_ns: i64,
    ready_at_ns: i64,
    /// A service-upgrade notice arrived while subscribing; this session must
    /// be replaced as soon as it is live.
    notice_code: Option<String>,
}

#[derive(Debug)]
enum OkxEstablishError {
    Connect(BoxError),
    Subscribe(BoxError),
    /// `OKX_ESTABLISH_BUDGET` ran out in `phase`; the attempt was cancelled
    /// (its socket dropped).
    Deadline {
        phase: OkxEstablishPhase,
        budget: Duration,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OkxEstablishPhase {
    Connect,
    Subscribe,
}

impl OkxEstablishError {
    /// Typed reason for a standby that did not become ready.
    fn standby_reason(&self) -> &'static str {
        match self {
            Self::Connect(_) => "STANDBY_CONNECT_ERROR",
            Self::Subscribe(_) => "STANDBY_SUBSCRIBE_ERROR",
            Self::Deadline { .. } => "STANDBY_DEADLINE",
        }
    }
}

impl std::fmt::Display for OkxEstablishError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(error) => write!(formatter, "connect: {error}"),
            Self::Subscribe(error) => write!(formatter, "subscribe: {error}"),
            Self::Deadline { phase, budget } => write!(
                formatter,
                "{} not complete within the {} ms establish budget",
                match phase {
                    OkxEstablishPhase::Connect => "connect",
                    OkxEstablishPhase::Subscribe => "subscribe",
                },
                budget.as_millis()
            ),
        }
    }
}

/// Connect one OKX session and wait until every subscription is ACKed.
///
/// It never publishes: frames that arrive before the last ACK are returned in
/// `pre_ack_frames`, stamped with this session's identity, for the caller to
/// publish only after the previous generation has stopped publishing. That
/// keeps the connection-generation fences (ordering tracker and L2 book core)
/// seeing generation N strictly before N+1 even when the two sockets overlap.
///
/// Connect and subscribe together must finish within `budget`, one absolute
/// deadline set once the pacer admits the attempt (review F1). Messages that
/// are not the missing ACKs never extend it; on expiry the attempt is
/// cancelled and its socket dropped (`OkxEstablishError::Deadline`).
#[allow(clippy::too_many_arguments)]
async fn okx_establish(
    url: String,
    subscriptions: Arc<Vec<OkxSubscription>>,
    bindings: Arc<BindingMap>,
    session_prefix: String,
    generation: u64,
    max_pre_ack_frames: usize,
    pacer: OkxConnectPacer,
    budget: Duration,
) -> Result<OkxEstablished, OkxEstablishError> {
    pacer.admit().await;
    let deadline = Instant::now() + budget;
    let expired = |phase| OkxEstablishError::Deadline { phase, budget };
    let connect_started_ns = now_ns().map_err(OkxEstablishError::Connect)?;
    let (socket, _) = tokio::time::timeout_at(deadline, connect_async(&url))
        .await
        .map_err(|_| expired(OkxEstablishPhase::Connect))?
        .map_err(|error| OkxEstablishError::Connect(error.into()))?;
    let (mut writer, mut reader) = socket.split();
    let subscribed = tokio::time::timeout_at(deadline, async {
        let command_id = generation.to_string();
        writer
            .send(Message::Text(okx_subscription_command(
                &command_id,
                &subscriptions,
            )?))
            .await?;
        let session_id = format!("{session_prefix}-{generation}-{}", now_ns()?);
        let mut pending = subscriptions
            .iter()
            .map(|item| (item.channel.clone(), item.inst_id.clone()))
            .collect::<Vec<_>>();
        let mut pre_ack_frames = VecDeque::new();
        let mut notice_code = None;
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
                    if raw_text == "pong" {
                        continue;
                    }
                    let decoded: Value = serde_json::from_str(&raw_text)?;
                    if let Some(code) = okx_notice_code(&decoded) {
                        notice_code = Some(code);
                        continue;
                    }
                    if parse_okx_subscription_ack(&decoded, &command_id, &mut pending)? {
                        continue;
                    }
                    if pre_ack_frames.len() >= max_pre_ack_frames {
                        return Err(
                            "OKX pre-ACK frame buffer exceeded durable publish bound".into()
                        );
                    }
                    pre_ack_frames.push_back(pending_okx_frame(
                        &bindings,
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
        Ok::<_, BoxError>((session_id, pre_ack_frames, notice_code))
    })
    .await;
    let (session_id, pre_ack_frames, notice_code) = subscribed
        .map_err(|_| expired(OkxEstablishPhase::Subscribe))?
        .map_err(OkxEstablishError::Subscribe)?;
    Ok(OkxEstablished {
        writer,
        reader,
        generation,
        session_id,
        pre_ack_frames,
        connect_started_ns,
        ready_at_ns: now_ns().map_err(OkxEstablishError::Subscribe)?,
        notice_code,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_okx_service(
    shard: OkxServiceShard,
    config: Arc<IngestorConfig>,
    publisher: Arc<RawPublisher>,
    telemetry: Arc<TelemetryRegistry>,
    accepted: Arc<AtomicU64>,
    coalesced_latest: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
    pacer: OkxConnectPacer,
) -> Result<(), BoxError> {
    let OkxServiceShard {
        service,
        shard_index,
        url,
        bindings,
        establish_budget,
    } = shard;
    if bindings.is_empty() {
        return Ok(());
    }
    let service_name = match service {
        OkxService::Public => "public",
        OkxService::Business => "business",
    };
    let lane = format!("okx-{service_name}-{shard_index:03}");
    let telemetry = telemetry.lane(&lane);
    let subscriptions: Arc<Vec<OkxSubscription>> = Arc::new(
        bindings
            .iter()
            .map(|binding| OkxSubscription {
                channel: binding.native_channel.clone(),
                inst_id: binding.native_symbol.clone(),
            })
            .collect(),
    );
    let binding_map: Arc<BindingMap> = Arc::new(
        bindings
            .into_iter()
            .map(|binding| (binding.key(), Arc::new(binding)))
            .collect(),
    );
    let mut resync_listener = book_resync_listener(&config, &lane, &binding_map)?;
    let liveness_policy = config
        .liveness()
        .ok_or("native raw ingestor liveness bounds are invalid")?;
    let backoff = BackoffPolicy {
        initial_ms: 250,
        maximum_ms: 30_000,
        multiplier: 2,
        jitter_bps: 2_000,
    }
    .validate()?;
    let expires = deadline(config.max_runtime_seconds);
    let generation_path = format!("{}.{lane}", config.generation_state_path);
    let mut failures = 0_u32;
    let mut budget = ControlRequestBudget::default();
    let mut liveness = SessionLivenessWriter::new(&config, &lane)?;
    let mut lineage = SessionLineageWriter::new(&config, &lane)?;
    // A standby session that was already established when the previous one
    // handed over; it is used instead of opening a new connection.
    let mut handover: Option<OkxEstablished> = None;
    // When the previous session ended, for the reconnect-time evidence.
    let mut previous_end: Option<(String, u64, i64, i64)> = None;
    let establish = |generation: u64| {
        okx_establish(
            url.clone(),
            subscriptions.clone(),
            binding_map.clone(),
            lane.clone(),
            generation,
            config.max_inflight_publishes,
            pacer.clone(),
            establish_budget,
        )
    };
    // IDR-112: one detector per lane, kept across sessions (baseline,
    // cooldown and cap survive a recycle).
    let staleness_config = config.latest_state_staleness;
    let mut staleness = (staleness_config.enabled && staleness_applies(&binding_map)).then(|| {
        StalenessDetector::new(
            staleness_config,
            binding_map.len(),
            now_ns().unwrap_or_default(),
            Instant::now(),
        )
    });
    if staleness.is_some() {
        eprintln!(
            "{}",
            serde_json::to_string(&json!({
                "event": "qdl_native_staleness_recycle_armed",
                "runtime": "OKX",
                "lane": lane,
                "bindings": binding_map.len(),
                "config": staleness_config,
            }))?
        );
    }
    while !should_stop(&stopped, &accepted, config.max_events, expires) {
        let continued = handover.is_some();
        let established = match handover.take() {
            Some(established) => established,
            None => {
                let generation = connection_generation(&generation_path).await?;
                budget.permit(now_ns()?)?;
                match establish(generation).await {
                    Ok(established) => established,
                    Err(OkxEstablishError::Subscribe(error)) => return Err(error),
                    // As before the budget: a subscription that never
                    // completes fails the lane; a connect that never completes
                    // is retried with backoff like a refused one.
                    Err(
                        error @ OkxEstablishError::Deadline {
                            phase: OkxEstablishPhase::Subscribe,
                            ..
                        },
                    ) => return Err(error.to_string().into()),
                    Err(error @ OkxEstablishError::Deadline { .. })
                    | Err(error @ OkxEstablishError::Connect(_)) => {
                        let error = match error {
                            OkxEstablishError::Connect(error) => error,
                            other => other.to_string().into(),
                        };
                        failures = failures.saturating_add(1);
                        eprintln!(
                            "{}",
                            serde_json::to_string(&json!({
                                "event": "qdl_native_connect_failed",
                                "runtime": "OKX",
                                "service": format!("{:?}", service).to_ascii_uppercase(),
                                "lane": lane,
                                "attempt": failures,
                                "error": error.to_string(),
                            }))?
                        );
                        tokio::time::sleep(Duration::from_millis(
                            backoff.delay_ms(failures, failures.min(10_000) as u16),
                        ))
                        .await;
                        continue;
                    }
                }
            }
        };
        let OkxEstablished {
            mut writer,
            mut reader,
            generation,
            session_id,
            mut pre_ack_frames,
            connect_started_ns,
            ready_at_ns,
            notice_code: early_notice,
        } = established;
        // Lineage first, then the session record: a reader that sees the new
        // record can always resolve the predecessor through the chain.
        lineage.begin(&session_id, generation, continued).await?;
        liveness.begin(&session_id, generation, ready_at_ns).await?;
        telemetry.session(generation);
        eprintln!(
            "{}",
            serde_json::to_string(&json!({
                "event": "qdl_native_session_ready",
                "runtime": "OKX",
                "service": format!("{:?}", service).to_ascii_uppercase(),
                "lane": lane,
                "generation": generation,
                "session": session_id,
                "continued": continued,
                "connect_to_ready_ms": (ready_at_ns - connect_started_ns) / 1_000_000,
                "previous_session": previous_end.as_ref().map(|value| value.0.clone()),
                "previous_generation": previous_end.as_ref().map(|value| value.1),
                "previous_last_frame_to_ready_ms": previous_end
                    .as_ref()
                    .map(|value| (ready_at_ns - value.2) / 1_000_000),
                "previous_end_to_ready_ms": previous_end
                    .as_ref()
                    .map(|value| (ready_at_ns - value.3) / 1_000_000),
            }))?
        );
        previous_end = None;
        let mut inflight = FuturesUnordered::<RawPublishFuture>::new();
        let mut latest = LatestStateBuffer::default();
        let mut latest_tick =
            tokio::time::interval(Duration::from_millis(config.latest_state_flush_ms));
        latest_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        latest_tick.tick().await;
        match book_session_bootstrap(ProviderRuntime::Okx, &binding_map) {
            Some(BookSessionBootstrap::InitialSnapshotAndGapResync) | None => {}
            Some(BookSessionBootstrap::PeriodicRestAnchor(_)) => {
                return Err("OKX BOOK lane cannot use a periodic REST-anchor policy".into())
            }
        }
        let mut ended: Option<OkxSessionEnd> = None;
        let mut publish_error = None;
        let mut exhausted = false;
        let mut last_frame_at_ns = ready_at_ns;
        let mut inbound = InboundDeadline::new(liveness_policy, Instant::now());
        let mut standby: Option<OkxStandby> = None;
        let mut notice: Option<String> = None;
        let mut recycle: Option<PendingRecycle> = None;
        if let Some(detector) = staleness.as_mut() {
            detector.session_started();
        }
        if let Some(code) = early_notice {
            let generation = connection_generation(&generation_path).await?;
            budget.permit(now_ns()?)?;
            standby = Some(Box::pin(establish(generation)));
            notice = Some(code);
        }
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
                    telemetry: &telemetry,
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
                    None => return Err("lossless publish window became inconsistent".into()),
                }
            }
            // Absolute deadline: other branches winning never extend it (R4).
            let read = tokio::time::timeout_at(inbound.deadline(), reader.next());
            tokio::pin!(read);
            // Control backlog must not take priority over socket and publish progress.
            let outcome = tokio::select! {
                requested = book_resync_requested(&mut resync_listener, &config, &binding_map, &session_id, generation) => {
                    if requested? {
                        eprintln!("{{\"event\":\"qdl_native_book_resync_requested\",\"session\":\"{}\",\"generation\":{}}}", session_id, generation);
                        ended = Some(OkxSessionEnd::BookResync);
                        break;
                    }
                    continue;
                }
                ready = async {
                    match standby.as_mut() {
                        Some(future) => future.await,
                        None => std::future::pending().await,
                    }
                } => {
                    standby = None;
                    match ready {
                        Ok(next) => {
                            // Make-before-break at subscription ACK: every
                            // subscription of the successor is ACKed while
                            // this socket still delivers, and this session
                            // stops before any successor frame is published,
                            // so generation N is never published after N+1.
                            // The ACK does NOT prove the successor already has
                            // a fresh frame for every binding (zero pre-ACK
                            // data is allowed): from the last frame published
                            // here to the successor's first frame for a
                            // binding, consumers keep the retained state and
                            // its own freshness/eligibility checks (review F2
                            // option A; the fake-venue tests measure that
                            // window per binding). A notice, if any, stays the
                            // cause (it would end the socket).
                            let successor_generation = next.generation;
                            handover = Some(next);
                            ended = Some(match (notice.as_ref(), recycle.take()) {
                                (None, Some(pending)) => {
                                    eprintln!(
                                        "{}",
                                        serde_json::to_string(&json!({
                                            "event": "qdl_native_staleness_recycled",
                                            "runtime": "OKX",
                                            "lane": lane,
                                            "generation": generation,
                                            "session": session_id,
                                            "successor_generation": successor_generation,
                                            "standby_ready_ms": pending.started.elapsed().as_millis() as u64,
                                            "trigger": pending.trigger.report(),
                                        }))?
                                    );
                                    telemetry.staleness_recycled();
                                    OkxSessionEnd::StalenessHandover {
                                        stale_frames: pending.trigger.stale_frames,
                                        max_excess_ms: pending.trigger.max_excess_ms,
                                    }
                                }
                                _ => OkxSessionEnd::NoticeHandover {
                                    code: notice.clone().unwrap_or_default(),
                                },
                            });
                            break;
                        }
                        Err(error) => {
                            // Only the standby is dropped: this session keeps
                            // delivering under its own checks, and the next
                            // trigger is governed by the cooldown and cap.
                            let failed_recycle = recycle.take();
                            let reason = error.standby_reason();
                            telemetry.standby_failed(reason);
                            eprintln!(
                                "{}",
                                serde_json::to_string(&json!({
                                    "event": "qdl_native_standby_failed",
                                    "runtime": "OKX",
                                    "lane": lane,
                                    "generation": generation,
                                    "cause": if notice.is_some() || failed_recycle.is_none() {
                                        "NOTICE"
                                    } else {
                                        "STALENESS"
                                    },
                                    "reason": reason,
                                    "successor_generation": failed_recycle
                                        .as_ref()
                                        .map(|pending| pending.successor_generation),
                                    "error": error.to_string(),
                                }))?
                            );
                            continue;
                        }
                    }
                }
                _ = latest_tick.tick(), if !latest.is_empty() => {
                    if !flush_latest_concurrent(
                        &mut latest,
                        &publisher,
                        &mut inflight,
                        &accepted,
                        config.max_events,
                        config.max_inflight_publishes,
                        &stopped,
                        &telemetry,
                    ).await? {
                        break;
                    }
                    failures = 0;
                    continue;
                }
                result = &mut read => result,
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
            let (message, read_at) = match outcome {
                Ok(Some(Ok(message))) => {
                    if let Message::Close(frame) = &message {
                        ended = Some(OkxSessionEnd::Closed {
                            code: frame.as_ref().map(|value| u16::from(value.code)),
                            reason: frame
                                .as_ref()
                                .map(|value| value.reason.to_string())
                                .unwrap_or_default(),
                        });
                        break;
                    }
                    let read_at_ns = now_ns()?;
                    last_frame_at_ns = read_at_ns;
                    let read_at = Instant::now();
                    inbound.inbound(read_at);
                    telemetry.read(read_at, read_at_ns);
                    liveness.live(&session_id, generation, read_at_ns)?;
                    (message, read_at)
                }
                Ok(Some(Err(error))) => {
                    ended = Some(OkxSessionEnd::ReadError(error.to_string()));
                    break;
                }
                Ok(None) => {
                    ended = Some(OkxSessionEnd::Eof);
                    break;
                }
                Err(_) => match inbound.expired(Instant::now()) {
                    Keepalive::Dead => {
                        ended = Some(OkxSessionEnd::HeartbeatTimeout);
                        break;
                    }
                    Keepalive::SendPing => {
                        if let Err(error) = writer.send(Message::Text("ping".into())).await {
                            ended = Some(OkxSessionEnd::WriteError(error.to_string()));
                            break;
                        }
                        continue;
                    }
                },
            };
            if let Message::Ping(payload) = message {
                if let Err(error) = writer.send(Message::Pong(payload)).await {
                    ended = Some(OkxSessionEnd::WriteError(error.to_string()));
                    break;
                }
                continue;
            }
            if !message.is_text() {
                continue;
            }
            // The text moves out of the message: no copy of the frame.
            let raw_text = message.into_text()?;
            if raw_text == "pong" {
                continue;
            }
            let routing = OkxRouting::parse(&raw_text)?;
            if let Some(code) = routing.notice_code() {
                // Keep publishing from this connection until the successor is
                // subscribed; OKX keeps it open for 60 seconds after the notice.
                // A repeated notice retries a standby that failed earlier.
                if standby.is_none() {
                    let next_generation = connection_generation(&generation_path).await?;
                    budget.permit(now_ns()?)?;
                    standby = Some(Box::pin(establish(next_generation)));
                }
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_native_session_notice",
                        "runtime": "OKX",
                        "lane": lane,
                        "generation": generation,
                        "code": code,
                    }))?
                );
                notice = Some(code);
                continue;
            }
            if routing.has_event() {
                return Err("unexpected OKX control event after subscription".into());
            }
            let binding = routing.data_binding(&binding_map)?;
            drop(routing);
            if let Some(detector) = staleness.as_mut() {
                if let Some(venue_ms) = okx_venue_ts_ms(&raw_text) {
                    let observed = detector.observe_at(read_at, venue_ms, &binding);
                    telemetry.venue_lag(observed.lag_ms, observed.stale, detector.baseline_ms());
                    if let Some(trigger) = observed.trigger {
                        let host_ms = detector.host_ms(read_at);
                        let decision = if standby.is_some() {
                            None
                        } else {
                            Some(detector.decide(host_ms))
                        };
                        let action = match decision {
                            None => "SUPPRESSED_STANDBY_PENDING",
                            Some(RecycleDecision::Cooldown { .. }) => "SUPPRESSED_COOLDOWN",
                            Some(RecycleDecision::Cap { .. }) => "SUPPRESSED_CAP",
                            // The cooldown/cap slot is taken only once the
                            // control budget admits the connection attempt.
                            Some(RecycleDecision::Recycle) => match budget.permit(now_ns()?) {
                                Ok(()) => {
                                    detector.recycle_started(host_ms);
                                    "RECYCLE"
                                }
                                Err(_) => "SUPPRESSED_CONTROL_BUDGET",
                            },
                        };
                        telemetry.staleness_action(action);
                        let mut successor_generation = None;
                        if action == "RECYCLE" {
                            let next_generation = connection_generation(&generation_path).await?;
                            standby = Some(Box::pin(establish(next_generation)));
                            successor_generation = Some(next_generation);
                            recycle = Some(PendingRecycle {
                                trigger: trigger.clone(),
                                started: Instant::now(),
                                successor_generation: next_generation,
                            });
                        }
                        let suppressed = if action == "RECYCLE" {
                            Some(0)
                        } else {
                            detector.suppressed_log_due(host_ms)
                        };
                        if let Some(suppressed_since_last_line) = suppressed {
                            eprintln!(
                                "{}",
                                serde_json::to_string(&json!({
                                    "event": "qdl_native_staleness_triggered",
                                    "runtime": "OKX",
                                    "lane": lane,
                                    "generation": generation,
                                    "session": session_id,
                                    "action": action,
                                    "successor_generation": successor_generation,
                                    "decision": match decision {
                                        Some(RecycleDecision::Cooldown { remaining_ms }) => {
                                            json!({"cooldown_remaining_ms": remaining_ms})
                                        }
                                        Some(RecycleDecision::Cap { recycles_last_hour }) => {
                                            json!({"recycles_last_hour": recycles_last_hour})
                                        }
                                        _ => Value::Null,
                                    },
                                    "suppressed_since_last_line": suppressed_since_last_line,
                                    "trigger": trigger.report(),
                                }))?
                            );
                        }
                    }
                }
            }
            let frame =
                okx_data_frame(binding, &session_id, generation, raw_text, last_frame_at_ns);
            telemetry.frame(frame.received_at_ns);
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
                    telemetry: &telemetry,
                },
            )
            .await?
            {
                break;
            }
        }
        // A notice whose standby never became ready is still the cause.
        if let (Some(code), Some(end)) = (notice.as_ref(), ended.as_ref()) {
            if !end.handed_over() {
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_native_notice_without_handover",
                        "runtime": "OKX",
                        "lane": lane,
                        "generation": generation,
                        "code": code,
                        "cause": end.cause(),
                    }))?
                );
                if !matches!(end, OkxSessionEnd::BookResync) {
                    ended = Some(OkxSessionEnd::Notice { code: code.clone() });
                }
            }
        }
        // An unfinished standby is dropped here; its generation number stays
        // consumed (generations only need to increase).
        drop(standby);
        let detected_at_ns = now_ns()?;
        let handed_over = ended.as_ref().is_some_and(OkxSessionEnd::handed_over);
        if ended.is_some() && !handed_over {
            // Not covered by a successor: mark this session down at once,
            // before the drain below, so readers stop trusting it.
            liveness
                .disconnected(&session_id, generation, detected_at_ns)
                .await?;
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
                &telemetry,
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
        let stopping = should_stop(&stopped, &accepted, config.max_events, expires);
        // A handed-over session stays LIVE until its successor's record
        // replaces it, unless that successor will not be used after all.
        if !handed_over || stopping || publish_error.is_some() {
            liveness
                .disconnected(&session_id, generation, now_ns()?)
                .await?;
        }
        if let Some(error) = publish_error {
            return Err(error.into());
        }
        let Some(end) = ended else {
            // Stopped, or the event budget is spent.
            continue;
        };
        if stopping {
            continue;
        }
        failures = failures.saturating_add(1);
        let drained_at_ns = now_ns()?;
        eprintln!(
            "{}",
            serde_json::to_string(&json!({
                "event": "qdl_native_session_disconnected",
                "runtime": "OKX",
                "service": format!("{:?}", service).to_ascii_uppercase(),
                "lane": lane,
                "attempt": failures,
                "generation": generation,
                "session": session_id,
                "cause": end.cause(),
                "detail": end.detail(),
                "last_frame_to_detected_ms": (detected_at_ns - last_frame_at_ns) / 1_000_000,
                "drain_ms": (drained_at_ns - detected_at_ns) / 1_000_000,
            }))?
        );
        previous_end = Some((
            session_id.clone(),
            generation,
            last_frame_at_ns,
            detected_at_ns,
        ));
        if handed_over {
            // The successor is live already; nothing to wait for.
            failures = 0;
            continue;
        }
        let delay = okx_reconnect_delay_ms(backoff, failures);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    }
    Ok(())
}

async fn run_okx(
    config: Arc<IngestorConfig>,
    telemetry: Arc<TelemetryRegistry>,
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
    let pacer = OkxConnectPacer::default();
    let public_lanes = partition_okx_bindings(&public, config.max_subscriptions_per_connection);
    let business_lanes = partition_okx_bindings(&business, config.max_subscriptions_per_connection);
    let lane_count = public_lanes.iter().filter(|lane| !lane.is_empty()).count()
        + business_lanes
            .iter()
            .filter(|lane| !lane.is_empty())
            .count();
    let publisher = Arc::new(RawPublisher::new(&config, "okx-raw", lane_count)?);
    let mut futures = Vec::new();
    for (index, bindings) in public_lanes.into_iter().enumerate() {
        futures.push(run_okx_service(
            OkxServiceShard {
                service: OkxService::Public,
                shard_index: index + 1,
                url: config.websocket_url.clone(),
                bindings,
                establish_budget: OKX_ESTABLISH_BUDGET,
            },
            config.clone(),
            Arc::clone(&publisher),
            Arc::clone(&telemetry),
            accepted.clone(),
            coalesced_latest.clone(),
            stopped.clone(),
            pacer.clone(),
        ));
    }
    for (index, bindings) in business_lanes.into_iter().enumerate() {
        futures.push(run_okx_service(
            OkxServiceShard {
                service: OkxService::Business,
                shard_index: index + 1,
                url: business_url.clone(),
                bindings,
                establish_budget: OKX_ESTABLISH_BUDGET,
            },
            config.clone(),
            Arc::clone(&publisher),
            Arc::clone(&telemetry),
            accepted.clone(),
            coalesced_latest.clone(),
            stopped.clone(),
            pacer.clone(),
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
            "latest_state_staleness": config.latest_state_staleness,
            "production_public_writes": 0,
            "production_legacy_writes": 0,
        }))?
    );
    let telemetry = Arc::new(TelemetryRegistry::default());
    tokio::spawn(report_progress(
        Arc::clone(&telemetry),
        match config.runtime {
            ProviderRuntime::Binance => "BINANCE",
            ProviderRuntime::Okx => "OKX",
        },
        Arc::clone(&accepted),
        Arc::clone(&coalesced_latest),
    ));
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
                    Arc::clone(&telemetry),
                    accepted.clone(),
                    coalesced_latest.clone(),
                    stopped.clone(),
                )
                .await
            }
            ProviderRuntime::Okx => {
                run_okx(
                    config.clone(),
                    Arc::clone(&telemetry),
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
    use super::BindingMap;
    use super::{
        authority_mode_name, book_delivery_remaining_ns, book_session_bootstrap,
        book_snapshot_renewal_period, feed_lane_name, next_connection_generation,
        partition_binance_bindings, partition_binance_route, partition_bindings,
        partition_okx_bindings, pending_binance_frame, pending_okx_frame, AuthorityMode,
        BinanceRoute, BookLivenessProof, BookSessionBootstrap, DeliveryClass, KafkaTransportError,
        LatestStateBuffer, PendingRawFrame, ProviderRuntime, RawBinding, RawFeed,
        SessionLivenessWriter, TransportProtocol,
    };
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::Instant;

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
                RawFeed::MarkIndex => "btcusdt@markPrice@1s",
            }
            .into(),
            subscription_id: "test-source".into(),
            adapter_version: "test/2.0.0".into(),
            instrument_catalog_revision: 1,
            feed,
            delivery_class,
            l2: None,
            mark_index_target: None,
        }
    }

    fn proof_bindings() -> BindingMap {
        let book = binding(RawFeed::Book, DeliveryClass::Lossless);
        let mut other = book.clone();
        other.native_symbol = "ETHUSDT".into();
        other.native_channel = "ethusdt@depth@100ms".into();
        let trade = binding(RawFeed::Trade, DeliveryClass::Lossless);
        [book, other, trade]
            .into_iter()
            .map(|binding| (binding.native_channel.clone(), Arc::new(binding)))
            .collect()
    }

    #[test]
    fn book_liveness_marker_needs_a_quiet_book_and_fresh_in_band_evidence() {
        const MS: i64 = 1_000_000;
        let bindings = proof_bindings();
        let start = 1_000_000 * MS;
        let mut proof = BookLivenessProof::new(Duration::from_millis(1_000), &bindings, start);

        // Nothing is quiet for a full interval yet.
        let (markers, ping) = proof.due(&bindings, "lane-1-1", 1, start + 400 * MS);
        assert!(markers.is_empty());
        assert!(!ping);

        // BTC keeps trading; ETH stays quiet while frames keep arriving on the connection.
        proof.frame("btcusdt@depth@100ms", start + 900 * MS);
        proof.transport(start + 1_200 * MS);
        let (markers, ping) = proof.due(&bindings, "lane-1-1", 1, start + 1_300 * MS);
        assert!(!ping);
        assert_eq!(markers.len(), 1, "only the quiet book gets a marker");
        let marker = &markers[0];
        assert_eq!(marker.binding.native_channel, "ethusdt@depth@100ms");
        assert_eq!(
            marker.received_at_ns,
            start + 1_200 * MS,
            "stamped with the read, not the tick"
        );
        assert_eq!(marker.transport_protocol, TransportProtocol::Websocket);
        assert_eq!(
            (marker.session_id.as_str(), marker.generation),
            ("lane-1-1", 1)
        );
        let body: Value = serde_json::from_slice(&marker.raw_frame).unwrap();
        assert_eq!(
            body,
            json!({"qdl_transport_liveness": "v1", "stream": "ethusdt@depth@100ms"})
        );

        // No new evidence: no repeated marker; the trade binding never gets one.
        let (markers, _) = proof.due(&bindings, "lane-1-1", 1, start + 1_400 * MS);
        assert!(markers.is_empty());

        // An idle connection asks for a ping, and stale evidence emits nothing.
        let (markers, ping) = proof.due(&bindings, "lane-1-1", 1, start + 1_800 * MS);
        assert!(markers.is_empty());
        assert!(ping);
        let (markers, ping) = proof.due(&bindings, "lane-1-1", 1, start + 2_300 * MS);
        assert!(
            markers.is_empty(),
            "evidence older than one interval proves nothing"
        );
        assert!(ping);

        // The pong is new evidence: both books are now quiet for an interval.
        proof.transport(start + 2_350 * MS);
        let (markers, _) = proof.due(&bindings, "lane-1-1", 1, start + 2_400 * MS);
        let mut channels = markers
            .iter()
            .map(|marker| marker.binding.native_channel.as_str())
            .collect::<Vec<_>>();
        channels.sort();
        assert_eq!(channels, ["btcusdt@depth@100ms", "ethusdt@depth@100ms"]);
        assert!(markers
            .iter()
            .all(|marker| marker.received_at_ns == start + 2_350 * MS));
    }

    #[test]
    fn book_delivery_bound_is_exact_and_never_applies_to_non_book_frames() {
        let mut book = binding(RawFeed::Book, DeliveryClass::Lossless);
        book.l2 = Some(super::RawL2Config {
            provider_protocol: "BINANCE_DIFF_DEPTH".into(),
            depth_per_side: 100,
            rest_snapshot_url: Some("https://example.test/depth".into()),
            snapshot_refresh_seconds: Some(30),
        });
        assert_eq!(
            book_delivery_remaining_ns(&book, 1_000, 29_000_001_000).unwrap(),
            Some(1_000_000_000)
        );
        assert!(matches!(
            book_delivery_remaining_ns(&book, 1_000, 30_000_001_001),
            Err(KafkaTransportError::SnapshotTimeout(_))
        ));

        let trade = binding(RawFeed::Trade, DeliveryClass::Lossless);
        assert_eq!(
            book_delivery_remaining_ns(&trade, 1, i64::MAX).unwrap(),
            None
        );
    }

    #[test]
    fn startup_authority_name_matches_fenced_mode() {
        assert_eq!(
            authority_mode_name(&AuthorityMode::RustShadow),
            "RUST_SHADOW"
        );
        assert_eq!(
            authority_mode_name(&AuthorityMode::RustCanary),
            "RUST_CANARY"
        );
        assert_eq!(
            authority_mode_name(&AuthorityMode::RustPrimary),
            "RUST_PRIMARY"
        );
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
    fn binance_market_group_carries_mark_price_and_klines() {
        // Binance serves `@markPrice` and `@kline_*` from `/market`. On an
        // unrouted connection both are acknowledged and then silent, which is
        // the defect this split exists to make impossible.
        let values = vec![
            binding(RawFeed::Book, DeliveryClass::Lossless),
            binding(RawFeed::Bar, DeliveryClass::Lossless),
            binding(RawFeed::Trade, DeliveryClass::Lossless),
            binding(RawFeed::Quote, DeliveryClass::LatestState),
            binding(RawFeed::MarkIndex, DeliveryClass::LatestState),
        ];
        let market = partition_binance_route(&values, BinanceRoute::Market, 100).unwrap();
        let feeds: Vec<RawFeed> = market.iter().map(|lane| lane.feed).collect();
        assert_eq!(feeds, vec![RawFeed::Bar, RawFeed::MarkIndex]);
    }

    #[test]
    fn binance_public_group_carries_book_trade_and_quote() {
        let values = vec![
            binding(RawFeed::Book, DeliveryClass::Lossless),
            binding(RawFeed::Bar, DeliveryClass::Lossless),
            binding(RawFeed::Trade, DeliveryClass::Lossless),
            binding(RawFeed::Quote, DeliveryClass::LatestState),
            binding(RawFeed::MarkIndex, DeliveryClass::LatestState),
        ];
        let public = partition_binance_route(&values, BinanceRoute::Public, 100).unwrap();
        let feeds: Vec<RawFeed> = public.iter().map(|lane| lane.feed).collect();
        assert_eq!(feeds, vec![RawFeed::Book, RawFeed::Trade, RawFeed::Quote]);
    }

    #[test]
    fn every_binance_binding_lands_in_exactly_one_routed_group() {
        let values = vec![
            binding(RawFeed::Book, DeliveryClass::Lossless),
            binding(RawFeed::Bar, DeliveryClass::Lossless),
            binding(RawFeed::Trade, DeliveryClass::Lossless),
            binding(RawFeed::Quote, DeliveryClass::LatestState),
            binding(RawFeed::MarkIndex, DeliveryClass::LatestState),
        ];
        let public = partition_binance_route(&values, BinanceRoute::Public, 100).unwrap();
        let market = partition_binance_route(&values, BinanceRoute::Market, 100).unwrap();
        let routed = public.iter().map(|lane| lane.bindings.len()).sum::<usize>()
            + market.iter().map(|lane| lane.bindings.len()).sum::<usize>();
        assert_eq!(routed, values.len());
        // Feed lanes survive the split: one socket per feed class inside a group.
        assert_eq!(public.len(), 3);
        assert_eq!(market.len(), 2);
    }

    #[test]
    fn a_binding_with_no_routed_base_fails_the_whole_role() {
        // Dropping it into whichever group is iterated first is how a channel
        // ends up subscribed on a socket that will never deliver it.
        let mut stray = binding(RawFeed::Trade, DeliveryClass::Lossless);
        stray.native_channel = "btcusdt@forceOrder".into();
        let values = vec![stray];
        assert!(partition_binance_route(&values, BinanceRoute::Public, 100).is_err());
        assert!(partition_binance_route(&values, BinanceRoute::Market, 100).is_err());
    }

    #[test]
    fn a_lane_name_does_not_move_when_another_feed_joins_its_route() {
        // A lane whose identity changes must re-bootstrap its book, so a lane
        // name may never depend on what else happens to share its route.
        // Numbering by position inside a route looked equivalent and is not:
        // Phase 3 adds a BAR lane to `/market`, and with a positional name that
        // would have renamed MARK_INDEX's lane and silently restarted a feed
        // that nothing had asked to change.
        let lane = |bindings: &[RawBinding], route: BinanceRoute| -> Vec<String> {
            partition_binance_route(bindings, route, 100)
                .unwrap()
                .iter()
                .map(|lane| {
                    format!(
                        "binance-{}-{}-{:03}",
                        route.segment(),
                        feed_lane_name(lane.feed),
                        lane.shard_index
                    )
                })
                .collect()
        };

        let before = vec![binding(RawFeed::MarkIndex, DeliveryClass::LatestState)];
        let after = vec![
            binding(RawFeed::Bar, DeliveryClass::Lossless),
            binding(RawFeed::MarkIndex, DeliveryClass::LatestState),
        ];
        assert_eq!(
            lane(&before, BinanceRoute::Market),
            ["binance-market-markindex-001"]
        );
        assert_eq!(
            lane(&after, BinanceRoute::Market),
            ["binance-market-bar-001", "binance-market-markindex-001"],
            "adding BAR must add a lane, never rename one"
        );

        // The same holds for the public route when a feed is removed.
        let public_full = vec![
            binding(RawFeed::Book, DeliveryClass::Lossless),
            binding(RawFeed::Trade, DeliveryClass::Lossless),
            binding(RawFeed::Quote, DeliveryClass::LatestState),
        ];
        let public_without_trade = vec![
            binding(RawFeed::Book, DeliveryClass::Lossless),
            binding(RawFeed::Quote, DeliveryClass::LatestState),
        ];
        assert_eq!(
            lane(&public_full, BinanceRoute::Public),
            [
                "binance-public-book-001",
                "binance-public-trade-001",
                "binance-public-quote-001"
            ]
        );
        assert_eq!(
            lane(&public_without_trade, BinanceRoute::Public),
            ["binance-public-book-001", "binance-public-quote-001"],
            "removing TRADE must not rename the book or quote lane"
        );
    }

    #[test]
    fn every_feed_class_has_a_distinct_lane_name() {
        let names: std::collections::HashSet<&'static str> = [
            RawFeed::Book,
            RawFeed::Bar,
            RawFeed::Trade,
            RawFeed::Quote,
            RawFeed::MarkIndex,
        ]
        .into_iter()
        .map(feed_lane_name)
        .collect();
        assert_eq!(
            names.len(),
            5,
            "two feeds sharing a lane name share a counter"
        );
    }

    #[test]
    fn routed_groups_cannot_share_a_lane_identity() {
        // Two lanes with the same name would share a transactional producer, a
        // session-liveness file and a connection-generation counter. A shared
        // generation counter across lanes is what quarantined every OKX candle
        // on 2026-09-17.
        assert_ne!(
            BinanceRoute::Public.segment(),
            BinanceRoute::Market.segment()
        );
        assert_eq!(
            format!("binance-{}-001", BinanceRoute::Public.segment()),
            "binance-public-001"
        );
        assert_eq!(
            format!("binance-{}-001", BinanceRoute::Market.segment()),
            "binance-market-001"
        );
    }

    #[test]
    fn okx_book_lane_uses_initial_snapshot_and_gap_resync_without_timer_renewal() {
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
            .map(|item| (item.key(), Arc::new(item)))
            .collect::<HashMap<_, _>>();
        let mixed = values
            .into_iter()
            .map(|item| (item.key(), Arc::new(item)))
            .collect::<HashMap<_, _>>();
        assert_eq!(
            book_session_bootstrap(ProviderRuntime::Okx, &books),
            Some(BookSessionBootstrap::InitialSnapshotAndGapResync)
        );
        assert_eq!(book_session_bootstrap(ProviderRuntime::Okx, &mixed), None);

        let mut binance_book = binding(RawFeed::Book, DeliveryClass::Lossless);
        binance_book.l2 = Some(super::RawL2Config {
            provider_protocol: "BINANCE_DIFF_DEPTH".into(),
            depth_per_side: 100,
            rest_snapshot_url: Some("https://fapi.binance.com/fapi/v1/depth".into()),
            snapshot_refresh_seconds: Some(30),
        });
        let binance_books = [(binance_book.key(), Arc::new(binance_book))]
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(
            book_session_bootstrap(ProviderRuntime::Binance, &binance_books),
            Some(BookSessionBootstrap::PeriodicRestAnchor(
                Duration::from_secs(30)
            ))
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
    fn mark_index_is_a_shared_latest_state_lane_with_one_logical_target() {
        let mut binance = binding(RawFeed::MarkIndex, DeliveryClass::LatestState);
        binance.mark_index_target = Some("uid-BINANCE-USDM-SOLUSDT".into());
        assert!(binance.validate(ProviderRuntime::Binance).is_ok());

        let mut okx_mark = binance.clone();
        okx_mark.provider = "OKX_DIRECT".into();
        okx_mark.venue = "OKX".into();
        okx_mark.market = "SWAP".into();
        okx_mark.native_symbol = "SOL-USDT-SWAP".into();
        okx_mark.native_channel = "mark-price".into();
        okx_mark.mark_index_target = Some("uid-OKX-SWAP-SOL-USDT-SWAP".into());
        let mut okx_index = okx_mark.clone();
        okx_index.native_symbol = "SOL-USDT".into();
        okx_index.native_channel = "index-tickers".into();
        assert!(okx_mark.validate(ProviderRuntime::Okx).is_ok());
        assert!(okx_index.validate(ProviderRuntime::Okx).is_ok());

        let lanes = partition_okx_bindings(&[okx_mark.clone(), okx_index.clone()], 100);
        assert_eq!(lanes.len(), 1);
        assert!(lanes[0].iter().all(|item| item.feed == RawFeed::MarkIndex));
        assert_eq!(okx_mark.mark_index_target, okx_index.mark_index_target);
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
            binding: Arc::new(binding(RawFeed::Quote, DeliveryClass::LatestState)),
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
        let bindings: BindingMap =
            HashMap::from([(binding.native_channel.clone(), Arc::new(binding))]);
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
        let bindings: BindingMap =
            HashMap::from([(binding.native_channel.clone(), Arc::new(binding))]);
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
        let bindings: BindingMap = HashMap::from([(binding.key(), Arc::new(binding))]);
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

    async fn wait_for_liveness(path: &Path, transport_at_ns: i64) -> serde_json::Value {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(bytes) = fs::read(path) {
                let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                if value["last_transport_at_ns"] == transport_at_ns {
                    return value;
                }
            }
            assert!(Instant::now() < deadline, "liveness write did not land");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_liveness_writer_is_atomic_bounded_and_disconnects_explicitly() {
        let directory = generation_path("session-liveness");
        let path = directory.join("binance-usdm").join("binance-000.json");
        let mut writer = SessionLivenessWriter {
            path: path.clone(),
            config_revision: 7,
            write_interval_ns: 1_000_000_000,
            last_written_ns: None,
            pending: None,
        };

        // LIVE is written in the background; the call returns at once.
        writer.live("session-1", 3, 1_000_000_000).unwrap();
        let first = wait_for_liveness(&path, 1_000_000_000).await;
        assert_eq!(first["schema"], "qdl.provider-session-liveness.v1");
        assert_eq!(first["source_session_id"], "session-1");
        assert_eq!(first["connection_generation"], 3);
        assert_eq!(first["state"], "LIVE");
        assert_eq!(first["config_revision"], 7);

        // Throttled inside the write interval: no new write is started.
        writer.live("session-1", 3, 1_500_000_000).unwrap();
        assert!(writer.pending.is_some());
        let bounded: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(bounded["last_transport_at_ns"], 1_000_000_000_i64);

        // Invalid identity is refused synchronously, as before.
        assert!(writer.live("", 3, 2_000_000_000).is_err());

        // DISCONNECTED waits for any LIVE write, then lands before returning.
        writer.live("session-1", 3, 2_000_000_000).unwrap();
        writer
            .disconnected("session-1", 3, 2_000_000_001)
            .await
            .unwrap();
        let disconnected: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(disconnected["state"], "DISCONNECTED");
        assert_eq!(disconnected["last_transport_at_ns"], 2_000_000_001_i64);
        assert!(writer.pending.is_none());
        assert!(directory.join("binance-usdm").is_dir());
        assert_eq!(
            fs::read_dir(directory.join("binance-usdm"))
                .unwrap()
                .count(),
            1
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_background_liveness_write_fails_the_lane_on_the_next_call() {
        let directory = generation_path("session-liveness-failure");
        fs::create_dir_all(&directory).unwrap();
        // The parent "directory" is a regular file: create_dir_all fails.
        let blocker = directory.join("not-a-directory");
        fs::write(&blocker, b"x").unwrap();
        let mut writer = SessionLivenessWriter {
            path: blocker.join("lane.json"),
            config_revision: 1,
            write_interval_ns: 1,
            last_written_ns: None,
            pending: None,
        };
        writer.live("session-1", 1, 10).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !writer.pending.as_ref().unwrap().is_finished() {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(writer.live("session-1", 1, 20).is_err());
        assert!(writer.disconnected("session-1", 1, 30).await.is_err());
        fs::remove_dir_all(directory).unwrap();
    }
}

#[cfg(test)]
mod resync_tests {
    use super::*;

    fn fixture(okx: bool) -> (IngestorConfig, BindingMap, RawProviderEnvelope) {
        let b = RawBinding {
            provider: if okx { "OKX_DIRECT" } else { "BINANCE_DIRECT" }.into(),
            venue: if okx { "OKX" } else { "BINANCE" }.into(),
            market: if okx { "SWAP" } else { "USDM" }.into(),
            product_type: "PERPETUAL".into(),
            native_symbol: if okx { "BNB-USDT-SWAP" } else { "BNBUSDT" }.into(),
            native_channel: if okx { "books" } else { "bnbusdt@depth@100ms" }.into(),
            subscription_id: "book-test".into(),
            adapter_version: "2.0.0".into(),
            instrument_catalog_revision: 3,
            feed: RawFeed::Book,
            delivery_class: DeliveryClass::Lossless,
            l2: Some(RawL2Config {
                provider_protocol: if okx {
                    "OKX_PUBLIC_BOOKS"
                } else {
                    "BINANCE_DIFF_DEPTH"
                }
                .into(),
                depth_per_side: 100,
                rest_snapshot_url: None,
                snapshot_refresh_seconds: Some(30),
            }),
            mark_index_target: None,
        };
        let raw = RawProviderEnvelope {
            provider: b.provider.clone(),
            venue: b.venue.clone(),
            market: b.market.clone(),
            product_type: b.product_type.clone(),
            native_symbol: b.native_symbol.clone(),
            native_channel: b.native_channel.clone(),
            subscription_id: b.subscription_id.clone(),
            adapter_version: b.adapter_version.clone(),
            instrument_catalog_revision: 3,
            source_session_id: "test-book-lane-7".into(),
            connection_generation: 7,
            config_revision: 9,
            lease_epoch: 1,
            authority_revision: 2,
            partition_plan_epoch: 1,
            received_at_ns: 1_000_000_000_000,
            raw_frame_bytes: b"{}".to_vec(),
            raw_frame_sha256: Sha256::digest(b"{}").to_vec(),
            capture_id: vec![1; 16],
            ..Default::default()
        };
        let config = IngestorConfig {
            runtime: if okx {
                ProviderRuntime::Okx
            } else {
                ProviderRuntime::Binance
            },
            websocket_url: "wss://unit.test".into(),
            business_websocket_url: None,
            market_websocket_url: None,
            raw_stream: "unit.raw".into(),
            shard_id: "unit".into(),
            lease_epoch: 1,
            partition_plan_epoch: 1,
            config_revision: 9,
            heartbeat_seconds: 10,
            max_events: 0,
            max_runtime_seconds: 0,
            metrics_every_events: 100,
            generation_state_path: "/tmp/unit".into(),
            session_liveness_dir: "/tmp/unit-session".into(),
            session_liveness_write_interval_ms: 1000,
            max_inflight_publishes: 16,
            max_subscriptions_per_connection: 100,
            latest_state_flush_ms: 50,
            book_liveness_proof_ms: 0,
            liveness_ping_ms: 0,
            liveness_dead_ms: 0,
            latest_state_staleness: LatestStateStalenessConfig::default(),
            authority: AuthorityRecord {
                schema: "qdl.authority-record.v1".into(),
                slice_id: "unit".into(),
                revision: 2,
                mode: AuthorityMode::RustShadow,
                candidate_image_digest: format!("sha256:{}", "0".repeat(64)),
                capability_manifest_digest: "0".repeat(64),
                contract_digest: "0".repeat(64),
                partition_plan_digest: "0".repeat(64),
                public_write_allowed: false,
                legacy_write_allowed: false,
                approved_by: "unit-test".into(),
                effective_at_ns: 1,
            },
            bindings: vec![b.clone()],
            book_resync: None,
        };
        (config, HashMap::from([(b.key(), Arc::new(b))]), raw)
    }

    type GoldenCase = (
        RawBinding,
        &'static str,
        u64,
        Vec<u8>,
        i64,
        TransportProtocol,
    );

    /// Inputs of the raw-envelope golden: every frame source and both venues.
    fn golden_cases() -> Vec<GoldenCase> {
        let (_, binance, _) = fixture(false);
        let (_, okx, _) = fixture(true);
        let binance = binance.values().next().unwrap().as_ref().clone();
        let okx = okx.values().next().unwrap().as_ref().clone();
        let mut trade = binance.clone();
        trade.native_channel = "bnbusdt@trade".into();
        trade.feed = RawFeed::Trade;
        trade.l2 = None;
        let mut mark = okx.clone();
        mark.native_channel = "mark-price".into();
        mark.feed = RawFeed::MarkIndex;
        mark.l2 = None;
        mark.delivery_class = DeliveryClass::LatestState;
        mark.mark_index_target = Some("BNB-USDT".into());
        vec![
            (trade, "binance-USDM-public-trade-000-7-1", 7, br#"{"e":"trade","E":1,"s":"BNBUSDT","t":9,"p":"600.1","q":"0.01","T":1,"m":true}"#.to_vec(), 1_791_000_000_000_000_001, TransportProtocol::Websocket),
            (binance.clone(), "binance-USDM-public-book-001-7-1", 7, br#"{"e":"depthUpdate","E":1,"T":1,"s":"BNBUSDT","U":10,"u":12,"pu":9,"b":[["600.1","1.5"]],"a":[]}"#.to_vec(), 1_791_000_000_000_000_002, TransportProtocol::Websocket),
            (binance, "binance-USDM-public-book-001-7-1", 7, br#"{"lastUpdateId":12,"bids":[["600.1","1.5"]],"asks":[["600.2","2"]]}"#.to_vec(), 1_791_000_000_000_000_003, TransportProtocol::Http),
            (okx.clone(), "okx-public-001-3-1", 3, br#"{"arg":{"channel":"books","instId":"BNB-USDT-SWAP"},"action":"update","data":[{"asks":[],"bids":[["600.1","3","0","1"]],"ts":"1","checksum":0,"seqId":5,"prevSeqId":4}]}"#.to_vec(), 1_791_000_000_000_000_004, TransportProtocol::Websocket),
            (mark, "okx-public-002-3-1", 3, br#"{"arg":{"channel":"mark-price","instId":"BNB-USDT-SWAP"},"data":[{"markPx":"600","ts":"1"}]}"#.to_vec(), 1_791_000_000_000_000_005, TransportProtocol::Websocket),
            (okx, "okx-public-001-3-1", 3, Vec::new(), 1_791_000_000_000_000_006, TransportProtocol::Websocket),
        ]
    }

    fn record_digest(record: &DurableRecord) -> String {
        let mut digest = Sha256::new();
        for part in [
            record.stream.as_bytes(),
            record.partition_key.as_bytes(),
            record.event_id.as_slice(),
            record.payload.as_slice(),
            &record.accepted_at_ns.to_be_bytes(),
        ] {
            digest.update((part.len() as u64).to_be_bytes());
            digest.update(part);
        }
        hex::encode(digest.finalize())
    }

    /// Codex A6 golden: the raw envelope records (stream, key, event id,
    /// payload bytes, accepted time) are byte-identical to daa8841's
    /// `RawPublisher::record`, which borrowed and copied the frame.
    const RECORD_EXPECTED: &[&str] = &[
        "3743ce454441775881d02d5f578dd3be4304df9458f87cb80b661caa50d7f573",
        "4acd68c5a63ae29b3db3899a1a0b312bf1746a21c631be7bc241d16cd1d9880b",
        "b701a8b53e4fb8233ea1310b92d75308e54ba3630dc51eb93583786b803f0458",
        "6f83914f8202399d8d99b533edff0dc6af3fde48b30fca5a9e1b33ada33d2c26",
        "8eef8f405bb6c63609cdef84a77ac8f53b47f69f64f5b2fdbbd66405c3fac532",
        "03511b2d13df74de92f06c386997d3a37e584501a3d8694930f6cf03dc6d009e",
    ];

    #[test]
    fn raw_envelope_records_are_byte_identical_after_moving_the_frame() {
        let (config, _, _) = fixture(false);
        let context = RawEnvelopeContext::new(&config);
        let actual = golden_cases()
            .into_iter()
            .map(
                |(binding, session, generation, frame, received_at_ns, protocol)| {
                    record_digest(&context.record(PendingRawFrame {
                        binding: Arc::new(binding),
                        session_id: session.into(),
                        generation,
                        raw_frame: frame,
                        received_at_ns,
                        transport_protocol: protocol,
                    }))
                },
            )
            .collect::<Vec<_>>();
        if std::env::var_os("QDL_PRINT_RECORD_GOLDEN").is_some() {
            for digest in &actual {
                println!("RECORDGOLDEN \"{digest}\",");
            }
        }
        assert_eq!(actual, RECORD_EXPECTED);
    }

    #[test]
    fn capture_id_and_raw_frame_sha_have_different_preimages() {
        let (config, bindings, _) = fixture(false);
        let context = RawEnvelopeContext::new(&config);
        let frame = br#"{"e":"depthUpdate"}"#.to_vec();
        let record = context.record(PendingRawFrame {
            binding: bindings.values().next().unwrap().clone(),
            session_id: "s".into(),
            generation: 1,
            raw_frame: frame.clone(),
            received_at_ns: 5,
            transport_protocol: TransportProtocol::Websocket,
        });
        let raw = RawProviderEnvelope::decode(record.payload.as_slice()).unwrap();
        assert_eq!(raw.raw_frame_bytes, frame);
        assert_eq!(raw.raw_frame_sha256, Sha256::digest(&frame).to_vec());
        assert_eq!(raw.capture_id, capture_id("s", 1, 5, &frame));
        assert_ne!(raw.capture_id.as_slice(), &raw.raw_frame_sha256[..16]);
        assert_eq!(record.event_id, raw.capture_id);
        assert_eq!(raw.correlation_id, hex::encode(&raw.capture_id));
    }

    /// The pre-change OKX data path, verbatim: a `Value` parse, the notice and
    /// control checks, then `pending_okx_frame`'s own `Value` parse.
    fn okx_route_oracle(raw_text: &str, bindings: &BindingMap) -> String {
        if raw_text == "pong" {
            return "pong".into();
        }
        let payload: Value = match serde_json::from_str(raw_text) {
            Ok(payload) => payload,
            Err(error) => return format!("parse:{error}"),
        };
        if let Some(code) = okx_notice_code(&payload) {
            return format!("notice:{code}");
        }
        if payload.get("event").is_some() {
            return "control".into();
        }
        let payload: Value = serde_json::from_str(raw_text).unwrap();
        let Some(argument) = payload.get("arg").and_then(Value::as_object) else {
            return "error:OKX data arg is missing".into();
        };
        let Some(channel) = argument.get("channel").and_then(Value::as_str) else {
            return "error:OKX data channel is missing".into();
        };
        let Some(instrument) = argument.get("instId").and_then(Value::as_str) else {
            return "error:OKX data instrument is missing".into();
        };
        match bindings.get(&format!("{channel}|{instrument}")) {
            Some(binding) => format!("data:{}", binding.key()),
            None => "error:OKX frame has no approved binding".into(),
        }
    }

    fn okx_route_new(raw_text: &str, bindings: &BindingMap) -> String {
        if raw_text == "pong" {
            return "pong".into();
        }
        let routing = match OkxRouting::parse(raw_text) {
            Ok(routing) => routing,
            Err(error) => return format!("parse:{error}"),
        };
        if let Some(code) = routing.notice_code() {
            return format!("notice:{code}");
        }
        if routing.has_event() {
            return "control".into();
        }
        match routing.data_binding(bindings) {
            Ok(binding) => format!("data:{}", binding.key()),
            Err(error) => format!("error:{error}"),
        }
    }

    #[test]
    fn okx_routing_parse_matches_the_two_dom_parses_exactly() {
        let (_, bindings, _) = fixture(true);
        let levels = (0..400)
            .map(|index| format!("[\"{}.1\",\"{index}\",\"0\",\"1\"]", 600 + index))
            .collect::<Vec<_>>()
            .join(",");
        let mut deep = String::new();
        for _ in 0..140 {
            deep.push('[');
        }
        for _ in 0..140 {
            deep.push(']');
        }
        let corpus = vec![
            format!(r#"{{"arg":{{"channel":"books","instId":"BNB-USDT-SWAP"}},"action":"update","data":[{{"asks":[{levels}],"bids":[{levels}],"seqId":5}}]}}"#),
            r#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"data":[]}"#.to_owned(),
            r#"{"arg":{"channel":"books"},"data":[]}"#.to_owned(),
            r#"{"arg":{"instId":"BNB-USDT-SWAP"},"data":[]}"#.to_owned(),
            r#"{"arg":{"channel":7,"instId":"BNB-USDT-SWAP"}}"#.to_owned(),
            r#"{"arg":["books"],"data":[]}"#.to_owned(),
            r#"{"arg":null}"#.to_owned(),
            r#"{"data":[]}"#.to_owned(),
            r#"{"arg":{"channel":"trades","instId":"X"},"arg":{"channel":"books","instId":"BNB-USDT-SWAP"}}"#.to_owned(),
            r#"{"arg":{"channel":"books","channel":"trades","instId":"BNB-USDT-SWAP"}}"#.to_owned(),
            r#"{"arg":{"channel":"books","instId":"BNB-USDT-SWAP"}}"#.to_owned(),
            r#"{"event":"notice","code":"64008","msg":"upgrade"}"#.to_owned(),
            r#"{"event":"notice","msg":"upgrade"}"#.to_owned(),
            r#"{"event":"notice","code":64008}"#.to_owned(),
            r#"{"event":"subscribe","arg":{"channel":"books","instId":"BNB-USDT-SWAP"}}"#.to_owned(),
            r#"{"event":null,"arg":{"channel":"books","instId":"BNB-USDT-SWAP"}}"#.to_owned(),
            r#"{"event":"error","code":"60012","msg":"bad"}"#.to_owned(),
            "pong".to_owned(),
            "ping".to_owned(),
            String::new(),
            "[]".to_owned(),
            "\"text\"".to_owned(),
            r#"{"arg":{"channel":"books","instId":"BNB-USDT-SWAP"}"#.to_owned(),
            r#"{"arg":{"channel":"books","instId":"BNB-USDT-SWAP"}} x"#.to_owned(),
            r#"{"arg":{"channel":"books","instId":"BNB-USDT-SWAP"},"x":1e999}"#.to_owned(),
            r#"{"arg":{"channel":"books","instId":"BNB-USDT-SWAP"},"x":"\uD800"}"#.to_owned(),
            r#"{"arg":{"channel":"books","instId":"BNB-USDT-SWAP"},"\uD800":1}"#.to_owned(),
            format!(r#"{{"arg":{{"channel":"books","instId":"BNB-USDT-SWAP"}},"x":{deep}}}"#),
        ];
        for frame in &corpus {
            assert_eq!(
                okx_route_new(frame, &bindings),
                okx_route_oracle(frame, &bindings),
                "frame: {frame}"
            );
        }
        assert!(okx_route_new(&corpus[0], &bindings).starts_with("data:"));
    }

    #[test]
    fn lane_telemetry_reports_and_resets_each_interval() {
        let registry = TelemetryRegistry::default();
        let lane = registry.lane("binance-public-book-001");
        assert!(Arc::ptr_eq(
            &lane,
            &registry.lane("binance-public-book-001")
        ));
        lane.session(7);
        let start = Instant::now();
        lane.read(start, 1_000);
        lane.read(start + Duration::from_millis(40), 2_000);
        lane.frame(1_000_000_000);
        lane.frame(1_500_000_000);
        lane.enqueued(Duration::from_millis(1), 100, 3);
        lane.acknowledged(Duration::from_millis(12));
        lane.coalesced();
        let line = registry.progress_line("BINANCE", Duration::from_secs(10), 2_000_000_000, 9, 1);
        assert_eq!(line["event"], "qdl_native_raw_ingestor_progress");
        assert_eq!(line["accepted_raw_frames"], 9);
        let report = &line["lanes"][0];
        assert_eq!(report["lane"], "binance-public-book-001");
        assert_eq!(report["generation"], 7);
        assert_eq!(report["frames"], 2);
        assert_eq!(report["frames_per_s"], 0.2);
        assert_eq!(report["bytes"], 100);
        assert_eq!(report["inflight"], 2);
        assert_eq!(report["inflight_max"], 3);
        assert_eq!(report["coalesced"], 1);
        assert_eq!(report["last_frame_age_ms"], 500.0);
        assert_eq!(report["read_gap_ms"]["max"], 40.0);
        assert_eq!(report["read_gap_ms"]["max_at_ns"], 2_000);
        assert_eq!(report["produce_ms"]["n"], 1);
        assert_eq!(report["produce_ms"]["max"], 12.0);
        let next = registry.progress_line("BINANCE", Duration::from_secs(10), 3_000_000_000, 9, 1);
        let report = &next["lanes"][0];
        assert_eq!(report["frames"], 0);
        assert_eq!(report["frames_total"], 2);
        assert!(report["produce_ms"].is_null());
        assert_eq!(report["last_frame_age_ms"], 1500.0);
    }

    fn request(raw: RawProviderEnvelope) -> DurableRecord {
        let mut evidence = Sha256::new();
        evidence.update(raw.encode_to_vec());
        evidence.update((QuarantineReason::SequenceGap as i32).to_be_bytes());
        let mut id = Sha256::new();
        id.update(&raw.capture_id);
        id.update(L2_RESNAPSHOT_REQUIRED_V1.as_bytes());
        let record = QuarantineRecord {
            quarantined_at_ns: raw.received_at_ns,
            raw: Some(raw),
            reason: QuarantineReason::SequenceGap as i32,
            safe_summary: L2_RESNAPSHOT_REQUIRED_V1.into(),
            retry_count: 0,
            evidence_sha256: evidence.finalize().to_vec(),
        };
        DurableRecord {
            stream: "unit.quarantine".into(),
            partition_key: "unit".into(),
            event_id: id.finalize().to_vec(),
            payload: record.encode_to_vec(),
            accepted_at_ns: record.quarantined_at_ns,
        }
    }

    #[test]
    fn book_resync_accepts_current_lane_and_rejects_superseded_replay() {
        for okx in [false, true] {
            let (config, bindings, raw) = fixture(okx);
            let record = request(raw.clone());
            assert!(matches_book_resync(
                &record,
                &config,
                &bindings,
                &raw.source_session_id,
                7,
                raw.received_at_ns + 1
            ));
            assert!(!matches_book_resync(
                &record,
                &config,
                &bindings,
                "next-session",
                8,
                raw.received_at_ns + 1
            ));
            assert!(!matches_book_resync(
                &record,
                &config,
                &bindings,
                &raw.source_session_id,
                8,
                raw.received_at_ns + 1
            ));
            assert!(!matches_book_resync(
                &record,
                &config,
                &bindings,
                &raw.source_session_id,
                7,
                raw.received_at_ns - 1
            ));
            assert!(!matches_book_resync(
                &record,
                &config,
                &bindings,
                &raw.source_session_id,
                7,
                raw.received_at_ns + 30_000_000_001
            ));
        }
    }

    #[test]
    fn book_resync_rejects_wrong_identity_fence_and_tampering() {
        for okx in [false, true] {
            let (config, bindings, raw) = fixture(okx);
            for field in [
                "provider",
                "venue",
                "market",
                "product",
                "symbol",
                "channel",
                "subscription",
                "adapter",
                "catalog",
                "config",
                "authority",
                "lease",
                "partition",
                "test",
            ] {
                let mut changed = raw.clone();
                match field {
                    "provider" => changed.provider.push('x'),
                    "venue" => changed.venue.push('x'),
                    "market" => changed.market.push('x'),
                    "product" => changed.product_type.push('x'),
                    "symbol" => changed.native_symbol.push('x'),
                    "channel" => changed.native_channel.push('x'),
                    "subscription" => changed.subscription_id.push('x'),
                    "adapter" => changed.adapter_version.push('x'),
                    "catalog" => changed.instrument_catalog_revision += 1,
                    "config" => changed.config_revision += 1,
                    "authority" => changed.authority_revision += 1,
                    "lease" => changed.lease_epoch += 1,
                    "partition" => changed.partition_plan_epoch += 1,
                    "test" => changed.test_provenance = true,
                    _ => unreachable!(),
                }
                assert!(
                    !matches_book_resync(
                        &request(changed),
                        &config,
                        &bindings,
                        &raw.source_session_id,
                        7,
                        raw.received_at_ns + 1
                    ),
                    "{field}"
                );
            }
            let mut record = request(raw.clone());
            record.event_id[0] ^= 1;
            assert!(!matches_book_resync(
                &record,
                &config,
                &bindings,
                &raw.source_session_id,
                7,
                raw.received_at_ns + 1
            ));
            let mut record = request(raw.clone());
            let mut decoded = QuarantineRecord::decode(record.payload.as_slice()).unwrap();
            decoded.evidence_sha256[0] ^= 1;
            record.payload = decoded.encode_to_vec();
            assert!(!matches_book_resync(
                &record,
                &config,
                &bindings,
                &raw.source_session_id,
                7,
                raw.received_at_ns + 1
            ));
            let mut nonbook = bindings.clone();
            for binding in nonbook.values_mut() {
                Arc::make_mut(binding).feed = RawFeed::Trade;
            }
            assert!(!matches_book_resync(
                &request(raw.clone()),
                &config,
                &nonbook,
                &raw.source_session_id,
                7,
                raw.received_at_ns + 1
            ));
        }
    }

    #[test]
    fn book_resync_late_bootstrap_hint_does_not_reconnect_fresh_anchor() {
        let (_, _, raw) = fixture(false);
        let record = request(raw.clone());
        let mut anchors = HashMap::new();
        assert!(!snapshot_satisfies_resync(&record, &anchors));
        anchors.insert(
            raw.subscription_id.clone(),
            (
                raw.source_session_id.clone(),
                raw.connection_generation,
                raw.received_at_ns + 1,
            ),
        );
        assert!(snapshot_satisfies_resync(&record, &anchors));
        let mut current = raw.clone();
        current.received_at_ns += 2;
        assert!(!snapshot_satisfies_resync(&request(current), &anchors));
        let mut other = raw.clone();
        other.connection_generation += 1;
        assert!(!snapshot_satisfies_resync(&request(other), &anchors));
        let mut other = raw.clone();
        other.source_session_id.push('x');
        assert!(!snapshot_satisfies_resync(&request(other), &anchors));
        let mut other = raw;
        other.subscription_id.push('x');
        assert!(!snapshot_satisfies_resync(&request(other), &anchors));
    }

    #[tokio::test(start_paused = true)]
    async fn book_resync_checkpoint_flood_is_coalesced() {
        let mut next = Instant::now();
        assert!(claim_hint_checkpoint(&mut next, Instant::now()));
        for _ in 0..40_000 {
            assert!(!claim_hint_checkpoint(&mut next, Instant::now()));
        }
        tokio::time::advance(Duration::from_millis(999)).await;
        assert!(!claim_hint_checkpoint(&mut next, Instant::now()));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(claim_hint_checkpoint(&mut next, Instant::now()));
        assert!(!claim_hint_checkpoint(&mut next, Instant::now()));
    }

    #[tokio::test(start_paused = true)]
    async fn book_resync_disabled_has_no_poll_or_connection_side_effect() {
        let (config, bindings, raw) = fixture(true);
        let mut listener = None;
        assert!(tokio::time::timeout(
            Duration::from_millis(1),
            book_resync_requested(&mut listener, &config, &bindings, &raw.source_session_id, 7)
        )
        .await
        .is_err());
    }
}

#[cfg(test)]
mod okx_reconnect_tests {
    use super::BindingMap;
    use super::{
        okx_establish, okx_notice_code, okx_reconnect_delay_ms, BackoffPolicy, DeliveryClass,
        InboundDeadline, Keepalive, LivenessPolicy, OkxConnectPacer, OkxEstablishError,
        OkxSessionEnd, OkxSubscription, RawBinding, RawFeed, SessionLineageWriter,
        OKX_CONNECT_SPACING, OKX_ESTABLISH_BUDGET,
    };
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::time::Instant;
    use tokio_tungstenite::tungstenite::Message;

    fn okx_trade_binding() -> RawBinding {
        RawBinding {
            provider: "OKX_DIRECT".into(),
            venue: "OKX".into(),
            market: "SWAP".into(),
            product_type: "PERPETUAL".into(),
            native_symbol: "BTC-USDT-SWAP".into(),
            native_channel: "trades".into(),
            subscription_id: "test-source".into(),
            adapter_version: "test/2.0.0".into(),
            instrument_catalog_revision: 1,
            feed: RawFeed::Trade,
            delivery_class: DeliveryClass::Lossless,
            l2: None,
            mark_index_target: None,
        }
    }

    fn backoff() -> BackoffPolicy {
        BackoffPolicy {
            initial_ms: 250,
            maximum_ms: 30_000,
            multiplier: 2,
            jitter_bps: 2_000,
        }
        .validate()
        .unwrap()
    }

    fn scratch_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "qdl-okx-lineage-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn a_session_that_delivered_reconnects_without_backoff() {
        // Production evidence 2026-10-10: every OKX reconnect took ~630 ms
        // from the disconnect log to the new session id, 400 ms of it this
        // fixed sleep (delay_ms(1, 1) of the 250 ms policy, jitter basis 1).
        assert_eq!(backoff().delay_ms(1, 1), 400);
        assert_eq!(okx_reconnect_delay_ms(backoff(), 1), 0);
        // A session that ended again before delivering anything still backs off.
        assert!(okx_reconnect_delay_ms(backoff(), 2) >= 800);
        assert!((24_000..=30_000).contains(&okx_reconnect_delay_ms(backoff(), 30)));
    }

    fn heartbeat_15s() -> LivenessPolicy {
        LivenessPolicy::heartbeat(Duration::from_secs(15))
    }

    /// The values the stable renderer emits: ping after 1.5 s of silence,
    /// dead 4 s after the last message read.
    fn fast() -> LivenessPolicy {
        LivenessPolicy::from_config(15, 1_500, 4_000).unwrap()
    }

    #[test]
    fn inbound_deadline_pings_once_then_declares_the_connection_dead() {
        let idle = Duration::from_secs(15);
        let start = Instant::now();
        let mut deadline = InboundDeadline::new(heartbeat_15s(), start);
        assert_eq!(deadline.deadline(), start + idle);
        assert_eq!(deadline.expired(start + idle), Keepalive::SendPing);
        // Sending the ping opens exactly one pong window; nothing renews it.
        assert_eq!(deadline.deadline(), start + idle * 2);
        assert_eq!(deadline.expired(start + idle * 2), Keepalive::Dead);
        assert_eq!(deadline.deadline(), start + idle * 2);
        // Only a message read from the socket moves it.
        let later = start + Duration::from_secs(20);
        deadline.inbound(later);
        assert_eq!(deadline.deadline(), later + idle);
        assert_eq!(deadline.expired(later + idle), Keepalive::SendPing);
    }

    #[test]
    fn sub_second_policy_pings_at_ping_after_and_is_dead_at_dead_after() {
        let start = Instant::now();
        let mut deadline = InboundDeadline::new(fast(), start);
        assert_eq!(deadline.deadline(), start + Duration::from_millis(1_500));
        assert_eq!(
            deadline.expired(start + Duration::from_millis(1_500)),
            Keepalive::SendPing
        );
        assert_eq!(deadline.deadline(), start + Duration::from_millis(4_000));
        assert_eq!(
            deadline.expired(start + Duration::from_millis(4_000)),
            Keepalive::Dead
        );
    }

    #[test]
    fn a_late_ping_still_gets_its_full_pong_window() {
        // The loop was held 5 s (a REST snapshot fetch inside a select
        // branch) and only then saw the expired ping deadline. Declaring the
        // socket dead at last_inbound + 4 s would be immediate, before the
        // venue could answer; the pong gets dead_after - ping_after = 2.5 s.
        let start = Instant::now();
        let mut deadline = InboundDeadline::new(fast(), start);
        let late = start + Duration::from_secs(5);
        assert_eq!(deadline.expired(late), Keepalive::SendPing);
        assert_eq!(deadline.deadline(), late + Duration::from_millis(2_500));
        // The same holds for the heartbeat rule.
        let mut deadline = InboundDeadline::new(heartbeat_15s(), start);
        let late = start + Duration::from_secs(25);
        assert_eq!(deadline.expired(late), Keepalive::SendPing);
        assert_eq!(deadline.deadline(), late + Duration::from_secs(15));
    }

    #[test]
    fn liveness_fields_are_bounded_and_absent_keeps_the_heartbeat_rule() {
        // Absent (serde default 0/0): today's behaviour, 15 s ping, 30 s dead.
        assert_eq!(LivenessPolicy::from_config(15, 0, 0), Some(heartbeat_15s()));
        assert_eq!(
            LivenessPolicy::from_config(15, 1_500, 4_000),
            Some(LivenessPolicy {
                ping_after: Duration::from_millis(1_500),
                dead_after: Duration::from_millis(4_000),
            })
        );
        // Edges that are accepted.
        assert!(LivenessPolicy::from_config(15, 500, 1_500).is_some());
        assert!(LivenessPolicy::from_config(15, 28_000, 29_000).is_some());
        for (ping, dead) in [
            (1_500, 0),      // only one field set
            (0, 4_000),      // only one field set
            (499, 4_000),    // ping faster than one per 500 ms of silence
            (1_500, 2_499),  // pong window under 1 s
            (4_000, 1_500),  // dead before the ping
            (4_000, 4_000),  // no pong window at all
            (1_500, 29_001), // reaches OKX's 30 s idle close
            (u64::MAX, 4_000),
        ] {
            assert_eq!(
                LivenessPolicy::from_config(15, ping, dead),
                None,
                "ping {ping} dead {dead}"
            );
        }
    }

    /// A venue socket that stays open and sends nothing: no frames, no pongs
    /// (it never reads, so tungstenite never answers our pings). With
    /// `answer_pings`, it reads and auto-answers every ping instead.
    async fn silent_venue(answer_pings: bool) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            if answer_pings {
                while socket.next().await.is_some() {}
            } else {
                std::future::pending::<()>().await;
            }
            drop(socket);
        });
        format!("ws://{address}/ws")
    }

    /// A venue that pushes a data frame every `every` and never reads (so it
    /// never answers a ping either).
    async fn busy_venue(every: Duration) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let mut tick = tokio::time::interval(every);
            loop {
                tick.tick().await;
                if socket.send(Message::Text("{}".into())).await.is_err() {
                    break;
                }
            }
        });
        format!("ws://{address}/ws")
    }

    #[derive(Debug)]
    struct Detection {
        /// When the connection was declared dead, or None if still alive.
        dead_after: Option<Duration>,
        /// Pings sent by the liveness deadline (not by the proof tick).
        liveness_pings: u32,
    }

    /// The production read-loop shape: a book-liveness proof tick (flag on)
    /// competes with the socket read in one select and `continue`s, sending a
    /// proof ping like `BookLivenessProof::due` asks for. Returns when the
    /// connection is declared dead, or with `dead_after` None if it is still
    /// considered alive at `give_up`.
    async fn detect_dead(
        url: String,
        policy: LivenessPolicy,
        proof_tick: Option<Duration>,
        absolute_deadline: bool,
        give_up: Duration,
    ) -> Detection {
        let (mut socket, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let start = Instant::now();
        let mut inbound = InboundDeadline::new(policy, start);
        let mut liveness_pings = 0_u32;
        let mut tick = proof_tick.map(tokio::time::interval);
        if let Some(tick) = tick.as_mut() {
            tick.tick().await;
        }
        while start.elapsed() < give_up {
            let read = if absolute_deadline {
                tokio::time::timeout_at(inbound.deadline(), socket.next())
            } else {
                // The pre-R4 shape: a fresh relative timeout every iteration.
                tokio::time::timeout_at(Instant::now() + policy.ping_after, socket.next())
            };
            tokio::pin!(read);
            tokio::select! {
                _ = async {
                    match tick.as_mut() {
                        Some(tick) => { tick.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                }, if tick.is_some() => {
                    socket.send(Message::Ping(Vec::new())).await.unwrap();
                    continue;
                }
                result = &mut read => match result {
                    Ok(Some(Ok(_))) => inbound.inbound(Instant::now()),
                    Ok(_) => panic!("the fake venue never closes"),
                    Err(_) => match inbound.expired(Instant::now()) {
                        Keepalive::SendPing => {
                            liveness_pings += 1;
                            socket.send(Message::Ping(Vec::new())).await.unwrap();
                        }
                        Keepalive::Dead => {
                            return Detection {
                                dead_after: Some(start.elapsed()),
                                liveness_pings,
                            }
                        }
                    },
                },
            }
        }
        Detection {
            dead_after: None,
            liveness_pings,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_socket_is_declared_dead_within_two_idle_periods_with_proof_on_and_off() {
        let bound = Duration::from_secs(30);
        for proof_tick in [None, Some(Duration::from_millis(250))] {
            let detected = detect_dead(
                silent_venue(false).await,
                heartbeat_15s(),
                proof_tick,
                true,
                Duration::from_secs(600),
            )
            .await
            .dead_after
            .unwrap_or_else(|| panic!("never detected, proof tick {proof_tick:?}"));
            assert!(
                detected >= Duration::from_secs(30) && detected <= bound + Duration::from_millis(1),
                "proof tick {proof_tick:?}: detected after {detected:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_socket_is_declared_dead_in_under_five_seconds_with_the_fast_policy() {
        for proof_tick in [None, Some(Duration::from_millis(250))] {
            let detection = detect_dead(
                silent_venue(false).await,
                fast(),
                proof_tick,
                true,
                Duration::from_secs(60),
            )
            .await;
            let detected = detection
                .dead_after
                .unwrap_or_else(|| panic!("never detected, proof tick {proof_tick:?}"));
            assert!(
                detected >= Duration::from_millis(4_000)
                    && detected <= Duration::from_millis(4_001)
                    && detected < Duration::from_secs(5),
                "proof tick {proof_tick:?}: detected after {detected:?}"
            );
            assert_eq!(detection.liveness_pings, 1, "proof tick {proof_tick:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_pre_r4_relative_timeout_never_detects_a_silent_socket_while_the_proof_ticks() {
        // Reproduces Astra R4 on the old shape: 10 idle periods, still "alive".
        assert_eq!(
            detect_dead(
                silent_venue(false).await,
                heartbeat_15s(),
                Some(Duration::from_millis(250)),
                false,
                Duration::from_secs(150),
            )
            .await
            .dead_after,
            None
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_socket_that_answers_pings_stays_alive_with_proof_on_and_off() {
        for policy in [heartbeat_15s(), fast()] {
            for proof_tick in [None, Some(Duration::from_millis(250))] {
                let give_up = if policy == fast() {
                    Duration::from_secs(60)
                } else {
                    Duration::from_secs(120)
                };
                let detection =
                    detect_dead(silent_venue(true).await, policy, proof_tick, true, give_up).await;
                assert_eq!(
                    detection.dead_after, None,
                    "policy {policy:?} proof tick {proof_tick:?}"
                );
                // At most one liveness ping per ping_after of silence: with the
                // fast policy that is <= 0.67/s, far under Binance's 10
                // incoming messages per second per connection.
                let ceiling = (give_up.as_millis() / policy.ping_after.as_millis()) as u32 + 1;
                assert!(
                    detection.liveness_pings <= ceiling,
                    "policy {policy:?} proof tick {proof_tick:?}: {} pings",
                    detection.liveness_pings
                );
                if proof_tick.is_none() && policy == fast() {
                    // A quiet-but-answering socket is pinged about every 1.5 s.
                    assert!(detection.liveness_pings >= 30, "{detection:?}");
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_socket_never_sends_a_liveness_ping() {
        let detection = detect_dead(
            busy_venue(Duration::from_millis(200)).await,
            fast(),
            None,
            true,
            Duration::from_secs(60),
        )
        .await;
        assert_eq!(detection.dead_after, None);
        assert_eq!(detection.liveness_pings, 0);
    }

    #[test]
    fn only_the_okx_notice_event_is_a_service_upgrade_notice() {
        let notice = json!({
            "event": "notice",
            "code": "64008",
            "msg": "The connection will soon be closed for a service upgrade. Please reconnect.",
            "connId": "a4d3ae55",
        });
        assert_eq!(okx_notice_code(&notice).as_deref(), Some("64008"));
        assert_eq!(
            okx_notice_code(&json!({"event": "notice"})).as_deref(),
            Some("missing")
        );
        assert_eq!(
            okx_notice_code(&json!({"event": "error", "code": "60012"})),
            None
        );
        assert_eq!(
            okx_notice_code(&json!({"arg": {"channel": "trades"}, "data": []})),
            None
        );
    }

    #[test]
    fn only_a_notice_handover_counts_as_covered() {
        let covered = OkxSessionEnd::NoticeHandover {
            code: "64008".into(),
        };
        assert!(covered.handed_over());
        assert_eq!(covered.cause(), "NOTICE_HANDOVER");
        for end in [
            OkxSessionEnd::Notice {
                code: "64008".into(),
            },
            OkxSessionEnd::Closed {
                code: Some(1001),
                reason: "going away".into(),
            },
            OkxSessionEnd::Eof,
            OkxSessionEnd::ReadError("reset".into()),
            OkxSessionEnd::WriteError("broken pipe".into()),
            OkxSessionEnd::HeartbeatTimeout,
            OkxSessionEnd::BookResync,
        ] {
            assert!(!end.handed_over(), "{end:?}");
        }
        assert_eq!(
            OkxSessionEnd::Closed {
                code: Some(1001),
                reason: "going away".into()
            }
            .detail(),
            json!({"code": 1001, "reason": "going away"})
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lineage_chain_grows_only_on_overlap_and_resets_after_a_gap() {
        let dir = scratch_dir("chain");
        let mut writer = SessionLineageWriter::at(&dir, "okx-public-002", 20).unwrap();
        let path = dir.join("lineage").join("okx-public-002.lineage");
        let read = || -> Value { serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap() };
        let chain = |value: &Value| -> Vec<(String, u64)> {
            value["chain"]
                .as_array()
                .unwrap()
                .iter()
                .map(|link| {
                    (
                        link["source_session_id"].as_str().unwrap().to_owned(),
                        link["connection_generation"].as_u64().unwrap(),
                    )
                })
                .collect()
        };

        writer
            .begin("okx-public-002-167-1", 167, false)
            .await
            .unwrap();
        writer
            .begin("okx-public-002-168-2", 168, true)
            .await
            .unwrap();
        let value = read();
        assert_eq!(value["schema"], "qdl.provider-session-lineage.v1");
        assert_eq!(value["lane"], "okx-public-002");
        assert_eq!(value["config_revision"], 20);
        assert_eq!(
            chain(&value),
            vec![
                ("okx-public-002-167-1".into(), 167),
                ("okx-public-002-168-2".into(), 168)
            ]
        );

        // A reconnect after a gap must not vouch for any earlier session.
        writer
            .begin("okx-public-002-169-3", 169, false)
            .await
            .unwrap();
        assert_eq!(chain(&read()), vec![("okx-public-002-169-3".into(), 169)]);

        for generation in 170..250_u64 {
            writer
                .begin(&format!("okx-public-002-{generation}-9"), generation, true)
                .await
                .unwrap();
        }
        let links = chain(&read());
        assert_eq!(links.len(), 32);
        assert_eq!(links.last().unwrap().1, 249);
        // The healthcheck matches '*.json' recursively; the lineage file must
        // never be mistaken for a session record.
        assert!(std::fs::read_dir(dir.join("lineage"))
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".json")));
        assert!(SessionLineageWriter::at(&dir, "OKX_PUBLIC", 20).is_err());
        assert!(writer.begin("", 1, true).await.is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn connect_pacer_keeps_okx_connection_requests_under_three_per_second() {
        let pacer = OkxConnectPacer::default();
        let start = Instant::now();
        for _ in 0..4 {
            pacer.admit().await;
        }
        // Four admissions need three full spacings: fewer than 3 per second.
        assert_eq!(start.elapsed(), OKX_CONNECT_SPACING * 3);
        assert!(OKX_CONNECT_SPACING * 3 > Duration::from_secs(1));
    }

    /// One fake OKX endpoint: sends `script` before (optionally) ACKing.
    async fn fake_okx(script: Vec<Value>, ack: bool) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let command = socket.next().await.unwrap().unwrap();
            let command: Value = serde_json::from_str(command.to_text().unwrap()).unwrap();
            assert_eq!(command["op"], "subscribe");
            for message in script {
                socket
                    .send(Message::Text(message.to_string()))
                    .await
                    .unwrap();
            }
            if ack {
                for arg in command["args"].as_array().unwrap() {
                    socket
                        .send(Message::Text(
                            json!({"event": "subscribe", "arg": arg, "id": command["id"]})
                                .to_string(),
                        ))
                        .await
                        .unwrap();
                }
                // Hold the connection open, like a live venue.
                while socket.next().await.is_some() {}
            } else {
                let _ = socket.close(None).await;
            }
        });
        format!("ws://{address}/ws/v5/public")
    }

    fn subscriptions() -> (Arc<Vec<OkxSubscription>>, Arc<BindingMap>) {
        let binding = okx_trade_binding();
        (
            Arc::new(vec![OkxSubscription {
                channel: binding.native_channel.clone(),
                inst_id: binding.native_symbol.clone(),
            }]),
            Arc::new(HashMap::from([(binding.key(), Arc::new(binding))])),
        )
    }

    #[tokio::test]
    async fn establish_buffers_pre_ack_frames_under_the_new_generation_and_reports_a_notice() {
        let trade = json!({
            "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
            "data": [{"instId": "BTC-USDT-SWAP", "tradeId": "42", "px": "1", "sz": "1", "side": "buy", "ts": "1"}],
        });
        let notice = json!({"event": "notice", "code": "64008", "msg": "upgrade"});
        let url = fake_okx(vec![trade, notice], true).await;
        let (subscriptions, bindings) = subscriptions();
        let established = okx_establish(
            url,
            subscriptions,
            bindings,
            "okx-public-002".into(),
            168,
            16,
            OkxConnectPacer::default(),
            OKX_ESTABLISH_BUDGET,
        )
        .await
        .unwrap();
        assert_eq!(established.generation, 168);
        assert!(established.session_id.starts_with("okx-public-002-168-"));
        assert!(established.ready_at_ns >= established.connect_started_ns);
        assert_eq!(established.notice_code.as_deref(), Some("64008"));
        // Nothing is published here: the caller publishes these only after the
        // previous generation stopped, so the generation fences see N before N+1.
        assert_eq!(established.pre_ack_frames.len(), 1);
        let frame = &established.pre_ack_frames[0];
        assert_eq!(frame.generation, 168);
        assert_eq!(frame.session_id, established.session_id);
    }

    #[tokio::test]
    async fn establish_failures_are_typed_so_a_standby_failure_spares_the_live_session() {
        let url = fake_okx(vec![], false).await;
        let (subscriptions, bindings) = subscriptions();
        let error = okx_establish(
            url,
            subscriptions.clone(),
            bindings.clone(),
            "okx-public-002".into(),
            169,
            16,
            OkxConnectPacer::default(),
            OKX_ESTABLISH_BUDGET,
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(error, OkxEstablishError::Subscribe(_)), "{error}");

        let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = unused.local_addr().unwrap();
        drop(unused);
        let error = okx_establish(
            format!("ws://{address}/ws/v5/public"),
            subscriptions,
            bindings,
            "okx-public-002".into(),
            170,
            16,
            OkxConnectPacer::default(),
            OKX_ESTABLISH_BUDGET,
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(error, OkxEstablishError::Connect(_)), "{error}");
    }
}

/// IDR-112: venue-timestamp staleness recycle of OKX latest-state QUOTE lanes.
#[cfg(test)]
mod staleness_tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::net::TcpListener;

    const T0: i64 = 1_792_000_000_000;
    const SYMBOLS: [&str; 3] = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "BTC-USD-SWAP"];

    fn quote(symbol: &str) -> RawBinding {
        RawBinding {
            provider: "OKX_DIRECT".into(),
            venue: "OKX".into(),
            market: "SWAP".into(),
            product_type: "PERPETUAL".into(),
            native_symbol: symbol.into(),
            native_channel: "bbo-tbt".into(),
            subscription_id: format!("quote-{symbol}"),
            adapter_version: "test/2.0.0".into(),
            instrument_catalog_revision: 1,
            feed: RawFeed::Quote,
            delivery_class: DeliveryClass::LatestState,
            l2: None,
            mark_index_target: None,
        }
    }

    fn quotes() -> Vec<RawBinding> {
        SYMBOLS.iter().map(|symbol| quote(symbol)).collect()
    }

    fn map(bindings: Vec<RawBinding>) -> BindingMap {
        bindings
            .into_iter()
            .map(|binding| (binding.key(), Arc::new(binding)))
            .collect()
    }

    fn detector(config: LatestStateStalenessConfig) -> StalenessDetector {
        StalenessDetector::new(config, SYMBOLS.len(), T0 * 1_000_000, Instant::now())
    }

    /// Healthy traffic like the parallel probe: one frame every 25 ms,
    /// rotating instruments, lag 29-35 ms. Returns the triggers seen.
    fn feed(
        detector: &mut StalenessDetector,
        bindings: &[RawBinding],
        from_ms: i64,
        to_ms: i64,
        lag: impl Fn(i64, usize) -> i64,
    ) -> Vec<(i64, StalenessTrigger)> {
        let mut triggers = Vec::new();
        let mut at = from_ms;
        let mut index = 0_usize;
        while at < to_ms {
            let host_ms = T0 + at;
            let binding = &bindings[index % bindings.len()];
            let observed = detector.observe(host_ms, host_ms - lag(at, index), binding);
            if let Some(trigger) = observed.trigger {
                triggers.push((at, trigger));
            }
            at += 25;
            index += 1;
        }
        triggers
    }

    fn healthy(_: i64, index: usize) -> i64 {
        29 + (index as i64 * 3) % 7
    }

    #[test]
    fn defaults_are_the_documented_conservative_values_and_validate() {
        let config = LatestStateStalenessConfig::default();
        assert!(config.enabled);
        assert_eq!(
            (
                config.excess_ms,
                config.window_ms,
                config.min_frames,
                config.min_bindings
            ),
            (250, 2_000, 5, 2)
        );
        assert_eq!(
            (
                config.baseline_bucket_ms,
                config.baseline_buckets,
                config.min_baseline_buckets
            ),
            (10_000, 60, 3)
        );
        assert_eq!(
            (config.cooldown_ms, config.max_recycles_per_hour),
            (60_000, 6)
        );
        config.validate().unwrap();
        // A single frame can never be a burst; sub-100 ms excess is jitter.
        for broken in [
            LatestStateStalenessConfig {
                min_frames: 1,
                ..config
            },
            LatestStateStalenessConfig {
                excess_ms: 50,
                ..config
            },
            LatestStateStalenessConfig {
                cooldown_ms: 1_000,
                ..config
            },
            LatestStateStalenessConfig {
                min_baseline_buckets: 61,
                ..config
            },
            LatestStateStalenessConfig {
                max_recycles_per_hour: 0,
                ..config
            },
        ] {
            assert!(broken.validate().is_err(), "{broken:?}");
        }
    }

    #[test]
    fn config_field_is_optional_partial_and_strict() {
        let parsed: LatestStateStalenessConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(parsed, LatestStateStalenessConfig::default());
        let off: LatestStateStalenessConfig =
            serde_json::from_value(json!({"enabled": false})).unwrap();
        assert!(!off.enabled);
        assert_eq!(off.excess_ms, 250);
        let tuned: LatestStateStalenessConfig =
            serde_json::from_value(json!({"excess_ms": 400, "cooldown_ms": 120000})).unwrap();
        assert_eq!(
            (tuned.excess_ms, tuned.cooldown_ms, tuned.min_frames),
            (400, 120_000, 5)
        );
        assert!(
            serde_json::from_value::<LatestStateStalenessConfig>(json!({"excess": 1})).is_err()
        );
        // A rendered production config without the field keeps working.
        let mut document = json!({
            "runtime": "OKX",
            "websocket_url": "wss://ws.okx.com/ws/v5/public",
            "business_websocket_url": "wss://ws.okx.com/ws/v5/business",
            "market_websocket_url": null,
            "raw_stream": "md.raw.v2",
            "shard_id": "qdl-v2-stable-okx-swap",
            "lease_epoch": 1,
            "partition_plan_epoch": 1,
            "config_revision": 20,
            "heartbeat_seconds": 15,
            "liveness_ping_ms": 1500,
            "liveness_dead_ms": 4000,
            "max_events": 0,
            "max_runtime_seconds": 0,
            "metrics_every_events": 1000,
            "generation_state_path": "/var/lib/qdl-stable/runtime/generations/okx-swap",
            "session_liveness_dir": "/var/lib/qdl-stable/runtime/session-liveness/okx-swap",
            "session_liveness_write_interval_ms": 1000,
            "max_inflight_publishes": 512,
            "max_subscriptions_per_connection": 100,
            "latest_state_flush_ms": 50,
            "authority": {
                "schema": "qdl.authority-record.v1",
                "slice_id": "unit",
                "revision": 2,
                "mode": "RUST_PRIMARY",
                "candidate_image_digest": format!("sha256:{}", "0".repeat(64)),
                "capability_manifest_digest": "0".repeat(64),
                "contract_digest": "0".repeat(64),
                "partition_plan_digest": "0".repeat(64),
                "public_write_allowed": false,
                "legacy_write_allowed": false,
                "approved_by": "unit-test",
                "effective_at_ns": 1
            },
            "bindings": [{
                "provider": "OKX_DIRECT", "venue": "OKX", "market": "SWAP",
                "product_type": "PERPETUAL", "native_symbol": "BTC-USDT-SWAP",
                "native_channel": "bbo-tbt", "subscription_id": "s",
                "adapter_version": "2.0.0", "instrument_catalog_revision": 1,
                "feed": "QUOTE", "delivery_class": "LATEST_STATE"
            }]
        });
        let config: IngestorConfig = serde_json::from_value(document.clone()).unwrap();
        assert_eq!(
            config.latest_state_staleness,
            LatestStateStalenessConfig::default()
        );
        config.validate().unwrap();
        document["latest_state_staleness"] = json!({"enabled": false});
        let config: IngestorConfig = serde_json::from_value(document.clone()).unwrap();
        assert!(!config.latest_state_staleness.enabled);
        config.validate().unwrap();
        document["latest_state_staleness"] = json!({"min_frames": 1});
        let config: IngestorConfig = serde_json::from_value(document).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn only_lanes_of_latest_state_quotes_qualify() {
        assert!(staleness_applies(&map(quotes())));
        assert!(!staleness_applies(&BindingMap::new()));
        let mut trade = quote("BTC-USDT-SWAP");
        trade.native_channel = "trades".into();
        trade.feed = RawFeed::Trade;
        trade.delivery_class = DeliveryClass::Lossless;
        assert!(!staleness_applies(&map(vec![trade.clone()])));
        assert!(!staleness_applies(&map(vec![
            quote("ETH-USDT-SWAP"),
            trade
        ])));
        let mut mark = quote("BTC-USDT-SWAP");
        mark.native_channel = "mark-price".into();
        mark.feed = RawFeed::MarkIndex;
        mark.mark_index_target = Some("x".into());
        assert!(!staleness_applies(&map(vec![mark])));
        let mut book = quote("BTC-USDT-SWAP");
        book.native_channel = "books".into();
        book.feed = RawFeed::Book;
        book.delivery_class = DeliveryClass::Lossless;
        assert!(!staleness_applies(&map(vec![book])));
    }

    #[test]
    fn venue_timestamp_is_the_newest_data_ts_or_none() {
        let frame = r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"},"data":[{"asks":[["1","2","0","1"]],"bids":[["1","2","0","1"]],"ts":"1792000000123","seqId":7}]}"#;
        assert_eq!(okx_venue_ts_ms(frame), Some(1_792_000_000_123));
        assert_eq!(
            okx_venue_ts_ms(r#"{"data":[{"ts":"5"},{"ts":"9"},{"ts":"x"}]}"#),
            Some(9)
        );
        for frame in [
            r#"{"data":[{"ts":5}]}"#,
            r#"{"data":[{"px":"1"}]}"#,
            r#"{"data":[{"ts":"0"}]}"#,
            r#"{"event":"subscribe","arg":{"channel":"bbo-tbt"}}"#,
            r#"[1,2]"#,
            "pong",
        ] {
            assert_eq!(okx_venue_ts_ms(frame), None, "{frame}");
        }
    }

    #[test]
    fn healthy_lane_with_single_and_paired_late_frames_never_triggers() {
        let mut detector = detector(LatestStateStalenessConfig::default());
        let bindings = quotes();
        // 10 minutes. Every 20 s one frame is 1.5 s late; every 60 s two
        // frames (two instruments) are 1.2 s late: below min_frames.
        let triggers = feed(&mut detector, &bindings, 0, 600_000, |at, index| {
            if at % 20_000 == 0 && at > 0 {
                1_500
            } else if at % 60_000 == 25 || at % 60_000 == 50 {
                1_200
            } else {
                healthy(at, index)
            }
        });
        assert!(triggers.is_empty(), "{triggers:?}");
        assert_eq!(detector.baseline_ms(), Some(29));
    }

    #[test]
    fn a_lane_wide_late_burst_triggers_once_per_min_frames_with_its_stats() {
        let mut detector = detector(LatestStateStalenessConfig::default());
        let bindings = quotes();
        assert!(feed(&mut detector, &bindings, 0, 40_000, healthy).is_empty());
        // The 14:08:17 signature: every instrument 0.65-0.7 s late together.
        let triggers = feed(&mut detector, &bindings, 40_000, 40_300, |at, _| {
            650 + (at % 50)
        });
        assert_eq!(triggers.len(), 2, "12 stale frames = two bursts of 5");
        let (at, trigger) = &triggers[0];
        assert_eq!(*at, 40_100, "the fifth stale frame triggers");
        assert_eq!(trigger.stale_frames, 5);
        assert_eq!(trigger.distinct_bindings, 3);
        assert_eq!(trigger.baseline_ms, 29);
        assert_eq!(trigger.span_ms, 100);
        assert!(trigger.max_lag_ms >= 650 && trigger.max_excess_ms >= 621);
    }

    #[test]
    fn a_skewed_clock_never_triggers_and_a_burst_on_it_still_does() {
        for skew_ms in [5_000_i64, -3_000] {
            let mut detector = detector(LatestStateStalenessConfig::default());
            let bindings = quotes();
            let triggers = feed(&mut detector, &bindings, 0, 300_000, |at, index| {
                skew_ms + healthy(at, index)
            });
            assert!(triggers.is_empty(), "skew {skew_ms}: {triggers:?}");
            assert_eq!(detector.baseline_ms(), Some(skew_ms + 29));
            let triggers = feed(&mut detector, &bindings, 300_000, 300_200, |_, _| {
                skew_ms + 900
            });
            assert_eq!(triggers.len(), 1, "skew {skew_ms}");
            assert_eq!(triggers[0].1.max_excess_ms, 871);
        }
    }

    #[test]
    fn a_burst_on_one_instrument_or_before_the_baseline_arms_does_not_trigger() {
        let config = LatestStateStalenessConfig::default();
        let bindings = quotes();
        let mut detector = detector(config);
        // 20 s < 3 completed 10 s buckets: not armed.
        let triggers = feed(&mut detector, &bindings, 0, 20_000, |at, index| {
            if at >= 19_000 {
                900
            } else {
                healthy(at, index)
            }
        });
        assert!(triggers.is_empty());
        assert_eq!(detector.baseline_ms(), None);
        let mut detector = StalenessDetector::new(config, 3, T0 * 1_000_000, Instant::now());
        assert!(feed(&mut detector, &bindings, 0, 40_000, healthy).is_empty());
        let one = [bindings[0].clone()];
        assert!(feed(&mut detector, &one, 40_000, 41_000, |_, _| 900).is_empty());
        // A one-binding lane only needs its one binding.
        let mut single = StalenessDetector::new(config, 1, T0 * 1_000_000, Instant::now());
        assert!(feed(&mut single, &one, 0, 40_000, healthy).is_empty());
        assert_eq!(feed(&mut single, &one, 40_000, 40_125, |_, _| 900).len(), 1);
    }

    #[test]
    fn the_baseline_resists_a_bogus_future_timestamp() {
        let mut detector = detector(LatestStateStalenessConfig::default());
        let bindings = quotes();
        // One frame per minute claims to be 10 s in the future.
        let triggers = feed(&mut detector, &bindings, 0, 300_000, |at, index| {
            if at % 60_000 == 0 {
                -10_000
            } else {
                healthy(at, index)
            }
        });
        assert!(triggers.is_empty(), "{triggers:?}");
        assert_eq!(detector.baseline_ms(), Some(29));
    }

    #[test]
    fn a_session_change_drops_the_previous_sessions_stale_frames() {
        let mut detector = detector(LatestStateStalenessConfig::default());
        let bindings = quotes();
        assert!(feed(&mut detector, &bindings, 0, 40_000, healthy).is_empty());
        assert!(feed(&mut detector, &bindings, 40_000, 40_100, |_, _| 900).is_empty());
        detector.session_started();
        assert!(feed(&mut detector, &bindings, 40_100, 40_200, |_, _| 900).is_empty());
        assert_eq!(
            feed(&mut detector, &bindings, 40_200, 40_225, |_, _| 900).len(),
            1
        );
    }

    #[test]
    fn recycles_respect_the_cooldown_and_the_hourly_cap() {
        let mut detector = detector(LatestStateStalenessConfig::default());
        assert_eq!(detector.decide(T0), RecycleDecision::Recycle);
        detector.recycle_started(T0);
        assert_eq!(
            detector.decide(T0 + 30_000),
            RecycleDecision::Cooldown {
                remaining_ms: 30_000
            }
        );
        for minute in 1..6 {
            assert_eq!(
                detector.decide(T0 + minute * 60_000),
                RecycleDecision::Recycle
            );
            detector.recycle_started(T0 + minute * 60_000);
        }
        assert_eq!(
            detector.decide(T0 + 6 * 60_000),
            RecycleDecision::Cap {
                recycles_last_hour: 6
            }
        );
        // The first recycle leaves the rolling hour.
        assert_eq!(detector.decide(T0 + 3_600_000), RecycleDecision::Recycle);
        // Suppressed lines: the first at once, then at most one per 10 s.
        assert_eq!(detector.suppressed_log_due(T0), Some(1));
        assert_eq!(detector.suppressed_log_due(T0 + 1_000), None);
        assert_eq!(detector.suppressed_log_due(T0 + 2_000), None);
        assert_eq!(detector.suppressed_log_due(T0 + 10_000), Some(3));
    }

    #[test]
    fn a_recycle_refused_after_the_decision_takes_no_cooldown_or_cap_slot() {
        // Review F3: `decide` is a pure check; a trigger refused further on
        // (OKX control budget) never called `recycle_started`, so the next
        // trigger may still recycle at once.
        let mut detector = detector(LatestStateStalenessConfig::default());
        for attempt in 0..10 {
            assert_eq!(
                detector.decide(T0 + attempt * 1_000),
                RecycleDecision::Recycle
            );
        }
        detector.recycle_started(T0 + 10_000);
        assert_eq!(
            detector.decide(T0 + 11_000),
            RecycleDecision::Cooldown {
                remaining_ms: 59_000
            }
        );
    }

    #[test]
    fn a_lane_already_delayed_at_startup_becomes_its_own_baseline() {
        // Documented trade-off (review F3): the detector is relative. A lane
        // that is 0.9 s late from its first frame arms on that lag and never
        // triggers; the absolute lag stays visible (`venue_lag_ms`) and the
        // downstream validator still refuses unusable data.
        let mut detector = detector(LatestStateStalenessConfig::default());
        let bindings = quotes();
        let triggers = feed(&mut detector, &bindings, 0, 120_000, |at, index| {
            900 + healthy(at, index)
        });
        assert!(triggers.is_empty(), "{triggers:?}");
        assert_eq!(detector.baseline_ms(), Some(929));
        // Recovery to a healthy lag is not staleness either.
        assert!(feed(&mut detector, &bindings, 120_000, 180_000, healthy).is_empty());
    }

    #[test]
    fn a_sustained_delay_on_one_instrument_never_triggers_with_two_min_bindings() {
        // Documented trade-off (review F3): with min_bindings 2 a delay
        // confined to one instrument is not a lane-wide burst, however long
        // it lasts; the other instruments keep the baseline healthy.
        let mut detector = detector(LatestStateStalenessConfig::default());
        let bindings = quotes();
        assert!(feed(&mut detector, &bindings, 0, 40_000, healthy).is_empty());
        let triggers = feed(&mut detector, &bindings, 40_000, 400_000, |at, index| {
            if index % bindings.len() == 0 {
                900
            } else {
                healthy(at, index)
            }
        });
        assert!(triggers.is_empty(), "{triggers:?}");
        assert_eq!(detector.baseline_ms(), Some(29));
    }

    // ---- The production session loop against a fake OKX venue ----------

    /// Delay (ms) the venue applies to `ts` for (connection, time since the
    /// venue started, sequence number).
    type Schedule = Arc<dyn Fn(usize, Duration, u64) -> i64 + Send + Sync>;

    #[derive(Debug, Default)]
    struct VenueLog {
        accepted: Vec<Option<Instant>>,
        acked: Vec<Option<Instant>>,
        closed: Vec<Option<Instant>>,
    }

    /// How the fake venue serves one connection.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum VenueMode {
        /// ACK every subscription, then stream quotes.
        Quotes,
        /// ACK all subscriptions but the last, never send data, and keep the
        /// socket busy with a WebSocket ping and a text `pong` every 100 ms
        /// (review F1: control traffic must not extend the establishment).
        AckAllButOne,
        /// ACK every subscription, never send data, answer text `ping` with
        /// `pong` (review F2: an ACK-only successor).
        AckOnly,
    }

    type Modes = Arc<dyn Fn(usize) -> VenueMode + Send + Sync>;

    fn all_quotes() -> Modes {
        Arc::new(|_| VenueMode::Quotes)
    }

    fn wall_ms() -> i64 {
        now_ns().unwrap() / 1_000_000
    }

    /// One OKX-like public endpoint: a 10 ms bbo-tbt update stream (rotating
    /// instruments, global seqId) fanned out to every subscribed connection,
    /// each stamping `ts = now - schedule(...)`.
    async fn fake_okx_quotes(schedule: Schedule, modes: Modes) -> (String, Arc<Mutex<VenueLog>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let log = Arc::new(Mutex::new(VenueLog::default()));
        let (updates, _) = tokio::sync::broadcast::channel::<(u64, usize, i64)>(4_096);
        let start = Instant::now();
        let generator = updates.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(10));
            let mut seq = 0_u64;
            loop {
                tick.tick().await;
                seq += 1;
                let _ = generator.send((seq, seq as usize % SYMBOLS.len(), wall_ms()));
            }
        });
        let accept_log = Arc::clone(&log);
        tokio::spawn(async move {
            let mut index = 0_usize;
            while let Ok((stream, _)) = listener.accept().await {
                let connection = index;
                index += 1;
                {
                    let mut log = accept_log.lock().unwrap();
                    log.accepted.push(Some(Instant::now()));
                    log.acked.push(None);
                    log.closed.push(None);
                }
                let log = Arc::clone(&accept_log);
                let schedule = Arc::clone(&schedule);
                let mode = modes(connection);
                let mut feed = updates.subscribe();
                tokio::spawn(async move {
                    let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                    let command = socket.next().await.unwrap().unwrap();
                    let command: Value = serde_json::from_str(command.to_text().unwrap()).unwrap();
                    let args = command["args"].as_array().unwrap();
                    let acked = if mode == VenueMode::AckAllButOne {
                        &args[..args.len() - 1]
                    } else {
                        &args[..]
                    };
                    for arg in acked {
                        let ack = json!({"event": "subscribe", "arg": arg, "id": command["id"]});
                        socket.send(Message::Text(ack.to_string())).await.unwrap();
                    }
                    if mode != VenueMode::AckAllButOne {
                        log.lock().unwrap().acked[connection] = Some(Instant::now());
                    }
                    let mut control = tokio::time::interval(Duration::from_millis(100));
                    // Control-only modes: until the client goes away.
                    loop {
                        if mode == VenueMode::Quotes {
                            break;
                        }
                        tokio::select! {
                            _ = control.tick(), if mode == VenueMode::AckAllButOne => {
                                let ping = socket.send(Message::Ping(Default::default())).await;
                                let pong = socket.send(Message::Text("pong".into())).await;
                                if ping.is_err() || pong.is_err() {
                                    break;
                                }
                            }
                            incoming = socket.next() => match incoming {
                                Some(Ok(Message::Text(text))) if text.as_str() == "ping" => {
                                    if socket.send(Message::Text("pong".into())).await.is_err() {
                                        break;
                                    }
                                }
                                Some(Ok(_)) => {}
                                _ => break,
                            },
                        }
                    }
                    loop {
                        if mode != VenueMode::Quotes {
                            break;
                        }
                        tokio::select! {
                            update = feed.recv() => {
                                let (seq, symbol, at_ms) = match update {
                                    Ok(update) => update,
                                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                                    Err(_) => break,
                                };
                                let delay = schedule(connection, start.elapsed(), seq);
                                let frame = json!({
                                    "arg": {"channel": "bbo-tbt", "instId": SYMBOLS[symbol]},
                                    "data": [{
                                        "asks": [["2", "1", "0", "1"]],
                                        "bids": [["1", "1", "0", "1"]],
                                        "ts": (at_ms - delay).to_string(),
                                        "seqId": seq,
                                    }],
                                });
                                if socket.send(Message::Text(frame.to_string())).await.is_err() {
                                    break;
                                }
                            }
                            incoming = socket.next() => {
                                if !matches!(incoming, Some(Ok(_))) {
                                    break;
                                }
                            }
                        }
                    }
                    log.lock().unwrap().closed[connection] = Some(Instant::now());
                });
            }
        });
        (format!("ws://{address}/ws/v5/public"), log)
    }

    fn scratch(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "qdl-idr112-{label}-{}-{}",
            std::process::id(),
            now_ns().unwrap()
        ));
        fs::create_dir_all(path.join("liveness")).unwrap();
        path
    }

    /// Small timings so a run takes seconds: 100 ms buckets over 20 s, armed
    /// after 300 ms, 5 stale frames from 2 instruments inside 500 ms.
    fn fast_staleness(cooldown_ms: u64) -> LatestStateStalenessConfig {
        LatestStateStalenessConfig {
            enabled: true,
            excess_ms: 250,
            window_ms: 500,
            min_frames: 5,
            min_bindings: 2,
            baseline_bucket_ms: 100,
            baseline_buckets: 200,
            min_baseline_buckets: 3,
            cooldown_ms,
            max_recycles_per_hour: 6,
        }
    }

    struct LaneRun {
        log: VenueLog,
        /// Published raw envelopes in publish order.
        records: Vec<RawProviderEnvelope>,
        report: Value,
        /// From the start of the lane to `run_okx_service` returning.
        returned_after: Duration,
        /// Wall clock (ns) when `run_okx_service` returned.
        returned_at_ns: i64,
    }

    /// Lane knobs that differ between scenarios.
    #[derive(Clone, Copy)]
    struct LaneOptions {
        establish_budget: Duration,
        /// `liveness_ping_ms`/`liveness_dead_ms`; (0, 0) = heartbeat rule.
        liveness_ms: (u64, u64),
    }

    impl Default for LaneOptions {
        fn default() -> Self {
            Self {
                establish_budget: OKX_ESTABLISH_BUDGET,
                liveness_ms: (0, 0),
            }
        }
    }

    /// Runs the production `run_okx_service` (lane okx-public-001) against
    /// the fake venue for `run_for`, publishing into the in-memory sink.
    async fn run_lane(
        schedule: Schedule,
        staleness: LatestStateStalenessConfig,
        run_for: Duration,
    ) -> LaneRun {
        run_lane_with(
            schedule,
            all_quotes(),
            staleness,
            run_for,
            LaneOptions::default(),
        )
        .await
    }

    async fn run_lane_with(
        schedule: Schedule,
        modes: Modes,
        staleness: LatestStateStalenessConfig,
        run_for: Duration,
        options: LaneOptions,
    ) -> LaneRun {
        let (url, log) = fake_okx_quotes(schedule, modes).await;
        let symbols = SYMBOLS.iter().map(|symbol| symbol.to_string()).collect();
        let (records, report, returned_after) =
            run_quote_lane(url, symbols, staleness, run_for, options).await;
        let returned_at_ns = now_ns().unwrap();
        // Let the venue notice the last close.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let log = std::mem::take(&mut *log.lock().unwrap());
        LaneRun {
            log,
            records,
            report,
            returned_after,
            returned_at_ns,
        }
    }

    /// The production `run_okx_service` on one QUOTE lane (okx-public-001)
    /// for `run_for`, publishing into the in-memory sink.
    async fn run_quote_lane(
        url: String,
        symbols: Vec<String>,
        staleness: LatestStateStalenessConfig,
        run_for: Duration,
        options: LaneOptions,
    ) -> (Vec<RawProviderEnvelope>, Value, Duration) {
        let bindings = symbols
            .iter()
            .map(|symbol| quote(symbol))
            .collect::<Vec<_>>();
        let dir = scratch("lane");
        let config = Arc::new(IngestorConfig {
            runtime: ProviderRuntime::Okx,
            websocket_url: "wss://unit.test/ws/v5/public".into(),
            business_websocket_url: Some("wss://unit.test/ws/v5/business".into()),
            market_websocket_url: None,
            raw_stream: "unit.raw".into(),
            shard_id: "unit".into(),
            lease_epoch: 1,
            partition_plan_epoch: 1,
            config_revision: 9,
            heartbeat_seconds: 10,
            max_events: 0,
            max_runtime_seconds: 0,
            metrics_every_events: 100,
            generation_state_path: dir.join("generation").display().to_string(),
            session_liveness_dir: dir.join("liveness").display().to_string(),
            session_liveness_write_interval_ms: 1000,
            max_inflight_publishes: 64,
            max_subscriptions_per_connection: 100,
            latest_state_flush_ms: 1,
            book_liveness_proof_ms: 0,
            liveness_ping_ms: options.liveness_ms.0,
            liveness_dead_ms: options.liveness_ms.1,
            latest_state_staleness: staleness,
            authority: AuthorityRecord {
                schema: "qdl.authority-record.v1".into(),
                slice_id: "unit".into(),
                revision: 2,
                mode: AuthorityMode::RustShadow,
                candidate_image_digest: format!("sha256:{}", "0".repeat(64)),
                capability_manifest_digest: "0".repeat(64),
                contract_digest: "0".repeat(64),
                partition_plan_digest: "0".repeat(64),
                public_write_allowed: false,
                legacy_write_allowed: false,
                approved_by: "unit-test".into(),
                effective_at_ns: 1,
            },
            bindings: bindings.clone(),
            book_resync: None,
        });
        let (publisher, records) = RawPublisher::memory(&config);
        let telemetry = Arc::new(TelemetryRegistry::default());
        let stopped = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&stopped);
        let started = Instant::now();
        let (result, ()) = tokio::join!(
            run_okx_service(
                OkxServiceShard {
                    service: OkxService::Public,
                    shard_index: 1,
                    url,
                    bindings,
                    establish_budget: options.establish_budget,
                },
                Arc::clone(&config),
                Arc::new(publisher),
                Arc::clone(&telemetry),
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU64::new(0)),
                stopped,
                OkxConnectPacer::default(),
            ),
            async move {
                tokio::time::sleep(run_for).await;
                stop.store(true, Ordering::Release);
            }
        );
        result.unwrap();
        let returned_after = started.elapsed();
        let report =
            telemetry.progress_line("OKX", run_for, now_ns().unwrap(), 0, 0)["lanes"][0].clone();
        let records = records
            .lock()
            .unwrap()
            .iter()
            .map(|record| RawProviderEnvelope::decode(record.payload.as_slice()).unwrap())
            .collect();
        let _ = fs::remove_dir_all(dir);
        (records, report, returned_after)
    }

    fn generations(records: &[RawProviderEnvelope]) -> Vec<u64> {
        let mut seen: Vec<u64> = Vec::new();
        for record in records {
            if seen.last() != Some(&record.connection_generation) {
                // Generation N is published strictly before N+1, never again after.
                assert!(
                    !seen.contains(&record.connection_generation),
                    "generation {} published again after its successor",
                    record.connection_generation
                );
                seen.push(record.connection_generation);
            }
        }
        seen
    }

    /// Review F2: per binding, the last frame published by generation `old`
    /// and the signed time (ms) from it to the first frame of `new`; None when
    /// `new` published nothing for that binding. Receipt times: a negative
    /// value is overlap (the successor received it before the old session's
    /// last frame; it was still published after it).
    fn handover_gaps(
        records: &[RawProviderEnvelope],
        old: u64,
        new: u64,
    ) -> Vec<(String, i64, Option<i64>)> {
        SYMBOLS
            .iter()
            .filter_map(|symbol| {
                let of = |generation: u64| {
                    records.iter().filter(move |record| {
                        record.connection_generation == generation
                            && record.native_symbol == *symbol
                    })
                };
                let last_old = of(old).map(|record| record.received_at_ns).max()?;
                let first_new = of(new).map(|record| record.received_at_ns).min();
                Some((
                    symbol.to_string(),
                    last_old,
                    first_new.map(|first| (first - last_old) / 1_000_000),
                ))
            })
            .collect()
    }

    fn symbols_of(records: &[RawProviderEnvelope], generation: u64) -> Vec<String> {
        let mut symbols = records
            .iter()
            .filter(|record| record.connection_generation == generation)
            .map(|record| record.native_symbol.clone())
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();
        symbols
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stale_session_is_recycled_make_before_break_and_keeps_every_quote() {
        // Connection 0 turns 0.9 s late after 1.5 s; its successor is healthy.
        let schedule: Schedule = Arc::new(|connection: usize, elapsed: Duration, _: u64| {
            if connection == 0 && elapsed >= Duration::from_millis(1_500) {
                900
            } else {
                0
            }
        });
        let run = run_lane(
            schedule,
            fast_staleness(3_000),
            Duration::from_millis(3_000),
        )
        .await;
        assert_eq!(run.log.acked.len(), 2, "exactly one recycle: {:?}", run.log);
        let successor_ready = run.log.acked[1].unwrap();
        let old_closed = run.log.closed[0].unwrap();
        // Make-before-break: the successor was subscribed while the stale
        // socket was still open.
        assert!(successor_ready < old_closed, "{:?}", run.log);
        let generations = generations(&run.records);
        assert_eq!(generations.len(), 2, "{generations:?}");
        for generation in &generations {
            assert_eq!(symbols_of(&run.records, *generation).len(), SYMBOLS.len());
        }
        // Continuity: the handover leaves no reception gap beyond a few ticks.
        let last_old = run
            .records
            .iter()
            .filter(|record| record.connection_generation == generations[0])
            .map(|record| record.received_at_ns)
            .max()
            .unwrap();
        let first_new = run
            .records
            .iter()
            .filter(|record| record.connection_generation == generations[1])
            .map(|record| record.received_at_ns)
            .min()
            .unwrap();
        assert!(
            first_new - last_old < 100_000_000,
            "handover gap {} ms",
            (first_new - last_old) / 1_000_000
        );
        // Review F2: what the ACK cutover gives each binding, measured, not
        // claimed. Here the successor streams at once (10 ms venue tick, a
        // binding every 30 ms), so each binding's window is a few ticks.
        let gaps = handover_gaps(&run.records, generations[0], generations[1]);
        println!(
            "{}",
            json!({
                "event": "qdl_idr112_handover_gap_per_binding",
                "scenario": "SUCCESSOR_STREAMING",
                "gaps_ms": gaps
                    .iter()
                    .map(|(symbol, _, gap)| json!({"binding": symbol, "last_old_to_first_new_ms": gap}))
                    .collect::<Vec<_>>(),
            })
        );
        assert_eq!(gaps.len(), SYMBOLS.len());
        for (symbol, _, gap) in &gaps {
            let gap = gap.unwrap_or_else(|| panic!("{symbol}: no successor frame"));
            assert!(gap < 250, "{symbol}: last old -> first new {gap} ms");
        }
        assert_eq!(run.report["staleness_recycles_total"], 1);
        assert!(run.report["stale_frames"].as_u64().unwrap() >= 5);
        assert!(run.report["venue_lag_ms"]["max"].as_f64().unwrap() >= 850.0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn healthy_skewed_and_single_outlier_sessions_are_never_recycled() {
        for skew in [5_000_i64, -3_000] {
            // Constant skew; every 50th update (one frame per 500 ms window)
            // is 2 s late.
            let schedule: Schedule = Arc::new(
                move |_: usize, _: Duration, seq: u64| {
                    if seq % 50 == 0 {
                        skew + 2_000
                    } else {
                        skew
                    }
                },
            );
            let run = run_lane(
                schedule,
                fast_staleness(3_000),
                Duration::from_millis(2_500),
            )
            .await;
            assert_eq!(run.log.acked.len(), 1, "skew {skew}: {:?}", run.log);
            assert_eq!(generations(&run.records).len(), 1);
            assert_eq!(run.report["staleness_recycles_total"], 0);
            assert!(
                run.report["stale_frames"].as_u64().unwrap() >= 1,
                "the outliers were measured as stale, alone: {}",
                run.report
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lane_that_stays_stale_is_recycled_again_only_after_the_cooldown() {
        // Healthy for 3 s, then every connection is 0.9 s late.
        let schedule: Schedule = Arc::new(|connection: usize, elapsed: Duration, _: u64| {
            if connection > 0 || elapsed >= Duration::from_millis(3_000) {
                900
            } else {
                0
            }
        });
        let run = run_lane(
            schedule,
            fast_staleness(1_200),
            Duration::from_millis(5_000),
        )
        .await;
        assert_eq!(run.log.acked.len(), 3, "{:?}", run.log);
        let first = run.log.acked[1].unwrap();
        let second = run.log.acked[2].unwrap();
        assert!(
            second.duration_since(first) >= Duration::from_millis(1_000),
            "second recycle {:?} after the first",
            second.duration_since(first)
        );
        assert_eq!(generations(&run.records).len(), 3);
        assert_eq!(run.report["staleness_recycles_total"], 2);
    }

    /// Connection 0 turns 0.9 s late after `after`; later connections are
    /// healthy (their mode decides whether they ACK and stream). The stale
    /// spell must stay under half the healthy history, or the 100 ms-bucket
    /// median baseline adopts it (the documented F3 trade-off).
    fn stale_first_connection(after: Duration) -> Schedule {
        Arc::new(move |connection: usize, elapsed: Duration, _: u64| {
            if connection == 0 && elapsed >= after {
                900
            } else {
                0
            }
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_standby_held_open_by_control_traffic_is_cancelled_at_the_budget() {
        // Review F1 negative: the first standby gets every ACK but one and a
        // ping plus a text pong every 100 ms, which used to keep the per-read
        // timeout from ever firing. The absolute budget cancels it; the active
        // lane keeps delivering; after the normal cooldown the next recycle
        // succeeds on a healthy connection.
        let budget = Duration::from_millis(600);
        let cooldown = Duration::from_millis(1_500);
        let modes: Modes = Arc::new(|connection| {
            if connection == 1 {
                VenueMode::AckAllButOne
            } else {
                VenueMode::Quotes
            }
        });
        let run = run_lane_with(
            stale_first_connection(Duration::from_millis(3_000)),
            modes,
            fast_staleness(cooldown.as_millis() as u64),
            Duration::from_millis(6_000),
            LaneOptions {
                establish_budget: budget,
                ..LaneOptions::default()
            },
        )
        .await;
        assert_eq!(run.log.accepted.len(), 3, "{:?}", run.log);
        assert!(
            run.log.acked[1].is_none(),
            "the held standby never completed"
        );
        let held = run.log.closed[1].unwrap() - run.log.accepted[1].unwrap();
        assert!(
            held >= budget - Duration::from_millis(100)
                && held < budget + Duration::from_millis(400),
            "held standby lived {held:?} for a {budget:?} budget"
        );
        // The later attempt waited for the cooldown counted from the failed
        // one, then completed.
        let retried = run.log.acked[2].expect("the later attempt completed");
        assert!(retried > run.log.closed[1].unwrap());
        assert!(
            retried - run.log.accepted[1].unwrap() >= cooldown - Duration::from_millis(200),
            "{:?}",
            run.log
        );
        // The active lane never paused: generation 0 streamed until the
        // successful handover, the failed generation published nothing.
        let generations = generations(&run.records);
        assert_eq!(generations.len(), 2, "{generations:?}");
        assert_eq!(
            generations[1],
            generations[0] + 2,
            "generation 1 was the cancelled standby"
        );
        let old = run
            .records
            .iter()
            .filter(|record| record.connection_generation == generations[0])
            .map(|record| record.received_at_ns)
            .collect::<Vec<_>>();
        let largest_pause_ms = old
            .windows(2)
            .map(|pair| (pair[1] - pair[0]) / 1_000_000)
            .max()
            .unwrap();
        assert!(
            largest_pause_ms < 100,
            "active lane paused {largest_pause_ms} ms"
        );
        assert_eq!(
            symbols_of(&run.records, generations[1]).len(),
            SYMBOLS.len()
        );
        assert_eq!(run.report["standby_deadline_total"], 1, "{}", run.report);
        assert_eq!(run.report["standby_failures_total"], 1);
        assert_eq!(run.report["staleness_recycles_total"], 1);
        assert!(run.report["staleness_suppressed"].as_u64().unwrap() >= 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_while_a_standby_is_pending_returns_promptly() {
        // Review F1: the production budget (15 s) is still running when the
        // lane is stopped; the pending standby is dropped with the session.
        let modes: Modes = Arc::new(|connection| {
            if connection == 1 {
                VenueMode::AckAllButOne
            } else {
                VenueMode::Quotes
            }
        });
        let run_for = Duration::from_millis(2_500);
        let run = run_lane_with(
            stale_first_connection(Duration::from_millis(1_500)),
            modes,
            fast_staleness(10_000),
            run_for,
            LaneOptions::default(),
        )
        .await;
        assert!(
            run.returned_after < run_for + Duration::from_millis(500),
            "returned after {:?}",
            run.returned_after
        );
        assert_eq!(run.log.accepted.len(), 2, "{:?}", run.log);
        assert!(run.log.acked[1].is_none());
        assert!(
            run.log.closed[1].is_some(),
            "the pending standby socket was dropped"
        );
        assert_eq!(generations(&run.records).len(), 1);
        assert_eq!(run.report["standby_failures_total"], 0);
        assert_eq!(
            run.report["staleness_last_action"],
            "SUPPRESSED_STANDBY_PENDING"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_ack_only_successor_leaves_each_binding_uncovered_and_is_reported() {
        // Review F2 option A: the cutover happens at subscription ACK, which
        // does not prove successor data. A successor that ACKs and then sends
        // nothing leaves every binding without a newer frame from the moment
        // the old session stops; the test measures and reports that window
        // (no bound is claimed) and must not hang: the successor answers the
        // liveness ping, so the lane stays up until it is stopped.
        let modes: Modes = Arc::new(|connection| {
            if connection == 1 {
                VenueMode::AckOnly
            } else {
                VenueMode::Quotes
            }
        });
        let run_for = Duration::from_millis(3_500);
        let run = run_lane_with(
            stale_first_connection(Duration::from_millis(1_500)),
            modes,
            fast_staleness(10_000),
            run_for,
            LaneOptions {
                liveness_ms: (500, 1_500),
                ..LaneOptions::default()
            },
        )
        .await;
        assert!(run.returned_after < run_for + Duration::from_millis(1_000));
        assert_eq!(run.log.acked.len(), 2, "{:?}", run.log);
        assert!(run.log.acked[1].is_some());
        let generations = generations(&run.records);
        assert_eq!(generations.len(), 1, "the successor published nothing");
        let successor = run.report["generation"].as_u64().unwrap();
        assert!(
            successor > generations[0],
            "the ACK-only successor took over"
        );
        let gaps = handover_gaps(&run.records, generations[0], successor);
        let report = gaps
            .iter()
            .map(|(symbol, last_old, gap)| {
                json!({
                    "binding": symbol,
                    "last_old_to_first_new_ms": gap,
                    "outcome": if gap.is_some() { "SUCCESSOR_FRAME" } else { "NO_SUCCESSOR_FRAME" },
                    "uncovered_until_stop_ms": (run.returned_at_ns - last_old) / 1_000_000,
                })
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            json!({
                "event": "qdl_idr112_handover_gap_per_binding",
                "scenario": "SUCCESSOR_ACK_ONLY",
                "gaps": report,
            })
        );
        assert_eq!(gaps.len(), SYMBOLS.len());
        for (symbol, last_old, gap) in &gaps {
            assert!(gap.is_none(), "{symbol}");
            // Stale from ~1.5 s, recycled within ~0.1 s, stopped at 3.5 s.
            assert!(
                (run.returned_at_ns - last_old) / 1_000_000 >= 1_000,
                "{symbol}: uncovered window under 1 s"
            );
        }
        assert_eq!(run.report["staleness_recycles_total"], 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disabled_detector_never_recycles_a_stale_session() {
        let schedule: Schedule = Arc::new(|_: usize, elapsed: Duration, _: u64| {
            if elapsed >= Duration::from_millis(1_000) {
                900
            } else {
                0
            }
        });
        let off = LatestStateStalenessConfig {
            enabled: false,
            ..fast_staleness(1_200)
        };
        let run = run_lane(schedule, off, Duration::from_millis(2_000)).await;
        assert_eq!(run.log.acked.len(), 1);
        assert!(run.report["venue_lag_ms"].is_null());
        assert_eq!(run.report["staleness_recycles_total"], 0);
    }

    /// TEST_ONLY live smoke (read-only, OKX public, no Kafka): the production
    /// lane loop on real bbo-tbt with the in-memory sink. Run explicitly:
    /// `cargo test ... live_okx_bbo_tbt -- --ignored --nocapture` with
    /// QDL_IDR112_LIVE_SECONDS (default 120), QDL_IDR112_LIVE_SYMBOLS
    /// (comma list) and QDL_IDR112_LIVE_MODE=defaults|forced. `forced` sets a
    /// 5 ms excess so live jitter triggers recycles and the make-before-break
    /// handover is exercised against the real venue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "live network: OKX public WebSocket"]
    async fn live_okx_bbo_tbt_smoke() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let seconds = std::env::var("QDL_IDR112_LIVE_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(120_u64);
        let symbols = std::env::var("QDL_IDR112_LIVE_SYMBOLS")
            .unwrap_or_else(|_| SYMBOLS.join(","))
            .split(',')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let forced = std::env::var("QDL_IDR112_LIVE_MODE").as_deref() == Ok("forced");
        let staleness = if forced {
            LatestStateStalenessConfig {
                excess_ms: 5,
                window_ms: 2_000,
                min_frames: 3,
                baseline_bucket_ms: 2_000,
                min_baseline_buckets: 3,
                cooldown_ms: 15_000,
                max_recycles_per_hour: 60,
                ..LatestStateStalenessConfig::default()
            }
        } else {
            LatestStateStalenessConfig::default()
        };
        let (records, report, _) = run_quote_lane(
            "wss://ws.okx.com/ws/v5/public".into(),
            symbols.clone(),
            staleness,
            Duration::from_secs(seconds),
            LaneOptions::default(),
        )
        .await;
        let generations = generations(&records);
        let summary = json!({
            "event": "qdl_idr112_live_smoke_summary",
            "mode": if forced { "forced" } else { "defaults" },
            "seconds": seconds,
            "symbols": symbols,
            "records": records.len(),
            "generations": generations,
            "symbols_per_generation": generations
                .iter()
                .map(|generation| symbols_of(&records, *generation).len())
                .collect::<Vec<_>>(),
            "lane_report": report,
        });
        println!("{summary}");
        assert!(!records.is_empty());
        let last = *generations.last().unwrap();
        assert_eq!(symbols_of(&records, last).len(), symbols.len());
        if forced {
            assert!(generations.len() >= 2, "{summary}");
            assert_eq!(
                report["staleness_recycles_total"].as_u64(),
                Some(generations.len() as u64 - 1)
            );
        } else {
            assert_eq!(report["staleness_recycles_total"], 0, "{summary}");
        }
    }
}
