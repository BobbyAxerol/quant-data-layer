#![forbid(unsafe_code)]

pub mod phase92_bootstrap;
pub mod phase92_runtime;

use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::{try_join_all, BoxFuture};
use qdl_core::transport::{AppendResult, Cursor, DurableRecord, RetryClass};
use qdl_venue_core::authority::{
    AuthorityFence, AuthorityRecord, Phase92AcceptedHandoff, Phase92AuthorityFence,
    Phase92AuthorityRecord, Phase92PublicationContext, Phase92TerminalCheckpoint,
    Phase9AuthorityFence, Phase9AuthorityRecord, Phase9PublicationContext, PublicationContext,
    SinkTarget,
};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

const EVENT_ID_HEADER: &str = "qdl-event-id";
const RAW_ENVELOPE_HEADER: &str = "qdl-raw-provider-envelope";
const COOPERATIVE_ASSIGNMENT_STRATEGY: &str = "cooperative-sticky";
const CONSUMER_GROUP_PROTOCOL: &str = "classic";

/// Group session bound of every classic Rust group consumer (F1, 2026-10-15).
///
/// Membership stays dynamic (architecture 28.6, C40.13): a SIGTERM-stopped
/// member sends LeaveGroup and its partitions move at once, which static
/// membership would delay to the session timeout. What this bounds is the
/// ungraceful death (SIGKILL, OOM, host loss): the dead member keeps its
/// partitions until the broker expires its session, and the librdkafka default
/// of 45 s was measured as the whole 42 s core-kill outage (N1 R2 corekill9k:
/// the replacement joined 0.4 s after the kill and waited 41.9 s for the old
/// member's removal). 10 s is above the broker's default
/// `group.min.session.timeout.ms` (6 s; no deployment overrides it) and leaves
/// ten heartbeats of margin. Heartbeats run on librdkafka's background thread,
/// not the batch loop, so a long transaction does not expire the session;
/// `max.poll.interval.ms` (300 s default) is unchanged.
const CONSUMER_SESSION_TIMEOUT_MS: &str = "10000";
const CONSUMER_HEARTBEAT_INTERVAL_MS: &str = "1000";

// Keep every consumer/producer instance bounded inside its cgroup. Durable Kafka
// offsets provide backpressure/recovery; unbounded librdkafka local queues do not.
const KAFKA_QUEUE_MAX_KBYTES: &str = "8192";
const KAFKA_FETCH_MAX_BYTES: &str = "4194304";
const KAFKA_MAX_PARTITION_FETCH_BYTES: &str = "1048576";
const KAFKA_PRODUCER_QUEUE_MAX_MESSAGES: &str = "4096";

/// Local producer queue of the realtime core's transactional producer.
///
/// One transaction enqueues a whole batch before it can commit, and since the
/// core batch moved from 256 to 2048 records (P18.4 capacity step 1) the
/// shared 8 MiB / 4096-message bound no longer holds one batch of L2 output.
/// Sizing (2026-10-12, 120 s public capture): Binance USD-M `@depth@100ms`
/// frames are 1.6 KiB p50, 2.5 KiB mean, 16 KiB p99, 38 KiB max; OKX `books`
/// updates 0.9 KiB p50, 9 KiB p99. A canonical BOOK record carries the
/// canonical payload (about the raw size again) plus the raw-envelope lineage
/// header (the raw frame plus ~300 B), so about 2.2x raw + 0.7 KiB: 6.3 KiB at
/// the Binance mean, 36 KiB at its p99. 2048 x 6.3 KiB = 12.6 MiB already
/// exceeds 8 MiB, and `FutureProducer::send` then sleeps in 100 ms QueueFull
/// retries inside the open transaction. 64 MiB holds 2048 records of 32 KiB,
/// and the queue stays a bound, not a reservation: memory is used only by an
/// actual batch (live core RSS 36-64 MiB under a 256 MiB limit). A batch above
/// it still completes, by waiting, as before. Messages: 16 outputs per input
/// bound OKX multi-row frames and materialized snapshots.
const TRANSACTIONAL_PRODUCER_QUEUE_MAX_KBYTES: &str = "65536";
const TRANSACTIONAL_PRODUCER_QUEUE_MAX_MESSAGES: &str = "32768";

/// Explicit bounds of a producer's local librdkafka queue, for a producer that
/// is shared by several publishers and must keep their aggregate bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerQueueBounds {
    pub max_messages: u64,
    pub max_kbytes: u64,
}

impl ProducerQueueBounds {
    /// The default per-client bound (`KAFKA_PRODUCER_QUEUE_MAX_MESSAGES`,
    /// `KAFKA_QUEUE_MAX_KBYTES`) times `publishers`: one producer serving
    /// `publishers` lanes keeps the aggregate bound those lanes had with one
    /// producer each. librdkafka caps both settings at i32::MAX.
    pub fn scaled_default(publishers: usize) -> Result<Self, KafkaTransportError> {
        let publishers = u64::try_from(publishers.max(1)).map_err(|_| {
            KafkaTransportError::Configuration("producer publisher count overflow".into())
        })?;
        let messages: u64 = KAFKA_PRODUCER_QUEUE_MAX_MESSAGES
            .parse()
            .expect("constant producer queue messages");
        let kbytes: u64 = KAFKA_QUEUE_MAX_KBYTES
            .parse()
            .expect("constant producer queue kbytes");
        let bounds = Self {
            max_messages: messages.saturating_mul(publishers),
            max_kbytes: kbytes.saturating_mul(publishers),
        };
        if bounds.max_messages > i32::MAX as u64 || bounds.max_kbytes > i32::MAX as u64 {
            return Err(KafkaTransportError::Configuration(
                "producer queue bound exceeds the librdkafka maximum".into(),
            ));
        }
        Ok(bounds)
    }

    fn apply(self, config: &mut ClientConfig) {
        config
            .set("queue.buffering.max.kbytes", self.max_kbytes.to_string())
            .set(
                "queue.buffering.max.messages",
                self.max_messages.to_string(),
            );
    }
}

fn configure_bounded_client_memory(config: &mut ClientConfig) {
    config
        .set("queued.max.messages.kbytes", KAFKA_QUEUE_MAX_KBYTES)
        .set("fetch.max.bytes", KAFKA_FETCH_MAX_BYTES)
        .set("max.partition.fetch.bytes", KAFKA_MAX_PARTITION_FETCH_BYTES)
        .set("queue.buffering.max.kbytes", KAFKA_QUEUE_MAX_KBYTES)
        .set(
            "queue.buffering.max.messages",
            KAFKA_PRODUCER_QUEUE_MAX_MESSAGES,
        );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownSignal {
    Interrupt,
    Terminate,
}

impl ShutdownSignal {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Interrupt => "SIGINT",
            Self::Terminate => "SIGTERM",
        }
    }
}

pub async fn shutdown_signal() -> io::Result<ShutdownSignal> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                Ok(ShutdownSignal::Interrupt)
            }
            _ = terminate.recv() => Ok(ShutdownSignal::Terminate),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok(ShutdownSignal::Interrupt)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaTlsConfig {
    pub ca_location: String,
    pub certificate_location: String,
    pub key_location: String,
    pub key_password: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaTransportConfig {
    pub bootstrap_servers: String,
    pub client_id: String,
    pub group_id: String,
    pub request_timeout: Duration,
    pub tls: KafkaTlsConfig,
}

impl KafkaTransportConfig {
    pub fn validate(&self) -> Result<(), KafkaTransportError> {
        for (field, value) in [
            ("bootstrap_servers", self.bootstrap_servers.as_str()),
            ("client_id", self.client_id.as_str()),
            ("group_id", self.group_id.as_str()),
            ("ca_location", self.tls.ca_location.as_str()),
            (
                "certificate_location",
                self.tls.certificate_location.as_str(),
            ),
            ("key_location", self.tls.key_location.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(KafkaTransportError::Configuration(format!(
                    "{field} must not be empty"
                )));
            }
        }
        if self.request_timeout.is_zero() {
            return Err(KafkaTransportError::Configuration(
                "request_timeout must be positive".into(),
            ));
        }
        for path in [
            &self.tls.ca_location,
            &self.tls.certificate_location,
            &self.tls.key_location,
        ] {
            if !Path::new(path).is_file() {
                return Err(KafkaTransportError::Configuration(format!(
                    "TLS file does not exist: {path}"
                )));
            }
        }
        Ok(())
    }

    fn client_config(&self) -> Result<ClientConfig, KafkaTransportError> {
        self.validate()?;
        let timeout_ms = self.request_timeout.as_millis().to_string();
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("client.id", &self.client_id)
            .set("security.protocol", "ssl")
            .set("ssl.ca.location", &self.tls.ca_location)
            .set("ssl.certificate.location", &self.tls.certificate_location)
            .set("ssl.key.location", &self.tls.key_location)
            .set("ssl.endpoint.identification.algorithm", "https")
            .set("socket.timeout.ms", &timeout_ms)
            .set("request.timeout.ms", &timeout_ms);
        configure_bounded_client_memory(&mut config);
        if let Some(password) = &self.tls.key_password {
            config.set("ssl.key.password", password);
        }
        Ok(config)
    }

    pub(crate) fn configure_group_consumer(&self, config: &mut ClientConfig) {
        self.configure_group_consumer_with_offset_reset(config, "earliest");
    }

    pub(crate) fn configure_group_consumer_fail_closed(&self, config: &mut ClientConfig) {
        self.configure_group_consumer_with_offset_reset(config, "error");
    }

    fn configure_group_consumer_with_offset_reset(
        &self,
        config: &mut ClientConfig,
        offset_reset: &str,
    ) {
        config
            .set("group.id", &self.group_id)
            .set("group.protocol", CONSUMER_GROUP_PROTOCOL)
            .set(
                "partition.assignment.strategy",
                COOPERATIVE_ASSIGNMENT_STRATEGY,
            )
            .set("session.timeout.ms", CONSUMER_SESSION_TIMEOUT_MS)
            .set("heartbeat.interval.ms", CONSUMER_HEARTBEAT_INTERVAL_MS)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", offset_reset)
            .set("isolation.level", "read_committed");
    }
}

#[derive(Debug)]
pub enum KafkaTransportError {
    Configuration(String),
    Kafka(KafkaError),
    Delivery(KafkaError),
    MissingField(&'static str),
    InvalidOffset(i64),
    InvalidUtf8(&'static str),
    Fencing(String),
    SnapshotTimeout(String),
    /// K3. The consumer processed a partition revocation (or rebalance error)
    /// after the batch's first input was read, so its transaction was aborted
    /// rather than commit offsets this member may no longer own. Retryable:
    /// the generation restarts from committed offsets.
    Rebalanced {
        observed: u64,
        current: u64,
    },
}

impl KafkaTransportError {
    pub fn retry_class(&self) -> RetryClass {
        match self {
            Self::Configuration(_)
            | Self::MissingField(_)
            | Self::InvalidUtf8(_)
            | Self::Fencing(_) => RetryClass::NonRetryable,
            Self::InvalidOffset(_) => RetryClass::NonRetryable,
            Self::Kafka(KafkaError::MessageProduction(code))
            | Self::Delivery(KafkaError::MessageProduction(code))
                if code.to_string().contains("Queue full") =>
            {
                RetryClass::Capacity
            }
            Self::Kafka(_)
            | Self::Delivery(_)
            | Self::SnapshotTimeout(_)
            | Self::Rebalanced { .. } => RetryClass::Retryable,
        }
    }
}

impl Display for KafkaTransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(message) => write!(formatter, "invalid Kafka config: {message}"),
            Self::Kafka(error) => write!(formatter, "Kafka client error: {error}"),
            Self::Delivery(error) => write!(formatter, "Kafka delivery error: {error}"),
            Self::MissingField(field) => write!(formatter, "Kafka record missing {field}"),
            Self::InvalidOffset(offset) => write!(formatter, "invalid Kafka offset: {offset}"),
            Self::InvalidUtf8(field) => write!(formatter, "Kafka {field} is not UTF-8"),
            Self::Fencing(message) => write!(formatter, "Kafka sink fencing rejected: {message}"),
            Self::SnapshotTimeout(message) => {
                write!(formatter, "Kafka compacted snapshot timed out: {message}")
            }
            Self::Rebalanced { observed, current } => write!(
                formatter,
                "consumer rebalanced during the transaction (epoch {observed} -> {current}); aborted"
            ),
        }
    }
}

impl std::error::Error for KafkaTransportError {}

impl From<KafkaError> for KafkaTransportError {
    fn from(error: KafkaError) -> Self {
        Self::Kafka(error)
    }
}

pub struct FencedKafkaSink {
    sink: KafkaDurableSink,
    fence: std::sync::Mutex<AuthorityFence>,
}

impl FencedKafkaSink {
    pub fn new(config: &KafkaTransportConfig) -> Result<Self, KafkaTransportError> {
        Ok(Self {
            sink: KafkaDurableSink::new(config)?,
            fence: std::sync::Mutex::new(AuthorityFence::default()),
        })
    }

    /// A sink whose producer queue holds `bounds` instead of the per-client
    /// default, for one producer shared by several publishers.
    pub fn with_queue_bounds(
        config: &KafkaTransportConfig,
        bounds: ProducerQueueBounds,
    ) -> Result<Self, KafkaTransportError> {
        Ok(Self {
            sink: KafkaDurableSink::with_queue_bounds(config, bounds)?,
            fence: std::sync::Mutex::new(AuthorityFence::default()),
        })
    }

    pub fn apply_authority(&self, record: AuthorityRecord) -> Result<(), KafkaTransportError> {
        self.fence
            .lock()
            .map_err(|_| KafkaTransportError::Fencing("authority lock poisoned".into()))?
            .apply(record)
            .map_err(KafkaTransportError::Fencing)
    }

    pub fn enqueue(
        &self,
        record: &DurableRecord,
        publication: &PublicationContext,
    ) -> Result<PendingKafkaAppend, KafkaTransportError> {
        self.fence
            .lock()
            .map_err(|_| KafkaTransportError::Fencing("authority lock poisoned".into()))?
            .permits(publication)
            .map_err(KafkaTransportError::Fencing)?;
        self.sink.enqueue(record)
    }

    pub async fn append(
        &self,
        record: &DurableRecord,
        publication: &PublicationContext,
    ) -> Result<AppendResult, KafkaTransportError> {
        self.enqueue(record, publication)?.wait().await
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Phase9SinkTopics {
    pub shadow_raw: String,
    pub shadow_canonical: String,
    pub shadow_quarantine: String,
    pub canary_canonical: String,
}

impl Phase9SinkTopics {
    pub fn validate(&self) -> Result<(), KafkaTransportError> {
        let topics = [
            self.shadow_raw.as_str(),
            self.shadow_canonical.as_str(),
            self.shadow_quarantine.as_str(),
            self.canary_canonical.as_str(),
        ];
        if topics.iter().any(|topic| topic.trim().is_empty()) {
            return Err(KafkaTransportError::Configuration(
                "Phase 9 sink topics must not be empty".into(),
            ));
        }
        let unique: std::collections::HashSet<&str> = topics.into_iter().collect();
        if unique.len() != 4 {
            return Err(KafkaTransportError::Configuration(
                "Phase 9 sink topics must be isolated and unique".into(),
            ));
        }
        Ok(())
    }

    fn permits(&self, target: qdl_venue_core::authority::SinkTarget, stream: &str) -> bool {
        use qdl_venue_core::authority::SinkTarget;
        match target {
            SinkTarget::ShadowRaw => stream == self.shadow_raw,
            SinkTarget::ShadowCanonical => stream == self.shadow_canonical,
            SinkTarget::ShadowQuarantine => stream == self.shadow_quarantine,
            SinkTarget::CanaryCanonical => stream == self.canary_canonical,
            SinkTarget::PrimaryRaw
            | SinkTarget::PrimaryCanonical
            | SinkTarget::PrimaryQuarantine
            | SinkTarget::PublicV2
            | SinkTarget::LegacyV1 => false,
        }
    }
}

/// Phase 9.1 sink keeps authority stable through durable ACK, then commits
/// the source watermark. A failed append remains retryable at the same watermark.
pub struct Phase9FencedKafkaSink {
    sink: KafkaDurableSink,
    fence: tokio::sync::Mutex<Phase9AuthorityFence>,
    topics: Phase9SinkTopics,
}

impl Phase9FencedKafkaSink {
    pub fn new(
        config: &KafkaTransportConfig,
        topics: Phase9SinkTopics,
    ) -> Result<Self, KafkaTransportError> {
        topics.validate()?;
        Ok(Self {
            sink: KafkaDurableSink::new(config)?,
            fence: tokio::sync::Mutex::new(Phase9AuthorityFence::default()),
            topics,
        })
    }

    pub async fn apply_authority(
        &self,
        record: Phase9AuthorityRecord,
    ) -> Result<(), KafkaTransportError> {
        self.fence
            .lock()
            .await
            .apply(record)
            .map_err(KafkaTransportError::Fencing)
    }

    pub async fn append(
        &self,
        record: &DurableRecord,
        publication: &Phase9PublicationContext,
        now_ns: i64,
    ) -> Result<AppendResult, KafkaTransportError> {
        if !self.topics.permits(publication.target, &record.stream) {
            return Err(KafkaTransportError::Fencing(
                "publication target does not match its isolated Kafka topic".into(),
            ));
        }
        let mut fence = self.fence.lock().await;
        fence
            .permits(publication, now_ns)
            .map_err(KafkaTransportError::Fencing)?;
        let result = self.sink.append(record).await?;
        fence
            .commit(publication)
            .map_err(KafkaTransportError::Fencing)?;
        Ok(result)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Phase92SinkTopics {
    pub primary_canonical: String,
    pub public_v2: String,
    pub legacy_v1: String,
}

impl Phase92SinkTopics {
    pub fn validate(&self) -> Result<(), KafkaTransportError> {
        let topics = [
            self.primary_canonical.as_str(),
            self.public_v2.as_str(),
            self.legacy_v1.as_str(),
        ];
        if topics.iter().any(|topic| topic.trim().is_empty()) {
            return Err(KafkaTransportError::Configuration(
                "Phase 9.2 sink/projector topics must not be empty".into(),
            ));
        }
        if topics[0] == topics[1] || topics[0] == topics[2] || topics[1] == topics[2] {
            return Err(KafkaTransportError::Configuration(
                "Phase 9.2 sink/projector topics must be isolated and unique".into(),
            ));
        }
        Ok(())
    }

    fn permits(&self, target: SinkTarget, stream: &str) -> bool {
        match target {
            SinkTarget::PrimaryCanonical => stream == self.primary_canonical,
            SinkTarget::PublicV2 => stream == self.public_v2,
            SinkTarget::LegacyV1 => stream == self.legacy_v1,
            SinkTarget::ShadowRaw
            | SinkTarget::ShadowCanonical
            | SinkTarget::ShadowQuarantine
            | SinkTarget::CanaryCanonical
            | SinkTarget::PrimaryRaw
            | SinkTarget::PrimaryQuarantine => false,
        }
    }
}

/// Phase 9.2 final sink and compatibility projector share one authority fence.
/// The mutex remains held through durable ACK so an authority update cannot race
/// between sink acceptance and watermark commit.
pub struct Phase92FencedKafkaSink {
    sink: KafkaDurableSink,
    fence: tokio::sync::Mutex<Phase92AuthorityFence>,
    topics: Phase92SinkTopics,
}

impl Phase92FencedKafkaSink {
    pub fn new(
        config: &KafkaTransportConfig,
        topics: Phase92SinkTopics,
    ) -> Result<Self, KafkaTransportError> {
        topics.validate()?;
        Ok(Self {
            sink: KafkaDurableSink::new(config)?,
            fence: tokio::sync::Mutex::new(Phase92AuthorityFence::default()),
            topics,
        })
    }

    pub async fn apply_authority(
        &self,
        record: Phase92AuthorityRecord,
    ) -> Result<(), KafkaTransportError> {
        self.fence
            .lock()
            .await
            .apply(record)
            .map_err(KafkaTransportError::Fencing)
    }

    pub async fn apply_handoff(
        &self,
        checkpoint: &Phase92TerminalCheckpoint,
        handoff: &Phase92AcceptedHandoff,
        record: Phase92AuthorityRecord,
        now_ns: i64,
    ) -> Result<(), KafkaTransportError> {
        self.fence
            .lock()
            .await
            .apply_handoff(checkpoint, handoff, record, now_ns)
            .map_err(KafkaTransportError::Fencing)
    }

    pub async fn restore_committed_watermark(
        &self,
        publication: &Phase92PublicationContext,
    ) -> Result<(), KafkaTransportError> {
        self.fence
            .lock()
            .await
            .restore_committed_watermark(publication)
            .map_err(KafkaTransportError::Fencing)
    }

    pub async fn append(
        &self,
        record: &DurableRecord,
        publication: &Phase92PublicationContext,
        now_ns: i64,
    ) -> Result<AppendResult, KafkaTransportError> {
        if !self.topics.permits(publication.target, &record.stream) {
            return Err(KafkaTransportError::Fencing(
                "Phase 9.2 publication target does not match its topic".into(),
            ));
        }
        let mut fence = self.fence.lock().await;
        fence
            .permits(publication, now_ns)
            .map_err(KafkaTransportError::Fencing)?;
        let result = self.sink.append(record).await?;
        fence
            .commit(publication)
            .map_err(KafkaTransportError::Fencing)?;
        Ok(result)
    }
}

pub struct PendingKafkaAppend {
    delivery: DeliveryFuture,
    stream: String,
    partition_key: String,
}

/// A successful broker delivery is not always a usable cursor: idempotent
/// duplicate-sequence responses can omit the original offset. Never invent one.
#[derive(Debug)]
pub enum PersistedKafkaAppend {
    Positioned(AppendResult),
    OffsetUnavailable {
        stream: String,
        partition: i32,
        reported_offset: i64,
    },
}

fn persisted_append(
    stream: String,
    partition_key: String,
    partition: i32,
    offset: i64,
) -> Result<PersistedKafkaAppend, KafkaTransportError> {
    if partition < 0 {
        return Err(KafkaTransportError::Configuration(
            "successful delivery has no partition".into(),
        ));
    }
    if matches!(offset, -1001 | -1) {
        return Ok(PersistedKafkaAppend::OffsetUnavailable {
            stream,
            partition,
            reported_offset: offset,
        });
    }
    let offset = u64::try_from(offset).map_err(|_| KafkaTransportError::InvalidOffset(offset))?;
    Ok(PersistedKafkaAppend::Positioned(AppendResult {
        cursor: Cursor {
            stream,
            transport_partition: partition,
            partition_key,
            offset,
        },
        duplicate: false,
    }))
}

impl PendingKafkaAppend {
    pub async fn wait_persisted(self) -> Result<PersistedKafkaAppend, KafkaTransportError> {
        // Neither cancellation nor failed delivery may enter persisted_append.
        let delivery = self
            .delivery
            .await
            .map_err(|_| KafkaTransportError::Delivery(KafkaError::Canceled))?
            .map_err(|(error, _)| KafkaTransportError::Delivery(error))?;
        persisted_append(
            self.stream,
            self.partition_key,
            delivery.partition,
            delivery.offset,
        )
    }

    pub async fn wait(self) -> Result<AppendResult, KafkaTransportError> {
        match self.wait_persisted().await? {
            PersistedKafkaAppend::Positioned(result) => Ok(result),
            PersistedKafkaAppend::OffsetUnavailable {
                reported_offset, ..
            } => Err(KafkaTransportError::InvalidOffset(reported_offset)),
        }
    }
}

pub struct KafkaDurableSink {
    producer: FutureProducer,
}

/// Idempotent, acks=all producer settings of [`KafkaDurableSink`].
fn durable_producer_config(
    config: &KafkaTransportConfig,
    bounds: Option<ProducerQueueBounds>,
) -> Result<ClientConfig, KafkaTransportError> {
    let mut client = config.client_config()?;
    client
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("max.in.flight.requests.per.connection", "5")
        .set("retries", "2147483647")
        .set("compression.type", "zstd")
        .set(
            "delivery.timeout.ms",
            config.request_timeout.as_millis().to_string(),
        );
    if let Some(bounds) = bounds {
        bounds.apply(&mut client);
    }
    Ok(client)
}

impl KafkaDurableSink {
    pub fn new(config: &KafkaTransportConfig) -> Result<Self, KafkaTransportError> {
        Ok(Self {
            producer: durable_producer_config(config, None)?.create()?,
        })
    }

    pub fn with_queue_bounds(
        config: &KafkaTransportConfig,
        bounds: ProducerQueueBounds,
    ) -> Result<Self, KafkaTransportError> {
        Ok(Self {
            producer: durable_producer_config(config, Some(bounds))?.create()?,
        })
    }

    pub fn enqueue(
        &self,
        record: &DurableRecord,
    ) -> Result<PendingKafkaAppend, KafkaTransportError> {
        if record.stream.trim().is_empty() {
            return Err(KafkaTransportError::MissingField("stream"));
        }
        if record.partition_key.trim().is_empty() {
            return Err(KafkaTransportError::MissingField("partition_key"));
        }
        if record.event_id.is_empty() {
            return Err(KafkaTransportError::MissingField("event_id"));
        }
        let headers = OwnedHeaders::new().insert(Header {
            key: EVENT_ID_HEADER,
            value: Some(record.event_id.as_slice()),
        });
        let delivery = self
            .producer
            .send_result(
                FutureRecord::to(&record.stream)
                    .key(record.partition_key.as_bytes())
                    .payload(record.payload.as_slice())
                    .headers(headers),
            )
            .map_err(|(error, _)| KafkaTransportError::Kafka(error))?;
        Ok(PendingKafkaAppend {
            delivery,
            stream: record.stream.clone(),
            partition_key: record.partition_key.clone(),
        })
    }

    pub async fn append(
        &self,
        record: &DurableRecord,
    ) -> Result<AppendResult, KafkaTransportError> {
        self.enqueue(record)?.wait().await
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionalShadowTopics {
    pub raw_inputs: Vec<String>,
    pub canonical: String,
    pub quarantine: String,
}

impl TransactionalShadowTopics {
    pub fn validate(&self) -> Result<(), KafkaTransportError> {
        if self.raw_inputs.is_empty()
            || self.raw_inputs.iter().any(|topic| topic.trim().is_empty())
            || self.canonical.trim().is_empty()
            || self.quarantine.trim().is_empty()
        {
            return Err(KafkaTransportError::Configuration(
                "transactional shadow topics must not be empty".into(),
            ));
        }
        let mut unique = std::collections::HashSet::new();
        for topic in self
            .raw_inputs
            .iter()
            .chain([&self.canonical, &self.quarantine])
        {
            if !unique.insert(topic.as_str()) {
                return Err(KafkaTransportError::Configuration(
                    "transactional shadow topics must be isolated".into(),
                ));
            }
        }
        Ok(())
    }

    fn permits(&self, target: SinkTarget, stream: &str) -> bool {
        match target {
            SinkTarget::ShadowCanonical | SinkTarget::PrimaryCanonical => stream == self.canonical,
            SinkTarget::ShadowQuarantine | SinkTarget::PrimaryQuarantine => {
                stream == self.quarantine
            }
            SinkTarget::ShadowRaw
            | SinkTarget::PrimaryRaw
            | SinkTarget::CanaryCanonical
            | SinkTarget::PublicV2
            | SinkTarget::LegacyV1 => false,
        }
    }
}

pub struct TransactionalKafkaInput {
    pub record: DurableRecord,
    pub cursor: Cursor,
}

pub struct TransactionalKafkaOutput {
    pub record: DurableRecord,
    pub publication: PublicationContext,
    pub raw_provider_envelope: Option<Vec<u8>>,
}

fn transactional_output_headers(
    event_id: &[u8],
    raw_provider_envelope: Option<&[u8]>,
) -> OwnedHeaders {
    let mut headers = OwnedHeaders::new().insert(Header {
        key: EVENT_ID_HEADER,
        value: Some(event_id),
    });
    if let Some(raw) = raw_provider_envelope {
        headers = headers.insert(Header {
            key: RAW_ENVELOPE_HEADER,
            value: Some(raw),
        });
    }
    headers
}

/// K3. Rebalance epoch latch shared by the bridge's consumer context and its
/// transactions.
///
/// The consumer uses the cooperative protocol, and since IDR-094 it polls batch
/// N+1 (and so serves rebalance callbacks) while batch N's transaction is in
/// flight. The group coordinator validates member id and generation on
/// `send_offsets_to_transaction`, not per-partition ownership, so a batch whose
/// partitions were revoked meanwhile could still commit their offsets with the
/// newer group metadata while the new owner already consumes them from the last
/// committed offset. The latch is bumped by the rebalance callbacks and checked
/// by the transaction; it never waits on anything, so a rebalance callback can
/// never block on an in-flight commit.
#[derive(Debug, Default)]
pub struct RebalanceEpoch(AtomicU64);

impl RebalanceEpoch {
    pub fn current(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }

    fn bump(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    /// `Ok` only if no invalidating rebalance happened since `observed` was read.
    pub fn ensure_unchanged(&self, observed: u64) -> Result<(), KafkaTransportError> {
        let current = self.current();
        if current == observed {
            Ok(())
        } else {
            Err(KafkaTransportError::Rebalanced { observed, current })
        }
    }
}

/// Which rebalance events can invalidate a batch already read. A revocation
/// (including partitions *lost* to a session timeout, which librdkafka reports
/// as a revoke) or a rebalance error can take away a partition the batch read
/// from. An assignment only adds partitions, so the batch's inputs stay owned;
/// cooperative rebalances also deliver empty assign/revoke lists on every
/// group change, and aborting on those would turn each restart of one core into
/// aborts on the other cores.
fn rebalance_invalidates_batch(rebalance: &Rebalance<'_>) -> bool {
    match rebalance {
        Rebalance::Assign(_) => false,
        Rebalance::Revoke(partitions) => partitions.count() > 0,
        Rebalance::Error(_) => true,
    }
}

/// Consumer context of [`TransactionalKafkaBridge`]. Keeps rdkafka's default
/// rebalance handling (incremental assign/unassign), logging and statistics;
/// it only bumps the [`RebalanceEpoch`] before and after an invalidating
/// rebalance. Both callbacks are wait-free.
pub struct RebalanceEpochContext {
    epoch: Arc<RebalanceEpoch>,
}

impl ClientContext for RebalanceEpochContext {}

impl ConsumerContext for RebalanceEpochContext {
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if rebalance_invalidates_batch(rebalance) {
            self.epoch.bump();
        }
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if rebalance_invalidates_batch(rebalance) {
            self.epoch.bump();
        }
    }
}

type BridgeConsumer = StreamConsumer<RebalanceEpochContext>;

/// K3. Send the batch's raw offsets and commit, unless the consumer processed
/// an invalidating rebalance since the batch's first input was polled. Checked
/// before send-offsets (no offsets are sent for a stale batch) and again after
/// it returns: a revocation served in between means the new owner may have
/// fetched the old committed offset before our offsets reached the
/// coordinator, so committing could duplicate output. Either failure is
/// returned before commit, so the caller aborts the transaction.
fn commit_offsets_unless_rebalanced(
    epoch: &RebalanceEpoch,
    observed: u64,
    send_offsets: impl FnOnce() -> Result<(), KafkaTransportError>,
    commit: impl FnOnce() -> Result<(), KafkaTransportError>,
) -> Result<(), KafkaTransportError> {
    epoch.ensure_unchanged(observed)?;
    send_offsets()?;
    epoch.ensure_unchanged(observed)?;
    commit()
}

/// Kafka consume-transform-produce boundary. Output records and the next raw
/// consumer offset commit atomically, so a process crash cannot acknowledge raw
/// input without its canonical/quarantine result or duplicate committed output.
/// The historic type name remains for source compatibility; the authority fence
/// binds these private streams to either shadow or primary publication targets.
pub struct TransactionalKafkaBridge {
    producer: FutureProducer,
    consumer: Arc<BridgeConsumer>,
    rebalance_epoch: Arc<RebalanceEpoch>,
    fence: Arc<tokio::sync::Mutex<AuthorityFence>>,
    topics: TransactionalShadowTopics,
    request_timeout: Duration,
}

/// A transaction whose inputs and outputs passed topic and authority fencing
/// but which has not begun. Awaiting it runs begin, send, send-offsets and
/// commit (or abort) once; dropping it unawaited begins nothing.
pub type PreparedKafkaTransaction =
    BoxFuture<'static, Result<Vec<AppendResult>, KafkaTransportError>>;

type NextRawOffsets = std::collections::BTreeMap<(String, i32), i64>;

/// IDR-094. librdkafka's transactional calls block their calling thread for up
/// to `request_timeout` (begin, send-offsets, commit, abort). Run them on the
/// blocking pool so the async runtime that receives raw input and serves the
/// admission listener is never parked behind a broker round trip.
async fn blocking_kafka_call<T: Send + 'static>(
    call: impl FnOnce() -> Result<T, KafkaTransportError> + Send + 'static,
) -> Result<T, KafkaTransportError> {
    match tokio::task::spawn_blocking(call).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(KafkaTransportError::Configuration(format!(
            "blocking Kafka transaction call was cancelled: {error}"
        ))),
    }
}

/// The one transaction state machine: a failed begin returns its error and
/// aborts nothing; a failure after begin aborts, and an abort failure replaces
/// the original error (the producer is then unusable and must be rebuilt).
async fn run_kafka_transaction<T, Begin, Body, Abort>(
    begin: Begin,
    body: Body,
    abort: impl FnOnce() -> Abort,
) -> Result<T, KafkaTransportError>
where
    Begin: Future<Output = Result<(), KafkaTransportError>>,
    Body: Future<Output = Result<T, KafkaTransportError>>,
    Abort: Future<Output = Result<(), KafkaTransportError>>,
{
    begin.await?;
    match body.await {
        Ok(value) => Ok(value),
        Err(error) => {
            abort().await?;
            Err(error)
        }
    }
}

async fn execute_kafka_transaction(
    producer: FutureProducer,
    consumer: Arc<BridgeConsumer>,
    rebalance: (Arc<RebalanceEpoch>, u64),
    request_timeout: Duration,
    next_offsets: NextRawOffsets,
    outputs: &[TransactionalKafkaOutput],
) -> Result<Vec<AppendResult>, KafkaTransportError> {
    let begin_producer = producer.clone();
    let abort_producer = producer.clone();
    let body_producer = &producer;
    run_kafka_transaction(
        blocking_kafka_call(move || {
            begin_producer
                .begin_transaction()
                .map_err(KafkaTransportError::Kafka)
        }),
        async move {
            let deliveries = outputs.iter().map(|output| async move {
                let headers = transactional_output_headers(
                    output.record.event_id.as_slice(),
                    output.raw_provider_envelope.as_deref(),
                );
                let delivery = body_producer
                    .send(
                        FutureRecord::to(&output.record.stream)
                            .key(output.record.partition_key.as_bytes())
                            .payload(output.record.payload.as_slice())
                            .headers(headers),
                        Timeout::After(request_timeout),
                    )
                    .await
                    .map_err(|(error, _)| KafkaTransportError::Delivery(error))?;
                let offset = u64::try_from(delivery.offset)
                    .map_err(|_| KafkaTransportError::InvalidOffset(delivery.offset))?;
                Ok::<_, KafkaTransportError>(AppendResult {
                    cursor: Cursor {
                        stream: output.record.stream.clone(),
                        transport_partition: delivery.partition,
                        partition_key: output.record.partition_key.clone(),
                        offset,
                    },
                    duplicate: false,
                })
            });
            let accepted = try_join_all(deliveries).await?;
            let commit_producer = body_producer.clone();
            let (epoch, observed) = rebalance;
            blocking_kafka_call(move || {
                let mut offsets = TopicPartitionList::new();
                for ((topic, partition), offset) in next_offsets {
                    offsets.add_partition_offset(&topic, partition, Offset::Offset(offset))?;
                }
                commit_offsets_unless_rebalanced(
                    &epoch,
                    observed,
                    || {
                        // Read at send-offsets time, as before IDR-094: the
                        // group generation in this metadata is what fences a
                        // zombie member. Ownership is the epoch check's job.
                        let group = consumer.group_metadata().ok_or_else(|| {
                            KafkaTransportError::Configuration(
                                "consumer group metadata is unavailable".into(),
                            )
                        })?;
                        commit_producer.send_offsets_to_transaction(
                            &offsets,
                            &group,
                            Timeout::After(request_timeout),
                        )?;
                        Ok(())
                    },
                    || {
                        commit_producer.commit_transaction(Timeout::After(request_timeout))?;
                        Ok(())
                    },
                )
            })
            .await?;
            Ok::<_, KafkaTransportError>(accepted)
        },
        move || {
            blocking_kafka_call(move || {
                abort_producer
                    .abort_transaction(Timeout::After(request_timeout))
                    .map_err(KafkaTransportError::Kafka)
            })
        },
    )
    .await
}

/// Transactional producer settings of [`TransactionalKafkaBridge`]: the
/// bounded client defaults, then the larger transactional queue bound.
fn transactional_producer_config(
    config: &KafkaTransportConfig,
    transactional_id: &str,
) -> Result<ClientConfig, KafkaTransportError> {
    let mut producer_config = config.client_config()?;
    producer_config
        .set("transactional.id", transactional_id)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("max.in.flight.requests.per.connection", "5")
        .set("retries", "2147483647")
        .set("compression.type", "zstd")
        .set(
            "transaction.timeout.ms",
            config.request_timeout.as_millis().to_string(),
        )
        .set(
            "delivery.timeout.ms",
            config.request_timeout.as_millis().to_string(),
        );
    producer_config
        .set(
            "queue.buffering.max.kbytes",
            TRANSACTIONAL_PRODUCER_QUEUE_MAX_KBYTES,
        )
        .set(
            "queue.buffering.max.messages",
            TRANSACTIONAL_PRODUCER_QUEUE_MAX_MESSAGES,
        );
    Ok(producer_config)
}

impl TransactionalKafkaBridge {
    pub fn new(
        config: &KafkaTransportConfig,
        topics: TransactionalShadowTopics,
        transactional_id: &str,
    ) -> Result<Self, KafkaTransportError> {
        topics.validate()?;
        if transactional_id.trim().is_empty() {
            return Err(KafkaTransportError::Configuration(
                "transactional.id must not be empty".into(),
            ));
        }
        let mut consumer_config = config.client_config()?;
        config.configure_group_consumer(&mut consumer_config);
        let rebalance_epoch = Arc::new(RebalanceEpoch::default());
        let consumer: BridgeConsumer =
            consumer_config.create_with_context(RebalanceEpochContext {
                epoch: Arc::clone(&rebalance_epoch),
            })?;
        let raw_topics: Vec<&str> = topics.raw_inputs.iter().map(String::as_str).collect();
        consumer.subscribe(&raw_topics)?;

        let producer_config = transactional_producer_config(config, transactional_id)?;
        let producer: FutureProducer = producer_config.create()?;
        producer.init_transactions(Timeout::After(config.request_timeout))?;
        Ok(Self {
            producer,
            consumer: Arc::new(consumer),
            rebalance_epoch,
            fence: Arc::new(tokio::sync::Mutex::new(AuthorityFence::default())),
            topics,
            request_timeout: config.request_timeout,
        })
    }

    pub async fn apply_authority(
        &self,
        record: AuthorityRecord,
    ) -> Result<(), KafkaTransportError> {
        self.fence
            .lock()
            .await
            .apply(record)
            .map_err(KafkaTransportError::Fencing)
    }

    pub fn unsubscribe(&self) {
        self.consumer.unsubscribe();
    }

    /// K3. Read before a batch's first input is polled and pass it to
    /// [`Self::prepare_commit`]; any revocation served after this read makes
    /// that batch's transaction abort with [`KafkaTransportError::Rebalanced`].
    pub fn rebalance_epoch(&self) -> u64 {
        self.rebalance_epoch.current()
    }

    pub async fn next(&self) -> Result<TransactionalKafkaInput, KafkaTransportError> {
        let message = self.consumer.recv().await?;
        let payload = message
            .payload()
            .ok_or(KafkaTransportError::MissingField("payload"))?
            .to_vec();
        let key = message
            .key()
            .ok_or(KafkaTransportError::MissingField("partition_key"))?;
        let partition_key = std::str::from_utf8(key)
            .map_err(|_| KafkaTransportError::InvalidUtf8("partition_key"))?
            .to_owned();
        let event_id = message
            .headers()
            .and_then(|headers| {
                headers
                    .iter()
                    .find(|header| header.key == EVENT_ID_HEADER)
                    .and_then(|header| header.value.map(ToOwned::to_owned))
            })
            .ok_or(KafkaTransportError::MissingField("event_id header"))?;
        let offset = u64::try_from(message.offset())
            .map_err(|_| KafkaTransportError::InvalidOffset(message.offset()))?;
        let cursor = Cursor {
            stream: message.topic().to_owned(),
            transport_partition: message.partition(),
            partition_key: partition_key.clone(),
            offset,
        };
        let accepted_at_ns = message.timestamp().to_millis().unwrap_or_default() * 1_000_000;
        Ok(TransactionalKafkaInput {
            record: DurableRecord {
                stream: message.topic().to_owned(),
                partition_key,
                event_id,
                payload,
                accepted_at_ns,
            },
            cursor,
        })
    }

    /// Validate one batch against topics and the authority fence, and compute
    /// the raw offsets it acknowledges. Returns the fence guard so the checked
    /// authority cannot change before the transaction ends, as before IDR-094.
    async fn prepare(
        &self,
        inputs: &[TransactionalKafkaInput],
        outputs: &[TransactionalKafkaOutput],
    ) -> Result<(tokio::sync::OwnedMutexGuard<AuthorityFence>, NextRawOffsets), KafkaTransportError>
    {
        if inputs.is_empty() {
            return Err(KafkaTransportError::Configuration(
                "transaction input batch must not be empty".into(),
            ));
        }
        if inputs
            .iter()
            .any(|input| !self.topics.raw_inputs.contains(&input.cursor.stream))
        {
            return Err(KafkaTransportError::Fencing(
                "transaction input is outside configured raw topics".into(),
            ));
        }
        let mut fence = Arc::clone(&self.fence).lock_owned().await;
        for output in outputs {
            if !self
                .topics
                .permits(output.publication.target, &output.record.stream)
            {
                return Err(KafkaTransportError::Fencing(
                    "transaction output target does not match shadow topic".into(),
                ));
            }
            fence
                .permits(&output.publication)
                .map_err(KafkaTransportError::Fencing)?;
        }
        let mut next_offsets = NextRawOffsets::new();
        for input in inputs {
            let next_offset = input
                .cursor
                .offset
                .checked_add(1)
                .and_then(|value| i64::try_from(value).ok())
                .ok_or(KafkaTransportError::InvalidOffset(i64::MAX))?;
            next_offsets
                .entry((
                    input.cursor.stream.clone(),
                    input.cursor.transport_partition,
                ))
                .and_modify(|current| *current = (*current).max(next_offset))
                .or_insert(next_offset);
        }
        Ok((fence, next_offsets))
    }

    /// Stop-and-wait commit. The rebalance epoch is read here, after `inputs`
    /// were polled, so it only guards against a rebalance during the
    /// transaction itself; pipelined callers use [`Self::prepare_commit`].
    pub async fn commit(
        &self,
        inputs: &[TransactionalKafkaInput],
        outputs: &[TransactionalKafkaOutput],
    ) -> Result<Vec<AppendResult>, KafkaTransportError> {
        let observed = self.rebalance_epoch();
        let (_fence, next_offsets) = self.prepare(inputs, outputs).await?;
        execute_kafka_transaction(
            self.producer.clone(),
            Arc::clone(&self.consumer),
            (Arc::clone(&self.rebalance_epoch), observed),
            self.request_timeout,
            next_offsets,
            outputs,
        )
        .await
    }

    /// IDR-094. Fence-check one batch now and return its transaction as an
    /// owned future, so the caller can run it on another task while it reads
    /// and normalizes the next batch. Only raw cursors are kept from `inputs`.
    /// The caller must not start a second prepared transaction before this one
    /// resolves: one transactional producer has at most one open transaction
    /// ([`TransactionPipeline`] enforces that).
    ///
    /// K3. `rebalance_epoch` is [`Self::rebalance_epoch`] as read before the
    /// batch's first input was polled. If a revocation was served since, the
    /// transaction aborts with the retryable
    /// [`KafkaTransportError::Rebalanced`] instead of committing.
    pub async fn prepare_commit(
        &self,
        inputs: &[TransactionalKafkaInput],
        outputs: Vec<TransactionalKafkaOutput>,
        rebalance_epoch: u64,
    ) -> Result<PreparedKafkaTransaction, KafkaTransportError> {
        let (fence, next_offsets) = self.prepare(inputs, &outputs).await?;
        let producer = self.producer.clone();
        let consumer = Arc::clone(&self.consumer);
        let rebalance = (Arc::clone(&self.rebalance_epoch), rebalance_epoch);
        let request_timeout = self.request_timeout;
        Ok(Box::pin(async move {
            let _fence = fence;
            execute_kafka_transaction(
                producer,
                consumer,
                rebalance,
                request_timeout,
                next_offsets,
                &outputs,
            )
            .await
        }))
    }
}

/// One finished pipelined transaction.
#[derive(Debug)]
pub struct CommittedTransaction<T> {
    pub value: T,
    /// Raw input records the transaction acknowledged.
    pub inputs: u64,
    /// Wall time of the transaction itself (begin through commit).
    pub commit_ns: i64,
    /// How long the caller was blocked waiting for it; `commit_ns - wait_ns`
    /// is the commit time hidden behind the caller's next batch.
    pub wait_ns: i64,
}

struct InFlightTransaction<T> {
    handle: tokio::task::JoinHandle<(Result<T, KafkaTransportError>, i64)>,
    inputs: u64,
}

/// IDR-094. Depth-one commit pipeline: at most one transaction is in flight,
/// and a transaction is started only after the previous one committed, so
/// batches commit in submission order and a failed batch stops every later
/// batch from beginning. The caller reads and normalizes batch N+1 while batch
/// N commits on a separate task.
///
/// A pipeline belongs to one generation and is terminal after a failure: once
/// a transaction failed, [`Self::submit`] refuses every later batch instead of
/// starting one, so recovery always goes through a new generation (and a new
/// pipeline) that resumes from committed offsets.
///
/// Dropping the pipeline does not cancel a transaction already started: its
/// task (and any `spawn_blocking` broker call inside it) runs to completion
/// detached, and nobody learns whether it committed. Callers must call
/// [`Self::complete`] before dropping it.
pub struct TransactionPipeline<T> {
    in_flight: Option<InFlightTransaction<T>>,
    failed: Option<String>,
}

impl<T: Send + 'static> Default for TransactionPipeline<T> {
    fn default() -> Self {
        Self {
            in_flight: None,
            failed: None,
        }
    }
}

impl<T: Send + 'static> TransactionPipeline<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    /// Raw inputs submitted but not yet known committed.
    pub fn in_flight_inputs(&self) -> u64 {
        self.in_flight.as_ref().map_or(0, |value| value.inputs)
    }

    /// Wait for the in-flight transaction, if any. Cancel-safe: dropping this
    /// future before it resolves leaves the transaction in flight. A failure
    /// is returned once and makes the pipeline terminal; with nothing in
    /// flight this returns `Ok(None)`.
    pub async fn complete(
        &mut self,
    ) -> Result<Option<CommittedTransaction<T>>, KafkaTransportError> {
        let Some(in_flight) = self.in_flight.as_mut() else {
            return Ok(None);
        };
        let wait_started = tokio::time::Instant::now();
        let joined = (&mut in_flight.handle).await;
        let wait_ns = elapsed_ns(wait_started);
        let inputs = in_flight.inputs;
        self.in_flight = None;
        let (result, commit_ns) = match joined {
            Ok(value) => value,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => {
                let error = KafkaTransportError::Configuration(format!(
                    "transaction task was cancelled: {error}"
                ));
                self.failed = Some(error.to_string());
                return Err(error);
            }
        };
        let value = result.inspect_err(|error| self.failed = Some(error.to_string()))?;
        Ok(Some(CommittedTransaction {
            value,
            inputs,
            commit_ns,
            wait_ns: wait_ns.min(commit_ns),
        }))
    }

    /// Wait for the previous transaction, then start `transaction` on its own
    /// task. If the previous transaction failed, `transaction` is dropped
    /// without being polled - it never begins - and that failure is returned.
    /// After any failure (returned here or by [`Self::complete`]) every later
    /// submit is refused with a non-retryable error and never begins either.
    pub async fn submit<F>(
        &mut self,
        inputs: u64,
        transaction: F,
    ) -> Result<Option<CommittedTransaction<T>>, KafkaTransportError>
    where
        F: Future<Output = Result<T, KafkaTransportError>> + Send + 'static,
    {
        let previous = self.complete().await?;
        if let Some(failure) = &self.failed {
            return Err(KafkaTransportError::Configuration(format!(
                "transaction pipeline is terminal after a failed transaction: {failure}"
            )));
        }
        let handle = tokio::spawn(async move {
            let started = tokio::time::Instant::now();
            let result = transaction.await;
            (result, elapsed_ns(started))
        });
        self.in_flight = Some(InFlightTransaction { handle, inputs });
        Ok(previous)
    }
}

fn elapsed_ns(started: tokio::time::Instant) -> i64 {
    started.elapsed().as_nanos().min(i64::MAX as u128) as i64
}

pub struct KafkaEventSource {
    consumer: StreamConsumer,
}

impl KafkaEventSource {
    pub fn new(
        config: &KafkaTransportConfig,
        topics: &[&str],
    ) -> Result<Self, KafkaTransportError> {
        Self::with_offset_reset(config, topics, "earliest")
    }

    /// Control notifications are session-fenced and reissued while unresolved.
    /// A new control group starts live; an existing group resumes its checkpoint.
    pub fn new_live_control(
        config: &KafkaTransportConfig,
        topics: &[&str],
    ) -> Result<Self, KafkaTransportError> {
        Self::with_offset_reset(config, topics, "latest")
    }

    fn with_offset_reset(
        config: &KafkaTransportConfig,
        topics: &[&str],
        reset: &str,
    ) -> Result<Self, KafkaTransportError> {
        if topics.is_empty() || topics.iter().any(|topic| topic.trim().is_empty()) {
            return Err(KafkaTransportError::Configuration(
                "at least one non-empty topic is required".into(),
            ));
        }
        let mut client = config.client_config()?;
        config.configure_group_consumer_with_offset_reset(&mut client, reset);
        let consumer: StreamConsumer = client.create()?;
        consumer.subscribe(topics)?;
        Ok(Self { consumer })
    }

    pub async fn next(&self) -> Result<(DurableRecord, Cursor), KafkaTransportError> {
        let message = self.consumer.recv().await?;
        let payload = message
            .payload()
            .ok_or(KafkaTransportError::MissingField("payload"))?
            .to_vec();
        let key = message
            .key()
            .ok_or(KafkaTransportError::MissingField("partition_key"))?;
        let partition_key = std::str::from_utf8(key)
            .map_err(|_| KafkaTransportError::InvalidUtf8("partition_key"))?
            .to_owned();
        let event_id = message
            .headers()
            .and_then(|headers| {
                headers
                    .iter()
                    .find(|header| header.key == EVENT_ID_HEADER)
                    .and_then(|header| header.value.map(ToOwned::to_owned))
            })
            .ok_or(KafkaTransportError::MissingField("event_id header"))?;
        let offset = u64::try_from(message.offset())
            .map_err(|_| KafkaTransportError::InvalidOffset(message.offset()))?;
        let cursor = Cursor {
            stream: message.topic().to_owned(),
            transport_partition: message.partition(),
            partition_key: partition_key.clone(),
            offset,
        };
        let accepted_at_ns = message.timestamp().to_millis().unwrap_or_default() * 1_000_000;
        // Store only in local consumer state. The caller explicitly commits
        // after its downstream projection has succeeded.
        self.consumer.store_offset_from_message(&message)?;
        Ok((
            DurableRecord {
                stream: message.topic().to_owned(),
                partition_key,
                event_id,
                payload,
                accepted_at_ns,
            },
            cursor,
        ))
    }

    pub fn checkpoint(&self) -> Result<(), KafkaTransportError> {
        self.consumer.commit_consumer_state(CommitMode::Sync)?;
        Ok(())
    }

    /// Only for idempotent, periodically reissued control hints. Submission is
    /// not a durable acknowledgement and must never acknowledge market data.
    pub fn checkpoint_hint(&self) -> Result<(), KafkaTransportError> {
        self.consumer.commit_consumer_state(CommitMode::Async)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        configure_bounded_client_memory, transactional_output_headers, KafkaTlsConfig,
        KafkaTransportConfig, KafkaTransportError, Phase92SinkTopics, Phase9SinkTopics,
        ShutdownSignal, TransactionalShadowTopics, CONSUMER_GROUP_PROTOCOL,
        CONSUMER_HEARTBEAT_INTERVAL_MS, CONSUMER_SESSION_TIMEOUT_MS,
        COOPERATIVE_ASSIGNMENT_STRATEGY, EVENT_ID_HEADER, KAFKA_FETCH_MAX_BYTES,
        KAFKA_MAX_PARTITION_FETCH_BYTES, KAFKA_PRODUCER_QUEUE_MAX_MESSAGES, KAFKA_QUEUE_MAX_KBYTES,
        RAW_ENVELOPE_HEADER,
    };
    use super::{
        durable_producer_config, transactional_producer_config, ProducerQueueBounds,
        TRANSACTIONAL_PRODUCER_QUEUE_MAX_KBYTES, TRANSACTIONAL_PRODUCER_QUEUE_MAX_MESSAGES,
    };
    use qdl_core::transport::RetryClass;
    use qdl_venue_core::authority::SinkTarget;
    use rdkafka::config::ClientConfig;
    use rdkafka::message::Headers;
    use std::time::Duration;

    #[test]
    fn consumer_group_policy_is_cooperative_manual_and_dynamic() {
        let config = KafkaTransportConfig {
            bootstrap_servers: "kafka:9092".into(),
            client_id: "qdl-core-001".into(),
            group_id: "qdl-core-v1".into(),
            request_timeout: Duration::from_secs(5),
            tls: KafkaTlsConfig {
                ca_location: "/not-read/ca".into(),
                certificate_location: "/not-read/cert".into(),
                key_location: "/not-read/key".into(),
                key_password: None,
            },
        };
        let mut client = ClientConfig::new();
        config.configure_group_consumer(&mut client);
        assert_eq!(client.get("group.id"), Some("qdl-core-v1"));
        assert_eq!(
            client.get("partition.assignment.strategy"),
            Some(COOPERATIVE_ASSIGNMENT_STRATEGY)
        );
        assert_eq!(client.get("group.protocol"), Some(CONSUMER_GROUP_PROTOCOL));
        assert_eq!(client.get("enable.auto.commit"), Some("false"));
        assert_eq!(client.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(client.get("isolation.level"), Some("read_committed"));
        // Dynamic membership is deliberate (architecture 28.6): a gracefully
        // stopped core must leave the group so N-1 redistribution is immediate.
        assert_eq!(client.get("group.instance.id"), None);
        assert_eq!(client.get("session.timeout.ms"), Some("10000"));
        assert_eq!(client.get("heartbeat.interval.ms"), Some("1000"));
    }

    #[test]
    fn group_session_bound_is_valid_for_the_broker_and_every_consumer_mode() {
        let session: u64 = CONSUMER_SESSION_TIMEOUT_MS.parse().unwrap();
        let heartbeat: u64 = CONSUMER_HEARTBEAT_INTERVAL_MS.parse().unwrap();
        // Broker defaults: group.min.session.timeout.ms 6000, max 1800000.
        assert!((6_000..=1_800_000).contains(&session));
        // librdkafka requires heartbeat < session; keep at least 3 missed beats.
        assert!(heartbeat * 3 <= session);
        // Below the default max.poll.interval.ms, which stays unchanged.
        assert!(session < 300_000);
        let config = KafkaTransportConfig {
            bootstrap_servers: "kafka:9092".into(),
            client_id: "qdl-core-002".into(),
            group_id: "qdl-core-v1".into(),
            request_timeout: Duration::from_secs(30),
            tls: KafkaTlsConfig {
                ca_location: "/not-read/ca".into(),
                certificate_location: "/not-read/cert".into(),
                key_location: "/not-read/key".into(),
                key_password: None,
            },
        };
        for fail_closed in [false, true] {
            let mut client = ClientConfig::new();
            if fail_closed {
                config.configure_group_consumer_fail_closed(&mut client);
            } else {
                config.configure_group_consumer(&mut client);
            }
            assert_eq!(
                client.get("session.timeout.ms"),
                Some(CONSUMER_SESSION_TIMEOUT_MS)
            );
            assert_eq!(
                client.get("heartbeat.interval.ms"),
                Some(CONSUMER_HEARTBEAT_INTERVAL_MS)
            );
            assert_eq!(client.get("group.instance.id"), None);
            assert_eq!(client.get("max.poll.interval.ms"), None);
        }
    }

    #[test]
    fn client_memory_policy_is_explicitly_bounded() {
        let mut client = ClientConfig::new();
        configure_bounded_client_memory(&mut client);
        assert_eq!(
            client.get("queued.max.messages.kbytes"),
            Some(KAFKA_QUEUE_MAX_KBYTES)
        );
        assert_eq!(client.get("fetch.max.bytes"), Some(KAFKA_FETCH_MAX_BYTES));
        assert_eq!(
            client.get("max.partition.fetch.bytes"),
            Some(KAFKA_MAX_PARTITION_FETCH_BYTES)
        );
        assert_eq!(
            client.get("queue.buffering.max.kbytes"),
            Some(KAFKA_QUEUE_MAX_KBYTES)
        );
        assert_eq!(
            client.get("queue.buffering.max.messages"),
            Some(KAFKA_PRODUCER_QUEUE_MAX_MESSAGES)
        );
    }

    fn config_with_tls_files(label: &str) -> (KafkaTransportConfig, std::path::PathBuf) {
        let directory =
            std::env::temp_dir().join(format!("qdl-kafka-config-{label}-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        for name in ["ca", "cert", "key"] {
            std::fs::write(directory.join(name), b"not read by config tests").unwrap();
        }
        let path = |name: &str| directory.join(name).to_string_lossy().into_owned();
        (
            KafkaTransportConfig {
                bootstrap_servers: "kafka:9092".into(),
                client_id: "qdl-config-test".into(),
                group_id: "qdl-config-test".into(),
                request_timeout: Duration::from_secs(30),
                tls: KafkaTlsConfig {
                    ca_location: path("ca"),
                    certificate_location: path("cert"),
                    key_location: path("key"),
                    key_password: None,
                },
            },
            directory,
        )
    }

    #[test]
    fn transactional_producer_queue_holds_a_2048_record_l2_batch() {
        let (config, directory) = config_with_tls_files("transactional");
        let producer = transactional_producer_config(&config, "core-001").unwrap();
        assert_eq!(producer.get("transactional.id"), Some("core-001"));
        assert_eq!(producer.get("enable.idempotence"), Some("true"));
        assert_eq!(producer.get("acks"), Some("all"));
        assert_eq!(producer.get("compression.type"), Some("zstd"));
        assert_eq!(producer.get("transaction.timeout.ms"), Some("30000"));
        assert_eq!(
            producer.get("queue.buffering.max.kbytes"),
            Some(TRANSACTIONAL_PRODUCER_QUEUE_MAX_KBYTES)
        );
        assert_eq!(
            producer.get("queue.buffering.max.messages"),
            Some(TRANSACTIONAL_PRODUCER_QUEUE_MAX_MESSAGES)
        );
        // 2048 records at 32 KiB (Binance depth p99 ~36 KiB canonical) fit.
        let kbytes: u64 = TRANSACTIONAL_PRODUCER_QUEUE_MAX_KBYTES.parse().unwrap();
        assert!(kbytes >= 2048 * 32);
        let messages: u64 = TRANSACTIONAL_PRODUCER_QUEUE_MAX_MESSAGES.parse().unwrap();
        assert!(messages >= 2048 * 16);
        // The consumer side keeps the shared bound.
        assert_eq!(
            producer.get("queued.max.messages.kbytes"),
            Some(KAFKA_QUEUE_MAX_KBYTES)
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn shared_producer_queue_keeps_the_aggregate_per_lane_bound() {
        let one = ProducerQueueBounds::scaled_default(1).unwrap();
        assert_eq!(
            one.max_messages,
            KAFKA_PRODUCER_QUEUE_MAX_MESSAGES.parse::<u64>().unwrap()
        );
        assert_eq!(
            one.max_kbytes,
            KAFKA_QUEUE_MAX_KBYTES.parse::<u64>().unwrap()
        );
        assert_eq!(ProducerQueueBounds::scaled_default(0).unwrap(), one);
        let twelve = ProducerQueueBounds::scaled_default(12).unwrap();
        assert_eq!(twelve.max_messages, 12 * one.max_messages);
        assert_eq!(twelve.max_kbytes, 12 * one.max_kbytes);
        assert!(ProducerQueueBounds::scaled_default(1_000_000).is_err());

        let (config, directory) = config_with_tls_files("durable");
        let default = durable_producer_config(&config, None).unwrap();
        assert_eq!(
            default.get("queue.buffering.max.kbytes"),
            Some(KAFKA_QUEUE_MAX_KBYTES)
        );
        let shared = durable_producer_config(&config, Some(twelve)).unwrap();
        assert_eq!(shared.get("queue.buffering.max.kbytes"), Some("98304"));
        assert_eq!(shared.get("queue.buffering.max.messages"), Some("49152"));
        for key in ["acks", "enable.idempotence", "compression.type", "retries"] {
            assert_eq!(shared.get(key), default.get(key), "{key}");
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn shutdown_signal_labels_are_stable_for_structured_logs() {
        assert_eq!(ShutdownSignal::Interrupt.as_str(), "SIGINT");
        assert_eq!(ShutdownSignal::Terminate.as_str(), "SIGTERM");
    }

    #[test]
    fn transactional_headers_preserve_private_raw_lineage() {
        let event_id = [7_u8; 16];
        let raw = b"raw-provider-envelope";
        let headers = transactional_output_headers(&event_id, Some(raw));
        let values = headers
            .iter()
            .map(|header| (header.key, header.value.unwrap_or_default().to_vec()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(values[EVENT_ID_HEADER], event_id);
        assert_eq!(values[RAW_ENVELOPE_HEADER], raw);
        let without_raw = transactional_output_headers(&event_id, None);
        assert_eq!(without_raw.count(), 1);
    }

    #[test]
    fn config_fails_closed_without_tls_files() {
        let config = KafkaTransportConfig {
            bootstrap_servers: "kafka:9092".into(),
            client_id: "phase8-test".into(),
            group_id: "phase8-test".into(),
            request_timeout: Duration::from_secs(5),
            tls: KafkaTlsConfig {
                ca_location: "/missing/ca".into(),
                certificate_location: "/missing/cert".into(),
                key_location: "/missing/key".into(),
                key_password: None,
            },
        };
        let error = config.validate().expect_err("missing TLS must fail");
        assert!(matches!(error, KafkaTransportError::Configuration(_)));
        assert_eq!(error.retry_class(), RetryClass::NonRetryable);
    }

    #[test]
    fn compacted_snapshot_timeout_is_retryable_without_weakening_config_errors() {
        let timeout = KafkaTransportError::SnapshotTimeout(
            "captured high watermarks were not reached".into(),
        );
        assert_eq!(timeout.retry_class(), RetryClass::Retryable);
        assert_eq!(
            KafkaTransportError::Configuration("invalid topic".into()).retry_class(),
            RetryClass::NonRetryable
        );
    }

    #[test]
    fn phase9_topics_are_unique_and_bind_target_to_stream() {
        let topics = Phase9SinkTopics {
            shadow_raw: "qdl.phase8.phase91.shadow.raw".into(),
            shadow_canonical: "qdl.phase8.phase91.shadow.canonical".into(),
            shadow_quarantine: "qdl.phase8.phase91.shadow.quarantine".into(),
            canary_canonical: "qdl.phase8.phase91.canary.canonical".into(),
        };
        topics.validate().unwrap();
        assert!(topics.permits(
            SinkTarget::CanaryCanonical,
            "qdl.phase8.phase91.canary.canonical"
        ));
        assert!(!topics.permits(SinkTarget::CanaryCanonical, "qdl.phase8.phase91.public"));
        assert!(!topics.permits(SinkTarget::PublicV2, &topics.canary_canonical));

        let duplicate = Phase9SinkTopics {
            shadow_raw: "same".into(),
            shadow_canonical: "same".into(),
            shadow_quarantine: "quarantine".into(),
            canary_canonical: "other".into(),
        };
        assert!(duplicate.validate().is_err());
    }

    #[test]
    fn phase92_topics_are_unique_and_bind_final_and_projector_targets() {
        let topics = Phase92SinkTopics {
            primary_canonical: "qdl.phase92.primary.canonical".into(),
            public_v2: "qdl.phase92.public.v2".into(),
            legacy_v1: "qdl.phase92.legacy.v1".into(),
        };
        topics.validate().unwrap();
        assert!(topics.permits(SinkTarget::PrimaryCanonical, &topics.primary_canonical));
        assert!(topics.permits(SinkTarget::PublicV2, &topics.public_v2));
        assert!(topics.permits(SinkTarget::LegacyV1, &topics.legacy_v1));
        assert!(!topics.permits(SinkTarget::PublicV2, &topics.legacy_v1));
        assert!(!topics.permits(SinkTarget::CanaryCanonical, &topics.primary_canonical));

        let duplicate = Phase92SinkTopics {
            primary_canonical: "same".into(),
            public_v2: "same".into(),
            legacy_v1: "other".into(),
        };
        assert!(duplicate.validate().is_err());
    }

    #[test]
    fn transactional_shadow_topics_are_isolated_and_target_bound() {
        let topics = TransactionalShadowTopics {
            raw_inputs: vec!["qdl.raw.binance".into(), "qdl.raw.okx".into()],
            canonical: "qdl.canonical.v2".into(),
            quarantine: "qdl.quarantine.v1".into(),
        };
        topics.validate().unwrap();
        assert!(topics.permits(SinkTarget::ShadowCanonical, &topics.canonical));
        assert!(topics.permits(SinkTarget::ShadowQuarantine, &topics.quarantine));
        assert!(topics.permits(SinkTarget::PrimaryCanonical, &topics.canonical));
        assert!(topics.permits(SinkTarget::PrimaryQuarantine, &topics.quarantine));
        assert!(!topics.permits(SinkTarget::PublicV2, &topics.canonical));
        assert!(!topics.permits(SinkTarget::PrimaryRaw, &topics.canonical));
        let duplicate = TransactionalShadowTopics {
            raw_inputs: vec!["same".into()],
            canonical: "same".into(),
            quarantine: "other".into(),
        };
        assert!(duplicate.validate().is_err());
    }

    #[test]
    fn zero_timeout_and_empty_identity_fail_closed() {
        let config = KafkaTransportConfig {
            bootstrap_servers: String::new(),
            client_id: String::new(),
            group_id: String::new(),
            request_timeout: Duration::ZERO,
            tls: KafkaTlsConfig {
                ca_location: String::new(),
                certificate_location: String::new(),
                key_location: String::new(),
                key_password: None,
            },
        };
        assert!(matches!(
            config.validate(),
            Err(KafkaTransportError::Configuration(_))
        ));
    }
}

#[cfg(test)]
mod control_broker_tests {
    use super::*;

    async fn send(producer: &FutureProducer, topic: &str, payload: &str) {
        producer
            .send(
                FutureRecord::to(topic)
                    .partition(0)
                    .key("test-control")
                    .payload(payload)
                    .headers(OwnedHeaders::new().insert(Header {
                        key: EVENT_ID_HEADER,
                        value: Some(payload.as_bytes()),
                    })),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .expect("isolated control delivery");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires explicitly provisioned isolated readiness Kafka and disposable TLS"]
    async fn live_control_committed_only_resume_and_cancel() {
        let bootstrap =
            std::env::var("QDL_CONTROL_TEST_BOOTSTRAP").expect("explicit isolated broker");
        assert_eq!(bootstrap, "qdl-readiness-kafka:9094");
        let root = std::env::var("QDL_CONTROL_TEST_TLS").expect("disposable TLS root");
        let topic = "readiness.control-test";
        let config = KafkaTransportConfig {
            bootstrap_servers: bootstrap,
            client_id: "readiness-control-test".into(),
            group_id: "readiness-control-test-group".into(),
            request_timeout: Duration::from_secs(10),
            tls: KafkaTlsConfig {
                ca_location: format!("{root}/ca.crt"),
                certificate_location: format!("{root}/client.crt"),
                key_location: format!("{root}/client.key"),
                key_password: None,
            },
        };
        let mut producer_config = config.client_config().unwrap();
        producer_config.set("transactional.id", "readiness-control-test-producer");
        let producer: FutureProducer = producer_config.create().unwrap();
        let timeout = Timeout::After(Duration::from_secs(10));
        producer.init_transactions(timeout).unwrap();
        producer.begin_transaction().unwrap();
        send(&producer, topic, "old-before-subscribe").await;
        producer.commit_transaction(timeout).unwrap();

        let source = KafkaEventSource::new_live_control(&config, &[topic]).unwrap();
        // position() may remain Invalid until the first delivered message. Prove
        // startup with a bounded fresh-marker handshake instead of assuming it.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "live startup handshake timeout"
            );
            producer.begin_transaction().unwrap();
            send(&producer, topic, "startup-handshake").await;
            producer.commit_transaction(timeout).unwrap();
            if let Ok(received) =
                tokio::time::timeout(Duration::from_millis(250), source.next()).await
            {
                assert_eq!(
                    received.unwrap().0.payload,
                    b"startup-handshake",
                    "new control group must skip pre-subscription history"
                );
                break;
            }
        }
        // Drain only explicitly generated startup markers, never data under test.
        while let Ok(received) =
            tokio::time::timeout(Duration::from_millis(250), source.next()).await
        {
            assert_eq!(received.unwrap().0.payload, b"startup-handshake");
        }
        producer.begin_transaction().unwrap();
        send(&producer, topic, "aborted-hint").await;
        producer.abort_transaction(timeout).unwrap();
        producer.begin_transaction().unwrap();
        send(&producer, topic, "committed-hint").await;
        producer.commit_transaction(timeout).unwrap();
        let (record, cursor) = tokio::time::timeout(Duration::from_secs(10), source.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.payload, b"committed-hint");
        source.checkpoint_hint().unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let committed = source.consumer.committed(timeout).unwrap();
            if committed.elements().iter().any(|p| {
                p.topic() == topic && p.offset() == Offset::Offset(cursor.offset as i64 + 1)
            }) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "hint checkpoint not durable yet"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        drop(source);
        // Published while the receiver is down: an existing group must resume,
        // not apply latest reset a second time and skip this hint.
        producer.begin_transaction().unwrap();
        send(&producer, topic, "resume-hint").await;
        producer.commit_transaction(timeout).unwrap();
        let resumed = KafkaEventSource::new_live_control(&config, &[topic]).unwrap();
        let (record, _) = tokio::time::timeout(Duration::from_secs(15), resumed.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.payload, b"resume-hint");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), resumed.next())
                .await
                .is_err()
        );
        producer.begin_transaction().unwrap();
        send(&producer, topic, "after-cancelled-receive").await;
        producer.commit_transaction(timeout).unwrap();
        let (record, _) = tokio::time::timeout(Duration::from_secs(10), resumed.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.payload, b"after-cancelled-receive");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), resumed.next())
                .await
                .is_err()
        );
    }
}

#[cfg(test)]
mod persisted_delivery_tests {
    use super::*;

    fn raw_record() -> DurableRecord {
        DurableRecord {
            stream: "raw-recovery-test".into(),
            partition_key: "key".into(),
            event_id: b"test-provenance-event".to_vec(),
            payload: b"test-only".to_vec(),
            accepted_at_ns: 1,
        }
    }

    #[tokio::test]
    async fn native_idempotent_duplicate_delivery_does_not_crash_or_invent_offset() {
        use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
        let producer: FutureProducer = ClientConfig::new()
            .set("test.mock.num.brokers", "1")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "3000")
            .create()
            .unwrap();
        let sink = KafkaDurableSink { producer };
        let cluster = sink.producer.client().mock_cluster().unwrap();
        cluster.create_topic("raw-recovery-test", 1, 1).unwrap();
        let first = sink.enqueue(&raw_record()).unwrap().wait().await.unwrap();
        assert_eq!(first.cursor.offset, 0);
        cluster.request_errors(
            RDKafkaApiKey::Produce,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_DUPLICATE_SEQUENCE_NUMBER],
        );
        assert!(matches!(
            sink.enqueue(&raw_record())
                .unwrap()
                .wait_persisted()
                .await
                .unwrap(),
            PersistedKafkaAppend::OffsetUnavailable { .. }
        ));
        // Cursor-returning APIs remain strict on the same native response.
        cluster.request_errors(
            RDKafkaApiKey::Produce,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_DUPLICATE_SEQUENCE_NUMBER],
        );
        assert!(matches!(
            sink.enqueue(&raw_record()).unwrap().wait().await,
            Err(KafkaTransportError::InvalidOffset(-1 | -1001))
        ));
    }

    #[tokio::test]
    async fn failed_native_delivery_never_becomes_persisted() {
        use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
        let producer: FutureProducer = ClientConfig::new()
            .set("test.mock.num.brokers", "1")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "3000")
            .create()
            .unwrap();
        let sink = KafkaDurableSink { producer };
        let cluster = sink.producer.client().mock_cluster().unwrap();
        cluster.create_topic("raw-recovery-test", 1, 1).unwrap();
        cluster.request_errors(
            RDKafkaApiKey::Produce,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
        );
        assert!(matches!(
            sink.enqueue(&raw_record()).unwrap().wait_persisted().await,
            Err(KafkaTransportError::Delivery(_))
        ));
    }

    #[test]
    fn successful_sentinel_has_no_fabricated_cursor() {
        assert!(matches!(
            persisted_append("raw".into(), "key".into(), 2, -1001).unwrap(),
            PersistedKafkaAppend::OffsetUnavailable { partition: 2, .. }
        ));
    }

    #[test]
    fn valid_offsets_including_zero_are_preserved() {
        for offset in [0, 1, 999999] {
            let PersistedKafkaAppend::Positioned(result) =
                persisted_append("raw".into(), "key".into(), 2, offset).unwrap()
            else {
                panic!("missing real cursor")
            };
            assert_eq!(result.cursor.offset, offset as u64);
            assert_eq!(result.cursor.transport_partition, 2);
        }
    }

    #[test]
    fn other_negative_offsets_and_missing_partition_are_not_success() {
        for offset in [-2, -1000, i64::MIN] {
            assert!(persisted_append("raw".into(), "key".into(), 2, offset).is_err());
        }
        assert!(persisted_append("raw".into(), "key".into(), -1, -1001).is_err());
    }
}

#[cfg(test)]
mod transaction_pipeline_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Journal {
        events: Mutex<Vec<(&'static str, u64)>>,
        open: AtomicUsize,
        max_open: AtomicUsize,
    }

    impl Journal {
        fn push(&self, event: &'static str, batch: u64) {
            self.events.lock().unwrap().push((event, batch));
        }
    }

    async fn fake_transaction(
        journal: Arc<Journal>,
        batch: u64,
        commit: Duration,
        fail: bool,
    ) -> Result<u64, KafkaTransportError> {
        let open = journal.open.fetch_add(1, Ordering::SeqCst) + 1;
        journal.max_open.fetch_max(open, Ordering::SeqCst);
        journal.push("begin", batch);
        tokio::time::sleep(commit).await;
        journal.push("end", batch);
        journal.open.fetch_sub(1, Ordering::SeqCst);
        if fail {
            Err(KafkaTransportError::SnapshotTimeout(format!(
                "batch {batch} commit"
            )))
        } else {
            Ok(batch)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pipeline_commits_batches_in_order_one_transaction_at_a_time() {
        let journal = Arc::new(Journal::default());
        let mut pipeline = TransactionPipeline::new();
        let mut committed = Vec::new();
        for batch in 0..5_u64 {
            if let Some(done) = pipeline
                .submit(
                    batch + 1,
                    fake_transaction(journal.clone(), batch, Duration::from_millis(10), false),
                )
                .await
                .unwrap()
            {
                committed.push((done.value, done.inputs));
            }
            assert_eq!(pipeline.in_flight_inputs(), batch + 1);
        }
        let last = pipeline.complete().await.unwrap().unwrap();
        committed.push((last.value, last.inputs));
        assert!(!pipeline.is_in_flight());
        assert_eq!(pipeline.complete().await.unwrap().map(|v| v.value), None);
        assert_eq!(committed, (0..5).map(|b| (b, b + 1)).collect::<Vec<_>>());
        assert_eq!(journal.max_open.load(Ordering::SeqCst), 1);
        let expected = (0..5)
            .flat_map(|batch| [("begin", batch), ("end", batch)])
            .collect::<Vec<_>>();
        assert_eq!(*journal.events.lock().unwrap(), expected);
    }

    #[tokio::test(start_paused = true)]
    async fn caller_work_overlaps_the_in_flight_commit() {
        // Five batches, 10 ms of caller work (read + normalize) and a 30 ms
        // commit each. Stop-and-wait costs 5 * 40 = 200 ms; the pipeline
        // hides each commit behind the next batch's work: 10 + 5 * 30 = 160.
        let journal = Arc::new(Journal::default());
        let mut pipeline = TransactionPipeline::new();
        let started = tokio::time::Instant::now();
        let mut hidden_ns = 0_i64;
        for batch in 0..5_u64 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if let Some(done) = pipeline.complete().await.unwrap() {
                hidden_ns += done.commit_ns - done.wait_ns;
            }
            pipeline
                .submit(
                    1,
                    fake_transaction(journal.clone(), batch, Duration::from_millis(30), false),
                )
                .await
                .unwrap();
        }
        pipeline.complete().await.unwrap().unwrap();
        assert_eq!(started.elapsed(), Duration::from_millis(160));
        assert_eq!(hidden_ns, 4 * 10_000_000);
        assert_eq!(journal.max_open.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_commit_fails_closed_and_never_begins_the_next_batch() {
        let journal = Arc::new(Journal::default());
        let mut pipeline = TransactionPipeline::new();
        pipeline
            .submit(
                3,
                fake_transaction(journal.clone(), 0, Duration::from_millis(5), true),
            )
            .await
            .unwrap();
        let next_polled = Arc::new(AtomicBool::new(false));
        let flag = next_polled.clone();
        let error = pipeline
            .submit(4, async move {
                flag.store(true, Ordering::SeqCst);
                Ok(1_u64)
            })
            .await
            .unwrap_err();
        // The same typed error reaches the runtime, so its generation
        // retry/exit classification is unchanged.
        assert!(matches!(error, KafkaTransportError::SnapshotTimeout(_)));
        assert_eq!(error.retry_class(), RetryClass::Retryable);
        assert!(!pipeline.is_in_flight());
        assert_eq!(pipeline.in_flight_inputs(), 0);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!next_polled.load(Ordering::SeqCst));
        assert_eq!(
            *journal.events.lock().unwrap(),
            vec![("begin", 0), ("end", 0)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn complete_is_cancel_safe() {
        let journal = Arc::new(Journal::default());
        let mut pipeline = TransactionPipeline::new();
        pipeline
            .submit(
                2,
                fake_transaction(journal, 7, Duration::from_millis(20), false),
            )
            .await
            .unwrap();
        tokio::select! {
            biased;
            _ = pipeline.complete() => panic!("commit cannot finish in 5 ms"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
        assert!(pipeline.is_in_flight());
        let done = pipeline.complete().await.unwrap().unwrap();
        assert_eq!((done.value, done.inputs), (7, 2));
        assert_eq!(done.commit_ns, 20_000_000);
    }

    #[tokio::test]
    #[should_panic(expected = "transaction panicked")]
    async fn transaction_panic_is_not_swallowed() {
        let mut pipeline = TransactionPipeline::<u64>::new();
        pipeline
            .submit(1, async { panic!("transaction panicked") })
            .await
            .unwrap();
        let _ = pipeline.complete().await;
    }

    #[derive(Default)]
    struct Calls {
        begin: AtomicUsize,
        body: AtomicUsize,
        abort: AtomicUsize,
    }

    async fn transaction(
        calls: &Calls,
        begin: Result<(), KafkaTransportError>,
        body: Result<u64, KafkaTransportError>,
        abort: Result<(), KafkaTransportError>,
    ) -> Result<u64, KafkaTransportError> {
        run_kafka_transaction(
            async {
                calls.begin.fetch_add(1, Ordering::SeqCst);
                begin
            },
            async {
                calls.body.fetch_add(1, Ordering::SeqCst);
                body
            },
            || async {
                calls.abort.fetch_add(1, Ordering::SeqCst);
                abort
            },
        )
        .await
    }

    fn counts(calls: &Calls) -> (usize, usize, usize) {
        (
            calls.begin.load(Ordering::SeqCst),
            calls.body.load(Ordering::SeqCst),
            calls.abort.load(Ordering::SeqCst),
        )
    }

    #[tokio::test]
    async fn transaction_state_machine_matches_pre_pipeline_semantics() {
        let calls = Calls::default();
        assert_eq!(transaction(&calls, Ok(()), Ok(9), Ok(())).await.unwrap(), 9);
        assert_eq!(counts(&calls), (1, 1, 0));

        let calls = Calls::default();
        let error = transaction(
            &calls,
            Err(KafkaTransportError::Fencing("begin".into())),
            Ok(9),
            Ok(()),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, KafkaTransportError::Fencing(_)));
        assert_eq!(counts(&calls), (1, 0, 0), "failed begin aborts nothing");

        let calls = Calls::default();
        let error = transaction(
            &calls,
            Ok(()),
            Err(KafkaTransportError::SnapshotTimeout("send".into())),
            Ok(()),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, KafkaTransportError::SnapshotTimeout(_)));
        assert_eq!(counts(&calls), (1, 1, 1), "failed body aborts once");

        let calls = Calls::default();
        let error = transaction(
            &calls,
            Ok(()),
            Err(KafkaTransportError::SnapshotTimeout("commit".into())),
            Err(KafkaTransportError::InvalidOffset(-1)),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, KafkaTransportError::InvalidOffset(-1)),
            "a failed abort replaces the original error"
        );
        assert_eq!(counts(&calls), (1, 1, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn pipeline_is_terminal_after_a_failed_complete() {
        let journal = Arc::new(Journal::default());
        let mut pipeline = TransactionPipeline::new();
        pipeline
            .submit(
                2,
                fake_transaction(journal.clone(), 0, Duration::from_millis(5), true),
            )
            .await
            .unwrap();
        let error = pipeline.complete().await.unwrap_err();
        assert_eq!(error.retry_class(), RetryClass::Retryable);
        // Nothing is in flight: the failure is not reported twice ...
        assert!(pipeline.complete().await.unwrap().is_none());
        // ... but no later batch can begin in this generation.
        let next_polled = Arc::new(AtomicBool::new(false));
        let flag = next_polled.clone();
        let refused = pipeline
            .submit(1, async move {
                flag.store(true, Ordering::SeqCst);
                Ok(1_u64)
            })
            .await
            .unwrap_err();
        assert!(matches!(refused, KafkaTransportError::Configuration(_)));
        assert_eq!(refused.retry_class(), RetryClass::NonRetryable);
        assert!(refused.to_string().contains("terminal"));
        assert!(!pipeline.is_in_flight());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!next_polled.load(Ordering::SeqCst));
        assert_eq!(
            *journal.events.lock().unwrap(),
            vec![("begin", 0), ("end", 0)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pipeline_is_terminal_after_a_failed_submit() {
        let mut pipeline = TransactionPipeline::new();
        pipeline
            .submit(1, async {
                Err::<u64, _>(KafkaTransportError::SnapshotTimeout("commit".into()))
            })
            .await
            .unwrap();
        assert!(pipeline.submit(1, async { Ok(2_u64) }).await.is_err());
        let next_polled = Arc::new(AtomicBool::new(false));
        let flag = next_polled.clone();
        assert!(pipeline
            .submit(1, async move {
                flag.store(true, Ordering::SeqCst);
                Ok(3_u64)
            })
            .await
            .is_err());
        assert!(!pipeline.is_in_flight());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!next_polled.load(Ordering::SeqCst));
    }

    fn partitions(count: i32) -> TopicPartitionList {
        let mut list = TopicPartitionList::new();
        for partition in 0..count {
            list.add_partition("raw", partition);
        }
        list
    }

    #[test]
    fn only_revocations_and_rebalance_errors_invalidate_a_batch() {
        let one = partitions(1);
        let none = partitions(0);
        assert!(rebalance_invalidates_batch(&Rebalance::Revoke(&one)));
        assert!(rebalance_invalidates_batch(&Rebalance::Error(
            KafkaError::Rebalance(rdkafka::error::RDKafkaErrorCode::IllegalGeneration)
        )));
        assert!(!rebalance_invalidates_batch(&Rebalance::Revoke(&none)));
        assert!(!rebalance_invalidates_batch(&Rebalance::Assign(&one)));
        assert!(!rebalance_invalidates_batch(&Rebalance::Assign(&none)));
    }

    #[test]
    fn consumer_context_bumps_the_epoch_without_a_broker() {
        // No bootstrap servers: librdkafka creates the client without
        // connecting, which is enough to drive the real callbacks.
        let epoch = Arc::new(RebalanceEpoch::default());
        let consumer: BaseConsumer<RebalanceEpochContext> = ClientConfig::new()
            .set("group.id", "k3-unit")
            .set("log_level", "0")
            .create_with_context(RebalanceEpochContext {
                epoch: Arc::clone(&epoch),
            })
            .unwrap();
        let context = consumer.context();
        let observed = epoch.current();
        context.pre_rebalance(&consumer, &Rebalance::Assign(&partitions(3)));
        context.post_rebalance(&consumer, &Rebalance::Assign(&partitions(3)));
        epoch.ensure_unchanged(observed).unwrap();
        let revoked = partitions(2);
        context.pre_rebalance(&consumer, &Rebalance::Revoke(&revoked));
        assert!(matches!(
            epoch.ensure_unchanged(observed),
            Err(KafkaTransportError::Rebalanced { .. })
        ));
        context.post_rebalance(&consumer, &Rebalance::Revoke(&revoked));
        assert_eq!(epoch.current(), observed + 2);
    }

    #[derive(Default)]
    struct OffsetCalls {
        send: AtomicUsize,
        commit: AtomicUsize,
    }

    #[test]
    fn unchanged_epoch_commits() {
        let epoch = RebalanceEpoch::default();
        let calls = OffsetCalls::default();
        commit_offsets_unless_rebalanced(
            &epoch,
            epoch.current(),
            || {
                calls.send.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            || {
                calls.commit.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(calls.send.load(Ordering::SeqCst), 1);
        assert_eq!(calls.commit.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn rebalance_during_collection_sends_no_offsets() {
        let epoch = RebalanceEpoch::default();
        let observed = epoch.current();
        // Revocation served while the batch was still being read.
        epoch.bump();
        let calls = OffsetCalls::default();
        let error = commit_offsets_unless_rebalanced(
            &epoch,
            observed,
            || {
                calls.send.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            || {
                calls.commit.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            KafkaTransportError::Rebalanced {
                observed: 0,
                current: 1
            }
        ));
        assert_eq!(error.retry_class(), RetryClass::Retryable);
        assert_eq!(calls.send.load(Ordering::SeqCst), 0);
        assert_eq!(calls.commit.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn rebalance_during_send_offsets_never_commits() {
        let epoch = RebalanceEpoch::default();
        let calls = OffsetCalls::default();
        let error = commit_offsets_unless_rebalanced(
            &epoch,
            epoch.current(),
            || {
                calls.send.fetch_add(1, Ordering::SeqCst);
                // Revocation served while send-offsets was on the wire.
                epoch.bump();
                Ok(())
            },
            || {
                calls.commit.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap_err();
        assert!(matches!(error, KafkaTransportError::Rebalanced { .. }));
        assert_eq!(calls.send.load(Ordering::SeqCst), 1);
        assert_eq!(calls.commit.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn rebalance_during_in_flight_commit_aborts_and_next_batch_never_begins() {
        let epoch = Arc::new(RebalanceEpoch::default());
        let calls = Arc::new(Calls::default());
        let offsets = Arc::new(OffsetCalls::default());
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        // Batch N: epoch read before its first input, then in flight.
        let observed = epoch.current();
        let transaction = {
            let (epoch, calls, offsets) = (epoch.clone(), calls.clone(), offsets.clone());
            async move {
                run_kafka_transaction(
                    async {
                        calls.begin.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                    async {
                        calls.body.fetch_add(1, Ordering::SeqCst);
                        // Outputs produced; broker round trip still pending.
                        released.await.unwrap();
                        commit_offsets_unless_rebalanced(
                            &epoch,
                            observed,
                            || {
                                offsets.send.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            },
                            || {
                                offsets.commit.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            },
                        )?;
                        Ok(7_u64)
                    },
                    || async {
                        calls.abort.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await
            }
        };
        let mut pipeline = TransactionPipeline::new();
        pipeline.submit(3, transaction).await.unwrap();
        // While batch N+1 is polled, the consumer serves a revocation.
        tokio::time::sleep(Duration::from_millis(5)).await;
        epoch.bump();
        release.send(()).unwrap();
        let error = pipeline.complete().await.unwrap_err();
        assert!(matches!(error, KafkaTransportError::Rebalanced { .. }));
        assert_eq!(error.retry_class(), RetryClass::Retryable);
        assert_eq!(counts(&calls), (1, 1, 1), "aborted exactly once");
        assert_eq!(offsets.send.load(Ordering::SeqCst), 0);
        assert_eq!(offsets.commit.load(Ordering::SeqCst), 0);
        let next_polled = Arc::new(AtomicBool::new(false));
        let flag = next_polled.clone();
        assert!(pipeline
            .submit(1, async move {
                flag.store(true, Ordering::SeqCst);
                Ok(8_u64)
            })
            .await
            .is_err());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!next_polled.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn blocking_kafka_calls_leave_the_runtime_free() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let blocked = tokio::spawn(blocking_kafka_call(move || {
            rx.blocking_recv().unwrap();
            Ok(1_u8)
        }));
        // The only worker thread must still run other tasks while the
        // blocking call is parked.
        let other = tokio::spawn(async { 2_u8 }).await.unwrap();
        tx.send(()).unwrap();
        assert_eq!((blocked.await.unwrap().unwrap(), other), (1, 2));
    }
}

/// IDR-094 micro-benchmark, run explicitly:
/// `cargo test -p qdl-kafka --release --lib idr094 -- --ignored --nocapture`.
/// A fake commit sleeps 85 ms (broker round trips, no CPU) and normalization
/// busy-spins 235 us per record on the loop thread, the fitted live costs.
#[cfg(test)]
mod idr094_pipeline_benchmark {
    use super::*;

    const RECORDS_PER_BATCH: u64 = 256;
    const BATCHES: u64 = 20;
    const NORMALIZE_PER_RECORD: Duration = Duration::from_micros(235);
    const COMMIT: Duration = Duration::from_millis(85);

    fn normalize_batch() {
        let started = std::time::Instant::now();
        let budget = NORMALIZE_PER_RECORD * RECORDS_PER_BATCH as u32;
        let mut spin = 0_u64;
        while started.elapsed() < budget {
            spin = std::hint::black_box(spin.wrapping_add(1));
        }
    }

    async fn fake_commit() -> Result<u64, KafkaTransportError> {
        tokio::time::sleep(COMMIT).await;
        Ok(RECORDS_PER_BATCH)
    }

    async fn stop_and_wait() -> f64 {
        let started = std::time::Instant::now();
        for _ in 0..BATCHES {
            normalize_batch();
            fake_commit().await.unwrap();
        }
        (RECORDS_PER_BATCH * BATCHES) as f64 / started.elapsed().as_secs_f64()
    }

    async fn pipelined() -> f64 {
        let started = std::time::Instant::now();
        let mut pipeline = TransactionPipeline::new();
        for _ in 0..BATCHES {
            normalize_batch();
            pipeline.complete().await.unwrap();
            pipeline
                .submit(RECORDS_PER_BATCH, fake_commit())
                .await
                .unwrap();
        }
        pipeline.complete().await.unwrap();
        (RECORDS_PER_BATCH * BATCHES) as f64 / started.elapsed().as_secs_f64()
    }

    #[test]
    #[ignore = "micro-benchmark; run explicitly with --ignored --nocapture"]
    fn idr094_pipelined_commit_throughput() {
        // Same shape as the binary: the loop runs in `block_on` on the main
        // thread, the commit task on a single worker.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let before = runtime.block_on(stop_and_wait());
        let after = runtime.block_on(pipelined());
        println!(
            "{}",
            serde_json::json!({
                "event": "idr094_pipeline_benchmark",
                "records_per_batch": RECORDS_PER_BATCH,
                "batches": BATCHES,
                "normalize_us_per_record": NORMALIZE_PER_RECORD.as_micros() as u64,
                "commit_ms": COMMIT.as_millis() as u64,
                "stop_and_wait_records_per_s": before.round(),
                "pipelined_records_per_s": after.round(),
                "speedup": (after / before * 100.0).round() / 100.0,
            })
        );
        assert!(after > before);
    }
}
