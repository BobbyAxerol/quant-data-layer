#![forbid(unsafe_code)]

use std::fmt::{Display, Formatter};
use std::path::Path;
use std::time::Duration;

use qdl_core::transport::{AppendResult, Cursor, DurableRecord, RetryClass};
use qdl_venue_core::authority::{
    AuthorityFence, AuthorityRecord, Phase92AcceptedHandoff, Phase92AuthorityFence,
    Phase92AuthorityRecord, Phase92PublicationContext, Phase92TerminalCheckpoint,
    Phase9AuthorityFence, Phase9AuthorityRecord, Phase9PublicationContext, PublicationContext,
    SinkTarget,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

const EVENT_ID_HEADER: &str = "qdl-event-id";

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
        if let Some(password) = &self.tls.key_password {
            config.set("ssl.key.password", password);
        }
        Ok(config)
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
            Self::Kafka(_) | Self::Delivery(_) => RetryClass::Retryable,
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

    pub fn apply_authority(&self, record: AuthorityRecord) -> Result<(), KafkaTransportError> {
        self.fence
            .lock()
            .map_err(|_| KafkaTransportError::Fencing("authority lock poisoned".into()))?
            .apply(record)
            .map_err(KafkaTransportError::Fencing)
    }

    pub async fn append(
        &self,
        record: &DurableRecord,
        publication: &PublicationContext,
    ) -> Result<AppendResult, KafkaTransportError> {
        self.fence
            .lock()
            .map_err(|_| KafkaTransportError::Fencing("authority lock poisoned".into()))?
            .permits(publication)
            .map_err(KafkaTransportError::Fencing)?;
        self.sink.append(record).await
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
            SinkTarget::PrimaryCanonical | SinkTarget::PublicV2 | SinkTarget::LegacyV1 => false,
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
            | SinkTarget::CanaryCanonical => false,
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

pub struct KafkaDurableSink {
    producer: FutureProducer,
    request_timeout: Duration,
}

impl KafkaDurableSink {
    pub fn new(config: &KafkaTransportConfig) -> Result<Self, KafkaTransportError> {
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
        Ok(Self {
            producer: client.create()?,
            request_timeout: config.request_timeout,
        })
    }

    pub async fn append(
        &self,
        record: &DurableRecord,
    ) -> Result<AppendResult, KafkaTransportError> {
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
            .send(
                FutureRecord::to(&record.stream)
                    .key(record.partition_key.as_bytes())
                    .payload(record.payload.as_slice())
                    .headers(headers),
                Timeout::After(self.request_timeout),
            )
            .await
            .map_err(|(error, _)| KafkaTransportError::Delivery(error))?;
        let offset = u64::try_from(delivery.offset)
            .map_err(|_| KafkaTransportError::InvalidOffset(delivery.offset))?;
        Ok(AppendResult {
            cursor: Cursor {
                stream: record.stream.clone(),
                transport_partition: delivery.partition,
                partition_key: record.partition_key.clone(),
                offset,
            },
            duplicate: false,
        })
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
            SinkTarget::ShadowCanonical => stream == self.canonical,
            SinkTarget::ShadowQuarantine => stream == self.quarantine,
            SinkTarget::ShadowRaw
            | SinkTarget::CanaryCanonical
            | SinkTarget::PrimaryCanonical
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
}

/// Kafka consume-transform-produce boundary. Output records and the next raw
/// consumer offset commit atomically, so a process crash cannot acknowledge raw
/// input without its canonical/quarantine result or duplicate committed output.
pub struct TransactionalKafkaBridge {
    producer: FutureProducer,
    consumer: StreamConsumer,
    fence: tokio::sync::Mutex<AuthorityFence>,
    topics: TransactionalShadowTopics,
    request_timeout: Duration,
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
        consumer_config
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("isolation.level", "read_committed");
        let consumer: StreamConsumer = consumer_config.create()?;
        let raw_topics: Vec<&str> = topics.raw_inputs.iter().map(String::as_str).collect();
        consumer.subscribe(&raw_topics)?;

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
        let producer: FutureProducer = producer_config.create()?;
        producer.init_transactions(Timeout::After(config.request_timeout))?;
        Ok(Self {
            producer,
            consumer,
            fence: tokio::sync::Mutex::new(AuthorityFence::default()),
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

    pub async fn commit(
        &self,
        inputs: &[TransactionalKafkaInput],
        outputs: &[TransactionalKafkaOutput],
    ) -> Result<Vec<AppendResult>, KafkaTransportError> {
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
        let mut fence = self.fence.lock().await;
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

        self.producer.begin_transaction()?;
        let transaction = async {
            let mut accepted = Vec::with_capacity(outputs.len());
            for output in outputs {
                let delivery = self
                    .producer
                    .send(
                        FutureRecord::to(&output.record.stream)
                            .key(output.record.partition_key.as_bytes())
                            .payload(output.record.payload.as_slice())
                            .headers(OwnedHeaders::new().insert(Header {
                                key: EVENT_ID_HEADER,
                                value: Some(output.record.event_id.as_slice()),
                            })),
                        Timeout::After(self.request_timeout),
                    )
                    .await
                    .map_err(|(error, _)| KafkaTransportError::Delivery(error))?;
                let offset = u64::try_from(delivery.offset)
                    .map_err(|_| KafkaTransportError::InvalidOffset(delivery.offset))?;
                accepted.push(AppendResult {
                    cursor: Cursor {
                        stream: output.record.stream.clone(),
                        transport_partition: delivery.partition,
                        partition_key: output.record.partition_key.clone(),
                        offset,
                    },
                    duplicate: false,
                });
            }
            let mut next_offsets: std::collections::BTreeMap<(String, i32), i64> =
                std::collections::BTreeMap::new();
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
            let mut offsets = TopicPartitionList::new();
            for ((topic, partition), offset) in next_offsets {
                offsets.add_partition_offset(&topic, partition, Offset::Offset(offset))?;
            }
            let group = self.consumer.group_metadata().ok_or_else(|| {
                KafkaTransportError::Configuration("consumer group metadata is unavailable".into())
            })?;
            self.producer.send_offsets_to_transaction(
                &offsets,
                &group,
                Timeout::After(self.request_timeout),
            )?;
            self.producer
                .commit_transaction(Timeout::After(self.request_timeout))?;
            Ok::<_, KafkaTransportError>(accepted)
        }
        .await;
        match transaction {
            Ok(value) => Ok(value),
            Err(error) => {
                self.producer
                    .abort_transaction(Timeout::After(self.request_timeout))
                    .map_err(KafkaTransportError::Kafka)?;
                Err(error)
            }
        }
    }
}

pub struct KafkaEventSource {
    consumer: StreamConsumer,
}

impl KafkaEventSource {
    pub fn new(
        config: &KafkaTransportConfig,
        topics: &[&str],
    ) -> Result<Self, KafkaTransportError> {
        if topics.is_empty() || topics.iter().any(|topic| topic.trim().is_empty()) {
            return Err(KafkaTransportError::Configuration(
                "at least one non-empty topic is required".into(),
            ));
        }
        let mut client = config.client_config()?;
        client
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("isolation.level", "read_committed");
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
}

#[cfg(test)]
mod tests {
    use super::{
        KafkaTlsConfig, KafkaTransportConfig, KafkaTransportError, Phase92SinkTopics,
        Phase9SinkTopics, TransactionalShadowTopics,
    };
    use qdl_core::transport::RetryClass;
    use qdl_venue_core::authority::SinkTarget;
    use std::time::Duration;

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
        assert!(!topics.permits(SinkTarget::PublicV2, &topics.canonical));
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
