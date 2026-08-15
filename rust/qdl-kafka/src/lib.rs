#![forbid(unsafe_code)]

use std::fmt::{Display, Formatter};
use std::path::Path;
use std::time::Duration;

use qdl_core::transport::{AppendResult, Cursor, DurableRecord, RetryClass};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
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
}

impl KafkaTransportError {
    pub fn retry_class(&self) -> RetryClass {
        match self {
            Self::Configuration(_) | Self::MissingField(_) | Self::InvalidUtf8(_) => {
                RetryClass::NonRetryable
            }
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
        }
    }
}

impl std::error::Error for KafkaTransportError {}

impl From<KafkaError> for KafkaTransportError {
    fn from(error: KafkaError) -> Self {
        Self::Kafka(error)
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
    use super::{KafkaTlsConfig, KafkaTransportConfig, KafkaTransportError};
    use qdl_core::transport::RetryClass;
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
