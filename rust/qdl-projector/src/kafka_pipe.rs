//! [`Pipe`] over Kafka: a group consumer of the committed canonical topic and
//! a transactional producer (KN-3 K3.2).
//!
//! The consumer reads `read_committed`, never auto-commits and starts from the
//! earliest retained record the first time the group runs. Its offsets are
//! committed only through the producer's transaction
//! (`send_offsets_to_transaction` with the consumer group metadata, so a
//! zombie member of an older generation is fenced by the coordinator). The
//! transactional id is fixed per replica: `init_transactions` fences any older
//! instance still holding it.

use crate::stage_a::{InputRecord, OutputRecord, Pipe, PipeError};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::Message;
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct KafkaPipeSettings {
    pub bootstrap: String,
    pub input_topic: String,
    pub group_id: String,
    pub transactional_id: String,
    pub client_id: String,
    /// `(ca, certificate, key)` files; `None` = plaintext (isolated broker only).
    pub tls: Option<(String, String, String)>,
    pub transaction_timeout: Duration,
}

impl KafkaPipeSettings {
    fn base(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap)
            .set("client.id", &self.client_id)
            .set("socket.timeout.ms", "10000");
        if let Some((ca, certificate, key)) = &self.tls {
            config
                .set("security.protocol", "ssl")
                .set("ssl.ca.location", ca)
                .set("ssl.certificate.location", certificate)
                .set("ssl.key.location", key)
                .set("ssl.endpoint.identification.algorithm", "https");
        }
        config
    }
}

pub struct KafkaPipe {
    settings: KafkaPipeSettings,
    consumer: BaseConsumer,
    producer: ThreadedProducer<DefaultProducerContext>,
}

fn classify(error: KafkaError) -> PipeError {
    if let KafkaError::Transaction(inner) = &error {
        if inner.is_fatal() {
            return PipeError::Fatal(error.to_string());
        }
    }
    PipeError::Abortable(error.to_string())
}

impl KafkaPipe {
    pub fn open(settings: KafkaPipeSettings) -> Result<Self, String> {
        let consumer: BaseConsumer = settings
            .base()
            .set("group.id", &settings.group_id)
            .set("isolation.level", "read_committed")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set("queued.max.messages.kbytes", "16384")
            .create()
            .map_err(|error| format!("stage A consumer: {error}"))?;
        consumer
            .subscribe(&[&settings.input_topic])
            .map_err(|error| format!("stage A subscribe: {error}"))?;
        let producer: ThreadedProducer<DefaultProducerContext> = settings
            .base()
            .set("transactional.id", &settings.transactional_id)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("compression.type", "zstd")
            .set("linger.ms", "5")
            .set(
                "transaction.timeout.ms",
                settings.transaction_timeout.as_millis().to_string(),
            )
            .create()
            .map_err(|error| format!("stage A producer: {error}"))?;
        producer
            .init_transactions(settings.transaction_timeout)
            .map_err(|error| format!("stage A init_transactions: {error}"))?;
        Ok(Self {
            settings,
            consumer,
            producer,
        })
    }

    pub fn consumer(&self) -> &BaseConsumer {
        &self.consumer
    }
}

impl Pipe for KafkaPipe {
    fn poll(&mut self, max: usize, timeout: Duration) -> Result<Vec<InputRecord>, PipeError> {
        let mut batch = Vec::new();
        let mut wait = timeout;
        while batch.len() < max {
            match self.consumer.poll(wait) {
                None => break,
                Some(Err(error)) => return Err(PipeError::Abortable(error.to_string())),
                Some(Ok(message)) => batch.push(InputRecord {
                    partition: message.partition(),
                    offset: message.offset(),
                    key: message.key().unwrap_or_default().to_vec(),
                    payload: message.payload().unwrap_or_default().to_vec(),
                }),
            }
            wait = Duration::ZERO;
        }
        Ok(batch)
    }

    fn begin(&mut self) -> Result<(), PipeError> {
        self.producer.begin_transaction().map_err(classify)
    }

    fn send(&mut self, record: &OutputRecord) -> Result<(), PipeError> {
        let mut pending = BaseRecord::<[u8], [u8]>::to(&record.topic)
            .partition(record.partition)
            .key(record.key.as_slice());
        if let Some(value) = &record.value {
            pending = pending.payload(value.as_slice());
        }
        loop {
            match self.producer.send(pending) {
                Ok(()) => return Ok(()),
                Err((
                    KafkaError::MessageProduction(rdkafka::types::RDKafkaErrorCode::QueueFull),
                    back,
                )) => {
                    std::thread::sleep(Duration::from_millis(2));
                    pending = back;
                }
                Err((error, _)) => return Err(classify(error)),
            }
        }
    }

    fn commit(&mut self, next: &BTreeMap<i32, i64>) -> Result<(), PipeError> {
        let mut offsets = TopicPartitionList::new();
        for (&partition, &offset) in next {
            offsets
                .add_partition_offset(
                    &self.settings.input_topic,
                    partition,
                    Offset::Offset(offset),
                )
                .map_err(|error| PipeError::Abortable(error.to_string()))?;
        }
        let metadata = self
            .consumer
            .group_metadata()
            .ok_or_else(|| PipeError::Abortable("consumer has no group metadata".into()))?;
        self.producer
            .send_offsets_to_transaction(&offsets, &metadata, self.settings.transaction_timeout)
            .map_err(classify)?;
        // A retriable commit error may be retried; anything else aborts.
        let mut attempts = 0;
        loop {
            match self
                .producer
                .commit_transaction(self.settings.transaction_timeout)
            {
                Ok(()) => return Ok(()),
                Err(KafkaError::Transaction(inner))
                    if inner.is_retriable() && !inner.txn_requires_abort() && attempts < 3 =>
                {
                    attempts += 1;
                }
                Err(error) => return Err(classify(error)),
            }
        }
    }

    fn abort(&mut self) -> Result<(), PipeError> {
        self.producer
            .abort_transaction(self.settings.transaction_timeout)
            .map_err(classify)
    }

    fn rewind(&mut self) -> Result<(), PipeError> {
        let assignment = self
            .consumer
            .assignment()
            .map_err(|error| PipeError::Fatal(error.to_string()))?;
        if assignment.count() == 0 {
            return Ok(());
        }
        let committed = self
            .consumer
            .committed_offsets(assignment, Duration::from_secs(10))
            .map_err(|error| PipeError::Fatal(error.to_string()))?;
        let mut positions = TopicPartitionList::new();
        for element in committed.elements() {
            let offset = match element.offset() {
                Offset::Offset(offset) => Offset::Offset(offset),
                // No committed offset yet: the group's start policy (earliest).
                _ => Offset::Beginning,
            };
            positions
                .add_partition_offset(element.topic(), element.partition(), offset)
                .map_err(|error| PipeError::Fatal(error.to_string()))?;
        }
        self.consumer
            .seek_partitions(positions, Duration::from_secs(10))
            .map_err(|error| PipeError::Fatal(error.to_string()))?;
        Ok(())
    }
}
