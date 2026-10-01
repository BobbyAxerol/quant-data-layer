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
use std::time::{Duration, Instant};

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
    transaction_deadline: Option<Instant>,
}

fn classify(error: KafkaError) -> PipeError {
    use rdkafka::types::RDKafkaErrorCode;
    if matches!(
        error,
        KafkaError::MessageProduction(RDKafkaErrorCode::ProducerFenced)
            | KafkaError::MessageProduction(RDKafkaErrorCode::InvalidProducerEpoch)
            | KafkaError::MessageProduction(RDKafkaErrorCode::TopicAuthorizationFailed)
    ) {
        return PipeError::Fatal(error.to_string());
    }
    if let KafkaError::Transaction(inner) = &error {
        if inner.is_fatal() {
            return PipeError::Fatal(error.to_string());
        }
    }
    PipeError::Abortable(error.to_string())
}

// All retries share the same monotonic budget. A commit timeout is ambiguous:
// retire this producer, then init_transactions resolves/fences it before replay.
fn remaining(deadline: Instant, operation: &str, ambiguous: bool) -> Result<Duration, PipeError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| {
            let message = format!("{operation}: shared recovery deadline exhausted");
            if ambiguous {
                PipeError::Fatal(message)
            } else {
                PipeError::Abortable(message)
            }
        })
}

fn retry_transaction(
    deadline: Instant,
    operation: &str,
    ambiguous: bool,
    mut call: impl FnMut(Duration) -> Result<(), KafkaError>,
) -> Result<(), PipeError> {
    loop {
        let left = remaining(deadline, operation, ambiguous)?;
        match call(left) {
            Ok(()) => return Ok(()),
            Err(KafkaError::Transaction(inner))
                if !inner.is_fatal() && inner.is_retriable() && !inner.txn_requires_abort() =>
            {
                let left = remaining(deadline, operation, ambiguous)?;
                std::thread::sleep(left.min(Duration::from_millis(2)));
            }
            Err(error) => return Err(classify(error)),
        }
    }
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
            transaction_deadline: None,
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
        self.producer.begin_transaction().map_err(classify)?;
        self.transaction_deadline = Some(Instant::now() + self.settings.transaction_timeout);
        Ok(())
    }

    fn send(&mut self, record: &OutputRecord) -> Result<(), PipeError> {
        let mut pending = BaseRecord::<[u8], [u8]>::to(&record.topic)
            .partition(record.partition)
            .key(record.key.as_slice());
        if let Some(value) = &record.value {
            pending = pending.payload(value.as_slice());
        }
        let deadline = self
            .transaction_deadline
            .ok_or_else(|| PipeError::Fatal("send without transaction deadline".into()))?;
        loop {
            remaining(deadline, "send", false)?;
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
        let deadline = self
            .transaction_deadline
            .ok_or_else(|| PipeError::Fatal("commit without transaction deadline".into()))?;
        retry_transaction(deadline, "send_offsets", false, |left| {
            self.producer
                .send_offsets_to_transaction(&offsets, &metadata, left)
        })?;
        retry_transaction(deadline, "commit", true, |left| {
            self.producer.commit_transaction(left)
        })?;
        self.transaction_deadline = None;
        Ok(())
    }

    fn abort(&mut self) -> Result<(), PipeError> {
        retry_transaction(
            Instant::now() + self.settings.transaction_timeout,
            "abort",
            true,
            |left| self.producer.abort_transaction(left),
        )?;
        self.transaction_deadline = None;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), PipeError> {
        let assignment = self
            .consumer
            .assignment()
            .map_err(|error| PipeError::Fatal(error.to_string()))?;
        if assignment.count() == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        let committed = self
            .consumer
            .committed_offsets(assignment, remaining(deadline, "committed offsets", true)?)
            .map_err(|error| PipeError::Fatal(error.to_string()))?;
        let mut positions = TopicPartitionList::new();
        for element in committed.elements() {
            element
                .error()
                .map_err(|error| PipeError::Fatal(error.to_string()))?;
            let offset = match element.offset() {
                Offset::Offset(offset) if offset >= 0 => Offset::Offset(offset),
                // Only the explicit "no committed offset" sentinel uses start policy.
                Offset::Invalid => Offset::Beginning,
                other => {
                    return Err(PipeError::Fatal(format!(
                        "invalid committed offset: {other:?}"
                    )))
                }
            };
            positions
                .add_partition_offset(element.topic(), element.partition(), offset)
                .map_err(|error| PipeError::Fatal(error.to_string()))?;
        }
        let sought = self
            .consumer
            .seek_partitions(
                positions,
                remaining(deadline, "seek committed offsets", true)?,
            )
            .map_err(|error| PipeError::Fatal(error.to_string()))?;
        for element in sought.elements() {
            element
                .error()
                .map_err(|error| PipeError::Fatal(error.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;

    #[test]
    fn fenced_or_unauthorized_producer_is_not_reused() {
        use rdkafka::types::RDKafkaErrorCode;
        for code in [
            RDKafkaErrorCode::ProducerFenced,
            RDKafkaErrorCode::InvalidProducerEpoch,
            RDKafkaErrorCode::TopicAuthorizationFailed,
        ] {
            assert!(matches!(
                classify(KafkaError::MessageProduction(code)),
                PipeError::Fatal(_)
            ));
        }
    }

    #[test]
    fn real_queue_full_is_bounded_by_the_transaction_deadline() {
        // No broker, one-slot native queue: exercise the real send loop, not a mock.
        let settings = KafkaPipeSettings {
            bootstrap: "127.0.0.1:1".into(),
            input_topic: "test-only".into(),
            group_id: "test-only".into(),
            transactional_id: "test-only".into(),
            client_id: "test-only".into(),
            tls: None,
            transaction_timeout: Duration::from_millis(30),
        };
        let consumer = settings
            .base()
            .set("group.id", "test-only")
            .create()
            .unwrap();
        let producer = settings
            .base()
            .set("queue.buffering.max.messages", "1")
            .set("message.timeout.ms", "1000")
            .create()
            .unwrap();
        let mut pipe = KafkaPipe {
            settings,
            consumer,
            producer,
            transaction_deadline: Some(Instant::now() + Duration::from_millis(30)),
        };
        let record = OutputRecord {
            topic: "test-only".into(),
            partition: 0,
            key: b"k".to_vec(),
            value: Some(b"v".to_vec()),
        };
        pipe.send(&record).unwrap();
        let started = Instant::now();
        assert!(matches!(pipe.send(&record), Err(PipeError::Abortable(_))));
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(pipe.producer.in_flight_count(), 1);
    }

    #[test]
    fn expired_send_budget_fails_without_calling_transport() {
        let mut calls = 0;
        let result = retry_transaction(Instant::now(), "send", false, |_| {
            calls += 1;
            Ok(())
        });
        assert!(matches!(result, Err(PipeError::Abortable(_))));
        assert_eq!(calls, 0);
    }

    #[test]
    fn ambiguous_commit_budget_retires_producer_without_retry_or_abort() {
        assert!(matches!(
            remaining(Instant::now(), "commit", true),
            Err(PipeError::Fatal(_))
        ));
    }

    #[test]
    fn time_spent_sending_is_not_given_back_to_commit() {
        let deadline = Instant::now() + Duration::from_millis(50);
        let first = remaining(deadline, "send", false).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        retry_transaction(deadline, "commit", true, |left| {
            assert!(left < first);
            Ok(())
        })
        .unwrap();
    }
}
