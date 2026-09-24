//! [`StateSource`] over Kafka: stage B's group consumer of the state topics
//! (KN-3 K3.3).
//!
//! `read_committed`, no auto-commit; the authoritative position is the
//! checkpoint in the cache (applied atomically with the data). Group offsets
//! are committed asynchronously for lag monitoring only. A seek on a newly
//! assigned partition purges records fetched from the old position.

use crate::stage_b::{StateInput, StateSource};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::Message;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct KafkaStateSettings {
    pub bootstrap: String,
    pub topics: Vec<String>,
    pub group_id: String,
    pub client_id: String,
    /// `(ca, certificate, key)`; `None` = plaintext (isolated broker only).
    pub tls: Option<(String, String, String)>,
}

pub struct KafkaStateSource {
    consumer: BaseConsumer,
}

impl KafkaStateSource {
    pub fn open(settings: &KafkaStateSettings) -> Result<Self, String> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &settings.bootstrap)
            .set("client.id", &settings.client_id)
            .set("group.id", &settings.group_id)
            .set("isolation.level", "read_committed")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set("queued.max.messages.kbytes", "16384")
            .set("socket.timeout.ms", "10000");
        if let Some((ca, certificate, key)) = &settings.tls {
            config
                .set("security.protocol", "ssl")
                .set("ssl.ca.location", ca)
                .set("ssl.certificate.location", certificate)
                .set("ssl.key.location", key)
                .set("ssl.endpoint.identification.algorithm", "https");
        }
        let consumer: BaseConsumer = config
            .create()
            .map_err(|error| format!("stage B consumer: {error}"))?;
        let topics: Vec<&str> = settings.topics.iter().map(String::as_str).collect();
        consumer
            .subscribe(&topics)
            .map_err(|error| format!("stage B subscribe: {error}"))?;
        Ok(Self { consumer })
    }
}

impl StateSource for KafkaStateSource {
    fn poll(&mut self, max: usize, timeout: Duration) -> Result<Vec<StateInput>, String> {
        let mut batch = Vec::new();
        let mut wait = timeout;
        while batch.len() < max {
            match self.consumer.poll(wait) {
                None => break,
                Some(Err(error)) => return Err(error.to_string()),
                Some(Ok(message)) => batch.push(StateInput {
                    topic: message.topic().to_owned(),
                    partition: message.partition(),
                    offset: message.offset(),
                    key: message.key().unwrap_or_default().to_vec(),
                    value: message.payload().map(<[u8]>::to_vec),
                }),
            }
            wait = Duration::ZERO;
        }
        Ok(batch)
    }

    fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<(), String> {
        let mut positions = TopicPartitionList::new();
        positions
            .add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(|error| error.to_string())?;
        self.consumer
            .seek_partitions(positions, Duration::from_secs(10))
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn watermarks(&mut self, topic: &str, partition: i32) -> Result<(i64, i64), String> {
        self.consumer
            .fetch_watermarks(topic, partition, Duration::from_secs(10))
            .map_err(|error| error.to_string())
    }

    fn position(&mut self, topic: &str, partition: i32) -> Result<Option<i64>, String> {
        let positions = self
            .consumer
            .position()
            .map_err(|error| error.to_string())?;
        Ok(positions
            .find_partition(topic, partition)
            .and_then(|element| match element.offset() {
                Offset::Offset(offset) => Some(offset),
                _ => None,
            }))
    }

    fn assigned(&mut self) -> Result<Vec<(String, i32)>, String> {
        let assignment = self
            .consumer
            .assignment()
            .map_err(|error| error.to_string())?;
        Ok(assignment
            .elements()
            .iter()
            .map(|element| (element.topic().to_owned(), element.partition()))
            .collect())
    }

    fn commit(&mut self, topic: &str, partition: i32, next: i64) -> Result<(), String> {
        let mut offsets = TopicPartitionList::new();
        offsets
            .add_partition_offset(topic, partition, Offset::Offset(next))
            .map_err(|error| error.to_string())?;
        self.consumer
            .commit(&offsets, CommitMode::Async)
            .map_err(|error| error.to_string())
    }
}
