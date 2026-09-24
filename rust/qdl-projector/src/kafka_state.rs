//! [`StateSource`] over Kafka: stage B's reader of the state topics
//! (KN-3 K3.3).
//!
//! Two consumers. The **group** consumer (group `kn-projector-v3-b`,
//! cooperative-sticky) only decides partition ownership; its partitions are
//! paused and whatever it fetched before the pause is discarded. The **data**
//! consumer reads the owned partitions in assign mode from explicit offsets
//! (the cache checkpoint, or the partition start for a build), so a start
//! offset can never be overridden by a committed group offset fetched
//! asynchronously (a seek racing the committed-offset fetch made a cold
//! build finish at the partition end without reading it - found in the
//! K3-T08 run). `auto.offset.reset=error`: an offset out of range is an
//! error, never a silent jump. `read_committed`, no auto-commit; group
//! offsets are committed asynchronously for lag monitoring only.

use crate::stage_b::{PartitionReader, StateInput, StateSource};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::Message;
use std::collections::BTreeSet;
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

fn base(settings: &KafkaStateSettings) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &settings.bootstrap)
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "false")
        .set("socket.timeout.ms", "10000");
    if let Some((ca, certificate, key)) = &settings.tls {
        config
            .set("security.protocol", "ssl")
            .set("ssl.ca.location", ca)
            .set("ssl.certificate.location", certificate)
            .set("ssl.key.location", key)
            .set("ssl.endpoint.identification.algorithm", "https");
    }
    config
}

fn one(topic: &str, partition: i32, offset: Option<i64>) -> Result<TopicPartitionList, String> {
    let mut list = TopicPartitionList::new();
    match offset {
        Some(offset) => list
            .add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(|error| error.to_string())?,
        None => {
            list.add_partition(topic, partition);
        }
    }
    Ok(list)
}

pub struct KafkaStateSource {
    group: BaseConsumer,
    data: BaseConsumer,
    /// Partitions the data consumer currently reads.
    reading: BTreeSet<(String, i32)>,
}

impl KafkaStateSource {
    pub fn open(settings: &KafkaStateSettings) -> Result<Self, String> {
        let group: BaseConsumer = base(settings)
            .set("client.id", &settings.client_id)
            .set("group.id", &settings.group_id)
            .set("auto.offset.reset", "earliest")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set("queued.max.messages.kbytes", "1024")
            .create()
            .map_err(|error| format!("stage B group consumer: {error}"))?;
        let topics: Vec<&str> = settings.topics.iter().map(String::as_str).collect();
        group
            .subscribe(&topics)
            .map_err(|error| format!("stage B subscribe: {error}"))?;
        // Assign mode only: never joins or commits this group id (librdkafka
        // needs one for positions).
        let data: BaseConsumer = base(settings)
            .set("client.id", format!("{}-data", settings.client_id))
            .set("group.id", format!("{}-data", settings.group_id))
            .set("auto.offset.reset", "error")
            .set("queued.max.messages.kbytes", "16384")
            .create()
            .map_err(|error| format!("stage B data consumer: {error}"))?;
        Ok(Self {
            group,
            data,
            reading: BTreeSet::new(),
        })
    }

    /// Serve the group protocol (heartbeats, rebalances), keep the owned
    /// partitions paused there, and stop reading partitions no longer owned.
    fn membership(&mut self) -> Result<Vec<(String, i32)>, String> {
        for _ in 0..1_000 {
            match self.group.poll(Duration::ZERO) {
                None => break,
                Some(Err(error)) => return Err(error.to_string()),
                // Fetched before the pause: the data consumer reads it.
                Some(Ok(_)) => {}
            }
        }
        let assignment = self.group.assignment().map_err(|error| error.to_string())?;
        if assignment.count() > 0 {
            self.group
                .pause(&assignment)
                .map_err(|error| error.to_string())?;
        }
        let owned: Vec<(String, i32)> = assignment
            .elements()
            .iter()
            .map(|element| (element.topic().to_owned(), element.partition()))
            .collect();
        let gone: Vec<(String, i32)> = self
            .reading
            .iter()
            .filter(|key| !owned.contains(key))
            .cloned()
            .collect();
        for (topic, partition) in gone {
            self.data
                .incremental_unassign(&one(&topic, partition, None)?)
                .map_err(|error| error.to_string())?;
            self.reading.remove(&(topic, partition));
        }
        Ok(owned)
    }
}

impl StateSource for KafkaStateSource {
    fn poll(&mut self, max: usize, timeout: Duration) -> Result<Vec<StateInput>, String> {
        self.membership()?;
        let mut batch = Vec::new();
        if self.reading.is_empty() {
            std::thread::sleep(timeout.min(Duration::from_millis(50)));
            return Ok(batch);
        }
        let mut wait = timeout;
        while batch.len() < max {
            match self.data.poll(wait) {
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

    /// (Re)start reading a partition at exactly `offset`.
    fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<(), String> {
        let key = (topic.to_owned(), partition);
        if self.reading.contains(&key) {
            self.data
                .incremental_unassign(&one(topic, partition, None)?)
                .map_err(|error| error.to_string())?;
        }
        self.data
            .incremental_assign(&one(topic, partition, Some(offset))?)
            .map_err(|error| error.to_string())?;
        self.reading.insert(key);
        Ok(())
    }

    fn watermarks(&mut self, topic: &str, partition: i32) -> Result<(i64, i64), String> {
        self.data
            .fetch_watermarks(topic, partition, Duration::from_secs(10))
            .map_err(|error| error.to_string())
    }

    fn position(&mut self, topic: &str, partition: i32) -> Result<Option<i64>, String> {
        if !self.reading.contains(&(topic.to_owned(), partition)) {
            return Ok(None);
        }
        let positions = self.data.position().map_err(|error| error.to_string())?;
        Ok(positions
            .find_partition(topic, partition)
            .and_then(|element| match element.offset() {
                Offset::Offset(offset) => Some(offset),
                _ => None,
            }))
    }

    fn assigned(&mut self) -> Result<Vec<(String, i32)>, String> {
        self.membership()
    }

    fn commit(&mut self, topic: &str, partition: i32, next: i64) -> Result<(), String> {
        self.group
            .commit(&one(topic, partition, Some(next))?, CommitMode::Async)
            .map_err(|error| error.to_string())
    }
}

/// [`PartitionReader`] over Kafka: assign mode, `read_committed`, explicit
/// start offsets, never commits. librdkafka needs a `group.id` for
/// `position()`; the group is never joined. Used by the per-product rebuild
/// (D17) and the bars-topic cleaner.
pub struct KafkaPartitionReader {
    consumer: BaseConsumer,
    assigned: Option<(String, i32)>,
}

impl KafkaPartitionReader {
    /// `id` is the client id and the (never joined) group id.
    pub fn open(
        bootstrap: &str,
        id: &str,
        tls: Option<&(String, String, String)>,
    ) -> Result<Self, String> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", bootstrap)
            .set("client.id", id)
            .set("group.id", id)
            .set("isolation.level", "read_committed")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("enable.partition.eof", "false")
            .set("queued.max.messages.kbytes", "16384")
            .set("socket.timeout.ms", "10000");
        if let Some((ca, certificate, key)) = tls {
            config
                .set("security.protocol", "ssl")
                .set("ssl.ca.location", ca)
                .set("ssl.certificate.location", certificate)
                .set("ssl.key.location", key)
                .set("ssl.endpoint.identification.algorithm", "https");
        }
        let consumer: BaseConsumer = config
            .create()
            .map_err(|error| format!("partition reader: {error}"))?;
        Ok(Self {
            consumer,
            assigned: None,
        })
    }
}

impl PartitionReader for KafkaPartitionReader {
    fn watermarks(&mut self, topic: &str, partition: i32) -> Result<(i64, i64), String> {
        self.consumer
            .fetch_watermarks(topic, partition, Duration::from_secs(10))
            .map_err(|error| error.to_string())
    }

    fn start(&mut self, topic: &str, partition: i32, offset: i64) -> Result<(), String> {
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(|error| error.to_string())?;
        self.consumer
            .assign(&assignment)
            .map_err(|error| error.to_string())?;
        self.assigned = Some((topic.to_owned(), partition));
        Ok(())
    }

    fn poll(&mut self, max: usize, timeout: Duration) -> Result<Vec<StateInput>, String> {
        let mut batch = Vec::new();
        if self.assigned.is_none() {
            return Ok(batch);
        }
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

    fn stop(&mut self) -> Result<(), String> {
        self.assigned = None;
        self.consumer.unassign().map_err(|error| error.to_string())
    }
}
