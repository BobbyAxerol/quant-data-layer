//! Committed canonical reads: the live [`LogSource`] and the replay
//! [`RangeSource`] over Kafka.
//!
//! Every consumer here is read-only by construction: `read_committed`, no
//! auto-commit, no offset store and no call that commits; it is positioned by
//! explicit assignment, never by a group. Aborted transactional records and
//! transaction markers are therefore invisible, which is why "caught up" is
//! decided from the consumer position, not from the last record seen.

use crate::hub::{LogSource, RawRecord};
use crate::replay::{RangeCursor, RangeError, RangeSource};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct KafkaSettings {
    pub bootstrap: String,
    pub topic: String,
    /// Required: librdkafka refuses `assign()` without a group ("Local:
    /// Unknown group") and then looks up the group coordinator, which needs
    /// DESCRIBE on the group. Readers use a `kn-` group with DESCRIBE only, so
    /// they can find the coordinator but can never commit (commit needs READ).
    pub group_id: String,
    pub client_id: String,
    /// `None` = plaintext (isolated test broker only).
    pub tls: Option<(String, String, String)>,
    pub fetch_wait_ms: u32,
}

use crate::memory::{LIVE_QUEUE_KBYTES, REPLAY_QUEUE_KBYTES};

/// Groups the shared read principal may touch in production; a gateway reader
/// must never use them (it never commits, but a mistaken id could).
const PRODUCTION_GROUP_PREFIXES: [&str; 4] = [
    "stable-projector-",
    "qdl-r1-reference-parity-",
    "qdl-c40-handoff-",
    "qdl-v2-realtime-core-",
];

impl KafkaSettings {
    /// Refuses a group id outside the KN namespace or inside a production one.
    pub fn validate(&self) -> Result<(), String> {
        let group = &self.group_id;
        if !group.starts_with("kn-")
            || PRODUCTION_GROUP_PREFIXES
                .iter()
                .any(|prefix| group.starts_with(prefix))
        {
            return Err(format!(
                "reader group id {group} must be a kn- id outside production groups"
            ));
        }
        Ok(())
    }

    fn client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap)
            .set("client.id", &self.client_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("isolation.level", "read_committed")
            .set("enable.partition.eof", "false")
            .set("fetch.wait.max.ms", self.fetch_wait_ms.to_string())
            .set("fetch.min.bytes", "1")
            .set("queued.max.messages.kbytes", LIVE_QUEUE_KBYTES.to_string())
            .set("socket.timeout.ms", "10000");
        config.set("group.id", &self.group_id);
        if let Some((ca, cert, key)) = &self.tls {
            config
                .set("security.protocol", "ssl")
                .set("ssl.ca.location", ca)
                .set("ssl.certificate.location", cert)
                .set("ssl.key.location", key)
                .set("ssl.endpoint.identification.algorithm", "https");
        }
        config
    }

    pub fn stream_consumer(&self) -> Result<StreamConsumer, String> {
        self.validate()?;
        self.client_config()
            .create()
            .map_err(|error| format!("kafka consumer: {error}"))
    }

    pub fn base_consumer(&self) -> Result<BaseConsumer, String> {
        self.validate()?;
        self.client_config()
            .create()
            .map_err(|error| format!("kafka consumer: {error}"))
    }

    /// A replay reader: an out-of-range start is an error, never a silent
    /// reset to the log end or start.
    fn range_consumer(&self) -> Result<BaseConsumer, String> {
        self.validate()?;
        let mut config = self.client_config();
        config
            .set("auto.offset.reset", "error")
            .set(
                "queued.max.messages.kbytes",
                REPLAY_QUEUE_KBYTES.to_string(),
            )
            .set("client.id", format!("{}-replay", self.client_id));
        config
            .create()
            .map_err(|error| format!("kafka replay consumer: {error}"))
    }

    pub fn partitions(&self, consumer: &impl Consumer) -> Result<Vec<i32>, String> {
        let metadata = consumer
            .fetch_metadata(Some(&self.topic), Duration::from_secs(10))
            .map_err(|error| error.to_string())?;
        let mut partitions: Vec<i32> = metadata
            .topics()
            .iter()
            .flat_map(|topic| topic.partitions().iter().map(|partition| partition.id()))
            .collect();
        partitions.sort_unstable();
        if partitions.is_empty() {
            return Err(format!("topic {} has no partitions", self.topic));
        }
        Ok(partitions)
    }
}

/// The replica's one live reader: every partition, from its high watermark.
pub struct KafkaLogSource {
    consumer: BaseConsumer,
    topic: String,
}

impl KafkaLogSource {
    /// Returns the source and the first offset it will deliver per partition.
    pub fn open_at_end(settings: &KafkaSettings) -> Result<(Self, Vec<(i32, i64)>), String> {
        Self::open_warm(settings, 0)
    }

    /// Start `warm_records` offsets before the high watermark (never below
    /// the retention floor) so the ring already covers recent cursors after a
    /// restart: a reconnect storm then replays from memory, not from a reader
    /// per stream. The ring's byte/age bounds still apply.
    pub fn open_warm(
        settings: &KafkaSettings,
        warm_records: i64,
    ) -> Result<(Self, Vec<(i32, i64)>), String> {
        let consumer = settings.base_consumer()?;
        let mut list = TopicPartitionList::new();
        let mut starts = Vec::new();
        for partition in settings.partitions(&consumer)? {
            let (low, high) = consumer
                .fetch_watermarks(&settings.topic, partition, Duration::from_secs(10))
                .map_err(|error| error.to_string())?;
            let start = (high - warm_records.max(0)).max(low);
            list.add_partition_offset(&settings.topic, partition, Offset::Offset(start))
                .map_err(|error| error.to_string())?;
            starts.push((partition, start));
        }
        consumer.assign(&list).map_err(|error| error.to_string())?;
        Ok((
            Self {
                consumer,
                topic: settings.topic.clone(),
            },
            starts,
        ))
    }
}

impl LogSource for KafkaLogSource {
    fn poll(&mut self, timeout: Duration) -> Result<Option<RawRecord>, String> {
        match self.consumer.poll(timeout) {
            Some(Ok(message)) => Ok(Some(RawRecord {
                partition: message.partition(),
                offset: message.offset(),
                key: message.key().unwrap_or_default().to_vec(),
                payload: message.payload().unwrap_or_default().to_vec(),
                timestamp_ms: message.timestamp().to_millis().unwrap_or(0),
            })),
            Some(Err(error)) => Err(error.to_string()),
            None => Ok(None),
        }
    }

    fn positions(&self) -> Vec<(i32, i64)> {
        let Ok(list) = self.consumer.position() else {
            return Vec::new();
        };
        list.elements_for_topic(&self.topic)
            .iter()
            .filter_map(|element| match element.offset() {
                Offset::Offset(offset) => Some((element.partition(), offset)),
                _ => None,
            })
            .collect()
    }
}

/// Replay readers over Kafka. Connected consumers are kept for reuse (at
/// most `idle_max`, the pool's reader count), so a replay does not pay a new
/// connection, metadata fetch and group coordinator lookup each time.
pub struct KafkaRangeSource {
    pub settings: KafkaSettings,
    idle: std::sync::Arc<std::sync::Mutex<Vec<BaseConsumer>>>,
    idle_max: usize,
}

impl KafkaRangeSource {
    pub fn new(settings: KafkaSettings, idle_max: usize) -> Self {
        Self {
            settings,
            idle: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            idle_max,
        }
    }

    pub fn idle(&self) -> usize {
        self.idle.lock().map(|idle| idle.len()).unwrap_or(0)
    }

    fn release(&self, consumer: BaseConsumer) {
        release(&self.idle, self.idle_max, consumer);
    }
}

/// Only an unassigned, healthy consumer goes back to the pool.
fn release(idle: &std::sync::Mutex<Vec<BaseConsumer>>, idle_max: usize, consumer: BaseConsumer) {
    if consumer.unassign().is_ok() {
        if let Ok(mut idle) = idle.lock() {
            if idle.len() < idle_max {
                idle.push(consumer);
            }
        }
    }
}

pub struct KafkaRangeCursor {
    consumer: Option<BaseConsumer>,
    topic: String,
    partition: i32,
    idle: std::sync::Arc<std::sync::Mutex<Vec<BaseConsumer>>>,
    idle_max: usize,
}

impl Drop for KafkaRangeCursor {
    fn drop(&mut self) {
        if let Some(consumer) = self.consumer.take() {
            release(&self.idle, self.idle_max, consumer);
        }
    }
}

fn is_out_of_range(error: &KafkaError) -> bool {
    matches!(
        error.rdkafka_error_code(),
        Some(RDKafkaErrorCode::OffsetOutOfRange)
    )
}

impl RangeSource for KafkaRangeSource {
    fn open(&self, partition: i32, from: i64) -> Result<Box<dyn RangeCursor>, RangeError> {
        let reused = self.idle.lock().ok().and_then(|mut idle| idle.pop());
        let consumer = match reused {
            Some(consumer) => consumer,
            None => self.settings.range_consumer().map_err(RangeError::Other)?,
        };
        let (low, _) = consumer
            .fetch_watermarks(&self.settings.topic, partition, Duration::from_secs(10))
            .map_err(|error| RangeError::Other(error.to_string()))?;
        if from < low {
            self.release(consumer);
            return Err(RangeError::Retention);
        }
        assign_from(&consumer, &self.settings.topic, partition, from).map_err(RangeError::Other)?;
        Ok(Box::new(KafkaRangeCursor {
            consumer: Some(consumer),
            topic: self.settings.topic.clone(),
            partition,
            idle: self.idle.clone(),
            idle_max: self.idle_max,
        }))
    }
}

impl RangeCursor for KafkaRangeCursor {
    fn next(&mut self, timeout: Duration) -> Result<Option<RawRecord>, RangeError> {
        let Some(consumer) = self.consumer.as_ref() else {
            return Err(RangeError::Other("replay reader closed".into()));
        };
        match consumer.poll(timeout) {
            Some(Ok(message)) => Ok(Some(RawRecord {
                partition: message.partition(),
                offset: message.offset(),
                key: message.key().unwrap_or_default().to_vec(),
                payload: message.payload().unwrap_or_default().to_vec(),
                timestamp_ms: message.timestamp().to_millis().unwrap_or(0),
            })),
            Some(Err(error)) if is_out_of_range(&error) => {
                // A consumer that hit an error is not reused.
                self.consumer = None;
                Err(RangeError::Retention)
            }
            Some(Err(error)) => {
                self.consumer = None;
                Err(RangeError::Other(error.to_string()))
            }
            None => Ok(None),
        }
    }

    fn position(&self) -> Option<i64> {
        position(self.consumer.as_ref()?, &self.topic, self.partition)
    }
}

pub fn assign_from(
    consumer: &impl Consumer,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<(), String> {
    let mut list = TopicPartitionList::new();
    list.add_partition_offset(topic, partition, Offset::Offset(offset))
        .map_err(|error| error.to_string())?;
    consumer.assign(&list).map_err(|error| error.to_string())
}

/// Next offset the consumer will read for this partition, if known.
pub fn position(consumer: &impl Consumer, topic: &str, partition: i32) -> Option<i64> {
    let list = consumer.position().ok()?;
    match list.find_partition(topic, partition)?.offset() {
        Offset::Offset(value) => Some(value),
        _ => None,
    }
}

pub fn high_watermark(
    consumer: &impl Consumer,
    topic: &str,
    partition: i32,
) -> Result<i64, String> {
    consumer
        .fetch_watermarks(topic, partition, Duration::from_secs(10))
        .map(|(_, high)| high)
        .map_err(|error| error.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatestRecord {
    pub partition: i32,
    pub offset: i64,
    pub payload: Vec<u8>,
}

/// Latest committed record carrying `key`, scanning the last `window`
/// offsets of every partition (probe for the vertical-slice harness).
pub fn latest_for_key(
    settings: &KafkaSettings,
    key: &[u8],
    window: i64,
    deadline: Duration,
) -> Result<Option<LatestRecord>, String> {
    let consumer = settings.base_consumer()?;
    let metadata = consumer
        .fetch_metadata(Some(&settings.topic), Duration::from_secs(10))
        .map_err(|error| error.to_string())?;
    let partitions: Vec<i32> = metadata
        .topics()
        .iter()
        .flat_map(|topic| topic.partitions().iter().map(|partition| partition.id()))
        .collect();
    let mut best: Option<LatestRecord> = None;
    for partition in partitions {
        let (low, high) = consumer
            .fetch_watermarks(&settings.topic, partition, Duration::from_secs(10))
            .map_err(|error| error.to_string())?;
        if high <= low {
            continue;
        }
        assign_from(
            &consumer,
            &settings.topic,
            partition,
            (high - window).max(low),
        )?;
        let started = Instant::now();
        while started.elapsed() < deadline {
            match consumer.poll(Duration::from_millis(200)) {
                Some(Ok(message)) => {
                    if message.key() == Some(key) {
                        let found = LatestRecord {
                            partition,
                            offset: message.offset(),
                            payload: message.payload().unwrap_or_default().to_vec(),
                        };
                        if best
                            .as_ref()
                            .map_or(true, |current| current.partition == partition)
                        {
                            best = Some(found);
                        }
                    }
                    if message.offset() + 1 >= high {
                        break;
                    }
                }
                Some(Err(error)) => return Err(error.to_string()),
                None => {
                    if position(&consumer, &settings.topic, partition)
                        .is_some_and(|next| next >= high)
                    {
                        break;
                    }
                }
            }
        }
        if best.is_some() {
            // A physical key lives on exactly one partition.
            break;
        }
    }
    Ok(best)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(group: &str) -> KafkaSettings {
        KafkaSettings {
            bootstrap: "localhost:1".into(),
            topic: "t".into(),
            group_id: group.into(),
            client_id: "c".into(),
            tls: None,
            fetch_wait_ms: 10,
        }
    }

    /// Why readers carry a group at all: librdkafka refuses assignment
    /// without one. If a future librdkafka lifts this, readers can drop the
    /// group and its DESCRIBE ACL.
    #[tokio::test]
    async fn librdkafka_requires_a_group_for_assignment() {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:1")
            .create()
            .expect("group-less consumer");
        let error = assign_from(&consumer, "t", 0, 5).expect_err("assign needs a group");
        assert!(error.contains("Unknown group"), "{error}");
    }

    #[test]
    fn production_groups_are_refused() {
        assert!(settings("kn-stream-reader").validate().is_ok());
        for group in [
            "stable-projector-v1",
            "qdl-v2-realtime-core-v2",
            "other",
            "qdl-c40-handoff-x",
        ] {
            assert!(settings(group).validate().is_err(), "{group}");
        }
    }
}
