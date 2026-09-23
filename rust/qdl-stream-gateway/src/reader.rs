//! Committed canonical reads for the slice.
//!
//! Every consumer here is read-only by construction: `read_committed`, no
//! auto-commit, no offset store and no call that commits; it is positioned by
//! explicit assignment from a cursor, never by a group. Aborted transactional
//! records and transaction markers are therefore invisible, which is why the
//! stream decides "caught up" from the consumer position, not from the last
//! record it saw.

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
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
