//! Bars-topic cleaner (K3.5, D11 amendment) over a real, isolated Kafka
//! broker (never production): orphan fact keys below a floor are tombstoned
//! exactly once, keys at/above the floor, floor keys and the other partition
//! are never touched; an undecodable floor or unparsable key publishes
//! nothing; the per-sweep cap converges; an aborted sweep leaves nothing.
//!
//! `QDL_KN_TEST_KAFKA=host:port cargo test -p qdl-projector --test cleaner_kafka -- --ignored`.

use prost::Message as _;
use qdl_contracts::qdl::marketdata::v2::{
    event_envelope::Payload, Bar, BarLifecycle, EventEnvelope,
};
use qdl_contracts::state_codec::{bar_key, floor_key, state_partition, StateFrame};
use qdl_contracts::state_contract::{LogicalProductKey, SourceCoordinate};
use qdl_projector::cleaner::{
    sweep_partition, CleanerError, CleanerKafkaSettings, KafkaPartitionReader, SweepLimits,
    SweepReport,
};
use qdl_projector::expiry::{publish_expiry, ExpiryPlan, ExpirySink};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{
    BaseProducer, BaseRecord, DefaultProducerContext, Producer, ThreadedProducer,
};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::Message;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PARTITIONS: u32 = 2;
const MIN: u64 = 60_000;
const TIMEOUT: Duration = Duration::from_secs(10);

fn bootstrap() -> String {
    std::env::var("QDL_KN_TEST_KAFKA").expect(
        "QDL_KN_TEST_KAFKA must name an isolated test broker; this test never runs against production",
    )
}

fn stamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

async fn create_compacted(bootstrap: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    let spec = NewTopic::new(topic, PARTITIONS as i32, TopicReplication::Fixed(1))
        .set("cleanup.policy", "compact");
    for result in admin
        .create_topics(
            &[spec],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .unwrap()
    {
        result.unwrap();
    }
}

fn bar_lpk(uid: &str) -> LogicalProductKey {
    LogicalProductKey::new("paper", "OKX", "SWAP", uid, "BAR", Some("1m")).unwrap()
}

fn partition_of(lpk: &LogicalProductKey) -> i32 {
    state_partition(lpk, PARTITIONS).unwrap() as i32
}

/// Two products on different partitions.
fn two_products() -> (LogicalProductKey, LogicalProductKey) {
    let a = bar_lpk("aaaaaaaa-0000-5000-8000-000000000001");
    let b = (2..100)
        .map(|n| bar_lpk(&format!("aaaaaaaa-0000-5000-8000-{n:012}")))
        .find(|b| partition_of(b) != partition_of(&a))
        .unwrap();
    (a, b)
}

/// `(key, frame)` of a bar fact.
fn fact(
    lpk: &LogicalProductKey,
    minute: u64,
    lifecycle: BarLifecycle,
    revision: u32,
    close: u64,
) -> (String, Vec<u8>) {
    let envelope = EventEnvelope {
        // Deterministic: the same fact always has the same key.
        event_id: format!("{minute}-{}-{revision}-{close}", lifecycle as i32).into_bytes(),
        instrument_uid: lpk.instrument_uid.clone(),
        venue: "OKX".into(),
        market: "SWAP".into(),
        payload: Some(Payload::Bar(Bar {
            interval: "1m".into(),
            open_time_ns: (minute * MIN * 1_000_000) as i64,
            close_time_ns: ((minute + 1) * MIN * 1_000_000 - 1) as i64,
            is_final: lifecycle != BarLifecycle::InProgress,
            revision,
            lifecycle: lifecycle as i32,
            trade_count: close,
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec();
    let frame = StateFrame::bar_revision(
        &envelope,
        lpk,
        SourceCoordinate {
            topic_id: "ljfjPYApRpWQd79McfTtZg".into(),
            partition: 1,
            offset: minute,
        },
        1,
    )
    .unwrap();
    (frame.key().unwrap(), frame.encode().unwrap())
}

fn writer(bootstrap: &str, id: &str) -> BaseProducer {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("transactional.id", id)
        .set("enable.idempotence", "true")
        .create()
        .unwrap();
    producer.init_transactions(Duration::from_secs(20)).unwrap();
    producer
}

/// Commit records `(partition, key, value)` in one transaction.
fn commit(producer: &BaseProducer, topic: &str, records: &[(i32, String, Option<Vec<u8>>)]) {
    producer.begin_transaction().unwrap();
    for (partition, key, value) in records {
        let mut record = BaseRecord::<str, [u8]>::to(topic)
            .partition(*partition)
            .key(key.as_str());
        if let Some(value) = value {
            record = record.payload(value.as_slice());
        }
        producer.send(record).unwrap();
    }
    producer.commit_transaction(TIMEOUT).unwrap();
}

fn facts(
    lpk: &LogicalProductKey,
    minutes: std::ops::Range<u64>,
) -> Vec<(i32, String, Option<Vec<u8>>)> {
    minutes
        .map(|minute| {
            let (key, value) = fact(lpk, minute, BarLifecycle::Final, 0, 1);
            (partition_of(lpk), key, Some(value))
        })
        .collect()
}

/// The expiry plan of `lpk` for opens below `floor_min` (final r0 + p keys).
fn expiry_plan(
    lpk: &LogicalProductKey,
    written: &[(i32, String, Option<Vec<u8>>)],
    floor_min: u64,
) -> ExpiryPlan {
    let mut tombstone_keys = Vec::new();
    for minute in 0..floor_min {
        let prefix = format!("{}|{}|", lpk.encode(), minute * MIN);
        tombstone_keys.extend(
            written
                .iter()
                .filter(|(_, key, _)| key.starts_with(&prefix))
                .map(|(_, key, _)| key.clone()),
        );
        tombstone_keys.push(bar_key(lpk, minute * MIN, false, 0, "").unwrap());
    }
    ExpiryPlan {
        lpk: lpk.clone(),
        generation: 1,
        previous_floor: None,
        floor_ms: floor_min * MIN,
        target_floor_ms: floor_min * MIN,
        tombstone_keys,
        expired_opens: floor_min,
    }
}

fn open_cleaner(
    bootstrap: &str,
) -> (
    KafkaPartitionReader,
    ThreadedProducer<DefaultProducerContext>,
) {
    let settings = CleanerKafkaSettings {
        bootstrap: bootstrap.into(),
        replica: format!("test-{}", stamp()),
        tls: None,
        transaction_timeout: TIMEOUT,
    };
    (
        settings.open_reader().unwrap(),
        settings.open_producer().unwrap(),
    )
}

fn limits(max_keys_per_txn: usize, max_tombstones: usize) -> SweepLimits {
    SweepLimits {
        max_keys_per_txn,
        max_tombstones,
        read_timeout: Duration::from_secs(30),
        transaction_timeout: TIMEOUT,
        ..SweepLimits::default()
    }
}

fn ends(bootstrap: &str, topic: &str) -> BTreeMap<i32, i64> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    (0..PARTITIONS as i32)
        .map(|partition| {
            let (_, high) = consumer
                .fetch_watermarks(topic, partition, TIMEOUT)
                .unwrap();
            (partition, high)
        })
        .collect()
}

type Visible = Vec<(i32, String, Option<Vec<u8>>)>;
type Fetched = Vec<(i64, String, Option<Vec<u8>>)>;

/// Every committed record from `from` to the current end: `(partition, key, payload)`.
fn read_committed(bootstrap: &str, topic: &str, from: &BTreeMap<i32, i64>) -> Visible {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("kn3-cleaner-verify-{}", stamp()))
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let high = ends(bootstrap, topic);
    let mut assignment = TopicPartitionList::new();
    for (partition, start) in from {
        assignment
            .add_partition_offset(topic, *partition, Offset::Offset(*start))
            .unwrap();
    }
    consumer.assign(&assignment).unwrap();
    let mut records: BTreeMap<i32, Fetched> = BTreeMap::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let position = consumer.position().unwrap();
        let done = high.iter().all(|(partition, high)| {
            match position
                .find_partition(topic, *partition)
                .map(|element| element.offset())
            {
                Some(Offset::Offset(at)) => at >= *high,
                _ => from[partition] >= *high,
            }
        });
        if done {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "read_committed did not reach the end"
        );
        if let Some(Ok(message)) = consumer.poll(Duration::from_millis(100)) {
            records.entry(message.partition()).or_default().push((
                message.offset(),
                String::from_utf8(message.key().unwrap_or_default().to_vec()).unwrap(),
                message.payload().map(<[u8]>::to_vec),
            ));
        }
    }
    let mut visible = Vec::new();
    for (partition, mut list) in records {
        list.sort_by_key(|(offset, _, _)| *offset);
        visible.extend(
            list.into_iter()
                .map(|(_, key, payload)| (partition, key, payload)),
        );
    }
    visible
}

/// Only tombstones of exactly `keys` (any order), on `partition`.
fn assert_tombstones(visible: &Visible, partition: i32, keys: &[String]) {
    let mut seen: Vec<String> = visible
        .iter()
        .map(|(p, key, payload)| {
            assert_eq!(*p, partition, "{key}: only the swept partition");
            assert!(payload.is_none(), "{key}: tombstone");
            key.clone()
        })
        .collect();
    seen.sort();
    let mut expected = keys.to_vec();
    expected.sort();
    assert_eq!(seen, expected);
}

/// A topic holding live facts, an expiry of opens 0..10 (floor 10 min) on
/// product A's partition, and the two orphans the cache never sees: a fact
/// for an expired open written between the plan read and the floor (race)
/// and a late fact below the floor (STALE_BELOW_FLOOR). Product B (other
/// partition) has live facts and no floor. Returns the two orphan keys.
async fn orphaned_topic(
    bootstrap: &str,
) -> (String, LogicalProductKey, LogicalProductKey, Vec<String>) {
    let topic = format!("kn3-cleaner-{}", stamp());
    create_compacted(bootstrap, &topic).await;
    let (a, b) = two_products();
    let producer = writer(bootstrap, &format!("kn3-cleaner-writer-{}", stamp()));
    let live = facts(&a, 0..20);
    commit(&producer, &topic, &live);
    commit(&producer, &topic, &facts(&b, 0..20));
    let plan = expiry_plan(&a, &live, 10);
    // The race: open 4 is revised after the plan was read, before its floor.
    let (race, race_value) = fact(&a, 4, BarLifecycle::Revised, 1, 2);
    commit(
        &producer,
        &topic,
        &[(partition_of(&a), race.clone(), Some(race_value))],
    );
    publish_expiry(&producer, &topic, PARTITIONS, &plan, 1, TIMEOUT).unwrap();
    // The late fact below the floor.
    let (late, late_value) = fact(&a, 5, BarLifecycle::Revised, 2, 3);
    // An in-progress fact at the floor and above it: never touched.
    let (at_floor, at_floor_value) = fact(&a, 10, BarLifecycle::InProgress, 0, 4);
    let (above, above_value) = fact(&a, 25, BarLifecycle::InProgress, 0, 5);
    commit(
        &producer,
        &topic,
        &[
            (partition_of(&a), late.clone(), Some(late_value)),
            (partition_of(&a), at_floor, Some(at_floor_value)),
            (partition_of(&a), above, Some(above_value)),
        ],
    );
    (topic, a, b, vec![race, late])
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA (isolated broker); run by the kn-native-integration job"]
async fn orphans_below_the_floor_are_tombstoned_once_and_nothing_else() {
    let bootstrap = bootstrap();
    let (topic, a, b, orphans) = orphaned_topic(&bootstrap).await;
    let (mut reader, producer) = open_cleaner(&bootstrap);
    let before = ends(&bootstrap, &topic);
    let report = sweep_partition(
        &mut reader,
        &topic,
        partition_of(&a),
        &producer,
        &limits(500, 10_000),
    )
    .unwrap();
    // Below the floor: 10 final r0 + 10 p (tombstoned by expiry) + 2 orphans.
    assert_eq!(
        (
            report.floors,
            report.below_floor_keys,
            report.already_tombstoned,
            report.tombstones_published,
            report.pending,
            report.transactions
        ),
        (1, 22, 20, 2, 0, 1),
        "{report:?}"
    );
    assert!(report.records_read > 0 && report.bytes_read > 0);
    assert_tombstones(
        &read_committed(&bootstrap, &topic, &before),
        partition_of(&a),
        &orphans,
    );
    // The other partition: no floor, nothing to do, nothing written.
    let other = sweep_partition(
        &mut reader,
        &topic,
        partition_of(&b),
        &producer,
        &limits(500, 10_000),
    )
    .unwrap();
    assert_eq!((other.floors, other.tombstones_published), (0, 0));
    // A second sweep finds every below-floor key dead.
    let again = ends(&bootstrap, &topic);
    let second = sweep_partition(
        &mut reader,
        &topic,
        partition_of(&a),
        &producer,
        &limits(500, 10_000),
    )
    .unwrap();
    assert_eq!(
        (
            second.below_floor_keys,
            second.already_tombstoned,
            second.tombstones_published
        ),
        (22, 22, 0)
    );
    assert!(read_committed(&bootstrap, &topic, &again).is_empty());
    // Keys at/above the floor and the floor key itself are still live.
    let everything = read_committed(
        &bootstrap,
        &topic,
        &(0..PARTITIONS as i32).map(|p| (p, 0)).collect(),
    );
    let last: BTreeMap<String, bool> = everything
        .iter()
        .map(|(_, key, payload)| (key.clone(), payload.is_some()))
        .collect();
    assert!(last[&floor_key(&a)], "the floor key is never tombstoned");
    for minute in 10..20 {
        let key = fact(&a, minute, BarLifecycle::Final, 0, 1).0;
        assert!(last[&key], "{key}: at/above the floor stays");
        let key_b = fact(&b, minute - 10, BarLifecycle::Final, 0, 1).0;
        assert!(last[&key_b], "{key_b}: product without a floor stays");
    }
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA (isolated broker); run by the kn-native-integration job"]
async fn an_undecodable_floor_or_unparsable_key_publishes_nothing() {
    let bootstrap = bootstrap();
    for (label, bad_key, bad_value) in [
        ("floor", true, Some(b"not a frame".to_vec())),
        ("key", false, Some(b"x".to_vec())),
    ] {
        let (topic, a, _b, _orphans) = orphaned_topic(&bootstrap).await;
        let producer = writer(&bootstrap, &format!("kn3-cleaner-bad-{}", stamp()));
        let key = if bad_key {
            floor_key(&a)
        } else {
            format!("{}|not-an-open|p", a.encode())
        };
        commit(&producer, &topic, &[(partition_of(&a), key, bad_value)]);
        let (mut reader, cleaner) = open_cleaner(&bootstrap);
        let before = ends(&bootstrap, &topic);
        let result = sweep_partition(
            &mut reader,
            &topic,
            partition_of(&a),
            &cleaner,
            &limits(500, 10_000),
        );
        assert!(
            matches!(result, Err(CleanerError::Integrity { .. })),
            "{label}: {result:?}"
        );
        assert!(
            read_committed(&bootstrap, &topic, &before).is_empty(),
            "{label}: nothing published"
        );
    }
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA (isolated broker); run by the kn-native-integration job"]
async fn the_per_sweep_cap_leaves_pending_work_that_converges() {
    let bootstrap = bootstrap();
    let topic = format!("kn3-cleaner-cap-{}", stamp());
    create_compacted(&bootstrap, &topic).await;
    let (a, _b) = two_products();
    let producer = writer(&bootstrap, &format!("kn3-cleaner-writer-{}", stamp()));
    let live = facts(&a, 0..10);
    commit(&producer, &topic, &live);
    publish_expiry(
        &producer,
        &topic,
        PARTITIONS,
        &expiry_plan(&a, &live, 8),
        1,
        TIMEOUT,
    )
    .unwrap();
    // Five late facts below the floor.
    let late: Vec<(i32, String, Option<Vec<u8>>)> = (0..5)
        .map(|minute| {
            let (key, value) = fact(&a, minute, BarLifecycle::Revised, 1, 9);
            (partition_of(&a), key, Some(value))
        })
        .collect();
    commit(&producer, &topic, &late);
    let (mut reader, cleaner) = open_cleaner(&bootstrap);
    let mut published = Vec::new();
    let mut reports: Vec<SweepReport> = Vec::new();
    for _ in 0..4 {
        let before = ends(&bootstrap, &topic);
        let report = sweep_partition(
            &mut reader,
            &topic,
            partition_of(&a),
            &cleaner,
            &limits(1, 2),
        )
        .unwrap();
        published.extend(
            read_committed(&bootstrap, &topic, &before)
                .into_iter()
                .map(|(_, key, _)| key),
        );
        reports.push(report);
    }
    let steps: Vec<(u64, u64, u64)> = reports
        .iter()
        .map(|r| (r.tombstones_published, r.transactions, r.pending))
        .collect();
    assert_eq!(steps, vec![(2, 2, 3), (2, 2, 1), (1, 1, 0), (0, 0, 0)]);
    published.sort();
    let mut expected: Vec<String> = late.into_iter().map(|(_, key, _)| key).collect();
    expected.sort();
    assert_eq!(published, expected, "each orphan exactly once");
}

/// Delegates to the cleaner's producer but fails the `fail_at`-th send.
struct FailingSink<'a> {
    inner: &'a ThreadedProducer<DefaultProducerContext>,
    sent: Cell<usize>,
    fail_at: usize,
}

impl ExpirySink for FailingSink<'_> {
    fn begin(&self) -> Result<(), String> {
        self.inner.begin()
    }
    fn send_record(
        &self,
        topic: &str,
        partition: i32,
        key: &str,
        payload: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<(), String> {
        self.sent.set(self.sent.get() + 1);
        if self.sent.get() == self.fail_at {
            return Err("injected send failure".into());
        }
        self.inner
            .send_record(topic, partition, key, payload, timeout)
    }
    fn commit(&self, timeout: Duration) -> Result<(), String> {
        self.inner.commit(timeout)
    }
    fn abort(&self, timeout: Duration) -> Result<(), String> {
        self.inner.abort(timeout)
    }
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA (isolated broker); run by the kn-native-integration job"]
async fn an_aborted_sweep_transaction_leaves_nothing_visible() {
    let bootstrap = bootstrap();
    let (topic, a, _b, orphans) = orphaned_topic(&bootstrap).await;
    let (mut reader, producer) = open_cleaner(&bootstrap);
    let before = ends(&bootstrap, &topic);
    let failing = FailingSink {
        inner: &producer,
        sent: Cell::new(0),
        fail_at: 2,
    };
    let result = sweep_partition(
        &mut reader,
        &topic,
        partition_of(&a),
        &failing,
        &limits(500, 10_000),
    );
    match result {
        Err(CleanerError::Publish(error)) => assert!(error.contains("aborted"), "{error}"),
        other => panic!("expected an aborted publish, got {other:?}"),
    }
    assert!(
        read_committed(&bootstrap, &topic, &before).is_empty(),
        "nothing visible"
    );
    // The next sweep publishes the orphans.
    let report = sweep_partition(
        &mut reader,
        &topic,
        partition_of(&a),
        &producer,
        &limits(500, 10_000),
    )
    .unwrap();
    assert_eq!(report.tombstones_published, 2);
    assert_tombstones(
        &read_committed(&bootstrap, &topic, &before),
        partition_of(&a),
        &orphans,
    );
}
