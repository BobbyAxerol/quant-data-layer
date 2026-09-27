//! K3.5 BAR expiry over a real, isolated Kafka broker and a real, isolated
//! Redis (never production): bars are committed to a fresh compacted state
//! topic, stage B builds the cache from it, the expiry plan is published in
//! one transaction (tombstones, then the RETENTION_FLOOR frame, on the
//! product's partition only) and stage B applies the floor; a failed publish
//! (injected send failure -> abort, fenced producer) leaves nothing visible.
//!
//! `QDL_KN_TEST_KAFKA=host:port QDL_KN_TEST_REDIS=redis://host:port
//!  cargo test -p qdl-projector --test expiry_kafka -- --ignored`.

use prost::Message as _;
use qdl_contracts::qdl::marketdata::v2::{
    event_envelope::Payload, Bar, BarLifecycle, EventEnvelope,
};
use qdl_contracts::state_codec::{floor_key, state_partition, FrameKind, StateFrame};
use qdl_contracts::state_contract::{LogicalProductKey, SourceCoordinate};
use qdl_projector::cache::{Cache, Layout};
use qdl_projector::expiry::{plan_expiry, publish_expiry, ExpiryPlan, ExpirySink};
use qdl_projector::kafka_state::{KafkaStateSettings, KafkaStateSource};
use qdl_projector::stage_b::{StageB, StageBLimits};
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

const UID: &str = "fb26214c-7b9b-5961-95b2-55154755af0f";
const PARTITIONS: u32 = 2;
const MIN: u64 = 60_000;
const TIMEOUT: Duration = Duration::from_secs(10);

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!("{name} must name an isolated test service; this test never runs against production")
    })
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

fn producer(bootstrap: &str, transactional_id: &str) -> BaseProducer {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("transactional.id", transactional_id)
        .set("enable.idempotence", "true")
        .create()
        .unwrap();
    producer.init_transactions(Duration::from_secs(20)).unwrap();
    producer
}

fn bar_frame(lpk: &LogicalProductKey, minute: u64) -> (String, Vec<u8>) {
    let envelope = EventEnvelope {
        event_id: (minute + 1).to_be_bytes().to_vec(),
        instrument_uid: UID.into(),
        venue: "OKX".into(),
        market: "SWAP".into(),
        payload: Some(Payload::Bar(Bar {
            interval: "1m".into(),
            open_time_ns: (minute * MIN * 1_000_000) as i64,
            close_time_ns: ((minute + 1) * MIN * 1_000_000 - 1) as i64,
            is_final: true,
            lifecycle: BarLifecycle::Final as i32,
            trade_count: minute,
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
            offset: 100 + minute,
        },
        1,
    )
    .unwrap();
    (frame.key().unwrap(), frame.encode().unwrap())
}

/// Commit `minutes` final bars of `lpk` to its state partition.
fn produce_bars(producer: &BaseProducer, topic: &str, lpk: &LogicalProductKey, minutes: u64) {
    let partition = state_partition(lpk, PARTITIONS).unwrap() as i32;
    producer.begin_transaction().unwrap();
    for minute in 0..minutes {
        let (key, value) = bar_frame(lpk, minute);
        producer
            .send(
                BaseRecord::to(topic)
                    .partition(partition)
                    .key(&key)
                    .payload(&value),
            )
            .unwrap();
    }
    producer.commit_transaction(TIMEOUT).unwrap();
}

fn stage(bootstrap: &str, topic: &str, id: u128) -> StageB<KafkaStateSource> {
    let source = KafkaStateSource::open(&KafkaStateSettings {
        bootstrap: bootstrap.into(),
        topics: vec![topic.into()],
        group_id: format!("kn-projector-test-expiry-{id}"),
        client_id: "expiry-b-0".into(),
        tls: None,
    })
    .unwrap();
    let cache = Cache::connect(
        &env("QDL_KN_TEST_REDIS"),
        Layout::new(&format!("expiry{id}")),
    )
    .unwrap();
    StageB::new(
        source,
        cache,
        StageBLimits {
            poll_timeout: Duration::from_millis(100),
            ..StageBLimits::default()
        },
    )
}

fn rows(stage: &mut StageB<KafkaStateSource>, lpk: &LogicalProductKey) -> Option<u64> {
    let generation = stage.cache.pointer(&lpk.encode()).unwrap().ready?;
    let rows: Option<String> = redis::cmd("HGET")
        .arg(stage.cache.layout.bar_meta(generation, &lpk.encode()))
        .arg("rows")
        .query(stage.cache.connection())
        .unwrap();
    rows.and_then(|rows| rows.parse().ok())
}

fn until<F: FnMut() -> bool>(deadline: Duration, mut done: F) -> bool {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if done() {
            return true;
        }
    }
    false
}

/// A fresh topic with `minutes` committed bars, built into the cache by
/// stage B, and the expiry plan for `cap`.
async fn prepared(
    minutes: u64,
    cap: u64,
) -> (
    String,
    String,
    LogicalProductKey,
    StageB<KafkaStateSource>,
    ExpiryPlan,
) {
    let bootstrap = env("QDL_KN_TEST_KAFKA");
    let id = stamp();
    let topic = format!("kn3-expiry-{id}");
    create_compacted(&bootstrap, &topic).await;
    let lpk = LogicalProductKey::new("paper", "OKX", "SWAP", UID, "BAR", Some("1m")).unwrap();
    let bars = producer(&bootstrap, &format!("kn3-expiry-bars-{id}"));
    produce_bars(&bars, &topic, &lpk, minutes);
    let mut stage = stage(&bootstrap, &topic, id);
    assert!(
        until(Duration::from_secs(60), || {
            stage.step().unwrap();
            rows(&mut stage, &lpk) == Some(minutes)
        }),
        "stage B built the product from Kafka"
    );
    let generation = stage.cache.pointer(&lpk.encode()).unwrap().ready.unwrap();
    let plan = plan_expiry(&mut stage.cache, generation, &lpk, cap, 100_000)
        .unwrap()
        .expect("a plan");
    (bootstrap, topic, lpk, stage, plan)
}

/// High watermark per partition.
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

/// Every committed record from `from` to the current end, per partition in
/// offset order: `(partition, key, payload)`.
fn read_committed(bootstrap: &str, topic: &str, from: &BTreeMap<i32, i64>) -> Visible {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("kn3-expiry-verify-{}", stamp()))
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

/// Exactly the plan: every tombstone, then the floor frame, on the product's
/// partition and nowhere else.
fn assert_exactly(visible: &Visible, plan: &ExpiryPlan) {
    let partition = state_partition(&plan.lpk, PARTITIONS).unwrap() as i32;
    assert_eq!(visible.len(), plan.tombstone_keys.len() + 1, "nothing else");
    assert!(
        visible.iter().all(|(p, _, _)| *p == partition),
        "product partition only"
    );
    for ((_, key, payload), expected) in visible.iter().zip(&plan.tombstone_keys) {
        assert_eq!(key, expected);
        assert!(payload.is_none(), "tombstone {key}");
    }
    let (_, key, payload) = visible.last().unwrap();
    assert_eq!(key, &floor_key(&plan.lpk));
    let frame = StateFrame::decode(payload.as_deref().expect("floor frame")).unwrap();
    assert_eq!(frame.kind, FrameKind::RetentionFloor);
    assert_eq!(frame.floor_open_time_ms, Some(plan.floor_ms));
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA and QDL_KN_TEST_REDIS (isolated); run by the kn-native-integration job"]
async fn a_plan_is_one_transaction_on_the_product_partition_and_stage_b_applies_it() {
    let (bootstrap, topic, lpk, mut stage, plan) = prepared(250, 100).await;
    assert_eq!(plan.floor_ms, 150 * MIN);
    assert_eq!(plan.expired_opens, 150);
    assert_eq!(
        plan.tombstone_keys.len(),
        300,
        "final + in-progress key per open"
    );
    let before = ends(&bootstrap, &topic);
    // The threaded producer sink (a background thread serves its delivery
    // reports); the base producer sink is exercised by the failure test.
    let expiry: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("transactional.id", format!("kn3-expiry-task-{}", stamp()))
        .set("enable.idempotence", "true")
        .create()
        .unwrap();
    expiry.init_transactions(Duration::from_secs(20)).unwrap();
    publish_expiry(&expiry, &topic, PARTITIONS, &plan, 1, TIMEOUT).unwrap();
    assert_exactly(&read_committed(&bootstrap, &topic, &before), &plan);
    assert!(
        until(Duration::from_secs(60), || {
            stage.step().unwrap();
            rows(&mut stage, &lpk) == Some(100)
        }),
        "stage B applied the floor from Kafka"
    );
}

/// Delegates to a real producer but fails the `fail_at`-th send.
struct FailingSink<'a> {
    inner: &'a BaseProducer,
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
#[ignore = "requires QDL_KN_TEST_KAFKA and QDL_KN_TEST_REDIS (isolated); run by the kn-native-integration job"]
async fn a_failed_publish_leaves_nothing_visible() {
    let (bootstrap, topic, _lpk, _stage, plan) = prepared(230, 100).await;
    let before = ends(&bootstrap, &topic);
    let transactional_id = format!("kn3-expiry-task-{}", stamp());
    let zombie = producer(&bootstrap, &transactional_id);
    // (1) A send fails mid-transaction after 10 records were produced: abort.
    let failing = FailingSink {
        inner: &zombie,
        sent: Cell::new(0),
        fail_at: 11,
    };
    let error = publish_expiry(&failing, &topic, PARTITIONS, &plan, 1, TIMEOUT).unwrap_err();
    assert!(error.contains("aborted"), "{error}");
    // (2) A newer instance with the same transactional id fences the old one.
    let current = producer(&bootstrap, &transactional_id);
    let error = publish_expiry(&zombie, &topic, PARTITIONS, &plan, 1, TIMEOUT).unwrap_err();
    assert!(error.contains("fenced"), "{error}");
    // Only the current instance's committed plan becomes visible.
    publish_expiry(&current, &topic, PARTITIONS, &plan, 1, TIMEOUT).unwrap();
    assert_exactly(&read_committed(&bootstrap, &topic, &before), &plan);
}
