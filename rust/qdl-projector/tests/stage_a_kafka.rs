//! K3-T01 over a real, isolated Kafka broker (never production).
//!
//! `QDL_KN_TEST_KAFKA=host:port cargo test -p qdl-projector --test stage_a_kafka -- --ignored`;
//! without the variable the tests fail loudly instead of skipping.

use qdl_projector::kafka_pipe::{KafkaPipe, KafkaPipeSettings};
use qdl_projector::stage_a::{
    InputRecord, OutputRecord, Pipe, StageA, StageALimits, Step, Transform, TransformError,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::Message;
use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn bootstrap() -> String {
    std::env::var("QDL_KN_TEST_KAFKA")
        .expect("QDL_KN_TEST_KAFKA must name an isolated test broker; this test never runs against production")
}

fn stamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
}

async fn create_topics(bootstrap: &str, topics: &[&str], partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin");
    let new: Vec<NewTopic> = topics
        .iter()
        .map(|topic| NewTopic::new(topic, partitions, TopicReplication::Fixed(1)))
        .collect();
    for result in admin
        .create_topics(
            &new,
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .expect("create topics")
    {
        result.expect("topic created");
    }
}

fn produce_inputs(bootstrap: &str, topic: &str, count: usize) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("producer");
    for index in 0..count {
        let key = format!("k{index}");
        let payload = format!("v{index}");
        producer
            .send(
                BaseRecord::to(topic)
                    .partition((index % 2) as i32)
                    .key(&key)
                    .payload(&payload),
            )
            .expect("send");
    }
    producer.flush(Duration::from_secs(10)).expect("flush");
}

/// Every committed record of `topic` as `(key, value)`.
fn read_committed(bootstrap: &str, topic: &str, partitions: i32) -> Vec<(String, String)> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("kn3-verify-{}", stamp()))
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .create()
        .expect("verifier");
    let mut assignment = TopicPartitionList::new();
    let mut ends = BTreeMap::new();
    for partition in 0..partitions {
        assignment
            .add_partition_offset(topic, partition, Offset::Beginning)
            .unwrap();
        let (_, high) = consumer
            .fetch_watermarks(topic, partition, Duration::from_secs(10))
            .expect("watermarks");
        ends.insert(partition, high);
    }
    consumer.assign(&assignment).expect("assign");
    let mut records = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        let position = consumer.position().ok();
        let done = ends.iter().all(|(&partition, &high)| {
            let at = position
                .as_ref()
                .and_then(|list| list.find_partition(topic, partition))
                .map(|element| element.offset());
            match at {
                Some(Offset::Offset(at)) => at >= high,
                _ => high == 0,
            }
        });
        if done {
            break;
        }
        if let Some(Ok(message)) = consumer.poll(Duration::from_millis(100)) {
            records.push((
                String::from_utf8_lossy(message.key().unwrap_or_default()).into_owned(),
                String::from_utf8_lossy(message.payload().unwrap_or_default()).into_owned(),
            ));
        }
    }
    records.sort();
    records
}

/// Poll until at least one record arrives (the first polls may only join
/// the group) or the deadline passes.
fn poll_some(pipe: &mut KafkaPipe, max: usize) -> Vec<InputRecord> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let batch = pipe.poll(max, Duration::from_millis(500)).expect("poll");
        if !batch.is_empty() || Instant::now() >= deadline {
            return batch;
        }
    }
}

struct Copy {
    topic: String,
}

impl Transform for Copy {
    fn transform(&mut self, record: &InputRecord) -> Result<Vec<OutputRecord>, TransformError> {
        Ok(vec![OutputRecord {
            topic: self.topic.clone(),
            partition: record.partition,
            key: record.key.clone(),
            value: Some(record.payload.clone()),
        }])
    }
}

fn settings(
    bootstrap: &str,
    input: &str,
    group: &str,
    transactional_id: &str,
) -> KafkaPipeSettings {
    KafkaPipeSettings {
        bootstrap: bootstrap.into(),
        input_topic: input.into(),
        group_id: group.into(),
        transactional_id: transactional_id.into(),
        client_id: transactional_id.into(),
        tls: None,
        transaction_timeout: Duration::from_secs(10),
    }
}

fn drain<P: Pipe, T: Transform>(stage: &mut StageA<P, T>, inputs: u64, deadline: Duration) {
    let until = Instant::now() + deadline;
    while stage.metrics.inputs < inputs && Instant::now() < until {
        stage.step().expect("stage A step");
    }
}

fn expected(count: usize) -> Vec<(String, String)> {
    let mut all: Vec<(String, String)> = (0..count)
        .map(|index| (format!("k{index}"), format!("v{index}")))
        .collect();
    all.sort();
    all
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA (isolated broker); run by the kn-native-integration job"]
async fn committed_batches_publish_state_and_offsets_exactly_once() {
    let bootstrap = bootstrap();
    let id = stamp();
    let (input, output) = (format!("kn3-in-{id}"), format!("kn3-out-{id}"));
    create_topics(&bootstrap, &[&input, &output], 2).await;
    produce_inputs(&bootstrap, &input, 40);
    let group = format!("kn-projector-test-{id}");
    let pipe = KafkaPipe::open(settings(
        &bootstrap,
        &input,
        &group,
        &format!("kn-projector-test-{id}-0"),
    ))
    .expect("pipe");
    let mut stage = StageA::new(
        pipe,
        Copy {
            topic: output.clone(),
        },
        StageALimits {
            max_batch_records: 7,
            poll_timeout: Duration::from_millis(200),
        },
    );
    drain(&mut stage, 40, Duration::from_secs(30));
    assert_eq!(stage.metrics.inputs, 40);
    assert!(stage.metrics.transactions >= 6, "bounded batches");
    assert_eq!(read_committed(&bootstrap, &output, 2), expected(40));
    // The group's committed offsets are exactly the input end.
    let committed = stage
        .pipe
        .consumer()
        .committed(Duration::from_secs(10))
        .expect("committed");
    let mut positions: Vec<(i32, i64)> = committed
        .elements()
        .iter()
        .filter_map(|element| match element.offset() {
            Offset::Offset(at) => Some((element.partition(), at)),
            _ => None,
        })
        .collect();
    positions.sort();
    assert_eq!(positions, vec![(0, 20), (1, 20)]);
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA (isolated broker); run by the kn-native-integration job"]
async fn a_crash_inside_a_transaction_leaves_nothing_and_a_restart_is_exact() {
    let bootstrap = bootstrap();
    let id = stamp();
    let (input, output) = (format!("kn3-in-{id}"), format!("kn3-out-{id}"));
    create_topics(&bootstrap, &[&input, &output], 2).await;
    produce_inputs(&bootstrap, &input, 10);
    let group = format!("kn-projector-test-{id}");
    let transactional_id = format!("kn-projector-test-{id}-0");
    {
        // The first instance publishes inside an open transaction and dies.
        let mut crashed =
            KafkaPipe::open(settings(&bootstrap, &input, &group, &transactional_id)).expect("pipe");
        let polled = poll_some(&mut crashed, 10);
        assert!(!polled.is_empty());
        crashed.begin().expect("begin");
        crashed
            .send(&OutputRecord {
                topic: output.clone(),
                partition: 0,
                key: b"orphan".to_vec(),
                value: Some(b"never".to_vec()),
            })
            .expect("send");
        // Dropped without commit or abort.
    }
    // Same transactional id: init_transactions fences and aborts the orphan.
    let pipe = KafkaPipe::open(settings(&bootstrap, &input, &group, &transactional_id))
        .expect("restarted pipe");
    let mut stage = StageA::new(
        pipe,
        Copy {
            topic: output.clone(),
        },
        StageALimits::default(),
    );
    drain(&mut stage, 10, Duration::from_secs(30));
    let published = read_committed(&bootstrap, &output, 2);
    assert!(!published.iter().any(|(key, _)| key == "orphan"));
    assert_eq!(published, expected(10));
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA (isolated broker); run by the kn-native-integration job"]
async fn a_zombie_with_the_same_transactional_id_is_fenced() {
    let bootstrap = bootstrap();
    let id = stamp();
    let (input, output) = (format!("kn3-in-{id}"), format!("kn3-out-{id}"));
    create_topics(&bootstrap, &[&input, &output], 2).await;
    produce_inputs(&bootstrap, &input, 4);
    let group = format!("kn-projector-test-{id}");
    let transactional_id = format!("kn-projector-test-{id}-0");
    let mut zombie =
        KafkaPipe::open(settings(&bootstrap, &input, &group, &transactional_id)).expect("zombie");
    let batch = poll_some(&mut zombie, 4);
    assert!(!batch.is_empty(), "the zombie holds input records");
    zombie.begin().expect("begin");
    for record in &batch {
        zombie
            .send(&OutputRecord {
                topic: output.clone(),
                partition: record.partition,
                key: b"zombie".to_vec(),
                value: Some(record.payload.clone()),
            })
            .expect("send");
    }
    // A replacement instance takes over the transactional id.
    let replacement =
        KafkaPipe::open(settings(&bootstrap, &input, &group, &transactional_id)).expect("new");
    let mut next = BTreeMap::new();
    for record in &batch {
        next.insert(record.partition, record.offset + 1);
    }
    let error = zombie.commit(&next).expect_err("the zombie must be fenced");
    assert!(
        matches!(error, qdl_projector::stage_a::PipeError::Fatal(_)),
        "{error:?}"
    );
    drop(zombie);
    let mut stage = StageA::new(
        replacement,
        Copy {
            topic: output.clone(),
        },
        StageALimits::default(),
    );
    drain(&mut stage, 4, Duration::from_secs(30));
    let published = read_committed(&bootstrap, &output, 2);
    assert!(!published.iter().any(|(key, _)| key == "zombie"));
    assert_eq!(published, expected(4));
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA (isolated broker); run by the kn-native-integration job"]
async fn two_members_rebalancing_publish_every_input_exactly_once() {
    let bootstrap = bootstrap();
    let id = stamp();
    let (input, output) = (format!("kn3-in-{id}"), format!("kn3-out-{id}"));
    create_topics(&bootstrap, &[&input, &output], 2).await;
    produce_inputs(&bootstrap, &input, 60);
    let group = format!("kn-projector-test-{id}");
    let open = |replica: usize| {
        KafkaPipe::open(settings(
            &bootstrap,
            &input,
            &group,
            &format!("kn-projector-test-{id}-{replica}"),
        ))
        .expect("pipe")
    };
    let limits = StageALimits {
        max_batch_records: 3,
        poll_timeout: Duration::from_millis(100),
    };
    let mut first = StageA::new(
        open(0),
        Copy {
            topic: output.clone(),
        },
        limits.clone(),
    );
    // The first member works alone for a while, then a second joins.
    let until = Instant::now() + Duration::from_secs(20);
    while first.metrics.inputs < 12 && Instant::now() < until {
        first.step().expect("first");
    }
    let mut second = StageA::new(
        open(1),
        Copy {
            topic: output.clone(),
        },
        limits,
    );
    let until = Instant::now() + Duration::from_secs(40);
    while first.metrics.inputs + second.metrics.inputs < 60 && Instant::now() < until {
        for stage in [&mut first, &mut second] {
            match stage.step().expect("step") {
                Step::Idle | Step::Committed { .. } | Step::Aborted(_) => {}
            }
        }
    }
    assert_eq!(read_committed(&bootstrap, &output, 2), expected(60));
    assert_eq!(first.metrics.inputs + second.metrics.inputs, 60);
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA (isolated broker)"]
async fn commit_succeeded_but_ack_was_lost_restart_does_not_duplicate() {
    let bootstrap = bootstrap();
    let id = stamp();
    let (input, output) = (format!("kn3-ack-in-{id}"), format!("kn3-ack-out-{id}"));
    create_topics(&bootstrap, &[&input, &output], 2).await;
    produce_inputs(&bootstrap, &input, 12);
    let group = format!("kn-projector-ack-{id}");
    let tx = format!("kn-projector-ack-{id}-0");
    {
        let mut pipe = KafkaPipe::open(settings(&bootstrap, &input, &group, &tx)).unwrap();
        let batch = poll_some(&mut pipe, 12);
        assert!(!batch.is_empty());
        let mut next = BTreeMap::new();
        pipe.begin().unwrap();
        for record in batch {
            pipe.send(&OutputRecord {
                topic: output.clone(),
                partition: record.partition,
                key: record.key,
                value: Some(record.payload),
            })
            .unwrap();
            next.insert(record.partition, record.offset + 1);
        }
        pipe.commit(&next).unwrap();
        // Fault seam: durable broker commit, caller loses its response/state.
        // This is not a network packet-drop claim.
    }
    let pipe = KafkaPipe::open(settings(&bootstrap, &input, &group, &tx)).unwrap();
    let mut stage = StageA::new(
        pipe,
        Copy {
            topic: output.clone(),
        },
        StageALimits::default(),
    );
    let until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < until {
        stage.step().unwrap();
    }
    assert_eq!(read_committed(&bootstrap, &output, 2), expected(12));
}

/// Real captured provider envelopes, real Kafka transaction coordinator outage,
/// then the actual Stage B -> Redis path. The controller pauses ONLY test Kafka.
#[tokio::test]
#[ignore = "requires isolated Kafka/Redis and QDL_RECOVERY_FAULT_DIR controller"]
async fn captured_frames_recover_after_broker_outage_without_loss_or_cache_regression() {
    use base64::Engine as _;
    use qdl_contracts::state_codec::{decode_latest_value, state_partition};
    use qdl_contracts::state_contract::LogicalProductKey;
    use qdl_projector::cache::{Cache, Layout};
    use qdl_projector::kafka_state::{KafkaStateSettings, KafkaStateSource};
    use qdl_projector::stage_b::{StageB, StageBLimits};
    let fault = std::path::PathBuf::from(std::env::var("QDL_RECOVERY_FAULT_DIR").unwrap());
    let bootstrap = bootstrap();
    let id = stamp();
    let (input, output) = (
        format!("kn-recovery-in-{id}"),
        format!("kn-recovery-out-{id}"),
    );
    create_topics(&bootstrap, &[&input, &output], 2).await;
    let golden: serde_json::Value = serde_json::from_str(include_str!(
        "../../../contracts/golden/kn_v220/state_codec.json"
    ))
    .unwrap();
    let real: std::collections::BTreeSet<_> = golden["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["synthetic"] == false)
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    let mut expected = BTreeMap::new();
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("acks", "all")
        .create()
        .unwrap();
    let mut count = 0u64;
    for record in golden["records"].as_array().unwrap() {
        if !real.contains(record["name"].as_str().unwrap()) {
            continue;
        }
        let key = record["lpk"].as_str().unwrap();
        let lpk = LogicalProductKey::parse(key).unwrap();
        let canonical = base64::engine::general_purpose::STANDARD
            .decode(record["canonical_b64"].as_str().unwrap())
            .unwrap();
        let source = &record["source"];
        let frame = qdl_contracts::state_codec::StateFrame::latest(
            &canonical,
            &lpk,
            qdl_contracts::state_contract::SourceCoordinate {
                topic_id: source["topic_id"].as_str().unwrap().into(),
                partition: source["partition"].as_u64().unwrap() as u32,
                offset: source["offset"].as_u64().unwrap(),
            },
            record["materializer_epoch"].as_u64().unwrap(),
        )
        .unwrap();
        let bytes = frame.encode().unwrap();
        producer
            .send(
                BaseRecord::to(&input)
                    .partition(state_partition(&lpk, 2).unwrap() as i32)
                    .key(key)
                    .payload(&bytes),
            )
            .unwrap();
        expected.insert(key.to_owned(), canonical);
        count += 1;
    }
    producer.flush(Duration::from_secs(10)).unwrap();
    assert!(count >= 20, "real capture coverage");
    let group = format!("kn-recovery-{id}");
    let tx = format!("kn-recovery-{id}-0");
    let mut pipe = KafkaPipe::open(settings(&bootstrap, &input, &group, &tx)).unwrap();
    let batch = poll_some(&mut pipe, 100);
    assert!(!batch.is_empty());
    pipe.begin().unwrap();
    let mut next = BTreeMap::new();
    for record in batch {
        pipe.send(&OutputRecord {
            topic: output.clone(),
            partition: record.partition,
            key: record.key,
            value: Some(record.payload),
        })
        .unwrap();
        next.insert(record.partition, record.offset + 1);
    }
    std::fs::write(fault.join("ready"), b"pause isolated test broker").unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !fault.join("paused").exists() {
        assert!(Instant::now() < deadline, "fault controller absent");
        std::thread::sleep(Duration::from_millis(10));
    }
    let outage_started = Instant::now();
    let outcome = pipe.commit(&next);
    assert!(
        outage_started.elapsed() < Duration::from_secs(12),
        "shared10s budget plus2s scheduling"
    );
    // A commit may have succeeded before outage or may be indeterminate.
    // Retire either instance; init_transactions fences it before committed replay.
    drop(pipe);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !fault.join("restored").exists() {
        assert!(Instant::now() < deadline, "broker was not restored");
        std::thread::sleep(Duration::from_millis(10));
    }
    let restored = Instant::now();
    let pipe = KafkaPipe::open(settings(&bootstrap, &input, &group, &tx)).unwrap();
    let mut stage = StageA::new(
        pipe,
        Copy {
            topic: output.clone(),
        },
        StageALimits::default(),
    );
    let until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < until {
        stage.step().unwrap();
    }
    let mut committed_count = read_committed(&bootstrap, &output, 2).len();
    while committed_count != count as usize && restored.elapsed() < Duration::from_secs(120) {
        // Group ownership recovery is asynchronous; readiness is output progress,
        // not a fixed sleep after recreating a client.
        for _ in 0..20 {
            stage.step().unwrap();
        }
        committed_count = read_committed(&bootstrap, &output, 2).len();
    }
    assert_eq!(
        committed_count, count as usize,
        "commit outcome {outcome:?}"
    );
    let redis_url = std::env::var("QDL_KN_TEST_REDIS").unwrap();
    let layout = Layout::new(&format!("recovery-{id}"));
    let open = || {
        let source = KafkaStateSource::open(&KafkaStateSettings {
            bootstrap: bootstrap.clone(),
            topics: vec![output.clone()],
            group_id: format!("kn-recovery-b-{id}"),
            client_id: format!("kn-recovery-b-{id}"),
            tls: None,
        })
        .unwrap();
        StageB::new(
            source,
            Cache::connect(&redis_url, layout.clone()).unwrap(),
            StageBLimits::default(),
        )
    };
    let mut b = open();
    let mut verified = false;
    while restored.elapsed() < Duration::from_secs(120) {
        b.step().unwrap();
        verified = expected.iter().all(|(key, canonical)| {
            let Some(generation) = b.cache.pointer(key).unwrap().ready else {
                return false;
            };
            let value: Option<Vec<u8>> = redis::cmd("HGET")
                .arg(b.cache.layout.latest(generation, key))
                .arg("v")
                .query(b.cache.connection())
                .unwrap();
            value.is_some_and(|v| decode_latest_value(&v).unwrap().canonical == *canonical)
        });
        if verified {
            break;
        }
    }
    assert!(
        verified,
        "all captured bytes materialized within frozen120s RTO"
    );
    let mut offsets = BTreeMap::new();
    for key in expected.keys() {
        let generation = b.cache.pointer(key).unwrap().ready.unwrap();
        offsets.insert(
            key.clone(),
            b.cache.latest_coordinate(generation, key).unwrap(),
        );
    }
    drop(b);
    let mut b = open();
    for _ in 0..50 {
        b.step().unwrap();
    }
    for (key, coordinate) in offsets {
        let generation = b.cache.pointer(&key).unwrap().ready.unwrap();
        assert_eq!(
            b.cache.latest_coordinate(generation, &key).unwrap(),
            coordinate
        );
    }
    let receipt = serde_json::json!({"captured_records":count, "cache_products":expected.len(),
        "commit_outcome":format!("{outcome:?}"), "peak_backlog_bound_records":count,
        "recovery_to_verified_cache_ms":restored.elapsed().as_millis(),
        "scope":"isolated provider capture; old timestamps remain old, not live eligibility"});
    std::fs::write(fault.join("receipt.json"), receipt.to_string()).unwrap();
}
