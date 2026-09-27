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
