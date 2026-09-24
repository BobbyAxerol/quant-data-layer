//! K1-T04 against a real, isolated Kafka broker (never the production stack).
//!
//! Run by the KN native-integration job:
//! `QDL_KN_TEST_KAFKA=host:port cargo test -p qdl-stream-gateway --test committed_reads -- --ignored`.
//! Without the variable the test fails loudly instead of skipping.

use qdl_stream_gateway::reader::{assign_from, latest_for_key, position, KafkaSettings};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn bootstrap() -> String {
    std::env::var("QDL_KN_TEST_KAFKA")
        .expect("QDL_KN_TEST_KAFKA must name an isolated test broker; this test never runs against production")
}

async fn create_topic(bootstrap: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin");
    let results = admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .expect("create topic");
    for result in results {
        result.expect("topic created");
    }
}

fn transactional_producer(bootstrap: &str, id: &str) -> BaseProducer {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("transactional.id", id)
        .set("enable.idempotence", "true")
        .create()
        .expect("producer");
    producer
        .init_transactions(Duration::from_secs(20))
        .expect("init transactions");
    producer
}

fn send_batch(producer: &BaseProducer, topic: &str, records: &[(&str, &str)], commit: bool) {
    producer.begin_transaction().expect("begin");
    for (key, value) in records {
        producer
            .send(BaseRecord::to(topic).key(*key).payload(*value))
            .expect("send");
    }
    producer.flush(Duration::from_secs(10)).expect("flush");
    if commit {
        producer
            .commit_transaction(Duration::from_secs(10))
            .expect("commit");
    } else {
        producer
            .abort_transaction(Duration::from_secs(10))
            .expect("abort");
    }
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA (isolated broker); run by the kn-native-integration job"]
async fn only_committed_records_reach_the_reader_and_nothing_is_committed() {
    let bootstrap = bootstrap();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let topic = format!("kn1-committed-{stamp}");
    create_topic(&bootstrap, &topic).await;
    let producer = transactional_producer(&bootstrap, &format!("kn1-test-tx-{stamp}"));
    send_batch(&producer, &topic, &[("K", "c1"), ("K", "c2")], true);
    send_batch(&producer, &topic, &[("K", "a1"), ("K", "a2")], false);
    send_batch(&producer, &topic, &[("K", "c3"), ("OTHER", "o1")], true);

    let settings = KafkaSettings {
        bootstrap: bootstrap.clone(),
        topic: topic.clone(),
        group_id: format!("kn-committed-reads-{stamp}"),
        client_id: "kn1-committed-reads-test".into(),
        tls: None,
        fetch_wait_ms: 10,
    };
    let consumer = settings.stream_consumer().expect("consumer");
    assign_from(&consumer, &topic, 0, 0).expect("assign");
    let high = consumer
        .fetch_watermarks(&topic, 0, Duration::from_secs(10))
        .expect("watermarks")
        .1;
    let mut seen: Vec<(String, String, i64)> = Vec::new();
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        if position(&consumer, &topic, 0).is_some_and(|next| next >= high) {
            break;
        }
        if let Ok(Ok(message)) =
            tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await
        {
            seen.push((
                String::from_utf8_lossy(message.key().unwrap_or_default()).into_owned(),
                String::from_utf8_lossy(message.payload().unwrap_or_default()).into_owned(),
                message.offset(),
            ));
        }
    }
    let values: Vec<&str> = seen
        .iter()
        .filter(|(key, _, _)| key == "K")
        .map(|(_, value, _)| value.as_str())
        .collect();
    assert_eq!(
        values,
        ["c1", "c2", "c3"],
        "aborted records must be invisible"
    );
    assert!(seen.iter().any(|(key, _, _)| key == "OTHER"));
    // Transaction markers and the aborted batch occupy offsets: gaps are
    // valid and never read as data loss.
    let offsets: Vec<i64> = seen.iter().map(|(_, _, offset)| *offset).collect();
    assert!(
        offsets.windows(2).any(|pair| pair[1] > pair[0] + 1),
        "{offsets:?}"
    );
    assert!(high > offsets.len() as i64);

    let latest = latest_for_key(&settings, b"K", 100, Duration::from_secs(20))
        .expect("probe")
        .expect("latest committed K");
    assert_eq!(latest.payload, b"c3");

    // The reader never commits: its group has no committed offsets. (Against
    // production the principal holds DESCRIBE only on `kn-` groups, so a
    // commit is also refused by the broker.) A fresh broker may answer
    // NotCoordinator until the group coordinator is elected.
    let mut committed = None;
    for _ in 0..20 {
        match consumer.committed(Duration::from_secs(5)) {
            Ok(list) => {
                committed = Some(list);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    let committed = committed.expect("committed offsets after coordinator election");
    assert!(committed
        .elements()
        .iter()
        .all(|element| !matches!(element.offset(), rdkafka::Offset::Offset(_))));
}

/// K2-T02 over real Kafka: the shared live reader (hub) and a separate replay
/// reader split the committed log exactly at the registration barrier, with
/// aborted batches and transaction markers in the range; the live reader is
/// never sought.
#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA (isolated broker); run by the kn-native-integration job"]
async fn hub_barrier_and_replay_reader_split_the_committed_log_exactly() {
    use qdl_stream_gateway::hub::{Hub, HubConfig};
    use qdl_stream_gateway::reader::{KafkaLogSource, KafkaRangeSource};
    use qdl_stream_gateway::replay::{scan_range, Emit, ReplayEnd, ReplayLimits, ReplayMetrics};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    let bootstrap = bootstrap();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let topic = format!("kn2-barrier-{stamp}");
    create_topic(&bootstrap, &topic).await;
    let producer = transactional_producer(&bootstrap, &format!("kn2-test-tx-{stamp}"));
    // Before the hub starts: committed, aborted and other-key records.
    send_batch(
        &producer,
        &topic,
        &[("K", "p1"), ("O", "x1"), ("K", "p2")],
        true,
    );
    send_batch(&producer, &topic, &[("K", "aborted")], false);
    send_batch(&producer, &topic, &[("K", "p3")], true);
    let settings = KafkaSettings {
        bootstrap: bootstrap.clone(),
        topic: topic.clone(),
        group_id: format!("kn-barrier-{stamp}"),
        client_id: "kn2-barrier-test".into(),
        tls: None,
        fetch_wait_ms: 10,
    };
    // The hub starts at the end; the cursor below its start forces the
    // replay reader (no ring), the range after it comes live.
    let (source, starts) = KafkaLogSource::open_at_end(&settings).expect("live source");
    let hub = Arc::new(Hub::new(&starts, HubConfig::default()));
    let reader = hub.run(Box::new(source));
    send_batch(
        &producer,
        &topic,
        &[("K", "l1"), ("K", "aborted-live")],
        false,
    );
    send_batch(&producer, &topic, &[("O", "x2"), ("K", "l2")], true);
    // Wait until the live reader has passed everything committed so far,
    // transaction markers included (it advances on idle polls).
    let high = settings
        .base_consumer()
        .expect("probe consumer")
        .fetch_watermarks(&topic, 0, Duration::from_secs(10))
        .expect("watermarks")
        .1;
    let started = Instant::now();
    while hub.next_offset(0).unwrap_or(0) < high && started.elapsed() < Duration::from_secs(20) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let barrier = hub.next_offset(0).expect("partition 0");
    assert_eq!(barrier, high, "the live reader reached the committed end");
    let range = KafkaRangeSource::new(settings, 2);
    let mut replayed = Vec::new();
    let end = tokio::task::spawn_blocking(move || {
        let mut emit = |record: qdl_stream_gateway::hub::RawRecord| {
            replayed.push(String::from_utf8_lossy(&record.payload).into_owned());
            Emit::Sent
        };
        let end = scan_range(
            &range,
            0,
            -1,
            barrier,
            b"K",
            &ReplayLimits::default(),
            None,
            &AtomicBool::new(false),
            &ReplayMetrics::default(),
            &mut emit,
        );
        (end, replayed, range)
    })
    .await
    .expect("replay task");
    let (end, replayed, range) = end;
    assert_eq!(end, ReplayEnd::Complete);
    assert_eq!(
        replayed,
        ["p1", "p2", "p3", "l2"],
        "committed K records below the barrier only: aborted ones never, O never"
    );
    // The reader went back to the pool and a second replay reuses it.
    assert_eq!(range.idle(), 1);
    let again = tokio::task::spawn_blocking(move || {
        let mut seen = Vec::new();
        let mut emit = |record: qdl_stream_gateway::hub::RawRecord| {
            seen.push(record.offset);
            Emit::Sent
        };
        let end = scan_range(
            &range,
            0,
            -1,
            barrier,
            b"O",
            &ReplayLimits::default(),
            None,
            &AtomicBool::new(false),
            &ReplayMetrics::default(),
            &mut emit,
        );
        (end, seen.len(), range.idle())
    })
    .await
    .expect("second replay");
    assert_eq!(again, (ReplayEnd::Complete, 2, 1));
    hub.stop();
    reader.join().expect("reader thread");
}
