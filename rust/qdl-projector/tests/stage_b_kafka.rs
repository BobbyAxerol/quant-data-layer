//! Stage B over a real, isolated Kafka broker and a real, isolated Redis
//! (never production): committed state frames reach the cache, aborted ones
//! never do, and a second member of the group takes partitions over with the
//! old owner fenced.
//!
//! `QDL_KN_TEST_KAFKA=host:port QDL_KN_TEST_REDIS=redis://host:port
//!  cargo test -p qdl-projector --test stage_b_kafka -- --ignored`.

use prost::Message;
use qdl_contracts::qdl::marketdata::v2::{
    event_envelope::Payload, Bar, BarLifecycle, EventEnvelope, Quote,
};
use qdl_contracts::state_codec::{
    decode_bar_row, decode_latest_value, state_partition, StateFrame,
};
use qdl_contracts::state_contract::{LogicalProductKey, SourceCoordinate};
use qdl_projector::cache::{bucket_of, Cache, Layout};
use qdl_projector::kafka_state::{KafkaPartitionReader, KafkaStateSettings, KafkaStateSource};
use qdl_projector::stage_b::{StageB, StageBLimits};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer as _;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const UID: &str = "fb26214c-7b9b-5961-95b2-55154755af0f";
const PARTITIONS: u32 = 2;

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

fn quote_frame(lpk: &LogicalProductKey, level: u32, offset: u64) -> (String, Vec<u8>) {
    let envelope = EventEnvelope {
        event_id: offset.to_be_bytes().to_vec(),
        instrument_uid: UID.into(),
        venue: "OKX".into(),
        market: "SWAP".into(),
        payload: Some(Payload::Quote(Quote {
            level,
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec();
    let frame = StateFrame::latest(
        &envelope,
        lpk,
        SourceCoordinate {
            topic_id: "ljfjPYApRpWQd79McfTtZg".into(),
            partition: 1,
            offset,
        },
        1,
    )
    .unwrap();
    (frame.key().unwrap(), frame.encode().unwrap())
}

fn produce(
    producer: &BaseProducer,
    topic: &str,
    lpk: &LogicalProductKey,
    frames: &[(String, Vec<u8>)],
    commit: bool,
) {
    producer.begin_transaction().unwrap();
    let partition = state_partition(lpk, PARTITIONS).unwrap() as i32;
    for (key, value) in frames {
        producer
            .send(
                BaseRecord::to(topic)
                    .partition(partition)
                    .key(key)
                    .payload(value),
            )
            .unwrap();
    }
    producer.flush(Duration::from_secs(10)).unwrap();
    if commit {
        producer
            .commit_transaction(Duration::from_secs(10))
            .unwrap();
    } else {
        producer.abort_transaction(Duration::from_secs(10)).unwrap();
    }
}

fn instance(
    bootstrap: &str,
    topic: &str,
    group: &str,
    environment: &str,
    id: &str,
) -> StageB<KafkaStateSource> {
    let source = KafkaStateSource::open(&KafkaStateSettings {
        bootstrap: bootstrap.into(),
        topics: vec![topic.into()],
        group_id: group.into(),
        client_id: id.into(),
        tls: None,
    })
    .unwrap();
    let cache = Cache::connect(&env("QDL_KN_TEST_REDIS"), Layout::new(environment)).unwrap();
    StageB::new(
        source,
        cache,
        StageBLimits {
            poll_timeout: Duration::from_millis(100),
            ..StageBLimits::default()
        },
    )
}

fn latest_level(
    stage: &mut StageB<KafkaStateSource>,
    lpk: &LogicalProductKey,
) -> Option<(u32, u64)> {
    let generation = stage.cache.pointer(&lpk.encode()).unwrap().ready?;
    let value: Option<Vec<u8>> = redis::cmd("HGET")
        .arg(stage.cache.layout.latest(generation, &lpk.encode()))
        .arg("v")
        .query(stage.cache.connection())
        .unwrap();
    let decoded = decode_latest_value(&value?).unwrap();
    let envelope = EventEnvelope::decode(decoded.canonical.as_slice()).unwrap();
    match envelope.payload {
        Some(Payload::Quote(quote)) => Some((quote.level, decoded.source_offset)),
        _ => None,
    }
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

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA and QDL_KN_TEST_REDIS (isolated); run by the kn-native-integration job"]
async fn committed_frames_reach_the_cache_aborted_never_and_a_new_member_takes_over() {
    let bootstrap = env("QDL_KN_TEST_KAFKA");
    let id = stamp();
    let topic = format!("kn3-latest-{id}");
    create_compacted(&bootstrap, &topic).await;
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("transactional.id", format!("kn3-stage-b-test-{id}"))
        .set("enable.idempotence", "true")
        .create()
        .unwrap();
    producer.init_transactions(Duration::from_secs(20)).unwrap();
    let quotes = LogicalProductKey::new("paper", "OKX", "SWAP", UID, "QUOTE", None).unwrap();
    produce(
        &producer,
        &topic,
        &quotes,
        &[quote_frame(&quotes, 1, 10), quote_frame(&quotes, 2, 11)],
        true,
    );
    // An aborted transaction with a newer offset must never be applied.
    produce(
        &producer,
        &topic,
        &quotes,
        &[quote_frame(&quotes, 99, 50)],
        false,
    );
    let group = format!("kn-projector-test-b-{id}");
    let environment = format!("stageb{id}");
    let mut first = instance(&bootstrap, &topic, &group, &environment, "b-0");
    assert!(
        until(Duration::from_secs(30), || {
            first.step().unwrap();
            latest_level(&mut first, &quotes).is_some()
        }),
        "the product became READY"
    );
    for _ in 0..20 {
        first.step().unwrap();
    }
    assert_eq!(
        latest_level(&mut first, &quotes),
        Some((2, 11)),
        "aborted frame never applied"
    );
    // A second member joins; partitions move with a new owner fence.
    let mut second = instance(&bootstrap, &topic, &group, &environment, "b-1");
    produce(
        &producer,
        &topic,
        &quotes,
        &[quote_frame(&quotes, 3, 12)],
        true,
    );
    assert!(
        until(Duration::from_secs(40), || {
            first.step().unwrap();
            second.step().unwrap();
            latest_level(&mut second, &quotes) == Some((3, 12))
        }),
        "the newest committed frame is applied by whichever member owns it"
    );
    assert_eq!(
        (first.metrics.builds, second.metrics.builds),
        (PARTITIONS as u64, 0),
        "each partition is built once; the new member tails the checkpoints"
    );
}

const MIN: u64 = 60_000;

fn bar_frame(lpk: &LogicalProductKey, minute: u64, close: u32, offset: u64) -> (String, Vec<u8>) {
    let envelope = EventEnvelope {
        event_id: [offset.to_be_bytes(), u64::from(close).to_be_bytes()].concat(),
        instrument_uid: UID.into(),
        venue: "OKX".into(),
        market: "SWAP".into(),
        payload: Some(Payload::Bar(Bar {
            interval: "1m".into(),
            open_time_ns: (minute * MIN * 1_000_000) as i64,
            close_time_ns: ((minute + 1) * MIN * 1_000_000 - 1) as i64,
            is_final: true,
            lifecycle: BarLifecycle::Final as i32,
            trade_count: u64::from(close),
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
            offset,
        },
        1,
    )
    .unwrap();
    (frame.key().unwrap(), frame.encode().unwrap())
}

fn trade_count(
    stage: &mut StageB<KafkaStateSource>,
    lpk: &LogicalProductKey,
    minute: u64,
) -> Option<u64> {
    let generation = stage.cache.pointer(&lpk.encode()).unwrap().ready?;
    let open_ms = minute * MIN;
    let row = stage
        .cache
        .bar_row(generation, &lpk.encode(), bucket_of(open_ms, MIN), open_ms)
        .unwrap()?;
    let envelope =
        EventEnvelope::decode(decode_bar_row(&row, lpk).unwrap().canonical.as_slice()).unwrap();
    match envelope.payload {
        Some(Payload::Bar(bar)) => Some(bar.trade_count),
        _ => None,
    }
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA and QDL_KN_TEST_REDIS (isolated); run by the kn-native-integration job"]
async fn a_product_rebuild_replays_the_committed_partition_through_a_second_reader() {
    let bootstrap = env("QDL_KN_TEST_KAFKA");
    let id = stamp();
    let topic = format!("kn3-bars-{id}");
    create_compacted(&bootstrap, &topic).await;
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("transactional.id", format!("kn3-rebuild-test-{id}"))
        .set("enable.idempotence", "true")
        .create()
        .unwrap();
    producer.init_transactions(Duration::from_secs(20)).unwrap();
    let bars = LogicalProductKey::new("paper", "OKX", "SWAP", UID, "BAR", Some("1m")).unwrap();
    let history: Vec<_> = (0..120u64)
        .map(|minute| bar_frame(&bars, minute, 1, minute))
        .collect();
    produce(&producer, &topic, &bars, &history, true);
    // An aborted conflicting fact must never reach either generation.
    produce(
        &producer,
        &topic,
        &bars,
        &[bar_frame(&bars, 7, 99, 500)],
        false,
    );
    let group = format!("kn-projector-test-rb-{id}");
    let environment = format!("rebuildk{id}");
    let mut stage =
        instance(&bootstrap, &topic, &group, &environment, "rb-0").with_rebuild_reader(Box::new(
            KafkaPartitionReader::open(&bootstrap, &format!("{group}-rebuild"), None).unwrap(),
        ));
    assert!(
        until(Duration::from_secs(30), || {
            stage.step().unwrap();
            trade_count(&mut stage, &bars, 119).is_some()
        }),
        "the product became READY"
    );
    let key = bars.encode();
    let old = stage.cache.pointer(&key).unwrap().ready.unwrap();
    stage.request_rebuild(&key);
    stage.step().unwrap();
    assert!(stage.rebuilding().is_some(), "rebuild started");
    // Live facts while the replay runs: a new open and a conflict for open 3.
    produce(
        &producer,
        &topic,
        &bars,
        &[bar_frame(&bars, 120, 1, 600), bar_frame(&bars, 3, 42, 601)],
        true,
    );
    assert!(
        until(Duration::from_secs(60), || {
            stage.step().unwrap();
            stage.rebuilding().is_none() && trade_count(&mut stage, &bars, 120).is_some()
        }),
        "the rebuild published and the live record is visible"
    );
    let pointer = stage.cache.pointer(&key).unwrap();
    assert_ne!(pointer.ready, Some(old));
    assert_eq!(pointer.staging, None);
    assert_eq!(
        stage.metrics.rebuilds_completed, 1,
        "{:?}",
        stage.last_rebuild_error
    );
    assert_eq!(
        trade_count(&mut stage, &bars, 3),
        Some(1),
        "first fact kept"
    );
    assert_eq!(
        trade_count(&mut stage, &bars, 7),
        Some(1),
        "aborted fact never applied"
    );
    let rows: Option<String> = redis::cmd("HGET")
        .arg(stage.cache.layout.bar_meta(pointer.ready.unwrap(), &key))
        .arg("rows")
        .query(stage.cache.connection())
        .unwrap();
    assert_eq!(rows.as_deref(), Some("121"));
}

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA and QDL_KN_TEST_REDIS (isolated); run by the kn-native-integration job"]
async fn a_wiped_cache_is_built_from_the_start_even_with_group_offsets_at_the_end() {
    let bootstrap = env("QDL_KN_TEST_KAFKA");
    let id = stamp();
    let topic = format!("kn3-wipe-{id}");
    create_compacted(&bootstrap, &topic).await;
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("transactional.id", format!("kn3-wipe-test-{id}"))
        .set("enable.idempotence", "true")
        .create()
        .unwrap();
    producer.init_transactions(Duration::from_secs(20)).unwrap();
    let bars = LogicalProductKey::new("paper", "OKX", "SWAP", UID, "BAR", Some("1m")).unwrap();
    let history: Vec<_> = (0..300u64)
        .map(|minute| bar_frame(&bars, minute, 1, minute))
        .collect();
    produce(&producer, &topic, &bars, &history, true);
    let group = format!("kn-projector-test-wipe-{id}");
    let environment = format!("wipe{id}");
    let mut first = instance(&bootstrap, &topic, &group, &environment, "w-0");
    assert!(until(Duration::from_secs(30), || {
        first.step().unwrap();
        trade_count(&mut first, &bars, 299).is_some()
    }));
    // Group offsets reach the end (informational commits), then the member
    // stops and the cache loses everything.
    for _ in 0..20 {
        first.step().unwrap();
    }
    drop(first);
    // Group offsets at the partition ends, committed synchronously (what the
    // informational commits of a long-running member leave behind).
    let committer: rdkafka::consumer::BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", &group)
        .create()
        .unwrap();
    let mut ends = rdkafka::TopicPartitionList::new();
    for partition in 0..PARTITIONS as i32 {
        let (_, end) = committer
            .fetch_watermarks(&topic, partition, Duration::from_secs(10))
            .unwrap();
        ends.add_partition_offset(&topic, partition, rdkafka::Offset::Offset(end))
            .unwrap();
    }
    rdkafka::consumer::Consumer::commit(&committer, &ends, rdkafka::consumer::CommitMode::Sync)
        .unwrap();
    drop(committer);
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("kn3:{environment}:*"))
        .query(
            &mut redis::Client::open(env("QDL_KN_TEST_REDIS").as_str())
                .unwrap()
                .get_connection()
                .unwrap(),
        )
        .unwrap();
    let mut connection = redis::Client::open(env("QDL_KN_TEST_REDIS").as_str())
        .unwrap()
        .get_connection()
        .unwrap();
    for key in keys {
        let _: u64 = redis::cmd("DEL").arg(key).query(&mut connection).unwrap();
    }
    let mut second = instance(&bootstrap, &topic, &group, &environment, "w-1");
    assert!(
        until(Duration::from_secs(60), || {
            second.step().unwrap();
            trade_count(&mut second, &bars, 0).is_some()
                && trade_count(&mut second, &bars, 299).is_some()
        }),
        "every open rebuilt from the partition start"
    );
    let generation = second.cache.pointer(&bars.encode()).unwrap().ready.unwrap();
    let rows: Option<String> = redis::cmd("HGET")
        .arg(second.cache.layout.bar_meta(generation, &bars.encode()))
        .arg("rows")
        .query(second.cache.connection())
        .unwrap();
    assert_eq!(rows.as_deref(), Some("300"));
}
