//! KN-3 Astra R1 combined recovery over a real, isolated Kafka broker and the
//! dedicated memory-test Redis (never production): memory pressure while
//! tailing, a crash during it, a checkpoint beyond the rebuild horizon (rolling
//! per-product rebuild), a crash in the middle of that rebuild and a resume by
//! a third owner. The end state must equal the log: every row, every product
//! READY on exactly one generation, no pending retirement or rebuild request.
//!
//! `QDL_KN_TEST_KAFKA=host:port QDL_KN_TEST_REDIS_EXCLUSIVE=redis://host:port
//!  cargo test -p qdl-projector --test recovery_kafka -- --ignored`.

use prost::Message;
use qdl_contracts::qdl::marketdata::v2::{
    event_envelope::Payload, Bar, BarLifecycle, EventEnvelope,
};
use qdl_contracts::state_codec::{state_partition, StateFrame};
use qdl_contracts::state_contract::{LogicalProductKey, SourceCoordinate};
use qdl_projector::cache::{Cache, CacheError, Layout};
use qdl_projector::kafka_state::{KafkaPartitionReader, KafkaStateSettings, KafkaStateSource};
use qdl_projector::stage_b::{StageB, StageBError, StageBLimits};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PARTITIONS: u32 = 2;
const MIN: u64 = 60_000;

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

fn uid(index: u32) -> String {
    format!("fb26214c-7b9b-5961-95b2-55154755af{index:02x}")
}

fn lpk(index: u32) -> LogicalProductKey {
    LogicalProductKey::new("paper", "OKX", "SWAP", &uid(index), "BAR", Some("1m")).unwrap()
}

fn frame(index: u32, minute: u64, offset: u64) -> (String, Vec<u8>) {
    let envelope = EventEnvelope {
        event_id: [offset.to_be_bytes(), minute.to_be_bytes()].concat(),
        instrument_uid: uid(index),
        venue: "OKX".into(),
        market: "SWAP".into(),
        source_id: "okx-swap-bar-1m-primary-v2".repeat(8),
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
        &lpk(index),
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

fn produce(producer: &BaseProducer, topic: &str, index: u32, minutes: std::ops::Range<u64>) {
    producer.begin_transaction().unwrap();
    let partition = state_partition(&lpk(index), PARTITIONS).unwrap() as i32;
    for minute in minutes {
        let (key, value) = frame(index, minute, u64::from(index) * 100_000 + minute);
        loop {
            match producer.send(
                BaseRecord::to(topic)
                    .partition(partition)
                    .key(&key)
                    .payload(&value),
            ) {
                Ok(()) => break,
                Err((_, _)) => {
                    producer.poll(Duration::from_millis(10));
                }
            }
        }
    }
    producer.flush(Duration::from_secs(20)).unwrap();
    producer
        .commit_transaction(Duration::from_secs(20))
        .unwrap();
}

struct Redis(redis::Connection);

impl Redis {
    fn new() -> Self {
        Self(
            redis::Client::open(env("QDL_KN_TEST_REDIS_EXCLUSIVE").as_str())
                .unwrap()
                .get_connection()
                .unwrap(),
        )
    }

    fn maxmemory(&mut self, bytes: u64) {
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory")
            .arg(bytes)
            .query(&mut self.0)
            .unwrap();
    }

    fn used(&mut self) -> u64 {
        let info: String = redis::cmd("INFO").arg("memory").query(&mut self.0).unwrap();
        info.lines()
            .find_map(|line| line.strip_prefix("used_memory:"))
            .and_then(|value| value.trim().parse().ok())
            .unwrap()
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        self.maxmemory(0);
    }
}

fn instance(
    bootstrap: &str,
    topic: &str,
    group: &str,
    environment: &str,
    id: &str,
    clock: Option<fn() -> u64>,
) -> StageB<KafkaStateSource> {
    let source = KafkaStateSource::open(&KafkaStateSettings {
        bootstrap: bootstrap.into(),
        topics: vec![topic.into()],
        group_id: group.into(),
        client_id: id.into(),
        tls: None,
    })
    .unwrap();
    let cache = Cache::connect(
        &env("QDL_KN_TEST_REDIS_EXCLUSIVE"),
        Layout::new(environment),
    )
    .unwrap();
    let reader =
        KafkaPartitionReader::open(bootstrap, &format!("{group}-rebuild-{id}"), None).unwrap();
    let stage = StageB::new(
        source,
        cache,
        StageBLimits {
            max_batch_records: 200,
            poll_timeout: Duration::from_millis(50),
            ownership_probe: Duration::ZERO,
            ..StageBLimits::default()
        },
    )
    .with_rebuild_reader(Box::new(reader));
    match clock {
        Some(clock) => stage.with_clock(clock),
        None => stage,
    }
}

fn rows(stage: &mut StageB<KafkaStateSource>, index: u32) -> Option<u64> {
    let generation = stage.cache.pointer(&lpk(index).encode()).unwrap().ready?;
    let value: Option<String> = redis::cmd("HGET")
        .arg(
            stage
                .cache
                .layout
                .bar_meta(generation, &lpk(index).encode()),
        )
        .arg("rows")
        .query(stage.cache.connection())
        .unwrap();
    value.map(|value| value.parse().unwrap())
}

fn run(stage: &mut StageB<KafkaStateSource>, steps: usize) -> usize {
    let mut refused = 0;
    for _ in 0..steps {
        match stage.step() {
            Ok(_) => {}
            Err(StageBError::Cache(CacheError::MemoryPressure(_))) => refused += 1,
            Err(error) => panic!("{error:?}"),
        }
    }
    refused
}

fn generations(stage: &mut StageB<KafkaStateSource>, index: u32) -> BTreeSet<u64> {
    let prefix = stage.cache.layout.prefix().to_owned();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}*"))
        .query(stage.cache.connection())
        .unwrap();
    let product = lpk(index).encode();
    keys.iter()
        .filter_map(|key| {
            let rest = key.strip_prefix(&prefix)?;
            let (kind, rest) = rest.split_once(':')?;
            if !matches!(kind, "l" | "bm" | "b" | "rk" | "cx") {
                return None;
            }
            let (generation, rest) = rest.split_once(':')?;
            rest.starts_with(&product)
                .then(|| generation.parse().ok())?
        })
        .collect()
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

#[tokio::test]
#[ignore = "requires QDL_KN_TEST_KAFKA and QDL_KN_TEST_REDIS_EXCLUSIVE (isolated); run by the kn-native-integration job"]
async fn r1_memory_pressure_crashes_and_a_rolling_rebuild_converge_to_the_log() {
    let bootstrap = env("QDL_KN_TEST_KAFKA");
    let id = stamp();
    let topic = format!("kn3-recovery-{id}");
    create_compacted(&bootstrap, &topic).await;
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("transactional.id", format!("kn3-recovery-test-{id}"))
        .set("enable.idempotence", "true")
        .create()
        .unwrap();
    producer.init_transactions(Duration::from_secs(20)).unwrap();
    let products = 4u32;
    for index in 0..products {
        produce(&producer, &topic, index, 0..300);
    }
    let group = format!("kn-projector-test-rec-{id}");
    let environment = format!("recovery{id}");
    let mut redis = Redis::new();

    // 1. Build, then memory pressure while tailing, then a crash under it.
    let mut first = instance(&bootstrap, &topic, &group, &environment, "a", None);
    for _ in 0..600 {
        first.step().unwrap();
        if (0..products).all(|index| rows(&mut first, index) == Some(300)) {
            break;
        }
    }
    assert!((0..products).all(|index| rows(&mut first, index) == Some(300)));
    let used = redis.used();
    redis.maxmemory(used + 80_000);
    for index in 0..products {
        produce(&producer, &topic, index, 300..1_300);
    }
    let refused = run(&mut first, 300);
    assert!(refused > 0, "memory pressure reached");
    for index in 0..products {
        let applied = rows(&mut first, index).unwrap();
        assert!((300..1_300).contains(&applied), "{index}: {applied}");
    }
    assert!(first.metrics.rewinds > 0);
    drop(first);
    redis.maxmemory(0);

    // 2. The cache was offline beyond the horizon: rolling rebuild; the
    // second owner crashes half way.
    fn late() -> u64 {
        u64::MAX / 2
    }
    let mut second = instance(&bootstrap, &topic, &group, &environment, "b", Some(late));
    let mut steps = 0;
    while second.metrics.rebuilds_completed < 2 && steps < 3_000 {
        second.step().unwrap();
        steps += 1;
    }
    assert!(second.metrics.rolling_rebuilds >= 1);
    assert!(second.metrics.rebuilds_completed >= 2);
    let pending: Vec<String> = redis::cmd("SMEMBERS")
        .arg(second.cache.layout.rebuild_requests())
        .query(second.cache.connection())
        .unwrap();
    assert!(!pending.is_empty(), "obligations left for the next owner");
    drop(second);

    // 3. A third owner (fresh checkpoint now) resumes the obligations.
    let mut third = instance(&bootstrap, &topic, &group, &environment, "c", None);
    for _ in 0..3_000 {
        third.step().unwrap();
        let pending: u64 = redis::cmd("SCARD")
            .arg(third.cache.layout.rebuild_requests())
            .query(third.cache.connection())
            .unwrap();
        if pending == 0 && (0..products).all(|index| rows(&mut third, index) == Some(1_300)) {
            break;
        }
    }
    for index in 0..products {
        assert_eq!(rows(&mut third, index), Some(1_300), "product {index}");
        let pointer = third.cache.pointer(&lpk(index).encode()).unwrap();
        assert_eq!(pointer.staging, None);
        let live: BTreeSet<u64> = pointer.ready.into_iter().collect();
        assert_eq!(generations(&mut third, index), live, "product {index}");
    }
    let retiring: u64 = redis::cmd("SCARD")
        .arg(third.cache.layout.retire())
        .query(third.cache.connection())
        .unwrap();
    assert_eq!(retiring, 0);
    let pending: u64 = redis::cmd("SCARD")
        .arg(third.cache.layout.rebuild_requests())
        .query(third.cache.connection())
        .unwrap();
    assert_eq!(pending, 0);
}
