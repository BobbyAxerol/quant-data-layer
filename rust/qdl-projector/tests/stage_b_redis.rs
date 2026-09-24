//! K3-T02..T06 for stage B against a real, isolated Redis (the market-cache
//! image). The state topics are an in-memory log here (their Kafka
//! transport is covered by `stage_b_kafka`); envelopes are synthetic unit
//! fixtures built with prost and framed with the real state codec.
//!
//! `QDL_KN_TEST_REDIS=redis://host:port cargo test -p qdl-projector --test stage_b_redis -- --ignored`.

use prost::Message;
use qdl_contracts::qdl::marketdata::v2::{
    event_envelope::Payload, Bar, BarLifecycle, EventEnvelope, MarkIndexPrice, OrderBookDelta,
    OrderBookSnapshot, Quote,
};
use qdl_contracts::state_codec::{decode_bar_row, decode_latest_value, StateFrame};
use qdl_contracts::state_contract::{LogicalProductKey, SourceCoordinate};
use qdl_projector::cache::{bucket_of, Cache, Layout};
use qdl_projector::stage_b::{
    PartitionReader, StageB, StageBError, StageBLimits, StateInput, StateSource,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const LATEST: &str = "md.latest.v2";
const BARS: &str = "md.bars.v2";
const TOPIC_ID: &str = "ljfjPYApRpWQd79McfTtZg";
const UID: &str = "fb26214c-7b9b-5961-95b2-55154755af0f";
const MIN: u64 = 60_000;

// ------------------------------------------------------------ memory log

#[derive(Default)]
struct LogInner {
    records: BTreeMap<(String, i32), Vec<StateInput>>,
}

#[derive(Clone, Default)]
struct Log(Arc<Mutex<LogInner>>);

impl Log {
    fn append(&self, topic: &str, partition: i32, key: &str, value: Option<Vec<u8>>) -> i64 {
        let mut inner = self.0.lock().unwrap();
        let records = inner.records.entry((topic.into(), partition)).or_default();
        let offset = records.last().map(|r| r.offset + 1).unwrap_or(0);
        records.push(StateInput {
            topic: topic.into(),
            partition,
            offset,
            key: key.as_bytes().to_vec(),
            value,
        });
        offset
    }
}

/// One consumer: its own positions over the shared log.
struct Source {
    log: Log,
    assigned: Vec<(String, i32)>,
    position: BTreeMap<(String, i32), i64>,
}

impl StateSource for Source {
    fn poll(&mut self, max: usize, _timeout: Duration) -> Result<Vec<StateInput>, String> {
        let inner = self.log.0.lock().unwrap();
        let mut batch = Vec::new();
        for key in &self.assigned {
            let from = *self.position.get(key).unwrap_or(&0);
            let records: Vec<StateInput> = inner
                .records
                .get(key)
                .map(|records| {
                    records
                        .iter()
                        .filter(|r| r.offset >= from)
                        .take(max)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            if let Some(last) = records.last() {
                self.position.insert(key.clone(), last.offset + 1);
            }
            batch.extend(records);
        }
        Ok(batch)
    }
    fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<(), String> {
        self.position.insert((topic.into(), partition), offset);
        Ok(())
    }
    fn watermarks(&mut self, topic: &str, partition: i32) -> Result<(i64, i64), String> {
        let inner = self.log.0.lock().unwrap();
        let end = inner
            .records
            .get(&(topic.into(), partition))
            .and_then(|r| r.last())
            .map(|r| r.offset + 1)
            .unwrap_or(0);
        Ok((0, end))
    }
    fn position(&mut self, topic: &str, partition: i32) -> Result<Option<i64>, String> {
        Ok(self.position.get(&(topic.into(), partition)).copied())
    }
    fn assigned(&mut self) -> Result<Vec<(String, i32)>, String> {
        Ok(self.assigned.clone())
    }
    fn commit(&mut self, _topic: &str, _partition: i32, _next: i64) -> Result<(), String> {
        Ok(())
    }
}

/// The rebuild replay reader over the same log (assign mode).
struct Reader {
    log: Log,
    at: Option<(String, i32, i64)>,
}

impl PartitionReader for Reader {
    fn watermarks(&mut self, topic: &str, partition: i32) -> Result<(i64, i64), String> {
        let inner = self.log.0.lock().unwrap();
        let end = inner
            .records
            .get(&(topic.into(), partition))
            .and_then(|r| r.last())
            .map(|r| r.offset + 1)
            .unwrap_or(0);
        Ok((0, end))
    }
    fn start(&mut self, topic: &str, partition: i32, offset: i64) -> Result<(), String> {
        self.at = Some((topic.into(), partition, offset));
        Ok(())
    }
    fn poll(&mut self, max: usize, _timeout: Duration) -> Result<Vec<StateInput>, String> {
        let Some((topic, partition, from)) = self.at.clone() else {
            return Ok(Vec::new());
        };
        let inner = self.log.0.lock().unwrap();
        let records: Vec<StateInput> = inner
            .records
            .get(&(topic.clone(), partition))
            .map(|records| {
                records
                    .iter()
                    .filter(|r| r.offset >= from)
                    .take(max)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        if let Some(last) = records.last() {
            self.at = Some((topic, partition, last.offset + 1));
        }
        Ok(records)
    }
    fn position(&mut self, _topic: &str, _partition: i32) -> Result<Option<i64>, String> {
        Ok(self.at.as_ref().map(|at| at.2))
    }
    fn stop(&mut self) -> Result<(), String> {
        self.at = None;
        Ok(())
    }
}

// ------------------------------------------------------------ fixtures

fn lpk(feed: &str, interval: Option<&str>) -> LogicalProductKey {
    LogicalProductKey::new("paper", "OKX", "SWAP", UID, feed, interval).unwrap()
}

static EVENT: AtomicU64 = AtomicU64::new(1);

fn envelope(payload: Payload) -> EventEnvelope {
    EventEnvelope {
        event_id: EVENT.fetch_add(1, Ordering::Relaxed).to_be_bytes().to_vec(),
        instrument_uid: UID.into(),
        venue: "OKX".into(),
        market: "SWAP".into(),
        payload: Some(payload),
        ..Default::default()
    }
}

fn quote(level: u32) -> Vec<u8> {
    envelope(Payload::Quote(Quote {
        level,
        ..Default::default()
    }))
    .encode_to_vec()
}

fn bar(open_min: u64, lifecycle: BarLifecycle, revision: u32, close: u32) -> Vec<u8> {
    envelope(Payload::Bar(Bar {
        interval: "1m".into(),
        open_time_ns: (open_min * MIN * 1_000_000) as i64,
        close_time_ns: ((open_min + 1) * MIN * 1_000_000 - 1) as i64,
        is_final: lifecycle != BarLifecycle::InProgress,
        revision,
        lifecycle: lifecycle as i32,
        trade_count: u64::from(close),
        ..Default::default()
    }))
    .encode_to_vec()
}

fn source(offset: u64) -> SourceCoordinate {
    SourceCoordinate {
        topic_id: TOPIC_ID.into(),
        partition: 2,
        offset,
    }
}

fn latest_frame(lpk: &LogicalProductKey, envelope: Vec<u8>, offset: u64) -> (String, Vec<u8>) {
    let frame = StateFrame::latest(&envelope, lpk, source(offset), 1).unwrap();
    (frame.key().unwrap(), frame.encode().unwrap())
}

fn bar_frame(lpk: &LogicalProductKey, envelope: Vec<u8>, offset: u64) -> (String, Vec<u8>) {
    let frame = StateFrame::bar_revision(&envelope, lpk, source(offset), 1).unwrap();
    (frame.key().unwrap(), frame.encode().unwrap())
}

fn floor_frame(lpk: &LogicalProductKey, floor_ms: u64) -> (String, Vec<u8>) {
    let frame = StateFrame::retention_floor(lpk, floor_ms, 1).unwrap();
    (frame.key().unwrap(), frame.encode().unwrap())
}

fn push(log: &Log, topic: &str, (key, value): (String, Vec<u8>)) -> i64 {
    log.append(topic, 0, &key, Some(value))
}

fn environment(label: &str) -> String {
    format!(
        "{label}{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn stage(log: &Log, environment: &str) -> StageB<Source> {
    let url = std::env::var("QDL_KN_TEST_REDIS")
        .expect("QDL_KN_TEST_REDIS must name an isolated test Redis; never the control Redis");
    let cache = Cache::connect(&url, Layout::new(environment)).unwrap();
    StageB::new(
        Source {
            log: log.clone(),
            assigned: vec![(LATEST.into(), 0), (BARS.into(), 0)],
            position: BTreeMap::new(),
        },
        cache,
        StageBLimits {
            max_batch_records: 7,
            poll_timeout: Duration::ZERO,
            ..StageBLimits::default()
        },
    )
}

fn drain(stage: &mut StageB<Source>) {
    for _ in 0..200 {
        stage.step().expect("step");
    }
}

/// The ready generation's latest canonical bytes of `lpk`, if READY.
fn read_latest(stage: &mut StageB<Source>, lpk: &LogicalProductKey) -> Option<(Vec<u8>, u64)> {
    let pointer = stage.cache.pointer(&lpk.encode()).unwrap();
    let generation = pointer.ready?;
    let value: Option<Vec<u8>> = redis::cmd("HGET")
        .arg(stage.cache.layout.latest(generation, &lpk.encode()))
        .arg("v")
        .query(stage.cache.connection())
        .unwrap();
    let decoded = decode_latest_value(&value?).unwrap();
    Some((decoded.canonical, decoded.source_offset))
}

fn read_bar(stage: &mut StageB<Source>, lpk: &LogicalProductKey, open_min: u64) -> Option<Vec<u8>> {
    let generation = stage.cache.pointer(&lpk.encode()).unwrap().ready?;
    let open_ms = open_min * MIN;
    let row = stage
        .cache
        .bar_row(generation, &lpk.encode(), bucket_of(open_ms, MIN), open_ms)
        .unwrap()?;
    Some(decode_bar_row(&row, lpk).unwrap().canonical)
}

fn meta(stage: &mut StageB<Source>, lpk: &LogicalProductKey, field: &str) -> Option<String> {
    let generation = stage.cache.pointer(&lpk.encode()).unwrap().ready?;
    redis::cmd("HGET")
        .arg(stage.cache.layout.bar_meta(generation, &lpk.encode()))
        .arg(field)
        .query(stage.cache.connection())
        .unwrap()
}

fn skip_redis() {
    // Present only so the ignore reason is uniform.
}

// ------------------------------------------------------------ tests

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_cold_build_publishes_every_product_and_missing_products_are_not_ready() {
    skip_redis();
    let log = Log::default();
    let quotes = lpk("QUOTE", None);
    let bars = lpk("BAR", Some("1m"));
    let latest_quote = quote(3);
    push(&log, LATEST, latest_frame(&quotes, quote(1), 10));
    push(&log, LATEST, latest_frame(&quotes, quote(2), 11));
    push(
        &log,
        LATEST,
        latest_frame(&quotes, latest_quote.clone(), 12),
    );
    let mut final_bars = Vec::new();
    for minute in 0..20u64 {
        let envelope = bar(minute, BarLifecycle::Final, 0, minute as u32);
        final_bars.push(envelope.clone());
        push(&log, BARS, bar_frame(&bars, envelope, 100 + minute));
    }
    let environment = environment("cold");
    let mut stage = stage(&log, &environment);
    drain(&mut stage);
    assert_eq!(
        stage.metrics.builds, 2,
        "both partitions built from the start"
    );
    assert_eq!(stage.metrics.published, 2);
    assert!(!stage.building(LATEST, 0) && !stage.building(BARS, 0));
    assert_eq!(read_latest(&mut stage, &quotes), Some((latest_quote, 12)));
    for minute in [0u64, 7, 19] {
        assert_eq!(
            read_bar(&mut stage, &bars, minute),
            Some(final_bars[minute as usize].clone())
        );
    }
    assert_eq!(meta(&mut stage, &bars, "rows").as_deref(), Some("20"));
    assert_eq!(
        meta(&mut stage, &bars, "last_final"),
        Some((19 * MIN).to_string())
    );
    // A product without state has no pointer: NOT_READY, never a default.
    let absent = lpk("TRADE", None);
    assert_eq!(stage.cache.pointer(&absent.encode()).unwrap().ready, None);
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_restart_tails_from_the_checkpoint_and_the_old_owner_is_a_zombie() {
    let log = Log::default();
    let quotes = lpk("QUOTE", None);
    push(&log, LATEST, latest_frame(&quotes, quote(1), 10));
    let environment = environment("restart");
    let mut old = stage(&log, &environment);
    drain(&mut old);
    let generation = old.cache.pointer(&quotes.encode()).unwrap().ready.unwrap();
    // A second instance takes over (same cache): no rebuild, same generation.
    let mut new = stage(&log, &environment);
    drain(&mut new);
    assert_eq!(
        new.metrics.builds, 0,
        "a fresh checkpoint tails, never rebuilds"
    );
    push(&log, LATEST, latest_frame(&quotes, quote(2), 11));
    // The old owner still polls the new record: its batch is refused whole.
    let newest = quote(9);
    push(&log, LATEST, latest_frame(&quotes, newest.clone(), 13));
    drain(&mut old);
    assert!(old.metrics.zombies >= 1, "the old owner is fenced");
    drain(&mut new);
    assert_eq!(read_latest(&mut new, &quotes), Some((newest, 13)));
    assert_eq!(
        new.cache.pointer(&quotes.encode()).unwrap().ready,
        Some(generation)
    );
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn beyond_the_rebuild_horizon_a_fresh_generation_replaces_the_cache() {
    let log = Log::default();
    let quotes = lpk("QUOTE", None);
    let gone = lpk("MARK_INDEX_PRICE", None);
    push(&log, LATEST, latest_frame(&quotes, quote(1), 10));
    let mark = envelope(Payload::MarkIndexPrice(MarkIndexPrice::default())).encode_to_vec();
    push(&log, LATEST, latest_frame(&gone, mark, 11));
    let environment = environment("horizon");
    let mut first = stage(&log, &environment);
    drain(&mut first);
    let old_generation = first
        .cache
        .pointer(&quotes.encode())
        .unwrap()
        .ready
        .unwrap();
    // The product `gone` is delisted: its latest state is tombstoned, and
    // compaction has removed its records; then the cache stays offline for
    // longer than the tombstone lifetime.
    {
        let mut inner = log.0.lock().unwrap();
        let records = inner.records.get_mut(&(LATEST.into(), 0)).unwrap();
        records.retain(|record| record.key != gone.encode().into_bytes());
    }
    let mut late = stage(&log, &environment).with_clock(|| u64::MAX / 2);
    drain(&mut late);
    assert_eq!(
        late.metrics.builds, 2,
        "no overlay of a cache that may have missed deletes"
    );
    let pointer = late.cache.pointer(&quotes.encode()).unwrap();
    assert!(pointer.ready.unwrap() > old_generation);
    assert_eq!(
        late.cache.pointer(&gone.encode()).unwrap().ready,
        None,
        "gone -> NOT_READY"
    );
    assert!(late.metrics.unpublished >= 1);
    assert!(late.metrics.reclaimed_keys >= 1);
    let old_keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!(
            "{}l:{old_generation}:*",
            late.cache.layout.prefix()
        ))
        .query(late.cache.connection())
        .unwrap();
    assert!(
        old_keys.is_empty(),
        "old generation reclaimed: {old_keys:?}"
    );
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn replaying_after_a_crash_changes_nothing() {
    let log = Log::default();
    let quotes = lpk("QUOTE", None);
    let bars = lpk("BAR", Some("1m"));
    let latest = quote(5);
    push(&log, LATEST, latest_frame(&quotes, quote(4), 20));
    push(&log, LATEST, latest_frame(&quotes, latest.clone(), 21));
    let final_bar = bar(3, BarLifecycle::Final, 0, 7);
    push(&log, BARS, bar_frame(&bars, final_bar.clone(), 30));
    let environment = environment("replay");
    let mut stage = stage(&log, &environment);
    drain(&mut stage);
    // The consumer is rewound (a crash before the group offset moved): the
    // same records come again and the rules call them DUPLICATE/STALE.
    stage.source.seek(LATEST, 0, 0).unwrap();
    stage.source.seek(BARS, 0, 0).unwrap();
    let before = (
        stage.metrics.latest_applied,
        stage.metrics.bars_applied,
        read_latest(&mut stage, &quotes),
    );
    drain(&mut stage);
    assert_eq!(read_latest(&mut stage, &quotes), before.2);
    assert_eq!(read_bar(&mut stage, &bars, 3), Some(final_bar));
    assert_eq!(
        (stage.metrics.latest_applied, stage.metrics.bars_applied),
        (before.0, before.1),
        "nothing re-applied"
    );
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn bar_revisions_follow_the_contract_rules() {
    let log = Log::default();
    let bars = lpk("BAR", Some("1m"));
    // Open 1: in-progress updates then the final -> the final.
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(1, BarLifecycle::InProgress, 0, 1), 1),
    );
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(1, BarLifecycle::InProgress, 0, 2), 2),
    );
    let final_1 = bar(1, BarLifecycle::Final, 0, 3);
    push(&log, BARS, bar_frame(&bars, final_1.clone(), 3));
    // A late in-progress after the final is stale.
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(1, BarLifecycle::InProgress, 0, 4), 4),
    );
    // Open 2: final r0, revised r1 (applied), a lower revision (stale).
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(2, BarLifecycle::Final, 0, 10), 5),
    );
    let revised = bar(2, BarLifecycle::Revised, 1, 11);
    push(&log, BARS, bar_frame(&bars, revised.clone(), 6));
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(2, BarLifecycle::Final, 0, 12), 7),
    );
    // Open 3: equal revision, different content -> CONFLICT, first kept.
    let first_3 = bar(3, BarLifecycle::Final, 0, 20);
    push(&log, BARS, bar_frame(&bars, first_3.clone(), 8));
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(3, BarLifecycle::Final, 0, 21), 9),
    );
    // Same final again later -> duplicate.
    push(&log, BARS, bar_frame(&bars, first_3.clone(), 10));
    let environment = environment("revisions");
    let mut stage = stage(&log, &environment);
    drain(&mut stage);
    assert_eq!(read_bar(&mut stage, &bars, 1), Some(final_1));
    assert_eq!(read_bar(&mut stage, &bars, 2), Some(revised));
    assert_eq!(
        read_bar(&mut stage, &bars, 3),
        Some(first_3),
        "never last-write-wins"
    );
    assert_eq!(meta(&mut stage, &bars, "conflicts").as_deref(), Some("1"));
    assert_eq!(meta(&mut stage, &bars, "rows").as_deref(), Some("3"));
    assert!(stage.metrics.duplicates >= 1);
    assert!(stage.metrics.stale >= 2);
    // Every fact that is not a current row is remembered for expiry.
    let generation = stage.cache.pointer(&bars.encode()).unwrap().ready.unwrap();
    let extras: BTreeMap<String, String> = redis::cmd("HGETALL")
        .arg(stage.cache.layout.fact_keys(generation, &bars.encode()))
        .query(stage.cache.connection())
        .unwrap();
    assert!(extras[&(2 * MIN).to_string()].contains("f0|"), "{extras:?}");
    assert!(extras[&(3 * MIN).to_string()].contains("f0|"), "{extras:?}");
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn book_snapshot_and_delta_are_separate_products_on_one_partition() {
    let log = Log::default();
    let snapshots = lpk("BOOK_SNAPSHOT", None);
    let deltas = lpk("BOOK_DELTA", None);
    let snapshot = envelope(Payload::BookSnapshot(OrderBookSnapshot {
        native_sequence: "10".into(),
        ..Default::default()
    }))
    .encode_to_vec();
    push(&log, LATEST, latest_frame(&snapshots, snapshot.clone(), 40));
    for sequence in 11..15u64 {
        let delta = envelope(Payload::BookDelta(OrderBookDelta {
            native_sequence_end: sequence.to_string(),
            reset: sequence == 13,
            ..Default::default()
        }))
        .encode_to_vec();
        push(&log, LATEST, latest_frame(&deltas, delta, 30 + sequence));
    }
    let environment = environment("book");
    let mut stage = stage(&log, &environment);
    drain(&mut stage);
    assert_eq!(
        read_latest(&mut stage, &snapshots),
        Some((snapshot, 40)),
        "a delta or reset never erases the snapshot"
    );
    assert_eq!(
        read_latest(&mut stage, &deltas).map(|(_, offset)| offset),
        Some(44)
    );
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_retention_floor_removes_old_rows_and_refuses_late_ones() {
    let log = Log::default();
    let bars = lpk("BAR", Some("1m"));
    for minute in 0..300u64 {
        push(
            &log,
            BARS,
            bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
        );
    }
    push(&log, BARS, floor_frame(&bars, 280 * MIN));
    // A late repair below the floor.
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(5, BarLifecycle::Revised, 1, 2), 999),
    );
    let environment = environment("floor");
    let mut stage = stage(&log, &environment);
    drain(&mut stage);
    assert_eq!(meta(&mut stage, &bars, "rows").as_deref(), Some("20"));
    assert_eq!(
        meta(&mut stage, &bars, "floor"),
        Some((280 * MIN).to_string())
    );
    assert!(read_bar(&mut stage, &bars, 5).is_none());
    assert!(read_bar(&mut stage, &bars, 280).is_some());
    assert!(stage.metrics.below_floor >= 1);
}

fn rebuild_stage(log: &Log, environment: &str) -> StageB<Source> {
    stage(log, environment).with_rebuild_reader(Box::new(Reader {
        log: log.clone(),
        at: None,
    }))
}

fn until_rebuilt(stage: &mut StageB<Source>) {
    for _ in 0..200 {
        stage.step().expect("step");
        if stage.rebuilding().is_none() {
            return;
        }
    }
    panic!("the rebuild did not finish");
}

fn meta_exists(stage: &mut StageB<Source>, generation: u64, lpk: &str) -> bool {
    let key = stage.cache.layout.bar_meta(generation, lpk);
    redis::cmd("EXISTS")
        .arg(key)
        .query::<u64>(stage.cache.connection())
        .unwrap()
        == 1
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_product_rebuild_replays_in_log_order_and_publishes_at_the_live_checkpoint() {
    let log = Log::default();
    let bars = lpk("BAR", Some("1m"));
    let quotes = lpk("QUOTE", None);
    let first_fact = bar(5, BarLifecycle::Final, 0, 1);
    for minute in 0..31u64 {
        let envelope = if minute == 5 {
            first_fact.clone()
        } else {
            bar(minute, BarLifecycle::Final, 0, 1)
        };
        push(&log, BARS, bar_frame(&bars, envelope, minute));
    }
    // Equal revision, other content: CONFLICT, the first fact stays.
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(5, BarLifecycle::Final, 0, 2), 100),
    );
    push(&log, LATEST, latest_frame(&quotes, quote(1), 10));
    let environment = environment("rebuild");
    let mut stage = rebuild_stage(&log, &environment);
    drain(&mut stage);
    let key = bars.encode();
    let before = stage.cache.pointer(&key).unwrap();
    let old = before.ready.unwrap();
    let quote_generation = stage.cache.pointer(&quotes.encode()).unwrap().ready;
    // Damage the ready generation: a row disappears.
    let damaged = 20 * MIN;
    let _: u64 = redis::cmd("HDEL")
        .arg(
            stage
                .cache
                .layout
                .bar_bucket(old, &key, bucket_of(damaged, MIN)),
        )
        .arg(damaged.to_string())
        .query(stage.cache.connection())
        .unwrap();
    assert!(read_bar(&mut stage, &bars, 20).is_none());

    stage.request_rebuild(&key);
    stage.step().unwrap();
    let (_, staged) = stage.rebuilding().expect("rebuild started");
    // Live records during the replay: a new open, and another conflicting
    // fact for open 5 that the staging generation sees before the old ones
    // under a dual write (the D10 defect) - here it must still lose.
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(40, BarLifecycle::Final, 0, 1), 200),
    );
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(5, BarLifecycle::Final, 0, 3), 201),
    );
    until_rebuilt(&mut stage);

    let after = stage.cache.pointer(&key).unwrap();
    assert_eq!(
        (after.ready, after.staging, after.fence),
        (Some(staged), None, before.fence + 1)
    );
    assert_ne!(staged, old);
    assert!(read_bar(&mut stage, &bars, 20).is_some(), "damage repaired");
    assert_eq!(
        read_bar(&mut stage, &bars, 5),
        Some(first_fact),
        "first fact kept"
    );
    assert!(
        read_bar(&mut stage, &bars, 40).is_some(),
        "live record replayed"
    );
    assert_eq!(meta(&mut stage, &bars, "rows").as_deref(), Some("32"));
    let conflicts: u64 = redis::cmd("LLEN")
        .arg(stage.cache.layout.conflicts(staged, &key))
        .query(stage.cache.connection())
        .unwrap();
    assert_eq!(conflicts, 2, "both later facts refused and recorded");
    assert!(!meta_exists(&mut stage, old, &key));
    assert_eq!(
        stage.cache.pointer(&quotes.encode()).unwrap().ready,
        quote_generation,
        "other products untouched"
    );
    assert_eq!(
        (
            stage.metrics.rebuilds_started,
            stage.metrics.rebuilds_completed
        ),
        (1, 1)
    );
    // Live tailing continues into the published generation.
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(41, BarLifecycle::Final, 0, 1), 300),
    );
    drain(&mut stage);
    assert_eq!(meta(&mut stage, &bars, "rows").as_deref(), Some("33"));
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_latest_rebuild_follows_deletes_and_bad_requests_are_refused() {
    let log = Log::default();
    let quotes = lpk("QUOTE", None);
    push(&log, LATEST, latest_frame(&quotes, quote(1), 10));
    push(&log, LATEST, latest_frame(&quotes, quote(2), 11));
    log.append(LATEST, 0, &quotes.encode(), None);
    let newest = quote(3);
    push(&log, LATEST, latest_frame(&quotes, newest.clone(), 12));
    let environment = environment("rebuildl");
    let mut stage = rebuild_stage(&log, &environment);
    drain(&mut stage);
    stage.request_rebuild(&quotes.encode());
    until_rebuilt(&mut stage);
    assert_eq!(
        stage.metrics.rebuilds_completed, 1,
        "{:?} {:?}",
        stage.metrics, stage.last_rebuild_error
    );
    assert_eq!(read_latest(&mut stage, &quotes), Some((newest, 12)));
    // A product nobody holds, and a stage without a reader.
    stage.request_rebuild(&lpk("TRADE", None).encode());
    stage.step().unwrap();
    assert_eq!(stage.metrics.rebuilds_refused, 1);
    assert!(stage
        .last_rebuild_error
        .as_deref()
        .unwrap()
        .contains("no owned partition"));
    let mut plain = stage_without_reader(&log, &environment);
    drain(&mut plain);
    plain.request_rebuild(&quotes.encode());
    plain.step().unwrap();
    assert_eq!(plain.metrics.rebuilds_refused, 1);
}

fn stage_without_reader(log: &Log, environment: &str) -> StageB<Source> {
    stage(log, environment)
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn losing_the_partition_abandons_the_rebuild_and_reclaims_the_staged_generation() {
    let log = Log::default();
    let bars = lpk("BAR", Some("1m"));
    for minute in 0..60u64 {
        push(
            &log,
            BARS,
            bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
        );
    }
    let environment = environment("rebuilda");
    let mut stage = rebuild_stage(&log, &environment);
    drain(&mut stage);
    let key = bars.encode();
    let ready = stage.cache.pointer(&key).unwrap().ready;
    stage.request_rebuild(&key);
    stage.step().unwrap();
    stage.step().unwrap();
    let (_, staged) = stage.rebuilding().expect("rebuild in progress");
    assert!(meta_exists(&mut stage, staged, &key));
    // Revocation: the partition leaves the assignment.
    stage.source.assigned.clear();
    stage.step().unwrap();
    assert!(stage.rebuilding().is_none());
    assert_eq!(stage.metrics.rebuilds_abandoned, 1);
    assert!(!meta_exists(&mut stage, staged, &key));
    assert_eq!(
        stage.cache.pointer(&key).unwrap().ready,
        ready,
        "ready untouched"
    );
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_latest_tombstone_takes_effect_in_log_order_within_one_batch() {
    let log = Log::default();
    let revived = lpk("QUOTE", None);
    let deleted = lpk("TRADE", None);
    // One batch (7 records): set, delete, set -> the last set wins;
    // set, delete -> the state is gone.
    push(&log, LATEST, latest_frame(&revived, quote(1), 10));
    log.append(LATEST, 0, &revived.encode(), None);
    let newest = quote(2);
    push(&log, LATEST, latest_frame(&revived, newest.clone(), 11));
    push(
        &log,
        LATEST,
        latest_frame(
            &deleted,
            envelope(Payload::Trade(Default::default())).encode_to_vec(),
            12,
        ),
    );
    log.append(LATEST, 0, &deleted.encode(), None);
    let environment = environment("tomb");
    let mut stage = stage(&log, &environment);
    drain(&mut stage);
    assert_eq!(read_latest(&mut stage, &revived), Some((newest, 11)));
    assert_eq!(read_latest(&mut stage, &deleted), None);
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn an_interrupted_cold_build_is_built_again_by_the_next_owner() {
    let log = Log::default();
    let uids: Vec<String> = (0..6u32)
        .map(|index| format!("fb26214c-7b9b-5961-95b2-55154755af{index:02x}"))
        .collect();
    let products: Vec<LogicalProductKey> = uids
        .iter()
        .map(|uid| LogicalProductKey::new("paper", "OKX", "SWAP", uid, "QUOTE", None).unwrap())
        .collect();
    for (index, (uid, product)) in uids.iter().zip(&products).enumerate() {
        for level in 0..3u32 {
            let mut quote = EventEnvelope::decode(quote(level).as_slice()).unwrap();
            quote.instrument_uid = uid.clone();
            push(
                &log,
                LATEST,
                latest_frame(
                    product,
                    quote.encode_to_vec(),
                    10 * index as u64 + u64::from(level),
                ),
            );
        }
    }
    let environment = environment("interrupted");
    let mut first = stage(&log, &environment);
    // Two batches of 7: the build stops half way (a crash).
    first.step().unwrap();
    first.step().unwrap();
    assert!(first.building(LATEST, 0), "still building");
    drop(first);
    let mut next = stage(&log, &environment);
    drain(&mut next);
    for product in &products {
        assert!(
            read_latest(&mut next, product).is_some(),
            "{} READY after the takeover",
            product.encode()
        );
    }
}

fn wipe(stage: &mut StageB<Source>, environment: &str) {
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("kn3:{environment}:*"))
        .query(stage.cache.connection())
        .unwrap();
    for key in keys {
        let _: u64 = redis::cmd("DEL")
            .arg(key)
            .query(stage.cache.connection())
            .unwrap();
    }
}

fn probing(log: &Log, environment: &str) -> StageB<Source> {
    let mut stage = stage(log, environment);
    stage.limits.ownership_probe = Duration::ZERO;
    stage
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn an_idle_owner_rebuilds_a_cache_that_lost_its_state() {
    let log = Log::default();
    let quotes = lpk("QUOTE", None);
    let bars = lpk("BAR", Some("1m"));
    let newest = quote(4);
    push(&log, LATEST, latest_frame(&quotes, newest.clone(), 10));
    for minute in 0..30u64 {
        push(
            &log,
            BARS,
            bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
        );
    }
    let environment = environment("wiped");
    let mut stage = probing(&log, &environment);
    drain(&mut stage);
    assert!(read_latest(&mut stage, &quotes).is_some());
    // The cache loses everything (restart without persistence); no record
    // arrives afterwards.
    wipe(&mut stage, &environment);
    drain(&mut stage);
    assert_eq!(stage.metrics.ownership_lost, 2, "both partitions noticed");
    assert_eq!(stage.metrics.builds, 4, "and were built again");
    assert_eq!(read_latest(&mut stage, &quotes), Some((newest, 10)));
    assert_eq!(meta(&mut stage, &bars, "rows").as_deref(), Some("30"));
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_fenced_member_does_not_take_the_partition_back() {
    let log = Log::default();
    let quotes = lpk("QUOTE", None);
    push(&log, LATEST, latest_frame(&quotes, quote(1), 10));
    let environment = environment("fenced");
    let mut old = probing(&log, &environment);
    drain(&mut old);
    let mut new = probing(&log, &environment);
    drain(&mut new);
    let owner = |stage: &mut StageB<Source>| -> u64 {
        redis::cmd("GET")
            .arg(stage.cache.layout.owner(LATEST, 0))
            .query(stage.cache.connection())
            .unwrap()
    };
    let taken = owner(&mut new);
    // The old member still believes it is assigned: it notices the newer
    // fence and stays away instead of incrementing the owner key again.
    drain(&mut old);
    assert!(old.metrics.zombies >= 1);
    assert_eq!(owner(&mut old), taken, "no ping-pong");
    assert!(old.owned().iter().all(|(topic, _)| topic != LATEST));
    push(&log, LATEST, latest_frame(&quotes, quote(2), 11));
    drain(&mut new);
    drain(&mut old);
    assert_eq!(new.metrics.zombies, 0, "the new owner keeps applying");
    assert_eq!(
        read_latest(&mut new, &quotes).map(|(_, offset)| offset),
        Some(11)
    );
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_broken_cache_connection_is_recovered() {
    let log = Log::default();
    let quotes = lpk("QUOTE", None);
    push(&log, LATEST, latest_frame(&quotes, quote(1), 10));
    let environment = environment("reconnect");
    let mut stage = probing(&log, &environment);
    drain(&mut stage);
    let id: u64 = redis::cmd("CLIENT")
        .arg("ID")
        .query(stage.cache.connection())
        .unwrap();
    // Kill this connection from another client (a restart or a broken link).
    let url = std::env::var("QDL_KN_TEST_REDIS").unwrap();
    let mut other = redis::Client::open(url.as_str())
        .unwrap()
        .get_connection()
        .unwrap();
    let _: u64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(id)
        .query(&mut other)
        .unwrap();
    let error = (0..5).find_map(|_| stage.step().err());
    assert!(matches!(error, Some(StageBError::Cache(_))), "{error:?}");
    stage.recover_cache().unwrap();
    push(&log, LATEST, latest_frame(&quotes, quote(2), 11));
    drain(&mut stage);
    assert_eq!(stage.metrics.cache_reconnects, 1);
    assert_eq!(
        stage.metrics.builds, 2,
        "the cache kept its state: tail, no rebuild"
    );
    assert_eq!(
        read_latest(&mut stage, &quotes).map(|(_, offset)| offset),
        Some(11)
    );
}
