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
use qdl_projector::stage_b::{StageB, StageBLimits, StateInput, StateSource};
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
