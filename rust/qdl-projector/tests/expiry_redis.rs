//! K3.5 BAR expiry (decision D11) against a real, isolated Redis: product
//! state is built by stage B from an in-memory state log (the same fixture
//! as `stage_b_redis`); plans are published into that log through an
//! in-memory transactional sink and applied by stage B again. The Kafka
//! transaction itself is covered by `expiry_kafka`. Envelopes are synthetic
//! unit fixtures built with prost and framed with the real state codec.
//!
//! `QDL_KN_TEST_REDIS=redis://host:port cargo test -p qdl-projector --test expiry_redis -- --ignored`.

use prost::Message;
use qdl_contracts::qdl::marketdata::v2::{
    event_envelope::Payload, Bar, BarLifecycle, EventEnvelope,
};
use qdl_contracts::state_codec::{bar_key, floor_key, StateFrame};
use qdl_contracts::state_contract::{LogicalProductKey, SourceCoordinate};
use qdl_projector::cache::{bucket_of, Cache, Layout};
use qdl_projector::expiry::{
    plan_expiry, publish_expiry, ExpiryError, ExpiryPlan, ExpiryPublish, ExpirySink, ExpiryTask,
};
use qdl_projector::stage_b::{StageB, StageBLimits, StateInput, StateSource};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

    fn records(&self, topic: &str, partition: i32) -> Vec<StateInput> {
        self.0
            .lock()
            .unwrap()
            .records
            .get(&(topic.into(), partition))
            .cloned()
            .unwrap_or_default()
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

/// `(topic, partition, key, payload)` records of an open transaction.
type Pending = Vec<(String, i32, String, Option<Vec<u8>>)>;

/// A transactional sink over the memory log: records become visible only
/// on commit, an abort drops them.
struct LogSink {
    log: Log,
    open: RefCell<Option<Pending>>,
}

impl LogSink {
    fn new(log: &Log) -> Self {
        Self {
            log: log.clone(),
            open: RefCell::new(None),
        }
    }
}

impl ExpirySink for LogSink {
    fn begin(&self) -> Result<(), String> {
        let mut open = self.open.borrow_mut();
        if open.is_some() {
            return Err("transaction already open".into());
        }
        *open = Some(Vec::new());
        Ok(())
    }
    fn send_record(
        &self,
        topic: &str,
        partition: i32,
        key: &str,
        payload: Option<&[u8]>,
        _timeout: Duration,
    ) -> Result<(), String> {
        self.open
            .borrow_mut()
            .as_mut()
            .ok_or("no transaction")?
            .push((
                topic.into(),
                partition,
                key.into(),
                payload.map(<[u8]>::to_vec),
            ));
        Ok(())
    }
    fn commit(&self, _timeout: Duration) -> Result<(), String> {
        for (topic, partition, key, value) in
            self.open.borrow_mut().take().ok_or("no transaction")?
        {
            self.log.append(&topic, partition, &key, value);
        }
        Ok(())
    }
    fn abort(&self, _timeout: Duration) -> Result<(), String> {
        self.open.borrow_mut().take();
        Ok(())
    }
}

// ------------------------------------------------------------ fixtures

fn bar_lpk(uid: &str) -> LogicalProductKey {
    LogicalProductKey::new("paper", "OKX", "SWAP", uid, "BAR", Some("1m")).unwrap()
}

static EVENT: AtomicU64 = AtomicU64::new(1);
static OFFSET: AtomicU64 = AtomicU64::new(1);

fn bar(uid: &str, open_min: u64, lifecycle: BarLifecycle, revision: u32, close: u32) -> Vec<u8> {
    EventEnvelope {
        event_id: EVENT.fetch_add(1, Ordering::Relaxed).to_be_bytes().to_vec(),
        instrument_uid: uid.into(),
        venue: "OKX".into(),
        market: "SWAP".into(),
        payload: Some(Payload::Bar(Bar {
            interval: "1m".into(),
            open_time_ns: (open_min * MIN * 1_000_000) as i64,
            close_time_ns: ((open_min + 1) * MIN * 1_000_000 - 1) as i64,
            is_final: lifecycle != BarLifecycle::InProgress,
            revision,
            lifecycle: lifecycle as i32,
            trade_count: u64::from(close),
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// A written fact: its compaction key and open minute.
struct Fact {
    key: String,
    open_min: u64,
}

fn push_bar(
    log: &Log,
    lpk: &LogicalProductKey,
    open_min: u64,
    lifecycle: BarLifecycle,
    revision: u32,
    close: u32,
) -> Fact {
    let envelope = bar(&lpk.instrument_uid, open_min, lifecycle, revision, close);
    let frame = StateFrame::bar_revision(
        &envelope,
        lpk,
        SourceCoordinate {
            topic_id: TOPIC_ID.into(),
            partition: 2,
            offset: OFFSET.fetch_add(1, Ordering::Relaxed),
        },
        1,
    )
    .unwrap();
    let key = frame.key().unwrap();
    log.append(BARS, 0, &key, Some(frame.encode().unwrap()));
    Fact { key, open_min }
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
            assigned: vec![(BARS.into(), 0)],
            position: BTreeMap::new(),
        },
        cache,
        StageBLimits {
            max_batch_records: 500,
            poll_timeout: Duration::ZERO,
            ..StageBLimits::default()
        },
    )
}

/// Step until the log is consumed (bounded).
fn drain(stage: &mut StageB<Source>) {
    let mut idle = 0;
    for _ in 0..2_000 {
        if stage.step().expect("step") == 0 {
            idle += 1;
            if idle >= 3 && !stage.building(BARS, 0) {
                return;
            }
        } else {
            idle = 0;
        }
    }
    panic!("stage B did not drain");
}

fn ready(stage: &mut StageB<Source>, lpk: &LogicalProductKey) -> u64 {
    stage
        .cache
        .pointer(&lpk.encode())
        .unwrap()
        .ready
        .expect("READY")
}

fn meta(stage: &mut StageB<Source>, lpk: &LogicalProductKey, field: &str) -> Option<String> {
    let generation = ready(stage, lpk);
    redis::cmd("HGET")
        .arg(stage.cache.layout.bar_meta(generation, &lpk.encode()))
        .arg(field)
        .query(stage.cache.connection())
        .unwrap()
}

fn has_row(stage: &mut StageB<Source>, lpk: &LogicalProductKey, open_min: u64) -> bool {
    let generation = ready(stage, lpk);
    let open_ms = open_min * MIN;
    stage
        .cache
        .bar_row(generation, &lpk.encode(), bucket_of(open_ms, MIN), open_ms)
        .unwrap()
        .is_some()
}

fn rk_opens(stage: &mut StageB<Source>, lpk: &LogicalProductKey) -> Vec<u64> {
    let generation = ready(stage, lpk);
    let fields: Vec<String> = redis::cmd("HKEYS")
        .arg(stage.cache.layout.fact_keys(generation, &lpk.encode()))
        .query(stage.cache.connection())
        .unwrap();
    let mut opens: Vec<u64> = fields.iter().map(|f| f.parse().unwrap()).collect();
    opens.sort_unstable();
    opens
}

fn plan(
    stage: &mut StageB<Source>,
    lpk: &LogicalProductKey,
    cap: u64,
    max: usize,
) -> Option<ExpiryPlan> {
    let generation = ready(stage, lpk);
    plan_expiry(&mut stage.cache, generation, lpk, cap, max).unwrap()
}

fn publish(log: &Log, plan: &ExpiryPlan) {
    publish_expiry(&LogSink::new(log), BARS, 1, plan, 1, Duration::from_secs(1)).unwrap();
}

fn publish_target() -> ExpiryPublish {
    ExpiryPublish {
        topic: BARS.into(),
        partitions: 1,
        materializer_epoch: 1,
        timeout: Duration::from_secs(1),
    }
}

/// Every fact below the floor: all written keys of the expired opens plus
/// the in-progress key of each expired open.
fn expected_tombstones(
    lpk: &LogicalProductKey,
    facts: &[Fact],
    opens: &[u64],
    floor_min: u64,
) -> BTreeSet<String> {
    let mut keys: BTreeSet<String> = facts
        .iter()
        .filter(|fact| fact.open_min < floor_min)
        .map(|fact| fact.key.clone())
        .collect();
    for open in opens.iter().filter(|open| **open < floor_min) {
        keys.insert(bar_key(lpk, open * MIN, false, 0, "").unwrap());
    }
    keys
}

/// 300 opens (minutes 0..=299) with every fact shape below and above the
/// future floor, plus an in-progress newest open (minute 300): 301 rows.
fn rich_history(log: &Log, lpk: &LogicalProductKey) -> (Vec<Fact>, Vec<u64>) {
    let mut facts = Vec::new();
    for minute in 0..300u64 {
        match minute {
            // Only in-progress updates: the current row is the p key.
            5 => {
                facts.push(push_bar(log, lpk, minute, BarLifecycle::InProgress, 0, 1));
                facts.push(push_bar(log, lpk, minute, BarLifecycle::InProgress, 0, 2));
            }
            // In-progress then final.
            30 => {
                facts.push(push_bar(log, lpk, minute, BarLifecycle::InProgress, 0, 1));
                facts.push(push_bar(log, lpk, minute, BarLifecycle::Final, 0, 2));
            }
            // Final r0 then revised r1: r0 is a superseded extra.
            10 | 250 => {
                facts.push(push_bar(log, lpk, minute, BarLifecycle::Final, 0, 1));
                facts.push(push_bar(log, lpk, minute, BarLifecycle::Revised, 1, 2));
            }
            // Equal revision, different content: CONFLICT, the refused fact is an extra.
            20 => {
                facts.push(push_bar(log, lpk, minute, BarLifecycle::Final, 0, 1));
                facts.push(push_bar(log, lpk, minute, BarLifecycle::Final, 0, 2));
            }
            // Revised r1 then a stale r0: the stale final is an extra.
            40 => {
                facts.push(push_bar(log, lpk, minute, BarLifecycle::Revised, 1, 1));
                facts.push(push_bar(log, lpk, minute, BarLifecycle::Final, 0, 2));
            }
            _ => facts.push(push_bar(log, lpk, minute, BarLifecycle::Final, 0, 1)),
        }
    }
    facts.push(push_bar(log, lpk, 300, BarLifecycle::InProgress, 0, 1));
    let opens: Vec<u64> = (0..=300).collect();
    (facts, opens)
}

// ------------------------------------------------------------ tests

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_plan_tombstones_every_fact_below_the_cap_and_stage_b_converges() {
    let log = Log::default();
    let lpk = bar_lpk(UID);
    let (facts, opens) = rich_history(&log, &lpk);
    let mut stage = stage(&log, &environment("expiry"));
    drain(&mut stage);
    assert_eq!(meta(&mut stage, &lpk, "rows").as_deref(), Some("301"));
    let extras_before = rk_opens(&mut stage, &lpk);
    assert_eq!(extras_before, vec![10 * MIN, 20 * MIN, 40 * MIN, 250 * MIN]);

    // cap 100 of 301 rows: the 100th newest open is minute 201.
    let step = plan(&mut stage, &lpk, 100, 10_000).expect("a plan");
    assert_eq!(step.floor_ms, 201 * MIN);
    assert!(step.complete());
    assert_eq!(step.previous_floor, None);
    assert_eq!(step.expired_opens, 201);
    let keys: BTreeSet<String> = step.tombstone_keys.iter().cloned().collect();
    assert_eq!(keys.len(), step.tombstone_keys.len(), "deduplicated");
    assert_eq!(keys, expected_tombstones(&lpk, &facts, &opens, 201));
    // 201 in-progress keys + 200 current finals (minute 5 is only
    // in-progress) + the 3 extras below the floor (minute 250's is kept).
    assert_eq!(step.tombstone_keys.len(), 201 + 200 + 3);

    // Publish: tombstones, then the floor frame, into the log; stage B applies.
    let before = log.records(BARS, 0).len();
    publish(&log, &step);
    let appended = log.records(BARS, 0).split_off(before);
    assert_eq!(appended.len(), step.tombstone_keys.len() + 1);
    for (record, key) in appended.iter().zip(&step.tombstone_keys) {
        assert_eq!(record.key, key.as_bytes());
        assert!(record.value.is_none(), "tombstone");
    }
    let last = appended.last().unwrap();
    assert_eq!(last.key, floor_key(&lpk).into_bytes());
    let frame = StateFrame::decode(last.value.as_deref().unwrap()).unwrap();
    assert_eq!(frame.floor_open_time_ms, Some(201 * MIN));
    drain(&mut stage);
    assert_eq!(meta(&mut stage, &lpk, "rows").as_deref(), Some("100"));
    assert_eq!(
        meta(&mut stage, &lpk, "floor"),
        Some((201 * MIN).to_string())
    );
    assert!(!has_row(&mut stage, &lpk, 200));
    assert!(has_row(&mut stage, &lpk, 201));
    assert!(has_row(&mut stage, &lpk, 300));
    assert_eq!(
        rk_opens(&mut stage, &lpk),
        vec![250 * MIN],
        "extras below the floor gone"
    );

    // A late fact below the floor is refused and counted.
    let refused = stage.metrics.below_floor;
    push_bar(&log, &lpk, 50, BarLifecycle::Revised, 2, 9);
    drain(&mut stage);
    assert!(stage.metrics.below_floor > refused);
    assert!(!has_row(&mut stage, &lpk, 50));
    assert_eq!(meta(&mut stage, &lpk, "rows").as_deref(), Some("100"));
    // Converged: nothing more to expire.
    assert_eq!(plan(&mut stage, &lpk, 100, 10_000), None);
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn within_one_bucket_of_slack_there_is_no_plan() {
    let log = Log::default();
    let lpk = bar_lpk(UID);
    for minute in 0..216u64 {
        push_bar(&log, &lpk, minute, BarLifecycle::Final, 0, 1);
    }
    let mut stage = stage(&log, &environment("slack"));
    drain(&mut stage);
    assert_eq!(meta(&mut stage, &lpk, "rows").as_deref(), Some("216"));
    assert_eq!(
        plan(&mut stage, &lpk, 100, 10_000),
        None,
        "rows == cap + 116"
    );
    push_bar(&log, &lpk, 216, BarLifecycle::Final, 0, 1);
    drain(&mut stage);
    let plan = plan(&mut stage, &lpk, 100, 10_000).expect("rows == cap + 117");
    assert_eq!(plan.floor_ms, 117 * MIN, "the 100th newest of 0..=216");
    assert_eq!(plan.expired_opens, 117);
    assert_eq!(
        plan.tombstone_keys.len(),
        2 * 117,
        "final + in-progress key per open"
    );
    // A floor never goes down: a plan whose floor does not rise is refused
    // before anything is sent.
    let lowered = ExpiryPlan {
        previous_floor: Some(plan.floor_ms),
        ..plan.clone()
    };
    let before = log.records(BARS, 0).len();
    assert!(publish_expiry(
        &LogSink::new(&log),
        BARS,
        1,
        &lowered,
        1,
        Duration::from_secs(1)
    )
    .is_err());
    assert_eq!(log.records(BARS, 0).len(), before);
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn bounded_steps_converge_to_the_unbounded_floor() {
    let log = Log::default();
    let lpk = bar_lpk(UID);
    let (facts, opens) = rich_history(&log, &lpk);
    let mut stage = stage(&log, &environment("bounded"));
    drain(&mut stage);
    let reference = plan(&mut stage, &lpk, 100, 10_000).expect("reference plan");
    let expected = expected_tombstones(&lpk, &facts, &opens, 201);
    assert_eq!(
        reference
            .tombstone_keys
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected
    );

    let caps: BTreeMap<String, u64> = [(lpk.encode(), 100)].into_iter().collect();
    let mut task = ExpiryTask::new(caps, 4, 60).unwrap();
    let sink = LogSink::new(&log);
    let mut floors = Vec::new();
    let mut tombstones = BTreeSet::new();
    let mut expired = 0;
    for round in 0..20 {
        let before = log.records(BARS, 0).len();
        let report = task
            .tick(&mut stage.cache, &sink, &publish_target())
            .unwrap();
        // Before stage B applied the floor, the next tick publishes nothing.
        let again = task
            .tick(&mut stage.cache, &sink, &publish_target())
            .unwrap();
        if report.floors_published == 1 {
            assert_eq!(
                (again.pending, again.floors_published),
                (1, 0),
                "round {round}"
            );
        }
        for record in log.records(BARS, 0).split_off(before) {
            match record.value {
                None => {
                    assert!(
                        tombstones.insert(String::from_utf8(record.key).unwrap()),
                        "each key once"
                    );
                }
                Some(value) => floors.push(
                    StateFrame::decode(&value)
                        .unwrap()
                        .floor_open_time_ms
                        .unwrap(),
                ),
            }
        }
        expired += report.expired_opens;
        drain(&mut stage);
        if report.floors_published == 0 {
            break;
        }
    }
    // 201 expired opens in steps of at most 60: floors at the 61st, 121st and
    // 181st oldest open, then the target.
    assert_eq!(floors, vec![60 * MIN, 120 * MIN, 180 * MIN, 201 * MIN]);
    assert_eq!(expired, 201);
    assert_eq!(tombstones, expected, "the same keys as the unbounded plan");
    assert_eq!(meta(&mut stage, &lpk, "rows").as_deref(), Some("100"));
    assert_eq!(
        meta(&mut stage, &lpk, "floor"),
        Some((201 * MIN).to_string())
    );
    let idle = task
        .tick(&mut stage.cache, &sink, &publish_target())
        .unwrap();
    assert_eq!((idle.floors_published, idle.pending), (0, 0));
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn the_task_skips_products_without_a_ready_pointer_and_is_bounded_per_tick() {
    let log = Log::default();
    let big = bar_lpk("aaaaaaaa-0000-5000-8000-000000000001");
    let small = bar_lpk("aaaaaaaa-0000-5000-8000-000000000002");
    let absent = bar_lpk("aaaaaaaa-0000-5000-8000-000000000003");
    for minute in 0..250u64 {
        push_bar(&log, &big, minute, BarLifecycle::Final, 0, 1);
    }
    for minute in 0..50u64 {
        push_bar(&log, &small, minute, BarLifecycle::Final, 0, 1);
    }
    let mut stage = stage(&log, &environment("task"));
    drain(&mut stage);
    let caps: BTreeMap<String, u64> = [&big, &small, &absent]
        .iter()
        .map(|lpk| (lpk.encode(), 100))
        .collect();
    let mut task = ExpiryTask::new(caps, 2, 10_000).unwrap();
    let sink = LogSink::new(&log);
    // Sorted order: big, small, absent. Two per tick, round robin.
    let first = task
        .tick(&mut stage.cache, &sink, &publish_target())
        .unwrap();
    assert_eq!(
        (
            first.products_visited,
            first.not_ready,
            first.floors_published
        ),
        (2, 0, 1)
    );
    assert_eq!(first.expired_opens, 150);
    assert_eq!(first.tombstones, 300);
    let second = task
        .tick(&mut stage.cache, &sink, &publish_target())
        .unwrap();
    assert_eq!(
        (
            second.products_visited,
            second.not_ready,
            second.pending,
            second.floors_published
        ),
        (2, 1, 1, 0),
        "absent is NOT_READY; big waits for stage B"
    );
    drain(&mut stage);
    // small, absent; then big (its floor applied, nothing left) and small.
    let third = task
        .tick(&mut stage.cache, &sink, &publish_target())
        .unwrap();
    assert_eq!(
        (
            third.products_visited,
            third.not_ready,
            third.floors_published
        ),
        (2, 1, 0)
    );
    let fourth = task
        .tick(&mut stage.cache, &sink, &publish_target())
        .unwrap();
    assert_eq!(
        (
            fourth.products_visited,
            fourth.not_ready,
            fourth.pending,
            fourth.floors_published
        ),
        (2, 0, 0, 0)
    );
    assert_eq!(meta(&mut stage, &big, "rows").as_deref(), Some("100"));
    assert_eq!(meta(&mut stage, &small, "rows").as_deref(), Some("50"));
    assert_eq!(stage.cache.pointer(&absent.encode()).unwrap().ready, None);
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn an_undecodable_row_stops_the_plan() {
    let log = Log::default();
    let lpk = bar_lpk(UID);
    for minute in 0..250u64 {
        push_bar(&log, &lpk, minute, BarLifecycle::Final, 0, 1);
    }
    let mut stage = stage(&log, &environment("integrity"));
    drain(&mut stage);
    let generation = ready(&mut stage, &lpk);
    let key = stage
        .cache
        .layout
        .bar_bucket(generation, &lpk.encode(), bucket_of(7 * MIN, MIN));
    let _: () = redis::cmd("HSET")
        .arg(key)
        .arg((7 * MIN).to_string())
        .arg(vec![0u8; 60])
        .query(stage.cache.connection())
        .unwrap();
    match plan_expiry(&mut stage.cache, generation, &lpk, 100, 10_000) {
        Err(ExpiryError::Integrity { open_ms, .. }) => assert_eq!(open_ms, 7 * MIN),
        other => panic!("expected an integrity stop, got {other:?}"),
    }
}

const DAY: u64 = 86_400_000;

/// A final bar of any fixed interval at `open_ms` (venue-grid opens).
fn push_final_at(log: &Log, lpk: &LogicalProductKey, interval_ms: u64, open_ms: u64) -> String {
    let envelope = EventEnvelope {
        event_id: EVENT.fetch_add(1, Ordering::Relaxed).to_be_bytes().to_vec(),
        instrument_uid: lpk.instrument_uid.clone(),
        venue: "OKX".into(),
        market: "SWAP".into(),
        payload: Some(Payload::Bar(Bar {
            interval: lpk.qualifier.clone(),
            open_time_ns: (open_ms * 1_000_000) as i64,
            close_time_ns: ((open_ms + interval_ms) * 1_000_000 - 1) as i64,
            is_final: true,
            lifecycle: BarLifecycle::Final as i32,
            trade_count: 1,
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec();
    let frame = StateFrame::bar_revision(
        &envelope,
        lpk,
        SourceCoordinate {
            topic_id: TOPIC_ID.into(),
            partition: 2,
            offset: OFFSET.fetch_add(1, Ordering::Relaxed),
        },
        1,
    )
    .unwrap();
    let key = frame.key().unwrap();
    log.append(BARS, 0, &key, Some(frame.encode().unwrap()));
    key
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn day_and_week_bars_expire_on_the_venue_grid_and_months_are_refused() {
    // (interval, duration, first open on the venue grid, count): OKX daily
    // bars open at 16:00 UTC (UTC+8 midnight), weekly bars on Monday 00:00
    // UTC (the epoch is a Thursday); neither grid is a multiple of the
    // bucket span, so buckets start mid-grid.
    let cases = [
        ("1d", DAY, 19_000 * DAY + 16 * 3_600_000, 300u64),
        ("1w", 7 * DAY, 2_700 * 7 * DAY + 4 * DAY, 250u64),
    ];
    for (interval, interval_ms, first_open, count) in cases {
        let log = Log::default();
        let lpk =
            LogicalProductKey::new("paper", "OKX", "SWAP", UID, "BAR", Some(interval)).unwrap();
        let opens: Vec<u64> = (0..count).map(|k| first_open + k * interval_ms).collect();
        let keys: Vec<String> = opens
            .iter()
            .map(|open| push_final_at(&log, &lpk, interval_ms, *open))
            .collect();
        let mut stage = stage(&log, &environment(&format!("grid{interval}")));
        drain(&mut stage);
        let generation = ready(&mut stage, &lpk);
        let expired = (count - 100) as usize;
        let step = plan_expiry(&mut stage.cache, generation, &lpk, 100, 10_000)
            .unwrap()
            .expect("a plan");
        assert_eq!(
            step.floor_ms, opens[expired],
            "{interval}: 100th newest open"
        );
        assert_eq!(
            step.floor_ms % interval_ms,
            first_open % interval_ms,
            "{interval}: on the venue grid"
        );
        assert_eq!(step.expired_opens, expired as u64);
        let mut expected: BTreeSet<String> = keys[..expired].iter().cloned().collect();
        for open in &opens[..expired] {
            expected.insert(bar_key(&lpk, *open, false, 0, "").unwrap());
        }
        assert_eq!(
            step.tombstone_keys.iter().cloned().collect::<BTreeSet<_>>(),
            expected,
            "{interval}"
        );
        publish(&log, &step);
        drain(&mut stage);
        assert_eq!(meta(&mut stage, &lpk, "rows").as_deref(), Some("100"));
        // Every retained open is in its bucket, every bucket <= 116 opens,
        // nothing below the floor is left.
        let lpk_text = lpk.encode();
        let mut retained = Vec::new();
        for bucket in
            bucket_of(opens[0], interval_ms)..=bucket_of(opens[count as usize - 1], interval_ms)
        {
            let fields: Vec<String> = redis::cmd("HKEYS")
                .arg(stage.cache.layout.bar_bucket(generation, &lpk_text, bucket))
                .query(stage.cache.connection())
                .unwrap();
            assert!(fields.len() as u64 <= 116, "{interval}: bucket {bucket}");
            for field in fields {
                let open: u64 = field.parse().unwrap();
                assert_eq!(bucket_of(open, interval_ms), bucket);
                retained.push(open);
            }
        }
        retained.sort_unstable();
        assert_eq!(retained, opens[expired..].to_vec(), "{interval}");
    }
    // Calendar months have no fixed duration: refused, nothing planned.
    let month = LogicalProductKey::new("paper", "OKX", "SWAP", UID, "BAR", Some("1M")).unwrap();
    let url = std::env::var("QDL_KN_TEST_REDIS").expect("QDL_KN_TEST_REDIS");
    let mut cache = Cache::connect(&url, Layout::new(&environment("month"))).unwrap();
    assert!(matches!(
        plan_expiry(&mut cache, 1, &month, 100, 10_000),
        Err(ExpiryError::Product(_))
    ));
    let caps: BTreeMap<String, u64> = [(month.encode(), 100)].into_iter().collect();
    assert!(matches!(
        ExpiryTask::new(caps, 1, 1),
        Err(ExpiryError::Product(_))
    ));
}
