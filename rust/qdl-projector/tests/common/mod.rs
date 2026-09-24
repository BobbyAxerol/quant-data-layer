//! Shared fixtures of the stage B tests: an in-memory state log with Kafka
//! semantics (offsets, high watermark kept across compaction), its group
//! source and assign-mode reader, synthetic canonical fixtures framed with the
//! real state codec, and helpers over a real, isolated Redis.
#![allow(dead_code, unused_imports)]

pub use prost::Message;
pub use qdl_contracts::qdl::marketdata::v2::{
    event_envelope::Payload, Bar, BarLifecycle, EventEnvelope, MarkIndexPrice, OrderBookDelta,
    OrderBookSnapshot, Quote,
};
pub use qdl_contracts::state_codec::{decode_bar_row, decode_latest_value, StateFrame};
pub use qdl_contracts::state_contract::{LogicalProductKey, SourceCoordinate};
pub use qdl_projector::cache::{bucket_of, Applied, Cache, Layout, Op};
pub use qdl_projector::stage_b::{
    PartitionReader, StageB, StageBError, StageBLimits, StateInput, StateSource,
};
pub use std::collections::{BTreeMap, BTreeSet};
pub use std::sync::atomic::{AtomicU64, Ordering};
pub use std::sync::{Arc, Mutex};
pub use std::time::Duration;

pub const LATEST: &str = "md.latest.v2";
pub const BARS: &str = "md.bars.v2";
pub const TOPIC_ID: &str = "ljfjPYApRpWQd79McfTtZg";
pub const UID: &str = "fb26214c-7b9b-5961-95b2-55154755af0f";
pub const MIN: u64 = 60_000;

// ------------------------------------------------------------ memory log

#[derive(Default)]
pub struct LogInner {
    pub records: BTreeMap<(String, i32), Vec<StateInput>>,
    /// High watermark per partition: compaction (removing records) never
    /// lowers it, as in Kafka.
    pub ends: BTreeMap<(String, i32), i64>,
}

impl LogInner {
    pub fn end(&self, key: &(String, i32)) -> i64 {
        self.ends.get(key).copied().unwrap_or(0)
    }
}

#[derive(Clone, Default)]
pub struct Log(pub Arc<Mutex<LogInner>>);

impl Log {
    pub fn append(&self, topic: &str, partition: i32, key: &str, value: Option<Vec<u8>>) -> i64 {
        let mut inner = self.0.lock().unwrap();
        let slot = (topic.to_owned(), partition);
        let offset = inner.end(&slot);
        inner.ends.insert(slot.clone(), offset + 1);
        let records = inner.records.entry(slot).or_default();
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
pub struct Source {
    pub log: Log,
    pub assigned: Vec<(String, i32)>,
    pub position: BTreeMap<(String, i32), i64>,
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
            match records.last() {
                Some(last) => {
                    self.position.insert(key.clone(), last.offset + 1);
                }
                // Caught up: past compaction gaps to the high watermark.
                None => {
                    self.position.insert(key.clone(), inner.end(key).max(from));
                }
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
        Ok((0, inner.end(&(topic.to_owned(), partition))))
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
pub struct Reader {
    pub log: Log,
    pub at: Option<(String, i32, i64)>,
}

impl PartitionReader for Reader {
    fn watermarks(&mut self, topic: &str, partition: i32) -> Result<(i64, i64), String> {
        let inner = self.log.0.lock().unwrap();
        Ok((0, inner.end(&(topic.to_owned(), partition))))
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
        match records.last() {
            Some(last) => self.at = Some((topic, partition, last.offset + 1)),
            None => {
                let end = inner.end(&(topic.clone(), partition)).max(from);
                self.at = Some((topic, partition, end));
            }
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

pub fn lpk(feed: &str, interval: Option<&str>) -> LogicalProductKey {
    LogicalProductKey::new("paper", "OKX", "SWAP", UID, feed, interval).unwrap()
}

pub static EVENT: AtomicU64 = AtomicU64::new(1);

pub fn envelope(payload: Payload) -> EventEnvelope {
    EventEnvelope {
        event_id: EVENT.fetch_add(1, Ordering::Relaxed).to_be_bytes().to_vec(),
        instrument_uid: UID.into(),
        venue: "OKX".into(),
        market: "SWAP".into(),
        payload: Some(payload),
        ..Default::default()
    }
}

pub fn quote(level: u32) -> Vec<u8> {
    envelope(Payload::Quote(Quote {
        level,
        ..Default::default()
    }))
    .encode_to_vec()
}

pub fn bar(open_min: u64, lifecycle: BarLifecycle, revision: u32, close: u32) -> Vec<u8> {
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

pub fn source(offset: u64) -> SourceCoordinate {
    SourceCoordinate {
        topic_id: TOPIC_ID.into(),
        partition: 2,
        offset,
    }
}

pub fn latest_frame(lpk: &LogicalProductKey, envelope: Vec<u8>, offset: u64) -> (String, Vec<u8>) {
    let frame = StateFrame::latest(&envelope, lpk, source(offset), 1).unwrap();
    (frame.key().unwrap(), frame.encode().unwrap())
}

pub fn bar_frame(lpk: &LogicalProductKey, envelope: Vec<u8>, offset: u64) -> (String, Vec<u8>) {
    let frame = StateFrame::bar_revision(&envelope, lpk, source(offset), 1).unwrap();
    (frame.key().unwrap(), frame.encode().unwrap())
}

pub fn floor_frame(lpk: &LogicalProductKey, floor_ms: u64) -> (String, Vec<u8>) {
    let frame = StateFrame::retention_floor(lpk, floor_ms, 1).unwrap();
    (frame.key().unwrap(), frame.encode().unwrap())
}

pub fn push(log: &Log, topic: &str, (key, value): (String, Vec<u8>)) -> i64 {
    log.append(topic, 0, &key, Some(value))
}

pub fn environment(label: &str) -> String {
    format!(
        "{label}{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

pub fn stage(log: &Log, environment: &str) -> StageB<Source> {
    let url = std::env::var("QDL_KN_TEST_REDIS")
        .expect("QDL_KN_TEST_REDIS must name an isolated test Redis; never the control Redis");
    stage_on(&url, log, environment)
}

/// A stage over `url` (the dedicated memory-test Redis for `maxmemory` cases).
pub fn stage_on(url: &str, log: &Log, environment: &str) -> StageB<Source> {
    let cache = Cache::connect(url, Layout::new(environment)).unwrap();
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

pub fn drain(stage: &mut StageB<Source>) {
    for _ in 0..200 {
        stage.step().expect("step");
    }
}

/// The ready generation's latest canonical bytes of `lpk`, if READY.
pub fn read_latest(stage: &mut StageB<Source>, lpk: &LogicalProductKey) -> Option<(Vec<u8>, u64)> {
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

pub fn read_bar(
    stage: &mut StageB<Source>,
    lpk: &LogicalProductKey,
    open_min: u64,
) -> Option<Vec<u8>> {
    let generation = stage.cache.pointer(&lpk.encode()).unwrap().ready?;
    let open_ms = open_min * MIN;
    let row = stage
        .cache
        .bar_row(generation, &lpk.encode(), bucket_of(open_ms, MIN), open_ms)
        .unwrap()?;
    Some(decode_bar_row(&row, lpk).unwrap().canonical)
}

pub fn meta(stage: &mut StageB<Source>, lpk: &LogicalProductKey, field: &str) -> Option<String> {
    let generation = stage.cache.pointer(&lpk.encode()).unwrap().ready?;
    redis::cmd("HGET")
        .arg(stage.cache.layout.bar_meta(generation, &lpk.encode()))
        .arg(field)
        .query(stage.cache.connection())
        .unwrap()
}

pub fn skip_redis() {
    // Present only so the ignore reason is uniform.
}

pub fn rebuild_stage(log: &Log, environment: &str) -> StageB<Source> {
    stage(log, environment).with_rebuild_reader(Box::new(Reader {
        log: log.clone(),
        at: None,
    }))
}

pub fn until_rebuilt(stage: &mut StageB<Source>) {
    for _ in 0..200 {
        stage.step().expect("step");
        if stage.rebuilding().is_none() {
            return;
        }
    }
    panic!("the rebuild did not finish");
}

pub fn meta_exists(stage: &mut StageB<Source>, generation: u64, lpk: &str) -> bool {
    let key = stage.cache.layout.bar_meta(generation, lpk);
    redis::cmd("EXISTS")
        .arg(key)
        .query::<u64>(stage.cache.connection())
        .unwrap()
        == 1
}
