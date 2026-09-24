//! Stage B: state topics -> market cache (KN-3 K3.3, decisions D7-D9, D12).
//!
//! Each assigned state-topic partition is owned through an owner fence in the
//! cache (`own:<t>:<p>`); every batch is applied atomically with its
//! checkpoint by `apply.lua`, so a zombie owner applies nothing and a crash
//! can only re-apply records the rules then call DUPLICATE or STALE.
//!
//! A partition is either **normal** (tailing from its cache checkpoint) or
//! **building** a fresh generation from the partition start (no checkpoint,
//! a checkpoint older than the rebuild horizon, or below the earliest offset:
//! a cache that may have missed deletes is never overlaid). At the build
//! boundary (the end offset read at assignment) every staged product is
//! published and products whose state is gone are unpublished (NOT_READY).
//!
//! Within one batch, records of the same entry are collapsed by the state
//! rules into one operation that expects the entry as it was before the
//! batch (the script checks every expectation before writing anything).
//! Pointer changes (stage, publish) run as their own atomic steps.
//!
//! A per-product rebuild (D17) runs inside the partition owner between its
//! batches: a second reader replays the partition from its start in log
//! order into a staged generation (live records keep going to the ready one
//! only), and the product is published when the replay reaches the live
//! checkpoint, in the same step, so no live record can interleave.

use crate::cache::{bucket_of, Applied, Cache, CacheError, Op, Pointer};
use prost::Message;
use qdl_contracts::interval::canonical_interval_ms;
use qdl_contracts::qdl::marketdata::v2::{event_envelope::Payload, EventEnvelope};
use qdl_contracts::state_codec::{decode_bar_row, encode_bar_row, FrameKind, StateFrame};
use qdl_contracts::state_contract::{
    bar_revision_decision, latest_apply_decision, ApplyDecision, BarState, LogicalProductKey,
    SourceCoordinate, MAX_OFFSET,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

/// The trailer offset of a legacy-imported row: no canonical coordinate
/// exists, and no real Kafka offset ever reaches 2^63-1 (decision D13).
pub const LEGACY_SOURCE_OFFSET: u64 = MAX_OFFSET;
const LEGACY_TOPIC: &str = "legacy_import";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateInput {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Vec<u8>,
    /// `None` = tombstone.
    pub value: Option<Vec<u8>>,
}

/// The consumer of the state topics (Kafka in production).
pub trait StateSource {
    fn poll(&mut self, max: usize, timeout: Duration) -> Result<Vec<StateInput>, String>;
    fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<(), String>;
    /// `(earliest, end)` offsets of a partition.
    fn watermarks(&mut self, topic: &str, partition: i32) -> Result<(i64, i64), String>;
    /// Next offset the consumer will read (past control records), if known.
    fn position(&mut self, topic: &str, partition: i32) -> Result<Option<i64>, String>;
    fn assigned(&mut self) -> Result<Vec<(String, i32)>, String>;
    /// Informational group offset (restart uses the cache checkpoint).
    fn commit(&mut self, topic: &str, partition: i32, next: i64) -> Result<(), String>;
}

/// A reader of one state-topic partition outside the consumer group
/// (`read_committed`, assign mode): the per-product rebuild replay (D17).
/// Also the bars-topic cleaner's reader.
pub trait PartitionReader {
    /// `(earliest, end)` offsets of a partition.
    fn watermarks(&mut self, topic: &str, partition: i32) -> Result<(i64, i64), String>;
    fn start(&mut self, topic: &str, partition: i32, offset: i64) -> Result<(), String>;
    fn poll(&mut self, max: usize, timeout: Duration) -> Result<Vec<StateInput>, String>;
    /// Next offset the reader will read (past control records), if known.
    fn position(&mut self, topic: &str, partition: i32) -> Result<Option<i64>, String>;
    fn stop(&mut self) -> Result<(), String>;
}

#[derive(Clone, Debug)]
pub struct StageBLimits {
    pub max_batch_records: usize,
    pub poll_timeout: Duration,
    /// Tombstone lifetime minus a margin (D9): a checkpoint older than this
    /// rebuilds the partition instead of overlaying the cache.
    pub rebuild_horizon: Duration,
    pub max_cas_retries: usize,
}

impl Default for StageBLimits {
    fn default() -> Self {
        Self {
            max_batch_records: 500,
            poll_timeout: Duration::from_millis(50),
            rebuild_horizon: Duration::from_secs(6 * 86_400),
            max_cas_retries: 5,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StageBMetrics {
    pub batches: u64,
    pub latest_applied: u64,
    pub bars_applied: u64,
    pub duplicates: u64,
    pub stale: u64,
    pub conflicts: u64,
    pub not_comparable: u64,
    pub below_floor: u64,
    pub floors: u64,
    pub cas_retries: u64,
    pub zombies: u64,
    pub builds: u64,
    pub published: u64,
    pub unpublished: u64,
    pub reclaimed_keys: u64,
    pub rebuilds_started: u64,
    pub rebuilds_completed: u64,
    pub rebuilds_abandoned: u64,
    pub rebuilds_refused: u64,
    pub rebuild_records: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageBError {
    /// A state record that does not decode: the partition stops (never skipped).
    Integrity {
        topic: String,
        partition: i32,
        offset: i64,
        reason: String,
    },
    Cache(CacheError),
    Source(String),
}

impl From<CacheError> for StageBError {
    fn from(error: CacheError) -> Self {
        StageBError::Cache(error)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Mode {
    Normal,
    Build { generation: u64, boundary: i64 },
}

#[derive(Clone, Debug)]
struct PartitionState {
    fence: u64,
    mode: Mode,
    next: i64,
}

/// The per-product rebuild in progress.
#[derive(Clone, Debug)]
struct ActiveRebuild {
    lpk: String,
    topic: String,
    partition: i32,
    generation: u64,
}

pub struct StageB<S: StateSource> {
    pub source: S,
    pub cache: Cache,
    pub limits: StageBLimits,
    pub metrics: StageBMetrics,
    /// Why the last requested rebuild was refused or abandoned.
    pub last_rebuild_error: Option<String>,
    partitions: HashMap<(String, i32), PartitionState>,
    now_ms: fn() -> u64,
    rebuild_reader: Option<Box<dyn PartitionReader + Send>>,
    rebuild_queue: VecDeque<String>,
    rebuild: Option<ActiveRebuild>,
}

fn system_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or_default()
}

/// Fact key suffix of a final row (`f<rev>|<sha16>`), as in the bar key.
fn fact_suffix(revision: u32, content_sha256: &str) -> String {
    format!(
        "f{revision}|{}",
        &content_sha256[..16.min(content_sha256.len())]
    )
}

fn bar_facts(canonical: &[u8]) -> Option<(bool, u32)> {
    let envelope = EventEnvelope::decode(canonical).ok()?;
    match envelope.payload {
        Some(Payload::Bar(bar)) => Some((bar.is_final, bar.revision)),
        _ => None,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

/// One entry's pending write within a batch (collapsed by the rules).
struct PendingLatest {
    expected: Option<u64>,
    current: Option<SourceCoordinate>,
    write: Option<(Vec<u8>, SourceCoordinate)>,
    /// A tombstone was the last change in log order so far.
    delete: bool,
}

struct PendingBar {
    bucket: u64,
    expected: Option<Vec<u8>>,
    current: Option<BarState>,
    current_is_final: bool,
    write: Option<(Vec<u8>, bool)>,
    superseded: Vec<String>,
}

enum Group {
    Latest(PendingLatest),
    Bar(PendingBar),
}

impl<S: StateSource> StageB<S> {
    pub fn new(source: S, cache: Cache, limits: StageBLimits) -> Self {
        Self {
            source,
            cache,
            limits,
            metrics: StageBMetrics::default(),
            last_rebuild_error: None,
            partitions: HashMap::new(),
            now_ms: system_now_ms,
            rebuild_reader: None,
            rebuild_queue: VecDeque::new(),
            rebuild: None,
        }
    }

    /// The reader per-product rebuilds replay with (without one, a request
    /// is refused).
    pub fn with_rebuild_reader(mut self, reader: Box<dyn PartitionReader + Send>) -> Self {
        self.rebuild_reader = Some(reader);
        self
    }

    /// Queue a per-product rebuild (D17); one runs at a time, in order.
    pub fn request_rebuild(&mut self, lpk: &str) {
        let queued = self.rebuild_queue.iter().any(|item| item == lpk)
            || self
                .rebuild
                .as_ref()
                .is_some_and(|active| active.lpk == lpk);
        if !queued {
            self.rebuild_queue.push_back(lpk.to_owned());
        }
    }

    /// Partitions this instance currently owns (tailing or building).
    pub fn owned(&self) -> Vec<(String, i32)> {
        let mut owned: Vec<(String, i32)> = self.partitions.keys().cloned().collect();
        owned.sort();
        owned
    }

    /// Whether a partition this instance owns holds the product.
    pub fn holds_product(&mut self, lpk: &str) -> Result<bool, StageBError> {
        for (topic, partition) in self.owned() {
            if self
                .cache
                .registry(&topic, partition)?
                .iter()
                .any(|item| item == lpk)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The product being rebuilt and its staged generation, if any.
    pub fn rebuilding(&self) -> Option<(&str, u64)> {
        self.rebuild
            .as_ref()
            .map(|active| (active.lpk.as_str(), active.generation))
    }

    /// Replace the clock (tests of the rebuild horizon).
    pub fn with_clock(mut self, now_ms: fn() -> u64) -> Self {
        self.now_ms = now_ms;
        self
    }

    pub fn building(&self, topic: &str, partition: i32) -> bool {
        matches!(
            self.partitions
                .get(&(topic.to_owned(), partition))
                .map(|state| &state.mode),
            Some(Mode::Build { .. })
        )
    }

    /// One poll -> apply cycle over every partition with records.
    pub fn step(&mut self) -> Result<usize, StageBError> {
        let batch = self
            .source
            .poll(self.limits.max_batch_records, self.limits.poll_timeout)
            .map_err(StageBError::Source)?;
        let assigned = self.source.assigned().map_err(StageBError::Source)?;
        self.partitions
            .retain(|key, _| assigned.iter().any(|(t, p)| t == &key.0 && *p == key.1));
        // Newly assigned partitions: take ownership, choose the mode and seek.
        // Their records in this batch were fetched before the seek and are
        // dropped (librdkafka purges the partition's queue on seek).
        let mut fresh_partitions = std::collections::BTreeSet::new();
        for (topic, partition) in &assigned {
            let key = (topic.clone(), *partition);
            if !self.partitions.contains_key(&key) {
                let state = self.prepare(topic, *partition)?;
                self.source
                    .seek(topic, *partition, state.next)
                    .map_err(StageBError::Source)?;
                self.partitions.insert(key.clone(), state);
                fresh_partitions.insert(key);
            }
        }
        let mut groups: BTreeMap<(String, i32), Vec<StateInput>> = BTreeMap::new();
        for input in batch {
            let key = (input.topic.clone(), input.partition);
            if fresh_partitions.contains(&key) || !self.partitions.contains_key(&key) {
                continue;
            }
            groups.entry(key).or_default().push(input);
        }
        let mut applied = 0;
        for ((topic, partition), inputs) in groups {
            applied += self.partition_batch(&topic, partition, inputs)?;
        }
        // Idle builders may have reached their boundary (trailing markers).
        for (topic, partition) in assigned {
            self.finish_build_if_done(&topic, partition)?;
        }
        self.advance_rebuild()?;
        Ok(applied)
    }

    fn prepare(&mut self, topic: &str, partition: i32) -> Result<PartitionState, StageBError> {
        let fence = self.cache.take_ownership(topic, partition)?;
        let checkpoint = self.cache.checkpoint(topic, partition)?;
        let (earliest, end) = self
            .source
            .watermarks(topic, partition)
            .map_err(StageBError::Source)?;
        let now = (self.now_ms)();
        let horizon = self.limits.rebuild_horizon.as_millis() as u64;
        let state = match checkpoint {
            Some(checkpoint)
                if now.saturating_sub(checkpoint.at_ms) <= horizon
                    && checkpoint.next >= earliest =>
            {
                PartitionState {
                    fence,
                    mode: Mode::Normal,
                    next: checkpoint.next,
                }
            }
            _ => {
                self.metrics.builds += 1;
                PartitionState {
                    fence,
                    mode: Mode::Build {
                        generation: self.cache.allocate_generation()?,
                        boundary: end,
                    },
                    next: earliest,
                }
            }
        };
        Ok(state)
    }

    fn partition_batch(
        &mut self,
        topic: &str,
        partition: i32,
        inputs: Vec<StateInput>,
    ) -> Result<usize, StageBError> {
        let key = (topic.to_owned(), partition);
        let state = self.partitions[&key].clone();
        let inputs: Vec<StateInput> = inputs
            .into_iter()
            .filter(|input| input.offset >= state.next)
            .collect();
        let Some(last) = inputs.last().map(|input| input.offset) else {
            return Ok(0);
        };
        let next = last + 1;
        let mut frames = Vec::with_capacity(inputs.len());
        for input in &inputs {
            frames.push(self.decode(topic, partition, input)?);
        }
        let mut attempt = 0;
        let before = self.metrics.clone();
        loop {
            if !self.stage_missing(topic, partition, &state, &frames)? {
                return Ok(0);
            }
            let (ops, fresh) = self.build_ops(&state, &frames)?;
            match self
                .cache
                .apply(topic, partition, state.fence, next, (self.now_ms)(), &ops)?
            {
                Applied::Ok(results) => {
                    self.metrics.batches += 1;
                    for result in &results {
                        match result.as_str() {
                            "STALE_BELOW_FLOOR" => self.metrics.below_floor += 1,
                            "APPLIED" => {}
                            _ => {}
                        }
                    }
                    if let Some(entry) = self.partitions.get_mut(&key) {
                        entry.next = next;
                    }
                    let _ = self.source.commit(topic, partition, next);
                    // Products first seen while tailing are ready at once.
                    if !fresh.is_empty() {
                        self.publish(topic, partition, &state, &fresh)?;
                    }
                    self.finish_build_if_done(topic, partition)?;
                    return Ok(inputs.len());
                }
                Applied::Zombie { .. } => {
                    // Another replica owns the partition now.
                    self.metrics.zombies += 1;
                    self.partitions.remove(&key);
                    return Ok(0);
                }
                Applied::Miss(_) => {
                    // Counters of the missed attempt are not kept.
                    let retries = self.metrics.cas_retries + 1;
                    self.metrics = before.clone();
                    self.metrics.cas_retries = retries;
                    attempt += 1;
                    if attempt > self.limits.max_cas_retries {
                        return Err(StageBError::Source(format!(
                            "{topic}/{partition}: expectations kept changing ({attempt} retries)"
                        )));
                    }
                }
            }
        }
    }

    /// `None` for a tombstone, else the strictly decoded frame.
    fn decode(
        &self,
        topic: &str,
        partition: i32,
        input: &StateInput,
    ) -> Result<(Vec<u8>, Option<StateFrame>), StageBError> {
        let frame = match &input.value {
            None => None,
            Some(value) => {
                Some(
                    StateFrame::decode(value).map_err(|error| StageBError::Integrity {
                        topic: topic.to_owned(),
                        partition,
                        offset: input.offset,
                        reason: format!("{}:{}", error.reason, error.detail),
                    })?,
                )
            }
        };
        Ok((input.key.clone(), frame))
    }

    /// The product a record belongs to (tombstones: from the key).
    fn product(key: &[u8], frame: &Option<StateFrame>) -> Option<String> {
        match frame {
            Some(frame) => Some(frame.lpk.encode()),
            None => {
                // A latest-topic tombstone key is the LPK itself; BAR fact
                // tombstones (`<lpk>|<ms>|...`) only serve compaction: the
                // floor frame already removed their rows.
                let text = std::str::from_utf8(key).ok()?;
                LogicalProductKey::parse(text).ok().map(|lpk| lpk.encode())
            }
        }
    }

    /// Stage the build generation (or a first generation for a product seen
    /// for the first time while tailing). Returns `false` if the partition
    /// was lost to another owner.
    fn stage_missing(
        &mut self,
        topic: &str,
        partition: i32,
        state: &PartitionState,
        frames: &[(Vec<u8>, Option<StateFrame>)],
    ) -> Result<bool, StageBError> {
        let mut ops = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for (key, frame) in frames {
            if frame.is_none() {
                continue;
            }
            let Some(lpk) = Self::product(key, frame) else {
                continue;
            };
            if !seen.insert(lpk.clone()) {
                continue;
            }
            let pointer = self.cache.pointer(&lpk)?;
            let wanted = match state.mode {
                Mode::Build { generation, .. } => {
                    (pointer.staging != Some(generation)).then_some(generation)
                }
                Mode::Normal => (pointer.ready.is_none() && pointer.staging.is_none())
                    .then(|| self.cache.allocate_generation())
                    .transpose()?,
            };
            if let Some(generation) = wanted {
                ops.push(Op::Stage {
                    lpk,
                    pointer,
                    generation,
                });
            }
        }
        if ops.is_empty() {
            return Ok(true);
        }
        match self.cache.apply(
            topic,
            partition,
            state.fence,
            state.next,
            (self.now_ms)(),
            &ops,
        )? {
            Applied::Ok(_) => Ok(true),
            Applied::Zombie { .. } => {
                self.metrics.zombies += 1;
                self.partitions.remove(&(topic.to_owned(), partition));
                Ok(false)
            }
            Applied::Miss(_) => {
                self.metrics.cas_retries += 1;
                Ok(true)
            }
        }
    }

    /// The generations a product's record is written to in this mode.
    fn targets(state: &PartitionState, pointer: &Pointer) -> Vec<u64> {
        match state.mode {
            Mode::Build { generation, .. } => vec![generation],
            Mode::Normal => pointer.targets(),
        }
    }

    /// Collapse the batch into one op per entry; returns the ops and the
    /// products staged for the first time while tailing (to publish).
    fn build_ops(
        &mut self,
        state: &PartitionState,
        frames: &[(Vec<u8>, Option<StateFrame>)],
    ) -> Result<(Vec<Op>, Vec<String>), StageBError> {
        let mut pointers: HashMap<String, Pointer> = HashMap::new();
        let mut order: Vec<(u64, String, Option<u64>)> = Vec::new();
        let mut groups: HashMap<(u64, String, Option<u64>), Group> = HashMap::new();
        let mut tail_ops: Vec<Op> = Vec::new();
        let mut fresh = Vec::new();
        for (key, frame) in frames {
            let Some(lpk) = Self::product(key, frame) else {
                continue;
            };
            if !pointers.contains_key(&lpk) {
                let pointer = self.cache.pointer(&lpk)?;
                if state.mode == Mode::Normal
                    && pointer.ready.is_none()
                    && pointer.staging.is_some()
                {
                    fresh.push(lpk.clone());
                }
                pointers.insert(lpk.clone(), pointer);
            }
            let pointer = pointers[&lpk].clone();
            let targets = Self::targets(state, &pointer);
            let Some(frame) = frame else {
                // Latest tombstone: the product's state is gone - in log
                // order with the other changes of the entry in this batch.
                for generation in targets {
                    let entry = self.latest_group(generation, &lpk, &mut order, &mut groups)?;
                    if let Some(Group::Latest(pending)) = groups.get_mut(&entry) {
                        pending.write = None;
                        pending.current = None;
                        pending.delete = true;
                    }
                }
                continue;
            };
            for generation in targets {
                match frame.kind {
                    FrameKind::Latest => {
                        self.collect_latest(generation, &lpk, frame, &mut order, &mut groups)?
                    }
                    FrameKind::BarRevision | FrameKind::LegacyBar => self.collect_bar(
                        generation,
                        &lpk,
                        frame,
                        &mut order,
                        &mut groups,
                        &mut tail_ops,
                        &pointer,
                    )?,
                    FrameKind::RetentionFloor => {
                        let floor_ms = frame.floor_open_time_ms.unwrap_or_default();
                        let interval_ms = canonical_interval_ms(&frame.lpk.qualifier)
                            .map_err(StageBError::Source)?;
                        let (first, floor) = self.cache.bar_bounds(generation, &lpk)?;
                        if floor.is_some_and(|floor| floor >= floor_ms) {
                            continue;
                        }
                        let boundary = bucket_of(floor_ms, interval_ms);
                        let buckets = match first {
                            Some(first) => (bucket_of(first, interval_ms)..boundary).collect(),
                            None => Vec::new(),
                        };
                        self.metrics.floors += 1;
                        tail_ops.push(Op::Floor {
                            lpk: lpk.clone(),
                            generation,
                            pointer: pointer.clone(),
                            floor_ms,
                            buckets,
                            boundary: Some(boundary),
                        });
                    }
                }
            }
        }
        let mut ops = Vec::new();
        for entry in order {
            let pointer = pointers[&entry.1].clone();
            match groups.remove(&entry) {
                Some(Group::Latest(pending)) => {
                    if pending.delete && pending.write.is_none() {
                        ops.push(Op::DeleteLatest {
                            lpk: entry.1.clone(),
                            generation: entry.0,
                            pointer: pointer.clone(),
                        });
                    }
                    if let Some((value, source)) = pending.write {
                        self.metrics.latest_applied += 1;
                        ops.push(Op::Latest {
                            lpk: entry.1.clone(),
                            generation: entry.0,
                            pointer,
                            expected_offset: pending.expected,
                            value,
                            topic_id: source.topic_id,
                            partition: source.partition,
                            offset: source.offset,
                        });
                    }
                }
                Some(Group::Bar(pending)) => {
                    if let Some((row, is_final)) = pending.write {
                        self.metrics.bars_applied += 1;
                        ops.push(Op::Bar {
                            lpk: entry.1.clone(),
                            generation: entry.0,
                            pointer,
                            bucket: pending.bucket,
                            open_ms: entry.2.unwrap_or_default(),
                            expected_trailer: pending.expected,
                            row,
                            is_final,
                            superseded: (!pending.superseded.is_empty())
                                .then(|| pending.superseded.join(",")),
                        });
                    }
                }
                None => {}
            }
        }
        // Notes, floors and deletes after the rows of this batch.
        ops.extend(tail_ops);
        Ok((ops, fresh))
    }

    /// The batch group of a latest entry, created from the cache state
    /// before the batch.
    fn latest_group(
        &mut self,
        generation: u64,
        lpk: &str,
        order: &mut Vec<(u64, String, Option<u64>)>,
        groups: &mut HashMap<(u64, String, Option<u64>), Group>,
    ) -> Result<(u64, String, Option<u64>), StageBError> {
        let entry = (generation, lpk.to_owned(), None);
        if !groups.contains_key(&entry) {
            let current = self.cache.latest_coordinate(generation, lpk)?;
            groups.insert(
                entry.clone(),
                Group::Latest(PendingLatest {
                    expected: current.as_ref().map(|c| c.2),
                    current: current.map(|(topic_id, partition, offset)| SourceCoordinate {
                        topic_id,
                        partition,
                        offset,
                    }),
                    write: None,
                    delete: false,
                }),
            );
            order.push(entry.clone());
        }
        Ok(entry)
    }

    fn collect_latest(
        &mut self,
        generation: u64,
        lpk: &str,
        frame: &StateFrame,
        order: &mut Vec<(u64, String, Option<u64>)>,
        groups: &mut HashMap<(u64, String, Option<u64>), Group>,
    ) -> Result<(), StageBError> {
        let entry = self.latest_group(generation, lpk, order, groups)?;
        let Some(Group::Latest(pending)) = groups.get_mut(&entry) else {
            return Ok(());
        };
        let Some(source) = frame.source.clone() else {
            return Ok(());
        };
        match latest_apply_decision(pending.current.as_ref(), &source) {
            ApplyDecision::Apply => {
                let value = qdl_contracts::state_codec::encode_latest_value(
                    &frame.envelope,
                    source.offset,
                    frame.materializer_epoch,
                )
                .map_err(|error| StageBError::Source(error.to_string()))?;
                pending.current = Some(source.clone());
                pending.write = Some((value, source));
                pending.delete = false;
            }
            ApplyDecision::Duplicate | ApplyDecision::Stale => self.metrics.stale += 1,
            ApplyDecision::NotComparable => self.metrics.not_comparable += 1,
            _ => {}
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_bar(
        &mut self,
        generation: u64,
        lpk: &str,
        frame: &StateFrame,
        order: &mut Vec<(u64, String, Option<u64>)>,
        groups: &mut HashMap<(u64, String, Option<u64>), Group>,
        tail_ops: &mut Vec<Op>,
        pointer: &Pointer,
    ) -> Result<(), StageBError> {
        let open_ms = frame.open_time_ms.unwrap_or_default();
        let interval_ms =
            canonical_interval_ms(&frame.lpk.qualifier).map_err(StageBError::Source)?;
        let bucket = bucket_of(open_ms, interval_ms);
        let is_legacy = frame.kind == FrameKind::LegacyBar;
        let source = frame.source.clone().unwrap_or(SourceCoordinate {
            topic_id: LEGACY_TOPIC.into(),
            partition: 0,
            offset: LEGACY_SOURCE_OFFSET,
        });
        let entry = (generation, lpk.to_owned(), Some(open_ms));
        if !groups.contains_key(&entry) {
            let row = self.cache.bar_row(generation, lpk, bucket, open_ms)?;
            let (current, current_is_final, expected) = match row {
                None => (None, false, None),
                Some(row) => {
                    let decoded = decode_bar_row(&row, &frame.lpk)
                        .map_err(|error| StageBError::Source(error.to_string()))?;
                    let (is_final, revision) = bar_facts(&decoded.canonical).unwrap_or_default();
                    let current_source = if decoded.source_offset == LEGACY_SOURCE_OFFSET {
                        SourceCoordinate {
                            topic_id: LEGACY_TOPIC.into(),
                            partition: 0,
                            offset: LEGACY_SOURCE_OFFSET,
                        }
                    } else {
                        SourceCoordinate {
                            offset: decoded.source_offset,
                            ..source.clone()
                        }
                    };
                    (
                        Some(BarState {
                            is_final,
                            revision,
                            content_sha256: sha256_hex(&decoded.canonical),
                            source: current_source,
                        }),
                        is_final,
                        Some(row[..48].to_vec()),
                    )
                }
            };
            groups.insert(
                entry.clone(),
                Group::Bar(PendingBar {
                    bucket,
                    expected,
                    current,
                    current_is_final,
                    write: None,
                    superseded: Vec::new(),
                }),
            );
            order.push(entry.clone());
        }
        let Some(Group::Bar(pending)) = groups.get_mut(&entry) else {
            return Ok(());
        };
        let incoming = BarState {
            is_final: frame.is_final.unwrap_or(false),
            revision: frame.revision.unwrap_or_default(),
            content_sha256: frame.content_sha256.clone().unwrap_or_default(),
            source: source.clone(),
        };
        let incoming_fact = incoming
            .is_final
            .then(|| fact_suffix(incoming.revision, &incoming.content_sha256));
        match bar_revision_decision(pending.current.as_ref(), &incoming) {
            ApplyDecision::Apply => {
                let offset = if is_legacy {
                    LEGACY_SOURCE_OFFSET
                } else {
                    source.offset
                };
                let row = encode_bar_row(
                    &frame.envelope,
                    &frame.lpk,
                    offset,
                    frame.materializer_epoch,
                )
                .map_err(|error| StageBError::Source(error.to_string()))?;
                if let Some(current) = &pending.current {
                    if pending.current_is_final {
                        pending
                            .superseded
                            .push(fact_suffix(current.revision, &current.content_sha256));
                    }
                }
                pending.current = Some(incoming.clone());
                pending.current_is_final = incoming.is_final;
                pending.write = Some((row, incoming.is_final));
            }
            ApplyDecision::Duplicate => self.metrics.duplicates += 1,
            ApplyDecision::Conflict => {
                self.metrics.conflicts += 1;
                let current_sha = pending
                    .current
                    .as_ref()
                    .map(|c| c.content_sha256.clone())
                    .unwrap_or_default();
                tail_ops.push(Op::BarNote {
                    lpk: lpk.to_owned(),
                    generation,
                    pointer: pointer.clone(),
                    open_ms,
                    fact: incoming_fact,
                    conflict: Some(format!(
                        "{{\"open_ms\":{open_ms},\"revision\":{},\"kept\":\"{current_sha}\",\"refused\":\"{}\",\"offset\":{}}}",
                        incoming.revision, incoming.content_sha256, source.offset
                    )),
                });
            }
            ApplyDecision::NotComparable => self.metrics.not_comparable += 1,
            _ => {
                // Stale in-progress or older revision: not applied, but a
                // final fact's key is remembered so expiry tombstones it.
                self.metrics.stale += 1;
                if incoming_fact.is_some() {
                    tail_ops.push(Op::BarNote {
                        lpk: lpk.to_owned(),
                        generation,
                        pointer: pointer.clone(),
                        open_ms,
                        fact: incoming_fact,
                        conflict: None,
                    });
                }
            }
        }
        Ok(())
    }

    /// Publish products staged for the first time while tailing.
    fn publish(
        &mut self,
        topic: &str,
        partition: i32,
        state: &PartitionState,
        lpks: &[String],
    ) -> Result<(), StageBError> {
        let mut ops = Vec::new();
        for lpk in lpks {
            let pointer = self.cache.pointer(lpk)?;
            if pointer.staging.is_some() && pointer.ready.is_none() {
                ops.push(Op::Publish {
                    lpk: lpk.clone(),
                    pointer,
                });
            }
        }
        self.apply_pointer_ops(topic, partition, state, ops)
    }

    /// At the build boundary: publish every staged product, unpublish those
    /// whose state no longer exists, reclaim superseded generations.
    fn finish_build_if_done(&mut self, topic: &str, partition: i32) -> Result<(), StageBError> {
        let key = (topic.to_owned(), partition);
        let Some(state) = self.partitions.get(&key).cloned() else {
            return Ok(());
        };
        let Mode::Build {
            generation,
            boundary,
        } = state.mode
        else {
            return Ok(());
        };
        let position = self
            .source
            .position(topic, partition)
            .map_err(StageBError::Source)?
            .unwrap_or(state.next);
        if state.next.max(position) < boundary {
            return Ok(());
        }
        let mut ops = Vec::new();
        for lpk in self.cache.registry(topic, partition)? {
            let pointer = self.cache.pointer(&lpk)?;
            if pointer.staging == Some(generation) {
                ops.push(Op::Publish { lpk, pointer });
            } else if pointer.ready.is_some() || pointer.staging.is_some() {
                ops.push(Op::Unpublish { lpk, pointer });
            }
        }
        self.apply_pointer_ops(topic, partition, &state, ops)?;
        if let Some(entry) = self.partitions.get_mut(&key) {
            entry.mode = Mode::Normal;
        }
        Ok(())
    }

    fn apply_pointer_ops(
        &mut self,
        topic: &str,
        partition: i32,
        state: &PartitionState,
        ops: Vec<Op>,
    ) -> Result<(), StageBError> {
        // Always applied, also without ops: a finished build records its
        // checkpoint so a restart tails instead of building again.
        let next = self
            .partitions
            .get(&(topic.to_owned(), partition))
            .map(|entry| entry.next)
            .unwrap_or(state.next);
        match self
            .cache
            .apply(topic, partition, state.fence, next, (self.now_ms)(), &ops)?
        {
            Applied::Ok(results) => {
                for (op, result) in ops.iter().zip(results) {
                    let (lpk, generations) = match op {
                        Op::Publish { lpk, .. } => {
                            self.metrics.published += 1;
                            (lpk, result)
                        }
                        Op::Unpublish { lpk, .. } => {
                            self.metrics.unpublished += 1;
                            (lpk, result)
                        }
                        _ => continue,
                    };
                    let interval_ms = LogicalProductKey::parse(lpk)
                        .ok()
                        .filter(|parsed| parsed.feed == "BAR")
                        .and_then(|parsed| canonical_interval_ms(&parsed.qualifier).ok());
                    for old in generations
                        .split(',')
                        .filter_map(|value| value.parse::<u64>().ok())
                    {
                        self.metrics.reclaimed_keys += self.cache.reclaim(old, lpk, interval_ms)?;
                    }
                }
                Ok(())
            }
            Applied::Zombie { .. } => {
                self.metrics.zombies += 1;
                self.partitions.remove(&(topic.to_owned(), partition));
                Ok(())
            }
            Applied::Miss(_) => {
                self.metrics.cas_retries += 1;
                Ok(())
            }
        }
    }

    // ------------------------------------------------------------ rebuild (D17)

    fn refuse_rebuild(&mut self, lpk: &str, reason: &str) {
        self.metrics.rebuilds_refused += 1;
        self.last_rebuild_error = Some(format!("{lpk}: {reason}"));
    }

    /// Start the next queued rebuild on a partition this instance tails.
    fn start_rebuild(&mut self) -> Result<(), StageBError> {
        while let Some(lpk) = self.rebuild_queue.pop_front() {
            if self.rebuild_reader.is_none() {
                self.refuse_rebuild(&lpk, "no rebuild reader configured");
                continue;
            }
            let mut owner = None;
            let mut owned: Vec<(String, i32)> = self.partitions.keys().cloned().collect();
            owned.sort();
            for (topic, partition) in owned {
                if self.cache.registry(&topic, partition)?.contains(&lpk) {
                    owner = Some((topic, partition));
                    break;
                }
            }
            let Some((topic, partition)) = owner else {
                self.refuse_rebuild(&lpk, "no owned partition holds the product");
                continue;
            };
            let state = self.partitions[&(topic.clone(), partition)].clone();
            if state.mode != Mode::Normal {
                self.refuse_rebuild(&lpk, "its partition is building");
                continue;
            }
            let pointer = self.cache.pointer(&lpk)?;
            if pointer.ready.is_none() {
                self.refuse_rebuild(&lpk, "not READY (a cold build covers it)");
                continue;
            }
            let generation = self.cache.allocate_generation()?;
            let stale = pointer.staging;
            match self.cache.apply(
                &topic,
                partition,
                state.fence,
                state.next,
                (self.now_ms)(),
                &[Op::Stage {
                    lpk: lpk.clone(),
                    pointer,
                    generation,
                }],
            )? {
                Applied::Ok(_) => {}
                Applied::Zombie { .. } => {
                    self.metrics.zombies += 1;
                    self.partitions.remove(&(topic, partition));
                    self.refuse_rebuild(&lpk, "partition lost");
                    continue;
                }
                Applied::Miss(_) => {
                    // The pointer moved under us: try again next step.
                    self.metrics.cas_retries += 1;
                    self.rebuild_queue.push_front(lpk);
                    return Ok(());
                }
            }
            if let Some(stale) = stale {
                self.metrics.reclaimed_keys +=
                    self.cache.reclaim(stale, &lpk, Self::bar_interval(&lpk))?;
            }
            let (earliest, _) = self
                .source
                .watermarks(&topic, partition)
                .map_err(StageBError::Source)?;
            if let Some(reader) = self.rebuild_reader.as_mut() {
                reader
                    .start(&topic, partition, earliest)
                    .map_err(StageBError::Source)?;
            }
            self.metrics.rebuilds_started += 1;
            self.rebuild = Some(ActiveRebuild {
                lpk,
                topic,
                partition,
                generation,
            });
            return Ok(());
        }
        Ok(())
    }

    fn bar_interval(lpk: &str) -> Option<u64> {
        LogicalProductKey::parse(lpk)
            .ok()
            .filter(|parsed| parsed.feed == "BAR")
            .and_then(|parsed| canonical_interval_ms(&parsed.qualifier).ok())
    }

    /// Drop the rebuild: stop the reader and reclaim what was staged.
    fn abandon_rebuild(&mut self, reason: &str) -> Result<(), StageBError> {
        let Some(active) = self.rebuild.take() else {
            return Ok(());
        };
        self.metrics.rebuilds_abandoned += 1;
        self.last_rebuild_error = Some(format!("{}: {reason}", active.lpk));
        if let Some(reader) = self.rebuild_reader.as_mut() {
            reader.stop().map_err(StageBError::Source)?;
        }
        self.metrics.reclaimed_keys += self.cache.reclaim(
            active.generation,
            &active.lpk,
            Self::bar_interval(&active.lpk),
        )?;
        Ok(())
    }

    /// One bounded replay step of the active rebuild; publish at the live
    /// checkpoint.
    fn advance_rebuild(&mut self) -> Result<(), StageBError> {
        if self.rebuild.is_none() {
            self.start_rebuild()?;
        }
        let Some(active) = self.rebuild.clone() else {
            return Ok(());
        };
        let key = (active.topic.clone(), active.partition);
        let Some(state) = self.partitions.get(&key).cloned() else {
            return self.abandon_rebuild("partition lost");
        };
        if state.mode != Mode::Normal {
            return self.abandon_rebuild("partition building");
        }
        let pointer = self.cache.pointer(&active.lpk)?;
        if pointer.staging != Some(active.generation) || pointer.ready.is_none() {
            return self.abandon_rebuild("pointer changed");
        }
        let Some(reader) = self.rebuild_reader.as_mut() else {
            return self.abandon_rebuild("no rebuild reader");
        };
        let inputs = reader
            .poll(self.limits.max_batch_records, self.limits.poll_timeout)
            .map_err(StageBError::Source)?;
        let position = reader
            .position(&active.topic, active.partition)
            .map_err(StageBError::Source)?;
        let mut frames = Vec::new();
        for input in inputs
            .iter()
            .filter(|input| input.topic == active.topic && input.partition == active.partition)
        {
            let frame = self.decode(&active.topic, active.partition, input)?;
            if Self::product(&frame.0, &frame.1).as_deref() == Some(active.lpk.as_str()) {
                frames.push(frame);
            }
        }
        if !frames.is_empty() && !self.rebuild_apply(&active, &state, &frames)? {
            return Ok(());
        }
        let replayed = position
            .or_else(|| inputs.last().map(|input| input.offset + 1))
            .unwrap_or(0);
        if replayed < state.next {
            return Ok(());
        }
        // The staged generation now holds every record ready applied.
        if let Some(interval_ms) = Self::bar_interval(&active.lpk) {
            let (rows, counted) =
                self.cache
                    .bar_row_count(active.generation, &active.lpk, interval_ms)?;
            if rows != counted {
                let reason =
                    format!("verification failed: meta rows {rows}, bucket rows {counted}");
                self.abandon_rebuild(&reason)?;
                return Err(StageBError::Source(format!("{}: {reason}", active.lpk)));
            }
        }
        let pointer = self.cache.pointer(&active.lpk)?;
        self.apply_pointer_ops(
            &active.topic,
            active.partition,
            &state,
            vec![Op::Publish {
                lpk: active.lpk.clone(),
                pointer,
            }],
        )?;
        if self.cache.pointer(&active.lpk)?.ready == Some(active.generation) {
            self.metrics.rebuilds_completed += 1;
            self.rebuild = None;
            if let Some(reader) = self.rebuild_reader.as_mut() {
                reader.stop().map_err(StageBError::Source)?;
            }
        }
        Ok(())
    }

    /// Apply replayed records of the product into the staged generation
    /// only, under the owner fence; the partition checkpoint stays where the
    /// live path put it. `false` if the partition was lost.
    fn rebuild_apply(
        &mut self,
        active: &ActiveRebuild,
        state: &PartitionState,
        frames: &[(Vec<u8>, Option<StateFrame>)],
    ) -> Result<bool, StageBError> {
        let replay = PartitionState {
            fence: state.fence,
            mode: Mode::Build {
                generation: active.generation,
                boundary: i64::MAX,
            },
            next: state.next,
        };
        let before = self.metrics.clone();
        let mut attempt = 0;
        loop {
            let (ops, _) = self.build_ops(&replay, frames)?;
            // Replay decisions are not live metrics.
            self.metrics = StageBMetrics {
                cas_retries: before.cas_retries + attempt as u64,
                ..before.clone()
            };
            match self.cache.apply(
                &active.topic,
                active.partition,
                state.fence,
                state.next,
                (self.now_ms)(),
                &ops,
            )? {
                Applied::Ok(_) => {
                    self.metrics.rebuild_records += frames.len() as u64;
                    return Ok(true);
                }
                Applied::Zombie { .. } => {
                    self.metrics.zombies += 1;
                    self.partitions
                        .remove(&(active.topic.clone(), active.partition));
                    self.abandon_rebuild("partition lost")?;
                    return Ok(false);
                }
                Applied::Miss(_) => {
                    attempt += 1;
                    self.metrics.cas_retries = before.cas_retries + attempt as u64;
                    if attempt > self.limits.max_cas_retries {
                        return Err(StageBError::Source(format!(
                            "{}: rebuild expectations kept changing ({attempt} retries)",
                            active.lpk
                        )));
                    }
                }
            }
        }
    }
}
