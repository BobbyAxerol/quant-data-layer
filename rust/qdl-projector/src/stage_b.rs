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
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
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
    /// How often owned partitions check their owner fence in the cache even
    /// without records: a cache that lost its state (restart, flush) is
    /// rebuilt, and a newer owner is not fought.
    pub ownership_probe: Duration,
    /// How long a partition fenced by a newer owner is left alone while it
    /// is still in this member's assignment (group rebalance catches up).
    pub fenced_backoff: Duration,
}

impl Default for StageBLimits {
    fn default() -> Self {
        Self {
            max_batch_records: 500,
            poll_timeout: Duration::from_millis(50),
            rebuild_horizon: Duration::from_secs(6 * 86_400),
            max_cas_retries: 5,
            ownership_probe: Duration::from_secs(5),
            fenced_backoff: Duration::from_secs(60),
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
    /// Partitions whose owner key vanished (cache state lost): rebuilt.
    pub ownership_lost: u64,
    pub cache_reconnects: u64,
    /// Partitions tailed with every product rebuilt one by one (D21).
    pub rolling_rebuilds: u64,
    /// Superseded generations reclaimed from the retirement set (D20).
    pub retired: u64,
    /// Batches whose partitions were sought back after a failed apply (D18).
    pub rewinds: u64,
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
    /// Records of the product replayed so far (0 at the end = state gone).
    records: u64,
    /// D23: the rebuild is being ended (an error or a changed context); the
    /// replay never continues, only the end decision is retried.
    ending: bool,
}

/// D23: how a rebuild ended, decided from a fresh pointer read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ending {
    /// `ready` is the rebuilt generation: the swap happened; nothing of it is
    /// reclaimed, the old generation goes through the retirement set.
    Published,
    /// `staging` was the rebuilt generation, now unstaged and retired.
    Discarded,
    /// Neither: the generation is not this rebuild's any more.
    NotOurs,
}

/// A retention floor met in a batch; its bucket range is fixed after every
/// row of the batch is known (D19).
struct PendingFloor {
    lpk: String,
    generation: u64,
    pointer: Pointer,
    floor_ms: u64,
    interval_ms: u64,
    first: Option<u64>,
    floor: Option<u64>,
}

/// A record key belongs to the product: the LPK itself (latest) or a BAR key
/// `<lpk>|...`. Lets the replay skip other products without decoding.
fn key_is_product(key: &[u8], lpk: &str) -> bool {
    key.starts_with(lpk.as_bytes()) && (key.len() == lpk.len() || key.get(lpk.len()) == Some(&b'|'))
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
    /// Partitions fenced by a newer owner: not prepared again before the
    /// time (ms) unless they leave the assignment first.
    fenced: HashMap<(String, i32), u64>,
    last_probe_ms: u64,
    /// Products with a pending rebuild obligation in the cache request set
    /// (D21): while one has no ready generation the live path leaves it to
    /// the replay (no dual write).
    obligations: HashSet<String>,
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
            fenced: HashMap::new(),
            last_probe_ms: 0,
            obligations: HashSet::new(),
        }
    }

    /// After a cache I/O error: reconnect and forget every partition's state,
    /// so each is prepared again - tailing from its checkpoint if the cache
    /// kept it, rebuilding if the cache lost it.
    pub fn recover_cache(&mut self) -> Result<(), StageBError> {
        self.cache.reconnect()?;
        self.metrics.cache_reconnects += 1;
        self.partitions.clear();
        self.fenced.clear();
        if self.rebuild.take().is_some() {
            self.metrics.rebuilds_abandoned += 1;
            self.last_rebuild_error = Some("cache reconnect".into());
            if let Some(reader) = self.rebuild_reader.as_mut() {
                reader.stop().map_err(StageBError::Source)?;
            }
        }
        Ok(())
    }

    /// A newer owner holds the partition: stop applying it and do not take
    /// it back for `fenced_backoff`.
    fn fence_out(&mut self, topic: &str, partition: i32) {
        self.metrics.zombies += 1;
        let key = (topic.to_owned(), partition);
        self.partitions.remove(&key);
        let until = (self.now_ms)() + self.limits.fenced_backoff.as_millis() as u64;
        self.fenced.insert(key, until);
    }

    /// Compare each owned partition's owner key with this instance's fence.
    fn probe_ownership(&mut self) -> Result<(), StageBError> {
        let now = (self.now_ms)();
        if now.saturating_sub(self.last_probe_ms) < self.limits.ownership_probe.as_millis() as u64 {
            return Ok(());
        }
        self.last_probe_ms = now;
        self.take_requests()?;
        let owned = self.owned();
        let owners = self.cache.owners(&owned)?;
        for ((topic, partition), owner) in owned.into_iter().zip(owners) {
            let fence = self.partitions[&(topic.clone(), partition)].fence;
            match owner {
                Some(owner) if owner == fence => {}
                Some(owner) if owner > fence => self.fence_out(&topic, partition),
                _ => {
                    // The cache lost the partition's state: prepare it again.
                    self.metrics.ownership_lost += 1;
                    self.partitions.remove(&(topic, partition));
                }
            }
        }
        Ok(())
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
        let polled: Vec<(String, i32)> = batch
            .iter()
            .map(|input| (input.topic.clone(), input.partition))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let applied = match self.apply_polled(batch) {
            Ok(applied) => applied,
            Err(error) => {
                // D18/D22: whatever failed between the poll and the end of
                // apply, the polled records are read again.
                self.rewind(&polled);
                return Err(error);
            }
        };
        self.probe_ownership()?;
        self.process_retirements(16)?;
        self.advance_rebuild()?;
        Ok(applied)
    }

    /// Assignment, preparation of new partitions and apply of one polled
    /// batch; any error leaves the caller to rewind the polled partitions.
    fn apply_polled(&mut self, batch: Vec<StateInput>) -> Result<usize, StageBError> {
        let assigned = self.source.assigned().map_err(StageBError::Source)?;
        self.partitions
            .retain(|key, _| assigned.iter().any(|(t, p)| t == &key.0 && *p == key.1));
        let now = (self.now_ms)();
        self.fenced.retain(|key, until| {
            *until > now && assigned.iter().any(|(t, p)| t == &key.0 && *p == key.1)
        });
        // Newly assigned partitions: take ownership, choose the mode and seek.
        // Their records in this batch were fetched before the seek and are
        // dropped (librdkafka purges the partition's queue on seek).
        let mut fresh_partitions = std::collections::BTreeSet::new();
        for (topic, partition) in &assigned {
            let key = (topic.clone(), *partition);
            if !self.partitions.contains_key(&key) && !self.fenced.contains_key(&key) {
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
        Ok(applied)
    }

    /// Seek every partition of a failed batch back to its applied checkpoint
    /// (a partition that cannot be sought is prepared again).
    fn rewind(&mut self, partitions: &[(String, i32)]) {
        self.metrics.rewinds += 1;
        for (topic, partition) in partitions {
            let key = (topic.clone(), *partition);
            let Some(next) = self.partitions.get(&key).map(|state| state.next) else {
                continue;
            };
            if self.source.seek(topic, *partition, next).is_err() {
                self.partitions.remove(&key);
            }
        }
    }

    /// Reclaim up to `max` retired generations (any replica may; a retired
    /// generation is never written again, D20).
    fn process_retirements(&mut self, max: usize) -> Result<usize, StageBError> {
        let members = self.cache.retirements(max)?;
        for member in &members {
            let parsed = member
                .split_once('|')
                .and_then(|(generation, lpk)| Some((generation.parse::<u64>().ok()?, lpk)));
            if let Some((generation, lpk)) = parsed {
                let pointer = self.cache.pointer(lpk)?;
                if pointer.ready != Some(generation) && pointer.staging != Some(generation) {
                    self.metrics.reclaimed_keys +=
                        self.cache
                            .reclaim(generation, lpk, Self::bar_interval(lpk))?;
                    self.metrics.retired += 1;
                }
            }
            self.cache.retired(member)?;
        }
        Ok(members.len())
    }

    /// Take the requests of products held by this instance's partitions
    /// into the rebuild queue (D17/D21); members stay until published.
    fn take_requests(&mut self) -> Result<(), StageBError> {
        let requested = self.cache.rebuild_requests()?;
        let mut held = HashSet::new();
        if !requested.is_empty() {
            for (topic, partition) in self.owned() {
                held.extend(self.cache.registry(&topic, partition)?);
            }
        }
        let mut mine: Vec<String> = requested
            .into_iter()
            .filter(|lpk| held.contains(lpk))
            .collect();
        mine.sort();
        self.obligations = mine.iter().cloned().collect();
        for lpk in mine {
            self.request_rebuild(&lpk);
        }
        Ok(())
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
        let mut any_ready = false;
        let mut staged = Vec::new();
        let mut held = Vec::new();
        for lpk in self.cache.registry(topic, partition)? {
            let pointer = self.cache.pointer(&lpk)?;
            any_ready |= pointer.ready.is_some();
            if pointer.staging.is_some() {
                staged.push(lpk.clone());
            }
            if pointer.ready.is_some() || pointer.staging.is_some() {
                held.push(lpk);
            }
        }
        let fresh = checkpoint
            .as_ref()
            .filter(|checkpoint| {
                now.saturating_sub(checkpoint.at_ms) <= horizon && checkpoint.next >= earliest
            })
            .map(|checkpoint| checkpoint.next);
        // D21: build the partition as a whole only when none of its products
        // is served (empty cache, or an interrupted first build): the peak is
        // the new data. Otherwise tail and rebuild product by product.
        if !any_ready && (fresh.is_none() || !staged.is_empty()) {
            self.metrics.builds += 1;
            return Ok(PartitionState {
                fence,
                mode: Mode::Build {
                    generation: self.cache.allocate_generation()?,
                    boundary: end,
                },
                next: earliest,
            });
        }
        let (next, obligations) = match fresh {
            // Products left staged by a stopped build/rebuild/first publish.
            Some(next) => (next, staged),
            // Beyond the horizon or below the earliest offset: the cache may
            // have missed deletes, so every product is rebuilt from the log.
            None => {
                self.metrics.rolling_rebuilds += 1;
                let start = checkpoint
                    .map(|checkpoint| checkpoint.next)
                    .unwrap_or(earliest)
                    .max(earliest);
                (start, held)
            }
        };
        self.cache.request_rebuilds(&obligations)?;
        for lpk in obligations {
            self.obligations.insert(lpk.clone());
            self.request_rebuild(&lpk);
        }
        Ok(PartitionState {
            fence,
            mode: Mode::Normal,
            next,
        })
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
            let (mut ops, fresh) = self.build_ops(&state, &frames)?;
            ops.extend(Self::source_ops(&frames));
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
                    self.fence_out(topic, partition);
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

    /// The live batch's source watermarks and BAR product sources (KN-4
    /// D27), applied in the batch's own script with its checkpoint. A state
    /// partition receives a product's frames in canonical order, so after
    /// this batch every fact at or below the highest source offset of a
    /// canonical partition is applied for each product of this partition on
    /// it. Only the live path adds them: a rebuild replay stays below the
    /// live checkpoint.
    fn source_ops(frames: &[(Vec<u8>, Option<StateFrame>)]) -> Vec<Op> {
        let mut marks: BTreeMap<(String, u32), u64> = BTreeMap::new();
        let mut products: BTreeMap<String, (String, u32)> = BTreeMap::new();
        for (_, frame) in frames {
            let Some(frame) = frame else {
                continue;
            };
            let Some(source) = &frame.source else {
                continue;
            };
            let mark = marks
                .entry((source.topic_id.clone(), source.partition))
                .or_insert(source.offset);
            *mark = (*mark).max(source.offset);
            if matches!(frame.kind, FrameKind::BarRevision) {
                products.insert(
                    frame.lpk.encode(),
                    (source.topic_id.clone(), source.partition),
                );
            }
        }
        let mut ops: Vec<Op> = products
            .into_iter()
            .map(|(lpk, (topic_id, partition))| Op::ProductSource {
                lpk,
                topic_id,
                partition,
            })
            .collect();
        ops.extend(
            marks
                .into_iter()
                .map(|((topic_id, partition), offset)| Op::SourceWatermark {
                    topic_id,
                    partition,
                    offset,
                }),
        );
        ops
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
                // A product with a pending rebuild obligation is staged by
                // its rebuild, not by the first-seen path (D24).
                Mode::Normal => (pointer.ready.is_none()
                    && pointer.staging.is_none()
                    && !self.obligations.contains(&lpk))
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
                self.fence_out(topic, partition);
                Ok(false)
            }
            Applied::Miss(_) => {
                self.metrics.cas_retries += 1;
                Ok(true)
            }
        }
    }

    /// The generations a product's record is written to in this mode. A
    /// product with a rebuild obligation and no ready generation is left to
    /// its replay (D21: no dual write into the staging generation).
    fn targets(&self, state: &PartitionState, lpk: &str, pointer: &Pointer) -> Vec<u64> {
        match state.mode {
            Mode::Build { generation, .. } => vec![generation],
            Mode::Normal if pointer.ready.is_none() && self.obligations.contains(lpk) => Vec::new(),
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
        let mut floors: Vec<PendingFloor> = Vec::new();
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
                    && !self.obligations.contains(&lpk)
                {
                    fresh.push(lpk.clone());
                }
                pointers.insert(lpk.clone(), pointer);
            }
            let pointer = pointers[&lpk].clone();
            let targets = self.targets(state, &lpk, &pointer);
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
                        self.metrics.floors += 1;
                        floors.push(PendingFloor {
                            lpk: lpk.clone(),
                            generation,
                            pointer: pointer.clone(),
                            floor_ms,
                            interval_ms,
                            first,
                            floor,
                        });
                    }
                }
            }
        }
        let mut ops = Vec::new();
        // The opens each (generation, product) row of this batch writes.
        let mut written: HashMap<(u64, String), std::collections::BTreeSet<u64>> = HashMap::new();
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
                        written
                            .entry((entry.0, entry.1.clone()))
                            .or_default()
                            .insert(entry.2.unwrap_or_default());
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
        // Notes, then floors, after the rows of this batch.
        ops.extend(tail_ops);
        ops.extend(Self::floor_ops(floors, &written));
        Ok((ops, fresh))
    }

    /// D19/D22: a floor deletes every bucket from the lowest retained open -
    /// the cached `first` or a lower open written by this very batch (rows are
    /// applied before floors in the script) - up to its boundary bucket. Rows
    /// of the batch below the cached floor are refused by the script, so the
    /// lowest write is taken among the opens at or above that floor.
    fn floor_ops(
        floors: Vec<PendingFloor>,
        written: &HashMap<(u64, String), std::collections::BTreeSet<u64>>,
    ) -> Vec<Op> {
        floors
            .into_iter()
            .map(|pending| {
                let batch_low = written
                    .get(&(pending.generation, pending.lpk.clone()))
                    .and_then(|opens| opens.range(pending.floor.unwrap_or(0)..).next().copied());
                let low = match (pending.first, batch_low) {
                    (Some(first), Some(batch)) => Some(first.min(batch)),
                    (first, batch) => first.or(batch),
                };
                let boundary = bucket_of(pending.floor_ms, pending.interval_ms);
                let buckets = low
                    .map(|low| (bucket_of(low, pending.interval_ms)..boundary).collect())
                    .unwrap_or_default();
                Op::Floor {
                    lpk: pending.lpk,
                    generation: pending.generation,
                    pointer: pending.pointer,
                    floor_ms: pending.floor_ms,
                    buckets,
                    boundary: Some(boundary),
                }
            })
            .collect()
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
                        // Retired by the script in the same call as the swap.
                        self.cache.retired(&format!("{old}|{lpk}"))?;
                        self.metrics.retired += 1;
                    }
                }
                Ok(())
            }
            Applied::Zombie { .. } => {
                self.fence_out(topic, partition);
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
    /// Contract section 5: the next product's staging starts only after every
    /// superseded generation is reclaimed and freed.
    fn start_rebuild(&mut self) -> Result<(), StageBError> {
        if self.rebuild_queue.is_empty() {
            return Ok(());
        }
        self.process_retirements(64)?;
        if !self.cache.retirements(1)?.is_empty() || self.cache.lazyfree_pending()? > 0 {
            return Ok(());
        }
        // A request leaves the queue only once decided; an error before its
        // staging exists keeps it for the next step.
        while let Some(lpk) = self.rebuild_queue.front().cloned() {
            if self.rebuild_reader.is_none() {
                self.rebuild_queue.pop_front();
                self.refuse_rebuild(&lpk, "no rebuild reader configured");
                continue;
            }
            let mut owner = None;
            for (topic, partition) in self.owned() {
                if self.cache.registry(&topic, partition)?.contains(&lpk) {
                    owner = Some((topic, partition));
                    break;
                }
            }
            let Some((topic, partition)) = owner else {
                self.rebuild_queue.pop_front();
                self.refuse_rebuild(&lpk, "no owned partition holds the product");
                continue;
            };
            let state = self.partitions[&(topic.clone(), partition)].clone();
            if state.mode != Mode::Normal {
                // Taken again from the request set once the build finished.
                self.rebuild_queue.pop_front();
                continue;
            }
            // D24: a product still in the registry of a partition this
            // instance owns is rebuilt even with an empty pointer (a staging
            // unstaged before its first publish); the replay builds it from
            // the log, and a product whose state is gone was unpublished
            // (removed from the registry) and is refused above.
            let pointer = self.cache.pointer(&lpk)?;
            let generation = self.cache.allocate_generation()?;
            // A replaced staging generation is retired by the script (D20).
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
                    self.rebuild_queue.pop_front();
                    self.fence_out(&topic, partition);
                    self.refuse_rebuild(&lpk, "partition lost");
                    continue;
                }
                Applied::Miss(_) => {
                    // The pointer moved under us: try again next step.
                    self.metrics.cas_retries += 1;
                    return Ok(());
                }
            }
            // The staging exists from here on: the rebuild state owns it, and
            // a failed setup ends it through the D23 decision (unstage).
            self.rebuild_queue.pop_front();
            self.metrics.rebuilds_started += 1;
            self.rebuild = Some(ActiveRebuild {
                lpk,
                topic: topic.clone(),
                partition,
                generation,
                records: 0,
                ending: true,
            });
            self.process_retirements(64)?;
            let (earliest, _) = self
                .source
                .watermarks(&topic, partition)
                .map_err(StageBError::Source)?;
            if let Some(reader) = self.rebuild_reader.as_mut() {
                reader
                    .start(&topic, partition, earliest)
                    .map_err(StageBError::Source)?;
            }
            if let Some(active) = self.rebuild.as_mut() {
                active.ending = false;
            }
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

    /// D23: end the active rebuild after an error or a changed context,
    /// deciding from a fresh pointer read. A generation that became READY is
    /// never reclaimed; an unpublished staging generation is unstaged by CAS
    /// under the owner fence (retired in the same script) and then
    /// reclaimed. `Err` = the outcome could not be established (pointer read
    /// or unstage failed): nothing is reclaimed, the rebuild stays in its
    /// ending state and the next step decides again.
    fn end_rebuild(&mut self, reason: &str) -> Result<Ending, StageBError> {
        let Some(active) = self.rebuild.as_mut() else {
            return Ok(Ending::NotOurs);
        };
        active.ending = true;
        let active = active.clone();
        self.last_rebuild_error = Some(format!("{}: {reason}", active.lpk));
        let pointer = self.cache.pointer(&active.lpk)?;
        if pointer.ready == Some(active.generation) {
            self.complete_rebuild(&active.lpk)?;
            self.process_retirements(64)?;
            return Ok(Ending::Published);
        }
        let ending = if pointer.staging == Some(active.generation) {
            let key = (active.topic.clone(), active.partition);
            if let Some(state) = self.partitions.get(&key).cloned() {
                match self.cache.apply(
                    &active.topic,
                    active.partition,
                    state.fence,
                    state.next,
                    (self.now_ms)(),
                    &[Op::Unstage {
                        lpk: active.lpk.clone(),
                        pointer,
                    }],
                )? {
                    Applied::Ok(_) => {}
                    Applied::Zombie { .. } => self.fence_out(&active.topic, active.partition),
                    Applied::Miss(_) => {
                        self.metrics.cas_retries += 1;
                        return Err(StageBError::Source(format!(
                            "{}: pointer moved while unstaging",
                            active.lpk
                        )));
                    }
                }
            } else {
                // Not owned any more (no fence to unstage with): only this
                // rebuild could ever publish `G`, and it has stopped, so the
                // confirmed-unpublished staging is reclaimed directly; the
                // next owner's stage op retires the pointer entry (D21).
                self.metrics.reclaimed_keys += self.cache.reclaim(
                    active.generation,
                    &active.lpk,
                    Self::bar_interval(&active.lpk),
                )?;
            }
            Ending::Discarded
        } else {
            Ending::NotOurs
        };
        self.rebuild = None;
        self.metrics.rebuilds_abandoned += 1;
        if let Some(reader) = self.rebuild_reader.as_mut() {
            reader.stop().map_err(StageBError::Source)?;
        }
        self.process_retirements(64)?;
        Ok(ending)
    }

    /// One bounded replay step of the active rebuild; publish at the live
    /// checkpoint. D22: any error once the reader was polled abandons the
    /// replay (its staging is reclaimed) and re-queues the product, so a
    /// generation never misses a record the reader moved past.
    fn advance_rebuild(&mut self) -> Result<(), StageBError> {
        if self.rebuild.is_none() {
            self.start_rebuild()?;
        }
        let Some(active) = self.rebuild.clone() else {
            return Ok(());
        };
        if active.ending {
            if self.end_rebuild("retrying the end of a rebuild")? == Ending::Discarded {
                self.rebuild_queue.push_front(active.lpk.clone());
            }
            return Ok(());
        }
        let key = (active.topic.clone(), active.partition);
        let Some(state) = self.partitions.get(&key).cloned() else {
            self.end_rebuild("partition lost")?;
            return Ok(());
        };
        if state.mode != Mode::Normal {
            self.end_rebuild("partition building")?;
            return Ok(());
        }
        let pointer = self.cache.pointer(&active.lpk)?;
        if pointer.staging != Some(active.generation) {
            // Published already (an earlier step lost its reply) or taken
            // over: decided by the pointer, never by deleting.
            self.end_rebuild("pointer changed")?;
            return Ok(());
        }
        match self.replay_step(&active, &state) {
            Ok(()) => Ok(()),
            Err(error) => {
                if self
                    .rebuild
                    .as_ref()
                    .is_some_and(|current| current.generation == active.generation)
                    && self.end_rebuild("replay failed")? == Ending::Discarded
                {
                    self.rebuild_queue.push_front(active.lpk.clone());
                }
                Err(error)
            }
        }
    }

    fn replay_step(
        &mut self,
        active: &ActiveRebuild,
        state: &PartitionState,
    ) -> Result<(), StageBError> {
        let Some(reader) = self.rebuild_reader.as_mut() else {
            self.end_rebuild("no rebuild reader")?;
            return Ok(());
        };
        let inputs = reader
            .poll(self.limits.max_batch_records, self.limits.poll_timeout)
            .map_err(StageBError::Source)?;
        let position = reader
            .position(&active.topic, active.partition)
            .map_err(StageBError::Source)?;
        let mut frames = Vec::new();
        for input in inputs.iter().filter(|input| {
            input.topic == active.topic
                && input.partition == active.partition
                && key_is_product(&input.key, &active.lpk)
        }) {
            let frame = self.decode(&active.topic, active.partition, input)?;
            if Self::product(&frame.0, &frame.1).as_deref() == Some(active.lpk.as_str()) {
                frames.push(frame);
            }
        }
        if !frames.is_empty() {
            if !self.rebuild_apply(active, state, &frames)? {
                return Ok(());
            }
            if let Some(current) = self.rebuild.as_mut() {
                current.records += frames.len() as u64;
            }
        }
        let replayed = position
            .or_else(|| inputs.last().map(|input| input.offset + 1))
            .unwrap_or(0);
        if replayed < state.next {
            return Ok(());
        }
        let records = self.rebuild.as_ref().map_or(0, |current| current.records);
        if records == 0 {
            // The log holds nothing of the product any more: its state is
            // gone (NOT_READY), both generations retired.
            let pointer = self.cache.pointer(&active.lpk)?;
            self.apply_pointer_ops(
                &active.topic,
                active.partition,
                state,
                vec![Op::Unpublish {
                    lpk: active.lpk.clone(),
                    pointer,
                }],
            )?;
            if self.cache.pointer(&active.lpk)?.staging.is_none() {
                self.complete_rebuild(&active.lpk)?;
            }
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
                self.end_rebuild(&reason)?;
                return Err(StageBError::Source(format!("{}: {reason}", active.lpk)));
            }
        }
        let pointer = self.cache.pointer(&active.lpk)?;
        self.apply_pointer_ops(
            &active.topic,
            active.partition,
            state,
            vec![Op::Publish {
                lpk: active.lpk.clone(),
                pointer,
            }],
        )?;
        if self.cache.pointer(&active.lpk)?.ready == Some(active.generation) {
            self.complete_rebuild(&active.lpk)?;
        }
        Ok(())
    }

    fn complete_rebuild(&mut self, lpk: &str) -> Result<(), StageBError> {
        self.metrics.rebuilds_completed += 1;
        self.rebuild = None;
        self.cache.rebuild_done(lpk)?;
        self.obligations.remove(lpk);
        if let Some(reader) = self.rebuild_reader.as_mut() {
            reader.stop().map_err(StageBError::Source)?;
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
                    self.fence_out(&active.topic, active.partition);
                    self.end_rebuild("partition lost")?;
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
