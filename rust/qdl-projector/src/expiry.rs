//! BAR expiry (KN-3 K3.5, decision D11): a bounded task inside the projector
//! role that raises each BAR product's retention floor and tombstones every
//! fact key below it.
//!
//! Per BAR product and its READY generation: when the cached `rows` exceed
//! the retained cap by more than one bucket (`BUCKET_OPENS` of slack), the
//! target floor is the open time of the cap-th newest row; every open below
//! it expires. One plan collects at most `max_opens` expired opens from the
//! oldest upward; when bounded, its floor is the first open it did not
//! collect (every step is exact) and the next plan continues toward the same
//! target. For each expired open the plan tombstones: the current row's own
//! fact key (derived from the row: the final key with the trailer hash, else
//! the in-progress key), the in-progress key `<lpk>|<ms>|p` (always), and
//! every extra fact remembered in `rk:<g>:<lpk>` (superseded revisions,
//! refused conflicts, stale finals).
//!
//! A plan is published in ONE Kafka transaction: every tombstone, then the
//! RETENTION_FLOOR frame, all on the product's state partition. Stage B
//! deletes the rows below the floor when it applies the frame. Floors only
//! rise. Boundaries: reads only the market cache (never writes it); writes
//! only the BAR state topic through the caller's transactional producer.

use crate::cache::{bucket_of, Cache, CacheError, BUCKET_OPENS};
use prost::Message;
use qdl_contracts::interval::canonical_interval_ms;
use qdl_contracts::qdl::marketdata::v2::{event_envelope::Payload, EventEnvelope};
use qdl_contracts::state_codec::{
    bar_key, decode_bar_row, floor_key, state_partition, StateFrame, TRAILER_BYTES,
};
use qdl_contracts::state_contract::LogicalProductKey;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{
    BaseProducer, BaseRecord, DefaultProducerContext, Producer, ThreadedProducer,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

/// Buckets read per Redis round trip (pipelined).
const BUCKETS_PER_ROUND_TRIP: u64 = 16;
/// Opens per `HMGET` of the extra fact keys.
const OPENS_PER_ROUND_TRIP: usize = 500;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExpiryError {
    Cache(CacheError),
    /// A cached row or fact list that does not decode: the plan stops (D12),
    /// nothing is published for the product.
    Integrity {
        lpk: String,
        open_ms: u64,
        reason: String,
    },
    /// Not a BAR product with a fixed interval, or an invalid cap.
    Product(String),
    /// The transaction was not committed (aborted, fenced or unreachable).
    Publish(String),
}

impl From<CacheError> for ExpiryError {
    fn from(error: CacheError) -> Self {
        ExpiryError::Cache(error)
    }
}

impl From<redis::RedisError> for ExpiryError {
    fn from(error: redis::RedisError) -> Self {
        let text = error.to_string();
        ExpiryError::Cache(if text.contains("OOM") {
            CacheError::MemoryPressure(text)
        } else {
            CacheError::Redis(text)
        })
    }
}

/// One exact expiry step of one BAR product.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpiryPlan {
    pub lpk: LogicalProductKey,
    /// The READY generation the plan was read from.
    pub generation: u64,
    /// The cached floor when planned (`None` = no floor yet).
    pub previous_floor: Option<u64>,
    /// The floor this step publishes: every open below it is tombstoned.
    pub floor_ms: u64,
    /// The open time of the cap-th newest row (`floor_ms` once complete).
    pub target_floor_ms: u64,
    /// Deduplicated, in open order: current fact, in-progress, extras.
    pub tombstone_keys: Vec<String>,
    pub expired_opens: u64,
}

impl ExpiryPlan {
    /// `false` when `max_opens` bounded this step below the target.
    pub fn complete(&self) -> bool {
        self.floor_ms == self.target_floor_ms
    }
}

struct Meta {
    rows: u64,
    first: Option<u64>,
    last: Option<u64>,
    floor: Option<u64>,
}

fn parse_u64(value: Option<String>) -> Option<u64> {
    value.and_then(|text| text.parse().ok())
}

fn read_meta(cache: &mut Cache, generation: u64, lpk: &str) -> Result<Meta, ExpiryError> {
    let key = cache.layout.bar_meta(generation, lpk);
    let values: Vec<Option<String>> = redis::cmd("HMGET")
        .arg(key)
        .arg("rows")
        .arg("first")
        .arg("last")
        .arg("floor")
        .query(cache.connection())?;
    let mut values = values.into_iter();
    Ok(Meta {
        rows: parse_u64(values.next().flatten()).unwrap_or(0),
        first: parse_u64(values.next().flatten()),
        last: parse_u64(values.next().flatten()),
        floor: parse_u64(values.next().flatten()),
    })
}

fn interval_of(lpk: &LogicalProductKey) -> Result<u64, ExpiryError> {
    if lpk.feed != "BAR" {
        return Err(ExpiryError::Product(format!(
            "expiry applies to BAR products only: {}",
            lpk.encode()
        )));
    }
    canonical_interval_ms(&lpk.qualifier).map_err(ExpiryError::Product)
}

fn integrity(lpk: &str, open_ms: u64, reason: impl Into<String>) -> ExpiryError {
    ExpiryError::Integrity {
        lpk: lpk.to_owned(),
        open_ms,
        reason: reason.into(),
    }
}

fn parse_open(lpk: &str, field: &str) -> Result<u64, ExpiryError> {
    field.parse::<u64>().map_err(|_| {
        integrity(
            lpk,
            0,
            format!("bucket field is not an open time: {field:?}"),
        )
    })
}

/// Open times of each bucket (one pipelined `HKEYS` per bucket).
fn bucket_opens(
    cache: &mut Cache,
    generation: u64,
    lpk: &str,
    buckets: &[u64],
) -> Result<Vec<Vec<u64>>, ExpiryError> {
    let mut pipe = redis::pipe();
    for bucket in buckets {
        pipe.cmd("HKEYS")
            .arg(cache.layout.bar_bucket(generation, lpk, *bucket));
    }
    let fields: Vec<Vec<String>> = pipe.query(cache.connection())?;
    fields
        .into_iter()
        .map(|fields| {
            fields
                .iter()
                .map(|field| parse_open(lpk, field))
                .collect::<Result<Vec<u64>, _>>()
        })
        .collect()
}

/// `(open_ms, row)` pairs of one bucket.
type BucketRows = Vec<(u64, Vec<u8>)>;

/// Rows of each bucket (one pipelined `HGETALL` per bucket).
fn bucket_rows(
    cache: &mut Cache,
    generation: u64,
    lpk: &str,
    buckets: &[u64],
) -> Result<Vec<BucketRows>, ExpiryError> {
    let mut pipe = redis::pipe();
    for bucket in buckets {
        pipe.cmd("HGETALL")
            .arg(cache.layout.bar_bucket(generation, lpk, *bucket));
    }
    let rows: Vec<HashMap<String, Vec<u8>>> = pipe.query(cache.connection())?;
    rows.into_iter()
        .map(|rows| {
            rows.into_iter()
                .map(|(field, row)| Ok((parse_open(lpk, &field)?, row)))
                .collect::<Result<BucketRows, ExpiryError>>()
        })
        .collect()
}

/// The open time of the cap-th newest retained row, walking buckets from
/// the newest downward. `None` if fewer than `cap` rows exist.
fn target_floor(
    cache: &mut Cache,
    generation: u64,
    lpk: &str,
    interval_ms: u64,
    meta: &Meta,
    cap: u64,
) -> Result<Option<u64>, ExpiryError> {
    let (Some(first), Some(last)) = (meta.first, meta.last) else {
        return Ok(None);
    };
    let floor = meta.floor.unwrap_or(0);
    let low = bucket_of(first.max(floor), interval_ms);
    let mut high = bucket_of(last, interval_ms);
    let mut seen = 0u64;
    loop {
        let start = high
            .saturating_sub(BUCKETS_PER_ROUND_TRIP - 1)
            .max(low)
            .min(high);
        let buckets: Vec<u64> = (start..=high).rev().collect();
        for mut opens in bucket_opens(cache, generation, lpk, &buckets)? {
            opens.retain(|open| *open >= floor);
            opens.sort_unstable_by(|a, b| b.cmp(a));
            for open in opens {
                seen += 1;
                if seen == cap {
                    return Ok(Some(open));
                }
            }
        }
        if start <= low {
            return Ok(None);
        }
        high = start - 1;
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

/// The fact key of the current row itself (proven against its trailer).
fn current_fact_key(
    lpk: &LogicalProductKey,
    lpk_text: &str,
    open_ms: u64,
    row: &[u8],
) -> Result<String, ExpiryError> {
    let decoded = decode_bar_row(row, lpk)
        .map_err(|error| integrity(lpk_text, open_ms, error.to_string()))?;
    let envelope = EventEnvelope::decode(decoded.canonical.as_slice())
        .map_err(|error| integrity(lpk_text, open_ms, error.to_string()))?;
    let Some(Payload::Bar(bar)) = envelope.payload else {
        return Err(integrity(lpk_text, open_ms, "row is not a BAR"));
    };
    let content_sha256 = hex(&row[16..TRAILER_BYTES]);
    bar_key(lpk, open_ms, bar.is_final, bar.revision, &content_sha256)
        .map_err(|error| integrity(lpk_text, open_ms, error.to_string()))
}

/// Collect the expired opens below `target` (at most `max_opens`) and build
/// the step's plan.
fn plan_toward(
    cache: &mut Cache,
    generation: u64,
    lpk: &LogicalProductKey,
    interval_ms: u64,
    meta: &Meta,
    target: u64,
    max_opens: usize,
) -> Result<ExpiryPlan, ExpiryError> {
    let lpk_text = lpk.encode();
    let max_opens = max_opens.max(1);
    let floor = meta.floor.unwrap_or(0);
    let first = meta.first.unwrap_or(target).max(floor);
    // Up to max_opens + 1: the extra one is the next step's first open.
    let mut collected: BucketRows = Vec::new();
    if first < target {
        let high = bucket_of(target, interval_ms);
        let mut low = bucket_of(first, interval_ms);
        'buckets: while low <= high {
            let end = (low + BUCKETS_PER_ROUND_TRIP - 1).min(high);
            let buckets: Vec<u64> = (low..=end).collect();
            for mut rows in bucket_rows(cache, generation, &lpk_text, &buckets)? {
                rows.retain(|(open, _)| *open >= floor && *open < target);
                rows.sort_unstable_by_key(|(open, _)| *open);
                for row in rows {
                    collected.push(row);
                    if collected.len() > max_opens {
                        break 'buckets;
                    }
                }
            }
            low = end + 1;
        }
    }
    let floor_ms = if collected.len() > max_opens {
        collected.pop().map(|(open, _)| open).unwrap_or(target)
    } else {
        target
    };
    // Extra fact keys of the expired opens.
    let mut extras: Vec<Option<String>> = Vec::with_capacity(collected.len());
    for chunk in collected.chunks(OPENS_PER_ROUND_TRIP) {
        let mut command = redis::cmd("HMGET");
        command.arg(cache.layout.fact_keys(generation, &lpk_text));
        for (open, _) in chunk {
            command.arg(open.to_string());
        }
        let values: Vec<Option<String>> = command.query(cache.connection())?;
        extras.extend(values);
    }
    let mut seen = HashSet::new();
    let mut tombstone_keys = Vec::new();
    for ((open_ms, row), extra) in collected.iter().zip(extras) {
        let open_ms = *open_ms;
        let mut keys = vec![
            current_fact_key(lpk, &lpk_text, open_ms, row)?,
            bar_key(lpk, open_ms, false, 0, "")
                .map_err(|error| integrity(&lpk_text, open_ms, error.to_string()))?,
        ];
        for suffix in extra.iter().flat_map(|list| list.split(',')) {
            if suffix.is_empty() {
                continue;
            }
            if !suffix.starts_with('f') || !suffix.contains('|') {
                return Err(integrity(
                    &lpk_text,
                    open_ms,
                    format!("extra fact suffix is not f<rev>|<sha16>: {suffix:?}"),
                ));
            }
            keys.push(format!("{lpk_text}|{open_ms}|{suffix}"));
        }
        for key in keys {
            if seen.insert(key.clone()) {
                tombstone_keys.push(key);
            }
        }
    }
    Ok(ExpiryPlan {
        lpk: lpk.clone(),
        generation,
        previous_floor: meta.floor,
        floor_ms,
        target_floor_ms: target,
        tombstone_keys,
        expired_opens: collected.len() as u64,
    })
}

/// Plan one expiry step of a BAR product in its READY `generation`, or
/// `None` while `rows <= cap + BUCKET_OPENS` (one bucket of slack) or when
/// the floor would not rise. Reads the cache only.
pub fn plan_expiry(
    cache: &mut Cache,
    generation: u64,
    lpk: &LogicalProductKey,
    cap: u64,
    max_opens: usize,
) -> Result<Option<ExpiryPlan>, ExpiryError> {
    let interval_ms = interval_of(lpk)?;
    if cap == 0 {
        return Err(ExpiryError::Product(format!(
            "retained cap must be positive: {}",
            lpk.encode()
        )));
    }
    let lpk_text = lpk.encode();
    let meta = read_meta(cache, generation, &lpk_text)?;
    if meta.rows <= cap.saturating_add(BUCKET_OPENS) {
        return Ok(None);
    }
    let Some(target) = target_floor(cache, generation, &lpk_text, interval_ms, &meta, cap)? else {
        return Ok(None);
    };
    if meta.floor.is_some_and(|floor| target <= floor) {
        return Ok(None);
    }
    plan_toward(
        cache,
        generation,
        lpk,
        interval_ms,
        &meta,
        target,
        max_opens,
    )
    .map(Some)
}

/// The next step toward the target of a bounded plan (no slack gate: the
/// target was decided by `plan_expiry`). `None` once the cached floor has
/// reached the target.
pub fn continue_expiry(
    cache: &mut Cache,
    generation: u64,
    lpk: &LogicalProductKey,
    target_floor_ms: u64,
    max_opens: usize,
) -> Result<Option<ExpiryPlan>, ExpiryError> {
    let interval_ms = interval_of(lpk)?;
    let meta = read_meta(cache, generation, &lpk.encode())?;
    if meta.floor.is_some_and(|floor| target_floor_ms <= floor) {
        return Ok(None);
    }
    plan_toward(
        cache,
        generation,
        lpk,
        interval_ms,
        &meta,
        target_floor_ms,
        max_opens,
    )
    .map(Some)
}

// ------------------------------------------------------------ publishing

/// The transactional producer a plan is published through (implemented for
/// rdkafka's `BaseProducer` and `ThreadedProducer`; transactions already
/// initialized by the caller).
pub trait ExpirySink {
    fn begin(&self) -> Result<(), String>;
    /// `payload = None` is a tombstone.
    fn send_record(
        &self,
        topic: &str,
        partition: i32,
        key: &str,
        payload: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<(), String>;
    fn commit(&self, timeout: Duration) -> Result<(), String>;
    fn abort(&self, timeout: Duration) -> Result<(), String>;
}

fn record<'a>(
    topic: &'a str,
    partition: i32,
    key: &'a str,
    payload: Option<&'a [u8]>,
) -> BaseRecord<'a, str, [u8]> {
    let record = BaseRecord::to(topic).partition(partition).key(key);
    match payload {
        Some(payload) => record.payload(payload),
        None => record,
    }
}

type SendResult<'a> = Result<(), (KafkaError, BaseRecord<'a, str, [u8]>)>;

/// Send, waiting (bounded by `timeout`) while the local queue is full.
fn send_retrying<'a>(
    mut record: BaseRecord<'a, str, [u8]>,
    timeout: Duration,
    mut attempt: impl FnMut(BaseRecord<'a, str, [u8]>) -> SendResult<'a>,
    mut wait: impl FnMut(),
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        match attempt(record) {
            Ok(()) => return Ok(()),
            Err((error, back))
                if error.rdkafka_error_code() == Some(RDKafkaErrorCode::QueueFull)
                    && Instant::now() < deadline =>
            {
                record = back;
                wait();
            }
            Err((error, _)) => return Err(format!("send: {error}")),
        }
    }
}

impl ExpirySink for BaseProducer<DefaultProducerContext> {
    fn begin(&self) -> Result<(), String> {
        Producer::begin_transaction(self).map_err(|error| format!("begin: {error}"))
    }
    fn send_record(
        &self,
        topic: &str,
        partition: i32,
        key: &str,
        payload: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<(), String> {
        send_retrying(
            record(topic, partition, key, payload),
            timeout,
            |record| BaseProducer::send(self, record),
            || {
                self.poll(Duration::from_millis(10));
            },
        )
    }
    fn commit(&self, timeout: Duration) -> Result<(), String> {
        Producer::commit_transaction(self, timeout).map_err(|error| format!("commit: {error}"))
    }
    fn abort(&self, timeout: Duration) -> Result<(), String> {
        // No thread serves this producer's delivery reports: without a
        // flush (which polls) the abort times out on outstanding messages.
        // A flush error is irrelevant here, the transaction is aborted.
        let _ = Producer::flush(self, timeout);
        Producer::abort_transaction(self, timeout).map_err(|error| format!("abort: {error}"))
    }
}

impl ExpirySink for ThreadedProducer<DefaultProducerContext> {
    fn begin(&self) -> Result<(), String> {
        Producer::begin_transaction(self).map_err(|error| format!("begin: {error}"))
    }
    fn send_record(
        &self,
        topic: &str,
        partition: i32,
        key: &str,
        payload: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<(), String> {
        send_retrying(
            record(topic, partition, key, payload),
            timeout,
            |record| ThreadedProducer::send(self, record),
            || std::thread::sleep(Duration::from_millis(10)),
        )
    }
    fn commit(&self, timeout: Duration) -> Result<(), String> {
        Producer::commit_transaction(self, timeout).map_err(|error| format!("commit: {error}"))
    }
    fn abort(&self, timeout: Duration) -> Result<(), String> {
        Producer::abort_transaction(self, timeout).map_err(|error| format!("abort: {error}"))
    }
}

/// Publish a plan in ONE transaction: every tombstone, then the
/// RETENTION_FLOOR frame (key `floor_key(lpk)`), all on the product's state
/// partition; any error aborts the whole transaction. A floor that does not
/// rise above the plan's previous floor is refused before anything is sent.
pub fn publish_expiry<S: ExpirySink + ?Sized>(
    sink: &S,
    topic: &str,
    partitions: u32,
    plan: &ExpiryPlan,
    materializer_epoch: u64,
    timeout: Duration,
) -> Result<(), String> {
    if plan
        .previous_floor
        .is_some_and(|floor| plan.floor_ms <= floor)
    {
        return Err(format!(
            "floor {} does not rise above {:?}",
            plan.floor_ms, plan.previous_floor
        ));
    }
    let partition = state_partition(&plan.lpk, partitions)
        .map_err(|error| format!("state partition: {error}"))? as i32;
    let frame = StateFrame::retention_floor(&plan.lpk, plan.floor_ms, materializer_epoch)
        .and_then(|frame| frame.encode())
        .map_err(|error| format!("retention floor frame: {error}"))?;
    let floor = floor_key(&plan.lpk);
    sink.begin()?;
    let sent = (|| {
        for key in &plan.tombstone_keys {
            sink.send_record(topic, partition, key, None, timeout)?;
        }
        sink.send_record(topic, partition, &floor, Some(&frame), timeout)?;
        sink.commit(timeout)
    })();
    match sent {
        Ok(()) => Ok(()),
        Err(error) => match sink.abort(timeout) {
            Ok(()) => Err(format!("expiry publish aborted: {error}")),
            Err(abort) => Err(format!(
                "expiry publish failed: {error}; abort failed: {abort}"
            )),
        },
    }
}

// ------------------------------------------------------------ the task

/// Where a task publishes its plans.
#[derive(Clone, Debug)]
pub struct ExpiryPublish {
    pub topic: String,
    pub partitions: u32,
    pub materializer_epoch: u64,
    pub timeout: Duration,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExpiryReport {
    pub products_visited: u64,
    /// Products without a ready pointer (NOT_READY): skipped.
    pub not_ready: u64,
    /// Products whose last published floor stage B has not applied yet.
    pub pending: u64,
    pub floors_published: u64,
    pub tombstones: u64,
    pub expired_opens: u64,
}

/// A published step: the next one waits until stage B applied it.
#[derive(Clone, Debug)]
struct Progress {
    generation: u64,
    floor_ms: u64,
    target_floor_ms: u64,
}

/// Bounded per tick: at most `max_products_per_tick` products (round robin
/// over the sorted caps) and `max_opens` opens per product plan.
pub struct ExpiryTask {
    caps: BTreeMap<String, u64>,
    max_products_per_tick: usize,
    max_opens: usize,
    /// The last product visited (round robin).
    cursor: Option<String>,
    progress: HashMap<String, Progress>,
}

impl ExpiryTask {
    /// `caps`: retained rows per BAR product (demanded rows + headroom).
    pub fn new(
        caps: BTreeMap<String, u64>,
        max_products_per_tick: usize,
        max_opens: usize,
    ) -> Result<Self, ExpiryError> {
        let mut task = Self {
            caps: BTreeMap::new(),
            max_products_per_tick: max_products_per_tick.max(1),
            max_opens: max_opens.max(1),
            cursor: None,
            progress: HashMap::new(),
        };
        task.set_caps(caps)?;
        Ok(task)
    }

    /// Replace the caps (demand changed); every product must be BAR with a
    /// fixed interval and a positive cap.
    pub fn set_caps(&mut self, caps: BTreeMap<String, u64>) -> Result<(), ExpiryError> {
        for (lpk, cap) in &caps {
            let parsed = LogicalProductKey::parse(lpk).map_err(ExpiryError::Product)?;
            interval_of(&parsed)?;
            if *cap == 0 {
                return Err(ExpiryError::Product(format!(
                    "retained cap must be positive: {lpk}"
                )));
            }
        }
        self.progress.retain(|lpk, _| caps.contains_key(lpk));
        self.caps = caps;
        Ok(())
    }

    pub fn caps(&self) -> &BTreeMap<String, u64> {
        &self.caps
    }

    /// The next products in round-robin order after the cursor.
    fn next_products(&self) -> Vec<String> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let take = self.max_products_per_tick.min(self.caps.len());
        let (after, before): (Vec<&String>, Vec<&String>) = match &self.cursor {
            None => (self.caps.keys().collect(), Vec::new()),
            Some(cursor) => (
                self.caps
                    .range::<String, _>((Excluded(cursor), Unbounded))
                    .map(|(key, _)| key)
                    .collect(),
                self.caps
                    .range::<String, _>((Unbounded, Included(cursor)))
                    .map(|(key, _)| key)
                    .collect(),
            ),
        };
        after
            .into_iter()
            .chain(before)
            .take(take)
            .cloned()
            .collect()
    }

    /// Visit at most `max_products_per_tick` products: plan and publish.
    pub fn tick<S: ExpirySink + ?Sized>(
        &mut self,
        cache: &mut Cache,
        sink: &S,
        publish: &ExpiryPublish,
    ) -> Result<ExpiryReport, ExpiryError> {
        let mut report = ExpiryReport::default();
        for lpk_text in self.next_products() {
            self.cursor = Some(lpk_text.clone());
            report.products_visited += 1;
            let cap = self.caps[&lpk_text];
            let lpk = LogicalProductKey::parse(&lpk_text).map_err(ExpiryError::Product)?;
            let Some(generation) = cache.pointer(&lpk_text)?.ready else {
                report.not_ready += 1;
                self.progress.remove(&lpk_text);
                continue;
            };
            let progress = self
                .progress
                .get(&lpk_text)
                .filter(|progress| progress.generation == generation)
                .cloned();
            let plan = match progress {
                Some(progress) => {
                    let (_, floor) = cache.bar_bounds(generation, &lpk_text)?;
                    if floor.unwrap_or(0) < progress.floor_ms {
                        report.pending += 1;
                        continue;
                    }
                    if progress.floor_ms < progress.target_floor_ms {
                        continue_expiry(
                            cache,
                            generation,
                            &lpk,
                            progress.target_floor_ms,
                            self.max_opens,
                        )?
                    } else {
                        plan_expiry(cache, generation, &lpk, cap, self.max_opens)?
                    }
                }
                None => plan_expiry(cache, generation, &lpk, cap, self.max_opens)?,
            };
            let Some(plan) = plan else {
                self.progress.remove(&lpk_text);
                continue;
            };
            publish_expiry(
                sink,
                &publish.topic,
                publish.partitions,
                &plan,
                publish.materializer_epoch,
                publish.timeout,
            )
            .map_err(ExpiryError::Publish)?;
            report.floors_published += 1;
            report.tombstones += plan.tombstone_keys.len() as u64;
            report.expired_opens += plan.expired_opens;
            self.progress.insert(
                lpk_text,
                Progress {
                    generation,
                    floor_ms: plan.floor_ms,
                    target_floor_ms: plan.target_floor_ms,
                },
            );
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps(keys: &[&str]) -> BTreeMap<String, u64> {
        keys.iter().map(|key| (key.to_string(), 100)).collect()
    }

    fn bar_lpk(uid: &str) -> String {
        LogicalProductKey::new("paper", "OKX", "SWAP", uid, "BAR", Some("1m"))
            .unwrap()
            .encode()
    }

    #[test]
    fn the_task_refuses_non_bar_products_and_zero_caps() {
        let quote = LogicalProductKey::new("paper", "OKX", "SWAP", "u1", "QUOTE", None)
            .unwrap()
            .encode();
        assert!(matches!(
            ExpiryTask::new(caps(&[&quote]), 1, 1),
            Err(ExpiryError::Product(_))
        ));
        let mut zero = caps(&[&bar_lpk("u1")]);
        zero.insert(bar_lpk("u2"), 0);
        assert!(ExpiryTask::new(zero, 1, 1).is_err());
    }

    #[test]
    fn products_are_visited_round_robin_bounded_per_tick() {
        let keys: Vec<String> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|u| bar_lpk(u))
            .collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let mut task = ExpiryTask::new(caps(&refs), 2, 10).unwrap();
        let mut visited = Vec::new();
        for _ in 0..5 {
            let batch = task.next_products();
            assert_eq!(batch.len(), 2);
            task.cursor = batch.last().cloned();
            visited.extend(batch);
        }
        // Two full rounds over the five sorted products.
        let expected: Vec<String> = keys.iter().chain(keys.iter()).cloned().collect();
        assert_eq!(visited, expected);
    }
}
