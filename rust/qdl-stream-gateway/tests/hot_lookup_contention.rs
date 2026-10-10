//! R2 (Astra advisory 2026-10-10): concurrent canonical hot lookups of
//! different products must not refuse each other while the live reader
//! dispatches. In-process workload over the public `Hub` API only (a paced
//! fake committed log driven by `Hub::run`), so the same file measures the
//! pre-fix hub as the counterfactual. Prints lookup latency, refusal counts
//! and the hub's commit -> dispatch lag; run with `--nocapture` to see them.

use prost::Message as _;
use qdl_stream_gateway::generated::marketdata_v2::{
    event_envelope::Payload, EventEnvelope, OrderBookDelta, OrderBookSnapshot, Quote,
};
use qdl_stream_gateway::hub::{Hub, HubConfig, LogSource, RawRecord};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PARTITIONS: i32 = 6;
/// Busy QUOTE products, one per (partition, index).
const BUSY: i32 = 48;
/// BOOK keys (snapshot every `SNAPSHOT_EVERY` records, deltas between).
const BOOKS: i32 = 6;
const SNAPSHOT_EVERY: u64 = 100;
/// Records per second offered by the fake log, over all partitions.
const RATE: u64 = 20_000;
const LOOKUP_THREADS: usize = 8;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn envelope(payload: Payload) -> Vec<u8> {
    EventEnvelope {
        payload: Some(payload),
        connection_generation: 1,
        ..Default::default()
    }
    .encode_to_vec()
}

fn quote() -> Vec<u8> {
    envelope(Payload::Quote(Quote::default()))
}

fn book(snapshot: bool) -> Vec<u8> {
    envelope(if snapshot {
        Payload::BookSnapshot(OrderBookSnapshot {
            book_generation: 1,
            sequence_verified: true,
            ..Default::default()
        })
    } else {
        Payload::BookDelta(OrderBookDelta {
            book_generation: 1,
            sequence_verified: true,
            ..Default::default()
        })
    })
}

/// Paced committed log: a few quiet records first, then busy QUOTE and BOOK
/// traffic round-robin. Broker heads are what was handed to the reader, so
/// the hub confirms each one right after dispatching it.
struct PacedLog {
    next: BTreeMap<i32, i64>,
    sent: u64,
    started: Instant,
    pending: Vec<(i32, Vec<u8>, Vec<u8>)>,
}

impl PacedLog {
    fn new() -> Self {
        let mut pending = Vec::new();
        for partition in 0..PARTITIONS {
            pending.push((
                partition,
                format!("quiet-{partition}").into_bytes(),
                quote(),
            ));
        }
        Self {
            next: (0..PARTITIONS).map(|p| (p, 0)).collect(),
            sent: 0,
            started: Instant::now(),
            pending,
        }
    }

    fn next_record(&mut self) -> (i32, Vec<u8>, Vec<u8>) {
        if let Some(record) = self.pending.pop() {
            return record;
        }
        let slot = (self.sent % (BUSY + BOOKS) as u64) as i32;
        let round = self.sent / (BUSY + BOOKS) as u64;
        if slot < BUSY {
            (
                slot % PARTITIONS,
                format!("busy-{slot}").into_bytes(),
                quote(),
            )
        } else {
            let index = slot - BUSY;
            let snapshot = round % SNAPSHOT_EVERY == 0;
            (
                index % PARTITIONS,
                format!("book-{index}").into_bytes(),
                book(snapshot),
            )
        }
    }
}

impl LogSource for PacedLog {
    fn poll(&mut self, timeout: Duration) -> Result<Option<RawRecord>, String> {
        let due = self.started + Duration::from_nanos(self.sent * 1_000_000_000 / RATE);
        let now = Instant::now();
        if due > now {
            let wait = (due - now).min(timeout);
            std::thread::sleep(wait);
            if Instant::now() < due {
                return Ok(None);
            }
        }
        let (partition, key, payload) = self.next_record();
        let offset = self.next[&partition];
        self.next.insert(partition, offset + 1);
        self.sent += 1;
        Ok(Some(RawRecord {
            partition,
            offset,
            key,
            payload,
            timestamp_ms: now_ms(),
        }))
    }

    fn positions(&self) -> Vec<(i32, i64)> {
        self.next.iter().map(|(p, o)| (*p, *o)).collect()
    }

    fn broker_heads(&self) -> Vec<(i32, i64, Instant)> {
        let now = Instant::now();
        self.next.iter().map(|(p, o)| (*p, *o, now)).collect()
    }
}

#[derive(Default)]
struct Outcome {
    latencies_us: Vec<u64>,
    refusals: BTreeMap<String, u64>,
    ok: u64,
}

fn quantile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() as f64 * q).ceil() as usize).clamp(1, sorted.len()) - 1]
}

/// Each lookup thread owns distinct products: QUOTE busy keys, quiet keys
/// (behind every busy record of their partition) and BOOK_SNAPSHOT keys
/// (behind up to `SNAPSHOT_EVERY` deltas).
fn lookup_targets(thread: usize) -> Vec<(Vec<u8>, &'static str)> {
    let mut targets = Vec::new();
    for slot in 0..BUSY {
        if slot as usize % LOOKUP_THREADS == thread {
            targets.push((format!("busy-{slot}").into_bytes(), "QUOTE"));
        }
    }
    for partition in 0..PARTITIONS {
        if partition as usize % LOOKUP_THREADS == thread {
            targets.push((format!("quiet-{partition}").into_bytes(), "QUOTE"));
        }
    }
    for index in 0..BOOKS {
        if (index as usize + 3) % LOOKUP_THREADS == thread {
            targets.push((format!("book-{index}").into_bytes(), "BOOK_SNAPSHOT"));
        }
    }
    targets
}

fn run_workload(duration: Duration) -> (Outcome, serde_json::Value, u64) {
    let hub = Arc::new(Hub::new(
        &(0..PARTITIONS).map(|p| (p, 0)).collect::<Vec<_>>(),
        HubConfig::default(),
    ));
    let reader = hub.run(Box::new(PacedLog::new()));
    // Warm: every product (including the first BOOK snapshot) is readable.
    let warm = Instant::now() + Duration::from_secs(10);
    for thread in 0..LOOKUP_THREADS {
        for (key, feed) in lookup_targets(thread) {
            while hub
                .latest_hot(&key, feed, Instant::now() + Duration::from_millis(100))
                .is_err()
            {
                assert!(Instant::now() < warm, "warm-up timed out");
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
    let records_before = hub.metrics.records.load(Ordering::Relaxed);
    let stop = Arc::new(AtomicBool::new(false));
    let workers: Vec<_> = (0..LOOKUP_THREADS)
        .map(|thread| {
            let (hub, stop) = (hub.clone(), stop.clone());
            std::thread::spawn(move || {
                let targets = lookup_targets(thread);
                let mut outcome = Outcome::default();
                let mut index = 0;
                while !stop.load(Ordering::Relaxed) {
                    let (key, feed) = &targets[index % targets.len()];
                    index += 1;
                    let started = Instant::now();
                    let result = hub.latest_hot(key, feed, started + Duration::from_millis(100));
                    outcome
                        .latencies_us
                        .push(started.elapsed().as_micros() as u64);
                    match result {
                        Ok(view) => {
                            assert_eq!(&view.raw.key, key);
                            assert!(view.raw.offset <= view.boundary_offset);
                            outcome.ok += 1;
                        }
                        Err(code) => *outcome.refusals.entry(code).or_default() += 1,
                    }
                    // A request rate, not a spin: the RPC layer admits 8 at once.
                    std::thread::sleep(Duration::from_micros(50));
                }
                outcome
            })
        })
        .collect();
    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);
    let mut total = Outcome::default();
    for worker in workers {
        let outcome = worker.join().unwrap();
        total.ok += outcome.ok;
        total.latencies_us.extend(outcome.latencies_us);
        for (code, count) in outcome.refusals {
            *total.refusals.entry(code).or_default() += count;
        }
    }
    let dispatched = hub.metrics.records.load(Ordering::Relaxed) - records_before;
    let lag = hub.metrics.dispatch_lag.summary();
    hub.stop();
    reader.join().unwrap();
    total.latencies_us.sort_unstable();
    (total, lag, dispatched)
}

#[test]
fn concurrent_lookups_of_different_products_do_not_refuse_each_other() {
    let duration = Duration::from_secs(2);
    let (outcome, lag, dispatched) = run_workload(duration);
    let lookups = outcome.latencies_us.len() as u64;
    let busy = outcome
        .refusals
        .get("HOT_READER_BUSY")
        .copied()
        .unwrap_or(0);
    println!(
        "R2_BENCH lookups={lookups} ok={} busy={busy} refusals={:?} \
         latency_us p50={} p95={} p99={} max={} dispatched={dispatched} \
         dispatch_rate_per_s={} dispatch_lag_ms={}",
        outcome.ok,
        outcome.refusals,
        quantile(&outcome.latencies_us, 0.50),
        quantile(&outcome.latencies_us, 0.95),
        quantile(&outcome.latencies_us, 0.99),
        outcome.latencies_us.last().copied().unwrap_or(0),
        dispatched / duration.as_secs(),
        lag,
    );
    assert!(lookups > 1_000, "workload too small: {lookups}");
    assert_eq!(
        busy, 0,
        "lookups refused each other: {:?}",
        outcome.refusals
    );
    assert_eq!(
        outcome.ok, lookups,
        "unexpected refusals: {:?}",
        outcome.refusals
    );
}
