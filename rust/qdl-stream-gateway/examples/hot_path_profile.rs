//! Bounded TEST_ONLY microprofile of existing Hub APIs, not capacity evidence.
//! Run with --release and --offline --locked, under the host measurement lock.
use prost::Message as _;
use qdl_stream_gateway::generated::{marketdata_v2 as md, query_v2 as query};
use qdl_stream_gateway::hub::{Hub, HubConfig, RawRecord};
use qdl_stream_gateway::requirement::StreamRequirement;
use qdl_stream_gateway::subscription::{ByteBudget, Subscription};
use serde_json::{json, Value};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct CountedSystem;
static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
#[global_allocator]
static ALLOCATOR: CountedSystem = CountedSystem;

fn account(bytes: usize) {
    if COUNTING.load(Ordering::Relaxed) {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}
unsafe impl GlobalAlloc for CountedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() { account(layout.size()); }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() { account(layout.size()); }
        ptr
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = System.realloc(ptr, layout, size);
        if !next.is_null() { account(size); }
        next
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) { System.dealloc(ptr, layout); }
}

fn start_counts() {
    ALLOCS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
}
fn stop_counts() -> (u64, u64) {
    COUNTING.store(false, Ordering::Relaxed);
    (ALLOCS.load(Ordering::Relaxed), BYTES.load(Ordering::Relaxed))
}
fn now_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
}
fn uid(key: usize) -> String { format!("00000000-0000-4000-8000-{key:012}") }
fn key(id: usize) -> Vec<u8> { format!("{}/trade/TEST_ONLY", uid(id)).into_bytes() }
fn record(offset: i64, physical_key: usize) -> RawRecord {
    let event = md::EventEnvelope {
        instrument_uid: uid(physical_key), source_event_time_ns: now_ns(),
        published_at_ns: now_ns(),
        payload: Some(md::event_envelope::Payload::Trade(md::Trade {
            native_trade_id: offset.to_string(), ..Default::default()
        })), ..Default::default()
    };
    RawRecord { partition: 0, offset, key: key(physical_key),
        payload: event.encode_to_vec(), timestamp_ms: 0 }
}
fn hub() -> Hub {
    Hub::new(&[(0, 1)], HubConfig {
        ring_max_bytes: 64 * 1024 * 1024, ring_max_age: Duration::from_secs(300),
    })
}
fn subscriber(id: u64, budget: &Arc<ByteBudget>, after: i64) -> Arc<Subscription> {
    let requirement = StreamRequirement::from_proto(&query::DataRequirement {
        instrument_uid: uid(0), source_policy_id: "crypto_primary_v2".into(),
        require_full_coverage: true, require_final_bars: true,
        feed_type: query::FeedType::from_str_name("FEED_TYPE_TRADE").unwrap() as i32,
        grade: query::ConsumerGrade::Alpha as i32,
        stale_policy_type: query::StalePolicy::Block as i32,
        gap_policy_type: query::GapPolicy::Block as i32,
        recovery_policy: query::RecoveryPolicy::SnapshotAndReplay as i32,
        revision_policy: query::BarRevisionPolicy::Latest as i32,
        ..Default::default()
    }).unwrap();
    Arc::new(Subscription::new(id, "TEST_ONLY.profile".into(), requirement,
        1000, 8 * 1024 * 1024, budget.clone(), after))
}
fn summary(mut samples: Vec<u64>, counts: (u64, u64)) -> Value {
    assert!(!samples.is_empty());
    samples.sort_unstable();
    let n = samples.len();
    let percentile = |p: usize| samples[(n * p).div_ceil(100).saturating_sub(1)] as f64 / 1e6;
    json!({"n": n, "unit": "ms", "p50": percentile(50), "p95": percentile(95),
        "p99": percentile(99), "max": samples[n-1] as f64 / 1e6,
        "allocated_calls": counts.0, "allocated_bytes_including_realloc_request": counts.1,
        "allocation_window": "timed operation only; fixture and drain excluded",
        "measurement": "inclusive wall time, not CPU time or isolated mutex hold"})
}
fn dispatch_profile(fanout: usize) -> Value {
    let hub = hub();
    let budget = ByteBudget::new(64 * 1024 * 1024);
    assert!(hub.attach_live_budget(budget.clone()));
    let subs: Vec<_> = (0..fanout).map(|i| subscriber(i as u64, &budget, 0)).collect();
    let physical = key(0);
    for sub in &subs { assert_eq!(hub.register(0, &physical, sub.clone(), 0).unwrap().barrier, 1); }
    let records: Vec<_> = (1..=5000).map(|i| record(i, 0)).collect();
    let mut samples = Vec::with_capacity(records.len());
    let mut counts = (0, 0);
    for raw in records {
        let expected = raw.offset;
        start_counts();
        let start = Instant::now();
        hub.dispatch(raw);
        let elapsed = start.elapsed().as_nanos() as u64;
        let count = stop_counts();
        counts.0 += count.0; counts.1 += count.1;
        samples.push(elapsed);
        for sub in &subs {
            match sub.try_next() {
                Some((record, _, charge)) => {
                    assert_eq!(record.raw.offset, expected);
                    drop(charge);
                },
                other => panic!("missing ordered delivery: {other:?}"),
            }
        }
        assert_eq!(budget.used(), 0);
    }
    for sub in &subs { hub.unregister(0, &physical, sub.id); }
    assert_eq!(hub.subscriber_count(), 0);
    json!({"scenario": "dispatch_and_fanout", "fanout": fanout,
        "lossless_records_per_subscriber": 5000, "timing": summary(samples, counts),
        "includes": ["partition lock", "decode", "fanout", "subscription locks", "ring update"],
        "excludes": ["Kafka", "gRPC", "Redis", "consumer", "fixture construction", "queue drain"]})
}
fn replay_profile(rows: usize, keys: usize) -> Value {
    let hub = hub();
    for index in 0..rows { hub.dispatch(record(index as i64 + 1, index % keys)); }
    let physical = key(0);
    let budget = ByteBudget::new(8 * 1024 * 1024);
    let mut samples = Vec::with_capacity(500);
    let mut counts = (0, 0);
    for iteration in 0..500 {
        let sub = subscriber(iteration, &budget, 0);
        start_counts();
        let started = Instant::now();
        let registration = hub.register(0, &physical, sub, 0).unwrap();
        let elapsed = started.elapsed().as_nanos() as u64;
        let count = stop_counts();
        counts.0 += count.0; counts.1 += count.1;
        samples.push(elapsed);
        assert_eq!(registration.barrier, rows as i64 + 1);
        let retained = registration.ring.expect("profile must fit retained ring");
        assert_eq!(retained.len(), rows.div_ceil(keys));
        for (index, raw) in retained.iter().enumerate() {
            assert_eq!(raw.offset, (index * keys + 1) as i64);
            assert_eq!(raw.key, physical);
        }
        hub.unregister(0, &physical, iteration);
    }
    assert_eq!(hub.subscriber_count(), 0);
    assert_eq!(budget.used(), 0);
    json!({"scenario": "register_retained_replay", "ring_records": rows,
        "physical_keys": keys, "matched_records": rows.div_ceil(keys),
        "timing": summary(samples, counts),
        "excludes": ["concurrent dispatcher", "Kafka replay", "gRPC", "Redis"]})
}
fn concurrent_register_dispatch_profile() -> Value {
    let hub = Arc::new(hub());
    for index in 0..10000 { hub.dispatch(record(index + 1, (index % 128) as usize)); }
    let budget = ByteBudget::new(64 * 1024 * 1024);
    assert!(hub.attach_live_budget(budget.clone()));
    let live = subscriber(100000, &budget, 10000);
    hub.register(0, &key(0), live.clone(), 10000).unwrap();
    let (start_tx, start_rx) = std::sync::mpsc::sync_channel(0);
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(0);
    let producer = {
        let hub = hub.clone();
        let budget = budget.clone();
        std::thread::spawn(move || {
            let mut samples = Vec::with_capacity(1000);
            for offset in 10001..=11000 {
                let raw = record(offset, 0);
                start_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                let started = Instant::now();
                hub.dispatch(raw);
                samples.push(started.elapsed().as_nanos() as u64);
                let (item, _, charge) = live.try_next().expect("live delivery");
                assert_eq!(item.raw.offset, offset);
                drop(charge);
                drop(item);
                assert_eq!(budget.used(), 0);
                done_tx.send(()).unwrap();
            }
            samples
        })
    };
    let mut samples = Vec::with_capacity(1000);
    // Use an inactive physical key so registration cannot create a second
    // undrained live queue; the shared partition still contends with dispatch.
    let physical = key(1);
    for id in 0..1000 {
        let sub = subscriber(id, &budget, 0);
        start_tx.send(()).unwrap();
        let started = Instant::now();
        let registration = hub.register(0, &physical, sub, 0).unwrap();
        samples.push(started.elapsed().as_nanos() as u64);
        let retained = registration.ring.unwrap();
        assert_eq!(retained.len(), 79);
        for (index, raw) in retained.iter().enumerate() {
            assert_eq!(raw.offset, (index * 128 + 2) as i64);
            assert_eq!(raw.key, physical);
        }
        hub.unregister(0, &physical, id);
        done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    let dispatch = producer.join().unwrap();
    hub.unregister(0, &key(0), 100000);
    assert_eq!(hub.subscriber_count(), 0);
    assert_eq!(budget.used(), 0);
    let mut registration = summary(samples, (0, 0));
    let mut dispatch = summary(dispatch, (0, 0));
    for summary in [&mut registration, &mut dispatch] {
        summary["allocated_calls"] = Value::Null;
        summary["allocated_bytes_including_realloc_request"] = Value::Null;
        summary["allocation_window"] = json!("NOT_MEASURED: global allocation counters disabled during concurrent scenario");
    }
    json!({"scenario":"concurrent_register_dispatch", "initial_ring_records":10000,
        "register_key_matches":79, "rounds":1000, "register":registration,
        "dispatch":dispatch, "scope":"inclusive operation under shared partition contention; not isolated lock wait/hold"})
}
fn main() {
    assert!(std::env::args().skip(1).eq(["--test-only"]), "explicit --test-only required");
    let mut cases = Vec::new();
    for fanout in [0, 1, 16, 64] { cases.push(dispatch_profile(fanout)); }
    for rows in [1000, 10000] {
        for keys in [1, 128] { cases.push(replay_profile(rows, keys)); }
    }
    println!("{}", json!({"schema": "p184.n1-opt.stream-profile.v1",
        "provenance": "TEST_ONLY_STRUCTURAL_MICROPROFILE_NOT_PROVIDER_NOT_CAPACITY",
        "allocator": "System with example-only allocation counters; overhead included",
        "lock_wait_hold": "NOT_MEASURED separately; inclusive dispatch/register measured",
        "cache_misses": "NOT_MEASURED", "concurrent": concurrent_register_dispatch_profile(), "cases": cases}));
}
