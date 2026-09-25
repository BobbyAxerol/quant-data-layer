//! KN-3 Astra R1 F1/F4 against the dedicated memory-test Redis (the
//! market-cache image; `maxmemory` is server-wide, so these tests never share
//! the Redis of the other suites and restore the limit when they end).
//!
//! F1: memory pressure between two batches, memory freed, the same stage
//! continues without a restart - no record lost, the checkpoint never ahead of
//! the applied data, no product published early.
//! F4: a full rolling rebuild of a near-full cache stays within steady state +
//! two largest products (contract section 5), with every superseded generation
//! reclaimed before the next staging.
//!
//! `QDL_KN_TEST_REDIS_EXCLUSIVE=redis://host:port cargo test -p qdl-projector
//!  --test stage_b_memory -- --ignored --test-threads=1`.

mod common;

use common::*;

fn exclusive() -> String {
    std::env::var("QDL_KN_TEST_REDIS_EXCLUSIVE")
        .expect("QDL_KN_TEST_REDIS_EXCLUSIVE must name a dedicated isolated test Redis")
}

/// Sets `maxmemory`; restores "no limit" when dropped (also on panic).
struct MaxMemory(redis::Connection);

impl MaxMemory {
    fn new() -> Self {
        Self(
            redis::Client::open(exclusive().as_str())
                .unwrap()
                .get_connection()
                .unwrap(),
        )
    }

    fn set(&mut self, bytes: u64) {
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory")
            .arg(bytes)
            .query(&mut self.0)
            .unwrap();
    }

    /// `used_memory` once lazy frees are done.
    fn used(&mut self) -> u64 {
        for _ in 0..100 {
            let info: String = redis::cmd("INFO").arg("memory").query(&mut self.0).unwrap();
            let field = |name: &str| -> u64 {
                info.lines()
                    .find_map(|line| line.strip_prefix(name))
                    .and_then(|value| value.trim().parse().ok())
                    .unwrap_or(0)
            };
            if field("lazyfree_pending_objects:") == 0 {
                return field("used_memory:");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("lazy free did not finish");
    }
}

impl Drop for MaxMemory {
    fn drop(&mut self) {
        self.set(0);
    }
}

fn bars_of(uid: &str) -> LogicalProductKey {
    LogicalProductKey::new("paper", "OKX", "SWAP", uid, "BAR", Some("1m")).unwrap()
}

/// A final 1m bar of `uid` with a payload of realistic size.
fn bar_of(uid: &str, minute: u64) -> Vec<u8> {
    let mut envelope =
        EventEnvelope::decode(bar(minute, BarLifecycle::Final, 0, 1).as_slice()).unwrap();
    envelope.instrument_uid = uid.to_owned();
    envelope.source_id = "okx-swap-bar-1m-primary-v2".repeat(8);
    envelope.encode_to_vec()
}

fn push_bars(log: &Log, uid: &str, minutes: std::ops::Range<u64>, offset: u64) {
    let lpk = bars_of(uid);
    for minute in minutes {
        push(
            log,
            BARS,
            bar_frame(&lpk, bar_of(uid, minute), offset + minute),
        );
    }
}

fn memory_pressure(result: &Result<usize, StageBError>) -> bool {
    matches!(
        result,
        Err(StageBError::Cache(
            qdl_projector::cache::CacheError::MemoryPressure(_)
        ))
    )
}

fn checkpoint_next(stage: &mut StageB<Source>) -> i64 {
    stage
        .cache
        .checkpoint(BARS, 0)
        .unwrap()
        .map_or(0, |checkpoint| checkpoint.next)
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS_EXCLUSIVE (dedicated isolated Redis); run by the kn-native-integration job"]
fn r1_f1_memory_pressure_between_batches_loses_nothing_while_tailing() {
    let mut memory = MaxMemory::new();
    let log = Log::default();
    let lpk = bars_of(UID);
    push_bars(&log, UID, 0..50, 0);
    let environment = environment("r1f1tail");
    let mut stage = stage_on(&exclusive(), &log, &environment);
    for _ in 0..50 {
        stage.step().unwrap();
    }
    assert_eq!(meta(&mut stage, &lpk, "rows").as_deref(), Some("50"));
    push_bars(&log, UID, 50..1_550, 0);
    let used = memory.used();
    memory.set(used + 60_000);
    let mut refused = 0;
    for _ in 0..400 {
        let result = stage.step();
        if memory_pressure(&result) {
            refused += 1;
            // The checkpoint never runs ahead of what the cache holds.
            let rows: u64 = meta(&mut stage, &lpk, "rows").unwrap().parse().unwrap();
            assert_eq!(rows as i64, checkpoint_next(&mut stage));
        } else {
            result.unwrap();
        }
    }
    assert!(
        refused > 10,
        "the limit was hit and kept refusing ({refused})"
    );
    assert!(stage.metrics.rewinds as usize >= refused);
    let stalled = checkpoint_next(&mut stage);
    assert!(stalled < 1_550);
    memory.set(0);
    for _ in 0..600 {
        stage.step().unwrap();
    }
    assert_eq!(checkpoint_next(&mut stage), 1_550);
    assert_eq!(meta(&mut stage, &lpk, "rows").as_deref(), Some("1550"));
    assert_eq!(meta(&mut stage, &lpk, "first").as_deref(), Some("0"));
    assert_eq!(
        meta(&mut stage, &lpk, "last"),
        Some((1_549 * MIN).to_string())
    );
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS_EXCLUSIVE (dedicated isolated Redis); run by the kn-native-integration job"]
fn r1_f1_memory_pressure_during_a_cold_build_never_publishes_early() {
    let mut memory = MaxMemory::new();
    let log = Log::default();
    let lpk = bars_of(UID);
    push_bars(&log, UID, 0..1_500, 0);
    let environment = environment("r1f1build");
    let mut stage = stage_on(&exclusive(), &log, &environment);
    let used = memory.used();
    memory.set(used + 60_000);
    let mut refused = 0;
    for _ in 0..400 {
        let result = stage.step();
        if memory_pressure(&result) {
            refused += 1;
        } else {
            result.unwrap();
        }
        assert!(
            stage.cache.pointer(&lpk.encode()).unwrap().ready.is_none(),
            "not READY before the build boundary is really applied"
        );
    }
    assert!(refused > 10, "{refused}");
    assert!(stage.building(BARS, 0));
    memory.set(0);
    for _ in 0..600 {
        stage.step().unwrap();
    }
    assert!(!stage.building(BARS, 0));
    assert!(stage.cache.pointer(&lpk.encode()).unwrap().ready.is_some());
    assert_eq!(meta(&mut stage, &lpk, "rows").as_deref(), Some("1500"));
    assert_eq!(checkpoint_next(&mut stage), 1_500);
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS_EXCLUSIVE (dedicated isolated Redis); run by the kn-native-integration job"]
fn r1_f4_a_rolling_rebuild_of_a_near_full_cache_stays_within_two_products() {
    let mut memory = MaxMemory::new();
    let log = Log::default();
    let uids: Vec<String> = (0..6u32)
        .map(|index| format!("fb26214c-7b9b-5961-95b2-55154755af{index:02x}"))
        .collect();
    for (index, uid) in uids.iter().enumerate() {
        push_bars(&log, uid, 0..2_000, 100_000 * index as u64);
    }
    let environment = environment("r1f4");
    let before_build = memory.used();
    let mut first = stage_on(&exclusive(), &log, &environment);
    first.limits.max_batch_records = 500;
    for _ in 0..200 {
        first.step().unwrap();
    }
    for uid in &uids {
        assert_eq!(
            meta(&mut first, &bars_of(uid), "rows").as_deref(),
            Some("2000")
        );
    }
    drop(first);
    let steady = memory.used();
    let product = (steady - before_build) / uids.len() as u64;
    // The old generations stay served; the cap leaves room for two
    // products in staging and nothing more (a partition-wide build would
    // need a second copy of all six and hit the limit).
    let limit = steady + 2 * product + 256 * 1024;
    memory.set(limit);
    let mut late = stage_on(&exclusive(), &log, &environment)
        .with_rebuild_reader(Box::new(Reader {
            log: log.clone(),
            at: None,
            faults: Faults::default(),
        }))
        .with_clock(|| u64::MAX / 2);
    late.limits.max_batch_records = 500;
    let mut peak = 0;
    for _ in 0..2_000 {
        late.step().unwrap();
        peak = peak.max(memory.used());
        let staged = uids
            .iter()
            .filter(|uid| {
                late.cache
                    .pointer(&bars_of(uid).encode())
                    .unwrap()
                    .staging
                    .is_some()
            })
            .count();
        assert!(staged <= 1, "one product in staging at a time");
        for uid in &uids {
            assert!(
                late.cache
                    .pointer(&bars_of(uid).encode())
                    .unwrap()
                    .ready
                    .is_some(),
                "every product stays READY during the rebuild"
            );
        }
        if late.metrics.rebuilds_completed == uids.len() as u64 {
            break;
        }
    }
    assert_eq!(late.metrics.rolling_rebuilds, 1);
    assert_eq!(late.metrics.builds, 1, "only the empty latest partition");
    assert_eq!(late.metrics.rebuilds_completed, uids.len() as u64);
    assert!(
        peak <= steady + 2 * product + 256 * 1024,
        "peak {peak} steady {steady} product {product}"
    );
    let after = memory.used();
    assert!(
        after <= steady + product / 4,
        "superseded generations reclaimed: after {after}, steady {steady}"
    );
    for uid in &uids {
        assert_eq!(
            meta(&mut late, &bars_of(uid), "rows").as_deref(),
            Some("2000")
        );
    }
    eprintln!(
        "r1_f4: steady {steady} B, product {product} B, peak {peak} B (steady + {:.2} products), after {after} B",
        (peak - steady) as f64 / product as f64
    );
}
