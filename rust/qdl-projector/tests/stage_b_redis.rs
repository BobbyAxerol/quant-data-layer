//! K3-T02..T06 for stage B against a real, isolated Redis (the market-cache
//! image). The state topics are an in-memory log here (their Kafka
//! transport is covered by `stage_b_kafka`); envelopes are synthetic unit
//! fixtures built with prost and framed with the real state codec.
//!
//! `QDL_KN_TEST_REDIS=redis://host:port cargo test -p qdl-projector --test stage_b_redis -- --ignored`.

mod common;

use common::*;

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
fn beyond_the_rebuild_horizon_every_product_is_swapped_one_by_one() {
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
    let mut late = rebuild_stage(&log, &environment).with_clock(|| u64::MAX / 2);
    // D21: no partition build beside the served generations; the products
    // are rebuilt one at a time and stay READY on the old generation until
    // their swap.
    late.step().unwrap();
    assert_eq!(late.metrics.rolling_rebuilds, 1, "the latest partition");
    assert_eq!(late.metrics.builds, 1, "only the empty bars partition");
    assert!(
        read_latest(&mut late, &quotes).is_some(),
        "served meanwhile"
    );
    for _ in 0..200 {
        late.step().unwrap();
        let staged = [&quotes, &gone]
            .iter()
            .filter(|lpk| late.cache.pointer(&lpk.encode()).unwrap().staging.is_some())
            .count();
        assert!(staged <= 1, "one product in staging at a time");
    }
    let pointer = late.cache.pointer(&quotes.encode()).unwrap();
    assert!(
        pointer.ready.unwrap() > old_generation,
        "{:?} {:?} {:?}",
        late.metrics,
        late.last_rebuild_error,
        late.rebuilding()
    );
    assert_eq!(
        read_latest(&mut late, &quotes).map(|(_, offset)| offset),
        Some(10)
    );
    assert_eq!(
        late.cache.pointer(&gone.encode()).unwrap().ready,
        None,
        "gone -> NOT_READY"
    );
    assert_eq!(late.metrics.rebuilds_completed, 2);
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
    let pending: Vec<String> = redis::cmd("SMEMBERS")
        .arg(late.cache.layout.rebuild_requests())
        .query(late.cache.connection())
        .unwrap();
    assert!(pending.is_empty(), "obligations cleared: {pending:?}");
    let retiring: u64 = redis::cmd("SCARD")
        .arg(late.cache.layout.retire())
        .query(late.cache.connection())
        .unwrap();
    assert_eq!(retiring, 0, "every superseded generation reclaimed");
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
fn late_venue_history_never_moves_the_latest_bar_back_and_live_is_kept() {
    // KN-4 D47-4: the live log (mirror) delivers opens 20 and 21 first; the
    // venue history fill arrives later with opens 10..=20, where its open 20
    // is the same bar with other content (BACKFILLED provenance).
    let log = Log::default();
    let bars = lpk("BAR", Some("1m"));
    let live_20 = bar(20, BarLifecycle::Final, 0, 200);
    let live_21 = bar(21, BarLifecycle::Final, 0, 210);
    push(&log, BARS, bar_frame(&bars, live_20.clone(), 1));
    push(&log, BARS, bar_frame(&bars, live_21.clone(), 2));
    let mut offset = 3;
    for open in 10..20u64 {
        push(
            &log,
            BARS,
            bar_frame(
                &bars,
                bar(open, BarLifecycle::Final, 0, 100 + open as u32),
                offset,
            ),
        );
        offset += 1;
    }
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(20, BarLifecycle::Final, 0, 999), offset),
    );
    let environment = environment("late-history");
    let mut stage = stage(&log, &environment);
    drain(&mut stage);
    assert_eq!(
        meta(&mut stage, &bars, "last").as_deref(),
        Some((21 * MIN).to_string().as_str())
    );
    assert_eq!(
        meta(&mut stage, &bars, "last_final").as_deref(),
        Some((21 * MIN).to_string().as_str())
    );
    assert_eq!(
        meta(&mut stage, &bars, "first").as_deref(),
        Some((10 * MIN).to_string().as_str())
    );
    assert_eq!(read_bar(&mut stage, &bars, 21), Some(live_21));
    assert_eq!(
        read_bar(&mut stage, &bars, 20),
        Some(live_20),
        "the live bar is kept, never last-write-wins"
    );
    assert_eq!(meta(&mut stage, &bars, "conflicts").as_deref(), Some("1"));
    assert_eq!(meta(&mut stage, &bars, "rows").as_deref(), Some("12"));
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

// ------------------------------------------------------------ Astra R1

fn wide(log: &Log, environment: &str) -> StageB<Source> {
    let mut stage = stage(log, environment);
    stage.limits.max_batch_records = 5_000;
    stage
}

/// Generations that still own keys of `lpk` (latest, BAR meta/buckets/facts,
/// conflicts).
fn key_generations(stage: &mut StageB<Source>, lpk: &LogicalProductKey) -> BTreeSet<u64> {
    let prefix = stage.cache.layout.prefix().to_owned();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}*"))
        .query(stage.cache.connection())
        .unwrap();
    let product = lpk.encode();
    keys.iter()
        .filter_map(|key| {
            let rest = key.strip_prefix(&prefix)?;
            let (kind, rest) = rest.split_once(':')?;
            if !matches!(kind, "l" | "bm" | "b" | "rk" | "cx") {
                return None;
            }
            let (generation, rest) = rest.split_once(':')?;
            rest.starts_with(&product)
                .then(|| generation.parse().ok())?
        })
        .collect()
}

fn bucket_ids(stage: &mut StageB<Source>, generation: u64, lpk: &LogicalProductKey) -> Vec<u64> {
    let pattern = format!(
        "{}b:{generation}:{}:*",
        stage.cache.layout.prefix(),
        lpk.encode()
    );
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(pattern)
        .query(stage.cache.connection())
        .unwrap();
    let mut ids: Vec<u64> = keys
        .iter()
        .map(|key| key.rsplit(':').next().unwrap().parse().unwrap())
        .collect();
    ids.sort();
    ids
}

fn assert_floor_state(stage: &mut StageB<Source>, bars: &LogicalProductKey, floor_min: u64) {
    let generation = stage.cache.pointer(&bars.encode()).unwrap().ready.unwrap();
    let boundary = bucket_of(floor_min * MIN, MIN);
    let ids = bucket_ids(stage, generation, bars);
    assert!(
        ids.iter().all(|id| *id >= boundary),
        "no bucket below the floor's boundary {boundary}: {ids:?}"
    );
    let (rows, counted) = stage
        .cache
        .bar_row_count(generation, &bars.encode(), MIN)
        .unwrap();
    assert_eq!(rows, counted, "meta rows = rows in buckets");
    assert_eq!(
        meta(stage, bars, "floor"),
        Some((floor_min * MIN).to_string())
    );
    let facts: Vec<String> = redis::cmd("HKEYS")
        .arg(stage.cache.layout.fact_keys(generation, &bars.encode()))
        .query(stage.cache.connection())
        .unwrap();
    assert!(facts
        .iter()
        .all(|open| open.parse::<u64>().unwrap() >= floor_min * MIN));
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn r1_f2_a_floor_in_the_batch_of_its_rows_leaves_no_bucket_below_it() {
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
    // A late repair below the floor, after it in the same batch.
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(5, BarLifecycle::Revised, 1, 2), 999),
    );
    let fresh_env = environment("r1f2fresh");
    let mut stage = wide(&log, &fresh_env);
    drain(&mut stage);
    assert!(
        stage.metrics.batches <= 2,
        "one batch carries rows and floor"
    );
    assert_eq!(meta(&mut stage, &bars, "rows").as_deref(), Some("20"));
    assert_eq!(
        meta(&mut stage, &bars, "first"),
        Some((280 * MIN).to_string())
    );
    assert!(
        read_bar(&mut stage, &bars, 5).is_none(),
        "late repair not kept"
    );
    assert!(read_bar(&mut stage, &bars, 280).is_some());
    assert_floor_state(&mut stage, &bars, 280);

    // An existing generation: older rows and the floor arrive together.
    let log = Log::default();
    for minute in 100..300u64 {
        push(
            &log,
            BARS,
            bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
        );
    }
    let environment = environment("r1f2existing");
    let mut stage = wide(&log, &environment);
    drain(&mut stage);
    for minute in 10..60u64 {
        push(
            &log,
            BARS,
            bar_frame(
                &bars,
                bar(minute, BarLifecycle::Final, 0, 1),
                1_000 + minute,
            ),
        );
    }
    push(&log, BARS, floor_frame(&bars, 280 * MIN));
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(50, BarLifecycle::Revised, 1, 2), 2_000),
    );
    drain(&mut stage);
    assert_eq!(meta(&mut stage, &bars, "rows").as_deref(), Some("20"));
    assert!(read_bar(&mut stage, &bars, 50).is_none());
    assert_floor_state(&mut stage, &bars, 280);
    // A later reclaim of the generation leaves nothing behind.
    let generation = stage.cache.pointer(&bars.encode()).unwrap().ready.unwrap();
    stage
        .cache
        .reclaim(generation, &bars.encode(), Some(MIN))
        .unwrap();
    assert!(key_generations(&mut stage, &bars).is_empty());
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn r1_f3_interrupted_builds_and_swaps_leave_no_generation_behind() {
    let log = Log::default();
    let quotes = lpk("QUOTE", None);
    let bars = lpk("BAR", Some("1m"));
    for level in 0..20u32 {
        push(
            &log,
            LATEST,
            latest_frame(&quotes, quote(level), 10 + u64::from(level)),
        );
    }
    for minute in 0..300u64 {
        push(
            &log,
            BARS,
            bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
        );
    }
    let environment = environment("r1f3");
    // Crash in the middle of the cold build, five times in a row.
    for _ in 0..5 {
        let mut interrupted = stage(&log, &environment);
        interrupted.step().unwrap();
        interrupted.step().unwrap();
        drop(interrupted);
    }
    let mut stage = rebuild_stage(&log, &environment);
    drain(&mut stage);
    for product in [&quotes, &bars] {
        let pointer = stage.cache.pointer(&product.encode()).unwrap();
        assert_eq!(pointer.staging, None);
        let live: BTreeSet<u64> = pointer.ready.into_iter().collect();
        assert_eq!(
            key_generations(&mut stage, product),
            live,
            "{}",
            product.encode()
        );
    }
    assert_eq!(meta(&mut stage, &bars, "rows").as_deref(), Some("300"));
    // Crash windows of a swap: a staging generation replaced by another
    // (interrupted rebuild) and a publish whose reclaim never ran.
    let old = stage
        .cache
        .pointer(&quotes.encode())
        .unwrap()
        .ready
        .unwrap();
    let mut pointer = stage.cache.pointer(&quotes.encode()).unwrap();
    let fence = stage.cache.take_ownership(LATEST, 0).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut staged = Vec::new();
    for _ in 0..2 {
        let generation = stage.cache.allocate_generation().unwrap();
        assert!(matches!(
            stage
                .cache
                .apply(
                    LATEST,
                    0,
                    fence,
                    20,
                    now,
                    &[Op::Stage {
                        lpk: quotes.encode(),
                        pointer: pointer.clone(),
                        generation,
                    }],
                )
                .unwrap(),
            Applied::Ok(_)
        ));
        pointer = stage.cache.pointer(&quotes.encode()).unwrap();
        assert!(matches!(
            stage
                .cache
                .apply(
                    LATEST,
                    0,
                    fence,
                    20,
                    now,
                    &[Op::Latest {
                        lpk: quotes.encode(),
                        generation,
                        pointer: pointer.clone(),
                        expected_offset: None,
                        value: vec![1; 64],
                        topic_id: "t".into(),
                        partition: 0,
                        offset: 1,
                    }],
                )
                .unwrap(),
            Applied::Ok(_)
        ));
        staged.push(generation);
    }
    assert!(matches!(
        stage
            .cache
            .apply(
                LATEST,
                0,
                fence,
                20,
                now,
                &[Op::Publish {
                    lpk: quotes.encode(),
                    pointer: pointer.clone(),
                }],
            )
            .unwrap(),
        Applied::Ok(_)
    ));
    drop(stage);
    // The next owner resumes the retirements (the old ready and the replaced
    // staging generation) without any other trigger.
    let mut next = rebuild_stage(&log, &environment);
    drain(&mut next);
    let live: BTreeSet<u64> = [staged[1]].into_iter().collect();
    assert_eq!(key_generations(&mut next, &quotes), live);
    assert!(!key_generations(&mut next, &quotes).contains(&old));
    let retiring: u64 = redis::cmd("SCARD")
        .arg(next.cache.layout.retire())
        .query(next.cache.connection())
        .unwrap();
    assert_eq!(retiring, 0);
    assert!(next.metrics.retired >= 2);
}

// ------------------------------------------------------------ Astra R2

fn bar_rows(stage: &mut StageB<Source>, bars: &LogicalProductKey) -> u64 {
    meta(stage, bars, "rows").map_or(0, |rows| rows.parse().unwrap())
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn r2_f1_an_assignment_error_after_the_poll_loses_no_record() {
    let log = Log::default();
    let bars = lpk("BAR", Some("1m"));
    for minute in 0..20u64 {
        push(
            &log,
            BARS,
            bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
        );
    }
    let environment = environment("r2assign");
    let mut stage = stage(&log, &environment);
    drain(&mut stage);
    assert_eq!(bar_rows(&mut stage, &bars), 20);
    for minute in 20..60u64 {
        push(
            &log,
            BARS,
            bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
        );
    }
    // The poll returns 7 records, then the assignment query fails.
    stage.source.faults.set(|plan| plan.assigned = 1);
    assert!(stage.step().is_err());
    assert_eq!(
        bar_rows(&mut stage, &bars),
        20,
        "nothing applied by the failed step"
    );
    drain(&mut stage);
    assert_eq!(
        bar_rows(&mut stage, &bars),
        60,
        "every polled record applied on retry"
    );
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn r2_f1_a_prepare_error_for_a_new_partition_keeps_the_running_batch() {
    let log = Log::default();
    let bars = lpk("BAR", Some("1m"));
    let quotes = lpk("QUOTE", None);
    for minute in 0..20u64 {
        push(
            &log,
            BARS,
            bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
        );
    }
    push(&log, LATEST, latest_frame(&quotes, quote(1), 10));
    let environment = environment("r2prepare");
    let mut stage = stage(&log, &environment);
    stage.source.assigned = vec![(BARS.into(), 0)];
    drain(&mut stage);
    for minute in 20..60u64 {
        push(
            &log,
            BARS,
            bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
        );
    }
    // A rebalance adds the latest partition; preparing it fails once while
    // the running bars partition has just been polled.
    stage.source.assigned.push((LATEST.into(), 0));
    stage.source.faults.set(|plan| plan.watermarks = 1);
    assert!(stage.step().is_err());
    drain(&mut stage);
    assert_eq!(bar_rows(&mut stage, &bars), 60);
    assert_eq!(
        read_latest(&mut stage, &quotes).map(|(_, offset)| offset),
        Some(10)
    );
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn r2_f1_a_replay_error_after_the_reader_moved_never_publishes_a_gap() {
    for (label, inject) in [("position", 0usize), ("poll", 1usize)] {
        let log = Log::default();
        let bars = lpk("BAR", Some("1m"));
        for minute in 0..60u64 {
            push(
                &log,
                BARS,
                bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
            );
        }
        let environment = environment(&format!("r2replay{label}"));
        let mut stage = rebuild_stage(&log, &environment);
        drain(&mut stage);
        let old = stage.cache.pointer(&bars.encode()).unwrap().ready.unwrap();
        stage.request_rebuild(&bars.encode());
        stage.step().unwrap(); // starts the rebuild and replays the first records
        stage.source.faults.set(|plan| {
            if inject == 0 {
                plan.reader_position = 1;
            } else {
                plan.reader_poll_after_move = 1;
            }
        });
        let mut failed = 0;
        for _ in 0..200 {
            if stage.step().is_err() {
                failed += 1;
            }
        }
        assert_eq!(failed, 1, "{label}: the injected error surfaced once");
        let pointer = stage.cache.pointer(&bars.encode()).unwrap();
        assert!(pointer.ready.unwrap() > old, "{label}: rebuilt");
        assert_eq!(bar_rows(&mut stage, &bars), 60, "{label}: no gap published");
        for minute in 0..60u64 {
            assert!(
                read_bar(&mut stage, &bars, minute).is_some(),
                "{label}: open {minute}"
            );
        }
        assert!(
            stage.metrics.rebuilds_abandoned >= 1,
            "{label}: the torn replay was dropped"
        );
    }
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn r2_f2_a_late_repair_below_the_old_floor_does_not_hide_a_valid_older_row() {
    let log = Log::default();
    let bars = lpk("BAR", Some("1m"));
    for minute in 224..400u64 {
        push(
            &log,
            BARS,
            bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
        );
    }
    let environment = environment("r2f2");
    let mut stage = wide(&log, &environment);
    drain(&mut stage);
    push(&log, BARS, floor_frame(&bars, 50 * MIN));
    drain(&mut stage);
    assert_eq!(
        meta(&mut stage, &bars, "first"),
        Some((224 * MIN).to_string())
    );
    // One batch: BAR 0 (below floor 50, refused), BAR 60 (valid, a new bucket
    // before `first`), then floor 300.
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(0, BarLifecycle::Final, 0, 1), 1_000),
    );
    push(
        &log,
        BARS,
        bar_frame(&bars, bar(60, BarLifecycle::Final, 0, 1), 1_001),
    );
    push(&log, BARS, floor_frame(&bars, 300 * MIN));
    drain(&mut stage);
    assert!(read_bar(&mut stage, &bars, 0).is_none());
    assert!(
        read_bar(&mut stage, &bars, 60).is_none(),
        "BAR 60 went with the floor"
    );
    assert_eq!(bar_rows(&mut stage, &bars), 100, "opens 300..399");
    assert_floor_state(&mut stage, &bars, 300);
    let generation = stage.cache.pointer(&bars.encode()).unwrap().ready.unwrap();
    stage
        .cache
        .reclaim(generation, &bars.encode(), Some(MIN))
        .unwrap();
    assert!(
        key_generations(&mut stage, &bars).is_empty(),
        "reclaim leaves no key"
    );
}

// ------------------------------------------------------------ Astra R3

/// A 1m BAR product of 60 rows, built, with a rebuild requested; `arm`
/// injects cache faults before the first step of the rebuild.
fn r3_rebuild(
    label: &str,
    arm: impl FnOnce(&mut qdl_projector::fault::CacheFaults),
) -> (Log, StageB<Source>, LogicalProductKey, u64, usize) {
    let log = Log::default();
    let bars = lpk("BAR", Some("1m"));
    for minute in 0..60u64 {
        push(
            &log,
            BARS,
            bar_frame(&bars, bar(minute, BarLifecycle::Final, 0, 1), minute),
        );
    }
    let mut stage = rebuild_stage(&log, &environment(label));
    drain(&mut stage);
    let old = stage.cache.pointer(&bars.encode()).unwrap().ready.unwrap();
    arm(&mut stage.cache.faults.lock().unwrap());
    stage.request_rebuild(&bars.encode());
    let mut errors = 0;
    for _ in 0..300 {
        if stage.step().is_err() {
            errors += 1;
        }
        // Invariant at every step: the READY generation holds its data.
        let ready = stage.cache.pointer(&bars.encode()).unwrap().ready;
        if let Some(generation) = ready {
            let (rows, counted) = stage
                .cache
                .bar_row_count(generation, &bars.encode(), MIN)
                .unwrap();
            assert_eq!(
                (rows, counted),
                (60, 60),
                "{label}: READY generation {generation} lost its data"
            );
        }
    }
    (log, stage, bars, old, errors)
}

/// Pointer, payload, checkpoint, generations and bookkeeping agree.
fn assert_rebuild_consistent(stage: &mut StageB<Source>, bars: &LogicalProductKey, old: u64) {
    let pointer = stage.cache.pointer(&bars.encode()).unwrap();
    let ready = pointer.ready.expect("READY");
    assert_ne!(ready, old, "a new generation is ready");
    assert_eq!(pointer.staging, None);
    assert_eq!(bar_rows(stage, bars), 60);
    for minute in 0..60u64 {
        assert!(
            read_bar(stage, bars, minute).is_some(),
            "payload of open {minute}"
        );
    }
    let (rows, counted) = stage
        .cache
        .bar_row_count(ready, &bars.encode(), MIN)
        .unwrap();
    assert_eq!(rows, counted);
    assert_eq!(stage.cache.checkpoint(BARS, 0).unwrap().unwrap().next, 60);
    let live: BTreeSet<u64> = [ready].into_iter().collect();
    assert_eq!(
        key_generations(stage, bars),
        live,
        "only the ready generation owns keys"
    );
    for set in [
        stage.cache.layout.retire(),
        stage.cache.layout.rebuild_requests(),
    ] {
        let count: u64 = redis::cmd("SCARD")
            .arg(&set)
            .query(stage.cache.connection())
            .unwrap();
        assert_eq!(count, 0, "{set}");
    }
    assert!(stage.rebuilding().is_none());
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn r3_an_error_before_the_publish_cas_discards_the_staging_and_retries() {
    let (_log, mut stage, bars, old, errors) =
        r3_rebuild("r3before", |faults| faults.fail_before_publish = 1);
    assert_eq!(errors, 1);
    assert_eq!(
        stage.metrics.rebuilds_abandoned, 1,
        "unpublished staging discarded"
    );
    assert_eq!(stage.metrics.rebuilds_started, 2, "and the replay redone");
    assert_eq!(stage.metrics.rebuilds_completed, 1);
    assert_rebuild_consistent(&mut stage, &bars, old);
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn r3_an_error_after_the_publish_cas_never_deletes_the_ready_generation() {
    // The swap commits; reclaiming the old generation fails.
    let (_log, mut stage, bars, old, errors) =
        r3_rebuild("r3after", |faults| faults.fail_reclaim = 1);
    assert_eq!(errors, 1);
    assert_eq!(
        stage.metrics.rebuilds_abandoned, 0,
        "the published generation is kept"
    );
    assert_eq!(
        (
            stage.metrics.rebuilds_started,
            stage.metrics.rebuilds_completed
        ),
        (1, 1),
        "no second replay"
    );
    assert_rebuild_consistent(&mut stage, &bars, old);
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn r3_a_lost_publish_reply_is_resolved_by_reading_the_pointer_back() {
    for (label, unreadable) in [("r3lost", 0usize), ("r3lostunread", 1usize)] {
        // The server commits the swap but the reply is lost; in the second
        // case the pointer cannot be read back at first either.
        let (_log, mut stage, bars, old, errors) = r3_rebuild(label, |faults| {
            faults.lose_publish_reply = 1;
            faults.unreadable_after_lost_reply = unreadable;
        });
        assert!(errors >= 1, "{label}");
        assert_eq!(
            stage.metrics.rebuilds_abandoned, 0,
            "{label}: published, not discarded"
        );
        assert_eq!(
            (
                stage.metrics.rebuilds_started,
                stage.metrics.rebuilds_completed
            ),
            (1, 1),
            "{label}"
        );
        assert_rebuild_consistent(&mut stage, &bars, old);
    }
}

// ------------------------------------------------------------ Astra R4

fn bars_of_uid(uid: &str) -> LogicalProductKey {
    LogicalProductKey::new("paper", "OKX", "SWAP", uid, "BAR", Some("1m")).unwrap()
}

fn bar_of_uid(uid: &str, minute: u64) -> Vec<u8> {
    let mut envelope =
        EventEnvelope::decode(bar(minute, BarLifecycle::Final, 0, 1).as_slice()).unwrap();
    envelope.instrument_uid = uid.to_owned();
    envelope.encode_to_vec()
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn r4_a_never_ready_product_is_rebuilt_after_its_staging_was_unstaged() {
    let uid_b = "fb26214c-7b9b-5961-95b2-55154755afbb";
    let (a, b) = (bars_of_uid(UID), bars_of_uid(uid_b));
    let log = Log::default();
    for minute in 0..50u64 {
        push(&log, BARS, bar_frame(&a, bar_of_uid(UID, minute), minute));
    }
    let environment = environment("r4");
    let mut first = stage(&log, &environment);
    drain(&mut first);
    let a_ready = first.cache.pointer(&a.encode()).unwrap().ready.unwrap();
    // B's records arrive and an earlier run staged B, then stopped before
    // publishing it (B was never READY).
    for minute in 0..40u64 {
        push(
            &log,
            BARS,
            bar_frame(&b, bar_of_uid(uid_b, minute), 100 + minute),
        );
    }
    let fence = first.cache.take_ownership(BARS, 0).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let pointer = first.cache.pointer(&b.encode()).unwrap();
    let generation = first.cache.allocate_generation().unwrap();
    assert!(matches!(
        first
            .cache
            .apply(
                BARS,
                0,
                fence,
                50,
                now,
                &[Op::Stage {
                    lpk: b.encode(),
                    pointer,
                    generation,
                }],
            )
            .unwrap(),
        Applied::Ok(_)
    ));
    drop(first);

    // Recovery: B is an obligation; its rebuild fails before the publish.
    let mut stage = rebuild_stage(&log, &environment);
    stage.cache.faults.lock().unwrap().fail_before_publish = 1;
    let mut errors = 0;
    for _ in 0..300 {
        if stage.step().is_err() {
            errors += 1;
        }
    }
    // No new record and no restart: B is READY with all its data.
    let b_pointer = stage.cache.pointer(&b.encode()).unwrap();
    let b_ready = b_pointer.ready.expect("B READY");
    assert_eq!(errors, 1);
    assert_eq!(stage.metrics.rebuilds_abandoned, 1, "B's staging unstaged");
    assert_eq!(
        stage.metrics.rebuilds_refused, 0,
        "the retry is not refused"
    );
    assert_eq!(
        (
            stage.metrics.rebuilds_started,
            stage.metrics.rebuilds_completed
        ),
        (2, 1)
    );
    assert_eq!(b_pointer.staging, None);
    assert_eq!(bar_rows(&mut stage, &b), 40);
    for minute in 0..40u64 {
        assert!(
            read_bar(&mut stage, &b, minute).is_some(),
            "B open {minute}"
        );
    }
    let (rows, counted) = stage
        .cache
        .bar_row_count(b_ready, &b.encode(), MIN)
        .unwrap();
    assert_eq!(rows, counted);
    let live: BTreeSet<u64> = [b_ready].into_iter().collect();
    assert_eq!(key_generations(&mut stage, &b), live);
    // A untouched; checkpoint at the log end; bookkeeping clean.
    assert_eq!(
        stage.cache.pointer(&a.encode()).unwrap().ready,
        Some(a_ready)
    );
    assert_eq!(bar_rows(&mut stage, &a), 50);
    assert_eq!(stage.cache.checkpoint(BARS, 0).unwrap().unwrap().next, 90);
    for set in [
        stage.cache.layout.retire(),
        stage.cache.layout.rebuild_requests(),
    ] {
        let count: u64 = redis::cmd("SCARD")
            .arg(&set)
            .query(stage.cache.connection())
            .unwrap();
        assert_eq!(count, 0, "{set}");
    }
}

// ------------------------------------------------------------ KN-4 D27

fn watermark(stage: &mut StageB<Source>, topic: &str, canonical_partition: u32) -> Option<u64> {
    stage
        .cache
        .source_watermark(topic, 0, TOPIC_ID, canonical_partition)
        .unwrap()
}

fn bar_frame_on(
    lpk: &LogicalProductKey,
    envelope: Vec<u8>,
    canonical_partition: u32,
    offset: u64,
) -> (String, Vec<u8>) {
    let source = SourceCoordinate {
        topic_id: TOPIC_ID.into(),
        partition: canonical_partition,
        offset,
    };
    let frame = StateFrame::bar_revision(&envelope, lpk, source, 1).unwrap();
    (frame.key().unwrap(), frame.encode().unwrap())
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn k4_the_live_batch_raises_source_watermarks_and_records_bar_product_sources() {
    let uid_b = "fb26214c-7b9b-5961-95b2-55154755afbb";
    let (a, b) = (bars_of_uid(UID), bars_of_uid(uid_b));
    let quotes = lpk("QUOTE", None);
    let log = Log::default();
    // A from canonical partition 2, B from canonical partition 4, both in
    // state partition 0; a floor frame carries no source coordinate.
    for minute in 0..10u64 {
        push(
            &log,
            BARS,
            bar_frame(&a, bar_of_uid(UID, minute), 100 + minute),
        );
    }
    for minute in 0..5u64 {
        push(
            &log,
            BARS,
            bar_frame_on(&b, bar_of_uid(uid_b, minute), 4, 70 + minute),
        );
    }
    push(&log, BARS, floor_frame(&a, 2 * MIN));
    push(&log, LATEST, latest_frame(&quotes, quote(1), 30));
    push(&log, LATEST, latest_frame(&quotes, quote(2), 31));
    let environment = environment("k4src");
    let mut stage = rebuild_stage(&log, &environment);
    drain(&mut stage);
    assert_eq!(watermark(&mut stage, BARS, 2), Some(109));
    assert_eq!(watermark(&mut stage, BARS, 4), Some(74));
    assert_eq!(watermark(&mut stage, LATEST, 2), Some(31));
    assert_eq!(
        watermark(&mut stage, LATEST, 4),
        None,
        "no fact of p4 in the latest partition"
    );
    assert_eq!(
        stage.cache.product_source(&a.encode()).unwrap(),
        Some((TOPIC_ID.to_owned(), 2))
    );
    assert_eq!(
        stage.cache.product_source(&b.encode()).unwrap(),
        Some((TOPIC_ID.to_owned(), 4))
    );
    assert_eq!(
        stage.cache.product_source(&quotes.encode()).unwrap(),
        None,
        "latest has l:t/p"
    );
    // The watermark bounds every applied fact: A's rows carry offsets <= it.
    let a_ready = stage.cache.pointer(&a.encode()).unwrap().ready.unwrap();
    for minute in 2..10u64 {
        let key = stage
            .cache
            .layout
            .bar_bucket(a_ready, &a.encode(), bucket_of(minute * MIN, MIN));
        let row: Vec<u8> = redis::cmd("HGET")
            .arg(key)
            .arg((minute * MIN).to_string())
            .query(stage.cache.connection())
            .unwrap();
        let offset = u64::from_be_bytes(row[..8].try_into().unwrap());
        assert!(offset <= 109);
    }

    // A product rebuild replays below the live checkpoint: no watermark move.
    stage.request_rebuild(&a.encode());
    until_rebuilt(&mut stage);
    assert_eq!(stage.metrics.rebuilds_completed, 1);
    assert_eq!(watermark(&mut stage, BARS, 2), Some(109));

    // A zombie's batch moves nothing; the owner's next batch raises it.
    let mut old = stage;
    let mut new = rebuild_stage(&log, &environment);
    drain(&mut new);
    push(&log, BARS, bar_frame(&a, bar_of_uid(UID, 10), 150));
    drain(&mut old);
    assert!(old.metrics.zombies >= 1, "the old owner is fenced");
    let zombie_mark = watermark(&mut old, BARS, 2);
    assert_eq!(zombie_mark, Some(109), "a fenced batch applies nothing");
    drain(&mut new);
    assert_eq!(watermark(&mut new, BARS, 2), Some(150));
    // Raise only: a lower watermark in a later script never lowers it.
    let fence = new.cache.checkpoint(BARS, 0).unwrap().unwrap().fence;
    let next = new.cache.checkpoint(BARS, 0).unwrap().unwrap().next;
    assert!(matches!(
        new.cache
            .apply(
                BARS,
                0,
                fence,
                next,
                0,
                &[Op::SourceWatermark {
                    topic_id: TOPIC_ID.into(),
                    partition: 2,
                    offset: 5,
                }],
            )
            .unwrap(),
        Applied::Ok(_)
    ));
    assert_eq!(watermark(&mut new, BARS, 2), Some(150));
    assert_eq!(
        watermark(&mut new, BARS, 4),
        Some(74),
        "other canonical partition kept"
    );
}
