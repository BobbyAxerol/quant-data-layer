//! K3-T02 (sink part) and K3-T06 against a real, isolated Redis of the
//! market-cache image (never the control/quota Redis).
//!
//! `QDL_KN_TEST_REDIS=redis://host:port cargo test -p qdl-projector --test cache_redis -- --ignored`;
//! without the variable the tests fail loudly instead of skipping.

use qdl_projector::cache::{bucket_of, Applied, Cache, Layout, Op, Pointer};

fn cache(environment: &str) -> Cache {
    let url = std::env::var("QDL_KN_TEST_REDIS")
        .expect("QDL_KN_TEST_REDIS must name an isolated test Redis; never the control Redis");
    Cache::connect(&url, Layout::new(environment)).expect("connect")
}

fn unique(label: &str) -> String {
    format!(
        "{label}{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

const LPK: &str = "lpk1|paper|OKX|SWAP|u|BAR|1m";
const MIN: u64 = 60_000;

fn trailer(offset: u64, fill: u8) -> Vec<u8> {
    let mut row = offset.to_be_bytes().to_vec();
    row.extend_from_slice(&1u64.to_be_bytes());
    row.extend_from_slice(&[fill; 32]);
    row
}

fn row(offset: u64, fill: u8) -> Vec<u8> {
    let mut row = trailer(offset, fill);
    row.extend_from_slice(b"body");
    row
}

fn get(cache: &mut Cache, key: &str, field: &str) -> Option<String> {
    redis::cmd("HGET")
        .arg(key)
        .arg(field)
        .query(cache.connection())
        .unwrap()
}

/// A ready product in generation `g` owned at `fence`.
fn ready_product(cache: &mut Cache, topic: &str) -> (u64, u64, Pointer) {
    let fence = cache.take_ownership(topic, 0).unwrap();
    let generation = cache.allocate_generation().unwrap();
    let empty = cache.pointer(LPK).unwrap();
    let stage = Op::Stage {
        lpk: LPK.into(),
        pointer: empty,
        generation,
    };
    assert!(matches!(
        cache.apply(topic, 0, fence, 1, 1, &[stage]).unwrap(),
        Applied::Ok(_)
    ));
    let staged = cache.pointer(LPK).unwrap();
    let publish = Op::Publish {
        lpk: LPK.into(),
        pointer: staged,
    };
    assert!(matches!(
        cache.apply(topic, 0, fence, 2, 2, &[publish]).unwrap(),
        Applied::Ok(_)
    ));
    let pointer = cache.pointer(LPK).unwrap();
    assert_eq!(pointer.ready, Some(generation));
    assert_eq!(pointer.staging, None);
    assert_eq!(pointer.fence, 1);
    (fence, generation, pointer)
}

fn bar(
    generation: u64,
    pointer: &Pointer,
    open_ms: u64,
    expected: Option<Vec<u8>>,
    row: Vec<u8>,
) -> Op {
    Op::Bar {
        lpk: LPK.into(),
        generation,
        pointer: pointer.clone(),
        bucket: bucket_of(open_ms, MIN),
        open_ms,
        expected_trailer: expected,
        row,
        diagnostic: "N".into(),
        is_final: true,
        superseded: None,
    }
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_zombie_owner_applies_nothing() {
    let mut cache = cache(&unique("zombie"));
    let topic = "md.bars.v2";
    let (old_fence, generation, pointer) = ready_product(&mut cache, topic);
    let new_fence = cache.take_ownership(topic, 0).unwrap();
    assert!(new_fence > old_fence);
    let op = bar(generation, &pointer, 10 * MIN, None, row(10, 1));
    assert_eq!(
        cache
            .apply(topic, 0, old_fence, 99, 3, &[op.clone()])
            .unwrap(),
        Applied::Zombie {
            current_owner: new_fence.to_string()
        }
    );
    assert_eq!(
        cache.checkpoint(topic, 0).unwrap().unwrap().next,
        2,
        "checkpoint unchanged"
    );
    assert!(cache
        .bar_row(generation, LPK, bucket_of(10 * MIN, MIN), 10 * MIN)
        .unwrap()
        .is_none());
    assert!(matches!(
        cache.apply(topic, 0, new_fence, 11, 4, &[op]).unwrap(),
        Applied::Ok(_)
    ));
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_batch_with_one_stale_expectation_applies_nothing() {
    let mut cache = cache(&unique("cas"));
    let topic = "md.bars.v2";
    let (fence, generation, pointer) = ready_product(&mut cache, topic);
    let first = bar(generation, &pointer, MIN, None, row(5, 1));
    assert!(matches!(
        cache.apply(topic, 0, fence, 6, 3, &[first]).unwrap(),
        Applied::Ok(_)
    ));
    // Second batch: a new open plus a replacement that expects a trailer
    // which is no longer there (someone else replaced the row).
    let fresh = bar(generation, &pointer, 2 * MIN, None, row(6, 2));
    let stale = bar(generation, &pointer, MIN, Some(trailer(4, 9)), row(7, 3));
    assert_eq!(
        cache.apply(topic, 0, fence, 8, 4, &[fresh, stale]).unwrap(),
        Applied::Miss(vec![1])
    );
    assert!(
        cache
            .bar_row(generation, LPK, bucket_of(2 * MIN, MIN), 2 * MIN)
            .unwrap()
            .is_none(),
        "all or nothing"
    );
    assert_eq!(cache.checkpoint(topic, 0).unwrap().unwrap().next, 6);
    // With the right expectation the replacement applies and rows stay 1.
    let replace = bar(generation, &pointer, MIN, Some(trailer(5, 1)), row(7, 3));
    assert!(matches!(
        cache.apply(topic, 0, fence, 8, 5, &[replace]).unwrap(),
        Applied::Ok(_)
    ));
    let meta = cache.layout.bar_meta(generation, LPK);
    assert_eq!(get(&mut cache, &meta, "rows").as_deref(), Some("1"));
    // A pointer change since the pre-read is a miss too.
    let moved = Pointer {
        fence: pointer.fence + 5,
        ..pointer.clone()
    };
    assert_eq!(
        cache
            .apply(
                topic,
                0,
                fence,
                9,
                6,
                &[bar(generation, &moved, 3 * MIN, None, row(8, 1))]
            )
            .unwrap(),
        Applied::Miss(vec![0])
    );
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn offsets_beyond_two_to_the_53_stay_exact() {
    let mut cache = cache(&unique("precision"));
    let topic = "md.latest.v2";
    let fence = cache.take_ownership(topic, 3).unwrap();
    let generation = cache.allocate_generation().unwrap();
    let lpk = "lpk1|paper|OKX|SWAP|u|QUOTE|-";
    let empty = cache.pointer(lpk).unwrap();
    cache
        .apply(
            topic,
            3,
            fence,
            1,
            1,
            &[Op::Stage {
                lpk: lpk.into(),
                pointer: empty,
                generation,
            }],
        )
        .unwrap();
    let pointer = cache.pointer(lpk).unwrap();
    let big = (1u64 << 62) + 1; // not representable as a double
    let latest = |expected: Option<u64>, offset: u64| Op::Latest {
        lpk: lpk.into(),
        generation,
        pointer: pointer.clone(),
        expected_offset: expected,
        value: vec![0xff, 0x00, 0x10],
        topic_id: "ljfjPYApRpWQd79McfTtZg".into(),
        partition: 3,
        offset,
    };
    assert!(matches!(
        cache
            .apply(topic, 3, fence, (big as i64) + 1, 2, &[latest(None, big)])
            .unwrap(),
        Applied::Ok(_)
    ));
    assert_eq!(cache.latest_offset(generation, lpk).unwrap(), Some(big));
    // big - 1 and big + 1 differ from big only below double precision.
    assert_eq!(
        cache
            .apply(topic, 3, fence, 0, 3, &[latest(Some(big - 1), big + 1)])
            .unwrap(),
        Applied::Miss(vec![0])
    );
    assert!(matches!(
        cache
            .apply(
                topic,
                3,
                fence,
                (big as i64) + 2,
                3,
                &[latest(Some(big), big + 1)]
            )
            .unwrap(),
        Applied::Ok(_)
    ));
    assert_eq!(cache.latest_offset(generation, lpk).unwrap(), Some(big + 1));
    let value: Vec<u8> = redis::cmd("HGET")
        .arg(cache.layout.latest(generation, lpk))
        .arg("v")
        .query(cache.connection())
        .unwrap();
    assert_eq!(value, vec![0xff, 0x00, 0x10], "binary-safe value");
    assert_eq!(
        cache.checkpoint(topic, 3).unwrap().unwrap().next,
        (big as i64) + 2
    );
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn a_floor_removes_rows_below_it_and_refuses_later_ones() {
    let mut cache = cache(&unique("floor"));
    let topic = "md.bars.v2";
    let (fence, generation, pointer) = ready_product(&mut cache, topic);
    // 300 opens across three buckets.
    let ops: Vec<Op> = (0..300u64)
        .map(|index| bar(generation, &pointer, index * MIN, None, row(index, 1)))
        .collect();
    assert!(matches!(
        cache.apply(topic, 0, fence, 300, 3, &ops).unwrap(),
        Applied::Ok(_)
    ));
    let meta = cache.layout.bar_meta(generation, LPK);
    assert_eq!(get(&mut cache, &meta, "rows").as_deref(), Some("300"));
    let floor_ms = 250 * MIN;
    let floor = Op::Floor {
        lpk: LPK.into(),
        generation,
        pointer: pointer.clone(),
        floor_ms,
        buckets: vec![0, 1],
        boundary: Some(bucket_of(floor_ms, MIN)),
    };
    let Applied::Ok(results) = cache
        .apply(topic, 0, fence, 301, 4, &[floor.clone()])
        .unwrap()
    else {
        panic!("floor not applied");
    };
    assert_eq!(results, vec!["FLOOR 250"]);
    assert_eq!(get(&mut cache, &meta, "rows").as_deref(), Some("50"));
    assert_eq!(get(&mut cache, &meta, "floor"), Some(floor_ms.to_string()));
    assert!(cache
        .bar_row(generation, LPK, bucket_of(249 * MIN, MIN), 249 * MIN)
        .unwrap()
        .is_none());
    assert!(cache
        .bar_row(generation, LPK, bucket_of(250 * MIN, MIN), 250 * MIN)
        .unwrap()
        .is_some());
    // A late fact below the floor is refused; a floor never goes down.
    let Applied::Ok(results) = cache
        .apply(
            topic,
            0,
            fence,
            302,
            5,
            &[bar(generation, &pointer, 10 * MIN, None, row(9, 1))],
        )
        .unwrap()
    else {
        panic!("batch refused");
    };
    assert_eq!(results, vec!["STALE_BELOW_FLOOR"]);
    let Applied::Ok(results) = cache.apply(topic, 0, fence, 303, 6, &[floor]).unwrap() else {
        panic!("batch refused");
    };
    assert_eq!(results, vec!["FLOOR_NOT_RAISED"]);
    assert_eq!(get(&mut cache, &meta, "rows").as_deref(), Some("50"));
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis); run by the kn-native-integration job"]
fn staging_publish_and_reclaim_swap_one_product() {
    let mut cache = cache(&unique("swap"));
    let topic = "md.bars.v2";
    let (fence, old, pointer) = ready_product(&mut cache, topic);
    let ops: Vec<Op> = (0..200u64)
        .map(|index| bar(old, &pointer, index * MIN, None, row(index, 1)))
        .collect();
    assert!(matches!(
        cache.apply(topic, 0, fence, 200, 3, &ops).unwrap(),
        Applied::Ok(_)
    ));
    // Rebuild into a new generation while the old one stays ready.
    let new = cache.allocate_generation().unwrap();
    cache
        .apply(
            topic,
            0,
            fence,
            200,
            4,
            &[Op::Stage {
                lpk: LPK.into(),
                pointer: pointer.clone(),
                generation: new,
            }],
        )
        .unwrap();
    let staging = cache.pointer(LPK).unwrap();
    assert_eq!(
        staging.targets(),
        vec![old],
        "live writes reach the ready generation only (D17)"
    );
    // Writing a generation that is neither ready nor staging is refused.
    assert_eq!(
        cache
            .apply(
                topic,
                0,
                fence,
                201,
                5,
                &[bar(new + 100, &staging, MIN, None, row(1, 1))]
            )
            .unwrap(),
        Applied::Miss(vec![0])
    );
    let copy: Vec<Op> = (0..200u64)
        .map(|index| bar(new, &staging, index * MIN, None, row(index, 1)))
        .collect();
    assert!(matches!(
        cache.apply(topic, 0, fence, 201, 6, &copy).unwrap(),
        Applied::Ok(_)
    ));
    let Applied::Ok(results) = cache
        .apply(
            topic,
            0,
            fence,
            201,
            7,
            &[Op::Publish {
                lpk: LPK.into(),
                pointer: staging.clone(),
            }],
        )
        .unwrap()
    else {
        panic!("publish refused");
    };
    assert_eq!(
        results,
        vec![old.to_string()],
        "the superseded generation is returned"
    );
    let published = cache.pointer(LPK).unwrap();
    assert_eq!(
        (published.ready, published.staging, published.fence),
        (Some(new), None, staging.fence + 1)
    );
    // A writer still holding the old pointer is now refused.
    assert_eq!(
        cache
            .apply(
                topic,
                0,
                fence,
                202,
                8,
                &[bar(old, &staging, 500 * MIN, None, row(2, 1))]
            )
            .unwrap(),
        Applied::Miss(vec![0])
    );
    let removed = cache.reclaim(old, LPK, Some(MIN)).unwrap();
    assert!(removed >= 2, "buckets and meta removed ({removed})");
    let left: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{}*:{old}:*", cache.layout.prefix()))
        .query(cache.connection())
        .unwrap();
    assert!(left.is_empty(), "old generation fully reclaimed: {left:?}");
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS (isolated Redis)"]
fn bar_diagnostic_is_atomic_with_revision_floor_and_reclaim() {
    let mut cache = cache(&unique("diagnostic"));
    let topic = "md.bars.v2";
    let (fence, generation, pointer) = ready_product(&mut cache, topic);
    let mut first = bar(generation, &pointer, 10 * MIN, None, row(10, 1));
    if let Op::Bar { diagnostic, .. } = &mut first {
        *diagnostic = "Gseq-7".into();
    }
    assert!(matches!(
        cache.apply(topic, 0, fence, 11, 3, &[first]).unwrap(),
        Applied::Ok(_)
    ));
    let key = cache
        .layout
        .bar_diagnostic(generation, LPK, bucket_of(10 * MIN, MIN));
    assert_eq!(
        get(&mut cache, &key, &(10 * MIN).to_string()),
        Some("Gseq-7".into())
    );
    let revision = bar(
        generation,
        &pointer,
        10 * MIN,
        Some(trailer(10, 1)),
        row(11, 2),
    );
    assert!(matches!(
        cache.apply(topic, 0, fence, 12, 4, &[revision]).unwrap(),
        Applied::Ok(_)
    ));
    assert_eq!(
        get(&mut cache, &key, &(10 * MIN).to_string()),
        Some("N".into())
    );
    let failed = bar(
        generation,
        &pointer,
        10 * MIN,
        Some(trailer(10, 1)),
        row(12, 3),
    );
    assert!(!matches!(
        cache.apply(topic, 0, fence, 13, 5, &[failed]).unwrap(),
        Applied::Ok(_)
    ));
    assert_eq!(
        get(&mut cache, &key, &(10 * MIN).to_string()),
        Some("N".into())
    );
    let floor = Op::Floor {
        lpk: LPK.into(),
        generation,
        pointer: pointer.clone(),
        floor_ms: 11 * MIN,
        buckets: vec![],
        boundary: Some(0),
    };
    assert!(matches!(
        cache.apply(topic, 0, fence, 14, 6, &[floor]).unwrap(),
        Applied::Ok(_)
    ));
    assert_eq!(get(&mut cache, &key, &(10 * MIN).to_string()), None);
    let new = bar(generation, &pointer, 12 * MIN, None, row(15, 4));
    assert!(matches!(
        cache.apply(topic, 0, fence, 16, 7, &[new]).unwrap(),
        Applied::Ok(_)
    ));
    cache.reclaim(generation, LPK, Some(MIN)).unwrap();
    assert_eq!(get(&mut cache, &key, &(12 * MIN).to_string()), None);
}
