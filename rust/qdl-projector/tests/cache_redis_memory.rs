//! K3-T06 memory-full isolation against a **dedicated** isolated Redis: the
//! test lowers `maxmemory` server-wide, so it must not share its Redis with
//! any other test.
//!
//! `QDL_KN_TEST_REDIS_EXCLUSIVE=redis://host:port cargo test -p qdl-projector --test cache_redis_memory -- --ignored`.

use qdl_projector::cache::{bucket_of, Applied, Cache, CacheError, Layout, Op};

const LPK: &str = "lpk1|paper|OKX|SWAP|u|BAR|1m";
const MIN: u64 = 60_000;

fn row(offset: u64) -> Vec<u8> {
    let mut row = offset.to_be_bytes().to_vec();
    row.extend_from_slice(&1u64.to_be_bytes());
    row.extend_from_slice(&[1; 32]);
    row.extend_from_slice(b"body");
    row
}

#[test]
#[ignore = "requires QDL_KN_TEST_REDIS_EXCLUSIVE (a dedicated isolated Redis)"]
fn memory_pressure_is_typed_and_applies_nothing() {
    let url = std::env::var("QDL_KN_TEST_REDIS_EXCLUSIVE")
        .expect("QDL_KN_TEST_REDIS_EXCLUSIVE must name a dedicated isolated Redis");
    let mut cache = Cache::connect(&url, Layout::new("oom")).unwrap();
    let topic = "md.bars.v2";
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
    let pointer = cache.pointer(LPK).unwrap();
    let used: String = redis::cmd("INFO")
        .arg("memory")
        .query(cache.connection())
        .unwrap();
    let used: u64 = used
        .lines()
        .find_map(|line| line.strip_prefix("used_memory:"))
        .and_then(|value| value.trim().parse().ok())
        .unwrap();
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg("noeviction")
        .query(cache.connection())
        .unwrap();
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg(used.to_string())
        .query(cache.connection())
        .unwrap();
    // Push memory above the limit, then a write batch must be refused whole.
    let _: Result<(), _> = redis::cmd("SET")
        .arg("kn3-ballast")
        .arg(vec![7u8; 1 << 20])
        .query::<()>(cache.connection());
    let outcome = cache.apply(
        topic,
        0,
        fence,
        50,
        3,
        &[Op::Bar {
            lpk: LPK.into(),
            generation,
            pointer: pointer.clone(),
            bucket: bucket_of(MIN, MIN),
            open_ms: MIN,
            expected_trailer: None,
            row: row(1),
            is_final: true,
            superseded: None,
        }],
    );
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("0")
        .query(cache.connection())
        .unwrap();
    let _: () = redis::cmd("DEL")
        .arg("kn3-ballast")
        .query(cache.connection())
        .unwrap();
    assert!(
        matches!(outcome, Err(CacheError::MemoryPressure(_))),
        "{outcome:?}"
    );
    assert!(cache
        .bar_row(generation, LPK, bucket_of(MIN, MIN), MIN)
        .unwrap()
        .is_none());
    assert_eq!(
        cache.checkpoint(topic, 0).unwrap().unwrap().next,
        1,
        "checkpoint stays behind"
    );
}
