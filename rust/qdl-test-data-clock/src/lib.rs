//! Shared freshness-only TEST clock. No security, metadata or deadline clock changes.
//! The frozen namespace.stream consumer ID names the shared schedule, not a role credential.

pub const INVALID: &str = "TEST_DATA_CLOCK_INVALID";
const ENV: [&str; 3] = [
    "QDL_TEST_DATA_CLOCK_NAMESPACE",
    "QDL_TEST_DATA_CLOCK_FILE",
    "QDL_TEST_DATA_CLOCK_SHA256",
];

pub enum DataClock {
    Real,
    #[cfg(feature = "isolated-replay-clock")]
    Replay(replay::ReplayClock),
}

impl DataClock {
    /// The caller must pass its own cfg!(feature = "isolated-replay-clock").
    /// Cargo may unify this dependency's features with another replay-enabled binary.
    pub fn from_env(caller_enabled: bool) -> Result<Self, &'static str> {
        Self::configured_for(caller_enabled, ENV.map(std::env::var_os))
    }

    fn configured_for(
        caller_enabled: bool,
        values: [Option<std::ffi::OsString>; 3],
    ) -> Result<Self, &'static str> {
        if !caller_enabled && values.iter().any(Option::is_some) {
            return Err(INVALID);
        }
        Self::configured(values)
    }

    fn configured(values: [Option<std::ffi::OsString>; 3]) -> Result<Self, &'static str> {
        if values.iter().all(Option::is_none) {
            return Ok(Self::Real);
        }
        #[cfg(not(feature = "isolated-replay-clock"))]
        {
            Err(INVALID)
        }
        #[cfg(feature = "isolated-replay-clock")]
        {
            let [namespace, file, hash] = values;
            let text = |value: Option<std::ffi::OsString>| {
                value
                    .and_then(|v| v.into_string().ok())
                    .filter(|v| !v.is_empty())
                    .ok_or(INVALID)
            };
            let namespace = text(namespace)?;
            let config = replay::ReplayClockConfig {
                timeline_file: text(file)?.into(),
                timeline_sha256: text(hash)?,
                namespace,
            };
            replay::ReplayClock::load(&config, &format!("{}.stream", config.namespace))
                .map(Self::Replay)
        }
    }

    #[cfg(feature = "isolated-replay-clock")]
    pub fn is_replay(&self) -> bool {
        matches!(self, Self::Replay(_))
    }

    #[cfg(all(feature = "isolated-replay-clock", any(test, feature = "test-support")))]
    #[doc(hidden)]
    pub fn test_fixture() -> (Self, impl Fn(bool)) {
        let (clock, set_expired) = replay::test_fixture();
        (Self::Replay(clock), set_expired)
    }

    #[cfg(all(feature = "isolated-replay-clock", any(test, feature = "test-support")))]
    #[doc(hidden)]
    pub fn test_fixture_at(source_ns: i64) -> (Self, impl Fn(bool)) {
        let (clock, set_expired) = replay::test_fixture_at(source_ns);
        (Self::Replay(clock), set_expired)
    }

    pub fn now_ns(&self, real_ns: i64) -> Result<i64, &'static str> {
        match self {
            Self::Real => Ok(real_ns),
            #[cfg(feature = "isolated-replay-clock")]
            Self::Replay(clock) => clock.now_ns(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_environment_preserves_original_wall_time() {
        let clock = DataClock::configured([None, None, None]).unwrap();
        assert_eq!(clock.now_ns(123456789), Ok(123456789));
    }

    #[test]
    fn disabled_caller_rejects_configuration_even_with_unified_dependency_features() {
        let values = [
            Some("p184-g12-dl-0123456789abcdef".into()),
            Some("/work/TEST_ONLY/p184-g12-dl-0123456789abcdef/clock.json".into()),
            Some("0".repeat(64).into()),
        ];
        assert!(matches!(
            DataClock::configured_for(false, values),
            Err(INVALID)
        ));
        assert_eq!(
            DataClock::configured_for(false, [None, None, None])
                .unwrap()
                .now_ns(42),
            Ok(42)
        );
    }

    #[test]
    fn partial_or_empty_environment_never_falls_back() {
        for i in 0..3 {
            let mut env = [None, None, None];
            env[i] = Some("".into());
            assert!(DataClock::configured(env).is_err());
        }
    }

    #[cfg(not(feature = "isolated-replay-clock"))]
    #[test]
    fn disabled_feature_rejects_even_complete_configuration() {
        assert!(DataClock::configured([
            Some("p184-g12-dl-0123456789abcdef".into()),
            Some("/work/TEST_ONLY/p184-g12-dl-0123456789abcdef/clock.json".into()),
            Some("0".repeat(64).into()),
        ])
        .is_err());
    }
}

#[cfg(feature = "isolated-replay-clock")]
mod replay {
    //! TEST-only frozen data time, never a replacement for security, TTL or pacing clocks.
    //!
    //! Shared DL schema: p184.test-data-clock.v1, namespace, consumer_id,
    //! expires_unix_ns, points: [{monotonic_ns, source_ns}]. Frozen offered schedule,
    //! independent of consumer progress. Inclusive endpoints; integer interpolation
    //! s0 + floor((m-m0)*(s1-s0)/(m1-m0)). No extrapolation; failures latch.
    use ring::digest::{digest, SHA256};
    use serde::Deserialize;
    use std::{
        fs::OpenOptions,
        io::Read,
        os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
        path::{Component, Path, PathBuf},
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    pub const SCHEMA: &str = "p184.test-data-clock.v1";
    pub const MAX_BYTES: usize = 1024 * 1024;
    pub const MAX_POINTS: usize = 16384;
    pub const MAX_WINDOW_NS: u64 = 3600 * 1_000_000_000;

    #[derive(Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct ReplayClockConfig {
        pub namespace: String,
        pub timeline_file: PathBuf,
        pub timeline_sha256: String,
    }
    fn namespace_valid(ns: &str) -> bool {
        ns.strip_prefix("p184-g12-dl-").is_some_and(|s| {
            s.len() == 16
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
    }
    fn consumer_valid(consumer: &str, ns: &str) -> bool {
        consumer == format!("{ns}.stream")
    }
    fn scoped_path(path: &Path, ns: &str) -> bool {
        path.is_absolute()
            && path
                .components()
                .all(|c| matches!(c, Component::RootDir | Component::Normal(_)))
            && path.starts_with(Path::new("/work/TEST_ONLY").join(ns))
            && path.extension().and_then(|v| v.to_str()) == Some("json")
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Point {
        monotonic_ns: u64,
        source_ns: i64,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Timeline {
        schema: String,
        namespace: String,
        consumer_id: String,
        expires_unix_ns: u64,
        points: Vec<Point>,
    }
    fn error() -> &'static str {
        super::INVALID
    }
    /// A loopback URL alone could target operational services in a shared namespace.
    /// TEST replay requires the holder's network-none namespace, not just its name.
    pub(super) fn ensure_loopback_namespace() -> Result<(), &'static str> {
        let mut interfaces = std::fs::read_dir("/sys/class/net")
            .map_err(|_| error())?
            .map(|entry| entry.map(|v| v.file_name()).map_err(|_| error()))
            .collect::<Result<Vec<_>, _>>()?;
        interfaces.sort();
        if interfaces != [std::ffi::OsString::from("lo")] {
            return Err(error());
        }
        Ok(())
    }
    /// Injection for deterministic tests; production uses CLOCK_MONOTONIC and real wall time.
    trait ClockPair: Send + Sync {
        fn sample(&self) -> Result<(u64, u64), &'static str>;
    }
    struct SystemClocks;
    impl ClockPair for SystemClocks {
        fn sample(&self) -> Result<(u64, u64), &'static str> {
            let wall = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| error())?
                    .as_nanos(),
            )
            .map_err(|_| error())?;
            Ok((monotonic_ns()?, wall))
        }
    }
    #[derive(Default)]
    struct State {
        previous: Option<(u64, u64)>,
        failed: bool,
    }
    pub struct ReplayClock {
        timeline: Timeline,
        clocks: Arc<dyn ClockPair>,
        state: Mutex<State>,
    }
    impl ReplayClock {
        pub(super) fn load(
            config: &ReplayClockConfig,
            consumer: &str,
        ) -> Result<Self, &'static str> {
            ensure_loopback_namespace()?;
            if !namespace_valid(&config.namespace)
                || !consumer_valid(consumer, &config.namespace)
                || !scoped_path(&config.timeline_file, &config.namespace)
                || config.timeline_sha256.len() != 64
                || !config
                    .timeline_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(error());
            }
            // Read once, bounded, rejecting symlink escapes and special files. Pin exact bytes.
            let canonical = config.timeline_file.canonicalize().map_err(|_| error())?;
            if canonical != config.timeline_file {
                return Err(error());
            }
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(&canonical)
                .map_err(|_| error())?;
            let opened = std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))
                .map_err(|_| error())?;
            if opened != canonical {
                return Err(error());
            }
            let metadata = file.metadata().map_err(|_| error())?;
            if !metadata.is_file() || metadata.len() > MAX_BYTES as u64 {
                return Err(error());
            }
            let mut bytes = Vec::new();
            file.take((MAX_BYTES + 1) as u64)
                .read_to_end(&mut bytes)
                .map_err(|_| error())?;
            if sha256(&bytes) != config.timeline_sha256 {
                return Err(error());
            }
            Self::from_bytes(&bytes, &config.namespace, consumer, Arc::new(SystemClocks))
        }
        fn from_bytes(
            bytes: &[u8],
            namespace: &str,
            consumer: &str,
            clocks: Arc<dyn ClockPair>,
        ) -> Result<Self, &'static str> {
            if bytes.is_empty()
                || bytes.len() > MAX_BYTES
                || !namespace_valid(namespace)
                || !consumer_valid(consumer, namespace)
            {
                return Err(error());
            }
            // Typed structs reject duplicate and unknown fields at every object level.
            let t: Timeline = serde_json::from_slice(bytes).map_err(|_| error())?;
            let (mono, wall) = clocks.sample()?;
            if mono == 0
                || wall == 0
                || t.schema != SCHEMA
                || t.namespace != namespace
                || t.consumer_id != consumer
                || !(2..=MAX_POINTS).contains(&t.points.len())
                || t.expires_unix_ns <= wall
                || t.expires_unix_ns - wall > MAX_WINDOW_NS
                || t.points
                    .iter()
                    .any(|p| p.monotonic_ns == 0 || p.source_ns <= 0 || p.source_ns as u64 > wall)
                || t.points.windows(2).any(|p| {
                    p[1].monotonic_ns <= p[0].monotonic_ns || p[1].source_ns < p[0].source_ns
                })
            {
                return Err(error());
            }
            if t.points.last().unwrap().monotonic_ns - t.points[0].monotonic_ns > MAX_WINDOW_NS {
                return Err(error());
            }
            let result = Self {
                timeline: t,
                clocks,
                state: Mutex::new(State {
                    previous: Some((mono, wall)),
                    failed: false,
                }),
            };
            result.at(mono, wall)?;
            Ok(result)
        }
        fn at(&self, mono: u64, wall: u64) -> Result<i64, &'static str> {
            let points = &self.timeline.points;
            if wall >= self.timeline.expires_unix_ns
                || mono < points[0].monotonic_ns
                || mono > points.last().unwrap().monotonic_ns
            {
                return Err(error());
            }
            let i = points.partition_point(|p| p.monotonic_ns <= mono) - 1;
            let a = &points[i];
            let source = if let Some(b) = points.get(i + 1) {
                a.source_ns as u128
                    + (mono - a.monotonic_ns) as u128 * (b.source_ns - a.source_ns) as u128
                        / (b.monotonic_ns - a.monotonic_ns) as u128
            } else {
                a.source_ns as u128
            };
            if source > wall as u128 {
                return Err(error());
            }
            i64::try_from(source).map_err(|_| error())
        }
    }
    impl ReplayClock {
        pub(super) fn now_ns(&self) -> Result<i64, &'static str> {
            // Sample under lock: concurrent slices cannot spuriously report clock rollback.
            let mut state = self.state.lock().map_err(|_| error())?;
            if state.failed {
                return Err(error());
            }
            let result = (|| {
                let (mono, wall) = self.clocks.sample()?;
                if mono == 0 || wall == 0 {
                    return Err(error());
                }
                if state.previous.is_some_and(|(m, w)| mono < m || wall < w) {
                    return Err(error());
                }
                state.previous = Some((mono, wall));
                self.at(mono, wall)
            })();
            if result.is_err() {
                state.failed = true;
            }
            result
        }
    }

    fn sha256(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        let mut encoded = String::with_capacity(64);
        for byte in digest(&SHA256, bytes).as_ref() {
            write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
        }
        encoded
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn test_fixture() -> (ReplayClock, impl Fn(bool)) {
        test_fixture_at(1000)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn test_fixture_at(source_ns: i64) -> (ReplayClock, impl Fn(bool)) {
        struct Samples(Arc<Mutex<(u64, u64)>>);
        impl ClockPair for Samples {
            fn sample(&self) -> Result<(u64, u64), &'static str> {
                Ok(*self.0.lock().unwrap())
            }
        }
        let source = u64::try_from(source_ns).unwrap();
        let wall = source.checked_add(9000).unwrap();
        let expiry = wall.checked_add(1000).unwrap();
        let sample = Arc::new(Mutex::new((100, wall)));
        let namespace = "p184-g12-dl-0123456789abcdef";
        let consumer = format!("{namespace}.stream");
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema": SCHEMA, "namespace": namespace, "consumer_id": consumer,
            "expires_unix_ns": expiry,
            "points": [{"monotonic_ns": 100, "source_ns": source_ns},
                       {"monotonic_ns": 200, "source_ns": source_ns.checked_add(100).unwrap()}]
        }))
        .unwrap();
        let clock = ReplayClock::from_bytes(
            &bytes,
            namespace,
            &consumer,
            Arc::new(Samples(sample.clone())),
        )
        .unwrap();
        (clock, move |expired| {
            *sample.lock().unwrap() = (100, if expired { expiry } else { wall });
        })
    }

    fn monotonic_ns() -> Result<u64, &'static str> {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: clock_gettime writes one initialized timespec through a valid pointer.
        if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) } != 0
            || time.tv_sec < 0
            || !(0..1_000_000_000).contains(&time.tv_nsec)
        {
            return Err(error());
        }
        (time.tv_sec as u64)
            .checked_mul(1_000_000_000)
            .and_then(|s| s.checked_add(time.tv_nsec as u64))
            .ok_or_else(error)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::DataClock;
        use serde_json::{json, Value};

        const NS: &str = "p184-g12-dl-0123456789abcdef";
        const WALL: u64 = 10_000_000_000;

        struct Samples(Mutex<Result<(u64, u64), &'static str>>);
        impl ClockPair for Samples {
            fn sample(&self) -> Result<(u64, u64), &'static str> {
                *self.0.lock().unwrap()
            }
        }
        impl Samples {
            fn set(&self, mono: u64, wall: u64) {
                *self.0.lock().unwrap() = Ok((mono, wall));
            }
        }
        fn timeline() -> Value {
            json!({
                "schema": SCHEMA,
                "namespace": NS,
                "consumer_id": format!("{NS}.stream"),
                "expires_unix_ns": WALL + 1000,
                "points": [
                    {"monotonic_ns": 100, "source_ns": 1_000_000_000},
                    {"monotonic_ns": 103, "source_ns": 1_000_000_002},
                    {"monotonic_ns": 110, "source_ns": 1_000_000_002}
                ]
            })
        }
        fn samples() -> Arc<Samples> {
            Arc::new(Samples(Mutex::new(Ok((100, WALL)))))
        }
        fn load(value: &Value, clocks: Arc<Samples>) -> Result<ReplayClock, &'static str> {
            ReplayClock::from_bytes(
                &serde_json::to_vec(value).unwrap(),
                NS,
                &format!("{NS}.stream"),
                clocks,
            )
        }

        #[test]
        fn exact_floor_interpolation_plateau_and_inclusive_endpoints() {
            let samples = samples();
            let clock = load(&timeline(), samples.clone()).unwrap();
            for (mono, expected) in [
                (100, 1_000_000_000),
                (101, 1_000_000_000),
                (102, 1_000_000_001),
                (103, 1_000_000_002),
                (110, 1_000_000_002),
            ] {
                samples.set(mono, WALL);
                assert_eq!(clock.now_ns(), Ok(expected));
            }
        }

        #[test]
        fn expiry_never_falls_back_even_after_clock_recovers() {
            let samples = samples();
            let clock = DataClock::Replay(load(&timeline(), samples.clone()).unwrap());
            assert_eq!(clock.now_ns(WALL as i64), Ok(1_000_000_000));
            samples.set(101, WALL + 1000);
            assert_eq!(clock.now_ns(WALL as i64), Err(error()));
            samples.set(102, WALL + 1);
            assert_eq!(clock.now_ns(WALL as i64), Err(error()));
        }

        #[test]
        fn regression_and_out_of_range_latch() {
            for invalid in [
                (99, WALL),
                (101, WALL - 1),
                (111, WALL),
                (0, WALL),
                (100, 0),
            ] {
                let samples = samples();
                let clock = load(&timeline(), samples.clone()).unwrap();
                samples.set(invalid.0, invalid.1);
                assert_eq!(clock.now_ns(), Err(error()));
                samples.set(102, WALL);
                assert_eq!(clock.now_ns(), Err(error()));
            }
        }

        #[test]
        fn sampling_failure_latches() {
            let samples = samples();
            let clock = load(&timeline(), samples.clone()).unwrap();
            *samples.0.lock().unwrap() = Err(error());
            assert_eq!(clock.now_ns(), Err(error()));
            samples.set(101, WALL);
            assert_eq!(clock.now_ns(), Err(error()));
        }

        #[test]
        fn malformed_identity_bounds_and_schedule_are_refused() {
            for (field, value) in [
                ("schema", json!("other")),
                ("namespace", json!("other")),
                ("consumer_id", json!(format!("{NS}.native"))),
                ("expires_unix_ns", json!(WALL)),
                ("expires_unix_ns", json!(WALL + MAX_WINDOW_NS + 1)),
                ("expires_unix_ns", json!(true)),
                ("extra", json!(0)),
                ("points", json!([])),
            ] {
                let mut t = timeline();
                t[field] = value;
                assert!(load(&t, samples()).is_err(), "{field}");
            }
            for (field, value) in [
                ("monotonic_ns", json!(100)),
                ("monotonic_ns", json!(0)),
                ("monotonic_ns", json!(100 + MAX_WINDOW_NS + 1)),
                ("monotonic_ns", json!(103.0)),
                ("source_ns", json!(0)),
                ("source_ns", json!(999_999_999)),
                ("source_ns", json!(WALL + 1)),
                ("source_ns", json!(u64::MAX)),
                ("extra", json!(1)),
            ] {
                let mut t = timeline();
                t["points"][1][field] = value;
                assert!(load(&t, samples()).is_err(), "{field}");
            }
            let mut t = timeline();
            t["points"] = json!((0..MAX_POINTS + 1)
                .map(|i| json!({
                    "monotonic_ns": 100 + i, "source_ns": 1,
                }))
                .collect::<Vec<_>>());
            assert!(load(&t, samples()).is_err());
            let mut t = timeline();
            t["points"] = json!([
                {"monotonic_ns": 100, "source_ns": 1},
                {"monotonic_ns": 100 + MAX_WINDOW_NS + 1, "source_ns": 1}
            ]);
            assert!(load(&t, samples()).is_err());
        }

        #[test]
        fn duplicate_fields_and_oversize_bytes_are_refused() {
            let t = serde_json::to_string(&timeline()).unwrap();
            let duplicate_top = t.replacen("{", r#"{"schema":"p184.test-data-clock.v1","#, 1);
            let duplicate_point = t.replacen(
                "\"monotonic_ns\":100",
                "\"monotonic_ns\":100,\"monotonic_ns\":100",
                1,
            );
            for bytes in [
                duplicate_top.into_bytes(),
                duplicate_point.into_bytes(),
                vec![b' '; MAX_BYTES + 1],
                Vec::new(),
            ] {
                assert!(
                    ReplayClock::from_bytes(&bytes, NS, &format!("{NS}.stream"), samples())
                        .is_err()
                );
            }
        }

        #[test]
        fn scoped_path_namespace_and_hash_contract() {
            assert!(namespace_valid(NS));
            assert!(!namespace_valid("p184-g12-dl-0123456789abcdeF"));
            assert!(consumer_valid(&format!("{NS}.stream"), NS));
            assert!(!consumer_valid(&format!("{NS}.query"), NS));
            assert!(scoped_path(
                Path::new(&format!("/work/TEST_ONLY/{NS}/clock.json")),
                NS
            ));
            for path in [
                format!("/work/TEST_ONLY/{NS}/../clock.json"),
                format!("/work/TEST_ONLY/{NS}-other/clock.json"),
                format!("/work/TEST_ONLY/{NS}/clock.txt"),
                format!("work/TEST_ONLY/{NS}/clock.json"),
            ] {
                assert!(!scoped_path(Path::new(&path), NS));
            }
            assert_eq!(
                sha256(b"abc"),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            );
        }
    }
}
