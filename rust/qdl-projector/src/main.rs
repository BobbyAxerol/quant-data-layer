//! `qdl-projector` binary (KN-3): one role, bounded task threads (D1).
//!
//! `qdl-projector run` starts, per replica:
//! - stage A: canonical -> state topics in Kafka transactions (group
//!   `kn-projector-v3-a`, transactional id `kn-projector-v3-a-<replica>`);
//! - stage B: state topics -> market cache with owner fences (group
//!   `kn-projector-v3-b`), per-product rebuild requests taken from the cache
//!   set `kn3:<env>:rebuild` for products this replica owns (D17);
//! - expiry (D11) for the BAR products of the bars partitions this replica
//!   owns, and the bars-topic cleaner over the same partitions;
//! - a status JSON (file + one stdout line per interval).
//!
//! Configuration is environment-only; TLS material is read from files named
//! by the environment, never logged. An integrity stop (a record that cannot
//! be interpreted) or a fatal Kafka error exits non-zero: nothing is skipped.

use qdl_contracts::gateway_bundle::Bundle;
use qdl_contracts::state_codec::state_partition;
use qdl_contracts::state_contract::LogicalProductKey;
use qdl_projector::cache::{Cache, CacheError, Layout};
use qdl_projector::cleaner::{sweep_partition, CleanerKafkaSettings, SweepLimits, SweepReport};
use qdl_projector::expiry::{ExpiryError, ExpiryPublish, ExpiryReport, ExpiryTask};
use qdl_projector::kafka_pipe::{KafkaPipe, KafkaPipeSettings};
use qdl_projector::kafka_state::{KafkaPartitionReader, KafkaStateSettings, KafkaStateSource};
use qdl_projector::products::{retained_caps, ProductMap, ProductTransform, TransformSettings};
use qdl_projector::stage_a::{StageA, StageAError, StageALimits, StageAMetrics};
use qdl_projector::stage_b::{StageB, StageBError, StageBLimits, StageBMetrics};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{DefaultProducerContext, Producer, ThreadedProducer};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn env(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is required"))
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    env_or(name, &default.to_string())
        .parse()
        .map_err(|_| format!("{name} must be an unsigned integer"))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or_default()
}

#[derive(Clone)]
struct Config {
    bootstrap: String,
    tls: Option<(String, String, String)>,
    replica: String,
    bundle: Bundle,
    source_topic_id: String,
    canonical_topic: String,
    latest_topic: String,
    bars_topic: String,
    epoch: u64,
    cache_url: String,
    status_path: Option<String>,
    status_interval: Duration,
    rebuild_poll: Duration,
    expiry_interval: Duration,
    expiry_products_per_tick: usize,
    expiry_max_opens: usize,
    cleaner_interval: Duration,
    /// Isolated evidence runs only (exercise expiry on real volumes); a
    /// production packet never sets it.
    bar_cap_clamp: Option<u64>,
    stage_a: bool,
    stage_b: bool,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let tls = match std::env::var("QDL_KN_KAFKA_CA") {
            Ok(ca) => Some((ca, env("QDL_KN_KAFKA_CERT")?, env("QDL_KN_KAFKA_KEY")?)),
            Err(_) => None,
        };
        let path = env("QDL_KN_BUNDLE_PATH")?;
        let raw = std::fs::read_to_string(&path)
            .map_err(|error| format!("QDL_KN_BUNDLE_PATH={path}: {error}"))?;
        let epoch = env_u64("QDL_KN_MATERIALIZER_EPOCH", 0)?;
        if epoch == 0 {
            return Err("QDL_KN_MATERIALIZER_EPOCH must be a positive integer".into());
        }
        let replica = env("QDL_KN_REPLICA")?;
        if replica.is_empty()
            || !replica
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err("QDL_KN_REPLICA must be [A-Za-z0-9-]+".into());
        }
        let seconds = |name: &str, default: u64| env_u64(name, default).map(Duration::from_secs);
        Ok(Self {
            bootstrap: env("QDL_KN_KAFKA_BOOTSTRAP")?,
            tls,
            replica,
            bundle: Bundle::parse(&raw)?,
            source_topic_id: env("QDL_KN_TOPIC_ID")?,
            canonical_topic: env_or("QDL_KN_CANONICAL_TOPIC", "md.canonical.v2"),
            latest_topic: env_or("QDL_KN_LATEST_TOPIC", "md.latest.v2"),
            bars_topic: env_or("QDL_KN_BARS_TOPIC", "md.bars.v2"),
            epoch,
            cache_url: env("QDL_KN_MARKET_CACHE_URL")?,
            status_path: std::env::var("QDL_KN_STATUS_PATH").ok(),
            status_interval: seconds("QDL_KN_STATUS_INTERVAL_S", 10)?,
            rebuild_poll: seconds("QDL_KN_REBUILD_POLL_S", 5)?,
            expiry_interval: seconds("QDL_KN_EXPIRY_INTERVAL_S", 5)?,
            expiry_products_per_tick: env_u64("QDL_KN_EXPIRY_PRODUCTS_PER_TICK", 16)? as usize,
            expiry_max_opens: env_u64("QDL_KN_EXPIRY_MAX_OPENS", 1_000)? as usize,
            cleaner_interval: seconds("QDL_KN_CLEANER_INTERVAL_S", 21_600)?,
            bar_cap_clamp: match std::env::var("QDL_KN_BAR_CAP_CLAMP") {
                Ok(value) => Some(
                    value
                        .parse()
                        .map_err(|_| "QDL_KN_BAR_CAP_CLAMP must be an unsigned integer")?,
                ),
                Err(_) => None,
            },
            stage_a: env_or("QDL_KN_STAGE_A", "1") == "1",
            stage_b: env_or("QDL_KN_STAGE_B", "1") == "1",
        })
    }

    fn id(&self, role: &str) -> String {
        format!("kn-projector-v3-{role}-{}", self.replica)
    }

    fn client(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap)
            .set("socket.timeout.ms", "10000");
        if let Some((ca, certificate, key)) = &self.tls {
            config
                .set("security.protocol", "ssl")
                .set("ssl.ca.location", ca)
                .set("ssl.certificate.location", certificate)
                .set("ssl.key.location", key)
                .set("ssl.endpoint.identification.algorithm", "https");
        }
        config
    }

    /// Partition count of a state topic (routing must match the broker).
    fn partitions(&self, topic: &str) -> Result<u32, String> {
        let consumer: BaseConsumer = self
            .client()
            .set("client.id", self.id("meta"))
            .create()
            .map_err(|error| error.to_string())?;
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .map_err(|error| format!("metadata {topic}: {error}"))?;
        let count = metadata
            .topics()
            .iter()
            .find(|item| item.name() == topic)
            .map(|item| item.partitions().len())
            .unwrap_or(0);
        if count == 0 {
            return Err(format!("state topic {topic} does not exist"));
        }
        Ok(count as u32)
    }

    fn producer(&self, role: &str) -> Result<ThreadedProducer<DefaultProducerContext>, String> {
        let timeout = Duration::from_secs(30);
        let producer: ThreadedProducer<DefaultProducerContext> = self
            .client()
            .set("client.id", self.id(role))
            .set("transactional.id", self.id(role))
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("transaction.timeout.ms", timeout.as_millis().to_string())
            .create()
            .map_err(|error| format!("{role} producer: {error}"))?;
        producer
            .init_transactions(timeout)
            .map_err(|error| format!("{role} init_transactions: {error}"))?;
        Ok(producer)
    }
}

#[derive(Default)]
struct Status {
    started_ms: u64,
    stage_a: StageAMetrics,
    stage_b: StageBMetrics,
    owned: Vec<(String, i32)>,
    rebuilding: Option<(String, u64)>,
    last_rebuild_error: Option<String>,
    memory_pressure_ms: Option<u64>,
    expiry: ExpiryReport,
    expiry_ticks: u64,
    cleaner: SweepReport,
    cleaner_sweeps: u64,
    errors: BTreeMap<String, String>,
}

type Shared = Arc<Mutex<Status>>;

fn record_error(status: &Shared, task: &str, error: String) {
    eprintln!("{{\"task\":\"{task}\",\"error\":{}}}", json!(error));
    if let Ok(mut status) = status.lock() {
        status.errors.insert(task.to_owned(), error);
    }
}

fn fatal(status: &Shared, task: &str, error: String) -> ! {
    record_error(status, task, error);
    write_status(status, None);
    std::process::exit(2);
}

fn status_json(status: &Status) -> serde_json::Value {
    let a = &status.stage_a;
    let b = &status.stage_b;
    let e = &status.expiry;
    let c = &status.cleaner;
    json!({
        "schema": "qdl.kn3.projector-status.v1",
        "at_ms": now_ms(),
        "started_ms": status.started_ms,
        "stage_a": {"transactions": a.transactions, "inputs": a.inputs, "outputs": a.outputs, "aborts": a.aborts},
        "stage_b": {
            "batches": b.batches, "latest_applied": b.latest_applied, "bars_applied": b.bars_applied,
            "duplicates": b.duplicates, "stale": b.stale, "conflicts": b.conflicts,
            "not_comparable": b.not_comparable, "below_floor": b.below_floor, "floors": b.floors,
            "cas_retries": b.cas_retries, "zombies": b.zombies, "builds": b.builds,
            "published": b.published, "unpublished": b.unpublished, "reclaimed_keys": b.reclaimed_keys,
            "rebuilds_started": b.rebuilds_started, "rebuilds_completed": b.rebuilds_completed,
            "rebuilds_abandoned": b.rebuilds_abandoned, "rebuilds_refused": b.rebuilds_refused,
            "rebuild_records": b.rebuild_records,
            "ownership_lost": b.ownership_lost, "cache_reconnects": b.cache_reconnects,
            "rolling_rebuilds": b.rolling_rebuilds, "retired": b.retired, "rewinds": b.rewinds,
            "owned": status.owned.iter().map(|(t, p)| format!("{t}/{p}")).collect::<Vec<_>>(),
            "rebuilding": status.rebuilding.as_ref().map(|(lpk, g)| json!({"lpk": lpk, "generation": g})),
            "last_rebuild_error": status.last_rebuild_error,
            "memory_pressure_ms": status.memory_pressure_ms,
        },
        "expiry": {"ticks": status.expiry_ticks, "products_visited": e.products_visited,
            "not_ready": e.not_ready, "pending": e.pending, "floors_published": e.floors_published,
            "tombstones": e.tombstones, "expired_opens": e.expired_opens},
        "cleaner": {"sweeps": status.cleaner_sweeps, "records_read": c.records_read,
            "bytes_read": c.bytes_read, "below_floor_keys": c.below_floor_keys,
            "tombstones_published": c.tombstones_published, "pending": c.pending},
        "errors": status.errors,
    })
}

fn write_status(status: &Shared, path: Option<&str>) {
    let Ok(guard) = status.lock() else {
        return;
    };
    let document = status_json(&guard);
    drop(guard);
    println!("{document}");
    if let Some(path) = path {
        let temporary = format!("{path}.tmp");
        if std::fs::write(&temporary, document.to_string()).is_ok() {
            let _ = std::fs::rename(&temporary, path);
        }
    }
}

fn run_stage_a(config: Config, status: Shared, latest: u32, bars: u32) {
    let products = ProductMap::from_bundle(&config.bundle)
        .unwrap_or_else(|error| fatal(&status, "stage_a", error));
    let transform = ProductTransform::new(
        products,
        TransformSettings {
            source_topic_id: config.source_topic_id.clone(),
            latest_topic: config.latest_topic.clone(),
            bars_topic: config.bars_topic.clone(),
            latest_partitions: latest,
            bars_partitions: bars,
            materializer_epoch: config.epoch,
        },
    );
    let pipe = KafkaPipe::open(KafkaPipeSettings {
        bootstrap: config.bootstrap.clone(),
        input_topic: config.canonical_topic.clone(),
        group_id: "kn-projector-v3-a".into(),
        transactional_id: config.id("a"),
        client_id: config.id("a"),
        tls: config.tls.clone(),
        transaction_timeout: Duration::from_secs(30),
    })
    .unwrap_or_else(|error| fatal(&status, "stage_a", error));
    let mut stage = StageA::new(pipe, transform, StageALimits::default());
    loop {
        match stage.step() {
            Ok(_) => {
                if let Ok(mut shared) = status.lock() {
                    shared.stage_a = stage.metrics.clone();
                }
            }
            Err(StageAError::Integrity {
                partition,
                offset,
                reason,
            }) => fatal(
                &status,
                "stage_a",
                format!("integrity stop at {partition}/{offset}: {reason}"),
            ),
            Err(StageAError::Fatal(reason)) => fatal(&status, "stage_a", reason),
        }
    }
}

fn run_stage_b(config: Config, status: Shared) {
    let layout = Layout::new(&config.bundle.environment);
    let settings = KafkaStateSettings {
        bootstrap: config.bootstrap.clone(),
        topics: vec![config.latest_topic.clone(), config.bars_topic.clone()],
        group_id: "kn-projector-v3-b".into(),
        client_id: config.id("b"),
        tls: config.tls.clone(),
    };
    let source =
        KafkaStateSource::open(&settings).unwrap_or_else(|error| fatal(&status, "stage_b", error));
    let reader = KafkaPartitionReader::open(
        &config.bootstrap,
        &config.id("rebuild"),
        config.tls.as_ref(),
    )
    .unwrap_or_else(|error| fatal(&status, "stage_b", error));
    let cache = Cache::connect(&config.cache_url, layout.clone())
        .unwrap_or_else(|error| fatal(&status, "stage_b", format!("{error:?}")));
    let mut stage = StageB::new(
        source,
        cache,
        StageBLimits {
            // Owner-fence probe and rebuild-request cadence.
            ownership_probe: config.rebuild_poll,
            ..StageBLimits::default()
        },
    )
    .with_rebuild_reader(Box::new(reader));
    let mut source_errors = 0u32;
    loop {
        match stage.step() {
            Ok(_) => {
                source_errors = 0;
                if let Ok(mut shared) = status.lock() {
                    shared.memory_pressure_ms = None;
                }
            }
            Err(StageBError::Integrity {
                topic,
                partition,
                offset,
                reason,
            }) => fatal(
                &status,
                "stage_b",
                format!("integrity stop at {topic}/{partition}/{offset}: {reason}"),
            ),
            Err(StageBError::Cache(CacheError::MemoryPressure(reason))) => {
                // noeviction: nothing applied, the checkpoint stays behind.
                record_error(&status, "stage_b", format!("memory pressure: {reason}"));
                if let Ok(mut shared) = status.lock() {
                    shared.memory_pressure_ms.get_or_insert(now_ms());
                }
                std::thread::sleep(Duration::from_secs(1));
            }
            Err(StageBError::Cache(CacheError::Redis(reason))) => {
                // Reconnect and prepare every partition again (tail if the
                // cache kept its state, rebuild if it lost it).
                record_error(&status, "stage_b", format!("cache: {reason}"));
                std::thread::sleep(Duration::from_secs(1));
                if let Err(error) = stage.recover_cache() {
                    record_error(&status, "stage_b", format!("reconnect: {error:?}"));
                }
            }
            Err(error) => {
                source_errors += 1;
                record_error(&status, "stage_b", format!("{error:?}"));
                if source_errors > 30 {
                    fatal(&status, "stage_b", "30 consecutive errors".into());
                }
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        if let Ok(mut shared) = status.lock() {
            shared.stage_b = stage.metrics.clone();
            shared.owned = stage.owned();
            shared.rebuilding = stage
                .rebuilding()
                .map(|(lpk, generation)| (lpk.to_owned(), generation));
            shared.last_rebuild_error = stage.last_rebuild_error.clone();
        }
    }
}

/// BAR products of the bars partitions this replica owns.
fn owned_bars(config: &Config, status: &Shared, bars: u32) -> BTreeSet<i32> {
    let Ok(shared) = status.lock() else {
        return BTreeSet::new();
    };
    shared
        .owned
        .iter()
        .filter(|(topic, partition)| topic == &config.bars_topic && (*partition as u32) < bars)
        .map(|(_, partition)| *partition)
        .collect()
}

fn run_expiry(config: Config, status: Shared, bars: u32) {
    let products = ProductMap::from_bundle(&config.bundle)
        .unwrap_or_else(|error| fatal(&status, "expiry", error));
    let caps = retained_caps(&config.bundle, &products);
    let mut task = ExpiryTask::new(
        BTreeMap::new(),
        config.expiry_products_per_tick,
        config.expiry_max_opens,
    )
    .unwrap_or_else(|error| fatal(&status, "expiry", format!("{error:?}")));
    let producer = config
        .producer("expiry")
        .unwrap_or_else(|error| fatal(&status, "expiry", error));
    let mut cache = Cache::connect(&config.cache_url, Layout::new(&config.bundle.environment))
        .unwrap_or_else(|error| fatal(&status, "expiry", format!("{error:?}")));
    let publish = ExpiryPublish {
        topic: config.bars_topic.clone(),
        partitions: bars,
        materializer_epoch: config.epoch,
        timeout: Duration::from_secs(30),
    };
    loop {
        std::thread::sleep(config.expiry_interval);
        let owned = owned_bars(&config, &status, bars);
        let mine: BTreeMap<String, u64> = caps
            .iter()
            .filter(|(lpk, _)| {
                LogicalProductKey::parse(lpk)
                    .ok()
                    .and_then(|parsed| state_partition(&parsed, bars).ok())
                    .is_some_and(|partition| owned.contains(&(partition as i32)))
            })
            .map(|(lpk, cap)| {
                let clamped = config.bar_cap_clamp.map_or(*cap, |clamp| (*cap).min(clamp));
                (lpk.clone(), clamped)
            })
            .collect();
        if let Err(error) = task.set_caps(mine) {
            fatal(&status, "expiry", format!("{error:?}"));
        }
        match task.tick(&mut cache, &producer, &publish) {
            Ok(report) => {
                if let Ok(mut shared) = status.lock() {
                    shared.expiry_ticks += 1;
                    let total = &mut shared.expiry;
                    total.products_visited += report.products_visited;
                    total.not_ready += report.not_ready;
                    total.pending += report.pending;
                    total.floors_published += report.floors_published;
                    total.tombstones += report.tombstones;
                    total.expired_opens += report.expired_opens;
                }
            }
            Err(error) => {
                record_error(&status, "expiry", format!("{error:?}"));
                if matches!(error, ExpiryError::Cache(CacheError::Redis(_))) {
                    let _ = cache.reconnect();
                }
            }
        }
    }
}

fn run_cleaner(config: Config, status: Shared, bars: u32) {
    let settings = CleanerKafkaSettings {
        bootstrap: config.bootstrap.clone(),
        replica: config.replica.clone(),
        tls: config.tls.clone(),
        transaction_timeout: Duration::from_secs(30),
    };
    let producer = settings
        .open_producer()
        .unwrap_or_else(|error| fatal(&status, "cleaner", error));
    let mut reader = settings
        .open_reader()
        .unwrap_or_else(|error| fatal(&status, "cleaner", error));
    loop {
        std::thread::sleep(config.cleaner_interval);
        for partition in owned_bars(&config, &status, bars) {
            match sweep_partition(
                &mut reader,
                &config.bars_topic,
                partition,
                &producer,
                &SweepLimits::default(),
            ) {
                Ok(report) => {
                    if let Ok(mut shared) = status.lock() {
                        shared.cleaner_sweeps += 1;
                        let total = &mut shared.cleaner;
                        total.records_read += report.records_read;
                        total.bytes_read += report.bytes_read;
                        total.below_floor_keys += report.below_floor_keys;
                        total.tombstones_published += report.tombstones_published;
                        total.pending = report.pending;
                    }
                }
                Err(error) => record_error(&status, "cleaner", format!("{error:?}")),
            }
        }
    }
}

fn run() -> Result<(), String> {
    let config = Config::from_env()?;
    let latest = config.partitions(&config.latest_topic)?;
    let bars = config.partitions(&config.bars_topic)?;
    let status: Shared = Arc::new(Mutex::new(Status {
        started_ms: now_ms(),
        ..Status::default()
    }));
    let mut threads = Vec::new();
    if config.stage_a {
        let (config, status) = (config.clone(), status.clone());
        threads.push(std::thread::spawn(move || {
            run_stage_a(config, status, latest, bars)
        }));
    }
    if config.stage_b {
        let (b_config, b_status) = (config.clone(), status.clone());
        threads.push(std::thread::spawn(move || run_stage_b(b_config, b_status)));
        let (e_config, e_status) = (config.clone(), status.clone());
        threads.push(std::thread::spawn(move || {
            run_expiry(e_config, e_status, bars)
        }));
        if !config.cleaner_interval.is_zero() {
            let (c_config, c_status) = (config.clone(), status.clone());
            threads.push(std::thread::spawn(move || {
                run_cleaner(c_config, c_status, bars)
            }));
        }
    }
    println!(
        "{}",
        json!({"event": "started", "replica": config.replica, "environment": config.bundle.environment,
            "bundle_sha256": config.bundle.sha256, "latest_partitions": latest, "bars_partitions": bars,
            "stage_a": config.stage_a, "stage_b": config.stage_b,
            "bar_cap_clamp": config.bar_cap_clamp})
    );
    loop {
        std::thread::sleep(config.status_interval);
        write_status(&status, config.status_path.as_deref());
        if threads.iter().any(|thread| thread.is_finished()) {
            return Err("a task thread ended".into());
        }
    }
}

fn main() -> ExitCode {
    match std::env::args().nth(1).as_deref() {
        Some("run") => match run() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("{}", json!({"event": "failed", "error": error}));
                ExitCode::FAILURE
            }
        },
        _ => {
            eprintln!("usage: qdl-projector run   (configuration: QDL_KN_* environment)");
            ExitCode::from(64)
        }
    }
}
