#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashSet};
use std::env;
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;
use qdl_contracts::qdl::provider::v1::{QuarantineReason, RawProviderEnvelope, TransportProtocol};
use qdl_core::backoff::BackoffPolicy;
use qdl_core::transport::{AppendResult, RetryClass};
use qdl_kafka::{
    shutdown_signal, CommittedTransaction, KafkaTlsConfig, KafkaTransportConfig,
    KafkaTransportError, ShutdownSignal, TransactionPipeline, TransactionalKafkaBridge,
    TransactionalKafkaOutput, TransactionalShadowTopics,
};
use qdl_realtime_core::{
    provider_admission_server::ProviderAdmissionServer, CoreError, RealtimeCore, RealtimeCoreConfig,
};
use qdl_test_data_clock::DataClock;
use qdl_venue_core::authority::{AuthorityMode, AuthorityRecord, PublicationContext, SinkTarget};
use serde::Deserialize;
use serde_json::json;

const MAX_IN_GENERATION_RECEIVE_RETRIES: u32 = 3;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeConfig {
    core: RealtimeCoreConfig,
    raw_topics: Vec<String>,
    authority: AuthorityRecord,
    shard_id: String,
    transactional_id: String,
    batch_size: usize,
    batch_wait_ms: u64,
    max_events: u64,
    metrics_every_batches: u64,
    #[serde(default)]
    strict_subscription_scope: bool,
    #[serde(default)]
    book_resync_quarantine: bool,
}

impl RuntimeConfig {
    fn validate(&self) -> Result<(), String> {
        self.core.validate().map_err(|error| error.to_string())?;
        self.authority.validate()?;
        if !matches!(
            self.authority.mode,
            AuthorityMode::RustShadow | AuthorityMode::RustPrimary
        ) || self.raw_topics.is_empty()
            || self.raw_topics.iter().any(|topic| topic.trim().is_empty())
            || self.shard_id.trim().is_empty()
            || self.transactional_id.trim().is_empty()
            || self.batch_size == 0
            || self.batch_size > 10_000
            || self.batch_wait_ms == 0
            || self.batch_wait_ms > 1_000
            || self.metrics_every_batches == 0
        {
            return Err(
                "realtime-core runtime config is invalid or not a shared Rust authority".into(),
            );
        }
        Ok(())
    }
}

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("required environment variable is missing: {name}"))
}

fn kafka_config() -> Result<KafkaTransportConfig, String> {
    let cert_root = required("QDL_KAFKA_CERT_ROOT")?;
    let timeout_seconds = env::var("QDL_KAFKA_REQUEST_TIMEOUT_SECONDS")
        .unwrap_or_else(|_| "30".into())
        .parse::<u64>()
        .map_err(|_| "QDL_KAFKA_REQUEST_TIMEOUT_SECONDS must be positive")?;
    if timeout_seconds == 0 {
        return Err("QDL_KAFKA_REQUEST_TIMEOUT_SECONDS must be positive".into());
    }
    Ok(KafkaTransportConfig {
        bootstrap_servers: required("QDL_KAFKA_BOOTSTRAP_SERVERS")?,
        client_id: required("QDL_KAFKA_CLIENT_ID")?,
        group_id: required("QDL_KAFKA_GROUP_ID")?,
        request_timeout: Duration::from_secs(timeout_seconds),
        tls: KafkaTlsConfig {
            ca_location: format!("{cert_root}/ca.crt"),
            certificate_location: format!("{cert_root}/client.crt"),
            key_location: format!("{cert_root}/client.key"),
            key_password: None,
        },
    })
}

type RuntimeError = Box<dyn std::error::Error + Send + Sync>;

fn now_ns() -> Result<i64, RuntimeError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .try_into()?)
}

fn check_data_clock(clock: &DataClock) -> Result<(), RuntimeError> {
    clock.now_ns(0).map(|_| ()).map_err(Into::into)
}

#[cfg(feature = "isolated-replay-clock")]
async fn data_clock_failure(clock: &DataClock) -> RuntimeError {
    loop {
        if let Err(error) = check_data_clock(clock) {
            return error;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// Only cancel the consume loop, never the owner of an in-flight transaction.
// run_generation always calls finish_generation after this guard returns.
async fn guard_data_clock<F: Future>(
    clock: &DataClock,
    work: F,
) -> Result<F::Output, RuntimeError> {
    check_data_clock(clock)?;
    #[cfg(feature = "isolated-replay-clock")]
    if clock.is_replay() {
        return tokio::select! {
            biased;
            error = data_clock_failure(clock) => Err(error),
            result = work => {
                check_data_clock(clock)?;
                Ok(result)
            }
        };
    }
    Ok(work.await)
}

fn should_retry_transport(class: RetryClass) -> bool {
    class != RetryClass::NonRetryable
}

fn retryable_runtime_error(error: &RuntimeError) -> bool {
    error
        .downcast_ref::<KafkaTransportError>()
        .is_some_and(|value| should_retry_transport(value.retry_class()))
}

fn should_retry_receive_in_generation(error: &KafkaTransportError, failures: u32) -> bool {
    should_retry_transport(error.retry_class()) && failures < MAX_IN_GENERATION_RECEIVE_RETRIES
}

fn approved_subscription_scope(config: &RuntimeConfig) -> HashSet<String> {
    config
        .core
        .bindings
        .iter()
        .map(|binding| binding.source_id.clone())
        .collect()
}

fn is_approved_subscription(
    raw: &RawProviderEnvelope,
    approved_subscriptions: &HashSet<String>,
) -> bool {
    approved_subscriptions.contains(&raw.subscription_id)
}

/// Min, median-free summary of one span, in nanoseconds, reported in milliseconds.
///
/// Deliberately not a histogram: this exists to say which half of the pipeline
/// a delay is in, and min/mean/max over one reporting interval answers that.
#[derive(Debug, Default)]
struct SpanSummary {
    count: u64,
    total_ns: i128,
    min_ns: i64,
    max_ns: i64,
}

impl SpanSummary {
    fn observe(&mut self, span_ns: i64) {
        let span_ns = span_ns.max(0);
        if self.count == 0 || span_ns < self.min_ns {
            self.min_ns = span_ns;
        }
        if span_ns > self.max_ns {
            self.max_ns = span_ns;
        }
        self.count += 1;
        self.total_ns += i128::from(span_ns);
    }

    fn report(&self) -> serde_json::Value {
        if self.count == 0 {
            return serde_json::Value::Null;
        }
        let mean_ns = (self.total_ns / i128::from(self.count)) as i64;
        json!({
            "n": self.count,
            "min": self.min_ns as f64 / 1e6,
            "mean": mean_ns as f64 / 1e6,
            "max": self.max_ns as f64 / 1e6,
        })
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

fn strict_quarantine_reason(error: &CoreError) -> Option<(QuarantineReason, &'static str)> {
    match error {
        CoreError::UnknownBinding => Some((
            QuarantineReason::FencingRejected,
            "raw identity does not match declared strict scope",
        )),
        CoreError::ProvenanceRejected => Some((
            QuarantineReason::FencingRejected,
            "test provenance is forbidden in strict scope",
        )),
        CoreError::RawEnvelope(_) => Some((
            QuarantineReason::Malformed,
            "raw envelope validation failed in strict scope",
        )),
        CoreError::Decode(_) => Some((
            QuarantineReason::SemanticInvalid,
            "provider frame cannot be canonicalized in strict scope",
        )),
        CoreError::Configuration(_) => None,
    }
}

fn output_targets(mode: AuthorityMode) -> Result<(SinkTarget, SinkTarget), String> {
    match mode {
        AuthorityMode::RustShadow => {
            Ok((SinkTarget::ShadowCanonical, SinkTarget::ShadowQuarantine))
        }
        AuthorityMode::RustPrimary => {
            Ok((SinkTarget::PrimaryCanonical, SinkTarget::PrimaryQuarantine))
        }
        AuthorityMode::RustCanary => Err("shared realtime core cannot run RUST_CANARY".into()),
    }
}

fn authority_mode_name(mode: AuthorityMode) -> &'static str {
    match mode {
        AuthorityMode::RustShadow => "RUST_SHADOW",
        AuthorityMode::RustCanary => "RUST_CANARY",
        AuthorityMode::RustPrimary => "RUST_PRIMARY",
    }
}

// A ready receive must not extend an expired batch. Tokio timeout alone polls
// the input first, so an always-ready raw queue needs this explicit guard.
async fn receive_batch_input<T>(
    deadline: tokio::time::Instant,
    next: impl std::future::Future<Output = T>,
) -> Option<T> {
    if tokio::time::Instant::now() >= deadline {
        return None;
    }
    tokio::time::timeout_at(deadline, next).await.ok()
}

/// Everything the progress and stopped lines report for one generation.
///
/// Normalization counters (canonical, quarantines, raw age, ...) advance when a
/// batch is normalized, as before IDR-094. `processed`, `batches` and the
/// commit spans advance only when that batch's transaction has committed, so
/// `processed` never counts input whose offsets are not durable.
#[derive(Default)]
struct Progress {
    processed: u64,
    canonical: u64,
    quarantines: u64,
    duplicates: u64,
    filtered: u64,
    // A bare `filtered` total hid an entire feed on 2026-09-18: every Binance
    // book frame was answered `IgnoredStaleGeneration` and the only visible
    // symptom was this counter rising slightly. Breaking it down by outcome
    // makes a feed that stops readable from the progress line alone.
    filtered_by_outcome: BTreeMap<&'static str, u64>,
    ignored_out_of_scope: u64,
    scope_quarantines: u64,
    batches: u64,
    // R1.29. Two spans this loop can see and nothing else can: how old a frame
    // is when the core first reads it, and how long its transactional commit
    // takes. Reported on the progress line and reset with it, so each line
    // describes its own interval rather than all of history.
    raw_age: SpanSummary,
    commit_latency: SpanSummary,
    // IDR-094. How long the loop actually stalled on the previous batch's
    // commit, and how much of that commit ran behind the next batch's
    // read + normalize. `commit_ms ~= commit_wait_ms + commit_hidden_ms`.
    commit_wait: SpanSummary,
    commit_hidden: SpanSummary,
    collect_latency: SpanSummary,
    normalize_latency: SpanSummary,
    raw_age_by_partition: BTreeMap<(String, i32), SpanSummary>,
    // raw_age split by how the frame was acquired. A REST bar edge stamps
    // received_at_ns when a bar settles and then waits for its whole cycle
    // before one publish, so its age is a batching wait, not upstream delay.
    // Mixed into one summary it looked like a periodic stall on the partitions
    // those bar keys hash to. WEBSOCKET is the realtime ingestor path; REST is
    // every other transport (bar edges, REST book anchors). Additive fields:
    // `raw_age_ms` keeps all frames.
    raw_age_websocket: SpanSummary,
    raw_age_rest: SpanSummary,
}

impl Progress {
    /// Observe one frame's raw_age in the total and in its transport split.
    fn observe_raw_age(&mut self, transport_protocol: i32, age_ns: i64) {
        self.raw_age.observe(age_ns);
        if transport_protocol == TransportProtocol::Websocket as i32 {
            self.raw_age_websocket.observe(age_ns);
        } else {
            self.raw_age_rest.observe(age_ns);
        }
    }

    fn record_commit<T>(
        &mut self,
        committed: Option<CommittedTransaction<T>>,
        generation: u64,
        metrics_every_batches: u64,
    ) -> Result<(), RuntimeError> {
        let Some(committed) = committed else {
            return Ok(());
        };
        self.commit_latency.observe(committed.commit_ns);
        self.commit_wait.observe(committed.wait_ns);
        self.commit_hidden
            .observe(committed.commit_ns.saturating_sub(committed.wait_ns));
        self.processed += committed.inputs;
        self.batches += 1;
        if self.batches % metrics_every_batches == 0 {
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "event": "qdl_realtime_core_progress",
                    "generation": generation,
                    "processed": self.processed,
                    "canonical": self.canonical,
                    "quarantines": self.quarantines,
                    "duplicates": self.duplicates,
                    "filtered": self.filtered,
                    "filtered_by_outcome": self.filtered_by_outcome,
                    "ignored_out_of_scope": self.ignored_out_of_scope,
                    "scope_quarantines": self.scope_quarantines,
                    "batches": self.batches,
                    "raw_age_ms": self.raw_age.report(),
                    "raw_age_websocket_ms": self.raw_age_websocket.report(),
                    "raw_age_rest_ms": self.raw_age_rest.report(),
                    "commit_ms": self.commit_latency.report(),
                    "commit_wait_ms": self.commit_wait.report(),
                    "commit_hidden_ms": self.commit_hidden.report(),
                    "collect_ms": self.collect_latency.report(),
                    "normalize_ms": self.normalize_latency.report(),
                    "raw_age_by_partition_ms": self.raw_age_by_partition.iter().map(|((topic, partition), age)| {
                        json!({"topic": topic, "partition": partition, "age": age.report()})
                    }).collect::<Vec<_>>(),
                }))?
            );
            self.raw_age.reset();
            self.raw_age_websocket.reset();
            self.raw_age_rest.reset();
            self.commit_latency.reset();
            self.commit_wait.reset();
            self.commit_hidden.reset();
            self.collect_latency.reset();
            self.normalize_latency.reset();
            self.raw_age_by_partition.clear();
        }
        Ok(())
    }
}

type CommitPipeline = TransactionPipeline<Vec<AppendResult>>;

type SignalOutcome = Option<Result<ShutdownSignal, String>>;

/// K1. Process-wide stop intent. One signal listener lives for the whole
/// process, not per generation, so a SIGTERM that arrives while a generation
/// fails, drains, or the supervisor sleeps in retry backoff is never lost.
#[derive(Clone)]
struct StopLatch {
    receiver: tokio::sync::watch::Receiver<SignalOutcome>,
}

impl StopLatch {
    fn install() -> Self {
        let (sender, latch) = Self::manual();
        tokio::spawn(async move {
            let outcome = shutdown_signal().await.map_err(|error| error.to_string());
            let _ = sender.send(Some(outcome));
        });
        latch
    }

    fn manual() -> (tokio::sync::watch::Sender<SignalOutcome>, Self) {
        let (sender, receiver) = tokio::sync::watch::channel(None);
        (sender, Self { receiver })
    }

    /// The stop already requested, if any. A failed signal listener counts as
    /// a stop: the process can no longer be stopped gracefully.
    fn requested(&self) -> Option<Result<&'static str, String>> {
        self.receiver
            .borrow()
            .as_ref()
            .map(|outcome| match outcome {
                Ok(signal) => Ok(signal.as_str()),
                Err(error) => Err(error.clone()),
            })
    }

    /// Resolves once a stop is requested. Cancel-safe.
    async fn wait(&self) -> Result<&'static str, RuntimeError> {
        let mut receiver = self.receiver.clone();
        loop {
            if let Some(outcome) = receiver.borrow_and_update().as_ref() {
                return outcome
                    .as_ref()
                    .map(|signal| signal.as_str())
                    .map_err(|error| format!("shutdown signal listener failed: {error}").into());
            }
            if receiver.changed().await.is_err() {
                // The listener task is gone without a signal; never resolve.
                std::future::pending::<()>().await;
            }
        }
    }
}

/// How a generation ended, after its in-flight transaction was settled.
#[derive(Debug)]
enum GenerationEnd {
    /// A stop (signal or MAX_EVENTS) was requested and the in-flight
    /// transaction, if any, committed. Clean exit.
    Stopped,
    /// A stop was requested but the in-flight transaction failed to commit (it
    /// was aborted, or its abort failed too). Terminal: never retried, exits
    /// non-zero, because those offsets are not durable.
    StopDrainFailed {
        reason: &'static str,
        error: KafkaTransportError,
    },
}

/// The settled outcome of one generation before logging.
enum Settled<T> {
    Stopped {
        reason: &'static str,
        committed: Option<CommittedTransaction<T>>,
    },
    StopDrainFailed {
        reason: &'static str,
        error: KafkaTransportError,
    },
    /// The generation failed without a stop intent of its own; the
    /// supervisor decides between recovery and exit.
    Failed {
        error: RuntimeError,
        drain_error: Option<KafkaTransportError>,
    },
}

/// K1. Settle the transaction still in `pipeline` and keep the stop intent
/// independent of that drain's result: once `served` says stop, a failed
/// drain is a terminal stop failure, never a generation error that recovery
/// could retry.
async fn finish_generation<T: Send + 'static>(
    served: Result<&'static str, RuntimeError>,
    pipeline: &mut TransactionPipeline<T>,
) -> Settled<T> {
    // IDR-094. Whatever ended the loop, the batch already handed to Kafka is
    // settled here - committed or aborted - before this generation returns,
    // so no transaction outlives its generation or races the next producer.
    let drained = pipeline.complete().await;
    match (served, drained) {
        (Ok(reason), Ok(committed)) => Settled::Stopped { reason, committed },
        (Ok(reason), Err(error)) => Settled::StopDrainFailed { reason, error },
        (Err(error), drained) => Settled::Failed {
            error,
            drain_error: drained.err(),
        },
    }
}

/// INCIDENT 2026-10-15. Consecutive in-place rewinds (no batch committed in
/// between) before a rebalance abort falls back to rebuilding the generation.
const MAX_CONSECUTIVE_REWINDS: u32 = 3;

/// INCIDENT 2026-10-15. Whether a failed pass is rewound inside the same
/// generation (same consumer and producer: no LeaveGroup/JoinGroup) instead of
/// ending it. Only an abort caused by a group change qualifies, its drain must
/// be clean or the same kind, no stop may be pending (the supervisor owns stop
/// handling), and the consecutive-rewind budget must remain. Everything else -
/// including an abort failure, which replaces the error - rebuilds as before.
fn rewind_in_generation(
    error: &RuntimeError,
    drain_error: Option<&KafkaTransportError>,
    stop_requested: bool,
    consecutive_rewinds: u32,
) -> bool {
    let rebalance_abort = |value: &KafkaTransportError| value.is_rebalance_abort();
    !stop_requested
        && consecutive_rewinds < MAX_CONSECUTIVE_REWINDS
        && error
            .downcast_ref::<KafkaTransportError>()
            .is_some_and(rebalance_abort)
        && drain_error.is_none_or(rebalance_abort)
}

/// The error a failed generation ends with when it is not rewound in place.
/// A non-retryable drain failure takes precedence over a retryable served
/// error, so settling the transaction can never downgrade a fatal outcome
/// into a generation rebuild.
fn generation_failure(
    error: RuntimeError,
    drain_error: Option<KafkaTransportError>,
) -> RuntimeError {
    match drain_error {
        Some(drain)
            if retryable_runtime_error(&error) && !should_retry_transport(drain.retry_class()) =>
        {
            drain.into()
        }
        _ => error,
    }
}

/// RB1 (review 2026-10-15). Whether a failed rewind attempt is tried again on
/// the same consumer: only when the rewind itself was invalidated by a group
/// change (the epoch fence of `rewind_to_committed`), no stop is pending and
/// the consecutive-rewind budget (which counts this attempt) remains.
fn rewind_again(rewind_error: &KafkaTransportError, stop_requested: bool, attempts: u32) -> bool {
    rewind_error.is_rebalance_abort() && !stop_requested && attempts < MAX_CONSECUTIVE_REWINDS
}

/// INCIDENT 2026-10-15 / RB1. Settle a failed pass: either rewind the same
/// consumer to its committed offsets (returns the number of partitions moved)
/// or end the generation with the error the supervisor must classify.
///
/// A failed rewind ends the generation with the rewind's own typed error, not
/// with the rebalance that started it (which stays in the log): a
/// non-retryable rewind failure (`Configuration` from an unexpected committed
/// offset, `Fencing`) must reach the supervisor as non-retryable, while a
/// retryable one (a timeout) still rebuilds the generation. Every attempt,
/// including one fenced by a rebalance during the rewind, counts toward
/// [`MAX_CONSECUTIVE_REWINDS`].
async fn recover_in_place<Rewind, Attempt>(
    error: RuntimeError,
    drain_error: Option<KafkaTransportError>,
    stop_requested: impl Fn() -> bool,
    consecutive_rewinds: &mut u32,
    generation: u64,
    mut rewind: Rewind,
) -> Result<usize, RuntimeError>
where
    Rewind: FnMut() -> Attempt,
    Attempt: Future<Output = Result<usize, KafkaTransportError>>,
{
    if !rewind_in_generation(
        &error,
        drain_error.as_ref(),
        stop_requested(),
        *consecutive_rewinds,
    ) {
        return Err(generation_failure(error, drain_error));
    }
    loop {
        *consecutive_rewinds += 1;
        match rewind().await {
            Ok(moved) => return Ok(moved),
            Err(rewind_error) => {
                let again = rewind_again(&rewind_error, stop_requested(), *consecutive_rewinds);
                log_line(
                    true,
                    json!({
                        "event": "qdl_realtime_core_rewind_failed",
                        "generation": generation,
                        "error": error.to_string(),
                        "rewind_error": rewind_error.to_string(),
                        "rewind_retryable": should_retry_transport(rewind_error.retry_class()),
                        "consecutive": *consecutive_rewinds,
                        "rewind_again": again,
                    }),
                );
                if !again {
                    return Err(rewind_error.into());
                }
            }
        }
    }
}

async fn run_generation(
    config: &RuntimeConfig,
    generation: u64,
    backoff: &BackoffPolicy,
    stop: &StopLatch,
    data_clock: &DataClock,
) -> Result<GenerationEnd, RuntimeError> {
    check_data_clock(data_clock)?;
    // Built here only to refuse an invalid core before any Kafka client exists.
    let mut core = RealtimeCore::new(config.core.clone())?;
    let approved_subscriptions = approved_subscription_scope(config);
    // Refuse a non-shared authority before any Kafka client exists, as before.
    output_targets(config.authority.mode)?;
    let bridge = TransactionalKafkaBridge::new(
        &kafka_config()?,
        TransactionalShadowTopics {
            raw_inputs: config.raw_topics.clone(),
            canonical: config.core.canonical_stream.clone(),
            quarantine: config.core.quarantine_stream.clone(),
        },
        &config.transactional_id,
    )?;
    bridge.apply_authority(config.authority.clone()).await?;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "qdl_realtime_core_started",
            "authority": authority_mode_name(config.authority.mode),
            "generation": generation,
            "bindings": config.core.bindings.len(),
            "approved_subscriptions": approved_subscriptions.len(),
            "batch_size": config.batch_size,
            "commit_pipeline_depth": 1,
            "public_writes": 0,
            "legacy_writes": 0,
        }))?
    );

    let mut progress = Progress::default();
    let mut rewinds = 0_u64;
    let mut consecutive_rewinds = 0_u32;
    let stop_reason = loop {
        let mut pipeline = CommitPipeline::new();
        let batches_before = progress.batches;
        let served = guard_data_clock(
            data_clock,
            serve_generation(
                config,
                generation,
                backoff,
                &mut core,
                &approved_subscriptions,
                &bridge,
                &mut pipeline,
                &mut progress,
                stop,
                data_clock,
            ),
        )
        .await
        .and_then(std::convert::identity);
        match finish_generation(served, &mut pipeline).await {
            Settled::Stopped { reason, committed } => {
                progress.record_commit(committed, generation, config.metrics_every_batches)?;
                break reason;
            }
            Settled::StopDrainFailed { reason, error } => {
                // The stop stands; the failed commit stays visible and is never
                // reported as a clean stop.
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_realtime_core_stop_drain_failed",
                        "generation": generation,
                        "reason": reason,
                        "processed": progress.processed,
                        "batches": progress.batches,
                        "error": error.to_string(),
                    }))?
                );
                return Ok(GenerationEnd::StopDrainFailed { reason, error });
            }
            Settled::Failed { error, drain_error } => {
                if let Some(commit_error) = &drain_error {
                    eprintln!(
                        "{}",
                        serde_json::to_string(&json!({
                            "event": "qdl_realtime_core_inflight_commit_failed",
                            "generation": generation,
                            "error": commit_error.to_string(),
                        }))?
                    );
                }
                if progress.batches > batches_before {
                    consecutive_rewinds = 0;
                }
                // INCIDENT 2026-10-15. The aborted batch (and the one read
                // behind it) are discarded with the core state they produced:
                // a fresh core replays them from the committed offsets, exactly
                // as a new generation would, but on the same group member.
                let started = tokio::time::Instant::now();
                let origin = error.to_string();
                let moved = recover_in_place(
                    error,
                    drain_error,
                    || stop.requested().is_some(),
                    &mut consecutive_rewinds,
                    generation,
                    || bridge.rewind_to_committed(),
                )
                .await?;
                rewinds += 1;
                core = RealtimeCore::new(config.core.clone())?;
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_realtime_core_rebalance_rewind",
                        "generation": generation,
                        "rewind": rewinds,
                        "consecutive": consecutive_rewinds,
                        "partitions": moved,
                        "rewind_ms": started.elapsed().as_millis() as u64,
                        "error": origin,
                    }))?
                );
            }
        }
    };
    bridge.unsubscribe();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "qdl_realtime_core_stopped",
            "generation": generation,
            "processed": progress.processed,
            "canonical": progress.canonical,
            "quarantines": progress.quarantines,
            "duplicates": progress.duplicates,
            "filtered": progress.filtered,
            "ignored_out_of_scope": progress.ignored_out_of_scope,
            "scope_quarantines": progress.scope_quarantines,
            "batches": progress.batches,
            "reason": stop_reason,
            "rebalance_rewinds": rewinds,
        }))?
    );
    Ok(GenerationEnd::Stopped)
}

/// The consume-normalize-commit loop of one generation. Returns the stop
/// reason; the caller settles the transaction still in `pipeline`.
///
/// IDR-094. Before this, each batch was read, normalized and then committed
/// with nothing read while the commit was in flight. Now batch N's
/// transaction runs on its own task while batch N+1 is read and normalized;
/// N+1's transaction is prepared only after N committed and carries only
/// N+1's own input offsets, so there is still one open transaction per
/// producer and the exactly-once boundary is unchanged.
#[allow(clippy::too_many_arguments)]
async fn serve_generation(
    config: &RuntimeConfig,
    generation: u64,
    backoff: &BackoffPolicy,
    core: &mut RealtimeCore,
    approved_subscriptions: &HashSet<String>,
    bridge: &TransactionalKafkaBridge,
    pipeline: &mut CommitPipeline,
    progress: &mut Progress,
    stop: &StopLatch,
    data_clock: &DataClock,
) -> Result<&'static str, RuntimeError> {
    let (canonical_target, quarantine_target) = output_targets(config.authority.mode)?;
    let mut receive_failures = 0_u32;
    let stop_reason = 'service: loop {
        let submitted = progress.processed + pipeline.in_flight_inputs();
        if config.max_events > 0 && submitted >= config.max_events {
            break 'service "MAX_EVENTS";
        }
        // K3. Read before this batch's first input is polled: a revocation
        // served while it is collected, or while it (or the batch before it)
        // is in flight, makes its transaction abort instead of commit.
        let rebalance_epoch = bridge.rebalance_epoch();
        let first = loop {
            let received = tokio::select! {
                result = bridge.next() => result,
                result = stop.wait() => break 'service result?,
                // Idle input must not hide a finished (or failed) commit.
                committed = pipeline.complete(), if pipeline.is_in_flight() => {
                    progress.record_commit(committed?, generation, config.metrics_every_batches)?;
                    continue;
                }
            };
            match received {
                Ok(input) => {
                    receive_failures = 0;
                    break input;
                }
                Err(error) if should_retry_receive_in_generation(&error, receive_failures) => {
                    receive_failures = receive_failures.saturating_add(1);
                    eprintln!(
                        "{}",
                        serde_json::to_string(&json!({
                            "event": "qdl_realtime_core_receive_retry",
                            "generation": generation,
                            "attempt": receive_failures,
                            "max_attempts": MAX_IN_GENERATION_RECEIVE_RETRIES,
                            "error": error.to_string(),
                        }))?
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(
                            backoff.delay_ms(
                                receive_failures.saturating_sub(1),
                                receive_failures as u16,
                            ),
                        )) => {}
                        result = stop.wait() => break 'service result?,
                    }
                }
                Err(error) => return Err(error.into()),
            }
        };
        let collect_started = tokio::time::Instant::now();
        let batch_deadline = collect_started + Duration::from_millis(config.batch_wait_ms);
        let mut inputs = vec![first];
        while inputs.len() < config.batch_size
            && (config.max_events == 0 || submitted + (inputs.len() as u64) < config.max_events)
        {
            let received = tokio::select! {
                biased;
                result = stop.wait() => break 'service result?,
                result = receive_batch_input(batch_deadline, bridge.next()) => result,
            };
            match received {
                Some(Ok(input)) => {
                    receive_failures = 0;
                    inputs.push(input);
                }
                Some(Err(error))
                    if should_retry_receive_in_generation(&error, receive_failures) =>
                {
                    receive_failures = receive_failures.saturating_add(1);
                    eprintln!(
                        "{}",
                        serde_json::to_string(&json!({
                            "event": "qdl_realtime_core_receive_deferred",
                            "generation": generation,
                            "attempt": receive_failures,
                            "max_attempts": MAX_IN_GENERATION_RECEIVE_RETRIES,
                            "error": error.to_string(),
                        }))?
                    );
                    break;
                }
                Some(Err(error)) => return Err(error.into()),
                None => break,
            }
        }

        progress
            .collect_latency
            .observe(collect_started.elapsed().as_nanos().min(i64::MAX as u128) as i64);
        let normalize_started = tokio::time::Instant::now();
        let normalized_at_ns = now_ns()?;
        let mut outputs = vec![];
        for input in &mut inputs {
            // Data time affects normalization freshness only; all telemetry and
            // ingress/resnapshot quarantine timestamps retain the batch wall clock.
            let processing_at_ns = data_clock.now_ns(normalized_at_ns)?;
            let raw = RawProviderEnvelope::decode(input.record.payload.as_slice())?;
            // The commit needs only the input cursor. The raw bytes move into
            // the lineage header of this input's canonical output instead of
            // being copied there (IDR-094); the header itself is unchanged.
            let mut raw_payload = Some(std::mem::take(&mut input.record.payload));
            // R1.29. How old a frame already is when this core first sees it:
            // the ingestor's produce, Kafka, and this consume. It is reported
            // rather than written into the envelope, because a canonical record
            // must stay a pure function of the raw event - putting a wall clock
            // in it breaks replay determinism, which
            // `transport_replay_is_byte_deterministic_across_fresh_cores`
            // catches. Every four-quantity report before this printed
            // `received -> published = 0 ms`, which was two copies of one field
            // subtracting to zero, not a measurement.
            let age_ns = normalized_at_ns.saturating_sub(raw.received_at_ns);
            progress.observe_raw_age(raw.transport_protocol, age_ns);
            progress
                .raw_age_by_partition
                .entry((
                    input.cursor.stream.clone(),
                    input.cursor.transport_partition,
                ))
                .or_default()
                .observe(age_ns);
            let mut result = if !is_approved_subscription(&raw, approved_subscriptions) {
                if config.strict_subscription_scope {
                    progress.scope_quarantines = progress.scope_quarantines.saturating_add(1);
                    core.quarantine_raw(
                        &raw,
                        QuarantineReason::FencingRejected,
                        "undeclared raw subscription in strict scope",
                        normalized_at_ns,
                    )
                } else {
                    progress.ignored_out_of_scope = progress.ignored_out_of_scope.saturating_add(1);
                    continue;
                }
            } else if raw.authority_revision != config.authority.revision {
                if config.strict_subscription_scope {
                    progress.scope_quarantines = progress.scope_quarantines.saturating_add(1);
                    core.quarantine_raw(
                        &raw,
                        QuarantineReason::FencingRejected,
                        "raw authority revision does not match strict runtime authority",
                        normalized_at_ns,
                    )
                } else {
                    return Err("raw authority revision does not match runtime authority".into());
                }
            } else {
                match core.process_at_transport_offset(
                    raw.clone(),
                    processing_at_ns,
                    input.cursor.offset,
                ) {
                    Ok(result) => result,
                    Err(error) if config.strict_subscription_scope => {
                        let Some((reason, summary)) = strict_quarantine_reason(&error) else {
                            return Err(error.into());
                        };
                        progress.scope_quarantines = progress.scope_quarantines.saturating_add(1);
                        core.quarantine_raw(&raw, reason, summary, normalized_at_ns)
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            if config.book_resync_quarantine
                && is_approved_subscription(&raw, approved_subscriptions)
                && raw.authority_revision == config.authority.revision
            {
                if let Some(request) = core.l2_resnapshot_request(&raw, normalized_at_ns) {
                    result.quarantines.push(request);
                }
            }
            progress.canonical += result.canonical.len() as u64;
            progress.quarantines += result.quarantines.len() as u64;
            progress.duplicates += result.duplicates as u64;
            progress.filtered += result.filtered as u64;
            if let Some(outcome) = result.filtered_outcome {
                *progress.filtered_by_outcome.entry(outcome).or_insert(0) += result.filtered as u64;
            }
            let canonical_outputs = result.canonical.len();
            for (index, record) in result.canonical.into_iter().enumerate() {
                outputs.push(TransactionalKafkaOutput {
                    record,
                    publication: PublicationContext {
                        slice_id: config.authority.slice_id.clone(),
                        authority_revision: raw.authority_revision,
                        shard_id: config.shard_id.clone(),
                        lease_epoch: raw.lease_epoch,
                        target: canonical_target,
                    },
                    raw_provider_envelope: if index + 1 == canonical_outputs {
                        raw_payload.take()
                    } else {
                        raw_payload.clone()
                    },
                });
            }
            for record in result.quarantines {
                outputs.push(TransactionalKafkaOutput {
                    record,
                    publication: PublicationContext {
                        slice_id: config.authority.slice_id.clone(),
                        authority_revision: raw.authority_revision,
                        shard_id: config.shard_id.clone(),
                        lease_epoch: raw.lease_epoch,
                        target: quarantine_target,
                    },
                    raw_provider_envelope: None,
                });
            }
        }
        progress
            .normalize_latency
            .observe(normalize_started.elapsed().as_nanos().min(i64::MAX as u128) as i64);
        // Batch N must have committed before batch N+1 is fence-checked and
        // begun: one open transaction per producer, batches in order.
        let previous = pipeline.complete().await?;
        progress.record_commit(previous, generation, config.metrics_every_batches)?;
        check_data_clock(data_clock)?;
        let transaction = bridge
            .prepare_commit(&inputs, outputs, rebalance_epoch)
            .await?;
        // prepare_commit returns an unstarted future. Expiry here must not
        // submit it; already-submitted work is settled by finish_generation.
        check_data_clock(data_clock)?;
        let previous = pipeline.submit(inputs.len() as u64, transaction).await?;
        progress.record_commit(previous, generation, config.metrics_every_batches)?;
    };
    Ok(stop_reason)
}

fn log_line(stream_err: bool, value: serde_json::Value) {
    let line = value.to_string();
    if stream_err {
        eprintln!("{line}");
    } else {
        println!("{line}");
    }
}

/// K1. Generation supervisor. A retryable generation failure starts a new
/// generation after backoff, as before; but once a stop is requested - before
/// a generation, inside one, or during the backoff - no further generation is
/// constructed. A stop whose drain failed exits non-zero.
async fn supervise<Run, Generation>(
    stop: &StopLatch,
    backoff: &BackoffPolicy,
    mut run: Run,
) -> Result<(), RuntimeError>
where
    Run: FnMut(u64) -> Generation,
    Generation: Future<Output = Result<GenerationEnd, RuntimeError>>,
{
    let mut generation = 0_u64;
    let mut failures = 0_u32;
    loop {
        if let Some(requested) = stop.requested() {
            // Between generations nothing is in flight.
            return requested.map(|_| ()).map_err(Into::into);
        }
        generation = generation.saturating_add(1);
        match run(generation).await {
            Ok(GenerationEnd::Stopped) => return Ok(()),
            Ok(GenerationEnd::StopDrainFailed { reason, error }) => {
                return Err(format!(
                    "stopped on {reason} but the in-flight transaction did not commit: {error}"
                )
                .into());
            }
            Err(error) if stop.requested().is_some() => {
                log_line(
                    true,
                    json!({
                        "event": "qdl_realtime_core_retry_suppressed_by_stop",
                        "generation": generation,
                        "error": error.to_string(),
                    }),
                );
                return Err(error);
            }
            Err(error) if retryable_runtime_error(&error) => {
                failures = failures.saturating_add(1);
                log_line(
                    true,
                    json!({
                        "event": "qdl_realtime_core_retry",
                        "generation": generation,
                        "attempt": failures,
                        "error": error.to_string(),
                    }),
                );
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(
                        backoff.delay_ms(failures, failures.min(10_000) as u16),
                    )) => {}
                    stopped = stop.wait() => {
                        let reason = stopped?;
                        log_line(
                            false,
                            json!({
                                "event": "qdl_realtime_core_stopped",
                                "generation": generation,
                                "reason": reason,
                                "during_retry_backoff": true,
                            }),
                        );
                        return Ok(());
                    }
                }
            }
            Err(error) => return Err(error),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), RuntimeError> {
    let config_path = env::args()
        .nth(1)
        .ok_or("usage: qdl-realtime-core CONFIG.json")?;
    let config: RuntimeConfig = serde_json::from_slice(&tokio::fs::read(config_path).await?)?;
    config.validate()?;
    // Load once before any listener/client, retaining failure latches across retries.
    // Feature-disabled binaries reject any TEST clock environment configuration.
    let data_clock = DataClock::from_env(cfg!(feature = "isolated-replay-clock"))?;
    check_data_clock(&data_clock)?;
    // This listener is private to the existing stable-internal network and is
    // absent unless the sealed C3.6 runtime binding enables it.  It survives
    // Kafka generation retries so all query roles share one Rust coordinator.
    let admission_server = ProviderAdmissionServer::start_from_environment().await?;
    let backoff = BackoffPolicy {
        initial_ms: 500,
        maximum_ms: 30_000,
        multiplier: 2,
        jitter_bps: 2_000,
    }
    .validate()?;
    let stop = StopLatch::install();
    let result = supervise(&stop, &backoff, |generation| {
        run_generation(&config, generation, &backoff, &stop, &data_clock)
    })
    .await;
    if let Some(server) = admission_server {
        server.stop().await?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn batch_deadline_does_not_slide_with_continuous_arrivals() {
        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_millis(25);
        let mut received = Vec::new();
        for offset in 1..256 {
            let value = receive_batch_input(deadline, async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                offset
            })
            .await;
            match value {
                Some(value) => received.push(value),
                None => break,
            }
        }
        assert_eq!(received, vec![1, 2]);
        assert!(started.elapsed() <= Duration::from_millis(26));
    }

    #[tokio::test(start_paused = true)]
    async fn batch_deadline_idle_wait_uses_remaining_budget() {
        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_millis(25);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            receive_batch_input(deadline, std::future::pending::<u64>()).await,
            None
        );
        assert!(started.elapsed() <= Duration::from_millis(26));
    }

    #[tokio::test(start_paused = true)]
    async fn batch_deadline_expired_does_not_consume_ready_record() {
        let deadline = tokio::time::Instant::now();
        let mut polled = false;
        let result = receive_batch_input(deadline, async {
            polled = true;
            42
        })
        .await;
        assert_eq!(result, None);
        assert!(!polled);
    }

    #[tokio::test(start_paused = true)]
    async fn batch_deadline_preserves_burst_order_and_receive_error() {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(25);
        let mut offsets = Vec::new();
        for offset in 0..256 {
            offsets.push(
                receive_batch_input(deadline, std::future::ready(offset))
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(offsets, (0..256).collect::<Vec<_>>());
        assert_eq!(
            receive_batch_input(deadline, std::future::ready(Err::<(), _>("receive"))).await,
            Some(Err("receive"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn batch_deadline_cancellation_does_not_eat_next_record() {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(25);
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tokio::select! {
            biased;
            _ = tokio::time::sleep(Duration::from_millis(5)) => {},
            _ = receive_batch_input(deadline, rx.recv()) => panic!("input should be waiting"),
        }
        tx.send(42).await.unwrap();
        assert_eq!(
            receive_batch_input(deadline, rx.recv()).await,
            Some(Some(42))
        );
    }

    #[test]
    fn raw_age_is_split_by_transport_and_the_total_is_unchanged() {
        let mut progress = Progress::default();
        progress.observe_raw_age(TransportProtocol::Websocket as i32, 10_000_000);
        progress.observe_raw_age(TransportProtocol::Websocket as i32, 30_000_000);
        progress.observe_raw_age(TransportProtocol::Http as i32, 2_000_000_000);
        progress.observe_raw_age(TransportProtocol::FileReplay as i32, 5_000_000);
        assert_eq!(progress.raw_age.count, 4);
        assert_eq!(progress.raw_age.max_ns, 2_000_000_000);
        assert_eq!(progress.raw_age_websocket.count, 2);
        assert_eq!(progress.raw_age_websocket.max_ns, 30_000_000);
        assert_eq!(progress.raw_age_rest.count, 2);
        assert_eq!(progress.raw_age_rest.max_ns, 2_000_000_000);
        let committed = CommittedTransaction {
            value: (),
            inputs: 4,
            commit_ns: 1,
            wait_ns: 0,
        };
        progress.record_commit(Some(committed), 1, 1).unwrap();
        assert_eq!(progress.raw_age_websocket.count, 0);
        assert_eq!(progress.raw_age_rest.count, 0);
    }

    #[test]
    fn processed_advances_only_when_a_transaction_commits() {
        let mut progress = Progress::default();
        progress.raw_age.observe(5);
        progress
            .record_commit::<()>(None, 1, 2)
            .expect("no commit is a no-op");
        assert_eq!((progress.processed, progress.batches), (0, 0));
        let committed = |inputs| CommittedTransaction {
            value: (),
            inputs,
            commit_ns: 30_000_000,
            wait_ns: 10_000_000,
        };
        progress.record_commit(Some(committed(256)), 1, 2).unwrap();
        assert_eq!((progress.processed, progress.batches), (256, 1));
        assert_eq!(progress.commit_hidden.max_ns, 20_000_000);
        assert_eq!(
            progress.raw_age.count, 1,
            "spans survive until a progress line"
        );
        progress.record_commit(Some(committed(10)), 1, 2).unwrap();
        assert_eq!((progress.processed, progress.batches), (266, 2));
        assert_eq!(
            progress.raw_age.count, 0,
            "progress line resets its interval"
        );
        assert_eq!(progress.commit_wait.count, 0);
    }

    use std::cell::Cell;

    fn test_backoff() -> BackoffPolicy {
        BackoffPolicy {
            initial_ms: 500,
            maximum_ms: 30_000,
            multiplier: 2,
            jitter_bps: 0,
        }
        .validate()
        .unwrap()
    }

    fn sigterm() -> SignalOutcome {
        Some(Ok(ShutdownSignal::Terminate))
    }

    /// One generation shaped like `run_generation`: batch N's commit is in
    /// flight, SIGTERM arrives, the loop stops and `finish_generation`
    /// settles the pending commit with `commit` as its outcome.
    async fn generation_with_pending_commit(
        stop: &StopLatch,
        signal: &tokio::sync::watch::Sender<SignalOutcome>,
        commit: Result<u64, KafkaTransportError>,
    ) -> Result<GenerationEnd, RuntimeError> {
        let mut pipeline = TransactionPipeline::<u64>::new();
        pipeline
            .submit(3, async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                commit
            })
            .await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(pipeline.is_in_flight(), "commit is still pending");
        signal.send(sigterm()).unwrap();
        let served = tokio::select! {
            result = stop.wait() => result,
            _ = std::future::pending::<()>() => unreachable!(),
        };
        match finish_generation(served, &mut pipeline).await {
            Settled::Stopped { reason, committed } => {
                assert_eq!(reason, "SIGTERM");
                assert_eq!(committed.map(|done| done.inputs), Some(3));
                Ok(GenerationEnd::Stopped)
            }
            Settled::StopDrainFailed { reason, error } => {
                Ok(GenerationEnd::StopDrainFailed { reason, error })
            }
            Settled::Failed { error, .. } => Err(error),
        }
    }

    async fn supervise_sigterm_with_pending_commit(
        commit: impl Fn() -> Result<u64, KafkaTransportError>,
    ) -> (Result<(), RuntimeError>, u64) {
        let (signal, stop) = StopLatch::manual();
        let generations = Cell::new(0_u64);
        let result = supervise(&stop, &test_backoff(), |_| {
            generations.set(generations.get() + 1);
            generation_with_pending_commit(&stop, &signal, commit())
        })
        .await;
        (result, generations.get())
    }

    #[tokio::test(start_paused = true)]
    async fn sigterm_with_pending_commit_that_commits_exits_cleanly() {
        let (result, generations) = supervise_sigterm_with_pending_commit(|| Ok(7)).await;
        result.expect("a committed drain is a clean stop");
        assert_eq!(generations, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn sigterm_with_pending_commit_that_fails_retryably_exits_without_new_generation() {
        let (result, generations) = supervise_sigterm_with_pending_commit(|| {
            Err(KafkaTransportError::SnapshotTimeout("commit".into()))
        })
        .await;
        let error = result.expect_err("a failed drain must not be reported as success");
        assert!(error.to_string().contains("SIGTERM"));
        assert!(error.to_string().contains("did not commit"));
        assert_eq!(generations, 1, "no generation after the signal");
    }

    #[tokio::test(start_paused = true)]
    async fn sigterm_with_pending_commit_whose_abort_fails_exits_without_new_generation() {
        // A failed abort replaces the commit error (`run_kafka_transaction`)
        // and is a retryable Kafka error, which used to start generation N+1.
        let (result, generations) = supervise_sigterm_with_pending_commit(|| {
            Err(KafkaTransportError::Kafka(
                rdkafka::error::KafkaError::Canceled,
            ))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(generations, 1, "no generation after the signal");
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_failure_without_a_stop_still_recovers() {
        let (signal, stop) = StopLatch::manual();
        let generations = Cell::new(0_u64);
        let result = supervise(&stop, &test_backoff(), |generation| {
            generations.set(generations.get() + 1);
            let signal = &signal;
            async move {
                if generation == 1 {
                    Err(KafkaTransportError::SnapshotTimeout("receive".into()).into())
                } else {
                    signal.send(sigterm()).unwrap();
                    Ok(GenerationEnd::Stopped)
                }
            }
        })
        .await;
        result.unwrap();
        assert_eq!(generations.get(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn sigterm_during_retry_backoff_constructs_no_new_generation() {
        let (signal, stop) = StopLatch::manual();
        let generations = Cell::new(0_u64);
        let backoff = test_backoff();
        let started = tokio::time::Instant::now();
        let supervised = supervise(&stop, &backoff, |_| {
            generations.set(generations.get() + 1);
            async { Err(KafkaTransportError::SnapshotTimeout("receive".into()).into()) }
        });
        let signal_later = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            signal.send(sigterm()).unwrap();
            std::future::pending::<()>().await;
        };
        let result = tokio::select! {
            result = supervised => result,
            _ = signal_later => unreachable!(),
        };
        result.expect("nothing is in flight during backoff: clean stop");
        assert_eq!(generations.get(), 1);
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "backoff cut short"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failure_after_a_stop_is_not_retried_and_signal_before_start_runs_nothing() {
        let (signal, stop) = StopLatch::manual();
        let generations = Cell::new(0_u64);
        let result = supervise(&stop, &test_backoff(), |_| {
            generations.set(generations.get() + 1);
            let signal = &signal;
            async move {
                signal.send(sigterm()).unwrap();
                Err(KafkaTransportError::SnapshotTimeout("receive".into()).into())
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(generations.get(), 1);

        let (signal, stop) = StopLatch::manual();
        signal.send(sigterm()).unwrap();
        let generations = Cell::new(0_u64);
        supervise(&stop, &test_backoff(), |_| {
            generations.set(generations.get() + 1);
            async { Ok(GenerationEnd::Stopped) }
        })
        .await
        .unwrap();
        assert_eq!(generations.get(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_signal_listener_is_a_failing_stop() {
        let (signal, stop) = StopLatch::manual();
        signal.send(Some(Err("no signal handler".into()))).unwrap();
        assert!(stop.wait().await.is_err());
        assert!(matches!(stop.requested(), Some(Err(_))));
    }

    #[test]
    fn only_group_change_aborts_rewind_inside_the_generation() {
        let rebalanced = || KafkaTransportError::Rebalanced {
            observed: 0,
            current: 2,
        };
        let boxed = |error: KafkaTransportError| -> RuntimeError { error.into() };
        // The incident shape: a served revocation aborted the batch, nothing
        // else in flight -> rewind on the same consumer, no new generation.
        assert!(rewind_in_generation(&boxed(rebalanced()), None, false, 0));
        // The drain may report the same abort.
        assert!(rewind_in_generation(
            &boxed(rebalanced()),
            Some(&rebalanced()),
            false,
            MAX_CONSECUTIVE_REWINDS - 1
        ));
        // Budget exhausted, a pending stop, a different drain error, or any
        // other failure keeps the old path (rebuild via the supervisor).
        assert!(!rewind_in_generation(
            &boxed(rebalanced()),
            None,
            false,
            MAX_CONSECUTIVE_REWINDS
        ));
        assert!(!rewind_in_generation(&boxed(rebalanced()), None, true, 0));
        assert!(!rewind_in_generation(
            &boxed(rebalanced()),
            Some(&KafkaTransportError::Configuration("abort failed".into())),
            false,
            0
        ));
        assert!(!rewind_in_generation(
            &boxed(KafkaTransportError::Configuration("abort failed".into())),
            None,
            false,
            0
        ));
        assert!(!rewind_in_generation(
            &boxed(KafkaTransportError::SnapshotTimeout("x".into())),
            Some(&rebalanced()),
            false,
            0
        ));
        let core_error: RuntimeError = "core rejected input".into();
        assert!(!rewind_in_generation(&core_error, None, false, 0));
    }

    fn rebalance_origin() -> RuntimeError {
        KafkaTransportError::Rebalanced {
            observed: 4,
            current: 6,
        }
        .into()
    }

    /// RB1. One generation that aborts on a group change and then rewinds
    /// with the scripted attempt results; returns the generation's outcome
    /// and how many rewind attempts were made.
    async fn generation_with_rewind_attempts(
        attempts: Vec<Result<usize, KafkaTransportError>>,
    ) -> (Result<usize, RuntimeError>, u32, usize) {
        let attempts = std::cell::RefCell::new(attempts.into_iter());
        let calls = Cell::new(0_usize);
        let mut consecutive = 0_u32;
        let result = recover_in_place(
            rebalance_origin(),
            None,
            || false,
            &mut consecutive,
            1,
            || {
                calls.set(calls.get() + 1);
                let next = attempts.borrow_mut().next().expect("scripted attempt");
                async move { next }
            },
        )
        .await;
        (result, consecutive, calls.get())
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_rewind_failure_rebuilds_the_generation() {
        let (result, _, calls) = generation_with_rewind_attempts(vec![Err(
            KafkaTransportError::SnapshotTimeout("committed offsets".into()),
        )])
        .await;
        let error = result.unwrap_err();
        assert_eq!(calls, 1);
        assert!(matches!(
            error.downcast_ref::<KafkaTransportError>(),
            Some(KafkaTransportError::SnapshotTimeout(_))
        ));
        // Through the supervisor: a second generation is built.
        let (signal, stop) = StopLatch::manual();
        let generations = Cell::new(0_u64);
        let mut error = Some(error);
        supervise(&stop, &test_backoff(), |_| {
            generations.set(generations.get() + 1);
            let first = error.take();
            let signal = &signal;
            async move {
                match first {
                    Some(error) => Err(error),
                    None => {
                        signal.send(sigterm()).unwrap();
                        Ok(GenerationEnd::Stopped)
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(generations.get(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn non_retryable_rewind_failure_reaches_the_supervisor_as_non_retryable() {
        for rewind_error in [
            KafkaTransportError::Configuration("unexpected committed offset".into()),
            KafkaTransportError::Fencing("authority".into()),
        ] {
            let (result, _, calls) = generation_with_rewind_attempts(vec![Err(rewind_error)]).await;
            let error = result.unwrap_err();
            assert_eq!(calls, 1);
            assert!(
                !retryable_runtime_error(&error),
                "the initiating rebalance must not mask {error}"
            );
            let (_signal, stop) = StopLatch::manual();
            let generations = Cell::new(0_u64);
            let mut error = Some(error);
            let supervised = supervise(&stop, &test_backoff(), |_| {
                generations.set(generations.get() + 1);
                let next = error.take();
                async move { Err(next.expect("only one generation")) }
            })
            .await;
            assert!(supervised.is_err());
            assert_eq!(generations.get(), 1, "no rebuild after a fatal rewind");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rewind_fenced_by_a_rebalance_rewinds_again_within_the_budget() {
        let fenced = || KafkaTransportError::Rebalanced {
            observed: 6,
            current: 8,
        };
        let (result, consecutive, calls) =
            generation_with_rewind_attempts(vec![Err(fenced()), Ok(4)]).await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!((consecutive, calls), (2, 2));
        // Every fenced attempt counts toward the cap; the last one ends the
        // generation with a retryable rebalance (the supervisor rebuilds).
        let (result, consecutive, calls) =
            generation_with_rewind_attempts(vec![Err(fenced()), Err(fenced()), Err(fenced())])
                .await;
        let error = result.unwrap_err();
        assert!(retryable_runtime_error(&error));
        assert_eq!((consecutive, calls), (MAX_CONSECUTIVE_REWINDS, 3));
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_drain_failure_takes_precedence_over_the_served_error() {
        let mut consecutive = 0_u32;
        let error = recover_in_place(
            KafkaTransportError::SnapshotTimeout("receive".into()).into(),
            Some(KafkaTransportError::Fencing("authority".into())),
            || false,
            &mut consecutive,
            1,
            || -> std::future::Ready<Result<usize, KafkaTransportError>> {
                panic!("not a rebalance abort: no rewind")
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<KafkaTransportError>(),
            Some(KafkaTransportError::Fencing(_))
        ));
        // A rebalance abort whose drain failed fatally is not rewound either.
        let error = recover_in_place(
            rebalance_origin(),
            Some(KafkaTransportError::Configuration("abort failed".into())),
            || false,
            &mut consecutive,
            1,
            || -> std::future::Ready<Result<usize, KafkaTransportError>> {
                panic!("fatal drain: no rewind")
            },
        )
        .await
        .unwrap_err();
        assert!(!retryable_runtime_error(&error));
        assert_eq!(consecutive, 0);
        // A retryable drain keeps the served error.
        let error = generation_failure(
            rebalance_origin(),
            Some(KafkaTransportError::SnapshotTimeout("drain".into())),
        );
        assert!(matches!(
            error.downcast_ref::<KafkaTransportError>(),
            Some(KafkaTransportError::Rebalanced { .. })
        ));
    }

    #[test]
    fn runtime_retries_only_retryable_or_capacity_transport_errors() {
        assert!(should_retry_transport(RetryClass::Retryable));
        assert!(should_retry_transport(RetryClass::Capacity));
        assert!(!should_retry_transport(RetryClass::NonRetryable));
    }

    #[test]
    fn retryable_receive_errors_preserve_generation_only_within_bound() {
        let retryable = KafkaTransportError::SnapshotTimeout("transient receive".into());
        assert!(should_retry_receive_in_generation(&retryable, 0));
        assert!(should_retry_receive_in_generation(
            &retryable,
            MAX_IN_GENERATION_RECEIVE_RETRIES - 1,
        ));
        assert!(!should_retry_receive_in_generation(
            &retryable,
            MAX_IN_GENERATION_RECEIVE_RETRIES,
        ));

        let fatal = KafkaTransportError::Configuration("bad config".into());
        assert!(!should_retry_receive_in_generation(&fatal, 0));
    }

    #[test]
    fn shared_raw_scope_accepts_only_exact_subscription_ids() {
        let approved_subscriptions = HashSet::from(["dnse-vn30f1m-trade-stable-001".to_owned()]);
        let approved = RawProviderEnvelope {
            subscription_id: "dnse-vn30f1m-trade-stable-001".into(),
            ..Default::default()
        };
        let foreign = RawProviderEnvelope {
            subscription_id: "binance-usdm-btcusdt-trade-stable-001".into(),
            ..Default::default()
        };

        assert!(is_approved_subscription(&approved, &approved_subscriptions));
        assert!(!is_approved_subscription(&foreign, &approved_subscriptions));
    }

    #[test]
    fn strict_scope_errors_map_to_durable_quarantine_reasons() {
        assert_eq!(
            strict_quarantine_reason(&CoreError::UnknownBinding),
            Some((
                QuarantineReason::FencingRejected,
                "raw identity does not match declared strict scope",
            )),
        );
        assert_eq!(
            strict_quarantine_reason(&CoreError::Decode("bad frame".into())),
            Some((
                QuarantineReason::SemanticInvalid,
                "provider frame cannot be canonicalized in strict scope",
            )),
        );
        assert_eq!(
            strict_quarantine_reason(&CoreError::Configuration("bad config".into())),
            None,
        );
    }

    #[test]
    fn shared_core_binds_private_outputs_to_authority_mode() {
        assert_eq!(
            output_targets(AuthorityMode::RustShadow).unwrap(),
            (SinkTarget::ShadowCanonical, SinkTarget::ShadowQuarantine),
        );
        assert_eq!(
            output_targets(AuthorityMode::RustPrimary).unwrap(),
            (SinkTarget::PrimaryCanonical, SinkTarget::PrimaryQuarantine),
        );
        assert!(output_targets(AuthorityMode::RustCanary).is_err());
    }
}

#[cfg(test)]
mod data_clock_boundary_tests {
    use super::*;

    #[tokio::test]
    async fn default_clock_preserves_batch_wall_time_and_future_result() {
        let clock = DataClock::Real;
        assert_eq!(clock.now_ns(123456789), Ok(123456789));
        assert!(check_data_clock(&clock).is_ok());
        assert_eq!(guard_data_clock(&clock, async { 42 }).await.unwrap(), 42);
    }

    #[cfg(feature = "isolated-replay-clock")]
    mod replay {
        use super::*;
        use qdl_contracts::qdl::marketdata::v2::EventEnvelope;
        use qdl_contracts::qdl::provider::v1::{
            CaptureBoundary, TransportCompression, TransportProtocol,
        };
        use sha2::{Digest, Sha256};
        use std::cell::Cell;
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };

        #[tokio::test]
        async fn expired_clock_is_fatal_latched_and_does_not_poll_work() {
            let (clock, expire) = DataClock::test_fixture();
            expire(true);
            let polled = Cell::new(false);
            let error = guard_data_clock(&clock, async {
                polled.set(true);
            })
            .await
            .unwrap_err();
            assert_eq!(error.to_string(), qdl_test_data_clock::INVALID);
            assert!(!retryable_runtime_error(&error));
            assert!(!polled.get());
            expire(false);
            assert!(check_data_clock(&clock).is_err());
        }

        #[tokio::test(start_paused = true)]
        async fn idle_consumer_observes_expiry_within_poll_bound() {
            let (clock, expire) = DataClock::test_fixture();
            let started = tokio::time::Instant::now();
            let (result, ()) = tokio::join!(
                guard_data_clock(&clock, std::future::pending::<()>()),
                async {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    expire(true);
                }
            );
            assert!(result.is_err());
            assert!(started.elapsed() <= Duration::from_millis(25));
        }

        #[tokio::test(start_paused = true)]
        async fn collection_expiry_does_not_wait_for_batch_deadline() {
            let (clock, expire) = DataClock::test_fixture();
            let received = Cell::new(false);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            let (result, ()) = tokio::join!(
                guard_data_clock(
                    &clock,
                    receive_batch_input(deadline, async {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        received.set(true);
                        42
                    })
                ),
                async {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    expire(true);
                }
            );
            assert!(result.is_err());
            assert!(!received.get());
        }

        #[tokio::test(start_paused = true)]
        async fn expiry_settles_prior_commit_and_never_submits_next_batch() {
            for commit_succeeds in [true, false] {
                let (clock, expire) = DataClock::test_fixture();
                let finished = Arc::new(AtomicBool::new(false));
                let finished_commit = finished.clone();
                let mut pipeline = TransactionPipeline::<u64>::new();
                pipeline
                    .submit(3, async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        finished_commit.store(true, Ordering::SeqCst);
                        if commit_succeeds {
                            Ok(42)
                        } else {
                            Err(KafkaTransportError::SnapshotTimeout(
                                "injected commit failure".into(),
                            ))
                        }
                    })
                    .await
                    .unwrap();
                let submitted_next = Cell::new(false);
                let (served, ()) = tokio::join!(
                    guard_data_clock(&clock, async {
                        pipeline.complete().await?;
                        check_data_clock(&clock)?;
                        submitted_next.set(true);
                        Ok::<_, RuntimeError>("UNEXPECTED_SUBMIT")
                    }),
                    async {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        expire(true);
                    }
                );
                assert!(pipeline.is_in_flight());
                let settled =
                    finish_generation(served.and_then(std::convert::identity), &mut pipeline).await;
                match settled {
                    Settled::Failed { error, drain_error } => {
                        assert_eq!(error.to_string(), qdl_test_data_clock::INVALID);
                        assert!(!retryable_runtime_error(&error));
                        assert_eq!(drain_error.is_none(), commit_succeeds);
                    }
                    _ => panic!("clock expiry must remain a fatal generation failure"),
                }
                assert!(finished.load(Ordering::SeqCst));
                assert!(!pipeline.is_in_flight());
                assert!(!submitted_next.get());
            }
        }

        #[tokio::test]
        async fn expiry_after_preparation_drops_unstarted_transaction() {
            let (clock, expire) = DataClock::test_fixture();
            let began = Arc::new(AtomicBool::new(false));
            let began_task = began.clone();
            let transaction = async move {
                began_task.store(true, Ordering::SeqCst);
                Ok::<_, KafkaTransportError>(42_u64)
            };
            let mut pipeline = TransactionPipeline::new();
            // Match the production boundary: prepared future exists, but has not been submitted.
            expire(true);
            let result = async {
                check_data_clock(&clock)?;
                pipeline.submit(1, transaction).await?;
                Ok::<_, RuntimeError>(())
            }
            .await;
            assert!(result.is_err());
            assert!(!began.load(Ordering::SeqCst));
            assert!(!pipeline.is_in_flight());
        }

        #[tokio::test]
        async fn clock_failure_does_not_start_another_generation() {
            let (clock, expire) = DataClock::test_fixture();
            expire(true);
            let (_signal, stop) = StopLatch::manual();
            let generations = Cell::new(0);
            let backoff = BackoffPolicy {
                initial_ms: 1,
                maximum_ms: 1,
                multiplier: 2,
                jitter_bps: 0,
            }
            .validate()
            .unwrap();
            let result = supervise(&stop, &backoff, |_| {
                generations.set(generations.get() + 1);
                async {
                    check_data_clock(&clock)?;
                    Ok(GenerationEnd::Stopped)
                }
            })
            .await;
            assert!(result.is_err());
            assert_eq!(generations.get(), 1);
        }

        fn mark_fixture() -> (RealtimeCoreConfig, Vec<RawProviderEnvelope>) {
            // Reuse the production core's historical OKX MARK/INDEX fixture and cadence.
            let mark_frame = br#"{"arg":{"channel":"mark-price","instId":"DOGE-USDT-SWAP"},"data":[{"instId":"DOGE-USDT-SWAP","markPx":"0.14525","ts":"1786352400000"}]}"#;
            let index_frame = br#"{"arg":{"channel":"index-tickers","instId":"DOGE-USDT"},"data":[{"instId":"DOGE-USDT","idxPx":"0.14510","ts":"1786352400100"}]}"#;
            let mark = json!({
                "provider":"OKX_DIRECT", "venue":"OKX", "market":"SWAP",
                "product_type":"PERPETUAL", "native_symbol":"DOGE-USDT-SWAP",
                "native_channel":"mark-price", "physical_native_symbol":"DOGE-USDT-SWAP",
                "physical_native_channel":"mark-price", "provider_kind":"okx_mark_price",
                "instrument_uid":"fixture-doge", "instrument_id":"OKX.SWAP.PERPETUAL.DOGE-USDT",
                "instrument_revision":1, "instrument_catalog_revision":3,
                "source_id":"fixture-mark-index", "source_role":"PRIMARY",
                "normalizer_version":"qdl-rust-core/2.0.0", "sequence_policy":"NONE",
                "mark_index":{"component":"MARK", "quiet_after_ms":250}
            });
            let mut index = mark.clone();
            index["physical_native_symbol"] = json!("DOGE-USDT");
            index["physical_native_channel"] = json!("index-tickers");
            index["provider_kind"] = json!("okx_index_price");
            index["mark_index"] = json!({"component":"INDEX", "quiet_after_ms":500});
            let config: RealtimeCoreConfig = serde_json::from_value(json!({
                "canonical_stream":"test.canonical", "quarantine_stream":"test.quarantine",
                "allow_test_provenance":true, "dedup_capacity":16, "bindings":[mark,index]
            }))
            .unwrap();
            let mut raws = Vec::new();
            for (binding, (frame, received_at_ns)) in config.bindings.iter().zip([
                (mark_frame.as_slice(), 1_786_352_400_000_000_000_i64),
                (index_frame.as_slice(), 1_786_352_400_100_000_000_i64),
            ]) {
                raws.push(RawProviderEnvelope {
                    raw_schema_name: "qdl.provider.raw".into(),
                    raw_schema_major: 1,
                    capture_id: Sha256::digest(frame)[..16].to_vec(),
                    provider: binding.provider.clone(),
                    venue: binding.venue.clone(),
                    market: binding.market.clone(),
                    product_type: binding.product_type.clone(),
                    native_symbol: binding.physical_native_symbol.clone().unwrap(),
                    native_channel: binding.physical_native_channel.clone().unwrap(),
                    subscription_id: binding.source_id.clone(),
                    source_session_id: "qdl-test-lane-001-1-1700000000000000000".into(),
                    connection_generation: 1,
                    lease_epoch: 7,
                    authority_revision: 1,
                    partition_plan_epoch: 1,
                    received_at_ns,
                    transport_protocol: TransportProtocol::Websocket as i32,
                    transport_compression: TransportCompression::None as i32,
                    capture_boundary: CaptureBoundary::PostDecompression as i32,
                    raw_frame_bytes: frame.to_vec(),
                    raw_frame_sha256: Sha256::digest(frame).to_vec(),
                    adapter_version: "unit-fixture/1".into(),
                    config_revision: 1,
                    instrument_catalog_revision: 3,
                    correlation_id: "unit-fixture-only".into(),
                    test_provenance: true,
                    ..Default::default()
                });
            }
            (config, raws)
        }

        #[test]
        fn historical_mark_uses_data_time_but_does_not_relax_quiet_cadence() {
            let (config, raws) = mark_fixture();
            let origin = raws[0].received_at_ns;
            let wall_ns = origin + 86_400_000_000_000;
            let (historical, expire) = DataClock::test_fixture_at(origin + 100_000_000);
            let (quiet_expired, _) = DataClock::test_fixture_at(origin + 601_000_000);
            for (clock, emits) in [
                (&DataClock::Real, false),
                (&historical, true),
                (&quiet_expired, false),
            ] {
                let mut core = RealtimeCore::new(config.clone()).unwrap();
                let mut result = None;
                for (offset, raw) in raws.iter().enumerate() {
                    result = Some(
                        core.process_at_transport_offset(
                            raw.clone(),
                            clock.now_ns(wall_ns).unwrap(),
                            offset as u64,
                        )
                        .unwrap(),
                    );
                }
                let result = result.unwrap();
                assert!(result.quarantines.is_empty());
                if emits {
                    assert_eq!(result.canonical.len(), 1);
                    let event =
                        EventEnvelope::decode(result.canonical[0].payload.as_slice()).unwrap();
                    assert_eq!(event.source_event_time_ns, origin);
                    assert_eq!(event.received_at_ns, origin);
                    assert_eq!(event.normalized_at_ns, raws[1].received_at_ns);
                } else {
                    assert!(result.canonical.is_empty());
                    assert_eq!(
                        result.filtered_outcome,
                        Some("MARK_INDEX_COMPONENT_EXPIRED")
                    );
                }
            }
            // Ingress quarantine uses operational wall time, not the TEST data clock.
            let core = RealtimeCore::new(config).unwrap();
            let evidence = core.quarantine_raw(
                &raws[0],
                QuarantineReason::FencingRejected,
                "unit fixture",
                wall_ns,
            );
            assert_eq!(evidence.quarantines[0].accepted_at_ns, wall_ns);
            expire(true);
            assert!(historical.now_ns(wall_ns).is_err());
        }
    }
}
