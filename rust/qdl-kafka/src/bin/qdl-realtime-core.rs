#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashSet};
use std::env;
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;
use qdl_contracts::qdl::provider::v1::{QuarantineReason, RawProviderEnvelope};
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
}

impl Progress {
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

async fn run_generation(
    config: &RuntimeConfig,
    generation: u64,
    backoff: &BackoffPolicy,
    stop: &StopLatch,
) -> Result<GenerationEnd, RuntimeError> {
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
    let mut pipeline = CommitPipeline::new();
    let served = serve_generation(
        config,
        generation,
        backoff,
        &mut core,
        &approved_subscriptions,
        &bridge,
        &mut pipeline,
        &mut progress,
        stop,
    )
    .await;
    let stop_reason = match finish_generation(served, &mut pipeline).await {
        Settled::Stopped { reason, committed } => {
            progress.record_commit(committed, generation, config.metrics_every_batches)?;
            reason
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
            if let Some(commit_error) = drain_error {
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "event": "qdl_realtime_core_inflight_commit_failed",
                        "generation": generation,
                        "error": commit_error.to_string(),
                    }))?
                );
            }
            return Err(error);
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
            progress.raw_age.observe(age_ns);
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
                    normalized_at_ns,
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
        let transaction = bridge
            .prepare_commit(&inputs, outputs, rebalance_epoch)
            .await?;
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
        run_generation(&config, generation, &backoff, &stop)
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
