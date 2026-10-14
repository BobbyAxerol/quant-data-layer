//! `MarketDataStreamService`, the native KN-2 service (K2.1-K2.4).
//!
//! Every RPC follows `qdl/stream/grpc_service.py` step for step - access
//! checks, their order, status codes and control frames - on top of the
//! shared committed reader ([`crate::hub`]), bounded subscriber queues
//! ([`crate::subscription`]), the coalesced replay coordinator ([`crate::replay`]) and
//! the snapshot/status read view ([`crate::readview`]).
//!
//! Subscribe: REPLAYING -> replay `(cursor, barrier)` from the ring or a
//! replay reader -> LIVE -> queued live records. Overflow of a lossless
//! record ends the stream with BACKPRESSURE `RATE_LIMITED` +
//! RESOURCE_EXHAUSTED and the last resume token (D5). A replica behind the
//! cursor waits a bounded time, then answers UNAVAILABLE `REPLICA_LAGGING`
//! (retryable, never a false expiry).

use crate::auth::{authenticate, Access, AccessError, Principal, RequestQuota};
use crate::auth::{HISTORY_READ, SNAPSHOT_READ, STATUS_READ};
use crate::authority::{reauthorize, Authority, AuthorityHandle, Entitlement};
use crate::generated::query_v2 as query;
use crate::generated::query_v2::market_data_stream_service_server::MarketDataStreamService;
use crate::hub::{live_weight, Hub, LagHistogram, LiveRecord, NearRegistration};
use crate::readview::ReadView;
use crate::replay::{
    ReplayCoordinator, ReplayEnd, ReplayEpisode, ReplayLimits, ReplayRequest, Replayed, REPLAY_CHANNEL,
};
use crate::requirement::{
    delivery_decision, is_product, require_requirement, Delivery, StreamRequirement,
};
use crate::subscription::{ByteBudget, Charge, Next, Subscription};
use qdl_contracts::cursor_v3::{CursorError, CursorV3Claims, CursorV3Codec};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

/// Records handed from a subscription task to the transport. Kept tiny: a
/// deeper channel would hold stale latest-state records behind the 8-deep
/// queue (DL-V2 R1.8) and hide a slow client from the overflow signal.
pub const OUTBOUND_HANDOFF: usize = 2;

/// How often a send waiting for live budget to copy a record retries.
const HANDOFF_BUDGET_RETRY: Duration = Duration::from_millis(5);

/// Most event records one batched `SubscribeResponse` carries, whatever the
/// client asks for (`SubscribeRequest.max_records_per_response`, SDK 2.3.0).
pub const MAX_RECORDS_PER_RESPONSE: usize = 256;

/// A batch stops growing once its records reach this many encoded bytes, far
/// below the 4 MiB default receive limit of gRPC clients. A single record is
/// always sent whatever its size, as before batching.
pub const MAX_BATCH_BYTES: usize = 1 << 20;
/// Encoded upper bound of the three gateway clocks a Subscribe event record
/// carries (SDK 2.4.0): 3 x (1-byte tag + 10-byte varint).
const TIMING_BYTES: usize = 33;

/// Records one response may carry for this request: 1 (the pre-2.3.0 shape)
/// unless the client asked for more and the feed is lossless. Latest-state
/// and lifecycle-coalesced feeds keep one record per response, so records
/// stay in the coalescing queue (DL-V2 R1.8) instead of the handoff.
pub fn response_batch_limit(requested: u32, feed: &str) -> usize {
    let lossless = qdl_contracts::delivery::delivery_policy(feed, None)
        == Ok(qdl_contracts::delivery::DeliveryPolicy::Lossless);
    if lossless {
        (requested as usize).clamp(1, MAX_RECORDS_PER_RESPONSE)
    } else {
        1
    }
}

#[derive(Clone, Debug)]
pub struct StreamLimits {
    pub cursor_ttl_seconds: u64,
    pub replay: ReplayLimits,
    /// How long a replica behind a cursor may take to reach it.
    pub catchup_deadline: Duration,
    /// How long a client may accept nothing before its stream is closed.
    pub slow_consumer_after: Duration,
    pub queue_bytes_per_subscription: usize,
    pub max_subscriptions: usize,
    /// Concurrent Replay RPCs per replica; per consumer they are bounded by
    /// the manifest's `max_streams` like Subscribe (F4 admission).
    pub max_replay_rpcs: usize,
    pub replay_page_default: u64,
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            cursor_ttl_seconds: 3_600,
            replay: ReplayLimits::default(),
            catchup_deadline: Duration::from_secs(10),
            slow_consumer_after: Duration::from_secs(10),
            // A fair share of the default 32 MiB live budget (memory.rs).
            queue_bytes_per_subscription: 16 << 20,
            max_subscriptions: 384,
            max_replay_rpcs: 32,
            replay_page_default: 1_000,
        }
    }
}

#[derive(Default)]
pub struct Metrics {
    pub subscriptions_opened: AtomicU64,
    pub subscriptions_active: AtomicU64,
    pub delivered: AtomicU64,
    pub replayed: AtomicU64,
    pub aged_out_at_read: AtomicU64,
    pub overflow: AtomicU64,
    pub refused: AtomicU64,
    pub revoked: AtomicU64,
    pub lagging: AtomicU64,
    pub expired: AtomicU64,
    pub replay_rpcs: AtomicU64,
    /// Send path of live records: queued -> taken by the stream task, and
    /// the wait for the transport to accept it (HTTP/2 flow control and a
    /// slow client show up here, not in the dispatch lag).
    pub queue_wait: LagHistogram,
    pub handoff_wait: LagHistogram,
}

pub struct GatewayState {
    pub authority: Arc<AuthorityHandle>,
    pub quota: Arc<dyn RequestQuota>,
    pub codec: CursorV3Codec,
    pub hub: Arc<Hub>,
    pub replay: Arc<ReplayCoordinator>,
    pub read_view: Arc<dyn ReadView>,
    pub budget: Arc<ByteBudget>,
    pub limits: StreamLimits,
    pub metrics: Metrics,
    streams: Mutex<HashMap<String, u64>>,
    replays: Mutex<HashMap<String, u64>>,
    next_id: AtomicU64,
    /// Set on SIGTERM/SIGINT: every open stream ends with a typed,
    /// retryable UNAVAILABLE so clients fail over at once.
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl GatewayState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        authority: Arc<AuthorityHandle>,
        quota: Arc<dyn RequestQuota>,
        codec: CursorV3Codec,
        hub: Arc<Hub>,
        replay: Arc<ReplayCoordinator>,
        read_view: Arc<dyn ReadView>,
        budget: Arc<ByteBudget>,
        limits: StreamLimits,
    ) -> Self {
        // Dispatch charges each decoded record to the live budget once.
        hub.attach_live_budget(budget.clone());
        Self {
            authority,
            quota,
            codec,
            hub,
            replay,
            read_view,
            budget,
            limits,
            metrics: Metrics::default(),
            streams: Mutex::new(HashMap::new()),
            replays: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            shutdown: tokio::sync::watch::channel(false).0,
        }
    }

    /// Begin a graceful stop (planned restart / `docker stop`).
    pub fn shut_down(&self) {
        self.shutdown.send_replace(true);
    }

    pub fn open_streams(&self) -> u64 {
        self.streams
            .lock()
            .map(|streams| streams.values().sum())
            .unwrap_or(0)
    }

    pub fn open_replays(&self) -> u64 {
        self.replays
            .lock()
            .map(|replays| replays.values().sum())
            .unwrap_or(0)
    }
}

/// Releases a Replay RPC's admission slot when its task ends.
struct ReplaySlot {
    state: Arc<GatewayState>,
    consumer_id: String,
}

impl Drop for ReplaySlot {
    fn drop(&mut self) {
        if let Ok(mut replays) = self.state.replays.lock() {
            if let Some(count) = replays.get_mut(&self.consumer_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    replays.remove(&self.consumer_id);
                }
            }
        }
    }
}

/// Releases the per-consumer stream slot when the subscription task ends.
struct StreamSlot {
    state: Arc<GatewayState>,
    consumer_id: String,
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        if let Ok(mut streams) = self.state.streams.lock() {
            if let Some(count) = streams.get_mut(&self.consumer_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    streams.remove(&self.consumer_id);
                }
            }
        }
        self.state
            .metrics
            .subscriptions_active
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// Removes the subscriber from the hub and releases its queue.
struct Registered {
    hub: Arc<Hub>,
    partition: i32,
    key: Vec<u8>,
    subscription: Arc<Subscription>,
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.hub
            .unregister(self.partition, &self.key, self.subscription.id);
        self.subscription.close();
    }
}

/// Stops a blocking replay reader when its async owner goes away.
struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

pub struct Gateway {
    pub state: Arc<GatewayState>,
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos() as u64)
        .unwrap_or_default()
}

fn credentials<T>(request: &Request<T>) -> (String, String, String) {
    let value = |name: &str| {
        request
            .metadata()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    (
        value("authorization"),
        value("x-qdl-consumer-id"),
        value("x-qdl-purpose"),
    )
}

fn cursor_status(error: CursorError) -> Status {
    match error {
        CursorError::Expired(reason) => Status::out_of_range(format!("CURSOR_EXPIRED:{reason}")),
        CursorError::Invalid(reason) => {
            Status::invalid_argument(format!("CURSOR_INVALID:{reason}"))
        }
    }
}

fn control(state: i32, code: &str, detail: &str, high: i64, token: &str) -> query::StreamRecord {
    query::StreamRecord {
        logical_offset: 0,
        resume_token: token.to_owned(),
        payload: Some(query::stream_record::Payload::Control(
            query::StreamControl {
                state,
                code: code.to_owned(),
                detail: detail.to_owned(),
                high_watermark: high.max(0) as u64,
            },
        )),
        ..Default::default()
    }
}

fn subscribe_control(
    state: query::StreamControlState,
    code: &str,
    detail: &str,
    high: i64,
    token: &str,
) -> query::SubscribeResponse {
    query::SubscribeResponse {
        record: Some(control(state as i32, code, detail, high, token)),
        records: Vec::new(),
    }
}

impl GatewayState {
    /// `DataPlaneIdentityService.authenticate` against the current authority
    /// (JWT + manifest + shared quota), off the async runtime.
    async fn access<T>(
        self: &Arc<Self>,
        request: &Request<T>,
    ) -> Result<(Access, Arc<Authority>, u64), Status> {
        let (authorization, consumer, purpose) = credentials(request);
        let (authority, generation) = self.authority.snapshot();
        let quota = self.quota.clone();
        let held = authority.clone();
        let result = tokio::task::spawn_blocking(move || {
            authenticate(
                &held.jwt,
                &held.bundle,
                quota.as_ref(),
                &authorization,
                &consumer,
                &purpose,
            )
        })
        .await
        .map_err(|_| Status::internal("authentication task failed"))?;
        match result {
            Ok(access) => Ok((access, authority, generation)),
            Err(error) => Err(self.refuse(error.to_status())),
        }
    }

    fn refuse(&self, status: Status) -> Status {
        self.metrics.refused.fetch_add(1, Ordering::Relaxed);
        status
    }

    fn sign(&self, claims: &CursorV3Claims, offset: i64) -> Result<String, Status> {
        let now = now_ns();
        let mut next = claims.clone();
        next.key_id = self.codec.active_key_id().to_owned();
        next.source_offset = offset.max(0) as u64;
        next.issued_at_ns = now;
        next.expires_at_ns = now + self.limits.cursor_ttl_seconds * 1_000_000_000;
        self.codec
            .encode(&next)
            .map_err(|error| Status::internal(format!("cursor signing failed: {}", error.reason())))
    }
}

type SubscribeStream = Pin<Box<dyn Stream<Item = Result<query::SubscribeResponse, Status>> + Send>>;

fn subscription_handoff(
    receiver: mpsc::Receiver<Outbound<query::SubscribeResponse>>,
    terminal: Arc<Mutex<Option<Status>>>,
) -> SubscribeStream {
    Box::pin(
        ReceiverStream::new(receiver)
            // tonic polls here when it encodes the next message, i.e.
            // when HTTP/2 can take more: the per-record send stamp.
            .map(|(item, _charge)| item.map(stamp_send))
            // Drain the bounded handoff in order, then emit the terminal
            // status even if a stalled receiver outlived the producer task.
            .chain(tokio_stream::once(()).map(move |_| {
                Err(terminal
                    .lock()
                    .ok()
                    .and_then(|mut status| status.take())
                    .unwrap_or_else(|| {
                        Status::internal("subscription task ended without a terminal status")
                    }))
            })),
    )
}

#[cfg(feature = "isolated-replay-clock")]
fn test_clock_status() -> Status {
    Status::failed_precondition(crate::replay_clock::INVALID)
}

/// Race the entire TEST subscription lifecycle, including replay, blocked send,
/// catch-up and idle LIVE. Dropping the work cancels its replay reader as usual.
#[cfg(feature = "isolated-replay-clock")]
async fn test_clock_guard<F: std::future::Future>(hub: &Hub, work: F) -> Result<F::Output, Status> {
    tokio::select! {
        biased;
        () = hub.test_clock_invalidated() => Err(test_clock_status()),
        result = work => {
            hub.check_test_clock().map_err(|_| test_clock_status())?;
            Ok(result)
        }
    }
}

/// Reserve capacity first, then validate immediately before enqueue. A blocked
/// send cannot use the clock sample taken before it started waiting.
#[cfg(feature = "isolated-replay-clock")]
async fn test_clock_handoff(
    hub: &Hub,
    sender: &mpsc::Sender<Outbound<query::SubscribeResponse>>,
    item: Outbound<query::SubscribeResponse>,
) -> Result<bool, Status> {
    let permit = test_clock_guard(hub, sender.reserve()).await?;
    let Ok(permit) = permit else { return Ok(false) };
    hub.check_test_clock().map_err(|_| test_clock_status())?;
    permit.send(item);
    Ok(true)
}

/// TEST-only last boundary before tonic owns the response. A fault discards the
/// handoff, including charged records and controls, instead of draining it.
#[cfg(feature = "isolated-replay-clock")]
struct TestClockStream {
    inner: Option<SubscribeStream>,
    hub: Arc<Hub>,
    tick: tokio::time::Interval,
}

#[cfg(feature = "isolated-replay-clock")]
impl TestClockStream {
    fn new(inner: SubscribeStream, hub: Arc<Hub>) -> Self {
        let mut tick = tokio::time::interval(Duration::from_millis(25));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            inner: Some(inner),
            hub,
            tick,
        }
    }

    fn fault(&mut self) -> std::task::Poll<Option<Result<query::SubscribeResponse, Status>>> {
        drop(self.inner.take());
        std::task::Poll::Ready(Some(Err(test_clock_status())))
    }
}

#[cfg(feature = "isolated-replay-clock")]
impl Stream for TestClockStream {
    type Item = Result<query::SubscribeResponse, Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.inner.is_none() {
            return std::task::Poll::Ready(None);
        }
        if this.hub.check_test_clock().is_err() {
            return this.fault();
        }
        // Register a wake even if no producer, data, or terminal status arrives.
        while this.tick.poll_tick(cx).is_ready() {}
        let result = this.inner.as_mut().unwrap().as_mut().poll_next(cx);
        if this.hub.check_test_clock().is_err() {
            return this.fault();
        }
        if matches!(result, std::task::Poll::Ready(None)) {
            drop(this.inner.take());
        }
        result
    }
}

type ReplayStream = Pin<Box<dyn Stream<Item = Result<query::ReplayResponse, Status>> + Send>>;

#[tonic::async_trait]
impl MarketDataStreamService for Gateway {
    type SubscribeStream = SubscribeStream;
    type ReplayStream = ReplayStream;

    async fn subscribe(
        &self,
        request: Request<query::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let state = self.state.clone();
        let (access, authority, generation) = state.access(&request).await?;
        let body = request.into_inner();
        let requirement = body
            .requirement
            .as_ref()
            .ok_or_else(|| "requirement is required".to_owned())
            .and_then(StreamRequirement::from_proto)
            .map_err(|error| {
                state.refuse(Status::invalid_argument(format!("CURSOR_INVALID:{error}")))
            })?;
        access
            .require_consumer(&body.consumer_id)
            .and_then(|()| access.require_stream_read())
            .and_then(|()| require_requirement(&access.manifest, &requirement))
            .map_err(|error| state.refuse(access_status(error, "CURSOR_INVALID")))?;
        let buffer = if body.max_buffer_events == 0 {
            access.manifest.quotas.max_buffer_events
        } else {
            u64::from(body.max_buffer_events)
        };
        access
            .require_stream_buffer(buffer)
            .map_err(|error| state.refuse(error.to_status()))?;
        let delivery = &requirement.delivery;
        let binding = authority
            .bundle
            .binding_for(
                &delivery.instrument_uid,
                &delivery.feed,
                delivery.interval.as_deref(),
                &delivery.source_policy_id,
            )
            .cloned()
            .ok_or_else(|| {
                state.refuse(Status::invalid_argument(
                    "CURSOR_INVALID:cursor requirement has no matching stable binding",
                ))
            })?;
        let claims = state
            .codec
            .verify(
                &body.cursor_token,
                &body.consumer_id,
                &authority.expectation.environment,
                &delivery.digest(),
                &authority.expectation,
                now_ns(),
            )
            .map_err(|error| state.refuse(cursor_status(error)))?;
        if claims.product_key != binding.product_key {
            return Err(state.refuse(Status::invalid_argument("CURSOR_INVALID:SCOPE")));
        }
        let partition = claims.source_partition as i32;
        if state.hub.next_offset(partition).is_none() {
            return Err(state.refuse(Status::invalid_argument("CURSOR_INVALID:PARTITION")));
        }
        {
            let mut streams = state
                .streams
                .lock()
                .map_err(|_| Status::internal("stream registry poisoned"))?;
            let total: u64 = streams.values().sum();
            if total >= state.limits.max_subscriptions as u64 {
                return Err(state.refuse(Status::resource_exhausted(
                    "RATE_LIMITED:stream subscriber capacity exhausted",
                )));
            }
            let count = streams.entry(body.consumer_id.clone()).or_insert(0);
            if *count >= access.manifest.quotas.max_streams {
                return Err(state.refuse(Status::resource_exhausted(
                    "RATE_LIMITED:consumer concurrent stream quota exhausted",
                )));
            }
            *count += 1;
        }
        state
            .metrics
            .subscriptions_opened
            .fetch_add(1, Ordering::Relaxed);
        state
            .metrics
            .subscriptions_active
            .fetch_add(1, Ordering::Relaxed);
        let slot = StreamSlot {
            state: state.clone(),
            consumer_id: body.consumer_id.clone(),
        };
        let after = claims.source_offset as i64;
        let key = binding.physical_key.into_bytes();
        let subscription = Arc::new(Subscription::new(
            state.next_id.fetch_add(1, Ordering::Relaxed),
            body.consumer_id.clone(),
            requirement.clone(),
            buffer as usize,
            state.limits.queue_bytes_per_subscription,
            state.budget.clone(),
            after,
        ));
        let (barrier, ring) = match state.hub.try_register_near(
            partition, &key, subscription.clone(), after, REPLAY_CHANNEL,
            state.replay.budget(),
        ).map_err(Status::internal)? {
            NearRegistration::NeedReplay { barrier } => (barrier, None),
            NearRegistration::Registered { barrier, bridge, charge } =>
                (barrier, Some(RingReplay { records: bridge, charge })),
        };
        let registered = Registered {
            hub: state.hub.clone(),
            partition,
            key: key.clone(),
            subscription: subscription.clone(),
        };
        let (sender, receiver) = mpsc::channel(OUTBOUND_HANDOFF);
        let terminal = Arc::new(Mutex::new(None));
        let auth = Authorized::new(
            state.authority.clone(),
            &access,
            &body.consumer_id,
            Entitlement::Requirement(Box::new(requirement.clone())),
            generation,
        );
        let batch_limit = response_batch_limit(
            body.max_records_per_response,
            &subscription.requirement.delivery.feed,
        );
        #[cfg(feature = "isolated-replay-clock")]
        let clock_guard = state.hub.test_clock_enabled().then(|| state.hub.clone());
        let task = SubscriptionTask {
            report: Report {
                started: Some(Instant::now()),
                started_wall_ns: now_ns(),
                outcome: Mutex::new("ended"),
                ..Report::default()
            },
            state,
            _slot: slot,
            _registered: registered,
            subscription,
            sender,
            terminal: terminal.clone(),
            claims,
            request_token: body.cursor_token,
            auth,
            partition,
            key,
            after,
            batch_limit,
            replay_budget_status_v2: body.replay_budget_status_v2,
        };
        tokio::spawn(task.run(barrier, ring));
        let stream = subscription_handoff(receiver, terminal);
        #[cfg(feature = "isolated-replay-clock")]
        if let Some(hub) = clock_guard {
            return Ok(Response::new(Box::pin(TestClockStream::new(stream, hub))));
        }
        Ok(Response::new(stream))
    }

    async fn replay(
        &self,
        request: Request<query::ReplayRequest>,
    ) -> Result<Response<Self::ReplayStream>, Status> {
        let state = self.state.clone();
        let (access, authority, generation) = state.access(&request).await?;
        let body = request.into_inner();
        state.metrics.replay_rpcs.fetch_add(1, Ordering::Relaxed);
        access
            .require_consumer(&body.consumer_id)
            .and_then(|()| access.require_stream_read())
            .map_err(|error| state.refuse(error.to_status()))?;
        let claims = state
            .codec
            .verify_scope(
                &body.cursor_token,
                &body.consumer_id,
                &authority.expectation.environment,
                &authority.expectation,
                now_ns(),
            )
            .map_err(|error| state.refuse(cursor_status(error)))?;
        let binding = authority
            .bundle
            .binding_by_product(&claims.product_key)
            .cloned()
            .ok_or_else(|| state.refuse(Status::invalid_argument("CURSOR_INVALID:SCOPE")))?;
        access
            .require_feed_scope(&binding.instrument_uid, &binding.feed)
            .map_err(|error| state.refuse(error.to_status()))?;
        let limit = if body.limit == 0 {
            state.limits.replay_page_default
        } else {
            u64::from(body.limit)
        };
        access
            .require_stream_buffer(limit)
            .map_err(|error| state.refuse(error.to_status()))?;
        if limit > state.limits.replay.max_matched {
            return Err(state.refuse(Status::invalid_argument(
                "CURSOR_INVALID:requested replay limit exceeds the server bound",
            )));
        }
        let partition = claims.source_partition as i32;
        let barrier = state
            .hub
            .next_offset(partition)
            .ok_or_else(|| state.refuse(Status::invalid_argument("CURSOR_INVALID:PARTITION")))?;
        // Admission (F4): concurrent Replay RPCs are bounded per replica and
        // per consumer (manifest `max_streams`), as Subscribe is.
        {
            let mut replays = state
                .replays
                .lock()
                .map_err(|_| Status::internal("replay registry poisoned"))?;
            let total: u64 = replays.values().sum();
            if total >= state.limits.max_replay_rpcs as u64 {
                return Err(state.refuse(Status::resource_exhausted(
                    "RATE_LIMITED:replay capacity exhausted",
                )));
            }
            let count = replays.entry(body.consumer_id.clone()).or_insert(0);
            if *count >= access.manifest.quotas.max_streams {
                return Err(state.refuse(Status::resource_exhausted(
                    "RATE_LIMITED:consumer concurrent replay quota exhausted",
                )));
            }
            *count += 1;
        }
        let slot = ReplaySlot {
            state: state.clone(),
            consumer_id: body.consumer_id.clone(),
        };
        let auth = Authorized::new(
            state.authority.clone(),
            &access,
            &body.consumer_id,
            Entitlement::FeedScope {
                instrument_uid: binding.instrument_uid.clone(),
                feed: binding.feed.clone(),
            },
            generation,
        );
        let (sender, receiver) = mpsc::channel::<Outbound<query::ReplayResponse>>(OUTBOUND_HANDOFF);
        let after = claims.source_offset as i64;
        let key = binding.physical_key.clone().into_bytes();
        tokio::spawn(async move {
            let _slot = slot;
            let (mut records, cancel, reader) = spawn_scan(
                &state,
                partition,
                after,
                barrier,
                key,
                (binding.feed.clone(), binding.interval.clone()),
                Some(limit),
            );
            let _cancel = CancelOnDrop(cancel);
            let fail = |status: Status| {
                let sender = sender.clone();
                async move {
                    let _ = tokio::time::timeout(
                        Duration::from_secs(5),
                        sender.send((Err(status), None)),
                    )
                    .await;
                }
            };
            loop {
                let replayed = tokio::select! {
                    biased;
                    status = auth.revoked() => {
                        state.metrics.revoked.fetch_add(1, Ordering::Relaxed);
                        fail(status).await;
                        return;
                    }
                    replayed = records.recv() => replayed,
                };
                let Some(replayed) = replayed else {
                    break;
                };
                if let Err(status) = auth.check() {
                    state.metrics.revoked.fetch_add(1, Ordering::Relaxed);
                    fail(status).await;
                    return;
                }
                let (record, charge) = replayed.into_parts();
                let token = match state.sign(&claims, record.raw.offset) {
                    Ok(token) => token,
                    Err(status) => {
                        fail(status).await;
                        return;
                    }
                };
                let response = query::ReplayResponse {
                    record: Some(event_record(&record, token)),
                };
                let remaining = state.limits.slow_consumer_after;
                let sent = tokio::select! {
                    biased;
                    status = auth.revoked() => {
                        state.metrics.revoked.fetch_add(1, Ordering::Relaxed);
                        fail(status).await;
                        return;
                    }
                    sent = tokio::time::timeout(remaining, sender.send((Ok(response), Some(charge)))) => sent,
                };
                match sent {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => return,
                    Err(_) => {
                        fail(Status::resource_exhausted(
                            "RATE_LIMITED:replay client too slow; resume from the last token",
                        ))
                        .await;
                        return;
                    }
                }
            }
            let end = reader
                .await
                .unwrap_or(ReplayEnd::Error("replay coordinator dropped".into()));
            // ReplayRequest has no budget-status field: pre-2.4.1 mapping.
            if let Some(status) = replay_end_status(&state, &end, false) {
                fail(status).await;
            }
        });
        Ok(Response::new(Box::pin(
            ReceiverStream::new(receiver).map(|(item, _charge)| item),
        )))
    }

    async fn get_snapshot(
        &self,
        request: Request<query::GetSnapshotRequest>,
    ) -> Result<Response<query::GetSnapshotResponse>, Status> {
        let state = self.state.clone();
        let (access, _authority, _generation) = state.access(&request).await?;
        let body = request.into_inner();
        let requirement = body
            .requirement
            .as_ref()
            .ok_or_else(|| "requirement is required".to_owned())
            .and_then(StreamRequirement::from_proto)
            .map_err(|error| {
                state.refuse(Status::invalid_argument(format!(
                    "INVALID_ARGUMENT:{error}"
                )))
            })?;
        let permission = if requirement.has_warmup {
            HISTORY_READ
        } else {
            SNAPSHOT_READ
        };
        access
            .require_consumer(&body.consumer_id)
            .and_then(|()| access.require_permission(permission))
            .and_then(|()| require_requirement(&access.manifest, &requirement))
            .map_err(|error| state.refuse(access_status(error, "INVALID_ARGUMENT")))?;
        let proto = body.requirement.as_ref().expect("validated above");
        state
            .read_view
            .snapshot(&requirement, proto, &body.consumer_id)
            .await
            .map(Response::new)
            .map_err(|error| error.to_status())
    }

    async fn get_feed_status(
        &self,
        request: Request<query::GetFeedStatusRequest>,
    ) -> Result<Response<query::GetFeedStatusResponse>, Status> {
        let state = self.state.clone();
        let (access, _authority, _generation) = state.access(&request).await?;
        let body = request.into_inner();
        let requirement = body
            .requirement
            .as_ref()
            .ok_or_else(|| "requirement is required".to_owned())
            .and_then(StreamRequirement::from_proto)
            .map_err(|error| {
                state.refuse(Status::invalid_argument(format!(
                    "INVALID_ARGUMENT:{error}"
                )))
            })?;
        access
            .require_consumer(&body.consumer_id)
            .and_then(|()| access.require_permission(STATUS_READ))
            .and_then(|()| require_requirement(&access.manifest, &requirement))
            .map_err(|error| state.refuse(access_status(error, "INVALID_ARGUMENT")))?;
        let proto = body.requirement.as_ref().expect("validated above");
        state
            .read_view
            .status(&requirement, proto, &body.consumer_id)
            .await
            .map(Response::new)
            .map_err(|error| error.to_status())
    }
}

/// An access failure as the Python handlers answer it: a `ValueError`
/// becomes INVALID_ARGUMENT with the handler's prefix, every
/// `DataPlaneAccessError` is PERMISSION_DENIED with its detail.
fn access_status(error: AccessError, invalid_prefix: &str) -> Status {
    match error {
        AccessError::Invalid(detail) => {
            Status::invalid_argument(format!("{invalid_prefix}:{detail}"))
        }
        AccessError::RateLimited(detail) => Status::permission_denied(detail),
        other => other.to_status(),
    }
}

fn event_record(record: &LiveRecord, token: String) -> query::StreamRecord {
    query::StreamRecord {
        logical_offset: record.raw.offset.max(0) as u64,
        resume_token: token,
        payload: Some(query::stream_record::Payload::Event(
            record.envelope.clone(),
        )),
        ..Default::default()
    }
}

/// A Subscribe event record with the gateway clocks known when it is built
/// (SDK 2.4.0, diagnostic only): the hub dispatch (0 for replayed records)
/// and the moment this task took it. The send clock is set by
/// [`stamp_send`] when the transport takes the response.
fn subscribe_event_record(
    record: &LiveRecord,
    token: String,
    dequeued_wall_ns: u64,
) -> query::StreamRecord {
    query::StreamRecord {
        gateway_dispatch_wall_ns: record.dispatched_wall_ns,
        gateway_dequeue_wall_ns: dequeued_wall_ns,
        ..event_record(record, token)
    }
}

/// Stamp every event record of a Subscribe response with the moment the
/// transport takes it from the bounded handoff (`gateway_send_wall_ns`).
/// Controls stay unstamped. One clock read per response; records of a batch
/// leave together, so they share it.
fn stamp_send(mut response: query::SubscribeResponse) -> query::SubscribeResponse {
    let now = now_ns();
    let is_event = |record: &query::StreamRecord| {
        matches!(
            record.payload,
            Some(query::stream_record::Payload::Event(_))
        )
    };
    for record in response
        .record
        .iter_mut()
        .chain(response.records.iter_mut())
    {
        if is_event(record) {
            record.gateway_send_wall_ns = now;
        }
    }
    response
}

/// Typed outcome of a replay that did not simply complete.
///
/// `budget_status_v2` is the client's additive opt-in
/// (`SubscribeRequest.replay_budget_status_v2`, SDK 2.4.1, IDR-096). Only an
/// opted-in client, which bounds retries by progress, sees a replay work
/// budget as retryable RESOURCE_EXHAUSTED; every other client (and the Replay
/// RPC, which has no such field) keeps the pre-2.4.1 OUT_OF_RANGE
/// CURSOR_EXPIRED mapping and its fresh-snapshot recovery.
fn replay_end_status(
    state: &GatewayState,
    end: &ReplayEnd,
    budget_status_v2: bool,
) -> Option<Status> {
    let expired = |detail: String| {
        state.metrics.expired.fetch_add(1, Ordering::Relaxed);
        Some(Status::out_of_range(format!("CURSOR_EXPIRED:{detail}")))
    };
    match end {
        ReplayEnd::Complete | ReplayEnd::PageFull | ReplayEnd::Cancelled => None,
        ReplayEnd::Backlog if budget_status_v2 => Some(Status::resource_exhausted(
            "RATE_LIMITED:REPLAY_MATCHED_LIMIT:replay reached its matched-record bound; \
             resume from the last token",
        )),
        ReplayEnd::Backlog => expired(
            "replay backlog exceeds the bounded gateway window; a fresh snapshot is required"
                .into(),
        ),
        ReplayEnd::Retention => expired(
            "RETENTION:the cursor is below the retained committed log; a fresh snapshot is required"
                .into(),
        ),
        // Refused before any scan: a retry cannot progress, so this is
        // CURSOR_EXPIRED for every client (IDR-096 (A)).
        ReplayEnd::SpanLimit => expired(
            "REPLAY_SCAN_LIMIT:replay exceeds the bounded scan; a fresh snapshot is required"
                .into(),
        ),
        // A work budget is not retention loss. A scan may stop before any
        // token advances; callers must bound retries when there is no progress.
        ReplayEnd::ScanLimit(reason) if budget_status_v2 => Some(Status::resource_exhausted(
            format!(
                "RATE_LIMITED:{reason}:replay reached its bounded scan; \
                 resume from the last token; bound retries if no progress"
            ),
        )),
        // Pre-2.4.1 mapping: out of time is retryable (the token advanced
        // through everything delivered); a scan or byte bound is expiry.
        ReplayEnd::ScanLimit("REPLAY_TIME_LIMIT") => Some(Status::resource_exhausted(
            "RATE_LIMITED:REPLAY_TIME_LIMIT:replay did not complete within its time bound; \
             resume from the last token",
        )),
        ReplayEnd::ScanLimit(reason) => expired(format!(
            "{reason}:replay exceeds the bounded scan; a fresh snapshot is required"
        )),
        ReplayEnd::Corrupt { partition, offset } => {
            Some(data_integrity(*partition, *offset, "failed to decode"))
        }
        ReplayEnd::Error(error) => Some(Status::unavailable(format!(
            "DEPENDENCY_UNAVAILABLE:replay reader failed: {error}"
        ))),
    }
}

/// Submit a replay to the coordinator. Records of the product flow through a
/// bounded channel in offset order; the receiver yields how the replay ended.
/// Setting the cancel flag (or dropping the channel) ends it at the next
/// record or poll.
fn spawn_scan(
    state: &Arc<GatewayState>,
    partition: i32,
    after: i64,
    barrier: i64,
    key: Vec<u8>,
    product: (String, Option<String>),
    page: Option<u64>,
) -> (
    mpsc::Receiver<Replayed>,
    Arc<AtomicBool>,
    tokio::sync::oneshot::Receiver<ReplayEnd>,
) {
    spawn_scan_episode(state, partition, after, barrier, key, product, page,
        ReplayEpisode::new(Instant::now() + state.limits.replay.max_duration))
}

#[allow(clippy::too_many_arguments)]
fn spawn_scan_episode(
    state: &Arc<GatewayState>, partition: i32, after: i64, barrier: i64,
    key: Vec<u8>, product: (String, Option<String>), page: Option<u64>,
    episode: Arc<ReplayEpisode>,
) -> (mpsc::Receiver<Replayed>, Arc<AtomicBool>, tokio::sync::oneshot::Receiver<ReplayEnd>) {
    let (sender, receiver) = mpsc::channel::<Replayed>(REPLAY_CHANNEL);
    let (done, end) = tokio::sync::oneshot::channel();
    let cancel = Arc::new(AtomicBool::new(false));
    let deadline = episode.deadline;
    state.replay.submit(
        partition,
        ReplayRequest::new(
            after,
            barrier,
            key,
            product,
            page,
            sender,
            done,
            cancel.clone(),
            deadline,
        ).with_episode(episode),
    );
    (receiver, cancel, end)
}

/// What one subscription did, logged once when it ends (the native
/// `qdl_stream_subscription` report): the evidence that reconciles a client's
/// view with this replica.
#[derive(Default)]
struct Report {
    started: Option<Instant>,
    started_wall_ns: u64,
    replay_handoff_elapsed_ns: AtomicU64,
    replayed: AtomicU64,
    delivered: AtomicU64,
    aged_out: AtomicU64,
    /// `ended` (client closed or run window over), a status description,
    /// or `backpressure`.
    outcome: Mutex<&'static str>,
}

/// A stream's admission, re-checked against every authority reload (Astra
/// KN-2 R1 F1): bound to the generation it was admitted under, checked
/// before every send and raced against every wait, so a reload during
/// replay, catch-up or a blocked send is never missed.
struct Authorized {
    authority: Arc<AuthorityHandle>,
    principal: Principal,
    consumer_id: String,
    purpose: String,
    entitlement: Entitlement,
    verified: AtomicU64,
}

impl Authorized {
    fn new(
        authority: Arc<AuthorityHandle>,
        access: &Access,
        consumer_id: &str,
        entitlement: Entitlement,
        generation: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            authority,
            principal: access.principal.clone(),
            consumer_id: consumer_id.to_owned(),
            purpose: access.purpose.clone(),
            entitlement,
            verified: AtomicU64::new(generation),
        })
    }

    /// Ok while still authorized; re-checks when the generation moved past
    /// the one last verified.
    fn check(&self) -> Result<(), Status> {
        let (authority, generation) = self.authority.snapshot();
        if generation == self.verified.load(Ordering::Acquire) {
            return Ok(());
        }
        reauthorize(
            &authority,
            &self.principal,
            &self.consumer_id,
            &self.purpose,
            &self.entitlement,
        )
        .map_err(|error| error.to_status())?;
        self.verified.store(generation, Ordering::Release);
        Ok(())
    }

    /// Resolves with the status once a reload revokes this stream.
    async fn revoked(&self) -> Status {
        loop {
            // Watch first, then check: a reload between the two wakes us.
            let mut watch = self.authority.watch();
            if let Err(status) = self.check() {
                return status;
            }
            if watch.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// What travels to the transport: a response and, for records, the budget
/// bytes it holds until tonic takes it (F4: handoffs are inside the budget).
type Outbound<T> = (Result<T, Status>, Option<Charge>);

fn data_integrity(partition: i32, offset: i64, detail: &str) -> Status {
    Status::data_loss(format!(
        "DATA_INTEGRITY:committed record {partition}:{offset} {detail}; \
         the product cannot be delivered past it"
    ))
}

struct SubscriptionTask {
    report: Report,
    state: Arc<GatewayState>,
    _slot: StreamSlot,
    _registered: Registered,
    subscription: Arc<Subscription>,
    sender: mpsc::Sender<Outbound<query::SubscribeResponse>>,
    terminal: Arc<Mutex<Option<Status>>>,
    claims: CursorV3Claims,
    request_token: String,
    auth: Arc<Authorized>,
    partition: i32,
    key: Vec<u8>,
    after: i64,
    /// Records per response (1 = one record per response, pre-2.3.0).
    batch_limit: usize,
    /// `SubscribeRequest.replay_budget_status_v2` (SDK 2.4.1, IDR-096).
    replay_budget_status_v2: bool,
}

#[derive(Clone, Copy)]
enum BatchPhase {
    Live,
    Replay,
}

/// Event records gathered for one response, in delivery order, with
/// their budget charges merged: the charge travels with the response until
/// tonic takes it, exactly as a single record's does.
#[derive(Default)]
struct Batch {
    records: Vec<query::StreamRecord>,
    charge: Option<Charge>,
    bytes: usize,
    /// Offset of the first and of the last record.
    first: i64,
    last: i64,
    /// When the first record was taken from the queue.
    started: Option<Instant>,
}

impl Batch {
    fn push(&mut self, record: query::StreamRecord, offset: i64, bytes: usize, charge: Charge) {
        if self.records.is_empty() {
            self.first = offset;
        }
        self.last = offset;
        self.bytes += bytes;
        self.records.push(record);
        match self.charge.as_mut() {
            Some(held) => held.absorb(charge),
            None => self.charge = Some(charge),
        }
    }

    /// One record goes in `record` (the shape every client reads), two or
    /// more in `records` (only clients that asked for batches get these).
    fn take_response(&mut self) -> (query::SubscribeResponse, Option<Charge>, usize) {
        let mut records = std::mem::take(&mut self.records);
        let count = records.len();
        let response = if count == 1 {
            query::SubscribeResponse {
                record: records.pop(),
                records: Vec::new(),
            }
        } else {
            query::SubscribeResponse {
                record: None,
                records,
            }
        };
        self.bytes = 0;
        self.started = None;
        (response, self.charge.take(), count)
    }
}

enum HandoffError {
    Budget(Charge),
    Clock(Status),
}

enum Sent {
    Ok,
    Closed,
    Slow,
}

impl Drop for SubscriptionTask {
    fn drop(&mut self) {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let counters = &self.subscription.counters;
        let (queued, queued_bytes) = self.subscription.queued();
        println!(
            "{}",
            serde_json::json!({
                "event": "qdl_kn_subscription_closed",
                "subscription_id": self.subscription.id,
                "consumer_id": self.subscription.consumer_id,
                "physical_key": String::from_utf8_lossy(&self.key),
                "feed": self.subscription.requirement.delivery.feed,
                "interval": self.subscription.requirement.delivery.interval,
                "after": self.after,
                "started_wall_ns": self.report.started_wall_ns,
                "ended_wall_ns": now_ns(),
                "elapsed_ns": self.report.started.map(|at| at.elapsed().as_nanos().min(u64::MAX as u128) as u64),
                "replay_handoff_elapsed_ns": match load(&self.report.replay_handoff_elapsed_ns) {
                    0 => None,
                    value => Some(value),
                },
                "replayed": load(&self.report.replayed),
                "delivered": load(&self.report.delivered),
                "aged_out_at_read": load(&self.report.aged_out),
                "offered": load(&counters.offered),
                "dequeued": load(&counters.dequeued),
                "rejected_at_push": load(&counters.rejected_at_push),
                "coalesced": load(&counters.coalesced),
                "queued": queued,
                "queued_bytes": queued_bytes,
                "outcome": self.report.outcome.lock().map(|value| *value).unwrap_or("?"),
            })
        );
    }
}

impl SubscriptionTask {
    fn outcome(&self, value: &'static str) {
        if let Ok(mut outcome) = self.report.outcome.lock() {
            *outcome = value;
        }
    }

    /// Hand one response to the transport. Revocation is checked first and
    /// raced against the wait, so a blocked send never outlives a revoked
    /// key; a record's budget charge travels with it until tonic takes it.
    async fn send(&self, response: query::SubscribeResponse, charge: Option<Charge>) -> Sent {
        #[cfg(feature = "isolated-replay-clock")]
        if self.state.hub.test_clock_enabled() {
            return self.send_test(response, charge).await;
        }
        if let Err(status) = self.auth.check() {
            self.revoke(status).await;
            return Sent::Closed;
        }
        let handoff = async {
            match tokio::time::timeout(
                self.state.limits.slow_consumer_after,
                self.sender.send((Ok(response), charge)),
            )
            .await
            {
                Ok(Ok(())) => Ok(true),
                Ok(Err(_)) => Ok(false),
                Err(_) => Err(()),
            }
        };
        tokio::select! {
            biased;
            status = self.auth.revoked() => {
                self.revoke(status).await;
                Sent::Closed
            }
            result = handoff => match result {
                Ok(true) => Sent::Ok,
                Ok(false) => Sent::Closed,
                Err(()) => Sent::Slow,
            },
        }
    }

    #[cfg(feature = "isolated-replay-clock")]
    async fn send_test(&self, response: query::SubscribeResponse, charge: Option<Charge>) -> Sent {
        if let Err(status) = self.auth.check() {
            self.revoke(status).await;
            return Sent::Closed;
        }
        let result = tokio::select! {
            biased;
            status = self.auth.revoked() => {
                self.revoke(status).await;
                return Sent::Closed;
            }
            result = tokio::time::timeout(
                self.state.limits.slow_consumer_after,
                test_clock_handoff(&self.state.hub, &self.sender, (Ok(response), charge)),
            ) => result,
        };
        match result {
            Ok(Ok(true)) => Sent::Ok,
            Ok(Ok(false)) => Sent::Closed,
            Err(_) => Sent::Slow,
            Ok(Err(status)) => {
                self.subscription.fail(crate::replay_clock::INVALID);
                self.subscription.close();
                self.fail(status).await;
                Sent::Closed
            }
        }
    }

    async fn revoke(&self, status: Status) {
        self.state.metrics.revoked.fetch_add(1, Ordering::Relaxed);
        self.fail(status).await;
    }

    async fn fail(&self, status: Status) {
        self.outcome(status.code().description());
        // One terminal slot cannot be blocked by the data channel. Dropping
        // the task closes that channel and wakes its receiver to drain it.
        if let Ok(mut terminal) = self.terminal.lock() {
            terminal.get_or_insert(status);
        }
    }

    /// End the stream with the public BACKPRESSURE contract.
    async fn backpressure(&self, last: i64, detail: &str) {
        self.outcome("backpressure");
        self.state.metrics.overflow.fetch_add(1, Ordering::Relaxed);
        let high = self.state.hub.next_offset(self.partition).unwrap_or(0) - 1;
        if let Ok(token) = self.state.sign(&self.claims, last) {
            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                self.sender.send((
                    Ok(subscribe_control(
                        query::StreamControlState::Backpressure,
                        "RATE_LIMITED",
                        detail,
                        high,
                        &token,
                    )),
                    None,
                )),
            )
            .await;
        }
        self.fail(Status::resource_exhausted(format!("RATE_LIMITED:{detail}")))
            .await;
    }

    async fn control(&self, response: query::SubscribeResponse) -> bool {
        matches!(self.send(response, None).await, Sent::Ok)
    }

    fn freshness_now_ns(&self) -> Result<i64, Status> {
        self.state
            .hub
            .freshness_now_ns(now_ns() as i64)
            .map_err(|error| {
                self.subscription.fail(error);
                self.subscription.close();
                Status::failed_precondition(error)
            })
    }

    /// Deliver one record if it is still fresh. `Some(delivered)`, or `None`
    /// when the stream must end.
    async fn deliver(&self, record: &LiveRecord, charge: Option<Charge>) -> Option<bool> {
        let data_now = match self.freshness_now_ns() {
            Ok(at) => at,
            Err(status) => {
                self.fail(status).await;
                return None;
            }
        };
        match delivery_decision(&self.subscription.requirement, &record.envelope, data_now) {
            Delivery::Deliver => {}
            Delivery::OtherProduct | Delivery::TooOld => return Some(false),
        }
        let token = match self.state.sign(&self.claims, record.raw.offset) {
            Ok(token) => token,
            Err(status) => {
                self.fail(status).await;
                return None;
            }
        };
        let response = query::SubscribeResponse {
            record: Some(subscribe_event_record(record, token, now_ns())),
            records: Vec::new(),
        };
        match self.send(response, charge).await {
            Sent::Ok => Some(true),
            Sent::Closed => None,
            Sent::Slow => {
                self.backpressure(
                    record.raw.offset - 1,
                    "slow consumer exceeded its stream buffer; replay is required",
                )
                .await;
                None
            }
        }
    }

    /// Top a dequeued entry's charge up to the record's full weight before
    /// the record is copied for the transport: the response owns a private
    /// decoded copy until tonic takes it (F4). The wait is this stream's own
    /// backpressure, bounded like a blocked send; `None` ends the stream.
    async fn handoff_charge(&self, record: &LiveRecord, mut charge: Charge) -> Option<Charge> {
        let missing = record.weight().saturating_sub(charge.bytes());
        let data_now = match self.freshness_now_ns() {
            Ok(at) => at,
            Err(status) => {
                self.fail(status).await;
                return None;
            }
        };
        let fresh = delivery_decision(&self.subscription.requirement, &record.envelope, data_now)
            == Delivery::Deliver;
        // A record that will be skipped is never copied: no top-up.
        if missing == 0 || !fresh {
            return Some(charge);
        }
        let deadline = Instant::now() + self.state.limits.slow_consumer_after;
        loop {
            if let Some(extra) = self.state.budget.charge(missing) {
                charge.absorb(extra);
                return Some(charge);
            }
            if Instant::now() >= deadline {
                self.backpressure(
                    record.raw.offset - 1,
                    "replica live memory budget exhausted at handoff; replay is required",
                )
                .await;
                return None;
            }
            tokio::select! {
                biased;
                status = self.auth.revoked() => {
                    self.revoke(status).await;
                    return None;
                }
                _ = tokio::time::sleep(HANDOFF_BUDGET_RETRY) => {}
            }
        }
    }

    /// [`Self::handoff_charge`] without waiting: a record joining a batch
    /// that already holds charges never waits for budget (no wait while
    /// holding). `Err` hands the entry charge back.
    fn try_handoff_charge(
        &self,
        record: &LiveRecord,
        mut charge: Charge,
    ) -> Result<Charge, HandoffError> {
        let missing = record.weight().saturating_sub(charge.bytes());
        let data_now = self.freshness_now_ns().map_err(HandoffError::Clock)?;
        let fresh = delivery_decision(&self.subscription.requirement, &record.envelope, data_now)
            == Delivery::Deliver;
        if missing == 0 || !fresh {
            return Ok(charge);
        }
        match self.state.budget.charge(missing) {
            Some(extra) => {
                charge.absorb(extra);
                Ok(charge)
            }
            None => Err(HandoffError::Budget(charge)),
        }
    }

    /// Send the gathered records as one response. `false` ends the stream
    /// (it already failed typed); a slow client gets BACKPRESSURE with the
    /// token before the batch's first record, as for a single record.
    async fn flush(&self, batch: &mut Batch, last: &mut i64, phase: BatchPhase) -> bool {
        if batch.records.is_empty() {
            return true;
        }
        // Do not flush an already assembled TEST batch after clock failure/expiry.
        if let Err(status) = self.freshness_now_ns() {
            self.fail(status).await;
            return false;
        }
        let (first, newest, started) = (batch.first, batch.last, batch.started);
        let (response, charge, count) = batch.take_response();
        let sent = self.send(response, charge).await;
        if let Some(started) = started {
            self.state
                .metrics
                .handoff_wait
                .record(started.elapsed().as_millis() as i64);
        }
        match sent {
            Sent::Ok => {
                *last = newest;
                let count = count as u64;
                let (metric, report) = match phase {
                    BatchPhase::Live => (&self.state.metrics.delivered, &self.report.delivered),
                    BatchPhase::Replay => (&self.state.metrics.replayed, &self.report.replayed),
                };
                metric.fetch_add(count, Ordering::Relaxed);
                report.fetch_add(count, Ordering::Relaxed);
                true
            }
            Sent::Closed => false,
            Sent::Slow => {
                self.backpressure(
                    first - 1,
                    "slow consumer exceeded its stream buffer; replay is required",
                )
                .await;
                false
            }
        }
    }

    /// Deliver a dequeued live record and, when the client asked for
    /// batches, the records already queued behind it (never waiting for more)
    /// in one response, in queue order. Every record keeps its own offset,
    /// resume token, freshness check and charge; overflow, failure and close
    /// are never crossed ([`Subscription::try_next`]), and controls are sent
    /// alone elsewhere. `false` ends the stream (it already failed typed).
    async fn deliver_live(
        &self,
        first: (Arc<LiveRecord>, Instant, Charge),
        last: &mut i64,
        filtered_since_delivery: &mut usize,
    ) -> bool {
        let mut batch = Batch::default();
        let mut item = Some(first);
        while let Some((record, enqueued, charge)) = item.take() {
            let taken = Instant::now();
            let taken_wall = now_ns();
            self.state
                .metrics
                .queue_wait
                .record(taken.duration_since(enqueued).as_millis() as i64);
            let charge = if batch.records.is_empty() {
                match self.handoff_charge(&record, charge).await {
                    Some(charge) => charge,
                    None => return false,
                }
            } else {
                match self.try_handoff_charge(&record, charge) {
                    Ok(charge) => charge,
                    Err(HandoffError::Clock(status)) => {
                        self.fail(status).await;
                        return false;
                    }
                    Err(HandoffError::Budget(charge)) => {
                        // Budget is short: send what is gathered, then wait
                        // for this record's top-up holding nothing else.
                        if !self.flush(&mut batch, last, BatchPhase::Live).await {
                            return false;
                        }
                        match self.handoff_charge(&record, charge).await {
                            Some(charge) => charge,
                            None => return false,
                        }
                    }
                }
            };
            let data_now = match self.freshness_now_ns() {
                Ok(at) => at,
                Err(status) => {
                    self.fail(status).await;
                    return false;
                }
            };
            match delivery_decision(&self.subscription.requirement, &record.envelope, data_now) {
                Delivery::Deliver => {
                    let token = match self.state.sign(&self.claims, record.raw.offset) {
                        Ok(token) => token,
                        Err(status) => {
                            self.fail(status).await;
                            return false;
                        }
                    };
                    let bytes = record.raw.payload.len() + token.len() + 32 + TIMING_BYTES;
                    let full = !batch.records.is_empty() && batch.bytes + bytes > MAX_BATCH_BYTES;
                    if full && !self.flush(&mut batch, last, BatchPhase::Live).await {
                        return false;
                    }
                    batch.started.get_or_insert(taken);
                    batch.push(
                        subscribe_event_record(&record, token, taken_wall),
                        record.raw.offset,
                        bytes,
                        charge,
                    );
                    *filtered_since_delivery = 0;
                }
                Delivery::OtherProduct | Delivery::TooOld => {
                    drop(charge);
                    // Delivered records ahead of it go first, so the cursor
                    // and the aged-out accounting match one-record delivery.
                    if !self.flush(&mut batch, last, BatchPhase::Live).await {
                        return false;
                    }
                    self.state
                        .metrics
                        .handoff_wait
                        .record(taken.elapsed().as_millis() as i64);
                    // Aged out while queued: the cursor moves past it, and
                    // a whole buffer of such records without a delivery
                    // is a slow consumer (DL-V2 R1.13).
                    *last = record.raw.offset;
                    *filtered_since_delivery += 1;
                    self.state
                        .metrics
                        .aged_out_at_read
                        .fetch_add(1, Ordering::Relaxed);
                    self.report.aged_out.fetch_add(1, Ordering::Relaxed);
                    if *filtered_since_delivery > self.subscription.depth {
                        self.backpressure(
                            *last,
                            "records aged out of the bounded buffer before delivery; \
                         replay from the last confirmed token is required",
                        )
                        .await;
                        return false;
                    }
                }
            }
            if batch.records.len() < self.batch_limit && self.batch_limit > 1 {
                item = self.subscription.try_next();
            }
        }
        self.flush(&mut batch, last, BatchPhase::Live).await
    }

    /// Replay one record; `false` ends the stream (it already failed typed).
    async fn replay_one(&self, record: &LiveRecord, charge: Charge, last: &mut i64) -> bool {
        match self.deliver(record, Some(charge)).await {
            Some(delivered) => {
                if delivered {
                    self.state.metrics.replayed.fetch_add(1, Ordering::Relaxed);
                    self.report.replayed.fetch_add(1, Ordering::Relaxed);
                }
                *last = record.raw.offset;
                true
            }
            None => false,
        }
    }

    /// Gather replay records without waiting for more or borrowing live budget.
    /// A filtered record is a boundary, not permission to skip pending delivery.
    async fn replay_record(
        &self,
        record: &LiveRecord,
        charge: Charge,
        batch: &mut Batch,
        last: &mut i64,
    ) -> bool {
        if self.batch_limit == 1 {
            return self.replay_one(record, charge, last).await;
        }
        if let Err(status) = self.auth.check() {
            self.revoke(status).await;
            return false;
        }
        let data_now = match self.freshness_now_ns() {
            Ok(at) => at,
            Err(status) => {
                self.fail(status).await;
                return false;
            }
        };
        match delivery_decision(&self.subscription.requirement, &record.envelope, data_now) {
            Delivery::OtherProduct | Delivery::TooOld => {
                drop(charge);
                if !self.flush(batch, last, BatchPhase::Replay).await {
                    return false;
                }
                *last = record.raw.offset;
                return true;
            }
            Delivery::Deliver => {}
        }
        let token = match self.state.sign(&self.claims, record.raw.offset) {
            Ok(token) => token,
            Err(status) => {
                if self.flush(batch, last, BatchPhase::Replay).await {
                    self.fail(status).await;
                }
                return false;
            }
        };
        let bytes = record.raw.payload.len() + token.len() + 32 + TIMING_BYTES;
        if !batch.records.is_empty()
            && batch.bytes + bytes > MAX_BATCH_BYTES
            && !self.flush(batch, last, BatchPhase::Replay).await
        {
            return false;
        }
        batch.started.get_or_insert_with(Instant::now);
        batch.push(
            subscribe_event_record(record, token, now_ns()),
            record.raw.offset,
            bytes,
            charge,
        );
        // Preserve the existing oversized-single-record exception, never combine it.
        if batch.records.len() >= self.batch_limit || batch.bytes >= MAX_BATCH_BYTES {
            return self.flush(batch, last, BatchPhase::Replay).await;
        }
        true
    }

    async fn run(self, barrier: i64, ring: Option<RingReplay>) {
        #[cfg(feature = "isolated-replay-clock")]
        if self.state.hub.test_clock_enabled() {
            if let Err(status) =
                test_clock_guard(&self.state.hub, self.run_inner(barrier, ring)).await
            {
                self.subscription.fail(crate::replay_clock::INVALID);
                self.subscription.close();
                self.fail(status).await;
            }
            return;
        }
        self.run_inner(barrier, ring).await;
    }

    async fn catch_up(&self, mut barrier: i64, mut ring: Option<RingReplay>,
        episode: Arc<ReplayEpisode>) -> Option<(i64, i64)> {
        let mut last = self.after;
        let max_matched = self.state.limits.replay.max_matched;
        loop {
        if Instant::now() >= episode.deadline {
            self.fail(Status::resource_exhausted("RATE_LIMITED:REPLAY_TIME_LIMIT:catch-up deadline expired")).await;
            return None;
        }
        tokio::task::yield_now().await;
        let registered = ring.is_some();
        if !registered && last + 1 < barrier {
            ring = self.state.hub.retained_replay(self.partition, &self.key, last,
                barrier, max_matched.saturating_add(1) as usize, self.state.replay.budget())
                .map(|(records, charge)| RingReplay { records, charge });
        }
        if last + 1 < barrier {
            let mut batch = Batch::default();
            match ring.take() {
                Some(mut ring) => {
                    self.state
                        .replay
                        .metrics
                        .ring_hits
                        .fetch_add(1, Ordering::Relaxed);
                    let records = std::mem::take(&mut ring.records);
                    for (index, raw) in records.into_iter().enumerate() {
                        if index % REPLAY_CHANNEL == 0 {
                            tokio::task::yield_now().await;
                            if Instant::now() >= episode.deadline {
                                if !self.flush(&mut batch, &mut last, BatchPhase::Replay).await { return None; }
                                self.fail(Status::resource_exhausted("RATE_LIMITED:REPLAY_TIME_LIMIT:ring deadline expired")).await;
                                return None;
                            }
                        }
                        if let Some(reason) = episode.scan(raw.payload.len() as u64, &self.state.limits.replay) {
                            if !self.flush(&mut batch, &mut last, BatchPhase::Replay).await { return None; }
                            if let Some(status) = replay_end_status(&self.state, &ReplayEnd::ScanLimit(reason), self.replay_budget_status_v2) {
                                self.fail(status).await;
                            }
                            return None;
                        }
                        // Its share of the ring charge goes with the record.
                        let charge = ring.charge.split(live_weight(&raw));
                        let (partition, offset) = (raw.partition, raw.offset);
                        let Ok(record) = LiveRecord::decode(raw) else {
                            // Same outcome as the reader path (F2 parity).
                            if !self.flush(&mut batch, &mut last, BatchPhase::Replay).await {
                                return None;
                            }
                            self.fail(data_integrity(partition, offset, "failed to decode"))
                                .await;
                            return None;
                        };
                        let feed = &self.subscription.requirement.delivery.feed;
                        let interval = self.subscription.requirement.delivery.interval.as_deref();
                        if !is_product(feed, interval, &record.envelope) {
                            if !self.flush(&mut batch, &mut last, BatchPhase::Replay).await {
                                return None;
                            }
                            continue;
                        }
                        let reservation = episode.reserve_match(max_matched);
                        if reservation.is_none() {
                            if !self.flush(&mut batch, &mut last, BatchPhase::Replay).await {
                                return None;
                            }
                            if let Some(status) =
                                replay_end_status(&self.state, &ReplayEnd::Backlog, self.replay_budget_status_v2)
                            {
                                self.fail(status).await;
                            }
                            return None;
                        }
                        if !self.replay_record(&record, charge, &mut batch, &mut last).await {
                            return None;
                        }
                        reservation.unwrap().delivered();
                    }
                    if !self.flush(&mut batch, &mut last, BatchPhase::Replay).await {
                        return None;
                    }
                }
                None => {
                    let requirement = &self.subscription.requirement.delivery;
                    let (mut records, cancel, reader) = spawn_scan_episode(
                        &self.state,
                        self.partition,
                        last,
                        barrier,
                        self.key.clone(),
                        (requirement.feed.clone(), requirement.interval.clone()),
                        None,
                        episode.clone(),
                    );
                    let _cancel = CancelOnDrop(cancel);
                    loop {
                        let replayed = tokio::select! {
                            biased;
                            status = self.auth.revoked() => {
                                self.revoke(status).await;
                                return None;
                            }
                            replayed = records.recv() => replayed,
                        };
                        let Some(replayed) = replayed else {
                            break;
                        };
                        let mut ready = Some(replayed);
                        while let Some(replayed) = ready.take() {
                            let (record, charge) = replayed.into_parts();
                            if !self.replay_record(&record, charge, &mut batch, &mut last).await {
                                return None;
                            }
                            if self.batch_limit > 1 && !batch.records.is_empty() {
                                ready = records.try_recv().ok();
                            }
                        }
                        // Never hold a partial batch while waiting for the reader.
                        if !self.flush(&mut batch, &mut last, BatchPhase::Replay).await {
                            return None;
                        }
                    }
                    let end = reader
                        .await
                        .unwrap_or(ReplayEnd::Error("replay coordinator dropped".into()));
                    if let Some(status) = replay_end_status(&self.state, &end, self.replay_budget_status_v2) {
                        self.fail(status).await;
                        return None;
                    }
                    if end != ReplayEnd::Complete {
                        self.fail(Status::unavailable("DEPENDENCY_UNAVAILABLE:replay range incomplete")).await;
                        return None;
                    }
                }
            }
            last = last.max(barrier - 1);
        } else if last >= barrier {
            // This replica has not dispatched the cursor's record yet (the
            // client came from a replica further ahead). Wait, bounded.
            let deadline = Instant::now() + self.state.limits.catchup_deadline;
            loop {
                if let Err(status) = self.auth.check() {
                    self.revoke(status).await;
                    return None;
                }
                let next = self.state.hub.next_offset(self.partition).unwrap_or(0);
                if next > last {
                    break;
                }
                if Instant::now() >= deadline {
                    self.state.metrics.lagging.fetch_add(1, Ordering::Relaxed);
                    self.fail(Status::unavailable(format!(
                        "REPLICA_LAGGING:this replica has dispatched offset {} of partition {}; \
                         the cursor is at {}; retry",
                        next - 1,
                        self.partition,
                        self.after
                    )))
                    .await;
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        if registered { return Some((last, barrier)); }
        if let Err(status) = self.auth.check() { self.revoke(status).await; return None; }
        match self.state.hub.try_register_near(self.partition, &self.key,
            self.subscription.clone(), last, REPLAY_CHANNEL,
            self.state.replay.budget()) {
            Ok(NearRegistration::Registered { barrier: next, bridge, charge }) => {
                barrier = next;
                ring = Some(RingReplay { records: bridge, charge });
            }
            Ok(NearRegistration::NeedReplay { barrier: next }) => {
                barrier = next;
                if last + 1 >= barrier {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            Err(error) => { self.fail(Status::internal(error)).await; return None; }
        }
        }
    }

    async fn run_inner(&self, barrier: i64, ring: Option<RingReplay>) {
        let replaying = subscribe_control(
            query::StreamControlState::Replaying,
            "REPLAYING",
            "replaying committed records after the supplied cursor",
            barrier - 1,
            &self.request_token,
        );
        if !self.control(replaying).await {
            return;
        }
        let episode = ReplayEpisode::new(Instant::now() + self.state.limits.replay.max_duration);
        let mut shutdown = self.state.shutdown.subscribe();
        let result = tokio::select! {
            biased;
            status = self.auth.revoked() => { self.revoke(status).await; return; }
            () = self.sender.closed() => return,
            // `wait_for` also sees a stop that began before this subscribe
            // (e.g. while REPLAYING was blocked): never catch up, then LIVE,
            // on a stopping replica.
            // The watch::Ref is dropped inside the block (it is not Send).
            () = async { let _ = shutdown.wait_for(|stopping| *stopping).await; } => {
                self.fail(Status::unavailable("GATEWAY_SHUTTING_DOWN:resume from last token")).await;
                return;
            }
            _ = tokio::time::sleep_until(episode.deadline.into()) => {
                self.fail(Status::resource_exhausted("RATE_LIMITED:REPLAY_TIME_LIMIT:catch-up deadline expired")).await;
                return;
            }
            result = self.catch_up(barrier, ring, episode.clone()) => result,
        };
        let Some((mut last, barrier)) = result else { return; };
        if let Some(terminal) = self.subscription.terminal() {
            match terminal {
                Next::Overflow => self.backpressure(last, "bounded bridge overflow; resume from last token").await,
                Next::Failed(reason) => self.fail(Status::data_loss(format!("DATA_INTEGRITY:{reason}"))).await,
                _ => (),
            }
            return;
        }
        let Ok(token) = self.state.sign(&self.claims, last) else {
            self.fail(Status::internal("cursor signing failed")).await;
            return;
        };
        let live = subscribe_control(
            query::StreamControlState::Live,
            "LIVE",
            "committed replay is complete; live delivery is active",
            barrier - 1,
            &token,
        );
        if !self.control(live).await {
            return;
        }
        // Transport handoff only: this does not assert consumer apply or eligibility.
        if let Some(started) = self.report.started {
            self.report.replay_handoff_elapsed_ns.store(
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
        }
        let mut shutdown = self.state.shutdown.subscribe();
        let mut filtered_since_delivery = 0usize;
        loop {
            if *shutdown.borrow() {
                self.fail(Status::unavailable(
                    "GATEWAY_SHUTTING_DOWN:this replica is stopping; resume from the last token",
                ))
                .await;
                return;
            }
            let next = tokio::select! {
                next = self.subscription.next() => next,
                _ = shutdown.changed() => continue,
                status = self.auth.revoked() => {
                    self.revoke(status).await;
                    return;
                }
                () = self.sender.closed() => return,
            };
            match next {
                Next::Record(record, enqueued, charge) => {
                    if !self
                        .deliver_live(
                            (record, enqueued, charge),
                            &mut last,
                            &mut filtered_since_delivery,
                        )
                        .await
                    {
                        return;
                    }
                }
                Next::Overflow => {
                    self.backpressure(
                        last,
                        "bounded outbound buffer exhausted; replay is required",
                    )
                    .await;
                    return;
                }
                Next::Failed(reason) => {
                    #[cfg(feature = "isolated-replay-clock")]
                    if reason == crate::replay_clock::INVALID {
                        self.fail(test_clock_status()).await;
                        return;
                    }
                    self.fail(Status::data_loss(format!("DATA_INTEGRITY:{reason}")))
                        .await;
                    return;
                }
                Next::Closed => return,
            }
        }
    }
}

/// Ring records handed to a new subscriber, charged to the replay budget
/// while it replays them (F4: evicted records it still references stay
/// bounded). No budget: the replay goes to the coordinator instead.
struct RingReplay {
    records: Vec<crate::hub::SharedRecord>,
    charge: Charge,
}

#[cfg(all(test, feature = "isolated-replay-clock"))]
mod test_clock_transport {
    use super::*;
    use crate::hub::HubConfig;
    use crate::replay_clock::DataClock;

    fn clocked_hub() -> (Arc<Hub>, impl Fn(bool)) {
        let (clock, expired) = DataClock::test_fixture();
        let hub = Hub::new(&[(0, 0)], HubConfig::default()).with_test_data_clock(clock);
        (Arc::new(hub), expired)
    }

    fn control() -> query::SubscribeResponse {
        subscribe_control(query::StreamControlState::Live, "LIVE", "TEST_ONLY", 0, "")
    }

    fn event() -> query::SubscribeResponse {
        query::SubscribeResponse {
            record: Some(query::StreamRecord {
                payload: Some(query::stream_record::Payload::Event(Default::default())),
                ..Default::default()
            }),
            records: Vec::new(),
        }
    }

    fn assert_clock_status(status: Status) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), crate::replay_clock::INVALID);
    }

    #[tokio::test]
    async fn full_handoff_expiry_discards_events_controls_and_old_terminal() {
        let (hub, expired) = clocked_hub();
        let (sender, receiver) = mpsc::channel(OUTBOUND_HANDOFF);
        let budget = ByteBudget::new(128);
        for response in [event(), control()] {
            sender
                .send((Ok(response), budget.charge(16)))
                .await
                .unwrap();
        }
        assert_eq!(sender.capacity(), 0);
        assert_eq!(budget.used(), 32);
        let terminal = Arc::new(Mutex::new(Some(Status::data_loss("older terminal"))));
        let mut stream = TestClockStream::new(subscription_handoff(receiver, terminal), hub);
        drop(sender);
        expired(true);
        assert_clock_status(stream.next().await.unwrap().unwrap_err());
        assert_eq!(budget.used(), 0);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn blocked_full_handoff_is_cancelled_without_a_dequeue() {
        let (hub, expired) = clocked_hub();
        let (sender, mut receiver) = mpsc::channel(OUTBOUND_HANDOFF);
        let budget = ByteBudget::new(128);
        for _ in 0..OUTBOUND_HANDOFF {
            sender
                .send((Ok(control()), budget.charge(16)))
                .await
                .unwrap();
        }
        let guarded = test_clock_handoff(&hub, &sender, (Ok(event()), budget.charge(16)));
        tokio::pin!(guarded);
        tokio::select! {
            _ = &mut guarded => panic!("full handoff must block"),
            () = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
        expired(true);
        let result = tokio::time::timeout(Duration::from_millis(250), &mut guarded)
            .await
            .expect("clock watchdog did not wake");
        assert_clock_status(result.unwrap_err());
        assert_eq!(budget.used(), OUTBOUND_HANDOFF * 16);
        for _ in 0..OUTBOUND_HANDOFF {
            assert!(receiver.try_recv().is_ok());
        }
        assert!(receiver.try_recv().is_err());
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn capacity_released_after_expiry_cannot_commit_blocked_response() {
        let (hub, expired) = clocked_hub();
        let (sender, mut receiver) = mpsc::channel(OUTBOUND_HANDOFF);
        for _ in 0..OUTBOUND_HANDOFF {
            sender.send((Ok(control()), None)).await.unwrap();
        }
        let blocked = test_clock_handoff(&hub, &sender, (Ok(event()), None));
        tokio::pin!(blocked);
        tokio::select! {
            _ = &mut blocked => panic!("full handoff must block"),
            () = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
        expired(true);
        // Wake the capacity waiter before its periodic watchdog needs to fire.
        assert!(receiver.try_recv().is_ok());
        let result = tokio::time::timeout(Duration::from_millis(250), &mut blocked)
            .await
            .expect("released capacity did not wake sender");
        assert_clock_status(result.unwrap_err());
        assert!(receiver.try_recv().is_ok());
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn idle_handoff_wakes_on_expiry_with_sender_still_open() {
        let (hub, expired) = clocked_hub();
        let (sender, receiver) = mpsc::channel(OUTBOUND_HANDOFF);
        let terminal = Arc::new(Mutex::new(None));
        let mut stream = TestClockStream::new(subscription_handoff(receiver, terminal), hub);
        {
            let next = stream.next();
            tokio::pin!(next);
            tokio::select! {
                _ = &mut next => panic!("idle handoff must wait"),
                () = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
            expired(true);
            let item = tokio::time::timeout(Duration::from_millis(250), &mut next)
                .await
                .expect("idle transport did not wake");
            assert_clock_status(item.unwrap().unwrap_err());
        }
        assert!(sender.is_closed());
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn peer_latched_fault_fences_queued_control_and_idle_lifecycle() {
        let (hub, expired) = clocked_hub();
        let (sender, receiver) = mpsc::channel(OUTBOUND_HANDOFF);
        sender.send((Ok(control()), None)).await.unwrap();
        let terminal = Arc::new(Mutex::new(None));
        let mut stream =
            TestClockStream::new(subscription_handoff(receiver, terminal), hub.clone());
        expired(true);
        // Another subscription/hub admission detects the fault first.
        assert!(hub.check_test_clock().is_err());
        expired(false);
        assert_clock_status(stream.next().await.unwrap().unwrap_err());
        let result = tokio::time::timeout(
            Duration::from_millis(250),
            test_clock_guard(&hub, std::future::pending::<()>()),
        )
        .await
        .expect("idle lifecycle ignored peer fault");
        assert_clock_status(result.unwrap_err());
        assert!(sender.is_closed());
    }

    #[tokio::test]
    async fn expired_startup_never_emits_live() {
        let (hub, expired) = clocked_hub();
        expired(true);
        let (sender, mut receiver) = mpsc::channel(OUTBOUND_HANDOFF);
        let result = test_clock_handoff(&hub, &sender, (Ok(control()), None)).await;
        assert_clock_status(result.unwrap_err());
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn valid_test_handoff_preserves_order_and_terminal_status() {
        let (hub, _) = clocked_hub();
        let (sender, receiver) = mpsc::channel(OUTBOUND_HANDOFF);
        sender.send((Ok(control()), None)).await.unwrap();
        sender.send((Ok(event()), None)).await.unwrap();
        drop(sender);
        let terminal = Arc::new(Mutex::new(Some(Status::unavailable("original terminal"))));
        let mut stream = TestClockStream::new(subscription_handoff(receiver, terminal), hub);
        assert!(matches!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .record
                .unwrap()
                .payload,
            Some(query::stream_record::Payload::Control(_))
        ));
        let record = stream.next().await.unwrap().unwrap().record.unwrap();
        assert!(matches!(
            record.payload,
            Some(query::stream_record::Payload::Event(_))
        ));
        assert!(record.gateway_send_wall_ns > 11_000);
        let status = stream.next().await.unwrap().unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "original terminal");
        assert!(stream.next().await.is_none());
    }
}
