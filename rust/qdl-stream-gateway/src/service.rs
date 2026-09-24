//! `MarketDataStreamService`, the native KN-2 service (K2.1-K2.4).
//!
//! Every RPC follows `qdl/stream/grpc_service.py` step for step - access
//! checks, their order, status codes and control frames - on top of the
//! shared committed reader ([`crate::hub`]), bounded subscriber queues
//! ([`crate::subscription`]), the bounded replay pool ([`crate::replay`]) and
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
use crate::authority::{reauthorize, Authority, AuthorityHandle};
use crate::generated::query_v2 as query;
use crate::generated::query_v2::market_data_stream_service_server::MarketDataStreamService;
use crate::hub::{Hub, LiveRecord, RawRecord};
use crate::readview::ReadView;
use crate::replay::{scan_range, RangeSource, ReplayEnd, ReplayLimits, ReplayPool};
use crate::requirement::{
    delivery_decision, is_product, require_requirement, Delivery, StreamRequirement,
};
use crate::subscription::{ByteBudget, Next, Subscription};
use qdl_contracts::cursor_v3::{CursorError, CursorV3Claims, CursorV3Codec};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

/// Records handed from a subscription task to the transport. Kept tiny: a
/// deeper channel would hold stale latest-state records behind the 8-deep
/// queue (DL-V2 R1.8) and hide a slow client from the overflow signal.
pub const OUTBOUND_HANDOFF: usize = 2;

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
    pub replay_page_default: u64,
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            cursor_ttl_seconds: 3_600,
            replay: ReplayLimits::default(),
            catchup_deadline: Duration::from_secs(10),
            slow_consumer_after: Duration::from_secs(10),
            queue_bytes_per_subscription: 32 << 20,
            max_subscriptions: 1_024,
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
}

pub struct GatewayState {
    pub authority: Arc<AuthorityHandle>,
    pub quota: Arc<dyn RequestQuota>,
    pub codec: CursorV3Codec,
    pub hub: Arc<Hub>,
    pub range: Arc<dyn RangeSource>,
    pub pool: ReplayPool,
    pub read_view: Arc<dyn ReadView>,
    pub budget: Arc<ByteBudget>,
    pub limits: StreamLimits,
    pub metrics: Metrics,
    streams: Mutex<HashMap<String, u64>>,
    next_id: AtomicU64,
}

impl GatewayState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        authority: Arc<AuthorityHandle>,
        quota: Arc<dyn RequestQuota>,
        codec: CursorV3Codec,
        hub: Arc<Hub>,
        range: Arc<dyn RangeSource>,
        pool: ReplayPool,
        read_view: Arc<dyn ReadView>,
        budget: Arc<ByteBudget>,
        limits: StreamLimits,
    ) -> Self {
        Self {
            authority,
            quota,
            codec,
            hub,
            range,
            pool,
            read_view,
            budget,
            limits,
            metrics: Metrics::default(),
            streams: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn open_streams(&self) -> u64 {
        self.streams
            .lock()
            .map(|streams| streams.values().sum())
            .unwrap_or(0)
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
    }
}

impl GatewayState {
    /// `DataPlaneIdentityService.authenticate` against the current authority
    /// (JWT + manifest + shared quota), off the async runtime.
    async fn access<T>(
        self: &Arc<Self>,
        request: &Request<T>,
    ) -> Result<(Access, Arc<Authority>), Status> {
        let (authorization, consumer, purpose) = credentials(request);
        let authority = self.authority.current();
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
            Ok(access) => Ok((access, authority)),
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
        let (access, authority) = state.access(&request).await?;
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
        let registration = state
            .hub
            .register(partition, &key, subscription.clone(), after)
            .map_err(Status::internal)?;
        let registered = Registered {
            hub: state.hub.clone(),
            partition,
            key: key.clone(),
            subscription: subscription.clone(),
        };
        let (sender, receiver) = mpsc::channel(OUTBOUND_HANDOFF);
        let task = SubscriptionTask {
            state,
            _slot: slot,
            _registered: registered,
            subscription,
            sender,
            claims,
            request_token: body.cursor_token,
            principal: access.principal,
            purpose: access.purpose,
            partition,
            key,
            after,
        };
        tokio::spawn(task.run(registration.barrier, registration.ring));
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn replay(
        &self,
        request: Request<query::ReplayRequest>,
    ) -> Result<Response<Self::ReplayStream>, Status> {
        let state = self.state.clone();
        let (access, authority) = state.access(&request).await?;
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
        let permit = state.pool.acquire(&body.consumer_id).await.ok_or_else(|| {
            state.refuse(Status::resource_exhausted(
                "RATE_LIMITED:replay reader capacity is exhausted",
            ))
        })?;
        let (sender, receiver) = mpsc::channel(32);
        let after = claims.source_offset as i64;
        let key = binding.physical_key.clone().into_bytes();
        tokio::spawn(async move {
            let (records, cancel, reader) = spawn_scan(
                state.clone(),
                partition,
                after,
                barrier,
                key,
                (binding.feed.clone(), binding.interval.clone()),
                Some(limit),
                permit,
            );
            let _cancel = CancelOnDrop(cancel);
            let mut records = records;
            while let Some(record) = records.recv().await {
                let token = match state.sign(&claims, record.raw.offset) {
                    Ok(token) => token,
                    Err(status) => {
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                };
                let response = query::ReplayResponse {
                    record: Some(event_record(&record, token)),
                };
                if sender.send(Ok(response)).await.is_err() {
                    return;
                }
            }
            let end = reader
                .await
                .unwrap_or(ReplayEnd::Error("replay task failed".into()));
            if let Some(status) = replay_end_status(&state, &end) {
                let _ = sender.send(Err(status)).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn get_snapshot(
        &self,
        request: Request<query::GetSnapshotRequest>,
    ) -> Result<Response<query::GetSnapshotResponse>, Status> {
        let state = self.state.clone();
        let (access, _authority) = state.access(&request).await?;
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
        state
            .read_view
            .snapshot(&requirement, &body.consumer_id)
            .await
            .map(Response::new)
            .map_err(|error| error.to_status())
    }

    async fn get_feed_status(
        &self,
        request: Request<query::GetFeedStatusRequest>,
    ) -> Result<Response<query::GetFeedStatusResponse>, Status> {
        let state = self.state.clone();
        let (access, _authority) = state.access(&request).await?;
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
        state
            .read_view
            .status(&requirement)
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
    }
}

/// Typed outcome of a replay that did not simply complete.
fn replay_end_status(state: &GatewayState, end: &ReplayEnd) -> Option<Status> {
    let expired = |detail: String| {
        state.metrics.expired.fetch_add(1, Ordering::Relaxed);
        Some(Status::out_of_range(format!("CURSOR_EXPIRED:{detail}")))
    };
    match end {
        ReplayEnd::Complete | ReplayEnd::PageFull | ReplayEnd::Cancelled => None,
        ReplayEnd::Backlog => expired(
            "replay backlog exceeds the bounded gateway window; a fresh snapshot is required"
                .into(),
        ),
        ReplayEnd::Retention => expired(
            "RETENTION:the cursor is below the retained committed log; a fresh snapshot is required"
                .into(),
        ),
        ReplayEnd::ScanLimit(reason) => {
            state.pool.metrics.scan_limited.fetch_add(1, Ordering::Relaxed);
            expired(format!(
                "{reason}:replay exceeds the bounded scan; a fresh snapshot is required"
            ))
        }
        ReplayEnd::Error(error) => Some(Status::unavailable(format!(
            "DEPENDENCY_UNAVAILABLE:replay reader failed: {error}"
        ))),
    }
}

/// Run a replay reader on a blocking thread. Records of the product flow
/// through a bounded channel (the reader waits for the client); the join
/// handle yields how the scan ended. The permit is held until the reader
/// thread finishes; setting the cancel flag stops it within one poll.
#[allow(clippy::too_many_arguments)]
fn spawn_scan(
    state: Arc<GatewayState>,
    partition: i32,
    after: i64,
    barrier: i64,
    key: Vec<u8>,
    product: (String, Option<String>),
    page: Option<u64>,
    permit: crate::replay::ReplayPermit,
) -> (
    mpsc::Receiver<Arc<LiveRecord>>,
    Arc<AtomicBool>,
    tokio::task::JoinHandle<ReplayEnd>,
) {
    let (sender, receiver) = mpsc::channel::<Arc<LiveRecord>>(256);
    let cancel = Arc::new(AtomicBool::new(false));
    let flag = cancel.clone();
    state
        .pool
        .metrics
        .reader_replays
        .fetch_add(1, Ordering::Relaxed);
    let handle = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        state.pool.metrics.active.fetch_add(1, Ordering::Relaxed);
        let limits = state.limits.replay.clone();
        let mut emit = |raw: RawRecord| -> crate::replay::Emit {
            let Ok(record) = LiveRecord::decode(Arc::new(raw)) else {
                return crate::replay::Emit::Skip;
            };
            if !is_product(&product.0, product.1.as_deref(), &record.envelope) {
                return crate::replay::Emit::Skip;
            }
            if sender.blocking_send(record).is_err() {
                return crate::replay::Emit::Stop;
            }
            crate::replay::Emit::Sent
        };
        let end = scan_range(
            state.range.as_ref(),
            partition,
            after,
            barrier,
            &key,
            &limits,
            page,
            &flag,
            &state.pool.metrics,
            &mut emit,
        );
        state.pool.metrics.active.fetch_sub(1, Ordering::Relaxed);
        end
    });
    (receiver, cancel, handle)
}

struct SubscriptionTask {
    state: Arc<GatewayState>,
    _slot: StreamSlot,
    _registered: Registered,
    subscription: Arc<Subscription>,
    sender: mpsc::Sender<Result<query::SubscribeResponse, Status>>,
    claims: CursorV3Claims,
    request_token: String,
    principal: Principal,
    purpose: String,
    partition: i32,
    key: Vec<u8>,
    after: i64,
}

enum Sent {
    Ok,
    Closed,
    Slow,
}

impl SubscriptionTask {
    async fn send(&self, response: Result<query::SubscribeResponse, Status>) -> Sent {
        match tokio::time::timeout(
            self.state.limits.slow_consumer_after,
            self.sender.send(response),
        )
        .await
        {
            Ok(Ok(())) => Sent::Ok,
            Ok(Err(_)) => Sent::Closed,
            Err(_) => Sent::Slow,
        }
    }

    async fn fail(&self, status: Status) {
        let _ = tokio::time::timeout(Duration::from_secs(5), self.sender.send(Err(status))).await;
    }

    /// End the stream with the public BACKPRESSURE contract.
    async fn backpressure(&self, last: i64, detail: &str) {
        self.state.metrics.overflow.fetch_add(1, Ordering::Relaxed);
        let high = self.state.hub.next_offset(self.partition).unwrap_or(0) - 1;
        if let Ok(token) = self.state.sign(&self.claims, last) {
            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                self.sender.send(Ok(subscribe_control(
                    query::StreamControlState::Backpressure,
                    "RATE_LIMITED",
                    detail,
                    high,
                    &token,
                ))),
            )
            .await;
        }
        self.fail(Status::resource_exhausted(format!("RATE_LIMITED:{detail}")))
            .await;
    }

    /// Deliver one record if it is still fresh. Returns the new resume
    /// offset, or `None` when the stream must end.
    async fn deliver(&self, record: &LiveRecord) -> Option<bool> {
        match delivery_decision(
            &self.subscription.requirement,
            &record.envelope,
            now_ns() as i64,
        ) {
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
            record: Some(event_record(record, token)),
        };
        match self.send(Ok(response)).await {
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

    async fn run(self, barrier: i64, ring: Option<Vec<crate::hub::SharedRecord>>) {
        let replaying = subscribe_control(
            query::StreamControlState::Replaying,
            "REPLAYING",
            "replaying committed records after the supplied cursor",
            barrier - 1,
            &self.request_token,
        );
        if !matches!(self.send(Ok(replaying)).await, Sent::Ok) {
            return;
        }
        let mut last = self.after;
        let mut matched: u64 = 0;
        let max_matched = self.state.limits.replay.max_matched;
        if self.after + 1 < barrier {
            match ring {
                Some(records) => {
                    self.state
                        .pool
                        .metrics
                        .ring_hits
                        .fetch_add(1, Ordering::Relaxed);
                    for raw in records {
                        let Ok(record) = LiveRecord::decode(raw) else {
                            self.fail(Status::data_loss("canonical record failed to decode"))
                                .await;
                            return;
                        };
                        let feed = &self.subscription.requirement.delivery.feed;
                        let interval = self.subscription.requirement.delivery.interval.as_deref();
                        if !is_product(feed, interval, &record.envelope) {
                            continue;
                        }
                        matched += 1;
                        if matched > max_matched {
                            if let Some(status) =
                                replay_end_status(&self.state, &ReplayEnd::Backlog)
                            {
                                self.fail(status).await;
                            }
                            return;
                        }
                        match self.deliver(&record).await {
                            Some(delivered) => {
                                if delivered {
                                    self.state.metrics.replayed.fetch_add(1, Ordering::Relaxed);
                                }
                                last = record.raw.offset;
                            }
                            None => return,
                        }
                    }
                }
                None => {
                    let Some(permit) = self
                        .state
                        .pool
                        .acquire(&self.subscription.consumer_id)
                        .await
                    else {
                        self.fail(Status::resource_exhausted(
                            "RATE_LIMITED:replay reader capacity is exhausted",
                        ))
                        .await;
                        return;
                    };
                    let requirement = &self.subscription.requirement.delivery;
                    let (mut records, cancel, reader) = spawn_scan(
                        self.state.clone(),
                        self.partition,
                        self.after,
                        barrier,
                        self.key.clone(),
                        (requirement.feed.clone(), requirement.interval.clone()),
                        None,
                        permit,
                    );
                    let _cancel = CancelOnDrop(cancel);
                    while let Some(record) = records.recv().await {
                        match self.deliver(&record).await {
                            Some(delivered) => {
                                if delivered {
                                    self.state.metrics.replayed.fetch_add(1, Ordering::Relaxed);
                                }
                                last = record.raw.offset;
                            }
                            None => return,
                        }
                    }
                    let end = reader
                        .await
                        .unwrap_or(ReplayEnd::Error("replay task failed".into()));
                    if let Some(status) = replay_end_status(&self.state, &end) {
                        self.fail(status).await;
                        return;
                    }
                }
            }
            last = last.max(barrier - 1);
        } else if self.after >= barrier {
            // This replica has not dispatched the cursor's record yet (the
            // client came from a replica further ahead). Wait, bounded.
            let deadline = Instant::now() + self.state.limits.catchup_deadline;
            loop {
                let next = self.state.hub.next_offset(self.partition).unwrap_or(0);
                if next > self.after {
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
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
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
        if !matches!(self.send(Ok(live)).await, Sent::Ok) {
            return;
        }
        let mut revocation = self.state.authority.watch();
        let mut filtered_since_delivery = 0usize;
        loop {
            let next = tokio::select! {
                next = self.subscription.next() => next,
                changed = revocation.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let authority = self.state.authority.current();
                    if let Err(error) = reauthorize(
                        &authority,
                        &self.principal,
                        &self.subscription.consumer_id,
                        &self.purpose,
                        &self.subscription.requirement,
                    ) {
                        self.state.metrics.revoked.fetch_add(1, Ordering::Relaxed);
                        self.fail(error.to_status()).await;
                        return;
                    }
                    continue;
                }
                () = self.sender.closed() => return,
            };
            match next {
                Next::Record(record) => match self.deliver(&record).await {
                    Some(true) => {
                        filtered_since_delivery = 0;
                        last = record.raw.offset;
                        self.state.metrics.delivered.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(false) => {
                        // Aged out while queued: the cursor moves past it, and
                        // a whole buffer of such records without a delivery
                        // is a slow consumer (DL-V2 R1.13).
                        last = record.raw.offset;
                        filtered_since_delivery += 1;
                        self.state
                            .metrics
                            .aged_out_at_read
                            .fetch_add(1, Ordering::Relaxed);
                        if filtered_since_delivery > self.subscription.depth {
                            self.backpressure(
                                last,
                                "records aged out of the bounded buffer before delivery; \
                                 replay from the last confirmed token is required",
                            )
                            .await;
                            return;
                        }
                    }
                    None => return,
                },
                Next::Overflow => {
                    self.backpressure(
                        last,
                        "bounded outbound buffer exhausted; replay is required",
                    )
                    .await;
                    return;
                }
                Next::Failed(reason) => {
                    self.fail(Status::data_loss(reason)).await;
                    return;
                }
                Next::Closed => return,
            }
        }
    }
}
