//! `MarketDataStreamService` for the KN-1 slice.
//!
//! `Subscribe` follows `qdl/stream/grpc_service.py`: authenticate, parse the
//! requirement, consumer/permission/requirement/buffer checks, cursor v3,
//! scope, per-consumer stream capacity, then REPLAYING -> records -> LIVE.
//! Replay, GetSnapshot and GetFeedStatus are implemented in KN-2; this
//! prototype never serves production consumers.

use crate::auth::{authenticate, AccessError, JwtConfig, RequestQuota};
use crate::bundle::Bundle;
use crate::generated::marketdata_v2::EventEnvelope;
use crate::generated::query_v2 as query;
use crate::generated::query_v2::market_data_stream_service_server::MarketDataStreamService;
use crate::reader::{assign_from, high_watermark, position, KafkaSettings};
use crate::requirement::{delivery_decision, require_requirement, Delivery, StreamRequirement};
use prost::Message as _;
use qdl_contracts::cursor_v3::{CursorError, CursorV3Claims, CursorV3Codec, CursorV3Expectation};
use rdkafka::message::Message;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

pub struct GatewayState {
    pub jwt: JwtConfig,
    pub bundle: Bundle,
    pub quota: Arc<dyn RequestQuota>,
    pub codec: CursorV3Codec,
    pub expectation: CursorV3Expectation,
    pub kafka: KafkaSettings,
    pub cursor_ttl_seconds: u64,
    pub metrics: Metrics,
    streams: Mutex<HashMap<String, u64>>,
}

#[derive(Default)]
pub struct Metrics {
    pub subscriptions_opened: AtomicU64,
    pub subscriptions_active: AtomicU64,
    pub delivered: AtomicU64,
    pub other_product: AtomicU64,
    pub too_old: AtomicU64,
    pub overflow: AtomicU64,
    pub refused: AtomicU64,
}

impl GatewayState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        jwt: JwtConfig,
        bundle: Bundle,
        quota: Arc<dyn RequestQuota>,
        codec: CursorV3Codec,
        expectation: CursorV3Expectation,
        kafka: KafkaSettings,
        cursor_ttl_seconds: u64,
    ) -> Self {
        Self {
            jwt,
            bundle,
            quota,
            codec,
            expectation,
            kafka,
            cursor_ttl_seconds,
            metrics: Metrics::default(),
            streams: Mutex::new(HashMap::new()),
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
            }
        }
        self.state
            .metrics
            .subscriptions_active
            .fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct Gateway {
    pub state: Arc<GatewayState>,
}

/// How long a subscription may accept nothing before it is closed as slow.
const SLOW_CONSUMER_AFTER: Duration = Duration::from_secs(10);

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos() as u64)
        .unwrap_or_default()
}

fn metadata<'a>(request: &'a Request<query::SubscribeRequest>, name: &str) -> &'a str {
    request
        .metadata()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
}

fn cursor_status(error: CursorError) -> Status {
    match error {
        CursorError::Expired(reason) => Status::out_of_range(format!("CURSOR_EXPIRED:{reason}")),
        CursorError::Invalid(reason) => {
            Status::invalid_argument(format!("CURSOR_INVALID:{reason}"))
        }
    }
}

fn control(
    state: i32,
    code: &str,
    detail: &str,
    high: i64,
    token: &str,
) -> query::SubscribeResponse {
    query::SubscribeResponse {
        record: Some(query::StreamRecord {
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
        }),
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
        let authorization = metadata(&request, "authorization").to_owned();
        let header_consumer = metadata(&request, "x-qdl-consumer-id").to_owned();
        let purpose = metadata(&request, "x-qdl-purpose").to_owned();
        let access = {
            let state = state.clone();
            tokio::task::spawn_blocking(move || {
                authenticate(
                    &state.jwt,
                    &state.bundle,
                    state.quota.as_ref(),
                    &authorization,
                    &header_consumer,
                    &purpose,
                )
            })
            .await
            .map_err(|_| Status::internal("authentication task failed"))?
        }
        .map_err(|error| {
            state.metrics.refused.fetch_add(1, Ordering::Relaxed);
            error.to_status()
        })?;
        let body = request.into_inner();
        let refuse = |status: Status| {
            state.metrics.refused.fetch_add(1, Ordering::Relaxed);
            status
        };
        let requirement = body
            .requirement
            .as_ref()
            .ok_or_else(|| "requirement is required".to_owned())
            .and_then(StreamRequirement::from_proto)
            .map_err(|error| refuse(Status::invalid_argument(format!("CURSOR_INVALID:{error}"))))?;
        if body.consumer_id != access.manifest.consumer_id {
            return Err(refuse(
                AccessError::PermissionDenied(
                    "authenticated workload is not bound to the requested consumer".into(),
                )
                .to_status(),
            ));
        }
        access
            .require_stream_read()
            .map_err(|error| refuse(error.to_status()))?;
        require_requirement(&access.manifest, &requirement)
            .map_err(|error| refuse(error.to_status()))?;
        let buffer = if body.max_buffer_events == 0 {
            access.manifest.quotas.max_buffer_events
        } else {
            u64::from(body.max_buffer_events)
        };
        if buffer < 1 || buffer > access.manifest.quotas.max_buffer_events {
            return Err(refuse(Status::permission_denied(
                "stream buffer exceeds the registered consumer quota",
            )));
        }
        let delivery = &requirement.delivery;
        let binding = state
            .bundle
            .binding_for(
                &delivery.instrument_uid,
                &delivery.feed,
                delivery.interval.as_deref(),
                &delivery.source_policy_id,
            )
            .cloned()
            .ok_or_else(|| {
                refuse(Status::invalid_argument(
                    "CURSOR_INVALID:requirement has no stable source binding",
                ))
            })?;
        let claims = state
            .codec
            .verify(
                &body.cursor_token,
                &body.consumer_id,
                &state.expectation.environment,
                &delivery.digest(),
                &state.expectation,
                now_ns(),
            )
            .map_err(|error| refuse(cursor_status(error)))?;
        if claims.product_key != binding.product_key {
            return Err(refuse(Status::invalid_argument("CURSOR_INVALID:SCOPE")));
        }
        {
            let mut streams = state
                .streams
                .lock()
                .map_err(|_| Status::internal("stream registry poisoned"))?;
            let count = streams.entry(body.consumer_id.clone()).or_insert(0);
            if *count >= access.manifest.quotas.max_streams {
                return Err(refuse(Status::resource_exhausted(
                    "RATE_LIMITED:consumer stream capacity is exhausted",
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
        let (sender, receiver) = mpsc::channel(buffer as usize + 2);
        tokio::spawn(run_subscription(
            state,
            slot,
            sender,
            claims,
            requirement,
            binding.physical_key.into_bytes(),
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn replay(
        &self,
        _request: Request<query::ReplayRequest>,
    ) -> Result<Response<Self::ReplayStream>, Status> {
        Err(Status::unimplemented("Replay is implemented in KN-2"))
    }

    async fn get_snapshot(
        &self,
        _request: Request<query::GetSnapshotRequest>,
    ) -> Result<Response<query::GetSnapshotResponse>, Status> {
        Err(Status::unimplemented("GetSnapshot is implemented in KN-2"))
    }

    async fn get_feed_status(
        &self,
        _request: Request<query::GetFeedStatusRequest>,
    ) -> Result<Response<query::GetFeedStatusResponse>, Status> {
        Err(Status::unimplemented(
            "GetFeedStatus is implemented in KN-2",
        ))
    }
}

fn sign(state: &GatewayState, claims: &CursorV3Claims, offset: i64) -> Result<String, Status> {
    let now = now_ns();
    let mut next = claims.clone();
    next.key_id = state.codec.active_key_id().to_owned();
    next.source_offset = offset.max(0) as u64;
    next.issued_at_ns = now;
    next.expires_at_ns = now + state.cursor_ttl_seconds * 1_000_000_000;
    state
        .codec
        .encode(&next)
        .map_err(|error| Status::internal(format!("cursor signing failed: {}", error.reason())))
}

async fn run_subscription(
    state: Arc<GatewayState>,
    _slot: StreamSlot,
    sender: mpsc::Sender<Result<query::SubscribeResponse, Status>>,
    claims: CursorV3Claims,
    requirement: StreamRequirement,
    physical_key: Vec<u8>,
) {
    let topic = state.kafka.topic.clone();
    let partition = claims.source_partition as i32;
    let start = claims.source_offset as i64 + 1;
    let consumer = match state
        .kafka
        .stream_consumer()
        .and_then(|consumer| assign_from(&consumer, &topic, partition, start).map(|_| consumer))
    {
        Ok(consumer) => consumer,
        Err(error) => {
            let _ = sender
                .send(Err(Status::unavailable(format!("kafka: {error}"))))
                .await;
            return;
        }
    };
    let high = match high_watermark(&consumer, &topic, partition) {
        Ok(high) => high,
        Err(error) => {
            let _ = sender
                .send(Err(Status::unavailable(format!("kafka: {error}"))))
                .await;
            return;
        }
    };
    let mut token = match sign(&state, &claims, start - 1) {
        Ok(token) => token,
        Err(status) => {
            let _ = sender.send(Err(status)).await;
            return;
        }
    };
    let replaying = query::StreamControlState::Replaying as i32;
    if sender
        .send(Ok(control(
            replaying,
            "REPLAYING",
            "replaying committed records after the supplied cursor",
            high,
            &token,
        )))
        .await
        .is_err()
    {
        return;
    }
    let mut live = false;
    let mut scanned = start - 1;
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    loop {
        if !live && position(&consumer, &topic, partition).is_some_and(|next| next >= high) {
            live = true;
            token = match sign(&state, &claims, scanned) {
                Ok(token) => token,
                Err(status) => {
                    let _ = sender.send(Err(status)).await;
                    return;
                }
            };
            let state_live = query::StreamControlState::Live as i32;
            if sender
                .send(Ok(control(
                    state_live,
                    "LIVE",
                    "committed replay is complete; live delivery is active",
                    high,
                    &token,
                )))
                .await
                .is_err()
            {
                return;
            }
        }
        let message = tokio::select! {
            message = consumer.recv() => message,
            _ = tick.tick() => continue,
            () = sender.closed() => return,
        };
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                let _ = sender
                    .send(Err(Status::unavailable(format!("kafka: {error}"))))
                    .await;
                return;
            }
        };
        scanned = message.offset();
        if message.key() != Some(physical_key.as_slice()) {
            continue;
        }
        let Ok(envelope) = EventEnvelope::decode(message.payload().unwrap_or_default()) else {
            let _ = sender
                .send(Err(Status::data_loss("canonical record failed to decode")))
                .await;
            return;
        };
        match delivery_decision(&requirement, &envelope, now_ns() as i64) {
            Delivery::OtherProduct => {
                state.metrics.other_product.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Delivery::TooOld => {
                state.metrics.too_old.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Delivery::Deliver => {}
        }
        token = match sign(&state, &claims, scanned) {
            Ok(token) => token,
            Err(status) => {
                let _ = sender.send(Err(status)).await;
                return;
            }
        };
        let record = query::SubscribeResponse {
            record: Some(query::StreamRecord {
                logical_offset: scanned as u64,
                resume_token: token.clone(),
                payload: Some(query::stream_record::Payload::Event(envelope)),
            }),
        };
        // One reader per subscription, so the reader can wait for the client
        // (replay bursts included). A client that accepts nothing for
        // SLOW_CONSUMER_AFTER is told so - lossless feeds never drop and
        // continue - and its reader and slot are released.
        match tokio::time::timeout(SLOW_CONSUMER_AFTER, sender.send(Ok(record))).await {
            Ok(Ok(())) => {
                state.metrics.delivered.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err(_)) => return,
            Err(_) => {
                state.metrics.overflow.fetch_add(1, Ordering::Relaxed);
                let _ = tokio::time::timeout(
                    Duration::from_secs(5),
                    sender.send(Err(Status::resource_exhausted(
                        "RATE_LIMITED:slow consumer exceeded its stream buffer",
                    ))),
                )
                .await;
                return;
            }
        }
    }
}
