//! Private Query -> canonical read. Never delegates to Query or reads Redis.
//! mTLS is inherited from the existing listener; domain-separated HMAC binds
//! the exact request. The public consumer entitlement/quality owner is Query.
use crate::{authority::AuthorityHandle, hub::Hub};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use prost::Message;
use ring::hmac;
use serde::Deserialize;
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tonic::{codegen::*, Request, Response, Status};

pub const PATH: &str = "/qdl.internal.v2.CanonicalHotView/ReadLatest";
pub const SCHEMA: &str = "qdl.kn.canonical-hot-view.v1";
pub const DOMAIN: &[u8] = b"qdl.kn.canonical-hot-view.v1\0";

// Wire-compatible with google.protobuf.BytesValue. Internal JSON is versioned
// separately from the public MarketDataStreamService contract.
#[derive(Clone, PartialEq, Message)]
pub struct Body {
    #[prost(bytes = "vec", tag = "1")]
    pub value: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadRequest {
    schema: String,
    binding_id: String,
    issued_at_ns: u64,
    environment: String,
    stream: String,
    source_topic_id: String,
    partition_plan_epoch: u64,
    source_policy_revision: u64,
    catalog_revision: u64,
    route_generation: String,
    schema_major: u64,
}

pub struct HotView {
    authority: Arc<AuthorityHandle>,
    hub: Arc<Hub>,
    key: hmac::Key,
    slots: Arc<tokio::sync::Semaphore>,
}

impl HotView {
    pub fn new(
        authority: Arc<AuthorityHandle>,
        hub: Arc<Hub>,
        secret: &[u8],
    ) -> Result<Arc<Self>, String> {
        if secret.len() < 32 {
            return Err("hot-view secret must contain at least 32 bytes".into());
        }
        Ok(Arc::new(Self {
            authority,
            hub,
            key: hmac::Key::new(hmac::HMAC_SHA256, secret),
            slots: Arc::new(tokio::sync::Semaphore::new(8)),
        }))
    }

    fn authorize(&self, request: &Request<Body>, now_ns: u64) -> Result<ReadRequest, Status> {
        let body = &request.get_ref().value;
        if body.len() > 8192 {
            return Err(Status::invalid_argument("HOT_REQUEST_TOO_LARGE"));
        }
        let tag = request
            .metadata()
            .get_bin("x-qdl-hot-signature-bin")
            .ok_or_else(|| Status::unauthenticated("HOT_SIGNATURE_REQUIRED"))?
            .to_bytes()
            .map_err(|_| Status::unauthenticated("HOT_SIGNATURE_INVALID"))?;
        let mut signed = Vec::with_capacity(DOMAIN.len() + body.len());
        signed.extend_from_slice(DOMAIN);
        signed.extend_from_slice(body);
        hmac::verify(&self.key, &signed, &tag)
            .map_err(|_| Status::unauthenticated("HOT_SIGNATURE_INVALID"))?;
        let value: ReadRequest = serde_json::from_slice(body)
            .map_err(|_| Status::invalid_argument("HOT_REQUEST_INVALID"))?;
        if value.schema != SCHEMA
            || value.issued_at_ns > now_ns
            || now_ns - value.issued_at_ns > 2_000_000_000
        {
            return Err(Status::failed_precondition("HOT_REQUEST_EXPIRED"));
        }
        Ok(value)
    }

    pub async fn read(self: Arc<Self>, request: Request<Body>) -> Result<Response<Body>, Status> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::unavailable("HOT_CLOCK_INVALID"))?
            .as_nanos() as u64;
        let value = self.authorize(&request, now)?;
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("HOT_READ_CAPACITY"))?;
        let deadline = Instant::now() + Duration::from_millis(100);
        // The permit is owned by the blocking operation, even if the RPC is
        // cancelled. No detached unbounded work and no per-request Kafka reader.
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let (authority, generation) = self.authority.snapshot();
            let expected = &authority.expectation;
            if value.environment != expected.environment || value.stream != expected.stream
                || value.source_topic_id != expected.source_topic_id
                || value.partition_plan_epoch != expected.partition_plan_epoch
                || value.source_policy_revision != expected.source_policy_revision
                || value.catalog_revision != expected.catalog_revision
                || value.route_generation != expected.route_generation
                || value.schema_major != expected.schema_major {
                return Err(Status::failed_precondition("HOT_AUTHORITY_MISMATCH"));
            }
            let binding = authority.bundle.bindings.iter().find(|b| b.binding_id == value.binding_id)
                .ok_or_else(|| Status::permission_denied("HOT_BINDING_UNKNOWN"))?;
            if binding.interval.is_some() { return Err(Status::permission_denied("HOT_FEED_UNSUPPORTED")); }
            let hot = self.hub.latest_hot(binding.physical_key.as_bytes(), &binding.feed, deadline)
                .map_err(|error| match error.as_str() {
                    "HOT_RECORD_INVALID" | "HOT_PARTITION_MISMATCH" | "HOT_RECORD_PRODUCT_MISMATCH" => Status::data_loss(error),
                    "HOT_RECORD_GAPPED" | "HOT_RECORD_GENERATION_MISMATCH" | "HOT_BOOK_UNVERIFIED" => Status::failed_precondition(error),
                    "HOT_FEED_UNSUPPORTED" => Status::permission_denied(error),
                    "HOT_READ_DEADLINE" => Status::deadline_exceeded(error),
                    "HOT_READER_BUSY" => Status::resource_exhausted(error),
                    _ => Status::unavailable(error),
                })?;
            let raw = &hot.raw;
            let envelope = crate::generated::marketdata_v2::EventEnvelope::decode(raw.payload.as_slice())
                .map_err(|_| Status::data_loss("HOT_RECORD_INVALID"))?;
            if envelope.instrument_uid != binding.instrument_uid || envelope.venue != binding.venue
                || envelope.market != binding.market {
                return Err(Status::data_loss("HOT_IDENTITY_MISMATCH"));
            }
            if self.authority.generation() != generation {
                return Err(Status::failed_precondition("HOT_AUTHORITY_CHANGED"));
            }
            if Instant::now() >= deadline { return Err(Status::deadline_exceeded("HOT_READ_DEADLINE")); }
            let body = serde_json::json!({"schema": SCHEMA, "binding_id": binding.binding_id,
                "product_key": binding.product_key, "physical_key": binding.physical_key,
                "environment": expected.environment, "stream": expected.stream,
                "source_topic_id": expected.source_topic_id, "source_partition": raw.partition,
                "source_offset": hot.boundary_offset, "record_offset": raw.offset, "partition_plan_epoch": expected.partition_plan_epoch,
                "catalog_revision": expected.catalog_revision, "source_policy_revision": expected.source_policy_revision,
                "route_generation": expected.route_generation, "schema_major": expected.schema_major,
                "canonical": STANDARD.encode(&raw.payload)});
            Ok(Response::new(Body { value: serde_json::to_vec(&body)
                .map_err(|_| Status::internal("HOT_ENCODING_FAILED"))? }))
        }).await.map_err(|_| Status::internal("HOT_WORKER_FAILED"))?
    }
}

#[derive(Clone)]
pub struct HotViewServer(pub Arc<HotView>);
impl tonic::server::NamedService for HotViewServer {
    const NAME: &'static str = "qdl.internal.v2.CanonicalHotView";
}
impl<B> Service<http::Request<B>> for HotViewServer
where
    B: BodyTrait + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path() != PATH {
            return Box::pin(async {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(tonic::body::empty_body())
                    .expect("static response"))
            });
        }
        struct Read(Arc<HotView>);
        impl tonic::server::UnaryService<Body> for Read {
            type Response = Body;
            type Future = BoxFuture<Response<Body>, Status>;
            fn call(&mut self, request: Request<Body>) -> Self::Future {
                Box::pin(self.0.clone().read(request))
            }
        }
        let inner = self.0.clone();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                .max_decoding_message_size(8256)
                .max_encoding_message_size(512 * 1024);
            Ok(grpc.unary(Read(inner), request).await)
        })
    }
}
use tonic::codegen::Body as BodyTrait;
