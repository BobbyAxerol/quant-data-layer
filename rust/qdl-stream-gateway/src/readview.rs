//! Snapshot and feed-status read view (KN-2 K2.1, D6).
//!
//! GetSnapshot and GetFeedStatus run the full Python access sequence in the
//! service, then ask a [`ReadView`]. KN-4 (D29) attaches the market cache
//! through the paired Query replica ([`crate::query_view::QueryReadView`]);
//! without that configuration the gateway uses [`NotReadyReadView`], which
//! answers typed `DATA_NOT_READY` (the `UnavailableSnapshotLoader`
//! precedent, mapped by the Python service to `FAILED_PRECONDITION
//! "{code}:{detail}"`). The route never returns UNIMPLEMENTED and never
//! serves fixture data outside tests.

use crate::generated::query_v2 as query;
use crate::requirement::StreamRequirement;

/// How a refusal is carried on gRPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadViewStatus {
    /// A typed `QueryServiceError`: `FAILED_PRECONDITION "{code}:{detail}"`.
    FailedPrecondition,
    /// An admission refusal: `RESOURCE_EXHAUSTED "RATE_LIMITED:{detail}"`.
    ResourceExhausted,
    /// The request itself is invalid: `INVALID_ARGUMENT "{detail}"`.
    InvalidArgument,
    /// The read view cannot be reached: `UNAVAILABLE "DEPENDENCY_UNAVAILABLE:..."`.
    Unavailable,
}

/// A typed refusal: canonical code + detail + gRPC carriage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadViewError {
    pub code: String,
    pub detail: String,
    pub status: ReadViewStatus,
}

impl ReadViewError {
    pub fn precondition(code: &str, detail: &str) -> Self {
        Self {
            code: code.to_owned(),
            detail: detail.to_owned(),
            status: ReadViewStatus::FailedPrecondition,
        }
    }

    pub fn unavailable(detail: String) -> Self {
        Self {
            code: "DEPENDENCY_UNAVAILABLE".into(),
            detail,
            status: ReadViewStatus::Unavailable,
        }
    }

    pub fn to_status(&self) -> tonic::Status {
        let message = format!("{}:{}", self.code, self.detail);
        match self.status {
            ReadViewStatus::FailedPrecondition => tonic::Status::failed_precondition(message),
            ReadViewStatus::ResourceExhausted => tonic::Status::resource_exhausted(message),
            ReadViewStatus::InvalidArgument => tonic::Status::invalid_argument(self.detail.clone()),
            ReadViewStatus::Unavailable => tonic::Status::unavailable(message),
        }
    }
}

#[tonic::async_trait]
pub trait ReadView: Send + Sync {
    /// `requirement` is the validated view, `proto` the exact request field
    /// (the Query read view validates it again with the Python oracle).
    async fn snapshot(
        &self,
        requirement: &StreamRequirement,
        proto: &query::DataRequirement,
        consumer_id: &str,
    ) -> Result<query::GetSnapshotResponse, ReadViewError>;

    async fn status(
        &self,
        requirement: &StreamRequirement,
        proto: &query::DataRequirement,
        consumer_id: &str,
    ) -> Result<query::GetFeedStatusResponse, ReadViewError>;
}

pub const NOT_READY_DETAIL: &str =
    "native read view is not attached: QDL_KN_READ_VIEW_URLS names no Query read view";

pub struct NotReadyReadView;

#[tonic::async_trait]
impl ReadView for NotReadyReadView {
    async fn snapshot(
        &self,
        _requirement: &StreamRequirement,
        _proto: &query::DataRequirement,
        _consumer_id: &str,
    ) -> Result<query::GetSnapshotResponse, ReadViewError> {
        Err(ReadViewError::precondition(
            "DATA_NOT_READY",
            NOT_READY_DETAIL,
        ))
    }

    async fn status(
        &self,
        _requirement: &StreamRequirement,
        _proto: &query::DataRequirement,
        _consumer_id: &str,
    ) -> Result<query::GetFeedStatusResponse, ReadViewError> {
        Err(ReadViewError::precondition(
            "DATA_NOT_READY",
            NOT_READY_DETAIL,
        ))
    }
}
