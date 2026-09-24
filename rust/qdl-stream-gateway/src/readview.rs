//! Snapshot and feed-status read view (KN-2 K2.1, D6).
//!
//! GetSnapshot and GetFeedStatus run the full Python access sequence in the
//! service, then ask a [`ReadView`]. The market cache that backs the real
//! view is KN-3/KN-4 work; until it is attached the gateway uses
//! [`NotReadyReadView`], which answers typed `DATA_NOT_READY` (the
//! `UnavailableSnapshotLoader` precedent, mapped by the Python service to
//! `FAILED_PRECONDITION "{code}:{detail}"`). The route never returns
//! UNIMPLEMENTED and never serves fixture data outside tests.

use crate::generated::query_v2 as query;
use crate::requirement::StreamRequirement;

/// A typed `QueryServiceError`: canonical code + detail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadViewError {
    pub code: String,
    pub detail: String,
}

impl ReadViewError {
    pub fn to_status(&self) -> tonic::Status {
        tonic::Status::failed_precondition(format!("{}:{}", self.code, self.detail))
    }
}

#[tonic::async_trait]
pub trait ReadView: Send + Sync {
    async fn snapshot(
        &self,
        requirement: &StreamRequirement,
        consumer_id: &str,
    ) -> Result<query::GetSnapshotResponse, ReadViewError>;

    async fn status(
        &self,
        requirement: &StreamRequirement,
    ) -> Result<query::GetFeedStatusResponse, ReadViewError>;
}

pub const NOT_READY_DETAIL: &str =
    "native read view is not attached: the market cache integration is KN-4";

pub struct NotReadyReadView;

#[tonic::async_trait]
impl ReadView for NotReadyReadView {
    async fn snapshot(
        &self,
        _requirement: &StreamRequirement,
        _consumer_id: &str,
    ) -> Result<query::GetSnapshotResponse, ReadViewError> {
        Err(ReadViewError {
            code: "DATA_NOT_READY".into(),
            detail: NOT_READY_DETAIL.into(),
        })
    }

    async fn status(
        &self,
        _requirement: &StreamRequirement,
    ) -> Result<query::GetFeedStatusResponse, ReadViewError> {
        Err(ReadViewError {
            code: "DATA_NOT_READY".into(),
            detail: NOT_READY_DETAIL.into(),
        })
    }
}
