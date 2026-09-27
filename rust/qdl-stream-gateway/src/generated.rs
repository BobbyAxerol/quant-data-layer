//! Generated gRPC service for `qdl.query.v2.MarketDataStreamService`.
//!
//! Messages come from `qdl-contracts` (prost); the service code comes from
//! the committed `buf` output (`neoeinstein-tonic`), which refers to the
//! messages through `super::`.

#[allow(clippy::all, clippy::pedantic)]
pub mod query_v2 {
    pub use qdl_contracts::qdl::query::v2::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../generated/rust/qdl.query.v2.tonic.rs"
    ));
}

pub use qdl_contracts::qdl::marketdata::v2 as marketdata_v2;
