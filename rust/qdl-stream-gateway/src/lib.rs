//! Kafka-native V2 stream gateway, KN-1 vertical slice (guide 18.8, K1.5).
//!
//! Scope of this slice: an authenticated `Subscribe` that reads committed
//! canonical records straight from Kafka and delivers them through the
//! existing public gRPC contract to the real SDK, with cursor v3. It proves
//! the path and measures its cost before KN-2 builds the full service
//! (shared live readers, ring, replay pool, every public RPC). Each
//! subscription owns one bounded Kafka reader here; that is a prototype
//! choice, recorded as such, not the KN-2 design.

pub mod auth;
pub mod bundle;
pub mod generated;
pub mod reader;
pub mod requirement;
pub mod service;
pub mod tls;
