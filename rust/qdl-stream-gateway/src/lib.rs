//! Kafka-native V2 stream gateway (guide 18.9, KN-2).
//!
//! The public `MarketDataStreamService` served natively from committed
//! canonical Kafka records: one shared committed reader per replica with an
//! indexed fan-out ([`hub`]), bounded subscriber queues with the lifecycle
//! delivery policy ([`subscription`]), replay-to-live through a barrier and a
//! bounded replay reader pool ([`replay`]), cursor v3, the Python access
//! rules ([`auth`], [`authority`]) and the snapshot/status read view
//! ([`readview`], market cache attached in KN-4).

pub mod auth;
pub mod authority;
pub mod bundle;
pub mod generated;
pub mod hub;
pub mod memory;
pub mod query_view;
pub mod reader;
pub mod readview;
pub mod replay;
pub mod requirement;
pub mod service;
pub mod subscription;
pub mod tls;
