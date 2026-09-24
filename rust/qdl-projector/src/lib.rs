//! Kafka-native V2 projector (guide 18.10, KN-3).
//!
//! Stage A ([`stage_a`]) reads committed canonical records and publishes
//! logical state records (`md.latest.v2`, `md.bars.v2`) together with the
//! consumed input offsets in one Kafka transaction: a state record is durable
//! only when that transaction commits, and an input offset never moves ahead
//! of the state it produced. Stage B applies the state topics to the market
//! cache with generation/owner fences (Kafka EOS alone does not protect an
//! external sink).

pub mod kafka_pipe;
pub mod stage_a;
