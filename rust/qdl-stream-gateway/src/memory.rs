//! Memory bounds of one gateway replica.
//!
//! Every budgeted pool must fit the container memory limit; the replica
//! refuses to start otherwise. The accounting uses measured allowances,
//! not a proof of process RSS or an OOM guarantee for arbitrary payloads.
//! The pools:
//! - the replay ring (raw records);
//! - two byte budgets for everything holding decoded records (charged
//!   `raw + 8 x payload`, see `hub::DECODED_FACTOR`, until the transport takes
//!   the record): live subscriber queues, and replay (channels and ring-replay
//!   references) - separate, so replay can never starve live delivery;
//! - librdkafka's prefetch per consumer;
//! - the transport of each admitted stream (Subscribe or Replay): tonic's
//!   encode buffer yields at 32 KiB and hyper holds one chunk until the peer's
//!   window opens, so a stream holds at most ~32 KiB plus one message;
//! - a measured process reserve (binary, runtime, TLS, allocator slack).

/// librdkafka prefetches up to `queued.max.messages.kbytes` per consumer
/// (default 64 MiB). With one live and up to `QDL_KN_REPLAY_READERS` replay
/// consumers per replica that default alone exceeds the container memory
/// (measured 268 MB RSS in the K2-T08 run), so both are bounded here.
pub const LIVE_QUEUE_KBYTES: u64 = 16_384;
pub const REPLAY_QUEUE_KBYTES: u64 = 4_096;

/// Transport bytes per admitted stream: tonic's 32 KiB encode yield plus one
/// message (canonical book max measured 56,495 B, plus its resume token).
pub const TRANSPORT_STREAM_BYTES: u64 = 96 * 1024;

/// Where the container memory limit is read (cgroup v2).
pub const CGROUP_MEMORY_MAX: &str = "/sys/fs/cgroup/memory.max";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryPlan {
    /// Replay ring bytes across all partitions (split evenly).
    pub ring_total: u64,
    /// Live subscriber queue bytes across all subscriptions.
    pub queue_total: u64,
    /// Replay channel and ring-replay bytes across all replays.
    pub replay_total: u64,
    pub replay_readers: u64,
    /// Admitted streams: `max_subscriptions + max_replay_rpcs`.
    pub streams: u64,
    /// Everything not bounded above: binary, TLS/HTTP/2 buffers, tasks and
    /// allocator slack (measured ~60 MiB RSS beyond the ring at the K2-T08
    /// fan-out target).
    pub reserve: u64,
    /// The container limit; `None` when unlimited.
    pub limit: Option<u64>,
}

impl MemoryPlan {
    pub fn kafka_bytes(&self) -> u64 {
        (LIVE_QUEUE_KBYTES + self.replay_readers * REPLAY_QUEUE_KBYTES) * 1024
    }

    pub fn transport_bytes(&self) -> u64 {
        self.streams * TRANSPORT_STREAM_BYTES
    }

    pub fn required(&self) -> u64 {
        self.ring_total
            + self.queue_total
            + self.replay_total
            + self.kafka_bytes()
            + self.transport_bytes()
            + self.reserve
    }

    /// Refuse a plan whose bounds exceed the container limit.
    pub fn check(&self) -> Result<(), String> {
        match self.limit {
            Some(limit) if self.required() > limit => Err(format!(
                "memory bounds exceed the container limit: ring {} + queues {} + replay {} + \
                 kafka {} + transport {} + reserve {} = {} > {limit} bytes; lower \
                 QDL_KN_RING_BYTES_TOTAL, QDL_KN_QUEUE_BYTES_TOTAL, QDL_KN_REPLAY_BYTES_TOTAL \
                 or QDL_KN_MAX_SUBSCRIPTIONS, or raise the limit",
                self.ring_total,
                self.queue_total,
                self.replay_total,
                self.kafka_bytes(),
                self.transport_bytes(),
                self.reserve,
                self.required()
            )),
            _ => Ok(()),
        }
    }

    pub fn ring_per_partition(&self, partitions: usize) -> usize {
        (self.ring_total / partitions.max(1) as u64) as usize
    }

    pub fn summary(&self) -> serde_json::Value {
        serde_json::json!({"ring_total": self.ring_total, "queue_total": self.queue_total,
            "replay_total": self.replay_total,
            "kafka": self.kafka_bytes(), "transport": self.transport_bytes(),
            "streams": self.streams, "reserve": self.reserve, "required": self.required(),
            "limit": self.limit})
    }
}

/// Parse cgroup v2 `memory.max` (`max` = unlimited).
pub fn parse_memory_max(text: &str) -> Option<u64> {
    text.trim().parse().ok()
}

pub fn container_memory_limit() -> Option<u64> {
    std::fs::read_to_string(CGROUP_MEMORY_MAX)
        .ok()
        .and_then(|text| parse_memory_max(&text))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;

    fn plan(limit: Option<u64>) -> MemoryPlan {
        MemoryPlan {
            ring_total: 48 * MIB,
            queue_total: 32 * MIB,
            replay_total: 32 * MIB,
            replay_readers: 4,
            streams: 384 + 32,
            reserve: 64 * MIB,
            limit,
        }
    }

    #[test]
    fn the_default_plan_fits_a_256_mib_replica() {
        // 48 + 64 + 32 + 416 x 96 KiB (39 MiB) + 64 = 247 MiB.
        assert_eq!(plan(None).required(), 208 * MIB + 416 * 96 * 1024);
        assert!(plan(Some(256 * MIB)).check().is_ok());
        // 1,024 admitted streams would not fit: transport is a real term.
        assert!(MemoryPlan {
            streams: 1_024 + 32,
            ..plan(Some(256 * MIB))
        }
        .check()
        .is_err());
    }

    #[test]
    fn the_1024_stream_candidate_keeps_measured_defaults_and_extra_headroom() {
        let candidate = MemoryPlan {
            streams: 1_024 + 32,
            ..plan(Some(384 * MIB))
        };
        assert_eq!(candidate.required(), 307 * MIB);
        assert_eq!(candidate.limit.unwrap() - candidate.required(), 77 * MIB);
        assert!(candidate.check().is_ok());
        // 320 MiB fits on paper but leaves just 13 MiB; neither profile is
        // a 1,024-stream load certificate without an actual capacity run.
        assert_eq!(320 * MIB - candidate.required(), 13 * MIB);
    }

    #[test]
    fn bounds_over_the_container_limit_refuse_to_start() {
        // The KN-2 slice-1 defaults (32 MiB ring per partition x 6, 256 MiB
        // of queues) could not fit a 256 MiB replica.
        let old = MemoryPlan {
            ring_total: 6 * 32 * MIB,
            queue_total: 256 * MIB,
            ..plan(Some(256 * MIB))
        };
        let error = old.check().unwrap_err();
        assert!(error.contains("exceed the container limit"), "{error}");
        assert!(MemoryPlan { limit: None, ..old }.check().is_ok());
    }

    #[test]
    fn the_ring_is_split_evenly_and_memory_max_parses() {
        assert_eq!(plan(None).ring_per_partition(6), (48 * MIB / 6) as usize);
        assert_eq!(parse_memory_max("268435456\n"), Some(256 * MIB));
        assert_eq!(parse_memory_max("max\n"), None);
    }
}
