//! Memory bounds of one gateway replica.
//!
//! Every bounded pool (the replay ring, the subscriber queues and librdkafka's
//! prefetch per consumer) plus a measured process reserve must fit the
//! container memory limit, so an overload ends in typed backpressure and never
//! in an OOM kill. The replica refuses to start when they do not fit.

/// librdkafka prefetches up to `queued.max.messages.kbytes` per consumer
/// (default 64 MiB). With one live and up to `QDL_KN_REPLAY_READERS` replay
/// consumers per replica that default alone exceeds the container memory
/// (measured 268 MB RSS in the K2-T08 run), so both are bounded here.
pub const LIVE_QUEUE_KBYTES: u64 = 16_384;
pub const REPLAY_QUEUE_KBYTES: u64 = 4_096;

/// Where the container memory limit is read (cgroup v2).
pub const CGROUP_MEMORY_MAX: &str = "/sys/fs/cgroup/memory.max";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryPlan {
    /// Replay ring bytes across all partitions (split evenly).
    pub ring_total: u64,
    /// Subscriber queue bytes across all subscriptions (`ByteBudget`).
    pub queue_total: u64,
    pub replay_readers: u64,
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

    pub fn required(&self) -> u64 {
        self.ring_total + self.queue_total + self.kafka_bytes() + self.reserve
    }

    /// Refuse a plan whose bounds exceed the container limit.
    pub fn check(&self) -> Result<(), String> {
        match self.limit {
            Some(limit) if self.required() > limit => Err(format!(
                "memory bounds exceed the container limit: ring {} + queues {} + kafka {} + \
                 reserve {} = {} > {limit} bytes; lower QDL_KN_RING_BYTES_TOTAL or \
                 QDL_KN_QUEUE_BYTES_TOTAL, or raise the limit",
                self.ring_total,
                self.queue_total,
                self.kafka_bytes(),
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
            "kafka": self.kafka_bytes(), "reserve": self.reserve, "required": self.required(),
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
            ring_total: 64 * MIB,
            queue_total: 64 * MIB,
            replay_readers: 4,
            reserve: 64 * MIB,
            limit,
        }
    }

    #[test]
    fn the_default_plan_fits_a_256_mib_replica() {
        assert_eq!(plan(None).required(), 224 * MIB);
        assert!(plan(Some(256 * MIB)).check().is_ok());
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
        assert_eq!(plan(None).ring_per_partition(6), (64 * MIB / 6) as usize);
        assert_eq!(parse_memory_max("268435456\n"), Some(256 * MIB));
        assert_eq!(parse_memory_max("max\n"), None);
    }
}
