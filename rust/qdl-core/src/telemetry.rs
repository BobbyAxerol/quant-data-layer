use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct TransportTelemetry {
    accepted: AtomicU64,
    duplicates: AtomicU64,
    retries: AtomicU64,
    blocked: AtomicU64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportSnapshot {
    pub accepted: u64,
    pub duplicates: u64,
    pub retries: u64,
    pub blocked: u64,
}

impl TransportTelemetry {
    pub fn record_accepted(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_duplicate(&self) {
        self.duplicates.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_blocked(&self) {
        self.blocked.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TransportSnapshot {
        TransportSnapshot {
            accepted: self.accepted.load(Ordering::Relaxed),
            duplicates: self.duplicates.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            blocked: self.blocked.load(Ordering::Relaxed),
        }
    }
}
