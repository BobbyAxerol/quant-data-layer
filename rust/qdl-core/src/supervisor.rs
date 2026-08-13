use crate::transport::{FencingLease, QueuePolicy};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    Disabled,
    Connecting,
    Subscribing,
    Live,
    Degraded,
    Gapped,
    Resyncing,
    Offline,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceSession {
    pub source_id: String,
    pub connection_generation: u64,
    pub lease: FencingLease,
}

impl SourceSession {
    pub fn permits_publish(&self, generation: u64, lease_epoch: u64, now_ns: i64) -> bool {
        generation == self.connection_generation && self.lease.permits(lease_epoch, now_ns)
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionSupervisor {
    state: ConnectionState,
    generation: u64,
}

impl Default for ConnectionSupervisor {
    fn default() -> Self {
        Self { state: ConnectionState::Disabled, generation: 0 }
    }
}

impl ConnectionSupervisor {
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn connect(&mut self) {
        self.generation = self.generation.saturating_add(1);
        self.state = ConnectionState::Connecting;
    }

    pub fn subscribed(&mut self) -> Result<(), String> {
        if self.state != ConnectionState::Connecting {
            return Err("subscription acknowledgement outside connecting state".into());
        }
        self.state = ConnectionState::Subscribing;
        Ok(())
    }

    pub fn synchronized(&mut self) -> Result<(), String> {
        if self.state != ConnectionState::Subscribing && self.state != ConnectionState::Resyncing {
            return Err("synchronization outside subscribing/resync state".into());
        }
        self.state = ConnectionState::Live;
        Ok(())
    }

    pub fn gap(&mut self) {
        self.state = ConnectionState::Gapped;
    }

    pub fn resync(&mut self) -> Result<(), String> {
        if self.state != ConnectionState::Gapped && self.state != ConnectionState::Degraded {
            return Err("resync requires gapped/degraded state".into());
        }
        self.state = ConnectionState::Resyncing;
        Ok(())
    }

    pub fn transport_down(&mut self) {
        self.state = ConnectionState::Degraded;
    }

    pub fn stop(&mut self) {
        self.state = ConnectionState::Offline;
    }
}

pub fn queue_accepts(policy: QueuePolicy, has_capacity: bool) -> bool {
    match policy {
        QueuePolicy::LosslessBackpressure => has_capacity,
        QueuePolicy::LatestStateCoalescing => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{queue_accepts, ConnectionState, ConnectionSupervisor, SourceSession};
    use crate::transport::{FencingLease, QueuePolicy};

    #[test]
    fn lifecycle_requires_resync_after_gap() {
        let mut supervisor = ConnectionSupervisor::default();
        supervisor.connect();
        supervisor.subscribed().unwrap();
        supervisor.synchronized().unwrap();
        supervisor.gap();
        assert_eq!(supervisor.state(), ConnectionState::Gapped);
        supervisor.resync().unwrap();
        supervisor.synchronized().unwrap();
        assert_eq!(supervisor.state(), ConnectionState::Live);
    }

    #[test]
    fn stale_generation_or_fencing_epoch_cannot_publish() {
        let session = SourceSession {
            source_id: "source-1".into(),
            connection_generation: 4,
            lease: FencingLease { epoch: 8, expires_at_ns: 1_000 },
        };
        assert!(session.permits_publish(4, 8, 999));
        assert!(!session.permits_publish(3, 8, 999));
        assert!(!session.permits_publish(4, 7, 999));
        assert!(!session.permits_publish(4, 8, 1_000));
        assert!(!queue_accepts(QueuePolicy::LosslessBackpressure, false));
        assert!(queue_accepts(QueuePolicy::LatestStateCoalescing, false));
    }
}
