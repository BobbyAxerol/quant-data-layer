use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionState {
    Disconnected,
    Connecting,
    Subscribing,
    Live,
    Degraded,
    Recovering,
    Draining,
    Blocked,
    Stopped,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionError {
    InvalidTransition,
    UnknownSubscription(String),
    SubscriptionRejected(String),
    StaleGeneration,
    LeaseRejected,
    HeartbeatExpired,
}

impl Display for SessionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTransition => write!(formatter, "invalid session transition"),
            Self::UnknownSubscription(value) => {
                write!(formatter, "unknown subscription: {value}")
            }
            Self::SubscriptionRejected(value) => {
                write!(formatter, "subscription rejected: {value}")
            }
            Self::StaleGeneration => write!(formatter, "stale connection generation"),
            Self::LeaseRejected => write!(formatter, "lease/fencing rejected"),
            Self::HeartbeatExpired => write!(formatter, "heartbeat/read deadline expired"),
        }
    }
}

impl std::error::Error for SessionError {}

#[derive(Clone, Debug)]
pub struct SessionPolicy {
    pub heartbeat_timeout_ns: i64,
    pub read_timeout_ns: i64,
    pub reconnect_budget: u32,
}

#[derive(Clone, Debug)]
pub struct VenueSession {
    state: SessionState,
    source_session_id: String,
    generation: u64,
    lease_epoch: u64,
    lease_expires_at_ns: i64,
    desired: BTreeSet<String>,
    acknowledged: BTreeSet<String>,
    pending: BTreeMap<String, u64>,
    last_frame_at_ns: i64,
    last_heartbeat_at_ns: i64,
    reconnect_attempts: u32,
    policy: SessionPolicy,
}

impl VenueSession {
    pub fn new(policy: SessionPolicy) -> Self {
        Self {
            state: SessionState::Disconnected,
            source_session_id: String::new(),
            generation: 0,
            lease_epoch: 0,
            lease_expires_at_ns: 0,
            desired: BTreeSet::new(),
            acknowledged: BTreeSet::new(),
            pending: BTreeMap::new(),
            last_frame_at_ns: 0,
            last_heartbeat_at_ns: 0,
            reconnect_attempts: 0,
            policy,
        }
    }

    pub fn state(&self) -> SessionState {
        self.state
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn source_session_id(&self) -> &str {
        &self.source_session_id
    }

    pub fn desired(&self) -> &BTreeSet<String> {
        &self.desired
    }

    pub fn acknowledged(&self) -> &BTreeSet<String> {
        &self.acknowledged
    }

    pub fn set_desired<I>(&mut self, subscriptions: I)
    where
        I: IntoIterator<Item = String>,
    {
        self.desired = subscriptions.into_iter().collect();
    }

    pub fn connect(
        &mut self,
        source_session_id: String,
        lease_epoch: u64,
        lease_expires_at_ns: i64,
    ) -> Result<u64, SessionError> {
        if source_session_id.trim().is_empty()
            || lease_epoch == 0
            || lease_expires_at_ns <= 0
            || self.state == SessionState::Stopped
        {
            return Err(SessionError::InvalidTransition);
        }
        if self.reconnect_attempts >= self.policy.reconnect_budget {
            self.state = SessionState::Blocked;
            return Err(SessionError::InvalidTransition);
        }
        self.generation = self.generation.saturating_add(1);
        self.source_session_id = source_session_id;
        self.lease_epoch = lease_epoch;
        self.lease_expires_at_ns = lease_expires_at_ns;
        self.acknowledged.clear();
        self.pending.clear();
        self.state = SessionState::Connecting;
        self.reconnect_attempts = self.reconnect_attempts.saturating_add(1);
        Ok(self.generation)
    }

    pub fn transport_ready(&mut self) -> Result<Vec<String>, SessionError> {
        if self.state != SessionState::Connecting {
            return Err(SessionError::InvalidTransition);
        }
        self.pending = self
            .desired
            .iter()
            .cloned()
            .map(|subscription| (subscription, self.generation))
            .collect();
        self.state = SessionState::Subscribing;
        Ok(self.pending.keys().cloned().collect())
    }

    pub fn subscription_ack(
        &mut self,
        subscription: &str,
        generation: u64,
    ) -> Result<(), SessionError> {
        if generation != self.generation {
            return Err(SessionError::StaleGeneration);
        }
        if self.pending.remove(subscription).is_none() {
            return Err(SessionError::UnknownSubscription(subscription.into()));
        }
        self.acknowledged.insert(subscription.into());
        if self.pending.is_empty() && self.acknowledged == self.desired {
            self.state = SessionState::Live;
            self.reconnect_attempts = 0;
        }
        Ok(())
    }

    pub fn subscription_reject(&mut self, subscription: &str) -> SessionError {
        self.pending.remove(subscription);
        self.state = SessionState::Blocked;
        SessionError::SubscriptionRejected(subscription.into())
    }

    pub fn heartbeat(&mut self, generation: u64, now_ns: i64) -> Result<(), SessionError> {
        self.permits_frame(generation, self.lease_epoch, now_ns)?;
        self.last_heartbeat_at_ns = now_ns;
        Ok(())
    }

    pub fn permits_frame(
        &mut self,
        generation: u64,
        lease_epoch: u64,
        now_ns: i64,
    ) -> Result<(), SessionError> {
        if generation != self.generation {
            return Err(SessionError::StaleGeneration);
        }
        if lease_epoch != self.lease_epoch || now_ns >= self.lease_expires_at_ns {
            self.state = SessionState::Blocked;
            return Err(SessionError::LeaseRejected);
        }
        if self.state != SessionState::Live && self.state != SessionState::Draining {
            return Err(SessionError::InvalidTransition);
        }
        self.last_frame_at_ns = now_ns;
        Ok(())
    }

    pub fn tick(&mut self, now_ns: i64) -> Result<(), SessionError> {
        if self.state != SessionState::Live {
            return Ok(());
        }
        let heartbeat_expired = self.last_heartbeat_at_ns > 0
            && now_ns - self.last_heartbeat_at_ns > self.policy.heartbeat_timeout_ns;
        let read_expired = self.last_frame_at_ns > 0
            && now_ns - self.last_frame_at_ns > self.policy.read_timeout_ns;
        if heartbeat_expired || read_expired {
            self.state = SessionState::Degraded;
            return Err(SessionError::HeartbeatExpired);
        }
        Ok(())
    }

    pub fn begin_recovery(&mut self) -> Result<(), SessionError> {
        if self.state != SessionState::Degraded && self.state != SessionState::Blocked {
            return Err(SessionError::InvalidTransition);
        }
        self.state = SessionState::Recovering;
        Ok(())
    }

    pub fn drain(&mut self) -> Result<(), SessionError> {
        if self.state != SessionState::Live {
            return Err(SessionError::InvalidTransition);
        }
        self.state = SessionState::Draining;
        Ok(())
    }

    pub fn stop(&mut self) {
        self.pending.clear();
        self.acknowledged.clear();
        self.state = SessionState::Stopped;
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionError, SessionPolicy, SessionState, VenueSession};

    fn session() -> VenueSession {
        VenueSession::new(SessionPolicy {
            heartbeat_timeout_ns: 100,
            read_timeout_ns: 100,
            reconnect_budget: 3,
        })
    }

    #[test]
    fn full_duplex_lifecycle_tracks_ack_and_rejects_stale_generation() {
        let mut value = session();
        value.set_desired(["btc@trade".into(), "eth@trade".into()]);
        let generation = value.connect("session-1".into(), 7, 10_000).unwrap();
        assert_eq!(value.transport_ready().unwrap().len(), 2);
        value.subscription_ack("btc@trade", generation).unwrap();
        value.subscription_ack("eth@trade", generation).unwrap();
        assert_eq!(value.state(), SessionState::Live);
        value.heartbeat(generation, 10).unwrap();
        assert_eq!(
            value.permits_frame(generation - 1, 7, 11),
            Err(SessionError::StaleGeneration)
        );
        value.permits_frame(generation, 7, 11).unwrap();
        assert_eq!(value.tick(200), Err(SessionError::HeartbeatExpired));
        assert_eq!(value.state(), SessionState::Degraded);
    }

    #[test]
    fn expired_or_wrong_lease_fails_closed() {
        let mut value = session();
        value.set_desired(["btc@trade".into()]);
        let generation = value.connect("session-1".into(), 7, 20).unwrap();
        value.transport_ready().unwrap();
        value.subscription_ack("btc@trade", generation).unwrap();
        assert_eq!(
            value.permits_frame(generation, 7, 20),
            Err(SessionError::LeaseRejected)
        );
        assert_eq!(value.state(), SessionState::Blocked);
    }
}
