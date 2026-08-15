use serde::Deserialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookState {
    Syncing,
    Live,
    Gapped,
    Degraded,
}

impl BookState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Syncing => "SYNCING",
            Self::Live => "LIVE",
            Self::Gapped => "GAPPED",
            Self::Degraded => "DEGRADED",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProtocolFrame {
    pub kind: String,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub seq_id: i64,
    #[serde(default)]
    pub prev_seq_id: i64,
    pub expected_state: String,
    pub expected_accepted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameResult {
    pub state: BookState,
    pub accepted: bool,
    pub response: Option<String>,
}

#[derive(Debug)]
pub struct OkxBookSimulator {
    generation: u64,
    last_sequence: Option<i64>,
    state: BookState,
}

impl Default for OkxBookSimulator {
    fn default() -> Self {
        Self {
            generation: 0,
            last_sequence: None,
            state: BookState::Syncing,
        }
    }
}

impl OkxBookSimulator {
    pub fn apply(&mut self, frame: &ProtocolFrame) -> FrameResult {
        let mut response = None;
        let accepted = match frame.kind.as_str() {
            "connect" => {
                self.generation = frame.generation;
                self.last_sequence = None;
                self.state = BookState::Syncing;
                true
            }
            "keepalive_ping" => {
                response = Some("pong".into());
                true
            }
            "keepalive_pong" | "rest_envelope" | "subscribe_ack" => true,
            "maintenance" => {
                self.last_sequence = None;
                self.state = BookState::Degraded;
                true
            }
            "book" if frame.generation < self.generation => false,
            "book" if frame.action == "snapshot" => {
                self.last_sequence = Some(frame.seq_id);
                self.state = BookState::Live;
                true
            }
            "book" if frame.action == "update" => {
                if self.state != BookState::Live || self.last_sequence != Some(frame.prev_seq_id) {
                    self.last_sequence = None;
                    self.state = BookState::Gapped;
                    false
                } else {
                    self.last_sequence = Some(frame.seq_id);
                    true
                }
            }
            _ => false,
        };
        FrameResult {
            state: self.state,
            accepted,
            response,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OkxBookSimulator, ProtocolFrame};

    #[test]
    fn protocol_corpus_matches_expected_state_machine() {
        let path = format!(
            "{}/../../tests/fixtures/phase2/okx_protocol_frames.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let frames: Vec<ProtocolFrame> =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let mut simulator = OkxBookSimulator::default();
        for frame in frames {
            let result = simulator.apply(&frame);
            assert_eq!(
                result.state.as_str(),
                frame.expected_state,
                "{}",
                frame.kind
            );
            assert_eq!(result.accepted, frame.expected_accepted, "{}", frame.kind);
        }
    }
}
