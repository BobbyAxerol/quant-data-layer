use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use ring::hmac;
use serde::{Deserialize, Serialize};

use crate::KafkaTransportError;

pub const SIGNED_BOOTSTRAP_SCHEMA: &str = "qdl.phase92.signed-bootstrap-cursor.v1";
pub const BOOTSTRAP_PAYLOAD_SCHEMA: &str = "qdl.phase92.bootstrap-cursor-payload.v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Phase92BootstrapPartition {
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Phase92BootstrapPayload {
    pub schema: String,
    pub cursor_id: String,
    pub generation: u64,
    pub issued_at_ns: i64,
    pub expires_at_ns: i64,
    pub consumer_group_id: String,
    pub raw_topics: Vec<String>,
    pub promotion_scope_digest: String,
    pub candidate_digest: String,
    pub partition_plan_epoch: u64,
    pub partitions: Vec<Phase92BootstrapPartition>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Phase92SignedBootstrapCursor {
    pub schema: String,
    pub key_id: String,
    pub payload_hex: String,
    pub signature_hex: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Phase92BootstrapScope {
    pub consumer_group_id: String,
    pub raw_topics: Vec<String>,
    pub promotion_scope_digest: String,
    // The signed payload always binds a candidate. Static runtime configuration
    // may defer it until durable authority reconstruction breaks the cycle.
    pub candidate_digest: Option<String>,
    pub partition_plan_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Phase92BootstrapAssignment {
    ResumeStored,
    SeedMissing(BTreeMap<(String, i32), u64>),
}

impl Phase92SignedBootstrapCursor {
    pub fn load_and_verify(
        path: impl AsRef<Path>,
        keyring_json: &str,
        scope: &Phase92BootstrapScope,
    ) -> Result<Phase92BootstrapPayload, KafkaTransportError> {
        let raw = fs::read(path.as_ref()).map_err(|error| {
            KafkaTransportError::Configuration(format!(
                "Phase 9.2 signed bootstrap cursor is unavailable: {error}"
            ))
        })?;
        let cursor: Self = serde_json::from_slice(&raw).map_err(|error| {
            KafkaTransportError::Configuration(format!(
                "Phase 9.2 signed bootstrap cursor is invalid JSON: {error}"
            ))
        })?;
        cursor.verify(keyring_json, scope)
    }

    pub fn verify(
        &self,
        keyring_json: &str,
        scope: &Phase92BootstrapScope,
    ) -> Result<Phase92BootstrapPayload, KafkaTransportError> {
        if self.schema != SIGNED_BOOTSTRAP_SCHEMA || self.key_id.trim().is_empty() {
            return Err(KafkaTransportError::Fencing(
                "Phase 9.2 signed bootstrap cursor envelope is invalid".into(),
            ));
        }
        let keyring: BTreeMap<String, String> =
            serde_json::from_str(keyring_json).map_err(|error| {
                KafkaTransportError::Configuration(format!(
                    "Phase 9.2 bootstrap keyring JSON is invalid: {error}"
                ))
            })?;
        let encoded_key = keyring.get(&self.key_id).ok_or_else(|| {
            KafkaTransportError::Fencing("Phase 9.2 bootstrap cursor key ID is unknown".into())
        })?;
        if encoded_key.is_empty()
            || !encoded_key.bytes().all(|byte| {
                byte.is_ascii_digit() || (byte.is_ascii_lowercase() && byte.is_ascii_hexdigit())
            })
        {
            return Err(KafkaTransportError::Configuration(
                "Phase 9.2 bootstrap signing keys must be lowercase hexadecimal".into(),
            ));
        }
        let key = hex::decode(encoded_key).map_err(|_| {
            KafkaTransportError::Configuration(
                "Phase 9.2 bootstrap signing keys must be lowercase hexadecimal".into(),
            )
        })?;
        if key.len() < 32 {
            return Err(KafkaTransportError::Configuration(
                "Phase 9.2 bootstrap signing key is shorter than 256 bits".into(),
            ));
        }
        let payload_bytes = hex::decode(&self.payload_hex).map_err(|_| {
            KafkaTransportError::Fencing("Phase 9.2 bootstrap cursor payload is not hex".into())
        })?;
        let signature = hex::decode(&self.signature_hex).map_err(|_| {
            KafkaTransportError::Fencing("Phase 9.2 bootstrap cursor signature is not hex".into())
        })?;
        if signature.len() != 32 {
            return Err(KafkaTransportError::Fencing(
                "Phase 9.2 bootstrap cursor signature length is invalid".into(),
            ));
        }
        let verification_key = hmac::Key::new(hmac::HMAC_SHA256, &key);
        hmac::verify(&verification_key, &payload_bytes, &signature).map_err(|_| {
            KafkaTransportError::Fencing(
                "Phase 9.2 bootstrap cursor signature verification failed".into(),
            )
        })?;
        let payload: Phase92BootstrapPayload =
            serde_json::from_slice(&payload_bytes).map_err(|error| {
                KafkaTransportError::Fencing(format!(
                    "Phase 9.2 bootstrap cursor payload is invalid: {error}"
                ))
            })?;
        validate_payload(&payload, scope)?;
        Ok(payload)
    }
}

impl Phase92BootstrapPayload {
    pub fn decide_assignment(
        &self,
        assigned: &[(String, i32)],
        committed: &BTreeMap<(String, i32), Option<u64>>,
        now_ns: i64,
    ) -> Result<Phase92BootstrapAssignment, KafkaTransportError> {
        if assigned.is_empty() {
            return Err(KafkaTransportError::Fencing(
                "Phase 9.2 bootstrap received an empty assignment".into(),
            ));
        }
        let cursor_offsets = self
            .partitions
            .iter()
            .map(|item| ((item.topic.clone(), item.partition), item.offset))
            .collect::<BTreeMap<_, _>>();
        let mut missing = BTreeMap::new();
        let mut seen = BTreeSet::new();
        for (topic, partition) in assigned {
            let identity = (topic.clone(), *partition);
            if !seen.insert(identity.clone()) {
                return Err(KafkaTransportError::Fencing(
                    "Phase 9.2 bootstrap assignment contains a duplicate partition".into(),
                ));
            }
            let cursor_offset = cursor_offsets.get(&identity).ok_or_else(|| {
                KafkaTransportError::Fencing(
                    "Phase 9.2 assigned raw partition is outside signed bootstrap cursor".into(),
                )
            })?;
            let stored = committed.get(&identity).ok_or_else(|| {
                KafkaTransportError::Fencing(
                    "Phase 9.2 committed offset response omitted an assigned partition".into(),
                )
            })?;
            match stored {
                Some(value) if value < cursor_offset => {
                    return Err(KafkaTransportError::Fencing(
                        "Phase 9.2 stored offset predates signed bootstrap cursor".into(),
                    ));
                }
                Some(_) => {}
                None => {
                    missing.insert(identity, *cursor_offset);
                }
            }
        }
        if missing.is_empty() {
            return Ok(Phase92BootstrapAssignment::ResumeStored);
        }
        if now_ns < self.issued_at_ns || now_ns >= self.expires_at_ns {
            return Err(KafkaTransportError::Fencing(
                "Phase 9.2 unconsumed bootstrap cursor is outside its approval window".into(),
            ));
        }
        Ok(Phase92BootstrapAssignment::SeedMissing(missing))
    }
}

fn validate_payload(
    payload: &Phase92BootstrapPayload,
    scope: &Phase92BootstrapScope,
) -> Result<(), KafkaTransportError> {
    if payload.schema != BOOTSTRAP_PAYLOAD_SCHEMA
        || !valid_uuid(&payload.cursor_id)
        || payload.generation == 0
        || payload.issued_at_ns <= 0
        || payload.expires_at_ns <= payload.issued_at_ns
        || payload.consumer_group_id != scope.consumer_group_id
        || payload.partition_plan_epoch != scope.partition_plan_epoch
        || !valid_digest(&payload.promotion_scope_digest)
        || !valid_digest(&payload.candidate_digest)
        || payload.promotion_scope_digest != scope.promotion_scope_digest
        || scope
            .candidate_digest
            .as_ref()
            .is_some_and(|value| payload.candidate_digest.as_str() != value.as_str())
    {
        return Err(KafkaTransportError::Fencing(
            "Phase 9.2 bootstrap cursor scope/identity is invalid".into(),
        ));
    }
    if normalized_topics(&payload.raw_topics)? != normalized_topics(&scope.raw_topics)? {
        return Err(KafkaTransportError::Fencing(
            "Phase 9.2 bootstrap cursor raw topic scope differs from runtime".into(),
        ));
    }
    if payload.partitions.is_empty() {
        return Err(KafkaTransportError::Fencing(
            "Phase 9.2 bootstrap cursor has no raw partitions".into(),
        ));
    }
    let topics = normalized_topics(&payload.raw_topics)?;
    let topic_set = topics.into_iter().collect::<BTreeSet<_>>();
    let mut partitions = BTreeSet::new();
    for item in &payload.partitions {
        if item.topic.trim().is_empty()
            || !topic_set.contains(&item.topic)
            || item.partition < 0
            || item.offset > i64::MAX as u64
            || !partitions.insert((item.topic.clone(), item.partition))
        {
            return Err(KafkaTransportError::Fencing(
                "Phase 9.2 bootstrap cursor partition map is invalid".into(),
            ));
        }
    }
    Ok(())
}

fn normalized_topics(values: &[String]) -> Result<Vec<String>, KafkaTransportError> {
    if values.is_empty() || values.iter().any(|value| value.trim().is_empty()) {
        return Err(KafkaTransportError::Fencing(
            "Phase 9.2 bootstrap cursor raw topic set is empty".into(),
        ));
    }
    let result = values.iter().cloned().collect::<BTreeSet<_>>();
    if result.len() != values.len() {
        return Err(KafkaTransportError::Fencing(
            "Phase 9.2 bootstrap cursor raw topic set contains duplicates".into(),
        ));
    }
    Ok(result.into_iter().collect())
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| {
            byte.is_ascii_digit() || (byte.is_ascii_lowercase() && byte.is_ascii_hexdigit())
        })
}

fn valid_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 8 | 13 | 18 | 23)
                .then_some(byte == b'-')
                .unwrap_or_else(|| byte.is_ascii_hexdigit())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_ID: &str = "phase92-test-k1";
    const KEY_HEX: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn scope() -> Phase92BootstrapScope {
        Phase92BootstrapScope {
            consumer_group_id: "qdl-production-core-canary-001-phase92-raw".into(),
            raw_topics: vec!["md.raw.stable.v1".into()],
            promotion_scope_digest: "a".repeat(64),
            candidate_digest: Some("b".repeat(64)),
            partition_plan_epoch: 2,
        }
    }

    fn payload() -> Phase92BootstrapPayload {
        Phase92BootstrapPayload {
            schema: BOOTSTRAP_PAYLOAD_SCHEMA.into(),
            cursor_id: "11111111-1111-4111-8111-111111111111".into(),
            generation: 7,
            issued_at_ns: 100,
            expires_at_ns: 1_000,
            consumer_group_id: scope().consumer_group_id,
            raw_topics: vec!["md.raw.stable.v1".into()],
            promotion_scope_digest: "a".repeat(64),
            candidate_digest: "b".repeat(64),
            partition_plan_epoch: 2,
            partitions: vec![
                Phase92BootstrapPartition {
                    topic: "md.raw.stable.v1".into(),
                    partition: 0,
                    offset: 500,
                },
                Phase92BootstrapPartition {
                    topic: "md.raw.stable.v1".into(),
                    partition: 1,
                    offset: 600,
                },
            ],
        }
    }

    fn signed(payload: &Phase92BootstrapPayload) -> Phase92SignedBootstrapCursor {
        let bytes = serde_json::to_vec(payload).unwrap();
        let key = hmac::Key::new(hmac::HMAC_SHA256, &hex::decode(KEY_HEX).unwrap());
        Phase92SignedBootstrapCursor {
            schema: SIGNED_BOOTSTRAP_SCHEMA.into(),
            key_id: KEY_ID.into(),
            payload_hex: hex::encode(&bytes),
            signature_hex: hex::encode(hmac::sign(&key, &bytes).as_ref()),
        }
    }

    fn keyring() -> String {
        serde_json::json!({KEY_ID: KEY_HEX}).to_string()
    }

    #[test]
    fn verifies_signed_cursor_and_seeds_only_uncommitted_partitions() {
        let payload = payload();
        let signed = signed(&payload);
        let verified = signed.verify(&keyring(), &scope()).unwrap();
        assert_eq!(verified, payload);
        let assigned = vec![
            ("md.raw.stable.v1".into(), 0),
            ("md.raw.stable.v1".into(), 1),
        ];
        let committed = BTreeMap::from([
            (("md.raw.stable.v1".into(), 0), Some(500)),
            (("md.raw.stable.v1".into(), 1), None),
        ]);
        assert_eq!(
            verified
                .decide_assignment(&assigned, &committed, 200)
                .unwrap(),
            Phase92BootstrapAssignment::SeedMissing(BTreeMap::from([(
                ("md.raw.stable.v1".into(), 1),
                600,
            )]))
        );
    }

    #[test]
    fn resumes_only_offsets_not_older_than_the_signed_tail() {
        let payload = payload();
        let signed = signed(&payload);
        let verified = signed.verify(&keyring(), &scope()).unwrap();
        let assigned = vec![("md.raw.stable.v1".into(), 0)];
        let ahead = BTreeMap::from([(("md.raw.stable.v1".into(), 0), Some(701))]);
        assert_eq!(
            verified
                .decide_assignment(&assigned, &ahead, 2_000)
                .unwrap(),
            Phase92BootstrapAssignment::ResumeStored
        );
        let stale = BTreeMap::from([(("md.raw.stable.v1".into(), 0), Some(499))]);
        assert!(verified.decide_assignment(&assigned, &stale, 200).is_err());
    }

    #[test]
    fn rejects_wrong_signature_scope_expiry_and_unknown_partition() {
        let payload = payload();
        let mut tampered = signed(&payload);
        tampered.signature_hex.replace_range(..2, "00");
        assert!(tampered.verify(&keyring(), &scope()).is_err());

        let signed = signed(&payload);
        let mut wrong_scope = scope();
        wrong_scope.consumer_group_id.push_str("-other");
        assert!(signed.verify(&keyring(), &wrong_scope).is_err());

        let verified = signed.verify(&keyring(), &scope()).unwrap();
        let missing = BTreeMap::from([(("md.raw.stable.v1".into(), 0), None)]);
        assert!(verified
            .decide_assignment(&[("md.raw.stable.v1".into(), 0)], &missing, 1_000)
            .is_err());
        let unknown = BTreeMap::from([(("md.raw.stable.v1".into(), 9), None)]);
        assert!(verified
            .decide_assignment(&[("md.raw.stable.v1".into(), 9)], &unknown, 200)
            .is_err());
    }
}
