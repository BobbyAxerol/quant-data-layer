use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::future::try_join_all;
use qdl_core::transport::{AppendResult, Cursor, DurableRecord};
use qdl_venue_core::authority::{
    Phase92AuthorityControlEvent, Phase92AuthorityFence, Phase92AuthorityRecord,
    Phase92PublicationContext, SinkTarget,
};
use rdkafka::client::ClientContext;
use rdkafka::consumer::{
    BaseConsumer, Consumer, ConsumerContext, RebalanceProtocol, StreamConsumer,
};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::RDKafkaRespErr;
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::phase92_bootstrap::{Phase92BootstrapAssignment, Phase92BootstrapPayload};
use super::{
    transactional_output_headers, KafkaTransportConfig, KafkaTransportError,
    TransactionalKafkaInput, EVENT_ID_HEADER,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Phase92TransactionalTopics {
    pub raw_inputs: Vec<String>,
    pub canary_canonical: String,
    pub primary_canonical: String,
    pub public_v2: String,
    pub legacy_v1: String,
    pub quarantine: String,
    pub target_checkpoints: String,
    pub authority_control: String,
}

impl Phase92TransactionalTopics {
    pub fn validate(&self) -> Result<(), KafkaTransportError> {
        let outputs = [
            self.canary_canonical.as_str(),
            self.primary_canonical.as_str(),
            self.public_v2.as_str(),
            self.legacy_v1.as_str(),
            self.quarantine.as_str(),
            self.target_checkpoints.as_str(),
            self.authority_control.as_str(),
        ];
        if self.raw_inputs.is_empty()
            || self.raw_inputs.iter().any(|topic| topic.trim().is_empty())
            || outputs.iter().any(|topic| topic.trim().is_empty())
        {
            return Err(KafkaTransportError::Configuration(
                "Phase 9.2 transactional topics must not be empty".into(),
            ));
        }
        let mut unique = HashSet::new();
        for topic in self.raw_inputs.iter().map(String::as_str).chain(outputs) {
            if !unique.insert(topic) {
                return Err(KafkaTransportError::Configuration(
                    "Phase 9.2 transactional topics must be isolated and unique".into(),
                ));
            }
        }
        Ok(())
    }

    fn permits_output(&self, target: SinkTarget, stream: &str) -> bool {
        match target {
            SinkTarget::CanaryCanonical => stream == self.canary_canonical,
            SinkTarget::PrimaryCanonical => {
                stream == self.primary_canonical || stream == self.quarantine
            }
            SinkTarget::PublicV2 => stream == self.public_v2,
            SinkTarget::LegacyV1 => stream == self.legacy_v1,
            SinkTarget::ShadowRaw
            | SinkTarget::ShadowCanonical
            | SinkTarget::ShadowQuarantine
            | SinkTarget::PrimaryRaw
            | SinkTarget::PrimaryQuarantine => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Phase92Decision {
    Canonical,
    Quarantine,
    Filtered,
    Duplicate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Phase92Progress {
    pub publication: Phase92PublicationContext,
    pub decision: Phase92Decision,
    pub source_cursor: Cursor,
    pub source_event_id: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Phase92TransactionalOutput {
    pub record: DurableRecord,
    pub publication: Phase92PublicationContext,
    pub raw_provider_envelope: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Phase92TargetCheckpoint {
    pub schema: String,
    pub slice_id: String,
    pub owner_id: String,
    pub authority_revision: u64,
    pub lease_epoch: u64,
    pub partition_plan_epoch: u64,
    pub shard_id: String,
    pub target: SinkTarget,
    pub source_watermark: u64,
    pub source_event_id: String,
    pub decision: Phase92Decision,
    pub output_payload_sha256: String,
    pub candidate_digest: String,
    pub committed_at_ns: i64,
}

impl Phase92TargetCheckpoint {
    pub fn validate(&self) -> Result<(), KafkaTransportError> {
        if self.schema != "qdl.target-watermark-checkpoint.v1"
            || self.slice_id.trim().is_empty()
            || self.owner_id.trim().is_empty()
            || self.authority_revision == 0
            || self.lease_epoch == 0
            || self.partition_plan_epoch == 0
            || self.shard_id.trim().is_empty()
            || self.source_event_id.len() < 2
            || self.output_payload_sha256.len() != 64
            || !self
                .output_payload_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self.candidate_digest.len() != 64
            || !self
                .candidate_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self.committed_at_ns <= 0
            || !matches!(
                self.target,
                SinkTarget::CanaryCanonical
                    | SinkTarget::PrimaryCanonical
                    | SinkTarget::PublicV2
                    | SinkTarget::LegacyV1
            )
        {
            return Err(KafkaTransportError::Fencing(
                "Phase 9.2 target checkpoint is invalid".into(),
            ));
        }
        Ok(())
    }

    pub fn key(&self) -> String {
        format!(
            "{}|{}|{}",
            self.slice_id,
            self.shard_id,
            target_name(self.target)
        )
    }

    pub fn publication(&self) -> Phase92PublicationContext {
        Phase92PublicationContext {
            slice_id: self.slice_id.clone(),
            owner_id: self.owner_id.clone(),
            authority_revision: self.authority_revision,
            shard_id: self.shard_id.clone(),
            lease_epoch: self.lease_epoch,
            partition_plan_epoch: self.partition_plan_epoch,
            source_watermark: self.source_watermark,
            target: self.target,
        }
    }
}

fn target_name(target: SinkTarget) -> &'static str {
    match target {
        SinkTarget::ShadowRaw => "SHADOW_RAW",
        SinkTarget::ShadowCanonical => "SHADOW_CANONICAL",
        SinkTarget::ShadowQuarantine => "SHADOW_QUARANTINE",
        SinkTarget::CanaryCanonical => "CANARY_CANONICAL",
        SinkTarget::PrimaryRaw => "PRIMARY_RAW",
        SinkTarget::PrimaryCanonical => "PRIMARY_CANONICAL",
        SinkTarget::PrimaryQuarantine => "PRIMARY_QUARANTINE",
        SinkTarget::PublicV2 => "PUBLIC_V2",
        SinkTarget::LegacyV1 => "LEGACY_V1",
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactedKafkaRecord {
    pub topic: String,
    pub key: String,
    pub payload: Vec<u8>,
    pub event_id: Option<Vec<u8>>,
    pub partition: i32,
    pub offset: i64,
    pub accepted_at_ns: i64,
}

pub struct KafkaCompactedSnapshotReader {
    config: KafkaTransportConfig,
}

fn retire_reached_snapshot_partitions(
    remaining: &mut BTreeMap<(String, i32), i64>,
    positions: &TopicPartitionList,
) -> Result<(), KafkaTransportError> {
    let completed = remaining
        .iter()
        .filter_map(|((topic, partition), terminal_offset)| {
            let position = positions.find_partition(topic, *partition)?;
            if let Err(error) = position.error() {
                return Some(Err(KafkaTransportError::Kafka(error)));
            }
            match position.offset() {
                Offset::Offset(next_offset) if next_offset > *terminal_offset => {
                    Some(Ok((topic.clone(), *partition)))
                }
                _ => None,
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    for identity in completed {
        remaining.remove(&identity);
    }
    Ok(())
}

impl KafkaCompactedSnapshotReader {
    pub fn new(config: KafkaTransportConfig) -> Result<Self, KafkaTransportError> {
        config.validate()?;
        Ok(Self { config })
    }

    pub fn read(&self, topics: &[&str]) -> Result<Vec<CompactedKafkaRecord>, KafkaTransportError> {
        if topics.is_empty() || topics.iter().any(|topic| topic.trim().is_empty()) {
            return Err(KafkaTransportError::Configuration(
                "compacted snapshot topics must not be empty".into(),
            ));
        }
        let mut client = self.config.client_config()?;
        client
            .set("group.id", &self.config.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("isolation.level", "read_committed")
            .set("enable.partition.eof", "true");
        let consumer: BaseConsumer = client.create()?;
        let timeout = Timeout::After(self.config.request_timeout);
        let mut assignment = TopicPartitionList::new();
        let mut remaining = BTreeMap::new();
        for topic in topics {
            let metadata = consumer.fetch_metadata(Some(topic), timeout)?;
            let metadata_topic = metadata
                .topics()
                .iter()
                .find(|value| value.name() == *topic)
                .ok_or(KafkaTransportError::MissingField(
                    "compacted topic metadata",
                ))?;
            if metadata_topic.partitions().is_empty() {
                return Err(KafkaTransportError::Configuration(format!(
                    "compacted topic has no partitions: {topic}"
                )));
            }
            for partition in metadata_topic.partitions() {
                let partition_id = partition.id();
                let (low, high) = consumer.fetch_watermarks(topic, partition_id, timeout)?;
                assignment.add_partition_offset(topic, partition_id, Offset::Beginning)?;
                if high > low {
                    remaining.insert(((*topic).to_owned(), partition_id), high - 1);
                }
            }
        }
        consumer.assign(&assignment)?;

        let deadline = Instant::now() + self.config.request_timeout;
        let mut latest: HashMap<(String, String), CompactedKafkaRecord> = HashMap::new();
        while !remaining.is_empty() {
            retire_reached_snapshot_partitions(&mut remaining, &consumer.position()?)?;
            if remaining.is_empty() {
                break;
            }
            if Instant::now() >= deadline {
                return Err(KafkaTransportError::SnapshotTimeout(
                    "captured high watermarks were not reached before the deadline".into(),
                ));
            }
            let Some(result) = consumer.poll(Duration::from_millis(100)) else {
                continue;
            };
            let message = match result {
                Ok(message) => message,
                Err(rdkafka::error::KafkaError::PartitionEOF(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            let topic = message.topic().to_owned();
            let partition = message.partition();
            let key_bytes = message
                .key()
                .ok_or(KafkaTransportError::MissingField("compacted record key"))?;
            let key = std::str::from_utf8(key_bytes)
                .map_err(|_| KafkaTransportError::InvalidUtf8("compacted record key"))?
                .to_owned();
            let identity = (topic.clone(), key.clone());
            if let Some(payload) = message.payload() {
                let event_id = message.headers().and_then(|headers| {
                    headers
                        .iter()
                        .find(|header| header.key == EVENT_ID_HEADER)
                        .and_then(|header| header.value.map(ToOwned::to_owned))
                });
                latest.insert(
                    identity,
                    CompactedKafkaRecord {
                        topic: topic.clone(),
                        key,
                        payload: payload.to_vec(),
                        event_id,
                        partition,
                        offset: message.offset(),
                        accepted_at_ns: message.timestamp().to_millis().unwrap_or_default()
                            * 1_000_000,
                    },
                );
            } else {
                latest.remove(&identity);
            }
            if remaining
                .get(&(topic.clone(), partition))
                .is_some_and(|high| message.offset() >= *high)
            {
                remaining.remove(&(topic, partition));
            }
        }
        let mut records: Vec<_> = latest.into_values().collect();
        records.sort_by(|left, right| {
            (&left.topic, &left.key, left.partition, left.offset).cmp(&(
                &right.topic,
                &right.key,
                right.partition,
                right.offset,
            ))
        });
        Ok(records)
    }
}

#[derive(Debug)]
pub struct Phase92CommitResult {
    pub outputs: Vec<AppendResult>,
    pub checkpoints: Vec<AppendResult>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Phase92BootstrapStatus {
    NotRequired,
    Pending,
    ResumeStored,
    Seeded,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase92RebalanceAction {
    PrepareAndAssign,
    Revoke,
    CooperativeEmptyNoop,
}

fn phase92_rebalance_action(
    error: RDKafkaRespErr,
    cooperative_protocol: bool,
    assignment_len: usize,
) -> Result<Phase92RebalanceAction, KafkaTransportError> {
    match error {
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
            if assignment_len == 0 {
                if cooperative_protocol {
                    Ok(Phase92RebalanceAction::CooperativeEmptyNoop)
                } else {
                    Err(KafkaTransportError::Fencing(
                        "Phase 9.2 bootstrap received an empty assignment outside cooperative rebalance"
                            .into(),
                    ))
                }
            } else {
                Ok(Phase92RebalanceAction::PrepareAndAssign)
            }
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => Ok(Phase92RebalanceAction::Revoke),
        _ => Err(KafkaTransportError::Fencing(
            "Phase 9.2 Kafka consumer received an unexpected rebalance event".into(),
        )),
    }
}

struct Phase92BootstrapConsumerContext {
    cursor: Option<Phase92BootstrapPayload>,
    status: Mutex<Phase92BootstrapStatus>,
    failure: Mutex<Option<String>>,
    request_timeout: Duration,
}

impl Phase92BootstrapConsumerContext {
    fn permissive(request_timeout: Duration) -> Self {
        Self {
            cursor: None,
            status: Mutex::new(Phase92BootstrapStatus::NotRequired),
            failure: Mutex::new(None),
            request_timeout,
        }
    }

    fn signed(cursor: Phase92BootstrapPayload, request_timeout: Duration) -> Self {
        Self {
            cursor: Some(cursor),
            status: Mutex::new(Phase92BootstrapStatus::Pending),
            failure: Mutex::new(None),
            request_timeout,
        }
    }

    fn status(&self) -> Phase92BootstrapStatus {
        self.status
            .lock()
            .map(|value| value.clone())
            .unwrap_or(Phase92BootstrapStatus::Failed)
    }

    fn failure(&self) -> Option<String> {
        self.failure
            .lock()
            .map(|value| value.clone())
            .unwrap_or_else(|_| Some("Phase 9.2 bootstrap failure state lock is poisoned".into()))
    }

    fn fail(&self, error: impl Into<String>) {
        let message = error.into();
        if let Ok(mut failure) = self.failure.lock() {
            *failure = Some(message);
        }
        if let Ok(mut status) = self.status.lock() {
            *status = Phase92BootstrapStatus::Failed;
        }
    }

    fn set_status(&self, status: Phase92BootstrapStatus) {
        if let Ok(mut current) = self.status.lock() {
            *current = status;
        } else {
            self.fail("Phase 9.2 bootstrap status lock is poisoned");
        }
    }

    fn prepare_assignment(
        &self,
        consumer: &BaseConsumer<Self>,
        assignment: &mut TopicPartitionList,
    ) -> Result<(), KafkaTransportError> {
        let Some(cursor) = &self.cursor else {
            return Ok(());
        };
        let assigned = assignment
            .elements()
            .iter()
            .map(|item| (item.topic().to_owned(), item.partition()))
            .collect::<Vec<_>>();
        let committed = consumer.committed_offsets(
            assignment.clone(),
            Timeout::After(self.request_timeout.min(Duration::from_secs(2))),
        )?;
        let mut offsets = BTreeMap::new();
        for item in committed.elements() {
            let offset = match item.offset() {
                Offset::Offset(value) if value >= 0 => Some(
                    u64::try_from(value).map_err(|_| KafkaTransportError::InvalidOffset(value))?,
                ),
                Offset::Invalid => None,
                value => {
                    return Err(KafkaTransportError::Fencing(format!(
                        "Phase 9.2 committed offset is not concrete for {}[{}]: {value:?}",
                        item.topic(),
                        item.partition(),
                    )));
                }
            };
            offsets.insert((item.topic().to_owned(), item.partition()), offset);
        }
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                KafkaTransportError::Configuration(format!(
                    "Phase 9.2 bootstrap clock is before Unix epoch: {error}"
                ))
            })?
            .as_nanos()
            .try_into()
            .map_err(|_| {
                KafkaTransportError::Configuration(
                    "Phase 9.2 bootstrap clock does not fit signed cursor range".into(),
                )
            })?;
        match cursor.decide_assignment(&assigned, &offsets, now_ns)? {
            Phase92BootstrapAssignment::ResumeStored => {
                self.set_status(Phase92BootstrapStatus::ResumeStored);
            }
            Phase92BootstrapAssignment::SeedMissing(values) => {
                for ((topic, partition), offset) in values {
                    assignment.set_partition_offset(
                        &topic,
                        partition,
                        Offset::Offset(
                            i64::try_from(offset)
                                .map_err(|_| KafkaTransportError::InvalidOffset(i64::MAX))?,
                        ),
                    )?;
                }
                self.set_status(Phase92BootstrapStatus::Seeded);
            }
        }
        Ok(())
    }

    fn complete_assignment(
        &self,
        consumer: &BaseConsumer<Self>,
        assignment: &TopicPartitionList,
    ) -> Result<(), KafkaTransportError> {
        match consumer.rebalance_protocol() {
            RebalanceProtocol::Cooperative => consumer.incremental_assign(assignment)?,
            RebalanceProtocol::Eager | RebalanceProtocol::None => consumer.assign(assignment)?,
        }
        Ok(())
    }

    fn complete_revocation(
        &self,
        consumer: &BaseConsumer<Self>,
        assignment: &TopicPartitionList,
    ) -> Result<(), KafkaTransportError> {
        match consumer.rebalance_protocol() {
            RebalanceProtocol::Cooperative => consumer.incremental_unassign(assignment)?,
            RebalanceProtocol::Eager | RebalanceProtocol::None => consumer.unassign()?,
        }
        Ok(())
    }
}

impl ClientContext for Phase92BootstrapConsumerContext {}

impl ConsumerContext for Phase92BootstrapConsumerContext {
    fn rebalance(
        &self,
        consumer: &BaseConsumer<Self>,
        error: RDKafkaRespErr,
        assignment: &mut TopicPartitionList,
    ) {
        let result = match phase92_rebalance_action(
            error,
            matches!(
                consumer.rebalance_protocol(),
                RebalanceProtocol::Cooperative
            ),
            assignment.elements().len(),
        ) {
            Ok(Phase92RebalanceAction::CooperativeEmptyNoop) => Ok(()),
            Ok(Phase92RebalanceAction::PrepareAndAssign) => self
                .prepare_assignment(consumer, assignment)
                .and_then(|()| self.complete_assignment(consumer, assignment)),
            Ok(Phase92RebalanceAction::Revoke) => self.complete_revocation(consumer, assignment),
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            self.fail(error.to_string());
        }
    }
}

type Phase92StreamConsumer = StreamConsumer<Phase92BootstrapConsumerContext>;

pub struct Phase92TransactionalKafkaBridge {
    producer: FutureProducer,
    consumer: Phase92StreamConsumer,
    fences: tokio::sync::Mutex<HashMap<String, Phase92AuthorityFence>>,
    topics: Phase92TransactionalTopics,
    request_timeout: Duration,
}

fn validate_commit_shape(
    topics: &Phase92TransactionalTopics,
    inputs: &[TransactionalKafkaInput],
    outputs: &[Phase92TransactionalOutput],
    progress: &[Phase92Progress],
    now_ns: i64,
) -> Result<(), KafkaTransportError> {
    if inputs.is_empty() || now_ns <= 0 {
        return Err(KafkaTransportError::Configuration(
            "Phase 9.2 transaction requires input and time".into(),
        ));
    }
    if progress.is_empty() && !outputs.is_empty() {
        return Err(KafkaTransportError::Fencing(
            "Phase 9.2 transaction cannot publish output without target progress".into(),
        ));
    }
    if inputs
        .iter()
        .any(|input| !topics.raw_inputs.contains(&input.cursor.stream))
    {
        return Err(KafkaTransportError::Fencing(
            "Phase 9.2 transaction input is outside raw topics".into(),
        ));
    }
    let mut progress_identities = HashSet::new();
    for item in progress {
        let identity = (
            item.publication.slice_id.clone(),
            item.publication.shard_id.clone(),
            item.publication.target,
            item.publication.source_watermark,
        );
        if !progress_identities.insert(identity) {
            return Err(KafkaTransportError::Fencing(
                "Phase 9.2 transaction has duplicate target progress".into(),
            ));
        }
        if item.source_event_id.is_empty()
            || !inputs.iter().any(|input| {
                input.cursor == item.source_cursor && input.record.event_id == item.source_event_id
            })
        {
            return Err(KafkaTransportError::Fencing(
                "Phase 9.2 progress has no exact matching raw input".into(),
            ));
        }
    }
    for output in outputs {
        if !topics.permits_output(output.publication.target, &output.record.stream)
            || !progress
                .iter()
                .any(|item| item.publication == output.publication)
        {
            return Err(KafkaTransportError::Fencing(
                "Phase 9.2 output target/topic/progress binding failed".into(),
            ));
        }
    }
    Ok(())
}

impl Phase92TransactionalKafkaBridge {
    pub fn new(
        config: &KafkaTransportConfig,
        topics: Phase92TransactionalTopics,
        transactional_id: &str,
    ) -> Result<Self, KafkaTransportError> {
        Self::new_inner(config, topics, transactional_id, None)
    }

    pub fn new_with_signed_bootstrap(
        config: &KafkaTransportConfig,
        topics: Phase92TransactionalTopics,
        transactional_id: &str,
        bootstrap: Phase92BootstrapPayload,
    ) -> Result<Self, KafkaTransportError> {
        Self::new_inner(config, topics, transactional_id, Some(bootstrap))
    }

    fn new_inner(
        config: &KafkaTransportConfig,
        topics: Phase92TransactionalTopics,
        transactional_id: &str,
        bootstrap: Option<Phase92BootstrapPayload>,
    ) -> Result<Self, KafkaTransportError> {
        topics.validate()?;
        if transactional_id.trim().is_empty() {
            return Err(KafkaTransportError::Configuration(
                "Phase 9.2 transactional.id must not be empty".into(),
            ));
        }
        let strict_bootstrap = bootstrap.is_some();
        let context = bootstrap.map_or_else(
            || Phase92BootstrapConsumerContext::permissive(config.request_timeout),
            |cursor| Phase92BootstrapConsumerContext::signed(cursor, config.request_timeout),
        );
        let mut consumer_config = config.client_config()?;
        if strict_bootstrap {
            config.configure_group_consumer_fail_closed(&mut consumer_config);
        } else {
            config.configure_group_consumer(&mut consumer_config);
        }
        let consumer: Phase92StreamConsumer = consumer_config.create_with_context(context)?;
        let raw_topics: Vec<&str> = topics.raw_inputs.iter().map(String::as_str).collect();
        consumer.subscribe(&raw_topics)?;

        let mut producer_config = config.client_config()?;
        producer_config
            .set("transactional.id", transactional_id)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("max.in.flight.requests.per.connection", "5")
            .set("retries", "2147483647")
            .set("compression.type", "zstd")
            .set(
                "transaction.timeout.ms",
                config.request_timeout.as_millis().to_string(),
            )
            .set(
                "delivery.timeout.ms",
                config.request_timeout.as_millis().to_string(),
            );
        let producer: FutureProducer = producer_config.create()?;
        producer.init_transactions(Timeout::After(config.request_timeout))?;
        Ok(Self {
            producer,
            consumer,
            fences: tokio::sync::Mutex::new(HashMap::new()),
            topics,
            request_timeout: config.request_timeout,
        })
    }

    pub fn bootstrap_status(&self) -> Phase92BootstrapStatus {
        self.consumer.context().status()
    }

    fn bootstrap_failure(&self) -> Result<(), KafkaTransportError> {
        if let Some(message) = self.consumer.context().failure() {
            return Err(KafkaTransportError::Fencing(message));
        }
        Ok(())
    }

    pub async fn apply_authority_event(
        &self,
        event: &Phase92AuthorityControlEvent,
        now_ns: i64,
    ) -> Result<(), KafkaTransportError> {
        let mut fences = self.fences.lock().await;
        fences
            .entry(event.slice_id.clone())
            .or_default()
            .apply_control_event(event, now_ns)
            .map_err(KafkaTransportError::Fencing)
    }

    pub async fn restore_checkpoint(
        &self,
        checkpoint: &Phase92TargetCheckpoint,
    ) -> Result<(), KafkaTransportError> {
        checkpoint.validate()?;
        self.fences
            .lock()
            .await
            .get_mut(&checkpoint.slice_id)
            .ok_or_else(|| {
                KafkaTransportError::Fencing("checkpoint slice authority is not loaded".into())
            })?
            .restore_committed_watermark(&checkpoint.publication())
            .map_err(KafkaTransportError::Fencing)
    }

    pub async fn current_authority(&self, slice_id: &str) -> Option<Phase92AuthorityRecord> {
        self.fences
            .lock()
            .await
            .get(slice_id)
            .and_then(|fence| fence.current().cloned())
    }

    pub async fn next_watermark(
        &self,
        slice_id: &str,
        shard_id: &str,
        target: SinkTarget,
    ) -> Result<u64, KafkaTransportError> {
        self.fences
            .lock()
            .await
            .get(slice_id)
            .ok_or_else(|| {
                KafkaTransportError::Fencing("publication slice authority is not loaded".into())
            })?
            .next_watermark(shard_id, target)
            .map_err(KafkaTransportError::Fencing)
    }

    pub fn unsubscribe(&self) {
        self.consumer.unsubscribe();
    }

    pub async fn next(&self) -> Result<TransactionalKafkaInput, KafkaTransportError> {
        let message = loop {
            self.bootstrap_failure()?;
            match tokio::time::timeout(Duration::from_millis(250), self.consumer.recv()).await {
                Ok(result) => {
                    self.bootstrap_failure()?;
                    break result?;
                }
                Err(_) => continue,
            }
        };
        let payload = message
            .payload()
            .ok_or(KafkaTransportError::MissingField("payload"))?
            .to_vec();
        let key = message
            .key()
            .ok_or(KafkaTransportError::MissingField("partition_key"))?;
        let partition_key = std::str::from_utf8(key)
            .map_err(|_| KafkaTransportError::InvalidUtf8("partition_key"))?
            .to_owned();
        let event_id = message
            .headers()
            .and_then(|headers| {
                headers
                    .iter()
                    .find(|header| header.key == EVENT_ID_HEADER)
                    .and_then(|header| header.value.map(ToOwned::to_owned))
            })
            .ok_or(KafkaTransportError::MissingField("event_id header"))?;
        let offset = u64::try_from(message.offset())
            .map_err(|_| KafkaTransportError::InvalidOffset(message.offset()))?;
        Ok(TransactionalKafkaInput {
            record: DurableRecord {
                stream: message.topic().to_owned(),
                partition_key: partition_key.clone(),
                event_id,
                payload,
                accepted_at_ns: message.timestamp().to_millis().unwrap_or_default() * 1_000_000,
            },
            cursor: Cursor {
                stream: message.topic().to_owned(),
                transport_partition: message.partition(),
                partition_key,
                offset,
            },
        })
    }

    pub async fn commit(
        &self,
        inputs: &[TransactionalKafkaInput],
        outputs: &[Phase92TransactionalOutput],
        progress: &[Phase92Progress],
        now_ns: i64,
    ) -> Result<Phase92CommitResult, KafkaTransportError> {
        validate_commit_shape(&self.topics, inputs, outputs, progress, now_ns)?;

        let mut fences = self.fences.lock().await;
        let mut next_fences = fences.clone();
        for item in progress {
            let next_fence = next_fences
                .get_mut(&item.publication.slice_id)
                .ok_or_else(|| {
                    KafkaTransportError::Fencing("progress slice authority is not loaded".into())
                })?;
            next_fence
                .permits(&item.publication, now_ns)
                .map_err(KafkaTransportError::Fencing)?;
            next_fence
                .commit(&item.publication)
                .map_err(KafkaTransportError::Fencing)?;
        }
        let checkpoints = progress
            .iter()
            .map(|item| {
                let authority = next_fences
                    .get(&item.publication.slice_id)
                    .and_then(Phase92AuthorityFence::current)
                    .ok_or_else(|| {
                        KafkaTransportError::Fencing(
                            "checkpoint slice authority is not loaded".into(),
                        )
                    })?;
                let mut output_digest = Sha256::new();
                for output in outputs
                    .iter()
                    .filter(|output| output.publication == item.publication)
                {
                    output_digest.update(
                        u64::try_from(output.record.payload.len())
                            .map_err(|_| {
                                KafkaTransportError::Configuration(
                                    "output payload length overflow".into(),
                                )
                            })?
                            .to_be_bytes(),
                    );
                    output_digest.update(&output.record.payload);
                }
                let output_payload_sha256 = hex::encode(output_digest.finalize());
                let checkpoint = Phase92TargetCheckpoint {
                    schema: "qdl.target-watermark-checkpoint.v1".into(),
                    slice_id: item.publication.slice_id.clone(),
                    owner_id: item.publication.owner_id.clone(),
                    authority_revision: item.publication.authority_revision,
                    lease_epoch: item.publication.lease_epoch,
                    partition_plan_epoch: item.publication.partition_plan_epoch,
                    shard_id: item.publication.shard_id.clone(),
                    target: item.publication.target,
                    source_watermark: item.publication.source_watermark,
                    source_event_id: hex::encode(&item.source_event_id),
                    decision: item.decision,
                    output_payload_sha256,
                    candidate_digest: authority.candidate_digest.clone(),
                    committed_at_ns: now_ns,
                };
                checkpoint.validate()?;
                let payload = serde_json::to_vec(&checkpoint).map_err(|error| {
                    KafkaTransportError::Configuration(format!(
                        "target checkpoint serialization failed: {error}"
                    ))
                })?;
                let event_id = Sha256::digest(&payload).to_vec();
                Ok::<_, KafkaTransportError>((checkpoint, payload, event_id))
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.producer.begin_transaction()?;
        let transaction = async {
            let output_deliveries = outputs.iter().map(|output| async {
                let headers = transactional_output_headers(
                    output.record.event_id.as_slice(),
                    output.raw_provider_envelope.as_deref(),
                );
                deliver(
                    &self.producer,
                    &output.record.stream,
                    output.record.partition_key.as_bytes(),
                    output.record.payload.as_slice(),
                    headers,
                    self.request_timeout,
                )
                .await
            });
            let accepted_outputs = try_join_all(output_deliveries).await?;

            let checkpoint_deliveries =
                checkpoints
                    .iter()
                    .map(|(checkpoint, payload, event_id)| async {
                        let headers = OwnedHeaders::new().insert(Header {
                            key: EVENT_ID_HEADER,
                            value: Some(event_id.as_slice()),
                        });
                        deliver(
                            &self.producer,
                            &self.topics.target_checkpoints,
                            checkpoint.key().as_bytes(),
                            payload.as_slice(),
                            headers,
                            self.request_timeout,
                        )
                        .await
                    });
            let accepted_checkpoints = try_join_all(checkpoint_deliveries).await?;

            let mut next_offsets: BTreeMap<(String, i32), i64> = BTreeMap::new();
            for input in inputs {
                let next_offset = input
                    .cursor
                    .offset
                    .checked_add(1)
                    .and_then(|value| i64::try_from(value).ok())
                    .ok_or(KafkaTransportError::InvalidOffset(i64::MAX))?;
                next_offsets
                    .entry((
                        input.cursor.stream.clone(),
                        input.cursor.transport_partition,
                    ))
                    .and_modify(|current| *current = (*current).max(next_offset))
                    .or_insert(next_offset);
            }
            let mut offsets = TopicPartitionList::new();
            for ((topic, partition), offset) in next_offsets {
                offsets.add_partition_offset(&topic, partition, Offset::Offset(offset))?;
            }
            let group = self.consumer.group_metadata().ok_or_else(|| {
                KafkaTransportError::Configuration(
                    "Phase 9.2 consumer group metadata is unavailable".into(),
                )
            })?;
            self.producer.send_offsets_to_transaction(
                &offsets,
                &group,
                Timeout::After(self.request_timeout),
            )?;
            self.producer
                .commit_transaction(Timeout::After(self.request_timeout))?;
            Ok::<_, KafkaTransportError>(Phase92CommitResult {
                outputs: accepted_outputs,
                checkpoints: accepted_checkpoints,
            })
        }
        .await;
        match transaction {
            Ok(result) => {
                *fences = next_fences;
                Ok(result)
            }
            Err(error) => {
                self.producer
                    .abort_transaction(Timeout::After(self.request_timeout))
                    .map_err(KafkaTransportError::Kafka)?;
                Err(error)
            }
        }
    }
}

async fn deliver(
    producer: &FutureProducer,
    topic: &str,
    key: &[u8],
    payload: &[u8],
    headers: OwnedHeaders,
    timeout: Duration,
) -> Result<AppendResult, KafkaTransportError> {
    let delivery = producer
        .send(
            FutureRecord::to(topic)
                .key(key)
                .payload(payload)
                .headers(headers),
            Timeout::After(timeout),
        )
        .await
        .map_err(|(error, _)| KafkaTransportError::Delivery(error))?;
    let offset = u64::try_from(delivery.offset)
        .map_err(|_| KafkaTransportError::InvalidOffset(delivery.offset))?;
    Ok(AppendResult {
        cursor: Cursor {
            stream: topic.to_owned(),
            transport_partition: delivery.partition,
            partition_key: std::str::from_utf8(key)
                .map_err(|_| KafkaTransportError::InvalidUtf8("output key"))?
                .to_owned(),
            offset,
        },
        duplicate: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics() -> Phase92TransactionalTopics {
        Phase92TransactionalTopics {
            raw_inputs: vec!["raw".into()],
            canary_canonical: "canary".into(),
            primary_canonical: "canonical".into(),
            public_v2: "public".into(),
            legacy_v1: "legacy".into(),
            quarantine: "quarantine".into(),
            target_checkpoints: "checkpoints".into(),
            authority_control: "authority".into(),
        }
    }

    #[test]
    fn cooperative_empty_assignment_is_a_noop_but_other_empty_assignments_fence() {
        assert_eq!(
            phase92_rebalance_action(
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
                true,
                0,
            )
            .unwrap(),
            Phase92RebalanceAction::CooperativeEmptyNoop
        );
        assert!(phase92_rebalance_action(
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
            false,
            0,
        )
        .is_err());
        assert_eq!(
            phase92_rebalance_action(
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
                true,
                1,
            )
            .unwrap(),
            Phase92RebalanceAction::PrepareAndAssign
        );
        assert_eq!(
            phase92_rebalance_action(
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS,
                true,
                0,
            )
            .unwrap(),
            Phase92RebalanceAction::Revoke
        );
        assert!(
            phase92_rebalance_action(RDKafkaRespErr::RD_KAFKA_RESP_ERR__TIMED_OUT, true, 0,)
                .is_err()
        );
    }

    #[test]
    fn snapshot_completion_uses_next_position_across_compacted_offset_holes() {
        let mut remaining = BTreeMap::from([
            (("authority".into(), 0), 5),
            (("checkpoints".into(), 1), 12),
        ]);
        let mut positions = TopicPartitionList::new();
        positions
            .add_partition_offset("authority", 0, Offset::Offset(6))
            .unwrap();
        positions
            .add_partition_offset("checkpoints", 1, Offset::Offset(12))
            .unwrap();

        retire_reached_snapshot_partitions(&mut remaining, &positions).unwrap();
        assert_eq!(remaining, BTreeMap::from([(("checkpoints".into(), 1), 12)]));

        positions
            .set_partition_offset("checkpoints", 1, Offset::Offset(13))
            .unwrap();
        retire_reached_snapshot_partitions(&mut remaining, &positions).unwrap();
        assert!(remaining.is_empty());
    }

    #[test]
    fn unresolved_snapshot_positions_never_infer_completion() {
        let mut remaining = BTreeMap::from([(("authority".into(), 0), 5)]);
        let mut positions = TopicPartitionList::new();
        positions
            .add_partition_offset("authority", 0, Offset::Beginning)
            .unwrap();
        retire_reached_snapshot_partitions(&mut remaining, &positions).unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn production_topics_are_unique_and_target_bound() {
        let values = topics();
        values.validate().unwrap();
        assert!(values.permits_output(SinkTarget::PrimaryCanonical, "canonical"));
        assert!(values.permits_output(SinkTarget::PrimaryCanonical, "quarantine"));
        assert!(values.permits_output(SinkTarget::PublicV2, "public"));
        assert!(values.permits_output(SinkTarget::LegacyV1, "legacy"));
        assert!(values.permits_output(SinkTarget::CanaryCanonical, "canary"));
        let mut duplicate = values;
        duplicate.public_v2 = "canonical".into();
        assert!(duplicate.validate().is_err());
    }

    fn input(key: &str, offset: u64, event_id: u8) -> TransactionalKafkaInput {
        TransactionalKafkaInput {
            record: DurableRecord {
                stream: "raw".into(),
                partition_key: key.into(),
                event_id: vec![event_id],
                payload: vec![2],
                accepted_at_ns: 1,
            },
            cursor: Cursor {
                stream: "raw".into(),
                transport_partition: 0,
                partition_key: key.into(),
                offset,
            },
        }
    }

    fn publication() -> Phase92PublicationContext {
        Phase92PublicationContext {
            slice_id: "production/binance/usdm/perpetual/trade/plan-1/btcusdt".into(),
            owner_id: "rust-canary".into(),
            authority_revision: 3,
            shard_id: "core-1".into(),
            lease_epoch: 2,
            partition_plan_epoch: 1,
            source_watermark: 8,
            target: SinkTarget::CanaryCanonical,
        }
    }

    #[test]
    fn offset_only_transaction_shape_requires_input_and_forbids_outputs() {
        let values = topics();
        assert!(
            validate_commit_shape(&values, &[input("outside-scope", 7, 1)], &[], &[], 1,).is_ok()
        );
        assert!(validate_commit_shape(&values, &[], &[], &[], 1).is_err());

        let output = Phase92TransactionalOutput {
            record: DurableRecord {
                stream: "canary".into(),
                partition_key: "bounded".into(),
                event_id: vec![3],
                payload: vec![4],
                accepted_at_ns: 1,
            },
            publication: publication(),
            raw_provider_envelope: None,
        };
        assert!(
            validate_commit_shape(&values, &[input("outside-scope", 7, 1)], &[output], &[], 1,)
                .is_err()
        );
    }

    #[test]
    fn mixed_scope_shape_allows_progress_for_only_the_approved_input() {
        let values = topics();
        let approved = input("approved", 8, 2);
        let progress = Phase92Progress {
            publication: publication(),
            decision: Phase92Decision::Filtered,
            source_cursor: approved.cursor.clone(),
            source_event_id: approved.record.event_id.clone(),
        };
        assert!(validate_commit_shape(
            &values,
            &[input("outside-scope", 7, 1), approved],
            &[],
            &[progress],
            1,
        )
        .is_ok());
    }

    #[test]
    fn checkpoint_roundtrip_preserves_exact_identity() {
        let value = Phase92TargetCheckpoint {
            schema: "qdl.target-watermark-checkpoint.v1".into(),
            slice_id: "production/binance/usdm/perpetual/trade/plan-1/btcusdt".into(),
            owner_id: "rust-primary".into(),
            authority_revision: 4,
            lease_epoch: 2,
            partition_plan_epoch: 1,
            shard_id: "core-1".into(),
            target: SinkTarget::PublicV2,
            source_watermark: 101,
            source_event_id: "00".repeat(16),
            decision: Phase92Decision::Canonical,
            output_payload_sha256: "1".repeat(64),
            candidate_digest: "2".repeat(64),
            committed_at_ns: 1,
        };
        value.validate().unwrap();
        let decoded: Phase92TargetCheckpoint =
            serde_json::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(decoded, value);
        assert!(decoded.key().ends_with("|PUBLIC_V2"));
    }
}
