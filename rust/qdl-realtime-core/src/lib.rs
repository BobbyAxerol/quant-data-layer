#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};

use prost::Message;
use qdl_contracts::qdl::marketdata::v2::{event_envelope, BarLifecycle, EventEnvelope};
use qdl_contracts::qdl::provider::v1::{
    QuarantineReason, QuarantineRecord, RawProviderEnvelope, TransportProtocol,
};
use qdl_core::canonical::{
    canonicalize_l2_publication, canonicalize_mark_index, canonicalize_trade, MarkIndexInput,
    TradeContext, TradeFixture,
};
use qdl_core::l2_adapter::{BookPublication, BookTransition, L2BookAdapter};
use qdl_core::l2_book::{BookIdentity, BookOutcome};
use qdl_core::okx::expand_data_frame;
use qdl_core::transport::DurableRecord;
use qdl_provider_envelope::validate as validate_raw;
use qdl_venue_core::ordering::{OrderingStage, OrderingTracker, SequenceDecision, SequencePolicy};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub mod provider_admission;
pub mod provider_admission_server;

const TRANSPORT_SEQUENCE_STRIDE: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum L2ProviderProtocol {
    BinanceDiffDepth,
    OkxPublicBooks,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarkIndexComponent {
    Both,
    Mark,
    Index,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MarkIndexBinding {
    pub component: MarkIndexComponent,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct L2Binding {
    pub provider_protocol: L2ProviderProtocol,
    pub depth_per_side: usize,
    // Provider bootstrap/renewal remains deliberately slower than the
    // canonical verified-view materialization cadence.
    pub snapshot_refresh_seconds: u64,
    #[serde(default)]
    pub materialized_snapshot_interval_ms: Option<u64>,
}

impl L2Binding {
    fn materialized_snapshot_interval_ms(&self) -> u64 {
        self.materialized_snapshot_interval_ms
            .unwrap_or_else(|| self.snapshot_refresh_seconds.saturating_mul(1_000))
    }

    fn raw_receipt_is_expired(&self, received_at_ns: i64, processing_at_ns: i64) -> bool {
        let max_age_ns = self.snapshot_refresh_seconds.saturating_mul(1_000_000_000);
        let max_age_ns = i64::try_from(max_age_ns).unwrap_or(i64::MAX);
        processing_at_ns.saturating_sub(received_at_ns) > max_age_ns
    }

    fn validate(&self, binding: &CoreBinding) -> Result<(), CoreError> {
        let materialized_snapshot_interval_ms = self.materialized_snapshot_interval_ms();
        if !(1..=10_000).contains(&self.depth_per_side)
            || !(5..=300).contains(&self.snapshot_refresh_seconds)
            || !(100..=self.snapshot_refresh_seconds.saturating_mul(1_000))
                .contains(&materialized_snapshot_interval_ms)
            || binding.sequence_policy != SequencePolicy::Contiguous
        {
            return Err(CoreError::Configuration(
                "L2 binding depth/sequence policy is invalid".into(),
            ));
        }
        match self.provider_protocol {
            L2ProviderProtocol::BinanceDiffDepth
                if binding.venue == "BINANCE"
                    && matches!(binding.market.as_str(), "USDM" | "SPOT")
                    && matches!(
                        binding.provider_kind.as_str(),
                        "binance_usdm_book" | "binance_spot_book"
                    )
                    && binding.native_channel
                        == format!(
                            "{}@depth@100ms",
                            binding.native_symbol.to_ascii_lowercase()
                        ) =>
            {
                Ok(())
            }
            L2ProviderProtocol::OkxPublicBooks
                if binding.venue == "OKX"
                    && matches!(binding.market.as_str(), "SWAP" | "FUTURES" | "SPOT")
                    && binding.provider_kind == "okx_book"
                    && binding.native_channel == "books" =>
            {
                Ok(())
            }
            _ => Err(CoreError::Configuration(
                "L2 provider protocol differs from approved binding identity".into(),
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreBinding {
    pub provider: String,
    pub venue: String,
    pub market: String,
    pub product_type: String,
    pub native_symbol: String,
    pub native_channel: String,
    #[serde(default)]
    pub physical_native_symbol: Option<String>,
    #[serde(default)]
    pub physical_native_channel: Option<String>,
    pub provider_kind: String,
    pub instrument_uid: String,
    pub instrument_id: String,
    pub instrument_revision: u64,
    pub instrument_catalog_revision: u64,
    pub source_id: String,
    pub source_role: String,
    pub normalizer_version: String,
    #[serde(default)]
    pub require_final_bar: bool,
    pub sequence_policy: SequencePolicy,
    #[serde(default)]
    pub l2: Option<L2Binding>,
    #[serde(default)]
    pub mark_index: Option<MarkIndexBinding>,
}

impl CoreBinding {
    fn physical_native_symbol(&self) -> &str {
        self.physical_native_symbol
            .as_deref()
            .unwrap_or(&self.native_symbol)
    }

    fn physical_native_channel(&self) -> &str {
        self.physical_native_channel
            .as_deref()
            .unwrap_or(&self.native_channel)
    }

    fn key(&self) -> String {
        binding_key(
            &self.provider,
            &self.venue,
            &self.market,
            &self.product_type,
            self.physical_native_symbol(),
            self.physical_native_channel(),
        )
    }

    fn mark_index_target_key(&self) -> String {
        format!("{}/{}", self.instrument_uid, self.source_id)
    }

    fn validate(&self) -> Result<(), CoreError> {
        for (name, value) in [
            ("provider", self.provider.as_str()),
            ("venue", self.venue.as_str()),
            ("market", self.market.as_str()),
            ("product_type", self.product_type.as_str()),
            ("native_symbol", self.native_symbol.as_str()),
            ("native_channel", self.native_channel.as_str()),
            ("provider_kind", self.provider_kind.as_str()),
            ("instrument_uid", self.instrument_uid.as_str()),
            ("instrument_id", self.instrument_id.as_str()),
            ("source_id", self.source_id.as_str()),
            ("source_role", self.source_role.as_str()),
            ("normalizer_version", self.normalizer_version.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(CoreError::Configuration(format!(
                    "binding {name} must not be empty"
                )));
            }
        }
        if self.instrument_revision == 0 || self.instrument_catalog_revision == 0 {
            return Err(CoreError::Configuration(
                "binding instrument revisions must be positive".into(),
            ));
        }
        if self.physical_native_symbol.is_some() != self.physical_native_channel.is_some() {
            return Err(CoreError::Configuration(
                "physical mark/index binding identity must be complete".into(),
            ));
        }
        if !matches!(
            self.source_role.as_str(),
            "PRIMARY" | "SECONDARY" | "REFERENCE" | "BACKFILL"
        ) {
            return Err(CoreError::Configuration(
                "binding source role is invalid".into(),
            ));
        }
        if let Some(mark_index) = &self.mark_index {
            if self.l2.is_some()
                || self.require_final_bar
                || self.sequence_policy != SequencePolicy::None
            {
                return Err(CoreError::Configuration(
                    "mark/index binding cannot carry L2, BAR finality or sequence continuity"
                        .into(),
                ));
            }
            let physical_symbol = self.physical_native_symbol();
            let physical_channel = self.physical_native_channel();
            match (
                self.venue.as_str(),
                self.market.as_str(),
                self.provider_kind.as_str(),
                mark_index.component,
            ) {
                ("BINANCE", "USDM", "binance_usdm_mark_index", MarkIndexComponent::Both)
                    if physical_symbol == self.native_symbol
                        && physical_channel
                            == format!(
                                "{}@markPrice@1s",
                                self.native_symbol.to_ascii_lowercase()
                            ) => {}
                ("OKX", "SWAP", "okx_mark_price", MarkIndexComponent::Mark)
                    if physical_symbol == self.native_symbol
                        && physical_channel == "mark-price" => {}
                ("OKX", "SWAP", "okx_index_price", MarkIndexComponent::Index)
                    if physical_symbol != self.native_symbol
                        && physical_channel == "index-tickers" => {}
                _ => {
                    return Err(CoreError::Configuration(
                        "mark/index provider protocol differs from approved binding identity"
                            .into(),
                    ))
                }
            }
        } else if self.physical_native_symbol.is_some() {
            return Err(CoreError::Configuration(
                "physical identity is reserved for a mark/index component binding".into(),
            ));
        } else if let Some(l2) = &self.l2 {
            l2.validate(self)?;
        } else if self.provider_kind.ends_with("_book") {
            return Err(CoreError::Configuration(
                "book provider binding requires an explicit L2 contract".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealtimeCoreConfig {
    pub canonical_stream: String,
    pub quarantine_stream: String,
    pub allow_test_provenance: bool,
    pub dedup_capacity: usize,
    pub bindings: Vec<CoreBinding>,
}

impl RealtimeCoreConfig {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.canonical_stream.trim().is_empty()
            || self.quarantine_stream.trim().is_empty()
            || self.canonical_stream == self.quarantine_stream
            || self.dedup_capacity == 0
            || self.bindings.is_empty()
        {
            return Err(CoreError::Configuration(
                "realtime core stream/bounds/bindings are invalid".into(),
            ));
        }
        let mut keys = HashSet::new();
        let mut ordinary_source_ids = HashSet::new();
        let mut mark_index_source_ids = HashSet::new();
        let mut mark_index_components: BTreeMap<String, HashSet<MarkIndexComponent>> =
            BTreeMap::new();
        for binding in &self.bindings {
            binding.validate()?;
            if !keys.insert(binding.key()) {
                return Err(CoreError::Configuration(
                    "duplicate realtime core binding".into(),
                ));
            }
            if let Some(mark_index) = &binding.mark_index {
                if ordinary_source_ids.contains(&binding.source_id)
                    || !mark_index_components
                        .entry(binding.mark_index_target_key())
                        .or_default()
                        .insert(mark_index.component)
                {
                    return Err(CoreError::Configuration(
                        "duplicate realtime core mark/index component binding".into(),
                    ));
                }
                mark_index_source_ids.insert(binding.source_id.clone());
            } else if mark_index_source_ids.contains(&binding.source_id)
                || !ordinary_source_ids.insert(binding.source_id.clone())
            {
                return Err(CoreError::Configuration(
                    "duplicate realtime core source ID".into(),
                ));
            }
        }
        for (target, components) in mark_index_components {
            let complete = components == HashSet::from([MarkIndexComponent::Both])
                || components
                    == HashSet::from([MarkIndexComponent::Mark, MarkIndexComponent::Index]);
            if !complete {
                return Err(CoreError::Configuration(format!(
                    "mark/index target has an incomplete component pair: {target}"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoreError {
    Configuration(String),
    Decode(String),
    RawEnvelope(String),
    UnknownBinding,
    ProvenanceRejected,
}

impl Display for CoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(value) => write!(formatter, "core config error: {value}"),
            Self::Decode(value) => write!(formatter, "core decode error: {value}"),
            Self::RawEnvelope(value) => write!(formatter, "raw envelope error: {value}"),
            Self::UnknownBinding => write!(formatter, "raw envelope has no approved binding"),
            Self::ProvenanceRejected => write!(formatter, "test provenance is not allowed"),
        }
    }
}

impl std::error::Error for CoreError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessBatch {
    pub canonical: Vec<DurableRecord>,
    pub quarantines: Vec<DurableRecord>,
    pub duplicates: usize,
    pub filtered: usize,
}

#[derive(Clone, Debug)]
struct MarkIndexComponentState {
    price: String,
    source_event_time_ms: i64,
    received_at_ns: i64,
    raw_capture_id: Vec<u8>,
    raw_frame_sha256: Vec<u8>,
}

#[derive(Clone, Debug)]
struct MarkIndexPairState {
    source_session_id: String,
    connection_generation: u64,
    mark: Option<MarkIndexComponentState>,
    index: Option<MarkIndexComponentState>,
}

pub struct RealtimeCore {
    config: RealtimeCoreConfig,
    bindings: BTreeMap<String, CoreBinding>,
    l2_adapters: BTreeMap<String, L2BookAdapter>,
    l2_snapshot_observed_at_ms: BTreeMap<String, i64>,
    mark_index_pairs: BTreeMap<String, MarkIndexPairState>,
    partition_sequences: BTreeMap<String, u64>,
    ordering: OrderingTracker,
    seen_ids: HashSet<Vec<u8>>,
    seen_order: VecDeque<Vec<u8>>,
}

impl RealtimeCore {
    pub fn new(config: RealtimeCoreConfig) -> Result<Self, CoreError> {
        config.validate()?;
        let mut bindings = BTreeMap::new();
        let mut l2_adapters = BTreeMap::new();
        for binding in &config.bindings {
            let key = binding.key();
            if let Some(l2) = &binding.l2 {
                let identity = BookIdentity::new(
                    binding.provider_kind.clone(),
                    binding.instrument_uid.clone(),
                    binding.native_channel.clone(),
                )
                .map_err(|error| CoreError::Configuration(error.to_string()))?;
                let adapter = match l2.provider_protocol {
                    L2ProviderProtocol::BinanceDiffDepth => L2BookAdapter::binance_diff_depth(
                        identity,
                        binding.native_symbol.clone(),
                        l2.depth_per_side,
                    ),
                    L2ProviderProtocol::OkxPublicBooks => L2BookAdapter::okx_public_books(
                        identity,
                        binding.native_symbol.clone(),
                        l2.depth_per_side,
                    ),
                }
                .map_err(|error| CoreError::Configuration(error.to_string()))?;
                l2_adapters.insert(key.clone(), adapter);
            }
            bindings.insert(key, binding.clone());
        }
        Ok(Self {
            config,
            bindings,
            l2_adapters,
            l2_snapshot_observed_at_ms: BTreeMap::new(),
            mark_index_pairs: BTreeMap::new(),
            partition_sequences: BTreeMap::new(),
            ordering: OrderingTracker::new(4096),
            seen_ids: HashSet::new(),
            seen_order: VecDeque::new(),
        })
    }

    pub fn process_bytes(
        &mut self,
        raw_bytes: &[u8],
        normalized_at_ns: i64,
    ) -> Result<ProcessBatch, CoreError> {
        if normalized_at_ns <= 0 {
            return Err(CoreError::Decode(
                "normalized_at_ns must be positive".into(),
            ));
        }
        let raw = RawProviderEnvelope::decode(raw_bytes)
            .map_err(|error| CoreError::Decode(error.to_string()))?;
        self.process(raw, normalized_at_ns)
    }

    pub fn process(
        &mut self,
        raw: RawProviderEnvelope,
        processing_at_ns: i64,
    ) -> Result<ProcessBatch, CoreError> {
        self.process_internal(raw, processing_at_ns, None)
    }

    pub fn process_at_transport_offset(
        &mut self,
        raw: RawProviderEnvelope,
        processing_at_ns: i64,
        transport_offset: u64,
    ) -> Result<ProcessBatch, CoreError> {
        self.process_internal(raw, processing_at_ns, Some(transport_offset))
    }

    /// Preserve a valid raw envelope as durable quarantine evidence when the
    /// runtime ingress boundary rejects it before canonicalization.
    pub fn quarantine_raw(
        &self,
        raw: &RawProviderEnvelope,
        reason: QuarantineReason,
        summary: &str,
        quarantined_at_ns: i64,
    ) -> ProcessBatch {
        self.quarantine(raw, reason, summary, quarantined_at_ns)
    }

    fn process_internal(
        &mut self,
        raw: RawProviderEnvelope,
        processing_at_ns: i64,
        transport_offset: Option<u64>,
    ) -> Result<ProcessBatch, CoreError> {
        if processing_at_ns <= 0 {
            return Err(CoreError::Decode(
                "processing_at_ns must be positive".into(),
            ));
        }
        validate_raw(&raw).map_err(|error| CoreError::RawEnvelope(error.to_string()))?;
        let materialized_at_ns = raw.received_at_ns;
        if raw.test_provenance && !self.config.allow_test_provenance {
            return Err(CoreError::ProvenanceRejected);
        }
        let key = binding_key(
            &raw.provider,
            &raw.venue,
            &raw.market,
            &raw.product_type,
            &raw.native_symbol,
            &raw.native_channel,
        );
        let binding = self
            .bindings
            .get(&key)
            .cloned()
            .ok_or(CoreError::UnknownBinding)?;
        if raw.instrument_catalog_revision != binding.instrument_catalog_revision {
            return Ok(self.quarantine(
                &raw,
                QuarantineReason::UnknownInstrument,
                "instrument catalog revision mismatch",
                materialized_at_ns,
            ));
        }
        let payload: Value = serde_json::from_slice(&raw.raw_frame_bytes)
            .map_err(|error| CoreError::Decode(error.to_string()))?;
        if binding.mark_index.is_some() {
            return Ok(self.process_mark_index(&binding, raw, payload, processing_at_ns));
        }
        if binding.l2.is_some() {
            return Ok(self.process_l2(
                &key,
                &binding,
                raw,
                payload,
                processing_at_ns,
                transport_offset,
            ));
        }
        let frames = expand_frames(&binding, payload).map_err(CoreError::Decode)?;
        if transport_offset.is_some() && frames.len() as u64 >= TRANSPORT_SEQUENCE_STRIDE {
            return Err(CoreError::Configuration(
                "expanded provider frame exceeds transport sequence stride".into(),
            ));
        }
        let mut staged_partition_sequences = BTreeMap::new();
        let mut staged_ordering: BTreeMap<String, OrderingStage> = BTreeMap::new();
        let mut staged_seen_ids = HashSet::new();
        let mut staged_seen_order = Vec::new();
        let mut batch = ProcessBatch {
            canonical: Vec::with_capacity(frames.len()),
            quarantines: vec![],
            duplicates: 0,
            filtered: 0,
        };
        let mut failure: Option<(QuarantineReason, &'static str)> = None;
        for (row_index, (provider_kind, frame)) in frames.into_iter().enumerate() {
            if is_binance_trade_status_frame(&binding, &provider_kind, &frame) {
                batch.filtered += 1;
                continue;
            }
            let partition_key = format!(
                "{}/{}/{}",
                binding.instrument_uid, provider_kind, binding.source_id
            );
            let partition_sequence = if let Some(offset) = transport_offset {
                offset
                    .checked_mul(TRANSPORT_SEQUENCE_STRIDE)
                    .and_then(|base| base.checked_add(row_index as u64 + 1))
                    .ok_or_else(|| {
                        CoreError::Configuration(
                            "transport-derived partition sequence overflow".into(),
                        )
                    })?
            } else {
                staged_partition_sequences
                    .get(&partition_key)
                    .or_else(|| self.partition_sequences.get(&partition_key))
                    .copied()
                    .unwrap_or(0_u64)
                    .saturating_add(1)
            };
            let fixture = TradeFixture {
                provider_kind,
                context: TradeContext {
                    instrument_uid: binding.instrument_uid.clone(),
                    instrument_id: binding.instrument_id.clone(),
                    instrument_revision: binding.instrument_revision,
                    venue: binding.venue.clone(),
                    market: binding.market.clone(),
                    product_type: binding.product_type.clone(),
                    native_symbol: binding.native_symbol.clone(),
                    provider: binding.provider.clone(),
                    source_id: binding.source_id.clone(),
                    lease_epoch: raw.lease_epoch,
                    received_at_ns: raw.received_at_ns,
                    normalized_at_ns: materialized_at_ns,
                    published_at_ns: materialized_at_ns,
                    partition_sequence,
                    normalizer_version: binding.normalizer_version.clone(),
                    adapter_version: raw.adapter_version.clone(),
                    config_revision: raw.config_revision,
                    correlation_id: raw.correlation_id.clone(),
                    source_session_id: raw.source_session_id.clone(),
                    connection_generation: raw.connection_generation,
                    authority_revision: raw.authority_revision,
                    partition_plan_epoch: raw.partition_plan_epoch,
                    raw_capture_id: raw.capture_id.clone(),
                    raw_frame_sha256: raw.raw_frame_sha256.clone(),
                    source_role: binding.source_role.clone(),
                },
                raw: frame,
            };
            let canonical = match canonicalize_trade(&fixture) {
                Ok(value) => value,
                Err(_) => {
                    failure = Some((QuarantineReason::SemanticInvalid, "canonicalization failed"));
                    break;
                }
            };
            if binding.require_final_bar {
                match canonical.payload.as_ref() {
                    Some(event_envelope::Payload::Bar(bar))
                        if bar.is_final
                            && matches!(
                                BarLifecycle::try_from(bar.lifecycle),
                                Ok(BarLifecycle::Final | BarLifecycle::Revised)
                            ) => {}
                    Some(event_envelope::Payload::Bar(_)) => {
                        batch.filtered += 1;
                        continue;
                    }
                    _ => {
                        failure = Some((
                            QuarantineReason::SemanticInvalid,
                            "final BAR policy applied to a non-BAR payload",
                        ));
                        break;
                    }
                }
            }
            if self.seen_ids.contains(&canonical.event_id)
                || staged_seen_ids.contains(&canonical.event_id)
            {
                batch.duplicates += 1;
                continue;
            }
            let sequence = match binding.sequence_policy {
                SequencePolicy::None => partition_sequence,
                SequencePolicy::Monotonic | SequencePolicy::Contiguous => {
                    match canonical.source_sequence.parse::<u64>() {
                        Ok(value) => value,
                        Err(_) => {
                            failure = Some((
                                QuarantineReason::SemanticInvalid,
                                "native sequence is not numeric",
                            ));
                            break;
                        }
                    }
                }
            };
            let ordering_stage = staged_ordering
                .entry(partition_key.clone())
                .or_insert_with(|| self.ordering.stage(&partition_key));
            match self.ordering.observe_staged(
                ordering_stage,
                &raw.source_session_id,
                raw.connection_generation,
                sequence,
                canonical.event_id.clone(),
                binding.sequence_policy,
            ) {
                SequenceDecision::Duplicate => {
                    batch.duplicates += 1;
                    continue;
                }
                SequenceDecision::Gap { .. } => {
                    failure = Some((
                        QuarantineReason::SequenceGap,
                        "native sequence gap requires recovery",
                    ));
                    break;
                }
                SequenceDecision::OutOfOrder => {
                    failure = Some((
                        QuarantineReason::SemanticInvalid,
                        "native sequence is out of order",
                    ));
                    break;
                }
                SequenceDecision::StaleSession => {
                    failure = Some((
                        QuarantineReason::StaleGeneration,
                        "connection generation is stale",
                    ));
                    break;
                }
                SequenceDecision::Accepted | SequenceDecision::SessionStarted => {}
            }
            staged_partition_sequences.insert(partition_key, partition_sequence);
            staged_seen_ids.insert(canonical.event_id.clone());
            staged_seen_order.push(canonical.event_id.clone());
            batch.canonical.push(canonical_record(
                &self.config.canonical_stream,
                canonical,
                materialized_at_ns,
            ));
        }
        if let Some((reason, summary)) = failure {
            return Ok(self.quarantine(&raw, reason, summary, materialized_at_ns));
        }
        self.partition_sequences.extend(staged_partition_sequences);
        for stage in staged_ordering.into_values() {
            self.ordering.commit_stage(stage);
        }
        for event_id in staged_seen_order {
            self.remember(event_id);
        }
        Ok(batch)
    }

    fn process_mark_index(
        &mut self,
        binding: &CoreBinding,
        raw: RawProviderEnvelope,
        payload: Value,
        processing_at_ns: i64,
    ) -> ProcessBatch {
        let materialized_at_ns = raw.received_at_ns;
        let Some(contract) = binding.mark_index.as_ref() else {
            unreachable!("mark/index processing requires a mark/index binding")
        };
        let parsed = match parse_mark_index_component(binding, contract.component, &payload) {
            Ok(value) => value,
            Err(summary) => {
                return self.quarantine(
                    &raw,
                    QuarantineReason::SemanticInvalid,
                    &summary,
                    materialized_at_ns,
                )
            }
        };
        let target_key = binding.mark_index_target_key();
        let mut state =
            self.mark_index_pairs
                .get(&target_key)
                .cloned()
                .unwrap_or(MarkIndexPairState {
                    source_session_id: raw.source_session_id.clone(),
                    connection_generation: raw.connection_generation,
                    mark: None,
                    index: None,
                });
        if raw.connection_generation < state.connection_generation {
            return self.quarantine(
                &raw,
                QuarantineReason::StaleGeneration,
                "mark/index raw frame belongs to an older connection generation",
                materialized_at_ns,
            );
        }
        if raw.connection_generation > state.connection_generation {
            state = MarkIndexPairState {
                source_session_id: raw.source_session_id.clone(),
                connection_generation: raw.connection_generation,
                mark: None,
                index: None,
            };
        } else if state.source_session_id != raw.source_session_id {
            return self.quarantine(
                &raw,
                QuarantineReason::StaleGeneration,
                "mark/index raw frame has a mismatched source session",
                materialized_at_ns,
            );
        }

        let candidate = |(price, source_event_time_ms): (String, i64)| MarkIndexComponentState {
            price,
            source_event_time_ms,
            received_at_ns: raw.received_at_ns,
            raw_capture_id: raw.capture_id.clone(),
            raw_frame_sha256: raw.raw_frame_sha256.clone(),
        };
        let update = |slot: &mut Option<MarkIndexComponentState>, value: (String, i64)| {
            update_mark_index_component(slot, candidate(value))
        };
        let apply = match parsed.component {
            MarkIndexComponent::Both => {
                let mark = parsed
                    .mark
                    .ok_or("Binance mark/index frame omitted mark price")
                    .and_then(|value| update(&mut state.mark, value));
                let index = parsed
                    .index
                    .ok_or("Binance mark/index frame omitted index price")
                    .and_then(|value| update(&mut state.index, value));
                match (mark, index) {
                    (Ok(mark), Ok(index)) => Ok(mark || index),
                    (Err(error), _) | (_, Err(error)) => Err(error),
                }
            }
            MarkIndexComponent::Mark => parsed
                .mark
                .ok_or("OKX mark frame omitted mark price")
                .and_then(|value| update(&mut state.mark, value)),
            MarkIndexComponent::Index => parsed
                .index
                .ok_or("OKX index frame omitted index price")
                .and_then(|value| update(&mut state.index, value)),
        };
        let changed = match apply {
            Ok(value) => value,
            Err(summary) => {
                return self.quarantine(
                    &raw,
                    QuarantineReason::SemanticInvalid,
                    summary,
                    materialized_at_ns,
                )
            }
        };
        if !changed {
            return ProcessBatch {
                canonical: vec![],
                quarantines: vec![],
                duplicates: 0,
                filtered: 1,
            };
        }
        let (Some(mark), Some(index)) = (state.mark.as_ref(), state.index.as_ref()) else {
            self.mark_index_pairs.insert(target_key, state);
            return ProcessBatch {
                canonical: vec![],
                quarantines: vec![],
                duplicates: 0,
                filtered: 0,
            };
        };
        let oldest_confirmation_ns = mark.received_at_ns.min(index.received_at_ns);
        let newest_confirmation_ns = mark.received_at_ns.max(index.received_at_ns);
        let source_event_time_ms = mark.source_event_time_ms.min(index.source_event_time_ms);
        let (raw_capture_id, raw_frame_sha256) = pair_raw_identity(mark, index);
        let source_sequence = format!(
            "{}:{}:{}:{}:{}:{}",
            mark.source_event_time_ms,
            index.source_event_time_ms,
            mark.received_at_ns,
            index.received_at_ns,
            hex::encode(&mark.raw_capture_id),
            hex::encode(&index.raw_capture_id),
        );
        let partition_key = format!(
            "{}/mark-index/{}",
            binding.instrument_uid, binding.source_id
        );
        let partition_sequence = self
            .partition_sequences
            .get(&partition_key)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        let fixture = TradeFixture {
            provider_kind: binding.provider_kind.clone(),
            context: TradeContext {
                instrument_uid: binding.instrument_uid.clone(),
                instrument_id: binding.instrument_id.clone(),
                instrument_revision: binding.instrument_revision,
                venue: binding.venue.clone(),
                market: binding.market.clone(),
                product_type: binding.product_type.clone(),
                native_symbol: binding.native_symbol.clone(),
                provider: binding.provider.clone(),
                source_id: binding.source_id.clone(),
                lease_epoch: raw.lease_epoch,
                // A paired execution view is only as fresh as its oldest
                // provider confirmation. The original source value time stays
                // in the envelope separately.
                received_at_ns: oldest_confirmation_ns,
                normalized_at_ns: newest_confirmation_ns,
                published_at_ns: newest_confirmation_ns,
                partition_sequence,
                normalizer_version: binding.normalizer_version.clone(),
                adapter_version: raw.adapter_version.clone(),
                config_revision: raw.config_revision,
                correlation_id: hex::encode(&raw_capture_id),
                source_session_id: raw.source_session_id.clone(),
                connection_generation: raw.connection_generation,
                authority_revision: raw.authority_revision,
                partition_plan_epoch: raw.partition_plan_epoch,
                raw_capture_id,
                raw_frame_sha256,
                source_role: binding.source_role.clone(),
            },
            raw: json!({
                "mark": {
                    "price": mark.price,
                    "source_event_time_ms": mark.source_event_time_ms,
                    "received_at_ns": mark.received_at_ns,
                },
                "index": {
                    "price": index.price,
                    "source_event_time_ms": index.source_event_time_ms,
                    "received_at_ns": index.received_at_ns,
                },
            }),
        };
        let canonical = match canonicalize_mark_index(
            &fixture,
            MarkIndexInput {
                mark_price: mark.price.clone(),
                index_price: index.price.clone(),
                source_event_time_ms,
                source_sequence,
            },
        ) {
            Ok(value) => value,
            Err(_) => {
                return self.quarantine(
                    &raw,
                    QuarantineReason::SemanticInvalid,
                    "mark/index pair failed canonical validation",
                    materialized_at_ns,
                )
            }
        };
        if self.seen_ids.contains(&canonical.event_id) {
            return ProcessBatch {
                canonical: vec![],
                quarantines: vec![],
                duplicates: 1,
                filtered: 0,
            };
        }
        self.partition_sequences
            .insert(partition_key, partition_sequence);
        self.mark_index_pairs.insert(target_key, state);
        self.remember(canonical.event_id.clone());
        ProcessBatch {
            canonical: vec![canonical_record(
                &self.config.canonical_stream,
                canonical,
                newest_confirmation_ns.max(processing_at_ns),
            )],
            quarantines: vec![],
            duplicates: 0,
            filtered: 0,
        }
    }

    fn process_l2(
        &mut self,
        binding_key: &str,
        binding: &CoreBinding,
        raw: RawProviderEnvelope,
        payload: Value,
        processing_at_ns: i64,
        transport_offset: Option<u64>,
    ) -> ProcessBatch {
        let materialized_at_ns = raw.received_at_ns;
        let Some(l2) = binding.l2.as_ref() else {
            unreachable!("process_l2 only accepts an L2 binding")
        };
        if l2.raw_receipt_is_expired(raw.received_at_ns, processing_at_ns) {
            if let Some(adapter) = self.l2_adapters.get_mut(binding_key) {
                adapter.request_resync(raw.connection_generation);
            }
            return self.quarantine(
                &raw,
                QuarantineReason::SemanticInvalid,
                "L2 raw receipt exceeded provider renewal bound; resync required",
                processing_at_ns,
            );
        }
        let transport = match TransportProtocol::try_from(raw.transport_protocol) {
            Ok(value) => value,
            Err(_) => {
                return self.quarantine(
                    &raw,
                    QuarantineReason::SemanticInvalid,
                    "L2 raw transport protocol is invalid",
                    materialized_at_ns,
                )
            }
        };
        let partition_sequence = match self.next_l2_partition_sequence(binding, transport_offset) {
            Some(value) => value,
            None => {
                return self.quarantine(
                    &raw,
                    QuarantineReason::SemanticInvalid,
                    "L2 transport-derived partition sequence overflow",
                    materialized_at_ns,
                )
            }
        };
        let fixture_payload = payload.clone();
        let transition = {
            let Some(adapter) = self.l2_adapters.get_mut(binding_key) else {
                return self.quarantine(
                    &raw,
                    QuarantineReason::SemanticInvalid,
                    "L2 binding has no initialized state machine",
                    materialized_at_ns,
                );
            };
            let transition = match (l2.provider_protocol, transport) {
                (L2ProviderProtocol::BinanceDiffDepth, TransportProtocol::Http) => adapter
                    .apply_binance_rest_snapshot(
                        &payload,
                        raw.connection_generation,
                        raw.received_at_ns / 1_000_000,
                    ),
                (L2ProviderProtocol::BinanceDiffDepth, TransportProtocol::Websocket) => {
                    let frame = payload.get("data").cloned().unwrap_or(payload);
                    adapter.apply_binance_ws_delta(&frame, raw.connection_generation)
                }
                (L2ProviderProtocol::OkxPublicBooks, TransportProtocol::Websocket) => {
                    adapter.apply_okx_ws_frame(&payload, raw.connection_generation)
                }
                _ => Err("L2 transport is not documented for the provider protocol".into()),
            };
            let transition = match transition {
                Ok(value) => value,
                Err(_) => {
                    return self.quarantine(
                        &raw,
                        QuarantineReason::SemanticInvalid,
                        "L2 provider frame failed strict parser validation",
                        materialized_at_ns,
                    )
                }
            };
            if matches!(
                transition.outcome,
                BookOutcome::SequenceGap
                    | BookOutcome::OutOfOrder
                    | BookOutcome::ChecksumRejected
                    | BookOutcome::InvalidFrame
            ) {
                adapter.request_resync(raw.connection_generation);
                let reason = if transition.outcome == BookOutcome::SequenceGap {
                    QuarantineReason::SequenceGap
                } else {
                    QuarantineReason::SemanticInvalid
                };
                return self.quarantine(
                    &raw,
                    reason,
                    "L2 state machine rejected continuity; resync required",
                    materialized_at_ns,
                );
            }
            transition
        };

        let canonical = match canonicalize_l2_publication(
            &l2_fixture(binding, &raw, fixture_payload.clone(), partition_sequence),
            &transition,
        ) {
            Ok(value) => value,
            Err(_) => {
                if let Some(adapter) = self.l2_adapters.get_mut(binding_key) {
                    adapter.request_resync(raw.connection_generation);
                }
                return self.quarantine(
                    &raw,
                    QuarantineReason::SemanticInvalid,
                    "L2 canonical projection failed strict validation",
                    materialized_at_ns,
                );
            }
        };
        let materialized_snapshot =
            self.due_l2_materialized_snapshot(binding_key, l2, &raw, &transition);
        let mut publications = Vec::new();
        if let Some(canonical) = canonical {
            let snapshot_observed_at_ms = if transition.publication == BookPublication::Snapshot {
                l2_observed_at_ms(&transition, &raw)
            } else {
                None
            };
            publications.push((canonical, snapshot_observed_at_ms));
        }
        if let Some(snapshot_transition) = materialized_snapshot {
            let Some(snapshot_partition_sequence) = partition_sequence.checked_add(1) else {
                return self.quarantine(
                    &raw,
                    QuarantineReason::SemanticInvalid,
                    "L2 materialized snapshot partition sequence overflow",
                    materialized_at_ns,
                );
            };
            let canonical = match canonicalize_l2_publication(
                &l2_fixture(binding, &raw, fixture_payload, snapshot_partition_sequence),
                &snapshot_transition,
            ) {
                Ok(Some(value)) => value,
                Ok(None) | Err(_) => {
                    if let Some(adapter) = self.l2_adapters.get_mut(binding_key) {
                        adapter.request_resync(raw.connection_generation);
                    }
                    return self.quarantine(
                        &raw,
                        QuarantineReason::SemanticInvalid,
                        "L2 materialized snapshot failed strict canonical projection",
                        materialized_at_ns,
                    );
                }
            };
            publications.push((canonical, snapshot_transition.materialized_snapshot_at_ms));
        }
        if publications.is_empty() {
            return ProcessBatch {
                canonical: vec![],
                quarantines: vec![],
                duplicates: usize::from(transition.outcome == BookOutcome::Duplicate),
                filtered: usize::from(transition.outcome != BookOutcome::Duplicate),
            };
        }
        let partition_key = l2_partition_key(binding);
        let mut durable = Vec::new();
        let mut duplicates = usize::from(transition.outcome == BookOutcome::Duplicate);
        for (canonical, snapshot_observed_at_ms) in publications {
            if self.seen_ids.contains(&canonical.event_id) {
                duplicates = duplicates.saturating_add(1);
                continue;
            }
            self.partition_sequences
                .insert(partition_key.clone(), canonical.partition_sequence);
            self.remember(canonical.event_id.clone());
            if let Some(observed_at_ms) = snapshot_observed_at_ms {
                self.l2_snapshot_observed_at_ms
                    .insert(binding_key.to_owned(), observed_at_ms);
            }
            durable.push(canonical_record(
                &self.config.canonical_stream,
                canonical,
                materialized_at_ns,
            ));
        }
        ProcessBatch {
            canonical: durable,
            quarantines: vec![],
            duplicates,
            filtered: 0,
        }
    }

    fn due_l2_materialized_snapshot(
        &self,
        binding_key: &str,
        l2: &L2Binding,
        raw: &RawProviderEnvelope,
        transition: &BookTransition,
    ) -> Option<BookTransition> {
        if transition.publication == BookPublication::Snapshot
            || !transition.sequence_verified()
            || !matches!(
                transition.outcome,
                BookOutcome::DeltaApplied | BookOutcome::Keepalive
            )
        {
            return None;
        }
        let observed_at_ms = l2_observed_at_ms(transition, raw)?;
        let refresh_ms = i64::try_from(l2.materialized_snapshot_interval_ms()).ok()?;
        let due = self
            .l2_snapshot_observed_at_ms
            .get(binding_key)
            .map(|previous| observed_at_ms.saturating_sub(*previous) >= refresh_ms)
            .unwrap_or(true);
        if !due {
            return None;
        }
        let mut snapshot = transition.clone();
        snapshot.publication = BookPublication::Snapshot;
        snapshot.materialized_snapshot_at_ms = Some(observed_at_ms);
        Some(snapshot)
    }

    fn next_l2_partition_sequence(
        &self,
        binding: &CoreBinding,
        transport_offset: Option<u64>,
    ) -> Option<u64> {
        if let Some(offset) = transport_offset {
            return offset
                .checked_mul(TRANSPORT_SEQUENCE_STRIDE)
                .and_then(|base| base.checked_add(1));
        }
        Some(
            self.partition_sequences
                .get(&l2_partition_key(binding))
                .copied()
                .unwrap_or(0)
                .saturating_add(1),
        )
    }

    fn remember(&mut self, event_id: Vec<u8>) {
        self.seen_ids.insert(event_id.clone());
        self.seen_order.push_back(event_id);
        while self.seen_order.len() > self.config.dedup_capacity {
            if let Some(expired) = self.seen_order.pop_front() {
                self.seen_ids.remove(&expired);
            }
        }
    }

    fn quarantine(
        &self,
        raw: &RawProviderEnvelope,
        reason: QuarantineReason,
        summary: &str,
        now_ns: i64,
    ) -> ProcessBatch {
        let mut evidence = Sha256::new();
        evidence.update(raw.encode_to_vec());
        evidence.update((reason as i32).to_be_bytes());
        let record = QuarantineRecord {
            raw: Some(raw.clone()),
            reason: reason as i32,
            safe_summary: summary.chars().take(200).collect(),
            quarantined_at_ns: now_ns,
            retry_count: 0,
            evidence_sha256: evidence.finalize().to_vec(),
        };
        ProcessBatch {
            canonical: vec![],
            quarantines: vec![DurableRecord {
                stream: self.config.quarantine_stream.clone(),
                partition_key: format!(
                    "{}/{}/{}",
                    raw.provider, raw.native_symbol, raw.source_session_id
                ),
                event_id: raw.capture_id.clone(),
                payload: record.encode_to_vec(),
                accepted_at_ns: now_ns,
            }],
            duplicates: 0,
            filtered: 0,
        }
    }
}

fn l2_observed_at_ms(transition: &BookTransition, raw: &RawProviderEnvelope) -> Option<i64> {
    let observed_at_ms = transition
        .observed_at_ms
        .unwrap_or(raw.received_at_ns / 1_000_000);
    (observed_at_ms > 0).then_some(observed_at_ms)
}

#[derive(Clone, Debug)]
struct ParsedMarkIndexComponent {
    component: MarkIndexComponent,
    mark: Option<(String, i64)>,
    index: Option<(String, i64)>,
}

fn parse_mark_index_component(
    binding: &CoreBinding,
    component: MarkIndexComponent,
    payload: &Value,
) -> Result<ParsedMarkIndexComponent, String> {
    match (binding.venue.as_str(), component) {
        ("BINANCE", MarkIndexComponent::Both) => {
            let frame = payload.get("data").unwrap_or(payload);
            if frame.get("e").and_then(Value::as_str) != Some("markPriceUpdate")
                || frame.get("s").and_then(Value::as_str) != Some(binding.physical_native_symbol())
            {
                return Err("Binance mark/index frame identity is invalid".into());
            }
            let source_event_time_ms = json_timestamp_ms(frame, "E")?;
            Ok(ParsedMarkIndexComponent {
                component,
                mark: Some((json_text(frame, "p")?, source_event_time_ms)),
                index: Some((json_text(frame, "i")?, source_event_time_ms)),
            })
        }
        ("OKX", MarkIndexComponent::Mark | MarkIndexComponent::Index) => {
            let arg = payload
                .get("arg")
                .and_then(Value::as_object)
                .ok_or("OKX mark/index frame has no arg")?;
            if arg.get("channel").and_then(Value::as_str) != Some(binding.physical_native_channel())
                || arg.get("instId").and_then(Value::as_str)
                    != Some(binding.physical_native_symbol())
            {
                return Err("OKX mark/index frame identity is invalid".into());
            }
            let rows = payload
                .get("data")
                .and_then(Value::as_array)
                .ok_or("OKX mark/index frame has no data rows")?;
            if rows.len() != 1 {
                return Err("OKX mark/index frame must contain exactly one row".into());
            }
            let row = &rows[0];
            if row
                .get("instId")
                .and_then(Value::as_str)
                .is_some_and(|value| value != binding.physical_native_symbol())
            {
                return Err("OKX mark/index row identity is invalid".into());
            }
            let source_event_time_ms = json_timestamp_ms(row, "ts")?;
            let price = match component {
                MarkIndexComponent::Mark => json_text(row, "markPx")?,
                MarkIndexComponent::Index => json_text(row, "idxPx")?,
                MarkIndexComponent::Both => unreachable!("OKX component was prevalidated"),
            };
            Ok(ParsedMarkIndexComponent {
                component,
                mark: (component == MarkIndexComponent::Mark)
                    .then_some((price.clone(), source_event_time_ms)),
                index: (component == MarkIndexComponent::Index)
                    .then_some((price, source_event_time_ms)),
            })
        }
        _ => Err("mark/index component differs from its approved provider protocol".into()),
    }
}

fn json_text(value: &Value, field: &str) -> Result<String, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("mark/index field {field} is missing or blank"))
}

fn json_timestamp_ms(value: &Value, field: &str) -> Result<i64, String> {
    let timestamp = value
        .get(field)
        .and_then(|raw| match raw {
            Value::String(text) => text.parse::<i64>().ok(),
            Value::Number(number) => number.as_i64(),
            _ => None,
        })
        .filter(|timestamp| *timestamp > 0)
        .ok_or_else(|| format!("mark/index timestamp {field} is invalid"))?;
    Ok(timestamp)
}

fn update_mark_index_component(
    slot: &mut Option<MarkIndexComponentState>,
    candidate: MarkIndexComponentState,
) -> Result<bool, &'static str> {
    let Some(current) = slot.as_ref() else {
        *slot = Some(candidate);
        return Ok(true);
    };
    if candidate.source_event_time_ms < current.source_event_time_ms {
        return Ok(false);
    }
    if candidate.source_event_time_ms == current.source_event_time_ms
        && candidate.price != current.price
    {
        return Err("mark/index provider timestamp has conflicting values");
    }
    // A same-value/same-source-time frame is still a real provider
    // confirmation. Keep the original value timestamp, but refresh the
    // receipt lineage so query freshness is not confused with price movement.
    *slot = Some(candidate);
    Ok(true)
}

fn pair_raw_identity(
    mark: &MarkIndexComponentState,
    index: &MarkIndexComponentState,
) -> (Vec<u8>, Vec<u8>) {
    let mut capture = Sha256::new();
    capture.update(b"qdl-mark-index-capture-v1");
    capture.update(&mark.raw_capture_id);
    capture.update(&index.raw_capture_id);
    // EventEnvelope carries a fixed 16-byte capture identity.  The paired
    // raw-frame SHA-256 below preserves the full digest lineage separately.
    let raw_capture_id = capture.finalize()[..16].to_vec();

    let mut payload = Sha256::new();
    payload.update(b"qdl-mark-index-payload-v1");
    payload.update(&mark.raw_frame_sha256);
    payload.update(&index.raw_frame_sha256);
    (raw_capture_id, payload.finalize().to_vec())
}

fn binding_key(
    provider: &str,
    venue: &str,
    market: &str,
    product_type: &str,
    native_symbol: &str,
    native_channel: &str,
) -> String {
    [
        provider,
        venue,
        market,
        product_type,
        native_symbol,
        native_channel,
    ]
    .join("|")
}

fn l2_partition_key(binding: &CoreBinding) -> String {
    format!("{}/book/{}", binding.instrument_uid, binding.source_id)
}

fn l2_fixture(
    binding: &CoreBinding,
    raw: &RawProviderEnvelope,
    payload: Value,
    partition_sequence: u64,
) -> TradeFixture {
    TradeFixture {
        provider_kind: binding.provider_kind.clone(),
        context: TradeContext {
            instrument_uid: binding.instrument_uid.clone(),
            instrument_id: binding.instrument_id.clone(),
            instrument_revision: binding.instrument_revision,
            venue: binding.venue.clone(),
            market: binding.market.clone(),
            product_type: binding.product_type.clone(),
            native_symbol: binding.native_symbol.clone(),
            provider: binding.provider.clone(),
            source_id: binding.source_id.clone(),
            lease_epoch: raw.lease_epoch,
            received_at_ns: raw.received_at_ns,
            normalized_at_ns: raw.received_at_ns,
            published_at_ns: raw.received_at_ns,
            partition_sequence,
            normalizer_version: binding.normalizer_version.clone(),
            adapter_version: raw.adapter_version.clone(),
            config_revision: raw.config_revision,
            correlation_id: raw.correlation_id.clone(),
            source_session_id: raw.source_session_id.clone(),
            connection_generation: raw.connection_generation,
            authority_revision: raw.authority_revision,
            partition_plan_epoch: raw.partition_plan_epoch,
            raw_capture_id: raw.capture_id.clone(),
            raw_frame_sha256: raw.raw_frame_sha256.clone(),
            source_role: binding.source_role.clone(),
        },
        raw: payload,
    }
}

fn is_binance_trade_status_frame(
    binding: &CoreBinding,
    provider_kind: &str,
    frame: &Value,
) -> bool {
    binding.venue == "BINANCE"
        && provider_kind.ends_with("_trade")
        && frame.get("e").and_then(Value::as_str) == Some("trade")
        && frame.get("p").and_then(Value::as_str) == Some("0")
        && frame.get("q").and_then(Value::as_str) == Some("0")
        && frame.get("X").and_then(Value::as_str) == Some("NA")
        && frame.get("st").and_then(Value::as_u64) == Some(1)
}

fn expand_frames(binding: &CoreBinding, payload: Value) -> Result<Vec<(String, Value)>, String> {
    if binding.venue == "OKX" && payload.get("arg").is_some() {
        return expand_data_frame(&payload).map(|frames| {
            frames
                .into_iter()
                .map(|frame| (frame.provider_kind.to_owned(), frame.raw))
                .collect()
        });
    }
    let frame = if binding.venue == "BINANCE" {
        payload.get("data").cloned().unwrap_or(payload)
    } else {
        payload
    };
    Ok(vec![(binding.provider_kind.clone(), frame)])
}

fn canonical_record(stream: &str, envelope: EventEnvelope, now_ns: i64) -> DurableRecord {
    let feed = match envelope.payload.as_ref() {
        Some(event_envelope::Payload::Trade(_)) => "trade",
        Some(event_envelope::Payload::Quote(_)) => "quote",
        Some(event_envelope::Payload::Bar(_)) => "bar",
        Some(event_envelope::Payload::BookSnapshot(_))
        | Some(event_envelope::Payload::BookDelta(_)) => "book",
        Some(event_envelope::Payload::FundingRate(_)) => "funding_rate",
        Some(event_envelope::Payload::OpenInterest(_)) => "open_interest",
        Some(event_envelope::Payload::MarkIndexPrice(_)) => "mark_index_price",
        Some(event_envelope::Payload::Ticker(_)) => "ticker",
        Some(event_envelope::Payload::LongShortRatio(_)) => "long_short_ratio",
        Some(event_envelope::Payload::TakerFlow(_)) => "taker_flow",
        Some(event_envelope::Payload::Basis(_)) => "basis",
        Some(event_envelope::Payload::ContractMetadata(_)) => "contract_metadata",
        Some(event_envelope::Payload::FeedState(_)) => "feed_state",
        Some(event_envelope::Payload::QualityEvent(_)) => "quality_event",
        None => "unknown",
    };
    DurableRecord {
        stream: stream.into(),
        partition_key: format!(
            "{}/{}/{}",
            envelope.instrument_uid, feed, envelope.source_id
        ),
        event_id: envelope.event_id.clone(),
        payload: envelope.encode_to_vec(),
        accepted_at_ns: now_ns,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CoreBinding, CoreError, L2Binding, L2ProviderProtocol, MarkIndexBinding,
        MarkIndexComponent, RealtimeCore, RealtimeCoreConfig,
    };
    use prost::Message;
    use qdl_contracts::qdl::common::v1::{QuantityUnit, SourceRole};
    use qdl_contracts::qdl::marketdata::v2::{event_envelope, EventEnvelope, TradeIdentityKind};
    use qdl_contracts::qdl::provider::v1::{
        CaptureBoundary, QuarantineReason, QuarantineRecord, RawProviderEnvelope,
        TransportCompression, TransportProtocol,
    };
    use qdl_venue_core::ordering::SequencePolicy;
    use sha2::{Digest, Sha256};

    type BindingSpec<'a> = (
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        SequencePolicy,
    );

    fn binding(
        (
            provider,
            venue,
            market,
            product,
            symbol,
            channel,
            provider_kind,
            source_role,
            policy,
        ): BindingSpec<'_>,
    ) -> CoreBinding {
        CoreBinding {
            provider: provider.into(),
            venue: venue.into(),
            market: market.into(),
            product_type: product.into(),
            native_symbol: symbol.into(),
            native_channel: channel.into(),
            physical_native_symbol: None,
            physical_native_channel: None,
            provider_kind: provider_kind.into(),
            instrument_uid: format!("uid-{venue}-{market}-{symbol}"),
            instrument_id: format!("{venue}.{market}.{product}.{symbol}"),
            instrument_revision: 1,
            instrument_catalog_revision: 3,
            source_id: format!("source-{provider}-{channel}"),
            source_role: source_role.into(),
            normalizer_version: "qdl-rust-core/2.0.0".into(),
            require_final_bar: provider_kind.ends_with("_bar"),
            sequence_policy: policy,
            l2: None,
            mark_index: None,
        }
    }

    fn core(binding: CoreBinding, allow_test: bool) -> RealtimeCore {
        core_many(vec![binding], allow_test)
    }

    fn core_many(bindings: Vec<CoreBinding>, allow_test: bool) -> RealtimeCore {
        RealtimeCore::new(RealtimeCoreConfig {
            canonical_stream: "qdl.test.canonical.v2".into(),
            quarantine_stream: "qdl.test.quarantine.v1".into(),
            allow_test_provenance: allow_test,
            dedup_capacity: 16,
            bindings,
        })
        .unwrap()
    }

    fn raw(binding: &CoreBinding, frame: &[u8], generation: u64) -> RawProviderEnvelope {
        RawProviderEnvelope {
            raw_schema_name: "qdl.provider.raw".into(),
            raw_schema_major: 1,
            raw_schema_minor: 0,
            capture_id: Sha256::digest([frame, &generation.to_be_bytes()].concat())[..16].to_vec(),
            provider: binding.provider.clone(),
            venue: binding.venue.clone(),
            market: binding.market.clone(),
            product_type: binding.product_type.clone(),
            native_symbol: binding.native_symbol.clone(),
            native_channel: binding.native_channel.clone(),
            subscription_id: "subscription-1".into(),
            source_session_id: format!("session-{generation}"),
            connection_generation: generation,
            lease_epoch: 7,
            authority_revision: 1,
            partition_plan_epoch: 1,
            received_at_ns: 1_786_352_400_123_456_000,
            transport_protocol: TransportProtocol::Websocket as i32,
            transport_compression: TransportCompression::None as i32,
            capture_boundary: CaptureBoundary::PostDecompression as i32,
            raw_frame_bytes: frame.to_vec(),
            raw_frame_sha256: Sha256::digest(frame).to_vec(),
            adapter_version: format!("{}/2.0.0", binding.provider.to_ascii_lowercase()),
            config_revision: 1,
            instrument_catalog_revision: binding.instrument_catalog_revision,
            correlation_id: "phase-a-core-test".into(),
            test_provenance: true,
        }
    }

    fn raw_with_receipt(
        binding: &CoreBinding,
        frame: &[u8],
        generation: u64,
        received_at_ns: i64,
    ) -> RawProviderEnvelope {
        let mut value = raw(binding, frame, generation);
        value.native_symbol = binding.physical_native_symbol().into();
        value.native_channel = binding.physical_native_channel().into();
        value.received_at_ns = received_at_ns;
        value.capture_id = Sha256::digest(
            [
                frame,
                &generation.to_be_bytes(),
                &received_at_ns.to_be_bytes(),
            ]
            .concat(),
        )[..16]
            .to_vec();
        value.correlation_id = hex::encode(&value.capture_id);
        value
    }

    fn binance_mark_index_binding(symbol: &str) -> CoreBinding {
        let mut result = binding((
            "BINANCE_DIRECT",
            "BINANCE",
            "USDM",
            "PERPETUAL",
            symbol,
            &format!("{}@markPrice@1s", symbol.to_ascii_lowercase()),
            "binance_usdm_mark_index",
            "PRIMARY",
            SequencePolicy::None,
        ));
        result.source_id = format!(
            "binance-usdm-{}-mark-index-primary-v2",
            symbol.to_ascii_lowercase()
        );
        result.mark_index = Some(MarkIndexBinding {
            component: MarkIndexComponent::Both,
        });
        result
    }

    fn okx_mark_index_bindings(symbol: &str, index_symbol: &str) -> (CoreBinding, CoreBinding) {
        let mut mark = binding((
            "OKX_DIRECT",
            "OKX",
            "SWAP",
            "PERPETUAL",
            symbol,
            "mark-price",
            "okx_mark_price",
            "PRIMARY",
            SequencePolicy::None,
        ));
        mark.source_id = format!(
            "okx-swap-{}-mark-index-primary-v2",
            symbol.to_ascii_lowercase()
        );
        mark.physical_native_symbol = Some(symbol.into());
        mark.physical_native_channel = Some("mark-price".into());
        mark.mark_index = Some(MarkIndexBinding {
            component: MarkIndexComponent::Mark,
        });
        let mut index = mark.clone();
        index.provider_kind = "okx_index_price".into();
        index.physical_native_symbol = Some(index_symbol.into());
        index.physical_native_channel = Some("index-tickers".into());
        index.mark_index = Some(MarkIndexBinding {
            component: MarkIndexComponent::Index,
        });
        (mark, index)
    }

    fn binance_book_binding() -> CoreBinding {
        let mut result = binding((
            "BINANCE_DIRECT",
            "BINANCE",
            "USDM",
            "PERPETUAL",
            "BTCUSDT",
            "btcusdt@depth@100ms",
            "binance_usdm_book",
            "PRIMARY",
            SequencePolicy::Contiguous,
        ));
        result.source_id = "binance-usdm-btcusdt-book-primary-v2".into();
        result.l2 = Some(L2Binding {
            provider_protocol: L2ProviderProtocol::BinanceDiffDepth,
            depth_per_side: 100,
            snapshot_refresh_seconds: 30,
            materialized_snapshot_interval_ms: None,
        });
        result
    }

    fn okx_book_binding() -> CoreBinding {
        let mut result = binding((
            "OKX_DIRECT",
            "OKX",
            "SWAP",
            "PERPETUAL",
            "BTC-USDT-SWAP",
            "books",
            "okx_book",
            "PRIMARY",
            SequencePolicy::Contiguous,
        ));
        result.source_id = "okx-swap-btc-usdt-swap-book-primary-v2".into();
        result.l2 = Some(L2Binding {
            provider_protocol: L2ProviderProtocol::OkxPublicBooks,
            depth_per_side: 100,
            snapshot_refresh_seconds: 30,
            materialized_snapshot_interval_ms: None,
        });
        result
    }

    #[test]
    fn binance_mark_index_reconfirmation_refreshes_receipt_without_redating_value() {
        let binding = binance_mark_index_binding("SOLUSDT");
        let mut core = core(binding.clone(), true);
        let frame =
            br#"{"e":"markPriceUpdate","E":1786352400000,"s":"SOLUSDT","p":"145.25","i":"145.10"}"#;
        let first = core
            .process(
                raw_with_receipt(&binding, frame, 1, 1_786_352_400_000_000_000),
                1_786_352_400_000_000_100,
            )
            .unwrap();
        assert_eq!(first.canonical.len(), 1);
        let first_event = EventEnvelope::decode(first.canonical[0].payload.as_slice()).unwrap();
        assert_eq!(first_event.source_event_time_ns, 1_786_352_400_000_000_000);
        assert_eq!(first_event.received_at_ns, 1_786_352_400_000_000_000);

        let second = core
            .process(
                raw_with_receipt(&binding, frame, 1, 1_786_352_401_000_000_000),
                1_786_352_401_000_000_100,
            )
            .unwrap();
        assert_eq!(second.canonical.len(), 1);
        let second_event = EventEnvelope::decode(second.canonical[0].payload.as_slice()).unwrap();
        // The provider's value timestamp is immutable, while the current
        // provider confirmation advances the execution freshness basis.
        assert_eq!(
            second_event.source_event_time_ns,
            first_event.source_event_time_ns
        );
        assert_eq!(second_event.received_at_ns, 1_786_352_401_000_000_000);
        assert_ne!(second_event.event_id, first_event.event_id);
    }

    #[test]
    fn okx_mark_index_requires_a_current_pair_on_one_generation() {
        let (mark, index) = okx_mark_index_bindings("SOL-USDT-SWAP", "SOL-USDT");
        let mut core = core_many(vec![mark.clone(), index.clone()], true);
        let mark_frame = br#"{"arg":{"channel":"mark-price","instId":"SOL-USDT-SWAP"},"data":[{"instId":"SOL-USDT-SWAP","markPx":"145.25","ts":"1786352400000"}]}"#;
        let only_mark = core
            .process(
                raw_with_receipt(&mark, mark_frame, 5, 1_786_352_400_000_000_000),
                1_786_352_400_000_000_100,
            )
            .unwrap();
        assert!(only_mark.canonical.is_empty());

        let index_frame = br#"{"arg":{"channel":"index-tickers","instId":"SOL-USDT"},"data":[{"instId":"SOL-USDT","idxPx":"145.10","ts":"1786352400100"}]}"#;
        let paired = core
            .process(
                raw_with_receipt(&index, index_frame, 5, 1_786_352_400_500_000_000),
                1_786_352_400_500_000_100,
            )
            .unwrap();
        assert_eq!(paired.canonical.len(), 1);
        let event = EventEnvelope::decode(paired.canonical[0].payload.as_slice()).unwrap();
        assert_eq!(event.native_symbol, "SOL-USDT-SWAP");
        assert_eq!(event.source_event_time_ns, 1_786_352_400_000_000_000);
        assert_eq!(event.received_at_ns, 1_786_352_400_000_000_000);

        let stale = core
            .process(
                raw_with_receipt(&index, index_frame, 4, 1_786_352_401_000_000_000),
                1_786_352_401_000_000_100,
            )
            .unwrap();
        assert_eq!(stale.quarantines.len(), 1);
        let evidence = QuarantineRecord::decode(stale.quarantines[0].payload.as_slice()).unwrap();
        assert_eq!(evidence.reason, QuarantineReason::StaleGeneration as i32);
    }

    #[test]
    fn mark_index_conflicting_same_timestamp_is_quarantined() {
        let binding = binance_mark_index_binding("DOGEUSDT");
        let mut core = core(binding.clone(), true);
        let first_frame = br#"{"e":"markPriceUpdate","E":1786352400000,"s":"DOGEUSDT","p":"0.14525","i":"0.14510"}"#;
        assert_eq!(
            core.process(
                raw_with_receipt(&binding, first_frame, 1, 1_786_352_400_000_000_000),
                1_786_352_400_000_000_100,
            )
            .unwrap()
            .canonical
            .len(),
            1
        );
        let conflicting_frame = br#"{"e":"markPriceUpdate","E":1786352400000,"s":"DOGEUSDT","p":"0.14526","i":"0.14510"}"#;
        let rejected = core
            .process(
                raw_with_receipt(&binding, conflicting_frame, 1, 1_786_352_401_000_000_000),
                1_786_352_401_000_000_100,
            )
            .unwrap();
        assert_eq!(rejected.quarantines.len(), 1);
        let evidence =
            QuarantineRecord::decode(rejected.quarantines[0].payload.as_slice()).unwrap();
        assert_eq!(evidence.reason, QuarantineReason::SemanticInvalid as i32);
    }

    fn with_transport(
        mut value: RawProviderEnvelope,
        transport: TransportProtocol,
    ) -> RawProviderEnvelope {
        value.transport_protocol = transport as i32;
        value
    }

    #[test]
    fn binance_l2_rest_bridge_then_delta_share_one_gap_free_partition() {
        let binding = binance_book_binding();
        let mut core = core(binding.clone(), true);
        let buffered = core
            .process(
                raw(
                    &binding,
                    br#"{"s":"BTCUSDT","U":99,"u":101,"pu":98,"E":1001,"b":[["60000","2"]],"a":[["60001","1"]]}"#,
                    1,
                ),
                10,
            )
            .unwrap();
        assert!(buffered.canonical.is_empty());
        assert_eq!(buffered.filtered, 1);

        let ready = core
            .process(
                with_transport(
                    raw(
                        &binding,
                        br#"{"lastUpdateId":100,"bids":[["60000","1"]],"asks":[["60001","1"]]}"#,
                        1,
                    ),
                    TransportProtocol::Http,
                ),
                11,
            )
            .unwrap();
        assert_eq!(ready.canonical.len(), 1);
        let snapshot = EventEnvelope::decode(ready.canonical[0].payload.as_slice()).unwrap();
        assert!(matches!(
            snapshot.payload,
            Some(event_envelope::Payload::BookSnapshot(_))
        ));
        assert_eq!(
            ready.canonical[0].partition_key,
            "uid-BINANCE-USDM-BTCUSDT/book/binance-usdm-btcusdt-book-primary-v2"
        );

        let delta = core
            .process(
                raw(
                    &binding,
                    br#"{"s":"BTCUSDT","U":102,"u":102,"pu":101,"E":1002,"b":[["60000","3"]],"a":[]}"#,
                    1,
                ),
                12,
            )
            .unwrap();
        assert_eq!(delta.canonical.len(), 1);
        let next = EventEnvelope::decode(delta.canonical[0].payload.as_slice()).unwrap();
        assert!(matches!(
            next.payload,
            Some(event_envelope::Payload::BookDelta(_))
        ));
        assert_eq!(
            delta.canonical[0].partition_key,
            ready.canonical[0].partition_key
        );
    }

    #[test]
    fn binance_l2_due_materialized_snapshot_keeps_delta_continuity() {
        let binding = binance_book_binding();
        let mut core = core(binding.clone(), true);
        let _ = core
            .process(
                raw(
                    &binding,
                    br#"{"s":"BTCUSDT","U":99,"u":101,"pu":98,"E":1001,"b":[["60000","2"]],"a":[["60001","1"]]}"#,
                    1,
                ),
                10,
            )
            .unwrap();
        let ready = core
            .process(
                with_transport(
                    raw(
                        &binding,
                        br#"{"lastUpdateId":100,"E":1000,"bids":[["60000","1"]],"asks":[["60001","1"]]}"#,
                        1,
                    ),
                    TransportProtocol::Http,
                ),
                11,
            )
            .unwrap();
        assert_eq!(ready.canonical.len(), 1);

        let early = core
            .process(
                raw(
                    &binding,
                    br#"{"s":"BTCUSDT","U":102,"u":102,"pu":101,"E":2000,"b":[["60000","3"]],"a":[]}"#,
                    1,
                ),
                12,
            )
            .unwrap();
        assert_eq!(early.canonical.len(), 1);
        let early_event = EventEnvelope::decode(early.canonical[0].payload.as_slice()).unwrap();
        assert!(matches!(
            early_event.payload,
            Some(event_envelope::Payload::BookDelta(_))
        ));

        let due = core
            .process(
                raw(
                    &binding,
                    br#"{"s":"BTCUSDT","U":103,"u":103,"pu":102,"E":31001,"b":[["60000","4"]],"a":[]}"#,
                    1,
                ),
                13,
            )
            .unwrap();
        assert_eq!(due.canonical.len(), 2);
        let delta = EventEnvelope::decode(due.canonical[0].payload.as_slice()).unwrap();
        let snapshot = EventEnvelope::decode(due.canonical[1].payload.as_slice()).unwrap();
        assert!(matches!(
            delta.payload,
            Some(event_envelope::Payload::BookDelta(_))
        ));
        assert!(matches!(
            snapshot.payload,
            Some(event_envelope::Payload::BookSnapshot(_))
        ));
        assert_eq!(delta.source_sequence, snapshot.source_sequence);
        assert_ne!(delta.event_id, snapshot.event_id);
        assert_eq!(snapshot.partition_sequence, delta.partition_sequence + 1);
    }

    #[test]
    fn binance_l2_hot_materialization_is_independent_of_provider_refresh() {
        let mut binding = binance_book_binding();
        let l2 = binding.l2.as_mut().expect("L2 binding");
        l2.materialized_snapshot_interval_ms = Some(1_000);
        assert_eq!(l2.snapshot_refresh_seconds, 30);
        let mut core = core(binding.clone(), true);

        let buffered = core
            .process(
                raw(
                    &binding,
                    br#"{"s":"BTCUSDT","U":101,"u":101,"E":1000,"b":[["60000","1"]],"a":[]}"#,
                    1,
                ),
                11,
            )
            .unwrap();
        assert!(buffered.canonical.is_empty());

        let initial = core
            .process(
                with_transport(
                    raw(
                        &binding,
                        br#"{"lastUpdateId":100,"E":1000,"bids":[["60000","1"]],"asks":[["60001","1"]]}"#,
                        1,
                    ),
                    TransportProtocol::Http,
                ),
                12,
            )
            .unwrap();
        assert_eq!(initial.canonical.len(), 1);

        let early = core
            .process(
                raw(
                    &binding,
                    br#"{"s":"BTCUSDT","U":102,"u":102,"pu":101,"E":1999,"b":[["60000","2"]],"a":[]}"#,
                    1,
                ),
                13,
            )
            .unwrap();
        assert_eq!(early.canonical.len(), 1);

        let due = core
            .process(
                raw(
                    &binding,
                    br#"{"s":"BTCUSDT","U":103,"u":103,"pu":102,"E":2000,"b":[["60000","3"]],"a":[]}"#,
                    1,
                ),
                14,
            )
            .unwrap();
        assert_eq!(due.canonical.len(), 2);
        let snapshot = EventEnvelope::decode(due.canonical[1].payload.as_slice()).unwrap();
        assert!(matches!(
            snapshot.payload,
            Some(event_envelope::Payload::BookSnapshot(_))
        ));
    }

    #[test]
    fn binance_l2_due_keepalive_materializes_current_verified_view() {
        let binding = binance_book_binding();
        let mut core = core(binding.clone(), true);
        let _ = core
            .process(
                raw(
                    &binding,
                    br#"{"s":"BTCUSDT","U":99,"u":101,"pu":98,"E":1001,"b":[["60000","2"]],"a":[["60001","1"]]}"#,
                    1,
                ),
                10,
            )
            .unwrap();
        let initial = core
            .process(
                with_transport(
                    raw(
                        &binding,
                        br#"{"lastUpdateId":100,"E":1000,"bids":[["60000","1"]],"asks":[["60001","1"]]}"#,
                        1,
                    ),
                    TransportProtocol::Http,
                ),
                11,
            )
            .unwrap();
        let initial_snapshot =
            EventEnvelope::decode(initial.canonical[0].payload.as_slice()).unwrap();

        let updated = core
            .process(
                raw(
                    &binding,
                    br#"{"s":"BTCUSDT","U":102,"u":102,"pu":101,"E":2000,"b":[["60000","3"]],"a":[]}"#,
                    1,
                ),
                12,
            )
            .unwrap();
        assert_eq!(updated.canonical.len(), 1);

        let keepalive = core
            .process(
                with_transport(
                    raw(
                        &binding,
                        br#"{"lastUpdateId":102,"E":31001,"bids":[["1","1"]],"asks":[["2","1"]]}"#,
                        1,
                    ),
                    TransportProtocol::Http,
                ),
                13,
            )
            .unwrap();
        assert_eq!(keepalive.canonical.len(), 1);
        let snapshot = EventEnvelope::decode(keepalive.canonical[0].payload.as_slice()).unwrap();
        let Some(event_envelope::Payload::BookSnapshot(book)) = snapshot.payload else {
            panic!("due Binance keepalive must materialize a verified snapshot");
        };
        assert_eq!(book.native_sequence, "102");
        assert_eq!(book.levels[0].price.as_ref().unwrap().source_text, "60000");
        assert_ne!(initial_snapshot.event_id, snapshot.event_id);
    }

    #[test]
    fn binance_l2_gap_is_quarantined_and_requires_resync() {
        let binding = binance_book_binding();
        let mut core = core(binding.clone(), true);
        let _ = core
            .process(
                with_transport(
                    raw(
                        &binding,
                        br#"{"lastUpdateId":100,"bids":[["60000","1"]],"asks":[["60001","1"]]}"#,
                        1,
                    ),
                    TransportProtocol::Http,
                ),
                10,
            )
            .unwrap();
        let gap = core
            .process(
                raw(
                    &binding,
                    br#"{"s":"BTCUSDT","U":103,"u":103,"pu":100,"E":1003,"b":[],"a":[]}"#,
                    1,
                ),
                11,
            )
            .unwrap();
        assert!(gap.canonical.is_empty());
        assert_eq!(gap.quarantines.len(), 1);
        let evidence = QuarantineRecord::decode(gap.quarantines[0].payload.as_slice()).unwrap();
        assert_eq!(evidence.reason, QuarantineReason::SequenceGap as i32);
    }

    #[test]
    fn stale_l2_raw_is_quarantined_without_redating_and_fresh_snapshot_recovers() {
        let binding = binance_book_binding();
        let mut core = core(binding.clone(), true);
        let stale = with_transport(
            raw(
                &binding,
                br#"{"lastUpdateId":100,"bids":[["60000","1"]],"asks":[["60001","1"]]}"#,
                1,
            ),
            TransportProtocol::Http,
        );
        let processing_at_ns = stale.received_at_ns + 30_000_000_001;
        let rejected = core.process(stale, processing_at_ns).unwrap();
        assert!(rejected.canonical.is_empty());
        assert_eq!(rejected.quarantines.len(), 1);
        let evidence =
            QuarantineRecord::decode(rejected.quarantines[0].payload.as_slice()).unwrap();
        assert_eq!(evidence.reason, QuarantineReason::SemanticInvalid as i32);
        assert_eq!(evidence.quarantined_at_ns, processing_at_ns);
        assert!(evidence.safe_summary.contains("renewal bound"));

        let mut fresh_delta = raw(
            &binding,
            br#"{"s":"BTCUSDT","U":99,"u":101,"pu":98,"E":1001,"b":[["60000","2"]],"a":[["60001","1"]]}"#,
            2,
        );
        fresh_delta.received_at_ns = processing_at_ns;
        let buffered = core.process(fresh_delta, processing_at_ns).unwrap();
        assert!(buffered.canonical.is_empty());

        let mut fresh_snapshot = with_transport(
            raw(
                &binding,
                br#"{"lastUpdateId":100,"bids":[["60000","2"]],"asks":[["60001","1"]]}"#,
                2,
            ),
            TransportProtocol::Http,
        );
        fresh_snapshot.received_at_ns = processing_at_ns;
        let recovered = core.process(fresh_snapshot, processing_at_ns).unwrap();
        assert_eq!(recovered.canonical.len(), 1);
    }

    #[test]
    fn okx_l2_snapshot_then_update_emit_distinct_public_events() {
        let binding = okx_book_binding();
        let mut core = core(binding.clone(), true);
        let snapshot = core
            .process(
                raw(
                    &binding,
                    br#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[{"seqId":"30","prevSeqId":"-1","ts":"10","bids":[["60000","1","0","1"]],"asks":[["60001","1","0","1"]]}]}"#,
                    7,
                ),
                10,
            )
            .unwrap();
        assert_eq!(snapshot.canonical.len(), 1);
        let update = core
            .process(
                raw(
                    &binding,
                    br#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update","data":[{"seqId":"31","prevSeqId":"30","ts":"11","bids":[["60000","2","0","1"]],"asks":[]}]}"#,
                    7,
                ),
                11,
            )
            .unwrap();
        assert_eq!(update.canonical.len(), 1);
        let event = EventEnvelope::decode(update.canonical[0].payload.as_slice()).unwrap();
        assert!(matches!(
            event.payload,
            Some(event_envelope::Payload::BookDelta(_))
        ));
        assert_eq!(
            snapshot.canonical[0].partition_key,
            update.canonical[0].partition_key
        );
    }

    #[test]
    fn okx_l2_due_materialized_snapshot_does_not_drop_update() {
        let binding = okx_book_binding();
        let mut core = core(binding.clone(), true);
        let snapshot = core
            .process(
                raw(
                    &binding,
                    br#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[{"seqId":"30","prevSeqId":"-1","ts":"10","bids":[["60000","1","0","1"]],"asks":[["60001","1","0","1"]]}]}"#,
                    7,
                ),
                10,
            )
            .unwrap();
        assert_eq!(snapshot.canonical.len(), 1);
        let early = core
            .process(
                raw(
                    &binding,
                    br#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update","data":[{"seqId":"31","prevSeqId":"30","ts":"11","bids":[["60000","2","0","1"]],"asks":[]}]}"#,
                    7,
                ),
                11,
            )
            .unwrap();
        assert_eq!(early.canonical.len(), 1);

        let due = core
            .process(
                raw(
                    &binding,
                    br#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update","data":[{"seqId":"32","prevSeqId":"31","ts":"30010","bids":[["60000","3","0","1"]],"asks":[]}]}"#,
                    7,
                ),
                12,
            )
            .unwrap();
        assert_eq!(due.canonical.len(), 2);
        let delta = EventEnvelope::decode(due.canonical[0].payload.as_slice()).unwrap();
        let materialized = EventEnvelope::decode(due.canonical[1].payload.as_slice()).unwrap();
        assert!(matches!(
            delta.payload,
            Some(event_envelope::Payload::BookDelta(_))
        ));
        let Some(event_envelope::Payload::BookSnapshot(book)) = materialized.payload else {
            panic!("due OKX update must materialize a verified snapshot");
        };
        assert_eq!(book.native_sequence, "32");
        assert!(book.sequence_verified);
        assert_eq!(delta.source_sequence, materialized.source_sequence);
        assert_ne!(delta.event_id, materialized.event_id);
    }

    #[test]
    fn exact_duplicate_across_reconnect_is_not_republished() {
        let binding = binding((
            "BINANCE_DIRECT",
            "BINANCE",
            "USDM",
            "PERPETUAL",
            "BTCUSDT",
            "trade",
            "binance_usdm_trade",
            "PRIMARY",
            SequencePolicy::Monotonic,
        ));
        let frame = br#"{"s":"BTCUSDT","t":10,"p":"60000.1","q":"0.01","T":3,"m":false}"#;
        let mut core = core(binding.clone(), true);
        let first = core.process(raw(&binding, frame, 1), 10).unwrap();
        assert_eq!(first.canonical.len(), 1);
        let repeated = core.process(raw(&binding, frame, 2), 11).unwrap();
        assert_eq!(repeated.canonical.len(), 0);
        assert_eq!(repeated.duplicates, 1);
    }

    #[test]
    fn binance_zero_trade_status_is_filtered_but_other_zero_trade_is_quarantined() {
        let binding = binding((
            "BINANCE_DIRECT",
            "BINANCE",
            "USDM",
            "PERPETUAL",
            "BTCUSDT",
            "trade",
            "binance_usdm_trade",
            "PRIMARY",
            SequencePolicy::Monotonic,
        ));
        let status = br#"{"e":"trade","s":"BTCUSDT","t":10,"p":"0","q":"0","T":3,"m":false,"X":"NA","st":1}"#;
        let valid = br#"{"e":"trade","s":"BTCUSDT","t":11,"p":"60000.1","q":"0.01","T":4,"m":false,"X":"MARKET","st":1}"#;
        let malformed = br#"{"e":"trade","s":"BTCUSDT","t":12,"p":"0","q":"0","T":5,"m":false,"X":"MARKET","st":1}"#;
        let mut core = core(binding.clone(), true);

        let filtered = core.process(raw(&binding, status, 1), 10).unwrap();
        assert!(filtered.canonical.is_empty());
        assert!(filtered.quarantines.is_empty());
        assert_eq!(filtered.filtered, 1);

        let accepted = core.process(raw(&binding, valid, 1), 11).unwrap();
        assert_eq!(accepted.canonical.len(), 1);
        assert!(accepted.quarantines.is_empty());
        assert_eq!(accepted.filtered, 0);

        let rejected = core.process(raw(&binding, malformed, 1), 12).unwrap();
        assert!(rejected.canonical.is_empty());
        assert_eq!(rejected.quarantines.len(), 1);
        assert_eq!(rejected.filtered, 0);
        let record = QuarantineRecord::decode(rejected.quarantines[0].payload.as_slice()).unwrap();
        assert_eq!(record.reason, QuarantineReason::SemanticInvalid as i32);
    }

    #[test]
    fn non_positive_trade_is_quarantined_before_canonical_publish() {
        let binding = binding((
            "BINANCE_DIRECT",
            "BINANCE",
            "USDM",
            "PERPETUAL",
            "BTCUSDT",
            "trade",
            "binance_usdm_trade",
            "PRIMARY",
            SequencePolicy::Monotonic,
        ));
        for frame in [
            br#"{"s":"BTCUSDT","t":10,"p":"0","q":"0.01","T":3,"m":false}"#.as_slice(),
            br#"{"s":"BTCUSDT","t":11,"p":"60000.1","q":"-0.01","T":4,"m":false}"#.as_slice(),
        ] {
            let mut core = core(binding.clone(), true);
            let result = core.process(raw(&binding, frame, 1), 10).unwrap();
            assert!(result.canonical.is_empty());
            assert_eq!(result.quarantines.len(), 1);
            let record =
                QuarantineRecord::decode(result.quarantines[0].payload.as_slice()).unwrap();
            assert_eq!(record.reason, QuarantineReason::SemanticInvalid as i32);
        }
    }

    #[test]
    fn transport_replay_is_byte_deterministic_across_fresh_cores() {
        let binding = binding((
            "BINANCE_DIRECT",
            "BINANCE",
            "USDM",
            "PERPETUAL",
            "BTCUSDT",
            "trade",
            "binance_usdm_trade",
            "PRIMARY",
            SequencePolicy::Monotonic,
        ));
        let frame = br#"{"s":"BTCUSDT","t":10,"p":"60000.1","q":"0.01","T":3,"m":false}"#;
        let captured = raw(&binding, frame, 1);
        let mut first_core = core(binding.clone(), true);
        let first = first_core
            .process_at_transport_offset(captured.clone(), 10, 42)
            .unwrap();
        let mut recovered_core = core(binding, true);
        let replay = recovered_core
            .process_at_transport_offset(captured.clone(), 999, 42)
            .unwrap();

        assert_eq!(first, replay);
        let envelope = EventEnvelope::decode(first.canonical[0].payload.as_slice()).unwrap();
        assert_eq!(envelope.received_at_ns, captured.received_at_ns);
        assert_eq!(envelope.normalized_at_ns, captured.received_at_ns);
        assert_eq!(envelope.published_at_ns, captured.received_at_ns);
        assert_eq!(envelope.partition_sequence, 42_000_001);
        assert_eq!(first.canonical[0].accepted_at_ns, captured.received_at_ns);
    }

    #[test]
    fn transport_offset_keeps_restart_and_expanded_row_sequences_monotonic() {
        let binding = binding((
            "OKX_DIRECT",
            "OKX",
            "SWAP",
            "PERPETUAL",
            "BTC-USDT-SWAP",
            "trades",
            "okx_trade",
            "PRIMARY",
            SequencePolicy::Monotonic,
        ));
        let first_frame = br#"{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"1","px":"1","sz":"2","side":"buy","ts":"3"},{"instId":"BTC-USDT-SWAP","tradeId":"2","px":"2","sz":"2","side":"buy","ts":"4"}]}"#;
        let next_frame = br#"{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"3","px":"3","sz":"2","side":"buy","ts":"5"}]}"#;
        let mut first_core = core(binding.clone(), true);
        let first = first_core
            .process_at_transport_offset(raw(&binding, first_frame, 1), 10, 42)
            .unwrap();
        let first_sequences: Vec<u64> = first
            .canonical
            .iter()
            .map(|record| {
                EventEnvelope::decode(record.payload.as_slice())
                    .unwrap()
                    .partition_sequence
            })
            .collect();
        assert_eq!(first_sequences, vec![42_000_001, 42_000_002]);

        let mut restarted_core = core(binding.clone(), true);
        let next = restarted_core
            .process_at_transport_offset(raw(&binding, next_frame, 1), 999, 43)
            .unwrap();
        let next_envelope = EventEnvelope::decode(next.canonical[0].payload.as_slice()).unwrap();
        assert_eq!(next_envelope.partition_sequence, 43_000_001);
        assert!(next_envelope.partition_sequence > first_sequences[1]);

        let mut overflow_core = core(binding.clone(), true);
        let overflow =
            overflow_core.process_at_transport_offset(raw(&binding, next_frame, 1), 1, u64::MAX);
        assert!(matches!(overflow, Err(CoreError::Configuration(_))));
    }

    #[test]
    fn quarantine_replay_is_deterministic_across_processing_clocks() {
        let binding = binding((
            "BINANCE_DIRECT",
            "BINANCE",
            "USDM",
            "PERPETUAL",
            "BTCUSDT",
            "trade",
            "binance_usdm_trade",
            "PRIMARY",
            SequencePolicy::Monotonic,
        ));
        let frame = br#"{"s":"BTCUSDT","t":10,"p":"1","q":"1","T":3,"m":false}"#;
        let mut captured = raw(&binding, frame, 1);
        captured.instrument_catalog_revision += 1;
        let mut first_core = core(binding.clone(), true);
        let first = first_core
            .process_at_transport_offset(captured.clone(), 10, 7)
            .unwrap();
        let mut recovered_core = core(binding, true);
        let replay = recovered_core
            .process_at_transport_offset(captured, 999, 7)
            .unwrap();
        assert_eq!(first, replay);
        assert_eq!(first.quarantines.len(), 1);
    }

    #[test]
    fn final_only_okx_bar_filters_provisional_and_publishes_confirmed() {
        let binding = binding((
            "OKX_DIRECT",
            "OKX",
            "SWAP",
            "PERPETUAL",
            "BTC-USDT-SWAP",
            "candle1m",
            "okx_bar",
            "PRIMARY",
            SequencePolicy::None,
        ));
        assert!(binding.require_final_bar);
        let frame = |confirm: u8| {
            format!(
                r#"{{"arg":{{"channel":"candle1m","instId":"BTC-USDT-SWAP"}},"data":[["1786352340000","61200.00","61240.00","61190.00","61234.10","12.500","12.500","765200.00","{confirm}"]]}}"#
            )
            .into_bytes()
        };
        let mut core = core(binding.clone(), true);
        let provisional = core.process(raw(&binding, &frame(0), 1), 10).unwrap();
        assert!(provisional.canonical.is_empty());
        assert!(provisional.quarantines.is_empty());
        assert_eq!(provisional.filtered, 1);

        let final_bar = core.process(raw(&binding, &frame(1), 1), 11).unwrap();
        assert_eq!(final_bar.canonical.len(), 1);
        assert!(final_bar.quarantines.is_empty());
        assert_eq!(final_bar.filtered, 0);
        let envelope = EventEnvelope::decode(final_bar.canonical[0].payload.as_slice()).unwrap();
        let event_envelope::Payload::Bar(bar) = envelope.payload.unwrap() else {
            panic!("OKX final candle must be BAR")
        };
        assert!(bar.is_final);
    }

    #[test]
    fn stale_generation_and_contiguous_gap_are_quarantined() {
        let binding = binding((
            "OKX_DIRECT",
            "OKX",
            "SWAP",
            "PERPETUAL",
            "BTC-USDT-SWAP",
            "bbo-tbt",
            "okx_bbo",
            "PRIMARY",
            SequencePolicy::Contiguous,
        ));
        let frame = |sequence: u64| {
            format!(
                r#"{{"arg":{{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"}},"data":[{{"bids":[["1","2","0","1"]],"asks":[["2","3","0","1"]],"seqId":{sequence},"ts":"3"}}]}}"#
            )
            .into_bytes()
        };
        let mut core = core(binding.clone(), true);
        assert_eq!(
            core.process(raw(&binding, &frame(10), 2), 10)
                .unwrap()
                .canonical
                .len(),
            1
        );
        let gap = core.process(raw(&binding, &frame(12), 2), 11).unwrap();
        assert_eq!(gap.quarantines.len(), 1);
        let decoded = QuarantineRecord::decode(gap.quarantines[0].payload.as_slice()).unwrap();
        assert_eq!(decoded.reason, 7);
        let stale = core.process(raw(&binding, &frame(11), 1), 12).unwrap();
        assert_eq!(stale.quarantines.len(), 1);
        let decoded = QuarantineRecord::decode(stale.quarantines[0].payload.as_slice()).unwrap();
        assert_eq!(decoded.reason, 5);
    }

    #[test]
    fn dnse_and_vnstock_keep_units_identity_and_source_role() {
        let dnse = binding((
            "DNSE_DIRECT",
            "HNX",
            "VN_DERIVATIVES",
            "FUTURE",
            "VN30F1M",
            "trades",
            "dnse_trade",
            "PRIMARY",
            SequencePolicy::None,
        ));
        let mut dnse_core = core(dnse.clone(), true);
        let event = dnse_core
            .process(
                raw(
                    &dnse,
                    br#"{"symbol":"VN30F1M","price":"1820.7","quantity":"12"}"#,
                    1,
                ),
                10,
            )
            .unwrap();
        let event = EventEnvelope::decode(event.canonical[0].payload.as_slice()).unwrap();
        let event_envelope::Payload::Trade(trade) = event.payload.unwrap() else {
            panic!("DNSE canonical payload must be trade")
        };
        assert_eq!(trade.quantity_unit, QuantityUnit::Contract as i32);
        assert_eq!(
            trade.identity_kind,
            TradeIdentityKind::DerivedRawCapture as i32
        );

        let vnstock = binding((
            "VNSTOCK",
            "HOSE",
            "EQUITIES",
            "COMMON_STOCK",
            "FPT",
            "ohlcv/1m",
            "vnstock_bar",
            "SECONDARY",
            SequencePolicy::None,
        ));
        let mut vnstock_core = core(vnstock.clone(), true);
        let event = vnstock_core
            .process(
                raw(
                    &vnstock,
                    br#"{"symbol":"FPT","interval":"1m","open_time_ms":1,"close_time_ms":2,"o":"1","h":"2","l":"1","c":"2","v":"100","is_final":true,"trade_count_available":false,"revision":0}"#,
                    1,
                ),
                10,
            )
            .unwrap();
        let event = EventEnvelope::decode(event.canonical[0].payload.as_slice()).unwrap();
        assert_eq!(event.source_role, SourceRole::Secondary as i32);
        let event_envelope::Payload::Bar(bar) = event.payload.unwrap() else {
            panic!("VNstock canonical payload must be bar")
        };
        assert_eq!(bar.volume_unit, QuantityUnit::Share as i32);
    }

    #[test]
    fn dnse_rest_and_closed_bar_callback_are_one_deterministic_bar() {
        let binding = binding((
            "DNSE_DIRECT",
            "HNX",
            "VN_DERIVATIVES",
            "FUTURE",
            "VN30F1M",
            "ohlcv/1m",
            "dnse_bar",
            "PRIMARY",
            SequencePolicy::None,
        ));
        let frame = br#"{"symbol":"VN30F1M","interval":"1m","open_time_ms":1786352340000,"close_time_ms":1786352399999,"o":"1820.7","h":"1821.2","l":"1820.2","c":"1820.9","v":"12","is_final":true,"trade_count_available":false,"revision":0}"#;
        let mut rest = raw(&binding, frame, 1);
        rest.transport_protocol = TransportProtocol::Http as i32;
        rest.capture_boundary = CaptureBoundary::PostDecompression as i32;
        let mut core = core(binding.clone(), true);
        let first = core.process(rest, 10).unwrap();
        assert_eq!(first.canonical.len(), 1);
        let event = EventEnvelope::decode(first.canonical[0].payload.as_slice()).unwrap();
        let event_envelope::Payload::Bar(bar) = event.payload.unwrap() else {
            panic!("DNSE canonical payload must be bar")
        };
        assert_eq!(bar.volume_unit, QuantityUnit::Contract as i32);
        assert_eq!(
            bar.lifecycle,
            qdl_contracts::qdl::marketdata::v2::BarLifecycle::Final as i32
        );

        let mut websocket = raw(&binding, frame, 2);
        websocket.transport_protocol = TransportProtocol::SdkCallback as i32;
        websocket.capture_boundary = CaptureBoundary::SdkDelivery as i32;
        let repeated = core.process(websocket, 11).unwrap();
        assert!(repeated.canonical.is_empty());
        assert_eq!(repeated.duplicates, 1);
        assert!(repeated.quarantines.is_empty());
    }

    #[test]
    fn aggregated_provider_frame_is_atomic_on_row_failure() {
        let binding = binding((
            "OKX_DIRECT",
            "OKX",
            "SWAP",
            "PERPETUAL",
            "BTC-USDT-SWAP",
            "trades",
            "okx_trade",
            "PRIMARY",
            SequencePolicy::Monotonic,
        ));
        let invalid = br#"{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"1","px":"1","sz":"2","side":"buy","ts":"3"},{"instId":"BTC-USDT-SWAP","tradeId":"2","sz":"2","side":"buy","ts":"4"}]}"#;
        let mut core = core(binding.clone(), true);
        let rejected = core.process(raw(&binding, invalid, 1), 10).unwrap();
        assert!(rejected.canonical.is_empty());
        assert_eq!(rejected.quarantines.len(), 1);

        let corrected = br#"{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"1","px":"1","sz":"2","side":"buy","ts":"3"},{"instId":"BTC-USDT-SWAP","tradeId":"2","px":"2","sz":"2","side":"buy","ts":"4"}]}"#;
        let accepted = core.process(raw(&binding, corrected, 1), 11).unwrap();
        assert_eq!(accepted.canonical.len(), 2);
        assert!(accepted.quarantines.is_empty());
        assert_eq!(accepted.duplicates, 0);
    }

    #[test]
    fn unknown_binding_and_forbidden_test_provenance_fail_closed() {
        let binding = binding((
            "BINANCE_DIRECT",
            "BINANCE",
            "USDM",
            "PERPETUAL",
            "BTCUSDT",
            "trade",
            "binance_usdm_trade",
            "PRIMARY",
            SequencePolicy::Monotonic,
        ));
        let frame = br#"{"s":"BTCUSDT","t":10,"p":"1","q":"1","T":3,"m":false}"#;
        let mut strict = core(binding.clone(), false);
        assert_eq!(
            strict.process(raw(&binding, frame, 1), 10),
            Err(CoreError::ProvenanceRejected)
        );
        let mut unknown = raw(&binding, frame, 1);
        unknown.native_channel = "unknown".into();
        let mut permissive = core(binding, true);
        assert_eq!(
            permissive.process(unknown, 10),
            Err(CoreError::UnknownBinding)
        );
    }

    #[test]
    fn explicit_ingress_quarantine_preserves_raw_lineage() {
        let binding = binding((
            "BINANCE_DIRECT",
            "BINANCE",
            "USDM",
            "PERPETUAL",
            "BTCUSDT",
            "trade",
            "binance_usdm_trade",
            "PRIMARY",
            SequencePolicy::Monotonic,
        ));
        let raw = raw(
            &binding,
            br#"{"s":"BTCUSDT","t":10,"p":"1","q":"1","T":3,"m":false}"#,
            1,
        );
        let core = core(binding, true);
        let result = core.quarantine_raw(
            &raw,
            QuarantineReason::FencingRejected,
            "undeclared raw subscription in strict scope",
            42,
        );
        assert!(result.canonical.is_empty());
        assert_eq!(result.quarantines.len(), 1);
        let record = QuarantineRecord::decode(result.quarantines[0].payload.as_slice()).unwrap();
        assert_eq!(record.reason, QuarantineReason::FencingRejected as i32);
        assert_eq!(record.quarantined_at_ns, 42);
        assert_eq!(record.raw.unwrap().capture_id, raw.capture_id);
    }

    #[test]
    fn duplicate_source_ids_are_rejected_before_runtime_scope_routing() {
        let first = binding((
            "BINANCE_DIRECT",
            "BINANCE",
            "USDM",
            "PERPETUAL",
            "BTCUSDT",
            "trade",
            "binance_usdm_trade",
            "PRIMARY",
            SequencePolicy::Monotonic,
        ));
        let mut second = binding((
            "BINANCE_DIRECT",
            "BINANCE",
            "USDM",
            "PERPETUAL",
            "BTCUSDT",
            "bookTicker",
            "binance_usdm_bbo",
            "PRIMARY",
            SequencePolicy::Monotonic,
        ));
        second.source_id = first.source_id.clone();

        let result = RealtimeCore::new(RealtimeCoreConfig {
            canonical_stream: "qdl.test.canonical.v2".into(),
            quarantine_stream: "qdl.test.quarantine.v1".into(),
            allow_test_provenance: true,
            dedup_capacity: 16,
            bindings: vec![first, second],
        });

        assert!(matches!(
            result,
            Err(CoreError::Configuration(message)) if message == "duplicate realtime core source ID"
        ));
    }
}
