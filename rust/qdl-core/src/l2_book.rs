//! Provider-neutral, exact-decimal L2 order-book state.
//!
//! This module intentionally owns no socket, provider parser, durable sink or
//! execution decision. Venue adapters select a sequence/checksum policy and
//! pass already scoped snapshot/delta frames into this core. A readable view
//! exists only while a complete, continuous WebSocket book is `Ready`.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::fmt;

const MAX_DECIMAL_DIGITS: usize = 256;
const MAX_ABS_DECIMAL_SCALE: i32 = 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BookError {
    InvalidIdentity(&'static str),
    InvalidDecimal,
    DecimalOutOfBounds,
    NonPositivePrice,
    NegativeQuantity,
    DuplicatePrice,
    InvalidSequenceRange,
    ChecksumRejected,
}

impl fmt::Display for BookError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentity(field) => write!(formatter, "book identity {field} is required"),
            Self::InvalidDecimal => {
                write!(formatter, "book price or quantity is not a finite decimal")
            }
            Self::DecimalOutOfBounds => {
                write!(formatter, "book decimal exceeds bounded core limits")
            }
            Self::NonPositivePrice => write!(formatter, "book price must be positive"),
            Self::NegativeQuantity => write!(formatter, "book quantity must be nonnegative"),
            Self::DuplicatePrice => write!(
                formatter,
                "book frame contains a duplicate side/price update"
            ),
            Self::InvalidSequenceRange => {
                write!(formatter, "book sequence start exceeds sequence end")
            }
            Self::ChecksumRejected => write!(formatter, "book checksum policy rejected the frame"),
        }
    }
}

impl std::error::Error for BookError {}

/// A normalized finite decimal that never uses a binary float key.
///
/// `digits * 10^-scale` is the exact value. `digits` is normalized without
/// leading zeroes, and trailing zeroes are removed across positive, zero and
/// negative scales, so equal numeric values have exactly one key representation.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ExactDecimal {
    sign: i8,
    digits: String,
    scale: i32,
}

impl ExactDecimal {
    pub fn parse(source: &str) -> Result<Self, BookError> {
        let source = source.trim();
        if source.is_empty() {
            return Err(BookError::InvalidDecimal);
        }

        let (negative, unsigned) = match source.as_bytes()[0] {
            b'-' => (true, &source[1..]),
            b'+' => (false, &source[1..]),
            _ => (false, source),
        };
        if unsigned.is_empty() {
            return Err(BookError::InvalidDecimal);
        }

        let (base, exponent) = match unsigned.split_once(['e', 'E']) {
            Some((base, exponent)) => (
                base,
                exponent
                    .parse::<i32>()
                    .map_err(|_| BookError::InvalidDecimal)?,
            ),
            None => (unsigned, 0),
        };
        let (whole, fraction) = base.split_once('.').unwrap_or((base, ""));
        if whole.is_empty() && fraction.is_empty() {
            return Err(BookError::InvalidDecimal);
        }
        if !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
        {
            return Err(BookError::InvalidDecimal);
        }

        let mut digits = format!("{whole}{fraction}");
        if digits.len() > MAX_DECIMAL_DIGITS {
            return Err(BookError::DecimalOutOfBounds);
        }
        let raw_scale = i64::try_from(fraction.len()).unwrap_or(i64::MAX) - i64::from(exponent);
        if raw_scale.unsigned_abs() > MAX_ABS_DECIMAL_SCALE as u64 {
            return Err(BookError::DecimalOutOfBounds);
        }
        let mut scale = raw_scale as i32;

        let trimmed = digits.trim_start_matches('0');
        if trimmed.is_empty() {
            return Ok(Self {
                sign: 0,
                digits: "0".to_owned(),
                scale: 0,
            });
        }
        digits = trimmed.to_owned();
        while digits.ends_with('0') {
            digits.pop();
            scale -= 1;
        }
        if scale.unsigned_abs() > MAX_ABS_DECIMAL_SCALE as u32 {
            return Err(BookError::DecimalOutOfBounds);
        }

        Ok(Self {
            sign: if negative { -1 } else { 1 },
            digits,
            scale,
        })
    }

    pub fn is_zero(&self) -> bool {
        self.sign == 0
    }

    pub fn is_positive(&self) -> bool {
        self.sign > 0
    }

    pub fn canonical_text(&self) -> String {
        self.to_string()
    }

    fn integer_digits(&self) -> i64 {
        self.digits.len() as i64 - i64::from(self.scale)
    }

    fn compare_magnitude(&self, other: &Self) -> Ordering {
        match self.integer_digits().cmp(&other.integer_digits()) {
            Ordering::Equal => {
                let target_scale = self.scale.max(other.scale);
                let left_padding = usize::try_from(target_scale - self.scale).unwrap_or_default();
                let right_padding = usize::try_from(target_scale - other.scale).unwrap_or_default();
                let mut left = self.digits.clone();
                let mut right = other.digits.clone();
                left.push_str(&"0".repeat(left_padding));
                right.push_str(&"0".repeat(right_padding));
                left.cmp(&right)
            }
            ordering => ordering,
        }
    }
}

impl fmt::Display for ExactDecimal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.sign == 0 {
            return formatter.write_str("0");
        }
        if self.sign < 0 {
            formatter.write_str("-")?;
        }
        if self.scale <= 0 {
            formatter.write_str(&self.digits)?;
            return formatter.write_str(&"0".repeat((-self.scale) as usize));
        }

        let scale = self.scale as usize;
        if self.digits.len() <= scale {
            formatter.write_str("0.")?;
            formatter.write_str(&"0".repeat(scale - self.digits.len()))?;
            return formatter.write_str(&self.digits);
        }
        let split = self.digits.len() - scale;
        formatter.write_str(&self.digits[..split])?;
        formatter.write_str(".")?;
        formatter.write_str(&self.digits[split..])
    }
}

impl Ord for ExactDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.sign.cmp(&other.sign) {
            Ordering::Equal if self.sign == 0 => Ordering::Equal,
            Ordering::Equal if self.sign > 0 => self.compare_magnitude(other),
            Ordering::Equal => other.compare_magnitude(self),
            ordering => ordering,
        }
    }
}

impl PartialOrd for ExactDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BookIdentity {
    pub provider_profile: String,
    pub instrument_uid: String,
    pub channel: String,
}

impl BookIdentity {
    pub fn new(
        provider_profile: impl Into<String>,
        instrument_uid: impl Into<String>,
        channel: impl Into<String>,
    ) -> Result<Self, BookError> {
        let identity = Self {
            provider_profile: provider_profile.into(),
            instrument_uid: instrument_uid.into(),
            channel: channel.into(),
        };
        for (field, value) in [
            ("provider_profile", &identity.provider_profile),
            ("instrument_uid", &identity.instrument_uid),
            ("channel", &identity.channel),
        ] {
            if value.trim().is_empty() {
                return Err(BookError::InvalidIdentity(field));
            }
        }
        Ok(identity)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum BookSide {
    Bid,
    Ask,
}

impl BookSide {
    pub fn parse(source: &str) -> Result<Self, BookError> {
        match source {
            "BID" => Ok(Self::Bid),
            "ASK" => Ok(Self::Ask),
            _ => Err(BookError::InvalidDecimal),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookLevelInput {
    pub side: BookSide,
    pub price: String,
    pub quantity: String,
    pub order_count: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SequencePolicy {
    /// Stateful channels such as OKX `books`: `previous == last` is required.
    PreviousSequence,
    /// Diff-depth channels such as Binance: bridge the first delta range after
    /// a snapshot, then require `previous == last`.
    RangeBridgeThenPrevious,
    /// Replace-only channels such as BBO/`books5`; deltas are unsupported.
    SnapshotOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChecksumPolicy {
    Ignore,
    VerifyIfPresent,
    RequireVerified,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChecksumEvidence {
    NotProvided,
    Verified,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotOrigin {
    WebSocket,
    Rest,
}

/// Admission rule for the authoritative snapshot that seeds a book
/// generation.  The generic core deliberately does not infer this from a
/// venue name: an adapter selects the documented provider rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotAdmissionPolicy {
    /// Stateful websocket channels (for example OKX `books`) must establish
    /// continuity from their own websocket snapshot.
    WebSocketOnly,
    /// Diff-depth protocols may bootstrap from a REST sequence anchor, but
    /// remain unreadable until the first websocket range proves continuity.
    RestBootstrapAllowed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookConfig {
    pub identity: BookIdentity,
    pub sequence_policy: SequencePolicy,
    pub checksum_policy: ChecksumPolicy,
    pub view_depth_per_side: usize,
    pub snapshot_admission_policy: SnapshotAdmissionPolicy,
}

impl BookConfig {
    pub fn new(
        identity: BookIdentity,
        sequence_policy: SequencePolicy,
        checksum_policy: ChecksumPolicy,
        view_depth_per_side: usize,
    ) -> Result<Self, BookError> {
        if view_depth_per_side == 0 {
            return Err(BookError::InvalidIdentity("view_depth_per_side"));
        }
        Ok(Self {
            identity,
            sequence_policy,
            checksum_policy,
            view_depth_per_side,
            snapshot_admission_policy: SnapshotAdmissionPolicy::WebSocketOnly,
        })
    }

    /// Opt into the only non-websocket snapshot shape currently supported by
    /// the core.  It is valid solely for range-bridged diff-depth protocols;
    /// a REST snapshot never becomes a readable book by itself.
    pub fn with_snapshot_admission_policy(
        mut self,
        snapshot_admission_policy: SnapshotAdmissionPolicy,
    ) -> Result<Self, BookError> {
        if snapshot_admission_policy == SnapshotAdmissionPolicy::RestBootstrapAllowed
            && self.sequence_policy != SequencePolicy::RangeBridgeThenPrevious
        {
            return Err(BookError::InvalidIdentity(
                "rest_bootstrap_requires_range_bridge_policy",
            ));
        }
        self.snapshot_admission_policy = snapshot_admission_policy;
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookSnapshot {
    pub identity: BookIdentity,
    pub generation: u64,
    pub sequence_end: u64,
    pub checksum: ChecksumEvidence,
    pub origin: SnapshotOrigin,
    pub levels: Vec<BookLevelInput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookDelta {
    pub identity: BookIdentity,
    pub generation: u64,
    pub sequence_start: Option<u64>,
    pub previous_sequence: Option<u64>,
    pub sequence_end: u64,
    pub checksum: ChecksumEvidence,
    pub updates: Vec<BookLevelInput>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookStatus {
    AwaitingSnapshot,
    /// A REST bootstrap anchor is loaded, but no websocket delta has yet
    /// proven the required sequence bridge. `view()` remains fail-closed.
    Bootstrapping,
    Ready,
    Gapped,
    Resyncing,
    Disconnected,
}

impl BookStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingSnapshot => "AWAITING_SNAPSHOT",
            Self::Bootstrapping => "BOOTSTRAPPING",
            Self::Ready => "READY",
            Self::Gapped => "GAPPED",
            Self::Resyncing => "RESYNCING",
            Self::Disconnected => "DISCONNECTED",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookOutcome {
    SnapshotApplied,
    BootstrapApplied,
    DeltaApplied,
    Keepalive,
    Duplicate,
    OutOfOrder,
    SequenceGap,
    ChecksumRejected,
    InvalidFrame,
    IdentityMismatch,
    IgnoredStaleGeneration,
    RejectedAwaitingSnapshot,
    /// Adapter-owned race buffering retained the delta before an authoritative
    /// snapshot exists. It is not a readable or durable book transition.
    BufferedAwaitingBootstrap,
    SnapshotSourceRejected,
    DeltaUnsupported,
    ResyncRequested,
    Disconnected,
}

impl BookOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SnapshotApplied => "SNAPSHOT_APPLIED",
            Self::BootstrapApplied => "BOOTSTRAP_APPLIED",
            Self::DeltaApplied => "DELTA_APPLIED",
            Self::Keepalive => "KEEPALIVE",
            Self::Duplicate => "DUPLICATE",
            Self::OutOfOrder => "OUT_OF_ORDER",
            Self::SequenceGap => "SEQUENCE_GAP",
            Self::ChecksumRejected => "CHECKSUM_REJECTED",
            Self::InvalidFrame => "INVALID_FRAME",
            Self::IdentityMismatch => "IDENTITY_MISMATCH",
            Self::IgnoredStaleGeneration => "IGNORED_STALE_GENERATION",
            Self::RejectedAwaitingSnapshot => "REJECTED_AWAITING_SNAPSHOT",
            Self::BufferedAwaitingBootstrap => "BUFFERED_AWAITING_BOOTSTRAP",
            Self::SnapshotSourceRejected => "SNAPSHOT_SOURCE_REJECTED",
            Self::DeltaUnsupported => "DELTA_UNSUPPORTED",
            Self::ResyncRequested => "RESYNC_REQUESTED",
            Self::Disconnected => "DISCONNECTED",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BookLevel {
    side: BookSide,
    price: ExactDecimal,
    quantity: ExactDecimal,
    order_count: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookViewLevel {
    pub side: BookSide,
    pub price: ExactDecimal,
    pub quantity: ExactDecimal,
    pub order_count: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookView {
    pub identity: BookIdentity,
    pub generation: u64,
    pub snapshot_sequence: u64,
    pub last_sequence: u64,
    pub bids: Vec<BookViewLevel>,
    pub asks: Vec<BookViewLevel>,
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Continuity {
    Apply,
    Keepalive,
    Duplicate,
    OutOfOrder,
    Gap,
    Unsupported,
}

/// The state is deliberately local to one complete provider profile,
/// instrument, channel and source generation. It does not open or repair a
/// provider connection; an adapter reacts to `Gapped`/`Resyncing` outcomes.
#[derive(Debug)]
pub struct L2BookCore {
    config: BookConfig,
    generation: u64,
    status: BookStatus,
    snapshot_sequence: Option<u64>,
    last_sequence: Option<u64>,
    range_bridge_complete: bool,
    bids: BTreeMap<ExactDecimal, BookLevel>,
    asks: BTreeMap<ExactDecimal, BookLevel>,
    last_error: Option<BookError>,
}

impl L2BookCore {
    pub fn new(config: BookConfig) -> Self {
        Self {
            config,
            generation: 0,
            status: BookStatus::AwaitingSnapshot,
            snapshot_sequence: None,
            last_sequence: None,
            range_bridge_complete: false,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_error: None,
        }
    }

    pub fn status(&self) -> BookStatus {
        self.status
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn last_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    /// The provider sequence of the snapshot that anchors the current
    /// generation.  It remains stable across valid deltas and is cleared on
    /// every gap, resync, disconnect or generation change.
    pub fn snapshot_sequence(&self) -> Option<u64> {
        self.snapshot_sequence
    }

    pub fn last_error(&self) -> Option<&BookError> {
        self.last_error.as_ref()
    }

    pub fn apply_snapshot(&mut self, frame: &BookSnapshot) -> BookOutcome {
        if frame.identity != self.config.identity {
            return BookOutcome::IdentityMismatch;
        }
        if !self.snapshot_origin_accepted(frame.origin) {
            return BookOutcome::SnapshotSourceRejected;
        }
        if !self.accept_generation(frame.generation) {
            return BookOutcome::IgnoredStaleGeneration;
        }
        if !self.checksum_accepted(frame.checksum) {
            self.invalidate(BookStatus::Gapped, BookError::ChecksumRejected);
            return BookOutcome::ChecksumRejected;
        }
        let (bids, asks) = match parse_snapshot_levels(&frame.levels) {
            Ok(value) => value,
            Err(error) => {
                self.invalidate(BookStatus::AwaitingSnapshot, error);
                return BookOutcome::InvalidFrame;
            }
        };

        self.bids = bids;
        self.asks = asks;
        self.snapshot_sequence = Some(frame.sequence_end);
        self.last_sequence = Some(frame.sequence_end);
        self.range_bridge_complete = false;
        let is_rest_bootstrap = frame.origin == SnapshotOrigin::Rest;
        self.status = if is_rest_bootstrap {
            BookStatus::Bootstrapping
        } else {
            BookStatus::Ready
        };
        self.last_error = None;
        if is_rest_bootstrap {
            BookOutcome::BootstrapApplied
        } else {
            BookOutcome::SnapshotApplied
        }
    }

    pub fn apply_delta(&mut self, frame: &BookDelta) -> BookOutcome {
        if frame.identity != self.config.identity {
            return BookOutcome::IdentityMismatch;
        }
        if !self.accept_generation(frame.generation) {
            return BookOutcome::IgnoredStaleGeneration;
        }
        if self.config.sequence_policy == SequencePolicy::SnapshotOnly {
            return BookOutcome::DeltaUnsupported;
        }
        if !matches!(self.status, BookStatus::Ready | BookStatus::Bootstrapping)
            || self.last_sequence.is_none()
        {
            return BookOutcome::RejectedAwaitingSnapshot;
        }

        match self.continuity(frame) {
            Continuity::Keepalive => return BookOutcome::Keepalive,
            Continuity::Duplicate => return BookOutcome::Duplicate,
            Continuity::OutOfOrder => {
                self.invalidate(BookStatus::Gapped, BookError::InvalidSequenceRange);
                return BookOutcome::OutOfOrder;
            }
            Continuity::Gap => {
                self.invalidate(BookStatus::Gapped, BookError::InvalidSequenceRange);
                return BookOutcome::SequenceGap;
            }
            Continuity::Unsupported => return BookOutcome::DeltaUnsupported,
            Continuity::Apply => {}
        }
        if !self.checksum_accepted(frame.checksum) {
            self.invalidate(BookStatus::Gapped, BookError::ChecksumRejected);
            return BookOutcome::ChecksumRejected;
        }
        let updates = match parse_delta_levels(&frame.updates) {
            Ok(value) => value,
            Err(error) => {
                self.invalidate(BookStatus::Gapped, error);
                return BookOutcome::InvalidFrame;
            }
        };

        // Parse all updates before cloning/committing either side. A consumer
        // can never observe a bid mutation without its matching ask mutation.
        let mut next_bids = self.bids.clone();
        let mut next_asks = self.asks.clone();
        apply_updates(&mut next_bids, &mut next_asks, updates);
        self.bids = next_bids;
        self.asks = next_asks;
        self.last_sequence = Some(frame.sequence_end);
        self.range_bridge_complete = self.range_bridge_complete
            || self.config.sequence_policy == SequencePolicy::RangeBridgeThenPrevious;
        self.status = BookStatus::Ready;
        self.last_error = None;
        BookOutcome::DeltaApplied
    }

    /// Request a transport-level resubscribe. The adapter supplies the next
    /// provider-valid snapshot: websocket-only for stateful protocols, or a
    /// new REST bootstrap plus websocket bridge for diff-depth protocols.
    pub fn request_resync(&mut self, generation: u64) -> BookOutcome {
        if generation < self.generation {
            return BookOutcome::IgnoredStaleGeneration;
        }
        self.generation = generation;
        self.clear_book();
        self.status = BookStatus::Resyncing;
        self.last_error = None;
        BookOutcome::ResyncRequested
    }

    pub fn disconnect(&mut self) -> BookOutcome {
        self.clear_book();
        self.status = BookStatus::Disconnected;
        self.last_error = None;
        BookOutcome::Disconnected
    }

    /// `None` is the fail-closed representation for every non-ready state.
    pub fn view(&self) -> Option<BookView> {
        if self.status != BookStatus::Ready {
            return None;
        }
        let snapshot_sequence = self.snapshot_sequence?;
        let last_sequence = self.last_sequence?;
        let truncated = self.bids.len() > self.config.view_depth_per_side
            || self.asks.len() > self.config.view_depth_per_side;
        Some(BookView {
            identity: self.config.identity.clone(),
            generation: self.generation,
            snapshot_sequence,
            last_sequence,
            bids: self
                .bids
                .values()
                .rev()
                .take(self.config.view_depth_per_side)
                .map(to_view_level)
                .collect(),
            asks: self
                .asks
                .values()
                .take(self.config.view_depth_per_side)
                .map(to_view_level)
                .collect(),
            truncated,
        })
    }

    fn accept_generation(&mut self, incoming: u64) -> bool {
        if incoming < self.generation {
            return false;
        }
        if incoming > self.generation {
            self.generation = incoming;
            self.clear_book();
            self.status = BookStatus::AwaitingSnapshot;
            self.last_error = None;
        }
        true
    }

    fn checksum_accepted(&self, evidence: ChecksumEvidence) -> bool {
        match self.config.checksum_policy {
            ChecksumPolicy::Ignore => true,
            ChecksumPolicy::VerifyIfPresent => evidence != ChecksumEvidence::Failed,
            ChecksumPolicy::RequireVerified => evidence == ChecksumEvidence::Verified,
        }
    }

    fn snapshot_origin_accepted(&self, origin: SnapshotOrigin) -> bool {
        match origin {
            SnapshotOrigin::WebSocket => true,
            SnapshotOrigin::Rest => {
                self.config.snapshot_admission_policy
                    == SnapshotAdmissionPolicy::RestBootstrapAllowed
                    && self.config.sequence_policy == SequencePolicy::RangeBridgeThenPrevious
            }
        }
    }

    fn continuity(&self, frame: &BookDelta) -> Continuity {
        let last = match self.last_sequence {
            Some(value) => value,
            None => return Continuity::Gap,
        };
        match self.config.sequence_policy {
            SequencePolicy::SnapshotOnly => Continuity::Unsupported,
            SequencePolicy::PreviousSequence => {
                let previous = match frame.previous_sequence {
                    Some(value) => value,
                    None => return Continuity::Gap,
                };
                if previous == last {
                    if frame.sequence_end == last {
                        if frame.updates.is_empty() {
                            Continuity::Keepalive
                        } else {
                            Continuity::Duplicate
                        }
                    } else {
                        // OKX permits a maintenance reset with a lower seqId as
                        // long as prevSeqId still chains to the prior sequence.
                        Continuity::Apply
                    }
                } else if frame.sequence_end <= last {
                    Continuity::Duplicate
                } else if previous < last {
                    Continuity::OutOfOrder
                } else {
                    Continuity::Gap
                }
            }
            SequencePolicy::RangeBridgeThenPrevious => {
                let start = match frame.sequence_start {
                    Some(value) if value <= frame.sequence_end => value,
                    _ => return Continuity::Gap,
                };
                if frame.sequence_end <= last {
                    return Continuity::Duplicate;
                }
                if !self.range_bridge_complete {
                    let expected = match last.checked_add(1) {
                        Some(value) => value,
                        None => return Continuity::Gap,
                    };
                    if start <= expected && frame.sequence_end >= expected {
                        Continuity::Apply
                    } else if start > expected {
                        Continuity::Gap
                    } else {
                        Continuity::OutOfOrder
                    }
                } else {
                    match frame.previous_sequence {
                        Some(previous) if previous == last => Continuity::Apply,
                        Some(previous) if previous < last => Continuity::OutOfOrder,
                        _ => Continuity::Gap,
                    }
                }
            }
        }
    }

    fn clear_book(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.snapshot_sequence = None;
        self.last_sequence = None;
        self.range_bridge_complete = false;
    }

    fn invalidate(&mut self, status: BookStatus, error: BookError) {
        self.clear_book();
        self.status = status;
        self.last_error = Some(error);
    }
}

fn parse_snapshot_levels(
    inputs: &[BookLevelInput],
) -> Result<
    (
        BTreeMap<ExactDecimal, BookLevel>,
        BTreeMap<ExactDecimal, BookLevel>,
    ),
    BookError,
> {
    let levels = parse_levels(inputs)?;
    let mut bids = BTreeMap::new();
    let mut asks = BTreeMap::new();
    let mut seen = HashSet::new();
    for level in levels {
        if !seen.insert((level.side, level.price.clone())) {
            return Err(BookError::DuplicatePrice);
        }
        // A zero quantity is never an active level, including in a provider
        // snapshot. Keep the explicit semantic without leaking a zero level
        // into a readable view.
        if level.quantity.is_zero() {
            continue;
        }
        let target = match level.side {
            BookSide::Bid => &mut bids,
            BookSide::Ask => &mut asks,
        };
        target.insert(level.price.clone(), level);
    }
    Ok((bids, asks))
}

fn parse_delta_levels(inputs: &[BookLevelInput]) -> Result<Vec<BookLevel>, BookError> {
    let levels = parse_levels(inputs)?;
    let mut seen = HashSet::new();
    for level in &levels {
        if !seen.insert((level.side, level.price.clone())) {
            return Err(BookError::DuplicatePrice);
        }
    }
    Ok(levels)
}

fn parse_levels(inputs: &[BookLevelInput]) -> Result<Vec<BookLevel>, BookError> {
    inputs
        .iter()
        .map(|input| {
            let price = ExactDecimal::parse(&input.price)?;
            let quantity = ExactDecimal::parse(&input.quantity)?;
            if !price.is_positive() {
                return Err(BookError::NonPositivePrice);
            }
            if quantity.sign < 0 {
                return Err(BookError::NegativeQuantity);
            }
            Ok(BookLevel {
                side: input.side,
                price,
                quantity,
                order_count: input.order_count,
            })
        })
        .collect()
}

fn apply_updates(
    bids: &mut BTreeMap<ExactDecimal, BookLevel>,
    asks: &mut BTreeMap<ExactDecimal, BookLevel>,
    updates: Vec<BookLevel>,
) {
    for level in updates {
        let target = match level.side {
            BookSide::Bid => &mut *bids,
            BookSide::Ask => &mut *asks,
        };
        if level.quantity.is_zero() {
            target.remove(&level.price);
        } else {
            target.insert(level.price.clone(), level);
        }
    }
}

fn to_view_level(level: &BookLevel) -> BookViewLevel {
    BookViewLevel {
        side: level.side,
        price: level.price.clone(),
        quantity: level.quantity.clone(),
        order_count: level.order_count,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BookConfig, BookDelta, BookIdentity, BookLevelInput, BookOutcome, BookSide, BookSnapshot,
        ChecksumEvidence, ChecksumPolicy, ExactDecimal, L2BookCore, SequencePolicy,
        SnapshotAdmissionPolicy, SnapshotOrigin,
    };
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Fixture {
        schema_version: u32,
        provenance: String,
        cases: Vec<FixtureCase>,
    }

    #[derive(Deserialize)]
    struct FixtureCase {
        name: String,
        config: FixtureConfig,
        actions: Vec<FixtureAction>,
    }

    #[derive(Deserialize)]
    struct FixtureConfig {
        key: FixtureKey,
        sequence_policy: String,
        checksum_policy: String,
        view_depth_per_side: usize,
        #[serde(default)]
        snapshot_admission_policy: Option<String>,
    }

    #[derive(Clone, Deserialize)]
    struct FixtureKey {
        provider_profile: String,
        instrument_uid: String,
        channel: String,
    }

    #[derive(Deserialize)]
    struct FixtureAction {
        kind: String,
        #[serde(default)]
        identity: Option<FixtureKey>,
        #[serde(default)]
        generation: Option<u64>,
        #[serde(default)]
        sequence_start: Option<u64>,
        #[serde(default)]
        previous_sequence: Option<u64>,
        #[serde(default)]
        sequence_end: Option<u64>,
        #[serde(default)]
        checksum: Option<String>,
        #[serde(default)]
        origin: Option<String>,
        #[serde(default)]
        levels: Vec<FixtureLevel>,
        expect: FixtureExpectation,
    }

    #[derive(Deserialize)]
    struct FixtureLevel {
        side: String,
        price: String,
        quantity: String,
        #[serde(default)]
        order_count: Option<u64>,
    }

    #[derive(Deserialize)]
    struct FixtureExpectation {
        outcome: String,
        status: String,
        generation: u64,
        last_sequence: Option<u64>,
        ready: bool,
        #[serde(default)]
        bids: Vec<Vec<String>>,
        #[serde(default)]
        asks: Vec<Vec<String>>,
        #[serde(default)]
        truncated: bool,
    }

    fn identity(value: &FixtureKey) -> BookIdentity {
        BookIdentity::new(
            value.provider_profile.clone(),
            value.instrument_uid.clone(),
            value.channel.clone(),
        )
        .unwrap()
    }

    fn policy(value: &str) -> SequencePolicy {
        match value {
            "PREVIOUS_SEQUENCE" => SequencePolicy::PreviousSequence,
            "RANGE_BRIDGE_THEN_PREVIOUS" => SequencePolicy::RangeBridgeThenPrevious,
            "SNAPSHOT_ONLY" => SequencePolicy::SnapshotOnly,
            other => panic!("unknown sequence policy {other}"),
        }
    }

    fn checksum_policy(value: &str) -> ChecksumPolicy {
        match value {
            "IGNORE" => ChecksumPolicy::Ignore,
            "VERIFY_IF_PRESENT" => ChecksumPolicy::VerifyIfPresent,
            "REQUIRE_VERIFIED" => ChecksumPolicy::RequireVerified,
            other => panic!("unknown checksum policy {other}"),
        }
    }

    fn checksum(value: Option<&str>) -> ChecksumEvidence {
        match value.unwrap_or("NOT_PROVIDED") {
            "NOT_PROVIDED" => ChecksumEvidence::NotProvided,
            "VERIFIED" => ChecksumEvidence::Verified,
            "FAILED" => ChecksumEvidence::Failed,
            other => panic!("unknown checksum evidence {other}"),
        }
    }

    fn origin(value: Option<&str>) -> SnapshotOrigin {
        match value.unwrap_or("WEBSOCKET") {
            "WEBSOCKET" => SnapshotOrigin::WebSocket,
            "REST" => SnapshotOrigin::Rest,
            other => panic!("unknown snapshot origin {other}"),
        }
    }

    fn snapshot_admission_policy(value: Option<&str>) -> SnapshotAdmissionPolicy {
        match value.unwrap_or("WEBSOCKET_ONLY") {
            "WEBSOCKET_ONLY" => SnapshotAdmissionPolicy::WebSocketOnly,
            "REST_BOOTSTRAP_ALLOWED" => SnapshotAdmissionPolicy::RestBootstrapAllowed,
            other => panic!("unknown snapshot admission policy {other}"),
        }
    }

    fn levels(values: &[FixtureLevel]) -> Vec<BookLevelInput> {
        values
            .iter()
            .map(|value| BookLevelInput {
                side: BookSide::parse(&value.side).unwrap(),
                price: value.price.clone(),
                quantity: value.quantity.clone(),
                order_count: value.order_count,
            })
            .collect()
    }

    fn action_identity(action: &FixtureAction, fallback: &FixtureKey) -> BookIdentity {
        identity(action.identity.as_ref().unwrap_or(fallback))
    }

    fn apply(core: &mut L2BookCore, action: &FixtureAction, fallback: &FixtureKey) -> BookOutcome {
        match action.kind.as_str() {
            "snapshot" => core.apply_snapshot(&BookSnapshot {
                identity: action_identity(action, fallback),
                generation: action.generation.unwrap(),
                sequence_end: action.sequence_end.unwrap(),
                checksum: checksum(action.checksum.as_deref()),
                origin: origin(action.origin.as_deref()),
                levels: levels(&action.levels),
            }),
            "delta" => core.apply_delta(&BookDelta {
                identity: action_identity(action, fallback),
                generation: action.generation.unwrap(),
                sequence_start: action.sequence_start,
                previous_sequence: action.previous_sequence,
                sequence_end: action.sequence_end.unwrap(),
                checksum: checksum(action.checksum.as_deref()),
                updates: levels(&action.levels),
            }),
            "request_resync" => core.request_resync(action.generation.unwrap()),
            "disconnect" => core.disconnect(),
            other => panic!("unknown fixture action {other}"),
        }
    }

    fn rows(levels: impl IntoIterator<Item = super::BookViewLevel>) -> Vec<Vec<String>> {
        levels
            .into_iter()
            .map(|level| {
                vec![
                    level.price.canonical_text(),
                    level.quantity.canonical_text(),
                ]
            })
            .collect()
    }

    #[test]
    fn shared_fixture_is_exactly_conformant_and_fail_closed() {
        let path = format!(
            "{}/../../tests/fixtures/phase104/l2_book_state_machine.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let fixture: Fixture = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(fixture.schema_version, 1);
        assert_eq!(fixture.provenance, "TEST_ONLY_SYNTHETIC_PROTOCOL_FIXTURE");

        for case in fixture.cases {
            let config = BookConfig::new(
                identity(&case.config.key),
                policy(&case.config.sequence_policy),
                checksum_policy(&case.config.checksum_policy),
                case.config.view_depth_per_side,
            )
            .unwrap()
            .with_snapshot_admission_policy(snapshot_admission_policy(
                case.config.snapshot_admission_policy.as_deref(),
            ))
            .unwrap();
            let mut core = L2BookCore::new(config);
            for action in &case.actions {
                let outcome = apply(&mut core, action, &case.config.key);
                let expected = &action.expect;
                assert_eq!(
                    outcome.as_str(),
                    expected.outcome,
                    "{} / {} outcome",
                    case.name,
                    action.kind
                );
                assert_eq!(
                    core.status().as_str(),
                    expected.status,
                    "{} / {} status",
                    case.name,
                    action.kind
                );
                assert_eq!(
                    core.generation(),
                    expected.generation,
                    "{} generation",
                    case.name
                );
                assert_eq!(
                    core.last_sequence(),
                    expected.last_sequence,
                    "{} / {} sequence",
                    case.name,
                    action.kind
                );
                assert_eq!(core.view().is_some(), expected.ready, "{} ready", case.name);
                if let Some(view) = core.view() {
                    assert_eq!(rows(view.bids), expected.bids, "{} bid view", case.name);
                    assert_eq!(rows(view.asks), expected.asks, "{} ask view", case.name);
                    assert_eq!(
                        view.truncated, expected.truncated,
                        "{} truncation",
                        case.name
                    );
                }
            }
        }
    }

    #[test]
    fn exact_decimal_is_float_free_normalized_and_ordered() {
        let one = ExactDecimal::parse("1.000").unwrap();
        let exponent = ExactDecimal::parse("1e0").unwrap();
        let whole_and_exponent = ExactDecimal::parse("1000").unwrap();
        let exponent_equivalent = ExactDecimal::parse("1e3").unwrap();
        let small = ExactDecimal::parse("0.00000001").unwrap();
        let high_precision = ExactDecimal::parse("12345678901234567890.123456789").unwrap();
        assert_eq!(one, exponent);
        assert_eq!(one.canonical_text(), "1");
        assert_eq!(whole_and_exponent, exponent_equivalent);
        assert_eq!(whole_and_exponent.canonical_text(), "1000");
        assert!(small < one);
        assert_eq!(
            high_precision.canonical_text(),
            "12345678901234567890.123456789"
        );
        assert!(ExactDecimal::parse("NaN").is_err());
        assert!(ExactDecimal::parse("Infinity").is_err());
        assert!(ExactDecimal::parse("1_0").is_err());
    }
}
