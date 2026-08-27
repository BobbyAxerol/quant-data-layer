use std::collections::{BTreeMap, BTreeSet};

use qdl_contracts::qdl::demand::v1::{
    universe_selector, warmup_specification, DataRequirement, DemandPurpose, DemandState, FeedType,
    IntervalSourcePolicy,
};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DemandSubscription {
    pub venue: String,
    pub market: String,
    pub feed: FeedType,
    pub native_symbol: String,
    pub interval: Option<String>,
    pub priority: u32,
}

impl DemandSubscription {
    pub fn new(
        venue: impl Into<String>,
        market: impl Into<String>,
        feed: FeedType,
        native_symbol: impl Into<String>,
        interval: Option<String>,
        priority: u32,
    ) -> Result<Self, String> {
        let result = Self {
            venue: venue.into().trim().to_ascii_uppercase(),
            market: market.into().trim().to_ascii_uppercase(),
            feed,
            native_symbol: native_symbol.into().trim().to_ascii_uppercase(),
            interval: interval
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty()),
            priority,
        };
        result.validate()?;
        Ok(result)
    }

    pub fn key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.venue,
            self.market,
            self.feed.as_str_name(),
            self.interval.as_deref().unwrap_or(""),
            self.native_symbol,
        )
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.venue.is_empty() || self.market.is_empty() || self.native_symbol.is_empty() {
            return Err("demand subscription identity is incomplete".into());
        }
        if self.feed == FeedType::Unspecified {
            return Err("demand subscription feed is unspecified".into());
        }
        if self.feed == FeedType::Bar && self.interval.is_none() {
            return Err("BAR demand subscription requires interval".into());
        }
        if self.feed != FeedType::Bar && self.interval.is_some() {
            return Err("interval is valid only for BAR demand subscriptions".into());
        }
        if self.priority > 1_000 {
            return Err("demand subscription priority is outside bounds".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DemandConnectionShard {
    pub shard_id: String,
    pub venue: String,
    pub market: String,
    pub feed: FeedType,
    pub config_revision: u64,
    pub subscriptions: Vec<DemandSubscription>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DemandShardPlan {
    pub revision: u64,
    pub shards: Vec<DemandConnectionShard>,
    pub runtime_roles: BTreeSet<(String, String)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileActionKind {
    Subscribe,
    Unsubscribe,
    Retain,
    RebindShard,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconcileAction {
    pub kind: ReconcileActionKind,
    pub subscription: DemandSubscription,
    pub reason: String,
}

pub fn validate_requirement(requirement: &DataRequirement) -> Result<(), String> {
    let purpose =
        DemandPurpose::try_from(requirement.purpose).map_err(|_| "demand purpose is invalid")?;
    let feed = FeedType::try_from(requirement.feed).map_err(|_| "demand feed is invalid")?;
    let universe = requirement
        .universe
        .as_ref()
        .ok_or("demand universe is required")?;
    if requirement.consumer_id.trim().is_empty()
        || requirement.source_policy_id.trim().is_empty()
        || universe.selector_id.trim().is_empty()
        || universe.venue.trim().is_empty()
        || universe.market.trim().is_empty()
        || universe.product_type.trim().is_empty()
    {
        return Err("demand requirement identity is incomplete".into());
    }
    let valid_selector = match &universe.selector {
        Some(universe_selector::Selector::ExplicitSymbols(values)) => {
            !values.native_symbols.is_empty()
                && values
                    .native_symbols
                    .iter()
                    .all(|value| !value.trim().is_empty())
                && universe.universe_ref.is_empty()
        }
        Some(universe_selector::Selector::SegmentId(value)) => {
            !value.trim().is_empty() && !universe.universe_ref.trim().is_empty()
        }
        Some(universe_selector::Selector::ContinuousContract(value)) => {
            !value.family.trim().is_empty()
                && !value.roll_policy.trim().is_empty()
                && universe.universe_ref.is_empty()
        }
        None => !universe.universe_ref.trim().is_empty(),
    };
    if !valid_selector {
        return Err("demand universe selector is incomplete or ambiguous".into());
    }
    if feed == FeedType::Unspecified {
        return Err("demand feed is unspecified".into());
    }
    if feed == FeedType::Bar && requirement.interval.trim().is_empty() {
        return Err("BAR demand requires interval".into());
    }
    if feed != FeedType::Bar && !requirement.interval.trim().is_empty() {
        return Err("interval is valid only for BAR demand".into());
    }
    if (!universe.expected_universe_sha256.is_empty()
        && universe.expected_universe_sha256.len() != 32)
        || requirement.warmup_limit > 100_000
        || requirement.max_freshness_ms > 86_400_000
        || requirement.priority > 1_000
        || requirement.ttl_seconds < 30
        || requirement.ttl_seconds > 3_600
        || requirement.depth_levels > 10_000
        || requirement.configuration_revision == 0
    {
        return Err("demand requirement bound is invalid".into());
    }
    if let Some(warmup) = &requirement.warmup {
        let policy = IntervalSourcePolicy::try_from(warmup.interval_source_policy)
            .map_err(|_| "warmup interval source policy is invalid")?;
        if policy == IntervalSourcePolicy::Unspecified
            || warmup.max_cache_age_ms > 86_400_000
            || !(100..=120_000).contains(&warmup.deadline_ms)
        {
            return Err("warmup policy bound is invalid".into());
        }
        match &warmup.horizon {
            Some(warmup_specification::Horizon::Rows(rows)) => {
                if !(1..=100_000).contains(rows)
                    || (requirement.warmup_limit != 0 && requirement.warmup_limit != *rows)
                {
                    return Err("warmup row horizon conflicts with legacy limit".into());
                }
            }
            Some(warmup_specification::Horizon::TimeRange(range)) => {
                if requirement.warmup_limit != 0
                    || range.start_time_ns <= 0
                    || range.end_time_ns <= range.start_time_ns
                {
                    return Err("warmup time range is invalid or ambiguous".into());
                }
            }
            None => return Err("warmup horizon is required".into()),
        }
    }
    let is_book = matches!(feed, FeedType::BookSnapshot | FeedType::BookDelta);
    if requirement.depth_levels > 0 && !is_book {
        return Err("depth levels are valid only for book demand".into());
    }
    if requirement.require_final_bars && feed != FeedType::Bar {
        return Err("final bar requirement is valid only for BAR".into());
    }
    if purpose == DemandPurpose::Execution
        && (!requirement.execution_grade || !requirement.require_live)
    {
        return Err("execution demand must be execution-grade and live".into());
    }
    if purpose != DemandPurpose::Execution && requirement.execution_grade {
        return Err("execution-grade is reserved for execution demand".into());
    }
    Ok(())
}

pub fn plan_shards(
    revision: u64,
    subscriptions: &[DemandSubscription],
    max_subscriptions_per_connection: usize,
) -> Result<DemandShardPlan, String> {
    if revision == 0
        || max_subscriptions_per_connection == 0
        || max_subscriptions_per_connection > 1_024
    {
        return Err("demand shard plan parameters are invalid".into());
    }
    let mut unique = BTreeMap::new();
    for subscription in subscriptions {
        subscription.validate()?;
        unique.insert(subscription.key(), subscription.clone());
    }
    let mut grouped: BTreeMap<(String, String, FeedType), Vec<DemandSubscription>> =
        BTreeMap::new();
    for subscription in unique.into_values() {
        grouped
            .entry((
                subscription.venue.clone(),
                subscription.market.clone(),
                subscription.feed,
            ))
            .or_default()
            .push(subscription);
    }
    let mut shards = Vec::new();
    let mut roles = BTreeSet::new();
    for ((venue, market, feed), mut members) in grouped {
        members.sort();
        roles.insert((venue.clone(), market.clone()));
        for batch in members.chunks(max_subscriptions_per_connection) {
            let keys = batch
                .iter()
                .map(DemandSubscription::key)
                .collect::<Vec<_>>()
                .join("\n");
            let digest = hex::encode(Sha256::digest(keys.as_bytes()))[..12].to_owned();
            shards.push(DemandConnectionShard {
                shard_id: format!(
                    "{}-{}-{}-{}",
                    venue.to_ascii_lowercase(),
                    market.to_ascii_lowercase(),
                    feed.as_str_name().to_ascii_lowercase(),
                    digest,
                ),
                venue: venue.clone(),
                market: market.clone(),
                feed,
                config_revision: revision,
                subscriptions: batch.to_vec(),
            });
        }
    }
    Ok(DemandShardPlan {
        revision,
        shards,
        runtime_roles: roles,
    })
}

pub fn reconcile(
    previous: Option<&DemandShardPlan>,
    next: &DemandShardPlan,
) -> Vec<ReconcileAction> {
    let previous_subscriptions = previous
        .map(|plan| {
            plan.shards
                .iter()
                .flat_map(|shard| shard.subscriptions.iter())
                .map(|subscription| (subscription.key(), subscription.clone()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let next_subscriptions = next
        .shards
        .iter()
        .flat_map(|shard| shard.subscriptions.iter())
        .map(|subscription| (subscription.key(), subscription.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut actions = Vec::new();
    for (key, subscription) in &previous_subscriptions {
        if !next_subscriptions.contains_key(key) {
            actions.push(ReconcileAction {
                kind: ReconcileActionKind::Unsubscribe,
                subscription: subscription.clone(),
                reason: "lease_expired_or_requirement_removed".into(),
            });
        }
    }
    for (key, subscription) in &next_subscriptions {
        actions.push(ReconcileAction {
            kind: if previous_subscriptions.contains_key(key) {
                ReconcileActionKind::Retain
            } else {
                ReconcileActionKind::Subscribe
            },
            subscription: subscription.clone(),
            reason: if previous_subscriptions.contains_key(key) {
                "unchanged_demand".into()
            } else {
                "new_or_reactivated_demand".into()
            },
        });
    }
    let old_shards = previous
        .map(|plan| {
            plan.shards
                .iter()
                .map(|shard| shard.shard_id.as_str())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let revision_changed = previous.is_some_and(|plan| plan.revision != next.revision);
    for shard in &next.shards {
        let has_retained_subscription = shard
            .subscriptions
            .iter()
            .any(|item| previous_subscriptions.contains_key(&item.key()));
        if (!old_shards.contains(shard.shard_id.as_str()) || revision_changed)
            && has_retained_subscription
        {
            if let Some(subscription) = shard.subscriptions.first() {
                actions.push(ReconcileAction {
                    kind: ReconcileActionKind::RebindShard,
                    subscription: subscription.clone(),
                    reason: "capacity_or_membership_change_requires_connection_rebind".into(),
                });
            }
        }
    }
    actions.sort_by(|left, right| {
        format!("{:?}:{}", left.kind, left.subscription.key()).cmp(&format!(
            "{:?}:{}",
            right.kind,
            right.subscription.key()
        ))
    });
    actions
}

pub fn transition_allowed(previous: DemandState, next: DemandState) -> bool {
    use DemandState as State;
    matches!(
        (previous, next),
        (
            State::Requested,
            State::Connecting | State::Unsupported | State::Expired
        ) | (
            State::Connecting,
            State::Warming | State::Degraded | State::Expired
        ) | (
            State::Warming,
            State::Live | State::Degraded | State::MarketClosed | State::Expired
        ) | (
            State::Live,
            State::Degraded | State::MarketClosed | State::Expired
        ) | (
            State::Degraded,
            State::Connecting | State::MarketClosed | State::Expired
        ) | (State::MarketClosed, State::Connecting | State::Expired)
            | (State::Unsupported, State::Requested | State::Expired)
            | (State::Expired, State::Requested)
    )
}

#[cfg(test)]
mod tests {
    use super::{
        plan_shards, reconcile, transition_allowed, validate_requirement, DemandSubscription,
        ReconcileActionKind,
    };
    use qdl_contracts::qdl::demand::v1::{
        universe_selector, warmup_specification, DataRequirement, DemandPurpose, DemandState,
        ExplicitSymbols, FeedType, IntervalSourcePolicy, UniverseSelector, WarmupSpecification,
    };

    fn requirement() -> DataRequirement {
        DataRequirement {
            consumer_id: "alpha.grid".into(),
            purpose: DemandPurpose::Alpha as i32,
            universe: Some(UniverseSelector {
                selector_id: "binance-usdm-major".into(),
                venue: "BINANCE".into(),
                market: "USDM".into(),
                product_type: "PERPETUAL".into(),
                universe_ref: "".into(),
                selector: Some(universe_selector::Selector::ExplicitSymbols(
                    ExplicitSymbols {
                        native_symbols: vec!["BTCUSDT".into(), "ETHUSDT".into()],
                    },
                )),
                expected_universe_sha256: vec![],
            }),
            feed: FeedType::Trade as i32,
            interval: "".into(),
            warmup_limit: 0,
            max_freshness_ms: 5_000,
            priority: 100,
            ttl_seconds: 180,
            require_final_bars: false,
            require_live: true,
            execution_grade: false,
            source_policy_id: "crypto_primary_v2".into(),
            depth_levels: 0,
            configuration_revision: 1,
            warmup: None,
            basis_contract_type: String::new(),
            basis_series: String::new(),
        }
    }

    fn typed_warmup(rows: u32) -> WarmupSpecification {
        WarmupSpecification {
            interval_source_policy: IntervalSourcePolicy::NativeOrExactResample as i32,
            max_cache_age_ms: 60_000,
            deadline_ms: 20_000,
            horizon: Some(warmup_specification::Horizon::Rows(rows)),
        }
    }

    #[test]
    fn requirement_validation_enforces_execution_and_selector_contract() {
        assert!(validate_requirement(&requirement()).is_ok());
        let mut invalid = requirement();
        invalid.purpose = DemandPurpose::Execution as i32;
        assert!(validate_requirement(&invalid).is_err());
        invalid.execution_grade = true;
        assert!(validate_requirement(&invalid).is_ok());
        invalid.ttl_seconds = 5;
        assert!(validate_requirement(&invalid).is_err());
        let mut invalid_digest = requirement();
        invalid_digest
            .universe
            .as_mut()
            .expect("test requirement has universe")
            .expected_universe_sha256 = vec![0; 31];
        assert!(validate_requirement(&invalid_digest).is_err());
        let mut invalid_freshness = requirement();
        invalid_freshness.max_freshness_ms = 86_400_001;
        assert!(validate_requirement(&invalid_freshness).is_err());
    }

    #[test]
    fn typed_warmup_validation_matches_python_contract() {
        let mut value = requirement();
        value.feed = FeedType::Bar as i32;
        value.interval = "15m".into();
        value.require_final_bars = true;
        value.warmup = Some(typed_warmup(700));
        assert!(validate_requirement(&value).is_ok());

        value.warmup_limit = 500;
        assert!(validate_requirement(&value).is_err());
        value.warmup_limit = 0;
        value.warmup = Some(typed_warmup(0));
        assert!(validate_requirement(&value).is_err());
    }

    #[test]
    fn sharding_is_deterministic_and_not_a_service_per_symbol() {
        let subscriptions = (0..500)
            .map(|index| {
                DemandSubscription::new(
                    "BINANCE",
                    "USDM",
                    FeedType::Trade,
                    format!("TOKEN{index}USDT"),
                    None,
                    100,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let first = plan_shards(7, &subscriptions, 200).unwrap();
        let second = plan_shards(7, &subscriptions, 200).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.shards.len(), 3);
        assert_eq!(first.runtime_roles.len(), 1);
    }

    #[test]
    fn reconciliation_reuses_process_and_marks_internal_rebind() {
        let btc = DemandSubscription::new("BINANCE", "USDM", FeedType::Trade, "BTCUSDT", None, 100)
            .unwrap();
        let eth = DemandSubscription::new("BINANCE", "USDM", FeedType::Trade, "ETHUSDT", None, 100)
            .unwrap();
        let old = plan_shards(1, &[btc.clone()], 200).unwrap();
        let next = plan_shards(2, &[btc, eth], 1).unwrap();
        let actions = reconcile(Some(&old), &next);
        assert!(actions
            .iter()
            .any(|item| item.kind == ReconcileActionKind::Subscribe));
        assert!(actions
            .iter()
            .any(|item| item.kind == ReconcileActionKind::Retain));
        assert!(actions
            .iter()
            .any(|item| item.kind == ReconcileActionKind::RebindShard));
        assert!(transition_allowed(
            DemandState::Requested,
            DemandState::Connecting
        ));
        assert!(!transition_allowed(
            DemandState::Live,
            DemandState::Requested
        ));
    }
}
