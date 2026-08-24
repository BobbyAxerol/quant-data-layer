from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable

from qdl.demand.contracts import DemandFeed, DemandState, ResolvedRequirement
from qdl.ingestion.contracts import ConnectionShard, FeedType, Subscription, plan_shards


class SubscriptionActionKind(str, Enum):
    SUBSCRIBE = "SUBSCRIBE"
    UNSUBSCRIBE = "UNSUBSCRIBE"
    RETAIN = "RETAIN"
    REBIND_SHARD = "REBIND_SHARD"


@dataclass(frozen=True, slots=True)
class SubscriptionAction:
    kind: SubscriptionActionKind
    subscription: Subscription
    reason: str


@dataclass(frozen=True, slots=True)
class DemandTopology:
    demand_revision: int
    subscriptions: tuple[Subscription, ...]
    shards: tuple[ConnectionShard, ...]
    runtime_roles: tuple[tuple[str, str], ...]
    core_binding_ids: tuple[str, ...]
    provisioning_required: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.demand_revision < 1:
            raise ValueError("demand_revision must be positive")
        keys = [item.key for item in self.subscriptions]
        if len(keys) != len(set(keys)):
            raise ValueError("topology subscriptions must be unique")
        shard_keys = [item.shard_id for item in self.shards]
        if len(shard_keys) != len(set(shard_keys)):
            raise ValueError("topology shard IDs must be unique")
        if len(self.runtime_roles) != len(set(self.runtime_roles)):
            raise ValueError("runtime roles must be unique")
        if len(self.core_binding_ids) != len(set(self.core_binding_ids)):
            raise ValueError("core binding IDs must be unique")
        if len(self.provisioning_required) != len(set(self.provisioning_required)):
            raise ValueError("provisioning requirements must be unique")

    @property
    def connection_count(self) -> int:
        return len(self.shards)

    @property
    def service_role_count(self) -> int:
        """Logical venue/market roles, never a per-symbol service count."""
        return len(self.runtime_roles)


class DemandTopologyPlanner:
    """Turns resolved leases into one dynamic acquisition/core topology.

    The plan is declarative. A venue adapter applies subscribe/unsubscribe and
    shard rebind actions within its existing process; this module never implies
    a Compose service, image or worker per symbol.
    """

    _FEED_MAP = {
        DemandFeed.TRADE: FeedType.TRADE,
        DemandFeed.QUOTE: FeedType.BBO,
        DemandFeed.BAR: FeedType.BAR,
        DemandFeed.BOOK_SNAPSHOT: FeedType.BOOK,
        DemandFeed.BOOK_DELTA: FeedType.BOOK,
    }
    _ACTIVE_STATES = {
        DemandState.REQUESTED,
        DemandState.CONNECTING,
        DemandState.WARMING,
        DemandState.LIVE,
        DemandState.DEGRADED,
    }

    def __init__(self, *, max_subscriptions_per_connection: int) -> None:
        if not 1 <= max_subscriptions_per_connection <= 1_024:
            raise ValueError("max_subscriptions_per_connection is outside bounds")
        self.max_subscriptions_per_connection = max_subscriptions_per_connection

    def build(
        self,
        requirements: Iterable[ResolvedRequirement],
        *,
        demand_revision: int,
    ) -> DemandTopology:
        if demand_revision < 1:
            raise ValueError("demand_revision must be positive")
        selected: dict[str, tuple[Subscription, ResolvedRequirement]] = {}
        provisioning_required: set[str] = set()
        binding_ids: set[str] = set()
        for requirement in requirements:
            if requirement.demand_revision != demand_revision:
                raise ValueError("resolved demand revision differs from topology revision")
            if requirement.state not in self._ACTIVE_STATES:
                continue
            if not requirement.capability.enabled:
                continue
            try:
                feed = self._FEED_MAP[requirement.requirement.feed]
            except KeyError:
                provisioning_required.add(requirement.requirement_id)
                continue
            subscription = Subscription(
                venue=requirement.requirement.universe.venue,
                market=requirement.requirement.universe.market,
                feed=feed,
                native_symbol=requirement.native_symbol,
                interval=requirement.requirement.interval,
                priority=requirement.effective_priority,
            )
            existing = selected.get(subscription.key)
            if existing is not None and existing[1].requirement.source_policy_id != requirement.requirement.source_policy_id:
                raise ValueError("subscription source policy conflict")
            selected[subscription.key] = (subscription, requirement)
            if requirement.binding_id:
                binding_ids.add(requirement.binding_id)
            else:
                provisioning_required.add(requirement.requirement_id)
        subscriptions = tuple(item[0] for _, item in sorted(selected.items()))
        shards = plan_shards(
            set(subscriptions),
            max_streams=self.max_subscriptions_per_connection,
            config_revision=demand_revision,
        )
        roles = tuple(
            sorted({(item.venue.upper(), item.market.upper()) for item in subscriptions})
        )
        return DemandTopology(
            demand_revision=demand_revision,
            subscriptions=subscriptions,
            shards=shards,
            runtime_roles=roles,
            core_binding_ids=tuple(sorted(binding_ids)),
            provisioning_required=tuple(sorted(provisioning_required)),
        )

    def reconcile(
        self,
        previous: DemandTopology | None,
        next_topology: DemandTopology,
    ) -> tuple[SubscriptionAction, ...]:
        previous_by_key = (
            {item.key: item for item in previous.subscriptions} if previous is not None else {}
        )
        next_by_key = {item.key: item for item in next_topology.subscriptions}
        actions: list[SubscriptionAction] = []
        for key, subscription in sorted(previous_by_key.items()):
            if key not in next_by_key:
                actions.append(
                    SubscriptionAction(
                        SubscriptionActionKind.UNSUBSCRIBE,
                        subscription,
                        "lease_expired_or_requirement_removed",
                    )
                )
        for key, subscription in sorted(next_by_key.items()):
            if key not in previous_by_key:
                actions.append(
                    SubscriptionAction(
                        SubscriptionActionKind.SUBSCRIBE,
                        subscription,
                        "new_or_reactivated_demand",
                    )
                )
            else:
                actions.append(
                    SubscriptionAction(
                        SubscriptionActionKind.RETAIN,
                        subscription,
                        "unchanged_demand",
                    )
                )
        previous_shards = set() if previous is None else {item.shard_id for item in previous.shards}
        revision_changed = (
            previous is not None
            and previous.demand_revision != next_topology.demand_revision
        )
        for shard in next_topology.shards:
            if (shard.shard_id not in previous_shards or revision_changed) and any(
                action.kind is SubscriptionActionKind.RETAIN
                and action.subscription in shard.subscriptions
                for action in actions
            ):
                actions.append(
                    SubscriptionAction(
                        SubscriptionActionKind.REBIND_SHARD,
                        shard.subscriptions[0],
                        "capacity_or_membership_change_requires_connection_rebind",
                    )
                )
        return tuple(
            sorted(
                actions,
                key=lambda item: (item.kind.value, item.subscription.key, item.reason),
            )
        )
