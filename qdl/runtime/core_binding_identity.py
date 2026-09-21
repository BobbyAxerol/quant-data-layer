"""Python-side validation for the Rust realtime-core binding identity rule.

The stable compiler may project one logical OKX MARK_INDEX requirement into
two physical provider channels.  They deliberately share the logical
``source_id`` but differ by MARK versus INDEX component.  Offline rollout
tools must accept that valid pair while retaining the Rust core's fail-closed
duplicate and completeness rules.
"""

from __future__ import annotations

from typing import Any, Mapping


CoreBindingIdentity = tuple[str, ...]
_MARK_INDEX_COMPONENTS = frozenset({"MARK", "INDEX", "BOTH"})


def _string(value: Mapping[str, Any], name: str, *, field: str) -> str:
    result = value.get(name)
    if not isinstance(result, str) or not result.strip():
        raise ValueError(f"{field} has an invalid {name}")
    return result


def _physical_identity(value: Mapping[str, Any], *, field: str) -> tuple[str, ...]:
    symbol = value.get("physical_native_symbol", value.get("native_symbol"))
    channel = value.get("physical_native_channel", value.get("native_channel"))
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError(f"{field} has an invalid physical native symbol")
    if not isinstance(channel, str) or not channel.strip():
        raise ValueError(f"{field} has an invalid physical native channel")
    return (
        _string(value, "provider", field=field),
        _string(value, "venue", field=field),
        _string(value, "market", field=field),
        _string(value, "product_type", field=field),
        symbol,
        channel,
    )


def core_binding_identity(value: Mapping[str, Any], *, field: str) -> CoreBindingIdentity:
    """Return the semantic identity used for offline core-config comparison."""
    source_id = _string(value, "source_id", field=field)
    mark_index = value.get("mark_index")
    if mark_index is None:
        return ("ORDINARY", source_id)
    if not isinstance(mark_index, Mapping):
        raise ValueError(f"{field} has an invalid mark_index contract")
    component = mark_index.get("component")
    if not isinstance(component, str) or component not in _MARK_INDEX_COMPONENTS:
        raise ValueError(f"{field} has an invalid mark_index component")
    return ("MARK_INDEX", _string(value, "instrument_uid", field=field), source_id, component)


def native_ingestor_binding_identity(
    value: Mapping[str, Any], *, field: str
) -> tuple[str, ...]:
    """Return a physical native subscription identity.

    Ordinary TRADE/QUOTE/BOOK sources have one physical subscription, so their
    logical ID is sufficient to expose a changed channel as semantic drift.
    An OKX MARK_INDEX source intentionally has two physical subscriptions;
    include symbol and channel only for that paired feed so neither component
    is collapsed or confused with a duplicate retry declaration.
    """
    feed = _string(value, "feed", field=field)
    subscription_id = _string(value, "subscription_id", field=field)
    if feed != "MARK_INDEX":
        return (feed, subscription_id)
    return (
        feed,
        subscription_id,
        _string(value, "native_symbol", field=field),
        _string(value, "native_channel", field=field),
    )


def format_core_binding_identity(identity: CoreBindingIdentity) -> str:
    if identity[0] == "ORDINARY":
        return identity[1]
    return f"{identity[2]}:{identity[3]}@{identity[1]}"


def core_binding_map(
    bindings: object,
    *,
    field: str,
) -> dict[CoreBindingIdentity, dict[str, Any]]:
    """Validate and index bindings with the same source rule as Rust.

    This mirrors ``RealtimeCoreConfig::validate`` at the declaration layer:
    physical channels must be unique; ordinary and MARK_INDEX source IDs may
    not collide; each MARK_INDEX target must be exactly ``BOTH`` or
    ``MARK`` plus ``INDEX``.
    """
    if not isinstance(bindings, list) or not bindings:
        raise ValueError(f"{field} bindings are invalid")

    result: dict[CoreBindingIdentity, dict[str, Any]] = {}
    physical: set[tuple[str, ...]] = set()
    ordinary_source_ids: set[str] = set()
    mark_index_source_ids: set[str] = set()
    mark_index_components: dict[tuple[str, str], set[str]] = {}

    for raw in bindings:
        if not isinstance(raw, dict):
            raise ValueError(f"{field} has a non-object binding")
        item = dict(raw)
        physical_key = _physical_identity(item, field=field)
        if physical_key in physical:
            raise ValueError(f"{field} has a duplicate physical binding")
        physical.add(physical_key)

        identity = core_binding_identity(item, field=field)
        source_id = identity[2] if identity[0] == "MARK_INDEX" else identity[1]
        if identity[0] == "MARK_INDEX":
            if source_id in ordinary_source_ids:
                raise ValueError(f"{field} mixes ordinary and mark_index source_id {source_id}")
            target = (identity[1], source_id)
            components = mark_index_components.setdefault(target, set())
            if identity[3] in components:
                raise ValueError(f"{field} has a duplicate mark_index component")
            components.add(identity[3])
            mark_index_source_ids.add(source_id)
        else:
            if source_id in mark_index_source_ids or source_id in ordinary_source_ids:
                raise ValueError(f"{field} has a duplicate ordinary source_id")
            ordinary_source_ids.add(source_id)
        if identity in result:
            raise ValueError(f"{field} has a duplicate semantic binding")
        result[identity] = item

    for target, components in mark_index_components.items():
        if components not in ({"BOTH"}, {"MARK", "INDEX"}):
            raise ValueError(
                f"{field} has an incomplete mark_index component pair: {target[0]}/{target[1]}"
            )
    return result
