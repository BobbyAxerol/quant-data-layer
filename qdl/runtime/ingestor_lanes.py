"""Sticky ingestor lanes: an existing binding never changes lane and a lane name never shifts.

DL v2.3.0. The native raw ingestor (``rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs``)
chunks its bindings into connections ("lanes") of at most
``max_subscriptions_per_connection``, in config order, per feed class. Its lane
name is the producer identity, the session-liveness file and the connection
generation counter, and the L2 book fence keys on it. Two growth effects broke
that identity:

* inserting an instrument in sorted order moves every later binding into the
  next chunk, so unchanged bindings change lane;
* OKX numbers lanes across feeds inside a service (``okx-public-001`` might be
  the BOOK lane today and the BAR lane after a BOOK lane is added), so a lane
  name shifts to another feed.

The fix keeps today's names and makes them sticky: each binding may carry a
``lane_shard`` pin (the number in its lane name). The ingestor groups pinned
bindings by (OKX service, shard) or (Binance route, feed, shard) and refuses a
mixed or overfull plan. This module is the renderer side:

* ``legacy_lane_assignment`` mirrors the ingestor's positional partition, so
  the first pinned render reproduces today's lanes exactly;
* ``sticky_lane_pins`` keeps every existing pin, removes departed bindings,
  fills free capacity of the same scope and feed, and opens a new shard only
  when every same-feed lane is full.

Boundary: pure functions over ingestor config documents; no I/O.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping

FEED_ORDER = ("BOOK", "BAR", "TRADE", "QUOTE", "MARK_INDEX")
LATEST_STATE_FEEDS = frozenset({"QUOTE", "MARK_INDEX"})
_OKX_PUBLIC = frozenset({"trades", "bbo-tbt", "books", "mark-price", "index-tickers"})


def binding_key(binding: Mapping[str, Any]) -> str:
    """The ingestor's own binding identity (``RawBinding::key``)."""

    return f"{binding['native_channel']}|{binding['native_symbol']}"


def lane_scope(runtime: str, binding: Mapping[str, Any]) -> str:
    """OKX service or Binance route: the connection group a binding lives in."""

    channel = str(binding["native_channel"]).strip()
    if runtime == "OKX":
        if channel in _OKX_PUBLIC:
            return "public"
        if channel == "trades-all" or channel.startswith("candle"):
            return "business"
        raise ValueError(f"unsupported OKX channel for lane planning: {channel}")
    if runtime == "BINANCE":
        if channel.endswith("@markPrice@1s") or "@kline_" in channel:
            return "market"
        if channel.endswith(("@trade", "@bookTicker", "@depth@100ms")):
            return "public"
        raise ValueError(f"unrouted Binance stream for lane planning: {channel}")
    raise ValueError(f"unsupported ingestor runtime: {runtime}")


def lane_name(runtime: str, scope: str, feed: str, shard: int) -> str:
    """The lane name the ingestor derives (Binance is feed-keyed, OKX is not)."""

    if runtime == "OKX":
        return f"okx-{scope}-{shard:03d}"
    feed_name = {"BOOK": "book", "BAR": "bar", "TRADE": "trade", "QUOTE": "quote",
                 "MARK_INDEX": "markindex"}[feed]
    return f"binance-{scope}-{feed_name}-{shard:03d}"


def _group_key(runtime: str, scope: str, feed: str, shard: int) -> tuple:
    return (scope, shard) if runtime == "OKX" else (scope, feed, shard)


def legacy_lane_assignment(
    runtime: str, bindings: Iterable[Mapping[str, Any]], max_subscriptions: int,
) -> dict[str, int]:
    """binding key -> shard, exactly as the unpinned ingestor partitions today."""

    if max_subscriptions < 1:
        raise ValueError("max_subscriptions must be positive")
    by_scope: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for binding in bindings:
        by_scope[lane_scope(runtime, binding)].append(binding)
    result: dict[str, int] = {}
    for scope, members in by_scope.items():
        counter = 0
        for feed in FEED_ORDER:
            lane = [item for item in members if item["feed"] == feed]
            for start in range(0, len(lane), max_subscriptions):
                if runtime == "OKX":
                    counter += 1
                    shard = counter
                else:
                    shard = start // max_subscriptions + 1
                for item in lane[start:start + max_subscriptions]:
                    result[binding_key(item)] = shard
    return result


def current_lane_assignment(
    runtime: str, bindings: Iterable[Mapping[str, Any]], max_subscriptions: int,
) -> dict[str, int]:
    """The assignment a config runs with: its pins, or the legacy partition."""

    rows = list(bindings)
    pinned = [item for item in rows if item.get("lane_shard") is not None]
    if not pinned:
        return legacy_lane_assignment(runtime, rows, max_subscriptions)
    if len(pinned) != len(rows):
        raise ValueError("ingestor config pins some bindings to lanes but not all")
    return {binding_key(item): int(item["lane_shard"]) for item in rows}


def validate_lane_plan(
    runtime: str, bindings: Iterable[Mapping[str, Any]], pins: Mapping[str, int],
    max_subscriptions: int,
) -> dict[tuple, list[str]]:
    """Group pinned bindings the way the ingestor does and apply its refusals."""

    lanes: dict[tuple, list[str]] = defaultdict(list)
    feeds: dict[tuple, set[str]] = defaultdict(set)
    for binding in bindings:
        key = binding_key(binding)
        shard = pins.get(key)
        if shard is None or isinstance(shard, bool) or int(shard) < 1:
            raise ValueError(f"binding {key} has no valid lane pin")
        scope = lane_scope(runtime, binding)
        group = _group_key(runtime, scope, str(binding["feed"]), int(shard))
        lanes[group].append(key)
        feeds[group].add(str(binding["feed"]))
    for group, members in lanes.items():
        if len(feeds[group]) != 1:
            raise ValueError(f"lane {group} mixes feed classes {sorted(feeds[group])}")
        if len(members) > max_subscriptions:
            raise ValueError(f"lane {group} holds {len(members)} > {max_subscriptions} subscriptions")
    return dict(lanes)


def sticky_lane_pins(
    runtime: str,
    previous_bindings: Iterable[Mapping[str, Any]],
    bindings: Iterable[Mapping[str, Any]],
    max_subscriptions: int,
) -> dict[str, int]:
    """Pins for ``bindings`` that keep every retained binding in its lane."""

    previous_rows = list(previous_bindings)
    rows = list(bindings)
    previous = current_lane_assignment(runtime, previous_rows, max_subscriptions)
    previous_feed = {binding_key(item): str(item["feed"]) for item in previous_rows}
    pins: dict[str, int] = {}
    load: dict[tuple, int] = defaultdict(int)
    lane_feed: dict[tuple, str] = {}
    added: list[Mapping[str, Any]] = []
    for binding in rows:
        key = binding_key(binding)
        feed = str(binding["feed"])
        if key in previous:
            if previous_feed[key] != feed:
                raise ValueError(f"binding {key} changed feed class; that is not a lane-stable change")
            shard = previous[key]
            group = _group_key(runtime, lane_scope(runtime, binding), feed, shard)
            pins[key] = shard
            load[group] += 1
            lane_feed[group] = feed
        else:
            added.append(binding)
    # Lanes that lost every member keep their number reserved for their feed, so
    # a later addition of that feed reuses the name rather than another feed.
    for item in previous_rows:
        key = binding_key(item)
        shard = previous[key]
        group = _group_key(runtime, lane_scope(runtime, item), str(item["feed"]), shard)
        lane_feed.setdefault(group, str(item["feed"]))
        load.setdefault(group, 0)
    for binding in added:
        key = binding_key(binding)
        feed = str(binding["feed"])
        scope = lane_scope(runtime, binding)
        candidates = sorted(
            group for group, owner in lane_feed.items()
            if group[0] == scope and owner == feed and load[group] < max_subscriptions
        )
        if candidates:
            group = candidates[0]
            shard = group[-1]
        else:
            used = [group[-1] for group in lane_feed if group[0] == scope
                    and (runtime == "OKX" or group[1] == feed)]
            shard = max(used, default=0) + 1
            group = _group_key(runtime, scope, feed, shard)
            lane_feed[group] = feed
        pins[key] = shard
        load[group] += 1
    validate_lane_plan(runtime, rows, pins, max_subscriptions)
    return pins


def apply_lane_pins(config: Mapping[str, Any], pins: Mapping[str, int]) -> dict[str, Any]:
    """A copy of an ingestor config with every binding pinned."""

    result = dict(config)
    result["bindings"] = [
        {**binding, "lane_shard": int(pins[binding_key(binding)])} for binding in config["bindings"]
    ]
    return result


def lane_plan_report(
    runtime: str, bindings: Iterable[Mapping[str, Any]], pins: Mapping[str, int],
) -> dict[str, dict[str, Any]]:
    """lane name -> {feed, members}: the gate compares this across revisions."""

    report: dict[str, dict[str, Any]] = {}
    for binding in bindings:
        key = binding_key(binding)
        feed = str(binding["feed"])
        name = lane_name(runtime, lane_scope(runtime, binding), feed, int(pins[key]))
        entry = report.setdefault(name, {"feed": feed, "members": []})
        entry["members"].append(key)
    return report
