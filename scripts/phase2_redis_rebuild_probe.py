from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import redis

from qdl.canonical.trade import (
    TradeContext,
    canonical_event,
    canonicalize_binance_usdm_trade,
    raw_trade_event,
)
from qdl.projection import RedisProjectionTarget, TradeProjector
from qdl.transport import SQLiteDurableSpool, SpoolConfig


ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "shadow:qdl:v2:phase2-probe"


def spool(path: Path) -> SQLiteDurableSpool:
    return SQLiteDurableSpool(
        SpoolConfig(
            path=path,
            max_records=100,
            max_payload_bytes=1_000_000,
            max_event_bytes=100_000,
            min_free_disk_bytes=0,
        )
    )


def projector(
    durable: SQLiteDurableSpool, client: redis.Redis
) -> tuple[RedisProjectionTarget, TradeProjector]:
    target = RedisProjectionTarget(client, namespace=NAMESPACE)
    return target, TradeProjector(
        target,
        namespace=NAMESPACE,
        raw_resolver=lambda stream, event_id: (
            found.event.payload
            if (found := durable.find_event(stream=stream, event_id=event_id))
            else None
        ),
    )


def seed(state_dir: Path, client: redis.Redis) -> dict:
    fixture = json.loads(
        (ROOT / "tests/fixtures/phase2/binance_usdm_trade.json").read_text()
    )
    context = TradeContext(**fixture["context"])
    with spool(state_dir / "bridge.sqlite3") as durable:
        raw = raw_trade_event(
            fixture["raw"], context=context, accepted_at_ns=context.received_at_ns
        )
        durable.append(raw)
        envelope = canonicalize_binance_usdm_trade(fixture["raw"], context)
        canonical = canonical_event(
            envelope,
            accepted_at_ns=context.normalized_at_ns,
            raw_event=raw,
        )
        result = durable.append(canonical)
        stored = durable.read(
            stream=canonical.stream, partition_key=result.cursor.partition_key
        )[0]
        target, trade_projector = projector(durable, client)
        target.clear_namespace()
        if not trade_projector.project(stored):
            raise RuntimeError("first Redis projection was not applied")
        output = {
            "checksum": target.checksum(),
            "stream": canonical.stream,
            "partition_key": canonical.partition_key,
            "redis_dbsize": client.dbsize(),
        }
    (state_dir / "expected.json").write_text(json.dumps(output, sort_keys=True))
    return output


def verify_rebuild(state_dir: Path, client: redis.Redis) -> dict:
    expected = json.loads((state_dir / "expected.json").read_text())
    with spool(state_dir / "bridge.sqlite3") as durable:
        target, trade_projector = projector(durable, client)
        persisted_checksum = target.checksum()
        if persisted_checksum != expected["checksum"]:
            raise RuntimeError("Redis restart did not preserve the projected checksum")
        client.flushdb()
        rows = durable.read(
            stream=expected["stream"], partition_key=expected["partition_key"]
        )
        if len(rows) != 1 or not trade_projector.project(rows[0]):
            raise RuntimeError("durable replay did not rebuild exactly one projection")
        rebuilt_checksum = target.checksum()
        if rebuilt_checksum != expected["checksum"]:
            raise RuntimeError("Redis rebuild checksum differs from pre-restart state")
        target.clear_namespace()
    return {
        "persisted_checksum": persisted_checksum,
        "rebuilt_checksum": rebuilt_checksum,
        "redis_dbsize_after_cleanup": client.dbsize(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("seed", "verify-rebuild"))
    parser.add_argument("--state-dir", type=Path, required=True)
    args = parser.parse_args()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    client = redis.Redis.from_url(os.environ["QDL_PHASE2_REDIS_URL"], decode_responses=False)
    result = seed(args.state_dir, client) if args.command == "seed" else verify_rebuild(args.state_dir, client)
    print(json.dumps({"status": "PASS", "command": args.command, **result}, sort_keys=True))


if __name__ == "__main__":
    main()
