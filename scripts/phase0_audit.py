#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(DEFAULT_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(DEFAULT_REPO_ROOT))

TEXT_SUFFIXES = {".py", ".json", ".toml", ".yml", ".yaml"}
SKIP_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    "data",
    "logs",
    "node_modules",
    "pgdata",
    "state",
    "tests",
}
ROUTE_PATTERN = re.compile(r"/v1/[A-Za-z0-9_./:{}-]+")
REDIS_PATTERN = re.compile(r"(?:stream|trade|kline|vn|feed):[A-Za-z0-9_./:{}*-]+")
PROVIDER_MARKERS = {
    "binance_direct": ("api.binance.com", "fstream.binance.com", "stream.binance.com"),
    "okx_direct": ("okx.com/api", "ws.okx.com"),
    "dnse_direct": ("openapi.dnse", "ws-openapi.dnse"),
    "vnstock_direct": ("from vnstock", "import vnstock"),
}
HTTP_PATHS = (
    "/v1/health",
    "/v1/health/streams",
    "/v1/control/runtime-roles",
    "/v1/control/feed-demands",
    "/v1/control/universe/active",
)


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _sha256(value: Any) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n")


def _compact_payload(value: Any, *, depth: int = 0) -> Any:
    if depth >= 5:
        return {"type": type(value).__name__, "sha256": _sha256(value)}
    if isinstance(value, dict):
        return {str(key): _compact_payload(item, depth=depth + 1) for key, item in value.items()}
    if isinstance(value, list):
        return {
            "count": len(value),
            "sha256": _sha256(value),
            "sample": [_compact_payload(item, depth=depth + 1) for item in value[:3]],
        }
    if isinstance(value, str) and len(value) > 200:
        return {"type": "str", "length": len(value), "sha256": _sha256(value)}
    return value


def _payload_shape(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _payload_shape(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        item_shapes = {_sha256(_payload_shape(item)): _payload_shape(item) for item in value[:20]}
        return {"type": "array", "item_shapes": list(item_shapes.values())}
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


def _feed_demand_summary(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return _compact_payload(payload)
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    by_source = Counter(str(item.get("source")) for item in items if isinstance(item, dict))
    by_feed = Counter(str(item.get("feed")) for item in items if isinstance(item, dict))
    return {
        "demanded_feed_count": payload.get("demanded_feed_count"),
        "lease_count": payload.get("lease_count"),
        "by_source": dict(sorted(by_source.items())),
        "by_feed": dict(sorted(by_feed.items())),
        "feed_keys_sha256": _sha256(sorted(payload.get("feed_keys") or [])),
    }


def _binance_stream_summary(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return _compact_payload(payload)
    shards = payload.get("shards") if isinstance(payload.get("shards"), dict) else {}
    feeds = payload.get("feeds") if isinstance(payload.get("feeds"), dict) else {}
    return {
        "status": payload.get("status"),
        "strict_feed_health": payload.get("strict_feed_health"),
        "uptime_seconds": payload.get("uptime_seconds"),
        "queue": payload.get("queue"),
        "publisher": payload.get("publisher"),
        "shards": {key: value for key, value in shards.items() if key != "items"},
        "feeds": {key: value for key, value in feeds.items() if not key.endswith("samples")},
        "health_warnings": payload.get("health_warnings"),
    }


def _http_payload_summary(path: str, payload: Any) -> Any:
    if path == "/v1/control/feed-demands":
        return _feed_demand_summary(payload)
    if path == "/v1/control/universe/active" and isinstance(payload, dict):
        providers = payload.get("providers") if isinstance(payload.get("providers"), dict) else {}
        return {
            "mode": payload.get("mode"),
            "priority": payload.get("priority"),
            "provider_names": sorted(providers),
            "providers_sha256": _sha256(providers),
        }
    if path in {"/v1/health", "/v1/health/streams"} and isinstance(payload, dict):
        return {
            "status": payload.get("status"),
            "redis": payload.get("redis"),
            "binance_stream": _binance_stream_summary(payload.get("binance_stream")),
            "dnse_stream": _compact_payload(payload.get("dnse_stream")),
            "feed_demands": _feed_demand_summary(payload.get("feed_demands")),
            "preload_topup": _compact_payload(payload.get("preload_topup")),
        }
    return _compact_payload(payload)


def _git_head(repo_root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def build_contract_snapshot() -> tuple[dict[str, Any], dict[str, Any]]:
    from app.main import app
    from app.sdk.client import DataLayerClient

    openapi = app.openapi()
    routes = []
    for route in app.routes:
        path = getattr(route, "path", "")
        if not path.startswith("/v1"):
            continue
        routes.append(
            {
                "path": path,
                "methods": sorted(getattr(route, "methods", set()) or set()),
                "name": getattr(route, "name", None),
            }
        )

    sdk_methods = []
    for name, member in inspect.getmembers(DataLayerClient, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        sdk_methods.append({"name": name, "signature": str(inspect.signature(member))})

    manifest = {
        "schema_version": 1,
        "scope": "v1-public-compatibility",
        "routes": sorted(routes, key=lambda row: (row["path"], row["methods"])),
        "sdk_methods": sorted(sdk_methods, key=lambda row: row["name"]),
        "openapi_sha256": _sha256(openapi),
    }
    return openapi, manifest


def write_contract_snapshot(contract_dir: Path) -> dict[str, Any]:
    openapi, manifest = build_contract_snapshot()
    _write_json(contract_dir / "openapi.snapshot.json", openapi)
    _write_json(contract_dir / "public-surface.snapshot.json", manifest)
    return manifest


def _iter_source_files(root: Path):
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        yield path


def scan_consumers(roots: list[tuple[str, Path]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for label, root in roots:
        if not root.exists():
            result[label] = {"status": "missing", "routes": [], "redis_contracts": []}
            continue

        routes: Counter[str] = Counter()
        redis_contracts: Counter[str] = Counter()
        provider_files: dict[str, set[str]] = defaultdict(set)
        sdk_files: set[str] = set()
        scanned_files = 0
        for path in _iter_source_files(root):
            scanned_files += 1
            try:
                text = path.read_text(errors="replace")
            except OSError:
                continue
            relative = str(path.relative_to(root))
            routes.update(ROUTE_PATTERN.findall(text))
            redis_contracts.update(REDIS_PATTERN.findall(text))
            if "DataLayerClient" in text or "data_layer_client" in text:
                sdk_files.add(relative)
            lowered = text.lower()
            for marker, needles in PROVIDER_MARKERS.items():
                if any(needle in lowered for needle in needles):
                    provider_files[marker].add(relative)

        result[label] = {
            "status": "ok",
            "scanned_files": scanned_files,
            "routes": [{"value": value, "references": count} for value, count in sorted(routes.items())],
            "redis_contracts": [
                {"value": value, "references": count} for value, count in sorted(redis_contracts.items())
            ],
            "sdk_files": sorted(sdk_files),
            "direct_provider_files": {
                marker: sorted(files) for marker, files in sorted(provider_files.items())
            },
        }
    return result


def collect_system_snapshot(repo_root: Path) -> dict[str, Any]:
    memory: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            key, raw = line.split(":", 1)
            memory[key] = int(raw.strip().split()[0]) * 1024
    except (OSError, ValueError):
        pass

    usage = shutil.disk_usage(repo_root)
    try:
        load = [float(value) for value in Path("/proc/loadavg").read_text().split()[:3]]
    except (OSError, ValueError):
        load = []
    return {
        "load_average": load,
        "memory_bytes": {
            "total": memory.get("MemTotal"),
            "available": memory.get("MemAvailable"),
        },
        "filesystem_bytes": {"total": usage.total, "used": usage.used, "free": usage.free},
    }


def collect_storage_snapshot(repo_root: Path) -> dict[str, Any]:
    groups: dict[str, dict[str, int]] = {}
    for relative in ("data/preload", "data/binance_vision_cache", "logs"):
        root = repo_root / relative
        files = [path for path in root.rglob("*") if path.is_file()] if root.exists() else []
        groups[relative] = {
            "file_count": len(files),
            "bytes": sum(path.stat().st_size for path in files),
            "parquet_files": sum(path.suffix == ".parquet" for path in files),
        }
    return groups


def collect_source_plan(repo_root: Path, batch_size: int = 100) -> dict[str, Any]:
    def symbol_count(filename: str) -> int | None:
        path = repo_root / filename
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return None
        return len(payload) if isinstance(payload, list) else None

    spot_symbols = symbol_count("symbols_spot.json")
    usdm_symbols = symbol_count("symbols.json")

    def feed_shards(count: int | None) -> int | None:
        return math.ceil(count / batch_size) if count is not None else None

    spot_per_feed = feed_shards(spot_symbols)
    usdm_per_feed = feed_shards(usdm_symbols)
    full_shards = (
        2 * spot_per_feed + 2 * usdm_per_feed
        if spot_per_feed is not None and usdm_per_feed is not None
        else None
    )
    spot_off_shards = 2 * usdm_per_feed if usdm_per_feed is not None else None
    reduction_percent = (
        round((full_shards - spot_off_shards) * 100 / full_shards, 3)
        if full_shards and spot_off_shards is not None
        else None
    )
    return {
        "batch_size": batch_size,
        "spot_symbols": spot_symbols,
        "usdm_symbols": usdm_symbols,
        "estimated_full_shards": full_shards,
        "estimated_spot_off_shards": spot_off_shards,
        "estimated_shard_reduction_percent": reduction_percent,
        "method": "ceil(symbol_count/batch_size) per trade and kline source",
    }


def _http_json(base_url: str, path: str) -> dict[str, Any]:
    request = Request(base_url.rstrip("/") + path, headers={"Accept": "application/json"})
    started = time.monotonic()
    try:
        with urlopen(request, timeout=5) as response:
            raw = response.read()
            payload = json.loads(raw)
            return {
                "ok": True,
                "status": response.status,
                "latency_ms": round((time.monotonic() - started) * 1000, 3),
                "payload_sha256": _sha256(payload),
                "payload": _http_payload_summary(path, payload),
            }
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "latency_ms": round((time.monotonic() - started) * 1000, 3),
            "error_type": type(exc).__name__,
            "error": str(exc)[:300],
        }


def collect_http_snapshot(base_url: str | None) -> dict[str, Any]:
    if not base_url:
        return {"status": "not_requested"}
    return {path: _http_json(base_url, path) for path in HTTP_PATHS}


def collect_redis_snapshot(host: str | None, port: int, db: int) -> dict[str, Any]:
    if not host:
        return {"status": "not_requested"}
    try:
        import redis

        client = redis.Redis(host=host, port=port, db=db, socket_timeout=3, decode_responses=True)
        memory = client.info("memory")
        stats = client.info("stats")
        clients = client.info("clients")
        persistence = client.info("persistence")
        demand_keys = sorted(client.scan_iter(match="feed:demand:lease:*", count=200))
        channels = sorted(client.pubsub_channels(pattern="stream:*"))
        payload_shapes: dict[str, Any] = {}
        for pattern in (
            "trade:price:binance_usdm:*",
            "trade:price:binance_spot:*",
            "trade:price:*",
            "kline:1m:*",
            "vn:quote:*",
        ):
            for key in client.scan_iter(match=pattern, count=100):
                raw = client.get(key)
                if raw:
                    try:
                        payload_shapes[pattern] = _payload_shape(json.loads(raw))
                    except (TypeError, json.JSONDecodeError):
                        payload_shapes[pattern] = {"type": "invalid_json"}
                    break
        return {
            "status": "ok",
            "memory": {
                "used_memory": memory.get("used_memory"),
                "used_memory_peak": memory.get("used_memory_peak"),
                "maxmemory": memory.get("maxmemory"),
                "mem_fragmentation_ratio": memory.get("mem_fragmentation_ratio"),
            },
            "stats": {
                "total_connections_received": stats.get("total_connections_received"),
                "total_commands_processed": stats.get("total_commands_processed"),
                "instantaneous_ops_per_sec": stats.get("instantaneous_ops_per_sec"),
                "total_net_input_bytes": stats.get("total_net_input_bytes"),
                "total_net_output_bytes": stats.get("total_net_output_bytes"),
                "rejected_connections": stats.get("rejected_connections"),
                "expired_keys": stats.get("expired_keys"),
                "evicted_keys": stats.get("evicted_keys"),
            },
            "clients": {
                "connected_clients": clients.get("connected_clients"),
                "blocked_clients": clients.get("blocked_clients"),
                "client_recent_max_output_buffer": clients.get("client_recent_max_output_buffer"),
            },
            "persistence": {
                "aof_enabled": persistence.get("aof_enabled"),
                "rdb_last_save_time": persistence.get("rdb_last_save_time"),
            },
            "demand_key_count": len(demand_keys),
            "demand_key_samples": demand_keys[:20],
            "pubsub_channel_count": len(channels),
            "pubsub_channel_samples": channels[:20],
            "payload_shapes": payload_shapes,
        }
    except Exception as exc:
        return {"status": "error", "error_type": type(exc).__name__, "error": str(exc)[:300]}


def collect_runtime_sample(
    repo_root: Path,
    base_url: str | None,
    redis_host: str | None,
    redis_port: int,
    redis_db: int,
) -> dict[str, Any]:
    return {
        "observed_at": _utc_now(),
        "system": collect_system_snapshot(repo_root),
        "storage": collect_storage_snapshot(repo_root),
        "source_plan": collect_source_plan(repo_root),
        "http": collect_http_snapshot(base_url),
        "redis": collect_redis_snapshot(redis_host, redis_port, redis_db),
    }


def _consumer_root(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("consumer root must be LABEL=/path")
    label, raw_path = value.split("=", 1)
    if not label.strip() or not raw_path.strip():
        raise argparse.ArgumentTypeError("consumer root must be LABEL=/path")
    return label.strip(), Path(raw_path).resolve()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only Phase 0 contract and runtime audit")
    parser.add_argument("--repo-root", type=Path, default=DEFAULT_REPO_ROOT)
    parser.add_argument("--consumer-root", action="append", default=[], type=_consumer_root)
    parser.add_argument("--base-url")
    parser.add_argument("--redis-host")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-db", type=int, default=0)
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--sample-interval", type=float, default=0.0)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--contract-dir", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    repo_root = args.repo_root.resolve()
    if args.samples < 1 or args.samples > 20:
        raise SystemExit("--samples must be between 1 and 20")
    if args.sample_interval < 0 or args.sample_interval > 3600:
        raise SystemExit("--sample-interval must be between 0 and 3600 seconds")
    if not args.output and not args.contract_dir:
        raise SystemExit("at least one of --output or --contract-dir is required")

    contract_manifest = None
    if args.contract_dir:
        contract_manifest = write_contract_snapshot(args.contract_dir.resolve())

    if args.output:
        samples = []
        for index in range(args.samples):
            samples.append(
                collect_runtime_sample(
                    repo_root,
                    args.base_url,
                    args.redis_host,
                    args.redis_port,
                    args.redis_db,
                )
            )
            if index + 1 < args.samples and args.sample_interval:
                time.sleep(args.sample_interval)
        if args.contract_dir and samples:
            redis_shapes = samples[-1].get("redis", {}).get("payload_shapes")
            if redis_shapes:
                _write_json(args.contract_dir.resolve() / "redis-payload-shapes.snapshot.json", redis_shapes)
                contract_manifest["redis_payload_shapes_sha256"] = _sha256(redis_shapes)
                _write_json(
                    args.contract_dir.resolve() / "public-surface.snapshot.json",
                    contract_manifest,
                )
        report = {
            "schema_version": 1,
            "audit_mode": "read_only",
            "generated_at": _utc_now(),
            "repository_commit": _git_head(repo_root),
            "contract_manifest_sha256": _sha256(contract_manifest) if contract_manifest else None,
            "consumer_inventory": scan_consumers(args.consumer_root),
            "samples": samples,
        }
        _write_json(args.output.resolve(), report)
        print(json.dumps({"output": str(args.output), "samples": len(samples)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
