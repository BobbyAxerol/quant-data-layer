#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import shutil
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ["QDL_PHASE8_PROJECT"] = "qdl_phasea_real_provider"

from scripts.phase80_broker_certification import (  # noqa: E402
    BOOTSTRAP,
    PROJECT,
    add_acls,
    cleanup,
    compose,
    create_topic,
    kafka,
    run,
    v1_health,
    v1_topology,
    wait_for_cluster,
    wait_for_replicas,
)


RUST_IMAGE = "qdl-phase-a-rust:real-provider"
PYTHON_IMAGE = "data-layer:phase-a-real-provider"
EVIDENCE = ROOT / "upgrade/evidence/phase-a-real-provider-core.json"
RAW_TOPIC = "qdl.phase8.phasea.real.raw.v1"
CANONICAL_TOPIC = "qdl.phase8.phasea.real.canonical.v2"
QUARANTINE_TOPIC = "qdl.phase8.phasea.real.quarantine.v1"
GROUP_ID = "phase8-phase-a-real-core"


def client_dir(root: pathlib.Path, principal: str) -> pathlib.Path:
    target = root / principal
    target.mkdir(mode=0o755)
    for source, destination in (
        (root / "ca.crt", target / "ca.crt"),
        (root / f"{principal}.crt", target / "client.crt"),
        (root / f"{principal}.key", target / "client.key"),
    ):
        shutil.copyfile(source, destination)
        destination.chmod(0o644)
    return target


def add_core_acls(env: dict[str, str]) -> None:
    common = (
        "--bootstrap-server", BOOTSTRAP,
        "--command-config", "/etc/kafka/secrets/admin.properties",
        "--add", "--allow-principal", "User:phase8-core",
    )
    for topic, operations in (
        (RAW_TOPIC, ("READ", "DESCRIBE")),
        (CANONICAL_TOPIC, ("WRITE", "DESCRIBE")),
        (QUARANTINE_TOPIC, ("WRITE", "DESCRIBE")),
    ):
        args = list(common)
        for operation in operations:
            args.extend(("--operation", operation))
        args.extend(("--topic", topic))
        kafka(env, "kafka-acls.sh", *args)
    kafka(env, "kafka-acls.sh", *common, "--operation", "READ", "--group", GROUP_ID)
    kafka(env, "kafka-acls.sh", *common, "--operation", "IdempotentWrite", "--cluster")
    kafka(
        env, "kafka-acls.sh", *common,
        "--operation", "WRITE", "--operation", "DESCRIBE",
        "--transactional-id", "qdl-phase-a-real-", "--resource-pattern-type", "prefixed",
    )


def authority(image_id: str) -> dict:
    digest = image_id.removeprefix("sha256:")
    return {
        "schema": "qdl.authority-record.v1",
        "slice_id": "phase-a-real-provider-shadow",
        "revision": 1,
        "mode": "RUST_SHADOW",
        "candidate_image_digest": f"sha256:{digest}",
        "capability_manifest_digest": hashlib.sha256(
            (ROOT / "config/v2/stable-capabilities.yaml").read_bytes()
        ).hexdigest(),
        "contract_digest": hashlib.sha256(
            (ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto").read_bytes()
        ).hexdigest(),
        "partition_plan_digest": hashlib.sha256(b"phase-a-real-provider-plan").hexdigest(),
        "public_write_allowed": False,
        "legacy_write_allowed": False,
        "approved_by": "phase-a-real-provider-certification",
        "effective_at_ns": time.time_ns(),
    }


def docker_run(
    image: str,
    certs: pathlib.Path,
    command: list[str],
    *,
    entrypoint: str | None = None,
    mounts: tuple[str, ...] = (),
    timeout: float = 120.0,
    egress: bool = False,
):
    args = [
        "docker", "run", "--rm", "--network", f"{PROJECT}_phase8_shadow",
        "--read-only", "--security-opt", "no-new-privileges:true",
        "--mount", f"type=bind,source={certs},target=/certs,readonly",
        "--env", f"QDL_KAFKA_BOOTSTRAP_SERVERS={BOOTSTRAP}",
        "--env", "QDL_KAFKA_CERT_ROOT=/certs",
        "--env", "QDL_KAFKA_CLIENT_ID=qdl-phase-a-real",
        "--env", f"QDL_KAFKA_GROUP_ID={GROUP_ID}",
    ]
    for mount in mounts:
        args.extend(("--mount", mount))
    if entrypoint:
        args.extend(("--entrypoint", entrypoint))
    args.append(image)
    args.extend(command)
    if not egress:
        return run(args, timeout=timeout)

    name = f"qdl-phase-a-egress-{time.time_ns()}"
    create_args = args.copy()
    create_args[1:3] = ["create", "--name", name]
    run(create_args, timeout=30.0)
    try:
        run(["docker", "network", "connect", "bridge", name], timeout=30.0)
        return run(["docker", "start", "--attach", name], timeout=timeout)
    finally:
        run(["docker", "rm", "--force", name], check=False, timeout=30.0)

def crypto_specs(authority_record: dict):
    products = (
        ("BINANCE", "USDM", "PERPETUAL", "BTCUSDT", "BINANCE_DIRECT",
         "wss://fstream.binance.com/public/stream", None,
         (("trade", "binance_usdm_trade", 3), ("bookTicker", "binance_usdm_bbo", 2))),
        ("BINANCE", "SPOT", "SPOT", "BTCUSDT", "BINANCE_DIRECT",
         "wss://stream.binance.com:9443/stream", None,
         (("trade", "binance_spot_trade", 3), ("bookTicker", "binance_spot_bbo", 2))),
        ("OKX", "SWAP", "PERPETUAL", "BTC-USDT-SWAP", "OKX_DIRECT",
         "wss://ws.okx.com:8443/ws/v5/public", "wss://ws.okx.com:8443/ws/v5/business",
         (("trades", "okx_trade", 3), ("bbo-tbt", "okx_bbo", 2), ("candle1m", "okx_bar", 1))),
        ("OKX", "SPOT", "SPOT", "BTC-USDT", "OKX_DIRECT",
         "wss://ws.okx.com:8443/ws/v5/public", "wss://ws.okx.com:8443/ws/v5/business",
         (("trades", "okx_trade", 3), ("bbo-tbt", "okx_bbo", 2), ("candle1m", "okx_bar", 1))),
    )
    specs = []
    core_bindings = []
    for venue, market, product, symbol, provider, public_url, business_url, feeds in products:
        runtime = "BINANCE" if venue == "BINANCE" else "OKX"
        for channel, provider_kind, count in feeds:
            native_channel = f"{symbol.lower()}@{channel}" if venue == "BINANCE" else channel
            source_id = f"{venue.lower()}-{market.lower()}-{channel.lower()}"
            binding = {
                "provider": provider,
                "venue": venue,
                "market": market,
                "product_type": product,
                "native_symbol": symbol,
                "native_channel": native_channel,
                "subscription_id": f"phase-a-real-{source_id}",
                "adapter_version": f"{venue.lower()}-json/2.0.0",
                "instrument_catalog_revision": 3,
            }
            specs.append({
                "runtime": runtime,
                "websocket_url": public_url,
                "business_websocket_url": business_url,
                "raw_stream": RAW_TOPIC,
                "shard_id": f"phase-a-real-{source_id}",
                "lease_epoch": 7,
                "partition_plan_epoch": 1,
                "config_revision": 1,
                "heartbeat_seconds": 15,
                "max_events": count,
                "max_runtime_seconds": 45,
                "metrics_every_events": 1,
                "authority": authority_record,
                "bindings": [binding],
            })
            core_bindings.append({
                "provider": provider,
                "venue": venue,
                "market": market,
                "product_type": product,
                "native_symbol": symbol,
                "native_channel": native_channel,
                "provider_kind": provider_kind,
                "instrument_uid": f"uid-{venue.lower()}-{market.lower()}-{symbol.lower()}",
                "instrument_id": f"{venue}.{market}.{product}.{symbol}",
                "instrument_revision": 1,
                "instrument_catalog_revision": 3,
                "source_id": source_id,
                "source_role": "PRIMARY",
                "normalizer_version": "qdl-rust-core/2.0.0",
                "sequence_policy": "MONOTONIC" if channel != "kline_1m" and channel != "candle1m" else "NONE",
            })
    for market, product, provider_kind in (
        ("USDM", "PERPETUAL", "binance_usdm_rest_bar"),
        ("SPOT", "SPOT", "binance_spot_rest_bar"),
    ):
        core_bindings.append({
            "provider": "BINANCE_DIRECT",
            "venue": "BINANCE",
            "market": market,
            "product_type": product,
            "native_symbol": "BTCUSDT",
            "native_channel": "rest-klines/1m",
            "provider_kind": provider_kind,
            "instrument_uid": f"uid-binance-{market.lower()}-btcusdt",
            "instrument_id": f"BINANCE.{market}.{product}.BTCUSDT",
            "instrument_revision": 1,
            "instrument_catalog_revision": 3,
            "source_id": f"binance-{market.lower()}-rest-bar",
            "source_role": "PRIMARY",
            "normalizer_version": "qdl-rust-core/2.0.0",
            "sequence_policy": "NONE",
        })
    return specs, core_bindings


def get_dnse_payload() -> dict:
    result = run([
        "docker", "exec", "redis_marketdata", "redis-cli", "GET", "vn:quote:last:GEX"
    ])
    payload = json.loads(result.stdout)
    if payload.get("source") != "dnse" or not payload.get("timestamp"):
        raise RuntimeError(f"durable VN payload is not DNSE provider data: {payload}")
    return payload


def main() -> int:
    before_topology = v1_topology()
    before_health = v1_health()
    result = {}
    cleanup_result = {}
    with tempfile.TemporaryDirectory(prefix="qdl-phase-a-real-") as directory:
        temp = pathlib.Path(directory)
        temp.chmod(0o755)
        env = os.environ.copy()
        env["QDL_PHASE8_CERT_DIR"] = str(temp)
        run([str(ROOT / "scripts/phase80_generate_tls.sh"), str(temp)], env=env, timeout=120.0)
        producer_certs = client_dir(temp, "phase8-producer")
        consumer_certs = client_dir(temp, "phase8-consumer")
        core_certs = client_dir(temp, "phase8-core")
        try:
            run(["docker", "build", "--provenance=false", "-f", str(ROOT / "Dockerfile.phase8-rust"),
                 "-t", RUST_IMAGE, str(ROOT)], timeout=1200.0)
            run(["docker", "build", "--provenance=false", "-t", PYTHON_IMAGE, str(ROOT)], timeout=1200.0)
            rust_image_id = run(["docker", "image", "inspect", RUST_IMAGE, "--format", "{{.Id}}"]).stdout.strip()
            authority_record = authority(rust_image_id)
            specs, core_bindings = crypto_specs(authority_record)
            dnse_payload = get_dnse_payload()
            dnse_payload_path = temp / "dnse-payload.json"
            dnse_payload_path.write_text(json.dumps(dnse_payload, sort_keys=True) + "\n")
            dnse_payload_path.chmod(0o644)
            symbol = str(dnse_payload["symbol"]).upper()
            core_bindings.append({
                "provider": "DNSE_DIRECT", "venue": "HOSE", "market": "EQUITIES",
                "product_type": "COMMON_STOCK", "native_symbol": symbol,
                "native_channel": "trades", "provider_kind": "dnse_trade",
                "instrument_uid": f"uid-hose-{symbol.lower()}",
                "instrument_id": f"HOSE.EQUITIES.COMMON_STOCK.{symbol}",
                "instrument_revision": 1, "instrument_catalog_revision": 3,
                "source_id": f"dnse-{symbol.lower()}-trade", "source_role": "PRIMARY",
                "normalizer_version": "qdl-rust-core/2.0.0", "sequence_policy": "NONE",
            })
            core_bindings.append({
                "provider": "DNSE_DIRECT", "venue": "HNX", "market": "VN_DERIVATIVES",
                "product_type": "FUTURE", "native_symbol": "VN30F1M",
                "native_channel": "ohlcv/1m", "provider_kind": "dnse_bar",
                "instrument_uid": "uid-hnx-vn30f1m",
                "instrument_id": "HNX.VN_DERIVATIVES.FUTURE.VN30F1M",
                "instrument_revision": 1, "instrument_catalog_revision": 3,
                "source_id": "dnse-vn30f1m-history", "source_role": "BACKFILL",
                "normalizer_version": "qdl-rust-core/2.0.0", "sequence_policy": "NONE",
            })

            compose(env, "up", "-d", timeout=180.0)
            wait_for_cluster(env)
            for topic in (RAW_TOPIC, CANONICAL_TOPIC, QUARANTINE_TOPIC):
                create_topic(env, topic, partitions=3)
            add_acls(env)
            add_core_acls(env)
            wait_for_replicas(env)

            native_results = []
            raw_total = 0
            for index, spec in enumerate(specs):
                config_path = temp / f"native-{index}.json"
                config_path.write_text(json.dumps(spec, indent=2, sort_keys=True) + "\n")
                config_path.chmod(0o644)
                execution = docker_run(
                    RUST_IMAGE,
                    producer_certs,
                    [f"/config/{config_path.name}"],
                    entrypoint="/usr/local/bin/qdl-native-raw-ingestor",
                    mounts=(f"type=bind,source={config_path},target=/config/{config_path.name},readonly",),
                    timeout=90.0,
                    egress=True,
                )
                summary = json.loads(execution.stdout.strip().splitlines()[-1])
                accepted = int(summary["accepted_raw_frames"])
                identity = {
                    "runtime": spec["runtime"],
                    "market": spec["bindings"][0]["market"],
                    "channel": spec["bindings"][0]["native_channel"],
                }
                if accepted != int(spec["max_events"]):
                    raise RuntimeError(
                        "native provider did not satisfy bounded count "
                        f"identity={identity}: {summary} stderr={execution.stderr[-2000:]}"
                    )
                print(json.dumps({
                    "event": "phase_a_native_feed_pass",
                    **identity,
                    "accepted_raw_frames": accepted,
                }, sort_keys=True), flush=True)
                raw_total += accepted
                native_results.append({
                    "runtime": spec["runtime"],
                    "market": spec["bindings"][0]["market"],
                    "channel": spec["bindings"][0]["native_channel"],
                    "accepted_raw_frames": accepted,
                })

            bar_seed = docker_run(
                PYTHON_IMAGE,
                producer_certs,
                ["python", "scripts/phasea_seed_binance_bars.py", "--bootstrap", BOOTSTRAP,
                 "--topic", RAW_TOPIC, "--cert-root", "/certs"],
                timeout=120.0,
                egress=True,
            )
            bar_result = json.loads(bar_seed.stdout.strip().splitlines()[-1])
            if int(bar_result["raw_acked"]) != 2:
                raise RuntimeError(f"Binance REST BAR seed failed: {bar_result}")
            raw_total += int(bar_result["raw_acked"])

            vn_seed = docker_run(
                PYTHON_IMAGE,
                producer_certs,
                ["python", "scripts/phasea_seed_vn_replay.py", "--bootstrap", BOOTSTRAP,
                 "--topic", RAW_TOPIC, "--cert-root", "/certs",
                 "--payload", "/input/dnse-payload.json"],
                mounts=(f"type=bind,source={dnse_payload_path},target=/input/dnse-payload.json,readonly",),
            )
            vn_result = json.loads(vn_seed.stdout.strip().splitlines()[-1])
            raw_total += int(vn_result["raw_acked"])

            vn_derivative_seed = docker_run(
                PYTHON_IMAGE,
                producer_certs,
                ["python", "scripts/phasea_seed_vn_derivative_history.py",
                 "--bootstrap", BOOTSTRAP, "--topic", RAW_TOPIC,
                 "--cert-root", "/certs", "--parquet", "/input/VN30F1M.parquet"],
                mounts=(
                    f"type=bind,source={ROOT / 'data/preload/1m/VN30F1M.parquet'},target=/input/VN30F1M.parquet,readonly",
                ),
            )
            vn_derivative_result = json.loads(
                vn_derivative_seed.stdout.strip().splitlines()[-1]
            )
            raw_total += int(vn_derivative_result["raw_acked"])

            core_config = {
                "core": {
                    "canonical_stream": CANONICAL_TOPIC,
                    "quarantine_stream": QUARANTINE_TOPIC,
                    "allow_test_provenance": False,
                    "dedup_capacity": 100000,
                    "bindings": core_bindings,
                },
                "raw_topics": [RAW_TOPIC],
                "authority": authority_record,
                "shard_id": "phase-a-real-core-shard",
                "transactional_id": "qdl-phase-a-real-core-shard",
                "batch_size": 128,
                "batch_wait_ms": 100,
                "max_events": raw_total,
                "metrics_every_batches": 1,
            }
            core_path = temp / "core-config.json"
            core_path.write_text(json.dumps(core_config, indent=2, sort_keys=True) + "\n")
            core_path.chmod(0o644)
            core_execution = docker_run(
                RUST_IMAGE,
                core_certs,
                ["/config/core-config.json"],
                entrypoint="/usr/local/bin/qdl-realtime-core",
                mounts=(f"type=bind,source={core_path},target=/config/core-config.json,readonly",),
                timeout=120.0,
            )
            core_summary = json.loads(core_execution.stdout.strip().splitlines()[-1])
            if core_summary.get("processed") != raw_total:
                raise RuntimeError(f"real-provider core processed count failed: {core_summary}")
            canonical_count = int(core_summary["canonical"])
            quarantine_count = int(core_summary["quarantines"])
            inspected = docker_run(
                PYTHON_IMAGE,
                consumer_certs,
                ["python", "scripts/phasea_inspect_kafka.py", "--bootstrap", BOOTSTRAP,
                 "--canonical-topic", CANONICAL_TOPIC, "--quarantine-topic", QUARANTINE_TOPIC,
                 "--cert-root", "/certs", "--expected-canonical", str(canonical_count),
                 "--expected-quarantine", str(quarantine_count), "--required-products",
                 "BINANCE/USDM/PERPETUAL,BINANCE/SPOT/SPOT,OKX/SWAP/PERPETUAL,OKX/SPOT/SPOT,HOSE/EQUITIES/COMMON_STOCK,HNX/VN_DERIVATIVES/FUTURE"],
                timeout=90.0,
            )
            inspect_result = json.loads(inspected.stdout.strip().splitlines()[-1])
            if quarantine_count:
                raise RuntimeError(
                    "real-provider records were quarantined "
                    f"summary={core_summary} details={inspect_result['quarantine_details']}"
                )
            result = {
                "schema": "qdl.phase-a.real-provider-core-certification.v1",
                "status": "PASS",
                "authority": "RUST_SHADOW",
                "runtime_cutover": False,
                "provenance": {
                    "crypto_trade_bbo": "REAL_PROVIDER_WEBSOCKET",
                    "binance_bar": bar_result["provenance"],
                    "okx_bar": "REAL_PROVIDER_WEBSOCKET",
                    "vn_equity": vn_result["provenance"],
                    "vn_equity_capture_boundary": vn_result["capture_boundary"],
                    "vn_derivative": vn_derivative_result["provenance"],
                    "vn_derivative_capture_boundary": vn_derivative_result["capture_boundary"],
                    "generated_market_data": 0,
                },
                "native_runs": native_results,
                "raw_acked": raw_total,
                "canonical_read_committed": canonical_count,
                "duplicates_suppressed": int(core_summary["duplicates"]),
                "quarantines": 0,
                "products": inspect_result["products"],
                "canonical_digest": inspect_result["canonical_digest"],
                "dnse_symbol": symbol,
                "dnse_snapshot_age_seconds_at_test": round(time.time() - float(dnse_payload["timestamp"]), 3),
                "rust_image_id": rust_image_id,
                "v1_health_before": before_health,
            }
        finally:
            cleanup_result = cleanup(env)
            run(["docker", "image", "rm", RUST_IMAGE], check=False, timeout=120.0)
            run(["docker", "image", "rm", PYTHON_IMAGE], check=False, timeout=120.0)
    cleanup_result["v1_topology_unchanged"] = before_topology == v1_topology()
    cleanup_result["v1_health_after"] = v1_health()
    result["cleanup"] = cleanup_result
    if not (
        result.get("status") == "PASS"
        and cleanup_result.get("containers_after") == 0
        and cleanup_result.get("networks_after") == 0
        and cleanup_result.get("volumes_after") == 0
        and cleanup_result["v1_topology_unchanged"]
    ):
        raise RuntimeError(f"real-provider cleanup/invariant failed: {result}")
    EVIDENCE.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "status": result["status"], "raw": result["raw_acked"],
        "canonical": result["canonical_read_committed"],
        "products": result["products"], "cleanup": cleanup_result,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
