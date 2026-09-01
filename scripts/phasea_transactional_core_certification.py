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
os.environ.setdefault("QDL_PHASE8_PROJECT", "qdl_phasea_transactional")

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


RUST_IMAGE = "qdl-phase-a-rust:test"
PYTHON_IMAGE = "data-layer:phase-a-test"
EVIDENCE = ROOT / "upgrade/evidence/phase-a-transactional-core.json"
RAW_TOPIC = "qdl.phase8.phasea.raw.v1"
CANONICAL_TOPIC = "qdl.phase8.phasea.canonical.v2"
QUARANTINE_TOPIC = "qdl.phase8.phasea.quarantine.v1"
GROUP_ID = "phase8-phase-a-core"


def core_acls(env: dict[str, str]) -> None:
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
        arguments = list(common)
        for operation in operations:
            arguments.extend(("--operation", operation))
        arguments.extend(("--topic", topic))
        kafka(env, "kafka-acls.sh", *arguments)
    kafka(
        env, "kafka-acls.sh", *common,
        "--operation", "READ", "--group", GROUP_ID,
    )
    kafka(
        env, "kafka-acls.sh", *common,
        "--operation", "IdempotentWrite", "--cluster",
    )
    kafka(
        env, "kafka-acls.sh", *common,
        "--operation", "WRITE", "--operation", "DESCRIBE",
        "--transactional-id", "qdl-phase-a-", "--resource-pattern-type", "prefixed",
    )


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


def authority(image_id: str) -> dict:
    digest = image_id.removeprefix("sha256:")
    static = hashlib.sha256(b"phase-a-multivenue").hexdigest()
    return {
        "schema": "qdl.authority-record.v1",
        "slice_id": "phase-a-multivenue-shadow",
        "revision": 1,
        "mode": "RUST_SHADOW",
        "candidate_image_digest": f"sha256:{digest}",
        "capability_manifest_digest": hashlib.sha256(
            (ROOT / "config/v2/stable-capabilities.yaml").read_bytes()
        ).hexdigest(),
        "contract_digest": hashlib.sha256(
            (ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto").read_bytes()
        ).hexdigest(),
        "partition_plan_digest": static,
        "public_write_allowed": False,
        "legacy_write_allowed": False,
        "approved_by": "phase-a-isolated-certification",
        "effective_at_ns": time.time_ns(),
    }


def bindings():
    common = {"instrument_revision": 1, "instrument_catalog_revision": 3,
              "normalizer_version": "qdl-rust-core/2.0.0"}
    return [
        {**common, "provider": "BINANCE_DIRECT", "venue": "BINANCE", "market": "USDM",
         "product_type": "PERPETUAL", "native_symbol": "BTCUSDT", "native_channel": "trade",
         "provider_kind": "binance_usdm_trade", "instrument_uid": "uid-binance-btcusdt",
         "instrument_id": "BINANCE.USDM.PERPETUAL.BTC-USDT", "source_id": "binance-usdm-trade",
         "source_role": "PRIMARY", "sequence_policy": "MONOTONIC"},
        {**common, "provider": "OKX_DIRECT", "venue": "OKX", "market": "SWAP",
         "product_type": "PERPETUAL", "native_symbol": "BTC-USDT-SWAP", "native_channel": "trades",
         "provider_kind": "okx_trade", "instrument_uid": "uid-okx-btc-swap",
         "instrument_id": "OKX.SWAP.PERPETUAL.BTC-USDT", "source_id": "okx-swap-trade",
         "source_role": "PRIMARY", "sequence_policy": "MONOTONIC"},
        {**common, "provider": "OKX_DIRECT", "venue": "OKX", "market": "SWAP",
         "product_type": "PERPETUAL", "native_symbol": "BTC-USDT-SWAP", "native_channel": "bbo-tbt",
         "provider_kind": "okx_bbo", "instrument_uid": "uid-okx-btc-swap",
         "instrument_id": "OKX.SWAP.PERPETUAL.BTC-USDT", "source_id": "okx-swap-bbo",
         "source_role": "PRIMARY", "sequence_policy": "CONTIGUOUS"},
        {**common, "provider": "DNSE_DIRECT", "venue": "HNX", "market": "VN_DERIVATIVES",
         "product_type": "FUTURE", "native_symbol": "VN30F1M", "native_channel": "trades",
         "provider_kind": "dnse_trade", "instrument_uid": "uid-hnx-vn30f1m",
         "instrument_id": "HNX.VN_DERIVATIVES.FUTURE.VN30F1M", "source_id": "dnse-vn30-trade",
         "source_role": "PRIMARY", "sequence_policy": "NONE"},
        {**common, "provider": "VNSTOCK", "venue": "HOSE", "market": "EQUITIES",
         "product_type": "COMMON_STOCK", "native_symbol": "FPT", "native_channel": "ohlcv/1m",
         "provider_kind": "vnstock_bar", "instrument_uid": "uid-hose-fpt",
         "instrument_id": "HOSE.EQUITIES.COMMON_STOCK.FPT", "source_id": "vnstock-fpt-bar",
         "source_role": "SECONDARY", "sequence_policy": "NONE"},
    ]


def docker_run(image: str, certs: pathlib.Path, command: list[str], *, entrypoint=None, mounts=()):
    args = [
        "docker", "run", "--rm", "--network", f"{PROJECT}_phase8_shadow",
        "--read-only", "--security-opt", "no-new-privileges:true",
        "--mount", f"type=bind,source={certs},target=/certs,readonly",
        "--env", f"QDL_KAFKA_BOOTSTRAP_SERVERS={BOOTSTRAP}",
        "--env", "QDL_KAFKA_CERT_ROOT=/certs",
        "--env", "QDL_KAFKA_CLIENT_ID=qdl-phase-a",
        "--env", f"QDL_KAFKA_GROUP_ID={GROUP_ID}",
    ]
    for mount in mounts:
        args.extend(("--mount", mount))
    if entrypoint:
        args.extend(("--entrypoint", entrypoint))
    args.append(image)
    args.extend(command)
    return run(args, timeout=120.0)


def main() -> int:
    before_topology = v1_topology()
    before_health = v1_health()
    result = {}
    cleanup_result = {}
    with tempfile.TemporaryDirectory(prefix="qdl-phase-a-") as directory:
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
            compose(env, "up", "-d", timeout=180.0)
            wait_for_cluster(env)
            for topic in (RAW_TOPIC, CANONICAL_TOPIC, QUARANTINE_TOPIC):
                create_topic(env, topic, partitions=3)
            add_acls(env)
            core_acls(env)
            wait_for_replicas(env)

            authority_record = authority(rust_image_id)
            config = {
                "core": {
                    "canonical_stream": CANONICAL_TOPIC,
                    "quarantine_stream": QUARANTINE_TOPIC,
                    "allow_test_provenance": True,
                    "dedup_capacity": 10000,
                    "bindings": bindings(),
                },
                "raw_topics": [RAW_TOPIC],
                "authority": authority_record,
                "shard_id": "phase-a-core-shard-1",
                "transactional_id": "qdl-phase-a-core-shard-1",
                "batch_size": 8,
                "batch_wait_ms": 100,
                "max_events": 8,
                "metrics_every_batches": 1,
            }
            config_path = temp / "core-config.json"
            config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
            config_path.chmod(0o644)

            seeded = docker_run(
                PYTHON_IMAGE, producer_certs,
                ["python", "scripts/phasea_seed_raw.py", "--bootstrap", BOOTSTRAP,
                 "--topic", RAW_TOPIC, "--cert-root", "/certs"],
            )
            seed_result = json.loads(seeded.stdout.strip().splitlines()[-1])
            compose(env, "stop", "kafka3", timeout=60.0)
            one_replica_loss_started_at = time.monotonic()
            core_result = docker_run(
                RUST_IMAGE, core_certs, ["/config/core-config.json"],
                entrypoint="/usr/local/bin/qdl-realtime-core",
                mounts=(f"type=bind,source={config_path},target=/config/core-config.json,readonly",),
            )
            core_summary = json.loads(core_result.stdout.strip().splitlines()[-1])
            one_replica_loss_elapsed_seconds = round(
                time.monotonic() - one_replica_loss_started_at, 6
            )
            compose(env, "start", "kafka3", timeout=90.0)
            wait_for_cluster(env)
            wait_for_replicas(env)
            inspected = docker_run(
                PYTHON_IMAGE, consumer_certs,
                ["python", "scripts/phasea_inspect_kafka.py", "--bootstrap", BOOTSTRAP,
                 "--canonical-topic", CANONICAL_TOPIC, "--quarantine-topic", QUARANTINE_TOPIC,
                 "--cert-root", "/certs", "--expected-canonical", "6",
                 "--expected-quarantine", "1", "--required-products",
                 "BINANCE/USDM/PERPETUAL,OKX/SWAP/PERPETUAL,HNX/VN_DERIVATIVES/FUTURE,HOSE/EQUITIES/COMMON_STOCK"],
            )
            inspect_result = json.loads(inspected.stdout.strip().splitlines()[-1])
            if seed_result.get("raw_acked") != 8:
                raise RuntimeError(f"raw seed ACK count mismatch: {seed_result}")
            if core_summary.get("processed") != 8 or core_summary.get("duplicates") != 1:
                raise RuntimeError(f"transactional core summary mismatch: {core_summary}")
            if inspect_result.get("canonical") != 6 or inspect_result.get("quarantines") != 1:
                raise RuntimeError(f"read_committed output mismatch: {inspect_result}")
            result = {
                "schema": "qdl.phase-a.transactional-core-certification.v1",
                "status": "PASS",
                "authority": "RUST_SHADOW",
                "runtime_cutover": False,
                "raw_acked": 8,
                "canonical_read_committed": 6,
                "quarantine_read_committed": 1,
                "duplicates_suppressed": 1,
                "one_replica_loss_transaction_committed": True,
                "one_replica_loss_elapsed_seconds": one_replica_loss_elapsed_seconds,
                "replicas_restored_before_inspection": True,
                "venues": inspect_result["venues"],
                "quarantine_reasons": inspect_result["quarantine_reasons"],
                "canonical_digest": inspect_result["canonical_digest"],
                "rust_image_id": rust_image_id,
                "python_image_id": run(["docker", "image", "inspect", PYTHON_IMAGE, "--format", "{{.Id}}"]).stdout.strip(),
                "v1_health_before": before_health,
            }
        finally:
            cleanup_result = cleanup(env)
            run(["docker", "image", "rm", RUST_IMAGE], check=False, timeout=120.0)
            run(["docker", "image", "rm", PYTHON_IMAGE], check=False, timeout=120.0)
    after_topology = v1_topology()
    after_health = v1_health()
    cleanup_result["v1_topology_unchanged"] = before_topology == after_topology
    cleanup_result["v1_health_after"] = after_health
    result["cleanup"] = cleanup_result
    if not (
        result.get("status") == "PASS"
        and cleanup_result.get("containers_after") == 0
        and cleanup_result.get("networks_after") == 0
        and cleanup_result.get("volumes_after") == 0
        and cleanup_result["v1_topology_unchanged"]
    ):
        raise RuntimeError(f"Phase A transactional cleanup/invariant failed: {result}")
    EVIDENCE.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "status": result["status"], "canonical": result["canonical_read_committed"],
        "quarantine": result["quarantine_read_committed"],
        "cleanup": cleanup_result,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
