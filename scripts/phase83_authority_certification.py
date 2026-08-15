#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import tempfile
import time

import yaml

from phase80_broker_certification import (
    BOOTSTRAP,
    PROJECT,
    ROOT,
    add_acls,
    cleanup,
    compose,
    consume,
    create_topic,
    run,
    total_end_offset,
    v1_health,
    v1_topology,
    wait_for_cluster,
    wait_for_replicas,
)


TOPOLOGY = ROOT / "config/phase8/broker-topology.yaml"
CAPABILITIES = ROOT / "config/phase8/capabilities"
CONTRACTS = ROOT / "contracts/proto"
PARTITION_PLAN = ROOT / "config/phase8/candidate-partition-plan.json"
OUTPUT = ROOT / "upgrade/evidence/phase8-authority-rehearsal.json"


def digest_files(paths: list[pathlib.Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        relative = str(path.relative_to(ROOT)).encode()
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--image-digest", required=True)
    args = parser.parse_args()
    if not args.image_digest.startswith("sha256:") or len(args.image_digest) != 71:
        raise ValueError("image digest must be sha256:<64 lowercase hex>")
    topology = yaml.safe_load(TOPOLOGY.read_text())
    capability_digest = digest_files(list(CAPABILITIES.glob("*.yaml")))
    contract_digest = digest_files(list(CONTRACTS.rglob("*.proto")))
    partition_digest = hashlib.sha256(PARTITION_PLAN.read_bytes()).hexdigest()
    v1_before = v1_topology()
    health_before = v1_health()
    cleanup_result: dict[str, object] = {}
    evidence: dict[str, object] = {}
    with tempfile.TemporaryDirectory(prefix="qdl-phase83-certs-") as cert_dir:
        env = os.environ.copy()
        env["QDL_PHASE8_CERT_DIR"] = cert_dir
        run([str(ROOT / "scripts/phase80_generate_tls.sh"), cert_dir], env=env, timeout=120)
        try:
            compose(env, "down", "--volumes", "--remove-orphans", check=False, timeout=120)
            compose(env, "up", "-d", timeout=180)
            wait_for_cluster(env)
            for item in topology["topics"]:
                create_topic(
                    env,
                    item["name"],
                    partitions=item["partitions"],
                    cleanup_policy=item.get("cleanup_policy", "delete"),
                )
            add_acls(env)
            wait_for_replicas(env)
            authority_topic = "qdl.phase8.control.authority.v1"
            canonical_topic = "qdl.phase8.canonical.trade.v2"
            authority_before = total_end_offset(env, authority_topic)
            canonical_before = total_end_offset(env, canonical_topic)
            nonce = hashlib.sha256(str(time.time_ns()).encode()).hexdigest()[:16]
            result = run(
                [
                    "docker", "run", "--rm",
                    "--network", f"{PROJECT}_phase8_shadow",
                    "--read-only", "--security-opt", "no-new-privileges:true",
                    "--mount", f"type=bind,source={cert_dir},target=/certs,readonly",
                    "--entrypoint", "/usr/local/bin/qdl-authority-rehearsal",
                    "--env", f"QDL_KAFKA_BOOTSTRAP_SERVERS={BOOTSTRAP}",
                    "--env", "QDL_KAFKA_CERT_ROOT=/certs",
                    "--env", f"QDL_AUTHORITY_TOPIC={authority_topic}",
                    "--env", f"QDL_CANONICAL_TOPIC={canonical_topic}",
                    "--env", f"QDL_AUTHORITY_NONCE={nonce}",
                    "--env", f"QDL_CANDIDATE_IMAGE_DIGEST={args.image_digest}",
                    "--env", f"QDL_CAPABILITY_DIGEST={capability_digest}",
                    "--env", f"QDL_CONTRACT_DIGEST={contract_digest}",
                    "--env", f"QDL_PARTITION_PLAN_DIGEST={partition_digest}",
                    args.image,
                ],
                timeout=120,
            )
            runtime = json.loads(result.stdout.strip().splitlines()[-1])
            authority_after = total_end_offset(env, authority_topic)
            canonical_after = total_end_offset(env, canonical_topic)
            offsets_exact = (
                authority_after - authority_before == 3
                and canonical_after - canonical_before == 3
            )
            if runtime.get("status") != "PASS" or not offsets_exact:
                raise RuntimeError(
                    f"authority rehearsal failed runtime={runtime} offsets_exact={offsets_exact}"
                )
            compose(env, "restart", "kafka1", "kafka2", "kafka3", timeout=120)
            wait_for_cluster(env)
            wait_for_replicas(env)
            records = consume(env, authority_topic, 3, f"phase83-restart-{nonce}")
            decoded = [json.loads(item) for item in records]
            revisions = [item["revision"] for item in decoded]
            modes = [item["mode"] for item in decoded]
            persisted_after_restart = revisions == [1, 2, 3] and modes == [
                "RUST_SHADOW", "RUST_CANARY", "RUST_SHADOW"
            ]
            if not persisted_after_restart:
                raise RuntimeError("authority records did not survive restart in order")
            evidence = {
                "schema": "qdl.phase8.authority-rehearsal.v1",
                "status": "PASS",
                "candidate_image_digest": args.image_digest,
                "capability_manifest_digest": capability_digest,
                "contract_digest": contract_digest,
                "partition_plan_digest": partition_digest,
                "runtime": runtime,
                "authority_offset_delta": authority_after - authority_before,
                "canonical_shadow_offset_delta": canonical_after - canonical_before,
                "rejected_write_offset_delta": 0,
                "persisted_after_full_broker_restart": persisted_after_restart,
                "authority_topic_cleanup_policy": "compact",
                "final_authority": "RUST_SHADOW",
                "public_writes": 0,
                "legacy_writes": 0,
            }
        finally:
            cleanup_result = cleanup(env)
    v1_after = v1_topology()
    health_after = v1_health()
    cleanup_result.update(
        {
            "v1_health_before": health_before,
            "v1_health_after": health_after,
            "v1_topology_unchanged": v1_before == v1_after,
        }
    )
    evidence["cleanup"] = cleanup_result
    if (
        health_before != 200
        or health_after != 200
        or not cleanup_result["v1_topology_unchanged"]
        or any(
            cleanup_result[key]
            for key in ("containers_after", "networks_after", "volumes_after")
        )
    ):
        raise RuntimeError(f"Phase 8.3 cleanup/V1 invariant failed: {cleanup_result}")
    OUTPUT.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "status": "PASS",
                "final_authority": "RUST_SHADOW",
                "image_digest": args.image_digest,
                "cleanup": cleanup_result,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
