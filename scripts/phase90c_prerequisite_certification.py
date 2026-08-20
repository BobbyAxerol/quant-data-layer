#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
import sys
import time
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from qdl.certification.prerequisites import (  # noqa: E402
    CandidateSlice,
    PrerequisitePolicy,
    evaluate_prerequisites,
    load_inventory,
)


def _v1_snapshot() -> dict[str, object]:
    completed = subprocess.run(
        ["docker", "inspect", "data_layer_service", "--format", "{{json .}}"],
        text=True, capture_output=True, check=False,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        return {"present": False}
    payload = json.loads(completed.stdout)
    return {
        "present": True,
        "id": payload["Id"],
        "image": payload["Config"]["Image"],
        "started_at": payload["State"]["StartedAt"],
        "restart_count": payload["RestartCount"],
        "networks": sorted(payload["NetworkSettings"]["Networks"]),
        "mounts": sorted((item["Source"], item["Destination"], item["RW"]) for item in payload["Mounts"]),
    }


def _v1_health() -> int:
    try:
        with urllib.request.urlopen("http://127.0.0.1:8100/v1/health", timeout=5) as response:
            return response.status
    except Exception:
        return 0


def _write_report(path: pathlib.Path, result: dict[str, object]) -> None:
    blocked = [item for item in result["gates"] if not item["passed"]]
    lines = [
        "# Phase 9.0-C Production Prerequisite Report",
        "",
        f"Decision: `{result['decision']}`",
        "",
        "## Candidate",
        "",
        f"- Slice: `{result['slice_id']}`",
        f"- Candidate digest: `{result['candidate_digest']}`",
        f"- Authority: `RUST_SHADOW`; V1 unchanged: `{result['v1_unchanged']}`",
        "",
        "## Gate Summary",
        "",
        f"- Passed: `{result['passed']}`",
        f"- Blocked: `{result['blocked']}`",
        "",
    ]
    if blocked:
        lines += ["## Blocking Evidence", ""]
        lines += [
            f"- `{item['gate_id']}`: `{item['reason']}` "
            f"(observed `{item['observed_scope']}`, required `{item['required_scope']}`)"
            for item in blocked
        ]
        lines += [
            "",
            "These are real infrastructure/operator blockers. Same-host fixtures or",
            "local rehearsals must not be relabeled to close them.",
        ]
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", default="config/phase9/production-prerequisite-policy.yaml")
    parser.add_argument("--candidate", default="config/phase9/candidate-slice.yaml")
    parser.add_argument("--inventory", default="config/phase9/evidence-inventory.local.yaml")
    parser.add_argument("--output", default="upgrade/evidence/phase90c-production-prerequisites.json")
    parser.add_argument("--report", default="upgrade/evidence/PHASE90C_PRODUCTION_PREREQUISITES_REPORT.md")
    parser.add_argument("--expect", choices=("GO", "NO_GO_EXTERNAL"), required=True)
    args = parser.parse_args()

    policy = PrerequisitePolicy.load(ROOT / args.policy)
    candidate = CandidateSlice.load(ROOT / args.candidate)
    inventory_path = ROOT / args.inventory
    inventory = load_inventory(inventory_path)
    before = _v1_snapshot()
    health_before = _v1_health()
    decision = evaluate_prerequisites(
        policy, candidate, inventory, repository_root=ROOT, now_ns=time.time_ns()
    ).as_mapping()
    after = _v1_snapshot()
    health_after = _v1_health()
    inventory_digest = hashlib.sha256(inventory_path.read_bytes()).hexdigest()
    bundle_seed = f"{candidate.digest}:{policy.revision}:{inventory_digest}"
    result = {
        **decision,
        "bundle_id": str(uuid.uuid5(uuid.NAMESPACE_URL, bundle_seed)),
        "slice_id": candidate.payload["slice_id"],
        "authority_state": candidate.payload["authority_state"],
        "inventory_sha256": inventory_digest,
        "issued_at_ns": time.time_ns(),
        "v1_health_before": health_before,
        "v1_health_after": health_after,
        "v1_unchanged": before == after and health_before == health_after == 200,
        "v1_before": before,
        "v1_after": after,
        "production_mutations": 0,
    }
    if not result["v1_unchanged"]:
        raise RuntimeError("V1 topology or health changed during prerequisite evaluation")
    output = ROOT / args.output
    report = ROOT / args.report
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    _write_report(report, result)
    print(json.dumps({
        "decision": result["decision"], "passed": result["passed"],
        "blocked": result["blocked"], "v1_unchanged": result["v1_unchanged"],
    }, sort_keys=True))
    return 0 if result["decision"] == args.expect else 2


if __name__ == "__main__":
    raise SystemExit(main())
