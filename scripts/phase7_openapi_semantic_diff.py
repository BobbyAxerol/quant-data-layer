from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


HTTP_METHODS = frozenset({"get", "post", "put", "patch", "delete"})


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _operations(document: dict) -> dict[str, dict]:
    return {
        f"{method.upper()} {path}": operation
        for path, methods in document.get("paths", {}).items()
        for method, operation in methods.items()
        if method.lower() in HTTP_METHODS
    }


def _enum_values(value, *, path: str = "$") -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    if isinstance(value, dict):
        if isinstance(value.get("enum"), list):
            found[path] = {str(item) for item in value["enum"]}
        for key, item in value.items():
            found.update(_enum_values(item, path=f"{path}.{key}"))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.update(_enum_values(item, path=f"{path}[{index}]"))
    return found


def compare(baseline: dict, candidate: dict) -> dict:
    old_operations = _operations(baseline)
    new_operations = _operations(candidate)
    removed_operations = sorted(set(old_operations) - set(new_operations))
    removed_responses: list[str] = []
    security_changes: list[str] = []
    newly_required_parameters: list[str] = []
    for name in sorted(set(old_operations) & set(new_operations)):
        old = old_operations[name]
        new = new_operations[name]
        for code in set(old.get("responses", {})) - set(new.get("responses", {})):
            removed_responses.append(f"{name} response={code}")
        if old.get("security") != new.get("security"):
            security_changes.append(name)
        old_parameters = {
            (item.get("in"), item.get("name")): bool(item.get("required"))
            for item in old.get("parameters", [])
        }
        for item in new.get("parameters", []):
            key = (item.get("in"), item.get("name"))
            if item.get("required") and old_parameters.get(key) is False:
                newly_required_parameters.append(f"{name} {key[0]}={key[1]}")

    old_schemas = set(baseline.get("components", {}).get("schemas", {}))
    new_schemas = set(candidate.get("components", {}).get("schemas", {}))
    removed_schemas = sorted(old_schemas - new_schemas)
    old_enums = _enum_values(baseline)
    new_enums = _enum_values(candidate)
    removed_enum_values = {
        path: sorted(values - new_enums.get(path, set()))
        for path, values in old_enums.items()
        if values - new_enums.get(path, set())
    }
    hard_breaks = {
        "removed_operations": removed_operations,
        "removed_responses": sorted(removed_responses),
        "removed_schemas": removed_schemas,
        "removed_enum_values": removed_enum_values,
    }
    return {
        "status": (
            "PASS_PRE_BETA_FREEZE"
            if not any(hard_breaks.values())
            else "FAIL_BREAKING_CHANGE"
        ),
        "hard_breaks": hard_breaks,
        "intentional_pre_freeze_hardening": {
            "security_changes": security_changes,
            "newly_required_parameters": newly_required_parameters,
        },
        "operation_count": len(new_operations),
        "schema_count": len(new_schemas),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-git-ref", required=True)
    parser.add_argument(
        "--candidate", type=Path, default=Path("contracts/v2/openapi.snapshot.json")
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    baseline_bytes = subprocess.check_output([
        "git", "show", f"{args.baseline_git_ref}:contracts/v2/openapi.snapshot.json"
    ])
    candidate_bytes = args.candidate.read_bytes()
    result = compare(json.loads(baseline_bytes), json.loads(candidate_bytes))
    result.update({
        "schema": "qdl.phase7.openapi-semantic-diff.v1",
        "baseline_git_ref": args.baseline_git_ref,
        "baseline_sha256": _sha256(baseline_bytes),
        "candidate_sha256": _sha256(candidate_bytes),
    })
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    if result["status"] != "PASS_PRE_BETA_FREEZE":
        raise SystemExit(json.dumps(result["hard_breaks"], sort_keys=True))


if __name__ == "__main__":
    main()
