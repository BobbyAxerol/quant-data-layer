#!/usr/bin/env python3
"""Prepare an additive sandbox read packet; never apply runtime or issue credentials."""
import argparse
import hashlib
import json
from pathlib import Path

import yaml

from qdl.runtime.production_catalog import ProductionDemandManifest
from qdl.runtime.sandbox_extension import prepare_sandbox_extension


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("catalog", "acquisition", "manifest", "binding", "demand", "metadata"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--template-uid", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise ValueError("new output directory required, never overwrite a sealed packet")
    files = {name: getattr(args, name).read_bytes()
             for name in ("catalog", "acquisition", "manifest", "binding", "demand", "metadata")}
    packet = prepare_sandbox_extension(
        **{name: yaml.safe_load(files[name]) for name in ("catalog", "acquisition", "manifest", "binding")},
        demand=ProductionDemandManifest.load_many([args.demand]),
        okx_rows=json.loads(files["metadata"]), template_uid=args.template_uid,
    )
    # Build every output before creating the destination; no partial validation.
    outputs = {name + ".json": json.dumps(value, indent=2, sort_keys=True).encode() + b"\n"
               for name, value in packet.items()}
    receipt = {"status": "PREPARED_NOT_ACTIVATED_NOT_CERTIFIED", "runtime_mutations": 0,
               "input_sha256": {name: hashlib.sha256(data).hexdigest() for name, data in files.items()},
               "output_sha256": {name: hashlib.sha256(data).hexdigest() for name, data in outputs.items()},
               "binding_sha256": packet["binding"]["binding_sha256"],
               "consumer_manifest_revision": packet["manifest"]["metadata"]["revision"],
               "requirements": len(packet["manifest"]["spec"]["requirements"])}
    args.output_dir.mkdir(mode=0o700, parents=True)
    for name, data in outputs.items():
        path = args.output_dir / name
        path.write_bytes(data)
        path.chmod(0o600)
    (args.output_dir / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()
