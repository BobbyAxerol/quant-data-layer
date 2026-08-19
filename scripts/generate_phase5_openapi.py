from __future__ import annotations

import argparse
import json
from pathlib import Path

from qdl.api_v2 import create_v2_app


def build_openapi() -> dict:
    # OpenAPI generation is pure and never invokes the injected query service.
    return create_v2_app(
        None, contract_version="2.0.0", authority="INTERNAL_STABLE"
    ).openapi()  # type: ignore[arg-type]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path,
        default=Path("contracts/v2/openapi.snapshot.json"),
    )
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(build_openapi(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
