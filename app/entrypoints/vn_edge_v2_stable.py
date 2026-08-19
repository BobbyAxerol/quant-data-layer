"""Stable DNSE vendor acquisition edge."""

import asyncio

from qdl.runtime.stable_vn_edge import main


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
