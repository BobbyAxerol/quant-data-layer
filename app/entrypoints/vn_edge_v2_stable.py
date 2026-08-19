"""Stable DNSE vendor acquisition edge."""

import asyncio

from qdl.adapters.vn.stable_edge import main


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
