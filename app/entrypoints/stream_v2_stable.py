"""Isolated Data Layer 2.0.0 active/passive stream edge."""

import asyncio

from qdl.runtime.stable import serve_stable_stream


if __name__ == "__main__":
    asyncio.run(serve_stable_stream())
