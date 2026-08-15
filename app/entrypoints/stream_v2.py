"""Isolated active/passive Phase 7 beta stream entrypoint."""

import asyncio

from qdl.runtime.beta_stream import serve_beta_stream


if __name__ == "__main__":
    asyncio.run(serve_beta_stream())
