"""Stable Data Layer 2.0.0 query edge with mandatory workload mTLS."""

import asyncio

from qdl.runtime.stable import serve_stable_query


if __name__ == "__main__":
    asyncio.run(serve_stable_query())
