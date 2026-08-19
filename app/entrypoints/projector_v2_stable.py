"""Kafka-authoritative Data Layer 2.0.0 stable projector."""

import asyncio

from qdl.runtime.stable import serve_stable_projector


if __name__ == "__main__":
    asyncio.run(serve_stable_projector())
