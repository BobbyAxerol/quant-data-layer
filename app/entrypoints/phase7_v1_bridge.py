import asyncio

from qdl.runtime.canary_bridge import serve_v1_readonly_bridge


if __name__ == "__main__":
    asyncio.run(serve_v1_readonly_bridge())
