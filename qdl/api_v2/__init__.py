"""Provider-neutral V2 REST boundary; provider diagnostics remain internal."""

from qdl.api_v2.router import create_v2_app, router

__all__ = ["create_v2_app", "router"]
