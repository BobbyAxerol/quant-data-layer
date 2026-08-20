"""Compatibility import for the provider-owned stable VN acquisition edge."""

from qdl.adapters.vn.stable_edge import (
    StableDnseVendorEdge,
    build_from_environment,
    main,
)

__all__ = ["StableDnseVendorEdge", "build_from_environment", "main"]
