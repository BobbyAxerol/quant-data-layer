"""Dark/shadow data pipelines. Existing V1 ingestion remains authoritative."""

from qdl.pipeline.shadow import ShadowCanonicalPipeline

__all__ = ["ShadowCanonicalPipeline"]
