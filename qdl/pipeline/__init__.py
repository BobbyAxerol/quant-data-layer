"""Dark/shadow data pipelines. Existing V1 ingestion remains authoritative."""

from qdl.pipeline.shadow import ShadowCanonicalPipeline
from qdl.pipeline.quality import QualityPipelineResult, ValidatedCanonicalPipeline

__all__ = [
    "QualityPipelineResult",
    "ShadowCanonicalPipeline",
    "ValidatedCanonicalPipeline",
]
