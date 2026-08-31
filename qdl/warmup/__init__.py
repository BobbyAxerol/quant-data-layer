"""Provider-neutral warmup contracts.

Implementation modules are imported explicitly so query-domain imports do not
form a package-initialisation cycle through the handoff result types.
"""

from .contracts import (
    IntervalSourcePolicy,
    WarmupSpecification,
    WarmupTimeRange,
)
__all__ = [
    "IntervalSourcePolicy",
    "WarmupSpecification",
    "WarmupTimeRange",
]
