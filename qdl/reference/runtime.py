"""Provider-neutral assembly for the bounded V2 reference-data capability.

The query service receives only a ``ReferenceBatch`` and a source-id resolver.
This module is the one place that maps declared venue/market capability edges
to those runtime dependencies.  Constructing it opens no socket and performs
no provider call; deployment must opt in through the stable-runtime feature
gate before the public endpoint becomes usable.
"""

from __future__ import annotations

from dataclasses import dataclass

from qdl.adapters.binance.reference import BinanceUsdmReferenceAdapter
from qdl.adapters.okx.client import OkxRestClient
from qdl.adapters.okx.reference import OkxSwapReferenceAdapter
from qdl.domain.instrument import InstrumentRecord
from qdl.query.entitlement import (
    AccessPurpose,
    DataProduct,
    EntitlementGrant,
)
from qdl.reference.batch import ReferenceBatch


_REFERENCE_SOURCES: dict[tuple[str, str], str] = {
    ("BINANCE", "USDM"): "qdl-reference-binance-usdm-v1",
    ("OKX", "SWAP"): "qdl-reference-okx-swap-v1",
    ("OKX", "FUTURES"): "qdl-reference-okx-futures-v1",
}
_REFERENCE_LICENSE_REVISION = "qdl-reference-provider-v1"


@dataclass(frozen=True, slots=True)
class ReferenceRuntime:
    """The default read-only reference provider graph for one V2 query role."""

    batch: ReferenceBatch

    def source_id_for(self, record: InstrumentRecord) -> str:
        key = (record.identity.venue, record.identity.market)
        try:
            return _REFERENCE_SOURCES[key]
        except KeyError as error:
            raise ValueError(
                "reference data has no approved provider edge for "
                f"{record.identity.venue}/{record.identity.market}"
            ) from error

    def entitlement_grants(self) -> tuple[EntitlementGrant, ...]:
        """Reference data is research/alpha input, never execution authority."""

        return tuple(
            EntitlementGrant(
                source_id=source_id,
                license_revision=_REFERENCE_LICENSE_REVISION,
                purposes=frozenset({
                    AccessPurpose.INTERNAL_ALPHA,
                    AccessPurpose.INTERNAL_RESEARCH,
                }),
                products=frozenset({
                    DataProduct.CANONICAL_HISTORY,
                    DataProduct.CANONICAL_SNAPSHOT,
                }),
                valid_from_ns=0,
            )
            for source_id in sorted(_REFERENCE_SOURCES.values())
        )


def build_default_reference_runtime() -> ReferenceRuntime:
    """Build supported public-provider adapters without making provider I/O.

    Adding a venue later is one registry entry plus an adapter and capability
    tests.  Query/SDK callers remain tied to canonical instrument identity and
    never need a venue branch.
    """

    okx = OkxSwapReferenceAdapter(OkxRestClient())
    return ReferenceRuntime(
        ReferenceBatch({
            ("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(),
            ("OKX", "SWAP"): okx,
            ("OKX", "FUTURES"): okx,
        })
    )
