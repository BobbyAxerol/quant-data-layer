from __future__ import annotations

from dataclasses import dataclass

from qdl.ingestion.contracts import FeedType


@dataclass(frozen=True)
class AdapterDeclaration:
    provider: str
    venue: str
    markets: frozenset[str]
    feeds: frozenset[FeedType]
    production_certified: bool

    def require(self, *, market: str, feed: FeedType, production: bool) -> None:
        if market.upper() not in self.markets:
            raise RuntimeError(f"adapter does not declare market={market}")
        if feed not in self.feeds:
            raise RuntimeError(f"adapter does not declare feed={feed.value}")
        if production and not self.production_certified:
            raise RuntimeError("adapter is extension-tested but not production-certified")
