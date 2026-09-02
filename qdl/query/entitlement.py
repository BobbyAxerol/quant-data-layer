from __future__ import annotations

from dataclasses import dataclass
from qdl._compat import StrEnum


class AccessPurpose(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    INTERNAL_EXECUTION = "INTERNAL_EXECUTION"
    INTERNAL_ALPHA = "INTERNAL_ALPHA"
    INTERNAL_RESEARCH = "INTERNAL_RESEARCH"
    EXTERNAL_REDISTRIBUTION = "EXTERNAL_REDISTRIBUTION"


class DataProduct(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    CANONICAL_SNAPSHOT = "CANONICAL_SNAPSHOT"
    CANONICAL_HISTORY = "CANONICAL_HISTORY"
    RAW_EVENTS = "RAW_EVENTS"


@dataclass(frozen=True)
class EntitlementGrant:
    source_id: str
    license_revision: str
    purposes: frozenset[AccessPurpose]
    products: frozenset[DataProduct]
    valid_from_ns: int
    valid_to_ns: int | None = None

    def __post_init__(self) -> None:
        if not self.source_id.strip() or not self.license_revision.strip():
            raise ValueError("source_id and license_revision are required")
        if not self.purposes or not self.products:
            raise ValueError("entitlement purposes and products cannot be empty")
        if self.valid_from_ns < 0:
            raise ValueError("valid_from_ns must be non-negative")
        if self.valid_to_ns is not None and self.valid_to_ns <= self.valid_from_ns:
            raise ValueError("valid_to_ns must be after valid_from_ns")


@dataclass(frozen=True)
class AccessDecision:
    allowed: bool
    reason_code: str
    license_revision: str | None


class EntitlementPolicy:
    """Fail-closed source licensing policy independent of provider capability."""

    def __init__(self, grants: tuple[EntitlementGrant, ...]):
        keys = [(grant.source_id, grant.license_revision) for grant in grants]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate source entitlement revision")
        self._grants = grants

    def with_grants(self, grants: tuple[EntitlementGrant, ...]) -> "EntitlementPolicy":
        """Return one policy with additive, independently named grants.

        Runtime assembly must not reach into the private grant collection or
        mutate a policy already shared by a V1-compatible query path.  The
        constructor keeps the duplicate `(source_id, license_revision)` guard
        intact, so a provider capability cannot silently override an existing
        entitlement revision.
        """

        return EntitlementPolicy(self._grants + tuple(grants))

    def authorize(
        self,
        *,
        source_id: str,
        purpose: AccessPurpose,
        product: DataProduct,
        at_ns: int,
    ) -> AccessDecision:
        if not source_id.strip() or at_ns < 0:
            raise ValueError("valid source_id and at_ns are required")
        candidates = sorted(
            (
                grant
                for grant in self._grants
                if grant.source_id == source_id
                and grant.valid_from_ns <= at_ns
                and (grant.valid_to_ns is None or at_ns < grant.valid_to_ns)
            ),
            key=lambda grant: grant.valid_from_ns,
            reverse=True,
        )
        if not candidates:
            return AccessDecision(False, "NO_ACTIVE_ENTITLEMENT", None)
        grant = candidates[0]
        if purpose not in grant.purposes:
            return AccessDecision(False, "PURPOSE_NOT_ALLOWED", grant.license_revision)
        if product not in grant.products:
            return AccessDecision(False, "PRODUCT_NOT_ALLOWED", grant.license_revision)
        return AccessDecision(True, "ALLOWED", grant.license_revision)
