from qdl.canary.phase92 import (
    AcceptedHandoff,
    HandoffDirection,
    PrimaryAuthorization,
    PrimaryAuthorizationMode,
    ProductionPrimaryAuthorizer,
    TerminalOwnerCheckpoint,
)
from qdl.canary.phase93 import (
    AuthorityRegistrySnapshot,
    ClosureApproval,
    ClosureAuthorization,
    ConsumerCheckpoint,
    ConsumerRegistrySnapshot,
    DecommissionDecision,
    DecommissionRequest,
    ExpansionManifest,
    ExpansionType,
    HoldScope,
    HoldStatus,
    PrimaryHoldDecision,
    PrimaryHoldEvaluator,
    PrimaryHoldIdentity,
    PrimaryHoldObservation,
    PrimaryHoldPolicy,
    ProductionClosureAuthorizer,
    RollbackRehearsalEvidence,
    RollbackWindowClosure,
    assess_decommission,
)
from qdl.canary.phase9 import (
    CanaryAuthorization,
    CanaryAuthorizationMode,
    CanaryGuardrailDecision,
    CanaryGuardrailEngine,
    CanaryGuardrailPolicy,
    CanaryObservation,
    ProductionCanaryAuthorizer,
)

__all__ = [
    "AcceptedHandoff",
    "AuthorityRegistrySnapshot",
    "ClosureApproval",
    "ClosureAuthorization",
    "ConsumerCheckpoint",
    "ConsumerRegistrySnapshot",
    "DecommissionDecision",
    "DecommissionRequest",
    "ExpansionManifest",
    "ExpansionType",
    "HoldScope",
    "HoldStatus",
    "PrimaryHoldDecision",
    "PrimaryHoldEvaluator",
    "PrimaryHoldIdentity",
    "PrimaryHoldObservation",
    "PrimaryHoldPolicy",
    "ProductionClosureAuthorizer",
    "RollbackRehearsalEvidence",
    "RollbackWindowClosure",
    "assess_decommission",
    "HandoffDirection",
    "PrimaryAuthorization",
    "PrimaryAuthorizationMode",
    "ProductionPrimaryAuthorizer",
    "TerminalOwnerCheckpoint",
    "CanaryAuthorization",
    "CanaryAuthorizationMode",
    "CanaryGuardrailDecision",
    "CanaryGuardrailEngine",
    "CanaryGuardrailPolicy",
    "CanaryObservation",
    "CanaryRunResult",
    "DeterministicPaperSignalState",
    "PaperAlphaCanary",
    "ProductionCanaryAuthorizer",
    "sdk_requirement",
]


_CONSUMER_EXPORTS = {
    "CanaryRunResult",
    "DeterministicPaperSignalState",
    "PaperAlphaCanary",
    "sdk_requirement",
}


def __getattr__(name: str):
    if name not in _CONSUMER_EXPORTS:
        raise AttributeError(name)
    from qdl.canary import consumer

    value = getattr(consumer, name)
    globals()[name] = value
    return value
