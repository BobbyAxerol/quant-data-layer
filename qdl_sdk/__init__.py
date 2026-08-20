"""Stable Python SDK V2 with typed public models and generated transports."""

__version__ = "2.0.0"

from qdl_sdk.client import AsyncDataLayerClient, DataLayerClientV2, WarmupStreamSession
from qdl_sdk.credentials import (
    CallbackCredentialProvider,
    CredentialProvider,
    RotatingJwtCredentialProvider,
    StaticBearerCredential,
)
from qdl_sdk.cursor import CursorCheckpoint, FileCursorStore, MemoryCursorStore
from qdl_sdk.errors import DataLayerError
from qdl_sdk.models import (
    BarRevisionPolicy,
    ControlEvent,
    DataRequirement,
    Feed,
    GapPolicy,
    Grade,
    InstrumentPageResponse,
    InstrumentResponse,
    InstrumentView,
    MarketDataView,
    QuantityUnit,
    RecoveryPolicy,
    SnapshotResponse,
    StalePolicy,
    StreamEvent,
    TradeIdentityKind,
    WarmupResponse,
)
from qdl_sdk.projection import market_data_view_from_stream
from qdl_sdk.transport import GrpcStreamTransport, RestQueryTransport
from qdl_sdk.tls import WorkloadTlsConfig
from qdl_sdk.v1_facade import V1CompatibilityFacade

__all__ = [
    "__version__",
    "AsyncDataLayerClient",
    "BarRevisionPolicy",
    "CallbackCredentialProvider",
    "CursorCheckpoint",
    "ControlEvent",
    "CredentialProvider",
    "DataLayerClientV2",
    "DataLayerError",
    "DataRequirement",
    "FileCursorStore",
    "Feed",
    "GapPolicy",
    "Grade",
    "GrpcStreamTransport",
    "InstrumentPageResponse",
    "InstrumentResponse",
    "InstrumentView",
    "MemoryCursorStore",
    "MarketDataView",
    "market_data_view_from_stream",
    "QuantityUnit",
    "RestQueryTransport",
    "RecoveryPolicy",
    "RotatingJwtCredentialProvider",
    "SnapshotResponse",
    "StalePolicy",
    "StaticBearerCredential",
    "StreamEvent",
    "TradeIdentityKind",
    "WarmupResponse",
    "WorkloadTlsConfig",
    "V1CompatibilityFacade",
    "WarmupStreamSession",
]
