from __future__ import annotations


class DataLayerError(RuntimeError):
    def __init__(
        self,
        code: str,
        detail: str,
        *,
        retryable: bool = False,
        retry_after_ms: int | None = None,
        diagnostics: dict | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.retryable = retryable
        self.retry_after_ms = retry_after_ms
        # Query's quality at the refusal, when the server sent it.
        self.diagnostics = diagnostics
        # Typed refusal reason from the ``X-QDL-Refusal-Reason`` response
        # header (2.2.0; e.g. ``REPLICA_LANE_BUSY``), None when absent.
        self.reason = reason


class ContinuityError(DataLayerError):
    pass


class CursorExpiredError(DataLayerError):
    pass


class SlowConsumerError(DataLayerError):
    pass
