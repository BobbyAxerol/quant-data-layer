from __future__ import annotations


class DataLayerError(RuntimeError):
    def __init__(
        self,
        code: str,
        detail: str,
        *,
        retryable: bool = False,
        retry_after_ms: int | None = None,
    ) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.retryable = retryable
        self.retry_after_ms = retry_after_ms


class ContinuityError(DataLayerError):
    pass


class CursorExpiredError(DataLayerError):
    pass


class SlowConsumerError(DataLayerError):
    pass
