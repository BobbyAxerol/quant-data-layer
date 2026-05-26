from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from fastapi import Request


@dataclass
class DataLayerContext:
    redis_cache: Any
    binance_stream_supervisor: Any
    get_dnse_stream_manager: Callable[[], Any]


def get_context(request: Request) -> DataLayerContext:
    return request.app.state.context

