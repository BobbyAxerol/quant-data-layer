"""Per-request cost notes that decide whether a data-plane request keeps its quota unit (DL v2.4.0 S3).

Purpose: the quota is a coarse guard against a runaway client, not a bill for
refusals. A request takes one unit when it is authenticated (so an exhausted
identity is refused before any work), and the unit is returned when the
request turns out not to have cost the Data Layer real work:

* the response is a refusal (any status >= 400: authentication, entitlement,
  admission, data-not-ready, rate limits);
* every item was a refusal or was served from a shared result (a key-level
  snapshot cache hit, or a warmup that joined an identical in-flight
  materialization), with no item doing its own read.

The serving code marks what happened through the helpers below; the REST
settlement middleware (``qdl.api_v2.router``) reads the notes once the
response status is known. The notes live in a ``ContextVar`` holding one
mutable object per request, so worker threads that run with a copy of the
request context (``qdl.query.cold_work.await_in_thread``) update the same
object.

Boundary: bookkeeping only. It never refuses, delays or changes a response.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class RequestCost:
    work: int = 0
    shared_hits: int = 0
    refusals: int = 0
    lease: Any = field(default=None)

    def free(self) -> bool:
        """True when nothing in this request did its own read."""

        return self.work == 0 and (self.shared_hits > 0 or self.refusals > 0)


_CURRENT: ContextVar[RequestCost | None] = ContextVar("qdl_request_cost", default=None)


def begin() -> tuple[RequestCost, Token]:
    cost = RequestCost()
    return cost, _CURRENT.set(cost)


def end(token: Token) -> None:
    _CURRENT.reset(token)


def current() -> RequestCost | None:
    return _CURRENT.get()


def note_work(units: int = 1) -> None:
    cost = _CURRENT.get()
    if cost is not None:
        cost.work += units


def note_shared_hit(units: int = 1) -> None:
    cost = _CURRENT.get()
    if cost is not None:
        cost.shared_hits += units


def note_refusal(units: int = 1) -> None:
    cost = _CURRENT.get()
    if cost is not None:
        cost.refusals += units
