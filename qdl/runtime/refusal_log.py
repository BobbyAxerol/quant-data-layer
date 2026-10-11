"""Bounded, payload-free refusal lines at the layer that refused (Astra advisory R6).

Purpose: P18.4 stage-100 triage had only a hash of a refusal's detail in the
harness receipt and no Query line at all (the HTTP problem handler and the
private execution MARK/INDEX edge returned refusals silently), so the failing
layer was inferred from code. Each refusal now leaves one structured line:
origin (which edge refused), canonical code, HTTP status, retryability, the
typed reason tokens of the detail (UPPER_SNAKE vocabulary such as
``HOT_BACKUP_UNAVAILABLE_PRIMARY_BEHIND`` / ``HOT_READER_BUSY``), the sha256 of
the detail (the same digest the load harness stores, so a receipt row joins its
server line exactly), request id, instrument uid, route template and replica.

Boundaries:
* failure-only and payload-free: never the detail text itself, a token, a
  price, a cursor or a request body;
* bounded volume: at most ``burst`` lines per (origin, code, first reason) key
  per ``window_seconds``; the number suppressed is reported once per key with
  the first line of a later window, so an incident costs a fixed log rate and
  its size is still recorded. At most ``max_keys`` keys per window (excess keys
  share one overflow key);
* per-item refusals inside a 200 batch response (``reference_batch_item`` /
  ``warmup_batch_item``) use the same line with the item's ``feed`` and any
  typed provider tokens (``extra_reasons``, e.g. a provider error code or a
  coverage terminal reason) appended after the detail's own tokens;
* WARNING level on logger ``qdl.refusal``: the Query role installs no handler of
  its own (``serve_stable_query``), so the line reaches stderr through the
  logging module's last-resort handler without changing any other log volume.
Changing no response, status or refusal decision.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import socket
import threading
import time
from typing import Callable

logger = logging.getLogger("qdl.refusal")

_REASON = re.compile(r"\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b")
MAX_REASONS = 4
_MAX_TOKEN = 64
_OVERFLOW = "OVERFLOW"


def reason_tokens(detail: object, *, limit: int = MAX_REASONS) -> tuple[str, ...]:
    """The typed UPPER_SNAKE reason codes inside a refusal detail, in order, deduplicated."""

    found: list[str] = []
    for token in _REASON.findall(str(detail or "")[:4096]):
        if len(token) <= _MAX_TOKEN and token not in found:
            found.append(token)
            if len(found) == limit:
                break
    return tuple(found)


def detail_sha256(detail: object) -> str:
    return hashlib.sha256(str(detail or "").encode()).hexdigest()


def _bounded(value: object, size: int = 256) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if len(text) <= size else text[:size]


class RefusalLog:
    """Rate-bounded structured refusal lines (thread-safe)."""

    def __init__(
        self,
        *,
        burst: int = 5,
        window_seconds: float = 10.0,
        max_keys: int = 256,
        clock: Callable[[], float] = time.monotonic,
        emit: Callable[[str], None] | None = None,
        replica: str | None = None,
    ) -> None:
        if burst < 1 or window_seconds <= 0 or max_keys < 1:
            raise ValueError("refusal log bounds must be positive")
        self._burst = burst
        self._window = window_seconds
        self._max_keys = max_keys
        self._clock = clock
        self._emit = emit or logger.warning
        self._replica = replica if replica is not None else socket.gethostname()
        self._lock = threading.Lock()
        self._window_start = clock()
        self._counts: dict[tuple[str, str, str], int] = {}
        self._carried: dict[tuple[str, str, str], int] = {}

    def record(
        self,
        *,
        origin: str,
        code: str,
        detail: object,
        status: int | None = None,
        retryable: bool | None = None,
        request_id: str | None = None,
        instrument_uid: str | None = None,
        route: str | None = None,
        feed: str | None = None,
        extra_reasons: tuple[object, ...] = (),
    ) -> bool:
        """Emit one line for this refusal unless its key is over budget; True when emitted.

        ``feed`` and ``extra_reasons`` are additive: the line gains a ``feed``
        key only when one is given, and an extra reason is kept only when it is
        itself one typed UPPER_SNAKE token (never free text).
        """

        reasons = reason_tokens(detail)
        for extra in extra_reasons:
            token = str(extra or "")
            if (len(reasons) < MAX_REASONS and len(token) <= _MAX_TOKEN
                    and _REASON.fullmatch(token) and token not in reasons):
                reasons = (*reasons, token)
        key = (str(origin), str(code), reasons[0] if reasons else "")
        with self._lock:
            now = self._clock()
            if now - self._window_start >= self._window:
                for prior, count in self._counts.items():
                    if count > self._burst:
                        self._carried[prior] = self._carried.get(prior, 0) + count - self._burst
                # Carried counts are bounded by the same key budget.
                while len(self._carried) > self._max_keys:
                    self._carried.pop(next(iter(self._carried)))
                self._counts = {}
                self._window_start = now
            if key not in self._counts and len(self._counts) >= self._max_keys:
                key = (key[0], key[1], _OVERFLOW)
            count = self._counts.get(key, 0) + 1
            self._counts[key] = count
            if count > self._burst:
                return False
            suppressed = self._carried.pop(key, 0)
        line = {
            "event": "qdl_refusal",
            "origin": key[0],
            "code": key[1],
            "status": status,
            "retryable": retryable,
            "reasons": list(reasons),
            "detail_sha256": detail_sha256(detail),
            "request_id": _bounded(request_id, 64),
            "instrument_uid": _bounded(instrument_uid),
            "route": _bounded(route),
            "replica": _bounded(self._replica, 128),
        }
        if feed is not None:
            line["feed"] = _bounded(feed, 64)
        if suppressed:
            line["suppressed_before"] = suppressed
        try:
            self._emit(json.dumps(line, separators=(",", ":"), sort_keys=True))
        except Exception:  # noqa: BLE001 - diagnostics never change a response
            return False
        return True


REFUSALS = RefusalLog()
