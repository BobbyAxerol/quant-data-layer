"""DL v2.4.4 (DL-BL-2026-11-26-K): instrument lifecycle observations, an append-only hash-chained ledger.

Purpose: a consumer must be able to tell "the venue delisted / halted / expired this instrument, observed at T" from
"DL removed it in catalogue revision N" and from "DL is unavailable". Before this module DL published no lifecycle
observation: every catalogue record is ACTIVE (``qdl/domain/instrument.py`` record default) and a delisting was only
a render-time retire (``scripts/dl_declaration.py`` ``retire_instruments``).

What it is:
- A ledger file (JSON Lines). Line 1 is the header ``{"schema": LEDGER_SCHEMA, "ledger_id": ...}``; every further line
  is one event (``EVENT_SCHEMA``). The file only grows: ``verify_append_only`` proves a new file starts with the old
  file's events byte for byte.
- Each event is a change of (status, venue_state) of one instrument, observed in a captured venue listing. Events are
  chained per instrument (``sequence``, ``previous_event_id``) and identified by ``event_id`` = sha256 of the canonical
  JSON of the event without ``event_id``. A ``position`` counts events over the whole file, so a dropped or reordered
  line is refused.
- ``record_capture`` turns one capture of a venue's full instruments list (the same captures the r10 venue-listing
  gate takes, ``scripts/catalog_venue_listing_gate.py``) into new events. It is the only writer, used by
  ``scripts/instrument_lifecycle_record.py``.
- ``InstrumentLifecycleIndex`` is the read side Query serves at ``GET /v2/instruments/{identity}/lifecycle``
  (response schema ``SCHEMA``). It is opt-in through ``QDL_INSTRUMENT_LIFECYCLE_LEDGER``; named but malformed fails
  closed (Query does not start), like the DL v2.4.1 canonical metadata file.

Rules:
- Venue evidence only. A status comes from the venue's own state field, or from the instrument's absence from a full,
  valid listing. ``effective_utc`` is only a time the venue states in that capture (OKX ``listTime``/``expTime``,
  Binance ``onboardDate``/``deliveryDate``); DL never infers one, otherwise it is null.
- A DL outage, a refused/stale feed or a provider error never produces an event: only a valid listing response is
  accepted, an empty one is refused, and a capture that would make more than ``max_absent_fraction`` of the tracked
  instruments absent at once is refused as implausible.
- History starts at the first recorded capture; nothing before it is padded.
- Retired instruments stay tracked (they are in the ledger), so a relisting is observed.

Boundary: pure functions over files; no network, no runtime state. Venue state vocabularies (checked 2026-11-27):
- OKX ``GET /api/v5/public/instruments`` ``state``: live, suspend, rebase, post_only, preopen, test, settling (EVENTS)
  https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments
- Binance USD-M ``GET /fapi/v1/exchangeInfo`` ``status`` (contract status): PENDING_TRADING, TRADING, PRE_DELIVERING,
  DELIVERING, DELIVERED, PRE_SETTLE, SETTLING, CLOSE, TRADING_HALT, TRADING_CANCEL_ONLY
  https://developers.binance.com/docs/derivatives/usds-margined-futures/common-definition
- Binance spot ``GET /api/v3/exchangeInfo`` ``status``: TRADING, END_OF_DAY, HALT, BREAK, CANCEL_ONLY
  https://developers.binance.com/docs/binance-spot-api-docs/enums
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

SCHEMA = "qdl.instrument-lifecycle.v1"
EVENT_SCHEMA = "qdl.instrument-lifecycle-event.v1"
LEDGER_SCHEMA = "qdl.v2.instrument-lifecycle-ledger.v1"
ENVIRONMENT_VARIABLE = "QDL_INSTRUMENT_LIFECYCLE_LEDGER"
STATUSES = ("PRELISTED", "ACTIVE", "HALTED", "EXPIRED", "DELISTED")
ABSENT = "ABSENT"
MAX_EVENTS_PER_PAGE = 500

DOC_URLS = {
    "OKX": "https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments",
    ("BINANCE", "USDM"): "https://developers.binance.com/docs/derivatives/usds-margined-futures/common-definition",
    ("BINANCE", "SPOT"): "https://developers.binance.com/docs/binance-spot-api-docs/enums",
}
# venue state -> lifecycle status. The venue state itself is kept in every event, so a restriction that has no
# status of its own (OKX post_only, Binance TRADING_CANCEL_ONLY) is still a recorded change.
OKX_STATES = {
    "live": "ACTIVE", "post_only": "ACTIVE", "suspend": "HALTED", "rebase": "HALTED", "settling": "HALTED",
    "preopen": "PRELISTED", "test": "PRELISTED",
}
BINANCE_USDM_STATES = {
    "PENDING_TRADING": "PRELISTED", "TRADING": "ACTIVE", "TRADING_CANCEL_ONLY": "HALTED", "TRADING_HALT": "HALTED",
    "PRE_DELIVERING": "HALTED", "DELIVERING": "HALTED", "PRE_SETTLE": "HALTED", "SETTLING": "HALTED",
    "DELIVERED": "EXPIRED", "CLOSE": "DELISTED",
}
BINANCE_SPOT_STATES = {
    "TRADING": "ACTIVE", "END_OF_DAY": "HALTED", "HALT": "HALTED", "BREAK": "HALTED", "CANCEL_ONLY": "HALTED",
}
_EVENT_KEYS = frozenset({
    "schema", "position", "instrument_uid", "instrument_id", "venue", "market", "native_symbol", "sequence",
    "status", "venue_state", "effective_utc", "effective_basis", "observed_utc", "evidence", "catalog_revision",
    "source_revision", "previous_event_id", "event_id",
})
_EVIDENCE_KEYS = frozenset({"endpoint", "doc_url", "capture_sha256", "provider_error", "venue_times"})
_IDENTITY_KEYS = ("instrument_id", "venue", "market", "native_symbol")


class LifecycleLedgerError(ValueError):
    """The ledger file is malformed or its chain is broken; it is refused as a whole."""


class LifecycleCaptureRefused(ValueError):
    """A capture cannot be recorded; ``code`` is the typed reason. Nothing is written."""

    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code = code


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def event_id_of(event: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json({k: v for k, v in event.items() if k != "event_id"})).hexdigest()


def _utc(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise LifecycleLedgerError(f"{field} must be an RFC 3339 UTC string ending in Z")
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise LifecycleLedgerError(f"{field} is not an RFC 3339 time") from error


def _ms_utc(value: Any) -> str | None:
    """A venue millisecond timestamp as RFC 3339 UTC; empty, zero or unparsable is no statement (None)."""
    try:
        millis = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    if millis <= 0:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _product_type(instrument_id: str) -> str:
    parts = instrument_id.split(".")
    return parts[2].upper() if len(parts) >= 4 else ""


# --- ledger file --------------------------------------------------------------------------------------------------

def header(ledger_id: str) -> dict[str, str]:
    if not isinstance(ledger_id, str) or not ledger_id.strip():
        raise LifecycleLedgerError("ledger_id must be a non-empty string")
    return {"schema": LEDGER_SCHEMA, "ledger_id": ledger_id}


def validate_events(head: Mapping[str, Any], events: Sequence[Mapping[str, Any]]) -> None:
    """Every rule of the file; raises LifecycleLedgerError on the first violation."""
    if dict(head) != header(str(head.get("ledger_id", ""))):
        raise LifecycleLedgerError("header must be exactly {schema, ledger_id} with the ledger schema")
    last: dict[str, Mapping[str, Any]] = {}
    for index, event in enumerate(events, start=1):
        where = f"event {index}"
        if not isinstance(event, Mapping) or set(event) != _EVENT_KEYS:
            raise LifecycleLedgerError(f"{where}: keys must be exactly {sorted(_EVENT_KEYS)}")
        if event["schema"] != EVENT_SCHEMA or event["position"] != index:
            raise LifecycleLedgerError(f"{where}: schema or position mismatch (append-only file order)")
        for key in ("instrument_uid", *_IDENTITY_KEYS, "source_revision"):
            if not isinstance(event[key], str) or not event[key].strip():
                raise LifecycleLedgerError(f"{where}: {key} must be a non-empty string")
        if event["status"] not in STATUSES:
            raise LifecycleLedgerError(f"{where}: unknown status {event['status']!r}")
        if not isinstance(event["venue_state"], str) or not event["venue_state"]:
            raise LifecycleLedgerError(f"{where}: venue_state must be a non-empty string")
        observed = _utc(event["observed_utc"], f"{where}.observed_utc")
        if event["effective_utc"] is not None:
            _utc(event["effective_utc"], f"{where}.effective_utc")
            if not isinstance(event["effective_basis"], str) or not event["effective_basis"]:
                raise LifecycleLedgerError(f"{where}: effective_utc needs its venue field in effective_basis")
        elif event["effective_basis"] is not None:
            raise LifecycleLedgerError(f"{where}: effective_basis without effective_utc")
        evidence = event["evidence"]
        if not isinstance(evidence, Mapping) or set(evidence) != _EVIDENCE_KEYS:
            raise LifecycleLedgerError(f"{where}: evidence keys must be exactly {sorted(_EVIDENCE_KEYS)}")
        sha = evidence["capture_sha256"]
        if not isinstance(sha, str) or len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
            raise LifecycleLedgerError(f"{where}: evidence.capture_sha256 must be lowercase sha256 hex")
        if not isinstance(evidence["endpoint"], str) or not evidence["endpoint"]:
            raise LifecycleLedgerError(f"{where}: evidence.endpoint must be a non-empty string")
        if not isinstance(evidence["venue_times"], Mapping) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in evidence["venue_times"].items()):
            raise LifecycleLedgerError(f"{where}: evidence.venue_times must map field names to strings")
        revision = event["catalog_revision"]
        if revision is not None and (type(revision) is not int or revision < 1):
            raise LifecycleLedgerError(f"{where}: catalog_revision must be a positive integer or null")
        previous = last.get(event["instrument_uid"])
        if previous is None:
            if event["sequence"] != 1 or event["previous_event_id"] is not None:
                raise LifecycleLedgerError(f"{where}: an instrument's first event has sequence 1 and no previous")
        else:
            if event["sequence"] != previous["sequence"] + 1 or event["previous_event_id"] != previous["event_id"]:
                raise LifecycleLedgerError(f"{where}: broken chain for {event['instrument_uid']}")
            if any(event[k] != previous[k] for k in _IDENTITY_KEYS):
                raise LifecycleLedgerError(f"{where}: identity of {event['instrument_uid']} changed")
            if observed < _utc(previous["observed_utc"], "previous.observed_utc"):
                raise LifecycleLedgerError(f"{where}: observed_utc goes backwards for {event['instrument_uid']}")
            if (event["status"], event["venue_state"]) == (previous["status"], previous["venue_state"]):
                raise LifecycleLedgerError(f"{where}: an event must change status or venue_state")
        if event["event_id"] != event_id_of(event):
            raise LifecycleLedgerError(f"{where}: event_id is not the hash of the event")
        last[event["instrument_uid"]] = event


def parse_ledger(raw: bytes) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not raw.endswith(b"\n"):
        raise LifecycleLedgerError("the ledger must end with a newline (a partial last line is refused)")
    try:
        lines = [json.loads(line) for line in raw.decode("utf-8").split("\n")[:-1]]
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LifecycleLedgerError("the ledger is not JSON Lines") from error
    if not lines or not isinstance(lines[0], dict):
        raise LifecycleLedgerError("the ledger has no header")
    head, events = lines[0], lines[1:]
    validate_events(head, events)
    return head, events


def render_ledger(head: Mapping[str, Any], events: Sequence[Mapping[str, Any]]) -> bytes:
    validate_events(head, events)
    return b"".join(canonical_json(item) + b"\n" for item in (head, *events))


def verify_append_only(old: bytes, new: bytes) -> None:
    """The new file is a valid ledger that starts with every line of the old one, unchanged."""
    old_head, old_events = parse_ledger(old)
    new_head, new_events = parse_ledger(new)
    if old_head != new_head or not new.startswith(old):
        raise LifecycleLedgerError("the new ledger rewrites or drops recorded history")
    if len(new_events) < len(old_events):
        raise LifecycleLedgerError("the new ledger has fewer events")


# --- writer: one venue listing capture ----------------------------------------------------------------------------

@dataclass(frozen=True)
class ListingCapture:
    venue: str
    market: str
    rows: Mapping[str, Mapping[str, Any]]  # native symbol (upper case) -> venue row
    endpoint: str
    capture_sha256: str
    observed_utc: str


def load_listing_capture(venue: str, market: str, raw: bytes, *, endpoint: str, observed_utc: str) -> ListingCapture:
    """A capture of a venue's full instruments list. Anything but a valid, non-empty response is refused."""
    venue, market = venue.upper(), market.upper()
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LifecycleCaptureRefused("LIFECYCLE_CAPTURE_INVALID", "the capture is not JSON") from error
    if venue == "OKX":
        if not isinstance(document, dict) or str(document.get("code")) != "0" or not isinstance(
                document.get("data"), list):
            raise LifecycleCaptureRefused("LIFECYCLE_CAPTURE_INVALID", "not an OKX instruments response (code 0)")
        rows = {str(row["instId"]).upper(): row for row in document["data"] if isinstance(row, dict) and "instId" in row}
    elif venue == "BINANCE":
        if not isinstance(document, dict) or not isinstance(document.get("symbols"), list):
            raise LifecycleCaptureRefused("LIFECYCLE_CAPTURE_INVALID", "not a Binance exchangeInfo response")
        rows = {str(row["symbol"]).upper(): row for row in document["symbols"] if isinstance(row, dict) and "symbol" in row}
    else:
        raise LifecycleCaptureRefused("LIFECYCLE_VENUE_UNSUPPORTED", f"no lifecycle rule for venue {venue}")
    if not rows:
        raise LifecycleCaptureRefused("LIFECYCLE_CAPTURE_EMPTY", f"{venue}/{market} listing has no instruments")
    try:
        _utc(observed_utc, "observed_utc")
    except LifecycleLedgerError as error:
        raise LifecycleCaptureRefused("LIFECYCLE_CAPTURE_INVALID", str(error)) from error
    return ListingCapture(venue, market, rows, endpoint, hashlib.sha256(raw).hexdigest(), observed_utc)


def _states(venue: str, market: str) -> Mapping[str, str]:
    if venue == "OKX":
        return OKX_STATES
    if venue == "BINANCE":
        return BINANCE_SPOT_STATES if market == "SPOT" else BINANCE_USDM_STATES
    raise LifecycleCaptureRefused("LIFECYCLE_VENUE_UNSUPPORTED", venue)


def _venue_times(venue: str, row: Mapping[str, Any]) -> dict[str, str]:
    fields = ("listTime", "expTime") if venue == "OKX" else ("onboardDate", "deliveryDate")
    return {name: str(row[name]) for name in fields if row.get(name) not in (None, "")}


def _effective(venue: str, status: str, row: Mapping[str, Any] | None) -> tuple[str | None, str | None]:
    """Only a time the venue states for this status in this capture."""
    if row is None:
        return None, None
    if venue == "OKX":
        field = {"PRELISTED": "listTime", "ACTIVE": "listTime", "EXPIRED": "expTime"}.get(status)
    else:
        field = {"PRELISTED": "onboardDate", "ACTIVE": "onboardDate", "EXPIRED": "deliveryDate"}.get(status)
    stated = _ms_utc(row.get(field)) if field else None
    return (stated, f"{venue}:{field}") if stated else (None, None)


def record_capture(head: Mapping[str, Any], events: Sequence[Mapping[str, Any]],
                   instruments: Iterable[Mapping[str, Any]], capture: ListingCapture, *,
                   catalog_revision: int | None, source_revision: str,
                   max_absent_fraction: float = 0.5) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """New events for one capture, and a report. ``instruments`` are the catalogue's instruments (dicts with
    instrument_uid, instrument_id, venue, market, native_symbol); instruments already in the ledger for this
    venue/market are tracked too. Raises LifecycleCaptureRefused and records nothing when the capture is unusable."""
    validate_events(head, events)
    if not 0 < max_absent_fraction <= 1:
        raise ValueError("max_absent_fraction must be in (0, 1]")
    current: dict[str, Mapping[str, Any]] = {}
    for event in events:
        current[event["instrument_uid"]] = event
    tracked: dict[str, dict[str, str]] = {}
    for uid, event in current.items():
        if (event["venue"], event["market"]) == (capture.venue, capture.market):
            tracked[uid] = {k: event[k] for k in _IDENTITY_KEYS}
    for item in instruments:
        if (str(item["venue"]).upper(), str(item["market"]).upper()) != (capture.venue, capture.market):
            continue
        identity = {"instrument_id": str(item["instrument_id"]), "venue": capture.venue, "market": capture.market,
                    "native_symbol": str(item["native_symbol"]).upper()}
        known = tracked.get(str(item["instrument_uid"]))
        if known is not None and known != identity:
            raise LifecycleCaptureRefused("LIFECYCLE_IDENTITY_CONFLICT",
                                          f"{item['instrument_uid']} differs from its ledger identity")
        tracked[str(item["instrument_uid"])] = identity
    states = _states(capture.venue, capture.market)
    observed = _utc(capture.observed_utc, "observed_utc")
    planned, unmapped, unchanged = [], [], 0
    newly_absent = present_before = 0
    for uid in sorted(tracked):
        identity = tracked[uid]
        previous = current.get(uid)
        if previous is not None and observed < _utc(previous["observed_utc"], "observed_utc"):
            raise LifecycleCaptureRefused("LIFECYCLE_CAPTURE_OUT_OF_ORDER",
                f"capture {capture.observed_utc} is older than the last event of {identity['instrument_id']}")
        row = capture.rows.get(identity["native_symbol"])
        if row is None:
            venue_state = ABSENT
            status = "EXPIRED" if _product_type(identity["instrument_id"]) in {"FUTURE", "FUTURES"} else "DELISTED"
        else:
            venue_state = str(row.get("state" if capture.venue == "OKX" else "status"))
            status = states.get(venue_state)
            if status is None:
                unmapped.append({"instrument_id": identity["instrument_id"], "venue_state": venue_state})
                continue
        if previous is None or previous["venue_state"] != ABSENT:
            present_before += 1
            newly_absent += row is None
        if previous is not None and (previous["status"], previous["venue_state"]) == (status, venue_state):
            unchanged += 1
            continue
        planned.append((uid, identity, status, venue_state, row, previous))
    if present_before and newly_absent / present_before > max_absent_fraction:
        raise LifecycleCaptureRefused("LIFECYCLE_CAPTURE_IMPLAUSIBLE",
            f"{newly_absent} of {present_before} tracked {capture.venue}/{capture.market} instruments absent at once")
    appended: list[dict[str, Any]] = []
    position = len(events)
    for uid, identity, status, venue_state, row, previous in planned:
        position += 1
        effective, basis = _effective(capture.venue, status, row)
        event = {
            "schema": EVENT_SCHEMA, "position": position, "instrument_uid": uid, **identity,
            "sequence": 1 if previous is None else previous["sequence"] + 1,
            "status": status, "venue_state": venue_state, "effective_utc": effective, "effective_basis": basis,
            "observed_utc": capture.observed_utc,
            "evidence": {"endpoint": capture.endpoint,
                         "doc_url": DOC_URLS.get(capture.venue) or DOC_URLS.get((capture.venue, capture.market)),
                         "capture_sha256": capture.capture_sha256, "provider_error": None,
                         "venue_times": _venue_times(capture.venue, row) if row is not None else {}},
            "catalog_revision": catalog_revision, "source_revision": source_revision,
            "previous_event_id": None if previous is None else previous["event_id"],
        }
        event["event_id"] = event_id_of(event)
        appended.append(event)
    validate_events(head, [*events, *appended])
    report = {"venue": capture.venue, "market": capture.market, "capture_sha256": capture.capture_sha256,
              "observed_utc": capture.observed_utc, "tracked": len(tracked), "appended": len(appended),
              "unchanged": unchanged, "unmapped": unmapped}
    return appended, report


# --- reader: what Query serves ------------------------------------------------------------------------------------

class InstrumentLifecycleIndex:
    def __init__(self, raw: bytes):
        self.head, self.events = parse_ledger(raw)
        self.sha256 = hashlib.sha256(raw).hexdigest()
        self._by_uid: dict[str, list[dict[str, Any]]] = {}
        self._uid_of_id: dict[str, str] = {}
        for event in self.events:
            self._by_uid.setdefault(event["instrument_uid"], []).append(event)
            self._uid_of_id[event["instrument_id"]] = event["instrument_uid"]

    @classmethod
    def load(cls, path: Path) -> "InstrumentLifecycleIndex":
        return cls(Path(path).read_bytes())

    def uid_for(self, identity: str) -> str | None:
        if identity in self._by_uid:
            return identity
        return self._uid_of_id.get(identity)

    def history(self, instrument_uid: str) -> list[dict[str, Any]]:
        return list(self._by_uid.get(instrument_uid, ()))

    def ledger_view(self) -> dict[str, Any]:
        return {"schema": LEDGER_SCHEMA, "ledger_id": self.head["ledger_id"], "sha256": self.sha256,
                "events_total": len(self.events),
                "head_event_id": self.events[-1]["event_id"] if self.events else None}


def lifecycle_document(*, identity: Mapping[str, str], in_catalog: bool, index: InstrumentLifecycleIndex | None,
                       after_sequence: int = 0, limit: int = 100) -> dict[str, Any]:
    """The ``qdl.instrument-lifecycle.v1`` body. ``identity`` has instrument_uid, instrument_id, venue, market and
    native_symbol (from the catalogue, or from the ledger for a retired instrument)."""
    if type(after_sequence) is not int or after_sequence < 0:
        raise ValueError("after_sequence must be a non-negative integer")
    if type(limit) is not int or not 1 <= limit <= MAX_EVENTS_PER_PAGE:
        raise ValueError(f"limit must be 1..{MAX_EVENTS_PER_PAGE}")
    history = index.history(identity["instrument_uid"]) if index is not None else []
    page = [event for event in history if event["sequence"] > after_sequence][:limit]
    latest = history[-1] if history else None
    return {
        "schema": SCHEMA,
        **{key: identity[key] for key in ("instrument_uid", *_IDENTITY_KEYS)},
        "in_catalog": in_catalog,
        "history_state": "NOT_CONFIGURED" if index is None else ("RECORDED" if history else "NO_OBSERVATION"),
        "current": None if latest is None else {
            "status": latest["status"], "venue_state": latest["venue_state"],
            "since_event_id": latest["event_id"], "since_observed_utc": latest["observed_utc"],
            "effective_utc": latest["effective_utc"]},
        "events": [{k: v for k, v in event.items() if k != "position"} for event in page],
        "next_after_sequence": page[-1]["sequence"] if page and page[-1] is not history[-1] else None,
        "ledger": None if index is None else index.ledger_view(),
    }


def index_from_environment(environ: Mapping[str, str]) -> InstrumentLifecycleIndex | None:
    """Opt-in: ``QDL_INSTRUMENT_LIFECYCLE_LEDGER`` names the file. Unset: the endpoint answers NOT_CONFIGURED;
    set but missing or malformed: refuses to start (fail closed)."""
    raw = environ.get(ENVIRONMENT_VARIABLE, "").strip()
    if not raw:
        return None
    return InstrumentLifecycleIndex.load(Path(raw))
