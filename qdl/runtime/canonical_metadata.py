"""DL v2.4.1 canonical instrument metadata v1: underlying asset, price/quantity multipliers and cross-venue aliases.

Purpose: venues name and size the same asset differently (Binance USDM `1000PEPEUSDT` is 1,000 PEPE per unit and is
priced 1,000x the PEPE index; OKX `PEPE-USDT-SWAP` is 10,000,000 PEPE per contract and is priced per PEPE; Binance
`LUNA2USDT` is OKX `LUNA-USDT-SWAP`). DL keeps every price, quantity and bar native. This module only loads the
separately versioned runtime file `canonical-instrument-metadata.json` (schema below) that says how a venue unit
relates to its underlying asset, so consumers can normalise explicitly.

Boundary and rules:
- The file is separate from the stable catalogue on purpose: it has its own `revision`, so adding or correcting
  metadata never moves `catalog_revision` (the Query <-> Stream hot-read authority, DL v2.4.0 window 1 RCA).
- Fail closed: a malformed file refuses to load (Query does not start); an entry whose rule could not be verified
  against venue documentation and a capture carries `status: UNVERIFIED` and no multipliers, so every consumer
  helper refuses it (never a silent 1:1).
- Read-only; no runtime access.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

SCHEMA = "qdl.v2.canonical-instrument-metadata.v1"
ENTRY_SCHEMA = "qdl.instrument.canonical-metadata.v1"
STATUS_VERIFIED = "VERIFIED"
STATUS_UNVERIFIED = "UNVERIFIED"
_STATUSES = frozenset({STATUS_VERIFIED, STATUS_UNVERIFIED})
_ENTRY_KEYS = frozenset({
    "instrument_uid", "instrument_id", "venue", "market", "native_symbol", "status", "rule_id",
    "underlying_asset", "underlying_instrument_uid", "price_multiplier", "quantity_multiplier", "aliases",
    "source",
})
_SOURCE_KEYS = frozenset({"endpoint", "doc_url", "captured_at_utc", "response_sha256", "evidence"})
_ALIAS_KEYS = frozenset({"venue", "market", "native_symbol", "instrument_uid"})


class CanonicalMetadataError(ValueError):
    """The canonical metadata file is malformed; it is refused as a whole."""


def _positive_decimal(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise CanonicalMetadataError(f"{field} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise CanonicalMetadataError(f"{field} is not a decimal") from error
    if not parsed.is_finite() or parsed <= 0:
        raise CanonicalMetadataError(f"{field} must be a positive finite decimal")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise CanonicalMetadataError(f"{field} must be a non-empty trimmed string")
    return value


@dataclass(frozen=True)
class CanonicalInstrumentMetadata:
    instrument_uid: str
    instrument_id: str
    venue: str
    market: str
    native_symbol: str
    status: str
    rule_id: str
    underlying_asset: str | None
    underlying_instrument_uid: str | None
    price_multiplier: str | None
    quantity_multiplier: str | None
    aliases: tuple[Mapping[str, str], ...]
    source: Mapping[str, Any]
    revision: int

    def as_public(self) -> dict[str, Any]:
        """The additive `canonical` object of `/v2/instruments` (schema `qdl.instrument.canonical-metadata.v1`)."""
        return {
            "schema": ENTRY_SCHEMA,
            "revision": self.revision,
            "status": self.status,
            "rule_id": self.rule_id,
            "underlying_asset": self.underlying_asset,
            "underlying_instrument_uid": self.underlying_instrument_uid,
            "price_multiplier": self.price_multiplier,
            "quantity_multiplier": self.quantity_multiplier,
            "aliases": [dict(item) for item in self.aliases],
            "source": dict(self.source),
        }


def _entry(raw: Any, revision: int) -> CanonicalInstrumentMetadata:
    if not isinstance(raw, Mapping) or set(raw) != _ENTRY_KEYS:
        raise CanonicalMetadataError("canonical metadata entry fields are incomplete or unknown")
    status = raw["status"]
    if status not in _STATUSES:
        raise CanonicalMetadataError(f"canonical metadata status {status!r} is unknown")
    numeric = ("underlying_asset", "underlying_instrument_uid", "price_multiplier", "quantity_multiplier")
    if status == STATUS_UNVERIFIED:
        if any(raw[key] is not None for key in numeric):
            raise CanonicalMetadataError("an UNVERIFIED entry must not carry multipliers or an underlying")
        values: dict[str, str | None] = {key: None for key in numeric}
    else:
        values = {
            "underlying_asset": _text(raw["underlying_asset"], "underlying_asset"),
            "underlying_instrument_uid": _text(raw["underlying_instrument_uid"], "underlying_instrument_uid"),
            "price_multiplier": _positive_decimal(raw["price_multiplier"], "price_multiplier"),
            "quantity_multiplier": _positive_decimal(raw["quantity_multiplier"], "quantity_multiplier"),
        }
    aliases = raw["aliases"]
    if not isinstance(aliases, list) or any(
        not isinstance(item, Mapping) or set(item) != _ALIAS_KEYS
        or not all(isinstance(item[key], str) and item[key] for key in _ALIAS_KEYS)
        for item in aliases
    ):
        raise CanonicalMetadataError("canonical metadata aliases are malformed")
    source = raw["source"]
    if not isinstance(source, Mapping) or set(source) != _SOURCE_KEYS:
        raise CanonicalMetadataError("canonical metadata source fields are incomplete or unknown")
    for key in ("endpoint", "doc_url", "captured_at_utc", "response_sha256"):
        _text(source[key], f"source.{key}")
    if not isinstance(source["evidence"], str):
        raise CanonicalMetadataError("source.evidence must be a string")
    return CanonicalInstrumentMetadata(
        instrument_uid=_text(raw["instrument_uid"], "instrument_uid"),
        instrument_id=_text(raw["instrument_id"], "instrument_id"),
        venue=_text(raw["venue"], "venue"),
        market=_text(raw["market"], "market"),
        native_symbol=_text(raw["native_symbol"], "native_symbol"),
        status=status,
        rule_id=_text(raw["rule_id"], "rule_id"),
        aliases=tuple(dict(item) for item in sorted(aliases, key=lambda a: (a["venue"], a["market"], a["native_symbol"]))),
        source=dict(source),
        revision=revision,
        **values,
    )


def document_sha256(document: Mapping[str, Any]) -> str:
    body = {key: value for key, value in document.items() if key != "sha256"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=True).encode()).hexdigest()


class CanonicalMetadataIndex:
    """instrument_uid -> canonical metadata, from one verified file."""

    def __init__(self, document: Mapping[str, Any]) -> None:
        if not isinstance(document, Mapping) or document.get("schema") != SCHEMA:
            raise CanonicalMetadataError("canonical metadata schema is not " + SCHEMA)
        revision = document.get("revision")
        if type(revision) is not int or revision < 1:
            raise CanonicalMetadataError("canonical metadata revision must be a positive integer")
        if document.get("sha256") != document_sha256(document):
            raise CanonicalMetadataError("canonical metadata sha256 does not verify")
        entries = document.get("instruments")
        if not isinstance(entries, list) or not entries:
            raise CanonicalMetadataError("canonical metadata has no instruments")
        self.revision = revision
        self.sha256 = document["sha256"]
        self._by_uid: dict[str, CanonicalInstrumentMetadata] = {}
        for raw in entries:
            entry = _entry(raw, revision)
            if entry.instrument_uid in self._by_uid:
                raise CanonicalMetadataError(f"duplicate canonical metadata for {entry.instrument_uid}")
            self._by_uid[entry.instrument_uid] = entry
        known = set(self._by_uid)
        for entry in self._by_uid.values():
            for alias in entry.aliases:
                if alias["instrument_uid"] not in known:
                    raise CanonicalMetadataError(f"alias of {entry.instrument_id} names an unknown instrument")

    @classmethod
    def load(cls, path: Path) -> "CanonicalMetadataIndex":
        try:
            document = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise CanonicalMetadataError(f"canonical metadata file is unreadable: {path}") from error
        return cls(document)

    def get(self, instrument_uid: str) -> CanonicalInstrumentMetadata | None:
        return self._by_uid.get(instrument_uid)

    def __len__(self) -> int:
        return len(self._by_uid)


def index_from_environment(environ: Mapping[str, str]) -> CanonicalMetadataIndex | None:
    """Opt-in: `QDL_CANONICAL_INSTRUMENT_METADATA` names the file. Unset keeps `/instruments` unchanged; set but
    missing or malformed refuses to start (fail closed)."""
    raw = environ.get("QDL_CANONICAL_INSTRUMENT_METADATA", "").strip()
    if not raw:
        return None
    return CanonicalMetadataIndex.load(Path(raw))
