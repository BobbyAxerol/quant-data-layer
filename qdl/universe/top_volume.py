"""Bounded, auditable top-volume universe resolution.

The universe is an eligibility inventory for history/warmup and future active
demand compilation.  It is intentionally not a websocket subscription plan:
only a separately leased active demand can create a realtime binding.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Any, Iterable, Mapping


_SCHEMA = "qdl.v2.top-volume-universe.v1"
_AUDIT_SCHEMA = "qdl.v2.top-volume-universe-audit.v1"
_SUPPORTED_POLICIES = {
    ("BINANCE", "USDM", "PERPETUAL"),
    ("OKX", "SWAP", "PERPETUAL"),
}


def _text(value: object, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{field} is required")
    return result


def _decimal(value: object, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{field} is not a decimal") from error
    if not result.is_finite() or result < 0:
        raise ValueError(f"{field} must be a finite non-negative decimal")
    return result


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class TopVolumeUniversePolicy:
    venue: str
    market: str
    product_type: str = "PERPETUAL"
    quote_asset: str = "USDT"
    size: int = 350

    def __post_init__(self) -> None:
        venue = _text(self.venue, "venue").upper()
        market = _text(self.market, "market").upper()
        product_type = _text(self.product_type, "product_type").upper()
        quote_asset = _text(self.quote_asset, "quote_asset").upper()
        if (venue, market, product_type) not in _SUPPORTED_POLICIES:
            raise ValueError("top-volume policy venue/market/product is unsupported")
        if quote_asset != "USDT":
            raise ValueError("top-volume policy currently supports USDT quote only")
        if not 1 <= self.size <= 1_000:
            raise ValueError("top-volume policy size is outside bounds")
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "market", market)
        object.__setattr__(self, "product_type", product_type)
        object.__setattr__(self, "quote_asset", quote_asset)

    @property
    def universe_id(self) -> str:
        return f"{self.venue.lower()}-{self.market.lower()}-top{self.size}-usdt-perpetual-v1"

    @property
    def state_stem(self) -> str:
        return f"{self.venue.lower()}-{self.market.lower()}-top{self.size}"

    def mapping(self) -> dict[str, object]:
        return {
            "venue": self.venue,
            "market": self.market,
            "product_type": self.product_type,
            "quote_asset": self.quote_asset,
            "size": self.size,
            "universe_id": self.universe_id,
        }


@dataclass(frozen=True, slots=True)
class TopVolumeMember:
    native_symbol: str
    quote_volume: str
    rank: int

    def __post_init__(self) -> None:
        symbol = _text(self.native_symbol, "native_symbol").upper()
        volume = _decimal(self.quote_volume, "quote_volume")
        if self.rank < 1:
            raise ValueError("rank must be positive")
        object.__setattr__(self, "native_symbol", symbol)
        object.__setattr__(self, "quote_volume", format(volume, "f"))

    def mapping(self) -> dict[str, object]:
        return {
            "native_symbol": self.native_symbol,
            "quote_volume": self.quote_volume,
            "rank": self.rank,
        }


@dataclass(frozen=True, slots=True)
class TopVolumeUniverse:
    policy: TopVolumeUniversePolicy
    generated_at_ns: int
    provider_metadata_sha256: str
    provider_ticker_sha256: str
    eligible_symbols: tuple[str, ...]
    members: tuple[TopVolumeMember, ...]

    def __post_init__(self) -> None:
        if self.generated_at_ns <= 0:
            raise ValueError("generated_at_ns must be positive")
        for field in ("provider_metadata_sha256", "provider_ticker_sha256"):
            value = str(getattr(self, field)).lower()
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise ValueError(f"{field} must be a SHA-256 digest")
        eligible = tuple(sorted({_text(value, "eligible_symbol").upper() for value in self.eligible_symbols}))
        if len(eligible) < self.policy.size:
            raise ValueError("eligible provider universe is smaller than the required top-volume size")
        members = tuple(sorted(self.members, key=lambda item: item.rank))
        if len(members) != self.policy.size:
            raise ValueError("top-volume member count differs from policy size")
        if tuple(item.rank for item in members) != tuple(range(1, self.policy.size + 1)):
            raise ValueError("top-volume ranks are not contiguous")
        symbols = tuple(item.native_symbol for item in members)
        if len(set(symbols)) != len(symbols) or not set(symbols).issubset(set(eligible)):
            raise ValueError("top-volume members are not a unique eligible subset")
        object.__setattr__(self, "eligible_symbols", eligible)
        object.__setattr__(self, "members", members)

    @property
    def selection_sha256(self) -> str:
        return _sha256({
            "policy": self.policy.mapping(),
            "eligible_symbols": list(self.eligible_symbols),
            "members": [item.mapping() for item in self.members],
            "provider_metadata_sha256": self.provider_metadata_sha256,
            "provider_ticker_sha256": self.provider_ticker_sha256,
        })

    def mapping(self) -> dict[str, object]:
        return {
            "schema": _SCHEMA,
            "policy": self.policy.mapping(),
            "generated_at_ns": self.generated_at_ns,
            "provider_metadata_sha256": self.provider_metadata_sha256,
            "provider_ticker_sha256": self.provider_ticker_sha256,
            "selection_sha256": self.selection_sha256,
            "eligible_symbol_count": len(self.eligible_symbols),
            "eligible_symbols": list(self.eligible_symbols),
            "symbols": [item.mapping() for item in self.members],
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "TopVolumeUniverse":
        expected = {
            "schema", "policy", "generated_at_ns", "provider_metadata_sha256",
            "provider_ticker_sha256", "selection_sha256", "eligible_symbol_count",
            "eligible_symbols", "symbols",
        }
        if set(value) != expected or value.get("schema") != _SCHEMA:
            raise ValueError("top-volume universe schema is invalid")
        policy_raw = value.get("policy")
        rows = value.get("symbols")
        eligible_symbols = value.get("eligible_symbols")
        if (
            not isinstance(policy_raw, Mapping)
            or not isinstance(rows, list)
            or not isinstance(eligible_symbols, list)
        ):
            raise ValueError("top-volume universe policy/symbols are invalid")
        policy = TopVolumeUniversePolicy(
            venue=str(policy_raw.get("venue") or ""),
            market=str(policy_raw.get("market") or ""),
            product_type=str(policy_raw.get("product_type") or ""),
            quote_asset=str(policy_raw.get("quote_asset") or ""),
            size=int(policy_raw.get("size") or 0),
        )
        universe = cls(
            policy=policy,
            generated_at_ns=int(value["generated_at_ns"]),
            provider_metadata_sha256=str(value["provider_metadata_sha256"]),
            provider_ticker_sha256=str(value["provider_ticker_sha256"]),
            eligible_symbols=tuple(str(item) for item in eligible_symbols),
            members=tuple(
                TopVolumeMember(
                    native_symbol=str(item.get("native_symbol") or ""),
                    quote_volume=str(item.get("quote_volume") or ""),
                    rank=int(item.get("rank") or 0),
                )
                for item in rows if isinstance(item, Mapping)
            ),
        )
        if int(value.get("eligible_symbol_count") or 0) != len(universe.eligible_symbols):
            raise ValueError("top-volume universe eligible_symbol_count is invalid")
        if str(value.get("selection_sha256") or "").lower() != universe.selection_sha256:
            raise ValueError("top-volume universe selection digest differs")
        return universe


def _ticker_map(rows: Iterable[Mapping[str, Any]], *, symbol_key: str) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        symbol = str(row.get(symbol_key) or "").strip().upper()
        if not symbol:
            continue
        if symbol in result:
            raise ValueError(f"provider ticker contains duplicate symbol: {symbol}")
        result[symbol] = row
    return result


def _rank(
    *,
    policy: TopVolumeUniversePolicy,
    eligible: Iterable[tuple[str, Decimal]],
    metadata_payload: Mapping[str, Any] | list[Any],
    ticker_payload: Mapping[str, Any] | list[Any],
    generated_at_ns: int | None,
) -> TopVolumeUniverse:
    values = sorted(
        ((symbol.upper(), volume) for symbol, volume in eligible),
        key=lambda item: (-item[1], item[0]),
    )
    symbols = tuple(symbol for symbol, _volume in values)
    members = tuple(
        TopVolumeMember(symbol, format(volume, "f"), rank=index)
        for index, (symbol, volume) in enumerate(values[:policy.size], start=1)
    )
    return TopVolumeUniverse(
        policy=policy,
        generated_at_ns=generated_at_ns if generated_at_ns is not None else time.time_ns(),
        provider_metadata_sha256=hashlib.sha256(json.dumps(metadata_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        provider_ticker_sha256=hashlib.sha256(json.dumps(ticker_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        eligible_symbols=symbols,
        members=members,
    )


def resolve_binance_usdm_top_volume(
    *,
    exchange_info: Mapping[str, Any],
    tickers: Iterable[Mapping[str, Any]],
    policy: TopVolumeUniversePolicy = TopVolumeUniversePolicy("BINANCE", "USDM"),
    generated_at_ns: int | None = None,
) -> TopVolumeUniverse:
    if (policy.venue, policy.market, policy.product_type) != ("BINANCE", "USDM", "PERPETUAL"):
        raise ValueError("Binance resolver requires a BINANCE/USDM/PERPETUAL policy")
    symbols = exchange_info.get("symbols")
    if not isinstance(symbols, list):
        raise ValueError("Binance exchangeInfo symbols are missing")
    ticker_rows = list(tickers)
    ticker_by_symbol = _ticker_map(ticker_rows, symbol_key="symbol")
    eligible: list[tuple[str, Decimal]] = []
    for row in symbols:
        if not isinstance(row, Mapping):
            continue
        if (
            str(row.get("status") or "").upper() != "TRADING"
            or str(row.get("contractType") or "").upper() != "PERPETUAL"
            or str(row.get("quoteAsset") or "").upper() != policy.quote_asset
            or str(row.get("marginAsset") or "").upper() != policy.quote_asset
        ):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        ticker = ticker_by_symbol.get(symbol)
        if not symbol or ticker is None:
            continue
        try:
            volume = _decimal(ticker.get("quoteVolume"), f"Binance quoteVolume {symbol}")
        except ValueError:
            continue
        eligible.append((symbol, volume))
    return _rank(
        policy=policy,
        eligible=eligible,
        metadata_payload=exchange_info,
        ticker_payload=ticker_rows,
        generated_at_ns=generated_at_ns,
    )


def _okx_quote_volume(row: Mapping[str, Any], *, symbol: str) -> Decimal:
    direct = str(row.get("volCcyQuote") or "").strip()
    if direct:
        return _decimal(direct, f"OKX volCcyQuote {symbol}")
    currency_volume = str(row.get("volCcy24h") or "").strip()
    last = str(row.get("last") or "").strip()
    if currency_volume and last:
        return _decimal(currency_volume, f"OKX volCcy24h {symbol}") * _decimal(last, f"OKX last {symbol}")
    raise ValueError(f"OKX quote-volume is unavailable for {symbol}")


def resolve_okx_swap_top_volume(
    *,
    instruments: Iterable[Mapping[str, Any]],
    tickers: Iterable[Mapping[str, Any]],
    policy: TopVolumeUniversePolicy = TopVolumeUniversePolicy("OKX", "SWAP"),
    generated_at_ns: int | None = None,
) -> TopVolumeUniverse:
    if (policy.venue, policy.market, policy.product_type) != ("OKX", "SWAP", "PERPETUAL"):
        raise ValueError("OKX resolver requires an OKX/SWAP/PERPETUAL policy")
    instrument_rows = list(instruments)
    ticker_rows = list(tickers)
    ticker_by_symbol = _ticker_map(ticker_rows, symbol_key="instId")
    eligible: list[tuple[str, Decimal]] = []
    suffix = f"-{policy.quote_asset}-SWAP"
    for row in instrument_rows:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("instId") or "").strip().upper()
        if (
            str(row.get("instType") or "").upper() != "SWAP"
            or str(row.get("state") or "").lower() != "live"
            or str(row.get("settleCcy") or "").upper() != policy.quote_asset
            or not symbol.endswith(suffix)
        ):
            continue
        ticker = ticker_by_symbol.get(symbol)
        if ticker is None:
            continue
        try:
            volume = _okx_quote_volume(ticker, symbol=symbol)
        except ValueError:
            continue
        eligible.append((symbol, volume))
    return _rank(
        policy=policy,
        eligible=eligible,
        metadata_payload=instrument_rows,
        ticker_payload=ticker_rows,
        generated_at_ns=generated_at_ns,
    )


class UniverseAuditStore:
    """Atomic host-state writer with bounded entrant/removal evidence."""

    def __init__(self, state_dir: str | Path, *, max_audit_files: int = 90) -> None:
        self.state_dir = Path(state_dir)
        if not 1 <= max_audit_files <= 365:
            raise ValueError("max_audit_files is outside bounds")
        self.max_audit_files = max_audit_files

    def publish(self, universe: TopVolumeUniverse) -> dict[str, object]:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        current_path = self.state_dir / f"{universe.policy.state_stem}-current.json"
        prior = self._load_current(current_path)
        prior_symbols = {item.native_symbol for item in prior.members} if prior is not None else set()
        current_symbols = {item.native_symbol for item in universe.members}
        current_eligible = set(universe.eligible_symbols)
        added = sorted(current_symbols - prior_symbols)
        removed = [
            {
                "native_symbol": symbol,
                "reason": "RANKED_OUT" if symbol in current_eligible else "DELISTED_OR_NOT_LIVE",
            }
            for symbol in sorted(prior_symbols - current_symbols)
        ]
        audit = {
            "schema": _AUDIT_SCHEMA,
            "generated_at_ns": universe.generated_at_ns,
            "universe_id": universe.policy.universe_id,
            "previous_selection_sha256": prior.selection_sha256 if prior is not None else None,
            "selection_sha256": universe.selection_sha256,
            "added": added,
            "removed": removed,
            "retained_count": len(prior_symbols & current_symbols),
            "eligible_symbol_count": len(universe.eligible_symbols),
            "member_count": len(universe.members),
        }
        self._atomic_json(current_path, universe.mapping())
        audit_path = self.state_dir / (
            f"{universe.policy.state_stem}-audit-{universe.generated_at_ns}-{universe.selection_sha256[:12]}.json"
        )
        self._atomic_json(audit_path, audit)
        self._prune(universe.policy.state_stem)
        return {"current_path": str(current_path), "audit_path": str(audit_path), **audit}

    @staticmethod
    def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
        encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_bytes(encoded)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _load_current(path: Path) -> TopVolumeUniverse | None:
        if not path.exists():
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping):
            raise ValueError("existing top-volume universe state is malformed")
        return TopVolumeUniverse.from_mapping(raw)

    def _prune(self, stem: str) -> None:
        paths = sorted(
            self.state_dir.glob(f"{stem}-audit-*.json"),
            key=lambda item: item.name,
        )
        for path in paths[:-self.max_audit_files]:
            path.unlink()
