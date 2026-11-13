#!/usr/bin/env python3
"""Render, gate and migrate one Data Layer declaration revision (DL v2.3.0).

Purpose: after v2.3.0, adding instruments or intervals is "declaration + gate +
ordered restart" with no certification campaign, image or tag. This is the one
tool that turns declarations into one coupled, additive revision set:

* ``render``: reads one declaration file (``qdl.v2.declaration.v1``) plus its
  adapters (Trading System ``ts.declared-execution-requirements.v1``, the alpha
  hosting intent + universe with an ``execution-alpha.data-requirements.v1``
  profile) and writes a NEW directory with every coupled artifact: demand,
  catalogue, acquisition, promotion scope, release routing, consumer manifests
  (TS explicit rows, alpha universe selectors with the accept-set floor),
  universe files, Query runtime catalogue + acquisition, ingestor configs (sticky
  lane pins + per-feed flush map), core configs, gateway bundles, sealed TS route
  binding, ``execution-scope/<consumer>.json`` and ``declaration-index.json``,
  plus ``report.json`` (BLOCKED rows, capacity estimate, coupled consumer env,
  ordered restart). A row the Data Layer cannot serve is a typed BLOCKED entry
  (``NOT_IN_CATALOG``, ...), never a silent drop.
* ``gate``: offline acceptance of a rendered directory: certified loaders,
  additivity against the base (live) revision set, lane stability, accept-set
  floor rule, bar-edge checkpoint crossing, capacity envelope, coverage chain
  (every declared row resolves; every derived bar has its parent).
* ``checkpoint-migration``: the bar-edge migration record
  (``qdl.stable-bar-edge-checkpoint-migration.v1``) for a crossing from the files
  a running bar edge loaded (e.g. image ``/app/config/v2`` r11/r19, readable from
  Git at the image revision) to the rendered runtime files.

Rules: deterministic and idempotent (same inputs -> same bytes; rendering the
rendered tree again changes nothing); strictly additive (every retained row is
kept byte for byte; a non-additive change is refused unless an explicit
migration record names it); outputs only under a new directory; no network, no
runtime, no secrets. The certified loaders and the existing renderers do the
work: ``ProductionCatalogBuilder`` (catalogue/acquisition, the stage A additive
merge), ``phase115c`` routing reference update, ``prepare_paper_read_extension``
(TS manifest + sealed binding), ``refresh_v2_native_ingestor_runtime`` and
``refresh_v2_core_runtime_bindings`` (strictly additive role configs),
``kn_gateway_bundle`` (bundle entries and hash), ``qdl.runtime.ingestor_lanes``
(sticky lanes), ``qdl.consumer.manifest_compat`` (accept-set floor) and
``qdl.runtime.bar_edge_checkpoint`` (series digests, crossing).

Boundary: reads repository/state files given on the command line or in the
declaration; writes one new output directory. It never mounts, restarts, writes
Kafka/Redis/Postgres, or edits a deployed file.

  python3 -B scripts/dl_declaration.py render DECLARATION.yaml --out NEW_DIR [--dry-run]
  python3 -B scripts/dl_declaration.py gate NEW_DIR
  python3 -B scripts/dl_declaration.py checkpoint-migration --git-rev a388537 \
      --catalog NEW_DIR/runtime/stable-source-bindings.yaml \
      --acquisition NEW_DIR/runtime/stable-acquisition-bindings.yaml \
      --reason ... --approved-by ... --out RECORD.json
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from copy import deepcopy
import hashlib
import inspect
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Iterable, Mapping

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.adapters.binance_spot import parse_spot_exchange_info  # noqa: E402
from qdl.adapters.intervals import (  # noqa: E402
    canonical_interval_ms,
    normalise_interval,
    provider_bar_calendar_anchor_ms,
)
from qdl.consumer.manifest import ConsumerManifestLoader  # noqa: E402
from qdl.consumer import quota_model  # noqa: E402
from qdl.consumer.manifest_compat import (  # noqa: E402
    additive_problems,
    declared_floor_problems,
    next_min_token_revision,
)
from qdl.consumer.realtime_route import requirement_key  # noqa: E402
from qdl.consumer.selectors import SelectorContext  # noqa: E402
from qdl.runtime import ingestor_lanes  # noqa: E402
from qdl.runtime.lineage import (  # noqa: E402
    binding_lineage,
    live_config_lineage,
    live_config_lineage_problems,
    pin_lineage,
    retained_unchanged_ids,
)
from qdl.runtime.bar_edge_checkpoint import (  # noqa: E402
    MIGRATION_SCHEMA,
    CheckpointCrossingRefused,
    binding_series_digest,
    plan_crossing,
    validate_migration_record,
)
from qdl.runtime.declaration import (  # noqa: E402
    DECLARATION_INDEX_SCHEMA,
    declared_universe_document,
    load_declaration_index,
)
from qdl.runtime.production_catalog import (  # noqa: E402
    ProductionCatalogBuilder,
    ProductionDemandManifest,
    load_binance_exchange_info,
    load_okx_instruments,
)
from qdl.runtime import stable_deployment  # noqa: E402
from qdl.runtime.stable_catalog import StableSourceCatalog  # noqa: E402
from qdl.runtime.stable_deployment import (  # noqa: E402
    AuthorityPromotionScope,
    StableAcquisitionPlan,
    validate_shared_authority_record,
)
from scripts import check_consumer_quota_model  # noqa: E402
from scripts import kn_gateway_bundle  # noqa: E402
from scripts import phase115c_materialize_active_native_bars as phase115c  # noqa: E402
from scripts import prepare_paper_read_extension as paper_extension  # noqa: E402
from scripts import refresh_v2_core_runtime_bindings as core_refresh  # noqa: E402
from scripts import refresh_v2_native_ingestor_runtime as ingestor_refresh  # noqa: E402

DECLARATION_SCHEMA = "qdl.v2.declaration.v1"
TS_REQUIREMENTS_SCHEMA = "ts.declared-execution-requirements.v1"
ALPHA_REQUIREMENTS_SCHEMA = "execution-alpha.data-requirements.v1"
ALPHA_HOSTING_SCHEMA = "alpha-hosting-source-intent.v1"
ALPHA_UNIVERSE_SCHEMA = "alpha-hosting-universe.v1"
EXECUTION_SCOPE_SCHEMA = "qdl.declared-execution-scope.v1"
REPORT_SCHEMA = "qdl.v2.declaration-render-report.v1"
GATE_SCHEMA = "qdl.v2.declaration-gate.v1"
INPUTS_SCHEMA = "qdl.v2.declaration-render-inputs.v1"

CONFIG = Path("config/v2")
REPO_FILES = {
    "demand": CONFIG / "stable-crypto-demand.yaml",
    "catalog": CONFIG / "stable-source-bindings.yaml",
    "acquisition": CONFIG / "stable-acquisition-bindings.yaml",
    "scope": CONFIG / "stable-authority-promotion-scope.yaml",
    "routing": CONFIG / "stable-v2-release-routing.yaml",
}
REPO_COPIED = (CONFIG / "stable-capabilities.yaml", CONFIG / "provider-admission-policy-kn-bar-edge-v1.json")
CAPTURES = {
    "binance_usdm": CONFIG / "captures/binance-usdm-exchangeinfo.filtered.json",
    "binance_spot": CONFIG / "captures/binance-spot-exchangeinfo.filtered.json",
    "okx_swap": CONFIG / "captures/okx-instruments-swap.filtered.json",
    "okx_spot": CONFIG / "captures/okx-instruments-spot.filtered.json",
}
CAPTURE_PROVENANCE = CONFIG / "captures/provenance.json"
TS_MARKETS = {"SWAP": "SWAP", "USD_M": "USDM", "COIN_M": "COINM", "SPOT": "SPOT", "FUTURES": "FUTURES"}
PRODUCT_TYPES = {("OKX", "SWAP"): "PERPETUAL", ("BINANCE", "USDM"): "PERPETUAL"}
EXECUTION_FEEDS = paper_extension.FEEDS  # QUOTE, MARK_INDEX_PRICE, BOOK_SNAPSHOT, BOOK_DELTA
SCOPE_FEEDS = ("TRADE", "QUOTE", "MARK_INDEX_PRICE", "BOOK_SNAPSHOT", "BOOK_DELTA")
# TS writer reader (trading_system 44c098d7 services/market_data/declared_scope.py MARKETS); widen only with it.
SCOPE_MARKETS = {"BINANCE": {"USDM"}, "OKX": {"SWAP"}}
REFERENCE_FEEDS = frozenset({"FUNDING_RATE"})
OKX_INTERVALS = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "2d", "3d", "1w")
FLUSH_FEEDS = frozenset({"QUOTE", "MARK_INDEX"})
RESTART_ORDER = (
    "ingestor_okx_swap / ingestor_binance_usdm (v2.3.0 image first: lane_shard + flush map)",
    "rust_core, rust_core_2, rust_core_3",
    "market_projector_1, market_projector_2",
    "stream_kn_1, stream_kn_2",
    "query_kn_1, query_kn_2",
    "binance_bar_edge (with the checkpoint migration record when it crosses a legacy checkpoint)",
    "consumers whose JWT is outside the new accept-set (TS writer: sealed binding + execution scope)",
)

# Capacity model. Source: scratchpad dlcov/COVERAGE_150_STUDY.md section 5.1
# (60 s public OKX WS, 150 E-crypto instruments, 2026-11-13 06:5xZ, LatestStateBuffer
# simulation) and section 3.2 (candle frames); 1.8 ms/record upper bound (PACKET 2.1).
RATE_PER_BINDING = {
    "okx_bbo": 638 / 150,
    "okx_mark_index@50": (747 + 201) / 150,
    "okx_mark_index@500": (300 + 186) / 150,
    "okx_bar": 78.8 / 150,
}
VCPU_PER_RECORD = 0.0018


class DeclarationRefused(ValueError):
    """The declaration cannot be rendered as one additive revision set."""


# --------------------------------------------------------------------------- io


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(Path(path).read_bytes())


def _yaml_bytes(value: Mapping[str, Any]) -> bytes:
    return yaml.safe_dump(dict(value), sort_keys=False, allow_unicode=False).encode("utf-8")


def _json_sorted(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _load_mapping(path: Path) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    value = json.loads(text) if str(path).endswith(".json") else yaml.safe_load(text)
    if not isinstance(value, dict):
        raise DeclarationRefused(f"document must be an object: {path}")
    return value


def _catalog_bytes(value: Mapping[str, Any], *, like: bytes) -> bytes:
    # Keep the committed serialisation style: the catalogue is JSON-form YAML.
    if like.lstrip().startswith(b"{"):
        return (json.dumps(value, indent=2) + "\n").encode("utf-8")
    return _yaml_bytes(value)


def _with_tempfile(content: bytes, name: str, loader):
    with tempfile.TemporaryDirectory(prefix="qdl-declaration-") as raw:
        path = Path(raw) / name
        path.write_bytes(content)
        return loader(path)


def _derived_final_supported() -> bool:
    """Whether the certified acquisition loader knows DERIVED_FINAL (derived-bars slice)."""
    return hasattr(stable_deployment, "StableDerivedBarAcquisition")


def _routing_selector_supported() -> bool:
    from qdl.consumer.release import StableReleaseRoutePlan

    return "selector_context" in inspect.signature(StableReleaseRoutePlan.load).parameters


def _blocked(consumer: str, reason: str, **fields: Any) -> dict[str, Any]:
    return {"consumer_id": consumer, "reason": reason, **fields}


# ------------------------------------------------------------------ declaration


def load_declaration(path: Path) -> dict[str, Any]:
    payload = _load_mapping(path)
    if payload.get("schema") != DECLARATION_SCHEMA:
        raise DeclarationRefused("declaration schema is unsupported")
    allowed = {"schema", "declaration_revision", "environment", "base", "consumers",
               "instrument_captures", "ingestor", "capacity", "accept_set", "description", "bar_edge"}
    if set(payload) - allowed:
        raise DeclarationRefused(f"declaration has unknown fields: {sorted(set(payload) - allowed)}")
    revision = payload.get("declaration_revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise DeclarationRefused("declaration_revision must be a positive integer")
    if not isinstance(payload.get("consumers"), list) or not payload["consumers"]:
        raise DeclarationRefused("declaration needs at least one consumer")
    ids = [str(item.get("consumer_id")) for item in payload["consumers"]]
    if len(ids) != len(set(ids)):
        raise DeclarationRefused("declaration consumers must be unique")
    base_dir = path.resolve().parent

    def resolve(value: Any) -> Path:
        candidate = Path(str(value)).expanduser()
        return candidate if candidate.is_absolute() else (base_dir / candidate).resolve()

    base = dict(payload.get("base") or {})
    base["repo"] = resolve(base.get("repo", ROOT))
    runtime = dict(base.get("runtime") or {})
    for name in ("authority", "query_acquisition"):
        if runtime.get(name):
            runtime[name] = resolve(runtime[name])
    runtime["ingestors"] = {k: resolve(v) for k, v in (runtime.get("ingestors") or {}).items()}
    runtime["cores"] = [resolve(v) for v in runtime.get("cores") or []]
    runtime["bundles"] = {k: resolve(v) for k, v in (runtime.get("bundles") or {}).items()}
    runtime["route_bindings"] = {k: resolve(v) for k, v in (runtime.get("route_bindings") or {}).items()}
    base["runtime"] = runtime
    payload["base"] = base
    payload["instrument_captures"] = [
        {**item, "path": resolve(item["path"])} for item in payload.get("instrument_captures") or []
    ]
    def symbols_file(item: dict) -> None:
        if item.get("symbols_file"):
            listed = json.loads(resolve(item.pop("symbols_file")).read_text(encoding="utf-8"))
            if not isinstance(listed, list) or not all(isinstance(v, str) for v in listed):
                raise DeclarationRefused("symbols_file must be a JSON list of native symbols")
            item["symbols"] = list(item.get("symbols") or []) + listed

    for consumer in payload["consumers"]:
        consumer["ts_requirements"] = [resolve(v) for v in consumer.get("ts_requirements") or []]
        for group in consumer.get("instruments") or []:
            symbols_file(group)
        for universe in consumer.get("universes") or []:
            symbols_file(universe)
            hosting = universe.get("alpha_hosting")
            if hosting:
                universe["alpha_hosting"] = {k: resolve(v) if k in {"registry", "manifest", "universe"} else v
                                             for k, v in hosting.items()}
    bar_edge = dict(payload.get("bar_edge") or {})
    migration = dict(bar_edge.get("migration") or {})
    for key in ("previous_catalog", "previous_acquisition"):
        if migration.get(key):
            migration[key] = resolve(migration[key])
    if migration:
        bar_edge["migration"] = migration
    if bar_edge.get("live_checkpoint"):
        bar_edge["live_checkpoint"] = resolve(bar_edge["live_checkpoint"])
    payload["bar_edge"] = bar_edge
    payload["ingestor"] = dict(payload.get("ingestor") or {})
    payload["capacity"] = dict(payload.get("capacity") or {})
    payload.setdefault("accept_set", True)
    payload.setdefault("environment", "paper")
    return payload


def ts_requirement_rows(path: Path, *, consumer_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Adapter: TS ``ts.declared-execution-requirements.v1`` -> DL execution rows."""
    document = _load_mapping(path)
    if document.get("schema") != TS_REQUIREMENTS_SCHEMA:
        raise DeclarationRefused(f"{path} is not {TS_REQUIREMENTS_SCHEMA}")
    if document.get("consumer_id") != consumer_id:
        raise DeclarationRefused(f"{path} declares consumer {document.get('consumer_id')}, not {consumer_id}")
    requirements = document.get("requirements")
    if not isinstance(requirements, list) or document.get("requirements_sha256") != _sha256_bytes(
            json.dumps(requirements, sort_keys=True, separators=(",", ":")).encode("utf-8")):
        raise DeclarationRefused(f"{path} requirements_sha256 does not match its requirements")
    rows, blocked = [], []
    for item in requirements:
        market = TS_MARKETS.get(str(item.get("market")).upper())
        identity = dict(venue=str(item.get("venue")).upper(), market=market,
                        native_symbol=str(item.get("native_symbol")), feed=str(item.get("feed")).upper())
        if str(item.get("role", "READ")).upper() != "READ":
            blocked.append(_blocked(consumer_id, "ROLE_NOT_A_DL_READ_GRANT", **identity))
        elif str(item.get("consumer_grade")).upper() != "EXECUTION":
            blocked.append(_blocked(consumer_id, "GRADE_NOT_EXECUTION", **identity))
        elif market is None:
            blocked.append(_blocked(consumer_id, "MARKET_TOKEN_UNMAPPED", **identity))
        else:
            rows.append(identity)
    return rows, blocked


def alpha_hosting_universe(spec: Mapping[str, Any]) -> dict[str, Any]:
    """Adapter: alpha hosting intent + universe (+ data-requirements profile) -> signal universe."""
    intent = _load_mapping(Path(spec["manifest"]))
    universe = _load_mapping(Path(spec["universe"]))
    if intent.get("schema") != ALPHA_HOSTING_SCHEMA or universe.get("schema") != ALPHA_UNIVERSE_SCHEMA:
        raise DeclarationRefused("alpha hosting manifest/universe schema is unsupported")
    if (intent.get("signal_provider"), intent.get("product")) != (universe.get("venue"), universe.get("product")):
        raise DeclarationRefused("alpha hosting signal provider/product differs from its universe")
    if intent.get("bar_clock") not in (None, "UTC_CLOSED_ONLY"):
        raise DeclarationRefused("alpha hosting bar clock must be UTC closed-only bars")
    reference: list[str] = []
    if spec.get("registry"):
        registry = _load_mapping(Path(spec["registry"]))
        if registry.get("schema") != ALPHA_REQUIREMENTS_SCHEMA:
            raise DeclarationRefused("alpha data requirements registry schema is unsupported")
        profile = (registry.get("profiles") or {}).get(spec.get("profile"))
        if not profile:
            raise DeclarationRefused(f"alpha data requirements profile is unknown: {spec.get('profile')}")
        feeds = {str(item["feed"]).upper() for item in profile["feeds"]}
        if "BAR" not in feeds:
            raise DeclarationRefused("alpha profile declares no BAR feed")
        reference = sorted(feeds & REFERENCE_FEEDS)
    return {
        "venue": str(universe["venue"]).upper(),
        "market": str(universe["product"]).upper(),
        "symbols": [str(item["venue_symbol"]) for item in universe["selected"]],
        "intervals": [normalise_interval(str(item)) for item in intent.get("intervals") or []],
        "reference": reference,
        "provenance": {
            "manifest_sha256": _sha256_file(Path(spec["manifest"])),
            "universe_sha256": _sha256_file(Path(spec["universe"])),
            "alpha_family": intent.get("alpha_family"),
            "status": intent.get("status"),
        },
    }


# ---------------------------------------------------------------------- base set


class BaseSet:
    """The live revision set a render is additive against (read-only)."""

    def __init__(self, declaration: Mapping[str, Any]) -> None:
        self.repo = Path(declaration["base"]["repo"])
        self.runtime = declaration["base"]["runtime"]
        self.raw: dict[str, bytes] = {name: (self.repo / rel).read_bytes() for name, rel in REPO_FILES.items()}
        self.docs = {name: (json.loads(raw) if raw.lstrip().startswith(b"{") else yaml.safe_load(raw))
                     for name, raw in self.raw.items()}
        self.catalog = StableSourceCatalog.load(self.repo / REPO_FILES["catalog"])
        self.acquisition = StableAcquisitionPlan.load(self.repo / REPO_FILES["acquisition"], catalog=self.catalog)

    def path(self, rel: Path) -> Path:
        return self.repo / rel

    def input_hashes(self) -> dict[str, str]:
        result = {str(rel): _sha256_bytes(self.raw[name]) for name, rel in REPO_FILES.items()}
        for name in ("authority", "query_acquisition"):
            if self.runtime.get(name):
                result[f"runtime:{name}"] = _sha256_file(self.runtime[name])
        for group in ("ingestors", "bundles", "route_bindings"):
            for key, path in (self.runtime.get(group) or {}).items():
                result[f"runtime:{group}:{key}"] = _sha256_file(path)
        for path in self.runtime.get("cores") or []:
            result[f"runtime:cores:{Path(path).name}"] = _sha256_file(path)
        return result


# ------------------------------------------------------------------ render core


class Render:
    def __init__(self, declaration: Mapping[str, Any]) -> None:
        self.declaration = declaration
        self.base = BaseSet(declaration)
        self.blocked: list[dict[str, Any]] = []
        self.notes: list[str] = []
        self.files: dict[str, bytes] = {}          # output relative path -> bytes
        self.derived: dict[tuple[str, str], str] = {}  # (uid, interval) -> parent interval
        self.manifests: dict[str, dict[str, Any]] = {}
        self.manifest_changed: dict[str, bool] = {}
        self.universes: dict[str, dict[str, Any]] = {}
        self.env: dict[str, dict[str, str]] = {}
        self.summary: dict[str, Any] = {}
        self._execution_added: list[tuple[str, str]] = []
        self.quota_models: dict[str, dict[str, Any]] = {}

    # ---------------------------------------------------------------- captures
    def _captures(self) -> dict[str, Path]:
        """Base captures plus verbatim appended OKX instrument rows (never by name alias)."""
        paths = {name: self.base.path(rel) for name, rel in CAPTURES.items()}
        extra = self.declaration["instrument_captures"]
        if not extra:
            return paths
        okx = json.loads(paths["okx_swap"].read_text(encoding="utf-8"))
        rows = okx["data"]
        known = {row["instId"] for row in rows}
        provenance = json.loads(self.base.path(CAPTURE_PROVENANCE).read_text(encoding="utf-8"))
        entry = next(item for item in provenance["captures"] if item["venue"] == "OKX" and item["market"] == "SWAP")
        appended = 0
        for item in extra:
            raw = Path(item["path"]).read_bytes()
            body = json.loads(raw)
            if str(body.get("code")) != "0" or not isinstance(body.get("data"), list):
                raise DeclarationRefused(f"{item['path']} is not a successful OKX V5 instruments response")
            for row in body["data"]:
                if row.get("instType") != "SWAP" or row.get("instId") in known:
                    continue
                rows.append(row)
                known.add(row["instId"])
                appended += 1
                entry.setdefault("appended_captures", []).append({
                    "captured_at_utc": str(item["captured_at_utc"]),
                    "endpoint": str(item["endpoint"]),
                    "full_response_sha256": _sha256_bytes(raw),
                    "symbols_added": 1,
                    "note": f"verbatim row appended by declaration r{self.declaration['declaration_revision']}",
                })
        if appended:
            capture_bytes = json.dumps(okx, separators=(",", ":")).encode("utf-8")
            entry["filtered_capture_sha256"] = _sha256_bytes(capture_bytes)
            entry["filtered_capture_bytes"] = len(capture_bytes)
            self.files[f"repo/{CAPTURES['okx_swap']}"] = capture_bytes
            self.files[f"repo/{CAPTURE_PROVENANCE}"] = (json.dumps(provenance, indent=2) + "\n").encode()
        return paths

    def _capture_index(self, paths: Mapping[str, Path]) -> set[tuple[str, str, str]]:
        okx_rel = f"repo/{CAPTURES['okx_swap']}"
        swap = (json.loads(self.files[okx_rel])["data"] if okx_rel in self.files
                else load_okx_instruments(paths["okx_swap"]))
        result = {("OKX", "SWAP", str(row["instId"])) for row in swap if row.get("state", "live") == "live"}
        usdm = load_binance_exchange_info(paths["binance_usdm"])
        result |= {(r.identity.venue, r.identity.market, r.native_symbol) for r in usdm.records}
        return result

    # ------------------------------------------------------------- intake rows
    def _instrument(self, venue: str, market: str, symbol: str):
        for record in self.base.catalog.instruments:
            identity = record.identity
            if (identity.venue, identity.market, record.native_symbol) == (venue, market, symbol):
                return record
        return None

    def intake(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Split declarations into EXECUTION rows and SIGNAL universes (both ordered, deduplicated)."""
        execution: list[dict[str, Any]] = []
        signal: list[dict[str, Any]] = []
        seen: set[tuple] = set()
        for consumer in self.declaration["consumers"]:
            cid = str(consumer["consumer_id"])
            klass = str(consumer.get("class", "")).upper()
            if klass == "EXECUTION":
                rows: list[dict[str, Any]] = []
                for group in consumer.get("instruments") or []:
                    for symbol in group["symbols"]:
                        for feed in (str(f).upper() for f in group["feeds"]):
                            rows.append(dict(venue=str(group["venue"]).upper(), market=str(group["market"]).upper(),
                                             native_symbol=str(symbol), feed=feed))
                for path in consumer["ts_requirements"]:
                    adapted, blocked = ts_requirement_rows(path, consumer_id=cid)
                    rows.extend(adapted)
                    self.blocked.extend(blocked)
                for row in rows:
                    key = (cid, row["venue"], row["market"], row["native_symbol"], row["feed"])
                    if key not in seen:
                        seen.add(key)
                        execution.append({"consumer_id": cid, **row})
            elif klass == "SIGNAL":
                for universe in consumer.get("universes") or []:
                    item = dict(universe)
                    if item.get("alpha_hosting"):
                        adapted = alpha_hosting_universe(item["alpha_hosting"])
                        item.setdefault("venue", adapted["venue"])
                        item.setdefault("market", adapted["market"])
                        item["symbols"] = list(item.get("symbols") or []) + adapted["symbols"]
                        bars = dict(item.get("bars") or {})
                        native = [normalise_interval(i) for i in bars.get("native") or ["1m"]]
                        derived = [normalise_interval(i) for i in bars.get("derived") or []]
                        derived += [i for i in adapted["intervals"] if i not in native and i not in derived]
                        item["bars"] = {"native": native, "derived": derived}
                        item["reference"] = sorted(set(item.get("reference") or []) | set(adapted["reference"]))
                        item["provenance"] = adapted["provenance"]
                    item["consumer_id"] = cid
                    item["symbols"] = list(dict.fromkeys(str(s) for s in item.get("symbols") or []))
                    signal.append(item)
            else:
                raise DeclarationRefused(f"consumer {cid} class must be EXECUTION or SIGNAL")
        return execution, signal

    # ------------------------------------------------------------------ demand
    def _demand_template(self, consumer_id: str, feed: str, interval: str | None, template_symbol: str):
        for consumer in self.base.docs["demand"]["consumers"]:
            if consumer["consumer_id"] != consumer_id:
                continue
            for row in consumer["requirements"]:
                if row["native_symbol"] == template_symbol and row["feed"] == feed and (
                        feed != "BAR" or row.get("interval") == (interval or row.get("interval"))):
                    return row
        return None

    def build_demand(self, execution, signal, capture_index) -> tuple[dict[str, Any], list[tuple]]:
        demand = deepcopy(self.base.docs["demand"])
        consumers = {item["consumer_id"]: item for item in demand["consumers"]}
        existing = {phase115c._requirement_key(row) + (row["source_policy_id"],)
                    for item in demand["consumers"] for row in item["requirements"]}
        added: list[tuple] = []
        derived_supported = _derived_final_supported()
        self.accepted_execution: list[dict[str, Any]] = []
        self.accepted_signal: list[dict[str, Any]] = []

        def admit(consumer_id, venue, market, symbol, feed, interval, template_symbol, *, index_rule,
                  bar_source=None):
            product = PRODUCT_TYPES.get((venue, market))
            template = self._demand_template(consumer_id, feed, interval, template_symbol)
            if template is None:
                return "NO_DEMAND_TEMPLATE"
            row = deepcopy(template)
            row.update(native_symbol=symbol, product_type=product)
            if feed == "BAR":
                row["interval"] = interval
                row.pop("bar_source", None)
                if bar_source is not None:
                    row["bar_source"] = bar_source
            if "index_native_symbol" in row:
                if not index_rule:
                    return "INDEX_SYMBOL_UNPROVEN"
                row["index_native_symbol"] = symbol[: -len("-SWAP")]
            key = phase115c._requirement_key(row) + (row["source_policy_id"],)
            if key in existing:
                return None
            if consumer_id not in consumers:
                return "CONSUMER_NOT_IN_DEMAND"
            consumers[consumer_id]["requirements"].append(row)
            existing.add(key)
            added.append(key)
            return None

        for row in execution:
            venue, market, symbol, feed, cid = (row["venue"], row["market"], row["native_symbol"],
                                                row["feed"], row["consumer_id"])
            identity = dict(venue=venue, market=market, native_symbol=symbol, feed=feed)
            reason = self._admissible(venue, market, symbol, capture_index)
            if reason is None and feed not in EXECUTION_FEEDS:
                reason = "FEED_NOT_DECLARABLE"
            if reason is None and (venue, market) != ("OKX", "SWAP"):
                reason = "EXECUTION_MARKET_NOT_DECLARABLE"
            if reason is None and not symbol.endswith("-USDT-SWAP"):
                reason = "NOT_LINEAR_USDT_SWAP"
            if reason is None and cid != paper_extension.CONSUMER:
                reason = "EXECUTION_CONSUMER_NOT_DECLARABLE"
            if reason is None:
                reason = admit(cid, venue, market, symbol, feed, None, "SOL-USDT-SWAP", index_rule=True)
            if reason:
                self.blocked.append(_blocked(cid, reason, **identity))
            else:
                self.accepted_execution.append(row)
        for universe in signal:
            cid, venue, market = universe["consumer_id"], str(universe["venue"]).upper(), str(universe["market"]).upper()
            bars = universe.get("bars") or {}
            native = [normalise_interval(i) for i in bars.get("native") or []]
            derived = [normalise_interval(i) for i in bars.get("derived") or []]
            members: list[str] = []
            for symbol in universe["symbols"]:
                reason = self._admissible(venue, market, symbol, capture_index)
                if reason:
                    self.blocked.append(_blocked(cid, reason, venue=venue, market=market, native_symbol=symbol,
                                                 universe_id=universe["universe_id"]))
                    continue
                members.append(symbol)
            intervals = []
            for interval in native + derived:
                if (venue, market) != ("OKX", "SWAP") or interval not in OKX_INTERVALS:
                    self.blocked.append(_blocked(cid, "INTERVAL_NOT_DECLARABLE", venue=venue, market=market,
                                                 interval=interval, universe_id=universe["universe_id"]))
                    continue
                if interval in derived and not derived_supported:
                    self.blocked.append(_blocked(cid, "DERIVED_FINAL_UNSUPPORTED_BY_LOADER", venue=venue,
                                                 market=market, interval=interval,
                                                 universe_id=universe["universe_id"], members=len(members)))
                    continue
                intervals.append(interval)
            declared_source = universe.get("bar_1m_source")
            if declared_source is not None and str(declared_source).lower() not in {"native", "rest_final"}:
                raise DeclarationRefused(f"{cid} {universe['universe_id']}: bar_1m_source is native or rest_final")
            source = None if declared_source is None else str(declared_source).upper()
            for symbol in members:
                for interval in intervals:
                    reason = admit(cid, venue, market, symbol, "BAR", interval, "SOL-USDT-SWAP", index_rule=False,
                                   bar_source=source if interval in native and interval not in derived else None)
                    if reason:
                        raise DeclarationRefused(f"{cid} BAR {interval}: {reason}")
            self.accepted_signal.append({**universe, "venue": venue, "market": market, "members": members,
                                         "intervals": intervals, "derived": [i for i in intervals if i in derived]})
        if added:
            demand["revision"] = int(demand["revision"]) + 1
        return demand, added

    def _admissible(self, venue, market, symbol, capture_index) -> str | None:
        if (venue, market) not in PRODUCT_TYPES:
            return "MARKET_NOT_DECLARABLE"
        if self._instrument(venue, market, symbol) is not None:
            return None
        if (venue, market, symbol) in capture_index:
            return None
        return "NOT_IN_CATALOG"

    # ------------------------------------------------------- catalog + acquisition
    def build_catalog(self, demand: Mapping[str, Any], capture_paths: Mapping[str, Path]):
        """The stage A additive merge: builder output, retained rows kept as committed, new rows appended."""
        base_catalog_doc = self.base.docs["catalog"]
        base_acq_doc = self.base.docs["acquisition"]
        okx_rel = f"repo/{CAPTURES['okx_swap']}"
        if okx_rel in self.files:
            okx_rows = json.loads(self.files[okx_rel])["data"]
        else:
            okx_rows = load_okx_instruments(capture_paths["okx_swap"])
        manifest = _with_tempfile(_yaml_bytes(demand), "demand.yaml",
                                  lambda p: ProductionDemandManifest.load_many([p]))
        revision = self.base.catalog.catalog_revision + 1
        bundle = ProductionCatalogBuilder(
            catalog_revision=revision,
            source_policy_revision=self.base.catalog.source_policy_revision,
            authority_revision=self.base.catalog.authority_revision,
        ).build(
            demand=manifest,
            binance_usdm=load_binance_exchange_info(capture_paths["binance_usdm"]),
            binance_spot=parse_spot_exchange_info(
                json.loads(capture_paths["binance_spot"].read_text(encoding="utf-8")), valid_from_ns=0),
            okx_rows=(*okx_rows, *load_okx_instruments(capture_paths["okx_spot"])),
        )
        generated_catalog, generated_acq = bundle.source_catalog, bundle.acquisition_plan
        have_instruments = {row["instrument_id"]: row for row in base_catalog_doc["instruments"]}
        uid_map: dict[str, str] = {}
        new_instruments = []
        for row in generated_catalog["instruments"]:
            current = have_instruments.get(row["instrument_id"])
            if current is None:
                new_instruments.append(row)
            elif current["instrument_uid"] != row["instrument_uid"]:
                raise DeclarationRefused(f"instrument uid differs for {row['instrument_id']}")
            else:
                uid_map[row["instrument_uid"]] = current["instrument_uid"]

        def requirement(row):
            return (uid_map.get(row["instrument_uid"], row["instrument_uid"]), row["feed"], row.get("interval"))

        have_bindings = {row["binding_id"] for row in base_catalog_doc["bindings"]}
        have_requirements = {requirement(row) for row in base_catalog_doc["bindings"]}
        new_bindings = [row for row in generated_catalog["bindings"] if requirement(row) not in have_requirements]
        if any(row["binding_id"] in have_bindings for row in new_bindings):
            raise DeclarationRefused("a new requirement reuses an existing binding id")
        drift = sum(1 for row in generated_catalog["bindings"] if row["binding_id"] in have_bindings
                    and row != next(b for b in base_catalog_doc["bindings"] if b["binding_id"] == row["binding_id"]))
        new_ids = [row["binding_id"] for row in new_bindings]
        acq_by_id = {row["binding_id"]: row for row in generated_acq["bindings"]}
        new_acq = [deepcopy(acq_by_id[binding_id]) for binding_id in new_ids]
        derived_ids = self._derive(new_bindings, new_acq)
        # Base (non-derived) signal intervals: the builder decides the lane from the declared ``bar_1m_source``
        # (demand ``bar_source``) or, without one, the bar-family rule (qdl.runtime.universal_demand).
        native_intervals = {i for u in self.accepted_signal for i in (u.get("bars") or {}).get("native") or []}
        interval_of = {row["binding_id"]: row.get("interval") for row in new_bindings if row["feed"] == "BAR"}
        native_modes = Counter(f"{interval_of[row['binding_id']]}:{row['mode']}" for row in new_acq
                               if interval_of.get(row["binding_id"]) in native_intervals)
        catalog_doc = deepcopy(base_catalog_doc)
        acq_doc = deepcopy(base_acq_doc)
        if new_bindings or new_instruments:
            catalog_doc["instruments"].extend(new_instruments)
            catalog_doc["bindings"].extend(new_bindings)
            catalog_doc["catalog_revision"] = revision
            acq_doc["bindings"].extend(new_acq)
            acq_doc["revision"] = int(base_acq_doc["revision"]) + 1
            # DL v2.3.0 2d: every retained-unchanged binding keeps its contract
            # lineage; added rows carry the new revisions (qdl.runtime.lineage).
            catalog_doc, acq_doc, self.summary["lineage"] = pin_lineage(
                base_catalog_doc, base_acq_doc, catalog_doc, acq_doc, live_config=self.live_config())
            pinned_acq = {row["binding_id"]: row for row in acq_doc["bindings"]}
            new_acq = [pinned_acq[row["binding_id"]] for row in new_acq]
        self.summary["catalog"] = {
            "revision": [self.base.catalog.catalog_revision, catalog_doc["catalog_revision"]],
            "new_instruments": [row["native_symbol"] for row in new_instruments],
            "new_bindings": len(new_bindings),
            "new_bindings_by_feed": dict(sorted(Counter(row["feed"] for row in new_bindings).items())),
            "generated_rows_differing_from_committed_kept_committed": drift,
        }
        self.summary["acquisition"] = {
            "revision": [int(base_acq_doc["revision"]), int(acq_doc["revision"])],
            "new_rows_by_mode": dict(sorted(Counter(row["mode"] for row in new_acq).items())),
            "derived_final": len(derived_ids),
            "base_bar_modes": dict(sorted(native_modes.items())),
        }
        return catalog_doc, acq_doc, new_ids, new_acq

    def _derive(self, new_bindings, new_acq) -> list[str]:
        """Turn declared derived BAR rows into DERIVED_FINAL with the largest finer materialised parent."""
        wanted = {(universe["consumer_id"], interval) for universe in self.accepted_signal
                  for interval in universe["derived"]}
        if not wanted:
            return []
        derived_intervals = {interval for _cid, interval in wanted}
        materialised: dict[str, set[str]] = defaultdict(set)
        for row in self.base.docs["catalog"]["bindings"] + list(new_bindings):
            if row["feed"] == "BAR" and row.get("interval"):
                materialised[row["instrument_uid"]].add(row["interval"])
        retained_native = {(row["instrument_uid"], row.get("interval")) for row in self.base.docs["catalog"]["bindings"]}
        source = {row["binding_id"]: row for row in new_bindings}
        result = []
        for acq in new_acq:
            row = source[acq["binding_id"]]
            interval = row.get("interval")
            if row["feed"] != "BAR" or interval not in derived_intervals or (row["instrument_uid"], interval) in retained_native:
                continue
            target_ms = canonical_interval_ms(interval)
            target_anchor = provider_bar_calendar_anchor_ms(interval, provider="OKX")
            candidates = [
                other for other in materialised[row["instrument_uid"]]
                if canonical_interval_ms(other) < target_ms and target_ms % canonical_interval_ms(other) == 0
                and (target_anchor - provider_bar_calendar_anchor_ms(other, provider="OKX"))
                % canonical_interval_ms(other) == 0
            ]
            if not candidates:
                raise DeclarationRefused(f"derived BAR {acq['binding_id']} has no finer materialised parent")
            parent = max(candidates, key=canonical_interval_ms)
            acq.update(mode="DERIVED_FINAL", sequence_policy="NONE", websocket_url=None,
                       business_websocket_url=None, market_websocket_url=None,
                       derived_from={"interval": parent})
            self.derived[(row["instrument_uid"], interval)] = parent
            result.append(acq["binding_id"])
        return result

    def build_scope(self, new_ids: list[str]) -> dict[str, Any]:
        scope = deepcopy(self.base.docs["scope"])
        catalog_rows = {row["binding_id"]: row for row in self._catalog_doc["bindings"]}
        instruments = {row["instrument_uid"]: row for row in self._catalog_doc["instruments"]}
        additions = [
            binding_id for binding_id in new_ids
            if (instruments[catalog_rows[binding_id]["instrument_uid"]]["venue"],
                instruments[catalog_rows[binding_id]["instrument_uid"]]["market"]) in phase115c.ACTIVE_BAR_FAMILIES
        ]
        merged = sorted(set(map(str, scope["binding_ids"])) | set(additions))
        if merged != list(scope["binding_ids"]):
            scope["binding_ids"] = merged
            scope["revision"] = int(scope["revision"]) + 1
        return scope

    # ------------------------------------------------------------------ manifests
    def _selector_context(self, catalog: StableSourceCatalog, universe_dir: Path) -> SelectorContext:
        return SelectorContext.from_catalog(catalog, universe_dir=universe_dir)

    def build_manifests(self, catalog_doc: Mapping[str, Any]) -> None:
        catalog = StableSourceCatalog.from_mapping(catalog_doc)
        instrument_by_symbol = {(r.identity.venue, r.identity.market, r.native_symbol): r for r in catalog.instruments}
        accept_set = bool(self.declaration["accept_set"])
        for consumer in self.declaration["consumers"]:
            cid = str(consumer["consumer_id"])
            rel = Path(consumer["manifest"])
            raw = self.base.path(rel).read_bytes()
            base_doc = yaml.safe_load(raw)
            target = deepcopy(base_doc)
            klass = str(consumer.get("class")).upper()
            changed = False
            if klass == "EXECUTION":
                changed = self._execution_manifest(cid, consumer, target, instrument_by_symbol)
            else:
                changed = self._signal_manifest(cid, consumer, target, catalog)
            quotas = consumer.get("quotas") or {}
            for name, value in sorted(quotas.items()):
                current = target["spec"]["quotas"][name]
                if type(value) is not int or value < current:
                    raise DeclarationRefused(f"{cid} quota {name} may only be raised")
                if value > current:
                    target["spec"]["quotas"][name] = value
                    changed = True
            changed = self._apply_quota_model(cid, klass, target, quotas) or changed
            if changed:
                # A retired revision is skipped, never reused (v2.2.6 r12 -> r13): ``revision`` in the declaration.
                base_revision = int(base_doc["metadata"]["revision"])
                wanted = consumer.get("revision")
                if wanted is not None and int(wanted) <= base_revision:
                    raise DeclarationRefused(f"{cid} target revision {wanted} must be above the base {base_revision}")
                target["metadata"]["revision"] = base_revision + 1 if wanted is None else int(wanted)
            self.manifests[cid] = {"rel": rel, "base_raw": raw, "base": base_doc, "doc": target,
                                   "class": klass}
            self.manifest_changed[cid] = changed
        # accept-set floor, decided on the EXPANDED manifests (selector context)
        universe_dir = self._universe_dir_for_load()
        context = self._selector_context(catalog, universe_dir)
        base_context = self._selector_context(self.base.catalog, self.base.path(CONFIG / "universes"))
        for cid, item in self.manifests.items():
            if not self.manifest_changed[cid]:
                continue
            previous = ConsumerManifestLoader.from_mapping(item["base"], selector_context=base_context)
            if accept_set:
                candidate = ConsumerManifestLoader.from_mapping(item["doc"], selector_context=context)
                floor = next_min_token_revision(previous, candidate)
                if floor < candidate.manifest_revision:
                    item["doc"]["metadata"]["min_token_revision"] = floor
                elif "min_token_revision" in item["doc"]["metadata"]:
                    item["doc"]["metadata"]["min_token_revision"] = floor
            parsed = ConsumerManifestLoader.from_mapping(item["doc"], selector_context=context)
            item["parsed"] = parsed
            item["previous"] = previous
            item["additive_problems"] = additive_problems(previous, parsed)

    def _apply_quota_model(self, cid: str, klass: str, target: dict[str, Any], declared: Mapping[str, Any]) -> bool:
        """EXECUTION quotas are derived from the declared products (qdl.consumer.quota_model), never hand-picked.

        A declared quota below the model is refused; the granted quota is max(current, declared, model) so a
        quota never falls. SIGNAL (alpha) manifests keep their declared quotas and are marked MODEL_PENDING:
        the model covers the Trading System writer's client only.
        """
        if klass != "EXECUTION":
            self.quota_models[cid] = {"status": "MODEL_PENDING",
                                      "reason": "no alpha client request model; quotas as declared"}
            return False
        model = quota_model.model_manifest(target)
        required = {"requests_per_minute": model.required_requests_per_minute,
                    "max_streams": model.required_max_streams}
        for name, value in sorted(declared.items()):
            if name in required and value < required[name]:
                raise DeclarationRefused(f"{cid} declares {name}={value} below the quota model's {required[name]}")
        changed = False
        for name, value in required.items():
            if target["spec"]["quotas"][name] < value:
                target["spec"]["quotas"][name] = value
                changed = True
        if quota_model.quota_shortfall(target):
            raise DeclarationRefused(f"{cid} quotas do not cover the quota model")
        self.quota_models[cid] = {"status": "MODELLED", "evidence": model.evidence(),
                                  "granted": {name: target["spec"]["quotas"][name] for name in required}}
        return changed

    def _execution_manifest(self, cid, consumer, target, instrument_by_symbol) -> bool:
        rows = [row for row in self.accepted_execution if row["consumer_id"] == cid]
        if not rows:
            return False
        requirements = {paper_extension.key(r): r for r in target["spec"]["requirements"]}
        template = paper_extension._okx_swap(self._catalog_doc, "ETH-USDT-SWAP")
        added = []
        # Manifest/binding convention of prepare_paper_read_extension: per symbol, feeds sorted.
        order = list(dict.fromkeys(row["native_symbol"] for row in rows))
        rows = sorted(rows, key=lambda row: (order.index(row["native_symbol"]), row["feed"]))
        for row in rows:
            record = instrument_by_symbol.get((row["venue"], row["market"], row["native_symbol"]))
            policy = "crypto_primary_v2" if row["feed"] == "QUOTE" else "crypto_liquid_v2"
            k = (template["instrument_uid"], row["feed"], None, policy)
            if k not in requirements:
                self.blocked.append(_blocked(cid, "NO_EXECUTION_TEMPLATE", **{x: row[x] for x in
                                    ("venue", "market", "native_symbol", "feed")}))
                continue
            new_key = (record.identity.instrument_uid, row["feed"], None, policy)
            if new_key in requirements:
                continue
            requirement = deepcopy(requirements[k])
            requirement["instrument_uid"] = record.identity.instrument_uid
            target["spec"]["requirements"].append(requirement)
            requirements[new_key] = requirement
            added.append(requirement)
            self._execution_added.append((row["native_symbol"], row["feed"]))
        return bool(added)

    def _signal_manifest(self, cid, consumer, target, catalog) -> bool:
        universes = [u for u in self.accepted_signal if u["consumer_id"] == cid]
        if not universes:
            return False
        selectors = target["spec"].setdefault("selectors", [])
        before = len(selectors)
        bar_rows = [r for r in target["spec"]["requirements"] if r["feed"] == "BAR"]
        for universe in universes:
            universe_id = str(universe["universe_id"])
            history = {normalise_interval(k) if k != "default" else k: int(v)
                       for k, v in (universe.get("history") or {}).items()}
            entries = []
            granted = [normalise_interval(i) for i in universe.get("grant_intervals") or universe["intervals"]]
            for interval in universe["intervals"]:
                if interval not in granted:
                    continue  # acquired (e.g. a 1m derivation parent) but not entitled to this consumer
                template = next((r for r in bar_rows if r.get("interval") == interval), None)
                if template is None:
                    raise DeclarationRefused(f"{cid} has no BAR {interval} manifest template row")
                body = {k: v for k, v in template.items() if k not in {"instrument_uid", "interval"}}
                if interval in history or "default" in history:
                    body["warmup_limit"] = history.get(interval, history.get("default"))
                entries.append({"universe": universe_id, "venue": universe["venue"], "market": universe["market"],
                                "intervals": [interval], "template": body})
            for feed in universe.get("reference") or []:
                template = next((r for r in target["spec"]["requirements"] if r["feed"] == feed), None)
                if template is None:
                    self.blocked.append(_blocked(cid, "NO_REFERENCE_TEMPLATE", feed=feed, universe_id=universe_id))
                    continue
                body = {k: v for k, v in template.items() if k != "instrument_uid"}
                entries.append({"universe": universe_id, "venue": universe["venue"], "market": universe["market"],
                                "template": body})
            for entry in entries:
                if entry not in selectors:
                    selectors.append(entry)
            self._universe_document(universe)
        if not selectors:
            target["spec"].pop("selectors")
        return len(selectors) != before

    def _universe_document(self, universe: Mapping[str, Any]) -> None:
        universe_id = str(universe["universe_id"])
        members = sorted(set(universe["members"]))
        base_path = self.base.path(CONFIG / "universes" / f"{universe_id}.json")
        revision = 1
        if base_path.is_file():
            previous = json.loads(base_path.read_text(encoding="utf-8"))
            removed = set(previous.get("members") or ()) - set(members)
            if removed:
                raise DeclarationRefused(
                    f"universe {universe_id} removes members {sorted(removed)[:5]}; a removal needs its own "
                    "migration declaration")
            members = sorted(set(members) | set(previous.get("members") or ()))
            revision = int(previous["revision"]) + (members != sorted(previous.get("members") or ()))
        if "revision" in universe and int(universe["revision"]) > revision:
            revision = int(universe["revision"])
        document = declared_universe_document(universe_id=universe_id, revision=revision,
                                              venue=universe["venue"], market=universe["market"], members=members)
        if base_path.is_file() and json.loads(base_path.read_text(encoding="utf-8")) == document:
            payload = base_path.read_bytes()
        else:
            payload = _json_sorted(document)
        self.universes[universe_id] = document
        self.files[f"repo/{CONFIG}/universes/{universe_id}.json"] = payload
        self.files[f"runtime/universes/{universe_id}.json"] = payload

    def _universe_dir_for_load(self) -> Path:
        # Rendered universes merged over the base directory, for in-memory loads.
        if not hasattr(self, "_universe_tmp"):
            self._universe_tmp = tempfile.TemporaryDirectory(prefix="qdl-declaration-universes-")
            root = Path(self._universe_tmp.name)
            base_dir = self.base.path(CONFIG / "universes")
            if base_dir.is_dir():
                for path in base_dir.glob("*.json"):
                    shutil.copyfile(path, root / path.name)
        root = Path(self._universe_tmp.name)
        for rel, payload in self.files.items():
            if rel.startswith("runtime/universes/"):
                (root / Path(rel).name).write_bytes(payload)
        return root

    # ------------------------------------------------------------------- routing
    def build_routing(self, demand_bytes: bytes, catalog_bytes: bytes, demand_doc, catalog_doc) -> dict[str, Any]:
        summary = {
            "source_catalog_sha256": _sha256_bytes(catalog_bytes),
            "catalog_revision": int(catalog_doc["catalog_revision"]),
            "demand_sha256": _sha256_bytes(demand_bytes),
            "demand_revision": int(demand_doc["revision"]),
        }
        routing = phase115c._update_release_route(self.base.docs["routing"], summary=summary)
        catalog = StableSourceCatalog.from_mapping(catalog_doc)
        bumped = routing["revision"] != self.base.docs["routing"]["revision"]
        changed_products = False
        for consumer in routing["consumers"]:
            item = self.manifests.get(consumer["consumer_id"])
            if item is None or not self.manifest_changed[consumer["consumer_id"]]:
                continue
            parsed = item["parsed"]
            consumer["manifest_revision"] = parsed.manifest_revision
            consumer["manifest_sha256"] = parsed.manifest_sha256
            routes = {product["requirement_key"]: product for product in consumer["products"]}
            for requirement in parsed.requirements:
                rk = requirement_key(requirement)
                if rk in routes:
                    continue
                wanted = (requirement.feed.value, requirement.interval or "", requirement.source_policy_id)
                identity = catalog.instrument_for(requirement.instrument_uid).identity
                candidates = [p for k, p in routes.items() if tuple(k.split(":")[1:4]) == wanted]
                same_market = [p for p in candidates if (lambda i: (i.venue, i.market))(
                    catalog.instrument_for(p["requirement_key"].split(":")[0]).identity)
                    == (identity.venue, identity.market)]
                template = (same_market or candidates or [None])[0]
                if template is None:
                    raise DeclarationRefused(f"{consumer['consumer_id']} has no product route template for "
                                             f"{requirement.feed.value} {requirement.interval}")
                product = {"requirement_key": rk, "route": template["route"], "fallback": template["fallback"],
                           "reason": template["reason"]}
                consumer["products"].append(product)
                routes[rk] = product
                changed_products = True
        if (changed_products or any(self.manifest_changed.values())) and not bumped:
            routing["revision"] = int(routing["revision"]) + 1
        return routing

    # ------------------------------------------------------------- runtime files
    def live_config(self) -> dict[str, int]:
        """binding -> config_revision the LIVE ingestors stamp (review H2: Binance 19 while the plan said 21)."""
        if not hasattr(self, "_live_config"):
            configs = [json.loads(Path(path).read_text(encoding="utf-8"))
                       for path in (self.base.runtime.get("ingestors") or {}).values()]
            self._live_config = live_config_lineage(self.base.catalog, configs) if configs else {}
        return self._live_config

    def build_bar_edge_runtime(self, catalog_bytes: bytes, acquisition_bytes: bytes) -> None:
        """The bar edge's own runtime pair: its records keep the raw lineage the cores hold today.

        Query and the bar edge used to read different catalogues (Query the mounted r13, the bar edge its image
        r11/r19). Query serves catalogue lineage (``kn_query_backend`` cursor/contract claims), the bar edge stamps
        it onto raw BAR envelopes, and the cores quarantine a raw envelope whose ``instrument_catalog_revision``
        differs from their row (``qdl-realtime-core`` lib.rs). One shared file cannot keep both, so the bar edge
        gets ``/runtime/bar-edge-{source,acquisition}-bindings.yaml``: every retained bar-edge-owned binding
        (enabled BINANCE/OKX BAR) is pinned at catalogue lineage = its live core rows' ``instrument_catalog_revision``
        and at config lineage = the declared bar-edge stamp (``bar_edge.config_lineage``, today 19); new rows keep
        the new revisions. Every other row equals the Query runtime file.
        """
        config = self.declaration["bar_edge"]
        config_lineage = int(config["config_lineage"])
        cores = [json.loads(Path(path).read_text(encoding="utf-8")) for path in self.base.runtime.get("cores") or []]
        if not cores:
            raise DeclarationRefused("bar_edge.split_runtime needs the live core configs (base.runtime.cores)")
        raw_by_source: dict[str, set[int]] = defaultdict(set)
        for core in cores:
            for row in core["core"]["bindings"]:
                raw_by_source[str(row["source_id"])].add(int(row["instrument_catalog_revision"]))
        catalog = yaml.safe_load(catalog_bytes) if not catalog_bytes.lstrip().startswith(b"{") else json.loads(
            catalog_bytes)
        plan = yaml.safe_load(acquisition_bytes)
        base_ids = {row["binding_id"] for row in self.base.docs["catalog"]["bindings"]}
        source_ids = {item.binding_id: item.source_id
                      for item in _with_tempfile(catalog_bytes, "catalog.yaml", StableSourceCatalog.load).bindings}
        acq_by_id = {row["binding_id"]: row for row in plan["bindings"]}
        owned_retained = []
        for row in catalog["bindings"]:
            acquisition = acq_by_id[row["binding_id"]]
            owned = (acquisition.get("enabled", True) and acquisition["runtime"] in {"BINANCE", "OKX"}
                     and row["feed"] == "BAR")
            if not owned or row["binding_id"] not in base_ids:
                continue
            revisions = raw_by_source.get(str(source_ids[row["binding_id"]]))
            if not revisions or len(revisions) != 1:
                raise DeclarationRefused(f"bar-edge binding {row['binding_id']} has no single live core raw "
                                         f"lineage: {sorted(revisions or [])}")
            row["lineage_revision"] = next(iter(revisions))
            acquisition["lineage_config_revision"] = config_lineage
            owned_retained.append(row["binding_id"])
        self.files["runtime/bar-edge-source-bindings.yaml"] = _catalog_bytes(catalog, like=catalog_bytes)
        self.files["runtime/bar-edge-acquisition-bindings.yaml"] = yaml.safe_dump(
            plan, allow_unicode=False).encode("utf-8")
        raw_counts = Counter(row["lineage_revision"] for row in catalog["bindings"]
                             if row["binding_id"] in set(owned_retained))
        self.summary["bar_edge_runtime"] = {
            "files": ["runtime/bar-edge-source-bindings.yaml", "runtime/bar-edge-acquisition-bindings.yaml"],
            "retained_owned_bindings": len(owned_retained),
            "catalog_lineage_from_live_cores": dict(sorted(raw_counts.items())),
            "config_lineage": config_lineage,
        }
        migration = config.get("migration")
        if migration:
            previous = (Path(migration["previous_catalog"]).read_bytes(),
                        Path(migration["previous_acquisition"]).read_bytes())
            expected = migration.get("expect_sha256")
            if expected and [_sha256_bytes(v) for v in previous] != list(expected):
                raise DeclarationRefused("bar-edge previous catalogue/acquisition sha256 differs from the declared "
                                         "image files")
            with tempfile.TemporaryDirectory(prefix="qdl-declaration-bar-edge-") as raw:
                root = Path(raw)
                (root / "catalog.yaml").write_bytes(self.files["runtime/bar-edge-source-bindings.yaml"])
                (root / "acquisition.yaml").write_bytes(self.files["runtime/bar-edge-acquisition-bindings.yaml"])
                record = checkpoint_migration(previous_catalog=previous[0], previous_acquisition=previous[1],
                                              catalog=root / "catalog.yaml", acquisition=root / "acquisition.yaml",
                                              reason=str(migration["reason"]),
                                              approved_by=str(migration["approved_by"]))
            self.files["runtime/bar-edge-checkpoint-migration.json"] = _json_sorted(record)
            self.summary["bar_edge_runtime"]["migration"] = {
                "from": record["from"], "to": record["to"], "removed": len(record["removed_binding_ids"]),
                "changed": len(record["changed_binding_ids"]),
                "previous_binding_digests": len(record["previous_binding_digests"]),
                "previous_sha256": [_sha256_bytes(v) for v in previous]}

    def build_query_acquisition(self, acq_doc, new_acq) -> bytes | None:
        path = self.base.runtime.get("query_acquisition")
        if not path:
            return None
        base_raw = Path(path).read_bytes()
        doc = yaml.safe_load(base_raw)
        have = {row["binding_id"] for row in doc["bindings"]}
        # Every repository row the deployed runtime copy lacks: this render's additions and any earlier declaration
        # not yet deployed (e.g. a rolled-back window), in repository order. Retained runtime rows keep their bytes.
        additions = [deepcopy(row) for row in acq_doc["bindings"] if row["binding_id"] not in have]
        if not additions:
            return base_raw
        # DL v2.3.0 2d: the bar edge stamps config_revision from this file, so
        # the retained rows keep the revision they were acquired at.
        live = self.live_config()
        # Re-review H2: a bar-edge-owned BAR row that no live ingestor stamps carries the bar edge's stamp (its
        # REST/repair envelopes), the same value as the bar-edge pair; an ingestor-stamped row keeps that stamp.
        bar_edge_lineage = self.declaration["bar_edge"].get("config_lineage")
        feeds = {row["binding_id"]: row["feed"] for row in self._catalog_doc["bindings"]}
        for row in doc["bindings"]:
            if "lineage_config_revision" in row:
                continue
            value = live.get(row["binding_id"])
            if value is None and bar_edge_lineage is not None and feeds.get(row["binding_id"]) == "BAR" and (
                    row.get("enabled", True) and row["runtime"] in {"BINANCE", "OKX"}):
                value = int(bar_edge_lineage)
            row["lineage_config_revision"] = int(doc["revision"] if value is None else value)
        doc["bindings"].extend(additions)
        doc["revision"] = int(acq_doc["revision"])
        # Stage A precedent (pinned Query runtime r21): sorted-key YAML.
        return yaml.safe_dump(doc, allow_unicode=False).encode("utf-8")

    def build_ingestors(self, catalog_path: Path, acquisition_path: Path) -> dict[str, Any]:
        runtime = self.base.runtime
        if not runtime.get("ingestors"):
            return {}
        authority = json.loads(Path(runtime["authority"]).read_text(encoding="utf-8"))
        validate_shared_authority_record(authority)
        catalog = StableSourceCatalog.load(catalog_path)
        acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
        generated_all = acquisition.native_ingestor_configs(catalog=catalog, authority=authority)
        pins_on = bool(self.declaration["ingestor"].get("lane_pins", True))
        flush = dict(self.declaration["ingestor"].get("latest_state_flush_ms_by_feed") or {})
        if set(flush) - FLUSH_FEEDS or any(type(v) is not int or not 1 <= v <= 1000 for v in flush.values()):
            raise DeclarationRefused("latest_state_flush_ms_by_feed keys are QUOTE/MARK_INDEX, values 1..1000")
        report = {}
        for lane, file_name in ingestor_refresh.TARGETS.items():
            path = runtime["ingestors"].get(file_name)
            if path is None:
                continue
            active_raw = Path(path).read_bytes()
            active = json.loads(active_raw)
            unpinned = dict(active, bindings=[{k: v for k, v in b.items() if k != "lane_shard"}
                                              for b in active["bindings"]])
            expected = ingestor_refresh._expected_config(lane=lane, catalog=catalog, acquisition=acquisition,
                                                         authority=authority)
            # The positional rename check is replaced by the sticky-lane plan when pins are on.
            label = f"{file_name} (sticky lanes)" if pins_on else file_name
            successor, change = ingestor_refresh._validate_transition(active=unpinned, expected=expected,
                                                                      target=label, pin_config_lineage=True)
            native_bars = self._native_bar_additions(unpinned, generated_all.get(lane) or {})
            if native_bars:
                successor["bindings"] = list(successor["bindings"]) + native_bars
                if successor.get("config_revision") == active.get("config_revision"):
                    successor["config_revision"] = int(expected["config_revision"])
            if successor.get("config_revision") != active.get("config_revision"):
                # DL v2.3.0 2d: bindings carried over from the active file keep
                # the config revision their records already carry.
                previous = int(active["config_revision"])
                carried = {ingestor_lanes.binding_key(b) for b in active["bindings"]}
                successor["bindings"] = [
                    dict(b, config_revision=b.get("config_revision", previous))
                    if ingestor_lanes.binding_key(b) in carried else b
                    for b in successor["bindings"]
                ]
            runtime_name = "OKX" if lane == "okx-swap" else "BINANCE"
            cap = int(active.get("max_subscriptions_per_connection", 100))
            lanes_before = ingestor_lanes.lane_plan_report(
                runtime_name, active["bindings"],
                ingestor_lanes.current_lane_assignment(runtime_name, active["bindings"], cap))
            changed = bool(change["added_binding_count"] or native_bars)
            if pins_on:
                pins = ingestor_lanes.sticky_lane_pins(runtime_name, active["bindings"], successor["bindings"], cap)
                pinned = ingestor_lanes.apply_lane_pins(successor, pins)
                if pinned["bindings"] != active["bindings"]:
                    successor = pinned
                    changed = True
            assignment = ingestor_lanes.current_lane_assignment(runtime_name, successor["bindings"], cap)
            lanes_after = ingestor_lanes.lane_plan_report(runtime_name, successor["bindings"], assignment)
            for name, value in lanes_before.items():
                after = lanes_after.get(name)
                if after is None or after["feed"] != value["feed"] or not set(value["members"]) <= set(after["members"]):
                    raise DeclarationRefused(f"{file_name} lane {name} is not stable across the declaration")
            if flush:
                if successor.get("latest_state_flush_ms_by_feed") != flush:
                    successor["latest_state_flush_ms_by_feed"] = dict(sorted(flush.items()))
                    changed = True
            payload = ingestor_refresh._canonical_bytes(successor) if changed else active_raw
            self.files[f"ingestor/{file_name}"] = payload
            report[file_name] = {
                "before_sha256": _sha256_bytes(active_raw), "after_sha256": _sha256_bytes(payload),
                "added_realtime_bindings": change["added_binding_count"], "added_native_bar_bindings": len(native_bars),
                "config_revision": [active.get("config_revision"), successor.get("config_revision")],
                "lane_plan": "PINNED" if pins_on else "POSITIONAL",
                "lanes": {name: {"feed": v["feed"], "members": len(v["members"])} for name, v in lanes_after.items()},
                "new_lanes": sorted(set(lanes_after) - set(lanes_before)),
                "latest_state_flush_ms_by_feed": successor.get("latest_state_flush_ms_by_feed"),
                "max_subscriptions_per_connection": cap,
            }
        return report

    @staticmethod
    def _native_bar_additions(active: Mapping[str, Any], generated: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Native BAR subscriptions the certified generator emits that the active file lacks (1m tier E)."""
        have = {ingestor_refresh._binding_key(b) for b in active["bindings"]}
        result = []
        for binding in generated.get("bindings") or []:
            if binding.get("feed") == "BAR" and ingestor_refresh._binding_key(binding) not in have:
                result.append(dict(binding))
        return result

    def build_cores(self, catalog_path: Path, acquisition_path: Path) -> dict[str, Any]:
        cores = self.base.runtime.get("cores") or []
        if not cores:
            return {}
        catalog = StableSourceCatalog.load(catalog_path)
        acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
        added_sets, files = set(), {}
        for path in cores:
            raw = Path(path).read_bytes()
            doc, added = core_refresh.successor(json.loads(raw), catalog=catalog, acquisition=acquisition,
                                                name=Path(path).name)
            added_sets.add(tuple(sorted(json.dumps(item, sort_keys=True) for item in added)))
            payload = core_refresh._canonical_bytes(doc) if added else raw
            self.files[f"cores/{Path(path).name}"] = payload
            files[Path(path).name] = {"before_sha256": _sha256_bytes(raw), "after_sha256": _sha256_bytes(payload),
                                      "added": len(added),
                                      "added_by_provider_kind": dict(sorted(Counter(i["provider_kind"] for i in added).items()))}
        if len(added_sets) != 1:
            raise DeclarationRefused("cores would receive different additions")
        return files

    def build_bundles(self, catalog_path: Path, universe_dir: Path) -> dict[str, Any]:
        bundles = self.base.runtime.get("bundles") or {}
        if not bundles:
            return {}
        environment = str(self.declaration["environment"])
        manifest_paths = {}
        tmp = tempfile.TemporaryDirectory(prefix="qdl-declaration-bundle-")
        try:
            for cid, item in self.manifests.items():
                if self.manifest_changed[cid]:
                    path = Path(tmp.name) / f"{cid}.yaml"
                    path.write_bytes(_yaml_bytes(item["doc"]))
                    manifest_paths[cid] = path
            compiled = kn_gateway_bundle.compile_bundle(environment=environment, catalog_path=catalog_path,
                                                        manifest_paths=list(manifest_paths.values()),
                                                        universe_dir=universe_dir)
        finally:
            tmp.cleanup()
        entries = {entry["consumer_id"]: entry for entry in compiled["manifests"]}
        result = {}
        for name, path in sorted(bundles.items()):
            raw = Path(path).read_bytes()
            document = json.loads(raw)
            before = document["sha256"]
            body = {k: v for k, v in document.items() if k != "sha256"}
            if kn_gateway_bundle.bundle_sha256(body) != before:
                raise DeclarationRefused(f"base {name} bundle hash does not verify")
            body["catalog"] = compiled["catalog"]
            replaced = []
            for index, entry in enumerate(body["manifests"]):
                new = entries.get(entry["consumer_id"])
                if new is None:
                    continue
                # The projector (kn) uses manifests only to size BAR retention caps.
                if name == "kn" and self._bar_rows(entry) == self._bar_rows(new):
                    continue
                body["manifests"][index] = new
                replaced.append(entry["consumer_id"])
            body["sha256"] = kn_gateway_bundle.bundle_sha256(body)
            payload = (json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            if body["sha256"] == before:
                payload = raw
            self.files[f"bundles/{name}-bundle.json"] = payload
            result[name] = {"base_sha256": before, "sha256": body["sha256"], "replaced_manifests": replaced,
                            "catalog_bindings": len(body["catalog"]["bindings"])}
        return result

    @staticmethod
    def _bar_rows(entry: Mapping[str, Any]) -> list:
        return sorted(json.dumps(r, sort_keys=True) for r in entry["requirements"] if r["feed"] == "BAR")

    def build_execution_binding(self, catalog_doc) -> dict[str, Any]:
        result = {}
        for cid, item in self.manifests.items():
            if item["class"] != "EXECUTION":
                continue
            base_binding_path = self.base.runtime.get("route_bindings", {}).get(cid)
            if base_binding_path is None:
                self.notes.append(f"{cid}: no base route binding given; sealed binding and execution scope "
                                  "are not rendered")
                continue
            base_binding = json.loads(Path(base_binding_path).read_text(encoding="utf-8"))
            if not self.manifest_changed[cid]:
                binding = base_binding
                payload = Path(base_binding_path).read_bytes()
            else:
                current, consumer, base, requirements, products = paper_extension._validated_base(
                    catalog_doc, item["base"], base_binding)
                target = item["doc"]
                new_binding = deepcopy(base_binding)
                template = paper_extension._okx_swap(catalog_doc, "ETH-USDT-SWAP")
                symbols = []
                for symbol, feed in self._execution_added:
                    row = paper_extension._okx_swap(catalog_doc, symbol)
                    _req, route = paper_extension._execution_template(requirements, products, template, feed)
                    route = deepcopy(route)
                    route.update(instrument_uid=row["instrument_uid"], instrument_id=row["instrument_id"],
                                 native_symbol=row["native_symbol"])
                    new_binding["products"].append(route)
                    if symbol not in symbols:
                        symbols.append(symbol)
                quota_change = None
                base_quotas, new_quotas = item["base"]["spec"]["quotas"], target["spec"]["quotas"]
                if base_quotas != new_quotas:
                    quota_change = {k: {"from": base_quotas[k], "to": new_quotas[k]}
                                    for k in sorted(new_quotas) if new_quotas[k] != base_quotas[k]}
                release_revision = next((c.get("release_revision") for c in self.declaration["consumers"]
                                         if c["consumer_id"] == cid), None)
                if release_revision is not None and int(release_revision) <= int(base_binding["release_revision"]):
                    raise DeclarationRefused(f"{cid} release_revision must be above the base binding")
                sealed = paper_extension._seal(
                    current=current, consumer=consumer, base=base, products=products, catalog=catalog_doc,
                    target=target, new_binding=new_binding, quota_change=quota_change,
                    added_products=len(self._execution_added), linear_okx_swaps=sorted(symbols),
                    feeds=sorted({feed for _s, feed in self._execution_added}),
                    quota_model=self.quota_models[cid]["evidence"],
                    release_revision=None if release_revision is None else int(release_revision))
                binding = sealed["binding"]
                payload = json.dumps(binding, indent=2, sort_keys=True).encode("utf-8")
                self.env[cid] = dict(sealed["consumer_environment"])
            revision = int(binding["consumer_manifest_revision"])
            self.files[f"bindings/{cid}-r{binding['release_revision']}.json"] = payload
            scope = self._execution_scope(cid, binding, catalog_doc, payload, revision)
            result[cid] = {"release_revision": binding["release_revision"], "binding_sha256": binding["binding_sha256"],
                           "file_sha256": _sha256_bytes(payload), "products": len(binding["products"]),
                           "execution_scope_instruments": len(scope["instruments"])}
        return result

    def _execution_scope(self, cid, binding, catalog_doc, payload: bytes, manifest_revision: int) -> dict[str, Any]:
        instruments = {row["instrument_uid"]: row for row in catalog_doc["instruments"]}
        feeds: dict[tuple, set[str]] = defaultdict(set)
        for product in binding["products"]:
            if not product.get("execution_grade") or product["feed"] not in SCOPE_FEEDS:
                continue
            record = instruments[product["instrument_uid"]]
            if record["market"] not in SCOPE_MARKETS.get(record["venue"], ()):
                self.notes.append(f"{cid}: {record['native_symbol']} {record['market']} is outside the TS writer "
                                  "declared-scope markets")
                continue
            feeds[(record["venue"], record["market"], record["native_symbol"], record["instrument_uid"])].add(
                product["feed"])
        rows = [{"venue": v, "market": m, "native_symbol": s, "instrument_uid": uid, "feeds": sorted(f)}
                for (v, m, s, uid), f in sorted(feeds.items())]
        if not 1 <= len(rows) <= 2000:
            raise DeclarationRefused(f"{cid} execution scope must hold 1..2000 instruments")
        document = {
            "schema": EXECUTION_SCOPE_SCHEMA,
            "consumer_id": cid,
            "revision": int(self.declaration["declaration_revision"]),
            "manifest_revision": manifest_revision,
            "catalog_revision": int(catalog_doc["catalog_revision"]),
            "binding_digest": _sha256_bytes(payload),
            "instruments": rows,
        }
        self.files[f"execution-scope/{cid}.json"] = _json_sorted(document)
        return document

    # ------------------------------------------------------------------ capacity
    def capacity(self, new_acq: list[dict[str, Any]], ingestors: Mapping[str, Any]) -> dict[str, Any]:
        config = self.declaration["capacity"]
        flush = (self.declaration["ingestor"].get("latest_state_flush_ms_by_feed") or {}).get("MARK_INDEX", 50)
        records = 0.0
        unmeasured: Counter = Counter()
        per_kind: Counter = Counter()
        intervals = {row["binding_id"]: row.get("interval") for row in self._catalog_doc["bindings"]}
        rates = {**RATE_PER_BINDING, **{str(k): float(v) for k, v in (config.get("rates_per_binding") or {}).items()}}
        rated: list[tuple[str, str, float | None]] = []
        for row in new_acq:
            kind, mode = row["provider_kind"], row["mode"]
            if mode == "DERIVED_FINAL":
                rate = 1000.0 / canonical_interval_ms(intervals[row["binding_id"]])
            elif mode != "RUST_NATIVE":
                rate = 0.0
            elif kind == "okx_mark_index":
                rate = rates["okx_mark_index@500" if flush >= 500 else "okx_mark_index@50"]
            elif kind in rates:
                rate = rates[kind]
            else:
                unmeasured[kind] += 1
                rated.append((row["binding_id"], row["runtime"], None))
                continue
            records += rate
            per_kind[f"{kind}:{mode}"] += 1
            rated.append((row["binding_id"], row["runtime"], rate))
        baseline = config.get("baseline_dl_vcpu")
        ceiling = config.get("ceiling_dl_vcpu")
        added_vcpu = round(records * VCPU_PER_RECORD, 3)
        projected = None if baseline is None else round(float(baseline) + added_vcpu, 3)
        status = "UNKNOWN"
        if unmeasured:
            status = "UNMEASURED_FEED_RATE"
        elif projected is not None and ceiling is not None:
            status = "WITHIN_ENVELOPE" if projected <= float(ceiling) else "EXCEEDS_ENVELOPE"
        subs = {name: sum(lane["members"] for lane in value["lanes"].values()) for name, value in ingestors.items()}
        priority = [str(v).upper() for v in config.get("venue_priority") or ("OKX", "BINANCE")]
        deferral = venue_priority_plan(rated, baseline=baseline, ceiling=ceiling, priority=priority)
        if status == "EXCEEDS_ENVELOPE" and deferral["status"] == "HIGHER_PRIORITY_TRIM_REQUIRED":
            status = "EXCEEDS_ENVELOPE_HIGHER_PRIORITY_TRIM_REQUIRED"
        return {
            "status": status,
            "venue_priority": priority,
            "deferral_plan": deferral,
            "bootstrap": self.bootstrap_estimate(new_acq),
            "basis": "estimate: mean per-binding rates of the 150 E-crypto set measured 2026-11-13 "
                     "(COVERAGE_150_STUDY 5.1/3.2; liquid subsets run above the mean, override with "
                     "capacity.rates_per_binding), 1.8 ms/record upper bound; not a measurement",
            "rates_per_binding": dict(sorted(rates.items())),
            "added_records_per_s": round(records, 1),
            "added_dl_vcpu_upper_bound": added_vcpu,
            "baseline_dl_vcpu": baseline,
            "projected_dl_vcpu": projected,
            "ceiling_dl_vcpu": ceiling,
            "mark_index_flush_ms": flush,
            "added_bindings_by_kind": dict(sorted(per_kind.items())),
            "unmeasured_bindings_by_kind": dict(sorted(unmeasured.items())),
            "ingestor_subscriptions_after": subs,
            "query_requests": self._query_requests(),
        }

    def bootstrap_estimate(self, new_acq: list[dict[str, Any]]) -> dict[str, Any]:
        kn = self.files.get("bundles/kn-bundle.json")
        if kn is None:
            return self._bootstrap_from_templates(new_acq)
        from qdl.runtime.bar_history_demand import demanded_history_rows

        demand = demanded_history_rows(json.loads(kn))
        intervals = {row["binding_id"]: row.get("interval") for row in self._catalog_doc["bindings"]}
        policy = json.loads(self.base.path(CONFIG / "provider-admission-policy-kn-bar-edge-v1.json").read_text())
        lane = next(item["policy"] for item in policy["lanes"] if item["lane"] == {
            "provider": "OKX", "market": "SWAP", "endpoint_family": "HISTORY_CANDLES"})
        rate_per_min = lane["refill_tokens"] / (lane["refill_interval_ns"] / 1e9) * 60
        lookback_ms = 1_095 * 86_400_000  # stable_bar_edge _BOOTSTRAP_HISTORY_LOOKBACK_DAYS
        by_interval: dict[str, dict[str, int]] = {}
        rows = requests = bindings = 0
        for row in new_acq:
            interval = intervals.get(row["binding_id"])
            if row["runtime"] != "OKX" or interval is None:
                continue
            depth = min(demand.get(row["binding_id"], 0), max(1, lookback_ms // canonical_interval_ms(interval)))
            entry = by_interval.setdefault(interval, {"bindings": 0, "rows_per_binding": depth, "requests": 0})
            entry["bindings"] += 1
            entry["requests"] += -(-depth // 100)
            bindings += 1
            rows += depth
            requests += -(-depth // 100)
        return {"bindings": bindings, "rows": rows, "requests": requests,
                "kn_cache_rows_added": rows, "kn_cache_row_cap_added": rows + 2_064 * bindings,
                "lane": "OKX/SWAP/HISTORY_CANDLES (separate token bucket from the live CANDLES lane)",
                "lane_requests_per_minute": rate_per_min,
                "duration_minutes": round(requests / rate_per_min, 1) if rate_per_min else None,
                "by_interval": dict(sorted(by_interval.items(), key=lambda kv: canonical_interval_ms(kv[0]))),
                "basis": "depth = qdl.runtime.bar_history_demand over the rendered kn bundle (largest granting "
                         "manifest max_warmup_rows; 0 when ungranted), capped by the bar edge's 1,095-day lookback; "
                         "100 rows per history request (OKX page, not re-verified); one pass, no retries"}

    def _bootstrap_from_templates(self, new_acq: list[dict[str, Any]]) -> dict[str, Any]:
        """One-off bar-edge history bootstrap of the new BAR bindings: depth = the largest declared warmup that
        grants the binding (selector templates of the rendered manifests; explicit rows of unchanged manifests are
        not re-read), read from the venue history through the OKX/SWAP/HISTORY_CANDLES admission lane.
        Rows per request: 100 (OKX history-candles page; not re-verified here)."""
        depth_by_interval: dict[str, int] = {}
        for item in self.manifests.values():
            for selector in item["doc"]["spec"].get("selectors") or []:
                template = selector["template"]
                if template.get("feed") != "BAR":
                    continue
                for interval in selector.get("intervals") or [template.get("interval")]:
                    depth_by_interval[interval] = max(depth_by_interval.get(interval, 0),
                                                      int(template.get("warmup_limit") or 0))
        intervals = {row["binding_id"]: row.get("interval") for row in self._catalog_doc["bindings"]}
        policy = json.loads(self.base.path(CONFIG / "provider-admission-policy-kn-bar-edge-v1.json").read_text())
        lane = next(item["policy"] for item in policy["lanes"] if item["lane"] == {
            "provider": "OKX", "market": "SWAP", "endpoint_family": "HISTORY_CANDLES"})
        rate_per_min = lane["refill_tokens"] / (lane["refill_interval_ns"] / 1e9) * 60
        rows = requests = bindings = 0
        for row in new_acq:
            interval = intervals.get(row["binding_id"])
            if row["runtime"] != "OKX" or interval is None:
                continue
            depth = depth_by_interval.get(interval, 0)
            bindings += 1
            rows += depth
            requests += -(-depth // 100)
        return {"bindings": bindings, "rows": rows, "requests": requests,
                "lane": "OKX/SWAP/HISTORY_CANDLES", "lane_requests_per_minute": rate_per_min,
                "duration_minutes": round(requests / rate_per_min, 1) if rate_per_min else None,
                "depth_by_interval": dict(sorted(depth_by_interval.items(), key=lambda kv: canonical_interval_ms(kv[0]))),
                "basis": "estimate: 100 rows per history request (OKX page size, not re-verified); one pass, no retries"}

    def okx_rest_budget(self, catalog_bytes: bytes, acquisition_bytes: bytes) -> dict[str, Any]:
        """OKX public REST used by the bar edge's REST final lane, and the final-bar visibility it implies.

        Every enabled OKX SWAP PYTHON_REST BAR binding is read once per boundary at boundary + settlement delay
        (``QDL_STABLE_BAR_SETTLEMENT_DELAY_SECONDS``, compose 0.10 s), through the bar-edge provider admission
        lane OKX/SWAP/CANDLES (``config/v2/provider-admission-policy-kn-bar-edge-v1.json``: a token bucket). All
        reads of one boundary queue on that bucket, so the last one waits (N - burst) / refill rate. Not
        modelled: OKX's own confirm lag, not-yet-final retries (0.1 s doubling to 1 s), and the
        core/projector pipeline after publication; R-cert 2 measures those.
        """
        config = self.declaration["capacity"]
        catalog = _with_tempfile(catalog_bytes, "catalog.yaml", StableSourceCatalog.load)
        plan = _with_tempfile(acquisition_bytes, "acquisition.yaml",
                              lambda path: StableAcquisitionPlan.load(path, catalog=catalog))
        sources = {item.binding_id: item for item in catalog.bindings}
        by_interval: Counter = Counter()
        for item in plan.bindings:
            source = sources[item.binding_id]
            identity = source.instrument.identity
            if (item.enabled and item.mode == "PYTHON_REST" and item.runtime == "OKX"
                    and (identity.venue, identity.market) == ("OKX", "SWAP") and source.feed.value == "BAR"):
                by_interval[source.interval] += 1
        policy_path = self.base.path(CONFIG / "provider-admission-policy-kn-bar-edge-v1.json")
        lane = next(item["policy"] for item in json.loads(policy_path.read_text(encoding="utf-8"))["lanes"]
                    if item["lane"] == {"provider": "OKX", "market": "SWAP", "endpoint_family": "CANDLES"})
        rate_per_s = lane["refill_tokens"] / (lane["refill_interval_ns"] / 1e9)
        burst = int(lane["token_capacity"])
        delay_ms = int(float(config.get("bar_settlement_delay_seconds", 0.10)) * 1000)
        reads = float(config.get("rest_reads_per_boundary", 1.0))
        avg_rpm = sum(n * 60_000 / canonical_interval_ms(interval) for interval, n in by_interval.items()) * reads
        other = float(config.get("okx_rest_other_requests_per_minute", 0))
        ip_limit = float(config.get("okx_rest_ip_limit_per_minute", 1200))
        max_util = float(config.get("okx_rest_max_utilization", 0.8))

        def drain_ms(count: int) -> int:
            return delay_ms + int(max(0, count * reads - burst) / rate_per_s * 1000)

        minute = by_interval.get("1m", 0)
        worst = sum(by_interval.values())
        lane_util = avg_rpm / (rate_per_s * 60)
        ip_util = (avg_rpm + other) / ip_limit
        limit_ms = config.get("rest_final_max_visibility_ms")
        problems = []
        if lane_util > max_util:
            problems.append(f"bar-edge OKX CANDLES lane utilisation {lane_util:.2f} > {max_util}")
        if ip_util > max_util:
            problems.append(f"host-IP OKX REST utilisation {ip_util:.2f} > {max_util}")
        if minute and drain_ms(minute) >= 60_000:
            problems.append("one minute's 1m polls do not drain within the minute (backlog)")
        if limit_ms is not None and minute and drain_ms(minute) > int(limit_ms):
            problems.append(f"last 1m final read at +{drain_ms(minute)} ms > declared {int(limit_ms)} ms")
        return {
            "status": "WITHIN_ENVELOPE" if not problems else "EXCEEDS_ENVELOPE",
            "problems": problems,
            "rest_final_bindings_by_interval": dict(sorted(by_interval.items(),
                                                           key=lambda kv: canonical_interval_ms(kv[0]))),
            "bar_edge_requests_per_minute_avg": round(avg_rpm, 1),
            "other_requests_per_minute_declared": other,
            "admission_lane": {"family": "OKX/SWAP/CANDLES", "refill_per_s": rate_per_s, "burst": burst,
                               "max_inflight": lane["max_inflight"], "requests_per_minute": rate_per_s * 60},
            "lane_utilisation": round(lane_util, 3),
            "host_ip_limit_per_minute_declared": ip_limit,
            "host_ip_utilisation": round(ip_util, 3),
            "max_utilisation": max_util,
            "final_1m_visibility_ms": None if not minute else {
                "first_read_after_boundary": delay_ms,
                "last_read_after_boundary": drain_ms(minute),
                "worst_boundary_reads": worst,
                "worst_boundary_last_read_after_boundary": drain_ms(worst),
                "excludes": "OKX confirm lag, not-yet-final retries, core/projector pipeline (R-cert 2 measures)",
            },
            "declared_max_visibility_ms": limit_ms,
            "basis": "model: one read per REST final binding per boundary through the bar-edge admission token "
                     "bucket; host-IP limit as declared (OKX docs give 40 requests/2 s per IP for market "
                     "candles; not re-verified here)",
        }

    def _query_requests(self) -> dict[str, Any]:
        """Modelled Query request load (shared per-consumer calendar-minute quota counter in stable_redis)."""
        consumers = {}
        for cid, item in self.manifests.items():
            if item["class"] != "EXECUTION":
                consumers[cid] = {"status": "MODEL_PENDING"}
                continue
            before = quota_model.model_manifest(item["base"])
            after = quota_model.model_manifest(item["doc"])
            consumers[cid] = {
                "status": "MODELLED",
                "steady_requests_per_minute": [before.steady_requests_per_minute, after.steady_requests_per_minute],
                "streams": [before.streams, after.streams],
                "required_requests_per_minute": after.required_requests_per_minute,
                "required_max_streams": after.required_max_streams,
                "granted": {k: item["doc"]["spec"]["quotas"][k] for k in ("requests_per_minute", "max_streams")},
            }
        modelled = [v for v in consumers.values() if v["status"] == "MODELLED"]
        return {
            "basis": "qdl.consumer.quota_model (TS writer client, calibrated on r11: model 2,212/min vs measured "
                     "1,940-2,034/min); Query/Redis vCPU per request is not measured, so no vCPU is derived",
            "execution_steady_requests_per_minute_after": sum(v["steady_requests_per_minute"][1] for v in modelled),
            "execution_steady_requests_per_minute_added": sum(
                v["steady_requests_per_minute"][1] - v["steady_requests_per_minute"][0] for v in modelled),
            "consumers": consumers,
        }

    # ------------------------------------------------------------------- driver
    def run(self) -> dict[str, Any]:
        capture_paths = self._captures()
        capture_index = self._capture_index(capture_paths)
        execution, signal = self.intake()
        demand, demand_added = self.build_demand(execution, signal, capture_index)
        # The builder must see the appended capture rows.
        if f"repo/{CAPTURES['okx_swap']}" in self.files:
            tmp_dir = tempfile.TemporaryDirectory(prefix="qdl-declaration-captures-")
            okx_path = Path(tmp_dir.name) / "okx-instruments-swap.filtered.json"
            okx_path.write_bytes(self.files[f"repo/{CAPTURES['okx_swap']}"])
            capture_paths = dict(capture_paths, okx_swap=okx_path)
        catalog_doc, acq_doc, new_ids, new_acq = self.build_catalog(demand, capture_paths)
        self._catalog_doc = catalog_doc
        scope = self.build_scope(new_ids)
        demand_bytes = self.base.raw["demand"] if not demand_added else _yaml_bytes(demand)
        catalog_bytes = (self.base.raw["catalog"] if catalog_doc == self.base.docs["catalog"]
                         else _catalog_bytes(catalog_doc, like=self.base.raw["catalog"]))
        acq_bytes = self.base.raw["acquisition"] if acq_doc == self.base.docs["acquisition"] else _yaml_bytes(acq_doc)
        scope_bytes = self.base.raw["scope"] if scope == self.base.docs["scope"] else _yaml_bytes(scope)
        self.build_manifests(catalog_doc)
        routing = self.build_routing(demand_bytes, catalog_bytes, demand, catalog_doc)
        routing_bytes = self.base.raw["routing"] if routing == self.base.docs["routing"] else _yaml_bytes(routing)
        for name, payload in (("demand", demand_bytes), ("catalog", catalog_bytes), ("acquisition", acq_bytes),
                              ("scope", scope_bytes), ("routing", routing_bytes)):
            self.files[f"repo/{REPO_FILES[name]}"] = payload
        for rel in REPO_COPIED:
            self.files[f"repo/{rel}"] = self.base.path(rel).read_bytes()
        for rel in sorted(CAPTURES.values()) + [CAPTURE_PROVENANCE]:
            self.files.setdefault(f"repo/{rel}", self.base.path(rel).read_bytes())
        base_universes = self.base.path(CONFIG / "universes")
        if base_universes.is_dir():
            for path in sorted(base_universes.iterdir()):
                if path.is_file():
                    self.files.setdefault(f"repo/{CONFIG}/universes/{path.name}", path.read_bytes())
        for path in sorted(self.base.path(Path("consumers/stable")).glob("*.yaml")):
            self.files[f"repo/consumers/stable/{path.name}"] = path.read_bytes()
        for cid, item in self.manifests.items():
            if self.manifest_changed[cid]:
                payload = _yaml_bytes(item["doc"])
                self.files[f"repo/{item['rel']}"] = payload
                self.files[f"consumer-realms/{cid}-r{item['doc']['metadata']['revision']}.yaml"] = payload
        # runtime: Query/bar_edge catalogue + acquisition, then the role configs built from them
        self.files["runtime/stable-source-bindings.yaml"] = catalog_bytes
        query_acq = self.build_query_acquisition(acq_doc, new_acq)
        if query_acq is not None:
            self.files["runtime/stable-acquisition-bindings.yaml"] = query_acq
        if self.declaration["bar_edge"].get("split_runtime"):
            self.build_bar_edge_runtime(catalog_bytes, query_acq if query_acq is not None else acq_bytes)
        with tempfile.TemporaryDirectory(prefix="qdl-declaration-runtime-") as raw:
            root = Path(raw)
            catalog_path = root / "stable-source-bindings.yaml"
            catalog_path.write_bytes(catalog_bytes)
            repo_acq_path = root / "stable-acquisition-bindings.yaml"
            repo_acq_path.write_bytes(acq_bytes)
            catalog_loaded = StableSourceCatalog.load(catalog_path)
            StableAcquisitionPlan.load(repo_acq_path, catalog=catalog_loaded)
            if query_acq is not None:
                query_acq_path = root / "query-acquisition.yaml"
                query_acq_path.write_bytes(query_acq)
                StableAcquisitionPlan.load(query_acq_path, catalog=catalog_loaded)
            _with_tempfile(scope_bytes, "scope.yaml", lambda p: AuthorityPromotionScope.load(p, catalog=catalog_loaded))
            # Ingestors and cores are generated from the repository plan (the stage A refresh tools' input);
            # the Query/bar-edge runtime copy keeps its deployed transport fields.
            ingestors = self.build_ingestors(catalog_path, repo_acq_path)
            cores = self.build_cores(catalog_path, repo_acq_path)
            bundles = self.build_bundles(catalog_path, self._universe_dir_for_load())
        bindings = self.build_execution_binding(catalog_doc)
        # Capacity is measured against what runs: with a deployed runtime acquisition, every row it lacks counts
        # (a repository that leads the deployment, e.g. after a rolled-back window, must not hide its rows).
        deployed_delta = new_acq
        if self.base.runtime.get("query_acquisition"):
            deployed = {row["binding_id"] for row in
                        yaml.safe_load(Path(self.base.runtime["query_acquisition"]).read_bytes())["bindings"]}
            deployed_delta = [row for row in acq_doc["bindings"] if row["binding_id"] not in deployed]
        capacity = self.capacity(deployed_delta, ingestors)
        capacity["okx_rest"] = self.okx_rest_budget(
            catalog_bytes, self.files.get("runtime/stable-acquisition-bindings.yaml", acq_bytes))
        capacity["basis_rows"] = ("rows absent from the deployed runtime acquisition"
                                  if deployed_delta is not new_acq else "rows added by this render")
        self.summary.update(
            demand={"revision": [int(self.base.docs["demand"]["revision"]), int(demand["revision"])],
                    "added_rows": len(demand_added)},
            promotion_scope={"revision": [int(self.base.docs["scope"]["revision"]), int(scope["revision"])]},
            routing={"revision": [int(self.base.docs["routing"]["revision"]), int(routing["revision"])]},
            manifests={cid: {"revision": [int(i["base"]["metadata"]["revision"]), int(i["doc"]["metadata"]["revision"])],
                             "min_token_revision": i["doc"]["metadata"].get("min_token_revision"),
                             "changed": self.manifest_changed[cid],
                             "requirements": None if "parsed" not in i else len(i["parsed"].requirements),
                             "additive_problems": i.get("additive_problems", [])}
                       for cid, i in self.manifests.items()},
            universes={uid: {"revision": d["revision"], "members": len(d["members"]),
                             "membership_sha256": d["membership_sha256"]} for uid, d in self.universes.items()},
            derived_bindings=len(self.derived),
            ingestors=ingestors, cores=cores, bundles=bundles, bindings=bindings,
        )
        index = self._index()
        self.files["runtime/declaration-index.json"] = _json_sorted(index)
        report = {
            "schema": REPORT_SCHEMA,
            "declaration_revision": int(self.declaration["declaration_revision"]),
            "status": "RENDERED_NOT_APPLIED",
            "loader_capabilities": {"derived_final": _derived_final_supported(),
                                    "routing_selector_context": _routing_selector_supported()},
            "summary": self.summary,
            "blocked": sorted(self.blocked, key=lambda r: json.dumps(r, sort_keys=True)),
            "blocked_by_reason": dict(sorted(Counter(r["reason"] for r in self.blocked).items())),
            "capacity": capacity,
            "quota_models": dict(sorted(self.quota_models.items())),
            "consumer_environment": self.env,
            "ordered_restart": list(RESTART_ORDER),
            "notes": sorted(set(self.notes)),
            "declaration_index_sha256": _sha256_bytes(self.files["runtime/declaration-index.json"]),
            "runtime_mutations": 0,
        }
        self.files["report.json"] = _json_sorted(report)
        self.files["render-inputs.json"] = _json_sorted({
            "schema": INPUTS_SCHEMA,
            "declaration_revision": int(self.declaration["declaration_revision"]),
            "base_repo": str(self.base.repo),
            "base_runtime": {k: (str(v) if isinstance(v, Path) else
                                 [str(i) for i in v] if isinstance(v, list) else
                                 {a: str(b) for a, b in v.items()} if isinstance(v, dict) else v)
                             for k, v in self.base.runtime.items()},
            "base_sha256": self.base.input_hashes(),
            "manifests": {cid: str(i["rel"]) for cid, i in self.manifests.items()},
            "accept_set": bool(self.declaration["accept_set"]),
            "bar_edge_config_lineage": self.declaration["bar_edge"].get("config_lineage"),
            "bar_edge_live_checkpoint": (None if not self.declaration["bar_edge"].get("live_checkpoint")
                                         else str(self.declaration["bar_edge"]["live_checkpoint"])),
            "live_sources": self.declaration["base"].get("live_sources"),
        })
        return report

    def _index(self) -> dict[str, Any]:
        loaded = sorted(rel for rel in self.files if rel.split("/", 1)[0] in {
            "runtime", "ingestor", "cores", "bundles", "consumer-realms", "bindings", "execution-scope"})
        files = {rel: _sha256_bytes(self.files[rel]) for rel in loaded if rel != "runtime/declaration-index.json"}
        names = [Path(rel).name for rel in files]
        if len(names) != len(set(names)):
            raise DeclarationRefused("declaration index would list two files with one name")
        return {
            "schema": DECLARATION_INDEX_SCHEMA,
            "declaration_revision": int(self.declaration["declaration_revision"]),
            "files": files,
            "repo_files": {rel[len("repo/"):]: _sha256_bytes(self.files[rel])
                           for rel in sorted(self.files) if rel.startswith("repo/")},
        }


def venue_priority_plan(rated: Iterable[tuple[str, str, float | None]], *, baseline: float | None,
                        ceiling: float | None, priority: list[str]) -> dict[str, Any]:
    """Which added bindings to defer to fit the DL CPU cap, never touching a higher-priority venue first.

    Owner rule (2026-11-13): DL CPU cap 6.0 vCPU, OKX before BINANCE. When the estimate exceeds the cap, the plan
    defers the lowest-priority venue's additions (largest estimated rate first) until it fits; if every
    lower-priority addition is deferred and it still exceeds, it reports HIGHER_PRIORITY_TRIM_REQUIRED and defers
    nothing of the higher-priority venues (that is a separate owner decision, never automatic).
    """
    rows = list(rated)
    if baseline is None or ceiling is None:
        return {"status": "NO_ENVELOPE", "defer": []}
    added = sum(rate or 0.0 for _b, _v, rate in rows) * VCPU_PER_RECORD
    excess = float(baseline) + added - float(ceiling)
    if excess <= 0:
        return {"status": "FITS", "defer": [], "headroom_vcpu": round(-excess, 3)}
    rank = {venue: index for index, venue in enumerate(priority)}
    lowest_first = sorted(rows, key=lambda r: (-rank.get(r[1], len(priority)), -(r[2] or 0.0), r[0]))
    defer: list[str] = []
    saved = 0.0
    top = priority[0] if priority else None
    for binding_id, venue, rate in lowest_first:
        if venue == top:
            break
        defer.append(binding_id)
        saved += (rate or 0.0) * VCPU_PER_RECORD
        if saved >= excess:
            return {"status": "FITS_WITH_DEFERRAL", "defer": defer, "saved_vcpu": round(saved, 3),
                    "excess_vcpu": round(excess, 3)}
    return {"status": "HIGHER_PRIORITY_TRIM_REQUIRED" if any(v == top for _b, v, _r in rows) else "EXCEEDS",
            "defer": defer, "saved_vcpu": round(saved, 3), "excess_vcpu": round(excess, 3),
            "note": f"deferring every lower-priority addition still exceeds the cap; {top} is never trimmed "
                    "automatically"}


def render(declaration_path: Path, out: Path | None, *, dry_run: bool = False) -> dict[str, Any]:
    declaration = load_declaration(declaration_path)
    job = Render(declaration)
    try:
        report = job.run()
    finally:
        if hasattr(job, "_universe_tmp"):
            job._universe_tmp.cleanup()
    if dry_run:
        return report
    if out is None:
        raise DeclarationRefused("render needs --out (a new directory) unless --dry-run")
    if out.exists():
        raise DeclarationRefused(f"output directory exists; renders never overwrite: {out}")
    out.mkdir(parents=True, mode=0o750)
    for rel, payload in sorted(job.files.items()):
        target = out / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "xb") as handle:
            handle.write(payload)
    return report


# ------------------------------------------------------------------------- gate


def _bar_edge_set(catalog: StableSourceCatalog, acquisition: StableAcquisitionPlan) -> dict[str, str]:
    """binding id -> series digest of the bindings the bar edge owns (enabled BINANCE/OKX BAR)."""
    sources = {item.binding_id: item for item in catalog.bindings}
    return {
        item.binding_id: binding_series_digest(sources[item.binding_id], item)
        for item in acquisition.bindings
        if item.enabled and item.runtime in {"BINANCE", "OKX"} and sources[item.binding_id].feed.value == "BAR"
    }


def gate(rendered: Path) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, ok: bool, detail: Any = None, *, status: str | None = None) -> None:
        checks.append({"check": name, "status": status or ("PASS" if ok else "FAIL"), "detail": detail})

    inputs = json.loads((rendered / "render-inputs.json").read_text(encoding="utf-8"))
    report = json.loads((rendered / "report.json").read_text(encoding="utf-8"))
    base_repo = Path(inputs["base_repo"])
    # 0. the base the render was made against is still the live set; the index matches the files
    drift = []
    for rel, digest in inputs["base_sha256"].items():
        if rel.startswith("runtime:"):
            continue
        if _sha256_file(base_repo / rel) != digest:
            drift.append(rel)
    check("base_unchanged_since_render", not drift, drift)
    index = load_declaration_index(rendered / "runtime/declaration-index.json")
    mismatched = [rel for rel, digest in index["files"].items() if _sha256_file(rendered / rel) != digest]
    mismatched += [rel for rel, digest in index.get("repo_files", {}).items()
                   if _sha256_file(rendered / "repo" / rel) != digest]
    check("declaration_index_matches_files", not mismatched, mismatched)
    repo = rendered / "repo"
    # 1. certified loaders
    try:
        catalog = StableSourceCatalog.load(repo / REPO_FILES["catalog"])
        acquisition = StableAcquisitionPlan.load(repo / REPO_FILES["acquisition"], catalog=catalog)
        AuthorityPromotionScope.load(repo / REPO_FILES["scope"], catalog=catalog)
        runtime_acq_path = rendered / "runtime/stable-acquisition-bindings.yaml"
        runtime_acquisition = (StableAcquisitionPlan.load(runtime_acq_path, catalog=catalog)
                               if runtime_acq_path.is_file() else acquisition)
        check("loaders_catalog_acquisition_scope", True)
    except Exception as error:  # noqa: BLE001 - every loader failure is a gate failure
        check("loaders_catalog_acquisition_scope", False, f"{type(error).__name__}: {error}")
        return _gate_result(checks, report)
    universe_dir = rendered / "runtime/universes"
    context = SelectorContext.from_catalog(catalog, universe_dir=universe_dir if universe_dir.is_dir()
                                           else repo / CONFIG / "universes")
    base_catalog = StableSourceCatalog.load(base_repo / REPO_FILES["catalog"])
    base_context = SelectorContext.from_catalog(base_catalog, universe_dir=base_repo / CONFIG / "universes")
    manifest_problems, floor_problems = [], []
    selector_manifests = []
    for cid, rel in inputs["manifests"].items():
        try:
            new_doc = yaml.safe_load((repo / rel).read_text(encoding="utf-8"))
            new = ConsumerManifestLoader.from_mapping(new_doc, selector_context=context)
            old = ConsumerManifestLoader.from_mapping(yaml.safe_load((base_repo / rel).read_text(encoding="utf-8")),
                                                      selector_context=base_context)
        except Exception as error:  # noqa: BLE001
            manifest_problems.append(f"{cid}: {type(error).__name__}: {error}")
            continue
        if new_doc["spec"].get("selectors"):
            selector_manifests.append(cid)
        if new.manifest_sha256 != old.manifest_sha256:
            problems = additive_problems(old, new)
            if problems:
                manifest_problems.append(f"{cid}: not additive: {problems}")
            floor_problems.extend(declared_floor_problems(old, new))
    check("manifests_load_and_are_additive", not manifest_problems, manifest_problems)
    # The stage A offline quota gate over every declared manifest; non-EXECUTION ones are MODEL_PENDING.
    verdict = check_consumer_quota_model.check([repo / rel for rel in inputs["manifests"].values()])
    pending = [cid for cid, rel in inputs["manifests"].items() if not quota_model.is_execution_manifest(
        yaml.safe_load((repo / rel).read_text(encoding="utf-8")))]
    check("consumer_quota_model", verdict["status"] == "PASS",
          {"manifests": {Path(k).name: v["status"] for k, v in verdict["manifests"].items()},
           "failing": {Path(k).name: v.get("shortfall") for k, v in verdict["manifests"].items()
                       if v["status"] != "PASS"},
           "model_pending": pending})
    check("accept_set_floor_rule", not floor_problems, floor_problems)
    # release routing (with the manifests' selectors when the loader supports it)
    try:
        from qdl.consumer.release import StableReleaseRoutePlan

        if selector_manifests and not _routing_selector_supported():
            check("release_routing_loads", False, {"consumers": selector_manifests},
                  status="BLOCKED_ROUTING_LOADER_SELECTOR_UNSUPPORTED")
        else:
            kwargs = {"selector_context": context} if _routing_selector_supported() else {}
            StableReleaseRoutePlan.load(repo / REPO_FILES["routing"], manifest_root=repo, **kwargs)
            check("release_routing_loads", True)
    except Exception as error:  # noqa: BLE001
        check("release_routing_loads", False, f"{type(error).__name__}: {error}")
    # 2. additivity of catalogue, acquisition, demand, scope against the base
    base_docs = {name: (json.loads(raw) if (raw := (base_repo / rel).read_bytes()).lstrip().startswith(b"{")
                        else yaml.safe_load(raw)) for name, rel in REPO_FILES.items()}
    new_docs = {name: (json.loads(raw) if (raw := (repo / rel).read_bytes()).lstrip().startswith(b"{")
                       else yaml.safe_load(raw)) for name, rel in REPO_FILES.items()}
    problems = []
    for name, field, key in (("catalog", "bindings", "binding_id"), ("catalog", "instruments", "instrument_id"),
                             ("acquisition", "bindings", "binding_id")):
        old_rows = {row[key]: row for row in base_docs[name][field]}
        new_rows = {row[key]: row for row in new_docs[name][field]}
        removed = sorted(set(old_rows) - set(new_rows))
        # Lineage pins (DL v2.3.0 2d) are gated by retained_lineage_unchanged, not here.
        lineage_keys = {"lineage_revision", "lineage_config_revision"}

        def body(row):
            return {k: v for k, v in row.items() if k not in lineage_keys}

        changed = sorted(k for k in set(old_rows) & set(new_rows) if body(old_rows[k]) != body(new_rows[k]))
        if removed or changed:
            problems.append({"file": name, "field": field, "removed": removed[:10], "changed": changed[:10]})
    old_demand = {json.dumps(r, sort_keys=True) for c in base_docs["demand"]["consumers"] for r in c["requirements"]}
    new_demand = {json.dumps(r, sort_keys=True) for c in new_docs["demand"]["consumers"] for r in c["requirements"]}
    if old_demand - new_demand:
        problems.append({"file": "demand", "removed": len(old_demand - new_demand)})
    if set(base_docs["scope"]["binding_ids"]) - set(new_docs["scope"]["binding_ids"]):
        problems.append({"file": "scope", "removed": len(set(base_docs["scope"]["binding_ids"])
                                                       - set(new_docs["scope"]["binding_ids"]))})
    for name in ("catalog", "acquisition", "demand", "scope"):
        field = {"catalog": "catalog_revision"}.get(name, "revision")
        if int(new_docs[name][field]) < int(base_docs[name][field]):
            problems.append({"file": name, "revision_decreased": True})
    old_routes = {(c["consumer_id"], p["requirement_key"]): p for c in base_docs["routing"]["consumers"]
                  for p in c["products"]}
    new_routes = {(c["consumer_id"], p["requirement_key"]): p for c in new_docs["routing"]["consumers"]
                  for p in c["products"]}
    if any(k not in new_routes or new_routes[k] != v for k, v in old_routes.items()):
        problems.append({"file": "routing", "routes_removed_or_changed": True})
    check("config_strictly_additive", not problems, problems)
    # 3. role configs: retained rows byte-identical (lane pins aside), lanes stable
    role_problems = []
    runtime = inputs["base_runtime"]
    lineage_problems: list[str] = []
    for file_name, base_path in (runtime.get("ingestors") or {}).items():
        new_path = rendered / "ingestor" / file_name
        old, new = json.loads(Path(base_path).read_text()), json.loads(new_path.read_text())
        strip = lambda rows: {ingestor_lanes.binding_key(b): {k: v for k, v in b.items()  # noqa: E731
                                                              if k not in {"lane_shard", "config_revision"}}
                              for b in rows}
        o, n = strip(old["bindings"]), strip(new["bindings"])
        if any(k not in n or n[k] != v for k, v in o.items()):
            role_problems.append(f"{file_name}: a retained binding was removed or changed")
        # DL v2.3.0 2d: the config lineage a retained binding's envelopes carry is unchanged.
        effective = lambda doc: {ingestor_lanes.binding_key(b): int(b.get("config_revision",  # noqa: E731
                                                                          doc["config_revision"])) for b in doc["bindings"]}
        old_lineage, new_lineage = effective(old), effective(new)
        moved = sorted(k for k, v in old_lineage.items() if new_lineage.get(k, v) != v)
        lineage_problems.extend(f"{file_name}: {k} config_revision {old_lineage[k]} -> {new_lineage[k]}"
                                for k in moved[:5])
        runtime_name = "OKX" if "okx" in file_name else "BINANCE"
        cap = int(new.get("max_subscriptions_per_connection", 100))
        try:
            before = ingestor_lanes.lane_plan_report(runtime_name, old["bindings"], ingestor_lanes.current_lane_assignment(
                runtime_name, old["bindings"], cap))
            assignment = ingestor_lanes.current_lane_assignment(runtime_name, new["bindings"], cap)
            ingestor_lanes.validate_lane_plan(runtime_name, new["bindings"], assignment, cap)
            after = ingestor_lanes.lane_plan_report(runtime_name, new["bindings"], assignment)
            for lane, value in before.items():
                if lane not in after or after[lane]["feed"] != value["feed"] or not set(value["members"]) <= set(
                        after[lane]["members"]):
                    role_problems.append(f"{file_name}: lane {lane} not stable")
        except ValueError as error:
            role_problems.append(f"{file_name}: {error}")
    for base_path in runtime.get("cores") or []:
        old = json.loads(Path(base_path).read_text())
        new = json.loads((rendered / "cores" / Path(base_path).name).read_text())
        ident = {core_refresh._identity(b): b for b in new["core"]["bindings"]}
        if any(ident.get(core_refresh._identity(b)) != b for b in old["core"]["bindings"]):
            role_problems.append(f"{Path(base_path).name}: a retained core binding changed")
    check("role_configs_additive_and_lanes_stable", not role_problems, role_problems)
    # 4. bar-edge checkpoint crossing from the base runtime plan (additive => no record needed)
    try:
        basis = "repository plan"
        base_plan = StableAcquisitionPlan.load(base_repo / REPO_FILES["acquisition"], catalog=base_catalog)
        if runtime.get("query_acquisition"):
            try:
                base_plan = StableAcquisitionPlan.load(runtime["query_acquisition"], catalog=base_catalog)
                basis = "deployed runtime plan"
            except ValueError:
                # The deployed runtime copy lags the repository (e.g. a rolled-back window): it does not pair with
                # the base catalogue, so the crossing is computed from the repository plan, and says so.
                basis = "repository plan (deployed runtime acquisition does not pair with the base catalogue)"
        before = _bar_edge_set(base_catalog, base_plan)
        after = _bar_edge_set(catalog, runtime_acquisition)
        record_path = rendered / "runtime/bar-edge-checkpoint-migration.json"
        record = json.loads(record_path.read_text()) if record_path.is_file() else None
        crossing = plan_crossing(
            checkpoint={"catalog_revision": base_catalog.catalog_revision, "acquisition_revision": base_plan.revision,
                        "binding_ids": sorted(before), "binding_digests": before},
            runtime={"catalog_revision": catalog.catalog_revision, "acquisition_revision": runtime_acquisition.revision,
                     "binding_ids": sorted(after)},
            current_digests=after, migration=record)
        check("bar_edge_checkpoint_crossing", True, f"{crossing.summary()} basis={basis}")
    except Exception as error:  # noqa: BLE001 - a crossing that cannot be computed is a failure
        check("bar_edge_checkpoint_crossing", False, f"{type(error).__name__}: {error}")
    # 4b. DL v2.3.0 2d: the contract lineage of every retained-unchanged binding is unchanged
    #     (catalogue + repository plan, the Query/bar-edge runtime plan, the ingestor files above).
    try:
        lineage_record_path = rendered / "runtime/lineage-migration.json"
        lineage_allowed = (json.loads(lineage_record_path.read_text())["binding_ids"]
                           if lineage_record_path.is_file() else [])
        base_docs = (_load_mapping(base_repo / REPO_FILES["catalog"]), _load_mapping(base_repo / REPO_FILES["acquisition"]))
        new_docs = (_load_mapping(repo / REPO_FILES["catalog"]), _load_mapping(repo / REPO_FILES["acquisition"]))
        retained = retained_unchanged_ids(*base_docs, *new_docs)
        base_repo_plan = StableAcquisitionPlan.load(base_repo / REPO_FILES["acquisition"], catalog=base_catalog)
        # Review H2: where a live ingestor stamps the binding, that stamp is the "before" config lineage, not the
        # plan document's revision (gated against the live configs by live_config_lineage).
        live_stamps = live_config_lineage(base_catalog, [json.loads(Path(path).read_text())
                                                         for path in (runtime.get("ingestors") or {}).values()])

        bar_edge_stamp = inputs.get("bar_edge_config_lineage")
        base_sources = {item.binding_id: item for item in base_catalog.bindings}
        bar_edge_owned = {item.binding_id for item in base_plan.bindings
                          if item.enabled and item.runtime in {"BINANCE", "OKX"}
                          and base_sources[item.binding_id].feed.value == "BAR"}

        def _live_aware(old, new, ids, *, runtime_plan=False):
            problems, moved = [], []
            for binding in sorted(set(ids) - set(lineage_allowed)):
                before, after = binding_lineage(*old, binding), binding_lineage(*new, binding)
                if binding in live_stamps:
                    before["config_revision"] = live_stamps[binding]
                elif runtime_plan and bar_edge_stamp is not None and binding in bar_edge_owned:
                    before["config_revision"] = bar_edge_stamp  # the bar edge's own REST/repair stamp today
                if before != after:
                    moved.append(binding)
                    if len(problems) < 5:
                        problems.append(f"{binding} lineage moved "
                                        f"{ {k: (before[k], after[k]) for k in before if before[k] != after[k]} }")
            return ([f"{len(moved)} retained bindings would change their contract lineage"] if moved else []) + problems

        lineage_problems.extend(_live_aware((base_catalog, base_repo_plan), (catalog, acquisition), retained))
        runtime_retained = retained & {b.binding_id for b in base_plan.bindings} & {
            b.binding_id for b in runtime_acquisition.bindings}
        lineage_problems.extend(f"runtime plan: {item}" for item in _live_aware(
            (base_catalog, base_plan), (catalog, runtime_acquisition), runtime_retained, runtime_plan=True))
        check("retained_lineage_unchanged", not lineage_problems, lineage_problems[:10])
    except Exception as error:  # noqa: BLE001 - a lineage that cannot be computed is a failure
        check("retained_lineage_unchanged", False, f"{type(error).__name__}: {error}")
    # 4b'. review H2: the repo and runtime plans keep the config lineage the live ingestors stamp
    try:
        configs = [json.loads(Path(path).read_text()) for path in (runtime.get("ingestors") or {}).values()]
        if configs:
            live = live_config_lineage(base_catalog, configs)
            problems = [f"repo plan: {item}" for item in
                        live_config_lineage_problems(acquisition, live, catalog=catalog)]
            problems += [f"runtime plan: {item}" for item in
                         live_config_lineage_problems(runtime_acquisition, live, catalog=catalog)]
            check("live_config_lineage", not problems,
                  {"live_bindings": len(live), "values": dict(sorted(Counter(live.values()).items())),
                   "problems": problems[:10]})
    except Exception as error:  # noqa: BLE001
        check("live_config_lineage", False, f"{type(error).__name__}: {error}")
    # 4c. bar-edge runtime pair: raw catalogue lineage equals the cores' rows; the migration record crosses
    pair = (rendered / "runtime/bar-edge-source-bindings.yaml", rendered / "runtime/bar-edge-acquisition-bindings.yaml")
    if pair[0].is_file():
        problems, checked = [], 0
        try:
            edge_catalog = StableSourceCatalog.load(pair[0])
            edge_plan = StableAcquisitionPlan.load(pair[1], catalog=edge_catalog)
            core_rows: dict[str, set[int]] = defaultdict(set)
            core_files = sorted((rendered / "cores").glob("*.json"))
            for path in core_files:
                for row in json.loads(path.read_text())["core"]["bindings"]:
                    core_rows[str(row["source_id"])].add(int(row["instrument_catalog_revision"]))
            sources = {item.binding_id: item for item in edge_catalog.bindings}
            for item in edge_plan.bindings:
                source = sources[item.binding_id]
                if not (item.enabled and item.runtime in {"BINANCE", "OKX"} and source.feed.value == "BAR"):
                    continue
                checked += 1
                want = edge_catalog.lineage_revision_for(source)
                have = core_rows.get(source.source_id)
                if have != {want}:
                    problems.append(f"{item.binding_id}: bar-edge lineage {want} vs core rows {sorted(have or [])}")
            check("bar_edge_core_raw_lineage", bool(core_files) and not problems,
                  {"bar_edge_bindings": checked, "cores": len(core_files), "problems": problems[:10]})
        except Exception as error:  # noqa: BLE001
            check("bar_edge_core_raw_lineage", False, f"{type(error).__name__}: {error}")
        record_path = rendered / "runtime/bar-edge-checkpoint-migration.json"
        if record_path.is_file():
            try:
                record = json.loads(record_path.read_text())
                after = _bar_edge_set(edge_catalog, edge_plan)
                legacy = {**record["from"], "binding_ids": sorted(record["previous_binding_digests"])}
                crossing = plan_crossing(checkpoint=legacy, runtime={
                    "catalog_revision": edge_catalog.catalog_revision, "acquisition_revision": edge_plan.revision,
                    "binding_ids": sorted(after)}, current_digests=after, migration=record)
                detail = {"crossing": crossing.summary()}
                live = inputs.get("bar_edge_live_checkpoint")
                ok = crossing.migration_used
                if live:
                    checkpoint = json.loads(Path(live).read_text())
                    same = (sorted(checkpoint["binding_ids"]) == sorted(record["previous_binding_digests"])
                            and {"catalog_revision": checkpoint["catalog_revision"],
                                 "acquisition_revision": checkpoint["acquisition_revision"]} == record["from"])
                    detail["live_checkpoint"] = {"schema": checkpoint.get("schema"),
                                                 "binding_ids": len(checkpoint["binding_ids"]),
                                                 "matches_record_from": same}
                    ok = ok and same
                check("bar_edge_migration_record", ok, detail)
            except Exception as error:  # noqa: BLE001
                check("bar_edge_migration_record", False, f"{type(error).__name__}: {error}")
    # 5. coverage chain: derived parents, execution scope = binding, declared rows resolved or BLOCKED
    chain = []
    if hasattr(runtime_acquisition, "derived_parents"):
        sources = {item.binding_id: item for item in catalog.bindings}
        try:
            parents = runtime_acquisition.derived_parents(sources)
            check("derived_chain_parents", True, {"derived": len(parents)})
        except ValueError as error:
            check("derived_chain_parents", False, str(error))
    elif any(getattr(item, "mode", "") == "DERIVED_FINAL" for item in runtime_acquisition.bindings):
        chain.append("DERIVED_FINAL rows without a loader that knows them")
    for scope_path in sorted((rendered / "execution-scope").glob("*.json")) if (rendered / "execution-scope").is_dir() else ():
        scope = json.loads(scope_path.read_text())
        binding_files = sorted((rendered / "bindings").glob(f"{scope['consumer_id']}-r*.json"))
        digests = {_sha256_file(p) for p in binding_files}
        if scope["binding_digest"] not in digests:
            chain.append(f"{scope_path.name}: binding_digest is not a rendered binding file")
        keys = [(r["venue"], r["market"], r["native_symbol"]) for r in scope["instruments"]]
        if keys != sorted(set(keys)):
            chain.append(f"{scope_path.name}: instruments not strictly sorted/unique")
    check("coverage_chain", not chain, chain)
    # 6. OKX REST budget of the REST final lane, then the ingest capacity envelope (estimates from the report)
    capacity = report["capacity"]
    rest = capacity.get("okx_rest") or {"status": "WITHIN_ENVELOPE", "problems": []}
    check("okx_rest_budget", rest["status"] == "WITHIN_ENVELOPE",
          {k: rest.get(k) for k in ("problems", "bar_edge_requests_per_minute_avg", "lane_utilisation",
                                     "host_ip_utilisation", "final_1m_visibility_ms")})
    capacity = {**capacity, "statement": (
        f"estimated DL vCPU after this declaration {capacity.get('projected_dl_vcpu')} against the "
        f"{capacity.get('ceiling_dl_vcpu')} cap (baseline {capacity.get('baseline_dl_vcpu')} + added "
        f"{capacity.get('added_dl_vcpu_upper_bound')}, upper bound)")}
    check("capacity_envelope", capacity["status"] == "WITHIN_ENVELOPE", capacity,
          status=None if capacity["status"] in {"WITHIN_ENVELOPE", "EXCEEDS_ENVELOPE"} else
          f"FAIL_{capacity['status']}")
    return _gate_result(checks, report)


def _gate_result(checks, report) -> dict[str, Any]:
    failing = [c["check"] for c in checks if c["status"] != "PASS"]
    return {
        "schema": GATE_SCHEMA,
        "declaration_revision": report.get("declaration_revision"),
        "status": "PASS" if not failing else "FAIL",
        "failing": failing,
        "checks": checks,
        "blocked_rows": len(report.get("blocked") or []),
        "blocked_by_reason": report.get("blocked_by_reason"),
        "note": "offline gate; the live coverage gate (TS market_coverage_gate) and the ordered restart remain",
    }


# ---------------------------------------------------------- checkpoint migration


def _git_file(rev: str, rel: Path, repo: Path = ROOT) -> bytes:
    return subprocess.run(["git", "-C", str(repo), "show", f"{rev}:{rel}"], check=True,
                          capture_output=True).stdout


def checkpoint_migration(*, previous_catalog: bytes, previous_acquisition: bytes, catalog: Path, acquisition: Path,
                         reason: str, approved_by: str) -> dict[str, Any]:
    """The record for a bar-edge crossing; ``previous_binding_digests`` covers EXACTLY the previous set."""
    with tempfile.TemporaryDirectory(prefix="qdl-declaration-migration-") as raw:
        root = Path(raw)
        (root / "catalog.yaml").write_bytes(previous_catalog)
        (root / "acquisition.yaml").write_bytes(previous_acquisition)
        old_catalog = StableSourceCatalog.load(root / "catalog.yaml")
        old_plan = StableAcquisitionPlan.load(root / "acquisition.yaml", catalog=old_catalog)
    new_catalog = StableSourceCatalog.load(catalog)
    new_plan = StableAcquisitionPlan.load(acquisition, catalog=new_catalog)
    before, after = _bar_edge_set(old_catalog, old_plan), _bar_edge_set(new_catalog, new_plan)
    record = {
        "schema": MIGRATION_SCHEMA,
        "from": {"catalog_revision": old_catalog.catalog_revision, "acquisition_revision": old_plan.revision},
        "to": {"catalog_revision": new_catalog.catalog_revision, "acquisition_revision": new_plan.revision},
        "removed_binding_ids": sorted(set(before) - set(after)),
        "changed_binding_ids": sorted(k for k in set(before) & set(after) if before[k] != after[k]),
        "previous_binding_digests": dict(sorted(before.items())),
        "reason": reason,
        "approved_by": approved_by,
    }
    validate_migration_record(record)
    plan_crossing(checkpoint={**record["from"], "binding_ids": sorted(before)},
                  runtime={**record["to"], "binding_ids": sorted(after)}, current_digests=after, migration=record)
    return record


# --------------------------------------------------------------------------- cli


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p_render = sub.add_parser("render", help="render one declaration into a new directory")
    p_render.add_argument("declaration", type=Path)
    p_render.add_argument("--out", type=Path)
    p_render.add_argument("--dry-run", action="store_true", help="compute and print the report; write nothing")
    p_gate = sub.add_parser("gate", help="offline gate of a rendered directory")
    p_gate.add_argument("rendered", type=Path)
    p_migration = sub.add_parser("checkpoint-migration", help="render the bar-edge checkpoint migration record")
    source = p_migration.add_mutually_exclusive_group(required=True)
    source.add_argument("--git-rev", help="read the previous catalogue/acquisition from this DL revision")
    source.add_argument("--previous-catalog", type=Path)
    p_migration.add_argument("--previous-acquisition", type=Path)
    p_migration.add_argument("--expect-previous-sha256", nargs=2, metavar=("CATALOG", "ACQUISITION"))
    p_migration.add_argument("--catalog", type=Path, required=True)
    p_migration.add_argument("--acquisition", type=Path, required=True)
    p_migration.add_argument("--reason", required=True)
    p_migration.add_argument("--approved-by", required=True)
    p_migration.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "render":
            result = render(args.declaration, args.out, dry_run=args.dry_run)
        elif args.command == "gate":
            result = gate(args.rendered)
        else:
            if args.git_rev:
                previous = (_git_file(args.git_rev, REPO_FILES["catalog"]),
                            _git_file(args.git_rev, REPO_FILES["acquisition"]))
            else:
                if args.previous_acquisition is None:
                    raise DeclarationRefused("--previous-catalog needs --previous-acquisition")
                previous = (args.previous_catalog.read_bytes(), args.previous_acquisition.read_bytes())
            if args.expect_previous_sha256 and [_sha256_bytes(v) for v in previous] != list(args.expect_previous_sha256):
                raise DeclarationRefused("previous catalogue/acquisition sha256 differs from the expected image files")
            result = checkpoint_migration(previous_catalog=previous[0], previous_acquisition=previous[1],
                                          catalog=args.catalog, acquisition=args.acquisition,
                                          reason=args.reason, approved_by=args.approved_by)
            result = {**result, "previous_sha256": [_sha256_bytes(v) for v in previous]}
            with open(args.out, "xb") as handle:
                handle.write(_json_sorted({k: v for k, v in result.items() if k != "previous_sha256"}))
            result = {"out": str(args.out), "from": result["from"], "to": result["to"],
                      "removed": len(result["removed_binding_ids"]), "changed": len(result["changed_binding_ids"]),
                      "previous_bindings": len(result["previous_binding_digests"]),
                      "previous_sha256": result["previous_sha256"]}
    except (DeclarationRefused, CheckpointCrossingRefused) as error:
        print(json.dumps({"status": "REFUSED", "error": str(error)}, indent=2), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.command == "gate" and result["status"] != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    os.umask(0o027)
    raise SystemExit(main())
