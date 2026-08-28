#!/usr/bin/env python3
"""Compile dark active demand and optionally admit it against provider metadata.

This is a Phase 11.1 control-plane tool.  It never touches a running Data
Layer role, consumer route, broker, cache, alpha, or order path.  The optional
provider admission performs at most one public instrument-metadata request per
required venue/market and persists only canonical report data and payload
digests, never raw provider responses.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
from typing import Any, Callable, Iterable, Mapping

import requests

from qdl.demand.inventory import (
    ActiveDemandConvergence,
    ActiveDemandCompiler,
    ActiveDemandInventory,
    ActiveDemandSourceRegistry,
    InventoryError,
    ProviderAdmission,
    admit_provider_metadata,
    converge_active_demand,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_REGISTRY = ROOT / "config/v2/active-demand-source-registry.yaml"
_USER_AGENT = "qdl-phase111-active-demand-inventory/1.0"
_MAX_ATTEMPTS = 5


class ProviderMetadataError(RuntimeError):
    """A bounded, read-only provider metadata admission could not complete."""


def _required_markets(inventory: ActiveDemandInventory) -> tuple[tuple[str, str], ...]:
    markets = {
        (item.universe.venue, item.universe.market)
        for item in inventory.requirements
    }
    unsupported = sorted(markets - {
        ("BINANCE", "USDM"),
        ("BINANCE", "SPOT"),
        ("OKX", "SWAP"),
        ("OKX", "SPOT"),
        ("OKX", "FUTURES"),
    })
    if unsupported:
        raise ProviderMetadataError(
            "active-demand admission has no metadata adapter for "
            + ",".join(f"{venue}/{market}" for venue, market in unsupported)
        )
    return tuple(sorted(markets))


def _metadata_endpoint(venue: str, market: str) -> tuple[str, dict[str, str]]:
    if (venue, market) == ("BINANCE", "USDM"):
        return "https://fapi.binance.com/fapi/v1/exchangeInfo", {}
    if (venue, market) == ("BINANCE", "SPOT"):
        return "https://api.binance.com/api/v3/exchangeInfo", {}
    if venue == "OKX" and market in {"SWAP", "SPOT", "FUTURES"}:
        return "https://www.okx.com/api/v5/public/instruments", {"instType": market}
    raise ProviderMetadataError(f"unsupported provider metadata market: {venue}/{market}")


def _fetch_json(
    url: str,
    *,
    params: Mapping[str, str],
    timeout_seconds: float,
    attempts: int,
    get: Callable[..., requests.Response],
    sleep: Callable[[float], None],
) -> Any:
    last_error: BaseException | None = None
    for attempt in range(attempts):
        try:
            response = get(
                url,
                params=dict(params),
                timeout=timeout_seconds,
                headers={"User-Agent": _USER_AGENT},
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError, TypeError) as error:
            last_error = error
            if attempt + 1 < attempts:
                sleep(min(0.25 * (2 ** attempt), 2.0))
    raise ProviderMetadataError(
        f"provider metadata request failed after {attempts} attempts: {url}: {last_error}"
    ) from last_error


def fetch_provider_metadata(
    inventory: ActiveDemandInventory,
    *,
    timeout_seconds: float,
    attempts: int,
    get: Callable[..., requests.Response] = requests.get,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[tuple[str, str], Any]:
    """Fetch one bounded authentic metadata capture for each demanded market."""
    payloads: dict[tuple[str, str], Any] = {}
    for venue, market in _required_markets(inventory):
        url, params = _metadata_endpoint(venue, market)
        payload = _fetch_json(
            url,
            params=params,
            timeout_seconds=timeout_seconds,
            attempts=attempts,
            get=get,
            sleep=sleep,
        )
        if venue == "BINANCE":
            if not isinstance(payload, Mapping):
                raise ProviderMetadataError(f"{venue}/{market} exchangeInfo is not an object")
            payloads[(venue, market)] = payload
            continue
        if not isinstance(payload, Mapping) or str(payload.get("code")) != "0":
            raise ProviderMetadataError(f"{venue}/{market} instrument response code is not 0")
        data = payload.get("data")
        if not isinstance(data, list):
            raise ProviderMetadataError(f"{venue}/{market} instrument response data is not a list")
        payloads[(venue, market)] = data
    return payloads


def compile_inventory(
    *,
    source_registry: Path,
    repository_root: Path,
    execution_alpha_root: Path,
    trading_system_root: Path,
) -> ActiveDemandInventory:
    registry = ActiveDemandSourceRegistry.load(source_registry)
    return ActiveDemandCompiler(
        registry=registry,
        repository_root=repository_root,
        execution_alpha_root=execution_alpha_root,
        trading_system_root=trading_system_root,
    ).compile()


def run(
    *,
    source_registry: Path,
    repository_root: Path,
    execution_alpha_root: Path,
    trading_system_root: Path,
    provider_admission: bool,
    timeout_seconds: float,
    attempts: int,
    get: Callable[..., requests.Response] = requests.get,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[ActiveDemandInventory, ProviderAdmission | None]:
    inventory = compile_inventory(
        source_registry=source_registry,
        repository_root=repository_root,
        execution_alpha_root=execution_alpha_root,
        trading_system_root=trading_system_root,
    )
    if not provider_admission:
        return inventory, None
    return inventory, admit_provider_metadata(
        inventory,
        fetch_provider_metadata(
            inventory,
            timeout_seconds=timeout_seconds,
            attempts=attempts,
            get=get,
            sleep=sleep,
        ),
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _summary(
    inventory: ActiveDemandInventory,
    admission: ProviderAdmission | None,
    convergence: ActiveDemandConvergence | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "schema": "qdl.phase111.active-demand-run.v1",
        "status": "PASS" if admission is None or admission.passed else "FAIL",
        "runtime_mutations": 0,
        "provider_writes": 0,
        "manifest_sha256": inventory.manifest_sha256,
        "input_sha256": inventory.input_sha256,
        "requirement_count": len(inventory.requirements),
        "source_document_count": len(inventory.source_documents),
        "exclusion_count": len(inventory.exclusions),
        "provider_admission": admission is not None,
    }
    if admission is not None:
        result.update(
            {
                "admission_status": "PASS" if admission.passed else "FAIL",
                "admission_row_count": len(admission.rows),
                "admission_failure_count": sum(
                    item.state != "ADMITTED" for item in admission.rows
                ),
            }
        )
    if convergence is not None:
        result.update(
            {
                "convergence_status": "PASS" if convergence.passed else "FAIL",
                "selected_slice_count": convergence.selected_slice_count,
                "planned_subscription_count": len(convergence.topology.subscriptions),
                "planned_connection_count": convergence.topology.connection_count,
                "planned_service_role_count": convergence.topology.service_role_count,
            }
        )
    return result


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-registry", type=Path, default=DEFAULT_SOURCE_REGISTRY)
    parser.add_argument("--repository-root", type=Path, default=ROOT)
    parser.add_argument("--execution-alpha-root", type=Path, required=True)
    parser.add_argument("--trading-system-root", type=Path, required=True)
    parser.add_argument("--provider-admission", action="store_true")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--manifest-output", type=Path)
    parser.add_argument("--inventory-output", type=Path)
    parser.add_argument("--admission-output", type=Path)
    parser.add_argument("--convergence-output", type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not 1.0 <= args.timeout_seconds <= 30.0:
        raise SystemExit("--timeout-seconds must be between 1 and 30")
    if not 1 <= args.attempts <= _MAX_ATTEMPTS:
        raise SystemExit(f"--attempts must be between 1 and {_MAX_ATTEMPTS}")
    if args.admission_output and not args.provider_admission:
        raise SystemExit("--admission-output requires --provider-admission")
    if args.convergence_output and not args.provider_admission:
        raise SystemExit("--convergence-output requires --provider-admission")
    try:
        inventory, admission = run(
            source_registry=args.source_registry,
            repository_root=args.repository_root,
            execution_alpha_root=args.execution_alpha_root,
            trading_system_root=args.trading_system_root,
            provider_admission=args.provider_admission,
            timeout_seconds=args.timeout_seconds,
            attempts=args.attempts,
        )
        convergence = (
            converge_active_demand(
                inventory,
                admission,
                ActiveDemandSourceRegistry.load(args.source_registry).admission_policy,
            )
            if admission is not None
            else None
        )
    except (InventoryError, ProviderMetadataError, requests.RequestException, ValueError) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}, sort_keys=True))
        return 1
    if args.manifest_output:
        _write_json(args.manifest_output, inventory.manifest_payload())
    if args.inventory_output:
        _write_json(args.inventory_output, inventory.report_payload())
    if args.admission_output and admission is not None:
        _write_json(args.admission_output, admission.report_payload())
    if args.convergence_output and convergence is not None:
        _write_json(args.convergence_output, convergence.report_payload())
    print(json.dumps(_summary(inventory, admission, convergence), sort_keys=True))
    return 0 if admission is None or admission.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
