from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

import yaml

from qdl.demand import (
    AdmissionBudget,
    AdmissionPolicy,
    ActiveDemandCompiler,
    ActiveDemandInventory,
    ActiveDemandSourceRegistry,
    CapabilityRegistry,
    DataRequirement,
    DemandFeed,
    DemandLeaseRegistry,
    DemandPurpose,
    DemandState,
    DemandTopologyPlanner,
    InventoryError,
    ResolvedRequirement,
    UniverseSelector,
    UniverseSelectorKind,
    admit_provider_metadata,
    converge_active_demand,
)
from scripts.phase111_active_demand_inventory import (
    fetch_provider_metadata,
    run,
)


def _binance_symbol(
    symbol: str,
    *,
    contract_type: str = "PERPETUAL",
    base: str = "BTC",
    quote: str = "USDT",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "status": "TRADING",
        "contractType": contract_type,
        "baseAsset": base,
        "quoteAsset": quote,
        "marginAsset": quote,
        "deliveryDate": 1_800_000_000_000 if contract_type != "PERPETUAL" else 0,
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
        ],
    }


def _okx_swap(inst_id: str = "BTC-USDT-SWAP") -> dict[str, str]:
    return {
        "instType": "SWAP",
        "instId": inst_id,
        "instFamily": "BTC-USDT",
        "baseCcy": "",
        "quoteCcy": "USDT",
        "settleCcy": "USDT",
        "ctVal": "0.01",
        "ctMult": "1",
        "ctValCcy": "BTC",
        "tickSz": "0.1",
        "lotSz": "0.01",
        "state": "live",
        "expTime": "",
    }


class ActiveDemandInventoryTests(unittest.TestCase):
    def _fixture(self) -> tuple[Path, Path, Path, Path]:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        repository = root / "repository"
        alpha = root / "execution_alpha"
        trading = root / "trading_system"
        for path in (repository / "config/v2", alpha / "alphas/fixture", trading / "config/_config"):
            path.mkdir(parents=True, exist_ok=True)
        (trading / "config/_config/portfolio_account_config_setup.yaml").write_text(
            yaml.safe_dump(
                {"alphas": [{"alpha_id": "fixture_alpha", "allowed_venues": ["BINANCE"]}]},
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        (repository / "config/v2/registry.yaml").write_text(
            yaml.safe_dump(
                {
                    "schema": "qdl.v2.active-demand-source-registry.v1",
                    "revision": 7,
                    "owner_registry": {
                        "root": "trading_system",
                        "path": "config/_config/portfolio_account_config_setup.yaml",
                    },
                    "admission": {
                        "max_subscriptions_per_connection": 200,
                        "max_total_slices": 64,
                        "budgets": [
                            {
                                "venue": "BINANCE",
                                "market": "USDM",
                                "feed": "BAR",
                                "max_slices": 32,
                            },
                            {
                                "venue": "BINANCE",
                                "market": "USDM",
                                "feed": "TRADE",
                                "max_slices": 32,
                            },
                        ],
                    },
                    "sources": [
                        {
                            "source_id": "alpha-compose",
                            "root": "execution_alpha",
                            "parser": "ALPHA_COMPOSE_V1",
                            "path": None,
                            "glob": "alphas/*/docker-compose.yml",
                            "source_policy_id": "crypto_primary_v2",
                            "priority": 100,
                            "ttl_seconds": 180,
                            "default_warmup_limit": 700,
                            "bar_freshness_multiplier": 3,
                        }
                    ],
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return repository, alpha, trading, repository / "config/v2/registry.yaml"

    @staticmethod
    def _compose(
        *,
        symbols: str | None = "BTCUSDT,ETHUSDT",
        realtime: str | None = None,
    ) -> dict[str, object]:
        environment: dict[str, str] = {
            "TRADING_ALPHA_ID": "fixture_alpha",
            "TRADING_VENUE": "BINANCE",
            "ALPHA_INTERVAL": "15m",
            "ALPHA_CONTRACT_TYPE": "USDM",
        }
        if symbols is not None:
            environment["ALPHA_SYMBOLS"] = symbols
        if realtime is not None:
            environment["ALPHA_ENABLE_REALTIME_STREAM"] = realtime
        return {"services": {"fixture": {"environment": environment}}}

    def _compile(self, repository: Path, alpha: Path, trading: Path, registry_path: Path):
        return ActiveDemandCompiler(
            registry=ActiveDemandSourceRegistry.load(registry_path),
            repository_root=repository,
            execution_alpha_root=alpha,
            trading_system_root=trading,
        ).compile()

    def test_compiles_declared_symbols_and_default_realtime_without_runtime_access(self):
        repository, alpha, trading, registry_path = self._fixture()
        compose = alpha / "alphas/fixture/docker-compose.yml"
        compose.write_text(yaml.safe_dump(self._compose(), sort_keys=False), encoding="utf-8")

        inventory = self._compile(repository, alpha, trading, registry_path)

        self.assertEqual({item.feed for item in inventory.requirements}, {DemandFeed.BAR, DemandFeed.TRADE})
        self.assertEqual(
            inventory.requirements[0].universe.native_symbols,
            ("BTCUSDT", "ETHUSDT"),
        )
        self.assertEqual(len(inventory.source_documents), 2)
        self.assertEqual(len(inventory.manifest_sha256), 64)
        report = inventory.report_payload()
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["requirement_count"], 2)
        self.assertNotIn("runtime", json.dumps(report, sort_keys=True).lower())

    def test_realtime_false_and_interval_group_are_resolved_from_declared_config(self):
        repository, alpha, trading, registry_path = self._fixture()
        compose = alpha / "alphas/fixture/docker-compose.yml"
        compose.write_text(
            yaml.safe_dump(self._compose(symbols=None, realtime="false"), sort_keys=False),
            encoding="utf-8",
        )
        (alpha / "alphas/fixture/config.yaml").write_text(
            yaml.safe_dump({"STRATEGY": {"symbols_15m": ["ETHUSDT"]}}, sort_keys=False),
            encoding="utf-8",
        )

        inventory = self._compile(repository, alpha, trading, registry_path)

        self.assertEqual(len(inventory.requirements), 1)
        item = inventory.requirements[0]
        self.assertEqual(item.feed, DemandFeed.BAR)
        self.assertEqual(item.universe.native_symbols, ("ETHUSDT",))
        self.assertEqual(item.warmup_limit, 700)

    def test_ambiguous_group_and_unregistered_owner_fail_closed(self):
        repository, alpha, trading, registry_path = self._fixture()
        compose = alpha / "alphas/fixture/docker-compose.yml"
        compose.write_text(
            yaml.safe_dump(self._compose(symbols=None), sort_keys=False), encoding="utf-8"
        )
        config = alpha / "alphas/fixture/config.yaml"
        config.write_text(
            yaml.safe_dump(
                {"ONE": {"symbols_15m": ["BTCUSDT"]}, "TWO": {"symbols_15m": ["ETHUSDT"]}},
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(InventoryError, "ambiguous"):
            self._compile(repository, alpha, trading, registry_path)

        compose.write_text(
            yaml.safe_dump(
                {
                    "services": {
                        "fixture": {
                            "environment": {
                                "TRADING_ALPHA_ID": "not_registered",
                                "TRADING_VENUE": "BINANCE",
                                "ALPHA_INTERVAL": "15m",
                                "ALPHA_CONTRACT_TYPE": "USDM",
                                "ALPHA_SYMBOLS": "BTCUSDT",
                            }
                        }
                    }
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(InventoryError, "absent"):
            self._compile(repository, alpha, trading, registry_path)

    @staticmethod
    def _requirement(
        *,
        feed: DemandFeed = DemandFeed.BAR,
        interval: str | None = "1d",
        freshness_ms: int | None = None,
        symbols: tuple[str, ...] = ("BTCUSDT",),
        market: str = "USDM",
        product_type: str = "PERPETUAL",
        selector_kind: UniverseSelectorKind = UniverseSelectorKind.EXPLICIT,
    ) -> DataRequirement:
        selector_kwargs = {
            "selector_id": "phase111-selector",
            "kind": selector_kind,
            "venue": "OKX" if market == "SWAP" else "BINANCE",
            "market": market,
            "product_type": product_type,
        }
        if selector_kind is UniverseSelectorKind.EXPLICIT:
            selector_kwargs["native_symbols"] = symbols
        else:
            selector_kwargs.update(
                {
                    "continuous_family": "BTC-USDT",
                    "continuous_roll_policy": "CURRENT_QUARTER",
                }
            )
        default_freshness_ms = 3 * 86_400_000 if feed is DemandFeed.BAR else 15_000
        return DataRequirement(
            consumer_id="phase111.alpha",
            purpose=DemandPurpose.ALPHA,
            universe=UniverseSelector(**selector_kwargs),
            feed=feed,
            source_policy_id="crypto_primary_v2",
            interval=interval,
            warmup_limit=700 if feed is DemandFeed.BAR else 0,
            max_freshness_ms=(
                default_freshness_ms if freshness_ms is None else freshness_ms
            ),
            priority=100,
            ttl_seconds=180,
            require_final_bars=feed is DemandFeed.BAR,
            require_live=True,
            execution_grade=False,
            configuration_revision=7,
        )

    @classmethod
    def _inventory(cls, *requirements: DataRequirement) -> ActiveDemandInventory:
        return ActiveDemandInventory(
            revision=7,
            requirements=tuple(requirements),
            source_documents=(),
            candidates=(),
            exclusions=(),
            input_sha256="a" * 64,
        )

    def test_sampled_freshness_is_bounded_without_relaxing_point_feed(self):
        daily = self._requirement(feed=DemandFeed.BAR, interval="1d")
        self.assertEqual(daily.max_freshness_ms, 3 * 86_400_000)
        basis = self._requirement(feed=DemandFeed.BASIS, interval="1d")
        self.assertEqual(basis.interval, "1d")
        with self.assertRaisesRegex(ValueError, "outside bounds"):
            self._requirement(
                feed=DemandFeed.TRADE,
                interval=None,
                freshness_ms=2 * 86_400_000,
            )

    def test_provider_metadata_admission_is_per_symbol_and_capability_truthful(self):
        explicit = self._requirement(symbols=("BTCUSDT", "ETHUSDT"))
        inventory = self._inventory(explicit)
        payloads = {
            ("BINANCE", "USDM"): {
                "serverTime": 1,
                "symbols": [_binance_symbol("BTCUSDT")],
            }
        }
        report = admit_provider_metadata(inventory, payloads)
        self.assertFalse(report.passed)
        by_symbol = {item.native_symbol: item for item in report.rows}
        self.assertEqual(by_symbol["BTCUSDT"].state, "ADMITTED")
        self.assertEqual(by_symbol["ETHUSDT"].state, "MISSING_INSTRUMENT")
        self.assertEqual(by_symbol["ETHUSDT"].reason, "ETHUSDT")
        self.assertIsNone(by_symbol["ETHUSDT"].instrument_id)

        continuous = self._requirement(
            feed=DemandFeed.BASIS,
            interval="1d",
            market="USDM",
            product_type="FUTURE",
            selector_kind=UniverseSelectorKind.CONTINUOUS,
        )
        continuous_report = admit_provider_metadata(
            self._inventory(continuous),
            {
                ("BINANCE", "USDM"): {
                    "serverTime": 1,
                    "symbols": [
                        _binance_symbol("BTCUSDT"),
                        _binance_symbol("BTCUSDT_270326", contract_type="CURRENT_QUARTER"),
                    ],
                }
            },
        )
        self.assertTrue(continuous_report.passed)
        self.assertEqual(continuous_report.rows[0].native_symbol, "BTCUSDT_270326")

        unavailable = self._requirement(
            feed=DemandFeed.LONG_SHORT_RATIO,
            interval="1d",
            symbols=("BTC-USDT-SWAP",),
            market="SWAP",
            product_type="PERPETUAL",
        )
        unavailable_report = admit_provider_metadata(
            self._inventory(unavailable),
            {("OKX", "SWAP"): [_okx_swap()]},
        )
        self.assertEqual(unavailable_report.rows[0].state, "UNSUPPORTED_CAPABILITY")
        self.assertEqual(unavailable_report.rows[0].capability, "NOT_EVALUATED")

        # An unrelated provider row cannot poison a selected demanded slice.
        # If the demanded row were malformed, the parser would still fail
        # closed rather than fabricating an instrument record.
        okx_trade = self._requirement(
            feed=DemandFeed.TRADE,
            interval=None,
            symbols=("BTC-USDT-SWAP",),
            market="SWAP",
            product_type="PERPETUAL",
        )
        unrelated_malformed = {"instType": "SWAP", "instId": "UNRELATED-SWAP"}
        filtered_report = admit_provider_metadata(
            self._inventory(okx_trade),
            {("OKX", "SWAP"): [_okx_swap(), unrelated_malformed]},
        )
        self.assertTrue(filtered_report.passed)
        self.assertEqual(filtered_report.rows[0].native_symbol, "BTC-USDT-SWAP")

    def test_active_metadata_digest_ignores_volatile_envelope_but_not_demanded_contract_changes(self):
        inventory = self._inventory(self._requirement(symbols=("BTCUSDT",)))
        first_payload = {
            "serverTime": 1,
            "symbols": [
                _binance_symbol("BTCUSDT"),
                _binance_symbol("UNRELATEDUSDT", base="UNRELATED"),
            ],
        }
        second_payload = json.loads(json.dumps(first_payload))
        second_payload["serverTime"] = 2
        second_payload["symbols"] = [
            _binance_symbol("NEWLISTINGUSDT", base="NEW"),
            second_payload["symbols"][1],
            second_payload["symbols"][0],
        ]
        first = admit_provider_metadata(inventory, {("BINANCE", "USDM"): first_payload})
        second = admit_provider_metadata(inventory, {("BINANCE", "USDM"): second_payload})
        self.assertEqual(first.metadata_sha256, second.metadata_sha256)

        changed_payload = json.loads(json.dumps(first_payload))
        changed_payload["symbols"][0]["filters"][0]["tickSize"] = "0.20"
        changed = admit_provider_metadata(
            inventory,
            {("BINANCE", "USDM"): changed_payload},
        )
        self.assertNotEqual(first.metadata_sha256, changed.metadata_sha256)

        okx_requirement = self._requirement(
            feed=DemandFeed.TRADE,
            interval=None,
            symbols=("BTC-USDT-SWAP", "ETH-USDT-SWAP"),
            market="SWAP",
            product_type="PERPETUAL",
        )
        btc = _okx_swap()
        eth = _okx_swap("ETH-USDT-SWAP")
        eth.update({"instFamily": "ETH-USDT", "ctValCcy": "ETH"})
        first_okx = admit_provider_metadata(
            self._inventory(okx_requirement),
            {("OKX", "SWAP"): [btc, eth]},
        )
        second_okx = admit_provider_metadata(
            self._inventory(okx_requirement),
            {("OKX", "SWAP"): [eth, btc, {"instType": "SWAP", "instId": "UNRELATED-SWAP"}]},
        )
        self.assertEqual(first_okx.metadata_sha256, second_okx.metadata_sha256)

    def test_convergence_deduplicates_physical_slices_and_fails_closed_on_budget_or_metadata(self):
        alpha = self._requirement(
            feed=DemandFeed.TRADE,
            interval=None,
            symbols=("BTCUSDT", "ETHUSDT"),
        )
        execution = replace(
            self._requirement(
                feed=DemandFeed.TRADE,
                interval=None,
                symbols=("BTCUSDT",),
            ),
            consumer_id="phase111.execution",
            purpose=DemandPurpose.EXECUTION,
            execution_grade=True,
        )
        inventory = self._inventory(alpha, execution)
        admission = admit_provider_metadata(
            inventory,
            {
                ("BINANCE", "USDM"): {
                    "serverTime": 1,
                    "symbols": [
                        _binance_symbol("BTCUSDT"),
                        _binance_symbol("ETHUSDT", base="ETH"),
                    ],
                }
            },
        )
        policy = AdmissionPolicy(
            max_subscriptions_per_connection=200,
            max_total_slices=1,
            budgets=(AdmissionBudget(
                venue="BINANCE",
                market="USDM",
                feed=DemandFeed.TRADE,
                max_slices=1,
            ),),
        )

        convergence = converge_active_demand(inventory, admission, policy)

        self.assertFalse(convergence.passed)
        self.assertEqual(convergence.selected_slice_count, 1)
        self.assertEqual(len(convergence.topology.subscriptions), 1)
        self.assertEqual(convergence.topology.service_role_count, 1)
        self.assertEqual(convergence.budget_usage[0].selected_slices, 1)
        readiness_by_symbol = {}
        for item in convergence.readiness:
            readiness_by_symbol.setdefault(item.native_symbol, []).append(item)
            self.assertFalse(item.execution_eligible)
        self.assertEqual(
            {item.state for item in readiness_by_symbol["BTCUSDT"]},
            {DemandState.WARMING},
        )
        self.assertEqual(
            {item.state for item in readiness_by_symbol["ETHUSDT"]},
            {DemandState.DEGRADED},
        )
        self.assertEqual(
            {item.reason for item in readiness_by_symbol["ETHUSDT"]},
            {"ADMISSION_BUDGET_EXHAUSTED"},
        )

        missing_inventory = self._inventory(
            self._requirement(symbols=("ETHUSDT",))
        )
        missing_admission = admit_provider_metadata(
            missing_inventory,
            {("BINANCE", "USDM"): {"serverTime": 1, "symbols": []}},
        )
        missing_policy = AdmissionPolicy(
            max_subscriptions_per_connection=200,
            max_total_slices=1,
            budgets=(AdmissionBudget(
                venue="BINANCE",
                market="USDM",
                feed=DemandFeed.BAR,
                max_slices=1,
            ),),
        )
        missing_convergence = converge_active_demand(
            missing_inventory,
            missing_admission,
            missing_policy,
        )
        self.assertEqual(
            missing_convergence.readiness[0].state,
            DemandState.UNSUPPORTED,
        )
        self.assertEqual(
            missing_convergence.readiness[0].reason,
            "MISSING_INSTRUMENT:ETHUSDT",
        )

    def test_convergence_preserves_continuous_selector_to_resolved_dated_contract(self):
        continuous = self._requirement(
            feed=DemandFeed.BASIS,
            interval="1d",
            market="USDM",
            product_type="FUTURE",
            selector_kind=UniverseSelectorKind.CONTINUOUS,
        )
        inventory = self._inventory(continuous)
        admission = admit_provider_metadata(
            inventory,
            {
                ("BINANCE", "USDM"): {
                    "serverTime": 1,
                    "symbols": [
                        _binance_symbol(
                            "BTCUSDT_270326",
                            contract_type="CURRENT_QUARTER",
                        ),
                    ],
                }
            },
        )
        policy = AdmissionPolicy(
            max_subscriptions_per_connection=200,
            max_total_slices=1,
            budgets=(AdmissionBudget(
                venue="BINANCE",
                market="USDM",
                feed=DemandFeed.BASIS,
                max_slices=1,
            ),),
        )

        convergence = converge_active_demand(inventory, admission, policy)

        self.assertTrue(convergence.passed)
        self.assertEqual(convergence.selected_slice_count, 1)
        self.assertEqual(len(convergence.topology.subscriptions), 0)
        self.assertEqual(len(convergence.topology.provisioning_required), 1)
        self.assertEqual(
            convergence.readiness[0].native_symbol,
            "BTCUSDT_270326",
        )
        self.assertEqual(convergence.readiness[0].state, DemandState.WARMING)

    def test_lease_and_topology_scale_symbols_without_runtime_role_growth(self):
        requirement = self._requirement(feed=DemandFeed.TRADE, interval=None)
        capability = CapabilityRegistry.defaults().resolve(
            venue="BINANCE", market="USDM", product_type="PERPETUAL", feed=DemandFeed.TRADE
        )
        base = ResolvedRequirement(
            requirement=requirement,
            instrument_uid="phase111-0000",
            instrument_id="BINANCE.USDM.PERPETUAL.TOKEN-0000",
            native_symbol="TOKEN0000USDT",
            capability=capability,
            binding_id=None,
            state=DemandState.REQUESTED,
            provisioned=False,
            catalog_revision=1,
            demand_revision=7,
        )
        # This is deliberately above the current 658 real admitted physical
        # slices.  It proves that a larger universe only adds bounded internal
        # shards; it never changes the single logical venue/market role.
        many = tuple(
            replace(
                base,
                instrument_uid=f"phase111-{index:04d}",
                instrument_id=f"BINANCE.USDM.PERPETUAL.TOKEN-{index:04d}",
                native_symbol=f"TOKEN{index:04d}USDT",
            )
            for index in range(1_025)
        )
        planner = DemandTopologyPlanner(max_subscriptions_per_connection=200)
        topology = planner.build(many, demand_revision=7)
        self.assertEqual(topology.connection_count, 6)
        self.assertEqual(topology.service_role_count, 1)
        self.assertEqual(len(topology.subscriptions), 1_025)

        leases = DemandLeaseRegistry()
        first = leases.renew("phase111-owner", (base,), now_ns=1_000)
        second = leases.renew("phase111-owner", (base,), now_ns=2_000)
        self.assertEqual(first[0].lease_id, second[0].lease_id)
        self.assertEqual(len(leases.desired(now_ns=2_000)), 1)
        self.assertEqual(len(leases.expire(now_ns=181_000_000_001)), 1)
        self.assertEqual(leases.desired(now_ns=181_000_000_001), ())

    def test_cli_fetches_one_metadata_document_per_required_market_without_raw_persistence(self):
        requirement = self._requirement(feed=DemandFeed.TRADE, interval=None)
        inventory = self._inventory(requirement)
        calls: list[tuple[str, dict[str, str]]] = []

        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self):
                return {"serverTime": 1, "symbols": [_binance_symbol("BTCUSDT")]}

        def get(url, *, params, **_):
            calls.append((url, params))
            return Response()

        payloads = fetch_provider_metadata(
            inventory,
            timeout_seconds=1.0,
            attempts=1,
            get=get,
            sleep=lambda _: None,
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(set(payloads), {("BINANCE", "USDM")})
        self.assertNotIn("payload", json.dumps(admit_provider_metadata(inventory, payloads).report_payload()))

    def test_run_compiles_without_provider_network_when_admission_is_disabled(self):
        repository, alpha, trading, registry_path = self._fixture()
        (alpha / "alphas/fixture/docker-compose.yml").write_text(
            yaml.safe_dump(self._compose(), sort_keys=False), encoding="utf-8"
        )
        inventory, admission = run(
            source_registry=registry_path,
            repository_root=repository,
            execution_alpha_root=alpha,
            trading_system_root=trading,
            provider_admission=False,
            timeout_seconds=1.0,
            attempts=1,
            get=lambda *_args, **_kwargs: self.fail("network must not be used"),
            sleep=lambda _: self.fail("sleep must not be used"),
        )
        self.assertEqual(len(inventory.requirements), 2)
        self.assertIsNone(admission)


if __name__ == "__main__":
    unittest.main()
