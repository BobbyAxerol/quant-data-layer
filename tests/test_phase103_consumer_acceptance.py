from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import yaml

from qdl.certification.phase103_consumer_acceptance import (
    DeliveryClass,
    PHASE103_CONSUMER_IDS,
    build_consumer_acceptance_scope,
    compact_receipt_evidence,
    sdk_requirement,
    validate_product_view,
    validate_final_bar_warmup_windows,
    validate_replica_views,
    validate_resume_offsets,
    warmup_content_fingerprint,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl.query.contracts import FeedType, RecoveryPolicy, StalePolicy
from scripts.phase103_consumer_receipt_acceptance import (
    _query_product,
    _query_product_with_quality,
    _stream_event_timeout_seconds,
)
from qdl_sdk import MarketDataView
from qdl_sdk.models import DecimalValue


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
TRADING_MANIFEST = ROOT / "consumers/stable/trading-system-paper.yaml"
ALPHA_MANIFEST = ROOT / "consumers/stable/alpha-binance-paper.yaml"


class Phase103ConsumerAcceptanceScopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = StableSourceCatalog.load(CATALOG_PATH)
        cls.acquisition = StableAcquisitionPlan.load(
            ACQUISITION_PATH,
            catalog=cls.catalog,
        )

    def scope(self, *paths: Path, acquisition: StableAcquisitionPlan | None = None):
        return build_consumer_acceptance_scope(
            paths or (TRADING_MANIFEST, ALPHA_MANIFEST),
            catalog=self.catalog,
            acquisition=acquisition or self.acquisition,
        )

    def test_governed_manifests_cover_exact_crypto_products_and_vn_is_explicitly_deferred(self):
        scope = self.scope()
        self.assertEqual(
            {item.consumer_id for item in scope.products},
            PHASE103_CONSUMER_IDS,
        )
        self.assertEqual(len(scope.products), 110)
        self.assertEqual(
            sum(item.delivery is DeliveryClass.DURABLE for item in scope.products),
            110,
        )
        pass_through = [
            item
            for item in scope.products
            if item.delivery is DeliveryClass.PROVIDER_PASS_THROUGH
        ]
        self.assertEqual(pass_through, [])
        self.assertEqual(
            {
                item.binding_id
                for item in scope.products
                if (
                    item.consumer_id == "alpha.binance.paper.stable"
                    and item.feed.value == "BAR"
                    and item.interval == "15m"
                )
            },
            {
                "binance-usdm-bnbusdt-bar-15m",
                "binance-usdm-btcusdt-bar-15m",
                "binance-usdm-dogeusdt-bar-15m",
                "binance-usdm-ethusdt-bar-15m",
                "binance-usdm-solusdt-bar-15m",
            },
        )
        self.assertEqual(len(scope.excluded), 76)
        excluded = next(
            item for item in scope.excluded
            if item.reason == "VENUE_NOT_IN_PHASE103_CRYPTO_SCOPE"
        )
        self.assertEqual(excluded.consumer_id, "trading-system.paper.stable")
        self.assertEqual(excluded.reason, "VENUE_NOT_IN_PHASE103_CRYPTO_SCOPE")
        self.assertEqual(excluded.feed.value, "TRADE")
        later_phase = [
            item for item in scope.excluded
            if item.reason == "LATER_PHASE_PRODUCT"
        ]
        self.assertEqual(len(later_phase), 75)
        self.assertEqual(
            {item.consumer_id for item in later_phase},
            {"trading-system.paper.stable", "alpha.binance.paper.stable"},
        )

    def test_scope_digest_is_deterministic_and_evidence_contains_no_market_payload(self):
        first = self.scope()
        second = self.scope(ALPHA_MANIFEST, TRADING_MANIFEST)
        self.assertEqual(first.sha256, second.sha256)
        evidence = first.evidence()
        self.assertEqual(evidence["scope_sha256"], first.sha256)
        self.assertNotIn("payload", str(evidence).lower())
        self.assertNotIn("secret", str(evidence).lower())
        self.assertNotIn("raw_frame", str(evidence).lower())

    def test_missing_or_foreign_manifest_set_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "requires exactly"):
            self.scope(TRADING_MANIFEST)
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-acceptance-") as directory:
            foreign = Path(directory) / "foreign.yaml"
            payload = yaml.safe_load(ALPHA_MANIFEST.read_text(encoding="utf-8"))
            payload["metadata"]["id"] = "alpha.unapproved.paper"
            foreign.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "requires exactly"):
                self.scope(TRADING_MANIFEST, foreign)

    def test_disabled_durable_binding_fails_closed(self):
        disabled_id = "binance-usdm-btcusdt-trade"
        acquisition = StableAcquisitionPlan(
            schema=self.acquisition.schema,
            revision=self.acquisition.revision,
            raw_topic=self.acquisition.raw_topic,
            canonical_topic=self.acquisition.canonical_topic,
            quarantine_topic=self.acquisition.quarantine_topic,
            bindings=tuple(
                replace(item, enabled=False) if item.binding_id == disabled_id else item
                for item in self.acquisition.bindings
            ),
        )
        with self.assertRaisesRegex(ValueError, "disabled acquisition"):
            self.scope(acquisition=acquisition)

    def test_missing_durable_binding_and_wrong_policy_fail_closed(self):
        missing_binding = "binance-usdm-btcusdt-trade"
        catalog = StableSourceCatalog(
            canonical_stream=self.catalog.canonical_stream,
            bindings=tuple(
                item for item in self.catalog.bindings if item.binding_id != missing_binding
            ),
            catalog_revision=self.catalog.catalog_revision,
            source_policy_revision=self.catalog.source_policy_revision,
            authority_revision=self.catalog.authority_revision,
            instruments=self.catalog.instruments,
        )
        with self.assertRaisesRegex(ValueError, "neither a durable binding"):
            build_consumer_acceptance_scope(
                (TRADING_MANIFEST, ALPHA_MANIFEST),
                catalog=catalog,
                acquisition=self.acquisition,
            )

        with tempfile.TemporaryDirectory(prefix="qdl-phase103-acceptance-") as directory:
            changed = Path(directory) / "alpha.yaml"
            payload = yaml.safe_load(ALPHA_MANIFEST.read_text(encoding="utf-8"))
            payload["spec"]["requirements"][0]["source_policy_id"] = "wrong_policy"
            changed.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unapproved source policy"):
                self.scope(TRADING_MANIFEST, changed)

    @staticmethod
    def _decimal(text: str) -> dict[str, object]:
        value = Decimal(text)
        sign, digits, exponent = value.as_tuple()
        coefficient = "".join(str(item) for item in digits) or "0"
        if sign and coefficient != "0":
            coefficient = "-" + coefficient
        return {
            "coefficient": coefficient,
            "scale": -exponent,
            "source_text": text,
        }

    def _product(
        self,
        *,
        feed: str,
        delivery: DeliveryClass = DeliveryClass.DURABLE,
        consumer_id: str | None = None,
        interval: str | None = None,
    ):
        return next(
            item
            for item in self.scope().products
            if (
                item.feed.value == feed
                and item.delivery is delivery
                and (consumer_id is None or item.consumer_id == consumer_id)
                and (interval is None or item.interval == interval)
            )
        )

    def _pass_through_bar(self):
        """Exercise the legal provider history path without weakening the manifest."""
        durable = self._product(
            feed="BAR",
            consumer_id="alpha.binance.paper.stable",
            interval="15m",
        )
        return replace(
            durable,
            delivery=DeliveryClass.PROVIDER_PASS_THROUGH,
            requirement=replace(
                durable.requirement,
                recovery=RecoveryPolicy.FRESH_SNAPSHOT,
            ),
        )

    def _quiet_book_delta_product(self):
        """Build one governed quiet-delta receipt without weakening price rules."""
        trade = self._product(feed="TRADE")
        return replace(
            trade,
            feed=FeedType.BOOK_DELTA,
            requirement=replace(
                trade.requirement,
                feed=FeedType.BOOK_DELTA,
                source_policy_id="crypto_liquid_v2",
                max_freshness_ms=2_000,
                event_recency_policy=StalePolicy.OBSERVE,
                max_session_liveness_ms=45_000,
                require_final_bars=False,
            ),
        )

    def _view(
        self,
        product,
        *,
        close: str = "10.2",
        source_authoritative: bool | None = None,
        execution_eligible: bool | None = None,
        freshness_ms: int = 1,
        gap_open: bool = False,
        open_time_ns: int = 1_000_000_000,
    ) -> MarketDataView:
        d = self._decimal
        if product.feed.value == "TRADE":
            payload = {
                "feed": "TRADE",
                "native_trade_id": "trade-1",
                "price": d(close),
                "quantity": d("0.1"),
                "quantity_unit": "BASE_ASSET",
                "aggressor_side": "BUY",
                "identity_kind": "NATIVE",
            }
        elif product.feed.value == "QUOTE":
            payload = {
                "feed": "QUOTE",
                "bid_price": d("10.1"),
                "bid_quantity": d("1"),
                "ask_price": d("10.2"),
                "ask_quantity": d("2"),
                "quantity_unit": "BASE_ASSET",
                "level": 1,
            }
        elif product.feed.value == "BOOK_DELTA":
            payload = {
                "feed": "BOOK_DELTA",
                "native_sequence_start": "101",
                "native_sequence_end": "102",
                "snapshot_sequence": "100",
                "updates": [{
                    "side": "BID",
                    "price": d("10.1"),
                    "quantity": d("1"),
                    "quantity_unit": "BASE_ASSET",
                }],
                "reset": False,
                "book_generation": 1,
                "sequence_verified": True,
            }
        else:
            payload = {
                "feed": "BAR",
                "interval": product.interval,
                "open_time_ns": open_time_ns,
                "close_time_ns": open_time_ns + 59_999_000_000,
                "open": d("10"),
                "high": d("12"),
                "low": d("9"),
                "close": d(close),
                "volume": d("5"),
                "volume_unit": "BASE_ASSET",
                "trade_count": 2,
                "lifecycle": "FINAL",
                "revision": 0,
                "origin": "VENUE_NATIVE",
            }
        durable = product.delivery is DeliveryClass.DURABLE
        return MarketDataView.model_validate(
            {
                "instrument_uid": product.instrument_uid,
                "instrument_id": product.instrument_id,
                "instrument_revision": 1,
                "feed": product.feed.value,
                "interval": product.interval,
                "observed_at_ns": 61_000_000_000,
                "received_at_ns": 61_000_100_000,
                "revision": 0,
                "payload": payload,
                "source": {
                    "venue": product.venue,
                    "provider": product.provider,
                    "source_id": "source-1",
                    "source_role": "PRIMARY" if durable else "REFERENCE",
                    "authoritative": (
                        durable if source_authoritative is None else source_authoritative
                    ),
                },
                "quality": {
                    "state": "LIVE",
                    "freshness_ms": freshness_ms,
                    "event_recency_state": "LIVE",
                    "provider_session_state": (
                        "LIVE"
                        if product.requirement.max_session_liveness_ms is not None
                        else "NOT_APPLICABLE"
                    ),
                    "provider_session_liveness_ms": (
                        1
                        if product.requirement.max_session_liveness_ms is not None
                        else None
                    ),
                    "gap_open": gap_open,
                    "complete": True,
                    "execution_eligible": (
                        durable if execution_eligible is None else execution_eligible
                    ),
                    "policy_id": product.source_policy_id,
                    "flags": [],
                },
                "contract": {
                    "schema_digest": "a" * 64,
                    "contract_version": "2.0.0",
                    "normalizer_version": "test",
                    "adapter_version": "test",
                    "instrument_catalog_revision": 1,
                    "source_policy_revision": 1,
                    "authority_revision": 1,
                    "config_revision": 1,
                    "correlation_id": "phase103-test",
                },
                "cursor": "sensitive-cursor-token",
                "snapshot_id": "snapshot-1",
                "watermark_offset": 7,
            }
        )

    def test_sdk_mapping_preserves_every_governed_requirement_field(self):
        product = self._product(feed="BAR")
        value = sdk_requirement(product)
        source = product.requirement
        self.assertEqual(value.instrument_uid, source.instrument_uid)
        self.assertEqual(value.feed.value, source.feed.value)
        self.assertEqual(value.consumer_grade.value, source.consumer_grade.value)
        self.assertEqual(value.interval, source.interval)
        self.assertEqual(value.warmup_limit, source.warmup_limit)
        self.assertEqual(value.event_recency_policy, source.event_recency_policy)
        self.assertEqual(
            value.max_session_liveness_ms,
            source.max_session_liveness_ms,
        )
        self.assertEqual(value.recovery.value, source.recovery.value)
        self.assertEqual(value.bar_revision_policy.value, source.bar_revision_policy.value)

    def test_quiet_connected_trade_is_accepted_for_no_order_observation_only(self):
        product = self._product(feed="TRADE")
        quiet = self._view(
            product,
            freshness_ms=product.requirement.max_freshness_ms + 1,
            execution_eligible=False,
        )
        quiet = quiet.model_copy(
            update={
                "quality": quiet.quality.model_copy(
                    update={
                        "event_recency_state": "STALE",
                        "provider_session_state": "LIVE",
                        "provider_session_liveness_ms": 1,
                    }
                )
            }
        )
        validate_product_view(product, quiet)

        disconnected = quiet.model_copy(
            update={
                "quality": quiet.quality.model_copy(
                    update={
                        "freshness_ms": 1,
                        "event_recency_state": "LIVE",
                        "provider_session_state": "DISCONNECTED",
                    }
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "provider session"):
            validate_product_view(product, disconnected)

    def test_quiet_connected_book_delta_is_observable_but_never_price_eligible(self):
        product = self._quiet_book_delta_product()
        quiet = self._view(
            product,
            freshness_ms=product.requirement.max_freshness_ms + 1,
            execution_eligible=False,
        )
        quiet = quiet.model_copy(
            update={
                "quality": quiet.quality.model_copy(
                    update={
                        "event_recency_state": "STALE",
                        "provider_session_state": "LIVE",
                        "provider_session_liveness_ms": 1,
                    }
                )
            }
        )
        validate_product_view(product, quiet)

        blocked = replace(
            product,
            requirement=replace(product.requirement, event_recency_policy=None),
        )
        with self.assertRaisesRegex(ValueError, "freshness"):
            validate_product_view(blocked, quiet)

        disconnected = quiet.model_copy(
            update={
                "quality": quiet.quality.model_copy(
                    update={"provider_session_state": "DISCONNECTED"}
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "provider session"):
            validate_product_view(product, disconnected)

        with self.assertRaisesRegex(ValueError, "gap"):
            validate_product_view(
                product,
                quiet.model_copy(
                    update={"quality": quiet.quality.model_copy(update={"gap_open": True})}
                ),
            )

        with self.assertRaisesRegex(ValueError, "verified"):
            validate_product_view(
                product,
                quiet.model_copy(
                    update={
                        "payload": quiet.payload.model_copy(
                            update={"sequence_verified": False}
                        )
                    }
                ),
            )

    def test_typed_views_enforce_durable_and_pass_through_domain_semantics(self):
        for feed in ("TRADE", "QUOTE", "BAR"):
            with self.subTest(feed=feed):
                product = self._product(feed=feed)
                validate_product_view(product, self._view(product))

        pass_through = self._pass_through_bar()
        validate_product_view(pass_through, self._view(pass_through))
        baseline = self._view(pass_through)
        durable_upgrade = baseline.model_copy(
            update={
                "source": baseline.source.model_copy(
                    update={"source_role": "PRIMARY", "authoritative": True}
                ),
                "quality": baseline.quality.model_copy(
                    update={"execution_eligible": True}
                ),
            }
        )
        validate_product_view(pass_through, durable_upgrade)
        with self.assertRaisesRegex(ValueError, "pass-through"):
            validate_product_view(
                pass_through,
                self._view(pass_through, source_authoritative=True),
            )
        with self.assertRaisesRegex(ValueError, "provider"):
            validate_product_view(
                pass_through,
                baseline.model_copy(
                    update={
                        "source": baseline.source.model_copy(
                            update={"provider": "WRONG_PROVIDER"}
                        )
                    }
                ),
            )

    def test_bad_quality_decimal_and_replica_divergence_fail_closed(self):
        trade = self._product(feed="TRADE")
        with self.assertRaisesRegex(ValueError, "freshness"):
            validate_product_view(
                trade,
                self._view(trade, freshness_ms=trade.requirement.max_freshness_ms + 1),
            )
        with self.assertRaisesRegex(ValueError, "gap"):
            validate_product_view(trade, self._view(trade, gap_open=True))
        bad_decimal = self._view(trade).model_copy(
            update={
                "payload": self._view(trade).payload.model_copy(
                    update={
                        # Bypass Pydantic construction deliberately so the
                        # receipt validator, rather than the model parser,
                        # proves it rejects inconsistent canonical decimals.
                        "price": DecimalValue.model_construct(
                            coefficient="1",
                            scale=0,
                            source_text="2",
                        )
                    }
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "decimal"):
            validate_product_view(trade, bad_decimal)
        wrong_provider = self._view(trade).model_copy(
            update={
                "source": self._view(trade).source.model_copy(
                    update={"provider": "UNAPPROVED"}
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "authoritative"):
            validate_product_view(trade, wrong_provider)

        bar = self._product(feed="BAR")
        first = self._view(bar, close="10.2")
        second = self._view(bar, close="10.2")
        first_hash, second_hash = validate_replica_views(bar, first, second)
        self.assertEqual(first_hash, second_hash)
        self.assertEqual(warmup_content_fingerprint((first,)), warmup_content_fingerprint((second,)))
        with self.assertRaisesRegex(ValueError, "diverged"):
            validate_replica_views(bar, first, self._view(bar, close="10.5"))

        # A live TRADE can advance between sequential replica reads; typed
        # identity/quality still has to agree, but payload equality is not a
        # valid production invariant.
        primary_hash, secondary_hash = validate_replica_views(
            trade,
            self._view(trade, close="10.2"),
            self._view(trade, close="10.3"),
        )
        self.assertNotEqual(primary_hash, secondary_hash)

    def test_final_bar_warmup_window_allows_only_one_boundary_rollover(self):
        bar = self._product(feed="BAR")
        minute = 60_000_000_000

        def window(opens, *, closes=None):
            return tuple(
                self._view(
                    bar,
                    open_time_ns=open_time_ns,
                    close=(closes or {}).get(open_time_ns, "10.2"),
                )
                for open_time_ns in opens
            )

        exact = window((minute, 2 * minute, 3 * minute))
        exact_evidence = validate_final_bar_warmup_windows(exact, exact)
        self.assertEqual(exact_evidence["comparison"], "EXACT")
        self.assertEqual(exact_evidence["common_row_count"], 3)
        self.assertEqual(exact_evidence["tail_skew_rows"], 0)

        primary = window((minute, 2 * minute, 3 * minute))
        secondary = window((2 * minute, 3 * minute, 4 * minute))
        rollover = validate_final_bar_warmup_windows(primary, secondary)
        self.assertEqual(rollover["comparison"], "SINGLE_FINAL_BAR_ROLLOVER")
        self.assertEqual(rollover["common_row_count"], 2)
        self.assertEqual(rollover["tail_skew_rows"], 1)
        self.assertNotEqual(
            rollover["primary_content_sha256"],
            rollover["secondary_content_sha256"],
        )

    def test_final_bar_warmup_window_rejects_immutable_or_multi_row_divergence(self):
        bar = self._product(feed="BAR")
        minute = 60_000_000_000

        def window(opens, *, closes=None):
            return tuple(
                self._view(
                    bar,
                    open_time_ns=open_time_ns,
                    close=(closes or {}).get(open_time_ns, "10.2"),
                )
                for open_time_ns in opens
            )

        primary = window((minute, 2 * minute, 3 * minute))
        conflicting = window(
            (2 * minute, 3 * minute, 4 * minute),
            closes={2 * minute: "10.3"},
        )
        with self.assertRaisesRegex(ValueError, "immutable common BAR content"):
            validate_final_bar_warmup_windows(primary, conflicting)

        with self.assertRaisesRegex(ValueError, "exceeds one final BAR"):
            validate_final_bar_warmup_windows(
                primary,
                window((3 * minute, 4 * minute, 5 * minute)),
            )
        with self.assertRaisesRegex(ValueError, "overlap is not contiguous"):
            validate_final_bar_warmup_windows(
                window((minute, 2 * minute, 3 * minute, 4 * minute)),
                window((minute, 3 * minute, 4 * minute, 5 * minute)),
            )
        with self.assertRaisesRegex(ValueError, "row counts differ"):
            validate_final_bar_warmup_windows(
                window((minute, 2 * minute, 3 * minute)),
                window((minute, 2 * minute)),
            )

    def test_query_product_accepts_a_single_final_bar_rollover(self):
        bar = self._product(feed="BAR")
        minute = 60_000_000_000
        primary_rows = [
            self._view(bar, open_time_ns=minute),
            self._view(bar, open_time_ns=2 * minute),
            self._view(bar, open_time_ns=3 * minute),
        ]
        secondary_rows = [
            self._view(bar, open_time_ns=2 * minute),
            self._view(bar, open_time_ns=3 * minute),
            self._view(bar, open_time_ns=4 * minute),
        ]

        class WarmupClient:
            def __init__(self, rows):
                self.rows = rows

            async def warmup(self, requirement):
                self.requirement = requirement
                return SimpleNamespace(data=self.rows)

        primary = WarmupClient(primary_rows)
        secondary = WarmupClient(secondary_rows)
        primary_hash, secondary_hash, _, _ = asyncio.run(
            _query_product(bar, primary=primary, secondary=secondary)
        )
        self.assertNotEqual(primary_hash, secondary_hash)
        self.assertEqual(primary.requirement, sdk_requirement(bar))
        self.assertEqual(secondary.requirement, sdk_requirement(bar))

        quality_primary = WarmupClient(primary_rows)
        quality_secondary = WarmupClient(secondary_rows)
        result = asyncio.run(
            _query_product_with_quality(
                bar,
                primary=quality_primary,
                secondary=quality_secondary,
            )
        )
        self.assertEqual(result[5]["comparison"], "SINGLE_FINAL_BAR_ROLLOVER")
        self.assertEqual(result[5]["common_row_count"], 2)

    def test_provider_pass_through_bar_keeps_exact_replica_comparison(self):
        bar = self._pass_through_bar()
        minute = 60_000_000_000

        class WarmupClient:
            def __init__(self, rows):
                self.rows = rows

            async def warmup(self, requirement):
                return SimpleNamespace(data=self.rows)

        primary = WarmupClient([
            self._view(bar, open_time_ns=minute),
            self._view(bar, open_time_ns=2 * minute),
        ])
        secondary = WarmupClient([
            self._view(bar, open_time_ns=2 * minute),
            self._view(bar, open_time_ns=3 * minute),
        ])
        with self.assertRaisesRegex(ValueError, "diverged"):
            asyncio.run(_query_product(bar, primary=primary, secondary=secondary))

    def test_historical_bar_keeps_domain_checks_but_not_current_sla(self):
        bar = self._product(feed="BAR")
        current = self._view(
            bar,
            freshness_ms=bar.requirement.max_freshness_ms + 1,
        )
        historical = current.model_copy(
            update={
                "quality": current.quality.model_copy(
                    update={"state": "STALE", "execution_eligible": False}
                )
            }
        )
        with self.assertRaisesRegex(ValueError, "freshness"):
            validate_product_view(bar, historical)
        validate_product_view(bar, historical, require_current_quality=False)

        gapped = historical.model_copy(
            update={
                "quality": historical.quality.model_copy(update={"gap_open": True})
            }
        )
        with self.assertRaisesRegex(ValueError, "gap"):
            validate_product_view(
                bar,
                gapped,
                require_current_quality=False,
            )

    def test_warmup_applies_current_quality_only_to_last_closed_bar(self):
        bar = self._product(feed="BAR")
        minute = 60_000_000_000
        current = self._view(bar, open_time_ns=2 * minute)
        historical = current.model_copy(
            update={
                "payload": current.payload.model_copy(
                    update={
                        "open_time_ns": minute,
                        "close_time_ns": minute + 59_999_000_000,
                    }
                ),
                "quality": current.quality.model_copy(
                    update={
                        "state": "STALE",
                        "freshness_ms": bar.requirement.max_freshness_ms + 1,
                        "execution_eligible": False,
                    }
                )
            }
        )

        class WarmupClient:
            async def warmup(self, requirement):
                self.requirement = requirement
                return SimpleNamespace(data=[historical, current])

        primary = WarmupClient()
        secondary = WarmupClient()
        primary_hash, secondary_hash, _, _ = asyncio.run(
            _query_product(bar, primary=primary, secondary=secondary)
        )
        self.assertEqual(primary_hash, secondary_hash)
        self.assertEqual(primary.requirement, sdk_requirement(bar))

    def test_bar_stream_wait_covers_one_close_but_stays_sla_bounded(self):
        bar = self._product(
            feed="BAR",
            consumer_id="trading-system.paper.stable",
            interval="1m",
        )
        self.assertEqual(_stream_event_timeout_seconds(bar, 15.0), 75.0)
        self.assertLessEqual(
            _stream_event_timeout_seconds(bar, 15.0) * 1_000,
            bar.requirement.max_freshness_ms,
        )
        alpha_bar = self._pass_through_bar()
        self.assertEqual(_stream_event_timeout_seconds(alpha_bar, 15.0), 915.0)
        trade = self._product(feed="TRADE")
        self.assertEqual(_stream_event_timeout_seconds(trade, 15.0), 15.0)
        book_snapshot = SimpleNamespace(
            feed=SimpleNamespace(value="BOOK_SNAPSHOT"),
            requirement=SimpleNamespace(max_freshness_ms=60_000),
        )
        self.assertEqual(_stream_event_timeout_seconds(book_snapshot, 15.0), 60.0)

    def test_cursor_continuity_and_evidence_redaction_are_strict(self):
        product = self._product(feed="TRADE")
        validate_resume_offsets(acknowledged_offset=7, resumed_offset=8)
        with self.assertRaisesRegex(ValueError, "strictly increasing"):
            validate_resume_offsets(acknowledged_offset=7, resumed_offset=7)
        evidence = compact_receipt_evidence(
            product,
            primary_hash="a" * 64,
            secondary_hash="b" * 64,
            primary_latency_ms=1.25,
            secondary_latency_ms=2.5,
            acknowledged_offset=7,
            resumed_offset=8,
        )
        self.assertNotIn("cursor", str(evidence).lower())
        self.assertNotIn("sensitive", str(evidence).lower())
        self.assertEqual(evidence["acknowledged_offset"], 7)
        self.assertEqual(evidence["resumed_offset"], 8)


if __name__ == "__main__":
    unittest.main()
