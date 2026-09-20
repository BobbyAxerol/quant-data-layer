from __future__ import annotations

import json
import tempfile
import unittest
import asyncio
import httpx
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from qdl.certification.phase103_consumer_acceptance import AcceptanceProduct, DeliveryClass
from qdl.certification.phase105_consumer_acceptance import (
    build_release_consumer_acceptance_scope,
)
from qdl.certification.phase105_fallback import build_v1_fallback_probes
from qdl.consumer import StableReleaseRoutePlan, requirement_key
from qdl.query import ConsumerGrade, DataRequirement, FeedType, RecoveryPolicy, StalePolicy
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl_sdk import (
    DataLayerError,
    DataRequirement as SdkDataRequirement,
    Feed,
    FeedStatusResponse,
    Grade,
    WarmupSpecification as SdkWarmupSpecification,
)
from scripts.phase103_consumer_receipt_acceptance import C2StatusEvidenceError
from scripts.phase105_consumer_v2_identity_acceptance import (
    C2ProductAcceptanceError,
    C2ReferenceProductError,
    C2ClosingBatchError,
    C2OpeningCapacityError,
    _C2ConsumerRequestPacer,
    _PacedQueryTransport,
    _PacedStreamTransport,
    IDENTITY_PREFIXES,
    _authority,
    _closing_batch_problem_evidence,
    _closing_batch_revalidation,
    _closing_batches,
    _closing_requirement,
    _c2_grpc_targets,
    _consumer_ids,
    _build_c2_opening_operation_plan,
    _certify_references,
    _effective_c2_opening_timeout_seconds,
    _identity_files,
    _identity_files_for_consumers,
    _read_plane_preflight_receipt,
    _route_summary,
    _reference_batch_concurrency,
    _reference_transport_timeout_seconds,
    _run_consumer_groups,
    _timing_policy,
    _paced_client_factory,
    _wait_for_minimum_observation,
    _v1_base_url,
    parser,
)


class Phase105IdentityAcceptanceTests(unittest.TestCase):
    def test_timing_policy_separates_final_bar_continuity_from_quiet_execution(self) -> None:
        bar = SimpleNamespace(requirement=DataRequirement(
            instrument_uid="bar-uid",
            feed=FeedType.BAR,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval="1m",
            max_freshness_ms=180_000,
            require_final_bars=True,
        ))
        quiet = SimpleNamespace(requirement=DataRequirement(
            instrument_uid="mark-uid",
            feed=FeedType.MARK_INDEX_PRICE,
            consumer_grade=ConsumerGrade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=2_000,
            event_recency_policy=StalePolicy.OBSERVE,
            max_session_liveness_ms=45_000,
        ))
        quote = SimpleNamespace(requirement=DataRequirement(
            instrument_uid="quote-uid",
            feed=FeedType.QUOTE,
            consumer_grade=ConsumerGrade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=2_000,
            max_session_liveness_ms=45_000,
        ))
        reference_mark = SimpleNamespace(requirement=DataRequirement(
            instrument_uid="reference-mark-uid",
            feed=FeedType.MARK_INDEX_PRICE,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="crypto_liquid_v2",
            max_freshness_ms=2_000,
        ))
        self.assertEqual(_timing_policy(bar)["semantic_class"], "FINAL_SCHEDULED")
        self.assertEqual(_timing_policy(bar)["freshness_role"], "CONTINUITY_DROPOUT_HORIZON")
        self.assertEqual(_timing_policy(bar)["interval_ms"], 60_000)
        self.assertEqual(_timing_policy(quiet)["semantic_class"], "QUIET_SESSION")
        self.assertEqual(_timing_policy(quote)["semantic_class"], "STRICT_EVENT_WITH_SESSION")
        self.assertEqual(_timing_policy(reference_mark)["semantic_class"], "REFERENCE_SNAPSHOT")
        self.assertEqual(
            _timing_policy(reference_mark)["freshness_role"], "PROVIDER_OBSERVATION_AGE"
        )

    def test_timing_policy_fails_closed_for_invalid_bar_or_quiet_session_contract(self) -> None:
        short_bar = SimpleNamespace(requirement=DataRequirement(
            instrument_uid="bar-uid",
            feed=FeedType.BAR,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval="1m",
            max_freshness_ms=59_999,
            require_final_bars=True,
        ))
        strict_without_session = SimpleNamespace(requirement=DataRequirement(
            instrument_uid="trade-uid",
            feed=FeedType.TRADE,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=2_000,
        ))
        missing_observed_session = SimpleNamespace(requirement=SimpleNamespace(
            feed=FeedType.TRADE,
            consumer_grade=ConsumerGrade.ALPHA,
            effective_event_recency_policy=StalePolicy.OBSERVE,
            max_freshness_ms=2_000,
            max_session_liveness_ms=None,
            interval=None,
            require_final_bars=False,
        ))
        # The public DataRequirement constructor rejects this malformed shape
        # first.  Keep a boundary-shaped object here to prove C2 independently
        # refuses it if a bad serialized/configured requirement reaches the
        # acceptance harness.
        missing_execution_mark_session = SimpleNamespace(requirement=SimpleNamespace(
            feed=FeedType.MARK_INDEX_PRICE,
            consumer_grade=ConsumerGrade.EXECUTION,
            effective_event_recency_policy=StalePolicy.OBSERVE,
            max_freshness_ms=2_000,
            max_session_liveness_ms=None,
            interval=None,
            require_final_bars=False,
        ))
        with self.assertRaisesRegex(ValueError, "continuity horizon"):
            _timing_policy(short_bar)
        self.assertEqual(_timing_policy(strict_without_session)["semantic_class"], "STRICT_EVENT")
        self.assertEqual(
            _timing_policy(strict_without_session)["session_contract"],
            "NOT_DECLARED_STRICT_EVENT",
        )
        with self.assertRaisesRegex(ValueError, "session-liveness"):
            _timing_policy(missing_observed_session)
        with self.assertRaisesRegex(ValueError, "session-liveness"):
            _timing_policy(missing_execution_mark_session)

    def test_closing_batches_isolate_hot_feeds_without_losing_scope(self) -> None:
        products = tuple(SimpleNamespace(identity=(venue, symbol, feed), feed=Feed(feed))
            for venue in ("BINANCE", "OKX")
            for symbol in ("BTC", "ETH", "SOL", "DOGE", "BNB")
            for feed in ("TRADE", "BAR", "QUOTE", "BOOK_SNAPSHOT", "BOOK_DELTA"))
        for limit in (1, 5, 50):
            batches = tuple(_closing_batches(products, limit))
            actual = [item.identity for batch in batches for item in batch]
            self.assertCountEqual(actual, [item.identity for item in products])
            self.assertEqual(len(actual), len(set(actual)))
            for batch in batches:
                self.assertEqual(len({item.feed for item in batch}), 1)
                bound = limit if batch[0].feed is Feed.BAR else min(limit, 8)
                self.assertLessEqual(len(batch), bound)
        self.assertEqual(tuple(_closing_batches((), 50)), ())

    def test_typed_c2_product_failure_keeps_status_without_market_payload(self) -> None:
        status = FeedStatusResponse.model_validate({
            "schema": "qdl.feed-status.v2",
            "instrument_uid": "book-uid",
            "feed": "BOOK_SNAPSHOT",
            "quality": {
                "state": "GAPPED",
                "freshness_ms": 12,
                "event_recency_state": "LIVE",
                "provider_session_state": "NOT_APPLICABLE",
                "provider_session_liveness_ms": None,
                "gap_open": True,
                "complete": False,
                "execution_eligible": False,
                "policy_id": "crypto_liquid_v2",
                "flags": ["SEQUENCE_GAP"],
            },
        })
        error = C2StatusEvidenceError("DATA_STALE", "strict book rejected", status=status)
        error.replica = "secondary"
        product = SimpleNamespace(
            consumer_id="trading-system.paper.stable",
            instrument_id="OKX.SWAP.PERPETUAL.SOL-USDT",
            feed=Feed.BOOK_SNAPSHOT,
            interval=None,
            evidence=lambda: {"instrument_uid": "book-uid", "feed": "BOOK_SNAPSHOT"},
        )
        failure = C2ProductAcceptanceError(product, error)
        self.assertEqual(failure.evidence["replica"], "secondary")
        self.assertEqual(failure.evidence["typed_status"]["quality"]["state"], "GAPPED")
        self.assertFalse(failure.evidence["payload_recorded"])
        self.assertNotIn("levels", repr(failure.evidence))

    def test_typed_c2_reference_failure_keeps_product_identity_without_payload(self) -> None:
        product = SimpleNamespace(
            consumer_id="trading-system.paper.stable",
            instrument_id="OKX.SWAP.PERPETUAL.DOGE-USDT",
            requirement=SimpleNamespace(feed=Feed.MARK_INDEX_PRICE),
            evidence=lambda: {
                "instrument_uid": "mark-uid",
                "feed": "MARK_INDEX_PRICE",
            },
        )
        failure = C2ReferenceProductError(
            product,
            replica="primary",
            error=ValueError("quiet execution MARK/INDEX component exceeded its cadence"),
        )
        self.assertEqual(failure.evidence["replica"], "primary")
        self.assertEqual(failure.evidence["product"]["instrument_uid"], "mark-uid")
        self.assertFalse(failure.evidence["payload_recorded"])
        self.assertNotIn("price", repr(failure.evidence))

    def test_reference_batch_failure_is_bound_to_exact_product_and_replica(self) -> None:
        product = SimpleNamespace(consumer_id="trading-system.paper.stable")
        reference_product = SimpleNamespace(
            consumer_id="trading-system.paper.stable",
            instrument_id="OKX.SWAP.PERPETUAL.DOGE-USDT",
            requirement=SimpleNamespace(feed=Feed.MARK_INDEX_PRICE),
            identity=("trading-system.paper.stable", "mark-uid", "MARK_INDEX_PRICE", "", "crypto"),
            evidence=lambda: {"instrument_uid": "mark-uid", "feed": "MARK_INDEX_PRICE"},
        )
        response = SimpleNamespace(results=(object(),))

        class Client:
            def __init__(self, replica: str) -> None:
                self.replica = replica
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        clients = []

        def client_factory(_identity, *, base_url, **_kwargs):
            client = Client("primary" if base_url == "https://primary" else "secondary")
            clients.append(client)
            return client

        async def batch_for_c2(client, *_args, **_kwargs):
            if client.replica == "secondary":
                await asyncio.sleep(0.01)
            return response, 1, 0

        with patch(
            "scripts.phase105_consumer_v2_identity_acceptance._reference_product",
            return_value=reference_product,
        ), patch(
            "scripts.phase105_consumer_v2_identity_acceptance._reference_transport_timeout_seconds",
            return_value=1.0,
        ), patch(
            "scripts.phase105_consumer_v2_identity_acceptance.reference_acceptance_batches",
            return_value=((reference_product,),),
        ), patch(
            "scripts.phase105_consumer_v2_identity_acceptance._reference_batch_for_c2",
            side_effect=batch_for_c2,
        ), patch(
            "scripts.phase105_consumer_v2_identity_acceptance.reference_evidence",
            side_effect=ValueError("quiet execution MARK/INDEX provenance is invalid"),
        ):
            with self.assertRaises(C2ReferenceProductError) as raised:
                asyncio.run(_certify_references(
                    (product,),
                    identity=object(),
                    primary_url="https://primary",
                    secondary_url="https://secondary",
                    grpc_target="stream:8210",
                    state_dir=Path("/tmp"),
                    timeout_seconds=1.0,
                    deadline_monotonic=10.0,
                    semaphore=asyncio.Semaphore(1),
                    native_basis_semaphore=asyncio.Semaphore(1),
                    client_factory=client_factory,
                ))
        self.assertEqual(raised.exception.evidence["replica"], "primary")
        self.assertEqual(
            raised.exception.evidence["product"]["instrument_uid"], "mark-uid",
        )
        self.assertTrue(all(client.closed for client in clients))

    def test_closing_bar_requirement_keeps_policy_and_reduces_only_history_rows(self) -> None:
        requirement = SdkDataRequirement(
            instrument_uid="bar-uid",
            feed=Feed.BAR,
            consumer_grade=Grade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval="15m",
            warmup_limit=700,
            max_freshness_ms=5_000,
            max_session_liveness_ms=8_000,
            require_full_coverage=True,
            require_final_bars=True,
            warmup=SdkWarmupSpecification(
                rows=700,
                deadline_ms=9_000,
                max_cache_age_ms=7_000,
            ),
        )
        product = SimpleNamespace()
        with patch(
            "scripts.phase105_consumer_v2_identity_acceptance.sdk_requirement",
            return_value=requirement,
        ), patch(
            "scripts.phase105_consumer_v2_identity_acceptance._c2_requirement",
            side_effect=lambda value: value,
        ):
            closing = _closing_requirement(product)
        self.assertEqual(requirement.warmup_limit, 700)
        self.assertEqual(requirement.warmup.rows, 700)
        self.assertEqual(closing.warmup_limit, 2)
        self.assertEqual(closing.warmup.rows, 2)
        self.assertEqual(closing.max_freshness_ms, requirement.max_freshness_ms)
        self.assertEqual(closing.max_session_liveness_ms, requirement.max_session_liveness_ms)
        self.assertEqual(closing.require_full_coverage, requirement.require_full_coverage)
        self.assertEqual(closing.require_final_bars, requirement.require_final_bars)
        self.assertEqual(closing.warmup.deadline_ms, requirement.warmup.deadline_ms)
        self.assertEqual(closing.warmup.max_cache_age_ms, requirement.warmup.max_cache_age_ms)

        limit_only = replace(requirement, warmup=None)
        with patch(
            "scripts.phase105_consumer_v2_identity_acceptance.sdk_requirement",
            return_value=limit_only,
        ), patch(
            "scripts.phase105_consumer_v2_identity_acceptance._c2_requirement",
            side_effect=lambda value: value,
        ):
            limit_only_closing = _closing_requirement(product)
        self.assertIsNone(limit_only_closing.warmup)
        self.assertEqual(limit_only_closing.warmup_limit, 2)

    def test_paced_client_factory_wraps_both_c2_transports(self) -> None:
        client = SimpleNamespace(query_transport=object(), stream_transport=object())
        with patch(
            "scripts.phase105_consumer_v2_identity_acceptance._client",
            return_value=client,
        ):
            result = _paced_client_factory(_C2ConsumerRequestPacer(8))(
                object(),
                base_url="https://query.example",
                grpc_target="stream.example:8210",
                cursor_path=Path("/tmp/cursor.json"),
                timeout_seconds=15.0,
            )
        self.assertIs(result, client)
        self.assertIsInstance(result.query_transport, _PacedQueryTransport)
        self.assertIsInstance(result.stream_transport, _PacedStreamTransport)

    def test_identity_prefixes_are_exactly_the_four_governed_paper_consumers(self) -> None:
        self.assertEqual(set(IDENTITY_PREFIXES), {
            "monitoring.multivenue.stable",
            "trading-system.paper.stable",
            "alpha.binance.paper.stable",
            "alpha.okx.paper.stable",
        })

    def test_authority_requires_fenced_rust_primary(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "authority.json"
            path.write_text(json.dumps({
                "schema": "qdl.authority-record.v1",
                "mode": "RUST_PRIMARY",
                "public_write_allowed": False,
                "revision": 1,
            }), encoding="utf-8")
            self.assertEqual(_authority(path)["mode"], "RUST_PRIMARY")
            path.write_text(json.dumps({
                "schema": "qdl.authority-record.v1",
                "mode": "RUST_SHADOW",
                "public_write_allowed": False,
            }), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "RUST_PRIMARY"):
                _authority(path)

    def test_cli_parser_resolves_dashed_alpha_identity_options(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            certificate = root / "client.crt"
            private_key = root / "client.key"
            jwt_private_key = root / "private.key"
            for path in (certificate, private_key, jwt_private_key):
                path.write_text("fixture", encoding="utf-8")
            arguments = [
                "--authority-record", str(root / "authority.json"),
                "--primary-url", "https://query.example",
                "--secondary-url", "https://query-two.example",
                "--grpc-target", "stream.example:8210",
                "--v1-base-url", "http://data_layer:8100",
                "--v1-provenance", str(root / "v1-provenance.json"),
                "--v1-runtime-binding", str(root / "v1-runtime-binding.json"),
                "--tls-ca-file", str(certificate),
            ]
            for prefix in IDENTITY_PREFIXES.values():
                arguments.extend((
                    f"--{prefix}-tls-certificate-file", str(certificate),
                    f"--{prefix}-tls-private-key-file", str(private_key),
                    f"--{prefix}-jwt-private-key-file", str(jwt_private_key),
                    f"--{prefix}-jwt-key-id", f"{prefix}-key",
                ))
            files = _identity_files(parser().parse_args(arguments))
        self.assertEqual(set(files), set(IDENTITY_PREFIXES))
        self.assertEqual(files["alpha.binance.paper.stable"].jwt_key_id, "alpha-binance-key")

    def test_cli_can_select_the_exact_three_consumer_five_liquid_scope(self) -> None:
        selected = (
            "trading-system.paper.stable",
            "alpha.binance.paper.stable",
            "alpha.okx.paper.stable",
        )
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            certificate = root / "client.crt"
            private_key = root / "client.key"
            jwt_private_key = root / "private.key"
            for path in (certificate, private_key, jwt_private_key):
                path.write_text("fixture", encoding="utf-8")
            arguments = [
                "--authority-record", str(root / "authority.json"),
                "--primary-url", "https://query.example",
                "--secondary-url", "https://query-two.example",
                "--grpc-target", "stream.example:8210",
                "--v1-base-url", "http://data_layer:8100",
                "--v1-provenance", str(root / "v1-provenance.json"),
                "--v1-runtime-binding", str(root / "v1-runtime-binding.json"),
                "--tls-ca-file", str(certificate),
            ]
            for consumer_id in selected:
                prefix = IDENTITY_PREFIXES[consumer_id]
                arguments.extend((
                    "--consumer-id", consumer_id,
                    f"--{prefix}-tls-certificate-file", str(certificate),
                    f"--{prefix}-tls-private-key-file", str(private_key),
                    f"--{prefix}-jwt-private-key-file", str(jwt_private_key),
                    f"--{prefix}-jwt-key-id", f"{prefix}-key",
                ))
            args = parser().parse_args(arguments)
            consumer_ids = _consumer_ids(args)
            files = _identity_files_for_consumers(args, consumer_ids)
        self.assertEqual(consumer_ids, selected)
        self.assertEqual(set(files), set(selected))

    def test_forced_v1_read_is_pinned_to_the_local_v1_service(self) -> None:
        self.assertEqual(_v1_base_url("http://data_layer:8100"), "http://data_layer:8100")
        for value in (
            "https://data_layer:8100",
            "http://data_layer:8101",
            "http://binance.example:8100",
            "http://data_layer:8100/v1/binance/price/BTCUSDT",
        ):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "exactly"):
                    _v1_base_url(value)

    def test_c2_requires_the_complete_stable_stream_pair(self) -> None:
        self.assertEqual(
            _c2_grpc_targets("qdl-v2-stream-a:8210,qdl-v2-stream-b:8210"),
            "qdl-v2-stream-a:8210,qdl-v2-stream-b:8210",
        )
        self.assertEqual(
            _c2_grpc_targets(" qdl-v2-stream-b:8210, qdl-v2-stream-a:8210 "),
            "qdl-v2-stream-b:8210,qdl-v2-stream-a:8210",
        )
        for value in (
            "qdl-v2-stream-a:8210",
            "qdl-v2-stream-a:8210,qdl-v2-stream-a:8210",
            "qdl-v2-stream-a:8210,other:8210",
            "qdl-v2-stream-a:8210,qdl-v2-stream-b:8210,other:8210",
        ):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "requires exactly"):
                    _c2_grpc_targets(value)

    def test_reference_batches_are_capped_without_throttling_product_reads(self) -> None:
        self.assertEqual(_reference_batch_concurrency(1), 1)
        self.assertEqual(_reference_batch_concurrency(4), 4)
        self.assertEqual(_reference_batch_concurrency(8), 4)
        with self.assertRaisesRegex(ValueError, "positive"):
            _reference_batch_concurrency(0)

    def test_reference_transport_uses_declared_deadline_not_generic_read_timeout(self) -> None:
        products = (
            SimpleNamespace(sdk_requirement=SimpleNamespace(deadline_ms=60_000)),
            SimpleNamespace(sdk_requirement=SimpleNamespace(deadline_ms=20_000)),
        )
        self.assertEqual(
            _reference_transport_timeout_seconds(
                products, generic_timeout_seconds=15.0
            ),
            75.0,
        )
        self.assertEqual(
            _reference_transport_timeout_seconds(
                (SimpleNamespace(sdk_requirement=SimpleNamespace(deadline_ms=5_000)),),
                generic_timeout_seconds=15.0,
            ),
            30.0,
        )
        with self.assertRaisesRegex(ValueError, "at least one"):
            _reference_transport_timeout_seconds((), generic_timeout_seconds=15.0)

    def test_route_summary_counts_declared_v1_and_blocked_without_claiming_transition(self) -> None:
        requirement = DataRequirement(
            instrument_uid="a953e16e-7138-5562-b5e8-c337a44d0b65",
            feed=FeedType.TRADE,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            recovery=RecoveryPolicy.SNAPSHOT_AND_REPLAY,
        )
        product = AcceptanceProduct(
            consumer_id="trading-system.paper.stable",
            consumer_subject="spiffe://qdl/paper/trading-system-stable",
            manifest_revision=1,
            manifest_sha256="a" * 64,
            instrument_uid=requirement.instrument_uid,
            instrument_id="BINANCE:USDM:BTCUSDT",
            venue="BINANCE",
            market="USDM",
            native_symbol="BTCUSDT",
            provider="BINANCE_DIRECT",
            feed=FeedType.TRADE,
            interval=None,
            source_policy_id="crypto_primary_v2",
            delivery="DURABLE",
            binding_id="binance-usdm-btcusdt-trade",
            requirement=requirement,
        )

        class Route:
            def __init__(self, fallback: str): self.fallback = fallback

        class Consumer:
            consumer_id = product.consumer_id
            products = (type("Product", (), {
                "requirement_key": f"{requirement.instrument_uid}:TRADE::crypto_primary_v2",
                "fallback": "V1",
            })(),)

        class Release:
            consumers = (Consumer(),)

        self.assertEqual(_route_summary(Release(), (product,)), {
            "v1_fallback_declared": 1,
            "blocked_fallback_declared": 0,
        })


class Phase105ConcurrentConsumerGroupTests(unittest.IsolatedAsyncioTestCase):
    async def test_opening_pacer_records_sdk_operation_categories(self) -> None:
        pacer = _C2ConsumerRequestPacer(
            180,
        )
        await pacer.acquire("QUERY_READ")
        await pacer.acquire("STREAM_SUBSCRIBE")
        self.assertEqual(pacer.evidence()["c2_operation_counts"], {
            "QUERY_READ": 1,
            "STREAM_SUBSCRIBE": 1,
        })

    def test_stable_scope_opening_budget_is_manifest_derived_and_bounded(self) -> None:
        root = Path(__file__).resolve().parents[1]
        catalog = StableSourceCatalog.load(
            root / "config/v2/stable-source-bindings.yaml"
        )
        acquisition = StableAcquisitionPlan.load(
            root / "config/v2/stable-acquisition-bindings.yaml", catalog=catalog
        )
        release = StableReleaseRoutePlan.load(
            root / "config/v2/stable-v2-release-routing.yaml", manifest_root=root
        )
        consumer_ids = (
            "monitoring.multivenue.stable",
            "trading-system.paper.stable",
            "alpha.binance.paper.stable",
            "alpha.okx.paper.stable",
        )
        scope = build_release_consumer_acceptance_scope(
            release,
            catalog=catalog,
            acquisition=acquisition,
            consumer_ids=consumer_ids,
        )
        probes = build_v1_fallback_probes(
            release,
            catalog=catalog,
            products=scope.products,
            consumer_ids=consumer_ids,
        )
        plan = _build_c2_opening_operation_plan(
            scope.products,
            release,
            probes,
            consumer_ids,
            generic_timeout_seconds=15.0,
            reference_now_ns=1_800_000_000_000_000_000,
        )
        selected_release_routes = {
            (consumer.consumer_id, product.requirement_key): product
            for consumer in release.consumers
            if consumer.consumer_id in consumer_ids
            for product in consumer.products
        }
        global_release_routes = {
            (consumer.consumer_id, product.requirement_key): product
            for consumer in release.consumers
            for product in consumer.products
        }
        expected_v2 = {
            identity for identity, product in selected_release_routes.items()
            if product.route == "V2_PRIMARY"
        }
        actual_v2 = {
            (product.consumer_id, requirement_key(product.requirement))
            for product in scope.products
        }
        self.assertEqual(actual_v2, expected_v2)
        self.assertEqual(plan["global_release_route_count"], len(global_release_routes))
        self.assertEqual(
            plan["global_v2_primary_product_count"],
            sum(product.route == "V2_PRIMARY" for product in global_release_routes.values()),
        )
        self.assertEqual(
            plan["global_v1_primary_route_count"],
            sum(product.route == "V1_PRIMARY" for product in global_release_routes.values()),
        )
        self.assertEqual(
            plan["selected_release_route_count"], len(selected_release_routes)
        )
        self.assertEqual(plan["selected_v2_primary_product_count"], len(actual_v2))
        self.assertEqual(
            plan["selected_v1_primary_excluded_count"],
            sum(product.route == "V1_PRIMARY" for product in selected_release_routes.values()),
        )
        self.assertEqual(
            len(scope.excluded), plan["selected_v1_primary_excluded_count"]
        )
        self.assertEqual(plan["global_v2_primary_product_count"], len(actual_v2))
        self.assertEqual(plan["product_count"], len(scope.products))
        self.assertEqual(
            {
                key: plan[key]
                for key in (
                    "global_release_route_count",
                    "global_v2_primary_product_count",
                    "global_v1_primary_route_count",
                    "selected_release_route_count",
                    "selected_v2_primary_product_count",
                    "selected_v1_primary_excluded_count",
                    "minimum_deadline_seconds",
                )
            },
            {
                "global_release_route_count": 303,
                "global_v2_primary_product_count": 299,
                "global_v1_primary_route_count": 4,
                "selected_release_route_count": 301,
                "selected_v2_primary_product_count": 299,
                "selected_v1_primary_excluded_count": 2,
                "minimum_deadline_seconds": 935.0,
            },
        )
        self.assertEqual(set(plan["consumers"]), set(consumer_ids))
        self.assertGreater(plan["total_operations"], len(scope.products))
        self.assertGreater(plan["minimum_deadline_seconds"], 0)
        for consumer_id, item in plan["consumers"].items():
            self.assertGreater(item["safe_requests_per_minute"], 0, consumer_id)
            self.assertGreater(item["max_streams"], 0, consumer_id)
            self.assertEqual(
                item["opening_total_operations"],
                sum(item["opening_operation_budget"].values()),
                consumer_id,
            )

    def test_operator_timeout_cannot_undercut_manifest_derived_floor(self) -> None:
        plan = {
            "minimum_deadline_seconds": 121.0,
            "consumers": {},
        }
        self.assertEqual(_effective_c2_opening_timeout_seconds(plan, None), 121.0)
        self.assertEqual(_effective_c2_opening_timeout_seconds(plan, 121.0), 121.0)
        with self.assertRaises(C2OpeningCapacityError) as raised:
            _effective_c2_opening_timeout_seconds(plan, 120.0)
        self.assertEqual(
            raised.exception.evidence["code"], "OPENING_TIMEOUT_BELOW_DERIVED_MINIMUM"
        )
        with self.assertRaises(C2OpeningCapacityError) as raised:
            _effective_c2_opening_timeout_seconds(plan, 0.0)
        self.assertEqual(raised.exception.evidence["code"], "OPENING_TIMEOUT_NOT_POSITIVE")

    def test_read_plane_preflight_receipt_requires_the_exact_release_scope(self) -> None:
        product = SimpleNamespace(
            consumer_id="alpha.binance.paper.stable",
            instrument_uid="uid-doge",
            feed=Feed.BAR,
            interval="12h",
            source_policy_id="crypto_primary_v2",
        )
        scope = SimpleNamespace(products=(product,), sha256="scope-sha")
        release = SimpleNamespace(digest="release-sha")
        observation = {
            "consumer_id": product.consumer_id,
            "instrument_uid": product.instrument_uid,
            "feed": "BAR",
            "interval": product.interval,
            "source_policy_id": product.source_policy_id,
            "primary_latency_ms": 11.0,
            "secondary_latency_ms": 13.0,
            "timing_policy": {"semantic_class": "FINAL_SCHEDULED"},
        }
        receipt = _read_plane_preflight_receipt(
            scope=scope,
            release=release,
            consumer_ids=(product.consumer_id,),
            observations=[observation],
            authority_revision=12,
            elapsed_seconds=0.25,
            quota_window_wait_seconds=0.0,
            pacers={product.consumer_id: _C2ConsumerRequestPacer(180)},
        )
        self.assertEqual(receipt["status"], "PASS_READ_PLANE_PREFLIGHT")
        self.assertEqual(receipt["product_count"], 1)
        self.assertEqual(receipt["feed_counts"], {"BAR": 1})
        self.assertEqual(receipt["timing_class_counts"], {"FINAL_SCHEDULED": 1})
        self.assertEqual(receipt["replica_read_count"], 2)
        with self.assertRaisesRegex(AssertionError, "scope differs"):
            _read_plane_preflight_receipt(
                scope=scope,
                release=release,
                consumer_ids=(product.consumer_id,),
                observations=[],
                authority_revision=12,
                elapsed_seconds=0.25,
                quota_window_wait_seconds=0.0,
                pacers={product.consumer_id: _C2ConsumerRequestPacer(180)},
            )

    async def test_c2_pacer_aligns_then_spaces_requests_below_manifest_quota(self) -> None:
        clock = {"value": 100.0}
        sleeps: list[float] = []

        async def fake_sleep(duration: float) -> None:
            sleeps.append(duration)
            clock["value"] += duration

        pacer = _C2ConsumerRequestPacer(
            4,
            safety_fraction=0.5,
            clock=lambda: clock["value"],
            sleep=fake_sleep,
        )
        self.assertAlmostEqual(await pacer.wait_for_clean_window(), 20.05)
        await pacer.acquire()
        await pacer.acquire()
        await pacer.acquire()
        self.assertEqual(len(sleeps), 3)
        for actual, expected in zip(sleeps, (20.05, 30.0, 30.0), strict=True):
            self.assertAlmostEqual(actual, expected)
        self.assertEqual(pacer.evidence()["c2_safe_requests_per_minute"], 2)
        self.assertEqual(pacer.evidence()["c2_request_count"], 3)

    async def test_stream_open_and_rest_read_share_one_identity_budget(self) -> None:
        clock = {"value": 0.0}
        sleeps: list[float] = []

        async def fake_sleep(duration: float) -> None:
            sleeps.append(duration)
            clock["value"] += duration

        class QueryDelegate:
            def __init__(self) -> None:
                self.calls = 0

            async def snapshot(self, *args, **kwargs):
                del args, kwargs
                self.calls += 1
                return {"snapshot": True}

            async def close(self) -> None:
                return None

        class StreamDelegate:
            def __init__(self) -> None:
                self.calls = 0

            async def subscribe(self, *args, **kwargs):
                del args, kwargs
                self.calls += 1
                yield "frame"

            async def close(self) -> None:
                return None

        pacer = _C2ConsumerRequestPacer(
            4,
            safety_fraction=0.5,
            clock=lambda: clock["value"],
            sleep=fake_sleep,
        )
        query_delegate = QueryDelegate()
        stream_delegate = StreamDelegate()
        query = _PacedQueryTransport(query_delegate, pacer)
        stream = _PacedStreamTransport(stream_delegate, pacer)

        self.assertEqual(await query.snapshot(object()), {"snapshot": True})
        self.assertEqual(await anext(stream.subscribe(object())), "frame")
        self.assertEqual(query_delegate.calls, 1)
        self.assertEqual(stream_delegate.calls, 1)
        self.assertEqual(sleeps, [30.0])
        self.assertEqual(pacer.evidence()["c2_request_count"], 2)

    async def test_stream_open_failure_remains_fail_closed(self) -> None:
        class StreamDelegate:
            async def subscribe(self, *args, **kwargs):
                del args, kwargs
                raise RuntimeError("stream transport rejected subscription")
                yield None

            async def close(self) -> None:
                return None

        pacer = _C2ConsumerRequestPacer(8)
        stream = _PacedStreamTransport(StreamDelegate(), pacer)
        with self.assertRaisesRegex(RuntimeError, "rejected subscription"):
            await anext(stream.subscribe(object()))
        self.assertEqual(pacer.evidence()["c2_request_count"], 1)

    async def test_distinct_identity_stream_pacers_do_not_share_a_lock(self) -> None:
        opened: list[str] = []

        class StreamDelegate:
            def __init__(self, label: str) -> None:
                self.label = label

            async def subscribe(self, *args, **kwargs):
                del args, kwargs
                opened.append(self.label)
                yield self.label

            async def close(self) -> None:
                return None

        first = _PacedStreamTransport(StreamDelegate("first"), _C2ConsumerRequestPacer(8))
        second = _PacedStreamTransport(StreamDelegate("second"), _C2ConsumerRequestPacer(8))
        values = await asyncio.gather(
            anext(first.subscribe(object())),
            anext(second.subscribe(object())),
        )
        self.assertEqual(set(values), {"first", "second"})
        self.assertEqual(set(opened), {"first", "second"})

    async def test_closing_batch_revalidates_every_product_on_both_replicas(self) -> None:
        class Product:
            def __init__(self, name: str) -> None:
                self.consumer_id = "alpha.binance.paper.stable"
                self.instrument_uid = f"uid-{name}"
                self.instrument_id = f"BINANCE.USDM.PERPETUAL.{name}-USDT"
                self.feed = SimpleNamespace(value="TRADE")
                self.interval = None
                self.source_policy_id = "crypto_primary_v2"
                self.delivery = DeliveryClass.DURABLE
                self.requirement = object()
                self.identity = (
                    self.consumer_id, self.instrument_uid, "TRADE", "",
                    self.source_policy_id,
                )

            def evidence(self) -> dict[str, object]:
                return {
                    "consumer_id": self.consumer_id,
                    "instrument_uid": self.instrument_uid,
                    "feed": "TRADE",
                    "interval": None,
                    "source_policy_id": self.source_policy_id,
                }

        products = (Product("BTC"), Product("ETH"))
        clients = []

        class Client:
            def __init__(self, label: str) -> None:
                self.label = label
                self.calls: list[tuple[object, ...]] = []

            async def warmup_batch(self, requirements, *, require_all: bool):
                if not require_all:
                    raise AssertionError("execution closing batch must require all items")
                self.calls.append(tuple(requirements))
                return SimpleNamespace(
                    partial=False,
                    results=[
                        SimpleNamespace(data=SimpleNamespace(data=[SimpleNamespace()]))
                        for _ in requirements
                    ],
                )

            async def close(self) -> None:
                return None

        def factory(identity, *, base_url, grpc_target, cursor_path, timeout_seconds):
            del identity, grpc_target, cursor_path, timeout_seconds
            client = Client(base_url)
            clients.append(client)
            return client

        with patch(
            "scripts.phase105_consumer_v2_identity_acceptance._closing_requirement",
            side_effect=lambda product: product.requirement,
        ), patch(
            "scripts.phase105_consumer_v2_identity_acceptance.validate_product_view",
        ), patch(
            "scripts.phase105_consumer_v2_identity_acceptance.validate_replica_views",
            return_value=("a" * 64, "b" * 64),
        ), patch(
            "scripts.phase105_consumer_v2_identity_acceptance.compact_view_quality",
            return_value={"state": "LIVE"},
        ), patch(
            "scripts.phase105_consumer_v2_identity_acceptance._timing_policy",
            return_value={"semantic_class": "QUIET_SESSION"},
        ):
            evidence = await _closing_batch_revalidation(
                products,
                identity=object(),
                primary_url="https://primary",
                secondary_url="https://secondary",
                grpc_target="stream:8210",
                state_dir=Path("/tmp/phase105-closing"),
                timeout_seconds=15.0,
                max_batch_items=2,
                client_factory=factory,
            )
        self.assertEqual(len(evidence), 2)
        self.assertEqual({item["closing_read"] for item in evidence}, {"BATCH_V2_PRIMARY"})
        self.assertEqual(len(clients), 2)
        self.assertEqual([len(call) for client in clients for call in client.calls], [2, 2])

    async def test_partial_batch_bisects_only_failing_leaf_for_typed_diagnostic(self) -> None:
        class Product:
            def __init__(self, name: str) -> None:
                self.requirement = name
                self.identity = ("alpha.binance.paper.stable", f"uid-{name}", "TRADE", "", "crypto")

            def evidence(self) -> dict[str, object]:
                return {"instrument_uid": self.identity[1], "feed": "TRADE"}

        good, bad = Product("good"), Product("bad")

        class Client:
            def __init__(self) -> None:
                self.batch_calls: list[tuple[str, ...]] = []
                self.warmup_calls: list[str] = []

            async def warmup_batch(self, requirements, *, require_all: bool):
                self.assert_true(require_all)
                values = tuple(requirements)
                self.batch_calls.append(values)
                if bad.requirement in values:
                    raise DataLayerError("PARTIAL_RESULT", "one item failed", retryable=True)
                return SimpleNamespace(partial=False, results=[])

            async def warmup(self, requirement):
                self.warmup_calls.append(requirement)
                if requirement == bad.requirement:
                    raise DataLayerError("DATA_STALE", "only the bad leaf is stale", retryable=True)
                raise AssertionError("successful leaf must not receive an individual diagnostic read")

            @staticmethod
            def assert_true(value):
                if not value:
                    raise AssertionError("strict batch expected")

        client = Client()
        observations = [
            {"product_identity": list(good.identity), "quality_sha256": "a" * 64},
            {"product_identity": list(bad.identity), "quality_sha256": "b" * 64},
        ]
        with patch(
            "scripts.phase105_consumer_v2_identity_acceptance._closing_requirement",
            side_effect=lambda product: product.requirement,
        ):
            evidence = await _closing_batch_problem_evidence(
                client,
                (good, bad),
                error=DataLayerError("PARTIAL_RESULT", "strict root failure", retryable=True),
                status_observations=observations,
            )
        self.assertEqual(client.batch_calls, [(good.requirement,), (bad.requirement,)])
        self.assertEqual(client.warmup_calls, [bad.requirement])
        self.assertEqual([item["instrument_uid"] for item in evidence], ["uid-bad"])
        self.assertEqual(evidence[0]["problem_code"], "DATA_STALE")
        self.assertEqual(evidence[0]["quality_sha256"], "b" * 64)

    async def test_closing_batch_partial_retains_typed_item_problem(self) -> None:
        product = SimpleNamespace(
            consumer_id="alpha.binance.paper.stable",
            instrument_uid="uid-doge",
            instrument_id="BINANCE.USDM.PERPETUAL.DOGE-USDT",
            feed=Feed.BAR,
            interval="12h",
            source_policy_id="crypto_primary_v2",
            delivery=DeliveryClass.DURABLE,
            requirement=object(),
            identity=("alpha.binance.paper.stable", "uid-doge", "BAR", "12h", "crypto_primary_v2"),
            evidence=lambda: {
                "consumer_id": "alpha.binance.paper.stable",
                "instrument_uid": "uid-doge",
                "feed": "BAR",
                "interval": "12h",
                "source_policy_id": "crypto_primary_v2",
            },
        )
        status = FeedStatusResponse.model_validate({
            "schema": "qdl.feed-status.v2",
            "instrument_uid": "uid-doge",
            "feed": "BAR",
            "quality": {
                "state": "LIVE",
                "freshness_ms": 1_500,
                "event_recency_state": "LIVE",
                "provider_session_state": "NOT_APPLICABLE",
                "provider_session_liveness_ms": None,
                "gap_open": False,
                "complete": True,
                "execution_eligible": True,
                "policy_id": "crypto_primary_v2",
                "flags": [],
            },
        })

        class Client:
            async def warmup_batch(self, requirements, *, require_all: bool):
                self.assertTrue(require_all)
                self.assertEqual(tuple(requirements), (product.requirement,))
                raise DataLayerError(
                    "PARTIAL_RESULT",
                    "required warmup batch contains one or more explicit failures",
                    retryable=True,
                )

            async def warmup(self, requirement):
                self.assertIs(requirement, product.requirement)
                raise DataLayerError(
                    "DATA_NOT_READY",
                    "cache is intentionally omitted from evidence",
                    retryable=True,
                )

            async def feed_status(self, requirement):
                self.assertIs(requirement, product.requirement)
                return status

            async def close(self) -> None:
                return None

            def assertFalse(self, value):
                if value:
                    raise AssertionError("expected false")

            def assertTrue(self, value):
                if not value:
                    raise AssertionError("expected true")

            def assertEqual(self, actual, expected):
                if actual != expected:
                    raise AssertionError(f"{actual!r} != {expected!r}")

            def assertIs(self, actual, expected):
                if actual is not expected:
                    raise AssertionError("objects differ")

        def factory(*args, **kwargs):
            del args, kwargs
            return Client()

        with patch(
            "scripts.phase105_consumer_v2_identity_acceptance._closing_requirement",
            return_value=product.requirement,
        ):
            with self.assertRaises(C2ClosingBatchError) as raised:
                await _closing_batch_revalidation(
                    (product,),
                    identity=object(),
                    primary_url="https://primary",
                    secondary_url="https://secondary",
                    grpc_target="stream:8210",
                    state_dir=Path("/tmp/phase105-closing"),
                    timeout_seconds=15.0,
                    max_batch_items=50,
                    client_factory=factory,
                )
        evidence = raised.exception.evidence
        self.assertEqual(evidence["transport_error"], "DataLayerError")
        self.assertEqual(evidence["transport_error_code"], "PARTIAL_RESULT")
        self.assertEqual(evidence["batch_item_problems"][0]["problem_code"], "DATA_NOT_READY")
        self.assertTrue(evidence["batch_item_problems"][0]["retryable"])
        self.assertIn("problem_detail_sha256", evidence["batch_item_problems"][0])
        self.assertIsInstance(evidence["batch_item_problems"][0]["quality_sha256"], str)
        self.assertNotIn("cache is intentionally", repr(evidence))
        self.assertFalse(evidence["payload_recorded"])

    async def test_closing_batch_rejects_partial_cardinality(self) -> None:
        product = SimpleNamespace(
            consumer_id="alpha.binance.paper.stable",
            instrument_uid="uid-btc",
            instrument_id="BINANCE.USDM.PERPETUAL.BTC-USDT",
            feed=SimpleNamespace(value="TRADE"),
            interval=None,
            source_policy_id="crypto_primary_v2",
            delivery=DeliveryClass.DURABLE,
            requirement=object(),
            identity=("alpha.binance.paper.stable", "uid-btc", "TRADE", "", "crypto_primary_v2"),
            evidence=lambda: {
                "consumer_id": "alpha.binance.paper.stable",
                "instrument_uid": "uid-btc",
                "feed": "TRADE",
                "interval": None,
                "source_policy_id": "crypto_primary_v2",
            },
        )

        class Client:
            async def warmup_batch(self, requirements, *, require_all: bool):
                del requirements, require_all
                return SimpleNamespace(partial=False, results=[])

            async def close(self) -> None:
                return None

        def factory(*args, **kwargs):
            del args, kwargs
            return Client()

        with patch(
            "scripts.phase105_consumer_v2_identity_acceptance._closing_requirement",
            return_value=product.requirement,
        ):
            with self.assertRaisesRegex(AssertionError, "cardinality"):
                await _closing_batch_revalidation(
                    (product,),
                    identity=object(),
                    primary_url="https://primary",
                    secondary_url="https://secondary",
                    grpc_target="stream:8210",
                    state_dir=Path("/tmp/phase105-closing"),
                    timeout_seconds=15.0,
                    max_batch_items=50,
                    client_factory=factory,
                )

    async def test_closing_batch_timeout_has_bounded_typed_status_evidence(self) -> None:
        product = SimpleNamespace(
            consumer_id="alpha.binance.paper.stable",
            instrument_uid="uid-btc",
            instrument_id="BINANCE.USDM.PERPETUAL.BTC-USDT",
            feed=Feed.TRADE,
            interval=None,
            source_policy_id="crypto_primary_v2",
            delivery=DeliveryClass.DURABLE,
            requirement=object(),
            identity=("alpha.binance.paper.stable", "uid-btc", "TRADE", "", "crypto_primary_v2"),
            evidence=lambda: {
                "consumer_id": "alpha.binance.paper.stable",
                "instrument_uid": "uid-btc",
                "feed": "TRADE",
                "interval": None,
                "source_policy_id": "crypto_primary_v2",
            },
        )
        status = FeedStatusResponse.model_validate({
            "schema": "qdl.feed-status.v2",
            "instrument_uid": "uid-btc",
            "feed": "TRADE",
            "quality": {
                "state": "LIVE",
                "freshness_ms": 7,
                "event_recency_state": "LIVE",
                "provider_session_state": "LIVE",
                "provider_session_liveness_ms": 3,
                "gap_open": False,
                "complete": True,
                "execution_eligible": True,
                "policy_id": "crypto_primary_v2",
                "flags": [],
            },
        })

        class Client:
            async def warmup_batch(self, requirements, *, require_all: bool):
                del requirements, require_all
                raise httpx.ReadTimeout("closing request timed out")

            async def feed_status(self, requirement):
                del requirement
                return status

            async def close(self) -> None:
                return None

        def factory(*args, **kwargs):
            del args, kwargs
            return Client()

        with patch(
            "scripts.phase105_consumer_v2_identity_acceptance._closing_requirement",
            return_value=product.requirement,
        ):
            with self.assertRaises(C2ClosingBatchError) as raised:
                await _closing_batch_revalidation(
                    (product,),
                    identity=object(),
                    primary_url="https://primary",
                    secondary_url="https://secondary",
                    grpc_target="stream:8210",
                    state_dir=Path("/tmp/phase105-closing"),
                    timeout_seconds=15.0,
                    max_batch_items=50,
                    client_factory=factory,
                )
        evidence = raised.exception.evidence
        self.assertEqual(evidence["transport_error"], "ReadTimeout")
        self.assertEqual(evidence["batch_size"], 1)
        self.assertEqual(evidence["typed_status"][0]["quality"]["quality"]["state"], "LIVE")
        self.assertFalse(evidence["payload_recorded"])
        self.assertNotIn("price", repr(evidence))

    async def test_observation_waits_until_the_declared_floor(self) -> None:
        clock = {"value": 100.0}

        async def fake_sleep(duration: float) -> None:
            clock["value"] += duration

        with patch(
            "scripts.phase105_consumer_v2_identity_acceptance.time.monotonic",
            side_effect=lambda: clock["value"],
        ), patch(
            "scripts.phase105_consumer_v2_identity_acceptance.asyncio.sleep",
            side_effect=fake_sleep,
        ):
            elapsed = await _wait_for_minimum_observation(
                started_monotonic=100.0,
                observation_seconds=300.0,
            )
        self.assertEqual(elapsed, 300.0)

    async def test_groups_start_in_declared_order_and_collect_in_that_order(self) -> None:
        started: list[str] = []
        release = asyncio.Event()

        async def run_group(consumer_id: str):
            started.append(consumer_id)
            await release.wait()
            return ([{"consumer": consumer_id}], [{"fallback": consumer_id}])

        task = asyncio.create_task(
            _run_consumer_groups(("monitoring", "trading", "alpha"), run_group)
        )
        for _ in range(20):
            if len(started) == 3:
                break
            await asyncio.sleep(0)
        self.assertEqual(started, ["monitoring", "trading", "alpha"])
        release.set()
        self.assertEqual(
            await task,
            (
                ([{"consumer": "monitoring"}], [{"fallback": "monitoring"}]),
                ([{"consumer": "trading"}], [{"fallback": "trading"}]),
                ([{"consumer": "alpha"}], [{"fallback": "alpha"}]),
            ),
        )

    async def test_group_failure_cancels_and_drains_siblings_before_cleanup(self) -> None:
        sibling_started = asyncio.Event()
        sibling_drained = asyncio.Event()

        async def run_group(consumer_id: str):
            if consumer_id == "failing":
                await sibling_started.wait()
                raise RuntimeError("expected C2 failure")
            try:
                sibling_started.set()
                await asyncio.Event().wait()
            finally:
                sibling_drained.set()

        with self.assertRaisesRegex(RuntimeError, "expected C2 failure"):
            await _run_consumer_groups(("failing", "sibling"), run_group)
        self.assertTrue(sibling_drained.is_set())


if __name__ == "__main__":
    unittest.main()
