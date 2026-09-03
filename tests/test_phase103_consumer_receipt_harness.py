from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import tempfile
import time
import unittest
from unittest.mock import ANY, call, patch

from scripts.phase103_consumer_receipt_acceptance import (
    _c2_requirement,
    _cursor_directory,
    _historical_bar_replay_requirement,
    _replay_precedes_handoff,
    _strict_snapshot_for_c2,
    _strict_warmup_then_stream_for_c2,
    _stream_handoff_mode,
    _stream_resume,
    _uses_historical_bar_replay,
    _validated_packet,
    parser,
)
from scripts.phase103_prepare_shared_primary_packet import (
    prepare_shared_primary_packet,
    validate_prepared_shared_primary_bundle as generator_bundle_validator,
)
from scripts.phase103_apply_shared_primary_broker_scope import (
    validate_prepared_shared_primary_bundle as broker_bundle_validator,
)
from scripts.phase103_packet_contract import (
    SHARED_REALTIME_CORE_GROUP_ID,
    SHARED_REALTIME_CORE_ID_PREFIX,
    validate_prepared_shared_primary_bundle as contract_bundle_validator,
)
from qdl.runtime.stable_deployment import (
    SHARED_REALTIME_CORE_GROUP_ID as DEPLOYMENT_CORE_GROUP_ID,
    SHARED_REALTIME_CORE_ID_PREFIX as DEPLOYMENT_CORE_ID_PREFIX,
)
from qdl.certification.phase103_consumer_acceptance import DeliveryClass
from qdl_sdk import (
    ControlEvent,
    DataRequirement,
    Feed,
    FeedStatusResponse,
    Grade,
    StalePolicy,
    StreamEvent,
)
from qdl_sdk.models import SnapshotResponse
from qdl_sdk.errors import ContinuityError, DataLayerError
from qdl.adapters.intervals import canonical_interval_ms, provider_bar_calendar_anchor_ms
from qdl.runtime.stable_bar_edge import durable_bar_history_capacity_rows


ROOT = Path(__file__).resolve().parents[1]


class Phase103ConsumerReceiptHarnessTests(unittest.TestCase):
    def test_reconnect_replay_before_new_handoff_is_state_only(self):
        self.assertTrue(_replay_precedes_handoff(logical_offset=40, watermark_offset=40))
        self.assertTrue(_replay_precedes_handoff(logical_offset=39, watermark_offset=40))
        self.assertFalse(_replay_precedes_handoff(logical_offset=41, watermark_offset=40))
        with self.assertRaisesRegex(ValueError, "non-negative"):
            _replay_precedes_handoff(logical_offset=-1, watermark_offset=40)

    def test_c2_bounds_bar_history_without_reducing_the_public_request(self):
        requirement = DataRequirement(
            instrument_uid="bar-uid",
            feed=Feed.BAR,
            consumer_grade=Grade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval="12h",
            warmup_limit=10_000,
            max_freshness_ms=86_400_000,
        )
        bounded = _c2_requirement(requirement)
        self.assertEqual(requirement.warmup_limit, 10_000)
        self.assertEqual(bounded.warmup_limit, 700)

    def test_c2_uses_the_shared_durable_capacity_for_calendar_bars(self):
        requirement = DataRequirement(
            instrument_uid="bar-uid",
            feed=Feed.BAR,
            consumer_grade=Grade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval="1w",
            warmup_limit=10_000,
            max_freshness_ms=604_800_000,
        )
        bounded = _c2_requirement(requirement)
        self.assertEqual(durable_bar_history_capacity_rows("1w"), 156)
        self.assertEqual(bounded.warmup_limit, 156)

    def test_c2_keeps_non_bar_requirement_exact(self):
        requirement = DataRequirement(
            instrument_uid="trade-uid",
            feed=Feed.TRADE,
            consumer_grade=Grade.ALPHA,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=3_000,
        )
        self.assertIs(_c2_requirement(requirement), requirement)

    def test_parser_requires_the_sealed_handoff_coordinates(self):
        parsed = parser().parse_args(
            [
                "--primary-url", "https://query-a",
                "--secondary-url", "https://query-b",
                "--grpc-target", "stream-a:8210,stream-b:8210",
                "--handoff-packet", "/tmp/packet.json",
                "--runtime-dir", "/tmp/runtime",
                "--tls-ca-file", "/tmp/ca.crt",
                "--trading-tls-certificate-file", "/tmp/trading.crt",
                "--trading-tls-private-key-file", "/tmp/trading.key",
                "--trading-jwt-private-key-file", "/tmp/trading-jwt.key",
                "--trading-jwt-key-id", "trading-key",
                "--alpha-tls-certificate-file", "/tmp/alpha.crt",
                "--alpha-tls-private-key-file", "/tmp/alpha.key",
                "--alpha-jwt-private-key-file", "/tmp/alpha-jwt.key",
                "--alpha-jwt-key-id", "alpha-key",
            ]
        )
        self.assertEqual(parsed.handoff_packet, Path("/tmp/packet.json"))
        self.assertEqual(parsed.runtime_dir, Path("/tmp/runtime"))

    def test_cursor_directory_is_new_private_and_removed(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-receipt-") as directory:
            path = Path(directory) / "cursor-state"
            with _cursor_directory(str(path)) as state:
                self.assertTrue(state.is_dir())
                state.joinpath("cursor.json").write_text("sensitive", encoding="utf-8")
            self.assertFalse(path.exists())
            path.mkdir()
            with self.assertRaisesRegex(ValueError, "must not already exist"):
                with _cursor_directory(str(path)):
                    pass

    def test_harness_rejects_an_expired_or_tampered_packet_before_sdk_io(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-receipt-") as directory:
            root = Path(directory)
            packet = prepare_shared_primary_packet(
                output_dir=root / "packet",
                host_runtime_dir=root / "packet" / "runtime",
                rust_image_digest="sha256:" + "b" * 64,
                python_image_digest="sha256:" + "c" * 64,
                source_commit="0123456789abcdef",
                actor="BobbyAxerol",
                change_ticket="QDL-PHASE103-HARNESS-TEST",
                observation_seconds=300,
                issued_at_ns=time.time_ns(),
            )
            packet_path = root / "packet" / "shared-primary-handoff-packet.json"
            validated = _validated_packet(packet_path, root / "packet" / "runtime")
            self.assertEqual(validated["packet_sha256"], packet["packet_sha256"])
            packet_path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "packet"):
                _validated_packet(packet_path, root / "packet" / "runtime")

    def test_runbook_preserves_the_sealed_host_runtime_path_inside_probe(self):
        runbook = (
            ROOT / "docs/runbooks/phase103-shared-rust-primary-handoff.md"
        ).read_text(encoding="utf-8")
        self.assertIn('-v "$QDL_PACKET_DIR:$QDL_PACKET_DIR:ro"', runbook)
        self.assertIn(
            '--handoff-packet "$QDL_PACKET_DIR/shared-primary-handoff-packet.json"',
            runbook,
        )
        self.assertIn('--runtime-dir "$QDL_RUNTIME_DIR"', runbook)
        self.assertNotIn("--handoff-packet /packet/", runbook)

    def test_packet_contract_identity_matches_runtime_deployment(self):
        self.assertEqual(SHARED_REALTIME_CORE_GROUP_ID, DEPLOYMENT_CORE_GROUP_ID)
        self.assertEqual(SHARED_REALTIME_CORE_ID_PREFIX, DEPLOYMENT_CORE_ID_PREFIX)

    def test_packet_validation_has_one_contract_source_of_truth(self):
        self.assertIs(generator_bundle_validator, contract_bundle_validator)
        self.assertIs(broker_bundle_validator, contract_bundle_validator)

    def test_runbook_uses_immutable_image_for_packet_preflight(self):
        runbook = (
            ROOT / "docs/runbooks/phase103-shared-rust-primary-handoff.md"
        ).read_text(encoding="utf-8")
        self.assertIn('export QDL_STABLE_PYTHON_IMAGE_REF=', runbook)
        self.assertIn('docker run --rm --read-only --network none', runbook)
        self.assertIn('-v "$QDL_PACKET_DIR:$QDL_PACKET_DIR"', runbook)
        for script in (
            "phase103_prepare_shared_primary_packet.py",
            "phase103_validate_shared_primary_packet.py",
        ):
            self.assertIn(f"python -B scripts/{script}", runbook)
            self.assertNotIn(f"python3 -B scripts/{script}", runbook)
        self.assertIn(
            "python3 -B scripts/phase103_apply_shared_primary_broker_scope.py",
            runbook,
        )
        self.assertIn(
            "  python -B scripts/phase103_consumer_receipt_acceptance.py",
            runbook,
        )
        self.assertIn("trading_system_handoff.route_lock", runbook)
        self.assertIn("QDL_TRADING_SYSTEM_SOURCE_ROOT", runbook)
        self.assertIn("docker run --rm --entrypoint sha256sum", runbook)


class Phase103HistoricalBarReplayTests(unittest.TestCase):
    INTERVAL_NS = 15 * 60 * 1_000_000_000

    def _requirement(
        self,
        *,
        feed: Feed = Feed.BAR,
        grade: Grade = Grade.ALPHA,
        interval: str | None = "15m",
        warmup_limit: int = 500,
    ) -> DataRequirement:
        return DataRequirement(
            instrument_uid="a953e16e-7138-5562-b5e8-c337a44d0b65",
            feed=feed,
            consumer_grade=grade,
            source_policy_id="crypto_primary_v2",
            interval=interval,
            warmup_limit=warmup_limit,
            max_freshness_ms=1_080_000 if feed is Feed.BAR else 20_000,
            stale_policy=StalePolicy.BLOCK,
        )

    def _product(
        self,
        *,
        feed: Feed = Feed.BAR,
        grade: Grade = Grade.ALPHA,
    ):
        return SimpleNamespace(
            delivery=DeliveryClass.DURABLE,
            feed=feed,
            requirement=SimpleNamespace(consumer_grade=grade),
            identity=("alpha.binance.paper.stable", "instrument", feed.value, "15m", "policy"),
        )

    def test_historical_seed_is_one_aligned_prior_bar_and_preserves_governed_policy(self):
        original = self._requirement()
        latest_open_ns = 20 * self.INTERVAL_NS
        seed = _historical_bar_replay_requirement(
            original,
            latest_open_time_ns=latest_open_ns,
        )

        self.assertEqual(seed.instrument_uid, original.instrument_uid)
        self.assertIs(seed.feed, original.feed)
        self.assertIs(seed.consumer_grade, original.consumer_grade)
        self.assertEqual(seed.interval, original.interval)
        self.assertEqual(seed.source_policy_id, original.source_policy_id)
        self.assertIs(seed.recovery, original.recovery)
        self.assertIs(seed.gap_policy, original.gap_policy)
        self.assertIs(seed.bar_revision_policy, original.bar_revision_policy)
        self.assertEqual(seed.require_final_bars, original.require_final_bars)
        self.assertEqual(seed.require_full_coverage, original.require_full_coverage)
        self.assertEqual(seed.warmup_limit, 0)
        self.assertIs(seed.stale_policy, StalePolicy.OBSERVE)
        self.assertIsNone(seed.max_freshness_ms)
        self.assertEqual(seed.warmup.time_range.start_time_ns, 18 * self.INTERVAL_NS)
        self.assertEqual(seed.warmup.time_range.end_time_ns, 19 * self.INTERVAL_NS)
        self.assertEqual(
            seed.warmup.interval_source_policy,
            original.warmup_specification.interval_source_policy,
        )
        self.assertEqual(seed.warmup.max_cache_age_ms, original.warmup_specification.max_cache_age_ms)
        self.assertEqual(seed.warmup.deadline_ms, original.warmup_specification.deadline_ms)
        self.assertEqual(original.warmup_limit, 500)
        self.assertIs(original.stale_policy, StalePolicy.BLOCK)

    def test_historical_seed_fails_closed_for_execution_unaligned_or_unbounded_requirements(self):
        with self.assertRaisesRegex(ValueError, "execution BAR"):
            _historical_bar_replay_requirement(
                self._requirement(grade=Grade.EXECUTION),
                latest_open_time_ns=20 * self.INTERVAL_NS,
            )
        with self.assertRaisesRegex(ValueError, "cannot form"):
            _historical_bar_replay_requirement(
                self._requirement(),
                latest_open_time_ns=20 * self.INTERVAL_NS + 1,
            )
        with self.assertRaisesRegex(ValueError, "governed warmup"):
            _historical_bar_replay_requirement(
                self._requirement(warmup_limit=0),
                latest_open_time_ns=20 * self.INTERVAL_NS,
            )

    def test_historical_seed_uses_shared_provider_calendar_anchors(self):
        for interval, venue in (
            ("1w", "BINANCE"),
            ("1w", "OKX"),
            ("3d", "BINANCE"),
            ("3d", "OKX"),
        ):
            interval_ns = canonical_interval_ms(interval) * 1_000_000
            anchor_ns = provider_bar_calendar_anchor_ms(
                interval,
                provider=venue,
            ) * 1_000_000
            latest_open_ns = anchor_ns + 20 * interval_ns
            seed = _historical_bar_replay_requirement(
                self._requirement(interval=interval),
                latest_open_time_ns=latest_open_ns,
                calendar_provider=venue,
            )

            self.assertEqual(seed.warmup.time_range.start_time_ns, latest_open_ns - 2 * interval_ns)
            self.assertEqual(seed.warmup.time_range.end_time_ns, latest_open_ns - interval_ns)
    def test_historical_replay_applies_only_to_non_execution_durable_bars(self):
        self.assertTrue(_uses_historical_bar_replay(self._product()))
        self.assertFalse(
            _uses_historical_bar_replay(self._product(grade=Grade.EXECUTION))
        )
        self.assertFalse(_uses_historical_bar_replay(self._product(feed=Feed.TRADE)))
        self.assertFalse(_uses_historical_bar_replay(self._product(feed=Feed.QUOTE)))


class Phase103HistoricalBarReplayResumeTests(unittest.IsolatedAsyncioTestCase):
    INTERVAL_NS = 15 * 60 * 1_000_000_000

    class _Session:
        def __init__(self, *, warmup, items, quiet: bool = False):
            self.warmup = warmup
            self._items = iter(items)
            self._quiet = quiet
            self.acknowledged = []

        async def __anext__(self):
            await asyncio.sleep(0)
            try:
                return next(self._items)
            except StopIteration as error:
                if self._quiet:
                    await asyncio.Event().wait()
                raise StopAsyncIteration from error

        def acknowledge(self, event):
            self.acknowledged.append(event)

    class _SessionContext:
        def __init__(self, session):
            self.session = session

        async def __aenter__(self):
            return self.session

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class _Client:
        def __init__(self, *, strict_warmup, session, snapshots=()):
            self.strict_warmup = strict_warmup
            self.session = session
            self._snapshots = iter(snapshots)
            self.warmup_calls = []
            self.stream_calls = []
            self.snapshot_calls = []
            self.closed = False

        async def warmup(self, requirement):
            self.warmup_calls.append(requirement)
            return self.strict_warmup

        def warmup_then_stream(self, requirement, *, resume_restored_state=False):
            self.stream_calls.append((requirement, resume_restored_state))
            return Phase103HistoricalBarReplayResumeTests._SessionContext(self.session)

        async def snapshot(self, requirement):
            self.snapshot_calls.append(requirement)
            try:
                return next(self._snapshots)
            except StopIteration as error:
                raise AssertionError("unexpected strict historical BAR snapshot") from error

        async def close(self):
            self.closed = True

    @staticmethod
    def _product(*, grade: Grade = Grade.ALPHA):
        return SimpleNamespace(
            delivery=DeliveryClass.DURABLE,
            feed=Feed.BAR,
            interval="15m",
            requirement=SimpleNamespace(
                consumer_grade=grade,
                max_freshness_ms=1_080_000,
            ),
            identity=("alpha.binance.paper.stable", "instrument", "BAR", "15m", "policy"),
        )

    @staticmethod
    def _requirement(*, grade: Grade = Grade.ALPHA):
        return DataRequirement(
            instrument_uid="a953e16e-7138-5562-b5e8-c337a44d0b65",
            feed=Feed.BAR,
            consumer_grade=grade,
            source_policy_id="crypto_primary_v2",
            interval="15m",
            warmup_limit=500,
            max_freshness_ms=1_080_000,
            stale_policy=StalePolicy.BLOCK,
        )

    async def test_non_execution_bar_replays_retained_offsets_across_replicas(self):
        product = self._product()
        requirement = self._requirement()
        strict_view = SimpleNamespace(
            payload=SimpleNamespace(open_time_ns=20 * self.INTERVAL_NS)
        )
        seed_view = SimpleNamespace(payload=SimpleNamespace(open_time_ns=18 * self.INTERVAL_NS))
        strict_warmup = SimpleNamespace(data=[strict_view], watermark_offset=40)
        seed_warmup = SimpleNamespace(data=[seed_view], watermark_offset=38)
        first = StreamEvent(39, "resume-39", object())
        resumed = StreamEvent(40, "resume-40", object())
        first_session = self._Session(
            warmup=seed_warmup,
            items=(ControlEvent("REPLAYING", "retained BAR replay"), first),
        )
        resumed_session = self._Session(warmup=seed_warmup, items=(resumed,))
        first_client = self._Client(strict_warmup=strict_warmup, session=first_session)
        resumed_client = self._Client(strict_warmup=strict_warmup, session=resumed_session)
        projected = []
        current = SimpleNamespace(data=SimpleNamespace())

        def project(event, *, template, requirement, **kwargs):
            projected.append((event.logical_offset, template, requirement, kwargs))
            return SimpleNamespace(logical_offset=event.logical_offset)

        with tempfile.TemporaryDirectory(prefix="qdl-c2-historical-bar-") as raw:
            with (
                patch("scripts.phase103_consumer_receipt_acceptance.sdk_requirement", return_value=requirement),
                patch("scripts.phase103_consumer_receipt_acceptance._client", side_effect=(first_client, resumed_client)),
                patch("scripts.phase103_consumer_receipt_acceptance.market_data_view_from_stream", side_effect=project),
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view") as validate,
                patch(
                    "scripts.phase103_consumer_receipt_acceptance._strict_snapshot_for_c2",
                    return_value=current,
                ) as readback,
            ):
                result = await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=1.0,
                )

        self.assertEqual(result, (39, 40, ("REPLAYING",), ()))
        self.assertEqual(first_client.warmup_calls, [requirement])
        self.assertTrue(first_client.closed)
        self.assertTrue(resumed_client.closed)
        self.assertEqual(first_session.acknowledged, [first])
        self.assertEqual(resumed_session.acknowledged, [resumed])
        seed_requirement, first_resume = first_client.stream_calls[0]
        resumed_requirement, resumed_flag = resumed_client.stream_calls[0]
        self.assertFalse(first_resume)
        self.assertTrue(resumed_flag)
        self.assertIs(seed_requirement, resumed_requirement)
        self.assertEqual(seed_requirement.warmup.time_range.start_time_ns, 18 * self.INTERVAL_NS)
        self.assertEqual(seed_requirement.warmup.time_range.end_time_ns, 19 * self.INTERVAL_NS)
        self.assertEqual([item[0] for item in projected], [39, 40])
        self.assertTrue(all(item[2] is seed_requirement for item in projected))
        self.assertEqual(projected[0][3], {})
        self.assertEqual(projected[1][3], {"replay_only": True})
        readback.assert_awaited_once()
        self.assertEqual(validate.call_args_list[0], call(product, strict_view))
        self.assertEqual(
            validate.call_args_list[1:],
            [
                call(product, ANY, require_current_quality=False),
                call(product, ANY, require_current_quality=False, state_replay=True),
                call(product, current.data),
            ],
        )

    async def test_non_execution_bar_replay_revalidates_current_snapshot_after_large_backfill_tail(self):
        product = self._product()
        requirement = self._requirement()
        strict_view = SimpleNamespace(
            payload=SimpleNamespace(open_time_ns=20 * self.INTERVAL_NS)
        )
        seed_view = SimpleNamespace(payload=SimpleNamespace(open_time_ns=18 * self.INTERVAL_NS))
        strict_warmup = SimpleNamespace(data=[strict_view], watermark_offset=11_024)
        seed_warmup = SimpleNamespace(data=[seed_view], watermark_offset=10_004)
        first = StreamEvent(10_005, "resume-10005", object())
        resumed = tuple(
            StreamEvent(offset, f"resume-{offset}", object())
            for offset in (10_006, 10_007, 10_008)
        )
        first_session = self._Session(warmup=seed_warmup, items=(first,))
        resumed_session = self._Session(warmup=seed_warmup, items=resumed)
        first_client = self._Client(strict_warmup=strict_warmup, session=first_session)
        resumed_client = self._Client(strict_warmup=strict_warmup, session=resumed_session)
        current = SimpleNamespace(data=SimpleNamespace())

        with tempfile.TemporaryDirectory(prefix="qdl-c2-historical-drain-") as raw:
            with (
                patch("scripts.phase103_consumer_receipt_acceptance.sdk_requirement", return_value=requirement),
                patch("scripts.phase103_consumer_receipt_acceptance._client", side_effect=(first_client, resumed_client)),
                patch("scripts.phase103_consumer_receipt_acceptance.market_data_view_from_stream", return_value=SimpleNamespace()),
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view"),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance._strict_snapshot_for_c2",
                    return_value=current,
                ) as readback,
            ):
                result = await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=1.0,
                )

        self.assertEqual(result, (10_005, 10_006, (), ()))
        self.assertEqual(first_session.acknowledged, [first])
        self.assertEqual(resumed_session.acknowledged, [resumed[0]])
        readback.assert_awaited_once()

    async def test_non_execution_bar_can_be_quiet_only_with_signed_controls_and_current_final_reads(self):
        product = self._product()
        requirement = self._requirement()
        strict_view = SimpleNamespace(
            payload=SimpleNamespace(open_time_ns=20 * self.INTERVAL_NS)
        )
        seed_view = SimpleNamespace(payload=SimpleNamespace(open_time_ns=18 * self.INTERVAL_NS))
        strict_warmup = SimpleNamespace(data=[strict_view], watermark_offset=40)
        seed_warmup = SimpleNamespace(data=[seed_view], watermark_offset=38)
        controls = (
            ControlEvent("REPLAYING", "retained BAR replay accepted"),
            ControlEvent("LIVE", "stream live"),
        )
        first_session = self._Session(warmup=seed_warmup, items=controls, quiet=True)
        resumed_session = self._Session(warmup=seed_warmup, items=controls, quiet=True)
        first_current = SimpleNamespace(data=SimpleNamespace())
        resumed_current = SimpleNamespace(data=SimpleNamespace())
        first_client = self._Client(
            strict_warmup=strict_warmup,
            session=first_session,
            snapshots=(first_current,),
        )
        resumed_client = self._Client(
            strict_warmup=strict_warmup,
            session=resumed_session,
            snapshots=(resumed_current,),
        )

        with tempfile.TemporaryDirectory(prefix="qdl-c2-quiet-historical-bar-") as raw:
            with (
                patch("scripts.phase103_consumer_receipt_acceptance.sdk_requirement", return_value=requirement),
                patch("scripts.phase103_consumer_receipt_acceptance._client", side_effect=(first_client, resumed_client)),
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view") as validate,
            ):
                result = await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=0.01,
                )

        self.assertEqual(
            result,
            (
                None,
                None,
                ("REPLAYING", "LIVE", "REPLAYING", "LIVE"),
                ("CURRENT_FINAL_BAR", "CURRENT_FINAL_BAR"),
            ),
        )
        self.assertEqual(
            _stream_handoff_mode(
                product,
                acknowledged_offset=result[0],
                resumed_offset=result[1],
                no_event_sessions=result[3],
            ),
            "CURRENT_FINAL_BAR_OBSERVED_NO_CURSOR",
        )
        self.assertEqual(first_client.snapshot_calls, [requirement])
        self.assertEqual(resumed_client.snapshot_calls, [requirement])
        self.assertEqual(first_session.acknowledged, [])
        self.assertEqual(resumed_session.acknowledged, [])
        self.assertEqual(first_client.stream_calls[0][1], False)
        self.assertEqual(resumed_client.stream_calls[0][1], False)
        self.assertEqual(
            validate.call_args_list,
            [
                call(product, strict_view),
                call(product, first_current.data),
                call(product, resumed_current.data),
            ],
        )

    async def test_non_execution_quiet_bar_requires_signed_controls_before_current_read(self):
        product = self._product()
        requirement = self._requirement()
        strict_view = SimpleNamespace(
            payload=SimpleNamespace(open_time_ns=20 * self.INTERVAL_NS)
        )
        strict_warmup = SimpleNamespace(data=[strict_view], watermark_offset=40)
        seed_warmup = SimpleNamespace(
            data=[SimpleNamespace(payload=SimpleNamespace(open_time_ns=18 * self.INTERVAL_NS))],
            watermark_offset=38,
        )
        client = self._Client(
            strict_warmup=strict_warmup,
            session=self._Session(warmup=seed_warmup, items=(), quiet=True),
            snapshots=(SimpleNamespace(data=SimpleNamespace()),),
        )

        with tempfile.TemporaryDirectory(prefix="qdl-c2-quiet-historical-bar-controls-") as raw:
            with (
                patch("scripts.phase103_consumer_receipt_acceptance.sdk_requirement", return_value=requirement),
                patch("scripts.phase103_consumer_receipt_acceptance._client", return_value=client),
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view"),
                self.assertRaisesRegex(ContinuityError, "signed cursor stream"),
            ):
                await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=0.01,
                )

        self.assertEqual(client.snapshot_calls, [])

    async def test_non_execution_quiet_bar_rejects_failed_current_read(self):
        product = self._product()
        requirement = self._requirement()
        strict_view = SimpleNamespace(
            payload=SimpleNamespace(open_time_ns=20 * self.INTERVAL_NS)
        )
        strict_warmup = SimpleNamespace(data=[strict_view], watermark_offset=40)
        seed_warmup = SimpleNamespace(
            data=[SimpleNamespace(payload=SimpleNamespace(open_time_ns=18 * self.INTERVAL_NS))],
            watermark_offset=38,
        )
        controls = (
            ControlEvent("REPLAYING", "retained BAR replay accepted"),
            ControlEvent("LIVE", "stream live"),
        )
        client = self._Client(
            strict_warmup=strict_warmup,
            session=self._Session(warmup=seed_warmup, items=controls, quiet=True),
        )

        with tempfile.TemporaryDirectory(prefix="qdl-c2-quiet-historical-bar-current-") as raw:
            with (
                patch("scripts.phase103_consumer_receipt_acceptance.sdk_requirement", return_value=requirement),
                patch("scripts.phase103_consumer_receipt_acceptance._client", return_value=client),
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view"),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance._strict_snapshot_for_c2",
                    side_effect=ContinuityError("DATA_STALE", "current historical BAR stale"),
                ),
                self.assertRaisesRegex(ContinuityError, "current historical BAR stale"),
            ):
                await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=0.01,
                )

    async def test_execution_bar_keeps_the_live_stream_requirement(self):
        product = self._product(grade=Grade.EXECUTION)
        requirement = self._requirement(grade=Grade.EXECUTION)
        warmup = SimpleNamespace(
            data=[SimpleNamespace(payload=SimpleNamespace(open_time_ns=self.INTERVAL_NS))],
            watermark_offset=10,
        )
        first = StreamEvent(10, "resume-10", object())
        resumed = StreamEvent(11, "resume-11", object())
        first_session = self._Session(warmup=warmup, items=(first,))
        resumed_session = self._Session(warmup=warmup, items=(resumed,))
        first_client = self._Client(strict_warmup=warmup, session=first_session)
        resumed_client = self._Client(strict_warmup=warmup, session=resumed_session)

        with tempfile.TemporaryDirectory(prefix="qdl-c2-live-bar-") as raw:
            with (
                patch("scripts.phase103_consumer_receipt_acceptance.sdk_requirement", return_value=requirement),
                patch("scripts.phase103_consumer_receipt_acceptance._client", side_effect=(first_client, resumed_client)),
                patch("scripts.phase103_consumer_receipt_acceptance.market_data_view_from_stream", return_value=SimpleNamespace()),
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view"),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance._strict_snapshot_for_c2",
                    return_value=SimpleNamespace(data=SimpleNamespace()),
                ) as readback,
            ):
                result = await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=1.0,
                )

        self.assertEqual(result, (10, 11, (), ()))
        self.assertEqual(first_client.warmup_calls, [])
        self.assertEqual(first_client.stream_calls, [(requirement, False)])
        self.assertEqual(resumed_client.stream_calls, [(requirement, True)])
        readback.assert_awaited_once()

    async def test_execution_quote_stale_first_frame_requires_current_snapshot_before_resume(self):
        requirement = DataRequirement(
            instrument_uid="a953e16e-7138-5562-b5e8-c337a44d0b65",
            feed=Feed.QUOTE,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            warmup_limit=0,
            max_freshness_ms=3_000,
            stale_policy=StalePolicy.BLOCK,
        )
        product = SimpleNamespace(
            delivery=DeliveryClass.DURABLE,
            feed=Feed.QUOTE,
            interval=None,
            requirement=SimpleNamespace(
                consumer_grade=Grade.EXECUTION,
                max_freshness_ms=3_000,
            ),
            identity=("trading-system.paper.stable", "instrument", "QUOTE", "", "policy"),
        )
        warmup = SimpleNamespace(data=[object()], watermark_offset=9)
        first = StreamEvent(10, "resume-10", object())
        resumed = StreamEvent(11, "resume-11", object())
        first_session = self._Session(warmup=warmup, items=(first,))
        resumed_session = self._Session(warmup=warmup, items=(resumed,))
        first_client = self._Client(strict_warmup=warmup, session=first_session)
        resumed_client = self._Client(strict_warmup=warmup, session=resumed_session)
        first_current = SimpleNamespace(data=SimpleNamespace())
        resumed_current = SimpleNamespace(data=SimpleNamespace())

        with tempfile.TemporaryDirectory(prefix="qdl-c2-stale-quote-") as raw:
            with (
                patch("scripts.phase103_consumer_receipt_acceptance.sdk_requirement", return_value=requirement),
                patch("scripts.phase103_consumer_receipt_acceptance._client", side_effect=(first_client, resumed_client)),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance.market_data_view_from_stream",
                    side_effect=(
                        ContinuityError("DATA_STALE", "delayed quote"),
                        SimpleNamespace(),
                        SimpleNamespace(),
                    ),
                ) as project,
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view") as validate,
                patch(
                    "scripts.phase103_consumer_receipt_acceptance._strict_snapshot_for_c2",
                    side_effect=(first_current, resumed_current),
                ) as readback,
            ):
                result = await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=1.0,
                )

        self.assertEqual(result, (10, 11, (), ()))
        self.assertEqual(first_session.acknowledged, [first])
        self.assertEqual(resumed_session.acknowledged, [resumed])
        self.assertEqual(readback.await_count, 2)
        self.assertNotIn("replay_only", project.call_args_list[0].kwargs)
        self.assertTrue(project.call_args_list[1].kwargs["replay_only"])
        self.assertTrue(project.call_args_list[2].kwargs["replay_only"])
        self.assertIn(
            call(product, first_current.data),
            validate.call_args_list,
        )
        self.assertIn(
            call(product, resumed_current.data),
            validate.call_args_list,
        )

    async def test_execution_quote_stale_first_frame_fails_closed_when_current_snapshot_is_stale(self):
        requirement = DataRequirement(
            instrument_uid="a953e16e-7138-5562-b5e8-c337a44d0b65",
            feed=Feed.QUOTE,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            warmup_limit=0,
            max_freshness_ms=3_000,
            stale_policy=StalePolicy.BLOCK,
        )
        product = SimpleNamespace(
            delivery=DeliveryClass.DURABLE,
            feed=Feed.QUOTE,
            interval=None,
            requirement=SimpleNamespace(
                consumer_grade=Grade.EXECUTION,
                max_freshness_ms=3_000,
            ),
            identity=("trading-system.paper.stable", "instrument", "QUOTE", "", "policy"),
        )
        warmup = SimpleNamespace(data=[object()], watermark_offset=9)
        first = StreamEvent(10, "resume-10", object())
        first_client = self._Client(
            strict_warmup=warmup,
            session=self._Session(warmup=warmup, items=(first,)),
        )

        with tempfile.TemporaryDirectory(prefix="qdl-c2-stale-quote-block-") as raw:
            with (
                patch("scripts.phase103_consumer_receipt_acceptance.sdk_requirement", return_value=requirement),
                patch("scripts.phase103_consumer_receipt_acceptance._client", return_value=first_client),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance.market_data_view_from_stream",
                    side_effect=(
                        ContinuityError("DATA_STALE", "delayed quote"),
                        SimpleNamespace(),
                    ),
                ),
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view"),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance._strict_snapshot_for_c2",
                    side_effect=ContinuityError("DATA_STALE", "current quote stale"),
                ),
                self.assertRaisesRegex(ContinuityError, "current quote stale"),
            ):
                await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=1.0,
                )


class Phase103ReplayReadbackTests(unittest.IsolatedAsyncioTestCase):
    class _Session:
        def __init__(self, *, watermark_offset: int, items):
            self.warmup = SimpleNamespace(data=[object()], watermark_offset=watermark_offset)
            self._items = iter(items)
            self.acknowledged = []

        async def __anext__(self):
            try:
                return next(self._items)
            except StopIteration as error:
                raise StopAsyncIteration from error

        def acknowledge(self, event):
            self.acknowledged.append(event)

    class _Context:
        def __init__(self, session):
            self.session = session

        async def __aenter__(self):
            return self.session

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class _Client:
        def __init__(self, session):
            self.session = session
            self.stream_calls = []
            self.closed = False

        def warmup_then_stream(self, requirement, *, resume_restored_state=False):
            self.stream_calls.append((requirement, resume_restored_state))
            return Phase103ReplayReadbackTests._Context(self.session)

        async def close(self):
            self.closed = True

    async def test_resumed_cursor_frame_requires_a_fresh_readback_before_receipt(self):
        requirement = DataRequirement(
            instrument_uid="a953e16e-7138-5562-b5e8-c337a44d0b65",
            feed=Feed.QUOTE,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=2_000,
            stale_policy=StalePolicy.BLOCK,
        )
        product = SimpleNamespace(
            delivery=DeliveryClass.DURABLE,
            feed=Feed.QUOTE,
            interval=None,
            requirement=SimpleNamespace(
                consumer_grade=Grade.EXECUTION,
                max_freshness_ms=2_000,
            ),
            identity=("trading-system.paper.stable", "instrument", "QUOTE", "", "policy"),
        )
        first = StreamEvent(10, "token-10", object())
        replayed = StreamEvent(11, "token-11", object())
        first_session = self._Session(watermark_offset=9, items=(first,))
        # Even an offset after the fresh snapshot watermark is state recovery
        # in the bounded restored-cursor probe, never a live execution price.
        resumed_session = self._Session(watermark_offset=10, items=(replayed,))
        first_client = self._Client(first_session)
        resumed_client = self._Client(resumed_session)
        current = SimpleNamespace(data=SimpleNamespace())
        projected = []

        def project(event, *, template, requirement, **kwargs):
            projected.append((event.logical_offset, kwargs))
            return SimpleNamespace()

        with tempfile.TemporaryDirectory(prefix="qdl-c2-replay-readback-") as raw:
            with (
                patch("scripts.phase103_consumer_receipt_acceptance.sdk_requirement", return_value=requirement),
                patch("scripts.phase103_consumer_receipt_acceptance._client", side_effect=(first_client, resumed_client)),
                patch("scripts.phase103_consumer_receipt_acceptance.market_data_view_from_stream", side_effect=project),
                patch("scripts.phase103_consumer_receipt_acceptance._strict_snapshot_for_c2", return_value=current) as readback,
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view") as validate,
            ):
                result = await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=1.0,
                )

        self.assertEqual(result, (10, 11, (), ()))
        self.assertEqual(projected, [(10, {}), (11, {"replay_only": True})])
        readback.assert_awaited_once()
        self.assertEqual(first_session.acknowledged, [first])
        self.assertEqual(resumed_session.acknowledged, [replayed])
        self.assertEqual(
            validate.call_args_list,
            [
                call(product, ANY, require_current_quality=True),
                call(product, ANY, require_current_quality=False, state_replay=True),
                call(product, current.data),
            ],
        )


class Phase103QuietTradeStreamTests(unittest.IsolatedAsyncioTestCase):
    class _Session:
        def __init__(self, *, items=(), quiet: bool = False):
            self.warmup = SimpleNamespace(data=[object()], watermark_offset=0)
            self._items = iter(items)
            self._quiet = quiet
            self.acknowledged = []

        async def __anext__(self):
            try:
                return next(self._items)
            except StopIteration as error:
                if self._quiet:
                    await asyncio.Event().wait()
                raise StopAsyncIteration from error

        def acknowledge(self, event):
            self.acknowledged.append(event)

    class _Context:
        def __init__(self, session):
            self._session = session

        async def __aenter__(self):
            return self._session

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class _Client:
        def __init__(self, *, sessions, status=None):
            self._sessions = iter(sessions)
            self._status = status
            self.stream_calls = []
            self.status_calls = 0
            self.closed = False

        def warmup_then_stream(self, requirement, *, resume_restored_state=False):
            self.stream_calls.append((requirement, resume_restored_state))
            return Phase103QuietTradeStreamTests._Context(next(self._sessions))

        async def feed_status(self, _requirement):
            self.status_calls += 1
            if isinstance(self._status, BaseException):
                raise self._status
            if self._status is None:
                raise AssertionError("normal durable replay must not query quiet status")
            return self._status

        async def close(self):
            self.closed = True

    @staticmethod
    def _requirement(*, event_recency_policy: StalePolicy = StalePolicy.OBSERVE):
        return DataRequirement(
            instrument_uid="a953e16e-7138-5562-b5e8-c337a44d0b65",
            feed=Feed.TRADE,
            consumer_grade=Grade.ALPHA,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=15_000,
            max_session_liveness_ms=45_000,
            event_recency_policy=event_recency_policy,
            stale_policy=StalePolicy.BLOCK,
        )

    @staticmethod
    def _product(requirement):
        return SimpleNamespace(
            delivery=DeliveryClass.DURABLE,
            feed=Feed.TRADE,
            interval=None,
            requirement=requirement,
            identity=(
                "alpha.okx.paper.stable",
                requirement.instrument_uid,
                "TRADE",
                "",
                requirement.source_policy_id,
            ),
        )

    @staticmethod
    def _status(
        requirement,
        *,
        event_recency_state: str = "STALE",
        freshness_ms: int | None = None,
        provider_session_state: str = "LIVE",
        provider_session_liveness_ms: int | None = 10,
        gap_open: bool = False,
        execution_eligible: bool = False,
    ) -> FeedStatusResponse:
        return FeedStatusResponse.model_validate({
            "schema": "qdl.feed-status.v2",
            "instrument_uid": requirement.instrument_uid,
            "feed": requirement.feed.value,
            "quality": {
                "state": "LIVE",
                "freshness_ms": (
                    requirement.max_freshness_ms + 1
                    if freshness_ms is None else freshness_ms
                ),
                "event_recency_state": event_recency_state,
                "provider_session_state": provider_session_state,
                "provider_session_liveness_ms": provider_session_liveness_ms,
                "gap_open": gap_open,
                "complete": True,
                "execution_eligible": execution_eligible,
                "policy_id": requirement.source_policy_id,
                "flags": [],
            },
        })

    async def test_quiet_live_trade_observes_two_sessions_without_cursor_claim(self):
        requirement = self._requirement()
        product = self._product(requirement)
        controls = (ControlEvent("REPLAYING", "cursor accepted"), ControlEvent("LIVE", "stream live"))
        first_session = self._Session(items=controls, quiet=True)
        second_session = self._Session(items=controls, quiet=True)
        first_client = self._Client(
            sessions=(first_session,), status=self._status(requirement)
        )
        second_client = self._Client(
            sessions=(second_session,), status=self._status(requirement)
        )

        with tempfile.TemporaryDirectory(prefix="qdl-c2-quiet-trade-") as raw:
            with patch(
                "scripts.phase103_consumer_receipt_acceptance.sdk_requirement",
                return_value=requirement,
            ), patch(
                "scripts.phase103_consumer_receipt_acceptance._client",
                side_effect=(first_client, second_client),
            ):
                result = await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=0.01,
                )

        self.assertEqual(
            result,
            (None, None, ("REPLAYING", "LIVE", "REPLAYING", "LIVE"), (
                "QUIET_NON_EXECUTABLE", "QUIET_NON_EXECUTABLE",
            )),
        )
        self.assertEqual(
            _stream_handoff_mode(
                product,
                acknowledged_offset=result[0],
                resumed_offset=result[1],
                no_event_sessions=result[3],
            ),
            "QUIET_OBSERVED_NO_CURSOR",
        )
        self.assertEqual(first_client.stream_calls, [(requirement, False)])
        self.assertEqual(second_client.stream_calls, [(requirement, False)])
        self.assertEqual(first_client.status_calls, 1)
        self.assertEqual(second_client.status_calls, 1)
        self.assertEqual(first_session.acknowledged, [])
        self.assertEqual(second_session.acknowledged, [])

    async def test_fresh_live_trade_without_new_print_keeps_signed_cursor_session(self):
        requirement = self._requirement()
        product = self._product(requirement)
        controls = (ControlEvent("REPLAYING", "cursor accepted"), ControlEvent("LIVE", "stream live"))
        status = self._status(
            requirement,
            event_recency_state="LIVE",
            freshness_ms=1,
            execution_eligible=True,
        )
        first_client = self._Client(
            sessions=(self._Session(items=controls, quiet=True),), status=status
        )
        second_client = self._Client(
            sessions=(self._Session(items=controls, quiet=True),), status=status
        )

        with tempfile.TemporaryDirectory(prefix="qdl-c2-fresh-trade-") as raw:
            with patch(
                "scripts.phase103_consumer_receipt_acceptance.sdk_requirement",
                return_value=requirement,
            ), patch(
                "scripts.phase103_consumer_receipt_acceptance._client",
                side_effect=(first_client, second_client),
            ):
                result = await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=0.01,
                )

        self.assertEqual(
            result,
            (None, None, ("REPLAYING", "LIVE", "REPLAYING", "LIVE"), (
                "FRESH_EXECUTABLE", "FRESH_EXECUTABLE",
            )),
        )
        self.assertEqual(
            _stream_handoff_mode(
                product,
                acknowledged_offset=result[0],
                resumed_offset=result[1],
                no_event_sessions=result[3],
            ),
            "LIVE_OBSERVED_NO_NEW_CURSOR",
        )
        self.assertEqual(first_client.status_calls, 1)
        self.assertEqual(second_client.status_calls, 1)

    async def test_no_event_trade_requires_signed_cursor_controls(self):
        requirement = self._requirement()
        product = self._product(requirement)
        status = self._status(
            requirement,
            event_recency_state="LIVE",
            freshness_ms=1,
            execution_eligible=True,
        )
        client = self._Client(sessions=(self._Session(quiet=True),), status=status)

        with tempfile.TemporaryDirectory(prefix="qdl-c2-missing-controls-") as raw:
            with patch(
                "scripts.phase103_consumer_receipt_acceptance.sdk_requirement",
                return_value=requirement,
            ), patch(
                "scripts.phase103_consumer_receipt_acceptance._client",
                return_value=client,
            ), self.assertRaisesRegex(ContinuityError, "signed cursor stream"):
                await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=0.01,
                )
        self.assertEqual(client.status_calls, 0)

    async def test_no_event_then_reopened_trade_event_is_explicit_not_replay(self):
        requirement = self._requirement()
        product = self._product(requirement)
        controls = (ControlEvent("REPLAYING", "cursor accepted"), ControlEvent("LIVE", "stream live"))
        status = self._status(
            requirement,
            event_recency_state="LIVE",
            freshness_ms=1,
            execution_eligible=True,
        )
        observed = StreamEvent(11, "resume-11", object())
        first_client = self._Client(
            sessions=(self._Session(items=controls, quiet=True),), status=status
        )
        second_session = self._Session(items=controls + (observed,))
        second_client = self._Client(sessions=(second_session,))

        with tempfile.TemporaryDirectory(prefix="qdl-c2-reopened-trade-") as raw:
            with (
                patch(
                    "scripts.phase103_consumer_receipt_acceptance.sdk_requirement",
                    return_value=requirement,
                ),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance._client",
                    side_effect=(first_client, second_client),
                ),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance.market_data_view_from_stream",
                    return_value=SimpleNamespace(),
                ),
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view"),
            ):
                result = await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=0.01,
                )

        self.assertEqual(
            result,
            (None, None, ("REPLAYING", "LIVE", "REPLAYING", "LIVE"), (
                "FRESH_EXECUTABLE", "EVENT_AFTER_REOPEN",
            )),
        )
        self.assertEqual(second_session.acknowledged, [observed])
        self.assertEqual(
            _stream_handoff_mode(
                product,
                acknowledged_offset=result[0],
                resumed_offset=result[1],
                no_event_sessions=result[3],
            ),
            "LIVE_EVENT_AFTER_REOPEN_NO_CURSOR",
        )

    async def test_checkpointed_trade_reopen_without_new_print_is_explicit(self):
        requirement = self._requirement()
        product = self._product(requirement)
        controls = (ControlEvent("REPLAYING", "cursor accepted"), ControlEvent("LIVE", "stream live"))
        first = StreamEvent(10, "resume-10", object())
        status = self._status(
            requirement,
            event_recency_state="LIVE",
            freshness_ms=1,
            execution_eligible=True,
        )
        first_session = self._Session(items=controls + (first,))
        second_session = self._Session(items=controls, quiet=True)
        first_client = self._Client(sessions=(first_session,))
        second_client = self._Client(sessions=(second_session,), status=status)

        with tempfile.TemporaryDirectory(prefix="qdl-c2-checkpointed-trade-") as raw:
            with (
                patch(
                    "scripts.phase103_consumer_receipt_acceptance.sdk_requirement",
                    return_value=requirement,
                ),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance._client",
                    side_effect=(first_client, second_client),
                ),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance.market_data_view_from_stream",
                    return_value=SimpleNamespace(),
                ),
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view"),
            ):
                result = await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=0.01,
                )

        self.assertEqual(
            result,
            (None, None, ("REPLAYING", "LIVE", "REPLAYING", "LIVE"), (
                "CURSOR_ACKNOWLEDGED", "FRESH_EXECUTABLE_AFTER_CURSOR",
            )),
        )
        self.assertEqual(first_session.acknowledged, [first])
        self.assertEqual(second_client.stream_calls, [(requirement, True)])
        self.assertEqual(second_client.status_calls, 1)
        self.assertEqual(
            _stream_handoff_mode(
                product,
                acknowledged_offset=result[0],
                resumed_offset=result[1],
                no_event_sessions=result[3],
            ),
            "SIGNED_CURSOR_REOPENED_NO_NEW_EVENT",
        )

    async def test_delivered_observed_trade_uses_state_replay_then_strict_readback(self):
        requirement = self._requirement()
        product = self._product(requirement)
        first = StreamEvent(10, "resume-10", object())
        resumed = StreamEvent(11, "resume-11", object())
        first_session = self._Session(items=(first,))
        second_session = self._Session(items=(resumed,))
        first_client = self._Client(sessions=(first_session,))
        second_client = self._Client(sessions=(second_session,))

        with tempfile.TemporaryDirectory(prefix="qdl-c2-delivered-trade-") as raw:
            with (
                patch(
                    "scripts.phase103_consumer_receipt_acceptance.sdk_requirement",
                    return_value=requirement,
                ),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance._client",
                    side_effect=(first_client, second_client),
                ),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance.market_data_view_from_stream",
                    return_value=SimpleNamespace(),
                ),
                patch("scripts.phase103_consumer_receipt_acceptance.validate_product_view"),
                patch(
                    "scripts.phase103_consumer_receipt_acceptance._strict_snapshot_for_c2",
                    return_value=SimpleNamespace(data=SimpleNamespace()),
                ) as readback,
            ):
                result = await _stream_resume(
                    product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=0.01,
                )

        self.assertEqual(result, (10, 11, (), ()))
        self.assertEqual(
            _stream_handoff_mode(
                product,
                acknowledged_offset=result[0],
                resumed_offset=result[1],
                no_event_sessions=result[3],
            ),
            "DURABLE_CURSOR_REPLAYED",
        )
        self.assertEqual(first_session.acknowledged, [first])
        self.assertEqual(second_session.acknowledged, [resumed])
        self.assertEqual(second_client.stream_calls, [(requirement, True)])
        readback.assert_awaited_once()

    async def test_quiet_trade_rejects_disconnected_stale_session_gap_and_wrong_policy(self):
        requirement = self._requirement()
        product = self._product(requirement)
        invalid_statuses = {
            "disconnected": self._status(
                requirement, provider_session_state="DISCONNECTED"
            ),
            "stale_session": self._status(
                requirement, provider_session_liveness_ms=45_001
            ),
            "open_gap": self._status(requirement, gap_open=True),
            "execution_eligible": self._status(requirement, execution_eligible=True),
        }
        for name, status in invalid_statuses.items():
            with self.subTest(status=name), tempfile.TemporaryDirectory(
                prefix="qdl-c2-quiet-trade-invalid-"
            ) as raw:
                client = self._Client(
                    sessions=(self._Session(items=(
                        ControlEvent("REPLAYING", "cursor accepted"),
                        ControlEvent("LIVE", "stream live"),
                    ), quiet=True),),
                    status=status,
                )
                with patch(
                    "scripts.phase103_consumer_receipt_acceptance.sdk_requirement",
                    return_value=requirement,
                ), patch(
                    "scripts.phase103_consumer_receipt_acceptance._client",
                    return_value=client,
                ), self.assertRaisesRegex(ContinuityError, "live fresh/executable"):
                    await _stream_resume(
                        product,
                        identity=SimpleNamespace(),
                        primary_url="https://query-primary",
                        secondary_url="https://query-secondary",
                        grpc_target="stream:8210",
                        state_dir=Path(raw),
                        timeout_seconds=0.01,
                    )

        blocked_requirement = self._requirement(event_recency_policy=StalePolicy.BLOCK)
        blocked_product = self._product(blocked_requirement)
        blocked_client = self._Client(sessions=(self._Session(quiet=True),))
        with tempfile.TemporaryDirectory(prefix="qdl-c2-quiet-trade-policy-") as raw:
            with patch(
                "scripts.phase103_consumer_receipt_acceptance.sdk_requirement",
                return_value=blocked_requirement,
            ), patch(
                "scripts.phase103_consumer_receipt_acceptance._client",
                return_value=blocked_client,
            ), self.assertRaises(TimeoutError):
                await _stream_resume(
                    blocked_product,
                    identity=SimpleNamespace(),
                    primary_url="https://query-primary",
                    secondary_url="https://query-secondary",
                    grpc_target="stream:8210",
                    state_dir=Path(raw),
                    timeout_seconds=0.01,
                )
        self.assertEqual(blocked_client.status_calls, 0)


class Phase103QuietQuoteRetryTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _requirement() -> DataRequirement:
        return DataRequirement(
            instrument_uid="a953e16e-7138-5562-b5e8-c337a44d0b65",
            feed=Feed.QUOTE,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=2_000,
            max_session_liveness_ms=45_000,
            stale_policy=StalePolicy.BLOCK,
        )

    @staticmethod
    def _product():
        return SimpleNamespace(feed=Feed.QUOTE)

    @staticmethod
    def _status(
        requirement: DataRequirement,
        *,
        state: str = "STALE",
        event_recency_state: str = "STALE",
        freshness_ms: int | None = None,
        provider_session_state: str = "LIVE",
        provider_session_liveness_ms: int | None = 10,
        gap_open: bool = False,
        complete: bool = True,
        execution_eligible: bool = False,
    ) -> FeedStatusResponse:
        return FeedStatusResponse.model_validate({
            "schema": "qdl.feed-status.v2",
            "instrument_uid": requirement.instrument_uid,
            "feed": requirement.feed.value,
            "quality": {
                "state": state,
                "freshness_ms": 2_001 if freshness_ms is None else freshness_ms,
                "event_recency_state": event_recency_state,
                "provider_session_state": provider_session_state,
                "provider_session_liveness_ms": provider_session_liveness_ms,
                "gap_open": gap_open,
                "complete": complete,
                "execution_eligible": execution_eligible,
                "policy_id": requirement.source_policy_id,
                "flags": [],
            },
        })

    class _Client:
        def __init__(self, snapshots, status):
            self._snapshots = iter(snapshots)
            self._status = status
            self.snapshot_calls = 0
            self.status_calls = 0

        async def snapshot(self, _requirement):
            self.snapshot_calls += 1
            value = next(self._snapshots)
            if isinstance(value, BaseException):
                raise value
            return value

        async def feed_status(self, _requirement):
            self.status_calls += 1
            return self._status

    class _StreamContext:
        def __init__(self, value):
            self._value = value

        async def __aenter__(self):
            if isinstance(self._value, BaseException):
                raise self._value
            return self._value

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class _StreamClient(_Client):
        def __init__(self, stream_entries, status):
            super().__init__((), status)
            self._stream_entries = iter(stream_entries)
            self.stream_calls = 0

        def warmup_then_stream(self, _requirement, *, resume_restored_state=False):
            self.stream_calls += 1
            return Phase103QuietQuoteRetryTests._StreamContext(
                next(self._stream_entries)
            )

    async def test_quiet_connected_quote_retries_only_until_a_fresh_snapshot_arrives(self):
        requirement = self._requirement()
        client = self._Client(
            (DataLayerError("DATA_STALE", "stale BBO"), "fresh-snapshot"),
            self._status(requirement),
        )

        result = await _strict_snapshot_for_c2(
            client,
            product=self._product(),
            requirement=requirement,
            timeout_seconds=0.25,
        )

        self.assertEqual(result, "fresh-snapshot")
        self.assertEqual(client.snapshot_calls, 2)
        self.assertEqual(client.status_calls, 1)

    async def test_fresh_quote_status_after_a_stale_snapshot_retries_under_the_same_sla(self):
        requirement = self._requirement()
        client = self._Client(
            (DataLayerError("DATA_STALE", "stale BBO"), "fresh-snapshot"),
            self._status(
                requirement,
                state="LIVE",
                event_recency_state="LIVE",
                freshness_ms=1,
                execution_eligible=True,
            ),
        )

        result = await _strict_snapshot_for_c2(
            client,
            product=self._product(),
            requirement=requirement,
            timeout_seconds=0.25,
        )

        self.assertEqual(result, "fresh-snapshot")
        self.assertEqual(client.snapshot_calls, 2)
        self.assertEqual(client.status_calls, 1)

    async def test_strict_snapshot_preserves_the_typed_sdk_envelope(self):
        requirement = self._requirement()
        expected = SimpleNamespace(kind="market-data-view")
        client = self._Client(
            (SnapshotResponse.model_construct(request_id="test", data=expected),),
            self._status(
                requirement,
                state="LIVE",
                event_recency_state="LIVE",
                freshness_ms=1,
                execution_eligible=True,
            ),
        )

        result = await _strict_snapshot_for_c2(
            client,
            product=self._product(),
            requirement=requirement,
            timeout_seconds=0.25,
        )

        self.assertIs(result.data, expected)
        self.assertEqual(client.snapshot_calls, 1)

    async def test_disconnected_quote_never_retries_or_accepts_stale_data(self):
        requirement = self._requirement()
        client = self._Client(
            (DataLayerError("DATA_STALE", "stale BBO"),),
            self._status(requirement, provider_session_state="DISCONNECTED"),
        )

        with self.assertRaisesRegex(ContinuityError, "live provider session"):
            await _strict_snapshot_for_c2(
                client,
                product=self._product(),
                requirement=requirement,
                timeout_seconds=0.25,
            )

        self.assertEqual(client.snapshot_calls, 1)
        self.assertEqual(client.status_calls, 1)

    async def test_quiet_quote_deadline_fails_closed_without_a_fresh_snapshot(self):
        requirement = self._requirement()
        client = self._Client(
            (DataLayerError("DATA_STALE", "stale BBO"),),
            self._status(requirement),
        )

        with self.assertRaisesRegex(ContinuityError, "before its deadline"):
            await _strict_snapshot_for_c2(
                client,
                product=self._product(),
                requirement=requirement,
                timeout_seconds=0.01,
            )

        self.assertEqual(client.snapshot_calls, 1)
        self.assertEqual(client.status_calls, 1)

    async def test_verified_book_snapshot_retries_only_until_a_fresh_renewal_arrives(self):
        requirement = DataRequirement(
            instrument_uid="6c7c9256-2905-5c75-a149-fa0ac36bbbc7",
            feed=Feed.BOOK_SNAPSHOT,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=60_000,
            stale_policy=StalePolicy.BLOCK,
        )
        product = SimpleNamespace(feed=Feed.BOOK_SNAPSHOT)
        client = self._Client(
            (DataLayerError("DATA_STALE", "book snapshot renewal pending"), "fresh-book"),
            self._status(
                requirement,
                provider_session_state="NOT_APPLICABLE",
                provider_session_liveness_ms=None,
            ),
        )

        with patch(
            "scripts.phase103_consumer_receipt_acceptance._BOOK_SNAPSHOT_RETRY_SECONDS",
            0.001,
        ):
            result = await _strict_snapshot_for_c2(
                client,
                product=product,
                requirement=requirement,
                timeout_seconds=0.25,
            )

        self.assertEqual(result, "fresh-book")
        self.assertEqual(client.snapshot_calls, 2)
        self.assertEqual(client.status_calls, 1)

    async def test_book_snapshot_gap_or_session_mismatch_never_retries(self):
        requirement = DataRequirement(
            instrument_uid="6c7c9256-2905-5c75-a149-fa0ac36bbbc7",
            feed=Feed.BOOK_SNAPSHOT,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=60_000,
            stale_policy=StalePolicy.BLOCK,
        )
        product = SimpleNamespace(feed=Feed.BOOK_SNAPSHOT)
        for status in (
            self._status(
                requirement,
                provider_session_state="NOT_APPLICABLE",
                provider_session_liveness_ms=None,
                gap_open=True,
            ),
            self._status(requirement),
        ):
            client = self._Client((DataLayerError("DATA_STALE", "bad book"),), status)
            with self.assertRaisesRegex(ContinuityError, "complete, gap-free snapshot state"):
                await _strict_snapshot_for_c2(
                    client,
                    product=product,
                    requirement=requirement,
                    timeout_seconds=0.25,
                )
            self.assertEqual(client.snapshot_calls, 1)
            self.assertEqual(client.status_calls, 1)

    async def test_quiet_connected_trade_retries_but_still_requires_a_fresh_snapshot(self):
        requirement = DataRequirement(
            instrument_uid="6c7c9256-2905-5c75-a149-fa0ac36bbbc7",
            feed=Feed.TRADE,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=3_000,
            event_recency_policy=StalePolicy.OBSERVE,
            max_session_liveness_ms=45_000,
            stale_policy=StalePolicy.BLOCK,
        )
        product = SimpleNamespace(feed=Feed.TRADE, delivery=DeliveryClass.DURABLE)
        client = self._Client(
            (DataLayerError("DATA_STALE", "quiet trade"), "fresh-trade-snapshot"),
            Phase103QuietTradeStreamTests._status(requirement),
        )

        result = await _strict_snapshot_for_c2(
            client,
            product=product,
            requirement=requirement,
            timeout_seconds=0.25,
        )

        self.assertEqual(result, "fresh-trade-snapshot")
        self.assertEqual(client.snapshot_calls, 2)
        self.assertEqual(client.status_calls, 1)

    async def test_quiet_trade_disconnected_or_blocked_policy_fails_closed(self):
        live_requirement = DataRequirement(
            instrument_uid="6c7c9256-2905-5c75-a149-fa0ac36bbbc7",
            feed=Feed.TRADE,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=3_000,
            event_recency_policy=StalePolicy.OBSERVE,
            max_session_liveness_ms=45_000,
            stale_policy=StalePolicy.BLOCK,
        )
        product = SimpleNamespace(feed=Feed.TRADE, delivery=DeliveryClass.DURABLE)
        disconnected = self._Client(
            (DataLayerError("DATA_STALE", "quiet trade"),),
            Phase103QuietTradeStreamTests._status(
                live_requirement, provider_session_state="DISCONNECTED"
            ),
        )
        with self.assertRaisesRegex(ContinuityError, "live provider session"):
            await _strict_snapshot_for_c2(
                disconnected,
                product=product,
                requirement=live_requirement,
                timeout_seconds=0.25,
            )

        blocked_requirement = replace(
            live_requirement,
            event_recency_policy=StalePolicy.BLOCK,
        )
        blocked = self._Client(
            (DataLayerError("DATA_STALE", "quiet trade"),),
            Phase103QuietTradeStreamTests._status(blocked_requirement),
        )
        with self.assertRaisesRegex(ContinuityError, "live provider session"):
            await _strict_snapshot_for_c2(
                blocked,
                product=product,
                requirement=blocked_requirement,
                timeout_seconds=0.25,
            )

    async def test_quiet_connected_quote_stream_handoff_retries_before_yielding(self):
        requirement = self._requirement()
        session = SimpleNamespace(warmup=SimpleNamespace(data=[object()]))
        client = self._StreamClient(
            (DataLayerError("DATA_STALE", "stale BBO"), session),
            self._status(requirement),
        )

        async with _strict_warmup_then_stream_for_c2(
            client,
            product=self._product(),
            requirement=requirement,
            timeout_seconds=0.25,
        ) as result:
            self.assertIs(result, session)

        self.assertEqual(client.stream_calls, 2)
        self.assertEqual(client.status_calls, 1)


if __name__ == "__main__":
    unittest.main()
