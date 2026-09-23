from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from argparse import Namespace
import json
from types import SimpleNamespace
from contextlib import asynccontextmanager
from unittest.mock import patch

from qdl.certification.phase3_consumer_load import LogicalConsumerSession


_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "phase3_consumer_load_acceptance.py"
_SPEC = importlib.util.spec_from_file_location("phase3_consumer_load_driver", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


class Phase3ConsumerLoadDriverTests(unittest.TestCase):
    def _profile(self, root: Path) -> dict[str, object]:
        runtime = root / "runtime"
        runtime.mkdir()
        identity = root / "identity"
        identity.mkdir()
        values = {}
        for name in ("ca.crt", "client.crt", "client.key", "private.key"):
            path = identity / name
            path.write_text(name, encoding="utf-8")
            values[name] = str(path)
        return {
            "image": "qdl-v2-python:2.1.1-test",
            "network": "qdl_v2_stable_candidate_default",
            "runtime_dir": str(runtime),
            "queries": ["https://query_v2_1:8200", "https://query_v2_2:8200"],
            "stream_targets": ["stream_v2_active:8210", "stream_v2_passive:8210"],
            "query_containers": ["qdl_v2_stable_candidate-query_v2_1-1", "qdl_v2_stable_candidate-query_v2_2-1"],
            "identities": [{
                "id": "alpha.binance.paper.stable",
                "tls": {
                    "ca_file": values["ca.crt"],
                    "cert_file": values["client.crt"],
                    "key_file": values["client.key"],
                },
                "jwt": {"private_key_file": values["private.key"], "key_id": "test-key"},
            }],
        }

    def test_disposable_command_is_bounded_read_only_and_secret_paths_are_not_exposed_inside(self):
        with tempfile.TemporaryDirectory() as temporary:
            profile = _MODULE.validate_profile(self._profile(Path(temporary)))
            command, inner = _MODULE.docker_command(
                profile,
                name="qdl-phase3-load-unit",
                image_id="sha256:" + "a" * 64,
                mode="load",
                sessions=5,
                duration_seconds=60,
            )
        self.assertIn("--read-only", command)
        self.assertIn("--security-opt", command)
        self.assertIn("no-new-privileges", command)
        self.assertIn("--memory", command)
        self.assertIn("512m", command)
        self.assertIn("--cpus", command)
        self.assertIn("1.0", command)
        self.assertIn("--pids-limit", command)
        self.assertIn("PYTHONPATH=/app:/driver", command)
        self.assertNotIn("--privileged", command)
        self.assertFalse(any("docker.sock" in value for value in command))
        self.assertTrue(any(
            "/app/qdl/certification/phase3_consumer_load.py" in value
            for value in command
        ))
        self.assertFalse(any("/driver/qdl" in value for value in command))
        identity = inner["identities"][0]
        self.assertTrue(identity["tls"]["ca_file"].startswith("/tmp/identity/"))
        self.assertTrue(identity["jwt"]["private_key_file"].startswith("/tmp/identity/"))

    def test_profile_rejects_unknown_field_before_docker_is_called(self):
        with tempfile.TemporaryDirectory() as temporary:
            profile = self._profile(Path(temporary))
            profile["unexpected"] = "not-permitted"
            with self.assertRaisesRegex(ValueError, "incomplete or unknown"):
                _MODULE.validate_profile(profile)

    def test_profile_rejects_identity_path_outside_managed_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            profile = self._profile(Path(temporary))
            profile["identities"][0]["tls"]["ca_file"] = "/etc/passwd"
            with self.assertRaisesRegex(ValueError, "identity file is unavailable"):
                _MODULE.validate_profile(profile)

    def test_percentiles_do_not_invent_p99_from_small_sample(self):
        result = _MODULE._percentiles([1.0, 2.0, 3.0])
        self.assertEqual(result["n"], 3)
        self.assertEqual(result["p50_ms"], 2.0)
        self.assertIsNone(result["p99_ms"])

    def test_slot_backed_pacer_constructs_and_records_a_local_acquire(self):
        async def exercise():
            pacer = _MODULE._Pacer(1.0)
            await pacer.acquire("snapshot")
            return pacer.evidence()

        evidence = asyncio.run(exercise())
        self.assertEqual(evidence["operations"], {"snapshot": 1})
        self.assertEqual(evidence["queue_wait_ms"], 0.0)

    def test_handoff_reserves_consecutive_snapshot_and_subscribe_quota_slots(self):
        class Clock:
            now = 0.0

            def __init__(self):
                self.waits: list[float] = []

            def read(self):
                return self.now

            async def sleep(self, seconds: float):
                self.waits.append(seconds)
                self.now += seconds

        async def exercise():
            clock = Clock()
            pacer = _MODULE._Pacer(3.0, clock=clock.read, sleep=clock.sleep)
            async with pacer.handoff():
                await pacer.acquire("snapshot")
                await pacer.acquire("stream_subscribe")
            await pacer.acquire("reference_batch")
            return clock.waits, pacer.evidence()

        waits, evidence = asyncio.run(exercise())
        self.assertEqual(waits, [3.0, 3.0])
        self.assertEqual(
            evidence["operations"],
            {"reference_batch": 1, "snapshot": 1, "stream_subscribe": 1},
        )
        self.assertEqual(evidence["handoff_reservations"], 1)
        self.assertEqual(evidence["incomplete_handoffs"], 0)

    def test_handoff_rejects_interleaving_and_conservatively_keeps_failed_slot(self):
        class Clock:
            now = 0.0

            def __init__(self):
                self.waits: list[float] = []

            def read(self):
                return self.now

            async def sleep(self, seconds: float):
                self.waits.append(seconds)
                self.now += seconds

        async def exercise():
            clock = Clock()
            pacer = _MODULE._Pacer(2.0, clock=clock.read, sleep=clock.sleep)
            async with pacer.handoff():
                await pacer.acquire("warmup")
                with self.assertRaisesRegex(RuntimeError, "expected stream subscribe"):
                    await pacer.acquire("reference_batch")
            await pacer.acquire("snapshot")
            return clock.waits, pacer.evidence()

        waits, evidence = asyncio.run(exercise())
        self.assertEqual(waits, [4.0])
        self.assertEqual(evidence["operations"], {"snapshot": 1, "warmup": 1})
        self.assertEqual(evidence["handoff_reservations"], 1)
        self.assertEqual(evidence["incomplete_handoffs"], 1)

    def test_measurement_excludes_quota_wait_and_summary_keeps_it_separate(self):
        async def exercise():
            pacer = _MODULE._Pacer(0.001)
            token = pacer.begin_measurement()
            await pacer.acquire("first")
            await pacer.acquire("second")
            return pacer.finish_measurement(token)

        queue_wait_ms = asyncio.run(exercise())
        summary = _MODULE._summarize_samples([{
            "group": ("SNAPSHOT", "query-1", "BINANCE", "BTCUSDT", "TRADE", ""),
            "usable_ms": 12.5,
            "queue_wait_ms": queue_wait_ms,
            "response_payload_bytes": 321,
        }])
        self.assertEqual(summary[0]["usable_latency"]["p50_ms"], 12.5)
        self.assertEqual(summary[0]["client_pacing_wait"]["n"], 1)
        self.assertEqual(summary[0]["response_payload"]["total_bytes"], 321)

    def test_matrix_selection_reports_all_required_venue_symbol_pairs(self):
        def product(venue: str, symbol: str, feed: str):
            return SimpleNamespace(
                consumer_id="trading-system.paper.stable",
                venue=venue,
                native_symbol=symbol,
                feed=SimpleNamespace(value=feed),
                interval=None,
                identity=("trading-system.paper.stable", venue, symbol, feed, ""),
            )

        products = tuple(
            product(venue, symbol, "QUOTE")
            for venue, symbols in _MODULE._FIVE_LIQUID.items()
            for symbol in symbols
        )
        session = LogicalConsumerSession(
            1, "trading-system.paper.stable", products[:2]
        )
        selected = _MODULE._matrix_selection(
            sessions=(session,), execution_products=products
        )
        pairs = {(item.venue, item.native_symbol) for item in selected}
        self.assertEqual(len(pairs), 10)

    def test_final_bar_probe_uses_the_shortest_declared_durable_interval(self):
        def product(feed: str, interval: str | None, symbol: str):
            return SimpleNamespace(
                consumer_id="alpha.binance.paper.stable",
                venue="BINANCE",
                native_symbol=symbol,
                feed=SimpleNamespace(value=feed),
                delivery=SimpleNamespace(value="DURABLE"),
                interval=interval,
                identity=("alpha.binance.paper.stable", "BINANCE", symbol, feed, interval or ""),
            )

        selected = _MODULE._final_bar_product((
            product("BAR", "15m", "ETHUSDT"),
            product("BAR", "5m", "BTCUSDT"),
            product("TRADE", None, "BTCUSDT"),
        ))
        self.assertIsNotNone(selected)
        self.assertEqual(selected.interval, "5m")
        self.assertEqual(selected.native_symbol, "BTCUSDT")

    def test_live_only_consumer_uses_continuity_not_false_final_bar_proof(self):
        def product(feed: str):
            return SimpleNamespace(
                consumer_id="monitoring.multivenue.stable",
                venue="OKX",
                native_symbol="BTC-USDT-SWAP",
                feed=SimpleNamespace(value=feed),
                delivery=SimpleNamespace(value="DURABLE"),
                interval=None,
                identity=("monitoring.multivenue.stable", "OKX", "BTC-USDT-SWAP", feed, ""),
            )

        values = (product("TRADE"), product("QUOTE"))
        self.assertIsNone(_MODULE._final_bar_product(values))
        spec = _MODULE._supplemental_stream_spec(
            "monitoring.multivenue.stable", values
        )
        self.assertEqual(spec.purpose, "CONTINUITY")
        self.assertEqual(spec.name, "continuity-monitoring.multivenue.stable")
        self.assertEqual(spec.product.feed.value, "TRADE")

    def test_stream_specs_keep_one_supplemental_probe_per_identity(self):
        def product(consumer_id: str, feed: str, interval: str | None = None):
            return SimpleNamespace(
                consumer_id=consumer_id,
                venue="BINANCE",
                native_symbol="BTCUSDT",
                feed=SimpleNamespace(value=feed),
                delivery=SimpleNamespace(value="DURABLE"),
                interval=interval,
                identity=(consumer_id, "BINANCE", "BTCUSDT", feed, interval or ""),
            )

        alpha = "alpha.binance.paper.stable"
        monitor = "monitoring.multivenue.stable"
        alpha_products = (product(alpha, "BAR", "1m"), product(alpha, "TRADE"))
        monitor_products = (product(monitor, "TRADE"), product(monitor, "QUOTE"))
        plan = SimpleNamespace(logical_sessions=(
            LogicalConsumerSession(1, alpha, alpha_products),
            LogicalConsumerSession(2, monitor, monitor_products),
        ))
        specs = _MODULE._build_stream_specs(
            plan,
            {alpha: alpha_products, monitor: monitor_products},
        )
        supplemental = {spec.consumer_id: spec for spec in specs if spec.purpose != "LOGICAL"}
        self.assertEqual(len(specs), 4)
        self.assertEqual(supplemental[alpha].purpose, "FINAL_BAR")
        self.assertEqual(supplemental[monitor].purpose, "CONTINUITY")

    def test_stream_buffer_comes_from_the_sealed_identity_quota(self):
        self.assertEqual(
            _MODULE._stream_buffer_bound(SimpleNamespace(max_buffer_events=2000)),
            2000,
        )
        with self.assertRaisesRegex(ValueError, "sealed stream buffer quota"):
            _MODULE._stream_buffer_bound(SimpleNamespace(max_buffer_events=0))
        with self.assertRaisesRegex(ValueError, "sealed stream buffer quota"):
            _MODULE._stream_buffer_bound(SimpleNamespace(max_buffer_events=10_001))

    def test_stream_quality_diagnostic_is_bounded_and_separates_event_from_receive_age(self):
        event = SimpleNamespace(
            logical_offset=17,
            event=SimpleNamespace(
                source_event_time_ns=1_000_000_000,
                received_at_ns=3_500_000_000,
                quality_flags=(),
                connection_generation=4,
                lease_epoch=7,
                authority_revision=9,
                config_revision=10,
                price="must-not-appear",
                source_session_id="must-not-appear",
            ),
        )
        requirement = SimpleNamespace(
            max_freshness_ms=15_000,
            effective_event_recency_policy=SimpleNamespace(value="BLOCK"),
            max_session_liveness_ms=None,
            stale_policy=SimpleNamespace(value="BLOCK"),
            gap_policy=SimpleNamespace(value="BLOCK"),
        )

        result = _MODULE._stream_frame_quality_diagnostic(
            event, requirement, now_ns=5_000_000_000
        )

        self.assertEqual(result["logical_offset"], 17)
        self.assertEqual(result["source_event_age_ms"], 4_000)
        self.assertEqual(result["receive_age_ms"], 1_500)
        self.assertEqual(result["max_freshness_ms"], 15_000)
        self.assertEqual(result["connection_generation"], 4)
        self.assertEqual(result["quality_flags"], [])
        encoded = json.dumps(result, sort_keys=True)
        self.assertNotIn("must-not-appear", encoded)
        self.assertNotIn("price", encoded.lower())

    def test_stream_quality_diagnostic_never_raises_for_incomplete_frame(self):
        result = _MODULE._stream_frame_quality_diagnostic(
            SimpleNamespace(logical_offset="not-an-offset", event=SimpleNamespace()),
            SimpleNamespace(),
            now_ns=5_000_000_000,
        )
        self.assertIsNone(result["logical_offset"])
        self.assertIsNone(result["source_event_age_ms"])
        self.assertIsNone(result["receive_age_ms"])
        self.assertEqual(result["quality_flags"], [])

    def test_quiet_stream_poll_does_not_cancel_pending_read(self):
        async def exercise():
            stopped = asyncio.Event()
            release = asyncio.Event()
            cancelled = []

            async def events():
                try:
                    await release.wait()
                    yield "actual-event"
                finally:
                    cancelled.append(True)

            stream = events()
            reader = _MODULE._stream_events_until_stop(stream, stopped, poll_seconds=0.001)
            waiting = asyncio.create_task(reader.__anext__())
            await asyncio.sleep(0.015)
            self.assertFalse(waiting.done())
            self.assertFalse(cancelled)
            release.set()
            event, _, _ = await asyncio.wait_for(waiting, 1)
            self.assertEqual(event, "actual-event")
            stopped.set()
            await reader.aclose()
            await stream.aclose()
        asyncio.run(exercise())

    def test_quiet_stream_shutdown_joins_pending_read(self):
        async def exercise():
            stopped = asyncio.Event()
            entered = asyncio.Event()
            closed = asyncio.Event()

            async def events():
                try:
                    entered.set()
                    await asyncio.Event().wait()
                    yield "unreachable"
                finally:
                    closed.set()

            stream = events()
            reader = _MODULE._stream_events_until_stop(stream, stopped, poll_seconds=0.001)
            waiting = asyncio.create_task(reader.__anext__())
            await entered.wait()
            stopped.set()
            with self.assertRaises(StopAsyncIteration):
                await asyncio.wait_for(waiting, 1)
            self.assertTrue(closed.is_set())
        asyncio.run(exercise())

    def test_stream_timeliness_is_not_reported_as_request_latency(self):
        result = _MODULE._summarize_samples([{
            "group": ("STREAM", "replicated", "OKX", "SOL-USDT-SWAP", "BAR", "1m"),
            "validation_ms": 0.5,
            "source_to_usable_ms": 123.0,
            "host_receive_to_usable_ms": 45.0,
            "interarrival_ms": 60_000.0,
        }])[0]
        self.assertEqual(result["usable_latency"]["n"], 0)
        self.assertEqual(result["source_to_usable"]["p50_ms"], 123.0)
        self.assertEqual(result["stream_interarrival"]["p50_ms"], 60_000.0)

    def test_load_samples_remain_bounded_with_exact_population_counts(self):
        samples = _MODULE._BoundedSamples(per_group=8)
        group = ("STREAM", "replicated", "OKX", "SOL-USDT-SWAP", "TRADE", "")
        for value in range(10_000):
            samples.append({"group": group, "validation_ms": float(value)})
        self.assertEqual(len(list(samples)), 8)
        result = _MODULE._summarize_samples(samples)[0]
        self.assertEqual(result["observed_samples"], 10_000)
        self.assertEqual(result["sampling"], "bounded_reservoir")

    def test_load_drains_early_stream_before_later_stream_finishes_opening(self):
        from qdl_sdk.models import StreamEvent

        async def exercise():
            drained = asyncio.Event()
            product = SimpleNamespace(consumer_id="test", venue="OKX", native_symbol="SOL-USDT-SWAP",
                                      feed=SimpleNamespace(value="TRADE"), interval=None,
                                      delivery=SimpleNamespace(value="DURABLE"), source_policy_id="fixture")
            specs = [SimpleNamespace(name=name, consumer_id="test", product=product, slow=False, purpose="CONTINUITY")
                     for name in ("early", "late")]
            clients = []

            class Session:
                warmup = SimpleNamespace(data=[object()])
                count = 0

                def __init__(self, ordinal):
                    self.ordinal = ordinal

                async def __anext__(self):
                    self.count += 1
                    if self.ordinal == 1 and self.count == 1:
                        await asyncio.wait_for(drained.wait(), 0.2)
                    await asyncio.sleep(0.001)
                    return StreamEvent(self.count, "fixture", SimpleNamespace(source_event_time_ns=1, received_at_ns=1))

                def acknowledge(self, event):
                    if self.ordinal == 0 and event.logical_offset >= 2:
                        drained.set()

            class Client:
                def __init__(self, ordinal):
                    self.session = Session(ordinal)
                    self.closed = False

                async def close(self):
                    self.closed = True

            def make_client(*args, **kwargs):
                client = Client(len(clients))
                clients.append(client)
                return client

            @asynccontextmanager
            async def handoff(client, *args):
                yield client.session

            with patch.object(_MODULE, '_build_stream_specs', return_value=specs), \
                 patch.object(_MODULE, '_make_client', side_effect=make_client), \
                 patch.object(_MODULE, '_paced_warmup_then_stream', side_effect=handoff), \
                 patch('qdl.certification.phase103_consumer_acceptance.sdk_requirement', return_value=object()), \
                 patch('qdl.certification.phase103_consumer_acceptance.validate_product_view'), \
                 patch('qdl_sdk.projection.market_data_view_from_stream', return_value=object()):
                _, errors, counters, _ = await _MODULE._run_load(
                    plan=SimpleNamespace(logical_sessions=(), identity_budgets=[SimpleNamespace(planned_streams=2, seconds_per_request=0.01)]),
                    products_by_consumer={}, identities={'test': object()}, queries=[], stream_targets=[],
                    pacers={'test': object()}, duration_seconds=0.01,
                )
            self.assertEqual(errors, [])
            self.assertTrue(drained.is_set())
            self.assertGreater(counters['stream_event:early'], 1)
            self.assertTrue(all(client.closed for client in clients))
        asyncio.run(exercise())

    def test_runtime_guard_detects_restart_oom_and_deployment_drift(self):
        initial = {'reader': {'id': 'one', 'image': 'fixed', 'running': True, 'oom': False, 'restarts': 0, 'health': 'healthy'}}
        self.assertIsNone(_MODULE._runtime_fault(initial, initial))
        for change in ({'oom': True}, {'running': False}, {'restarts': 1}, {'id': 'two'}, {'health': 'unhealthy'}):
            with self.subTest(change=change):
                self.assertIsNotNone(_MODULE._runtime_fault(initial, {'reader': {**initial['reader'], **change}}))

    def test_cold_workload_requires_declared_large_history_on_both_venues(self):
        def product(consumer, symbol, limit):
            return SimpleNamespace(consumer_id=consumer, native_symbol=symbol, feed=SimpleNamespace(value='BAR'),
                                   interval='1m', delivery=SimpleNamespace(value='DURABLE'),
                                   requirement=SimpleNamespace(warmup_limit=limit))
        binance = 'alpha.binance.paper.stable'
        okx = 'alpha.okx.paper.stable'
        products = {binance: [product(binance, 'BTCUSDT', 10000)], okx: [product(okx, 'BTC-USDT-SWAP', 10000)]}
        self.assertEqual(len(_MODULE._cold_history_selection(products)), 2)
        products[okx] = [product(okx, 'BTC-USDT-SWAP', 100)]
        with self.assertRaisesRegex(ValueError, 'no declared 5000-row'):
            _MODULE._cold_history_selection(products)

    def test_host_refuses_partial_identity_scope_before_docker_is_called(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            profile = root / "profile.json"
            profile.write_text(json.dumps(self._profile(root)), encoding="utf-8")
            args = Namespace(
                profile=profile,
                output=root / "output",
                mode="matrix",
                sessions=5,
                duration_seconds=0,
            )
            with self.assertRaisesRegex(ValueError, "exactly the four approved"):
                _MODULE.run_host(args)

    def test_bootstrap_transfers_tmpfs_identity_ownership_before_privilege_drop(self):
        script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "phase3_consumer_load_bootstrap.sh"
        ).read_text(encoding="utf-8")
        self.assertIn("chown -R 10001:10001 /tmp/identity", script)
        self.assertIn("chmod -R u=rwX,go= /tmp/identity", script)
        self.assertLess(
            script.index("chown -R 10001:10001 /tmp/identity"),
            script.index("exec setpriv"),
        )


if __name__ == "__main__":
    unittest.main()
