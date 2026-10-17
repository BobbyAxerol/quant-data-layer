"""scripts/p184_dr05_quiet.py: TRADE lane seal, session-liveness records the Query reads, channel ops, argv.

Offline (python image: qdl, yaml, cryptography); no Docker, no broker (the producer is a fake)."""
import copy
import io
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock

from scripts import p184_dr05_dl_fault as dr
from scripts import p184_dr05_quiet as quiet

NS = "p184-dr05-dl-0123456789abcdef"


def sealed_work(tmp):
    from tests.test_p184_dr05_dl_fault import MaterialTests
    work = Path(tmp) / "work"
    work.mkdir()
    (work / "handoff.json").write_text(json.dumps({"namespace": NS, "provenance": "TEST_ONLY",
                                                   "images": dict(dr.IMAGES)}))
    dr.material(work, MaterialTests.exported())
    return work, quiet.seal(work)


class SealTests(unittest.TestCase):
    def test_seal_adds_one_trade_lane_per_scope_with_the_ts_paper_trade_requirement(self):
        from qdl.consumer.manifest import ConsumerManifestLoader
        from qdl.runtime.stable_catalog import StableSourceCatalog
        from scripts.kn_gateway_bundle import compile_bundle
        with tempfile.TemporaryDirectory() as tmp:
            work, sealed = sealed_work(tmp)
            self.assertEqual(sealed["feeds"], ["BOOK_SNAPSHOT", "BOOK_DELTA", "TRADE"])
            self.assertEqual(sealed["trade_requirement"]["event_recency_policy"], "OBSERVE")
            self.assertEqual(sealed["sessions"], {"BINANCE": NS + ".BINANCE.trade", "OKX": NS + ".OKX.trade"})
            config = json.loads((work / "core.json").read_text())
            trades = quiet.trade_bindings(config)
            self.assertEqual({v: (b["provider_kind"], b["native_channel"]) for v, b in trades.items()},
                             {"BINANCE": ("binance_usdm_trade", "ethusdt@trade"), "OKX": ("okx_trade", "trades")})
            self.assertEqual(len(config["core"]["bindings"]), 4)  # 2 BOOK + 2 TRADE
            self.assertEqual(config["authority"]["contract_digest"], sealed["core_bindings_sha256"])
            self.assertEqual(len(StableSourceCatalog.load(work / "catalog.yaml").bindings), 4 + 2)
            manifests = [work / "binance.yaml", work / "okx.yaml"]
            for path in manifests:
                manifest = ConsumerManifestLoader.load(path)
                trade = next(r for r in manifest.requirements if r.feed.value == "TRADE")
                # = DL consumers/stable/trading-system-paper.yaml r10 TRADE (the deployed TS paper consumer)
                value = lambda v: getattr(v, "value", v)  # noqa: E731 - enums or plain strings
                self.assertEqual((trade.max_freshness_ms, value(trade.event_recency_policy),
                                  trade.max_session_liveness_ms, trade.source_policy_id, trade.warmup_limit,
                                  value(trade.stale_policy), value(trade.gap_policy), value(trade.recovery)),
                                 (3000, "OBSERVE", 45000, "crypto_primary_v2", 0, "BLOCK", "BLOCK",
                                  "SNAPSHOT_AND_REPLAY"))
            for label, symbol in (("binance", "ETHUSDT"), ("okx", "ETH-USDT-SWAP")):
                route = json.loads((work / (label + ".binding.json")).read_text())
                product = next(p for p in route["products"] if p["feed"] == "TRADE")
                self.assertEqual((product["native_symbol"], product["provider_plane"]), (symbol, "REALTIME"))
                policy = next(p for p in route["request_policies"] if p["requirement_id"] == product["requirement_id"])
                self.assertEqual((policy["event_recency_policy"], policy["max_session_liveness_ms"]), ("OBSERVE", 45000))
            self.assertEqual(json.loads((work / "bundle.json").read_text()), compile_bundle(
                environment="paper", catalog_path=work / "catalog.yaml", manifest_paths=manifests))
            self.assertEqual(sorted(p.name for p in (work / "session-liveness").iterdir()), ["binance-usdm", "okx-swap"])
            self.assertEqual(dr.query_entry(work), "scripts/p184_dr05_quiet.py")
            self.assertIn("scripts/p184_dr05_quiet.py", dr.topology(NS, work)["query"])
            with self.assertRaises(ValueError):  # sealed once
                quiet.seal(work)

    def test_unsealed_material_keeps_the_dr05_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "work"
            work.mkdir()
            (work / "handoff.json").write_text(json.dumps({"namespace": NS}))
            self.assertEqual(dr.query_entry(work), "scripts/p184_dr05_observer.py")
            self.assertIn("scripts/p184_dr05_observer.py", dr.topology(NS, work)["query"])

    def test_inverse_scope_material_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "work"
            work.mkdir()
            (work / "handoff.json").write_text(json.dumps({"namespace": NS, "material_ready": True,
                                                           "scopes": ["BINANCE", "OKX", "OKX_INVERSE"]}))
            with self.assertRaises(ValueError):
                quiet.seal(work)


class LivenessTests(unittest.TestCase):
    def test_record_is_read_by_the_query_session_reader_as_live_then_ages(self):
        from qdl.runtime.session_liveness import StableSessionLivenessReader
        binding = {"venue": "BINANCE", "market": "USDM"}
        with tempfile.TemporaryDirectory() as tmp:
            path = quiet.liveness_path(tmp, NS, binding)
            path.parent.mkdir(parents=True)
            now = time.time_ns()
            quiet.write_atomic(path, quiet.liveness_record(NS, "BINANCE", now))
            self.assertEqual(path.name, NS + ".binance.trade.json")
            reader = StableSessionLivenessReader(Path(tmp) / "session-liveness")
            status = reader.status(venue="BINANCE", market="USDM", source_session_id=NS + ".BINANCE.trade",
                                   connection_generation=1, config_revision=1, now_ns=now + 400_000_000)
            self.assertEqual((status.state, status.liveness_ms, status.flags), ("LIVE", 400, ()))
            later = reader.status(venue="BINANCE", market="USDM", source_session_id=NS + ".BINANCE.trade",
                                  connection_generation=1, config_revision=1, now_ns=now + 50_000_000_000)
            self.assertEqual(later.liveness_ms, 50_000)  # no heartbeat -> ages past the 45 s bound
            other = reader.status(venue="BINANCE", market="USDM", source_session_id=NS + ".BINANCE.book",
                                  connection_generation=1, config_revision=1, now_ns=now)
            self.assertEqual(other.state, "UNKNOWN")  # never vouches for another session
            self.assertFalse([p for p in path.parent.iterdir() if p.name.endswith(".tmp")])

    def test_record_validation(self):
        with self.assertRaises(ValueError):
            quiet.liveness_record(NS, "BINANCE", 0)
        with self.assertRaises(ValueError):
            quiet.liveness_record(NS, "BINANCE", 1, state="STALE")


class FakeProducer:
    def __init__(self):
        self.sent, self.flushed = [], 0

    def produce(self, topic, partition, key, value, headers, on_delivery):
        self.sent.append((topic, partition, key, value))
        message = mock.Mock()
        message.offset.return_value = len(self.sent) - 1
        on_delivery(None, message)

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        self.flushed += 1
        return 0


class ChannelTests(unittest.TestCase):
    def channel(self, tmp):
        work, _ = sealed_work(tmp)
        packet = json.loads((work / "packet.json").read_text())
        packet["slice"]["topic_id"] = "t" * 22
        (work / "packet.json").write_text(json.dumps(packet))
        producer = FakeProducer()
        with mock.patch.dict(os.environ, {"P184_DR05_ISOLATED_NAMESPACE": NS}):
            return work, producer, quiet.QuietChannel(work, producer_factory=lambda: producer)

    def test_session_heartbeat_and_trades_on_their_own_lane(self):
        from qdl.raw.envelope import validate_raw_envelope
        from scripts.p184_g12_isolated_fault import trade_frame
        with tempfile.TemporaryDirectory() as tmp:
            work, producer, channel = self.channel(tmp)
            try:
                started = channel.handle("session", {"venue": "OKX", "live": True})
                self.assertEqual(started["session_id"], NS + ".OKX.trade")
                record = json.loads(quiet.liveness_path(work, NS, channel.bindings["OKX"]).read_text())
                self.assertEqual((record["state"], record["connection_generation"], record["config_revision"]),
                                 ("LIVE", 1, 1))
                with self.assertRaises(ValueError):
                    channel.handle("session", {"venue": "OKX", "live": True})  # one heartbeat per venue
                channel.handle("trade_run", {"venue": "OKX", "hz": 10})
                time.sleep(0.35)
                stop = channel.handle("trade_stop", {"venue": "OKX"})
                self.assertGreaterEqual(stop["published"], 2)
                self.assertEqual((stop["acked"], stop["errors"], stop["loop_error"]), (stop["published"], [], None))
                self.assertEqual(stop["last_trade_id"], stop["published"])
                time.sleep(0.6)  # the heartbeat continues after the TRADE source stopped
                status = channel.handle("status", {})
                self.assertEqual((status["sessions"], status["trades"]), (["OKX"], []))
                self.assertGreaterEqual(status["stats"]["OKX"]["heartbeats"], 2)
                self.assertEqual(status["stats"]["OKX"]["published"], stop["published"])
                raw = quiet.trade_envelope(channel.bindings["OKX"], trade_frame("OKX", 7, time.time_ns()), NS)
                validate_raw_envelope(raw)
                self.assertEqual(producer.sent[0][2], raw.subscription_id.encode())
                with self.assertRaises(ValueError):  # a trade frame never goes on another venue's lane
                    quiet.trade_envelope(channel.bindings["BINANCE"], trade_frame("OKX", 8, time.time_ns()), NS)
                trade = channel.bindings["OKX"]
                self.assertEqual((raw.source_session_id, raw.connection_generation, raw.native_channel,
                                  raw.subscription_id, raw.test_provenance, raw.config_revision),
                                 (NS + ".OKX.trade", 1, "trades", trade["source_id"], True, 1))
                self.assertEqual(producer.sent[0][:2], (NS + ".raw", 0))
                stopped = channel.handle("session", {"venue": "OKX", "live": False})
                self.assertFalse(stopped["live"])
                with self.assertRaises(RuntimeError):
                    channel.handle("trade_stop", {"venue": "OKX"})
                for bad in (("trade_run", {"venue": "OKX", "hz": 0}), ("trade_run", {"venue": "OKX", "hz": True}),
                            ("session", {"venue": "OKX", "live": "yes"}), ("session", {"venue": "OKX_INVERSE"}),
                            ("drop", {})):
                    with self.subTest(bad=bad), self.assertRaises(ValueError):
                        channel.handle(*bad)
            finally:
                channel.close()

    def test_jsonl_protocol_replies_one_line_per_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            work, producer, channel = self.channel(tmp)
            channel.close()
            out = io.StringIO()
            stdin = io.StringIO('{"op": "status"}\nnot json\n{"op": "session", "args": {"venue": "X", "live": true}}\n')
            with mock.patch.dict(os.environ, {"P184_DR05_ISOLATED_NAMESPACE": NS}):
                quiet.serve_channel(work, stdin=stdin, stdout=out, producer_factory=lambda: producer)
            replies = [json.loads(line) for line in out.getvalue().splitlines()]
            self.assertEqual([r["ok"] for r in replies], [True, False, False])
            self.assertEqual(replies[2]["error"], "ValueError:unknown venue")

    def test_channel_needs_the_namespace_attestation_and_the_seal(self):
        with tempfile.TemporaryDirectory() as tmp:
            work, producer, channel = self.channel(tmp)
            channel.close()
            with mock.patch.dict(os.environ, {"P184_DR05_ISOLATED_NAMESPACE": "p184-dr05-dl-ffffffffffffffff"}):
                with self.assertRaises(ValueError):
                    quiet.QuietChannel(work, producer_factory=lambda: producer)
            handoff = json.loads((work / "handoff.json").read_text())
            handoff.pop(quiet.MARK)
            (work / "handoff.json").write_text(json.dumps(handoff))
            with mock.patch.dict(os.environ, {"P184_DR05_ISOLATED_NAMESPACE": NS}):
                with self.assertRaises(ValueError):
                    quiet.QuietChannel(work, producer_factory=lambda: producer)


class ArgvTests(unittest.TestCase):
    def test_channel_and_seal_run_this_module_in_the_dr05_helper(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "work"
            work.mkdir()
            (work / "handoff.json").write_text(json.dumps({"namespace": NS}))
            argv = quiet.channel_argv(work)
            self.assertEqual(argv[:3], ["docker", "run", "-i"])
            self.assertIn("container:" + NS + "-kafka", argv)
            self.assertIn("P184_DR05_ISOLATED_NAMESPACE=" + NS, argv)
            self.assertEqual(argv[-2:], ["--channel", "/work"])
            self.assertIn("scripts/p184_dr05_quiet.py", argv)
            self.assertNotIn("scripts/p184_dr05_dl_fault.py", argv)
            _, seal = quiet.helper_argv(work, ["--seal", "/work"], network="none")
            self.assertEqual(seal[seal.index("--network") + 1], "none")
            self.assertEqual(seal[-2:], ["--seal", "/work"])


if __name__ == "__main__":
    unittest.main()
