"""Offline contracts of the G12 isolated fault harness; no Docker daemon or broker."""
import json
from pathlib import Path
import re
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import patch

from scripts import p184_g12_isolated_fault as g12

NS = "p184-g12-dl-0123456789abcdef"
S = 1_000_000_000  # one second in ns
MS = 1_000_000


def stream_rows(venue, start, end, *, step_ms=250, transit_ms=100, approved=True, first_seq=1001):
    """Publisher rows every step_ms and sampler rows showing each frame transit_ms later."""
    pubs, views = [], []
    t, seq = start, first_seq
    while t < end:
        pubs.append({"k": "pub", "t": t, "venue": venue, "seq": seq, "gen": 7, "snapshot": False})
        views.append({"k": "view", "t": t + transit_ms * MS, "venue": venue, "seq": seq,
                      "v": "APPROVED" if approved else "ExecutionMarketContextError:STALE"})
        t += step_ms * MS
        seq += 1
    return pubs, views


def md_rows(venue, start, end, *, trade_ms=100, quote_ms=100, bar_ms=800, mark_ms=300, step_ms=250, trades=True,
            quotes=True, bars=True, marks=True):
    """Publisher pub_md rows (a trade every step, a quote every other step, a mark every fourth step, a closed bar
    per minute) and the clone's applied notifications (k == ev) the given transit later."""
    pubs, evs = [], []
    t, n = start, 0
    while t < end:
        n += 1
        if trades:
            pubs.append({"k": "pub_md", "t": t, "venue": venue, "feed": "TRADE", "id": str(n)})
            evs.append({"k": "ev", "t": t + trade_ms * MS, "venue": venue, "feed": "TRADE", "id": str(n),
                        "ev_ms": t // MS})
        if quotes and n % 2 == 0:
            pubs.append({"k": "pub_md", "t": t, "venue": venue, "feed": "QUOTE", "id": str(t // MS)})
            evs.append({"k": "ev", "t": t + quote_ms * MS, "venue": venue, "feed": "QUOTE", "id": str(t // MS),
                        "ev_ms": t // MS})
        if marks and n % 4 == 0:
            pubs.append({"k": "pub_md", "t": t, "venue": venue, "feed": "MARK", "id": str(t // MS)})
            evs.append({"k": "ev", "t": t + mark_ms * MS, "venue": venue, "feed": "MARK", "id": str(t // MS),
                        "ev_ms": t // MS})
        t += step_ms * MS
    if bars:
        for close_ms in range((start // MS // 60_000 + 1) * 60_000, end // MS, 60_000):
            open_ms = close_ms - 60_000
            pubs.append({"k": "pub_md", "t": (close_ms + 200) * MS, "venue": venue, "feed": "BAR",
                         "id": str(open_ms), "close_ms": close_ms, "seed": False})
            evs.append({"k": "ev", "t": close_ms * MS + bar_ms * MS, "venue": venue, "feed": "BAR",
                        "id": str(open_ms), "ev_ms": open_ms})
    return pubs, evs


class NamespaceAndTopologyTests(unittest.TestCase):
    def test_namespace_is_exact(self):
        self.assertEqual(g12.namespace(NS), NS)
        for value in ("p184-dr05-dl-0123456789abcdef", NS + ";sh", "", None, "qdl_v2_stable_candidate"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                g12.namespace(value)

    def test_topology_is_disposable_isolated_and_bounded(self):
        roles = g12.topology(NS, "/home/bobby/.local/state/qdl-v2/g12-test/work", "TOPIC", uid="1001:1001")
        self.assertEqual(set(roles), {"net", "kafka-1", "kafka-2", "kafka-3", *g12.SERVICE_ORDER})
        for role, argv in roles.items():
            with self.subTest(role=role):
                for flag in ("--rm", "--pull=never", "--memory", "--cpus", "--pids-limit", "--log-opt", "--read-only"):
                    self.assertIn(flag, argv)
                self.assertEqual(argv[argv.index("--name") + 1], f"{NS}-{role}")
                self.assertIn(f"{g12.LABEL}={NS}", argv)
                network = argv[argv.index("--network") + 1]
                self.assertEqual(network, "none" if role == "net" else f"container:{NS}-net")
                self.assertFalse({"-p", "--publish", "--privileged"} & set(argv))
                self.assertFalse(any("qdl_v2_stable_candidate" in a or "/var/run/docker.sock" in a for a in argv))
                self.assertTrue(any(a.startswith("sha256:") for a in argv))
        # Brokers keep their log on a per-node bind dir so a restart is a real broker restart.
        self.assertIn("/home/bobby/.local/state/qdl-v2/g12-test/work/kafka-2:/data", roles["kafka-2"])
        self.assertIn("--ignore-formatted", roles["kafka-3"][-1])
        self.assertIn(f"QDL_KN_KAFKA_BOOTSTRAP={g12.BOOTSTRAP}", roles["stream"])
        self.assertIn("QDL_KN_TOPIC_ID=TOPIC", roles["projector"])

    def test_topology_refuses_work_inside_repository(self):
        with self.assertRaises(ValueError):
            g12.topology(NS, g12.ROOT / "work")

    def test_topics_are_rf3_min_isr2(self):
        wanted = g12.topics(NS)
        self.assertEqual(set(wanted), {NS + s for s in (".raw", ".canonical", ".quarantine", ".latest", ".bars")})
        self.assertTrue(all(cfg["min.insync.replicas"] == "2" for _, cfg in wanted.values()))
        self.assertEqual(wanted[NS + ".latest"][1]["cleanup.policy"], "compact")

    def test_broker_properties(self):
        password = g12.broker_password("a=b\nssl.keystore.password=s3cr3t\nssl.key.password=s3cr3t\n")
        self.assertEqual(password, "s3cr3t")
        with self.assertRaises(ValueError):
            g12.broker_password("ssl.key.password=x\n")
        text = g12.broker_properties(password, 2)
        self.assertIn("node.id=2\n", text)
        self.assertIn("listeners=SSL://:9192,CONTROLLER://:9193\n", text)
        self.assertIn("advertised.listeners=SSL://localhost:9192\n", text)
        self.assertIn("controller.quorum.voters=1@localhost:9093,2@localhost:9193,3@localhost:9293\n", text)
        for line in ("transaction.state.log.min.isr=2", "offsets.topic.replication.factor=3",
                     "min.insync.replicas=2", "unclean.leader.election.enable=false"):
            self.assertIn(line + "\n", text)
        with self.assertRaises(ValueError):
            g12.broker_properties(password, 4)

    def test_helpers_and_sampler_stay_in_namespace(self):
        argv = g12.helper_argv(NS, "/home/bobby/x/work", ["--topics", "/work"])
        self.assertNotIn("-d", argv)
        self.assertIn(f"container:{NS}-net", argv)
        self.assertIn("P184_G12_NAMESPACE=" + NS, argv)
        sampler = g12.sampler_argv(NS, "/home/bobby/x/evidence", 100)
        self.assertIn("-d", sampler)
        self.assertEqual(sampler[sampler.index("--name") + 1], NS + "-sampler")
        self.assertIn(g12.IMAGES["ts"], sampler)
        self.assertIn("BINANCE=redis://localhost:6391/1,OKX=redis://localhost:6392/1", sampler)

    def test_labelled_run_labels_every_detached_container(self):
        calls = []

        def fake(args, **_):
            calls.append(args)
            return type("R", (), {"returncode": 0, "stdout": "ok\n", "stderr": ""})()
        with patch.object(g12.subprocess, "run", side_effect=fake):
            run = g12.labelled_run(NS)
            run("docker", "run", "-d", "--rm", "--name", "x", "img")
            run("docker", "inspect", "x")
            run("docker", "rm", "-f", "x")
        self.assertEqual(calls[0][:5], ["docker", "run", "-d", "--label", f"{g12.LABEL}={NS}"])
        self.assertEqual(calls[1], ["docker", "inspect", "x"])
        self.assertEqual(calls[2], ["docker", "rm", "-f", "-v", "x"])  # no anonymous volume left


class AnalysisTests(unittest.TestCase):
    def test_caught_up_requires_post_clear_frame_age_and_hold(self):
        cleared = 100 * S
        pubs, views = stream_rows("BINANCE", 90 * S, 200 * S)
        index, series = g12.publish_index(pubs), g12.view_series(views)
        state = g12.caught_up(series, index, "BINANCE", cleared)
        self.assertEqual(state["first_valid_apply_ns"], cleared + 100 * MS)
        self.assertEqual(state["caught_up_ns"], cleared + 100 * MS)

    def test_stale_tail_delays_caught_up(self):
        cleared = 100 * S
        pubs, _ = stream_rows("OKX", 100 * S, 200 * S)
        # The consumer shows frames 1.5 s late until t=130 s, then 100 ms late.
        applied = sorted(({"k": "view", "t": p["t"] + (1500 if p["t"] < 130 * S else 100) * MS, "venue": "OKX",
                           "seq": p["seq"], "v": "APPROVED"} for p in pubs), key=lambda r: r["t"])
        views, newest = [], 0  # the view shows the newest applied frame; an older late apply stays hidden
        for row in applied:
            if row["seq"] > newest:
                views.append(row)
                newest = row["seq"]
        state = g12.caught_up(g12.view_series(views), g12.publish_index(pubs), "OKX", cleared)
        self.assertEqual(state["first_valid_apply_ns"], cleared + 1500 * MS)
        # Predicate v2: the 130.1 s sample shows frame 130.0 s right after frame 128.5 s (the late 128.75-129.75 s
        # applies stay hidden): a 1.5 s source gap past the emission due after 128.5 s (frame 129.75 s), so the
        # first sample proving cadence too is the next update's, 130.35 s.
        self.assertEqual(state["caught_up_ns"], 130 * S + 350 * MS)
        self.assertEqual(state["rejected_samples"]["update_transit"] >= 1, True)

    def test_not_approved_or_undecidable_is_not_caught_up(self):
        pubs, views = stream_rows("OKX", 100 * S, 200 * S, approved=False)
        state = g12.caught_up(g12.view_series(views), g12.publish_index(pubs), "OKX", 100 * S)
        self.assertIsNone(state["caught_up_ns"])
        self.assertIsNone(state["first_valid_apply_ns"])
        pubs, views = stream_rows("OKX", 100 * S, 110 * S)  # shorter than the 30 s hold
        self.assertIsNone(g12.caught_up(g12.view_series(views), g12.publish_index(pubs), "OKX", 100 * S)
                          ["caught_up_ns"])

    def test_frames_published_before_clear_do_not_count(self):
        pubs, views = stream_rows("OKX", 0, 200 * S)
        # The view is frozen on a pre-clear frame until 150 s.
        frozen = [v for v in views if v["t"] < 50 * S][-1]
        views = [dict(frozen, t=t) for t in range(50 * S, 150 * S, S // 2)] + [v for v in views if v["t"] >= 150 * S]
        state = g12.caught_up(g12.view_series(views), g12.publish_index(pubs), "OKX", 100 * S)
        self.assertEqual(state["caught_up_ns"], 150 * S + 100 * MS)

    def test_transit_and_g1_stats(self):
        pubs, views = stream_rows("BINANCE", 0, 100 * S, transit_ms=120)
        index, series = g12.publish_index(pubs), g12.view_series(views)
        values = g12.transit(series, index, "BINANCE", 0, 100 * S)
        self.assertEqual(len(values), 400)
        stats = g12.g1_stats(values)
        self.assertEqual((stats["p95_ms"], stats["max_ms"]), (120.0, 120.0))
        self.assertTrue(stats["pass"])
        self.assertFalse(g12.g1_stats([100.0] * 99 + [2500.0])["pass"])
        small = g12.g1_stats([100.0] * 10)
        self.assertIsNone(small["p99_ms"])
        self.assertFalse(g12.g1_stats([])["pass"])

    def test_cleared_from_monitor(self):
        rows = [{"t": 5, "cluster": {"broker_ids": [1, 3]}, "groups": {"kn-projector-v3-a": {"members": 1}},
                 "ports": {"8210": True}},
                {"t": 10, "cluster": {"broker_ids": [1, 3]}, "groups": {"kn-projector-v3-a": {"members": 0}},
                 "ports": {"8210": False}},
                {"t": 20, "cluster": {"broker_ids": [1, 2, 3]}, "groups": {"kn-projector-v3-a": {"members": 1}},
                 "ports": {"8210": True}}]
        self.assertEqual(g12.cleared_at("broker", rows, 8, 2), 20)
        self.assertEqual(g12.cleared_at("projector", rows, 8), 20)
        self.assertEqual(g12.cleared_at("stream", rows, 8), 20)
        self.assertIsNone(g12.cleared_at("broker", rows, 25, 2))

    def test_evaluate_fault_end_to_end(self):
        pubs, views = [], []
        for venue in g12.VENUES:
            p, v = stream_rows(venue, 0, 500 * S)
            pubs += p
            views += v
            p, v = md_rows(venue, 0, 500 * S)
            pubs += p
            views += v
        monitor = [{"t": t * S, "ports": {"8210": t >= 62}, "projector_lag": 0, "under_replicated": 0}
                   for t in range(0, 500)]
        fault = {"k": "fault", "name": "stream_restart", "kind": "stream", "injected_ns": 60 * S,
                 "restart_ns": 61 * S}
        result = g12.evaluate_fault(fault, pubs, views, monitor)
        self.assertEqual(result["fault_cleared_ns"], 62 * S)
        self.assertTrue(result["g12_caught_up_pass"])
        self.assertLess(result["caught_up_s"], 1)
        self.assertTrue(result["g1_window_complete"])
        self.assertTrue(result["pass"])
        self.assertEqual(result["g1"]["OKX"]["n"], 1200)
        self.assertEqual(result["parts"], {"caught_up": True, "window_complete": True, "g1": "PASS",
                                           "g2": "PASS", "g2_feeds": {"TRADE": "PASS", "QUOTE": "PASS",
                                                                      "MARK": "PASS"}, "g3": "PASS",
                                           "monitor": "PASS", "sampler": "PASS"})
        self.assertEqual(result["g2"]["TRADE"]["BINANCE"]["n"], 1200)
        self.assertEqual(result["g3"]["BAR_1m"]["OKX"]["n"], 5)  # closes 120..360 s in [62.1, 362.1) s
        short = g12.evaluate_fault(fault, pubs, [v for v in views if v["t"] < 200 * S], monitor)
        self.assertFalse(short["g1_window_complete"])
        self.assertFalse(short["pass"])
        never = g12.evaluate_fault(dict(fault, restart_ns=600 * S), pubs, views, monitor)
        self.assertEqual(never["reason"], "FAULT_NOT_CLEARED")


class GuardTests(unittest.TestCase):
    def test_guard_breach_and_external_recreate(self):
        before = {"a": [0, "t1", "running"], "b": [0, "t1", "running"], "c": [1, "t1", "running"]}
        now = {"a": [0, "t1", "running"], "b": [0, "t2", "running"], "c": [2, "t3", "running"]}
        breaches, recreated = g12.guard_breaches(before, now)
        self.assertEqual(set(breaches), {"c"})
        self.assertEqual(recreated, {"b": ["t1", "t2"]})
        breaches, _ = g12.guard_breaches(before, {"a": [0, "t1", "exited"], "c": [1, "t1", "running"]})
        self.assertEqual(set(breaches), {"a", "b"})

    def test_teardown_refuses_foreign_names(self):
        with patch.object(g12, "sh", return_value="qdl_v2_stable_candidate-kafka1-1\n"):
            with self.assertRaises(RuntimeError):
                g12.owned_names(NS)


FINAL = {"python": "sha256:" + "e" * 64, "core": "sha256:" + "6" * 64, "projector": "sha256:" + "6" * 64,
         "stream": "sha256:" + "6" * 64, "ts": "sha256:" + "3" * 64}


class ImageSetTests(unittest.TestCase):
    def test_defaults_file_and_overrides(self):
        import json
        import os
        import tempfile
        self.assertEqual(g12.resolve_image_set(), g12.IMAGES)
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(FINAL, handle)
        try:
            images = g12.resolve_image_set(handle.name, ["ts=sha256:" + "4" * 64])
        finally:
            os.unlink(handle.name)
        self.assertEqual(images["core"], FINAL["core"])
        self.assertEqual(images["ts"], "sha256:" + "4" * 64)  # --image applies last
        self.assertEqual(images["kafka"], g12.IMAGES["kafka"])  # unnamed roles keep the default
        self.assertEqual(set(images), set(g12.IMAGES))

    def test_rejects_unknown_role_mutable_ref_and_duplicates(self):
        for bad in ({"querry": FINAL["python"]}, {"python": "qdl-v2-python:latest"}, {"core": "sha256:abc"}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                g12.resolve_image_set(bad)
        with self.assertRaises(ValueError):
            g12.resolve_image_set(None, ["core=" + FINAL["core"], "core=" + FINAL["core"]])
        with self.assertRaises(ValueError):
            g12.resolve_image_set(None, ["core"])

    def test_topology_helpers_and_sampler_use_the_passed_set(self):
        images = g12.resolve_image_set(FINAL)
        roles = g12.topology(NS, "/home/bobby/x/work", "T", uid="1:1", images=images)
        for role, argv in roles.items():
            with self.subTest(role=role):
                self.assertIn(images[g12.role_image_key(role)], argv)
        helper = g12.helper_argv(NS, "/home/bobby/x/work", ["--topics", "/work"], images=images)
        self.assertIn(images["python"], helper)
        env = [a for a in helper if a.startswith("P184_G12_IMAGES=")]
        self.assertEqual(len(env), 1)
        self.assertEqual(g12.json.loads(env[0].split("=", 1)[1]), images)
        self.assertIn(images["ts"], g12.sampler_argv(NS, "/home/bobby/x/ev", 10, images=images))

    def test_query_code_from_image_has_no_worktree_mount(self):
        mounted = g12.topology(NS, "/home/bobby/x/work", uid="1:1")["query"]
        self.assertIn(f"{g12.ROOT}:/app:ro", mounted)
        own = g12.topology(NS, "/home/bobby/x/work", uid="1:1", query_code="image")["query"]
        volumes = [own[i + 1] for i, a in enumerate(own) if a == "-v"]
        self.assertFalse(any(v.split(":")[1] == "/app" for v in volumes), volumes)  # image's own /app
        self.assertEqual(own[own.index("-w") + 1], "/app")
        with self.assertRaises(ValueError):
            g12.topology(NS, "/home/bobby/x/work", uid="1:1", query_code="host")

    def test_role_image_key_and_dr05_roles(self):
        from scripts import p184_dr08_isolated as dr08
        expected = {"net": "holder", "kafka-2": "kafka", "quota": "redis", "core": "core", "projector": "projector",
                    "stream": "stream", "query": "python", "monitor": "python", "publisher": "python",
                    "sampler": "ts", "tswriter-okx": "ts", "tsredis-binance": "redis", "tspg-okx": "postgres"}
        for role, key in expected.items():
            self.assertEqual(g12.role_image_key(role), key, role)
        images = g12.resolve_image_set(FINAL)
        sealed = g12.dr05_images(images)
        self.assertEqual(set(sealed), set(dr08.IMAGES))  # dr05 image_set() requires exactly these roles
        self.assertEqual(sealed["core"], FINAL["core"])
        self.assertEqual(sealed["query"], FINAL["python"])

    def test_in_container_check_compares_passed_and_sealed(self):
        import json
        import tempfile
        from pathlib import Path
        images = g12.resolve_image_set(FINAL)
        with tempfile.TemporaryDirectory() as work:
            handoff = Path(work) / "handoff.json"
            handoff.write_text(json.dumps({"namespace": NS}))
            self.assertIsNone(g12.check_sealed_images(work, None))  # sealed before the parameter existed
            with self.assertRaises(ValueError):
                g12.check_sealed_images(work, json.dumps(images))
            handoff.write_text(json.dumps({"namespace": NS, "g12_images": images}))
            self.assertEqual(g12.check_sealed_images(work, json.dumps(images)), images)
            for attested in (None, json.dumps(g12.IMAGES)):
                with self.subTest(attested=attested), self.assertRaises(ValueError):
                    g12.check_sealed_images(work, attested)

    def test_cli_passes_the_image_set_to_the_orchestrator(self):
        with patch.object(g12, "orchestrate", return_value=0) as orchestrate:
            g12.main(["--run", "/home/bobby/x", "--export", "/home/bobby/e.json", "--namespace", NS,
                      "--image", "core=" + FINAL["core"], "--query-code", "image"])
        images, query_code = orchestrate.call_args.args[3:5]
        self.assertEqual(images["core"], FINAL["core"])
        self.assertEqual(images["python"], g12.IMAGES["python"])
        self.assertEqual(query_code, "image")
        with patch.object(g12, "orchestrate", return_value=0) as orchestrate:
            g12.main(["--run", "/home/bobby/x", "--export", "/home/bobby/e.json", "--namespace", NS])
        self.assertEqual(orchestrate.call_args.args[3:5], (g12.IMAGES, "worktree"))


class TopicReadinessTests(unittest.TestCase):
    def test_describe_retries_only_the_metadata_race(self):
        calls = []

        def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise RuntimeError('KafkaError{code=UNKNOWN_TOPIC_OR_PART,val=3}')
            return "described"
        self.assertEqual(g12.describe_until_known(flaky, seconds=5, pause=0), "described")
        self.assertEqual(len(calls), 3)

        def other():
            raise RuntimeError("KafkaError{code=TOPIC_AUTHORIZATION_FAILED}")
        with self.assertRaises(RuntimeError):
            g12.describe_until_known(other, seconds=5, pause=0)

        def never():
            raise RuntimeError("UNKNOWN_TOPIC_OR_PART")
        with self.assertRaises(RuntimeError):
            g12.describe_until_known(never, seconds=0, pause=0)



class OrchestratorSignalTests(unittest.TestCase):
    def test_sigterm_still_writes_verdict_and_tears_down(self):
        import os
        import signal
        import tempfile
        from pathlib import Path
        calls = []

        class FakeRun:
            def __init__(self, base, *_):
                self.evidence = Path(base) / "evidence"
                self.evidence.mkdir(parents=True)
                self.images, self.query_code = g12.IMAGES, "worktree"

            def prepare(self):
                os.kill(os.getpid(), signal.SIGTERM)

            def note(self, step, **fields):
                calls.append(step)

            def down(self):
                calls.append("down")
        before = signal.getsignal(signal.SIGTERM)
        with tempfile.TemporaryDirectory() as base, patch.object(g12, "Run", FakeRun):
            self.assertEqual(g12.orchestrate(base, "/x", NS), 1)
            verdict = g12.json.loads((Path(base) / "evidence" / "verdict.json").read_text())
        self.assertIn("signal", verdict["error"])
        self.assertEqual(calls, ["stopped", "down"])
        self.assertIs(signal.getsignal(signal.SIGTERM), before)  # handlers restored

    def test_detached_roles_keep_stderr_next_to_evidence(self):
        import sys
        import tempfile
        from pathlib import Path
        self.assertIsNone(g12.keep_diagnostics(None))
        saved = sys.stderr
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "monitor.jsonl"
            stream = g12.keep_diagnostics(out)
            try:
                sys.stderr.write("boom\n")
            finally:
                import faulthandler
                faulthandler.disable()
                sys.stderr = saved
                stream.close()
            text = (Path(tmp) / "monitor.jsonl.stderr").read_text()
        self.assertIn('"k": "start"', text)
        self.assertIn("boom", text)


class MarketDataFrameTests(unittest.TestCase):
    AT = 1_791_696_000_123 * MS  # a minute boundary + 123 ms

    def test_trade_frames_are_provider_shaped_and_deterministic(self):
        b = g12.trade_frame("BINANCE", 7, self.AT)
        self.assertEqual(b["payload"], {"e": "trade", "E": 1_791_696_000_123, "T": 1_791_696_000_123, "s": "ETHUSDT",
                                        "t": 7, "p": "60000.7", "q": "0.010", "X": "MARKET", "m": False})
        o = g12.trade_frame("OKX", 8, self.AT)
        self.assertEqual(o["payload"]["arg"], {"channel": "trades", "instId": "ETH-USDT-SWAP"})
        self.assertEqual(o["payload"]["data"], [{"instId": "ETH-USDT-SWAP", "tradeId": "8", "px": "60000.8",
                                                 "sz": "1", "side": "sell", "ts": "1791696000123"}])
        for frame in (b, o):
            self.assertEqual((frame["provenance"], frame["test_provenance"], frame["adapter_version"], frame["lane"]),
                             ("TEST_ONLY", True, g12.TEST_ADAPTER, "trade"))
        self.assertEqual(g12.trade_frame("OKX", 8, self.AT), o)
        for bad in (("BINANCE", 0, self.AT), ("BINANCE", 1, 0), ("DERIBIT", 1, self.AT), ("OKX", "1", self.AT)):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                g12.trade_frame(*bad)

    def test_quotes_change_every_time(self):
        b1, b2 = (g12.quote_frame("BINANCE", n, self.AT)["payload"] for n in (1, 2))
        self.assertEqual((b1["u"], b1["T"], b1["b"], b1["a"]), (1, 1_791_696_000_123, "60000.0", "60001.0"))
        self.assertNotEqual(b1["B"], b2["B"])
        o = g12.quote_frame("OKX", 3, self.AT)["payload"]
        self.assertEqual(o["arg"]["channel"], "bbo-tbt")
        self.assertEqual((o["data"][0]["seqId"], o["data"][0]["ts"]), (3, "1791696000123"))

    def test_bar_frames_are_final_and_only_after_close(self):
        opened = 1_791_695_940_000
        b = g12.bar_frame("BINANCE", opened, self.AT, trades=4)["payload"]
        self.assertTrue(b["k"]["x"])
        self.assertEqual((b["k"]["t"], b["k"]["T"], b["k"]["i"], b["k"]["n"]), (opened, opened + 59_999, "1m", 4))
        o = g12.bar_frame("OKX", opened, self.AT)["payload"]
        self.assertEqual(o["arg"]["channel"], "candle1m")
        self.assertEqual((o["data"][0][0], o["data"][0][8]), (str(opened), "1"))
        with self.assertRaises(ValueError):  # not closed yet at the stamp
            g12.bar_frame("OKX", opened + 60_000, self.AT)
        with self.assertRaises(ValueError):  # not minute aligned
            g12.bar_frame("OKX", opened + 1, self.AT)

    def test_market_plan_rates_and_burst(self):
        hz = 4
        plans = [g12.market_plan(tick, hz) for tick in range(60 * hz)]
        trades = sum(t for t, _, _ in plans)
        quotes = sum(q for _, q, _ in plans)
        self.assertEqual(quotes, 2 * 60)
        self.assertEqual(sum(m for _, _, m in plans), 60)  # one MARK/INDEX observation per second
        bursts = 60 // g12.MD_RATE["burst_every_s"] - 1  # tick 0 never bursts
        self.assertEqual(trades, 3 * 60 + bursts * g12.MD_RATE["burst_trades"])
        base = (15 * hz + 1) * 3 // hz - 15 * hz * 3 // hz  # the steady share of that tick
        self.assertEqual(g12.market_plan(15 * hz, hz)[0], base + g12.MD_RATE["burst_trades"])
        self.assertEqual(plans, [g12.market_plan(tick, hz) for tick in range(60 * hz)])  # deterministic

    def test_bars_due_and_seed(self):
        now = 1_791_696_000_150
        self.assertEqual(g12.seed_opens(now, 3), [1_791_695_820_000, 1_791_695_880_000, 1_791_695_940_000])
        last = 1_791_695_940_000
        self.assertEqual(g12.bars_due(now, last), [])  # close + 200 ms not reached yet
        self.assertEqual(g12.bars_due(1_791_696_060_200, last), [1_791_696_000_000])
        self.assertEqual(g12.bars_due(1_791_696_180_300, last),  # a stalled loop catches up without a gap
                         [1_791_696_000_000, 1_791_696_060_000, 1_791_696_120_000])

    def test_source_is_monotonic_contiguous_and_seeded(self):
        clock = [1_791_696_000_000 * MS]

        def now():
            clock[0] += 1 * MS
            return clock[0]
        source = g12.MarketDataSource(clock=now)
        seeds = source.seed(clock[0])
        self.assertEqual(len(seeds), 2 * g12.MD_RATE["seed_bars"])
        self.assertTrue(all(e["extra"]["seed"] for e in seeds))
        events = []
        for tick in range(4 * 200):  # 200 s at 4 Hz
            clock[0] += 250 * MS
            events += source.tick(tick, 4)
        for venue in g12.VENUES:
            trades = [int(e["id"]) for e in events if e["venue"] == venue and e["feed"] == "TRADE"]
            self.assertEqual(trades, list(range(1, len(trades) + 1)))
            quotes = [int(e["id"]) for e in events if e["venue"] == venue and e["feed"] == "QUOTE"]
            self.assertEqual(quotes, sorted(set(quotes)))
            opens = [int(e["id"]) for e in seeds + events if e["venue"] == venue and e["feed"] == "BAR"]
            self.assertEqual(opens, list(range(opens[0], opens[-1] + 1, 60_000)))  # contiguous, each once
            live = [e for e in events if e["venue"] == venue and e["feed"] == "BAR"]
            self.assertEqual(len(live), 3)
            for e in live:
                self.assertFalse(e["extra"]["seed"])
                self.assertEqual(e["extra"]["close_ms"], int(e["id"]) + 60_000)
                self.assertGreaterEqual(e["frame"]["received_at_ns"] // MS, e["extra"]["close_ms"] + 200)
            if venue == "BINANCE":  # a closed kline counts the trades of its minute
                self.assertGreater(sum(e["frame"]["payload"]["k"]["n"] for e in live), 0)

    def test_mark_frames_binance_one_okx_pair(self):
        (b,) = g12.mark_frames("BINANCE", 3, self.AT)
        self.assertEqual((b["feed"], b["lane"], b["native_symbol"]), ("MARK", "mark", "ETHUSDT"))
        self.assertEqual({k: b["payload"][k] for k in ("e", "E", "s", "p", "i")},
                         {"e": "markPriceUpdate", "E": 1_791_696_000_123, "s": "ETHUSDT", "p": "60000.53",
                          "i": "60000.43"})
        self.assertEqual(b["payload"]["T"] % 28_800_000, 0)
        mark, index = g12.mark_frames("OKX", 3, self.AT, "ETH-USDT")
        self.assertEqual((mark["feed"], mark["native_symbol"], mark["payload"]["arg"]),
                         ("MARK", "ETH-USDT-SWAP", {"channel": "mark-price", "instId": "ETH-USDT-SWAP"}))
        self.assertEqual((index["feed"], index["native_symbol"], index["payload"]["arg"]),
                         ("INDEX", "ETH-USDT", {"channel": "index-tickers", "instId": "ETH-USDT"}))
        self.assertEqual(mark["payload"]["data"][0]["ts"], index["payload"]["data"][0]["ts"])  # one pair, one ms
        self.assertEqual((mark["lane"], index["lane"]), ("mark", "mark"))  # one session / generation
        with self.assertRaises(ValueError):
            g12.mark_frames("OKX", 0, self.AT)

    def test_source_marks_once_per_second(self):
        clock = [1_791_696_000_000 * MS]

        def now():
            clock[0] += 1 * MS
            return clock[0]
        source = g12.MarketDataSource(clock=now)
        events = []
        for tick in range(4 * 30):
            clock[0] += 250 * MS
            events += source.tick(tick, 4)
        for venue, parts in (("BINANCE", 1), ("OKX", 2)):
            marks = [e for e in events if e["venue"] == venue and e["feed"] == "MARK"]
            self.assertEqual(len(marks), 30)
            self.assertEqual([int(e["id"]) for e in marks], sorted({int(e["id"]) for e in marks}))
            self.assertTrue(all(len(e["frames"]) == parts for e in marks))
            self.assertTrue(all(e["id"] == str(e["frames"][0]["received_at_ns"] // MS) for e in marks))

    def test_query_role_runs_the_harness_query_entry(self):
        image = g12.topology(NS, "/home/bobby/x/work", uid="1:1", query_code="image")["query"]
        self.assertIn(f"{g12.ROOT / 'scripts/p184_g12_isolated_fault.py'}:/harness/p184_g12_isolated_fault.py:ro",
                      image)
        self.assertEqual(image[-3:], ["/harness/p184_g12_isolated_fault.py", "--query", "/work"])
        worktree = g12.topology(NS, "/home/bobby/x/work", uid="1:1")["query"]
        self.assertEqual(worktree[-3:], ["scripts/p184_g12_isolated_fault.py", "--query", "/work"])
        with patch.object(g12, "serve_query") as serve:
            self.assertEqual(g12.main(["--query", "/work"]), 0)
        serve.assert_called_once()

    def test_notification_parser(self):
        trade = g12.json.dumps({"observed_at_ms": 5, "payload": {"native_trade_id": "42", "price": "1"}})
        self.assertEqual(g12.md_event_row("events.market.v2.execution.trade.OKX.SWAP.ETH-USDT-SWAP", trade),
                         {"feed": "TRADE", "id": "42", "ev_ms": 5})
        quote = g12.json.dumps({"observed_at_ms": 9, "payload": {"bid_price": "1"}})
        self.assertEqual(g12.md_event_row("events.market.v2.execution.quote.BINANCE.USD_M.ETHUSDT", quote),
                         {"feed": "QUOTE", "id": "9", "ev_ms": 9})
        mark = g12.json.dumps({"observed_at_ms": 11, "feed": "MARK_INDEX_PRICE",
                               "payload": {"mark_price": {}, "index_price": {}}})
        self.assertEqual(g12.md_event_row("events.market.v2.execution.mark_index_price.OKX.SWAP.ETH-USDT-SWAP", mark),
                         {"feed": "MARK", "id": "11", "ev_ms": 11})
        closed = g12.json.dumps({"timestamp_ms": 60_000, "is_closed": True, "interval": "1m"})
        self.assertEqual(g12.md_event_row("events.market.v2.bar.OKX.SWAP.ETH-USDT-SWAP.1m", closed),
                         {"feed": "BAR", "id": "60000", "ev_ms": 60_000})
        for channel, data in (("events.market.v2.bar.OKX.SWAP.X.1m", g12.json.dumps({"timestamp_ms": 1,
                                                                                     "is_closed": False,
                                                                                     "interval": "1m"})),
                              ("events.market.v2.bar.OKX.SWAP.X.5m", g12.json.dumps({"timestamp_ms": 1,
                                                                                     "is_closed": True,
                                                                                     "interval": "5m"})),
                              ("events.market.v2.execution.book_delta.OKX.SWAP.X", trade),
                              ("events.market.v2.execution.trade.OKX.SWAP.X", "not json"),
                              ("events.market.trade.OKX.X", trade)):
            with self.subTest(channel=channel):
                self.assertIsNone(g12.md_event_row(channel, data))
        self.assertIn(g12.inspect.getsource(g12.md_event_row), g12.SAMPLER)  # the sampler runs this parser


class MarketDataWindowTests(unittest.TestCase):
    def window(self, pubs, evs, start=100 * S, end=400 * S):
        return g12.md_window(pubs, evs + [{"k": "view", "t": 500 * S, "venue": "OKX", "seq": 1, "v": "APPROVED"}],
                             start, end)

    def rows(self, **kwargs):
        pubs, evs = [], []
        for venue in g12.VENUES:
            p, e = md_rows(venue, 0, 500 * S, **kwargs)
            pubs += p
            evs += e
        return pubs, evs

    def test_pass(self):
        result = self.window(*self.rows())
        self.assertEqual((result["g2_status"], result["g3_status"]), ("PASS", "PASS"))
        self.assertEqual(result["g2"]["TRADE"]["OKX"]["n"], 1200)
        self.assertEqual(result["g2"]["QUOTE"]["OKX"]["n"], 600)
        self.assertEqual(result["g2"]["TRADE"]["OKX"]["p95_ms"], 100.0)
        self.assertEqual(result["g3"]["BAR_1m"]["BINANCE"]["n"], 5)  # closes 120..360 s
        self.assertEqual(result["g3"]["BAR_1m"]["BINANCE"]["max_ms"], 800.0)  # from close, not from publish

    def test_fail_on_max_and_p95(self):
        pubs, evs = self.rows()
        slow = next(e for e in evs if e["feed"] == "TRADE" and e["venue"] == "OKX" and e["t"] > 200 * S)
        slow["t"] += 1500 * MS  # one trade 1.6 s: max >= 1500
        result = self.window(pubs, evs)
        self.assertEqual(result["g2"]["TRADE"]["OKX"]["status"], "FAIL")
        self.assertEqual(result["g2"]["TRADE"]["BINANCE"]["status"], "PASS")
        self.assertEqual(result["g2_status"], "FAIL")
        result = self.window(*self.rows(bar_ms=6000))  # every bar 6 s after close: p95 > 5 s
        self.assertEqual(result["g3_status"], "FAIL")
        result = self.window(*self.rows(quote_ms=1000))  # QUOTE max must stay < 1000
        self.assertEqual(result["g2"]["QUOTE"]["OKX"]["status"], "FAIL")

    def test_few_samples_are_not_measured_never_pass(self):
        pubs, evs = self.rows()
        short = self.window(pubs, evs, 100 * S, 190 * S)  # one bar close (120 s), 360 trades
        self.assertEqual(short["g3"]["BAR_1m"]["OKX"]["status"], "NOT_MEASURED")
        self.assertEqual(short["g3_status"], "NOT_MEASURED")
        self.assertEqual(short["g2_feed_status"], {"TRADE": "PASS", "QUOTE": "PASS", "MARK": "NOT_MEASURED"})
        self.assertEqual(short["g2"]["QUOTE"]["OKX"]["n"], 180)
        self.assertEqual(short["g2"]["MARK"]["OKX"]["n"], 90)
        self.assertEqual(short["g2_status"], "NOT_MEASURED")
        sparse = self.window(*self.rows(step_ms=4000))  # 75 trades in 300 s
        self.assertEqual(sparse["g2"]["TRADE"]["OKX"]["status"], "NOT_MEASURED")
        none = self.window(*self.rows(trades=False, quotes=False, bars=False, marks=False))
        self.assertEqual((none["g2_status"], none["g3_status"]), ("NOT_MEASURED", "NOT_MEASURED"))

    def test_lost_trade_fails_unseen_quote_does_not_pending_excluded(self):
        pubs, evs = self.rows()
        lost = next(e for e in evs if e["feed"] == "TRADE" and e["venue"] == "OKX" and e["t"] > 200 * S)
        conflated = next(e for e in evs if e["feed"] == "QUOTE" and e["venue"] == "OKX" and e["t"] > 200 * S)
        evs = [e for e in evs if e is not lost and e is not conflated]
        result = self.window(pubs, evs)
        self.assertEqual((result["g2"]["TRADE"]["OKX"]["lost"], result["g2"]["TRADE"]["OKX"]["status"]), (1, "FAIL"))
        self.assertEqual(result["g2"]["QUOTE"]["OKX"]["unseen"], 1)
        self.assertEqual(result["g2"]["QUOTE"]["OKX"]["status"], "PASS")
        # The sampler has not yet covered reference + 10 s: undecidable, excluded rather than lost.
        early = g12.md_window(pubs, [e for e in evs if e["t"] < 205 * S], 100 * S, 400 * S)
        self.assertEqual(early["g2"]["TRADE"]["OKX"]["lost"], 0)
        self.assertGreater(early["g2"]["TRADE"]["OKX"]["pending"], 0)

    def test_mark_window_gate_and_unseen(self):
        result = self.window(*self.rows())
        self.assertEqual(result["g2"]["MARK"]["OKX"]["n"], 300)
        self.assertEqual(result["g2_feed_status"]["MARK"], "PASS")
        slow = self.window(*self.rows(mark_ms=1000))  # max must stay < 1000
        self.assertEqual(slow["g2"]["MARK"]["BINANCE"]["status"], "FAIL")
        self.assertEqual(slow["g2_status"], "FAIL")
        pubs, evs = self.rows()
        skipped = [e for e in evs if e["feed"] == "MARK" and e["venue"] == "OKX" and 200 * S < e["t"] < 210 * S]
        evs = [e for e in evs if not any(e is x for x in skipped)]
        result = self.window(pubs, evs)  # a latest-state read may skip a superseded observation
        self.assertEqual((result["g2"]["MARK"]["OKX"]["unseen"], result["g2"]["MARK"]["OKX"]["lost"],
                          result["g2"]["MARK"]["OKX"]["status"]), (len(skipped), 0, "PASS"))
        sparse = self.window(*self.rows(marks=False))
        self.assertEqual(sparse["g2"]["MARK"]["OKX"]["status"], "NOT_MEASURED")
        self.assertEqual(sparse["g2_status"], "NOT_MEASURED")

    def test_seed_bars_and_replays_do_not_count(self):
        pubs, evs = self.rows()
        for row in pubs:
            if row["feed"] == "BAR":
                row["seed"] = True
        self.assertEqual(self.window(pubs, evs)["g3"]["BAR_1m"]["OKX"]["n"], 0)
        pubs, evs = self.rows()
        replay = [dict(e, t=e["t"] + 30 * S) for e in evs]  # re-projected after a restart: first receipt counts
        result = self.window(pubs, evs + replay)
        self.assertEqual(result["g2"]["TRADE"]["OKX"]["max_ms"], 100.0)

    def test_fault_verdict_needs_g1_g2_and_g3(self):
        monitor = [{"t": t * S, "ports": {"8210": t >= 62}, "projector_lag": 0, "under_replicated": 0}
                   for t in range(0, 500)]
        fault = {"k": "fault", "name": "stream_restart", "kind": "stream", "injected_ns": 60 * S,
                 "restart_ns": 61 * S}

        def run(**kwargs):
            pubs, views = [], []
            for venue in g12.VENUES:
                p, v = stream_rows(venue, 0, 500 * S)
                pubs += p
                views += v
                p, v = md_rows(venue, 0, 500 * S, **kwargs)
                pubs += p
                views += v
            return g12.evaluate_fault(fault, pubs, views, monitor)
        self.assertTrue(run()["pass"])
        no_bars = run(bars=False)
        self.assertEqual(no_bars["parts"]["g1"], "PASS")
        self.assertEqual(no_bars["parts"]["g3"], "NOT_MEASURED")
        self.assertFalse(no_bars["pass"])
        slow = run(trade_ms=1600)
        self.assertEqual(slow["parts"]["g2"], "FAIL")
        self.assertFalse(slow["pass"])
        no_mark = run(marks=False)
        self.assertEqual(no_mark["parts"]["g2_feeds"], {"TRADE": "PASS", "QUOTE": "PASS", "MARK": "NOT_MEASURED"})
        self.assertEqual(no_mark["parts"]["g2"], "NOT_MEASURED")
        self.assertFalse(no_mark["pass"])
        slow_mark = run(mark_ms=1000)  # MARK max must stay < 1000 ms
        self.assertEqual(slow_mark["parts"]["g2_feeds"]["MARK"], "FAIL")
        self.assertFalse(slow_mark["pass"])
        book_only = run(trades=False, quotes=False, bars=False, marks=False)  # the 2026-10-11 rc10 evidence
        self.assertEqual((book_only["parts"]["g2"], book_only["parts"]["g3"]), ("NOT_MEASURED", "NOT_MEASURED"))
        self.assertFalse(book_only["pass"])

    def test_readiness_and_publisher_summary(self):
        pubs, evs = self.rows()
        self.assertEqual(g12.md_missing(evs), [])
        self.assertEqual(g12.md_missing([e for e in evs if e["feed"] != "BAR"]), ["BAR@BINANCE", "BAR@OKX"])
        self.assertEqual(g12.md_missing([e for e in evs if e["feed"] != "MARK"]), ["MARK@BINANCE", "MARK@OKX"])
        summary = g12.publisher_md_summary(pubs + [{"k": "ack_md", "feed": "TRADE", "venue": "OKX", "id": "1"},
                                                   {"k": "ack_md", "feed": "TRADE", "venue": "OKX", "id": "2",
                                                    "error": "_MSG_TIMED_OUT"}])
        self.assertEqual(summary["TRADE"]["acked"], 1)
        self.assertEqual(summary["TRADE"]["delivery_errors"], 1)
        self.assertEqual(summary["BAR"]["published"], 2 * 8)


class SealedFeedMaterialTests(unittest.TestCase):
    """Real offline DR05 material + the G12 feed seal (python image: qdl, yaml, cryptography)."""

    def test_seal_adds_trade_quote_bar_lanes_end_to_end(self):
        import json
        import tempfile
        from pathlib import Path
        from scripts import p184_dr05_dl_fault as dr
        from scripts.kn_gateway_bundle import compile_bundle
        from qdl.consumer.manifest import ConsumerManifestLoader
        from qdl.runtime.stable_catalog import StableSourceCatalog
        from tests.test_p184_dr05_dl_fault import MaterialTests
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "work"
            work.mkdir()
            (work / "handoff.json").write_text(json.dumps({"namespace": NS, "images": g12.dr05_images(g12.IMAGES),
                                                           "g12_images": g12.IMAGES}))
            dr.material(work, MaterialTests.exported())
            sealed = g12.seal_market_feeds(work, NS)
            self.assertEqual(sealed["feeds"], ["BOOK_SNAPSHOT", "BOOK_DELTA", "TRADE", "QUOTE", "BAR",
                                               "MARK_INDEX_PRICE"])
            config = json.loads((work / "core.json").read_text())
            lanes = g12.md_bindings(config)
            expected = {("BINANCE", "TRADE"): ("binance_usdm_trade", "ethusdt@trade", "MONOTONIC", False),
                        ("BINANCE", "QUOTE"): ("binance_usdm_bbo", "ethusdt@bookTicker", "MONOTONIC", False),
                        ("BINANCE", "BAR"): ("binance_usdm_bar", "ethusdt@kline_1m", "NONE", True),
                        ("OKX", "TRADE"): ("okx_trade", "trades", "MONOTONIC", False),
                        ("OKX", "QUOTE"): ("okx_bbo", "bbo-tbt", "NONE", False),
                        ("OKX", "BAR"): ("okx_bar", "candle1m", "NONE", True)}
            for key, (kind, channel, policy, final) in expected.items():
                binding = lanes[key]
                self.assertEqual((binding["provider_kind"], binding["native_channel"], binding["sequence_policy"],
                                  binding["require_final_bar"]), (kind, channel, policy, final), key)
                self.assertNotIn("l2", binding)
            # MARK_INDEX_PRICE as the deployment's physical components (production cadences).
            mark = lanes[("BINANCE", "MARK")]
            self.assertEqual((mark["provider_kind"], mark["physical_native_symbol"], mark["physical_native_channel"],
                              mark["mark_index"], mark["sequence_policy"]),
                             ("binance_usdm_mark_index", "ETHUSDT", "ethusdt@markPrice@1s",
                              {"component": "BOTH", "quiet_after_ms": 5000}, "NONE"))
            okx_mark, okx_index = lanes[("OKX", "MARK")], lanes[("OKX", "INDEX")]
            self.assertEqual((okx_mark["provider_kind"], okx_mark["physical_native_symbol"],
                              okx_mark["physical_native_channel"], okx_mark["mark_index"]),
                             ("okx_mark_price", "ETH-USDT-SWAP", "mark-price",
                              {"component": "MARK", "quiet_after_ms": 15000}))
            self.assertEqual((okx_index["provider_kind"], okx_index["physical_native_symbol"],
                              okx_index["physical_native_channel"], okx_index["mark_index"]),
                             ("okx_index_price", "ETH-USDT", "index-tickers",
                              {"component": "INDEX", "quiet_after_ms": 70000}))
            self.assertEqual(okx_mark["source_id"], okx_index["source_id"])  # one logical source, two components
            self.assertEqual((okx_mark["native_symbol"], okx_mark["native_channel"]), ("ETH-USDT-SWAP", "mark-price"))
            books = [b for b in config["core"]["bindings"] if b.get("l2")]
            self.assertEqual(len(books), 2)
            self.assertEqual(len(config["core"]["bindings"]), 2 + 2 * 4 + 1)
            self.assertEqual(len({b["source_id"] for b in config["core"]["bindings"]}), 10)
            self.assertEqual(config["authority"]["contract_digest"], sealed["core_bindings_sha256"])
            catalog = StableSourceCatalog.load(work / "catalog.yaml")
            self.assertEqual(len(catalog.bindings), 4 + 8)
            manifests = [work / "binance.yaml", work / "okx.yaml"]
            for path in manifests:
                manifest = ConsumerManifestLoader.load(path)
                self.assertEqual({r.feed.value for r in manifest.requirements},
                                 {"BOOK_SNAPSHOT", "BOOK_DELTA", "TRADE", "QUOTE", "BAR", "MARK_INDEX_PRICE"})
                ref = next(r for r in manifest.requirements if r.feed.value == "MARK_INDEX_PRICE")
                self.assertEqual((ref.max_freshness_ms, ref.source_policy_id, ref.recovery.value,
                                  ref.require_final_bars, ref.event_recency_policy),
                                 (2000, "crypto_liquid_v2", "FRESH_SNAPSHOT", False, None))
                bar = next(r for r in manifest.requirements if r.feed.value == "BAR")
                self.assertEqual((bar.interval, bar.max_freshness_ms, bar.require_final_bars), ("1m", 180000, True))
                trade = next(r for r in manifest.requirements if r.feed.value == "TRADE")
                self.assertEqual((trade.max_freshness_ms, trade.source_policy_id, trade.event_recency_policy),
                                 (3000, "crypto_primary_v2", None))
            for label in ("binance", "okx"):
                route = json.loads((work / (label + ".binding.json")).read_text())
                planes = {p["feed"]: p["provider_plane"] for p in route["products"]}
                self.assertEqual(planes, {"BOOK_SNAPSHOT": "L2", "BOOK_DELTA": "L2", "TRADE": "REALTIME",
                                          "QUOTE": "REALTIME", "BAR": "REALTIME", "MARK_INDEX_PRICE": "REFERENCE"})
                self.assertEqual(next(p["interval"] for p in route["products"] if p["feed"] == "BAR"), "1m")
                consumer = json.loads((work / ("ts-consumer." + label + ".json")).read_text())
                # TS WriterClone maps crypto_source_policy_id -> DATA_LAYER_V2_CRYPTO_SOURCE_POLICY_ID.
                self.assertEqual((consumer["crypto_source_policy_id"], consumer["execution_book_source_policy_id"],
                                  consumer["execution_reference_source_policy_id"]),
                                 ("crypto_primary_v2", "crypto_liquid_v2", "crypto_liquid_v2"))
                import hashlib
                self.assertEqual(consumer["release_manifest_sha256"],
                                 hashlib.sha256((work / (label + ".yaml")).read_bytes()).hexdigest())
            self.assertEqual(json.loads((work / "bundle.json").read_text()), compile_bundle(
                environment="paper", catalog_path=work / "catalog.yaml", manifest_paths=manifests))
            self.assertFalse(list(work.glob("*.g12-tmp")))
            with self.assertRaises(ValueError):  # sealed once
                g12.seal_market_feeds(work, NS)

            # Raw envelopes of each lane: own session, generation 1, test provenance, exact binding identity.
            from qdl.raw.envelope import validate_raw_envelope
            at = 1_791_696_000_123 * MS
            frames = {"TRADE": g12.trade_frame("OKX", 1, at), "QUOTE": g12.quote_frame("OKX", 1, at),
                      "BAR": g12.bar_frame("OKX", 1_791_695_940_000, at)}
            for feed, frame in frames.items():
                raw = g12.md_envelope(lanes[("OKX", feed)], frame, NS)
                validate_raw_envelope(raw)
                self.assertEqual((raw.source_session_id, raw.connection_generation, raw.test_provenance),
                                 (f"{NS}.OKX.{feed.lower()}", 1, True))
                self.assertEqual((raw.subscription_id, raw.native_channel),
                                 (lanes[("OKX", feed)]["source_id"], lanes[("OKX", feed)]["native_channel"]))
            with self.assertRaises(ValueError):  # a trade frame on the bar lane
                g12.md_envelope(lanes[("OKX", "BAR")], frames["TRADE"], NS)
            okx_pair = g12.mark_frames("OKX", 1, at, okx_index["physical_native_symbol"])
            for part, lane in zip(okx_pair, (okx_mark, okx_index)):
                raw = g12.md_envelope(lanes[("OKX", part["feed"])], part, NS)
                validate_raw_envelope(raw)
                self.assertEqual((raw.native_symbol, raw.native_channel, raw.source_session_id,
                                  raw.connection_generation),
                                 (lane["physical_native_symbol"], lane["physical_native_channel"], f"{NS}.OKX.mark", 1))
            (binance_mark,) = g12.mark_frames("BINANCE", 1, at)
            raw = g12.md_envelope(mark, binance_mark, NS)
            self.assertEqual((raw.native_symbol, raw.native_channel), ("ETHUSDT", "ethusdt@markPrice@1s"))
            with self.assertRaises(ValueError):  # an index frame on the mark component
                g12.md_envelope(okx_mark, okx_pair[1], NS)

            # Query: the DR05 Query stack with the production KN execution MARK/INDEX reader wired in.
            from qdl.reference.execution_live import HttpExecutionMarkIndexReader
            captured = {}

            def build(**kwargs):
                captured.update(kwargs)
                return "stack"
            backend = object()
            stack = g12.with_execution_mark_index(build)
            self.assertEqual(stack(spool=None, catalog=catalog, backend=backend, issuer="i"), "stack")
            self.assertTrue(captured["reference_data_enabled"])
            self.assertIsInstance(captured["execution_mark_index_reader"], HttpExecutionMarkIndexReader)
            self.assertIs(captured["backend"], backend)
            self.assertEqual(captured["issuer"], "i")
            self.assertIsNotNone(captured["alpha_mark_index_reader_factory"])
            with self.assertRaises(ValueError):
                g12.md_envelope(lanes[("BINANCE", "TRADE")], frames["TRADE"], NS)
            with self.assertRaises(ValueError):
                g12.md_envelope(lanes[("OKX", "TRADE")], dict(frames["TRADE"], provenance="LIVE"), NS)

    def test_dr05_consumer_packet_default_is_unchanged(self):
        import json
        import tempfile
        from pathlib import Path
        from scripts import p184_dr05_dl_fault as dr
        from tests.test_p184_dr05_dl_fault import MaterialTests
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "work"
            dr.prepare(work)
            dr.material(work, MaterialTests.exported())
            consumer = json.loads((work / "ts-consumer.okx.json").read_text())
            self.assertNotIn("crypto_source_policy_id", consumer)
            route = json.loads((work / "okx.binding.json").read_text())
            self.assertEqual({p["provider_plane"] for p in route["products"]}, {"L2"})


class ExitStateTests(unittest.TestCase):
    @staticmethod
    def event(action, role, t, **attrs):
        return {"Type": "container", "Action": action, "status": action, "timeNano": t,
                "Actor": {"ID": "x", "Attributes": {"name": f"{NS}-{role}", g12.LABEL: NS, **attrs}}}

    def test_events_argv_is_bounded_and_label_scoped(self):
        argv = g12.events_argv(NS, 100, 200)
        self.assertEqual(argv[:6], ["docker", "events", "--since", "100", "--until", "200"])
        self.assertIn(f"label={g12.LABEL}={NS}", argv)
        for event in ("die", "oom", "kill"):
            self.assertIn("event=" + event, argv)
        for bad in ((NS, 0, 1), (NS, 5, 5), ("qdl_v2_stable_candidate", 1, 2)):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                g12.events_argv(*bad)

    def test_exit_states_per_life(self):
        rows = [self.event("kill", "projector", 1, signal="15"), self.event("die", "projector", 2, exitCode="143"),
                self.event("oom", "monitor", 5), self.event("die", "monitor", 6, exitCode="137"),
                self.event("die", "tswriter-okx", 7, exitCode="1"),
                self.event("die", "projector", 9, exitCode="0"),
                {"Action": "die", "timeNano": 3, "Actor": {"Attributes": {"name": "qdl_v2_stable_candidate-x",
                                                                           "exitCode": "1"}}}]
        states = g12.exit_states(rows)
        self.assertEqual(set(states), {"projector", "monitor", "tswriter-okx"})
        self.assertEqual((states["monitor"]["exit_code"], states["monitor"]["oom_killed"]), (137, True))
        self.assertEqual(len(states["projector"]["lives"]), 2)
        self.assertEqual(states["projector"]["lives"][0]["kill_signals"], ["15"])
        self.assertEqual((states["projector"]["exit_code"], states["projector"]["oom_killed"]), (0, False))
        self.assertEqual(states["tswriter-okx"]["exit_code"], 1)
        self.assertTrue(g12.exit_states([self.event("die", "publisher", 1, exitCode="137",
                                                    oomKilled="true")])["publisher"]["oom_killed"])

    def test_check_stops_with_the_exit_state_of_a_vanished_role(self):
        import json
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as base:
            run = g12.Run(base, "/home/bobby/e.json", NS)
            run.evidence.mkdir(parents=True)
            with (run.evidence / "container-events.jsonl").open("w") as handle:
                for row in (self.event("oom", "monitor", 1), self.event("die", "monitor", 2, exitCode="137")):
                    handle.write(json.dumps(row) + "\n")
            run.expected = {"monitor", "publisher", "sampler"}
            with patch.object(g12, "host_resources", return_value={"disk_free_bytes": 10**12,
                                                                   "mem_available_bytes": 10**11}), \
                    patch.object(g12, "running_names", return_value={NS + "-publisher"}), \
                    self.assertRaises(RuntimeError) as stop:
                run.check()
            message = str(stop.exception)
            self.assertIn("STOP role exited", message)
            self.assertIn('"monitor": {"exit_code": 137, "oom_killed": true}', message)
            self.assertIn('"sampler": "NO_DIE_EVENT"', message)
            notes = [json.loads(line) for line in
                     (Path(base) / "evidence" / "orchestrator.jsonl").read_text().splitlines()]
            self.assertEqual(notes[-1]["step"], "role_exited")

    def stats_case(self, base, results, listings):
        """Run.sample_stats with fake `docker stats` results and successive running_names listings."""
        run = g12.Run(base, "/home/bobby/e.json", NS)
        run.evidence.mkdir(parents=True, exist_ok=True)
        calls = []

        def fake_run(argv, **kwargs):
            calls.append(argv)
            code, out = results.pop(0)
            return types.SimpleNamespace(returncode=code, stdout=out, stderr="EOF" if code else "")
        listing = iter(listings)
        with patch.object(g12.subprocess, "run", side_effect=fake_run), \
                patch.object(g12, "running_names", side_effect=lambda ns: set(next(listing))):
            value = run.sample_stats()
        return run, value, calls

    def test_stats_sample_writes_rows(self):
        with tempfile.TemporaryDirectory() as base:
            line = json.dumps({"Name": NS + "-core", "CPUPerc": "1%", "MemUsage": "1MiB"})
            run, value, calls = self.stats_case(base, [(0, line + "\n")], [[NS + "-core"]])
            self.assertEqual(value, 1)
            self.assertEqual(calls[0][:4], ["docker", "stats", "--no-stream", "--format"])
            self.assertEqual(run.stats_skipped, 0)

    def test_stats_sample_retries_without_a_role_that_exited_during_it(self):
        """N1 r2b5a-6k-1: capacity-observer exited 0 during the sample; docker stats returned EOF."""
        with tempfile.TemporaryDirectory() as base:
            line = json.dumps({"Name": NS + "-core", "CPUPerc": "1%", "MemUsage": "1MiB"})
            names = [NS + "-capacity-observer", NS + "-core"]
            run, value, calls = self.stats_case(base, [(1, ""), (0, line + "\n")],
                                                [names, [NS + "-core"]])
            self.assertEqual(value, 1)
            self.assertEqual(calls[1][5:], [NS + "-core"])
            self.assertEqual(run.stats_skipped, 0)

    def test_stats_sample_is_skipped_and_counted_when_roles_keep_exiting(self):
        with tempfile.TemporaryDirectory() as base:
            names = [NS + "-a", NS + "-b", NS + "-c"]
            run, value, _ = self.stats_case(base, [(1, ""), (1, "")], [names, names[1:], names[2:]])
            self.assertIsNone(value)
            self.assertEqual(run.stats_skipped, 1)
            marker = json.loads((run.evidence / "stats.jsonl").read_text().splitlines()[-1])
            self.assertEqual((marker["skipped"], marker["roles"]), ("ROLE_EXITED_DURING_SAMPLE", ["a", "b"]))
            notes = [json.loads(line) for line in (run.evidence / "orchestrator.jsonl").read_text().splitlines()]
            self.assertEqual(notes[-1]["step"], "stats_skipped_role_exited")

    def test_stats_failure_with_every_role_running_still_raises(self):
        with tempfile.TemporaryDirectory() as base:
            names = [NS + "-a", NS + "-b"]
            with self.assertRaises(RuntimeError):
                self.stats_case(base, [(1, "")], [names, names])
            with self.assertRaises(RuntimeError):  # retried survivors that still fail while all run: real failure
                self.stats_case(base, [(1, ""), (1, "")], [names, names[1:], names[1:]])

    def test_stop_events_terminates_the_follower(self):
        import tempfile

        class Proc:
            returncode = None

            def terminate(self):
                self.returncode = -15

            def wait(self, timeout):
                return self.returncode
        with tempfile.TemporaryDirectory() as base:
            run = g12.Run(base, "/home/bobby/e.json", NS)
            self.assertIsNone(run.stop_events())
            run.events_proc = Proc()
            with patch.object(g12.time, "sleep"):
                self.assertEqual(run.stop_events(), -15)
            self.assertIsNone(run.events_proc)


class QueryStateTopicTests(unittest.TestCase):
    """G12 run 2026-10-11 (p184-g12-rc10-full): BAR never reached the clones because the isolated Query read the
    BAR source boundary from the production bars checkpoint (DR08 serve_query passes a literal two-key env)."""

    DR08_ENV = {"QDL_KN3_MARKET_CACHE_URL": "redis://localhost:6379/1", "QDL_KN3_ENVIRONMENT": "paper"}

    def test_reader_reads_this_namespace_state_topics(self):
        from qdl.runtime import kn_market_cache
        default = kn_market_cache.reader_from_environment(self.DR08_ENV)  # the cause: production defaults
        self.assertEqual((default.bars_topic, default.bars_partitions), ("md.bars.v2", 6))
        reader = g12.with_isolated_state_topics(kn_market_cache.reader_from_environment, NS)(self.DR08_ENV)
        wanted = g12.topics(NS)
        self.assertEqual((reader.bars_topic, reader.bars_partitions), (NS + ".bars", wanted[NS + ".bars"][0]))
        self.assertEqual((reader.latest_topic, reader.latest_partitions),
                         (NS + ".latest", wanted[NS + ".latest"][0]))
        # The BAR head reads its source watermark from the checkpoint the projector writes for its own topic.
        self.assertEqual(reader.checkpoint_key(reader.bars_topic, 1), f"kn3:paper:ckpt:{NS}.bars:1")
        self.assertEqual(g12.isolated_state_topics(NS)["QDL_KN_BARS_PARTITIONS"], "3")

    def test_serve_query_patches_reader_and_stack_then_runs_the_dr05_query(self):
        import json
        import tempfile
        from pathlib import Path
        from qdl.runtime import kn_market_cache, stable_source
        from scripts import p184_dr05_observer
        reader, stack = kn_market_cache.reader_from_environment, stable_source.build_stable_query_stack
        seen = {}

        def fake_query(work):
            seen["reader"] = kn_market_cache.reader_from_environment(self.DR08_ENV)
            seen["stack"] = stable_source.build_stable_query_stack
            seen["work"] = work
        try:
            with tempfile.TemporaryDirectory() as tmp, patch.object(p184_dr05_observer, "diagnostic_query",
                                                                    side_effect=fake_query):
                (Path(tmp) / "packet.json").write_text(json.dumps({"namespace": NS}))
                g12.serve_query(tmp)
        finally:
            kn_market_cache.reader_from_environment, stable_source.build_stable_query_stack = reader, stack
        self.assertEqual(seen["reader"].bars_topic, NS + ".bars")
        self.assertIsNot(seen["stack"], stack)
        self.assertEqual(str(seen["work"]), tmp)

    def test_keep_logs_keeps_the_writer_stderr(self):
        import tempfile
        from pathlib import Path

        class Clone:
            def stderr_tail(self, limit):
                return "Traceback: boom\n"
        with tempfile.TemporaryDirectory() as base:
            run = g12.Run(base, "/home/bobby/e.json", NS)
            run.evidence.mkdir(parents=True)
            run.clones = {"OKX": Clone()}
            with patch.object(g12, "owned_names", return_value=[]):
                run.keep_logs()
            self.assertEqual((Path(base) / "evidence/logs/tswriter-okx.stderr.log").read_text(), "Traceback: boom\n")


def _crash_on_second(calls=[]):  # noqa: B006 - per-process call counter of the forked child
    def sampler(phase):
        calls.append(1)
        phase("list_offsets:kn-projector-v3-a")
        if len(calls) == 2:
            import os
            import signal
            os.kill(os.getpid(), signal.SIGSEGV)
        return {"cluster": {"broker_ids": [1, 2, 3]}, "n": len(calls)}
    return sampler


class MonitorSupervisorTests(unittest.TestCase):
    """G12 run 2026-10-11 full2: the monitor (librdkafka admin calls) died with SIGSEGV (exit 139) one second
    into a broker stop and the run stopped. A crash now costs one typed sample, never the monitor."""

    def test_native_crash_is_a_typed_sample_error_and_the_next_sample_works(self):
        supervisor = g12.SampleSupervisor(_crash_on_second, timeout_s=5)
        try:
            first, error = supervisor.sample()
            self.assertEqual((first["n"], error), (1, None))
            fields, error = supervisor.sample()
            self.assertIsNone(fields)
            self.assertEqual((error["error"], error["phase"], error["exitcode"]),
                             ("SIGSEGV", "list_offsets:kn-projector-v3-a", -11))
            again, error = supervisor.sample()  # a fresh child (its own counter)
            self.assertEqual((again["n"], error, supervisor.restarts), (1, None, 1))
        finally:
            supervisor.close()

    def test_hang_and_exception_are_bounded(self):
        def hang():
            def sampler(phase):
                phase("describe_consumer_groups")
                import time as clock
                clock.sleep(30)
            return sampler

        def broken():
            raise RuntimeError("no TLS material")
        supervisor = g12.SampleSupervisor(hang, timeout_s=0.5)
        try:
            started = g12.time.monotonic()
            fields, error = supervisor.sample()
            self.assertLess(g12.time.monotonic() - started, 5)
            self.assertEqual((fields, error["error"], error["phase"]), (None, "TIMEOUT", "describe_consumer_groups"))
        finally:
            supervisor.close()
        supervisor = g12.SampleSupervisor(broken, timeout_s=5)
        try:
            fields, error = supervisor.sample()
            self.assertEqual((fields, error["error"]), (None, "EXCEPTION"))
            self.assertIn("no TLS material", error["detail"])
        finally:
            supervisor.close()
        self.assertEqual([g12.exit_reason(c) for c in (-11, -9, 1, None)],
                         ["SIGSEGV", "SIGKILL", "EXIT_1", "EXIT_None"])

    def test_monitor_writes_sample_error_rows_and_keeps_running(self):
        import signal
        import tempfile
        from pathlib import Path
        outcomes = [({"cluster": {"broker_ids": [1, 2, 3]}}, None),
                    (None, {"error": "SIGSEGV", "phase": "describe_cluster", "exitcode": -11}),
                    ({"cluster": {"broker_ids": [1, 2, 3]}}, None)]

        class Supervisor:
            restarts = 1

            def __init__(self, make_sampler, timeout_s):
                self.timeout_s = timeout_s

            def sample(self):
                return outcomes.pop(0) if outcomes else ({"cluster": {}}, None)

            def close(self):
                pass
        before = signal.getsignal(signal.SIGTERM)
        try:
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(g12, "_packet", return_value={"namespace": NS}), \
                    patch.object(g12, "SampleSupervisor", Supervisor), \
                    patch.object(g12, "local_sample", return_value={"ports": {"8210": True}}):
                out = Path(tmp) / "monitor.jsonl"
                g12.monitor(tmp, out, seconds=0.35, period=0.1)
                rows = g12.load_jsonl(out)
        finally:
            signal.signal(signal.SIGTERM, before)
        self.assertEqual(rows[1]["k"], "sample_error")
        self.assertEqual((rows[1]["error"], rows[1]["phase"], rows[1]["restarts"]), ("SIGSEGV", "describe_cluster", 1))
        self.assertNotIn("ports", rows[1])  # a crashed sample observes nothing
        self.assertEqual(rows[2]["ports"], {"8210": True})
        self.assertEqual(rows[-1]["k"], "end")


class MonitorGapRuleTests(unittest.TestCase):
    FAULT = {"k": "fault", "name": "stream_restart", "kind": "stream", "injected_ns": 60 * S, "restart_ns": 61 * S}

    def run_fault(self, monitor):
        pubs, views = [], []
        for venue in g12.VENUES:
            p, v = stream_rows(venue, 0, 500 * S)
            pubs += p
            views += v
            p, v = md_rows(venue, 0, 500 * S)
            pubs += p
            views += v
        return g12.evaluate_fault(self.FAULT, pubs, views, sorted(monitor, key=lambda r: r["t"]))

    @staticmethod
    def sample(t_s, up):
        return {"t": int(t_s * S), "ports": {"8210": up}, "projector_lag": 0, "under_replicated": 0}

    def test_crash_samples_are_excluded_and_a_short_hole_still_passes(self):
        monitor = [self.sample(t, t >= 64) for t in range(0, 500) if not 62 <= t <= 63]
        # Crashed samples in the hole: even carrying an "up" port they are never evidence.
        monitor += [{"t": t * S, "k": "sample_error", "error": "SIGSEGV", "phase": "describe_cluster",
                     "ports": {"8210": True}} for t in (62, 63)]
        result = self.run_fault(monitor)
        self.assertEqual(result["fault_cleared_ns"], 64 * S)
        self.assertEqual(result["monitor_max_gap_s"], 3.0)  # 61 -> 64 s, restart to clearance
        self.assertEqual(result["parts"]["monitor"], "PASS")
        self.assertEqual([e["error"] for e in result["monitor_sample_errors"]], ["SIGSEGV", "SIGSEGV"])
        self.assertTrue(result["pass"])

    def test_a_gap_over_five_seconds_at_clearance_is_not_measured(self):
        monitor = [self.sample(t, t >= 62) for t in range(0, 500) if not 55 <= t <= 67]
        monitor += [{"t": 61.5 * S, "k": "sample_error", "error": "SIGSEGV", "phase": "list_offsets:x",
                     "ports": {"8210": True}}]
        result = self.run_fault(monitor)
        self.assertEqual(result["fault_cleared_ns"], 68 * S)  # the first valid sample after the hole
        self.assertGreater(result["monitor_max_gap_s"], g12.MONITOR_MAX_GAP_S)
        self.assertEqual((result["parts"]["monitor"], result["reason"]), ("NOT_MEASURED", "MONITOR_GAP"))
        self.assertFalse(result["pass"])

    def test_gap_helper_counts_edges(self):
        rows = [{"t": 3 * S}, {"t": 4 * S, "k": "sample_error"}, {"t": 9 * S}]
        self.assertEqual(g12.monitor_max_gap_s(rows, 0, 10 * S), 6.0)
        self.assertEqual(g12.monitor_max_gap_s([], 0, 2 * S), 2.0)
        self.assertEqual(g12.cleared_at("stream", [{"t": 5, "k": "sample_error", "ports": {"8210": True}}], 0), None)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------- caught-up predicate v2 and the moving top

def core_emissions(frames, interval_ms=1000):
    """The core's BOOK_SNAPSHOT materialization (due_l2_materialized_snapshot): first frame, then a frame whose
    integer-ms provider time is >= interval after the last emission, or >= 100 ms with another top."""
    out, last = [], None
    for t, seq, top in frames:
        ms = t // MS
        if last is None or ms - last[0] >= interval_ms or (top != last[1] and ms - last[0] >= 100):
            out.append((t, seq))
            last = (ms, top)
    return out


def book_run(venue, start, end, *, moving=False, transit_ms=100, keep=lambda i: True, interval_ms=1000,
             freeze=None, sample_ms=50, hz=4):
    """Publisher BOOK rows (4 Hz, +0..2 ms deterministic stamp jitter; ``moving``: the publisher's book_top, else a
    fixed top with no top recorded), the core's emissions, and sampler rows every ``sample_ms`` showing the newest
    kept emission ``transit_ms`` after its frame. ``freeze=(from_ns, to_ns)``: no view update applies in it."""
    period = S // hz
    frames, pubs = [], []
    t, seq = start, 1001
    while t < end:
        stamp = t + (seq * 7919 % 3) * MS
        top = g12.book_top(seq) if moving else None
        frames.append((stamp, seq, top))
        row = {"k": "pub", "t": stamp, "venue": venue, "seq": seq, "gen": 7, "snapshot": seq == 1001}
        if moving:
            row["top"] = list(top)
        pubs.append(row)
        t += period
        seq += 1
    emitted = [e for i, e in enumerate(core_emissions(frames, interval_ms)) if keep(i)]
    updates = [(pub + transit_ms * MS, seq) for pub, seq in emitted]
    if freeze is not None:
        updates = [(at, seq) for at, seq in updates if not freeze[0] <= at < freeze[1]]
    views, k, shown = [], -1, None
    t = start
    while t < end:
        while k + 1 < len(updates) and updates[k + 1][0] <= t:
            k += 1
            shown = updates[k][1]
        if shown is not None:
            views.append({"k": "view", "t": t, "venue": venue, "seq": shown, "v": "APPROVED"})
        t += sample_ms * MS
    return pubs, views, emitted


def caught(pubs, views, venue, cleared, predicate=None):
    return g12.caught_up(g12.view_series(views), g12.publish_index(pubs), venue, cleared,
                         frames=g12.book_frames(pubs), predicate=predicate)


class CaughtUpPredicateV2Tests(unittest.TestCase):
    CLEARED = 100 * S

    def test_parameters_come_from_the_core_not_from_data(self):
        default = g12.caught_up_predicate()
        self.assertEqual((default["version"], default["transit_max_ms"], default["hold_s"], default["slack_ms"],
                          default["publisher_tick_ms"], default["top_change_min_interval_ms"]),
                         ("p184.g12.caught_up.v2", 1000, 30, 1, 250.0, 100))
        self.assertEqual(default["materialized_snapshot_interval_ms"], {"BINANCE": 1000, "OKX": 1000})
        from qdl.runtime.execution_l2 import EXECUTION_L2_MATERIALIZATION_INTERVAL_MS
        self.assertEqual(g12.L2_DEFAULT_MATERIALIZATION_MS, EXECUTION_L2_MATERIALIZATION_INTERVAL_MS)
        core = (Path(g12.ROOT) / "rust/qdl-realtime-core/src/lib.rs").read_text()
        self.assertEqual(int(re.search(r"const L2_TOP_CHANGE_MIN_INTERVAL_MS: i64 = (\d+);", core).group(1)),
                         g12.L2_TOP_CHANGE_MIN_INTERVAL_MS)
        # The core resolves the interval as L2Binding::materialized_snapshot_interval_ms does.
        config = {"core": {"bindings": [
            {"venue": "BINANCE", "l2": {"snapshot_refresh_seconds": 30, "materialized_snapshot_interval_ms": 1000}},
            {"venue": "OKX", "l2": {"snapshot_refresh_seconds": 30}},
            {"venue": "OKX", "provider_kind": "okx_trade"}]}}
        frozen = g12.caught_up_predicate(config, 4.0)
        self.assertEqual(frozen["materialized_snapshot_interval_ms"], {"BINANCE": 1000, "OKX": 30000})
        self.assertEqual(frozen["materialization_interval_source"], "core.json")
        with self.assertRaises(ValueError):
            g12.caught_up_predicate({"core": {"bindings": [config["core"]["bindings"][0]]}})

    def test_quiet_but_timely_passes(self):
        # Fixed top: one snapshot per ~1 s (4 or 5 frames); every sample of a timely view, not an age limit.
        pubs, views, emitted = book_run("BINANCE", 90 * S, 200 * S, transit_ms=100)
        gaps = {round((b[0] - a[0]) / (250 * MS)) for a, b in zip(emitted, emitted[1:])}
        self.assertTrue(gaps <= {4, 5} and 5 in gaps)  # the integer-ms rule moves some emissions a frame later
        index = g12.publish_index(pubs)
        ages = [(v["t"] - index[("BINANCE", v["seq"])]) / MS for v in views if v["t"] >= self.CLEARED]
        self.assertGreater(max(ages), 1000)  # the v1 per-sample age limit failed exactly this timely view
        state = caught(pubs, views, "BINANCE", self.CLEARED)
        self.assertIsNotNone(state["caught_up_ns"])
        self.assertLess(state["caught_up_ns"] - self.CLEARED, int(1.5 * S))
        self.assertEqual(state["rejected_samples"].get("update_gap", 0), 0)
        self.assertEqual(state["rejected_samples"].get("open_gap", 0), 0)

    def test_moving_top_fires_the_top_change_rule_and_passes(self):
        pubs, views, emitted = book_run("OKX", 90 * S, 200 * S, moving=True, transit_ms=150)
        frames = len(pubs)
        self.assertGreater(len(emitted), 0.8 * frames)  # 32 of 40 frames move the top, each one emitted
        state = caught(pubs, views, "OKX", self.CLEARED)
        self.assertIsNotNone(state["caught_up_ns"])
        self.assertLess(state["caught_up_ns"] - self.CLEARED, S)

    def test_slow_transit_fails(self):
        for moving in (False, True):
            with self.subTest(moving=moving):
                pubs, views, _ = book_run("BINANCE", 90 * S, 200 * S, moving=moving, transit_ms=1100)
                state = caught(pubs, views, "BINANCE", self.CLEARED)
                self.assertIsNone(state["caught_up_ns"])
                self.assertGreater(state["rejected_samples"]["update_transit"], 0)

    def test_large_update_gap_fails(self):
        # Quiet: the view applies every other snapshot (2-2.5 s source gap > 1000 + 250 + 1 ms).
        pubs, views, _ = book_run("BINANCE", 90 * S, 200 * S, keep=lambda i: i % 2 == 0)
        state = caught(pubs, views, "BINANCE", self.CLEARED)
        self.assertIsNone(state["caught_up_ns"])
        self.assertGreater(state["rejected_samples"]["update_gap"], 0)
        # Moving top: every third frame (750 ms) > max(100, 250) + one 250 ms tick + 1 ms.
        pubs, views, _ = book_run("OKX", 90 * S, 200 * S, moving=True, keep=lambda i: i % 3 == 0)
        state = caught(pubs, views, "OKX", self.CLEARED)
        self.assertIsNone(state["caught_up_ns"])
        self.assertGreater(state["rejected_samples"]["update_gap"], 0)

    def test_stale_after_fault_fails(self):
        for moving in (False, True):
            with self.subTest(moving=moving):
                # Stuck on a post-clear frame for the rest of the run: never caught up (open gap).
                pubs, views, _ = book_run("OKX", 90 * S, 200 * S, moving=moving, freeze=(101 * S, 300 * S))
                state = caught(pubs, views, "OKX", self.CLEARED)
                self.assertIsNone(state["caught_up_ns"])
                self.assertGreater(state["rejected_samples"]["open_gap"], 0)
                # Stuck for 5 s, then timely again: caught up only after the stall, not across it.
                pubs, views, _ = book_run("OKX", 90 * S, 200 * S, moving=moving, freeze=(101 * S, 106 * S))
                state = caught(pubs, views, "OKX", self.CLEARED)
                self.assertGreaterEqual(state["caught_up_ns"], 106 * S)
                self.assertLess(state["caught_up_ns"], 108 * S)

    def test_frozen_predicate_is_read_back_and_used(self):
        # Core configured with a 2 s quiet interval: the frozen run parameters pass it, the default ones do not.
        pubs, views, _ = book_run("BINANCE", 90 * S, 200 * S, interval_ms=2000)
        self.assertIsNone(caught(pubs, views, "BINANCE", self.CLEARED)["caught_up_ns"])
        frozen = dict(g12.caught_up_predicate(), materialized_snapshot_interval_ms={"BINANCE": 2000, "OKX": 2000})
        self.assertIsNotNone(caught(pubs, views, "BINANCE", self.CLEARED, frozen)["caught_up_ns"])
        with tempfile.TemporaryDirectory() as tmp:
            evidence = Path(tmp)
            self.assertFalse(g12.load_predicate(evidence)["frozen"])
            self.assertFalse(g12.evaluate(evidence)["caught_up_predicate"]["frozen"])
            (evidence / g12.PREDICATE_FILE).write_text(json.dumps(frozen))
            loaded = g12.load_predicate(evidence)
            self.assertTrue(loaded["frozen"])
            self.assertEqual(loaded["materialized_snapshot_interval_ms"]["BINANCE"], 2000)
            self.assertEqual(g12.evaluate(evidence)["caught_up_predicate"], loaded)
            (evidence / g12.PREDICATE_FILE).write_text(json.dumps(dict(frozen, version="p184.g12.caught_up.v1")))
            with self.assertRaises(ValueError):
                g12.load_predicate(evidence)

    def test_prepare_freezes_the_predicate_before_data(self):
        """Run.prepare writes evidence/caught-up-predicate.json from the run's core.json once the material exists,
        before any role (publisher included) starts; it is never overwritten."""
        core = {"raw_topics": [NS + ".raw"], "core": {"bindings": [
            {"venue": "BINANCE", "l2": {"snapshot_refresh_seconds": 30, "materialized_snapshot_interval_ms": 1000}},
            {"venue": "OKX", "l2": {"snapshot_refresh_seconds": 30, "materialized_snapshot_interval_ms": 500}}]}}
        calls = []

        def sh(argv, timeout=60, check=True):
            calls.append(list(argv))
            if "--material" in argv:
                (run.work / "packet.json").write_text(json.dumps({"namespace": NS}))
                (run.work / "core.json").write_text(json.dumps(core))
                (run.work / "broker.properties").write_text("ssl.keystore.password=x\n")
            return ""

        with tempfile.TemporaryDirectory() as tmp:
            export = Path(tmp) / "export.json"
            export.write_text("{}")
            run = g12.Run(Path(tmp) / "run", export, NS)
            with patch.object(g12, "sh", side_effect=sh), \
                    patch.object(g12, "host_resources", return_value={"disk_free_bytes": 10**12,
                                                                      "mem_available_bytes": 10**12}), \
                    patch.object(g12, "image_record", side_effect=lambda image: {"id": image}), \
                    patch.object(g12, "owned_names", return_value=[]), \
                    patch.object(g12, "production_guard", return_value={}), \
                    patch.object(g12.Run, "start_events"):
                run.prepare()
            frozen = json.loads((run.evidence / g12.PREDICATE_FILE).read_text())
            self.assertEqual(frozen, g12.caught_up_predicate(core, g12.PUBLISH_HZ))
            self.assertEqual(frozen["materialized_snapshot_interval_ms"], {"BINANCE": 1000, "OKX": 500})
            self.assertEqual(run.predicate, frozen)
            notes = [json.loads(line) for line in (run.evidence / "orchestrator.jsonl").read_text().splitlines()]
            self.assertEqual(notes[-1]["step"], "caught_up_predicate_frozen")
            self.assertFalse(any("--publish" in argv for argv in calls))  # no data source started yet
            self.assertFalse((run.evidence / "publish.jsonl").exists())


class CaptureAdapterModeTests(unittest.TestCase):
    """CAPTURE adapter mode (RCA 2026-10-14): real-capture replay keeps the source catalog adapter_version;
    SYNTHETIC (default) is unchanged. Real offline material, python image (qdl, yaml, cryptography)."""

    RAW_SHA = "ab" * 32

    def material(self, tmp, **handoff):
        from scripts import p184_dr05_dl_fault as dr
        from tests.test_p184_dr05_dl_fault import MaterialTests
        work = Path(tmp) / "work"
        work.mkdir()
        (work / "handoff.json").write_text(json.dumps({"namespace": NS, "images": g12.dr05_images(g12.IMAGES),
                                                       "g12_images": g12.IMAGES, **handoff}))
        dr.material(work, MaterialTests.exported())
        return work, g12.seal_market_feeds(work, NS)

    def capture(self, tmp):
        return self.material(tmp, provenance="TEST_ONLY", adapter_mode="CAPTURE",
                             capture_raw_sha256=self.RAW_SHA, test_clock_images=True)

    @staticmethod
    def raws(work, override=None):
        """One raw per bound route with the catalog's value (override: route index -> adapter_version)."""
        import yaml
        catalog = yaml.safe_load((work / "catalog.yaml").read_text())
        versions = {b["source"]["source_id"]: b["source"]["adapter_version"] for b in catalog["bindings"]}
        out = []
        for index, binding in enumerate(json.loads((work / "core.json").read_text())["core"]["bindings"]):
            version = (override or {}).get(index, versions[binding["source_id"]])
            out.append((g12.physical_route(binding), version))
        return out

    def test_synthetic_default_is_unchanged(self):
        import yaml
        with tempfile.TemporaryDirectory() as tmp:
            work, sealed = self.material(tmp)
            catalog = yaml.safe_load((work / "catalog.yaml").read_text())
            self.assertEqual({b["source"]["adapter_version"] for b in catalog["bindings"]}, {g12.TEST_ADAPTER})
            self.assertEqual(sealed["adapter_mode"], "SYNTHETIC")
            with self.assertRaises(ValueError):  # no parity proof on synthetic material
                g12.capture_adapter_parity(work, self.raws(work), self.RAW_SHA)

    def test_capture_keeps_source_adapter_and_proves_parity(self):
        import yaml
        from scripts.p184_dr05_dl_fault import source_catalog_path
        from qdl.runtime.stable_catalog import StableSourceCatalog
        with tempfile.TemporaryDirectory() as tmp:
            work, sealed = self.capture(tmp)
            source = {b["source"]["source_id"]: b["source"]["adapter_version"]
                      for b in yaml.safe_load(source_catalog_path(work).read_text())["bindings"]}
            catalog = yaml.safe_load((work / "catalog.yaml").read_text())
            self.assertEqual(len(catalog["bindings"]), 4 + 8)
            for binding in catalog["bindings"]:
                self.assertEqual(binding["source"]["adapter_version"], source[binding["source"]["source_id"]])
                self.assertNotEqual(binding["source"]["adapter_version"], g12.TEST_ADAPTER)
            self.assertEqual(sealed["adapter_mode"], "CAPTURE")
            self.assertEqual(set(sealed["adapter_versions"].values()), {"binance-usdm/2.0.0", "okx-v5/2.0.0"})
            StableSourceCatalog.load(work / "catalog.yaml")  # the bundle/catalog generation still loads
            receipt = g12.capture_adapter_parity(work, self.raws(work) * 3, self.RAW_SHA)
            self.assertEqual((receipt["status"], receipt["relabelled"], receipt["selected_raw_frames"]),
                             ("PROVEN", False, 3 * 11))
            self.assertEqual(receipt["adapter_versions"], ["binance-usdm/2.0.0", "okx-v5/2.0.0"])
            self.assertEqual(len(receipt["routes"]), 11)
            self.assertTrue(all(r["raw_frames"] == 3 for r in receipt["routes"]))
            with self.assertRaisesRegex(ValueError, "differs from its binding"):  # one raw off
                g12.capture_adapter_parity(work, self.raws(work) + self.raws(work, {3: g12.TEST_ADAPTER}),
                                           self.RAW_SHA)
            with self.assertRaisesRegex(ValueError, "exactly this capture"):
                g12.capture_adapter_parity(work, self.raws(work), "cd" * 32)
            with self.assertRaisesRegex(ValueError, "outside the bound routes"):
                g12.capture_adapter_parity(work, self.raws(work) + [(("X",) * 6, "okx-v5/2.0.0")], self.RAW_SHA)
            with self.assertRaisesRegex(ValueError, "every bound route"):
                g12.capture_adapter_parity(work, self.raws(work)[1:], self.RAW_SHA)

    def test_capture_mode_handoff_must_be_fully_sealed(self):
        from scripts.p184_dr05_dl_fault import adapter_mode
        sealed = {"namespace": NS, "provenance": "TEST_ONLY", "adapter_mode": "CAPTURE",
                  "capture_raw_sha256": self.RAW_SHA, "test_clock_images": True}
        self.assertEqual(adapter_mode(sealed), "CAPTURE")
        self.assertEqual(adapter_mode({"namespace": NS}), "SYNTHETIC")
        for key, value in (("provenance", "PRODUCTION"), ("capture_raw_sha256", "short"),
                           ("test_clock_images", False), ("namespace", "prod"), ("adapter_mode", "RELABEL")):
            with self.subTest(key=key), self.assertRaises(ValueError):
                adapter_mode({**sealed, key: value})
        with self.assertRaises(ValueError):  # a capture field without the mode is never silently synthetic
            adapter_mode({"namespace": NS, "capture_raw_sha256": self.RAW_SHA})

    def test_run_requires_test_clock_images_and_seals_the_pin(self):
        calls = []

        def sh(argv, timeout=60, check=True):
            calls.append(list(argv))
            if "--material" in argv:
                (run.work / "packet.json").write_text(json.dumps({"namespace": NS}))
                (run.work / "core.json").write_text(json.dumps({"raw_topics": [NS + ".raw"], "core": {"bindings": [
                    {"venue": v, "l2": {"snapshot_refresh_seconds": 30}} for v in g12.VENUES]}}))
                (run.work / "broker.properties").write_text("ssl.keystore.password=x\n")
            return ""

        def record(labelled):
            return lambda image: {"id": image, "test_only": "isolated-replay-clock" if image in labelled else None}

        for labelled_roles, ok in ((("core", "stream"), True), (("core",), False)):
            with self.subTest(labelled=labelled_roles), tempfile.TemporaryDirectory() as tmp:
                export = Path(tmp) / "export.json"
                export.write_text("{}")
                run = g12.Run(Path(tmp) / "run", export, NS)
                with self.assertRaises(ValueError):
                    run.use_capture_adapter("not-a-sha")
                run.use_capture_adapter(self.RAW_SHA)
                labelled = {run.images[role] for role in labelled_roles}
                with patch.object(g12, "sh", side_effect=sh), \
                        patch.object(g12, "host_resources", return_value={"disk_free_bytes": 10**12,
                                                                          "mem_available_bytes": 10**12}), \
                        patch.object(g12, "image_record", side_effect=record(labelled)), \
                        patch.object(g12, "owned_names", return_value=[]), \
                        patch.object(g12, "production_guard", return_value={}), \
                        patch.object(g12.Run, "start_events"):
                    if not ok:
                        with self.assertRaisesRegex(RuntimeError, "stream image lacks"):
                            run.prepare()
                        self.assertFalse((run.work / "handoff.json").exists())
                        continue
                    run.prepare()
                handoff = json.loads((run.work / "handoff.json").read_text())
                self.assertEqual((handoff["adapter_mode"], handoff["capture_raw_sha256"], handoff["test_clock_images"]),
                                 ("CAPTURE", self.RAW_SHA, True))
                with self.assertRaises(ValueError):  # fixed before prepare
                    run.use_capture_adapter(self.RAW_SHA)


class PublisherBookTopTests(unittest.TestCase):
    def test_top_is_deterministic_bounded_moving_then_quiet(self):
        shape = g12.BOOK_TOP
        tops = [g12.book_top(seq) for seq in range(1001, 1001 + 4 * shape["cycle"])]
        self.assertEqual(tops, [g12.book_top(seq) for seq in range(1001, 1001 + 4 * shape["cycle"])])
        for bid, ask in tops:
            self.assertTrue(shape["base"] <= int(bid) <= shape["base"] + shape["steps"] - 1)
            self.assertEqual(int(ask) - int(bid), shape["spread"])
        moving = shape["cycle"] - shape["quiet"]
        for seq in range(1000, 1000 + 2 * shape["cycle"]):
            changed = g12.book_top(seq + 1) != g12.book_top(seq)
            self.assertEqual(changed, (seq + 1) % shape["cycle"] < moving, seq)
        self.assertEqual(len(set(tops)), shape["steps"])

    def test_levels_and_frames_keep_one_level_per_side(self):
        from scripts import p184_dr05_dl_fault as dr
        at = 1_791_696_000_123 * MS
        for seq in range(1001, 1001 + 2 * g12.BOOK_TOP["cycle"]):
            levels = g12.book_levels(seq, seq - 1, False, "5")
            bid, ask = g12.book_top(seq)
            self.assertEqual((levels["bids"][0], levels["asks"][0]), ((bid, "5"), (ask, "1")))
            for side in ("bids", "asks"):
                prices = [price for price, _ in levels[side]]
                self.assertEqual(len(prices), len(set(prices)))  # the core refuses a duplicate price
                self.assertTrue(all(qty == "0" for _, qty in levels[side][1:]))
            self.assertLess(max(int(p) for p, q in levels["bids"] if q != "0"),
                            min(int(p) for p, q in levels["asks"] if q != "0"))
        self.assertEqual(g12.book_levels(1039, 0, True, "5"), {"bids": [("60007", "5")], "asks": [("60017", "1")]})
        b = g12.book_frame("BINANCE", False, 1009, 1008, 7, at)["payload"]
        self.assertEqual((b["U"], b["u"], b["pu"], b["b"], b["a"]),
                         (1008, 1009, 1008, [["60001", "10"], ["60000", "0"]], [["60011", "1"], ["60010", "0"]]))
        o = g12.book_frame("OKX", True, 1009, 1008, 8, at)["payload"]["data"][0]
        self.assertEqual((o["prevSeqId"], o["bids"], o["asks"]), ("-1", [["60001", "10", "0", "1"]],
                                                                  [["60011", "1", "0", "1"]]))
        # The DR05 cells keep the fixed default book byte for byte.
        self.assertEqual(dr.frame("BINANCE", False, 5, 4, 7, "3", at)["payload"],
                         {"s": "ETHUSDT", "U": 4, "u": 5, "pu": 4, "E": 1_791_696_000_123,
                          "b": [["60000", "3"]], "a": [["60001", "1"]]})
        self.assertEqual(dr.frame("OKX", True, 5, 0, 7, "3", at)["payload"]["data"][0]["bids"],
                         [["60000", "3", "0", "1"]])

    def test_core_rule_fires_on_top_change_and_quiet_interval(self):
        frames = [(t * 250 * MS, seq, g12.book_top(seq)) for t, seq in enumerate(range(1000, 1400))]
        emitted = core_emissions(frames)
        gaps = sorted({(b[0] - a[0]) // MS for a, b in zip(emitted, emitted[1:])})
        self.assertEqual(gaps, [250, 1000])  # moving: every frame; quiet 2 s: the 1 s rule
        fixed = core_emissions([(t, seq, None) for t, seq, _ in frames])
        self.assertEqual({(b[0] - a[0]) // MS for a, b in zip(fixed, fixed[1:])}, {1000})

    def test_market_data_is_unchanged_by_the_book_top(self):
        def run(interleave):
            clock = iter(range(1_791_696_000_000 * MS, 1_791_697_000_000 * MS, 7 * MS))
            source = g12.MarketDataSource(clock=lambda: next(clock))
            events = source.seed(1_791_696_000_000 * MS)
            for tick in range(40):
                if interleave:
                    g12.book_frame("BINANCE", False, 1001 + tick, 1000 + tick, 7, 1_791_696_000_000 * MS)
                events += source.tick(tick, 4.0)
            return events
        plain, interleaved = run(False), run(True)
        self.assertEqual(plain, interleaved)
        feeds = {e["feed"] for e in plain}
        self.assertTrue({"TRADE", "QUOTE"} <= feeds)


BOOK_IDENTITY = {"BINANCE": ("BINANCE_DIRECT", "USDM", "ETHUSDT", "ethusdt@depth@100ms",
                             "binance-usdm-ethusdt-book-primary-v2"),
                 "OKX": ("OKX_DIRECT", "SWAP", "ETH-USDT-SWAP", "books", "okx-swap-eth-usdt-swap-book-primary-v2")}


def book_binding(venue):
    """The sealed core.json BOOK binding shape (r2b6 run) of one venue."""
    provider, market, symbol, channel, source = BOOK_IDENTITY[venue]
    return {"venue": venue, "provider": provider, "market": market, "product_type": "PERPETUAL",
            "native_symbol": symbol, "native_channel": channel, "source_id": source,
            "instrument_catalog_revision": 11, "l2": {"snapshot_refresh_seconds": 30}}


def resync_request(venue, generation, received_at_ns, *, session=None, summary=g12.L2_RESNAPSHOT_REQUIRED_V1,
                   reason="QUARANTINE_REASON_SEQUENCE_GAP", test_provenance=True, quarantined_at_ns=None):
    """(wire, event_id) of a core resnapshot request built as the Rust core builds it
    (``l2_resnapshot_request``: the raw frame, evidence = sha256(raw || reason be32), id = sha256(capture || V1))."""
    import hashlib
    from qdl.provider.v1.raw_provider_pb2 import QuarantineReason, QuarantineRecord, RawProviderEnvelope
    b = book_binding(venue)
    payload = b'{"u":1}'
    raw = RawProviderEnvelope(
        capture_id=bytes(16), provider=b["provider"], venue=venue, market=b["market"],
        product_type=b["product_type"], native_symbol=b["native_symbol"], native_channel=b["native_channel"],
        subscription_id=b["source_id"], source_session_id=session or f"{NS}.{venue}",
        connection_generation=generation, received_at_ns=received_at_ns, raw_frame_bytes=payload,
        raw_frame_sha256=hashlib.sha256(payload).digest(), adapter_version="p184-synthetic-test-only",
        instrument_catalog_revision=11, test_provenance=test_provenance)
    code = QuarantineReason.Value(reason)
    record = QuarantineRecord(
        raw=raw, reason=code, safe_summary=summary,
        quarantined_at_ns=received_at_ns + MS if quarantined_at_ns is None else quarantined_at_ns,
        evidence_sha256=hashlib.sha256(raw.SerializeToString() + code.to_bytes(4, "big", signed=True)).digest())
    event_id = hashlib.sha256(raw.capture_id + summary.encode()).digest()
    return record.SerializeToString(), event_id


def fake_kafka(producer, deliveries):
    """A confluent_kafka stand-in: ``deliveries`` is a list of (after_consume_calls, make_record) quarantine
    records the publisher's read-only listener receives; ``make_record()`` -> (wire, event_id) runs at delivery,
    so a request can name a raw frame received after the lane's snapshot, as the core's requests do."""
    class Message:
        def __init__(self, wire, event_id, offset):
            self.wire, self.event_id, self._offset = wire, event_id, offset

        def error(self):
            return None

        def value(self):
            return self.wire

        def headers(self):
            return [("qdl-event-id", self.event_id)]

        def offset(self):
            return self._offset

    class Consumer:
        assigned, closed = [], []

        def __init__(self, config):
            self.config, self.calls, self.pending = config, 0, list(deliveries)
            Consumer.config_seen = config

        def assign(self, partitions):
            Consumer.assigned.extend(partitions)

        def list_topics(self, topic, timeout):
            meta = types.SimpleNamespace(error=None, partitions={5: None, 0: None, 3: None, 1: None, 2: None, 4: None})
            return types.SimpleNamespace(topics={topic: meta})

        def consume(self, num_messages, timeout):
            self.calls += 1
            ready = [d for d in self.pending if d[0] <= self.calls]
            self.pending = [d for d in self.pending if d[0] > self.calls]
            return [Message(*make(), offset) for offset, (_, make) in enumerate(ready)]

        def close(self):
            Consumer.closed.append(True)

    fake = types.ModuleType("confluent_kafka")
    fake.Producer, fake.Consumer, fake.OFFSET_BEGINNING = producer, Consumer, -2
    fake.TopicPartition = lambda topic, partition, offset: (topic, partition, offset)
    return fake


class ResyncListenerAssignTests(unittest.TestCase):
    def test_missing_or_failed_topic_metadata_is_an_error(self):
        fake = fake_kafka(None, [])
        for topics in ({}, {NS + ".quarantine": types.SimpleNamespace(error="UNKNOWN_TOPIC", partitions={0: None})},
                       {NS + ".quarantine": types.SimpleNamespace(error=None, partitions={})}):
            consumer = types.SimpleNamespace(list_topics=lambda topic, timeout, t=topics: types.SimpleNamespace(topics=t),
                                             assign=lambda partitions: self.fail("assigned without metadata"))
            with patch.dict(sys.modules, {"confluent_kafka": fake}), self.assertRaises(RuntimeError):
                g12.assign_resync_listener(consumer, NS + ".quarantine", -2)


class BookResyncRequestTests(unittest.TestCase):
    """book_resync_request: the TEST twin of the ingestor's matches_book_resync + snapshot_satisfies_resync."""

    NOW = 1_792_000_000 * S

    def check(self, wire_and_id, venue="BINANCE", generation=7, anchor=None, now=None):
        wire, event_id = wire_and_id
        return g12.book_resync_request(wire, event_id, book_binding(venue), f"{NS}.{venue}", generation,
                                       self.NOW if now is None else now, anchor)

    def test_own_lane_current_generation_fresh_request_is_accepted(self):
        self.assertEqual(self.check(resync_request("BINANCE", 7, self.NOW - 2 * S)), (True, None))
        self.assertEqual(self.check(resync_request("OKX", 9, self.NOW - 2 * S), venue="OKX", generation=9),
                         (True, None))

    def test_other_records_are_ignored_without_a_refusal(self):
        self.assertEqual(self.check((b"\xff\x00garbage", b"")), (False, None))
        self.assertEqual(self.check(resync_request("BINANCE", 7, self.NOW - S, summary="SEQUENCE_GAP")),
                         (False, None))
        # The OKX lane's request is not the Binance lane's business (and vice versa).
        self.assertEqual(self.check(resync_request("OKX", 7, self.NOW - S)), (False, None))

    def test_refusals_mirror_the_ingestor(self):
        cases = {
            "GENERATION": self.check(resync_request("BINANCE", 6, self.NOW - S)),
            "SESSION": self.check(resync_request("BINANCE", 7, self.NOW - S, session=NS + ".OKX")),
            # Older than l2.snapshot_refresh_seconds (30 s): the ingestor drops it, so does the TEST lane.
            "AGE": self.check(resync_request("BINANCE", 7, self.NOW - 31 * S)),
            "REASON": self.check(resync_request("BINANCE", 7, self.NOW - S,
                                                reason="QUARANTINE_REASON_UNSPECIFIED")),
            "IDENTITY": self.check(resync_request("BINANCE", 7, self.NOW - S, test_provenance=False)),
        }
        for label, result in cases.items():
            self.assertEqual(result, (False, label), label)
        future = resync_request("BINANCE", 7, self.NOW - S, quarantined_at_ns=self.NOW + S)
        self.assertEqual(self.check(future), (False, "AGE"))

    def test_digests_are_verified(self):
        wire, event_id = resync_request("BINANCE", 7, self.NOW - S)
        self.assertEqual(self.check((wire, bytes(32))), (False, "DIGEST"))
        from qdl.provider.v1.raw_provider_pb2 import QuarantineRecord
        record = QuarantineRecord.FromString(wire)
        record.evidence_sha256 = bytes(32)
        self.assertEqual(self.check((record.SerializeToString(), event_id)), (False, "DIGEST"))
        record = QuarantineRecord.FromString(wire)
        record.raw.raw_frame_bytes = b'{"u":2}'
        self.assertEqual(self.check((record.SerializeToString(), event_id)), (False, "DIGEST"))

    def test_a_request_already_satisfied_by_the_published_snapshot_is_refused(self):
        request = resync_request("BINANCE", 7, self.NOW - 2 * S)
        self.assertEqual(self.check(request, anchor=(7, self.NOW - 2 * S)), (False, "SATISFIED"))
        self.assertEqual(self.check(request, anchor=(7, self.NOW - 3 * S)), (True, None))
        self.assertEqual(self.check(request, anchor=(6, self.NOW)), (True, None))


class PublisherResyncTests(unittest.TestCase):
    """publish(): a core resnapshot request on <ns>.quarantine becomes gen+1 + snapshot on that lane only."""

    def test_core_request_starts_a_new_generation_with_a_snapshot(self):
        book_frames = []

        class Producer:
            def __init__(self, config):
                pass

            def produce(self, topic, partition, key, value, headers, on_delivery):
                pass

            def poll(self, timeout):
                return 0

            def flush(self, timeout):
                return 0

        class Raw:
            subscription_id, capture_id = "sub", "cap"

            def SerializeToString(self):
                return b"x"

        def book_raw(binding, frame, ns):
            book_frames.append((binding["venue"], frame["connection_generation"], frame["snapshot"]))
            return Raw()

        # Each request names a generation-7 raw received after the lane's snapshot (wall clock at delivery).
        def request():
            return resync_request("BINANCE", 7, time.time_ns() - MS)
        lanes = {(v, f): {"physical_native_symbol": "ETH-USDT"} for v in g12.VENUES for f in g12.MD_FEEDS}
        lanes[("OKX", "INDEX")] = {"physical_native_symbol": "ETH-USDT"}
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            (work / "core.json").write_text(json.dumps({
                "raw_topics": [NS + ".raw"], "core": {"allow_test_provenance": True,
                                                       "quarantine_stream": NS + ".quarantine",
                                                       "bindings": [book_binding(v) for v in g12.VENUES]}}))
            out = work / "publish.jsonl"
            fake = fake_kafka(Producer, [(3, request), (8, request)])
            with patch.dict(sys.modules, {"confluent_kafka": fake}), \
                    patch.object(g12, "_packet", return_value={"namespace": NS}), \
                    patch.object(g12, "_security", return_value={}), \
                    patch.object(g12, "md_bindings", return_value=lanes), \
                    patch.object(g12, "md_envelope", return_value=Raw()), \
                    patch("scripts.p184_dr05_raw_publisher.raw_envelope", side_effect=book_raw):
                g12.publish(work, out, 2.0, 4.0)
            rows = g12.load_jsonl(out)
        resyncs = [r for r in rows if r.get("k") == "resync"]
        self.assertEqual([(r["venue"], r["new_gen"], r["cause"]) for r in resyncs],
                         [("BINANCE", 8, g12.L2_RESNAPSHOT_REQUIRED_V1)])
        # The repeated request names generation 7, which is no longer current: refused, no second resync.
        self.assertIn({"k": "resync_request_refused", "venue": "BINANCE", "reason": "GENERATION"},
                      [{k: r[k] for k in ("k", "venue", "reason")} for r in rows
                       if r.get("k") == "resync_request_refused"])
        binance = [(g, snap) for v, g, snap in book_frames if v == "BINANCE"]
        okx = [(g, snap) for v, g, snap in book_frames if v == "OKX"]
        self.assertEqual(binance[0], (7, True))
        first_new = binance.index((8, True))
        self.assertTrue(all(g == 7 and not snap for g, snap in binance[1:first_new]))
        self.assertTrue(all(g == 8 and not snap for g, snap in binance[first_new + 1:]))
        self.assertEqual(okx[0], (7, True))
        self.assertTrue(all(g == 7 and not snap for g, snap in okx[1:]))
        # Every partition of the quarantine topic (the core keys requests; six partitions in capacity runs).
        self.assertEqual(fake.Consumer.assigned, [(NS + ".quarantine", p, -2) for p in range(6)])
        self.assertEqual(fake.Consumer.config_seen["isolation.level"], "read_committed")
        self.assertFalse(fake.Consumer.config_seen["enable.auto.commit"])
        self.assertEqual(fake.Consumer.closed, [True])


class PublisherScheduleTests(unittest.TestCase):
    """publish(): BOOK frames keep their own schedule while market-data sends are slow."""

    def test_book_is_not_delayed_by_market_data_sends(self):
        produced, lock = [], threading.Lock()

        class Producer:
            def __init__(self, config):
                pass

            def produce(self, topic, partition, key, value, headers, on_delivery):
                with lock:
                    produced.append((time.monotonic(), value))

            def poll(self, timeout):
                return 0

            def flush(self, timeout):
                return 0

        class Raw:
            subscription_id, capture_id = "sub", "cap"

            def __init__(self, tag):
                self.tag = tag

            def SerializeToString(self):
                return self.tag

        def slow_md(binding, frame, ns):
            time.sleep(0.12)  # a slow (CPU-starved) market-data send
            return Raw(b"md")

        book_frames = []

        def book_raw(binding, frame, ns):
            book_frames.append(frame)
            return Raw(b"book")

        lanes = {(v, f): {"physical_native_symbol": "ETH-USDT"} for v in g12.VENUES for f in g12.MD_FEEDS}
        lanes[("OKX", "INDEX")] = {"physical_native_symbol": "ETH-USDT"}
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            (work / "core.json").write_text(json.dumps({
                "raw_topics": [NS + ".raw"], "core": {"allow_test_provenance": True,
                                                       "quarantine_stream": NS + ".quarantine",
                                                       "bindings": [book_binding(v) for v in g12.VENUES]}}))
            out = work / "publish.jsonl"
            fake = fake_kafka(Producer, [])
            with patch.dict(sys.modules, {"confluent_kafka": fake}), \
                    patch.object(g12, "_packet", return_value={"namespace": NS}), \
                    patch.object(g12, "_security", return_value={}), \
                    patch.object(g12, "md_bindings", return_value=lanes), \
                    patch.object(g12, "md_envelope", side_effect=slow_md), \
                    patch("scripts.p184_dr05_raw_publisher.raw_envelope", side_effect=book_raw):
                g12.publish(work, out, 3.0, 4.0)
            rows = g12.load_jsonl(out)
        pubs = [r for r in rows if r.get("k") == "pub" and r["venue"] == "BINANCE"]
        self.assertGreaterEqual(len(pubs), 11)
        gaps = [(b["t"] - a["t"]) / MS for a, b in zip(pubs, pubs[1:])]
        # Each tick sends >= 2 slow market-data events (>= 240 ms of sends at 4 Hz, seeds first): sharing the
        # tick, BOOK would slip by those sends; on its own schedule it stays on the 250 ms grid.
        self.assertLess(max(gaps), 300, gaps)
        self.assertLess(max(r["late_ms"] for r in pubs), 50)
        self.assertEqual([tuple(r["top"]) for r in pubs], [g12.book_top(r["seq"]) for r in pubs])
        self.assertTrue(any(r.get("k") == "pub_md" for r in rows))
        self.assertEqual(rows[-1]["k"], "end")
        self.assertFalse(rows[-1]["md_thread_alive"])
        self.assertEqual(book_frames[0]["payload"]["bids"], [[g12.book_top(1001)[0], "2"]])


# ---------------------------------------------------------------- BOOK sampler coverage (undersampled, holes)

def dedup(views, step_ms=50):
    """The sampler's logged rows: a row on a view change or every 500 ms, each with its previous poll ``pt``."""
    out, last = [], {}
    for v in views:
        key, seen = (v["venue"], v["seq"]), last.get(v["venue"])
        if seen is None or seen[0] != key or v["t"] - seen[1] >= 500 * MS:
            out.append(dict(v, pt=v["t"] - step_ms * MS))
            last[v["venue"]] = (key, v["t"])
    return out


def hide_emissions(views, emitted, after_ns, transit_ms, n=2):
    """Drop every sampler row showing ``n`` consecutive moving-top emissions (250 ms apart) after ``after_ns``:
    the core emitted and the view applied each, the sampler never saw them (one hidden 250 ms emission is still
    within regime interval + one tick: two make the observed source gap exceed the cadence)."""
    k = next(k for k in range(1, len(emitted) - n) if emitted[k][0] >= after_ns
             and all(emitted[i + 1][0] - emitted[i][0] < 300 * MS for i in range(k - 1, k + n)))
    lo, hi = emitted[k][0] + transit_ms * MS, emitted[k + n][0] + transit_ms * MS
    return [v for v in views if not lo <= v["t"] < hi], [seq for _, seq in emitted[k:k + n]]


class SamplerCoverageTests(unittest.TestCase):
    CLEARED = 100 * S

    def run_fault(self, hide=None, okx_views=lambda views: views):
        pubs, views = [], []
        for venue in g12.VENUES:
            p, v, emitted = book_run(venue, 0, 500 * S, moving=True, transit_ms=150)
            if venue == "OKX":
                if hide is not None:
                    v, _ = hide_emissions(v, emitted, hide, 150)
                v = okx_views(v)
            pubs += p
            views += v
            p, v = md_rows(venue, 0, 500 * S)
            pubs += p
            views += v
        monitor = [{"t": t * S, "ports": {"8210": t >= 62}, "projector_lag": 0, "under_replicated": 0}
                   for t in range(0, 500)]
        fault = {"k": "fault", "name": "stream_restart", "kind": "stream", "injected_ns": 60 * S,
                 "restart_ns": 61 * S}
        return g12.evaluate_fault(fault, pubs, views, monitor)

    def test_missed_samples_of_applied_emissions_are_undersampled_not_a_gap(self):
        pubs, views, emitted = book_run("OKX", 90 * S, 200 * S, moving=True, transit_ms=150)
        holed, hidden = hide_emissions(views, emitted, 110 * S, 150)
        self.assertFalse(set(hidden) & {v["seq"] for v in holed})
        state = caught(pubs, holed, "OKX", self.CLEARED)
        self.assertEqual(state["rejected_samples"].get("update_gap", 0), 0)
        self.assertEqual(state["rejected_samples"].get("update_transit", 0), 0)
        self.assertGreater(state["rejected_samples"]["undersampled"], 0)
        self.assertLess(state["caught_up_ns"] - self.CLEARED, S)  # the hole neither starts nor breaks the hold
        self.assertGreater(state["undersampled_in_hold"], 0)
        # Fault level: no per-apply BOOK evidence proves the apply, so NOT_MEASURED, never a PASS or a FAIL.
        result = self.run_fault(hide=70 * S)
        self.assertTrue(result["g12_caught_up_pass"])
        self.assertEqual((result["parts"]["sampler"], result["reason"]), ("NOT_MEASURED", "SAMPLER_UNDERSAMPLED"))
        self.assertFalse(result["pass"])
        whole = self.run_fault()
        self.assertEqual((whole["parts"]["sampler"], whole["pass"]), ("PASS", True))
        self.assertEqual(whole["venues"]["OKX"]["undersampled_in_hold"], 0)

    def test_a_real_skipped_apply_stays_a_gap_with_logged_polls(self):
        # Production sampler shape (rows only on change / 500 ms, ``pt`` = previous poll 50 ms before): the view
        # skipping two of every three emissions is a product update_gap, never excused as undersampled.
        pubs, views, _ = book_run("OKX", 90 * S, 200 * S, moving=True, keep=lambda i: i % 3 == 0)
        logged = dedup(views)
        state = g12.caught_up(g12.view_series(logged), g12.publish_index(pubs), "OKX", self.CLEARED,
                              frames=g12.book_frames(pubs), polls=g12.view_polls(logged))
        self.assertIsNone(state["caught_up_ns"])
        self.assertGreater(state["rejected_samples"]["update_gap"], 0)
        self.assertEqual(state["rejected_samples"].get("undersampled", 0), 0)
        # Rows without ``pt`` (a run logged before it): the logged-row hole is all there is, so a gap after a
        # > 100 ms silent stretch is undersampled; either way the venue is never cleanly caught up (no PASS).
        bare = [{k: v for k, v in row.items() if k != "pt"} for row in logged]
        state = caught(pubs, bare, "OKX", self.CLEARED)
        self.assertGreater(state["rejected_samples"]["undersampled"], 0)
        self.assertTrue(state["caught_up_ns"] is None or state["undersampled_in_hold"] > 0, state)

    def test_a_sampler_hole_over_the_rule_is_not_measured(self):
        hole = (70 * S, 105 * S)  # longer than the 30 s hold, inside the caught-up hold
        result = self.run_fault(okx_views=lambda v: [r for r in v if not hole[0] <= r["t"] < hole[1]])
        self.assertGreater(result["venues"]["OKX"]["sampler_max_gap_s"], g12.SAMPLER_MAX_GAP_S)
        self.assertLessEqual(result["venues"]["BINANCE"]["sampler_max_gap_s"], 1)
        self.assertEqual((result["parts"]["sampler"], result["reason"]), ("NOT_MEASURED", "SAMPLER_GAP"))
        self.assertFalse(result["pass"])
        # The sampler gone for the rest of the run: not caught up is undecidable, not a product failure.
        dead = self.run_fault(okx_views=lambda v: [r for r in v if r["t"] < 63 * S])
        self.assertEqual((dead["sampler"], dead["reason"]), ("NOT_MEASURED", "SAMPLER_GAP"))
        self.assertFalse(dead["pass"])
        rows = [(1 * S, 5, True), (2 * S, None, False), (8 * S, 6, True)]
        self.assertEqual(g12.sampler_max_gap_s({"OKX": rows}, "OKX", 0, 10 * S), 7.0)  # READ_ERROR is no view
        self.assertEqual(g12.sampler_max_gap_s({}, "OKX", 0, 2 * S), 2.0)

    def test_regressing_view_and_future_publication_are_rejected(self):
        pubs, views, _ = book_run("OKX", 90 * S, 200 * S)
        old = next(v["seq"] for v in views if v["t"] >= 117 * S)
        # After 120 s the view regresses to a 3 s older post-clear frame and stays there (repeated seq).
        regressed = [v if v["t"] < 120 * S else dict(v, seq=old) for v in views]
        state = caught(pubs, regressed, "OKX", self.CLEARED)
        self.assertIsNone(state["caught_up_ns"])
        self.assertGreater(state["rejected_samples"]["open_gap"], 0)
        # A sample showing a frame before its publish time is an impossible apply, never caught-up evidence.
        pubs, views = stream_rows("OKX", 90 * S, 200 * S, transit_ms=-50)
        state = g12.caught_up(g12.view_series(views), g12.publish_index(pubs), "OKX", self.CLEARED)
        self.assertIsNone(state["caught_up_ns"])
        self.assertGreater(state["rejected_samples"]["future_publish"], 0)
