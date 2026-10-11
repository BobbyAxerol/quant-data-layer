"""Offline contracts of the G12 isolated fault harness; no Docker daemon or broker."""
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
        self.assertEqual(state["caught_up_ns"], 130 * S + 100 * MS)

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
                                                                      "MARK": "PASS"}, "g3": "PASS"})
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


if __name__ == "__main__":
    unittest.main()
