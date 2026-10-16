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


NATIVE_NAMES = ("gaps", "continuity_errors", "snapshot_replaced", "sequence_resets", "cursor_expired_errors",
                "slow_consumer_errors", "duplicates_applied", "offset_regressions", "reconnects", "replayed_events",
                "applied", "refreshed", "duplicates_rejected")


def hb_rows(venue, start, end, *, step_s=2, counters=None, workers=(4, 4), liveness="LIVE", key="hb-key-0001"):
    """Sampler heartbeat rows (k == hb) of one clone every step_s: pooled typed TRADE counters, all workers LIVE.
    ``counters(t_ns) -> dict`` overrides the default (every failure counter 0, applied growing)."""
    rows, t = [], start
    while t <= end:
        values = counters(t) if counters else {**{n: 0 for n in NATIVE_NAMES}, "applied": t // S}
        rows.append({"k": "hb", "t": t, "venue": venue, "heartbeat": True, "status": "READY",
                     "key_tail": key, "last_seen_unix": t / S - 0.5, "trade_integrity": values,
                     "trade_integrity_scope": {"counter_scope": "current_live_worker_epochs",
                                               "workers_reporting": workers[0], "workers_expected": workers[1]},
                     "worker_liveness": {str(i): liveness for i in range(workers[1])}})
        t += step_s * S
    return rows


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
            views += hb_rows(venue, 0, 500 * S)
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
                                           "integrity_report_only": "PASS", "monitor": "PASS",
                                           "sampler": "PASS"})
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
                      "--image", "core=" + FINAL["core"], "--query-code", "image", "--auto-leader-rebalance", "true"])
        images, query_code = orchestrate.call_args.args[3:5]
        self.assertEqual(images["core"], FINAL["core"])
        self.assertEqual(images["python"], g12.IMAGES["python"])
        self.assertEqual(query_code, "image")
        with patch.object(g12, "orchestrate", return_value=0) as orchestrate:
            g12.main(["--run", "/home/bobby/x", "--export", "/home/bobby/e.json", "--namespace", NS,
                      "--auto-leader-rebalance", "false"])
        self.assertEqual(orchestrate.call_args.args[3:5], (g12.IMAGES, "worktree"))
        self.assertIs(orchestrate.call_args.kwargs["auto_leader_rebalance"], False)
        # The flag is explicit: --run without it is refused, never a silent default.
        with patch.object(g12, "orchestrate", return_value=0) as orchestrate, \
                patch("sys.stderr"), self.assertRaises(SystemExit):
            g12.main(["--run", "/home/bobby/x", "--export", "/home/bobby/e.json", "--namespace", NS])
        orchestrate.assert_not_called()
        with self.assertRaises(ValueError):
            g12.orchestrate("/home/bobby/x", "/x", NS)


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
            self.assertEqual(g12.orchestrate(base, "/x", NS, auto_leader_rebalance=True), 1)
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

    def test_lost_trade_fails_unseen_quote_does_not_pending_blocks(self):
        pubs, evs = self.rows()
        lost = next(e for e in evs if e["feed"] == "TRADE" and e["venue"] == "OKX" and e["t"] > 200 * S)
        conflated = next(e for e in evs if e["feed"] == "QUOTE" and e["venue"] == "OKX" and e["t"] > 200 * S)
        evs = [e for e in evs if e is not lost and e is not conflated]
        result = self.window(pubs, evs)
        self.assertEqual((result["g2"]["TRADE"]["OKX"]["lost"], result["g2"]["TRADE"]["OKX"]["status"]), (1, "FAIL"))
        self.assertEqual(result["g2"]["QUOTE"]["OKX"]["unseen"], 1)
        self.assertEqual(result["g2"]["QUOTE"]["OKX"]["status"], "PASS")
        # The sampler has not yet covered reference + 10 s: undecidable, not lost, and never a PASS (OF-G12-2).
        early = g12.md_window(pubs, [e for e in evs if e["t"] < 205 * S], 100 * S, 400 * S)
        self.assertEqual(early["g2"]["TRADE"]["OKX"]["lost"], 0)
        self.assertGreater(early["g2"]["TRADE"]["OKX"]["pending"], 0)
        self.assertEqual(early["g2"]["TRADE"]["OKX"]["status"], "NOT_MEASURED")
        self.assertEqual(early["g2_status"], "NOT_MEASURED")

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
                views += hb_rows(venue, 0, 500 * S)
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


class LogFollowerTests(unittest.TestCase):
    """IDR-107: the projector exited 2 during the broker fault and --rm took its stderr with it."""

    def test_follow_argv_is_bounded_and_product_role_scoped(self):
        argv = g12.follow_argv(NS, "projector", 600)
        self.assertEqual(argv, ["timeout", "600", "docker", "logs", "--follow", "--timestamps", NS + "-projector"])
        self.assertIn("kafka-2", g12.FOLLOWED_ROLES)
        for bad in ((NS, "monitor", 10), (NS, "projector", 0), (NS, "projector", g12.LIMITS["deadline_s"] + 901),
                    ("qdl_v2_stable_candidate", "projector", 10)):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                g12.follow_argv(*bad)

    def test_an_exited_role_keeps_its_output_per_life_without_a_restart_policy(self):
        def exits(ns, role, seconds):
            return [sys.executable, "-c", f"print('{role} life'); import sys; sys.exit(2)"]
        with tempfile.TemporaryDirectory() as base:
            run = g12.Run(base, "/home/bobby/e.json", NS)
            run.evidence.mkdir(parents=True)
            with patch.object(g12, "follow_argv", side_effect=exits):
                self.assertEqual(run.follow_logs("projector"), 1)
                self.assertEqual(run.follow_logs("projector"), 2)  # restarted by the fault: a new file
                self.assertIsNone(run.follow_logs("monitor"))
            self.assertEqual(run.stop_followers(), {"followers": 2, "forced": []})
            logs = run.evidence / "logs"
            for life in (1, 2):
                self.assertEqual((logs / f"projector.life{life}.log").read_text(), "projector life\n")
            self.assertEqual(run.followers, [])
            self.assertNotIn("--restart", " ".join(g12._base(NS, "projector", network="none", memory="1g",
                                                               cpus="1")))

    def test_the_tail_is_bounded_and_a_stuck_follower_is_ended(self):
        with tempfile.TemporaryDirectory() as base:
            run = g12.Run(base, "/home/bobby/e.json", NS)
            run.evidence.mkdir(parents=True)
            with patch.object(g12, "FOLLOW_TAIL_BYTES", 8), \
                    patch.object(g12, "follow_argv", return_value=[sys.executable, "-c", "print('x' * 100 + 'END')"]):
                run.follow_logs("core")
            with patch.object(g12, "follow_argv", return_value=[sys.executable, "-c", "import time; time.sleep(60)"]):
                run.follow_logs("query")
            started = time.monotonic()
            self.assertEqual(run.stop_followers(wait_s=1.0), {"followers": 2, "forced": ["query"]})
            self.assertLess(time.monotonic() - started, 10)
            self.assertEqual((run.evidence / "logs/core.life1.log").read_text(), "xxxxEND\n")

    def test_start_follows_the_started_role(self):
        with tempfile.TemporaryDirectory() as base:
            run = g12.Run(base, "/home/bobby/e.json", NS)
            run.topology = lambda topic_id="": {"stream": ["docker", "run"]}
            with patch.object(g12, "sh"), patch.object(g12.Run, "verify_image"), \
                    patch.object(g12.Run, "follow_logs") as follow:
                run.start("stream", "topic")
            follow.assert_called_once_with("stream")
            self.assertIn("stream", run.expected)


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
            views += hb_rows(venue, 0, 500 * S)
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
            views += hb_rows(venue, 0, 500 * S)
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


class CoreGroupReproductionTests(unittest.TestCase):
    """INCIDENT 2026-10-15: --core-group runs the production core-group shape (3 members, 6 raw partitions)."""

    def test_default_shape_is_the_single_core_g12(self):
        with tempfile.TemporaryDirectory() as work:
            self.assertEqual(g12.core_group(work), {"members": 1, "raw_partitions": 1})
        self.assertEqual(g12.topics(NS)[NS + ".raw"][0], 1)
        roles = g12.topology(NS, "/home/bobby/x/work", "T", uid="1:1")
        self.assertFalse({"core-2", "core-3"} & set(roles))
        self.assertEqual(roles["core"][-1], "/work/core.json")

    def test_three_members_share_one_group_with_own_config_and_client(self):
        roles = g12.topology(NS, "/home/bobby/x/work", "T", uid="1:1", core_members=3)
        for index, role in enumerate(g12.CORE_MEMBERS, start=1):
            with self.subTest(role=role):
                argv = roles[role]
                suffix = "" if index == 1 else f"-{index}"
                self.assertEqual(argv[argv.index("--name") + 1], f"{NS}-{role}")
                self.assertIn(f"QDL_KAFKA_GROUP_ID={NS}", argv)
                self.assertIn(f"QDL_KAFKA_CLIENT_ID={NS}{suffix}", argv)
                self.assertEqual(argv[-1], f"/work/core{suffix}.json")
                self.assertIn("--rm", argv)
        for role in g12.CORE_MEMBERS[1:]:
            self.assertIn(role, g12.TEARDOWN_ORDER)
            self.assertIn(role, g12.FOLLOWED_ROLES)
            self.assertEqual(g12.role_image_key(role), "core")

    def test_member_configs_differ_only_in_transactional_id(self):
        with tempfile.TemporaryDirectory() as work:
            base = {"transactional_id": NS, "raw_topics": [NS + ".raw"], "batch_size": 1024}
            (Path(work) / "core.json").write_text(json.dumps(base))
            g12.write_member_configs(work, 3)
            ids = {NS}
            for index in (2, 3):
                member = json.loads((Path(work) / f"core-{index}.json").read_text())
                self.assertEqual({k: v for k, v in member.items() if k != "transactional_id"},
                                 {k: v for k, v in base.items() if k != "transactional_id"})
                ids.add(member["transactional_id"])
            self.assertEqual(len(ids), 3)
            (Path(work) / g12.CORE_GROUP_FILE).write_text(json.dumps({"members": 3, "raw_partitions": 6}))
            self.assertEqual(g12.core_group(work)["raw_partitions"], 6)
            (Path(work) / g12.CORE_GROUP_FILE).write_text(json.dumps({"members": 2, "raw_partitions": 6}))
            with self.assertRaises(ValueError):
                g12.core_group(work)
        self.assertEqual(g12.topics(NS, 6)[NS + ".raw"][0], 6)

    # The sealed core roster of B2 (2026-10-15): 2 BOOK + per venue TRADE/QUOTE/BAR/MARK, OKX MARK and INDEX
    # components sharing one source_id - 10 keys; crc32 % 6 left raw/5 without a record for the whole run.
    SEALED = [{"source_id": f"{v}-book", "venue": v, "l2": {"depth_per_side": 100}, "provider_kind": f"{v}_book"}
              for v in ("binance", "okx")] + [
        {"source_id": f"{v}-{lane}", "venue": v, "provider_kind": f"{v}_{kind}"}
        for v in ("binance", "okx") for lane, kind in (("trade", "trade"), ("quote", "bbo"), ("bar-1m", "bar"))] + [
        {"source_id": "binance-mark_index_price", "venue": "binance", "provider_kind": "binance_usdm_mark_index"},
        {"source_id": "okx-mark_index_price", "venue": "okx", "provider_kind": "okx_mark_price"},
        {"source_id": "okx-mark_index_price", "venue": "okx", "provider_kind": "okx_index_price"}]

    def test_raw_partition_plan_covers_every_partition_and_is_deterministic(self):
        import zlib
        crc = {zlib.crc32(b["source_id"].encode()) % 6 for b in self.SEALED}
        plan = g12.raw_partition_plan(self.SEALED, 6)
        self.assertEqual(len(plan), 10)
        self.assertEqual(set(plan.values()), set(range(6)), crc)  # every raw partition gets traffic
        self.assertEqual(plan, g12.raw_partition_plan(list(reversed(self.SEALED)), 6))  # order-independent
        # the six highest-rate lanes (BOOK, TRADE, QUOTE x 2 venues) land on six different partitions
        fast = [plan[b["source_id"]] for b in self.SEALED if b["source_id"].endswith(("book", "trade", "quote"))]
        self.assertEqual(sorted(fast), list(range(6)))
        self.assertEqual(set(g12.raw_partition_plan(self.SEALED, 1).values()), {0})

    def test_kill_uses_sigkill_and_stop_uses_sigterm(self):
        row = {"Id": "abc", "Name": "/" + NS + "-core-2", "Config": {"Labels": {g12.LABEL: NS}},
               "HostConfig": {"AutoRemove": True}}
        for kill, expected in ((False, ["docker", "stop", "--time", "30", "abc"]),
                               (True, ["docker", "kill", "--signal", "KILL", "abc"])):
            calls = []

            def sh(argv, timeout=60, check=True):
                calls.append(argv)
                return json.dumps([row]) if argv[:2] == ["docker", "inspect"] else ""
            with self.subTest(kill=kill), patch.object(g12, "sh", sh):
                self.assertTrue(g12.stop_role(NS, "core-2", kill=kill))
                self.assertIn(expected, calls)

    def _evidence(self, root, *, final_retries, kill_lag):
        ev = Path(root)
        (ev / "logs").mkdir()
        (ev / "verdict-ns.json").write_text(json.dumps({"namespace": NS}))
        t0 = 1_800_000_000 * S
        windows = [("baseline", t0, t0 + 100 * S), ("after_roll_core", t0 + 100 * S, t0 + 200 * S),
                   ("after_kill", t0 + 205 * S, t0 + 300 * S)]
        faults = [{"k": "core_window", "name": n, "start_ns": a, "end_ns": b} for n, a, b in windows]
        faults.insert(2, {"k": "core_kill", "role": "core-2", "injected_ns": t0 + 200 * S,
                          "restart_ns": t0 + 200 * S + MS})
        (ev / "faults.jsonl").write_text("".join(json.dumps(f) + "\n" for f in faults))

        def stamp(t):
            from datetime import datetime, timezone
            return datetime.fromtimestamp(t // S, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + ".000000001Z"
        lines = [f"{stamp(t0 + 150 * S)} " + json.dumps({"event": "qdl_realtime_core_rebalance_rewind"}),
                 f"{stamp(t0 + 150 * S)} " + json.dumps({"event": "qdl_realtime_core_progress"})]
        lines += [f"{stamp(t0 + (210 + 10 * n) * S)} " + json.dumps(
            {"event": "qdl_realtime_core_retry", "error": "consumer rebalanced"}) for n in range(final_retries)]
        (ev / "logs" / "core-2.life1.log").write_text("\n".join(lines) + "\n")
        rows = []
        for t in range(0, 300):
            members, assigned, lag = 3, [2, 2, 2], 5
            if 200 <= t < 212:
                members, assigned, lag = 4, [0, 2, 2, 2], 400
            elif t >= 212:
                lag = kill_lag
            rows.append({"t": t0 + t * S, "groups": {NS: {"members": members, "assigned": assigned}},
                         "lag": {NS: lag}})
        (ev / "monitor.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        return ev

    def test_verdict_counts_windows_storm_and_kill_catch_up(self):
        with tempfile.TemporaryDirectory() as root:
            verdict = g12.core_group_verdict(self._evidence(root, final_retries=0, kill_lag=3))
            self.assertFalse(verdict["storm"])
            self.assertEqual(verdict["windows"]["after_roll_core"]["rewinds"], 1)
            self.assertEqual(verdict["windows"]["after_roll_core"]["retries"], 0)
            self.assertEqual(verdict["windows"]["baseline"]["max_lag"], 5)
            # The dead member (empty, until its session expires) counts as short.
            self.assertEqual(verdict["windows"]["after_kill"]["short_member_samples"], 7)
            # RB2: the count-only monitor has no member ids, so recovery is not a clock; the old reading stays
            # a diagnostic that closes nothing.
            self.assertEqual(verdict["core_kill_first_healthy_sample_s"], 12.0)
            self.assertIsNone(verdict["core_kill_caught_up_s"])
            self.assertEqual(verdict["core_kill"]["reason"], "NO_EXIT_EVENT")
            self.assertEqual(verdict["totals"]["qdl_realtime_core_rebalance_rewind"], 1)
        with tempfile.TemporaryDirectory() as root:
            verdict = g12.core_group_verdict(self._evidence(root, final_retries=3, kill_lag=50))
            self.assertTrue(verdict["storm"])
            self.assertEqual(verdict["windows"]["after_kill"]["retries"], 3)
            self.assertIsNone(verdict["core_kill_caught_up_s"])


PREREG = {
    "schema": "p184-coregroup-prereg.v1",
    "criteria": {
        "run_A_control": "storm reproduced: storm=true OR retries >= 2 per member after the last roll OR group < 3 "
                         "members at a window end. If not reproduced: harness NOT valid evidence; run B cannot "
                         "certify the fix.",
        "run_B_fix": ["storm=false", "retries == 0 and rewind_failed == 0 in every window",
                      "rewinds <= 2 per roll window and 0 in baseline",
                      "last 30 s of every window: 3 members, each >= 1 partition",
                      "core_kill_caught_up_s <= 60 and dead member's partitions moved <= 15 s",
                      "no inflight_commit_failed / stop_drain_failed", "0 production guard breaches"]}}


OLD_CORE, NEW_CORE = "sha256:" + "a" * 64, "sha256:" + "b" * 64
ADDENDUM_2 = {
    "schema": "p184-coregroup-prereg-addendum.v1",
    "run_B2": {"criteria": [
        "storm=false",
        "new-image member lives: retries == 0, rewind_failed == 0, rewinds <= 2 per window, 0 in baseline",
        "old-image (RUST18) member lives: <= 1 retry per member, only in the roll window in which it is a survivor "
        "of a roll; 0 in baseline",
        "once all 3 members run the new image (after_roll_core-3 and after_kill): retries == 0 absolute",
        "last 30 s of every window: 3 members, all 6 partitions owned exactly once",
        "core kill (RB2 clocks): ownership restored <= 15 s after exit confirmed; caught up <= 60 s",
        "no inflight_commit_failed / stop_drain_failed",
        "0 production guard breaches",
        "coverage: monitor gap <= 5 s, member logs cover every window (else NOT_MEASURED, never PASS)"]}}
B2 = ADDENDUM_2["run_B2"]["criteria"]


class CoreGroupAcceptanceTests(unittest.TestCase):
    """RB2/RB3 (review 2026-10-15): recovery clocks tied to the dead member and a versioned acceptance layer."""

    T0 = 1_800_000_000 * S
    # (window, start s, end s); rolls injected at 120/272/424, kill at 576 (core-2), teardown at 730.
    WINDOWS = [("baseline", 0, 120), ("after_roll_core", 122, 272), ("after_roll_core-2", 274, 424),
               ("after_roll_core-3", 426, 576), ("after_kill", 578, 728)]
    OWNERS = {"core": (0, 1), "core-2": (2, 3), "core-3": (4, 5)}

    @staticmethod
    def stamp(t):
        from datetime import datetime, timezone
        return datetime.fromtimestamp(t // S, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{t % S:09d}Z"

    def build(self, root, *, drop_life=None, monitor_from=0, events=(), drop_window=None, detail=True,
              restored_at=586, truncate=None, images=False, unknown_image=None):
        """A complete healthy 5-window run; keyword arguments break one thing at a time. ``images``: record each
        life's image (life 1 OLD, later lives NEW = the roll target) in provenance and die events."""
        T, ev = self.T0, Path(root) / "evidence"
        (ev / "logs").mkdir(parents=True)
        (ev / "verdict-ns.json").write_text(json.dumps({"namespace": NS}))
        faults = []
        for name, a, b in self.WINDOWS:
            if name != drop_window:
                faults.append({"k": "core_window", "name": name, "start_ns": T + a * S, "end_ns": T + b * S})
            if name.startswith("after_roll_"):
                role = name[len("after_roll_"):]
                faults.insert(-1, {"k": "core_roll", "role": role, "injected_ns": T + (a - 2) * S,
                                   "restart_ns": T + (a - 1) * S, **({"image": NEW_CORE} if images else {})})
        faults.insert(-1, {"k": "core_kill", "role": "core-2", "injected_ns": T + 576 * S, "restart_ns": T + 577 * S})
        (ev / "faults.jsonl").write_text("".join(json.dumps(f) + "\n" for f in faults))
        # Lives: (role, life, first s, die s); the kill's die is 0.2 s after injection.
        lives = [("core", 1, 0.5, 120.5), ("core", 2, 121.5, 730), ("core-2", 1, 0.5, 272.5),
                 ("core-2", 2, 273.5, 576.2), ("core-2", 3, 577.5, 731), ("core-3", 1, 0.5, 424.5),
                 ("core-3", 2, 425.5, 732)]
        die, provenance = [], []
        for role, life, first, end in lives:
            image = None if (role, life) == unknown_image else OLD_CORE if life == 1 else NEW_CORE
            attributes = {"name": f"{NS}-{role}", "exitCode": "137" if (role, life) == ("core-2", 2) else "0"}
            if images and image:
                attributes["image"] = image
                provenance.append({"t": T + int((first - 0.4) * S), "role": role, "image_role": "core",
                                   "expected": image, "actual": image, "match": True})
            die.append({"Action": "die", "timeNano": T + int(end * S), "Actor": {"Attributes": attributes}})
            if (role, life) == drop_life:
                continue
            lines = [f"{self.stamp(T + int(first * S))} " + json.dumps(
                {"event": "qdl_realtime_core_started", "generation": 1})]
            lines += [f"{self.stamp(T + int(t * S))} " + json.dumps({"event": "qdl_realtime_core_progress"})
                      for t in range(int(first) + 1, int(end))]
            lines += [f"{self.stamp(T + int(t * S))} " + json.dumps(dict({"event": e}, **extra))
                      for r, t, e, extra in events if r == role and first <= t < end]
            text = "\n".join(lines) + "\n"
            if (role, life) == truncate:
                text = text[len(text) // 2:]  # the follower's tail: starts mid-line
            (ev / "logs" / f"{role}.life{life}.log").write_text(text)
        (ev / "container-events.jsonl").write_text("".join(json.dumps(d) + "\n" for d in die))
        if images:
            (ev / "provenance.jsonl").write_text("".join(json.dumps(r) + "\n" for r in provenance))
        rows = []
        for t in range(monitor_from, 730):
            lives_at = {"core": 1 if t < 121 else 2, "core-2": 1 if t < 273 else 2 if t < restored_at else 3,
                        "core-3": 1 if t < 425 else 2}
            owners = dict(self.OWNERS)
            if t >= restored_at:  # new owners after the dead member left
                owners = {"core": (0, 1, 2), "core-2": (5,), "core-3": (3, 4)}
            members = [{"member_id": f"m-{role}-{lives_at[role]}", "client_id": g12.core_client_id(NS, role),
                        "host": "/10.0.0.1", "partitions": [f"{NS}.raw/{p}" for p in owners[role]]}
                       for role in g12.CORE_MEMBERS]
            frozen = 576 <= t < 589  # the dead member's partitions stand still until the new owner commits
            offsets = {f"{NS}.raw/{p}": [575 if frozen and p in (2, 3) else t - 1, t] for p in range(6)}
            lag = sum(e - c for c, e in offsets.values())
            if t == 60:
                lag = 20  # baseline maximum: the stale samples right after the kill are "within baseline"
            group = {"state": "STABLE", "members": 3, "assigned": sorted(len(m["partitions"]) for m in members)}
            if detail:
                group["detail"] = members
            row = {"t": T + t * S, "groups": {NS: group}, "lag": {NS: lag}}
            if detail:
                row["offsets"] = {NS: offsets}
            rows.append(row)
        (ev / "monitor.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        (ev / "orchestrator.jsonl").write_text(json.dumps({"step": "teardown", "production_guard_breaches": {}})
                                               + "\n")
        (ev / "core-group-verdict.json").write_text(json.dumps({"status": "COMPLETED", "error": None}))
        return Path(root)

    def accept(self, base):
        result = g12.core_group_accept(base, self.prereg)
        return result, {row["condition"]: row["result"] for row in result["acceptance"]["run_B_fix"]["conditions"]}

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.prereg = Path(self.tmp.name) / "prereg.json"
        self.prereg.write_text(json.dumps(PREREG))
        self.addendum = Path(self.tmp.name) / "addendum.json"
        self.addendum.write_text(json.dumps(ADDENDUM_2))

    def root(self, name):
        path = Path(self.tmp.name) / name
        path.mkdir()
        return path

    def test_healthy_run_passes_every_frozen_condition(self):
        base = self.build(self.root("ok"), events=[("core-3", 150, "qdl_realtime_core_rebalance_rewind", {})])
        result, rows = self.accept(base)
        self.assertEqual(result["acceptance"]["run_B_fix"]["result"], "PASS", rows)
        self.assertEqual(len(rows), 4 + len(PREREG["criteria"]["run_B_fix"]))
        self.assertEqual(result["acceptance"]["run_A_control"]["result"], "FAIL")  # no storm to reproduce
        self.assertEqual(result["windows"]["after_roll_core"]["tail_coverage_basis"], ["partitions"])

    def test_stale_three_member_view_after_kill_does_not_close_recovery(self):
        verdict = g12.core_group_verdict(self.build(self.root("stale")) / "evidence")
        clocks = verdict["core_kill"]
        # 577-585: the old 3-member assignment (dead member still listed) with lag inside the baseline range.
        self.assertEqual(verdict["core_kill_first_healthy_sample_s"], 0.0)  # the old reading took the stale view
        self.assertEqual(clocks["exit_confirmed_s"], 0.2)
        self.assertEqual(clocks["dead_member_id"], "m-core-2-2")
        self.assertEqual(clocks["affected_partitions"], [f"{NS}.raw/2", f"{NS}.raw/3"])
        self.assertEqual(clocks["ownership_restored_s"], 10.0)  # only the ownership change closes it
        self.assertEqual(clocks["progress_s"], 13.0)  # and progress past the post-fault offsets
        self.assertEqual(clocks["caught_up_s"], 13.0)
        self.assertEqual(verdict["core_kill_caught_up_s"], 13.0)
        # The stale view never changes: recovery is not restored and, with full monitor coverage, that is a FAIL.
        result, rows = self.accept(self.build(self.root("never"), restored_at=10_000))
        self.assertEqual(result["verdict"]["core_kill"]["reason"], "OWNERSHIP_NOT_RESTORED")
        self.assertEqual(rows["core_kill_caught_up_s <= 60 and dead member's partitions moved <= 15 s"], "FAIL")
        # Moved, but after the 15 s deadline.
        _, rows = self.accept(self.build(self.root("late"), restored_at=600))
        self.assertEqual(rows["core_kill_caught_up_s <= 60 and dead member's partitions moved <= 15 s"], "FAIL")

    def test_absent_transfer_measurement_is_not_measured(self):
        result, rows = self.accept(self.build(self.root("counts"), detail=False))
        self.assertEqual(result["verdict"]["core_kill"]["reason"], "NO_MEMBER_DETAIL")
        self.assertEqual(rows["core_kill_caught_up_s <= 60 and dead member's partitions moved <= 15 s"],
                         "NOT_MEASURED")
        self.assertEqual(result["acceptance"]["run_B_fix"]["result"], "NOT_MEASURED")
        self.assertEqual(result["windows"]["baseline"]["tail_coverage_basis"], ["counts"])

    def test_missing_member_log_is_not_measured(self):
        result, rows = self.accept(self.build(self.root("nolog"), drop_life=("core-3", 2)))
        self.assertFalse(result["windows"]["after_roll_core-3"]["log_covered"]["core-3"])
        self.assertEqual(rows["harness: every member's retained log covers every window"], "NOT_MEASURED")
        self.assertEqual(rows["retries == 0 and rewind_failed == 0 in every window"], "NOT_MEASURED")
        self.assertEqual(rows["no inflight_commit_failed / stop_drain_failed"], "NOT_MEASURED")
        self.assertEqual(result["acceptance"]["run_B_fix"]["result"], "NOT_MEASURED")

    def test_truncated_life_covers_only_its_retained_tail(self):
        result, rows = self.accept(self.build(self.root("tail"), truncate=("core", 2)))
        covered = {name: w["log_covered"]["core"] for name, w in result["windows"].items()}
        self.assertFalse(covered["after_roll_core"])
        self.assertTrue(covered["after_kill"])
        self.assertEqual(result["acceptance"]["run_B_fix"]["result"], "NOT_MEASURED")

    def test_one_tail_sample_only_is_not_a_pass(self):
        base = self.build(self.root("tailonly"), monitor_from=728)
        verdict = g12.core_group_verdict(base / "evidence")
        self.assertFalse(verdict["storm"])  # the old diagnostic would call this healthy
        result, rows = self.accept(base)
        self.assertEqual(rows["harness: monitor coverage per window (gap <= 5 s)"], "NOT_MEASURED")
        self.assertEqual(rows["storm=false"], "NOT_MEASURED")
        self.assertEqual(rows["last 30 s of every window: 3 members, each >= 1 partition"], "NOT_MEASURED")
        self.assertEqual(result["acceptance"]["run_B_fix"]["result"], "NOT_MEASURED")

    def test_early_roll_failure_then_healthy_finish_fails(self):
        events = [("core", 130 + n, "qdl_realtime_core_retry", {"error": "rebalanced"}) for n in range(3)]
        base = self.build(self.root("early"), events=events)
        self.assertFalse(g12.core_group_verdict(base / "evidence")["storm"])
        result, rows = self.accept(base)
        self.assertEqual(rows["storm=false"], "PASS")
        self.assertEqual(rows["retries == 0 and rewind_failed == 0 in every window"], "FAIL")
        self.assertEqual(result["acceptance"]["run_B_fix"]["result"], "FAIL")

    def test_missing_roll_window_is_not_measured(self):
        result, rows = self.accept(self.build(self.root("nowin"), drop_window="after_roll_core-2"))
        self.assertEqual(rows["harness: all windows present"], "NOT_MEASURED")
        self.assertEqual(rows["rewinds <= 2 per roll window and 0 in baseline"], "NOT_MEASURED")
        self.assertEqual(result["acceptance"]["run_B_fix"]["result"], "NOT_MEASURED")

    def test_more_than_two_rewinds_in_a_roll_window_fails(self):
        events = [("core-3", 300 + n, "qdl_realtime_core_rebalance_rewind", {}) for n in range(3)]
        _, rows = self.accept(self.build(self.root("rewinds"), events=events))
        self.assertEqual(rows["rewinds <= 2 per roll window and 0 in baseline"], "FAIL")
        events = [("core", 30, "qdl_realtime_core_rebalance_rewind", {})]
        _, rows = self.accept(self.build(self.root("baseline"), events=events))
        self.assertEqual(rows["rewinds <= 2 per roll window and 0 in baseline"], "FAIL")
        events = [("core", 140, "qdl_realtime_core_rewind_failed", {}),
                  ("core-2", 140, "qdl_realtime_core_inflight_commit_failed", {})]
        _, rows = self.accept(self.build(self.root("failed"), events=events))
        self.assertEqual(rows["retries == 0 and rewind_failed == 0 in every window"], "FAIL")
        self.assertEqual(rows["no inflight_commit_failed / stop_drain_failed"], "FAIL")

    def test_control_run_reproduces_on_any_sub_condition_and_unknown_text_is_not_guessed(self):
        events = [(role, 600 + n, "qdl_realtime_core_retry", {"error": "rebalanced"})
                  for role in g12.CORE_MEMBERS for n in range(2)]
        result, _ = self.accept(self.build(self.root("storm"), events=events))
        control = result["acceptance"]["run_A_control"]
        self.assertEqual(control["conditions"][-1]["result"], "PASS")
        self.assertEqual(control["conditions"][-1]["detail"]["storm"], True)
        verdict = g12.core_group_verdict(self.build(self.root("plain")) / "evidence")
        verdict["status"] = "COMPLETED"
        out = g12.core_group_acceptance(verdict, {"criteria": {"run_B_fix": ["storm=false", "something new"]}})
        rows = {r["condition"]: (r["result"], r["detail"]) for r in out["run_B_fix"]["conditions"]}
        self.assertEqual(rows["something new"], ("NOT_MEASURED", {"reason": "UNKNOWN_CONDITION"}))
        self.assertEqual(out["run_B_fix"]["result"], "NOT_MEASURED")

    def test_cli_reevaluates_saved_evidence_without_writing(self):
        import contextlib
        import io
        base = self.build(self.root("cli"))
        before = {p: p.stat().st_mtime_ns for p in base.rglob("*")}
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = g12.main(["--core-group-accept", str(base), "--prereg", str(self.prereg)])
        self.assertEqual(code, 0)
        printed = json.loads(buffer.getvalue())
        self.assertEqual(printed["acceptance"]["schema"], g12.CORE_GROUP_ACCEPTANCE_SCHEMA_V1)
        self.assertEqual(printed["acceptance"]["run_B_fix"]["result"], "PASS")
        self.assertEqual(before, {p: p.stat().st_mtime_ns for p in base.rglob("*")})
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            g12.main(["--core-group-accept", str(base)])

    # -- addendum-2 (owner decision 2026-10-15): per-image acceptance --
    def accept_b2(self, base):
        result = g12.core_group_accept(base, self.prereg, self.addendum)
        b2 = result["acceptance"]["run_B2"]
        return result, b2, {row["condition"]: row["result"] for row in b2["conditions"]}

    def retry(self, role, t):
        return (role, t, "qdl_realtime_core_retry", {"error": "consumer rebalanced"})

    def test_b_like_old_image_survivor_retries_pass_only_per_image(self):
        # Run B's shape: the two old-image survivors retry once each in the first roll window.
        base = self.build(self.root("blike"), images=True, events=[self.retry("core-2", 150),
                                                                   self.retry("core-3", 160)])
        result, b2, rows = self.accept_b2(base)
        self.assertEqual(result["acceptance"]["schema"], "p184.coregroup.acceptance.v2")
        self.assertEqual(b2["result"], "PASS", rows)
        self.assertEqual(len(rows), 5 + len(B2))
        self.assertEqual(result["acceptance"]["run_B_fix"]["result"], "FAIL")  # v1 reading kept, unchanged
        lives = result["by_life"]["after_roll_core"]
        self.assertEqual({k: (v["image"], v["retries"]) for k, v in lives.items()},
                         {"core-2.life1": (OLD_CORE, 1), "core-3.life1": (OLD_CORE, 1)})
        self.assertEqual(result["core_images"]["lives"]["core-2"], [OLD_CORE, NEW_CORE, NEW_CORE])

    def test_old_image_member_with_two_retries_fails(self):
        _, b2, rows = self.accept_b2(self.build(self.root("old2"), images=True,
                                                events=[self.retry("core-3", 150), self.retry("core-3", 300)]))
        self.assertEqual(rows[B2[2]], "FAIL")
        self.assertEqual(b2["result"], "FAIL")
        self.assertEqual(rows[B2[1]], "PASS")

    def test_old_image_retry_outside_a_survivor_window_fails(self):
        # core-2's old life retries while it is itself being rolled (span opens at the roll injection, 272 s).
        _, _, rows = self.accept_b2(self.build(self.root("ownroll"), images=True, events=[self.retry("core-2", 272)]))
        self.assertEqual(rows[B2[2]], "FAIL")
        _, _, rows = self.accept_b2(self.build(self.root("base"), images=True, events=[self.retry("core", 30)]))
        self.assertEqual(rows[B2[2]], "FAIL")

    def test_new_image_retry_fails(self):
        _, b2, rows = self.accept_b2(self.build(self.root("new"), images=True, events=[self.retry("core", 200)]))
        self.assertEqual(rows[B2[1]], "FAIL")
        self.assertEqual(rows[B2[2]], "PASS")
        self.assertEqual(b2["result"], "FAIL")
        events = [("core", 200 + n, "qdl_realtime_core_rebalance_rewind", {}) for n in range(3)]
        _, _, rows = self.accept_b2(self.build(self.root("rew"), images=True, events=events))
        self.assertEqual(rows[B2[1]], "FAIL")
        events = [("core", 200, "qdl_realtime_core_rewind_failed", {})]
        _, _, rows = self.accept_b2(self.build(self.root("rwf"), images=True, events=events))
        self.assertEqual(rows[B2[1]], "FAIL")

    def test_retry_after_all_members_on_new_image_fails(self):
        _, b2, rows = self.accept_b2(self.build(self.root("allnew"), images=True,
                                                events=[self.retry("core-3", 600)]))
        self.assertEqual(rows[B2[3]], "FAIL")
        self.assertEqual(b2["result"], "FAIL")

    def test_unknown_life_image_or_counts_only_monitor_is_not_measured(self):
        base = self.build(self.root("unknown"), images=True, unknown_image=("core-2", 1),
                          events=[self.retry("core-2", 150)])
        _, b2, rows = self.accept_b2(base)
        self.assertEqual(rows[B2[2]], "NOT_MEASURED")
        self.assertEqual(b2["result"], "NOT_MEASURED")
        _, b2, rows = self.accept_b2(self.build(self.root("noimg")))  # no provenance at all
        self.assertEqual(rows[B2[1]], "NOT_MEASURED")
        _, b2, rows = self.accept_b2(self.build(self.root("cnt"), images=True, detail=False))
        self.assertEqual(rows[B2[4]], "NOT_MEASURED")
        self.assertEqual(b2["result"], "NOT_MEASURED")
        _, b2, rows = self.accept_b2(self.build(self.root("nolog2"), images=True, drop_life=("core-3", 2)))
        self.assertEqual(rows[B2[8]], "NOT_MEASURED")
        self.assertEqual(b2["result"], "NOT_MEASURED")

    # -- B2 apparatus fixes (2026-10-15): stable-group baseline and idle partitions in the kill clocks --
    def test_formation_retry_before_stability_is_not_counted_in_baseline(self):
        retry = [self.retry("core-2", 5)]
        # B2 as run: the baseline opened at member start, so core-2's formation retry is a baseline retry.
        verdict = g12.core_group_verdict(self.build(self.root("asrun"), events=retry) / "evidence")
        self.assertEqual(verdict["windows"]["baseline"]["retries"], 1)
        formation = verdict["baseline_formation"]
        self.assertFalse(formation["formation_recorded"])
        self.assertFalse(formation["baseline_opened_on_stable_group"])
        self.assertEqual([(e["role"], e["event"]) for e in formation["baseline_events_before_stable"]],
                         [("core-2", "qdl_realtime_core_retry")])
        # Fixed: a diagnostic formation window up to the stable hold, the baseline after it.
        base = self.build(self.root("fixed"), events=retry)
        faults = g12.load_jsonl(base / "evidence" / "faults.jsonl")
        faults[0]["start_ns"] = self.T0 + 20 * S
        faults.insert(0, {"k": "core_window", "name": "formation", "start_ns": self.T0, "end_ns": self.T0 + 20 * S,
                          "diagnostic": True})
        (base / "evidence" / "faults.jsonl").write_text("".join(json.dumps(f) + "\n" for f in faults))
        verdict = g12.core_group_verdict(base / "evidence")
        self.assertEqual(verdict["windows"]["formation"]["retries"], 1)
        self.assertEqual(verdict["windows"]["baseline"]["retries"], 0)
        self.assertTrue(verdict["baseline_formation"]["baseline_opened_on_stable_group"])
        self.assertEqual(verdict["baseline_formation"]["baseline_events_before_stable"], [])
        _, _, rows = self.accept_b2(base)
        self.assertNotIn("formation", g12.CORE_GROUP_WINDOWS)  # diagnostic, never judged
        self.assertEqual(rows["once all 3 members run the new image (after_roll_core-3 and after_kill): retries == 0 "
                              "absolute"], "PASS")

    def monitor_row(self, t, members=3, state="STABLE", owners=None):
        owners = owners or self.OWNERS
        detail = [{"member_id": f"m-{role}", "client_id": g12.core_client_id(NS, role), "host": "/10.0.0.1",
                   "partitions": [f"{NS}.raw/{p}" for p in owners[role]]} for role in g12.CORE_MEMBERS[:members]]
        return {"t": self.T0 + int(t * S), "groups": {NS: {"state": state, "members": members, "detail": detail,
                                                         "assigned": [len(m["partitions"]) for m in detail]}}}

    def test_group_stability_hold(self):
        stable_at = lambda rows: g12.core_group_stable_at(rows, NS, 6, 3, 10, self.T0)  # noqa: E731
        joining = [self.monitor_row(t, members=2, owners={"core": (0, 1, 2), "core-2": (3, 4, 5)}) for t in range(5)]
        rows = joining + [self.monitor_row(t) for t in range(5, 30)]
        self.assertEqual(stable_at(rows), self.T0 + 15 * S)
        rows[8] = self.monitor_row(8, state="PREPARING_REBALANCE")  # a rebalance restarts the hold
        self.assertEqual(stable_at(rows), self.T0 + 19 * S)
        del rows[10:17]  # a monitor gap > MONITOR_MAX_GAP_S restarts it too
        self.assertEqual(stable_at(rows), self.T0 + 27 * S)
        self.assertIsNone(stable_at(joining))

    def formation_run(self, rows, name):
        work, evidence = self.root(name + "-work"), self.root(name + "-ev")
        (work / g12.CORE_GROUP_FILE).write_text(json.dumps({"members": 3, "raw_partitions": 6}))
        (evidence / "monitor.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        faults = []

        def wait(seconds, faulted=(), until=None, step=5.0):
            return until() or None  # the bound elapsed after one look
        run = types.SimpleNamespace(work=work, evidence=evidence, ns=NS, wait=wait, fault_row=faults.append,
                                    note=lambda *a, **k: None)
        return run, faults

    def test_never_stable_group_makes_the_run_not_measured(self):
        run, faults = self.formation_run([self.monitor_row(t, members=2, owners={"core": (0, 1, 2),
                                                                                "core-2": (3, 4, 5)})
                                          for t in range(120)], "never")
        with self.assertRaises(g12.CoreGroupNotMeasured):
            g12.Run.core_group_formation(run, self.T0)
        self.assertEqual((faults[0]["name"], faults[0]["reason"], faults[0]["stable_at_ns"]),
                         ("formation", "GROUP_NEVER_STABLE", None))
        run, faults = self.formation_run([self.monitor_row(t) for t in range(30)], "stable")
        self.assertEqual(g12.Run.core_group_formation(run, self.T0), self.T0 + 10 * S)
        self.assertNotIn("reason", faults[0])
        # the orchestrator records NOT_MEASURED (not FAIL) and the harness row follows it
        verdict = {"status": "NOT_MEASURED", "error": "CoreGroupNotMeasured: GROUP_NEVER_STABLE", "windows": {}}
        harness = g12.core_group_acceptance(verdict, PREREG)["run_B_fix"]["conditions"][0]
        self.assertEqual((harness["condition"], harness["result"]), ("harness: run COMPLETED", "NOT_MEASURED"))

    def test_orchestrator_stops_not_measured_when_the_group_never_forms(self):
        tmp = Path(self.tmp.name)

        class FakeRun:
            def __init__(self, base, export, ns, images):
                self.images = images
                self.evidence = tmp / "orch"
                self.evidence.mkdir()

            def prepare(self):
                pass

            def core_group_up(self):
                pass

            def core_group_formation(self, start_ns):
                raise g12.CoreGroupNotMeasured("GROUP_NEVER_STABLE within 120 s")

            def core_group_window(self, *a, **k):
                raise AssertionError("no baseline after a never-stable formation")

            def note(self, *a, **k):
                pass

            def down(self):
                pass
        with patch.object(g12, "Run", FakeRun), patch.object(g12, "core_group_verdict", return_value={}):
            self.assertEqual(g12.orchestrate_core_group(tmp, tmp, NS, {}, NEW_CORE), 1)
        saved = json.loads((tmp / "orch" / "core-group-verdict.json").read_text())
        self.assertEqual(saved["status"], "NOT_MEASURED")
        self.assertIn("GROUP_NEVER_STABLE", saved["error"])

    def kill_clocks(self, pre, post_of):
        """core-2 (owner of raw/2, raw/3) killed at t=1 s, exit at 1.2 s; ownership moves at t=3 s.
        ``pre``: {p: [committed, end]} before the kill; ``post_of(t)``: offsets from t=3 s on."""
        base = {p: [100, 100] for p in range(6)}
        rows = [{**self.monitor_row(0), "offsets": {NS: {f"{NS}.raw/{p}": v for p, v in {**base, **pre}.items()}},
                 "lag": {NS: 0}}]
        moved = {"core": (0, 1, 2), "core-2": (5,), "core-3": (3, 4)}
        for t in range(3, 40):
            row = self.monitor_row(t, owners=moved)
            row["groups"][NS]["detail"][1]["member_id"] = "m-core-2-new"
            offsets = {**base, **post_of(t)}
            row["offsets"] = {NS: {f"{NS}.raw/{p}": v for p, v in offsets.items()}}
            row["lag"] = {NS: sum(max(0, e - max(c, 0)) for c, e in offsets.values())}
            rows.append(row)
        die = [{"Action": "die", "timeNano": self.T0 + 1200 * MS,
                "Actor": {"Attributes": {"name": f"{NS}-core-2", "exitCode": "137"}}}]
        return g12.core_kill_recovery({"role": "core-2", "injected_ns": self.T0 + S}, rows, die, NS, 6, 3, 5)

    def test_kill_clock_excludes_a_partition_that_never_had_a_record(self):
        # B2: raw/2 [-1001, 0] for the whole run; raw/3 has a backlog the new owner consumes by t=6 s.
        clocks = self.kill_clocks({2: [-1001, 0], 3: [100, 120]},
                                  lambda t: {2: [-1001, 0], 3: [100 if t < 6 else 130, 130]})
        self.assertEqual(clocks["idle_partitions"], [f"{NS}.raw/2"])
        self.assertEqual(clocks["idle_reasons"], {f"{NS}.raw/2": "NO_RECORDS"})
        self.assertEqual(clocks["post_fault_offsets"], {f"{NS}.raw/3": 130})
        self.assertEqual((clocks["ownership_restored_s"], clocks["progress_s"], clocks["caught_up_s"]),
                         (2.0, 5.0, 5.0))
        self.assertEqual(clocks["progress_basis"], "NON_IDLE_PAST_POST_FAULT_OFFSETS")
        self.assertIsNone(clocks["reason"])

    def test_kill_clock_idle_at_committed_log_end_and_new_input_is_not_idle(self):
        # raw/2 fully committed and no record after the kill: idle; raw/3 fully committed but new records arrive.
        clocks = self.kill_clocks({2: [50, 50], 3: [70, 70]},
                                  lambda t: {2: [50, 50], 3: [70 if t < 8 else 75, 75]})
        self.assertEqual(clocks["idle_reasons"], {f"{NS}.raw/2": "COMMITTED_AT_LOG_END"})
        self.assertEqual(clocks["post_fault_offsets"], {f"{NS}.raw/3": 75})
        self.assertEqual(clocks["progress_s"], 7.0)
        # a non-idle partition that never advances is still NO_PROGRESS
        clocks = self.kill_clocks({2: [50, 50], 3: [70, 70]}, lambda t: {2: [50, 50], 3: [70, 75]})
        self.assertEqual((clocks["reason"], clocks["progress_s"]), ("NO_PROGRESS", None))

    def test_kill_clock_all_affected_idle_progress_is_ownership(self):
        clocks = self.kill_clocks({2: [-1001, 0], 3: [50, 50]}, lambda t: {2: [-1001, 0], 3: [50, 50]})
        self.assertEqual(clocks["idle_partitions"], [f"{NS}.raw/2", f"{NS}.raw/3"])
        self.assertEqual(clocks["progress_basis"], "ALL_AFFECTED_IDLE")
        self.assertEqual(clocks["progress_s"], clocks["ownership_restored_s"])
        self.assertEqual(clocks["caught_up_s"], 2.0)
        self.assertIsNone(clocks["reason"])

    def test_cli_addendum_flag(self):
        import contextlib
        import io
        base = self.build(self.root("cli2"), images=True)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = g12.main(["--core-group-accept", str(base), "--prereg", str(self.prereg),
                             "--addendum", str(self.addendum)])
        self.assertEqual(code, 0)
        printed = json.loads(buffer.getvalue())
        self.assertEqual(printed["acceptance"]["schema"], g12.CORE_GROUP_ACCEPTANCE_SCHEMA)
        self.assertEqual(printed["acceptance"]["run_B2"]["result"], "PASS")

    def test_monitor_records_member_ownership_and_core_offsets(self):
        tp = lambda t, p: types.SimpleNamespace(topic=t, partition=p)  # noqa: E731
        member = types.SimpleNamespace(member_id="m-1", client_id=NS + "-2", host="/10.0.0.2",
                                       assignment=types.SimpleNamespace(topic_partitions=[tp(NS + ".raw", 3),
                                                                                          tp(NS + ".raw", 2)]))

        class Future:
            def result(self, timeout=None):
                return types.SimpleNamespace(state="ConsumerGroupState.STABLE", members=[member])

        class Admin:
            def describe_consumer_groups(self, groups, request_timeout=None):
                return {group: Future() for group in groups}

        class Source:
            def describe_cluster(self, deadline):
                return {}

            def describe_topics(self, roster, deadline):
                return {}

            def committed_offsets(self, group, roster, deadline):
                return {(t, p): 5 for t, n in roster.items() for p in range(n)}

            def log_end_offsets(self, partitions, deadline):
                return {tp: 7 for tp in partitions}
        rosters = {"kn-projector-v3-a": {"c": 1}, "kn-projector-v3-b": {"l": 1}, NS: {NS + ".raw": 6}}
        stub = types.ModuleType("scripts.kn_recovery_observe")
        stub.Deadline = lambda seconds: seconds
        with patch.dict(sys.modules, {"scripts.kn_recovery_observe": stub}):
            row = g12.kafka_sample(Source(), Admin(), {}, rosters, (NS,))
        self.assertEqual(row["groups"][NS]["detail"], [{"member_id": "m-1", "client_id": NS + "-2",
                                                        "host": "/10.0.0.2",
                                                        "partitions": [f"{NS}.raw/2", f"{NS}.raw/3"]}])
        self.assertEqual(row["offsets"][NS][f"{NS}.raw/4"], [5, 7])
        self.assertNotIn("kn-projector-v3-a", row["offsets"])
        self.assertEqual(g12.core_client_id(NS, "core"), NS)
        self.assertEqual(g12.core_client_id(NS, "core-3"), NS + "-3")


ADDENDUM_S = {
    "schema": "p184-coregroup-prereg-addendum.v1",
    "run_A_prime": {"criteria": [
        "reproduction: each old-image survivor of the first roll retries >= 2 times in after_roll_core"]},
    "run_S": {"criteria": [
        "storm=false",
        "replace-all: group re-formed (3 members, 6 partitions owned exactly once) <= 30 s after the first new "
        "member started, then stable >= 10 s",
        "replace-all: core lag within the baseline maximum <= 60 s after the first new member started",
        "new-image member lives: retries == 0, rewind_failed == 0, rewinds <= 2 per window, 0 in baseline",
        "old-image member lives: 0 retries, rewinds and rewind failures in every judged window",
        "every window after the replace (after_replace_all, after_restart_core-3, after_kill): retries == 0 "
        "absolute",
        "last 30 s of every window: 3 members, all 6 partitions owned exactly once",
        "core kill (RB2 clocks): ownership restored <= 15 s after exit confirmed; caught up <= 60 s",
        "no inflight_commit_failed / stop_drain_failed",
        "0 production guard breaches",
        "coverage: monitor gap <= 5 s, member logs cover every window (else NOT_MEASURED, never PASS)"]}}

# The frozen addendum-4 (prereg-addendum-4.json, sha256 06c561ed...) criteria, verbatim.
ADDENDUM_4 = {
    "schema": "p184-coregroup-prereg-addendum.v1",
    "run_A_prime": {"mode": "rolling (--core-group)", "criterion":
                    "control valid iff each old-image survivor of the first roll retries >= 2 times in "
                    "after_roll_core; else harness not production-like and S cannot be credited"},
    "run_S": {"mode": "--core-group-simultaneous", "criteria": [
        "storm=false",
        "group re-forms (3 members, 6 partitions owned exactly once) <= 30 s after the first new member starts and "
        "holds stable >= 10 s; lag back within the baseline maximum <= 60 s",
        "new-image lives: retries 0, rewind_failed 0, rewinds <= 2 per window",
        "old-image lives: 0 retries, rewinds, rewind failures in every judged window",
        "every window after the replace: retries 0 absolute",
        "last 30 s of every window: 3 members, 6 partitions owned exactly once",
        "core kill: ownership restored <= 15 s after exit confirmed, caught up <= 60 s",
        "no inflight_commit_failed / stop_drain_failed; 0 production guard breaches",
        "monitor/log coverage gaps -> NOT_MEASURED, never PASS"]}}


# The frozen addendum-5 (prereg-addendum-5.json, frozen 2026-10-15T12:17:23Z) run_S2 criteria, verbatim.
ADDENDUM_5 = {
    "schema": "p184-coregroup-prereg-addendum.v1",
    "run_S2": {"mode": "--core-group-simultaneous", "criteria": [
        ADDENDUM_4["run_S"]["criteria"][0], ADDENDUM_4["run_S"]["criteria"][1],
        "new-image lives: retries 0, rewind_failed 0, rewinds <= 2 per surviving member per window (a member "
        "restarted in that window is not a survivor)",
        *ADDENDUM_4["run_S"]["criteria"][3:]]}}

class CoreGroupProfileTests(unittest.TestCase):
    """INCIDENT 2026-10-15 #2: production-like core profile; defaults keep the B3 harness."""

    def test_defaults_keep_the_b3_harness(self):
        profile = g12.core_profile()
        self.assertEqual(profile, {**g12.CORE_PROFILE_DEFAULTS, "core_cpus": "0.5"})
        config = {"batch_size": 25, "batch_wait_ms": 10, "transactional_id": NS}
        self.assertEqual(g12.apply_core_profile(config, profile), config)
        roles = g12.topology(NS, "/home/bobby/x/work", "T", uid="1:1", core_members=3)
        for role in g12.CORE_MEMBERS:
            self.assertEqual(roles[role][roles[role].index("--cpus") + 1], "0.5")
        self.assertIn("group.initial.rebalance.delay.ms=0\n", g12.broker_properties("pw", 1))

    def test_production_values_reach_config_topology_and_brokers(self):
        profile = g12.core_profile(batch_size=1024, batch_wait_ms=25, core_cpus="1.0", hz=20.0,
                                   replace_gap_s=3.0, precondition_restarts=3, initial_rebalance_delay_ms=3000)
        config = g12.apply_core_profile({"batch_size": 25, "batch_wait_ms": 10, "x": 1}, profile)
        self.assertEqual(config, {"batch_size": 1024, "batch_wait_ms": 25, "x": 1})
        roles = g12.topology(NS, "/home/bobby/x/work", "T", uid="1:1", core_members=3,
                             core_cpus=profile["core_cpus"])
        for role in g12.CORE_MEMBERS:
            self.assertEqual(roles[role][roles[role].index("--cpus") + 1], "1.0")
        self.assertIn("group.initial.rebalance.delay.ms=3000\n", g12.broker_properties("pw", 2, 3000))

    def test_out_of_range_profiles_are_refused(self):
        for bad in ({"batch_size": 0}, {"batch_size": True}, {"hz": 25}, {"hz": 0}, {"core_cpus": "3"},
                    {"core_cpus": "x"}, {"replace_gap_s": 31}, {"precondition_restarts": 6}, {"nope": 1},
                    {"initial_rebalance_delay_ms": -1}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                g12.core_profile(**bad)

    def test_cli_profile_needs_a_core_group_mode_and_reaches_the_orchestrator(self):
        import contextlib
        import io
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            g12.main(["--analyze", "/x", "--core-batch-size", "1024"])
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            g12.main(["--core-group", "/x", "--export", "/e", "--namespace", NS, "--new-core", NEW_CORE,
                      "--core-hz", "50"])
        calls = []
        with patch.object(g12, "image_record", return_value={"id": NEW_CORE}), \
                patch.object(g12, "orchestrate_core_group", lambda *a, **k: calls.append((a, k)) or 0):
            for mode in ("--core-group", "--core-group-simultaneous"):
                g12.main([mode, "/x", "--export", "/e", "--namespace", NS, "--new-core", NEW_CORE,
                          "--core-batch-size", "1024", "--core-replace-gap-s", "3"])
            g12.main(["--core-group", "/x", "--export", "/e", "--namespace", NS, "--new-core", NEW_CORE])
        (args, kwargs), (_, kwargs_s), (_, kwargs_default) = calls
        self.assertEqual(str(args[0]), "/x")
        self.assertFalse(kwargs["simultaneous"])
        self.assertTrue(kwargs_s["simultaneous"])
        self.assertEqual((kwargs["profile"]["batch_size"], kwargs["profile"]["replace_gap_s"]), (1024, 3.0))
        self.assertEqual(kwargs_default["profile"], g12.core_profile())

    def fake_run(self, steps):
        """A Run stand-in recording each orchestration step."""
        class FakeRun:
            def __init__(self, base, export, ns, images):
                self.images = images
                self.evidence = Path(tempfile.mkdtemp(dir=self_tmp))

            def prepare(self):
                pass

            def core_group_up(self):
                steps.append(("up",))

            def core_group_formation(self, start_ns):
                steps.append(("formation",))

            def core_group_window(self, name, seconds, **fields):
                steps.append(("window", name, seconds, fields.get("diagnostic", False)))

            def core_replace(self, role, image=None, kill=False, gap_s=0.0, kind=None):
                steps.append(("replace", role, image, kill, gap_s, kind))

            def core_replace_all(self, image, gap_s=0.0):
                steps.append(("replace_all", image, gap_s))

            def note(self, *a, **k):
                pass

            def down(self):
                pass
        self_tmp = self.tmp
        return FakeRun

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name

    def orchestrate(self, **kwargs):
        steps = []
        with patch.object(g12, "Run", self.fake_run(steps)), patch.object(g12, "core_group_verdict", return_value={}):
            self.assertEqual(g12.orchestrate_core_group(Path(self.tmp), Path(self.tmp), NS, {}, NEW_CORE, **kwargs), 0)
        return steps

    def test_default_rolling_sequence_is_unchanged(self):
        hold = g12.CORE_GROUP["hold_s"]
        self.assertEqual(self.orchestrate(), [
            ("up",), ("formation",), ("window", "baseline", g12.CORE_GROUP["baseline_s"], False),
            ("replace", "core", NEW_CORE, False, 0.0, None), ("window", "after_roll_core", hold, False),
            ("replace", "core-2", NEW_CORE, False, 0.0, None), ("window", "after_roll_core-2", hold, False),
            ("replace", "core-3", NEW_CORE, False, 0.0, None), ("window", "after_roll_core-3", hold, False),
            ("replace", "core-2", None, True, 0.0, None), ("window", "after_kill", hold, False)])

    def test_production_like_rolling_preconditions_then_rolls_with_the_gap(self):
        steps = self.orchestrate(profile={"replace_gap_s": 3.0, "precondition_restarts": 2})
        self.assertEqual(steps[2:6], [
            ("replace", "core-2", None, False, 3.0, "core_precondition"),
            ("window", "precondition_1", g12.CORE_GROUP["precondition_hold_s"], True),
            ("replace", "core-2", None, False, 3.0, "core_precondition"),
            ("window", "precondition_2", g12.CORE_GROUP["precondition_hold_s"], True)])
        self.assertEqual(steps[6][1], "baseline")
        self.assertEqual([s[4] for s in steps if s[0] == "replace" and s[2] == NEW_CORE], [3.0, 3.0, 3.0])
        self.assertEqual(steps[-2], ("replace", "core-2", None, True, 0.0, None))  # the kill restarts at once

    def test_simultaneous_sequence(self):
        hold = g12.CORE_GROUP["hold_s"]
        self.assertEqual(self.orchestrate(simultaneous=True, profile={"replace_gap_s": 2.0}), [
            ("up",), ("formation",), ("window", "baseline", g12.CORE_GROUP["baseline_s"], False),
            ("replace_all", NEW_CORE, 2.0), ("window", "after_replace_all", hold, False),
            ("replace", "core-3", NEW_CORE, False, 2.0, None), ("window", "after_restart_core-3", hold, False),
            ("replace", "core-2", None, True, 0.0, None), ("window", "after_kill", hold, False)])
        self.assertEqual(g12.CORE_GROUP_S_WINDOWS[1:], ("after_replace_all", "after_restart_core-3", "after_kill"))

    def fake_self(self, rows):
        started, followed = [], []
        fake = types.SimpleNamespace(
            images={"core": OLD_CORE}, ns=NS, topic_id="T",
            topology=lambda topic_id: {role: ["run", role] for role in g12.CORE_MEMBERS},
            verify_image=lambda role: None, follow_logs=followed.append, fault_row=rows.append,
            note=lambda *a, **k: None)
        return fake, started, followed

    def test_replace_waits_the_gap_after_the_exit(self):
        rows, stops, sleeps, runs = [], [], [], []
        fake, _, followed = self.fake_self(rows)
        with patch.object(g12, "stop_role", lambda ns, role, kill=False: stops.append((role, kill)) or True), \
                patch.object(g12, "sh", lambda argv, timeout=60, check=True: runs.append(argv) or ""), \
                patch.object(g12.time, "sleep", sleeps.append):
            g12.Run.core_replace(fake, "core", image=NEW_CORE, gap_s=3.0)
            g12.Run.core_replace(fake, "core-2", kill=True)
            g12.Run.core_replace(fake, "core-2", gap_s=1.0, kind="core_precondition")
        self.assertEqual(sleeps, [3.0, 1.0])  # a kill restarts at once
        self.assertEqual([r["k"] for r in rows], ["core_roll", "core_kill", "core_precondition"])
        self.assertEqual([r["gap_s"] for r in rows], [3.0, 0.0, 1.0])
        self.assertTrue(all(r["injected_ns"] <= r["stopped_ns"] <= r["restart_ns"] for r in rows))
        self.assertEqual(stops, [("core", False), ("core-2", True), ("core-2", False)])

    def test_replace_all_stops_every_member_before_any_start(self):
        rows, order = [], []
        fake, _, followed = self.fake_self(rows)
        lock = threading.Lock()

        def stop(ns, role, kill=False):
            with lock:
                order.append(("stop", role, kill))
            return True
        with patch.object(g12, "stop_role", stop), \
                patch.object(g12, "sh", lambda argv, timeout=60, check=True: order.append(("start", argv[1])) or ""), \
                patch.object(g12.time, "sleep", lambda s: order.append(("sleep", s))):
            row = g12.Run.core_replace_all(fake, NEW_CORE, gap_s=2.0)
        self.assertEqual(sorted(order[:3]), [("stop", r, False) for r in sorted(g12.CORE_MEMBERS)])
        self.assertEqual(order[3:], [("sleep", 2.0)] + [("start", r) for r in g12.CORE_MEMBERS])
        self.assertEqual(fake.images["core"], NEW_CORE)
        self.assertEqual(followed, list(g12.CORE_MEMBERS))
        self.assertEqual((row["k"], row["image"], rows), ("core_replace_all", NEW_CORE, [row]))
        with patch.object(g12, "stop_role", lambda ns, role, kill=False: role != "core-3"), \
                self.assertRaises(RuntimeError):
            g12.Run.core_replace_all(fake, NEW_CORE)


class CoreGroupSimultaneousAcceptanceTests(unittest.TestCase):
    """``--core-group-simultaneous`` verdict and run_S / run_A_prime acceptance on synthetic evidence."""

    T0 = 1_800_000_000 * S
    OWNERS = {"core": (0, 1), "core-2": (2, 3), "core-3": (4, 5)}
    stamp = staticmethod(CoreGroupAcceptanceTests.stamp)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.prereg = Path(self.tmp.name) / "prereg.json"
        self.prereg.write_text(json.dumps(PREREG))
        self.addendum = Path(self.tmp.name) / "addendum.json"
        self.addendum.write_text(json.dumps(ADDENDUM_S))

    def build(self, name, *, reform_at=128, events=()):
        """Baseline 0-120 (old image); replace-all injected 120 (all gone 120.6, first start 121); restart of
        core-3 at 272; SIGKILL of core-2 at 424 (ownership moves at 432); end 580."""
        T, ev = self.T0, Path(self.tmp.name) / name / "evidence"
        (ev / "logs").mkdir(parents=True)
        (ev / "verdict-ns.json").write_text(json.dumps({"namespace": NS}))
        windows = [("baseline", 0, 120), ("after_replace_all", 122, 272), ("after_restart_core-3", 274, 424),
                   ("after_kill", 426, 576)]
        faults = [{"k": "core_window", "name": "baseline", "start_ns": T, "end_ns": T + 120 * S},
                  {"k": "core_replace_all", "roles": list(g12.CORE_MEMBERS), "image": NEW_CORE,
                   "injected_ns": T + 120 * S, "stopped_ns": T + 120 * S + 600 * MS, "gap_s": 0.0,
                   "restart_ns": T + 121 * S, "started_ns": T + 121 * S + 500 * MS}]
        faults.append({"k": "core_window", "name": "after_replace_all", "start_ns": T + 122 * S, "end_ns": T + 272 * S})
        faults.append({"k": "core_roll", "role": "core-3", "image": NEW_CORE, "injected_ns": T + 272 * S,
                       "restart_ns": T + 273 * S})
        faults.append({"k": "core_window", "name": "after_restart_core-3", "start_ns": T + 274 * S,
                       "end_ns": T + 424 * S})
        faults.append({"k": "core_kill", "role": "core-2", "injected_ns": T + 424 * S, "restart_ns": T + 425 * S})
        faults.append({"k": "core_window", "name": "after_kill", "start_ns": T + 426 * S, "end_ns": T + 576 * S})
        self.assertEqual([w[0] for w in windows], list(g12.CORE_GROUP_S_WINDOWS))
        (ev / "faults.jsonl").write_text("".join(json.dumps(f) + "\n" for f in faults))
        lives = [("core", 1, 0.5, 120.5), ("core", 2, 121.5, 580), ("core-2", 1, 0.5, 120.5),
                 ("core-2", 2, 121.5, 424.2), ("core-2", 3, 425.5, 581), ("core-3", 1, 0.5, 120.5),
                 ("core-3", 2, 121.5, 272.5), ("core-3", 3, 273.5, 582)]
        die, provenance = [], []
        for role, life, first, end in lives:
            image = OLD_CORE if life == 1 else NEW_CORE
            die.append({"Action": "die", "timeNano": T + int(end * S), "Actor": {"Attributes": {
                "name": f"{NS}-{role}", "image": image, "exitCode": "137" if (role, life) == ("core-2", 2) else "0"}}})
            provenance.append({"t": T + int((first - 0.4) * S), "role": role, "image_role": "core",
                               "expected": image, "actual": image, "match": True})
            lines = [f"{self.stamp(T + int(first * S))} " + json.dumps(
                {"event": "qdl_realtime_core_started", "generation": 1})]
            lines += [f"{self.stamp(T + int(t * S))} " + json.dumps({"event": "qdl_realtime_core_progress"})
                      for t in range(int(first) + 1, int(end))]
            lines += [f"{self.stamp(T + int(t * S))} " + json.dumps(dict({"event": e}, **extra))
                      for r, t, e, extra in events if r == role and first <= t < end]
            (ev / "logs" / f"{role}.life{life}.log").write_text("\n".join(lines) + "\n")
        (ev / "container-events.jsonl").write_text("".join(json.dumps(d) + "\n" for d in die))
        (ev / "provenance.jsonl").write_text("".join(json.dumps(r) + "\n" for r in provenance))
        rows = []
        for t in range(0, 580):
            gen = {"core": 1 if t < 121 else 2, "core-2": 1 if t < 121 else 2 if t < 432 else 3,
                   "core-3": 1 if t < 121 else 2 if t < 273 else 3}
            owners = dict(self.OWNERS) if t < 432 else {"core": (0, 1, 2), "core-2": (5,), "core-3": (3, 4)}
            members = [{"member_id": f"m-{role}-{gen[role]}", "client_id": g12.core_client_id(NS, role),
                        "host": "/10.0.0.1", "partitions": [f"{NS}.raw/{p}" for p in owners[role]]}
                       for role in g12.CORE_MEMBERS]
            if 121 <= t < reform_at:  # all members gone, then re-joining
                members, state = members[:1 if t >= 123 else 0], "PREPARING_REBALANCE"
            else:
                state = "STABLE"
            stalled = 121 <= t < max(130, reform_at + 2)
            frozen = 424 <= t < 437
            offsets = {f"{NS}.raw/{p}": [119 if stalled else 423 if frozen and p in (2, 3) else t - 1, t]
                       for p in range(6)}
            lag = 20 if t == 60 else sum(e - c for c, e in offsets.values())
            group = {"state": state, "members": len(members), "detail": members,
                     "assigned": sorted(len(m["partitions"]) for m in members)}
            rows.append({"t": T + t * S, "groups": {NS: group}, "lag": {NS: lag}, "offsets": {NS: offsets}})
        (ev / "monitor.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        (ev / "orchestrator.jsonl").write_text(json.dumps({"step": "teardown", "production_guard_breaches": {}})
                                               + "\n")
        (ev / "core-group-verdict.json").write_text(json.dumps({"status": "COMPLETED", "error": None}))
        return ev.parent

    def accept(self, base, key="run_S"):
        result = g12.core_group_accept(base, self.prereg, self.addendum)
        run = result["acceptance"][key]
        return result, run, {row["condition"]: row["result"] for row in run["conditions"]}

    def test_clean_simultaneous_replace_passes_run_s(self):
        base = self.build("ok", events=[("core-3", 280, "qdl_realtime_core_rebalance_rewind", {})])
        verdict = g12.core_group_verdict(base / "evidence")
        self.assertEqual(verdict["mode"], "simultaneous")
        self.assertEqual(verdict["core_images"]["target"], NEW_CORE)
        clocks = verdict["replace_all"]
        self.assertEqual((clocks["stopped_s"], clocks["reformed_s"], clocks["stable_s"], clocks["caught_up_s"]),
                         (0.6, 7.0, 17.0, 9.0))
        self.assertFalse(verdict["storm"])
        result, run, rows = self.accept(base)
        self.assertEqual(run["result"], "PASS", rows)
        self.assertEqual(rows["harness: run mode is simultaneous"], "PASS")
        self.assertEqual(result["verdict"]["core_kill"]["ownership_restored_s"], 8.0)
        # run_A_prime belongs to a rolling run: its mode row fails here
        _, _, rows = self.accept(base, "run_A_prime")
        self.assertEqual(rows["harness: run mode is rolling"], "FAIL")

    def test_all_new_retry_and_slow_reform_fail(self):
        base = self.build("retry", events=[("core", 300, "qdl_realtime_core_retry", {"error": "rebalanced"})])
        _, run, rows = self.accept(base)
        self.assertEqual(rows["every window after the replace (after_replace_all, after_restart_core-3, after_kill): "
                              "retries == 0 absolute"], "FAIL")
        self.assertEqual(rows["new-image member lives: retries == 0, rewind_failed == 0, rewinds <= 2 per window, "
                              "0 in baseline"], "FAIL")
        self.assertEqual(run["result"], "FAIL")
        _, run, rows = self.accept(self.build("slow", reform_at=160))
        self.assertEqual(rows["replace-all: group re-formed (3 members, 6 partitions owned exactly once) <= 30 s "
                              "after the first new member started, then stable >= 10 s"], "FAIL")
        self.assertEqual(run["result"], "FAIL")

    def test_old_image_retry_in_the_baseline_fails(self):
        base = self.build("old", events=[("core-2", 60, "qdl_realtime_core_retry", {"error": "rebalanced"})])
        _, _, rows = self.accept(base)
        self.assertEqual(rows["old-image member lives: 0 retries, rewinds and rewind failures in every judged "
                              "window"], "FAIL")

    def test_a_prime_reproduction_counts_first_roll_survivor_retries(self):
        helper = CoreGroupAcceptanceTests("test_healthy_run_passes_every_frozen_condition")
        root = Path(self.tmp.name)
        reproduction = "reproduction: each old-image survivor of the first roll retries >= 2 times in after_roll_core"
        for name, per, expected in (("rep", 2, "PASS"), ("norep", 1, "FAIL")):
            (root / name).mkdir()
            events = [(role, 130 + 10 * n, "qdl_realtime_core_retry", {"error": "rebalanced"})
                      for role in ("core-2", "core-3") for n in range(per)]
            base = helper.build(root / name, images=True, events=events)
            _, run, rows = self.accept(base, "run_A_prime")
            with self.subTest(name=name):
                self.assertEqual(rows[reproduction], expected)
                self.assertEqual(rows["harness: run mode is rolling"], "PASS")

    def accept4(self, base, key="run_S", addendum=None):
        path = Path(self.tmp.name) / "addendum-4.json"
        path.write_text(json.dumps(addendum or ADDENDUM_4))
        result = g12.core_group_accept(base, self.prereg, path)
        run = result["acceptance"][key]
        return result, run, {row["condition"]: row for row in run["conditions"]}

    def test_addendum_4_frozen_strings_map_to_rules(self):
        frozen = ADDENDUM_4["run_S"]["criteria"]
        self.assertEqual(set(frozen) - {"storm=false"}, set(g12.CORE_GROUP_CRITERIA_MAP)
                         - {ADDENDUM_4["run_A_prime"]["criterion"]})
        result, run, rows = self.accept4(self.build("ok4", events=[("core-3", 280, "qdl_realtime_core_rebalance_rewind",
                                                                       {})]))
        self.assertEqual(result["acceptance"]["criteria_map"], g12.CORE_GROUP_CRITERIA_MAP_VERSION)
        for text in frozen:
            with self.subTest(text=text):
                self.assertEqual(rows[text]["result"], "PASS", rows[text]["detail"])
        self.assertEqual(run["result"], "PASS")
        group = rows[frozen[1]]["detail"]["rules"]
        self.assertEqual(len(group), 2)
        # a violation still reaches the mapped rule: a retry after the replace fails three frozen strings
        _, run, rows = self.accept4(self.build("retry4", events=[("core", 300, "qdl_realtime_core_retry",
                                                                   {"error": "rebalanced"})]))
        for text in (frozen[2], frozen[4]):
            self.assertEqual(rows[text]["result"], "FAIL")
        self.assertEqual(run["result"], "FAIL")
        _, _, rows = self.accept4(self.build("slow4", reform_at=160))
        self.assertEqual(rows[frozen[1]]["result"], "FAIL")
        # old-image rewind outside the baseline (stop of the old group) fails the literal old-image rule
        _, _, rows = self.accept4(self.build("oldrw", events=[("core-2", 120, "qdl_realtime_core_rebalance_rewind",
                                                                {})]))
        self.assertEqual(rows[frozen[3]]["result"], "FAIL")

    def test_addendum_4_a_prime_singular_criterion_is_evaluated(self):
        helper = CoreGroupAcceptanceTests("test_healthy_run_passes_every_frozen_condition")
        text = ADDENDUM_4["run_A_prime"]["criterion"]
        root = Path(self.tmp.name)
        for name, per, expected in (("rep4", 2, "PASS"), ("norep4", 1, "FAIL")):
            (root / name).mkdir()
            events = [(role, 130 + 10 * n, "qdl_realtime_core_retry", {"error": "rebalanced"})
                      for role in ("core-2", "core-3") for n in range(per)]
            _, run, rows = self.accept4(helper.build(root / name, images=True, events=events), "run_A_prime")
            with self.subTest(name=name):
                self.assertEqual(rows[text]["result"], expected)
                self.assertEqual(rows["harness: run mode is rolling"]["result"], "PASS")
        # a frozen run block without a readable criterion fails closed
        _, run, rows = self.accept4(self.build("nocrit"), "run_S", {"run_S": {"mode": "simultaneous"}})
        self.assertEqual(rows["harness: frozen criteria present"]["result"], "NOT_MEASURED")
        self.assertEqual(run["result"], "NOT_MEASURED")

    def test_unknown_criterion_string_is_not_measured(self):
        addendum = {"run_S": {"criteria": ["storm=false", "group re-forms quickly"]}}
        _, run, rows = self.accept4(self.build("unknown4"), "run_S", addendum)
        self.assertEqual(rows["storm=false"]["result"], "PASS")
        self.assertEqual(rows["group re-forms quickly"]["result"], "NOT_MEASURED")
        self.assertEqual(rows["group re-forms quickly"]["detail"], {"reason": "UNKNOWN_CONDITION"})
        self.assertEqual(run["result"], "NOT_MEASURED")
        # the map is not applied to the original prereg's run_B_fix rules
        out = g12.core_group_acceptance({"windows": {}}, {"criteria": {"run_B_fix": [
            ADDENDUM_4["run_S"]["criteria"][1]]}})
        self.assertEqual(out["run_B_fix"]["conditions"][-1]["detail"], {"reason": "UNKNOWN_CONDITION"})

    def accept5(self, base, addendum=None):
        path = Path(self.tmp.name) / "addendum-5.json"
        path.write_text(json.dumps(addendum or ADDENDUM_5))
        result = g12.core_group_accept(base, self.prereg, path)
        run = result["acceptance"]["run_S2"]
        return result, run, {row["condition"]: row for row in run["conditions"]}

    NEW5 = ADDENDUM_5["run_S2"]["criteria"][2]

    @staticmethod
    def rewinds(*pairs):
        """Rebalance rewinds in after_restart_core-3 (span 272-424): (role, count) pairs."""
        return [(role, 280 + 10 * n, "qdl_realtime_core_rebalance_rewind", {}) for role, k in pairs
                for n in range(k)]

    def test_addendum_5_strings_map_verbatim_and_pass_clean_run(self):
        frozen = ADDENDUM_5["run_S2"]["criteria"]
        self.assertEqual(set(frozen) - {"storm=false"}, set(g12.CORE_GROUP_CRITERIA_MAP_ADDENDUM_5))
        result, run, rows = self.accept5(self.build("ok5", events=self.rewinds(("core", 1))))
        self.assertEqual(result["acceptance"]["criteria_map"], "p184.coregroup.criteria-map.addendum-5.v1")
        for text in frozen:
            with self.subTest(text=text):
                self.assertEqual(rows[text]["result"], "PASS", rows[text]["detail"])
        self.assertEqual(rows["harness: run mode is simultaneous"]["result"], "PASS")
        self.assertEqual(run["result"], "PASS")
        self.assertEqual(rows[self.NEW5]["detail"]["map"], g12.CORE_GROUP_CRITERIA_MAP_ADDENDUM_5_VERSION)
        restarted = {n: w["restarted_roles"] for n, w in result["windows"].items()}
        self.assertEqual(restarted, {"baseline": [], "after_replace_all": list(g12.CORE_MEMBERS),
                                     "after_restart_core-3": ["core-3"], "after_kill": ["core-2"]})

    def test_addendum_5_rewinds_are_counted_per_surviving_member(self):
        rule = ("new-image member lives: retries == 0, rewind_failed == 0, rewinds <= 2 per surviving member per "
                "window (members restarted / killed in the window excluded), 0 in baseline")
        # S-like: 3 rewinds after the restart of core-3, split 2 + 1 across the two survivors -> PASS
        _, run, rows = self.accept5(self.build("split", events=self.rewinds(("core-2", 2), ("core", 1))))
        self.assertEqual(rows[self.NEW5]["result"], "PASS", rows[self.NEW5]["detail"])
        window = rows[self.NEW5]["detail"]["rules"][rule]["detail"]["windows"]["after_restart_core-3"]
        self.assertEqual((window["survivors"], window["rewinds_per_survivor"]),
                         (["core", "core-2"], {"core": 1, "core-2": 2}))
        self.assertEqual(run["result"], "PASS")
        # ... and the same evidence still FAILs under addendum-4 (rewinds 3 > 2 per window), unchanged
        _, run4, rows4 = self.accept4(self.build("split4", events=self.rewinds(("core-2", 2), ("core", 1))))
        self.assertEqual(rows4[ADDENDUM_4["run_S"]["criteria"][2]]["result"], "FAIL")
        self.assertEqual(run4["result"], "FAIL")
        # 3 rewinds on one survivor -> FAIL
        _, run, rows = self.accept5(self.build("one", events=self.rewinds(("core-2", 3))))
        self.assertEqual(rows[self.NEW5]["result"], "FAIL")
        self.assertEqual(run["result"], "FAIL")
        # the restarted member's rewinds are excluded (reported), its retries are not
        _, run, rows = self.accept5(self.build("restarted", events=self.rewinds(("core-3", 3), ("core", 2))))
        self.assertEqual(rows[self.NEW5]["result"], "PASS", rows[self.NEW5]["detail"])
        window = rows[self.NEW5]["detail"]["rules"][rule]["detail"]["windows"]["after_restart_core-3"]
        self.assertEqual(window["rewinds_excluded_restarted"], {"core-3": 3})
        _, _, rows = self.accept5(self.build("restarted-retry", events=[
            ("core-3", 300, "qdl_realtime_core_retry", {"error": "rebalanced"})]))
        self.assertEqual(rows[self.NEW5]["result"], "FAIL")
        # the killed member (core-2) is not a survivor of after_kill; core-3 is
        _, _, rows = self.accept5(self.build("kill", events=[
            (role, 440 + n, "qdl_realtime_core_rebalance_rewind", {}) for role, k in (("core-2", 3), ("core-3", 3))
            for n in range(k)]))
        self.assertEqual(rows[self.NEW5]["result"], "FAIL")
        window = rows[self.NEW5]["detail"]["rules"][rule]["detail"]["windows"]["after_kill"]
        self.assertEqual((window["rewinds_per_survivor"], window["rewinds_excluded_restarted"]),
                         ({"core": 0, "core-3": 3}, {"core-2": 3}))

    def test_addendum_5_without_restart_attribution_is_not_measured(self):
        base = self.build("legacy5")
        verdict = g12.core_group_verdict(base / "evidence")
        verdict["status"] = "COMPLETED"
        for window in verdict["windows"].values():
            window.pop("restarted_roles")
        out = g12.core_group_acceptance(verdict, PREREG, ADDENDUM_5)
        rows = {row["condition"]: row["result"] for row in out["run_S2"]["conditions"]}
        self.assertEqual(rows[self.NEW5], "NOT_MEASURED")
        # an unknown string under run_S2 stays NOT_MEASURED; addendum-4's run_S map version is unchanged
        out = g12.core_group_acceptance(verdict, PREREG, {"run_S2": {"criteria": [
            "new-image lives: retries 0, rewind_failed 0, rewinds <= 2 per window"]}})
        self.assertEqual(out["run_S2"]["conditions"][-1]["detail"], {"reason": "UNKNOWN_CONDITION"})
        self.assertEqual(g12.CORE_GROUP_CRITERIA_MAP_VERSION, "p184.coregroup.criteria-map.addendum-4.v1")

    def test_replace_all_clocks_from_fixture(self):
        base = self.build("clocks")
        clocks = g12.core_group_verdict(base / "evidence")["replace_all"]
        # injected 120, all gone 120.6, first die event 120.5, first start 121, re-formed 128, stable 138
        self.assertEqual((clocks["stopped_s"], clocks["reformed_s"], clocks["stable_s"], clocks["caught_up_s"]),
                         (0.6, 7.0, 17.0, 9.0))
        self.assertEqual((clocks["pause_s"], clocks["injected_to_reformed_s"], clocks["first_stop_ns"]),
                         (7.5, 8.0, self.T0 + int(120.5 * S)))
        self.assertIsNone(clocks["reason"])
        result, _, _ = self.accept4(base)
        self.assertEqual(result["verdict"]["mode"], "simultaneous")
        self.assertEqual(result["verdict"]["replace_all"]["pause_s"], 7.5)
        never = g12.core_group_verdict(self.build("never", reform_at=600) / "evidence")["replace_all"]
        self.assertEqual((never["reason"], never["reformed_s"], never["pause_s"]), ("NEVER_REFORMED", None, None))


class IntegrityOracleTests(unittest.TestCase):
    """OF-G12-2 (inheritance audit 2026-10-16): rewinds, duplicates applied and native typed counters per fault;
    absent observations are NOT_MEASURED. The part is REPORT-ONLY (coordinator 2026-10-16): never gates G12.
    OF-G12-1: only a frozen predicate v2 run is credited."""
    FAULT = {"k": "fault", "name": "stream_restart", "kind": "stream", "injected_ns": 60 * S, "restart_ns": 61 * S}
    MONITOR = [{"t": t * S, "ports": {"8210": t >= 62}, "projector_lag": 0, "under_replicated": 0}
               for t in range(0, 500)]

    def inputs(self, *, hb=None, edit=None):
        pubs, views = [], []
        for venue in g12.VENUES:
            p, v = stream_rows(venue, 0, 500 * S)
            pubs += p
            views += v
            p, v = md_rows(venue, 0, 500 * S)
            pubs += p
            views += v
            views += (hb or {}).get(venue, hb_rows(venue, 0, 500 * S)) if hb is not None else hb_rows(venue, 0, 500 * S)
        if edit:
            views = edit(views)
        return pubs, views

    def fault(self, **kwargs):
        pubs, views = self.inputs(**kwargs)
        return g12.evaluate_fault(self.FAULT, pubs, views, self.MONITOR)

    def test_clean_run_passes_with_every_check_measured(self):
        result = self.fault()
        self.assertEqual(result["parts"]["integrity_report_only"], "PASS")
        okx = result["integrity"]["venues"]["OKX"]
        self.assertEqual((okx["book_rewinds"], okx["trade_duplicates_n"], okx["native"]["status"]), ([], 0, "PASS"))
        self.assertEqual(okx["rewinds"], {"TRADE": 0, "QUOTE": 0, "MARK": 0})
        self.assertTrue(okx["native"]["complete"])
        self.assertEqual(result["integrity"]["scope_ns"][0], self.FAULT["injected_ns"])
        self.assertEqual(result["integrity"]["scope_ns"][1], result["g1_window_ns"][1])
        self.assertTrue(result["pass"])

    def test_book_rewind_is_post_to_post_against_the_high_water(self):
        # 200 s: the OKX view goes back to an older sequence after newer ones (all inside the scope).
        def back(views):
            older = next(v for v in views if v.get("k") == "view" and v["venue"] == "OKX" and v["t"] >= 150 * S)
            return views + [dict(older, t=200 * S + 1)]
        result = self.fault(edit=back)
        self.assertEqual(result["integrity"]["venues"]["OKX"]["status"], "FAIL")
        self.assertIn("BOOK rewind", result["integrity"]["venues"]["OKX"]["reasons"])
        self.assertEqual(result["integrity"]["venues"]["BINANCE"]["status"], "PASS")
        self.assertTrue(result["pass"])  # report-only: G12 still PASSes
        # Before the injection: outside the scope (the high-water still includes it).
        def early(views):
            older = next(v for v in views if v.get("k") == "view" and v["venue"] == "OKX" and v["t"] >= 10 * S)
            return views + [dict(older, t=30 * S + 1)]
        self.assertEqual(self.fault(edit=early)["parts"]["integrity_report_only"], "PASS")
        rows = [{"k": "view", "t": 1, "venue": "OKX", "seq": 100}, {"k": "view", "t": 2, "venue": "OKX", "seq": 200},
                {"k": "view", "t": 3, "venue": "OKX", "seq": 150}, {"k": "view", "t": 4, "venue": "OKX", "seq": None}]
        self.assertEqual(g12.book_rewinds(rows)["OKX"], [{"t": 3, "before": 200, "after": 150}])  # PRE100->200->150

    def test_trade_duplicate_applied_fails_bar_reapply_is_report_only(self):
        def dup(feed, later_s=30):
            def edit(views):
                ev = next(v for v in views if v.get("k") == "ev" and v["venue"] == "OKX" and v["feed"] == feed
                          and v["t"] >= 100 * S)
                return views + [dict(ev, t=ev["t"] + later_s * S)]
            return edit
        trade = self.fault(edit=dup("TRADE"))
        self.assertEqual(trade["integrity"]["venues"]["OKX"]["trade_duplicates_n"], 1)
        self.assertIn("TRADE duplicate applied", trade["integrity"]["venues"]["OKX"]["reasons"])
        self.assertEqual(trade["parts"]["integrity_report_only"], "FAIL")
        self.assertEqual(trade["parts"]["g2"], "PASS")  # transit counts the first receipt; integrity catches it
        self.assertTrue(trade["pass"])  # report-only: G12 still PASSes
        bar = self.fault(edit=dup("BAR", 90))  # a retained closed bar re-written after a newer one
        self.assertEqual(bar["integrity"]["venues"]["OKX"]["report_only_rewinds"]["BAR"], 1)
        self.assertEqual(bar["parts"]["integrity_report_only"], "PASS")

    def test_quote_and_mark_event_time_rewind_fails(self):
        for feed in ("QUOTE", "MARK", "TRADE"):
            with self.subTest(feed=feed):
                def edit(views, feed=feed):
                    ev = next(v for v in views if v.get("k") == "ev" and v["venue"] == "BINANCE"
                              and v["feed"] == feed and v["t"] >= 100 * S)
                    return views + [dict(ev, t=ev["t"] + 20 * S, id="x-" + str(ev["id"]))]
                result = self.fault(edit=edit)
                self.assertEqual(result["integrity"]["venues"]["BINANCE"]["rewinds"][feed], 1)
                self.assertEqual(result["parts"]["integrity_report_only"], "FAIL")
                self.assertTrue(result["pass"])  # report-only

    def test_native_failure_counter_fails(self):
        for name in ("gaps", "cursor_expired_errors", "slow_consumer_errors", "duplicates_applied",
                     "offset_regressions", "snapshot_replaced"):
            with self.subTest(name=name):
                def counters(t, name=name):
                    return {**{n: 0 for n in NATIVE_NAMES}, "applied": t // S, name: 1 if t >= 120 * S else 0}
                result = self.fault(hb={"OKX": hb_rows("OKX", 0, 500 * S, counters=counters)})
                self.assertEqual(result["integrity"]["venues"]["OKX"]["native"]["status"], "FAIL")
                self.assertEqual(result["parts"]["integrity_report_only"], "FAIL")
                self.assertFalse(result["integrity"]["gating"])
                self.assertTrue(result["pass"])  # report-only
        # A failure counter that moved before the injection is outside the scope.
        before = lambda t: {**{n: 0 for n in NATIVE_NAMES}, "applied": t // S, "gaps": 1 if t >= 30 * S else 0}
        early = self.fault(hb={"OKX": hb_rows("OKX", 0, 500 * S, counters=before)})
        self.assertEqual(early["parts"]["integrity_report_only"], "PASS")

    def test_native_coverage_gaps_are_not_measured(self):
        cases = {
            "absent": [],
            "old sampler rows": [{"k": "hb", "t": t * S, "venue": "OKX", "heartbeat": True, "status": "READY",
                                  "demanded": 4, "session_ready": 4} for t in range(0, 500, 2)],
            "worker not live": hb_rows("OKX", 0, 500 * S, liveness="RESTARTING"),
            "roster incomplete": hb_rows("OKX", 0, 500 * S, workers=(3, 4)),
            "no sample after the scope": hb_rows("OKX", 0, 300 * S),
            "counters decreased": hb_rows("OKX", 0, 500 * S, counters=lambda t: {
                **{n: 0 for n in NATIVE_NAMES}, "applied": (t // S) if t < 200 * S else 1}),
            "bool is not a count": hb_rows("OKX", 0, 500 * S, counters=lambda t: {
                **{n: 0 for n in NATIVE_NAMES}, "gaps": False}),
        }
        for label, rows in cases.items():
            with self.subTest(label):
                result = self.fault(hb={"OKX": rows})
                okx = result["integrity"]["venues"]["OKX"]
                self.assertEqual(okx["native"]["status"], "NOT_MEASURED", okx["native"])
                self.assertEqual(okx["status"], "NOT_MEASURED")
                self.assertEqual(result["parts"]["integrity_report_only"], "NOT_MEASURED")
                self.assertTrue(result["pass"])  # report-only: G12 still PASSes
        late = self.fault(hb={"OKX": hb_rows("OKX", 0, 300 * S)})
        self.assertEqual(g12.verdict_undecided(late), ["integrity.OKX no heartbeat after the scope"])

    def test_notification_coverage_gaps_are_not_measured(self):
        listener = self.fault(edit=lambda v: v + [{"k": "ev_error", "t": 150 * S, "venue": "OKX",
                                                   "error": "ConnectionError"}])
        self.assertIn("notification listener errors", listener["integrity"]["venues"]["OKX"]["reasons"])
        self.assertEqual(listener["parts"]["integrity_report_only"], "NOT_MEASURED")
        anonymous = self.fault(edit=lambda v: v + [{"k": "ev", "t": 150 * S, "venue": "OKX", "feed": "TRADE",
                                                    "id": None, "ev_ms": None}])
        self.assertEqual(anonymous["parts"]["integrity_report_only"], "NOT_MEASURED")
        no_trade = self.fault(edit=lambda v: [r for r in v if not (r.get("k") == "ev" and r.get("feed") == "TRADE")])
        self.assertIn("no TRADE notification", no_trade["integrity"]["venues"]["OKX"]["reasons"])
        self.assertEqual(no_trade["parts"]["g2"], "FAIL")  # G2 (lost trades), not the integrity report, blocks it
        self.assertFalse(no_trade["pass"])
        self.assertTrue(listener["pass"] and anonymous["pass"])  # missing integrity coverage never blocks G12

    def test_pending_is_undecided_until_resolved(self):
        pubs, views = self.inputs()
        # The sampler stops at 365 s: a TRADE published at 358 s and not yet applied is undecidable (pending).
        views = [r for r in views if r["t"] <= 365 * S
                 and not (r.get("feed") == "TRADE" and r["venue"] == "OKX" and 358 * S <= r["t"] < 358.2 * S)]
        result = g12.evaluate_fault(self.FAULT, pubs, views, self.MONITOR)
        self.assertEqual(result["g2"]["TRADE"]["OKX"]["status"], "NOT_MEASURED")
        undecided = g12.verdict_undecided(result)
        self.assertTrue(any("pending" in item for item in undecided), undecided)
        self.assertFalse(result["pass"])
        self.assertEqual(g12.verdict_undecided(self.fault()), [])

    def test_sampler_keeps_the_typed_counters_raw(self):
        for field in g12.NATIVE_HEARTBEAT_FIELDS:
            self.assertIn(f'"{field}"', g12.SAMPLER)
        self.assertIn('"last_seen_unix": b.get("last_seen_unix")', g12.SAMPLER)
        self.assertIn('"key_tail"', g12.SAMPLER)

    def test_unfrozen_predicate_never_passes_and_v1_receipts_are_not_credited(self):
        faults = [dict(self.FAULT, name=n) for n in ("broker_stop_start", "projector_restart", "stream_restart")]
        good = {"pass": True, "parts": {"integrity": "PASS"}}
        with tempfile.TemporaryDirectory() as tmp:
            evidence = Path(tmp)
            (evidence / "faults.jsonl").write_text("".join(json.dumps(f) + "\n" for f in faults))
            with patch.object(g12, "evaluate_fault", return_value=good):
                unfrozen = g12.evaluate(evidence)
                self.assertFalse(unfrozen["pass"])
                self.assertTrue(unfrozen["credit"].startswith("NOT_CREDITABLE"))
                (evidence / g12.PREDICATE_FILE).write_text(json.dumps(g12.caught_up_predicate()))
                frozen = g12.evaluate(evidence)
                self.assertTrue(frozen["pass"])
                self.assertEqual(frozen["schema"], g12.VERDICT_SCHEMA)
        frozen.update(status="COMPLETED")
        self.assertEqual(g12.g12_receipt_creditable(frozen), (True, "PASS"))
        stale = [dict(frozen, schema="p184.g12.verdict.v2"),  # rc10-full3 / rc12 / rc18-control2 era
                 dict(frozen, caught_up_predicate=dict(frozen["caught_up_predicate"], version="p184.g12.caught_up.v1")),
                 dict(frozen, caught_up_predicate=dict(frozen["caught_up_predicate"], frozen=False)),
                 dict(frozen, status="FAIL"),
                 dict(frozen, faults=frozen["faults"][:2]),
                 dict(frozen, **{"pass": False}),
                 None]
        for verdict in stale:
            with self.subTest(verdict=str(verdict)[:60]):
                self.assertFalse(g12.g12_receipt_creditable(verdict)[0])


class BrokerAndWriterPrecheckTests(unittest.TestCase):
    """Coordinator 2026-10-16: explicit auto.leader.rebalance.enable (production's value, recorded) and a
    WriterClone precheck for the production native writer (B16 image, native env, BOOK batch wait 0)."""

    def test_auto_leader_rebalance_is_always_explicit(self):
        self.assertIn("auto.leader.rebalance.enable=true\n", g12.broker_properties("pw", 1))
        self.assertIn("auto.leader.rebalance.enable=false\n", g12.broker_properties("pw", 1, 0, False))
        self.assertNotIn("auto.leader.rebalance.enable=true", g12.broker_properties("pw", 3, 3000, False))
        for value in ("false", 0, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                g12.broker_properties("pw", 1, 0, value)

    @staticmethod
    def argv(**changes):
        env = {"TRADING_REDIS_URL": "redis://localhost:6391/2", **g12.WRITER_NATIVE_ENV,
               **g12.WRITER_PRODUCTION_ENV, **changes}
        argv = ["docker", "run", "-d", "--rm", "--name", "x"]
        for key, value in env.items():
            if value is not None:
                argv += ["-e", f"{key}={value}"]
        return argv + ["--entrypoint", "python", "sha256:ts", "-B", "-m", "services.market_data.main"]

    def test_production_native_writer_passes(self):
        check = g12.writer_clone_precheck(self.argv(), "sha256:" + "d" * 64, "sha256:" + "d" * 64)
        self.assertEqual((check["ok"], check["problems"]), (True, []))
        self.assertEqual(check["native_env"]["MARKET_DATA_NATIVE_APPLY_MAX_ROWS"], "16")
        self.assertEqual(check["production_env"], {"DATA_LAYER_V2_BOOK_SNAPSHOT_BATCH_MAX_WAIT_MS": "0"})
        self.assertNotIn("TRADING_REDIS_URL", json.dumps(check))  # only the native/production keys are recorded

    def test_non_production_writer_is_refused(self):
        image = "sha256:" + "d" * 64
        cases = {
            "python writer (no native env)": self.argv(**{k: None for k in g12.WRITER_NATIVE_ENV}),
            "apply rows not 16": self.argv(MARKET_DATA_NATIVE_APPLY_MAX_ROWS="64"),
            "apply rows absent": self.argv(MARKET_DATA_NATIVE_APPLY_MAX_ROWS=None),
            "resume not required": self.argv(MARKET_DATA_NATIVE_REQUIRE_RESUME="0"),
            "extra native key": self.argv(MARKET_DATA_NATIVE_TEST_CLOCK_FILE="/x"),
            "book wait default": self.argv(DATA_LAYER_V2_BOOK_SNAPSHOT_BATCH_MAX_WAIT_MS=None),
            "book wait 25": self.argv(DATA_LAYER_V2_BOOK_SNAPSHOT_BATCH_MAX_WAIT_MS="25"),
        }
        for label, argv in cases.items():
            with self.subTest(label):
                check = g12.writer_clone_precheck(argv, image, image)
                self.assertFalse(check["ok"])
                self.assertTrue(check["problems"])
        for clone, deployed in ((image, "sha256:" + "e" * 64), (image, None), (None, None)):
            with self.subTest(clone=clone, deployed=deployed):
                self.assertFalse(g12.writer_clone_precheck(self.argv(), clone, deployed)["ok"])

    def test_argv_env(self):
        self.assertEqual(g12.argv_env(["-e", "A=1", "-e", "B=x=y", "-v", "C=2", "-e"]), {"A": "1", "B": "x=y"})

    def test_heartbeat_trade_integrity_exposure_is_reported(self):
        rows = hb_rows("OKX", 0, 10 * S) + hb_rows("BINANCE", 0, 10 * S, workers=(3, 4))
        exposed = g12.heartbeat_integrity_exposed(rows)
        self.assertIsNone(exposed["OKX"])
        self.assertIn("workers reporting 3 of 4", exposed["BINANCE"])
        old = [{"k": "hb", "t": S, "venue": "OKX", "heartbeat": True, "status": "READY"}]
        self.assertEqual(g12.heartbeat_integrity_exposed(old)["BINANCE"], "no heartbeat sample")
        self.assertIsNotNone(g12.heartbeat_integrity_exposed(old)["OKX"])

    def test_verdict_carries_the_writer_precheck(self):
        with tempfile.TemporaryDirectory() as tmp:
            evidence = Path(tmp)
            self.assertIsNone(g12.evaluate(evidence)["writer_precheck"])
            (evidence / g12.WRITER_PRECHECK_FILE).write_text(json.dumps({"OKX": {"ok": True}}))
            self.assertEqual(g12.evaluate(evidence)["writer_precheck"], {"OKX": {"ok": True}})


class InternalTopicParityTests(unittest.TestCase):
    """Codex K1 (2026-10-16): a G12 receipt carries the effective internal-topic overrides before/after the faults,
    so a B-only run is never credited as C+B proof."""

    @staticmethod
    def snap(**extra):
        base = {"cleanup.policy": "compact", "segment.bytes": "104857600"}
        return {t: {"overrides": {**base, **extra.get(t, {})}, "sha256": "x"} for t in g12.INTERNAL_TOPICS}

    def test_classification(self):
        c = {"__consumer_offsets": {"delete.retention.ms": "3600000", "segment.ms": "600000"}}
        self.assertEqual(g12.internal_topic_parity({}), "NOT_MEASURED")
        self.assertEqual(g12.internal_topic_parity({"before": self.snap()}), "NOT_MEASURED")
        self.assertEqual(g12.internal_topic_parity({"before": self.snap(), "after": {"error": "x"}}), "NOT_MEASURED")
        self.assertEqual(g12.internal_topic_parity({"before": self.snap(), "after": self.snap()}),
                         "C_ABSENT_B_ONLY")
        self.assertEqual(g12.internal_topic_parity({"before": self.snap(**c), "after": self.snap(**c)}), "C_PRESENT")
        self.assertEqual(g12.internal_topic_parity({"before": self.snap(), "after": self.snap(**c)}),
                         "CHANGED_DURING_RUN")
        # a non-C override changing is not a C claim either way
        other = {"__transaction_state": {"min.insync.replicas": "1"}}
        self.assertEqual(g12.internal_topic_parity({"before": self.snap(), "after": self.snap(**other)}),
                         "C_ABSENT_B_ONLY")

    def test_receipt_records_snapshots_and_failed_reads(self):
        run = g12.Run.__new__(g12.Run)
        run.auto_leader_rebalance, notes = False, []
        run.note = lambda step, **fields: notes.append((step, fields))
        run.ns, run.work, run.images = "p184-g12-dl-0123456789abcdef", Path("/w"), g12.IMAGES
        before = self.snap()
        with patch.object(g12, "sh", return_value="noise\n" + json.dumps(before) + "\n"):
            run.record_internal_topics("before")
        with patch.object(g12, "sh", side_effect=RuntimeError("helper failed")):
            run.record_internal_topics("after")
        config = run.broker_config()
        self.assertIs(config["auto.leader.rebalance.enable"], False)
        self.assertEqual(config["internal_topics"]["before"], before)
        self.assertIn("helper failed", config["internal_topics"]["after"]["error"])
        self.assertEqual(config["internal_topic_parity"], "NOT_MEASURED")
        self.assertEqual([n[0] for n in notes], ["internal_topics", "internal_topics"])
