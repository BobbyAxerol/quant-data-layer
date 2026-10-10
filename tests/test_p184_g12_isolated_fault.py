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


if __name__ == "__main__":
    unittest.main()
