"""scripts/kn_window_writer.py: the W12-A compose variants differ from the live writer only as intended.

The render against the real writer needs Docker and sudo (run once before the window, receipt kept in the window
dir); here the pure planning, delta and readiness rules are proven.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("kn_window_writer", ROOT / "scripts/kn_window_writer.py")
tool = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tool)

OLD = "/state/qdl-v2/655d/bundle/identities-rotate-20260822T144323Z/trading-system"
NEW = "/state/qdl-v2/tls/gen-20261019T084200Z/identities/trading-system"


def live_writer(**env) -> dict:
    environment = {"MARKET_DATA_NATIVE_REQUIRE_RESUME": "1", "DATA_LAYER_V2_JWT_MANIFEST_REVISION": "11",
                   "DATA_LAYER_V2_TLS_CA_FILE": "/run/secrets/qdl-v2/ca.crt", "PRICE_FMT": "$x"}
    environment.update(env)
    return {"image": "sha256:2deb90b0", "environment": environment, "networks": {"executor_network": {}},
            "volumes": [{"type": "bind", "source": "/logs", "target": "/app/logs", "read_only": False},
                        {"type": "bind", "source": OLD, "target": "/run/secrets/qdl-v2", "read_only": True},
                        {"type": "bind", "source": "/jwt/private.key", "target": "/run/secrets/qdl-v2-jwt/private.key",
                         "read_only": True}]}


class PlanTest(unittest.TestCase):
    def test_variants_change_only_identity_source_and_resume_flag(self):
        live = live_writer()
        targets = tool.plan(live, NEW)
        self.assertEqual(set(targets), {"bootstrap", "strict", "rollback-bootstrap", "rollback"})
        self.assertEqual(tool.identity_mount(targets["bootstrap"])["source"], NEW)
        self.assertEqual(targets["bootstrap"]["environment"]["MARKET_DATA_NATIVE_REQUIRE_RESUME"], "0")
        self.assertEqual(tool.identity_mount(targets["strict"])["source"], NEW)
        self.assertEqual(targets["strict"]["environment"]["MARKET_DATA_NATIVE_REQUIRE_RESUME"], "1")
        self.assertEqual(tool.identity_mount(targets["rollback-bootstrap"])["source"], OLD)
        self.assertEqual(targets["rollback"], live)
        # the JWT key mount and every other field are untouched
        for service in targets.values():
            jwt = [m for m in service["volumes"] if m["target"] == "/run/secrets/qdl-v2-jwt/private.key"]
            self.assertEqual(jwt[0]["source"], "/jwt/private.key")
        self.assertEqual(tool.identity_mount(live)["source"], OLD, "the live dict is not mutated")

    def test_refuses_a_writer_that_is_not_strict_or_not_on_manifest_11(self):
        with self.assertRaises(tool.Refused):
            tool.plan(live_writer(MARKET_DATA_NATIVE_REQUIRE_RESUME="0"), NEW)
        with self.assertRaises(tool.Refused):
            tool.plan(live_writer(DATA_LAYER_V2_JWT_MANIFEST_REVISION="10"), NEW)

    def test_refuses_when_the_new_identity_is_already_mounted(self):
        with self.assertRaises(tool.Refused):
            tool.plan(live_writer(), OLD)

    def test_refuses_without_exactly_one_read_only_identity_bind(self):
        live = live_writer()
        live["volumes"][1]["read_only"] = False
        with self.assertRaises(tool.Refused):
            tool.plan(live, NEW)
        live = live_writer()
        live["volumes"].append(dict(live["volumes"][1]))
        with self.assertRaises(tool.Refused):
            tool.plan(live, NEW)

    def test_deltas_compare_volumes_by_target_not_order(self):
        live = live_writer()
        reordered = dict(live, volumes=list(reversed(live["volumes"])))
        self.assertEqual(tool.deltas(live, reordered), [])

    def test_compose_doc_escapes_dollar_and_names_the_writer_project(self):
        doc = tool.compose_doc(live_writer())
        self.assertEqual(doc["name"], "trading-system-market-data")
        service = doc["services"]["market_data_service_p184w"]
        self.assertEqual(service["environment"]["PRICE_FMT"], "$$x")
        self.assertEqual(doc["networks"], {"executor_network": {"external": True, "name": "executor_network"}})


class ReadyRuleTest(unittest.TestCase):
    ROW = {"status": "READY", "consumer_mode": "V2_PRIMARY", "demanded": "64", "ready": "64",
           "execution_ready": "60", "unhealthy": "0", "v1_fallback": "0", "last_seen_unix": 200.0}

    def test_ready_needs_a_heartbeat_of_this_container_and_full_demand(self):
        self.assertEqual(tool.evaluate(self.ROW, 64, since_unix=100.0), [])
        self.assertIn("heartbeat older than the container start", tool.evaluate(self.ROW, 64, since_unix=300.0))
        self.assertTrue(tool.evaluate(dict(self.ROW, ready="63"), 64, 100.0))
        self.assertTrue(tool.evaluate(dict(self.ROW, unhealthy="1"), 64, 100.0))
        self.assertTrue(tool.evaluate(dict(self.ROW, consumer_mode="V1_FALLBACK"), 64, 100.0))
        self.assertEqual(tool.evaluate(None, 64, 0.0), ["no heartbeat row"])

    def test_execution_ready_is_reported_not_gated(self):
        self.assertEqual(tool.evaluate(dict(self.ROW, execution_ready="0"), 64, 100.0), [])

    def test_started_at_parses_nanosecond_rfc3339_as_utc(self):
        self.assertAlmostEqual(tool.started_unix({"State": {"StartedAt": "2026-10-19T12:35:21.859115093Z"}}),
                               1792413321.859115, places=3)


class SoakArithmeticTest(unittest.TestCase):
    @staticmethod
    def probe(count, total, buckets, failed=0):
        return {"probe_latency_ms": {"count": count, "sum": total, "max": 9.0, "buckets": buckets},
                "probes_failed": failed, "probes_unavailable": 0}

    def test_probe_totals_sum_workers_and_delta_gives_window_quantiles(self):
        b0 = [0] * 12
        first = tool.probe_totals({"0": {"feeds": {"QUOTE": self.probe(10, 100.0, [10] + b0[1:])}},
                                   "1": {"feeds": {"QUOTE": self.probe(5, 50.0, [5] + b0[1:])}}})
        self.assertEqual(first["QUOTE"]["count"], 15)
        later = [15, 0, 80, 0, 0, 0, 0, 0, 0, 0, 0, 5]  # +80 in (25, 50], +5 overflow
        last = tool.probe_totals({"0": {"feeds": {"QUOTE": self.probe(100, 4150.0, later, failed=1)}}})
        delta = tool.probe_delta(first, last)["QUOTE"]
        self.assertEqual(delta["requests"], 85)
        self.assertEqual(delta["mean_ms"], 47.06)
        self.assertEqual(delta["p50_le_ms"], 50)
        self.assertEqual(delta["p99_le_ms"], ">2000")
        self.assertEqual(delta["failed"], 1)

    def test_probe_delta_reports_a_counter_reset_instead_of_a_negative_window(self):
        first = {"TRADE": {"count": 10, "sum": 1.0, "buckets": [10] + [0] * 11, "max": 1.0, "failed": 0,
                           "unavailable": 0}}
        last = {"TRADE": dict(first["TRADE"], count=3)}
        self.assertIn("error", tool.probe_delta(first, last)["TRADE"])

    def test_memory_and_log_classification(self):
        self.assertEqual(tool.memory_mib("557.1MiB / 2GiB"), 557.1)
        self.assertEqual(tool.memory_mib("1.5GiB / 2GiB"), 1536.0)
        lines = [
            "2026-10-17 05:43:03,089 - market_data_worker-0 - x - WARNING - Data Layer V2 slice disconnected "
            "venue=BINANCE symbol=BNBUSDT feed=BOOK_DELTA interval=None reason=DataLayerError code=DATA_STALE "
            "retryable=True detail=required data exceeds its freshness policy",
            "handshake failed: CERTIFICATE_VERIFY_FAILED unable to get local issuer certificate",
            "native slice refused: HANDOFF_RESUME_REQUIRED",
            "market active scope refresh failed reason=ConnectError",
        ]
        counts = tool.count_log_lines(lines)
        self.assertEqual(counts["auth_protocol"], {"CERTIFICATE_VERIFY_FAILED": 1, "HANDOFF_RESUME_REQUIRED": 1})
        self.assertEqual(counts["data_policy_by_code_venue_symbol_feed"],
                         {"DATA_STALE BINANCE BNBUSDT BOOK_DELTA": 1})


if __name__ == "__main__":
    unittest.main()
