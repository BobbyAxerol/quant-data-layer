"""Offline parts of the R-cert 2 shadow driver (`scripts/v230_rcert2_shadow.py`).

Starts nothing: render into a temporary directory, the docker argv per role
(caps, isolation), and the comparator's verdicts with a fake cache and a
fake REST client.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import tempfile
import unittest

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.runtime.kn_bar_readback import DurableBarRow
from scripts import v230_rcert2_shadow as shadow

NS = "qdl-v230-rcert2-20261113t1000z"
RUST = "sha256:" + "a" * 64
PYTHON = "sha256:" + "b" * 64


class RenderAndTopologyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.work = Path(cls.tmp.name) / "work"
        cls.packet = shadow.render(cls.work, NS, rust_image=RUST, python_image=PYTHON, tls_hours=2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.tmp.cleanup()

    def test_render_declares_native_1m_and_the_derived_chain(self):
        import yaml

        plan = yaml.safe_load((self.work / "acquisition.yaml").read_text())
        modes = {}
        for row in plan["bindings"]:
            modes[row["mode"]] = modes.get(row["mode"], 0) + 1
            if row["mode"] == "DERIVED_FINAL":
                self.assertIsNone(row["websocket_url"])
                self.assertEqual(row["sequence_policy"], "NONE")
        self.assertEqual(modes, {"RUST_NATIVE": 3, "PYTHON_REST": 2, "DERIVED_FINAL": 65})
        rest = [row for row in plan["bindings"] if row["mode"] == "PYTHON_REST"]
        self.assertTrue(all(row["native_channel"] == "candle1m" and row["websocket_url"] is None for row in rest))
        self.assertEqual(self.packet["lanes"]["DOGE-USDT-SWAP"], "REST_FINAL")
        self.assertEqual(self.packet["lanes"]["BTC-USDT-SWAP"], "NATIVE_WS")
        self.assertEqual(len(self.packet["base_bindings"]), 5)
        self.assertEqual(plan["topics"]["raw"], NS + ".raw")
        self.assertEqual(len(self.packet["derived_parents"]), 65)
        catalog = json.loads((self.work / "catalog.yaml").read_text())
        intervals = {row["binding_id"]: row["interval"] for row in catalog["bindings"]}
        for child, parent in self.packet["derived_parents"].items():
            self.assertEqual(shadow.CHAIN[intervals[child]], intervals[parent])
        self.assertEqual(catalog["canonical_stream"], NS + ".canonical")

    def test_core_and_ingestor_configs(self):
        core = json.loads((self.work / "core.json").read_text())
        derived = [item for item in core["core"]["bindings"] if "derived_bar" in item]
        self.assertEqual(len(derived), 65)
        self.assertEqual(len(core["core"]["bindings"]), 70)
        week = [item["derived_bar"] for item in derived if item["derived_bar"]["interval"] == "1w"]
        self.assertTrue(week and all(item["anchor_ms"] == 4 * 86_400_000 for item in week))
        ingestor = json.loads((self.work / "ingestor.json").read_text())
        self.assertEqual(sorted({item["native_channel"] for item in ingestor["bindings"]}), ["candle1m"])
        self.assertEqual(sorted(item["native_symbol"] for item in ingestor["bindings"]),
                         sorted(set(shadow.SYMBOLS) - set(shadow.REST_FINAL_SYMBOLS)))
        self.assertTrue(ingestor["generation_state_path"].startswith("/work/"))
        bundle = json.loads((self.work / "bundle.json").read_text())
        self.assertTrue(bundle)

    def test_private_material_is_owner_only_and_render_refuses_reuse(self):
        for name in ("client.key", "server.p12", "broker.properties"):
            mode = stat.S_IMODE(os.stat(self.work / name).st_mode)
            self.assertEqual(mode, 0o600, name)
        with self.assertRaises(ValueError):
            shadow.render(self.work, NS, rust_image=RUST, python_image=PYTHON)
        with self.assertRaises(ValueError):
            shadow.render(Path(self.tmp.name) / "x", "prod", rust_image=RUST, python_image=PYTHON)
        with self.assertRaises(ValueError):
            shadow.render(Path(self.tmp.name) / "y", NS, rust_image="qdl-v2-rust:latest", python_image=PYTHON)

    def test_topology_stays_inside_the_caps_and_the_namespace(self):
        self.assertEqual(shadow.caps_total(), (shadow.CAP_TOTAL_CPUS, shadow.CAP_TOTAL_MIB))
        self.assertLessEqual(shadow.CAP_TOTAL_MIB, int(2.3 * 1024))
        roles = shadow.topology(self.work, uid="1000:1000", deadline_s=3600)
        self.assertEqual(set(roles), {"net", "kafka", *shadow.SERVICE_ORDER})
        for role, argv in roles.items():
            joined = " ".join(argv)
            with self.subTest(role):
                self.assertIn("--rm", argv)
                self.assertEqual(argv[argv.index("--cpu-shares") + 1], "256")
                self.assertEqual(argv[argv.index("--cpus") + 1], f"{shadow.CAPS[role][0]:.2f}")
                self.assertEqual(argv[argv.index("--label") + 1], f"{shadow.LABEL}={NS}")
                self.assertEqual(argv[argv.index("--name") + 1], f"{NS}-{role}")
                self.assertNotIn("-p", argv)
                self.assertNotIn("--publish", joined)
                self.assertNotIn("qdl_v2_stable", joined)
                network = argv[argv.index("--network") + 1]
                self.assertEqual(network, NS + "-net" if role == "net" else f"container:{NS}-net")
        self.assertIn(RUST, roles["core"])
        self.assertIn(PYTHON, roles["baredge"])
        helper = shadow.helper_argv(self.work, ["--create-topics", "/work"], uid="1000:1000")
        self.assertEqual(helper[helper.index("--cpu-shares") + 1], "256")

    def test_namespace_and_topics(self):
        topics = shadow.topic_names(NS)
        self.assertEqual(set(topics), {NS + suffix for suffix in (".raw", ".canonical", ".quarantine",
                                                                  ".latest", ".bars")})
        self.assertEqual(topics[NS + ".bars"][0], shadow.BARS_PARTITIONS)
        with self.assertRaises(ValueError):
            shadow.namespace("qdl_v2_stable_candidate")


def _row(interval_ms: int, *, origin=common_pb2.BAR_ORIGIN_AGGREGATED, count=3, base="1m",
         values=("1", "2", "0.5", "1.5", "10", "0.1", "15")) -> DurableBarRow:
    envelope = market_data_pb2.EventEnvelope(published_at_ns=(1_000 + interval_ms + 2_000) * 1_000_000)
    bar = envelope.bar
    bar.open_time_ns = 1_000 * 1_000_000
    bar.close_time_ns = (1_000 + interval_ms - 1) * 1_000_000
    for name, text in zip(("open", "high", "low", "close", "volume", "base_volume", "quote_volume"), values):
        getattr(bar, name).CopyFrom(common_pb2.DecimalValue(source_text=text, mantissa=int(text.replace(".", "")),
                                                            scale=len(text.partition(".")[2])))
    bar.is_final = True
    bar.origin = origin
    bar.lifecycle = market_data_pb2.BAR_LIFECYCLE_FINAL
    bar.volume_unit = common_pb2.QUANTITY_UNIT_CONTRACT
    if origin == common_pb2.BAR_ORIGIN_AGGREGATED:
        bar.aggregation.constituent_count = count
        bar.aggregation.base_interval = base
    return DurableBarRow(envelope, 7)


NATIVE = ["1000", "1", "2", "0.5", "1.50", "10", "0.10", "15", "1"]


class ComparatorTests(unittest.TestCase):
    def test_verdicts(self):
        classify = shadow.classify
        self.assertEqual(classify(derived=_row(180_000), native_row=NATIVE, interval="3m", base_interval="1m")
                         ["verdict"], "MATCH")
        other = list(NATIVE)
        other[2] = "2.1"
        result = classify(derived=_row(180_000), native_row=other, interval="3m", base_interval="1m")
        self.assertEqual((result["verdict"], result["fields"]), ("MISMATCH", ["high"]))
        self.assertEqual(result["derive_latency_ms"], 2_001)
        self.assertEqual(classify(derived=_row(180_000, count=2), native_row=NATIVE, interval="3m",
                                  base_interval="1m")["verdict"], "LINEAGE_MISMATCH")
        self.assertEqual(classify(derived=_row(180_000, origin=common_pb2.BAR_ORIGIN_VENUE_NATIVE),
                                  native_row=NATIVE, interval="3m", base_interval="1m")["verdict"],
                         "BOOTSTRAP_NATIVE")
        self.assertEqual(classify(derived=None, native_row=NATIVE, interval="3m", base_interval="1m")["verdict"],
                         "MISSING")
        provisional = list(NATIVE)
        provisional[8] = "0"
        self.assertEqual(classify(derived=_row(180_000), native_row=provisional, interval="3m",
                                  base_interval="1m")["verdict"], "NATIVE_UNAVAILABLE")

    def test_base_verdicts_and_latency_gates(self):
        row = _row(60_000, origin=common_pb2.BAR_ORIGIN_VENUE_NATIVE)
        result = shadow.classify_base(row=row, native_row=NATIVE)
        self.assertEqual((result["verdict"], result["final_latency_ms"]), ("MATCH", 2_001))
        gates = shadow.latency_gates({"REST_FINAL/1m": {"n": 9, "p50": 3_000, "p99": 19_000},
                                      "REST_FINAL/1h": {"n": 3, "p50": 9_000, "p99": 31_000},
                                      "NATIVE_WS/1h": {"n": 3, "p50": 1_000, "p99": 2_000}})
        self.assertEqual((gates["REST_FINAL/1m"], gates["REST_FINAL/derived"]), ("PASS", "FAIL"))
        self.assertEqual(shadow.latency_gates({})["REST_FINAL/1m"], "NOT_MEASURED")

    def test_compare_once_records_and_summarizes(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory) / "work"
            packet = shadow.render(work, NS, rust_image=RUST, python_image=PYTHON, tls_hours=1)
            catalog = json.loads((work / "catalog.yaml").read_text())
            intervals = {row["binding_id"]: row["interval"] for row in catalog["bindings"]}
            child = next(key for key in sorted(packet["derived_parents"]) if intervals[key] == "3m")

            class Readback:
                def final_bar_rows(self, source, opens):
                    if source.binding_id != child:
                        return {}
                    return {value: _row(180_000) for value in opens}

            calls = []

            def http(params):
                calls.append(params)
                if params["bar"] != "3m":
                    return {"code": "0", "data": []}
                return {"code": "0", "data": [[str(int(params["after"]) - 1), *NATIVE[1:]]]}

            summary = shadow.compare(work, once=True, http=http, readback=Readback(), started_ms=0)
            self.assertEqual(summary["mismatches"], 0)
            self.assertEqual(summary["by_interval"]["3m"]["MATCH"], 3)
            self.assertEqual(summary["missing"], (64 + 5) * 3, "every other binding and base has no row here")
            self.assertNotIn("NATIVE_UNAVAILABLE", summary["by_verdict"])
            self.assertEqual([value["n"] for key, value in summary["latency_ms"].items() if key.endswith("/3m")], [3])
            self.assertIn("latency_gates", summary)
            # A second pass compares nothing twice (restart-safe evidence).
            again = shadow.compare(work, once=True, http=http, readback=Readback(), started_ms=0)
            self.assertEqual(again["by_interval"]["3m"]["MATCH"], 3)


if __name__ == "__main__":
    unittest.main()
