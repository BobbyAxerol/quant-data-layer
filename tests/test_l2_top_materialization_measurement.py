"""Offline measurement must not hide censored changes or invent matching tops."""
import asyncio
import unittest
from scripts.measure_l2_top_materialization import distribution, top_delay, top_observations
from scripts.phase114_l2_real_provider_capture import capture_replay_window

class MeasurementTests(unittest.TestCase):
    def test_censored_changes_stay_in_denominator(self):
        rows=[{"seq":str(i),"ms":i*100,"top":top} for i,top in enumerate([("1","3"),("2","3"),("2","4"),("2","4"),("1","4")])]
        result=top_delay(rows,[{"seq":"3","top":("2","4")}])
        self.assertEqual(result["top_changes"],3)
        self.assertEqual(result["superseded_before_snapshot"],1)
        self.assertEqual(result["unmatched_tail"],1)
        self.assertEqual(result["matched_delay"]["p50"],100)
        self.assertTrue(result["denominator_check"])
        self.assertEqual(result["next_verified_snapshot_delay"]["n"],2)
        self.assertEqual(result["next_verified_snapshot_delay"]["max"],200)
        self.assertEqual(result["next_snapshot_uncovered_tail"],1)

    def test_snapshot_must_match_its_sequence(self):
        with self.assertRaises(ValueError):
            top_delay([{"seq":"1","ms":1,"top":("1","2")}],[{"seq":"2","top":("1","2")}])
        with self.assertRaises(ValueError):
            top_delay([{"seq":"1","ms":1,"top":("1","2")}],[{"seq":"1","top":("0","2")}])

    def test_keepalive_sequence_does_not_backdate_later_snapshot(self):
        rows=[{"seq":"1","ms":0,"top":("1","3")},
              {"seq":"2","ms":100,"top":("2","3")},
              {"seq":"2","ms":1000,"top":("2","3")}]
        r=top_delay(rows,[{"seq":"2","ms":1000,"top":("2","3")}])
        self.assertEqual(r["next_verified_snapshot_delay"]["p50"],900)

    def test_no_samples_not_zero_latency(self):
        self.assertIsNone(distribution([])["p95"])
        self.assertEqual(distribution([1,2,3])["max"],3)

    def test_same_event_snapshot_has_zero_policy_delay(self):
        rows=[{"seq":"1","ms":100,"top":("1","3")},{"seq":"2","ms":200,"top":("2","3")}]
        self.assertEqual(top_delay(rows,[{"seq":"2","top":("2","3")}])["matched_delay"]["max"],0)

    def test_window_bounds_fail_before_network(self):
        for seconds,bindings in [(0,[{}]),(121,[{}]),(60,[]),(60,[{}]*13)]:
            with self.assertRaises(ValueError):
                asyncio.run(capture_replay_window(bindings,seconds))

    def test_oracle_rejects_sequence_gap(self):
        import json
        frames=[{"action":"snapshot","data":[{"seqId":1,"prevSeqId":-1,"ts":"100","bids":[["1","2"]],"asks":[["3","2"]]}]},
                {"action":"update","data":[{"seqId":3,"prevSeqId":2,"ts":"200","bids":[],"asks":[]}]}]
        with self.assertRaises(ValueError):
            top_observations({"binding":{"venue":"OKX"},"rows":[{"raw":json.dumps(f),"protocol":"WS"} for f in frames]})

    def test_analysis_classifies_bootstrap_using_protobuf_enum(self):
        import base64
        import json
        import tempfile
        from pathlib import Path
        from scripts.measure_l2_top_materialization import analyse
        from qdl.provider.v1.raw_provider_pb2 import QuarantineRecord, QUARANTINE_REASON_SEQUENCE_GAP
        from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
        with tempfile.TemporaryDirectory() as folder:
            work=Path(folder)
            frames=[]; output=[]
            for seq, stamp, bid in [(1,100,1),(2,200,2)]:
                frame={"action":"snapshot" if seq==1 else "update","data":[{"seqId":seq,"prevSeqId":seq-1,"ts":str(stamp),"bids":[[str(bid),"1"]],"asks":[["3","1"]]}]}
                frames.append({"protocol":"WS","received_at_ns":stamp*1000000,"raw":json.dumps(frame)})
                e=EventEnvelope(venue="OKX",native_symbol="TEST",source_event_time_ns=stamp*1000000,received_at_ns=stamp*1000000)
                e.book_snapshot.native_sequence=str(seq);e.book_snapshot.sequence_verified=True
                for side, price in [(1,bid),(2,3)]:
                    level=e.book_snapshot.levels.add(side=side);level.price.mantissa=price
                output.append({"topic":"idr.canonical","bytes":base64.b64encode(e.SerializeToString()).decode()})
            q=QuarantineRecord(reason=QUARANTINE_REASON_SEQUENCE_GAP,safe_summary="L2_RESNAPSHOT_REQUIRED_V1")
            q.raw.venue="OKX";q.raw.native_symbol="TEST";q.raw.received_at_ns=1
            output.append({"topic":"idr.quarantine","bytes":base64.b64encode(q.SerializeToString()).decode()})
            (work/"capture.json").write_text(json.dumps({"captures":[{"binding":{"venue":"OKX","native_symbol":"TEST"},"rows":frames}]}))
            (work/"output.json").write_text(json.dumps(output));analyse(work)
            metrics=json.loads((work/"metrics.json").read_text())["OKX/TEST"]
            self.assertEqual(metrics["bootstrap_resnapshot_requests"],1)
            self.assertEqual(metrics["top_delay"]["next_verified_snapshot_delay"]["n"],1)
            q.raw.received_at_ns=300000000
            output[-1]["bytes"]=base64.b64encode(q.SerializeToString()).decode()
            (work/"output.json").write_text(json.dumps(output))
            with self.assertRaises(ValueError): analyse(work)

    def test_cleanup_waits_for_auto_remove_and_refuses_shared_names(self):
        from unittest.mock import patch, Mock
        from scripts.measure_l2_top_materialization import stop_disposable
        with self.assertRaises(ValueError): stop_disposable("rust_core")
        with patch("scripts.measure_l2_top_materialization.subprocess.run", side_effect=[Mock(returncode=0),Mock(returncode=0),Mock(returncode=1)]) as run, patch("scripts.measure_l2_top_materialization.time.sleep"):
            stop_disposable("qdl-idr062-pair-core")
            self.assertEqual(run.call_count,3)
