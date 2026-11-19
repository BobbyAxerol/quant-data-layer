"""DL v2.4.1 S6: bar_edge provider-short proofs survive a restart (sidecar), bound the restore gap check, and fail
open to the previous behaviour (re-prove from the provider) whenever a proof is absent, stale, of another binding
lineage or unreadable. Also: a not-yet-ready readback is a quiet retry, not an ERROR traceback."""
import json
import tempfile
import unittest
from pathlib import Path

from tests import test_phaseb_bar_history_bootstrap as bootstrap_tests
from qdl.runtime import stable_bar_edge as module


class ShortProofTests(unittest.TestCase):
    def setUp(self):
        self.helper = bootstrap_tests.StableBarBootstrapTests("test_checkpoint_history_gap_is_binding_scoped_and_detected_from_cache")
        self.helper.setUp()
        self.directory = tempfile.TemporaryDirectory(prefix="qdl-v241-short-proofs-")
        self.spool, self.edge = self.helper._cached_edge(self.directory.name, self.helper._NoopPublisher())
        self.edge.state_path = Path(self.directory.name) / "crypto-bar-checkpoint.json"
        self.source, self.acquisition = next(
            pair for pair in self.edge.history_bindings
            if pair[0].instrument.native_symbol == "BTCUSDT" and pair[0].interval == "1m")
        # Only the newest bar exists: a short listing whose first bar is 120_000.
        self.helper._cached_final_bar(self.spool, self.source, open_ms=120_000)

    def tearDown(self):
        self.spool.close()
        self.directory.cleanup()

    def gaps(self):
        return self.edge._checkpoint_history_gaps({self.source.binding_id: 120_000})

    def proof(self, **overrides):
        requested = self.edge._bootstrap_rows_for(self.source)
        record = {"binding_digest": self.edge._binding_digests[self.source.binding_id], "requested": requested,
                  "available": 1, "earliest_open_ms": 120_000, "proved_at_ms": 170_000}
        record.update(overrides)
        return record

    def write(self, proofs, schema=module._SHORT_PROOFS_SCHEMA):
        self.edge._short_proofs_path().write_text(json.dumps({"schema": schema, "proofs": proofs}))

    def test_without_a_proof_the_pre_listing_open_counts_as_missing(self):
        self.assertEqual(self.gaps(), {self.source.binding_id: 1})

    def test_a_valid_proof_bounds_the_window_and_restores_the_short_state(self):
        self.write({self.source.binding_id: self.proof()})
        self.assertEqual(self.gaps(), {})
        self.assertEqual(self.edge._history_short[self.source.binding_id], (self.proof()["requested"], 1))

    def test_a_proof_never_hides_a_real_gap_after_the_earliest_open(self):
        self.write({self.source.binding_id: self.proof(earliest_open_ms=60_000)})
        self.assertEqual(self.gaps(), {self.source.binding_id: 1})

    def test_stale_other_lineage_or_other_depth_proofs_are_not_used(self):
        for override in ({"proved_at_ms": 170_000 - module._SHORT_PROOF_MAX_AGE_MS - 60_000},
                         {"binding_digest": "0" * 64}, {"requested": 9}):
            with self.subTest(override=override):
                self.write({self.source.binding_id: self.proof(**override)})
                self.assertEqual(self.gaps(), {self.source.binding_id: 1})

    def test_an_unreadable_or_foreign_file_is_ignored_as_a_whole(self):
        for body in ("{", json.dumps({"schema": "other", "proofs": {}}),
                     json.dumps({"schema": module._SHORT_PROOFS_SCHEMA, "proofs": {"x": {"requested": 1}}})):
            with self.subTest(body=body[:20]):
                self.edge._short_proofs_path().write_text(body)
                self.assertEqual(self.edge._load_short_proofs(), {})
                self.assertEqual(self.gaps(), {self.source.binding_id: 1})

    def test_recording_a_proof_persists_it_atomically_and_only_for_known_bindings(self):
        self.write({"not-a-binding": self.proof()})
        requested = self.edge._bootstrap_rows_for(self.source)
        self.edge._record_short_proof(self.source.binding_id, requested, 1, 120_000)
        document = json.loads(self.edge._short_proofs_path().read_text())
        self.assertEqual(document["schema"], module._SHORT_PROOFS_SCHEMA)
        self.assertEqual(list(document["proofs"]), [self.source.binding_id])
        self.assertEqual(document["proofs"][self.source.binding_id]["earliest_open_ms"], 120_000)
        self.assertEqual(list(Path(self.directory.name).glob(".*.tmp")), [])
        self.assertEqual(self.gaps(), {})

    def test_the_checkpoint_file_itself_is_untouched(self):
        self.assertFalse(self.edge.state_path.exists())
        self.edge._record_short_proof(self.source.binding_id, self.edge._bootstrap_rows_for(self.source), 1, 120_000)
        self.assertFalse(self.edge.state_path.exists())
        self.assertTrue(self.edge._short_proofs_path().name.endswith(".history-short.json"))


class ReadbackNotReadyIsAQuietRetryTests(unittest.TestCase):
    def test_not_ready_is_logged_as_a_warning_without_traceback(self):
        from qdl.runtime.kn_bar_readback import KnBarReadbackNotReady

        class Edge(module.StableBinanceBarEdge):
            pass

        edge = Edge.__new__(Edge)
        edge.repair_only = False
        edge._history_bootstrap_active = True
        edge._heartbeat_path = None
        edge.bar_readback = None
        edge._rest_fallback_active = False
        turns = {"n": 0}

        class Stopped:
            def is_set(self):
                return turns["n"] >= 1

            def wait(self, _delay):
                turns["n"] += 1
                return True

        edge._stopped = Stopped()
        edge.clock = lambda: 0.0
        edge._loop_sleep_seconds = lambda now: 1.0

        lpk = type("Lpk", (), {"encode": lambda self: "lpk1|paper|BINANCE|USDM|x|BAR|1h"})()

        def boom(**_kwargs):
            raise KnBarReadbackNotReady(lpk, "REBUILDING")

        edge.bootstrap_history = boom
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="WARNING") as logs:
            edge.run_forever()
        self.assertTrue(any("readback not ready, retrying" in line for line in logs.output))
        self.assertFalse(any("cycle failed" in line for line in logs.output))


if __name__ == "__main__":
    unittest.main()
