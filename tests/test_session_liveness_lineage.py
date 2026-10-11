"""Session lineage: an event from a session that handed over without a gap.

Stage 100e (2026-10-10) refused OKX BNB-USDT-SWAP TRADE with DATA_STALE /
SOURCE_SESSION_UNKNOWN although its event recency was LIVE: the lane's
liveness file had been replaced by the next session's record, so the last
trade, published by generation 167, matched no record. When the OKX ingestor
replaces a session make-before-break (service-upgrade notice 64008), it writes
``lineage/<lane>.lineage`` with the chain of sessions that overlapped. These
tests pin that the reader then judges such an event by the chain's current
session, and that every doubtful case still fails closed: a chain reset by a
gap, a successor that is down, ambiguous chains, a config mismatch and a
malformed lineage file. Readers that predate lineage never list the
subdirectory, so the rollout order of reader and writer does not matter.
"""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from qdl.runtime.session_liveness import (
    SESSION_CONTINUED_FLAG,
    SESSION_LINEAGE_SCHEMA,
    SESSION_LIVENESS_SCHEMA,
    StableSessionLivenessReader,
)

OLD = ("okx-public-002-167-1791612000000000000", 167)
NEW = ("okx-public-002-168-1791612713096478415", 168)
NOW = 1_791_612_714_551_564_264


class SessionLineageTests(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.directory = Path(self._dir.name) / "okx-swap"
        self.directory.mkdir()
        self.reader = StableSessionLivenessReader(Path(self._dir.name))

    def tearDown(self) -> None:
        self._dir.cleanup()

    def record(self, session: tuple[str, int], *, state: str = "LIVE",
               revision: int = 20, lane: str = "okx-public-002") -> None:
        (self.directory / f"{lane}.json").write_text(json.dumps({
            "schema": SESSION_LIVENESS_SCHEMA, "source_session_id": session[0],
            "connection_generation": session[1], "state": state,
            "last_transport_at_ns": NOW - 3_000_000, "updated_at_ns": NOW - 3_000_000,
            "config_revision": revision}), encoding="utf-8")

    def lineage(self, *chain: tuple[str, int], lane: str = "okx-public-002",
                revision: int = 20, body: object | None = None) -> None:
        directory = self.directory / "lineage"
        directory.mkdir(exist_ok=True)
        payload = body if body is not None else {
            "schema": SESSION_LINEAGE_SCHEMA, "lane": lane,
            "chain": [{"source_session_id": session, "connection_generation": generation}
                      for session, generation in chain],
            "updated_at_ns": NOW - 3_000_000, "config_revision": revision}
        (directory / f"{lane}.lineage").write_text(json.dumps(payload), encoding="utf-8")

    def status(self, session: tuple[str, int] = OLD, revision: int = 20):
        return self.reader.status(venue="OKX", market="SWAP", source_session_id=session[0],
                                  connection_generation=session[1],
                                  config_revision=revision, now_ns=NOW)

    def test_without_lineage_an_event_of_a_replaced_session_is_unknown(self) -> None:
        # The stage 100e refusal, reproduced.
        self.record(NEW)
        result = self.status()
        self.assertEqual((result.state, result.flags), ("UNKNOWN", ("SOURCE_SESSION_UNAVAILABLE",)))

    def test_an_overlapped_predecessor_is_judged_by_its_live_successor(self) -> None:
        self.record(NEW)
        self.lineage(OLD, NEW)
        result = self.status()
        self.assertEqual((result.state, result.liveness_ms, result.flags),
                         ("LIVE", 3, (SESSION_CONTINUED_FLAG,)))
        # The current session itself still matches directly, with no flag.
        self.assertEqual(self.status(NEW).flags, ())

    def test_a_chain_reset_by_a_gap_never_vouches_for_the_old_session(self) -> None:
        self.record(NEW)
        self.lineage(NEW)
        self.assertEqual(self.status().state, "UNKNOWN")

    def test_a_successor_that_is_down_is_not_live_evidence(self) -> None:
        self.record(NEW, state="DISCONNECTED")
        self.lineage(OLD, NEW)
        result = self.status()
        self.assertEqual((result.state, result.flags), ("DISCONNECTED", (SESSION_CONTINUED_FLAG,)))

    def test_a_chain_whose_last_link_is_not_the_current_record_is_ignored(self) -> None:
        # The lineage was written for 168, but the record already names 169.
        self.record(("okx-public-002-169-1", 169))
        self.lineage(OLD, NEW)
        self.assertEqual(self.status().state, "UNKNOWN")

    def test_config_revision_must_match_on_lineage_successor_and_event(self) -> None:
        self.record(NEW)
        self.lineage(OLD, NEW, revision=21)
        self.assertEqual(self.status().state, "UNKNOWN")
        self.lineage(OLD, NEW)
        result = self.status(revision=19)
        self.assertEqual((result.state, result.flags), ("UNKNOWN", ("SOURCE_SESSION_CONFIG_MISMATCH",)))

    def test_two_chains_claiming_one_session_fail_closed(self) -> None:
        other = ("okx-public-003-175-1", 175)
        self.record(NEW)
        self.record(other, lane="okx-public-003")
        self.lineage(OLD, NEW)
        self.lineage(OLD, other, lane="okx-public-003")
        self.assertEqual(self.status().state, "UNKNOWN")

    def test_malformed_lineage_fails_closed(self) -> None:
        self.record(NEW)
        for body in (
            {"schema": SESSION_LINEAGE_SCHEMA, "lane": "okx-public-002", "chain": [],
             "updated_at_ns": 1, "config_revision": 20},
            {"schema": "qdl.provider-session-lineage.v0", "lane": "x",
             "chain": [{"source_session_id": OLD[0], "connection_generation": OLD[1]}],
             "updated_at_ns": 1, "config_revision": 20},
            # Generations must increase along the chain.
            {"schema": SESSION_LINEAGE_SCHEMA, "lane": "okx-public-002",
             "chain": [{"source_session_id": NEW[0], "connection_generation": NEW[1]},
                       {"source_session_id": OLD[0], "connection_generation": OLD[1]}],
             "updated_at_ns": 1, "config_revision": 20},
            {"schema": SESSION_LINEAGE_SCHEMA, "lane": "okx-public-002",
             "chain": [{"source_session_id": OLD[0], "connection_generation": True},
                       {"source_session_id": NEW[0], "connection_generation": NEW[1]}],
             "updated_at_ns": 1, "config_revision": 20, "extra": 1},
        ):
            self.lineage(body=body)
            self.assertEqual(self.status().state, "UNKNOWN", body)

    def test_lineage_change_is_seen_on_the_next_call(self) -> None:
        self.record(NEW)
        self.lineage(OLD, NEW)
        self.assertEqual(self.status().state, "LIVE")
        self.lineage(NEW)  # reset after a gap: same name, new content
        self.assertEqual(self.status().state, "UNKNOWN")

    def test_the_lineage_directory_is_not_a_session_record(self) -> None:
        # Only the subdirectory exists next to the record; the direct lookup is
        # unchanged and the healthcheck-visible '*.json' set is untouched.
        self.record(NEW)
        self.lineage(OLD, NEW)
        self.assertEqual(sorted(path.name for path in self.directory.glob("**/*.json")),
                         ["okx-public-002.json"])
        self.assertEqual(self.status(NEW).state, "LIVE")


if __name__ == "__main__":
    unittest.main()
