import unittest
from scripts.execution_view_diagnostics import classify_trade_observation, error_evidence


class ExecutionViewDiagnosticsTests(unittest.TestCase):
    def test_preserves_exact_refusal_without_claiming_authority_failure(self):
        error = RuntimeError("do not expose arbitrary exception text")
        error.code = "SOURCE_NON_AUTHORITATIVE"
        error.diagnostics = {"quality": {"state": "LIVE", "execution_eligible": False,
                                        "flags": ["LAST_EVENT_STALE"]},
                             "source": {"authoritative": True}, "watermark_offset": 123}
        result = error_evidence(error)
        self.assertEqual(result["diagnostics"], error.diagnostics)
        self.assertNotIn("error_detail", result)

    def test_exact_trade_identity_classification_across_all_symbols(self):
        for venue in ("BINANCE", "OKX"):
            for symbol in ("BTC", "ETH", "SOL", "DOGE", "BNB"):
                with self.subTest(venue=venue, symbol=symbol):
                    view = {"payload": {"native_trade_id": "123"}}
                    self.assertEqual(classify_trade_observation(view, "123"), "MATCHED_PROVIDER_LAST_TRADE")
                    self.assertEqual(classify_trade_observation(view, "124"), "PIPELINE_BEHIND_PROVIDER")
                    self.assertEqual(classify_trade_observation(view, "122"), "OBSERVER_BEHIND_PIPELINE")
                    self.assertEqual(classify_trade_observation(view, None), "PROVIDER_EVIDENCE_MISSING")

    def test_unknown_ids_never_imply_quiet_market(self):
        self.assertEqual(classify_trade_observation({}, "1"), "VIEW_IDENTITY_MISSING")
        self.assertEqual(classify_trade_observation({"payload": {"native_trade_id": "uuid-a"}}, "uuid-b"), "INCOMPARABLE_TRADE_IDS")


class ExecutionComponentMatrixTests(unittest.IsolatedAsyncioTestCase):
    async def test_five_symbols_both_venues_keep_strict_and_quiet_contracts_distinct(self):
        import tempfile
        from pathlib import Path
        from qdl.query import StalePolicy
        from qdl.runtime.execution_mark_index import ExecutionMarkIndexLiveView, ExecutionMarkIndexQuietPolicy
        from qdl.runtime.session_liveness import StableSessionLivenessReader
        from tests.test_execution_mark_index_live_view import (
            NOW_NS, _record, _binding, _paired_envelope, _stored, _write_session,
        )
        for venue, market in (("BINANCE", "USDM"), ("OKX", "SWAP")):
            for symbol in ("BTC", "ETH", "SOL", "DOGE", "BNB"):
                with self.subTest(venue=venue, symbol=symbol), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    record = _record(venue=venue, market=market, base=symbol,
                        native_symbol=symbol + ("USDT" if venue == "BINANCE" else "-USDT-SWAP"))
                    binding = _binding(record)
                    mark_limit, index_limit = ((5000, 5000) if venue == "BINANCE" else (15000, 70000))
                    envelope = _paired_envelope(binding, sequence=1, generation=3,
                        mark_received_at_ns=NOW_NS - 3_000_000_000,
                        index_received_at_ns=NOW_NS - 3_000_000_000)
                    view = ExecutionMarkIndexLiveView(frozenset({record.instrument_uid}),
                        quiet_policies={record.instrument_uid: ExecutionMarkIndexQuietPolicy(
                            (("MARK", mark_limit), ("INDEX", index_limit)))},
                        session_liveness_reader=StableSessionLivenessReader(root))
                    await view.remember(binding=binding, envelope=envelope,
                        stored=_stored(envelope, offset=31), gateway_epoch=5)
                    _write_session(root, envelope)
                    args = dict(instrument_uid=record.instrument_uid,
                        instrument_revision=record.metadata_revision, source_policy_id="crypto_liquid_v2",
                        max_freshness_ms=2000, gateway_epoch=5, now_ns=NOW_NS)
                    quiet_args = dict(args, event_recency_policy=StalePolicy.OBSERVE,
                                      max_session_liveness_ms=2000)
                    self.assertEqual((await view.read(**args)).reason, "STALE")
                    quiet = await view.read(**quiet_args)
                    self.assertIsNotNone(quiet.record)
                    before = quiet.component_receipts_ns
                    await view.remember(binding=binding, envelope=envelope,
                        stored=_stored(envelope, offset=31), gateway_epoch=5)
                    self.assertEqual((await view.read(**quiet_args)).component_receipts_ns, before)
                    _write_session(root, envelope, state="DISCONNECTED")
                    self.assertIsNone((await view.read(**quiet_args)).record)
                    _write_session(root, envelope, generation=4)
                    self.assertIsNone((await view.read(**quiet_args)).record)
                    _write_session(root, envelope)
                    # A new MARK cannot refresh an expired INDEX component.
                    later_ns = NOW_NS + (index_limit - 3000 + 1) * 1_000_000
                    stale_index = _paired_envelope(binding, sequence=2, generation=3,
                        mark_received_at_ns=later_ns - 1_000_000,
                        index_received_at_ns=NOW_NS - 3_000_000_000)
                    await view.remember(binding=binding, envelope=stale_index,
                        stored=_stored(stale_index, offset=32), gateway_epoch=5)
                    _write_session(root, stale_index, last_transport_at_ns=later_ns - 1_000_000)
                    self.assertEqual((await view.read(**dict(quiet_args, now_ns=later_ns))).reason,
                                     "COMPONENT_STALE")


if __name__ == "__main__":
    unittest.main()
