"""DR05 OKX inverse scope (BTC-USD-SWAP BOOK): scopes, frames, export/catalog filter, consumers, publisher, resets.

Offline only (no Docker). The inverse catalog fixture is the repository catalog plus the deployed inverse instrument
and its BOOK snapshot/delta bindings (identity as in the rev-12 paper catalog that binding r23 was compiled from)."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

from scripts import p184_dr05_dl_fault as dr

NS = "p184-dr05-dl-0123456789abcdef"
ALL = ("BINANCE", "OKX", "OKX_INVERSE")
INV_UID = "8482b54b-6d9d-5aad-abe8-e22447d183a1"
INV_SOURCE = "okx-swap-btc-usd-swap-book-primary-v2"


def inverse_catalog(path):
    """Write the repo catalog + the inverse instrument and its two BOOK bindings; return the path."""
    catalog = yaml.safe_load((dr.ROOT / "config/v2/stable-source-bindings.yaml").read_text())
    catalog["catalog_revision"] += 1
    catalog["instruments"].append({
        "instrument_uid": INV_UID, "instrument_id": "OKX.SWAP.PERPETUAL.BTC-USD", "metadata_revision": 1,
        "venue": "OKX", "market": "SWAP", "product_type": "PERPETUAL", "canonical_symbol": "BTC-USD",
        "native_symbol": "BTC-USD-SWAP", "asset_class": "DERIVATIVE", "base_asset": "BTC", "quote_asset": "USD",
        "settlement_asset": "BTC", "price_tick": "0.1", "quantity_step": "0.1", "contract_multiplier": "100",
        "session_calendar_id": "CRYPTO_24X7", "attributes": {"ctMult": "1", "ctType": "inverse", "ctVal": "100",
        "ctValCcy": "USD", "groupId": "4", "instFamily": "BTC-USD", "instIdCode": "10458", "uly": "BTC-USD"}})
    for feed in ("BOOK_DELTA", "BOOK_SNAPSHOT"):
        catalog["bindings"].append({
            "binding_id": "okx-swap-btc-usd-swap-" + feed.lower(), "instrument_uid": INV_UID, "feed": feed,
            "interval": None, "source": {"provider": "OKX_DIRECT", "source_id": INV_SOURCE, "source_role": "PRIMARY",
            "source_policy_id": "crypto_liquid_v2", "authoritative": True, "adapter_version": "okx-v5/2.0.0",
            "normalizer_version": "qdl-rust-core/2.0.0"},
            "quality": {"stale_after_ms": 60000, "require_final_bar": False, "continuous_calendar": True},
            "v1_compatibility": "NONE"})
    path.write_text(yaml.safe_dump(catalog))
    return path


def exported(with_inverse=True):
    from tests.test_p184_dr05_dl_fault import MaterialTests
    out = MaterialTests.exported()
    if with_inverse:
        okx = next(b for b in out["core"]["bindings"] if b["venue"] == "OKX")
        inverse = copy.deepcopy(okx)
        inverse.update(native_symbol="BTC-USD-SWAP", instrument_uid=INV_UID,
                       instrument_id="OKX.SWAP.PERPETUAL.BTC-USD", instrument_catalog_revision=12,
                       source_id=INV_SOURCE)
        out["core"]["bindings"].append(inverse)
    return out


class ScopeTests(unittest.TestCase):
    def test_default_scopes_and_cells_unchanged(self):
        self.assertEqual(dr.validate_scopes(None), dr.VENUES)
        self.assertEqual(dr.cells_for(dr.VENUES), dr.CELLS)
        self.assertEqual(dr.scope_set(None), dr.VENUES)

    def test_scope_validation_orders_and_refuses(self):
        self.assertEqual(dr.validate_scopes(["OKX_INVERSE", "BINANCE", "OKX"]), ALL)
        for bad in ([], ["OKX", "OKX"], ["OKX_LINEAR"], ["BINANCE_INVERSE"]):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                dr.validate_scopes(bad)

    def test_prepare_seals_inverse_scope_cells_schedules_and_catalog(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = inverse_catalog(Path(tmp) / "catalog.yaml")
            work = Path(tmp) / "case"
            packet = dr.prepare(work, scopes=["OKX_INVERSE", "BINANCE", "OKX"], catalog=catalog)
            self.assertEqual(packet["scopes"], list(ALL))
            self.assertEqual(dr.scope_set(work), ALL)
            self.assertIn("DR05.GAP_REFUSAL@OKX_INVERSE", packet["schedules"])
            self.assertIn("DR05.GENERATION_CHANGE_FENCED@OKX_INVERSE", packet["schedules"])
            pending = [json.loads(line)["cell"] for line in (work / "pending-cells.jsonl").read_text().splitlines()]
            self.assertEqual(len(pending), 6)
            self.assertEqual((work / dr.SOURCE_CATALOG).read_bytes(), catalog.read_bytes())
            self.assertEqual(dr.source_catalog_path(work), work / dr.SOURCE_CATALOG)
            self.assertEqual(len(packet["source_catalog"]["sha256"]), 64)
            handoff = json.loads((work / "handoff.json").read_text())
            (work / "handoff.json").write_text(json.dumps({**handoff, "scopes": ["OKX", "BINANCE"]}))
            with self.assertRaises(ValueError):  # unordered/tampered scopes refused
                dr.scope_set(work)


class FrameTests(unittest.TestCase):
    def test_inverse_frame_is_okx_books_for_btc_usd_swap(self):
        value = dr.frame("OKX_INVERSE", False, 101, 100, 7, "2", 123_000_000)
        self.assertEqual((value["venue"], value["native_symbol"], value["transport"]),
                         ("OKX", "BTC-USD-SWAP", "WEBSOCKET"))
        self.assertEqual(value["payload"]["arg"], {"channel": "books", "instId": "BTC-USD-SWAP"})
        self.assertEqual(value["payload"]["data"][0]["prevSeqId"], "100")
        snapshot = dr.frame("OKX_INVERSE", True, 100, 0, 7, "1", 123_000_000)
        self.assertEqual((snapshot["payload"]["action"], snapshot["transport"]), ("snapshot", "WEBSOCKET"))
        self.assertEqual(dr.scope_of(value["venue"], value["native_symbol"]), "OKX_INVERSE")

    def test_inverse_schedule_follows_okx_semantics(self):
        for fault in dr.FAULTS:
            inverse = dr.schedule("OKX_INVERSE", fault)
            linear = dr.schedule("OKX", fault)
            self.assertEqual([{k: v for k, v in s.items() if k != "venue"} for s in inverse],
                             [{k: v for k, v in s.items() if k != "venue"} for s in linear])
            self.assertTrue(all(s["venue"] == "OKX_INVERSE" for s in inverse))


class ConfigTests(unittest.TestCase):
    def test_isolated_config_selects_the_inverse_book_binding(self):
        cfg = dr.isolated_config(exported(), NS, scopes=ALL)
        self.assertEqual([(b["venue"], b["native_symbol"]) for b in cfg["core"]["bindings"]],
                         [("BINANCE", "ETHUSDT"), ("OKX", "ETH-USDT-SWAP"), ("OKX", "BTC-USD-SWAP")])
        self.assertEqual(len(dr.isolated_config(exported(), NS)["core"]["bindings"]), 2)  # default linear only
        with self.assertRaisesRegex(ValueError, "OKX_INVERSE"):
            dr.isolated_config(exported(with_inverse=False), NS, scopes=ALL)

    def test_material_filters_catalog_to_granted_inverse_book_feeds(self):
        from qdl.consumer.manifest import ConsumerManifestLoader
        from qdl.runtime.stable_catalog import StableSourceCatalog
        from scripts.kn_gateway_bundle import compile_bundle
        with tempfile.TemporaryDirectory() as tmp:
            catalog = inverse_catalog(Path(tmp) / "catalog.yaml")
            work = Path(tmp) / "case"
            packet = dr.prepare(work, scopes=ALL, catalog=catalog)
            dr.material(work, exported())
            loaded = StableSourceCatalog.load(work / "catalog.yaml")
            inverse = [b for b in loaded.bindings if b.instrument.instrument_uid == INV_UID]
            self.assertEqual(sorted(b.feed.value for b in inverse), ["BOOK_DELTA", "BOOK_SNAPSHOT"])
            self.assertEqual(len(loaded.bindings), 6)
            manifest = ConsumerManifestLoader.load(work / "okx_inverse.yaml")
            self.assertEqual(manifest.consumer_id, packet["namespace"] + ".okx_inverse")
            self.assertEqual({(r.instrument_uid, r.feed.value) for r in manifest.requirements},
                             {(INV_UID, "BOOK_SNAPSHOT"), (INV_UID, "BOOK_DELTA")})
            consumer = json.loads((work / "ts-consumer.okx_inverse.json").read_text())
            self.assertEqual((consumer["venue"], consumer["jwt_key_id"]), ("OKX_INVERSE", "okx_inverse"))
            route = json.loads((work / "okx_inverse.binding.json").read_text())
            self.assertEqual({(p["venue"], p["native_symbol"], p["feed"]) for p in route["products"]},
                             {("OKX", "BTC-USD-SWAP", "BOOK_SNAPSHOT"), ("OKX", "BTC-USD-SWAP", "BOOK_DELTA")})
            runtime = json.loads((work / "packet.json").read_text())
            self.assertEqual(runtime["isolated_manifest_paths"],
                             ["/work/binance.yaml", "/work/okx.yaml", "/work/okx_inverse.yaml"])
            manifests = [work / Path(p).name for p in runtime["isolated_manifest_paths"]]
            self.assertEqual(json.loads((work / "bundle.json").read_text()), compile_bundle(
                environment="paper", catalog_path=work / "catalog.yaml", manifest_paths=manifests))
            self.assertEqual((work / "okx_inverse.key").stat().st_mode & 0o777, 0o600)

    def test_inverse_scope_without_a_catalog_that_defines_it_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "case"
            dr.prepare(work, scopes=ALL)  # repo catalog has no BTC-USD-SWAP BOOK
            with self.assertRaises(ValueError):
                dr.material(work, exported())

    def test_cli_scope_and_catalog_only_at_init_or_prepare(self):
        with tempfile.TemporaryDirectory() as tmp, patch("sys.stdout"), patch("sys.stderr"):
            catalog = inverse_catalog(Path(tmp) / "catalog.yaml")
            work = Path(tmp) / "case"
            self.assertEqual(dr.dispatch(["--prepare", str(work), "--scope", "BINANCE", "--scope", "OKX",
                                          "--scope", "OKX_INVERSE", "--catalog", str(catalog)]), 0)
            self.assertEqual(dr.scope_set(work), ALL)
            for argv in (["--up", str(work), "--scope", "OKX_INVERSE"], ["--up", str(work), "--catalog", str(catalog)],
                         ["--prepare", str(Path(tmp) / "b"), "--scope", "BINANCE_INVERSE"]):
                with self.subTest(argv=argv), self.assertRaises(SystemExit):
                    dr.dispatch(argv)


class ConsumerAndPublisherTests(unittest.TestCase):
    def test_consumer_packet_refuses_a_linear_manifest_for_the_inverse_scope(self):
        from qdl.runtime.stable_catalog import StableSourceCatalog
        from scripts.p184_dr05_case import consumer_packet
        from scripts.kn_native_slice_probe import ISSUER, AUDIENCE
        with tempfile.TemporaryDirectory() as tmp:
            catalog = inverse_catalog(Path(tmp) / "catalog.yaml")
            work = Path(tmp) / "case"
            packet = dr.prepare(work, scopes=ALL, catalog=catalog)
            dr.material(work, exported())
            loaded = StableSourceCatalog.load(work / "catalog.yaml")
            linear = yaml.safe_load((work / "okx.yaml").read_text())
            linear["metadata"]["consumer_id"] = packet["namespace"] + ".okx_inverse"
            forged = work / "forged.yaml"
            forged.write_text(yaml.safe_dump(linear))
            with self.assertRaises(ValueError):
                consumer_packet(forged, loaded, "OKX_INVERSE", ISSUER, AUDIENCE)

    def test_publisher_roster_and_inverse_envelope(self):
        from qdl.raw.envelope import validate_raw_envelope
        from scripts.p184_dr05_raw_publisher import RawPublisher, raw_envelope
        from tests.test_p184_dr05_raw_publisher import Producer
        bindings = dr.isolated_config(exported(), NS, scopes=ALL)["core"]["bindings"]
        inverse = bindings[2]
        value = raw_envelope(inverse, dr.frame("OKX_INVERSE", True, 100, 0, 7, "1", 123000000), NS)
        validate_raw_envelope(value)
        self.assertEqual((value.native_symbol, value.source_session_id), ("BTC-USD-SWAP", NS + ".OKX_INVERSE"))
        linear = raw_envelope(bindings[1], dr.frame("OKX", True, 100, 0, 7, "1", 123000000), NS)
        self.assertEqual(linear.source_session_id, NS + ".OKX")  # linear sessions unchanged
        with self.assertRaises(ValueError):
            raw_envelope(bindings[1], dr.frame("OKX_INVERSE", True, 100, 0, 7, "1", 123000000), NS)
        packet = {"namespace": NS, "bootstrap": "localhost:9092"}
        config = {"raw_topics": [NS + ".raw"], "core": {"allow_test_provenance": True, "bindings": bindings}}
        with patch.dict("os.environ", {"P184_DR05_ISOLATED_NAMESPACE": NS}), \
                patch("confluent_kafka.Producer", Producer):
            publisher = RawPublisher(packet, config)
            self.assertEqual(set(publisher.bindings), set(ALL))
            publisher.publish(dr.frame("OKX_INVERSE", True, 100, 0, 7, "1", 123000000))
            self.assertEqual(publisher.producer.calls[-1][1]["key"], INV_SOURCE.encode())
            for broken in (bindings[:1] + bindings[2:], bindings + bindings[2:]):
                with self.subTest(n=len(broken)), self.assertRaises(ValueError):
                    RawPublisher(packet, {**config, "core": {**config["core"], "bindings": broken}})


class ResetScopeTests(unittest.TestCase):
    def test_reset_and_channel_refuse_a_scope_not_prepared(self):
        from scripts import p184_dr05_reset as reset
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "case"
            dr.prepare(work)  # default linear scopes
            with patch("scripts.p184_dr05_reset._packet", return_value=(work, NS, {}, {})), \
                    self.assertRaisesRegex(ValueError, "not prepared"):
                reset.reset_preserving_broker(work, "OKX_INVERSE", 5)
            with self.assertRaisesRegex(ValueError, "unknown venue"):
                reset.reset_preserving_broker(work, "BINANCE_INVERSE", 5)

    def test_channel_reset_checks_sealed_scopes(self):
        from scripts import p184_dr05_channel as ch
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            (work / "handoff.json").write_text(json.dumps({"scopes": ["BINANCE", "OKX"]}))
            loader = Mock(return_value=(work, NS, {"namespace": NS, "slice": {"topic_id": "t"}},
                                        {"raw_topics": [NS + ".raw"]}))
            channel = ch.Channel(work, loader=loader, publisher_factory=Mock(), observer_factory=Mock())
            try:
                with self.assertRaisesRegex(ValueError, "unknown cell"):
                    channel.reset("OKX_INVERSE", "GAP_REFUSAL", 10 ** 12)
            finally:
                channel.close()


class QueryRosterTests(unittest.TestCase):
    def test_reader_roster_accepts_the_sealed_inverse_manifest_only(self):
        from scripts import p184_dr05_observer as obs
        paths = ["/work/binance.yaml", "/work/okx.yaml", "/work/okx_inverse.yaml"]
        expected = {NS + "." + Path(p).stem for p in paths}

        class Slice:
            def __init__(self, _args):
                self.identities = {k: None for k in expected}

        manifests = [type("M", (), {"consumer_id": cid})() for cid in sorted(expected)]
        with patch("scripts.kn_native_slice_probe.Slice", Slice), \
                patch("qdl.consumer.manifest.ConsumerManifestLoader.load", side_effect=manifests):
            runner = obs.reader_harness({"namespace": NS, "slice": {}, "isolated_manifest_paths": paths})
        self.assertEqual(set(runner.manifests), expected)
        with self.assertRaises(ValueError):
            obs.reader_harness({"namespace": NS, "slice": {}, "isolated_manifest_paths": ["/work/okx_linear.yaml"]})


if __name__ == "__main__":
    unittest.main()
