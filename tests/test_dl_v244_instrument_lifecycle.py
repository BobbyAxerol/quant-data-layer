"""DL v2.4.4 (DL-BL-2026-11-26-K): instrument lifecycle observation ledger, Query endpoint and SDK.

Test-only provenance: the listing captures below are hand-built in the documented response shapes of
OKX `GET /api/v5/public/instruments` and Binance `GET /fapi/v1/exchangeInfo`; they are not venue bytes.

Covered:
- the ledger rules: hash chain per instrument, file position, tamper / drop / reorder / partial-line refusal,
  append-only proof between two files;
- the recorder: initial observation, idempotent replay, venue-state changes (suspend, post_only, absence -> DELISTED,
  FUTURES absence -> EXPIRED, relisting), only venue-stated effective times, unmapped states, and the fail-closed
  refusals (invalid / empty / implausible / out-of-order captures, identity conflicts);
- the CLI writer (init, record, verify, typed refusal exit 1);
- Query: catalogue instrument, retired instrument served from the ledger, NO_OBSERVATION / NOT_CONFIGURED, paging,
  404, permission; end to end over real HTTP (uvicorn on 127.0.0.1) through the SDK transport and client model.
"""
from __future__ import annotations

import asyncio
import contextlib
import io
import json
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from qdl.api_v2 import create_v2_app
from qdl.consumer import ConsumerManifestLoader
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import AssetClass, InstrumentIdentity, InstrumentRecord, InstrumentRegistry, ProductType
from qdl.query import EntitlementPolicy, InstrumentQuery, MemoryMarketDataBackend, V2QueryService
from qdl.runtime import instrument_lifecycle as lc
from tests.phase7_support import make_identity, make_token, manifest_mapping

T0, T1, T2, T3 = "2026-11-19T14:03:37Z", "2026-11-20T00:00:00Z", "2026-11-21T00:00:00Z", "2026-11-22T00:00:00Z"
OKX_ENDPOINT = "GET https://www.okx.com/api/v5/public/instruments?instType=SWAP"


def record(venue, market, symbol, native, product_type=ProductType.PERPETUAL):
    identity = InstrumentIdentity.create(venue=venue, market=market, product_type=product_type,
                                         canonical_symbol=symbol)
    return InstrumentRecord(
        identity=identity, metadata_revision=1, asset_class=AssetClass.DERIVATIVE, native_symbol=native,
        base_asset=symbol.split("-")[0], quote_asset="USDT", settlement_asset="USDT",
        price_tick=CanonicalDecimal.from_text("0.1"), quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"), session_calendar_id="CRYPTO_24X7",
        expiry_time_ns=1_798_185_600_000_000_000 if product_type is ProductType.FUTURE else None)


BTC = record("OKX", "SWAP", "BTC-USDT", "BTC-USDT-SWAP")
RVN = record("OKX", "SWAP", "RVN-USDT", "RVN-USDT-SWAP")
ETH = record("OKX", "SWAP", "ETH-USDT", "ETH-USDT-SWAP")
SOL = record("OKX", "SWAP", "SOL-USDT", "SOL-USDT-SWAP")


def catalog_item(rec):
    return {"instrument_uid": rec.instrument_uid, "instrument_id": rec.instrument_id,
            "venue": rec.identity.venue, "market": rec.identity.market, "native_symbol": rec.native_symbol}


CATALOG = [catalog_item(r) for r in (BTC, RVN, ETH, SOL)]


def okx(rows, code="0"):
    return json.dumps({"code": code, "msg": "", "data": [
        {"instType": "SWAP", "instId": inst, "state": state, "listTime": "1600000000000", "expTime": exp}
        for inst, state, exp in rows]}).encode()


def okx_all(**overrides):
    states = {"BTC-USDT-SWAP": "live", "RVN-USDT-SWAP": "live", "ETH-USDT-SWAP": "live", "SOL-USDT-SWAP": "live"}
    states.update(overrides)
    return okx([(k, v, "") for k, v in states.items() if v is not None])


def capture(raw, observed, venue="OKX", market="SWAP", endpoint=OKX_ENDPOINT):
    return lc.load_listing_capture(venue, market, raw, endpoint=endpoint, observed_utc=observed)


class Ledger:
    """An in-memory ledger driven only through the module's public functions."""

    def __init__(self):
        self.head, self.events = lc.header("test-ledger"), []

    def record(self, raw, observed, instruments=CATALOG, **kwargs):
        appended, report = lc.record_capture(self.head, self.events, instruments, capture(raw, observed, **{
            k: kwargs.pop(k) for k in ("venue", "market") if k in kwargs}),
            catalog_revision=kwargs.pop("catalog_revision", 18), source_revision="test", **kwargs)
        self.events = [*self.events, *appended]
        return appended, report

    def raw(self):
        return lc.render_ledger(self.head, self.events)


def history(ledger, rec):
    return [e for e in ledger.events if e["instrument_uid"] == rec.instrument_uid]


class RecorderTests(unittest.TestCase):
    def test_first_capture_records_one_event_per_instrument_and_replay_is_idempotent(self):
        ledger = Ledger()
        appended, report = ledger.record(okx_all(), T0)
        self.assertEqual((len(appended), report["tracked"], report["unchanged"]), (4, 4, 0))
        first = history(ledger, BTC)[0]
        self.assertEqual((first["status"], first["venue_state"], first["sequence"]), ("ACTIVE", "live", 1))
        self.assertIsNone(first["previous_event_id"])
        self.assertEqual((first["effective_utc"], first["effective_basis"]), ("2020-09-13T12:26:40.000Z", "OKX:listTime"))
        self.assertEqual(first["evidence"]["capture_sha256"], capture(okx_all(), T0).capture_sha256)
        self.assertEqual(first["evidence"]["doc_url"], lc.DOC_URLS["OKX"])
        self.assertEqual(first["event_id"], lc.event_id_of(first))
        appended, report = ledger.record(okx_all(), T1)
        self.assertEqual((appended, report["unchanged"]), ([], 4))

    def test_state_changes_delisting_and_relisting_are_chained(self):
        ledger = Ledger()
        ledger.record(okx_all(), T0)
        ledger.record(okx_all(**{"RVN-USDT-SWAP": "suspend", "ETH-USDT-SWAP": "post_only"}), T1)
        ledger.record(okx_all(**{"RVN-USDT-SWAP": None, "ETH-USDT-SWAP": "post_only"}), T2)
        ledger.record(okx_all(), T3)
        rvn = history(ledger, RVN)
        self.assertEqual([(e["status"], e["venue_state"]) for e in rvn],
                         [("ACTIVE", "live"), ("HALTED", "suspend"), ("DELISTED", "ABSENT"), ("ACTIVE", "live")])
        self.assertEqual([e["sequence"] for e in rvn], [1, 2, 3, 4])
        self.assertEqual([e["previous_event_id"] for e in rvn[1:]], [e["event_id"] for e in rvn[:-1]])
        self.assertEqual((rvn[1]["effective_utc"], rvn[2]["effective_utc"], rvn[2]["evidence"]["venue_times"]),
                         (None, None, {}))
        eth = history(ledger, ETH)
        self.assertEqual([(e["status"], e["venue_state"]) for e in eth],
                         [("ACTIVE", "live"), ("ACTIVE", "post_only"), ("ACTIVE", "live")])
        self.assertEqual(len(history(ledger, BTC)), 1)
        self.assertEqual([e["position"] for e in ledger.events], list(range(1, len(ledger.events) + 1)))

    def test_a_retired_instrument_stays_tracked_without_the_catalogue(self):
        ledger = Ledger()
        ledger.record(okx_all(), T0)
        without_rvn = [item for item in CATALOG if item["instrument_uid"] != RVN.instrument_uid]
        ledger.record(okx_all(**{"RVN-USDT-SWAP": None}), T1, instruments=without_rvn)
        self.assertEqual(history(ledger, RVN)[-1]["status"], "DELISTED")
        ledger.record(okx_all(), T2, instruments=without_rvn)
        self.assertEqual(history(ledger, RVN)[-1]["status"], "ACTIVE")

    def test_futures_absence_is_expired_and_only_venue_times_are_used(self):
        future = record("OKX", "FUTURES", "BTC-USDT-261225", "BTC-USDT-261225", ProductType.FUTURE)
        items = [catalog_item(future)]
        ledger = Ledger()
        ledger.record(okx([("BTC-USDT-261225", "live", "1798185600000")]), T0, instruments=items, market="FUTURES")
        self.assertEqual(ledger.events[0]["evidence"]["venue_times"],
                         {"listTime": "1600000000000", "expTime": "1798185600000"})
        ledger.record(okx([("BTC-USDT-261226", "live", "")]), T1, instruments=items, market="FUTURES",
                      max_absent_fraction=1.0)
        self.assertEqual((ledger.events[-1]["status"], ledger.events[-1]["effective_utc"]), ("EXPIRED", None))

    def test_binance_usdm_states(self):
        perp = record("BINANCE", "USDM", "BTC-USDT", "BTCUSDT")
        quarter = record("BINANCE", "USDM", "ETH-USDT-260925", "ETHUSDT_260925", ProductType.FUTURE)
        items = [catalog_item(perp), catalog_item(quarter)]
        doc = lambda a, b: json.dumps({"symbols": [
            {"symbol": "BTCUSDT", "status": a, "onboardDate": 1569398400000, "deliveryDate": 4133404800000},
            {"symbol": "ETHUSDT_260925", "status": b, "onboardDate": 1750000000000, "deliveryDate": 1790323200000},
        ]}).encode()
        ledger = Ledger()
        kw = dict(venue="BINANCE", market="USDM", instruments=items)
        ledger.record(doc("TRADING", "TRADING"), T0, **kw)
        ledger.record(doc("CLOSE", "DELIVERED"), T1, **kw)
        last = {e["instrument_uid"]: e for e in ledger.events}
        self.assertEqual(last[perp.instrument_uid]["status"], "DELISTED")
        self.assertEqual((last[quarter.instrument_uid]["status"], last[quarter.instrument_uid]["effective_utc"],
                          last[quarter.instrument_uid]["effective_basis"]),
                         ("EXPIRED", "2026-09-25T08:00:00.000Z", "BINANCE:deliveryDate"))
        self.assertEqual(last[perp.instrument_uid]["evidence"]["doc_url"], lc.DOC_URLS[("BINANCE", "USDM")])

    def test_unmapped_venue_state_is_reported_and_not_recorded(self):
        ledger = Ledger()
        ledger.record(okx_all(), T0)
        appended, report = ledger.record(okx_all(**{"SOL-USDT-SWAP": "something_new"}), T1)
        self.assertEqual(appended, [])
        self.assertEqual(report["unmapped"], [{"instrument_id": SOL.instrument_id, "venue_state": "something_new"}])

    def test_unusable_captures_are_refused_and_record_nothing(self):
        cases = (
            (lambda: capture(okx_all(), T0.replace("Z", "")), "LIFECYCLE_CAPTURE_INVALID"),
            (lambda: capture(okx_all(), T0, venue="DERIBIT"), "LIFECYCLE_VENUE_UNSUPPORTED"),
            (lambda: capture(okx([], code="0"), T0), "LIFECYCLE_CAPTURE_EMPTY"),
            (lambda: capture(okx([("BTC-USDT-SWAP", "live", "")], code="50011"), T0), "LIFECYCLE_CAPTURE_INVALID"),
            (lambda: capture(b"<html>busy</html>", T0), "LIFECYCLE_CAPTURE_INVALID"),
        )
        for make, code in cases:
            with self.subTest(code=code), self.assertRaises(lc.LifecycleCaptureRefused) as refused:
                make()
            self.assertEqual(refused.exception.code, code)
        ledger = Ledger()
        ledger.record(okx_all(), T1)
        before = list(ledger.events)
        only_btc = okx([("BTC-USDT-SWAP", "live", "")])
        for raw, observed, code in ((only_btc, T2, "LIFECYCLE_CAPTURE_IMPLAUSIBLE"),
                                    (okx_all(**{"RVN-USDT-SWAP": "suspend"}), T0, "LIFECYCLE_CAPTURE_OUT_OF_ORDER")):
            with self.subTest(code=code), self.assertRaises(lc.LifecycleCaptureRefused) as refused:
                ledger.record(raw, observed)
            self.assertEqual(refused.exception.code, code)
            self.assertEqual(ledger.events, before)
        conflict = [{**CATALOG[0], "native_symbol": "BTC-USDT-SWAP-OTHER"}]
        with self.assertRaises(lc.LifecycleCaptureRefused) as refused:
            ledger.record(okx_all(), T2, instruments=conflict)
        self.assertEqual(refused.exception.code, "LIFECYCLE_IDENTITY_CONFLICT")


class LedgerFileTests(unittest.TestCase):
    def setUp(self):
        self.ledger = Ledger()
        self.ledger.record(okx_all(), T0)
        self.ledger.record(okx_all(**{"RVN-USDT-SWAP": "suspend"}), T1)
        self.raw = self.ledger.raw()

    def lines(self):
        return self.raw.decode().split("\n")[:-1]

    def test_round_trip_and_index(self):
        index = lc.InstrumentLifecycleIndex(self.raw)
        self.assertEqual(len(index.events), 5)
        self.assertEqual(index.uid_for(RVN.instrument_id), RVN.instrument_uid)
        self.assertEqual(index.uid_for(RVN.instrument_uid), RVN.instrument_uid)
        self.assertIsNone(index.uid_for("OKX.SWAP.PERPETUAL.NOPE-USDT"))
        view = index.ledger_view()
        self.assertEqual((view["events_total"], view["head_event_id"]), (5, self.ledger.events[-1]["event_id"]))

    def test_tampered_dropped_reordered_or_partial_files_are_refused(self):
        lines = self.lines()
        tampered = json.loads(lines[5]); tampered["status"] = "ACTIVE"; tampered["venue_state"] = "live2"
        rehashed = dict(tampered); rehashed["event_id"] = lc.event_id_of(rehashed)
        variants = {
            "tampered": [*lines[:5], json.dumps(tampered)],
            "tampered and rehashed breaks nothing but the evidence is still checked": None,
            "dropped": [*lines[:2], *lines[3:]],
            "reordered": [lines[0], lines[2], lines[1], *lines[3:]],
            "no header": lines[1:],
            "header changed": [json.dumps({"schema": "other", "ledger_id": "x"}), *lines[1:]],
        }
        for name, variant in variants.items():
            if variant is None:
                continue
            with self.subTest(name), self.assertRaises(lc.LifecycleLedgerError):
                lc.parse_ledger(("\n".join(variant) + "\n").encode())
        with self.assertRaises(lc.LifecycleLedgerError):
            lc.parse_ledger(self.raw[:-1])
        with self.assertRaises(lc.LifecycleLedgerError):  # an event that changes nothing is not an event
            same = json.loads(lines[5]); same.update(status="ACTIVE", venue_state="live")
            same["event_id"] = lc.event_id_of(same)
            lc.parse_ledger(("\n".join([*lines[:5], json.dumps(same)]) + "\n").encode())
        # a rehashed tamper is a valid file on its own; the append-only proof against the old file catches it.
        forged = ("\n".join([*lines[:5], json.dumps(rehashed)]) + "\n").encode()
        lc.parse_ledger(forged)
        with self.assertRaises(lc.LifecycleLedgerError):
            lc.verify_append_only(self.raw, forged)

    def test_append_only_proof(self):
        old = self.raw
        self.ledger.record(okx_all(**{"RVN-USDT-SWAP": None}), T2)
        lc.verify_append_only(old, self.ledger.raw())
        with self.assertRaises(lc.LifecycleLedgerError):
            lc.verify_append_only(self.ledger.raw(), old)

    def test_environment_opt_in(self):
        self.assertIsNone(lc.index_from_environment({}))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "ledger.jsonl")
            path.write_bytes(self.raw)
            self.assertEqual(len(lc.index_from_environment({lc.ENVIRONMENT_VARIABLE: str(path)}).events), 5)
            with self.assertRaises(FileNotFoundError):
                lc.index_from_environment({lc.ENVIRONMENT_VARIABLE: str(path) + ".missing"})
            path.write_bytes(self.raw[:-1])
            with self.assertRaises(lc.LifecycleLedgerError):
                lc.index_from_environment({lc.ENVIRONMENT_VARIABLE: str(path)})


class CliTests(unittest.TestCase):
    def run_cli(self, *args):
        from scripts.instrument_lifecycle_record import main
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = main(list(args))
        return code, json.loads(out.getvalue())

    def test_init_record_verify_and_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            d = Path(directory)
            (d / "catalog.json").write_text(json.dumps({"instruments": CATALOG}))
            (d / "c0.json").write_bytes(okx_all())
            (d / "c1.json").write_bytes(okx_all(**{"RVN-USDT-SWAP": None}))
            (d / "empty.json").write_bytes(okx([]))
            ledger = d / "ledger.jsonl"
            self.assertEqual(self.run_cli("init", "--ledger", str(ledger), "--ledger-id", "t")[0], 0)
            self.assertEqual(self.run_cli("init", "--ledger", str(ledger), "--ledger-id", "t")[0], 1)
            common = ("--ledger", str(ledger), "--catalog", str(d / "catalog.json"), "--endpoint", OKX_ENDPOINT,
                      "--catalog-revision", "18", "--source-revision", "declaration-r10")
            code, result = self.run_cli("record", *common, "--listing", f"OKX:SWAP:{d / 'c0.json'}",
                                        "--observed-utc", T0)
            self.assertEqual((code, result["appended"]), (0, 4))
            first = ledger.read_bytes()
            code, result = self.run_cli("record", *common, "--listing", f"OKX:SWAP:{d / 'c1.json'}",
                                        "--observed-utc", T1)
            self.assertEqual((code, result["appended"], result["ledger"]["events_total"]), (0, 1, 5))
            code, result = self.run_cli("record", *common, "--listing", f"OKX:SWAP:{d / 'empty.json'}",
                                        "--observed-utc", T2)
            self.assertEqual((code, result["code"]), (1, "LIFECYCLE_CAPTURE_EMPTY"))
            previous = d / "previous.jsonl"
            previous.write_bytes(first)
            code, result = self.run_cli("verify", "--ledger", str(ledger), "--previous", str(previous))
            self.assertEqual((code, result["status"]), (0, "VERIFIED"))
            code, result = self.run_cli("verify", "--ledger", str(previous), "--previous", str(ledger))
            self.assertEqual((code, result["code"]), (1, "LIFECYCLE_LEDGER_INVALID"))
            self.assertFalse(any(p.name.endswith(".tmp") for p in d.iterdir()))


def ledger_index():
    ledger = Ledger()
    ledger.record(okx_all(), T0)
    ledger.record(okx_all(**{"RVN-USDT-SWAP": "suspend"}), T1)
    ledger.record(okx_all(**{"RVN-USDT-SWAP": None}), T2)
    return lc.InstrumentLifecycleIndex(ledger.raw())


def service(index, records=(BTC, ETH, SOL)):
    registry = InstrumentRegistry()
    for rec in records:  # RVN is retired: no longer in the catalogue
        registry.register(rec, [])
    return V2QueryService(instruments=InstrumentQuery(registry), backend=MemoryMarketDataBackend(),
                          entitlements=EntitlementPolicy(()), instrument_lifecycle=index)


class QueryEndpointTests(unittest.TestCase):
    consumer_id = "lifecycle-reader"
    subject = "spiffe://qdl/paper/lifecycle-reader"

    def client(self, index, permissions=None):
        kwargs = {} if permissions is None else {"permissions": permissions}
        manifest = ConsumerManifestLoader.from_mapping(manifest_mapping(
            consumer_id=self.consumer_id, subject=self.subject, instrument_uid=BTC.instrument_uid, **kwargs))
        client = TestClient(create_v2_app(service(index), identity_service=make_identity(manifest)),
                            raise_server_exceptions=False)
        client.headers.update({"Authorization": f"Bearer {make_token(self.subject)}",
                               "X-QDL-Consumer-ID": self.consumer_id, "X-QDL-Purpose": "INTERNAL_ALPHA"})
        return client

    def test_retired_catalogue_unknown_and_paging(self):
        client = self.client(ledger_index())
        retired = client.get(f"/v2/instruments/{RVN.instrument_id}/lifecycle")
        self.assertEqual(retired.status_code, 200, retired.text)
        body = retired.json()
        self.assertEqual((body["schema"], body["in_catalog"], body["history_state"]),
                         ("qdl.instrument-lifecycle.v1", False, "RECORDED"))
        self.assertEqual((body["current"]["status"], body["current"]["venue_state"]), ("DELISTED", "ABSENT"))
        self.assertEqual([e["status"] for e in body["events"]], ["ACTIVE", "HALTED", "DELISTED"])
        self.assertNotIn("position", body["events"][0])
        self.assertIsNone(body["next_after_sequence"])
        page = client.get(f"/v2/instruments/{RVN.instrument_uid}/lifecycle", params={"limit": 2}).json()
        self.assertEqual(([e["sequence"] for e in page["events"]], page["next_after_sequence"]), ([1, 2], 2))
        rest = client.get(f"/v2/instruments/{RVN.instrument_uid}/lifecycle",
                          params={"after_sequence": 2}).json()
        self.assertEqual(([e["sequence"] for e in rest["events"]], rest["next_after_sequence"]), ([3], None))
        active = client.get(f"/v2/instruments/{BTC.instrument_uid}/lifecycle").json()
        self.assertEqual((active["in_catalog"], active["current"]["status"], len(active["events"])),
                         (True, "ACTIVE", 1))
        self.assertEqual(active["ledger"]["events_total"], 6)
        missing = client.get("/v2/instruments/OKX.SWAP.PERPETUAL.NOPE-USDT/lifecycle")
        self.assertEqual((missing.status_code, missing.json()["code"]), (404, "INSTRUMENT_NOT_FOUND"))
        self.assertEqual(client.get(f"/v2/instruments/{BTC.instrument_uid}/lifecycle",
                                    params={"limit": 501}).status_code, 400)  # typed request validation

    def test_no_observation_and_not_configured(self):
        new = record("OKX", "SWAP", "NEW-USDT", "NEW-USDT-SWAP")
        index = ledger_index()
        svc = service(index, (BTC, new))
        body = svc.lifecycle_for(new.instrument_uid)
        self.assertEqual((body["history_state"], body["current"], body["events"]), ("NO_OBSERVATION", None, []))
        body = self.client(None).get(f"/v2/instruments/{BTC.instrument_uid}/lifecycle").json()
        self.assertEqual((body["history_state"], body["current"], body["ledger"]), ("NOT_CONFIGURED", None, None))
        missing = self.client(None).get(f"/v2/instruments/{RVN.instrument_uid}/lifecycle")
        self.assertEqual(missing.status_code, 404)

    def test_requires_instruments_read(self):
        client = self.client(ledger_index(), permissions=("snapshot:read",))
        self.assertEqual(client.get(f"/v2/instruments/{BTC.instrument_uid}/lifecycle").status_code, 403)


class RealHttpEndToEndTests(unittest.TestCase):
    """uvicorn on a loopback socket, the SDK REST transport (httpx) and the SDK response model."""

    def test_sdk_reads_the_lifecycle_over_http(self):
        import uvicorn
        from qdl_sdk import InstrumentLifecycleResponse, StaticBearerCredential
        from qdl_sdk.errors import DataLayerError
        from qdl_sdk.models import Grade
        from qdl_sdk.transport import RestQueryTransport

        consumer_id, subject = "lifecycle-e2e", "spiffe://qdl/paper/lifecycle-e2e"
        manifest = ConsumerManifestLoader.from_mapping(manifest_mapping(
            consumer_id=consumer_id, subject=subject, instrument_uid=BTC.instrument_uid))
        app = create_v2_app(service(ledger_index()), identity_service=make_identity(manifest))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="off"))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
        thread.start()
        self.addCleanup(sock.close)
        self.addCleanup(thread.join, 10)
        self.addCleanup(setattr, server, "should_exit", True)
        deadline = time.monotonic() + 10
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertTrue(server.started)

        async def scenario():
            transport = RestQueryTransport(f"http://127.0.0.1:{port}",
                                           credential_provider=StaticBearerCredential(make_token(subject)))
            try:
                payload = await transport.instrument_lifecycle(RVN.instrument_id, consumer_id=consumer_id,
                                                               consumer_grade=Grade.ALPHA)
                with self.assertRaises(DataLayerError) as refused:
                    await transport.instrument_lifecycle("OKX.SWAP.PERPETUAL.NOPE-USDT", consumer_id=consumer_id,
                                                         consumer_grade=Grade.ALPHA)
                return payload, refused.exception
            finally:
                await transport.close()

        payload, refused = asyncio.run(scenario())
        view = InstrumentLifecycleResponse.model_validate(payload)
        self.assertEqual((view.in_catalog, view.current.status), (False, "DELISTED"))
        self.assertEqual([e.status for e in view.events], ["ACTIVE", "HALTED", "DELISTED"])
        self.assertEqual(view.events[1].previous_event_id, view.events[0].event_id)
        self.assertEqual(view.ledger.events_total, 6)
        self.assertEqual(refused.code, "INSTRUMENT_NOT_FOUND")
        # Tolerant reader: an additive field from a later server is ignored.
        self.assertEqual(InstrumentLifecycleResponse.model_validate({**payload, "future_field": 1}).instrument_uid,
                         RVN.instrument_uid)


if __name__ == "__main__":
    unittest.main()
