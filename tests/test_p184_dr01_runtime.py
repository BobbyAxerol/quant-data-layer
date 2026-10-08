"""Source checks are not runtime evidence; opt-in test uses real loopback HTTP."""
import json
import os
from pathlib import Path
import tempfile
import unittest
import threading
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from scripts import p184_dr01_runtime as dr


def observations():
    return {venue: {"baseline": {"status": 200, "identity_matches": True},
        "binding": {"compiled": True},
        "realm": {"status": 401, "code": "UNAUTHENTICATED"},
        "revision": {"status": 401, "code": "UNAUTHENTICATED"},
        "consumer": {"status": 403, "code": "CONSUMER_MISMATCH"},
        "principal": {"status": 401, "code": "UNAUTHENTICATED"},
        "ungranted": {"status": 403, "code": "PERMISSION_DENIED"}} for venue in dr.VENUES}


class SourceTests(unittest.TestCase):
    def test_exact_nine_ids(self):
        expected = {f"DR01.{case}@{venue}" for case in (
            "BINDING_COMPILE", "WRONG_REALM_DENIED", "WRONG_REVISION_DENIED", "WRONG_ACCOUNT_DENIED")
            for venue in ("BINANCE", "OKX")} | {"DR01.NO_GLOBAL_GRANT@ANY"}
        self.assertEqual(set(dr.CELLS), expected)
        self.assertEqual(len(dr.CELLS), 9)

    def test_strict_booleans(self):
        for bad in (1, 0, "false", [], None):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                dr.result(dr.CELLS[0], bad, {})
        with self.assertRaises(ValueError):
            dr.result(dr.CELLS[0], True, {}, observed=False)
        with self.assertRaises(ValueError):
            dr.result("invented", False, {})

    def test_provenance_format_and_placeholder_gate(self):
        for values in ((False, dr.DUMMY_SHA, dr.DUMMY_IMAGE),
                       (True, "dummy-ts-sha", "dummy-ts-image"),
                       (False, "a" * 40, "sha256:" + "b" * 64),
                       (1, dr.DUMMY_SHA, dr.DUMMY_IMAGE)):
            with self.assertRaises(ValueError):
                dr.provenance(*values)
        header = dr.provenance(True, dr.DUMMY_SHA, dr.DUMMY_IMAGE)
        self.assertIs(header["final_qualification"], False)
        supplied = dr.provenance(False, "0123456789" * 4, "sha256:" + "0123456789abcdef" * 4)
        self.assertIs(supplied["development"], False)
        self.assertIs(supplied["shipped_image_verified"], False)
        self.assertIs(supplied["mtls_verified"], False)

    def test_account_stays_open_and_binding_uses_compile_result(self):
        rows = dr.assess(observations())
        self.assertEqual(sum(row["ok"] is True for row in rows), 7)
        self.assertEqual(tuple(r["cell"] for r in rows), dr.CELLS)
        for row in rows:
            if "BINDING_COMPILE" in row["cell"]:
                self.assertIs(row["evidence"]["full_r22_roster_verified"], False)
                self.assertIs(row["evidence"]["market_data_read_observed"], False)
                self.assertEqual(row["evidence"]["binding_scope"], "ONE_BAR_1M_TEST_PROJECTION_PER_VENUE")
        for row in rows:
            if "WRONG_ACCOUNT" in row["cell"]:
                self.assertIs(row["ok"], False)
                self.assertEqual(row["outcome"], "NOT_OBSERVED")

    def test_compile_must_be_strict_positive(self):
        for bad in (False, "true", 1, None):
            obs = observations()
            obs["BINANCE"]["binding"]["compiled"] = bad
            row = next(r for r in dr.assess(obs) if r["cell"] == "DR01.BINDING_COMPILE@BINANCE")
            self.assertIs(row["ok"], False)

    def test_key_cleanup_on_probe_failure(self):
        with patch.object(dr, "_observe", side_effect=RuntimeError) as probe:
            with self.assertRaises(RuntimeError):
                dr.observe()
        self.assertFalse(Path(probe.call_args.args[0]).exists())

    def test_outage_or_generic_refusal_never_passes(self):
        for status, code in ((500, "UNAUTHENTICATED"), (403, "PERMISSION_DENIED"),
                             (401, None), (200, "UNAUTHENTICATED")):
            obs = observations()
            obs["BINANCE"]["realm"] = {"status": status, "code": code}
            row = next(r for r in dr.assess(obs) if r["cell"] == "DR01.WRONG_REALM_DENIED@BINANCE")
            self.assertIs(row["ok"], False)

    def test_positive_control_required(self):
        obs = observations()
        obs["BINANCE"]["baseline"]["identity_matches"] = False
        rows = dr.assess(obs)
        self.assertFalse(any(r["ok"] for r in rows if r["cell"].endswith("@BINANCE")))
        self.assertIs(rows[-1]["ok"], False)

    def test_both_venues_need_no_global_grant(self):
        obs = observations()
        obs["OKX"]["ungranted"] = {"status": 200}
        self.assertIs(dr.assess(obs)[-1]["ok"], False)

    def test_rehearsal_filename_required(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "final.jsonl"
            with patch.object(dr, "observe") as observe, self.assertRaises(ValueError):
                dr.run(path, development=True, source_sha=dr.DUMMY_SHA, image_digest=dr.DUMMY_IMAGE)
            observe.assert_not_called()
            self.assertFalse(path.exists())

    def test_output_is_exclusive_before_any_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "existing.rehearsal.jsonl"
            path.write_text("untouched")
            with patch.object(dr, "observe") as observe, self.assertRaises(FileExistsError):
                dr.run(path, development=True, source_sha=dr.DUMMY_SHA, image_digest=dr.DUMMY_IMAGE)
            observe.assert_not_called()
            self.assertEqual(path.read_text(), "untouched")

    def test_runtime_failure_is_nine_blockers_without_secret_messages(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "development.rehearsal.jsonl"
            with patch.object(dr, "observe", side_effect=TimeoutError("secret-token")):
                rows = dr.run(path, development=True, source_sha=dr.DUMMY_SHA, image_digest=dr.DUMMY_IMAGE)
            text = path.read_text()
            self.assertNotIn("secret-token", text)
            self.assertEqual(len(text.splitlines()), 10)
            header = json.loads(text.splitlines()[0])
            self.assertEqual(set(header), {"provenance"})
            self.assertIs(header["provenance"]["development"], True)
            self.assertEqual(header["provenance"]["schema"], "p184.surface-evidence-provenance.v1")
            self.assertEqual(header["provenance"]["source_sha"], dr.DUMMY_SHA)
            self.assertEqual(header["provenance"]["image_digest"], dr.DUMMY_IMAGE)
            self.assertTrue(all(r["ok"] is False and r["outcome"] == "NOT_OBSERVED" for r in rows))


class AccountHttpUnitTests(unittest.TestCase):
    """Scripted local HTTP mock, UNIT ONLY; never actual TS Gateway evidence."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="p184-account-unit-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.calls, self.answers = [], []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                owner.calls.append((self.command, self.path, self.headers.get("X-API-Key")))
                status, body = owner.answers.pop(0) if owner.answers else (500, {})
                self.send_response(status)
                if status == 302:
                    self.send_header("Location", "/redirect-must-not-be-followed")
                self.end_headers()
                self.wfile.write(body if isinstance(body, bytes) else json.dumps(body).encode())

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .01})
        self.thread.start()
        self.addCleanup(self.close_server)
        self.packet = {"schema": "p184.dr01-account-packet.v1", "isolated": True, "mode": "paper",
            "source_sha": dr.DUMMY_SHA, "image_digest": dr.DUMMY_IMAGE,
            "gateway_url": f"http://127.0.0.1:{self.server.server_port}", "bindings": {}}
        for venue in dr.VENUES:
            pair = {}
            for label in ("A", "B"):
                key = self.root / (venue + label + ".key")
                key.write_text("unit-only-secret-" + label)
                key.chmod(0o600)
                pair[label] = {"alpha_id": "unit-alpha-" + label,
                    "account_id": "unit-paper-" + venue + label, "api_key_file": str(key),
                    "balances": [{"currency": "USDT", "total": "1000.125", "locked": "25.125", "free": "975"}]}
            self.packet["bindings"][venue] = pair
        self.path = self.root / "account-packet.json"

    def close_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    def load(self):
        self.path.write_text(json.dumps(self.packet))
        return dr.load_account_packet(self.path, source_sha=dr.DUMMY_SHA, image_digest=dr.DUMMY_IMAGE)

    def positive(self, venue, label):
        binding = self.packet["bindings"][venue][label]
        return (200, {"status": "OK", "count": 1, "balances": [{**binding["balances"][0],
            "strategy_id": binding["alpha_id"], "account_id": binding["account_id"],
            "mode": "paper", "venue": venue}]})

    def answers_for(self, venue):
        return [self.positive(venue, "A"), self.positive(venue, "B"),
            (403, {"status": "REJECTED", "reason": "ACCOUNT_ALPHA_SCOPE_MISMATCH"}),
            (403, {"status": "REJECTED", "reason": "INVALID_API_KEY"})]

    def test_unit_http_exact_route_headers_and_independent_controls(self):
        packet = self.load()
        for venue in dr.VENUES:
            self.answers.extend(self.answers_for(venue))
            row = dr.account_probe(packet, venue)
            self.assertIs(row["ok"], True)
            self.assertEqual(row["cell"], f"DR01.WRONG_ACCOUNT_DENIED@{venue}")
            pair = packet["bindings"][venue]
            for index, (method, route, key) in enumerate(self.calls[-4:]):
                self.assertEqual(method, "GET")
                target = pair["B"] if index in (1, 2) else pair["A"]
                principal = pair["B"] if index == 1 else pair["A"]
                parsed = urlsplit(route)
                self.assertEqual(parsed.path, f"/v1/accounts/{target['account_id']}/balances")
                self.assertEqual(parse_qs(parsed.query), {"alpha_id": [principal["alpha_id"]],
                    "account_id": [target["account_id"]], "mode": ["paper"], "venue": [venue]})
                self.assertTrue(key == principal["key"] if index != 3 else key not in {pair["A"]["key"], pair["B"]["key"]})
            serialized = json.dumps(row)
            self.assertNotIn("unit-only-secret", serialized)
            self.assertNotIn(str(self.root), serialized)
            self.assertNotIn("1000.125", serialized)

    def test_unit_positive_identity_amount_and_nonempty_gates(self):
        packet = self.load()
        for mutation in ("empty", "account", "strategy", "venue", "mode", "amount", "count", "float", "duplicate"):
            with self.subTest(mutation=mutation):
                self.answers = self.answers_for("BINANCE")
                body = self.answers[0][1]
                row = body["balances"][0]
                if mutation == "empty":
                    body.update(balances=[], count=0)
                elif mutation == "count":
                    body["count"] = True
                elif mutation == "duplicate":
                    body["balances"].append(dict(row))
                    body["count"] = 2
                else:
                    field = {"account": "account_id", "strategy": "strategy_id", "amount": "total", "float": "total"}.get(mutation, mutation)
                    row[field] = 1000.125 if mutation == "float" else "wrong" if mutation not in {"amount"} else "1000.1250000000000000000001"
                self.assertIs(dr.account_probe(packet, "BINANCE")["ok"], False)

    def test_unit_wrong_reason_and_independent_invalid_key_gate(self):
        packet = self.load()
        for index, status, body in ((2, 403, {"status": "REJECTED", "reason": "INVALID_API_KEY"}),
                (2, 403, {"status": "REJECTED", "reason": "ACCOUNT_SCOPE_MISMATCH"}),
                (2, 500, {"status": "REJECTED", "reason": "ACCOUNT_ALPHA_SCOPE_MISMATCH"}),
                (3, 403, {"status": "REJECTED", "reason": "ACCOUNT_ALPHA_SCOPE_MISMATCH"}),
                (3, 200, {"status": "OK"})):
            self.answers = self.answers_for("OKX")
            self.answers[index] = status, body
            self.assertIs(dr.account_probe(packet, "OKX")["ok"], False)

    def test_unit_timeout_is_never_account_denial(self):
        packet = self.load()
        with patch.object(dr, "request_json", side_effect=TimeoutError("sensitive-message")) as request:
            row = dr.account_probe(packet, "BINANCE")
        self.assertEqual(request.call_count, 4)
        self.assertIs(row["ok"], False)
        self.assertEqual(row["outcome"], "NOT_OBSERVED")
        self.assertNotIn("sensitive-message", json.dumps(row))

    def test_unit_transport_uses_bounded_timeout(self):
        with patch.object(dr, "build_opener") as factory:
            factory.return_value.open.side_effect = TimeoutError
            with self.assertRaises(TimeoutError):
                dr.request_json(self.packet["gateway_url"], "/test", {"X-API-Key": "unit-only"})
        self.assertEqual(factory.return_value.open.call_args.kwargs["timeout"], 3)

    def test_unit_redirect_malformed_and_response_bounds(self):
        packet = self.load()
        for status, body in ((302, {}), (200, b"not-json"), (200, b"x" * (dr.MAX_RESPONSE + 1))):
            before = len(self.calls)
            self.answers = self.answers_for("BINANCE")
            self.answers[0] = status, body
            self.assertIs(dr.account_probe(packet, "BINANCE")["ok"], False)
            self.assertEqual(len(self.calls) - before, 4)

    def test_packet_provenance_isolation_origin_and_pair_fail_closed(self):
        good = deepcopy(self.packet)
        for field, value in (("isolated", 1), ("mode", "live"), ("source_sha", "a" * 40),
                ("image_digest", "sha256:" + "a" * 64), ("gateway_url", "http://example.com:1234"),
                ("gateway_url", "http://localhost:1234"), ("gateway_url", "http://127.0.0.1:1234/a"),
                ("gateway_url", "http://user:secret@127.0.0.1:1234")):
            self.packet = deepcopy(good)
            self.packet[field] = value
            with self.assertRaises(ValueError):
                self.load()
        self.packet = deepcopy(good)
        self.packet["bindings"]["BINANCE"]["B"]["alpha_id"] = self.packet["bindings"]["BINANCE"]["A"]["alpha_id"]
        with self.assertRaises(ValueError):
            self.load()
        self.assertEqual(self.calls, [])

    def test_keys_require_external_regular_0600_files(self):
        binding = self.packet["bindings"]["BINANCE"]["A"]
        path = Path(binding["api_key_file"])
        path.chmod(0o644)
        with self.assertRaises(ValueError):
            self.load()
        path.chmod(0o600)
        link = self.root / "symlink.key"
        link.symlink_to(path)
        binding["api_key_file"] = str(link)
        with self.assertRaises(OSError):
            self.load()
        binding["api_key_file"] = str(dr.ROOT / "not-an-external-key")
        with self.assertRaises(ValueError):
            self.load()
        self.assertEqual(self.calls, [])

    def test_unit_option_replaces_only_account_rows_and_never_logs_keys(self):
        self.load()
        for venue in dr.VENUES:
            self.answers.extend(self.answers_for(venue))
        output = self.root / "unit-contract.rehearsal.jsonl"
        with patch.object(dr, "observe", return_value=observations()):
            rows = dr.run(output, development=True, source_sha=dr.DUMMY_SHA,
                          image_digest=dr.DUMMY_IMAGE, account_packet=self.path)
        self.assertEqual(len(rows), 9)
        self.assertTrue(all(row["ok"] is True for row in rows))
        self.assertEqual(len(self.calls), 8)
        text = output.read_text()
        self.assertNotIn("unit-only-secret", text)
        self.assertNotIn(str(self.root), text)
        self.assertIs(json.loads(text.splitlines()[0])["provenance"]["final_qualification"], False)

    def test_invalid_packet_does_not_create_output_or_start_dl(self):
        self.packet["mode"] = "live"
        self.path.write_text(json.dumps(self.packet))
        output = self.root / "unit-contract.rehearsal.jsonl"
        with patch.object(dr, "observe") as probe, self.assertRaisesRegex(ValueError, "invalid isolated account packet"):
            dr.run(output, development=True, source_sha=dr.DUMMY_SHA,
                   image_digest=dr.DUMMY_IMAGE, account_packet=self.path)
        probe.assert_not_called()
        self.assertFalse(output.exists())
        self.assertEqual(self.calls, [])


@unittest.skipUnless(os.environ.get("P184_DR01_HTTP_TEST") == "1",
                     "requires approved isolated image with app dependencies; no runtime claim")
class ActualHttpTests(unittest.TestCase):
    def test_real_application_ephemeral_credentials(self):
        obs = dr.observe()
        rows = dr.assess(obs)
        self.assertEqual(sum(row["ok"] is True for row in rows), 7, rows)
        for venue in dr.VENUES:
            self.assertTrue(dr.denied(obs[venue]["consumer"], 403, "CONSUMER_MISMATCH"))
            self.assertTrue(dr.denied(obs[venue]["principal"], 401, "UNAUTHENTICATED"))
            self.assertIs(obs[venue]["baseline"]["identity_matches"], True)


if __name__ == "__main__":
    unittest.main()
