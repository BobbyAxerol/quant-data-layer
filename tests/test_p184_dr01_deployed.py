"""Offline runner regression. No deployed endpoint/credential is accessed."""
import argparse
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts import p184_dr01_deployed as dr


def fixture():
    req = dict(instrument_uid="e49b54ae-c23d-5351-9e64-47934aac28f8", feed="QUOTE",
               interval=None, source_policy_id="crypto_primary_v2", consumer_grade="ALPHA")
    binding = dict(consumer_id="alpha.deployment", release_revision=27,
                   universal_manifest_sha256="c" * 64, products=[req])
    binding["binding_sha256"] = dr.digest(dr.canonical(binding))
    packet = dict(schema="p184.dr01.deployed-packet.v1", venue="OKX", binding=binding,
                  access_consumer_id="alpha.okx.paper.stable", subject="spiffe://qdl/paper/alpha-okx-stable",
                  jwt_revision=15, wrong_revision=14, baseline=req, forbidden=dict(req, feed="BOOK_DELTA"),
                  entitlements={"wildcard_paths": [], "manifest_revision": 15})
    env = dict(DATA_LAYER_V2_ACCESS_CONSUMER_ID=packet["access_consumer_id"],
               DATA_LAYER_V2_CONSUMER_ID=binding["consumer_id"], DATA_LAYER_V2_JWT_SUBJECT=packet["subject"],
               DATA_LAYER_V2_JWT_ENVIRONMENT="paper", DATA_LAYER_V2_JWT_MANIFEST_REVISION="15",
               DATA_LAYER_V2_RELEASE_MANIFEST_SHA256="c" * 64, DATA_LAYER_V2_QUERY_URL="https://query_kn:8200")
    return packet, binding, env


def observations(packet):
    return dict(baseline=dict(status=200, instrument_uid=packet["baseline"]["instrument_uid"], feed="QUOTE"),
                realm=dict(status=401, code="UNAUTHENTICATED"), revision=dict(status=401, code="UNAUTHENTICATED"), forbidden=dict(status=403, code="PERMISSION_DENIED"))


class DeployedTests(unittest.TestCase):
    def test_binding_identity_and_revisions(self):
        packet, binding, env = fixture()
        dr.validate(packet, binding, env)
        for key in env:
            bad = dict(env, **{key: "bad"})
            with self.subTest(key=key), self.assertRaises(ValueError):
                dr.validate(packet, binding, bad)

    def test_compiled_binding_diff_and_rehashed_mutation(self):
        packet, binding, env = fixture()
        for rehash in (False, True):
            changed = dict(binding, release_revision=22)
            if rehash:
                changed["binding_sha256"] = dr.digest(dr.canonical({k: v for k, v in changed.items() if k != "binding_sha256"}))
            with self.assertRaises(ValueError):
                dr.validate(packet, changed, env)

    def test_no_redirect_credentials_or_query_in_origin(self):
        packet, binding, env = fixture()
        for url in ("http://query_kn", "https://user:password@query_kn", "https://query_kn/?key=x", "https://query_kn/v2"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                dr.validate(packet, binding, dict(env, DATA_LAYER_V2_QUERY_URL=url))
        self.assertIsNone(dr.NoRedirect().redirect_request(None, None, 302, "", {}, "https://other"))

    def test_wildcard_and_baseline_outside_binding(self):
        packet, binding, env = fixture()
        packet["entitlements"]["wildcard_paths"] = ["$.instrument"]
        with self.assertRaises(ValueError):
            dr.validate(packet, binding, env)
        packet["entitlements"]["wildcard_paths"] = []
        packet["baseline"] = dict(packet["baseline"], feed="TRADE")
        with self.assertRaises(ValueError):
            dr.validate(packet, binding, env)
        self.assertEqual(dr.wildcard_paths({"a": ["uid", "*"]}), ["$.a[1]"])

    def test_baseline_failure_never_negative_pass(self):
        packet, _, _ = fixture()
        for mutation in (dict(status=503), dict(instrument_uid="wrong"), dict(feed="TRADE")):
            obs = observations(packet)
            obs["baseline"].update(mutation)
            self.assertFalse(any(r["ok"] for r in dr.evaluate(packet, obs)))

    def test_exact_wire_denials_and_no_account_cell(self):
        packet, _, _ = fixture()
        obs = observations(packet)
        rows = dr.evaluate(packet, obs)
        self.assertEqual(len(rows), 4)
        self.assertTrue(all(r["ok"] and r["level"] == "RUNTIME" for r in rows))
        self.assertFalse(any("ACCOUNT" in r["cell"] for r in rows))
        for key, status in (("realm", 403), ("revision", 429), ("forbidden", 404)):
            bad = deepcopy(obs); bad[key]["status"] = status
            self.assertFalse(all(r["ok"] for r in dr.evaluate(packet, bad)))
        obs["forbidden"]["code"] = "RATE_LIMITED"
        self.assertFalse(dr.evaluate(packet, obs)[-1]["ok"])

    def test_get_only_bounded_no_body_logging(self):
        packet, _, _ = fixture()
        response = Mock()
        response.__enter__ = Mock(return_value=response); response.__exit__ = Mock(return_value=False)
        response.code = 200
        response.read.return_value = json.dumps({"data": {"instrument_uid": packet["baseline"]["instrument_uid"],
                                                          "feed": "QUOTE", "private": "not-logged"}}).encode()
        opener = Mock(); opener.open.return_value = response
        result = dr.get(opener, "https://query_kn", dr.route(packet["baseline"]), {"Authorization": "Bearer secret"})
        self.assertEqual(opener.open.call_args.args[0].method, "GET")
        self.assertEqual(opener.open.call_args.kwargs["timeout"], 10)
        self.assertNotIn("not-logged", json.dumps(result))
        response.read.return_value = b"x" * (dr.MAX_BYTES + 1)
        with self.assertRaises(ValueError):
            dr.get(opener, "https://query_kn", "/v2/anything", {})

    def test_probe_stdin_shape_mtls_and_exact_claim_changes(self):
        packet, binding, env = fixture()
        env.update(DATA_LAYER_V2_TLS_CA_FILE="/test/ca", DATA_LAYER_V2_TLS_CERT_FILE="/test/cert",
                   DATA_LAYER_V2_TLS_KEY_FILE="/test/key")
        calls = []
        async def signed(env, **changes):
            calls.append(changes)
            return "SECRET_TEST_TOKEN"
        wire = observations(packet)
        with patch.dict(dr.os.environ, env, clear=True), patch.object(dr, "read_json", side_effect=[packet, binding]), \
             patch.object(dr.ssl, "create_default_context") as tls, patch.object(dr, "HTTPSHandler"), \
             patch.object(dr, "build_opener"), patch.object(dr, "token", side_effect=signed), \
             patch.object(dr, "get", side_effect=[wire[k] for k in ("baseline", "realm", "revision", "forbidden")]) as get, \
             patch.object(dr.time, "sleep"), patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(dr.probe(argparse.Namespace(packet="/packet")), 0)
            tls.assert_called_once_with(cafile="/test/ca")
            tls.return_value.load_cert_chain.assert_called_once_with("/test/cert", "/test/key")
            self.assertEqual(calls, [{}, {"environment": "live"}, {"consumer_manifest_revision": 14}, {}])
            self.assertEqual(get.call_count, 4)
            self.assertNotIn("SECRET_TEST_TOKEN", output.getvalue())
            self.assertEqual(len(output.getvalue().splitlines()), 4)

    def test_record_exact_two_venue_union_exclusive_and_ci(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = []
            for venue in dr.VENUES:
                packet, _, _ = fixture(); packet["venue"] = venue
                rows = dr.evaluate(packet, observations(packet))
                path = Path(tmp) / venue; path.write_text("\n".join(json.dumps(x) for x in rows)); paths.append(str(path))
            args = argparse.Namespace(rows=paths, output=str(Path(tmp) / "out"), ci_confirmed=False,
                                      source_sha="0123456789" * 4, image_digest="sha256:" + "0123456789abcdef" * 4)
            with self.assertRaises(ValueError): dr.record(args)
            self.assertFalse(Path(args.output).exists())
            args.ci_confirmed = True
            self.assertEqual(dr.record(args), 0)
            result = [json.loads(line) for line in Path(args.output).read_text().splitlines()]
            self.assertEqual(len(result), 8)
            self.assertEqual(result[0]["provenance"]["source_sha"], args.source_sha)
            self.assertEqual(sum(r.get("cell") == "DR01.NO_GLOBAL_GRANT@ANY" for r in result), 1)
            with self.assertRaises(FileExistsError): dr.record(args)
            args.rows = [paths[0], paths[0]]
            with self.assertRaises(ValueError): dr.record(args)


def write_realm(directory, venue="OKX", revision=15, bundle_revision=None, extra=()):
    """A realm dir shaped like the deployed reader-public: realm-named manifest files plus bundle.json."""
    consumer = f"alpha.{venue.lower()}.paper.stable"
    doc = {"apiVersion": "qdl/v2", "kind": "DataRequirement",
           "metadata": {"id": consumer, "subject": "spiffe://qdl/paper/x", "environment": "paper", "revision": revision},
           "spec": {"requirements": []}}
    (Path(directory) / f"{consumer}-r{revision}.yaml").write_text(json.dumps(doc))
    other = dict(doc, metadata=dict(doc["metadata"], id=f"alpha.{venue.lower()}.live.stable", environment="live",
                                    revision=1))
    (Path(directory) / "live.yaml").write_text(json.dumps(other))
    for name, body in extra:
        (Path(directory) / name).write_text(json.dumps(body))
    bundle = {"environment": "paper", "manifests": [
        {"consumer_id": consumer, "environment": "paper", "manifest_sha256": dr.manifest_sha(doc),
         "manifest_revision": revision if bundle_revision is None else bundle_revision}]}
    (Path(directory) / "bundle.json").write_text(json.dumps(bundle))
    return doc


class RevisionParameterTests(unittest.TestCase):
    def test_wrong_revision_defaults_to_previous_and_is_validated(self):
        self.assertEqual(dr.revisions(15), (15, 14))
        self.assertEqual(dr.revisions(15, 9), (15, 9))
        for bad in ((1, None), (15, 15), (0, None), ("15", None), (15, 0), (True, None)):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                dr.revisions(*bad)

    def test_deployed_realm_revision_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = write_realm(tmp)
            path, raw, loaded, bundle_sha = dr.realm_manifest(tmp, "OKX", 15)
            self.assertEqual(path.name, "alpha.okx.paper.stable-r15.yaml")
            self.assertEqual(loaded, doc)
            self.assertEqual(bundle_sha, dr.digest((Path(tmp) / "bundle.json").read_bytes()))

    def test_refuses_when_realm_revision_disagrees_with_declared(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_realm(tmp, revision=15)
            with self.assertRaisesRegex(ValueError, "differs from the declared"):
                dr.realm_manifest(tmp, "OKX", 14)
        with tempfile.TemporaryDirectory() as tmp:
            write_realm(tmp, revision=15, bundle_revision=14)
            with self.assertRaisesRegex(ValueError, "bundle does not serve"):
                dr.realm_manifest(tmp, "OKX", 15)
        with tempfile.TemporaryDirectory() as tmp:
            doc = write_realm(tmp)
            bundle = json.loads((Path(tmp) / "bundle.json").read_text())
            bundle["manifests"][0]["manifest_sha256"] = "0" * 64
            (Path(tmp) / "bundle.json").write_text(json.dumps(bundle))
            with self.assertRaisesRegex(ValueError, "bundle does not serve"):
                dr.realm_manifest(tmp, "OKX", 15)
        with tempfile.TemporaryDirectory() as tmp:
            doc = write_realm(tmp)
            write_realm(tmp, extra=[("dup.yaml", dict(doc, metadata=dict(doc["metadata"], revision=14)))])
            with self.assertRaisesRegex(ValueError, "exactly one"):
                dr.realm_manifest(tmp, "OKX", 15)
        with tempfile.TemporaryDirectory() as tmp:
            write_realm(tmp, venue="BINANCE")
            with self.assertRaisesRegex(ValueError, "exactly one"):
                dr.realm_manifest(tmp, "OKX", 15)

    def test_packet_revision_drives_env_check_and_wrong_revision(self):
        packet, binding, env = fixture()
        for mutation in (dict(jwt_revision=14), dict(wrong_revision=15), dict(wrong_revision=None),
                         dict(entitlements={"wildcard_paths": [], "manifest_revision": 14})):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                dr.validate(dict(packet, **mutation), binding, env)
        with self.assertRaises(ValueError):
            dr.validate(packet, binding, dict(env, DATA_LAYER_V2_JWT_MANIFEST_REVISION="14"))
        rows = dr.evaluate(packet, observations(packet))
        self.assertTrue(all(r["evidence"]["jwt_revision"] == 15 and r["evidence"]["wrong_revision"] == 14
                            for r in rows))

    def test_prepare_cli_requires_deployed_manifest_source(self):
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit):
            dr.main(["prepare", "--root", "/r", "--inventory", "/i", "--deployment", "d", "--venue", "OKX",
                     "--output", "/o"])


if __name__ == "__main__":
    unittest.main()
