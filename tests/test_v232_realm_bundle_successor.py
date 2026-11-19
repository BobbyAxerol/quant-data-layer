"""DL v2.3.2 P2: the bundle successor adds only `delivery_semantics: ON_CHANGE` on QUOTE bindings."""
import copy
import unittest

from scripts import kn_gateway_bundle
from scripts.v232_realm_bundle_successor import successor


def binding(binding_id, feed):
    return {"binding_id": binding_id, "feed": feed, "venue": "OKX", "market": "SWAP", "instrument_uid": "u",
            "interval": None, "source_policy_id": "p", "physical_key": "k", "source_id": "s",
            "stale_after_ms": 5000, "product_key": "lpk1"}


def bundle(bindings):
    body = {"schema": kn_gateway_bundle.SCHEMA, "environment": "paper", "manifests": [{"consumer_id": "c"}],
            "catalog": {"catalog_revision": 16, "source_policy_revision": 1, "canonical_stream": "s",
                        "bindings": bindings}}
    body["sha256"] = kn_gateway_bundle.bundle_sha256(body)
    return body


class SuccessorTests(unittest.TestCase):
    def setUp(self):
        self.base = bundle([binding("q", "QUOTE"), binding("t", "TRADE")])
        self.catalog = copy.deepcopy(self.base["catalog"])
        self.catalog["bindings"][0]["delivery_semantics"] = "ON_CHANGE"

    def test_adds_only_the_field_and_keeps_manifests(self):
        body, report = successor(self.base, self.catalog)
        self.assertEqual(report["marked_on_change"], 1)
        self.assertTrue(report["manifests_unchanged"])
        self.assertEqual(body["sha256"], kn_gateway_bundle.bundle_sha256(body))
        self.assertNotEqual(body["sha256"], self.base["sha256"])

    def test_refuses_any_other_difference(self):
        for mutate in (lambda c: c["bindings"][1].update(stale_after_ms=1),
                       lambda c: c["bindings"][1].update(delivery_semantics="ON_CHANGE"),
                       lambda c: c["bindings"].pop(),
                       lambda c: c.update(catalog_revision=17)):
            catalog = copy.deepcopy(self.catalog)
            mutate(catalog)
            with self.assertRaises(ValueError):
                successor(self.base, catalog)

    def test_refuses_a_base_whose_hash_does_not_verify(self):
        base = copy.deepcopy(self.base)
        base["manifests"].append({"consumer_id": "x"})
        with self.assertRaises(ValueError):
            successor(base, self.catalog)


if __name__ == "__main__":
    unittest.main()
