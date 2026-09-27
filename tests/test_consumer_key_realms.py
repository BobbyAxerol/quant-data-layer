"""Public market storage is shared; workload keys/subjects/realms are not."""
import unittest
from dataclasses import replace

from qdl.consumer.manifest import ConsumerManifestLoader, ConsumerManifestRegistry
from qdl.security.data_plane import DataPlaneAccessError, DataPlaneIdentityService, DataPlaneSecurityConfig
from tests.phase7_support import TEST_AUDIENCE, TEST_ISSUER, make_token, manifest_mapping


class ConsumerKeyRealmTests(unittest.TestCase):
    def setUp(self):
        self.realms = ("paper", "sandbox", "live")
        self.manifests = {}
        for realm in self.realms:
            mapping = manifest_mapping(consumer_id="ts." + realm,
                subject="spiffe://qdl/" + realm + "/ts", instrument_uid="instrument-test")
            mapping["metadata"]["environment"] = realm
            mapping["spec"]["execution_dependency"] = "ALLOWED"
            self.manifests[realm] = ConsumerManifestLoader.from_mapping(mapping)
        self.config = DataPlaneSecurityConfig(environment="paper", issuer=TEST_ISSUER,
            audience=TEST_AUDIENCE, algorithms=("HS256",),
            keys_by_id={r: ("test-only-" + r).encode() * 8 for r in self.realms},
            subjects_by_key_id={r: m.subject for r, m in self.manifests.items()},
            environments_by_key_id={r: r for r in self.realms})
        self.identity = DataPlaneIdentityService(self.config, ConsumerManifestRegistry(tuple(self.manifests.values())))

    def token(self, realm, **kwargs):
        defaults = dict(environment=realm, key_id=realm, secret=self.config.keys_by_id[realm])
        defaults.update(kwargs)
        return make_token(self.manifests[realm].subject, **defaults)

    def test_each_mode_authenticates_without_changing_storage_realm(self):
        for realm, manifest in self.manifests.items():
            with self.subTest(realm=realm):
                access = self.identity.authenticate(self.token(realm), consumer_id=manifest.consumer_id)
                self.assertEqual(access.principal.environment, realm)
                self.assertEqual(access.manifest, manifest)
                self.assertEqual(self.config.environment, "paper")

    def test_key_cannot_assert_another_realm_or_consumer(self):
        for realm in self.realms:
            for other in set(self.realms) - {realm}:
                with self.subTest(realm=realm, other=other):
                    with self.assertRaises(DataPlaneAccessError):
                        self.identity.authenticate(self.token(realm, environment=other), consumer_id="ts." + other)
                    with self.assertRaises(DataPlaneAccessError):
                        self.identity.authenticate(self.token(realm), consumer_id="ts." + other)
                    with self.assertRaises(DataPlaneAccessError):
                        self.identity.authenticate(self.token(realm, key_id=other), consumer_id="ts." + realm)

    def test_subject_and_revision_stay_bound(self):
        for realm in self.realms:
            with self.assertRaises(DataPlaneAccessError):
                self.identity.authenticate(self.token(realm, manifest_revision=2), consumer_id="ts." + realm)
            forged = make_token("spiffe://qdl/" + realm + "/other", environment=realm,
                key_id=realm, secret=self.config.keys_by_id[realm])
            with self.assertRaises(DataPlaneAccessError):
                self.identity.authenticate(forged, consumer_id="ts." + realm)

    def test_unconfigured_legacy_single_realm_stays_closed(self):
        legacy = DataPlaneIdentityService(replace(self.config, environments_by_key_id=None),
            ConsumerManifestRegistry(tuple(self.manifests.values())))
        legacy.authenticate(self.token("paper"), consumer_id="ts.paper")
        for realm in ("sandbox", "live"):
            with self.assertRaises(DataPlaneAccessError):
                legacy.authenticate(self.token(realm), consumer_id="ts." + realm)

    def test_mapping_is_exact_and_has_no_wildcard(self):
        for value in ({}, {"live": "live"}, {r: "*" for r in self.realms}, [], "live"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                replace(self.config, environments_by_key_id=value)
        with self.assertRaises(ValueError):
            replace(self.config, subjects_by_key_id=None)

    def test_live_and_sandbox_do_not_accept_paper_only_contract(self):
        for realm in ("sandbox", "live"):
            with self.assertRaises(ValueError):
                replace(self.manifests[realm], execution_dependency="PAPER_ONLY")


class ConsumerRealmCompilerTests(unittest.TestCase):
    def test_stable_scopes_preserved_for_both_additional_modes(self):
        from pathlib import Path
        import yaml
        from scripts.compile_consumer_realms import compile_realm
        root = Path(__file__).resolve().parents[1]
        for name in ("trading-system-paper", "alpha-binance-paper", "alpha-okx-paper"):
            payload = yaml.safe_load((root / "consumers/stable" / (name + ".yaml")).read_text())
            original = ConsumerManifestLoader.from_mapping(payload)
            for realm in ("sandbox", "live"):
                result = ConsumerManifestLoader.from_mapping(compile_realm(payload, realm))
                self.assertEqual(result.requirements, tuple(r for r in original.requirements
                    if r.source_policy_id in {"crypto_primary_v2", "crypto_liquid_v2"}))
                self.assertFalse(any(r.source_policy_id == "vn_primary_v2" for r in result.requirements))
                if name == "trading-system-paper":
                    self.assertEqual(len(result.requirements), 60)
                self.assertEqual(result.quotas, original.quotas)
                self.assertEqual(result.allowed_permissions, original.allowed_permissions)
                self.assertEqual(result.allowed_purposes, original.allowed_purposes)
                self.assertNotEqual(result.subject, original.subject)
                self.assertNotEqual(result.consumer_id, original.consumer_id)
                self.assertEqual(result.environment, realm)
                self.assertEqual(payload["metadata"]["environment"], "paper")
                self.assertEqual(payload["spec"]["execution_dependency"], original.execution_dependency)
                self.assertEqual(result.execution_dependency,
                    "ALLOWED" if original.execution_dependency == "PAPER_ONLY" else "FORBIDDEN")

    def test_other_consumer_is_not_implicitly_promoted(self):
        from scripts.compile_consumer_realms import compile_realm
        payload = manifest_mapping(consumer_id="unapproved.paper.stable",
            subject="spiffe://qdl/paper/unapproved", instrument_uid="instrument-test")
        for policy in ("FORBIDDEN", "PAPER_ONLY", "ALLOWED"):
            payload["spec"]["execution_dependency"] = policy
            with self.assertRaises(ValueError):
                compile_realm(payload, "live")
