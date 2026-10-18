"""A1 (owner 2026-10-18) on the host: the mounted verifier inside the PROFILE client image, and the host-side probe of
the DEPLOYED Query image (E7: the 092255Z defect, an import of a Query constant the 2.2.2 client image lacks, showed only
in the image the driver really runs).

Host-runnable (stdlib + docker): the driver's docker_command builds the exact client container (mounts, PYTHONPATH,
inner configuration with the host-resolved allowance, image), only the network is replaced by `none` and the bootstrap
by an in-image check that imports the driver, runs its real _inside_config() and judges 8 h + 14 s / + 60 s / + 61 s
OKX funding series with the mounted verifier. The server side of the same ages (real V2QueryService) is
tests/test_p184_a1_reference_bound.py, run in the deployed image. Skipped only when docker or the image is absent.
"""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def fake_profile(root: Path, driver):
    (root / "runtime").mkdir()
    files = {}
    for name in ("ca.crt", "client.crt", "client.key", "private.key"):
        files[name] = root / name
        files[name].write_text(name)
    return driver.validate_profile({
        "image": "qdl-v2-python:test", "network": "qdl_test", "runtime_dir": str(root / "runtime"),
        "queries": ["https://query_v2_1:8200", "https://query_v2_2:8200"],
        "stream_targets": ["stream_v2_active:8210", "stream_v2_passive:8210"],
        "query_containers": ["qdl_v2_stable_candidate-query_v2_1-1", "qdl_v2_stable_candidate-query_v2_2-1"],
        "sdk_from_source": True,
        "identities": [{"id": "alpha.binance.paper.stable",
                        "tls": {"ca_file": str(files["ca.crt"]), "cert_file": str(files["client.crt"]),
                                "key_file": str(files["client.key"])},
                        "jwt": {"private_key_file": str(files["private.key"]), "key_id": "k"}}]})


# The verifier/driver code exactly as the driver mounts it, run inside the PROFILE client image (E7: the 092255Z
# defect only showed there). --network none: nothing leaves the container.
CLIENT_IMAGE = "qdl-v2-python:2.2.2-d6d2637"
DEPLOYED_QUERY_IMAGE = "qdl-v2-python:2.2.4-31a5155"
IN_IMAGE = r"""
import json, sys
from pathlib import Path
from types import SimpleNamespace
import phase3_consumer_load_acceptance as driver
from qdl.certification import reference_l2_acceptance as v
out = {"configured_before": v.configured_query_funding_allowance_ms()}
try:
    import qdl.query.service as service
    out["image_query_has_constant"] = hasattr(service, "_FUNDING_SETTLEMENT_PUBLICATION_GRACE_MS")
except Exception as error:
    out["image_query_has_constant"] = type(error).__name__
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
NOW = 1_800_000_000_000_000_000
catalog = StableSourceCatalog.load(Path("/app/config/v2/stable-source-bindings.yaml"))
acquisition = StableAcquisitionPlan.load(Path("/app/config/v2/stable-acquisition-bindings.yaml"), catalog=catalog)
scope = v.build_reference_l2_acceptance_scope(Path("/app/consumers/stable/reference-l2-stable.yaml"), catalog=catalog,
                                              acquisition=acquisition, now_ns=NOW)
product = next(p for p in scope.references if p.requirement.feed.value == "FUNDING_RATE" and p.venue == "OKX"
               and p.sdk_requirement.max_freshness_ms == 28_800_000)
def item(observed):
    field = SimpleNamespace(name="funding_rate", unit="DIMENSIONLESS_RATE",
                            value=SimpleNamespace(source_text="0", coefficient="0", scale=0))
    obs = SimpleNamespace(instrument_uid=product.instrument_uid, product=product.sdk_requirement.product,
                          observed_at_ns=observed, fields=[field])
    data = SimpleNamespace(instrument_uid=product.instrument_uid, product=product.sdk_requirement.product,
                           received_at_ns=NOW, observations=[obs], cache_hit=True, coalesced=False)
    return SimpleNamespace(instrument_uid=product.instrument_uid, product=product.sdk_requirement.product,
                           status="OK", problem=None, data=data)
def accepts(age_ms, which=None):
    try:
        v.reference_quality(which or product, item(NOW - age_ms * 1_000_000), observed_at_ns=NOW)
    except ValueError as error:
        if "governed freshness bound" in str(error):
            return False
        return "ERROR:" + str(error)
    return True
out["unconfigured"] = str(accepts(28_814_000))  # "ERROR:<reason>" when the verifier refuses to judge
config = driver._inside_config()  # the real client path: validates and configures from QDL_PHASE3_LOAD_CONFIG
out["configured"] = v.configured_query_funding_allowance_ms()
out["ages"] = {name: accepts(age) for name, age in (("before", 28_700_000), ("8h+14s", 28_814_000),
                                                    ("8h+60s", 28_860_000), ("8h+61s", 28_861_000))}
import dataclasses
latest = dataclasses.replace(product, sdk_requirement=product.sdk_requirement.model_copy(
    update={"start_time_ns": None, "end_time_ns": None}))
out["latest_ages"] = {name: accepts(age, latest) for name, age in (("before", 28_700_000), ("8h+14s", 28_814_000))}
timing = driver.reference_timing([product], SimpleNamespace(results=[item(NOW - 28_814_000 * 1_000_000)]),
                                 sent_ns=NOW, now_ns=NOW)
out["timing_bound_ms"] = timing[0]["effective_bound_ms"]
print("A1-IN-IMAGE " + json.dumps(out, sort_keys=True))
"""


def _image_present(image: str) -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "image", "inspect", image], capture_output=True, timeout=30).returncode == 0


@unittest.skipUnless(_image_present(CLIENT_IMAGE), f"{CLIENT_IMAGE} not present")
class A1InProfileClientImageTests(unittest.TestCase):
    """The driver's mounts and inner config, its client image, the verifier code: --network none."""

    def run_in(self, image: str, *, allowance: int) -> dict:
        from scripts import phase3_consumer_load_acceptance as driver
        with tempfile.TemporaryDirectory() as temporary:
            profile = fake_profile(Path(temporary), driver)
            command, _ = driver.docker_command(profile, name="qdl-phase3-a1-test-" + uuid.uuid4().hex[:8],
                                               image_id=image, mode="load", sessions=5, duration_seconds=60,
                                               query_funding_allowance_ms=allowance)
            command[command.index("--network") + 1] = "none"
            # The client's own entrypoint (bootstrap.sh -> --inside) is replaced by the checks above; every mount,
            # the env (PYTHONPATH, inner config) and the image are the driver's.
            tail = command.index("/bin/sh")
            command = command[:tail] + ["python", "-B", "-c", IN_IMAGE]
            result = subprocess.run(command, capture_output=True, text=True, timeout=180)
        line = next((x for x in result.stdout.splitlines() if x.startswith("A1-IN-IMAGE ")), None)
        self.assertIsNotNone(line, f"rc={result.returncode} stderr={result.stderr[-2000:]}")
        return json.loads(line.split(" ", 1)[1])

    def test_mounted_verifier_runs_in_the_profile_image_with_the_deployed_bound(self):
        out = self.run_in(CLIENT_IMAGE, allowance=60_000)
        self.assertIs(out["image_query_has_constant"], False)  # the 092255Z condition is reproduced ...
        self.assertIsNone(out["configured_before"])
        self.assertIn("not configured", out["unconfigured"])    # ... fail closed before configuration ...
        self.assertEqual(out["configured"], 60_000)
        self.assertEqual(out["ages"], {"before": True, "8h+14s": True, "8h+60s": True,
                                       "8h+61s": False})  # ... same bound as Query (history)
        self.assertEqual(out["latest_ages"], {"before": True, "8h+14s": False})  # latest: no allowance, distinct
        self.assertEqual(out["timing_bound_ms"], 28_800_000 + 60_000)



@unittest.skipUnless(_image_present(DEPLOYED_QUERY_IMAGE), f"{DEPLOYED_QUERY_IMAGE} not present")
class A1DeployedQueryProbeTests(unittest.TestCase):
    def test_the_host_probe_reads_60s_from_the_deployed_query_image_and_none_from_2_2_2(self):
        from scripts import phase3_consumer_load_acceptance as driver
        deployed = driver._query_image_allowance(DEPLOYED_QUERY_IMAGE)
        self.assertEqual(deployed, {"allowance_ms": 60_000, "applied_in_reference_problem": True})
        if _image_present(CLIENT_IMAGE):
            old = driver._query_image_allowance(CLIENT_IMAGE)
            self.assertEqual(old, {"allowance_ms": None, "applied_in_reference_problem": False})
            with self.assertRaises(RuntimeError):
                driver.resolve_query_funding_allowance(["q"], image_of=lambda name: CLIENT_IMAGE)


class A1CampaignInputTests(unittest.TestCase):
    """The allowance as versioned campaign input: schema, digest, int type and the running Query images."""

    def setUp(self):
        from scripts import phase3_consumer_load_acceptance as driver
        self.driver = driver
        image = "sha256:" + "3" * 64
        self.images = {"q1": image, "q2": image}
        self.resolved = {"allowance_ms": 60_000, "query_images": self.images, "basis": "unit",
                         "per_image": {image: {"allowance_ms": 60_000, "applied_in_reference_problem": True}}}

    def write(self, root: Path, document) -> Path:
        path = root / "allowance.json"
        path.write_text(json.dumps(document))
        return path

    def test_valid_input_is_accepted_and_verified_against_the_running_images(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = self.write(Path(temporary), self.driver.query_allowance_document(self.resolved, now=1.0))
            out = self.driver.load_query_allowance(path, ["q1", "q2"], image_of=self.images.get)
        self.assertEqual(out["allowance_ms"], 60_000)
        self.assertEqual(out["verified_against_running_images"], self.images)

    def test_every_inconsistency_refuses_before_the_client_starts(self):
        good = self.driver.query_allowance_document(self.resolved, now=1.0)
        tampered = {**good, "allowance_ms": 120_000}
        retyped_body = {k: v for k, v in good.items() if k != "body_sha256"} | {"allowance_ms": "60000"}
        retyped = {**retyped_body, "body_sha256": self.driver._sha256(
            json.dumps(retyped_body, sort_keys=True, separators=(",", ":")).encode())}
        cases = {"schema": ({**good, "schema": "x"}, self.images.get),
                 "digest (edited value)": (tampered, self.images.get),
                 "non-int allowance": (retyped, self.images.get),
                 "query redeployed since": (good, {"q1": "sha256:" + "4" * 64, "q2": self.images["q2"]}.get)}
        for name, (document, image_of) in cases.items():
            with self.subTest(case=name), tempfile.TemporaryDirectory() as temporary:
                path = self.write(Path(temporary), document)
                with self.assertRaises(RuntimeError):
                    self.driver.load_query_allowance(path, ["q1", "q2"], image_of=image_of)


if __name__ == "__main__":
    unittest.main()
