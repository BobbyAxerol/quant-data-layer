"""P18.4 D3 window B renderer: only the revision and the named quotas may change, upward only."""
from pathlib import Path
import unittest

import yaml

from scripts.p184_render_quota_revision import render

ROOT = Path(__file__).resolve().parents[1]


class RenderQuotaRevisionTests(unittest.TestCase):
    def setUp(self):
        self.manifest = yaml.safe_load((ROOT / "consumers/stable/alpha-binance-paper.yaml").read_text())

    def test_only_revision_and_named_quotas_change(self):
        out = render(self.manifest, {"requests_per_minute": 4950, "max_streams": 300})
        self.assertEqual(out["metadata"]["revision"], self.manifest["metadata"]["revision"] + 1)
        self.assertEqual((out["spec"]["quotas"]["requests_per_minute"], out["spec"]["quotas"]["max_streams"]), (4950, 300))
        self.assertEqual(out["spec"]["requirements"], self.manifest["spec"]["requirements"])
        self.assertEqual(out["spec"]["quotas"]["max_warmup_rows"], self.manifest["spec"]["quotas"]["max_warmup_rows"])

    def test_lowering_other_keys_or_nothing_is_refused(self):
        for bad in ({}, {"requests_per_minute": 1}, {"max_batch_items": 99}, {"max_streams": "300"}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                render(self.manifest, bad)


if __name__ == "__main__":
    unittest.main()
