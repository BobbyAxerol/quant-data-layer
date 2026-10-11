"""Replica configuration stays explicit without touching any live consumer."""
import unittest
from unittest.mock import patch
from scripts.benchmark_kn_ts_consumer import query_replicas, _dist


class ProbeReplicaTests(unittest.TestCase):
    def test_existing_packets_keep_default(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(query_replicas(), ("query_v2_1", "query_v2_2"))

    def test_kn_names_and_fqdn(self):
        for value in ("query_kn_1,query_kn_2", "query-1.test,query-2.test"):
            with self.subTest(value=value), patch.dict("os.environ", KN_QUERY_REPLICAS=value):
                self.assertEqual(query_replicas(), tuple(value.split(",")))

    def test_duplicate_missing_url_and_injection_rejected(self):
        for value in ("", "query_kn_1", "a,a", "a,b,c", "https://a,b", "a,b/path", "a, b"):
            with self.subTest(value=value), patch.dict("os.environ", KN_QUERY_REPLICAS=value):
                with self.assertRaises(ValueError):
                    query_replicas()

    def test_sparse_samples_do_not_claim_p99(self):
        self.assertIsNone(_dist([1, 2, 3])["p99"])
        self.assertEqual(_dist(list(range(100)))["p99"], 98)
