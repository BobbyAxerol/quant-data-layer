from __future__ import annotations

import unittest

from scripts.phase5_api_replica_load import run


class Phase5ApiReplicaLoadTests(unittest.IsolatedAsyncioTestCase):
    async def test_replicas_serve_concurrently_without_owning_ingestion(self):
        result = await run(replicas=4, requests=100, concurrency=20)
        self.assertEqual(result["status"], "MEASURED")
        self.assertEqual(result["venue_connection_attempts"], 0)
        self.assertEqual(result["live_ingestion_owners"], 0)
        self.assertEqual(result["requests"], 100)


if __name__ == "__main__":
    unittest.main()
