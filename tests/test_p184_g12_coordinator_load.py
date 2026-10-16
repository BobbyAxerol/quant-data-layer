"""Coordinator-load harness: the broker load lines it parses (Kafka 4.2 wording), production-shaped broker
caps, a namespace-only network and no host port, auto rebalance off (moves are the harness's own)."""

from __future__ import annotations

import unittest

from scripts import p184_g12_coordinator_load as harness

NS = "p184-g12-dl-0123456789abcdef"
GROUP_LINE = ("[2026-10-15 12:46:16,563] INFO [GroupCoordinator id=1] Finished loading of metadata from "
              "__consumer_offsets-0 with epoch 1 in 3109ms where 1ms was spent in the scheduler. Loaded 61466 "
              "records which total to 7277457 bytes. (org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime)")
TXN_LINE = ("[2026-10-15 12:46:15,363] INFO [Transaction State Manager 1]: Finished loading 3 transaction metadata "
            "from __transaction_state-0 in 1996 milliseconds, of which 2 milliseconds was spent in the scheduler. "
            "(kafka.coordinator.transaction.TransactionStateManager)")


class Parsing(unittest.TestCase):
    def test_load_lines(self):
        text = "noise\n" + GROUP_LINE + "\n" + TXN_LINE + "\n"
        group = harness.load_lines(text, "__consumer_offsets")
        self.assertEqual(len(group), 1)
        self.assertEqual((group[0]["ms"], group[0]["records"], group[0]["bytes"]), ("3109", "61466", "7277457"))
        txn = harness.load_lines(text, "__transaction_state")
        self.assertEqual((txn[0]["ms"], txn[0]["ids"], txn[0]["tp"]), ("1996", "3", "__transaction_state-0"))


class Topology(unittest.TestCase):
    def test_broker_shape(self):
        props = harness.broker_properties(2)
        for line in ("auto.leader.rebalance.enable=false", "offsets.topic.num.partitions=1",
                     "transaction.state.log.num.partitions=1", "log.cleaner.io.max.bytes.per.second=4194304",
                     "transaction.state.log.min.isr=2", "node.id=2"):
            self.assertIn(line + "\n", props)
        argv = harness.broker_argv(NS, "/w", 2, "1:1")
        self.assertEqual(argv[argv.index("--network") + 1], f"container:{NS}-net")
        self.assertEqual(argv[argv.index("--cpus") + 1], "1.75")
        self.assertEqual(argv[argv.index("--memory") + 1], "1536m")
        self.assertIn("--rm", argv)
        self.assertNotIn("-p", argv)
        self.assertIn(harness.IMAGES["kafka"], argv)

    def test_namespace_and_work_guards(self):
        with self.assertRaises(ValueError):
            harness.up("/tmp/x", "qdl_v2_stable_candidate")
        with self.assertRaises(ValueError):
            harness.up(harness.ROOT / "work", NS)
        with self.assertRaises(ValueError):
            harness.move(NS, "/w", "md.latest.v2", 2)


if __name__ == "__main__":
    unittest.main()
