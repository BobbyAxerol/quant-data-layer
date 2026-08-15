from __future__ import annotations

import pathlib
import unittest

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


class Phase80BrokerSubstrateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.topology = yaml.safe_load(
            (ROOT / "config/phase8/broker-topology.yaml").read_text()
        )
        self.compose = yaml.safe_load(
            (ROOT / "docker-compose.phase8-kafka.yml").read_text()
        )

    def test_topology_is_replicated_fail_closed_and_shadow_only(self) -> None:
        durability = self.topology["durability"]
        self.assertEqual(durability["broker_count"], 3)
        self.assertEqual(durability["replication_factor"], 3)
        self.assertEqual(durability["min_in_sync_replicas"], 2)
        self.assertEqual(durability["producer_acks"], "all")
        self.assertTrue(durability["idempotent_producer"])
        self.assertFalse(durability["unclean_leader_election"])
        self.assertEqual(self.topology["security"]["transport"], "mutual_tls")
        self.assertFalse(
            self.topology["security"]["allow_everyone_if_no_acl_found"]
        )
        self.assertEqual(self.topology["authority"]["mode"], "RUST_SHADOW")
        self.assertFalse(self.topology["authority"]["public_write_allowed"])
        self.assertTrue(self.topology["authority"]["v1_authoritative"])

    def test_topic_contract_is_complete_and_transport_internal(self) -> None:
        topics = {item["name"]: item for item in self.topology["topics"]}
        self.assertEqual(
            set(topics),
            {
                "qdl.phase8.raw.binance.usdm.trade.v1",
                "qdl.phase8.canonical.trade.v2",
                "qdl.phase8.quality.v2",
                "qdl.phase8.control.authority.v1",
                "qdl.phase8.quarantine.binance.trade.v1",
                "qdl.phase8.audit.v1",
            },
        )
        self.assertTrue(all(item["partitions"] == 3 for item in topics.values()))
        self.assertTrue(all(item["partition_key"] for item in topics.values()))

        services = self.compose["services"]
        self.assertEqual(
            {name for name in services if name.startswith("kafka")},
            {"kafka1", "kafka2", "kafka3"},
        )
        self.assertTrue(self.compose["networks"]["phase8_shadow"]["internal"])
        for service in services.values():
            self.assertNotIn("ports", service)

    def test_brokers_are_bounded_pinned_and_have_independent_state(self) -> None:
        services = self.compose["services"]
        volumes = self.compose["volumes"]
        for index in range(1, 4):
            name = f"kafka{index}"
            service = services[name]
            self.assertIn("@sha256:", service["image"])
            self.assertEqual(service["environment"]["KAFKA_NODE_ID"], index)
            self.assertEqual(service["environment"]["KAFKA_MIN_INSYNC_REPLICAS"], 2)
            self.assertEqual(service["environment"]["KAFKA_SSL_CLIENT_AUTH"], "required")
            self.assertEqual(service["mem_limit"], "512m")
            self.assertIn(f"{name}_data", volumes)

        redis_service = services["phase8_redis"]
        self.assertTrue(redis_service["read_only"])
        self.assertEqual(redis_service["mem_limit"], "64m")
        self.assertIn("noeviction", redis_service["command"])

    def test_observability_contract_covers_failure_and_recovery(self) -> None:
        metrics = set(self.topology["observability"]["metrics"])
        alerts = set(self.topology["observability"]["alerts"])
        self.assertTrue(
            {
                "broker_ack_latency_seconds",
                "broker_produce_failures_total",
                "broker_partition_under_replicated",
                "broker_consumer_lag",
                "broker_disk_bytes",
                "local_spool_bytes",
                "replay_events_total",
            }.issubset(metrics)
        )
        self.assertIn("phase8_min_isr_unavailable", alerts)
        self.assertIn("phase8_spool_quota_breach", alerts)

        rules = yaml.safe_load(
            (ROOT / "config/observability/phase8-alerts.yaml").read_text()
        )
        self.assertFalse(
            set(rules["labels_allowed"]) & set(rules["labels_forbidden"])
        )
        names = {item["name"] for item in rules["alerts"]}
        self.assertTrue(
            {
                "Phase8BrokerAckLatencyHigh",
                "Phase8ProduceFailure",
                "Phase8ConsumerLagHigh",
                "Phase8SpoolPressure",
                "Phase8BrokerDiskPressure",
                "Phase8LeaderChangeStorm",
                "Phase8ReplayThroughputStalled",
            }.issubset(names)
        )
        collector = yaml.safe_load(
            (ROOT / "config/observability/phase8-otel-collector.yaml").read_text()
        )
        self.assertIn("memory_limiter", collector["processors"])
        self.assertEqual(
            collector["service"]["pipelines"]["metrics"]["receivers"], ["otlp"]
        )


if __name__ == "__main__":
    unittest.main()
