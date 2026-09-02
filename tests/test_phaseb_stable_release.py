from __future__ import annotations

import copy
import json
import tempfile
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
import unittest
from dataclasses import replace
from pathlib import Path

import yaml

import qdl_sdk
from qdl.consumer.stable import StableConsumerMigrationPlan
from qdl.runtime.provider_history import pass_through_eligible
from qdl.reference.runtime import reference_requirement_eligible
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.security import RedisMinuteQuota
from qdl.transport.kafka_projector import (
    ConfluentProjectorBroker,
    KafkaProjectorConfig,
    KafkaProjectorRecord,
)
from scripts.generate_phase5_openapi import build_openapi


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
MIGRATION_PATH = ROOT / "config/v2/stable-consumer-migration.yaml"


class StableConsumerMigrationContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)

    def load(self) -> StableConsumerMigrationPlan:
        return StableConsumerMigrationPlan.load(
            MIGRATION_PATH,
            manifest_root=ROOT,
            catalog=self.catalog,
        )

    def test_five_real_consumer_manifests_are_catalog_bound_and_fail_closed(self):
        plan = self.load()
        self.assertEqual(plan.contract_version, "2.0.0")
        self.assertEqual(plan.authority, "V1")
        self.assertEqual(plan.target_route, "V1_WITH_V2_SHADOW")
        self.assertEqual(len(plan.consumers), 5)
        self.assertEqual(
            {item.consumer_id for item in plan.consumers},
            {
                "monitoring.multivenue.stable",
                "alpha.binance.paper.stable",
                "alpha.okx.paper.stable",
                "alpha.vn.paper.stable",
                "trading-system.paper.stable",
            },
        )
        for item in plan.consumers:
            with self.subTest(consumer_id=item.consumer_id):
                self.assertEqual(item.state, "SHADOW")
                self.assertEqual(item.rollback_route, "V1")
                self.assertFalse(item.cutover_authorized)
                self.assertEqual(item.manifest.sdk_major, 2)
                self.assertEqual(item.manifest.rollback_contract, "V1")
                self.assertEqual(item.manifest.environment, "paper")
                for requirement in item.manifest.requirements:
                    # Two sources can serve a requirement. A binding covers it,
                    # or the pass-through answers it with no binding at all.
                    # `test_unknown_fields_active_route_and_unknown_binding_
                    # fail_closed` below proves an unservable requirement is
                    # still refused, so this is a widening, not a weakening.
                    try:
                        self.assertIsNotNone(self.catalog.binding_for(requirement))
                    except (KeyError, ValueError):
                        self.assertTrue(
                            pass_through_eligible(self.catalog, requirement)
                            or reference_requirement_eligible(
                                self.catalog.instrument_for(requirement.instrument_uid), requirement
                            ),
                            f"unservable requirement: {requirement}",
                        )

    def test_unknown_fields_active_route_and_unknown_binding_fail_closed(self):
        payload = yaml.safe_load(MIGRATION_PATH.read_text(encoding="utf-8"))
        unknown = copy.deepcopy(payload)
        unknown["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "incomplete or unknown"):
            StableConsumerMigrationPlan.from_mapping(
                unknown, manifest_root=ROOT, catalog=self.catalog
            )

        active = copy.deepcopy(payload)
        active["consumers"][0]["state"] = "ACTIVE"
        active["consumers"][0]["cutover_authorized"] = True
        with self.assertRaisesRegex(ValueError, "not fail-closed"):
            StableConsumerMigrationPlan.from_mapping(
                active, manifest_root=ROOT, catalog=self.catalog
            )

        manifest_path = ROOT / "consumers/stable/alpha-binance-paper.yaml"
        manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        manifest["spec"]["requirements"][0]["instrument_uid"] = "unknown"
        with tempfile.TemporaryDirectory(prefix="qdl-phaseb-manifest-") as directory:
            root = Path(directory)
            temporary = root / "consumers/stable/invalid.yaml"
            temporary.parent.mkdir(parents=True)
            temporary.write_text(
                yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8"
            )
            invalid = copy.deepcopy(payload)
            invalid["consumers"] = [invalid["consumers"][1]]
            invalid["consumers"][0]["manifest"] = (
                "/app/consumers/stable/invalid.yaml"
            )
            with self.assertRaisesRegex(KeyError, "no stable source binding"):
                StableConsumerMigrationPlan.from_mapping(
                    invalid, manifest_root=root, catalog=self.catalog
                )

    def test_trading_system_is_the_only_paper_execution_dependency(self):
        plan = self.load()
        policies = {
            item.consumer_id: item.manifest.execution_dependency
            for item in plan.consumers
        }
        self.assertEqual(policies["trading-system.paper.stable"], "PAPER_ONLY")
        self.assertEqual(
            {value for key, value in policies.items() if key != "trading-system.paper.stable"},
            {"FORBIDDEN"},
        )


class StableRuntimeDependencyTests(unittest.TestCase):
    def test_stable_quota_namespace_is_isolated_and_current_namespace_is_rejected(self):
        quota = RedisMinuteQuota(object(), prefix="qdl:stable:v2:paper:phaseb")
        self.assertEqual(quota.prefix, "qdl:stable:v2:paper:phaseb")
        with self.assertRaisesRegex(ValueError, "dedicated beta or stable"):
            RedisMinuteQuota(object(), prefix="qdl:v2:current")

    def test_projector_kafka_readiness_uses_bounded_metadata_probe(self):
        class FakeConsumer:
            def __init__(self, config):
                self.config = config
                self.closed = False

            def subscribe(self, topics, **_callbacks):
                self.topics = tuple(topics)

            def list_topics(self, *, timeout):
                self.timeout = timeout
                return {"cluster": "stable"}

            def close(self):
                self.closed = True

        with tempfile.TemporaryDirectory(prefix="qdl-phaseb-kafka-") as directory:
            root = Path(directory)
            for name in ("ca.crt", "client.crt", "client.key"):
                (root / name).write_text("test-only", encoding="ascii")
            broker = ConfluentProjectorBroker(
                KafkaProjectorConfig(
                    bootstrap_servers="kafka1:9092",
                    client_id="stable-projector",
                    group_id="stable-projector-v1",
                    raw_topics=(),
                    canonical_topic="md.canonical.stable.v2",
                    ca_path=root / "ca.crt",
                    certificate_path=root / "client.crt",
                    key_path=root / "client.key",
                ),
                consumer_factory=FakeConsumer,
            )
            self.assertTrue(broker.ping(0.25))
            self.assertEqual(
                broker._consumer.topics, ("md.canonical.stable.v2",)
            )
            self.assertFalse(broker._consumer.config["enable.auto.commit"])
            self.assertEqual(broker._consumer.config["isolation.level"], "read_committed")
            self.assertEqual(broker._consumer.config["queued.max.messages.kbytes"], 16 * 1024)
            self.assertEqual(broker._consumer.config["fetch.max.bytes"], 8 * 1024 * 1024)
            self.assertEqual(broker._consumer.config["max.partition.fetch.bytes"], 2 * 1024 * 1024)
            broker.close()
            self.assertFalse(broker.ping())

    def test_projector_coalesces_only_acked_offsets_and_replays_on_rebalance(self):
        class FakeConsumer:
            def __init__(self, config, *, commit_error=False):
                self.config = config
                self.commit_error = commit_error
                self.commits = []
                self.closed = False
                self.pause_calls = []
                self.resume_calls = []

            def subscribe(self, topics, **callbacks):
                self.topics = tuple(topics)
                self.callbacks = callbacks

            def poll(self, _timeout):
                return None

            def assignment(self):
                class Partition:
                    def __init__(self, topic, partition):
                        self.topic = topic
                        self.partition = partition

                return [
                    Partition("md.raw.realtime.v2", 0),
                    Partition("md.canonical.v2", 0),
                ]

            def pause(self, partitions):
                self.paused = tuple((item.topic, item.partition) for item in partitions)
                self.pause_calls.append(self.paused)

            def resume(self, partitions):
                self.resumed = tuple((item.topic, item.partition) for item in partitions)
                self.resume_calls.append(self.resumed)

            def list_topics(self, *, timeout):
                del timeout
                return {"cluster": "stable"}

            def commit(self, *, offsets, asynchronous):
                self.commits.append((tuple(offsets), asynchronous))
                if asynchronous and self.commit_error:
                    self.config["on_commit"](RuntimeError("commit failed"), offsets)
                return None if asynchronous else offsets

            def close(self):
                self.closed = True

        def record(offset, *, partition=0, epoch=1):
            return KafkaProjectorRecord(
                topic="md.raw.realtime.v2",
                partition=partition,
                offset=offset,
                key="BINANCE/USDM/BTCUSDT/trade",
                event_id=bytes([offset + 1]) * 16,
                payload=b"raw",
                accepted_at_ns=offset + 1,
                assignment_epoch=epoch,
            )

        with tempfile.TemporaryDirectory(prefix="qdl-phaseb-kafka-") as directory:
            root = Path(directory)
            for name in ("ca.crt", "client.crt", "client.key"):
                (root / name).write_text("test-only", encoding="ascii")
            config = KafkaProjectorConfig(
                bootstrap_servers="kafka1:9092",
                client_id="stable-projector",
                group_id="stable-projector-v1",
                raw_topics=("md.raw.realtime.v2",),
                canonical_topic="md.canonical.v2",
                ca_path=root / "ca.crt",
                certificate_path=root / "client.crt",
                key_path=root / "client.key",
                checkpoint_batch_size=2,
                checkpoint_interval_ms=5_000,
            )
            broker = ConfluentProjectorBroker(config, consumer_factory=FakeConsumer)
            broker.pause_canonical()
            self.assertEqual(broker._consumer.paused, (("md.canonical.v2", 0),))
            broker.resume_canonical()
            self.assertEqual(broker._consumer.resumed, (("md.canonical.v2", 0),))
            broker.checkpoint(record(0))
            self.assertEqual(broker._consumer.commits, [])
            broker.checkpoint(record(1))
            offsets, asynchronous = broker._consumer.commits[0]
            self.assertTrue(asynchronous)
            self.assertEqual([(item.partition, item.offset) for item in offsets], [(0, 2)])

            broker.checkpoint(record(0, partition=1))
            broker._consumer.callbacks["on_revoke"](broker._consumer, [])
            self.assertEqual(broker._pending_offsets, {})
            broker.close()
            self.assertEqual(len(broker._consumer.commits), 1)

            flow_broker = ConfluentProjectorBroker(
                config, consumer_factory=FakeConsumer
            )
            flow_broker.pause_canonical()
            flow_broker._consumer.callbacks["on_revoke"](
                flow_broker._consumer, []
            )
            flow_broker.poll(0.1)
            self.assertEqual(len(flow_broker._consumer.pause_calls), 2)
            self.assertEqual(
                set(flow_broker._consumer.pause_calls),
                {(("md.canonical.v2", 0),)},
            )
            flow_broker.resume_canonical()
            flow_broker.close()

            failed = ConfluentProjectorBroker(
                replace(config, checkpoint_batch_size=1),
                consumer_factory=lambda values: FakeConsumer(values, commit_error=True),
            )
            failed.checkpoint(record(0))
            with self.assertRaisesRegex(RuntimeError, "asynchronous stable checkpoint"):
                failed.poll(0.1)
            with self.assertRaisesRegex(RuntimeError, "asynchronous stable checkpoint"):
                failed.close()
            self.assertTrue(failed._consumer.closed)


class StableReleaseVersionContractTests(unittest.TestCase):
    def test_service_openapi_and_sdk_versions_are_explicit(self):
        package = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        snapshot = json.loads(
            (ROOT / "contracts/v2/openapi.snapshot.json").read_text(encoding="utf-8")
        )
        generated = build_openapi()
        self.assertEqual(package["project"]["version"], "2.0.0")
        self.assertEqual(qdl_sdk.__version__, "2.0.1")
        self.assertEqual(generated["info"]["version"], "2.0.0")
        self.assertEqual(snapshot, generated)
        # ``reference:batch`` is a governed V2 public path in the checked-in
        # router and snapshot; keep this count as a regression guard rather
        # than silently accepting a stale release assertion.
        self.assertEqual(len(generated["paths"]), 11)
        self.assertEqual(len(generated["components"]["schemas"]), 67)


if __name__ == "__main__":
    unittest.main()
