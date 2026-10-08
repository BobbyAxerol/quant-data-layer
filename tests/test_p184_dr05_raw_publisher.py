import copy
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.p184_dr05_dl_fault import frame
from scripts.p184_dr05_raw_publisher import RawPublisher, raw_envelope

NS = "p184-dr05-dl-0123456789abcdef"


def binding(venue):
    return dict(provider=venue, venue=venue, market="USDM" if venue == "BINANCE" else "SWAP",
                product_type="PERPETUAL", native_symbol="ETHUSDT" if venue == "BINANCE" else "ETH-USDT-SWAP",
                native_channel="ethusdt@depth@100ms" if venue == "BINANCE" else "books",
                source_id="test-" + venue, instrument_catalog_revision=10)


class Producer:
    def __init__(self, config):
        self.config, self.calls, self.failure = config, [], None
        self.offset, self.partition, self.topic_override = 0, 0, None
        self.deliver = True

    def produce(self, topic, **kwargs):
        self.calls.append((topic, kwargs))
        self.callback = kwargs["on_delivery"]

    def poll(self, timeout):
        if self.deliver:
            self.deliver = False
            self.callback(self.failure, SimpleNamespace(topic=lambda: self.topic_override or self.calls[-1][0],
                partition=lambda: self.partition, offset=lambda: self.offset))


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.packet = {"namespace": NS, "bootstrap": "localhost:9092"}
        self.config = {"raw_topics": [NS + ".raw"], "core": {"allow_test_provenance": True,
            "bindings": [binding(v) for v in ("BINANCE", "OKX")]}}
        self.frame = frame("BINANCE", True, 100, 0, 7, "1", 123000000)
        self.addCleanup(patch.stopall)
        patch.dict("os.environ", {"P184_DR05_ISOLATED_NAMESPACE": NS}).start()
        patch("confluent_kafka.Producer", Producer).start()

    def test_envelope_both_venues_and_generations(self):
        from qdl.raw.envelope import validate_raw_envelope
        for venue in ("BINANCE", "OKX"):
            for gen in (7, 8):
                value = raw_envelope(binding(venue), frame(venue, True, 100, 0, gen, "1", 123000000), NS)
                validate_raw_envelope(value)
                self.assertTrue(value.test_provenance)
                self.assertEqual(value.connection_generation, gen)
                self.assertEqual(value.source_session_id, NS + "." + venue)
                self.assertEqual(value.received_at_ns, 123000000)
                self.assertIsInstance(json.loads(value.raw_frame_bytes), dict)

    def test_bad_provenance_identity_transport_rejected(self):
        for key, value in (("test_provenance", False), ("provenance", "LIVE"),
                           ("native_symbol", "BTCUSDT"), ("transport", "OTHER"),
                           ("adapter_version", "production")):
            changed = {**self.frame, key: value}
            with self.subTest(key=key), self.assertRaises(ValueError):
                raw_envelope(binding("BINANCE"), changed, NS)

    def test_zero_offset_valid_and_no_acceptance(self):
        pub = RawPublisher(self.packet, self.config)
        receipt = pub.publish(self.frame)
        self.assertEqual(receipt["offset"], 0)
        self.assertFalse(receipt["acceptance"])
        self.assertEqual(len(receipt["raw_sha256"]), 64)
        self.assertEqual(len(pub.producer.calls), 1)

    def test_negative_offset_wrong_partition_topic_fences(self):
        for attr, value in (("offset", -1001), ("partition", 1), ("topic_override", "shared")):
            pub = RawPublisher(self.packet, self.config)
            setattr(pub.producer, attr, value)
            with self.subTest(attr=attr), self.assertRaises(RuntimeError):
                pub.publish(self.frame)
            with self.assertRaises(RuntimeError):
                pub.publish(self.frame)
            self.assertEqual(len(pub.producer.calls), 1)

    def test_delivery_error_fences(self):
        pub = RawPublisher(self.packet, self.config)
        pub.producer.failure = SimpleNamespace(code=lambda: 123)
        with self.assertRaises(RuntimeError):
            pub.publish(self.frame)
        self.assertTrue(pub.failed)

    def test_timeout_no_retry(self):
        pub = RawPublisher(self.packet, self.config)
        pub.producer.deliver = False
        with patch("scripts.p184_dr05_raw_publisher.time.monotonic", side_effect=[10, 11]), self.assertRaises(TimeoutError):
            pub.publish(self.frame, timeout=.5)
        self.assertTrue(pub.failed)
        self.assertEqual(len(pub.producer.calls), 1)

    def test_scope_checks(self):
        for key, value in (("bootstrap", "production:9092"), ("namespace", "production")):
            with self.assertRaises(ValueError):
                RawPublisher({**self.packet, key: value}, self.config)
        with patch.dict("os.environ", {"P184_DR05_ISOLATED_NAMESPACE": "wrong"}), self.assertRaises(ValueError):
            RawPublisher(self.packet, self.config)
        for modify in (lambda c: c.update(raw_topics=["shared"]),
                       lambda c: c["core"].update(allow_test_provenance=False),
                       lambda c: c["core"]["bindings"].pop()):
            cfg = copy.deepcopy(self.config); modify(cfg)
            with self.assertRaises(ValueError):
                RawPublisher(self.packet, cfg)

    def test_bad_deadline_never_sends(self):
        pub = RawPublisher(self.packet, self.config)
        for value in (0, -1, 6, True, float("nan")):
            with self.assertRaises(ValueError):
                pub.publish(self.frame, timeout=value)
        self.assertFalse(pub.producer.calls)


if __name__ == "__main__":
    unittest.main()
