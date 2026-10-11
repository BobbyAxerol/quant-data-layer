"""Kafka UUIDs use standard base64, including '/', without identity rewriting."""
import unittest
from types import SimpleNamespace
from qdl.runtime.kn_query_backend import parse_placeholder, source_placeholder


class KafkaTopicCoordinateTests(unittest.TestCase):
    def test_librdkafka_uuid_roundtrip(self):
        from confluent_kafka import Uuid
        topic_id = str(Uuid(9223372036854775807, 9223372036854775807))
        self.assertIn("/", topic_id)
        view = SimpleNamespace(boundary=SimpleNamespace(topic_id=topic_id, partition=0, offset=0))
        self.assertEqual(parse_placeholder(source_placeholder(view)), (topic_id, 0, 0))

    def test_legacy_and_urlsafe_coordinates_unchanged(self):
        for value in ("topic", "topic_a-b", "a+b/c=="):
            self.assertEqual(parse_placeholder(f"kn3-source:{value}:12:345"), (value, 12, 345))

    def test_delimiters_whitespace_and_negative_offset_refused(self):
        for value in ("kn3-source:a:b:0:0", "kn3-source:a\n:0:0", "kn3-source:a:0:-1001",
                      "kn3-source::0:0", "kn3-source:a:0:0\n"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_placeholder(value)


if __name__ == "__main__":
    unittest.main()
