import copy
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from scripts import p184_dr05_channel as ch
from scripts.p184_dr05_dl_fault import frame

NS = "p184-dr05-dl-0123456789abcdef"


def receipt():
    return {"topic": NS + ".raw", "partition": 0, "offset": 0, "capture_id": "a" * 32,
            "raw_sha256": "b" * 64, "generation": 7, "broker_ack_ms": 1.5,
            "provenance": "TEST_ONLY", "acceptance": False}


def row(kind="canonical"):
    result = {"kind": kind, "topic": NS + "." + kind, "partition": 0, "offset": 2,
              "capture_id": "a" * 32}
    if kind == "quarantine":
        result["reason"] = "QUARANTINE_REASON_SEQUENCE_GAP"
    return result


def barrier(rows=None):
    return {"raw_committed_next": 1, "read_committed_eof": [NS + ".canonical", NS + ".quarantine"],
            "matched": [row()] if rows is None else rows}


class ChannelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.packet = {"namespace": NS, "slice": {"topic_id": "old-topic"}}
        self.config = {"raw_topics": [NS + ".raw"]}
        self.loader = Mock(side_effect=lambda work: (Path(work), NS, copy.deepcopy(self.packet), copy.deepcopy(self.config)))
        self.publishers, self.observers = [], []
        def publisher(packet, config):
            value = Mock()
            value.publish.return_value = receipt()
            self.publishers.append(value)
            return value
        def observer(packet, config):
            value = Mock()
            value.observe.return_value = barrier()
            self.observers.append(value)
            return value
        self.publisher_factory, self.observer_factory = publisher, observer
        self.channel = ch.Channel(self.work, loader=self.loader, publisher_factory=publisher, observer_factory=observer)
        self.addCleanup(self.channel.close)

    def call(self, op, args, timeout=1):
        return self.channel.dispatch({"op": op, "args": args, "timeout": timeout})

    def publish(self):
        return self.call("publish", {"frame": frame("OKX", True, 100, 0, 7, "1", 1000000)})

    def host_reply(self, *, ok=True, changed=True, bad_namespace=False):
        original = ch._atomic
        def write(path, request):
            self.assertEqual(set(request), {"request_id", "namespace", "venue", "fault", "timeout"})
            self.assertEqual(request["namespace"], NS)
            self.assertEqual(len(request["request_id"]), 32)
            self.assertGreater(request["timeout"], 0)
            self.assertLessEqual(request["timeout"], 1)
            self.assertIsNone(self.channel.publisher)
            self.assertIsNone(self.channel.observer)
            original(path, request)
            self.observers[0].close.assert_called_once()
            if changed:
                self.packet["slice"]["topic_id"] = "new-topic"
            if ok:
                result = {"namespace": "shared" if bad_namespace else NS,
                          "topic_id": self.packet["slice"]["topic_id"], "reset": True}
                reply = {"ok": True, "result": result}
            else:
                reply = {"ok": False, "error": "PRIVATE KEY must never be echoed"}
            original(path.parent / ("reset-" + request["request_id"] + ".json"), reply)
        return patch.object(ch, "_atomic", side_effect=write)

    def test_real_clients_are_default_factories(self):
        from scripts.p184_dr05_raw_publisher import RawPublisher
        from scripts.p184_dr05_observer import CanonicalObserver
        self.assertTrue(callable(RawPublisher))
        self.assertTrue(callable(CanonicalObserver))

    def test_publish_and_observe_use_cached_instances(self):
        r = self.publish()
        result = self.call("canonical_observe", {"receipt": r})
        self.assertTrue(result["observed"])
        self.assertFalse(result["acceptance"])
        self.assertEqual(result["records"], [row()])
        again = self.call("canonical_observe", {"receipt": r})
        self.assertEqual(again, result)
        self.observers[0].observe.assert_called_once()
        self.assertEqual(len(self.publishers), 1)
        self.assertEqual(len(self.observers), 1)

    def test_quarantine_preserves_actual_coordinates_and_reason(self):
        r = self.publish()
        self.observers[0].observe.return_value = barrier([row("quarantine")])
        self.assertEqual(self.call("canonical_observe", {"receipt": r})["records"], [row("quarantine")])

    def test_filtered_requires_actual_postcommit_barrier(self):
        r = self.publish()
        self.observers[0].observe.return_value = barrier([])
        self.assertEqual(self.call("canonical_observe", {"receipt": r})["records"], [])

    def test_unobserved_publish_cannot_be_overwritten(self):
        self.publish()
        with self.assertRaises(ValueError):
            self.publish()
        self.publishers[0].publish.assert_called_once()

    def test_publish_ambiguity_fences_all_followup_operations(self):
        self.publishers[0].publish.side_effect = TimeoutError("private diagnostic")
        with self.assertRaises(TimeoutError):
            self.publish()
        self.assertTrue(self.channel.poisoned)
        with self.assertRaisesRegex(RuntimeError, "fenced"):
            self.call("reset_case", {"venue": "OKX", "fault": "GAP_REFUSAL"})
        self.assertFalse((self.work / "control/reset-request.json").exists())

    def test_foreign_or_modified_receipt_never_reaches_observer(self):
        r = self.publish()
        changes = {"topic": "shared.raw", "partition": 1, "offset": 1, "capture_id": "c" * 32,
                   "raw_sha256": "d" * 64, "generation": 8, "provenance": "LIVE", "extra": "bad"}
        for key, value in changes.items():
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.call("canonical_observe", {"receipt": {**r, key: value}})
        self.observers[0].observe.assert_not_called()

    def test_sentinel_or_boolean_offsets_rejected(self):
        for key, value in (("offset", -1), ("offset", True), ("partition", False)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.channel._validate_receipt({**receipt(), key: value})

    def test_malformed_requests_do_not_reach_publisher(self):
        requests = [[], {}, {"op": "publish", "args": {}, "timeout": 1},
                    {"op": "other", "args": {}, "timeout": 1},
                    {"op": "reset_case", "args": {"venue": "OKX", "fault": "GAP_REFUSAL", "path": "/shared"}, "timeout": 1}]
        for request in requests:
            with self.subTest(request=request), self.assertRaises(ValueError):
                self.channel.dispatch(request)
        self.publishers[0].publish.assert_not_called()

    def test_unknown_frame_fields_refused_before_publish(self):
        f = frame("OKX", True, 100, 0, 7, "1", 1000000)
        with self.assertRaises(ValueError):
            self.call("publish", {"frame": {**f, "phase": "positive"}})
        self.publishers[0].publish.assert_not_called()

    def test_deadline_bounds_native_poll_and_commit(self):
        r = self.publish()
        observer = self.observers[0]
        consumer, offsets = observer.consumer, observer.offset_reader
        def observe(receipt, timeout):
            observer.consumer.poll(3)
            observer.offset_reader.committed(["partition"], timeout=3)
            return barrier()
        observer.observe.side_effect = observe
        self.call("canonical_observe", {"receipt": r}, timeout=.1)
        self.assertLessEqual(consumer.poll.call_args.args[0], .1)
        self.assertLessEqual(offsets.committed.call_args.kwargs["timeout"], .1)
        self.assertIs(observer.consumer, consumer)
        self.assertIs(observer.offset_reader, offsets)

    def test_invalid_barrier_or_coordinates_fences(self):
        r = self.publish()
        self.observers[0].observe.return_value = {**barrier(), "raw_committed_next": 0}
        with self.assertRaises(ValueError):
            self.call("canonical_observe", {"receipt": r})
        self.assertTrue(self.channel.poisoned)

    def test_missing_partition_not_fabricated(self):
        r = self.publish()
        record = row(); record.pop("partition")
        self.observers[0].observe.return_value = barrier([record])
        with self.assertRaises(ValueError):
            self.call("canonical_observe", {"receipt": r})

    def test_reset_atomic_ipc_reopens_clients_and_clears_receipts(self):
        old = self.publish()
        with self.host_reply():
            result = self.call("reset_case", {"venue": "OKX", "fault": "GAP_REFUSAL"})
        self.assertTrue(result["reset"])
        self.assertEqual(len(self.publishers), 2)
        self.assertEqual(len(self.observers), 2)
        self.assertFalse(self.channel.receipts)
        self.assertIsNone(self.channel.pending)
        self.assertEqual(self.channel.packet["slice"]["topic_id"], "new-topic")
        self.assertEqual(list((self.work / "control").glob("*.json")), [])
        with self.assertRaises(ValueError):
            self.call("canonical_observe", {"receipt": old})

    def test_parent_watcher_consumes_request_and_replies_on_same_ipc(self):
        from scripts import p184_dr05_reset as host
        result, errors = [], []

        def reset(work, venue, timeout):
            self.assertEqual(Path(work), self.work)
            self.assertEqual(venue, "OKX")
            self.assertGreater(timeout, 0)
            self.assertFalse((self.work / "control/reset-request.json").exists())
            self.assertIsNone(self.channel.publisher)
            self.assertIsNone(self.channel.observer)
            self.observers[0].close.assert_called_once()
            self.packet["slice"]["topic_id"] = "host-created-topic"
            return {"namespace": NS, "venue": venue, "topic_id": "host-created-topic",
                    "previous_topic_id": "old-topic", "broker_id": "unchanged-broker",
                    "reset": True, "acceptance": False}

        def client():
            try:
                result.append(self.call("reset_case", {"venue": "OKX", "fault": "GAP_REFUSAL"}, timeout=2))
            except Exception as error:
                errors.append(error)

        with patch.object(host, "_packet", side_effect=self.loader), patch.object(host, "reset_preserving_broker", side_effect=reset), patch.object(host.signal, "signal"), patch.object(host.dr, "bounded", side_effect=AssertionError("no Docker in IPC test")):
            thread = threading.Thread(target=client, daemon=True)
            thread.start()
            try:
                self.assertEqual(host.serve_host(self.work, duration=.3), 0)
            finally:
                thread.join(3)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(result[0]["broker_id"], "unchanged-broker")
        self.assertEqual(self.channel.packet["slice"]["topic_id"], "host-created-topic")
        self.assertEqual(len(self.publishers), 2)
        self.assertEqual(list((self.work / "control").glob("*.json")), [])

    def test_reset_same_uuid_refused_and_fenced(self):
        with self.host_reply(changed=False), self.assertRaisesRegex(ValueError, "fresh topic"):
            self.call("reset_case", {"venue": "OKX", "fault": "GAP_REFUSAL"})
        self.assertTrue(self.channel.poisoned)
        self.assertEqual(len(self.publishers), 1)

    def test_reset_foreign_response_refused(self):
        with self.host_reply(bad_namespace=True), self.assertRaises(ValueError):
            self.call("reset_case", {"venue": "OKX", "fault": "GAP_REFUSAL"})
        self.assertTrue(self.channel.poisoned)

    def test_reset_timeout_leaves_one_request_and_no_clients(self):
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            self.call("reset_case", {"venue": "BINANCE", "fault": "GAP_REFUSAL"}, timeout=.06)
        self.assertLess(time.monotonic() - start, .5)
        request = json.loads((self.work / "control/reset-request.json").read_text())
        self.assertEqual(request["namespace"], NS)
        self.assertEqual(len(list((self.work / "control").glob("reset-request*"))), 1)
        self.assertIsNone(self.channel.publisher)
        self.assertTrue(self.channel.poisoned)

    def test_single_channel_lock_refuses_second_owner(self):
        with self.assertRaises(BlockingIOError):
            ch.Channel(self.work, loader=self.loader, publisher_factory=self.publisher_factory, observer_factory=self.observer_factory)

    def test_stdout_is_jsonl_and_errors_do_not_echo_secrets(self):
        incoming = io.BytesIO((json.dumps({"op": "reset_case", "args": {"venue": "OKX", "fault": "GAP_REFUSAL"}, "timeout": 1}) + "\n").encode())
        outgoing = io.StringIO()
        with self.host_reply(ok=False):
            ch.serve(self.channel, incoming, outgoing)
        reply = json.loads(outgoing.getvalue())
        self.assertFalse(reply["ok"])
        self.assertNotIn("PRIVATE KEY", outgoing.getvalue())
        self.assertEqual(len(outgoing.getvalue().splitlines()), 1)

    def test_oversize_request_fences_without_parsing_or_reading_rest(self):
        incoming = io.BytesIO(b"x" * (ch.MAX_BYTES + 10) + b"\n")
        outgoing = io.StringIO()
        ch.serve(self.channel, incoming, outgoing)
        self.assertFalse(json.loads(outgoing.getvalue())["ok"])
        self.assertTrue(self.channel.poisoned)
        self.assertEqual(incoming.tell(), ch.MAX_BYTES + 1)


class ProtocolTests(unittest.TestCase):
    def test_duplicate_keys_and_nonfinite_json_refused(self):
        for value in ('{"op":1,"op":2}', '{"timeout":NaN}', '{"timeout":Infinity}'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ch.decode(value)

    def test_invalid_deadlines(self):
        for value in (True, 0, -1, 61, float("inf"), float("nan"), "2"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ch._timeout(value)

    def test_atomic_file_private_and_no_temporary_leftovers(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "request.json"
            ch._atomic(path, {"request_id": "a" * 32})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(ch._read(path), {"request_id": "a" * 32})
            self.assertEqual(list(Path(tmp).iterdir()), [path])

    def test_response_symlink_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "response.json"
            target = Path(tmp) / "target.json"
            target.write_text('{"ok":true}')
            path.symlink_to(target)
            with self.assertRaises(OSError):
                ch._read(path)

    def test_fifo_response_refused_without_blocking(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "response.json"
            os.mkfifo(path)
            with self.assertRaises(ValueError):
                ch._read(path)

    def test_load_requires_attestation_and_topic_uuid(self):
        work = Path("/work")
        packet = {"namespace": NS, "slice": {"topic_id": "actual-topic"}}
        with patch("scripts.p184_dr05_case._packet", return_value=(work, NS, packet, {})):
            with patch.dict(os.environ, {"P184_DR05_ISOLATED_NAMESPACE": "foreign"}), self.assertRaises(ValueError):
                ch.load_packet(work)
            with patch.dict(os.environ, {"P184_DR05_ISOLATED_NAMESPACE": NS}):
                self.assertEqual(ch.load_packet(work)[1], NS)
                packet["slice"].pop("topic_id")
                with self.assertRaises(ValueError):
                    ch.load_packet(work)


if __name__ == "__main__":
    unittest.main()
