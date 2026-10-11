#!/usr/bin/env python3
"""DR05 JSONL channel inside the owned broker namespace; no Docker access.

Host watcher owns reset. IPC is control/reset-request.json ->
control/reset-<request_id>.json. Run --channel /work with the isolated namespace
attestation. stdout contains protocol replies only; failures never echo inputs.
"""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.p184_dr05_dl_fault import FAULTS, SCOPES, VENUES, scope_set

MAX_BYTES = 4 * 1024 * 1024
MAX_RECEIPTS = 64
FRAME_FIELDS = {"venue", "native_symbol", "connection_generation", "sequence", "previous",
                "snapshot", "transport", "received_at_ns", "test_provenance", "provenance",
                "adapter_version", "payload"}
RECEIPT_FIELDS = {"topic", "partition", "offset", "raw_sha256", "capture_id", "generation",
                  "broker_ack_ms", "provenance", "acceptance"}


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def decode(value):
    def invalid(_value):
        raise ValueError("non-finite JSON number")
    return json.loads(value, object_pairs_hook=_pairs, parse_constant=invalid)


def _remaining(deadline):
    value = deadline - time.monotonic()
    if value <= 0:
        raise TimeoutError("channel deadline")
    return value


def _timeout(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 < value <= 60:
        raise ValueError("timeout must be in (0, 60]")
    return value


def _read(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ValueError("regular control response required")
        data = handle.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("oversize control response")
    return decode(data)


def _atomic(path, document):
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    data = json.dumps(document, allow_nan=False, separators=(",", ":")).encode()
    if len(data) > MAX_BYTES:
        raise ValueError("oversize control request")
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_packet(work):
    from scripts.p184_dr05_case import _packet
    work, ns, packet, config = _packet(work)
    if os.environ.get("P184_DR05_ISOLATED_NAMESPACE") != ns:
        raise ValueError("isolated namespace attestation required")
    topic = packet.get("slice", {}).get("topic_id")
    if not isinstance(topic, str) or not topic or len(topic) > 128:
        raise ValueError("broker-assigned canonical topic UUID required")
    return work, ns, packet, config


class Channel:
    def __init__(self, work, *, loader=load_packet, publisher_factory=None, observer_factory=None):
        from scripts.p184_dr05_raw_publisher import RawPublisher
        from scripts.p184_dr05_observer import CanonicalObserver
        self.loader = loader
        self.publisher_factory = publisher_factory or RawPublisher
        self.observer_factory = observer_factory or CanonicalObserver
        self.work, self.ns, self.packet, self.config = loader(work)
        self.publisher = self.observer = None
        self.receipts, self.observations = {}, {}
        self.pending = None
        self.poisoned = False
        self.control = self.work / "control"
        if self.control.is_symlink():
            raise ValueError("control directory may not be a symlink")
        self.control.mkdir(mode=0o700, exist_ok=True)
        fd = os.open(self.control / "channel.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        self.lock = os.fdopen(fd, "a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if (self.control / "reset-request.json").exists():
                raise ValueError("unresolved reset request; host reconciliation required")
            self._open_clients()
        except BaseException:
            self.lock.close()
            raise

    def _open_clients(self):
        self.observer = self.observer_factory(self.packet, self.config)
        try:
            self.publisher = self.publisher_factory(self.packet, self.config)
        except BaseException:
            self.observer.close()
            self.observer = None
            raise

    def _close_clients(self):
        observer, publisher = self.observer, self.publisher
        self.observer = self.publisher = None
        try:
            if observer is not None:
                observer.close()
        finally:
            if publisher is not None:
                # RawPublisher has no close method. Acknowledged single-flight
                # publication leaves no queued records; never flush/retry here.
                del publisher

    def close(self):
        try:
            self._close_clients()
        finally:
            self.lock.close()

    def reset(self, venue, fault, deadline):
        if venue not in SCOPES or venue not in scope_set(self.work) or fault not in FAULTS:
            raise ValueError("unknown cell")
        request_id = uuid.uuid4().hex
        request = self.control / "reset-request.json"
        response = self.control / ("reset-" + request_id + ".json")
        old_topic = self.packet["slice"]["topic_id"]
        self._close_clients()
        try:
            if request.exists():
                raise ValueError("one pending reset only")
            _atomic(request, {"request_id": request_id, "namespace": self.ns,
                "venue": venue, "fault": fault, "timeout": _remaining(deadline)})
            while True:
                _remaining(deadline)
                try:
                    reply = _read(response)
                    break
                except FileNotFoundError:
                    time.sleep(min(.05, _remaining(deadline)))
            if not isinstance(reply, dict) or type(reply.get("ok")) is not bool:
                raise ValueError("invalid host reset response")
            if reply["ok"] is not True:
                raise RuntimeError("host reset refused")
            if set(reply) != {"ok", "result"} or not isinstance(reply["result"], dict):
                raise ValueError("invalid host reset result")
            work, ns, packet, config = self.loader(self.work)
            if work != self.work or ns != self.ns or packet["slice"]["topic_id"] == old_topic:
                raise ValueError("reset did not establish a fresh topic UUID")
            result = reply["result"]
            if result.get("namespace", self.ns) != self.ns or result.get("topic_id", packet["slice"]["topic_id"]) != packet["slice"]["topic_id"]:
                raise ValueError("reset result identity mismatch")
            _remaining(deadline)
            self.packet, self.config = packet, config
            self._open_clients()
            self.receipts.clear()
            self.observations.clear()
            self.pending = None
            # Host may already have consumed the request. Never erase another ID.
            if request.exists():
                if _read(request).get("request_id") != request_id:
                    raise ValueError("reset request replaced unexpectedly")
                request.unlink()
            response.unlink()
            return result
        except BaseException:
            self.poisoned = True
            raise

    def publish(self, frame, deadline):
        if self.pending is not None or len(self.receipts) >= MAX_RECEIPTS:
            raise ValueError("observe pending publication or reset case first")
        if not isinstance(frame, dict) or set(frame) != FRAME_FIELDS:
            raise ValueError("exact frame fields required")
        if (frame["venue"] not in VENUES or frame["provenance"] != "TEST_ONLY"
                or frame["test_provenance"] is not True or type(frame["snapshot"]) is not bool
                or not isinstance(frame["payload"], dict)
                or any(type(frame[k]) is not int or frame[k] < 0 for k in
                       ("connection_generation", "sequence", "previous", "received_at_ns"))
                or frame["connection_generation"] == 0 or frame["received_at_ns"] == 0):
            raise ValueError("invalid test frame")
        try:
            receipt = self.publisher.publish(frame, timeout=min(5, _remaining(deadline)))
            self._validate_receipt(receipt)
            capture = receipt["capture_id"]
            if capture in self.receipts:
                raise ValueError("duplicate capture identity")
            self.receipts[capture] = dict(receipt)
            self.pending = capture
            return receipt
        except BaseException:
            self.poisoned = True
            raise

    def _validate_receipt(self, receipt):
        if (not isinstance(receipt, dict) or set(receipt) != RECEIPT_FIELDS
                or receipt["topic"] != self.ns + ".raw"
                or type(receipt["partition"]) is not int or receipt["partition"] != 0
                or type(receipt["offset"]) is not int or receipt["offset"] < 0
                or type(receipt["generation"]) is not int or receipt["generation"] < 1
                or isinstance(receipt["broker_ack_ms"], bool)
                or not isinstance(receipt["broker_ack_ms"], (int, float))
                or not math.isfinite(receipt["broker_ack_ms"]) or receipt["broker_ack_ms"] < 0
                or receipt["provenance"] != "TEST_ONLY" or receipt["acceptance"] is not False
                or not re.fullmatch("[0-9a-f]{32}", str(receipt["capture_id"]))
                or not re.fullmatch("[0-9a-f]{64}", str(receipt["raw_sha256"]))):
            raise ValueError("invalid broker receipt")

    def observe(self, receipt, deadline):
        self._validate_receipt(receipt)
        capture = receipt["capture_id"]
        if self.receipts.get(capture) != receipt:
            raise ValueError("receipt was not published by this case/channel")
        if capture in self.observations:
            return self.observations[capture]
        observer = self.observer
        consumer, offsets = observer.consumer, observer.offset_reader

        class BoundedConsumer:
            def __init__(self, target):
                self.target = target

            def __getattr__(self, key):
                return getattr(self.target, key)

            def poll(self, timeout):
                return self.target.poll(min(timeout, _remaining(deadline)))

            def committed(self, partitions, timeout):
                return self.target.committed(partitions, timeout=min(timeout, _remaining(deadline)))

        try:
            observer.consumer, observer.offset_reader = BoundedConsumer(consumer), BoundedConsumer(offsets)
            result = observer.observe(receipt, timeout=_remaining(deadline))
            expected = {self.ns + ".canonical", self.ns + ".quarantine"}
            if (type(result.get("raw_committed_next")) is not int or result["raw_committed_next"] <= receipt["offset"]
                    or set(result.get("read_committed_eof", [])) != expected):
                raise ValueError("committed observation barrier missing")
            records = result["matched"]
            if not isinstance(records, list) or len(records) > 64:
                raise ValueError("unbounded observation")
            for row in records:
                if (row.get("capture_id") != capture or row.get("topic") not in expected
                        or type(row.get("partition")) is not int or row["partition"] != 0
                        or type(row.get("offset")) is not int or row["offset"] < 0
                        or row.get("kind") not in ("canonical", "quarantine")
                        or row["topic"] != self.ns + "." + row["kind"]
                        or (row["kind"] == "quarantine" and not row.get("reason"))):
                    raise ValueError("canonical coordinate/reason mismatch")
            output = {"observed": True, "records": records,
                "raw": {k: receipt[k] for k in ("topic", "partition", "offset", "capture_id")},
                "raw_committed_next": result["raw_committed_next"],
                "read_committed_eof": result["read_committed_eof"], "provenance": "TEST_ONLY", "acceptance": False}
            if len(json.dumps(output).encode()) > MAX_BYTES - 128:
                raise ValueError("oversize observation")
            self.observations[capture] = output
            self.pending = None
            return output
        except BaseException:
            self.poisoned = True
            raise
        finally:
            observer.consumer, observer.offset_reader = consumer, offsets

    def dispatch(self, request):
        if self.poisoned:
            raise RuntimeError("channel fenced")
        if not isinstance(request, dict) or set(request) != {"op", "args", "timeout"}:
            raise ValueError("exact request fields required")
        deadline = time.monotonic() + _timeout(request["timeout"])
        op, args = request["op"], request["args"]
        fields = {"publish": {"frame"}, "reset_case": {"venue", "fault"}, "canonical_observe": {"receipt"}}
        if not isinstance(op, str) or op not in fields or not isinstance(args, dict) or set(args) != fields[op]:
            raise ValueError("invalid operation arguments")
        if op == "reset_case":
            return self.reset(args["venue"], args["fault"], deadline)
        if op == "publish":
            return self.publish(args["frame"], deadline)
        return self.observe(args["receipt"], deadline)


def serve(channel, incoming, outgoing):
    while True:
        wire = incoming.readline(MAX_BYTES + 1)
        if not wire:
            return
        oversize = len(wire) > MAX_BYTES
        try:
            if oversize:
                channel.poisoned = True
                raise ValueError("oversize request")
            with contextlib.redirect_stdout(sys.stderr):
                result = channel.dispatch(decode(wire))
            reply = {"ok": True, "result": result}
        except Exception as error:
            # Never echo input, native client diagnostics, keys or host errors.
            reply = {"ok": False, "error": "DR05_CHANNEL_" + type(error).__name__}
        outgoing.write(json.dumps(reply, allow_nan=False, separators=(",", ":")) + "\n")
        outgoing.flush()
        if channel.poisoned or oversize:
            return


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", required=True, type=Path)
    args = parser.parse_args(argv)
    channel = None
    try:
        with contextlib.redirect_stdout(sys.stderr):
            channel = Channel(args.channel)
        serve(channel, sys.stdin.buffer, sys.stdout)
        return 1 if channel.poisoned else 0
    except Exception as error:
        print(json.dumps({"ok": False, "error": "DR05_CHANNEL_" + type(error).__name__}))
        return 1
    finally:
        if channel is not None:
            with contextlib.redirect_stdout(sys.stderr):
                channel.close()


if __name__ == "__main__":
    raise SystemExit(main())
