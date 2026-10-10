"""Bounded host reset bridge for the disposable DR05 JSONL channel.

The broker/network namespace survives. Only owned readers/core/quota and the
three exact TEST_ONLY topics are reset. No Docker socket enters a container.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts import p184_dr05_dl_fault as dr
from scripts.p184_dr05_case import _deadline, _remaining, _packet, _reset_preflight


def reset_topics(work, timeout):
    from confluent_kafka import KafkaError, KafkaException, TopicCollection
    from confluent_kafka.admin import AdminClient, NewTopic
    from scripts.p184_dr08_isolated import kafka_security

    deadline = _deadline(timeout)
    work, ns, packet, config = _packet(work)
    if os.environ.get("P184_DR05_ISOLATED_NAMESPACE") != ns:
        raise ValueError("isolated namespace attestation required")
    topics = [ns + suffix for suffix in (".raw", ".canonical", ".quarantine")]
    admin = AdminClient(kafka_security(packet))
    for future in admin.delete_topics(topics, operation_timeout=min(5, _remaining(deadline))).values():
        future.result(_remaining(deadline))
    while any(name in admin.list_topics(timeout=min(2, _remaining(deadline))).topics for name in topics):
        time.sleep(min(.1, _remaining(deadline)))
    for future in admin.delete_consumer_groups([ns, "kn-" + ns], request_timeout=_remaining(deadline)).values():
        try:
            future.result(_remaining(deadline))
        except KafkaException as error:
            if error.args[0].code() != KafkaError.GROUP_ID_NOT_FOUND:
                raise
    for future in admin.create_topics([NewTopic(name, 1, 1) for name in topics],
                                      operation_timeout=min(5, _remaining(deadline))).values():
        future.result(_remaining(deadline))
    desc = admin.describe_topics(TopicCollection([ns + ".canonical"]), request_timeout=_remaining(deadline))
    new_id = str(desc[ns + ".canonical"].result(_remaining(deadline)).topic_id)
    old_id = packet["slice"]["topic_id"]
    if not new_id or new_id == old_id:
        raise RuntimeError("topic incarnation did not change")
    packet["slice"]["topic_id"] = new_id
    temp = work / "packet.reset.tmp"
    temp.write_text(json.dumps(packet))
    temp.replace(work / "packet.json")
    return {"topic_id": new_id, "previous_topic_id": old_id, "topics": topics}


class ResetStageError(RuntimeError):
    """A reset that failed after it stopped the readers: names the stage and keeps a bounded stderr tail (D6, r13)."""

    def __init__(self, stage, error):
        stderr = getattr(error, "stderr", None) or ""
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", "replace")
        tail = "".join(ch if ch.isprintable() else " " for ch in stderr)[-240:]
        self.stage = stage
        super().__init__(f"stage={stage} readers_stopped=true {type(error).__name__}:{str(error)[:80]}"
                         f" rc={getattr(error, 'returncode', None)} stderr_tail={tail!r}"[:380])


def reset_preserving_broker(work, venue, timeout=30):
    deadline = _deadline(timeout)
    if venue not in dr.SCOPES:
        raise ValueError("unknown venue")
    work, ns, packet, _ = _packet(work)
    if venue not in dr.scope_set(work):
        raise ValueError("scope not prepared in this case")
    selected = dr.core_override(json.loads((work / "handoff.json").read_text()).get("core_binary"))

    def call(argv):
        return dr.bounded(argv, min(15, _remaining(deadline)))

    channel = dr.owned(ns, "helper", call)
    broker = _reset_preflight(work, ns, call, selected, allow_channel=channel is not None)
    if not broker:
        raise ValueError("running owned broker required")
    rows = [dr.owned(ns, role, call) for role in ("query", "stream", "core", "quota")]
    if not all(rows):
        raise ValueError("complete owned stack required before reset")
    ids = [row["Id"] for row in rows]
    call(["docker", "stop", "--time", "1", *ids])
    stage = "stop_readers"
    try:
        while any(dr.owned(ns, role, call) for role in ("query", "stream", "core", "quota")):
            time.sleep(min(.1, _remaining(deadline)))
        return _reset_after_stop(work, ns, venue, selected, broker, call, deadline, channel)
    except _StageFailure as failure:
        raise ResetStageError(failure.stage, failure.error) from failure.error
    except Exception as error:
        raise ResetStageError(stage, error) from error


class _StageFailure(Exception):
    def __init__(self, stage, error):
        super().__init__(stage)
        self.stage, self.error = stage, error


def _reset_after_stop(work, ns, venue, selected, broker, call, deadline, channel):

    def inside(script, args):
        if channel:
            argv = ["docker", "exec", ns + "-helper", "python", "-B", script, *args]
        else:
            argv = dr.helper_command(ns, work, args, network="container:" + ns + "-kafka")
            argv[argv.index("scripts/p184_dr05_dl_fault.py")] = script
            pos = argv.index(dr.image_set(work)["python"])
            argv[pos:pos] = ["-e", "P184_DR05_ISOLATED_NAMESPACE=" + ns]
        return call(argv)

    try:
        topic = json.loads(inside("scripts/p184_dr05_reset.py",
                                 ["--topics", "/work", "--timeout", str(_remaining(deadline))]))
    except Exception as error:
        raise _StageFailure("reset_topics", error) from error
    (work / "query-handoffs.jsonl").unlink(missing_ok=True)
    try:
        commands = dr.topology(ns, work, topic["topic_id"], core_binary=selected)
        for role in ("quota", "core", "stream", "query"):
            call(commands[role])
    except Exception as error:
        raise _StageFailure("start_readers", error) from error
    while True:
        try:
            _remaining(deadline)
        except Exception as error:
            raise _StageFailure("probe", error) from error
        try:
            inside("scripts/p184_dr05_dl_fault.py", ["--probe", "/work"])
            break
        except __import__("subprocess").CalledProcessError:
            time.sleep(min(.2, _remaining(deadline)))
    current = dr.owned(ns, "kafka", call)
    if not current or current["Id"] != broker["Id"]:
        raise _StageFailure("verify_broker", RuntimeError("broker namespace changed during reset"))
    return {"namespace": ns, "venue": venue, "generation": 7, "reset": True,
            "broker_id": current["Id"], "topic_id": topic["topic_id"],
            "previous_topic_id": topic["previous_topic_id"], "acceptance": False,
            "core_binary": selected, "services_reachable": True}


def serve_host(work, duration=1800):
    work, ns, _, _ = _packet(work)
    if not 0 < duration <= 3600:
        raise ValueError("host watcher duration must be bounded")
    control = work / "control"
    if control.is_symlink():
        raise ValueError("control path cannot be a symlink")
    control.mkdir(mode=0o700, exist_ok=True)
    stopped = False

    def stop(*_):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    deadline = time.monotonic() + duration
    seen = set()
    while not stopped and time.monotonic() < deadline:
        request = control / "reset-request.json"
        if not request.exists():
            time.sleep(.05)
            continue
        if request.is_symlink() or request.stat().st_size > 8192:
            raise ValueError("invalid reset request file")
        row = json.loads(request.read_text())
        ident = row.get("request_id", "")
        if (not re.fullmatch(r"[0-9a-f]{32}", ident) or ident in seen
                or row.get("namespace") != ns or row.get("fault") not in dr.FAULTS
                or set(row) != {"request_id", "namespace", "venue", "fault", "timeout"}):
            raise ValueError("invalid reset identity/shape")
        seen.add(ident)
        if len(seen) > 16:
            raise ValueError("reset attempt cap exceeded")
        request.unlink()
        try:
            result = {"ok": True, "result": reset_preserving_broker(work, row["venue"],
                        min(float(row["timeout"]), deadline - time.monotonic()))}
        except Exception as error:
            result = {"ok": False, "error": type(error).__name__ + ":" + str(error)[:400]}
        temp = control / (ident + ".tmp")
        temp.write_text(json.dumps(result))
        temp.replace(control / ("reset-" + ident + ".json"))
        if not result["ok"]:
            return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--topics", type=Path)
    group.add_argument("--serve-reset-host", type=Path)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--duration", type=float, default=1800)
    args = parser.parse_args()
    if args.topics:
        print(json.dumps(reset_topics(args.topics, args.timeout)))
        return 0
    return serve_host(args.serve_reset_host, args.duration)


if __name__ == "__main__":
    raise SystemExit(main())
