#!/usr/bin/env python3
"""P18.4 G13 DR05.QUIET_TYPED at ISOLATED_FAULT: a quiet TRADE session on the DR05 isolated Data Layer.

Purpose (TS case matrix v2 amendment-1, owner 2026-10-17 ~03:25Z): the two cells DR05.QUIET_TYPED@BINANCE/@OKX
need a TRADE view whose provider session stays LIVE while no TRADE event arrives. A liquid live market does not
go quiet in a 300 s runtime window, so this module produces that state on the disposable DR05 stack
(scripts/p184_dr05_dl_fault.py). It reuses that stack; no new service type and no new image:
- ``--seal WORK`` (in-container, network none, after --init): adds one TRADE lane per linear scope (BINANCE
  ETHUSDT, OKX ETH-USDT-SWAP) to the DR05 BOOK material. The core binding comes from the production acquisition
  mapping (checked equal to config/v2/stable-acquisition-bindings.yaml). The source binding comes from the source
  catalog. The consumer requirement has the deployed TS paper manifest's TRADE shape
  (consumers/stable/trading-system-paper.yaml r10: max_freshness_ms 3000, event_recency_policy OBSERVE,
  max_session_liveness_ms 45000). Route bindings, ts-consumers and the stream bundle are recompiled, and an
  empty ``session-liveness/<venue>-<market>/`` tree is created. Marker: handoff.json "dr05_quiet".
- ``--query WORK`` (the stack's query role once sealed; see p184_dr05_dl_fault.topology): the unchanged DR05
  diagnostic Query (p184_dr05_observer --query) whose KN backend reads provider-session liveness from
  /work/session-liveness, the role the production Query gets from QDL_STABLE_STATE_DIR/session-liveness
  (qdl/runtime/stable.py session_liveness_dir).
- ``--channel WORK`` (in-container, inside the broker namespace, P184_DR05_ISOLATED_NAMESPACE attested): a JSONL
  channel for the TS runner (TS scripts/p184_dr05_quiet.py). Ops:
  ``session {venue, live}``: start/stop the synthetic provider-session heartbeat. Like the native ingestor, it
  rewrites one qdl.provider-session-liveness.v1 record for the lane's session (source_session_id
  ``<ns>.<VENUE>.trade``, generation 1, config revision 1) every 0.5 s by atomic rename.
  ``trade_run {venue, hz}`` / ``trade_stop {venue}``: start/stop provider-shaped trades
  (p184_g12_isolated_fault.trade_frame) on that lane.
  ``status {}``: the stats.
  The fault is ``trade_stop`` while the session heartbeat continues. Nothing else changes.
- ``--seal-host WORK``: host wrapper that runs --seal in the DR05 helper container.
Boundary: only the namespace's own broker and work dir. No shared Data Layer, Redis, Kafka or Postgres. No
acceptance verdict here: the TS runner reads its writer clone and applies the amended predicate.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.p184_dr05_dl_fault as dr  # noqa: E402  (stdlib-only module top level)

MARK = "dr05_quiet"
LIVENESS_DIR = "session-liveness"
LIVENESS_SCHEMA = "qdl.provider-session-liveness.v1"  # qdl/runtime/session_liveness.py SESSION_LIVENESS_SCHEMA
HEARTBEAT_S = 0.5
LANE = "trade"
# TS paper manifest TRADE requirement shape (consumers/stable/trading-system-paper.yaml r10).
TRADE_REQUIREMENT = {"max_freshness_ms": 3000, "warmup_limit": 0, "require_final_bars": True,
                     "event_recency_policy": "OBSERVE", "max_session_liveness_ms": 45000}
MAX_HZ = 10


def sealed(work):
    """The handoff's quiet seal record, or None."""
    handoff = Path(work) / "handoff.json"
    if not handoff.is_file():
        return None
    return json.loads(handoff.read_text()).get(MARK)


def session_id(ns, venue):
    """The lane's provider session id: the raw envelope's source_session_id (trade_envelope)."""
    return f"{dr.namespace(ns)}.{venue}.{LANE}"


def liveness_path(work, ns, binding):
    """``<work>/session-liveness/<venue>-<market>/<ns>.<venue>.trade.json`` (StableSessionLivenessReader layout)."""
    directory = f"{binding['venue'].lower()}-{binding['market'].lower()}"
    return Path(work) / LIVENESS_DIR / directory / f"{dr.namespace(ns)}.{binding['venue'].lower()}.{LANE}.json"


def liveness_record(ns, venue, at_ns, state="LIVE"):
    if type(at_ns) is not int or at_ns <= 0 or state not in ("LIVE", "DISCONNECTED"):
        raise ValueError("positive integer transport time and a session state required")
    return {"schema": LIVENESS_SCHEMA, "source_session_id": session_id(ns, venue), "connection_generation": 1,
            "state": state, "last_transport_at_ns": at_ns, "updated_at_ns": at_ns, "config_revision": 1}


def write_atomic(path, document):
    path = Path(path)
    temporary = path.with_name("." + path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("x") as handle:
            os.chmod(temporary, 0o644)
            handle.write(json.dumps(document, separators=(",", ":")))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def trade_bindings(config):
    """venue -> the sealed TRADE core binding (exactly one per linear scope)."""
    from scripts.p184_g12_isolated_fault import lane_feed
    out = {}
    for binding in config["core"]["bindings"]:
        if binding.get("l2") or lane_feed(binding["provider_kind"]) != "TRADE":
            continue
        if binding["venue"] in out:
            raise ValueError("duplicate TRADE lane " + binding["venue"])
        out[binding["venue"]] = binding
    if set(out) != set(dr.VENUES):
        raise ValueError("one sealed TRADE lane per linear scope required")
    return out


# ---------------------------------------------------------------- --seal (offline, in-container)

def seal(work):
    """Add the TRADE lane of both linear scopes to the DR05 BOOK material (once; handoff "dr05_quiet")."""
    import yaml
    from qdl.query import ConsumerGrade, FeedType
    from qdl.runtime.production_catalog import ProductionCatalogBuilder, ProductionDemand
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from scripts.kn_gateway_bundle import compile_bundle
    from scripts.kn_native_slice_probe import ISSUER, AUDIENCE
    from scripts.p184_dr05_case import BOOK_FEEDS, consumer_packet
    from scripts import p184_g12_isolated_fault as g12

    work = Path(work).resolve()
    handoff = json.loads((work / "handoff.json").read_text())
    ns = dr.namespace(handoff["namespace"])
    if handoff.get("material_ready") is not True or handoff.get(MARK) is not None or handoff.get("g12_feeds"):
        raise ValueError("unsealed DR05 material required")
    if dr.adapter_mode(handoff) != "SYNTHETIC" or dr.scope_set(work) != dr.VENUES:
        raise ValueError("synthetic linear BINANCE/OKX DR05 material required")
    config = json.loads((work / "core.json").read_text())
    books = config["core"]["bindings"]
    if any(not b.get("l2") for b in books) or sorted(b["venue"] for b in books) != sorted(dr.VENUES):
        raise ValueError("the DR05 two-venue BOOK material is required")
    source = yaml.safe_load(dr.source_catalog_path(work).read_text())
    catalog = yaml.safe_load((work / "catalog.yaml").read_text())
    production = {item["binding_id"]: item for item in
                  yaml.safe_load((ROOT / g12.PRODUCTION_ACQUISITION).read_text())["bindings"]}
    lanes = []
    for book in books:
        matches = [b for b in source["bindings"] if b["instrument_uid"] == book["instrument_uid"]
                   and b["feed"] == "TRADE" and b.get("interval") is None
                   and b["source"]["source_role"] == "PRIMARY"]
        if len(matches) != 1:
            raise ValueError("exactly one primary TRADE source required for " + book["instrument_id"])
        planned = production.get(matches[0]["binding_id"]) or {}
        demand = ProductionDemand(
            consumer_id=ns + "." + book["venue"].lower(), consumer_grade=ConsumerGrade.EXECUTION,
            venue=book["venue"], market=book["market"], product_type=book["product_type"],
            native_symbol=book["native_symbol"], feed=FeedType("TRADE"), interval=None,
            source_policy_id=matches[0]["source"]["source_policy_id"])
        acquisition = ProductionCatalogBuilder._acquisition(matches[0]["binding_id"], demand)
        fields = ("mode", "provider_kind", "native_channel", "sequence_policy", "mark_index")
        if {k: acquisition.get(k) for k in fields} != {k: planned.get(k) for k in fields}:
            raise ValueError("generated acquisition differs from the production plan: " + matches[0]["binding_id"])
        entry = copy.deepcopy(matches[0])
        entry["source"]["adapter_version"] = g12.TEST_ADAPTER
        lanes.append((book, entry, g12.md_core_bindings(book, matches[0], acquisition)))
    config["core"]["bindings"] = books + [item for lane in lanes for item in lane[2]]
    digest = hashlib.sha256(json.dumps(config["core"]["bindings"], sort_keys=True).encode()).hexdigest()
    config["authority"].update(capability_manifest_digest=digest, contract_digest=digest,
                               partition_plan_digest=digest)
    catalog["bindings"] = catalog["bindings"] + [lane[1] for lane in lanes]
    g12._rewrite(work / "core.json", config)
    g12._rewrite(work / "catalog.yaml", yaml.safe_dump(catalog))
    loaded = StableSourceCatalog.load(work / "catalog.yaml")
    manifests = []
    for book, entry, _ in lanes:
        label = book["venue"].lower()
        path = work / (label + ".yaml")
        mapping = yaml.safe_load(path.read_text())
        template = next(r for r in mapping["spec"]["requirements"] if r["feed"] == "BOOK_SNAPSHOT")
        requirement = {k: v for k, v in template.items()
                       if k not in ("interval", "event_recency_policy", "max_session_liveness_ms")}
        requirement.update(feed="TRADE", source_policy_id=entry["source"]["source_policy_id"],
                           bar_revision_policy="LATEST", **TRADE_REQUIREMENT)
        mapping["spec"]["requirements"].append(requirement)
        mapping["spec"]["quotas"]["max_streams"] = 50  # the TS paper manifest's
        g12._rewrite(path, yaml.safe_dump(mapping))
        consumer, route = consumer_packet(path, loaded, book["venue"], ISSUER, AUDIENCE, feeds=BOOK_FEEDS + ("TRADE",))
        g12._rewrite(work / (label + ".binding.json"), route)
        g12._rewrite(work / ("ts-consumer." + label + ".json"), consumer)
        manifests.append(path)
        (work / LIVENESS_DIR / f"{book['venue'].lower()}-{book['market'].lower()}").mkdir(
            mode=0o755, parents=True, exist_ok=False)
    g12._rewrite(work / "bundle.json", compile_bundle(environment="paper", catalog_path=work / "catalog.yaml",
                                                      manifest_paths=manifests))
    handoff[MARK] = {"feeds": list(BOOK_FEEDS) + ["TRADE"], "trade_requirement": dict(TRADE_REQUIREMENT),
                     "source_ids": [lane[1]["source"]["source_id"] for lane in lanes],
                     "core_bindings_sha256": digest, "session_liveness_dir": "/work/" + LIVENESS_DIR,
                     "sessions": {lane[0]["venue"]: session_id(ns, lane[0]["venue"]) for lane in lanes}}
    (work / "handoff.json").write_text(json.dumps(handoff, indent=2) + "\n")
    return handoff[MARK]


# ---------------------------------------------------------------- --query (the stack's query role)

def serve_query(work):
    """The DR05 diagnostic Query with provider-session liveness read from /work/session-liveness."""
    import qdl.runtime.kn_query_backend as kn
    from scripts.p184_dr05_observer import diagnostic_query

    work = Path(work)
    if sealed(work) is None or not (work / LIVENESS_DIR).is_dir():
        raise ValueError("sealed DR05 quiet material required")
    base, root = kn.KnMarketCacheQueryBackend, str(work / LIVENESS_DIR)

    class SessionLivenessBackend(base):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("session_liveness_root", root)
            super().__init__(*args, **kwargs)

    kn.KnMarketCacheQueryBackend = SessionLivenessBackend  # serve_query imports it at call time
    diagnostic_query(work)


# ---------------------------------------------------------------- --channel (in the broker namespace)

def trade_envelope(binding, frame, ns):
    """Raw envelope of one test trade on the lane's own session (p184_g12_isolated_fault.md_envelope with the DR05
    namespace): session ``<ns>.<VENUE>.trade``, generation 1, config revision 1, never changed."""
    from qdl.raw.envelope import build_raw_envelope
    from scripts.p184_g12_isolated_fault import TEST_ADAPTER, lane_feed

    if frame.get("test_provenance") is not True or frame.get("provenance") != "TEST_ONLY" \
            or frame.get("adapter_version") != TEST_ADAPTER or frame.get("transport") != "WEBSOCKET":
        raise ValueError("explicit test provenance and websocket transport required")
    if (binding["venue"], binding["native_symbol"]) != (frame["venue"], frame["native_symbol"]) or \
            lane_feed(binding["provider_kind"]) != "TRADE" or frame.get("feed") != "TRADE":
        raise ValueError("frame/binding identity mismatch")
    payload = json.dumps(frame["payload"], separators=(",", ":"), allow_nan=False).encode()
    return build_raw_envelope(
        capture_id=uuid.uuid4().bytes, provider=binding["provider"], venue=binding["venue"],
        market=binding["market"], product_type=binding["product_type"],
        native_symbol=binding["native_symbol"], native_channel=binding["native_channel"],
        subscription_id=binding["source_id"], source_session_id=session_id(ns, binding["venue"]),
        connection_generation=1, lease_epoch=1, authority_revision=1, partition_plan_epoch=1,
        received_at_ns=frame["received_at_ns"], transport_protocol=1, transport_compression=1,
        capture_boundary=2, raw_frame_bytes=payload, adapter_version=TEST_ADAPTER, config_revision=1,
        instrument_catalog_revision=binding["instrument_catalog_revision"],
        correlation_id=str(uuid.uuid4()), test_provenance=True)


class _Loop:
    """One daemon thread repeating ``step`` every ``period`` seconds until stopped."""

    def __init__(self, step, period):
        self.stop_event, self.period, self.step = threading.Event(), period, step
        self.error = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                self.step()
            except Exception as error:  # noqa: BLE001 - recorded, the loop ends
                self.error = f"{type(error).__name__}:{error}"[:200]
                return
            self.stop_event.wait(max(0.0, self.period - (time.monotonic() - started)))

    def stop(self, timeout=5.0):
        self.stop_event.set()
        self.thread.join(timeout)
        return not self.thread.is_alive()


class QuietChannel:
    """Session heartbeat + trade publisher per venue, owned by one JSONL process."""

    def __init__(self, work, *, producer_factory=None, clock=time.time_ns):
        from scripts.p184_dr05_case import _packet
        self.work, self.ns, self.packet, self.config = _packet(work)
        if os.environ.get("P184_DR05_ISOLATED_NAMESPACE") != self.ns:
            raise ValueError("isolated namespace attestation required")
        if sealed(self.work) is None:
            raise ValueError("sealed DR05 quiet material required")
        self.bindings = trade_bindings(self.config)
        self.clock = clock
        self.producer = (producer_factory or self._producer)()
        self.sessions, self.trades = {}, {}
        self.stats = {v: {"published": 0, "acked": 0, "errors": [], "last_trade_id": 0, "last_offset": None,
                          "last_event_ns": None, "heartbeats": 0, "last_transport_at_ns": None}
                      for v in self.bindings}
        self.next_trade_id = {v: 1 for v in self.bindings}
        self.lock = threading.Lock()

    def _producer(self):
        from confluent_kafka import Producer
        if self.packet.get("bootstrap") != "localhost:9092":
            raise ValueError("isolated loopback broker required")
        return Producer({"bootstrap.servers": "localhost:9092", "security.protocol": "SSL",
                         "ssl.ca.location": "/work/ca.crt", "ssl.certificate.location": "/work/client.crt",
                         "ssl.key.location": "/work/client.key", "enable.idempotence": True, "acks": "all",
                         "message.timeout.ms": 5000, "linger.ms": 0})

    def _venue(self, args):
        venue = (args or {}).get("venue")
        if venue not in self.bindings:
            raise ValueError("unknown venue")
        return venue

    def heartbeat(self, venue):
        at_ns = int(self.clock())
        write_atomic(liveness_path(self.work, self.ns, self.bindings[venue]), liveness_record(self.ns, venue, at_ns))
        with self.lock:
            self.stats[venue]["heartbeats"] += 1
            self.stats[venue]["last_transport_at_ns"] = at_ns

    def publish_trade(self, venue):
        from scripts.p184_g12_isolated_fault import trade_frame
        with self.lock:
            trade_id = self.next_trade_id[venue]
            self.next_trade_id[venue] += 1
        at_ns = int(self.clock())
        raw = trade_envelope(self.bindings[venue], trade_frame(venue, trade_id, at_ns), self.ns)
        stats = self.stats[venue]

        def ack(error, message):
            with self.lock:
                if error is not None:
                    stats["errors"].append(str(error.code()))
                    del stats["errors"][:-8]
                else:
                    stats["acked"] += 1
                    stats["last_offset"] = message.offset()

        self.producer.produce(self.config["raw_topics"][0], partition=0, key=raw.subscription_id.encode(),
                              value=raw.SerializeToString(), headers=[("qdl-event-id", raw.capture_id)],
                              on_delivery=ack)
        self.producer.poll(0)
        with self.lock:
            stats["published"] += 1
            stats["last_trade_id"] = trade_id
            stats["last_event_ns"] = at_ns

    def handle(self, op, args):
        if op == "session":
            venue = self._venue(args)
            if type(args.get("live")) is not bool:
                raise ValueError("live must be a boolean")
            if args["live"]:
                if venue in self.sessions:
                    raise ValueError("session heartbeat already running")
                self.heartbeat(venue)  # the record exists before the reply
                self.sessions[venue] = _Loop(lambda: self.heartbeat(venue), HEARTBEAT_S)
            else:
                loop = self.sessions.pop(venue, None)
                if loop is None or not loop.stop():
                    raise RuntimeError("no running session heartbeat")
            return {"venue": venue, "live": args["live"], "session_id": session_id(self.ns, venue),
                    "path": str(liveness_path(Path("/work"), self.ns, self.bindings[venue])),
                    "heartbeats": self.stats[venue]["heartbeats"],
                    "last_transport_at_ns": self.stats[venue]["last_transport_at_ns"]}
        if op == "trade_run":
            venue = self._venue(args)
            hz = args.get("hz")
            if isinstance(hz, bool) or not isinstance(hz, (int, float)) or not 0 < hz <= MAX_HZ:
                raise ValueError(f"hz must be in (0, {MAX_HZ}]")
            if venue in self.trades:
                raise ValueError("trade publisher already running")
            self.trades[venue] = _Loop(lambda: self.publish_trade(venue), 1.0 / hz)
            return {"venue": venue, "hz": hz, "session_id": session_id(self.ns, venue),
                    "started_ns": int(self.clock())}
        if op == "trade_stop":
            venue = self._venue(args)
            loop = self.trades.pop(venue, None)
            if loop is None or not loop.stop():
                raise RuntimeError("no running trade publisher")
            remaining = self.producer.flush(5)
            stopped_ns = int(self.clock())
            with self.lock:
                stats = copy.deepcopy(self.stats[venue])
            return {"venue": venue, "stopped_ns": stopped_ns, "unflushed": remaining, "loop_error": loop.error,
                    **{k: stats[k] for k in ("published", "acked", "errors", "last_trade_id", "last_offset",
                                             "last_event_ns")}}
        if op == "status":
            with self.lock:
                stats = copy.deepcopy(self.stats)
            return {"stats": stats, "sessions": sorted(self.sessions), "trades": sorted(self.trades),
                    "loop_errors": {f"{kind}:{v}": loop.error for kind, loops in
                                    (("session", self.sessions), ("trade", self.trades))
                                    for v, loop in loops.items() if loop.error}}
        raise ValueError("unknown op")

    def close(self):
        for loops in (self.trades, self.sessions):
            for loop in loops.values():
                loop.stop()
            loops.clear()
        try:
            self.producer.flush(5)
        except Exception:  # noqa: BLE001 - shutdown
            pass


def serve_channel(work, stdin=sys.stdin, stdout=sys.stdout, **kwargs):
    """One JSON request per line -> one reply per line; failures never echo inputs."""
    channel = QuietChannel(work, **kwargs)
    try:
        for line in stdin:
            try:
                request = json.loads(line)
                if not isinstance(request, dict) or not isinstance(request.get("op"), str):
                    raise ValueError("request must be an object with an op")
                reply = {"ok": True, "result": channel.handle(request["op"], request.get("args") or {})}
            except Exception as error:  # noqa: BLE001 - the reply carries the type and a bounded message
                reply = {"ok": False, "error": f"{type(error).__name__}:{str(error)[:160]}"}
            stdout.write(json.dumps(reply, separators=(",", ":")) + "\n")
            stdout.flush()
    finally:
        channel.close()


# ---------------------------------------------------------------- host-side argv (stdlib only)

def helper_argv(work, args, *, network):
    """The DR05 helper container running this module (p184_dr05_dl_fault.helper_command, own script)."""
    work = Path(work).resolve()
    ns = dr.namespace(json.loads((work / "handoff.json").read_text())["namespace"])
    net = "none" if network == "none" else "container:" + ns + "-kafka"
    argv = dr.helper_command(ns, work, list(args), network=net)
    argv[argv.index("scripts/p184_dr05_dl_fault.py")] = "scripts/p184_dr05_quiet.py"
    return ns, argv


def channel_argv(work):
    """Interactive (-i) channel in the broker namespace with the namespace attestation."""
    ns, argv = helper_argv(work, ["--channel", "/work"], network="broker")
    argv.insert(2, "-i")
    position = argv.index(dr.image_set(Path(work).resolve())["python"])
    argv[position:position] = ["-e", "P184_DR05_ISOLATED_NAMESPACE=" + ns]
    return argv


def seal_host(work, timeout=180):
    _, argv = helper_argv(work, ["--seal", "/work"], network="none")
    out = subprocess.run(argv, check=True, capture_output=True, text=True, timeout=timeout).stdout
    return json.loads(out.strip().splitlines()[-1])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    for flag in ("seal", "seal-host", "query", "channel"):
        mode.add_argument("--" + flag, type=Path)
    args = parser.parse_args(argv)
    if args.seal:
        print(json.dumps(seal(args.seal)))
    elif args.seal_host:
        print(json.dumps(seal_host(args.seal_host)))
    elif args.query:
        serve_query(args.query)
    else:
        serve_channel(args.channel)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
