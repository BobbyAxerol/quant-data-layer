"""KN-3 K3.6 (D16b): BAR-edge readback from the KN-3 market cache.

The cache is populated in the exact layout of ``rust/qdl-projector/src/cache.rs``
and ``apply.lua`` (``kn3:<env>:ptr:<lpk>`` {ready, staging, fence},
``kn3:<env>:b:<g>:<lpk>:<open div (BUCKET_OPENS x interval)>`` {<open_ms>: trailer+row},
``BUCKET_OPENS`` = 112)
with rows encoded by the shared codec from real canonical BAR records (the
committed codec golden) and synthetic derivations marked ``k36-test``.

Unit cases use an in-process fake (no service). The Redis case needs
``QDL_KN_TEST_REDIS`` = URL of a disposable Redis; it writes only keys under a
unique ``kn3:k36<run>:`` prefix, deletes exactly those keys, and is skipped
loudly otherwise. A cross-check against rows written by the Rust stage B
itself belongs to the full-flow run (K3-T08), not to this file.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import time
import unittest
import uuid
from pathlib import Path

from qdl.adapters.intervals import canonical_interval_ms
from qdl.marketdata.v2 import market_data_pb2
from qdl.projection.kn_state_codec import encode_bar_row
from qdl.projection.state_contract import MAX_OFFSET, CacheReadState
from qdl.runtime.kn_bar_readback import (
    BUCKET_OPENS,
    KnBarReadback,
    KnBarReadbackError,
    KnBarReadbackNotReady,
    binding_product_key,
    bucket_of,
    readback_from_environment,
)
from qdl.runtime.stable_bar_edge import StableBinanceBarEdge, _canonical_cache_id
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan, stable_authority_record
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig
from tests.test_kn_bar_legacy_import import derived_bar, golden_records, open_ms_of

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
ENVIRONMENT = "paper"
EPOCH = 3


class FakeRedis:
    """Hashes only; records every command so tests can bound what is read."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, bytes]] = {}
        self.commands: list[tuple[str, str, tuple[str, ...]]] = []
        self.on_pointer_read = None

    def hset(self, key: str, field: str, value) -> None:
        self.hashes.setdefault(key, {})[str(field)] = value if isinstance(value, bytes) else str(value).encode()

    def hget(self, key: str, field: str):
        self.commands.append(("HGET", key, (field,)))
        return self.hashes.get(key, {}).get(field)

    def delete(self, key: str) -> None:
        self.hashes.pop(key, None)

    def hmget(self, key: str, fields) -> list:
        fields = tuple(fields)
        self.commands.append(("HMGET", key, fields))
        if ":ptr:" in key and self.on_pointer_read is not None:
            self.on_pointer_read(self)
        stored = self.hashes.get(key, {})
        return [stored.get(field) for field in fields]

    def pipeline(self, transaction: bool = True):
        assert transaction is False
        return _FakePipeline(self)


class _FakePipeline:
    def __init__(self, client: FakeRedis) -> None:
        self.client = client
        self.calls: list[tuple[str, list[str]]] = []

    def hmget(self, key: str, fields) -> None:
        self.calls.append(("hmget", key, list(fields)))

    def hget(self, key: str, field: str) -> None:
        self.calls.append(("hget", key, field))

    def execute(self) -> list:
        return [self.client.hmget(key, arg) if kind == "hmget" else self.client.hget(key, arg)
                for kind, key, arg in self.calls]


def build_partitions(client, environment: str, partitions=range(6), topic: str = "md.bars.v2") -> None:
    """Stage B's checkpoint of each bars state partition (a finished build)."""

    for partition in partitions:
        client.hset(f"kn3:{environment}:ckpt:{topic}:{partition}", "next", "1")


def load_catalog():
    return StableSourceCatalog.load(CATALOG_PATH)


def binding_for(catalog, partition_key: str):
    return next(item for item in catalog.bindings if item.partition_key == partition_key)


def put_rows(client, environment: str, source, generation: int, payloads, *, source_offset: int = MAX_OFFSET,
             ready: bool = True, fence: int = 1, written: list | None = None) -> None:
    """Write rows exactly as ``apply.lua`` op B does (one field per open)."""

    lpk = binding_product_key(source, environment)
    prefix = f"kn3:{environment}:"
    pointer = f"{prefix}ptr:{lpk.encode()}"
    if ready:
        client.hset(pointer, "ready", str(generation))
    client.hset(pointer, "fence", str(fence))
    if written is not None:
        written.append(pointer)
    interval_ms = canonical_interval_ms(source.interval)
    for payload in payloads:
        open_ms = open_ms_of(payload)
        key = f"{prefix}b:{generation}:{lpk.encode()}:{open_ms // (BUCKET_OPENS * interval_ms)}"
        client.hset(key, str(open_ms), encode_bar_row(payload, lpk, source_offset, EPOCH))
        if written is not None:
            written.append(key)


def real_bar(suffix: str) -> tuple[str, bytes]:
    return next((pk, payload) for pk, payload in golden_records(bar=True) if pk.endswith(suffix))


class ReadbackUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = load_catalog()

    def setUp(self) -> None:
        self.redis = FakeRedis()
        self.readback = KnBarReadback(self.redis, ENVIRONMENT)
        pk, self.real = real_bar("okx-swap-doge-usdt-swap-bar-1m-primary-v2")
        self.source = binding_for(self.catalog, pk)
        self.interval_ms = canonical_interval_ms(self.source.interval)
        self.base = open_ms_of(self.real)

    def test_counts_only_final_rows_of_the_asked_opens(self):
        in_progress = derived_bar(self.real, 1, final=False)
        revised = derived_bar(self.real, -1, revision=1)
        older = [derived_bar(self.real, -shift) for shift in range(2, 250)]  # spans several buckets
        put_rows(self.redis, ENVIRONMENT, self.source, 5, [self.real, in_progress, revised, *older])
        asked = frozenset(self.base + shift * self.interval_ms for shift in range(-300, 3))
        covered = self.readback.durable_final_bar_opens(self.source, asked)
        expected = {self.base, self.base - self.interval_ms} | {
            self.base - shift * self.interval_ms for shift in range(2, 250)}
        self.assertEqual(covered, frozenset(expected))
        buckets = {bucket_of(open_ms, self.interval_ms) for open_ms in asked}
        bucket_reads = [command for command in self.redis.commands if ":b:" in command[1]]
        self.assertEqual(len(bucket_reads), len(buckets))  # one HMGET per bucket, no more
        self.assertEqual({field for _c, _k, fields in bucket_reads for field in fields}, {str(o) for o in asked})
        self.assertTrue(all(key.startswith(f"kn3:{ENVIRONMENT}:b:5:") for _c, key, _f in bucket_reads))
        self.assertEqual({command[0] for command in self.redis.commands}, {"HMGET"})

    def test_rows_written_with_a_canonical_offset_or_the_legacy_offset_both_count(self):
        put_rows(self.redis, ENVIRONMENT, self.source, 2, [self.real], source_offset=41)
        put_rows(self.redis, ENVIRONMENT, self.source, 2, [derived_bar(self.real, -1)])
        asked = frozenset({self.base, self.base - self.interval_ms})
        self.assertEqual(self.readback.durable_final_bar_opens(self.source, asked), asked)

    def test_not_ready_states_and_the_never_materialized_product(self):
        """KN-4 D47-1: only a product never materialized in a built cache reads empty."""
        asked = frozenset({self.base})
        # Cache lost or first build unfinished: no checkpoint -> never empty.
        with self.assertRaises(KnBarReadbackNotReady) as caught:
            self.readback.durable_final_bar_opens(self.source, asked)
        self.assertIs(caught.exception.state, CacheReadState.NOT_READY_NO_GENERATION)
        self.assertEqual(caught.exception.reason, "PARTITION_NOT_BUILT")
        # Built partition, no pointer: nothing of it is durable.
        build_partitions(self.redis, ENVIRONMENT)
        self.assertEqual(self.readback.durable_final_bar_opens(self.source, asked), frozenset())
        # Staging only (building/rebuilding): not provable, fail closed.
        lpk = binding_product_key(self.source, ENVIRONMENT).encode()
        put_rows(self.redis, ENVIRONMENT, self.source, 9, [self.real], ready=False, fence=0)
        self.redis.hset(f"kn3:{ENVIRONMENT}:ptr:{lpk}", "staging", "9")
        with self.assertRaises(KnBarReadbackNotReady) as caught:
            self.readback.durable_final_bar_opens(self.source, asked)
        self.assertEqual(caught.exception.reason, "REBUILDING")
        # The staging discarded unpublished (apply.lua op X) leaves only the fence.
        self.redis.hashes[f"kn3:{ENVIRONMENT}:ptr:{lpk}"].pop("staging")
        self.assertEqual(self.readback.durable_final_bar_opens(self.source, asked), frozenset())
        # Published: served from the READY generation.
        self.redis.hset(f"kn3:{ENVIRONMENT}:ptr:{lpk}", "ready", "9")
        self.assertEqual(self.readback.durable_final_bar_opens(self.source, asked), asked)
        # The partition's checkpoint lost with the pointer (Redis state gone) -> not empty.
        self.redis.delete(f"kn3:{ENVIRONMENT}:ptr:{lpk}")
        self.redis.delete(f"kn3:{ENVIRONMENT}:ckpt:md.bars.v2:{self.readback.state_partition_of(binding_product_key(self.source, ENVIRONMENT))}")
        with self.assertRaises(KnBarReadbackNotReady):
            self.readback.durable_final_bar_opens(self.source, asked)
        self.assertEqual(self.readback.durable_final_bar_opens(self.source, frozenset()), frozenset())

    def test_a_row_of_another_identity_fails_closed(self):
        foreign = derived_bar(self.real, -1, source_id="okx-other-source")
        put_rows(self.redis, ENVIRONMENT, self.source, 4, [self.real, foreign])
        with self.assertRaisesRegex(KnBarReadbackError, "differs from its binding"):
            self.readback.durable_final_bar_opens(self.source, frozenset({self.base - self.interval_ms}))
        # A row of another product under this LPK does not even decode (hash over the restored fields).
        lpk = binding_product_key(self.source, ENVIRONMENT)
        _pk, other = real_bar("okx-swap-doge-usdt-swap-bar-5m-primary-v2")
        other_source = binding_for(self.catalog, _pk)
        row = encode_bar_row(other, binding_product_key(other_source, ENVIRONMENT), MAX_OFFSET, EPOCH)
        bucket = bucket_of(self.base, self.interval_ms)
        self.redis.hset(f"kn3:{ENVIRONMENT}:b:4:{lpk.encode()}:{bucket}", str(self.base), row)
        with self.assertRaisesRegex(KnBarReadbackError, "unreadable"):
            self.readback.durable_final_bar_opens(self.source, frozenset({self.base}))

    def test_a_row_filed_under_another_open_fails_closed(self):
        put_rows(self.redis, ENVIRONMENT, self.source, 4, [self.real])
        lpk = binding_product_key(self.source, ENVIRONMENT).encode()
        wrong = self.base + self.interval_ms
        self.redis.hset(f"kn3:{ENVIRONMENT}:b:4:{lpk}:{bucket_of(wrong, self.interval_ms)}", str(wrong),
                        encode_bar_row(self.real, binding_product_key(self.source, ENVIRONMENT), MAX_OFFSET, EPOCH))
        with self.assertRaisesRegex(KnBarReadbackError, "another open"):
            self.readback.durable_final_bar_opens(self.source, frozenset({wrong}))

    def test_a_generation_change_during_the_read_raises(self):
        put_rows(self.redis, ENVIRONMENT, self.source, 4, [self.real])
        lpk = binding_product_key(self.source, ENVIRONMENT).encode()
        reads = []

        def publish_new_generation(client) -> None:
            reads.append(1)
            if len(reads) == 2:  # the pointer read after the bucket reads
                client.hset(f"kn3:{ENVIRONMENT}:ptr:{lpk}", "ready", "8")
                client.hset(f"kn3:{ENVIRONMENT}:ptr:{lpk}", "fence", "2")

        self.redis.on_pointer_read = publish_new_generation
        with self.assertRaisesRegex(KnBarReadbackError, "generation changed during readback"):
            self.readback.durable_final_bar_opens(self.source, frozenset({self.base}))

    def test_the_cache_epoch_does_not_change_when_products_become_ready_or_rebuild(self):
        """KN-4 D47-1: identity = the epoch, not the product generations."""
        from qdl.runtime.kn_bar_readback import KnBarReadbackEpochNotReady

        lpk = binding_product_key(self.source, ENVIRONMENT).encode()
        self.assertEqual(self.readback.generation_identity(self.source), f"kn3:{ENVIRONMENT}:{lpk}@-")
        bars = [item for item in self.catalog.bindings if item.feed.value == "BAR"]
        with self.assertRaises(KnBarReadbackEpochNotReady):
            self.readback.cache_identity(bars)  # nothing built yet
        build_partitions(self.redis, ENVIRONMENT)
        first = self.readback.cache_identity(bars)
        self.assertRegex(first, r"^[0-9a-f]{32}$")
        self.assertEqual(self.readback.cache_identity(reversed(bars)), first)
        self.assertEqual(self.readback.generation_identities(bars)[self.source.binding_id], None)
        # First READY, then a rebuild, then a second product READY: same epoch.
        put_rows(self.redis, ENVIRONMENT, self.source, 12, [self.real])
        self.assertEqual(self.readback.generation_identity(self.source), f"kn3:{ENVIRONMENT}:{lpk}@12")
        self.assertEqual(self.readback.cache_identity(bars), first)
        self.redis.hset(f"kn3:{ENVIRONMENT}:ptr:{lpk}", "ready", "13")
        other = next(item for item in bars if item.binding_id != self.source.binding_id)
        self.redis.hset(f"kn3:{ENVIRONMENT}:ptr:{binding_product_key(other, ENVIRONMENT).encode()}", "ready", "1")
        self.assertEqual(self.readback.cache_identity(bars), first)
        self.assertEqual({key: value for key, value in self.readback.generation_identities(bars).items()
                          if value is not None}, {self.source.binding_id: 13, other.binding_id: 1})
        # A lost partition checkpoint (cache gone) is not a new epoch: it fails closed.
        self.redis.delete(f"kn3:{ENVIRONMENT}:ckpt:md.bars.v2:0")
        with self.assertRaises(KnBarReadbackEpochNotReady):
            self.readback.cache_identity(bars)

    def test_bucket_size_matches_the_rust_writer_and_the_budget(self):
        # Owner decision, Astra KN-3 review R1: 112 opens, changed together in
        # the Rust writer, this readback, the checker and the budget.
        self.assertEqual(BUCKET_OPENS, 112)
        rust = (ROOT / "rust/qdl-projector/src/cache.rs").read_text(encoding="utf-8")
        match = re.search(r"^pub const BUCKET_OPENS: u64 = (\d+);$", rust, flags=re.M)
        self.assertIsNotNone(match)
        self.assertEqual(int(match.group(1)), BUCKET_OPENS)
        budget = json.loads((ROOT / "config/v2/kn-v220-candidate-budget.json").read_text(encoding="utf-8"))
        self.assertIn(f"buckets of {BUCKET_OPENS} opens", budget["market_cache"]["bar_layout"])
        self.assertEqual(bucket_of(BUCKET_OPENS * 60_000 - 1, 60_000), 0)
        self.assertEqual(bucket_of(BUCKET_OPENS * 60_000, 60_000), 1)

    def test_environment_selection(self):
        with self.assertRaises(ValueError):
            readback_from_environment({"QDL_KN3_ENVIRONMENT": "paper"})
        with self.assertRaises(ValueError):
            KnBarReadback(self.redis, "bad env")


class EdgeBackendParityTests(unittest.TestCase):
    """The edge asks the same question of both backends and gets the same answer."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = load_catalog()
        cls.acquisition = StableAcquisitionPlan.load(ACQUISITION_PATH, catalog=cls.catalog)
        cls.authority = stable_authority_record(
            rust_image_digest="a" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION_PATH.read_bytes(),
            effective_at_ns=time.time_ns(),
        )

    class _NoopPublisher:
        def publish_many(self, values):
            return tuple(range(len(tuple(values))))

    def _edge(self, **backend) -> StableBinanceBarEdge:
        return StableBinanceBarEdge(catalog=self.catalog, acquisition=self.acquisition, authority=self.authority,
                                    publisher=self._NoopPublisher(), warmup_rows=2, clock=lambda: 180.0, **backend)

    def test_sqlite_and_market_cache_answer_the_same_and_only_a_rebuilt_product_rebases(self):
        pk, real = real_bar("binance-usdm-dogeusdt-bar-15m-primary-v2")
        source = binding_for(self.catalog, pk)
        interval_ms = canonical_interval_ms(source.interval)
        base = open_ms_of(real)
        payloads = [real, derived_bar(real, -1), derived_bar(real, -3), derived_bar(real, 1, final=False)]
        asked = frozenset(base + shift * interval_ms for shift in range(-5, 3))
        with tempfile.TemporaryDirectory(prefix="kn3-k36-edge-") as directory:
            cache_path = Path(directory) / "canonical-cache.sqlite3"
            spool = SQLiteDurableSpool(SpoolConfig(path=cache_path, max_records=100, max_payload_bytes=1_000_000,
                                                   max_event_bytes=64_000, max_storage_bytes=2_000_000,
                                                   min_free_disk_bytes=0))
            try:
                for index, payload in enumerate(payloads):
                    spool.append(DurableEvent(
                        stream=source.canonical_stream, partition_key=source.partition_key,
                        event_id=bytes(market_data_pb2.EventEnvelope.FromString(payload).event_id),
                        payload=payload, accepted_at_ns=1_000 + index))
                sqlite_edge = self._edge(canonical_cache_id=_canonical_cache_id(cache_path),
                                         canonical_cache_path=cache_path)
                sqlite_answer = sqlite_edge._durable_final_bar_opens(source, asked)
            finally:
                spool.close()
        redis = FakeRedis()
        build_partitions(redis, ENVIRONMENT)
        put_rows(redis, ENVIRONMENT, source, 2, payloads)
        kn_edge = self._edge(bar_readback=KnBarReadback(redis, ENVIRONMENT))
        self.assertRegex(kn_edge.canonical_cache_id, r"^[0-9a-f]{32}$")
        kn_answer = kn_edge._durable_final_bar_opens(source, asked)
        self.assertEqual(kn_answer, sqlite_answer)
        self.assertEqual(kn_answer, frozenset({base, base - interval_ms, base - 3 * interval_ms}))
        # The first observation records generations; nothing rebases.
        self.assertEqual(kn_edge._rebase_changed_products(), ())
        others = [item for item, _a in kn_edge.history_bindings + kn_edge.history_okx_bindings
                  if item.binding_id != source.binding_id][:2]
        for binding_id in [source.binding_id] + [item.binding_id for item in others]:
            kn_edge._last_open_ms[binding_id] = base
        # Two other products become READY one after the other: normal, no rebase.
        for generation, item in enumerate(others, start=1):
            redis.hset(f"kn3:{ENVIRONMENT}:ptr:{binding_product_key(item, ENVIRONMENT).encode()}", "ready",
                       str(generation))
            self.assertEqual(kn_edge._rebase_changed_products(), ())
        self.assertFalse(kn_edge._rebase_if_canonical_cache_generation_changed())
        # This product is rebuilt into generation 3: only its checkpoint goes.
        lpk = binding_product_key(source, ENVIRONMENT).encode()
        redis.hset(f"kn3:{ENVIRONMENT}:ptr:{lpk}", "ready", "3")
        self.assertEqual(kn_edge._rebase_changed_products(), (source.binding_id,))
        self.assertNotIn(source.binding_id, kn_edge._last_open_ms)
        self.assertTrue(all(item.binding_id in kn_edge._last_open_ms for item in others))
        self.assertFalse(kn_edge._rebase_if_canonical_cache_generation_changed())
        with self.assertRaises(ValueError):
            self._edge(bar_readback=KnBarReadback(redis, ENVIRONMENT), canonical_cache_id="0" * 32)

    def test_published_history_is_served_only_after_the_cache_reads_it_back(self):
        """KN-4 D47-1: an ACK is publish progress; serving is proven by readback,
        also across a restart between publish and materialization."""
        pk, real = real_bar("binance-usdm-dogeusdt-bar-15m-primary-v2")
        source = binding_for(self.catalog, pk)
        interval_ms = canonical_interval_ms(source.interval)
        base = open_ms_of(real)
        payloads = [real, derived_bar(real, -1)]
        opens = frozenset({base, base - interval_ms})
        redis = FakeRedis()
        build_partitions(redis, ENVIRONMENT)
        now = [180.0]
        edge = StableBinanceBarEdge(catalog=self.catalog, acquisition=self.acquisition, authority=self.authority,
                                    publisher=self._NoopPublisher(), warmup_rows=2, clock=lambda: now[0],
                                    bar_readback=KnBarReadback(redis, ENVIRONMENT))
        edge._last_open_ms[source.binding_id] = base
        edge._record_serving(source.binding_id, opens, published=True)
        self.assertEqual(edge.verify_serving()["pending"], 1)  # ACKed, never materialized
        # A rebuilding product cannot prove anything yet: still pending.
        lpk = binding_product_key(source, ENVIRONMENT).encode()
        redis.hset(f"kn3:{ENVIRONMENT}:ptr:{lpk}", "staging", "4")
        self.assertEqual(edge.verify_serving()["pending"], 1)
        now[0] += 901
        self.assertEqual(edge.verify_serving()["overdue"], 1)
        # A restart here keeps no in-memory progress: the checkpoint gap check
        # finds the unmaterialized window, so the binding is filled again.
        with self.assertRaises(KnBarReadbackNotReady):  # while rebuilding: fail closed, never "complete"
            edge._checkpoint_history_gaps({source.binding_id: base})
        redis.hashes[f"kn3:{ENVIRONMENT}:ptr:{lpk}"].pop("staging")
        self.assertEqual(edge._checkpoint_history_gaps({source.binding_id: base}), {source.binding_id: 2})
        # The projector materializes the rows: served, no longer overdue.
        put_rows(redis, ENVIRONMENT, source, 4, payloads)
        status = edge.verify_serving()
        self.assertEqual((status["served"], status["pending"], status["overdue"]), (1, 0, 0))
        self.assertEqual(edge._checkpoint_history_gaps({source.binding_id: base}), {})
        # Everything already durable: served without waiting.
        edge._record_serving(source.binding_id, opens, published=False)
        self.assertEqual(edge.serving_status()["served"], 1)


@unittest.skipUnless(os.environ.get("QDL_KN_TEST_REDIS"),
                     "needs QDL_KN_TEST_REDIS = URL of a disposable Redis")
class ReadbackRedisTests(unittest.TestCase):
    def setUp(self) -> None:
        import redis

        self.client = redis.Redis.from_url(os.environ["QDL_KN_TEST_REDIS"])
        self.environment = f"k36{uuid.uuid4().hex[:8]}"
        self.written: list[str] = []
        self.catalog = load_catalog()

    def tearDown(self) -> None:
        if self.written:
            self.client.delete(*set(self.written))
        self.client.close()

    def test_real_redis_layout_is_read_like_the_fake(self):
        pk, real = real_bar("okx-swap-doge-usdt-swap-bar-1h-primary-v2")
        source = binding_for(self.catalog, pk)
        interval_ms = canonical_interval_ms(source.interval)
        base = open_ms_of(real)
        payloads = [real, derived_bar(real, 1, final=False),
                    *(derived_bar(real, -shift) for shift in range(1, 130))]
        put_rows(self.client, self.environment, source, 21, payloads, written=self.written)
        for partition in range(6):  # stage B's checkpoints of a built cache
            key = f"kn3:{self.environment}:ckpt:md.bars.v2:{partition}"
            self.client.hset(key, "next", "1")
            self.written.append(key)
        keys_before = {key: self.client.hgetall(key) for key in set(self.written)}
        readback = KnBarReadback(self.client, self.environment)
        asked = frozenset(base + shift * interval_ms for shift in range(-140, 2))
        covered = readback.durable_final_bar_opens(source, asked)
        self.assertEqual(covered, frozenset(base - shift * interval_ms for shift in range(0, 130)))
        self.assertEqual({key: self.client.hgetall(key) for key in set(self.written)}, keys_before)  # read-only
        lpk = binding_product_key(source, self.environment).encode()
        self.assertEqual(readback.generation_identity(source), f"kn3:{self.environment}:{lpk}@21")
        other = binding_for(self.catalog, real_bar("okx-swap-doge-usdt-swap-bar-4h-primary-v2")[0])
        other_lpk = binding_product_key(other, self.environment).encode()
        self.client.hset(f"kn3:{self.environment}:ptr:{other_lpk}", "staging", "5")
        self.written.append(f"kn3:{self.environment}:ptr:{other_lpk}")
        with self.assertRaises(KnBarReadbackNotReady):
            readback.durable_final_bar_opens(other, frozenset({base}))


if __name__ == "__main__":
    unittest.main()
