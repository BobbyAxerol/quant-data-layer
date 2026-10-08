"""Observer-only fixtures, never provider/certification data."""
import asyncio
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import time
import unittest

spec = importlib.util.spec_from_file_location("observer", Path(__file__).parents[1] / "scripts/measure_kn_ts_cache_delivery.py")
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)
CHANNEL = "events.market.v2.execution.book_delta.OKX.SWAP.ETH-USDT-SWAP"


def view(offset=3, stamp=None):
    return {"canonical_instrument_id": "OKX:SWAP:ETH-USDT-SWAP", "venue": "OKX",
            "venue_symbol": "ETH-USDT-SWAP", "feed": "BOOK_DELTA",
            "observed_at_ms": stamp or int(time.time() * 1000) - 100,
            "watermark_offset": offset, "payload": {"book_generation": 7},
            "contract": {"config_revision": 20}, "quality": {"execution_eligible": True}}


class ComparisonTests(unittest.TestCase):
    def test_exact_view(self):
        v=view(stamp=1000)
        self.assertEqual(observer.compare_observation(CHANNEL,v,v,1200)["match"],"EXACT")

    def test_newer_cache_is_not_matched_delivery(self):
        result=observer.compare_observation(CHANNEL,view(3,1000),view(4,1900),2000)
        self.assertEqual(result["match"],"SUPERSEDED_AT_READ")
        self.assertEqual(result["current_cache_age_ms"],100)
        self.assertEqual(result["notification_age_ms"],1000)

    def test_offset_regression(self):
        with self.assertRaisesRegex(ValueError,"WATERMARK_REGRESSION"):
            observer.compare_observation(CHANNEL,view(4),view(3),time.time()*1000)

    def test_generation_reset_is_not_offset_regression(self):
        old=view(100);new=view(2);new["payload"]["book_generation"]=8
        self.assertEqual(observer.compare_observation(CHANNEL,old,new,time.time()*1000)["match"],"CONTEXT_CHANGED")

    def test_config_change(self):
        old=view();new=deepcopy(old);new["contract"]["config_revision"]=21
        self.assertEqual(observer.compare_observation(CHANNEL,old,new,time.time()*1000)["match"],"CONTEXT_CHANGED")

    def test_authority_change_cannot_be_matched(self):
        old=view();new=deepcopy(old);new["contract"]["authority_revision"]=99
        self.assertEqual(observer.compare_observation(CHANNEL,old,new,time.time()*1000)["match"],"CONTEXT_CHANGED")

    def test_same_offset_changed_event(self):
        with self.assertRaisesRegex(ValueError,"EVENT_IDENTITY_MISMATCH"):
            observer.compare_observation(CHANNEL,view(3,1000),view(3,1001),2000)

    def test_identity_mismatch(self):
        old=view();new=deepcopy(old);new["canonical_instrument_id"]="OKX:SWAP:BTC-USDT-SWAP"
        with self.assertRaisesRegex(ValueError,"IDENTITY_MISMATCH"):
            observer.compare_observation(CHANNEL,old,new,time.time()*1000)

    def test_offsets_must_be_integers(self):
        for bad in [None,True,-1,"3"]:
            v=view();v["watermark_offset"]=bad
            with self.subTest(bad=bad),self.assertRaisesRegex(ValueError,"WATERMARK_MISSING"):
                observer.compare_observation(CHANNEL,v,v,time.time()*1000)

    def test_future_event(self):
        v=view(stamp=2000)
        with self.assertRaisesRegex(ValueError,"INVALID_OR_FUTURE_TIMESTAMP"):
            observer.compare_observation(CHANNEL,v,v,1999)

    def test_bar_close(self):
        v={"canonical_instrument_id":"OKX:SWAP:ETH-USDT-SWAP","timestamp_ms":1000,
           "interval":"1m","watermark_offset":1}
        self.assertEqual(observer.describe("events.market.v2.bar.OKX.SWAP.ETH-USDT-SWAP.1m",v)[2],61000)
        v["interval"]="1h"
        with self.assertRaises(ValueError):observer.describe("events.market.v2.bar",v)

    def mark(self):
        v=view(stamp=1000);v["feed"]="MARK_INDEX_PRICE";v["watermark_offset"]=None
        labels={"connection_generation":"1"}
        for name in ("mark","index"):
            labels.update({f"component_{name}_capture_id":"a"*32,
                           f"component_{name}_source_event_time_ns":"1000000000",
                           f"component_{name}_received_at_ns":"1010000000"})
        v["contract"]["reference_observations"]=[{"labels":labels}]
        v["payload"]={"mark_price":"12.1","index_price":"12.2"}
        return v

    def test_mark_pair_without_scalar_watermark(self):
        v=self.mark()
        self.assertEqual(observer.compare_observation(CHANNEL,v,v,1200)["match"],"EXACT")

    def test_mark_one_component_changed_is_not_matched(self):
        old=self.mark();new=deepcopy(old)
        new["contract"]["reference_observations"][0]["labels"]["component_index_capture_id"]="b"*32
        self.assertEqual(observer.compare_observation(CHANNEL,old,new,1200)["match"],"PAIR_CHANGED_AT_READ")

    def test_mark_price_changed_is_not_matched(self):
        old=self.mark();new=deepcopy(old);new["payload"]["mark_price"]="13"
        self.assertEqual(observer.compare_observation(CHANNEL,old,new,1200)["match"],"PAIR_CHANGED_AT_READ")

    def test_mark_missing_component_proof(self):
        v=self.mark();v["contract"]["reference_observations"][0]["labels"].pop("component_mark_capture_id")
        with self.assertRaisesRegex(ValueError,"REFERENCE_PAIR_MISSING"):
            observer.compare_observation(CHANNEL,v,v,1200)

    def test_mark_future_component(self):
        v=self.mark();v["contract"]["reference_observations"][0]["labels"]["component_index_received_at_ns"]="2000000000"
        with self.assertRaisesRegex(ValueError,"INVALID_OR_FUTURE_TIMESTAMP"):
            observer.compare_observation(CHANNEL,v,v,1200)

    def test_p99_requires_100(self):
        self.assertIsNone(observer.summary(list(range(99)))["p99_ms"])
        self.assertIsNotNone(observer.summary(list(range(100)))["p99_ms"])


class Pub:
    def __init__(self, items):self.items=list(items);self.reads=0
    async def get_message(self, **kwargs):
        await asyncio.sleep(.001)
        if self.items:
            self.reads+=1
            return self.items.pop(0)
        return None


def message(v, channel=CHANNEL):return {"channel":channel.encode(),"data":json.dumps(v).encode()}


class Client:
    def __init__(self,v):self.value=v;self.calls=0
    async def mget(self,keys):
        self.calls+=1
        return [json.dumps(self.value) if self.value else None for _ in keys]


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_reads_are_batched(self):
        v=view();client=Client(v);pub=Pub([message(v) for _ in range(20)])
        result=await observer.observe(client,pub,.06,period=.01)
        self.assertEqual(result["status"],"OBSERVED")
        self.assertLess(client.calls,pub.reads)
        self.assertTrue(result["event_to_cache_readable"])
        self.assertEqual(result["writes"],0)
        self.assertEqual(result["redis_set_ack_latency"],"NOT_MEASURED")

    async def test_drain_continues_during_slow_mget(self):
        v=view();pub=Pub([message(v) for _ in range(30)])
        class Slow(Client):
            async def mget(self,keys):
                await asyncio.sleep(.045)
                self.reads_while_waiting=pub.reads
                return await super().mget(keys)
        client=Slow(v)
        await observer.observe(client,pub,.08,period=.005)
        self.assertEqual(client.reads_while_waiting,30)

    async def test_superseded_is_not_a_latency_sample(self):
        old=view(3);new=view(4)
        result=await observer.observe(Client(new),Pub([message(old)]),.03,period=.005)
        self.assertEqual(result["event_to_cache_readable"],{})
        self.assertTrue(result["current_cache_age_ms"])

    async def test_missing_cache_is_incomplete(self):
        result=await observer.observe(Client(None),Pub([message(view())]),.03,period=.005)
        self.assertEqual(result["status"],"INCOMPLETE")

    async def test_transport_failure_is_retained(self):
        class Broken(Pub):
            async def get_message(self,**kwargs):raise ConnectionError("test")
        result=await observer.observe(Client(view()),Broken([]),.03,period=.005)
        self.assertEqual(result["status"],"INCOMPLETE")
        self.assertIn("OBSERVER_FAILED:ConnectionError",result["errors"])

    async def test_empty_window_is_not_pass(self):
        result=await observer.observe(Client(view()),Pub([]),.02,period=.005)
        self.assertEqual(result["status"],"INCOMPLETE")

    async def test_size_limit(self):
        result=await observer.observe(Client(view()),Pub([message(view())]),.02,period=.005,max_bytes=5)
        self.assertIn("MESSAGE_SIZE_LIMIT",result["errors"])

    async def test_large_valid_book_is_observed(self):
        v = view()
        v["padding"] = "x" * 300000
        result = await observer.observe(Client(v), Pub([message(v)]), .03, period=.005)
        self.assertEqual(result["status"], "OBSERVED")
        self.assertEqual(result["message_sizes"][CHANNEL]["over_256k"], 1)
        self.assertTrue(result["event_to_cache_readable"])
        self.assertEqual(result["redis_set_ack_latency"], "NOT_MEASURED")

    async def test_aggregate_budget_rejects_without_hiding_loss(self):
        v = view()
        size = len(message(v)["data"])
        result = await observer.observe(Client(v), Pub([message(v), message(v, CHANNEL+".extra")]),
                                        .03, period=.005, max_pending_bytes=size)
        self.assertEqual(result["status"], "INCOMPLETE")
        self.assertIn("PENDING_BYTES_LIMIT", result["errors"])
        self.assertLessEqual(result["sampling"]["peak_pending_bytes"], size)

    async def test_replacement_does_not_accumulate_bytes(self):
        v = view()
        size = len(message(v)["data"])
        result = await observer.observe(Client(v), Pub([message(v)] * 20),
                                        .04, period=.005, max_pending_bytes=size)
        self.assertEqual(result["status"], "OBSERVED")
        self.assertEqual(result["sampling"]["peak_pending_bytes"], size)

    async def test_oversized_cache_is_incomplete(self):
        v = view()
        current = dict(v, padding="x" * 1100000)
        result = await observer.observe(Client(current), Pub([message(v)]), .03, period=.005)
        self.assertEqual(result["status"], "INCOMPLETE")
        self.assertIn("CACHE_SIZE_LIMIT", result["errors"])

    async def test_channel_limit(self):
        v=view();result=await observer.observe(Client(v),Pub([message(v),message(v,CHANNEL+".extra")]),.03,period=.005,max_channels=1)
        self.assertIn("CHANNEL_LIMIT",result["errors"])


if __name__ == "__main__": unittest.main()
