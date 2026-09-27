#!/usr/bin/env python3
"""Bounded wire-only driver regression; not SDK/TS/Redis/ACK acceptance."""
import argparse
import asyncio
import base64
import json
from pathlib import Path
import tempfile

import grpc
from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
from qdl.query.v2 import query_pb2 as pb


async def run(args):
    expected = {}
    for line in Path(args.capture).read_text().splitlines():
        payload = base64.b64decode(json.loads(line)["payload"], validate=True)
        uid = EventEnvelope.FromString(payload).instrument_uid
        expected.setdefault(uid, []).append(payload)
    with tempfile.TemporaryDirectory(prefix="kn-trade-wire-") as directory:
        root = Path(directory)
        process = await asyncio.create_subprocess_exec(
            args.driver, args.capture, args.templates, directory, "4000", "1", str(args.port),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        tasks = []
        try:
            async with asyncio.timeout(30):
                while not (root / "ready.json").exists():
                    if process.returncode is not None:
                        raise RuntimeError("driver exited before readiness")
                    await asyncio.sleep(0.02)
                packet = json.loads((root / "ready.json").read_text())
                assert set(packet["total_per_uid"]) == set(expected)
                ready = {uid: asyncio.Event() for uid in expected}
                metadata = (("authorization", "Bearer " + packet["token"]),
                            ("x-qdl-consumer-id", packet["consumer_id"]),
                            ("x-qdl-purpose", packet["purpose"]))
                async with grpc.aio.insecure_channel(packet["target"]) as channel:
                    rpc = channel.unary_stream(
                        "/qdl.query.v2.MarketDataStreamService/Subscribe",
                        request_serializer=pb.SubscribeRequest.SerializeToString,
                        response_deserializer=pb.SubscribeResponse.FromString)

                    async def consume(route):
                        uid = route["instrument_uid"]
                        req = pb.DataRequirement.FromString(base64.b64decode(route["requirement_proto_b64"]))
                        call = rpc(pb.SubscribeRequest(consumer_id=packet["consumer_id"], requirement=req,
                                   cursor_token=route["cursor"], max_buffer_events=1000), metadata=metadata)
                        count = 0
                        try:
                            async for response in call:
                                record = response.record
                                if record.HasField("control"):
                                    if pb.StreamControlState.Name(record.control.state).endswith("LIVE"):
                                        ready[uid].set()
                                    continue
                                if not record.HasField("event"):
                                    raise AssertionError("unexpected record")
                                assert record.logical_offset == count + 1
                                assert record.event.SerializeToString(deterministic=True) == expected[uid][count]
                                count += 1
                                if count == len(expected[uid]):
                                    return count
                            raise AssertionError("early stream end")
                        finally:
                            call.cancel()

                    tasks = [asyncio.create_task(consume(route)) for route in packet["routes"]]
                    await asyncio.gather(*(event.wait() for event in ready.values()))
                    (root / "start").touch()
                    counts = await asyncio.gather(*tasks)
                (root / "stop").touch()
                assert await process.wait() == 0
                metrics = json.loads((root / "gateway.json").read_text())
                assert metrics["overflow"] == metrics["reader_errors"] == metrics["decode_errors"] == 0
                print(json.dumps({"pass": True, "scope": "wire-only; no SDK/apply/ACK claim",
                                  "products": len(counts), "events": sum(counts),
                                  "producer": json.loads((root / "producer.json").read_text()),
                                  "gateway": metrics}))
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if process.returncode is None:
                process.terminate()
                await process.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", required=True)
    parser.add_argument("--capture", required=True)
    parser.add_argument("--templates", required=True)
    parser.add_argument("--port", type=int, default=18229)
    asyncio.run(run(parser.parse_args()))
