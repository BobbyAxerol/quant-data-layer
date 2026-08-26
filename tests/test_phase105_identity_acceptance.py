from __future__ import annotations

import json
import tempfile
import unittest
import asyncio
from pathlib import Path

from qdl.certification.phase103_consumer_acceptance import AcceptanceProduct
from qdl.query import DataRequirement, FeedType, RecoveryPolicy
from qdl_sdk import Grade
from scripts.phase105_consumer_v2_identity_acceptance import (
    IDENTITY_PREFIXES,
    _authority,
    _c2_grpc_targets,
    _identity_files,
    _route_summary,
    _run_consumer_groups,
    _v1_base_url,
    parser,
)


class Phase105IdentityAcceptanceTests(unittest.TestCase):
    def test_identity_prefixes_are_exactly_the_four_governed_paper_consumers(self) -> None:
        self.assertEqual(set(IDENTITY_PREFIXES), {
            "monitoring.multivenue.stable",
            "trading-system.paper.stable",
            "alpha.binance.paper.stable",
            "alpha.okx.paper.stable",
        })

    def test_authority_requires_fenced_rust_primary(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "authority.json"
            path.write_text(json.dumps({
                "schema": "qdl.authority-record.v1",
                "mode": "RUST_PRIMARY",
                "public_write_allowed": False,
                "revision": 1,
            }), encoding="utf-8")
            self.assertEqual(_authority(path)["mode"], "RUST_PRIMARY")
            path.write_text(json.dumps({
                "schema": "qdl.authority-record.v1",
                "mode": "RUST_SHADOW",
                "public_write_allowed": False,
            }), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "RUST_PRIMARY"):
                _authority(path)

    def test_cli_parser_resolves_dashed_alpha_identity_options(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            certificate = root / "client.crt"
            private_key = root / "client.key"
            jwt_private_key = root / "private.key"
            for path in (certificate, private_key, jwt_private_key):
                path.write_text("fixture", encoding="utf-8")
            arguments = [
                "--authority-record", str(root / "authority.json"),
                "--primary-url", "https://query.example",
                "--secondary-url", "https://query-two.example",
                "--grpc-target", "stream.example:8210",
                "--v1-base-url", "http://data_layer:8100",
                "--v1-provenance", str(root / "v1-provenance.json"),
                "--v1-runtime-binding", str(root / "v1-runtime-binding.json"),
                "--tls-ca-file", str(certificate),
            ]
            for prefix in IDENTITY_PREFIXES.values():
                arguments.extend((
                    f"--{prefix}-tls-certificate-file", str(certificate),
                    f"--{prefix}-tls-private-key-file", str(private_key),
                    f"--{prefix}-jwt-private-key-file", str(jwt_private_key),
                    f"--{prefix}-jwt-key-id", f"{prefix}-key",
                ))
            files = _identity_files(parser().parse_args(arguments))
        self.assertEqual(set(files), set(IDENTITY_PREFIXES))
        self.assertEqual(files["alpha.binance.paper.stable"].jwt_key_id, "alpha-binance-key")

    def test_forced_v1_read_is_pinned_to_the_local_v1_service(self) -> None:
        self.assertEqual(_v1_base_url("http://data_layer:8100"), "http://data_layer:8100")
        for value in (
            "https://data_layer:8100",
            "http://data_layer:8101",
            "http://binance.example:8100",
            "http://data_layer:8100/v1/binance/price/BTCUSDT",
        ):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "exactly"):
                    _v1_base_url(value)

    def test_c2_requires_the_complete_stable_stream_pair(self) -> None:
        self.assertEqual(
            _c2_grpc_targets("qdl-v2-stream-a:8210,qdl-v2-stream-b:8210"),
            "qdl-v2-stream-a:8210,qdl-v2-stream-b:8210",
        )
        self.assertEqual(
            _c2_grpc_targets(" qdl-v2-stream-b:8210, qdl-v2-stream-a:8210 "),
            "qdl-v2-stream-b:8210,qdl-v2-stream-a:8210",
        )
        for value in (
            "qdl-v2-stream-a:8210",
            "qdl-v2-stream-a:8210,qdl-v2-stream-a:8210",
            "qdl-v2-stream-a:8210,other:8210",
            "qdl-v2-stream-a:8210,qdl-v2-stream-b:8210,other:8210",
        ):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "requires exactly"):
                    _c2_grpc_targets(value)

    def test_route_summary_counts_declared_v1_and_blocked_without_claiming_transition(self) -> None:
        requirement = DataRequirement(
            instrument_uid="a953e16e-7138-5562-b5e8-c337a44d0b65",
            feed=FeedType.TRADE,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            recovery=RecoveryPolicy.SNAPSHOT_AND_REPLAY,
        )
        product = AcceptanceProduct(
            consumer_id="trading-system.paper.stable",
            consumer_subject="spiffe://qdl/paper/trading-system-stable",
            manifest_revision=1,
            manifest_sha256="a" * 64,
            instrument_uid=requirement.instrument_uid,
            instrument_id="BINANCE:USDM:BTCUSDT",
            venue="BINANCE",
            market="USDM",
            native_symbol="BTCUSDT",
            provider="BINANCE_DIRECT",
            feed=FeedType.TRADE,
            interval=None,
            source_policy_id="crypto_primary_v2",
            delivery="DURABLE",
            binding_id="binance-usdm-btcusdt-trade",
            requirement=requirement,
        )

        class Route:
            def __init__(self, fallback: str): self.fallback = fallback

        class Consumer:
            consumer_id = product.consumer_id
            products = (type("Product", (), {
                "requirement_key": f"{requirement.instrument_uid}:TRADE::crypto_primary_v2",
                "fallback": "V1",
            })(),)

        class Release:
            consumers = (Consumer(),)

        self.assertEqual(_route_summary(Release(), (product,)), {
            "v1_fallback_declared": 1,
            "blocked_fallback_declared": 0,
        })


class Phase105ConcurrentConsumerGroupTests(unittest.IsolatedAsyncioTestCase):
    async def test_groups_start_in_declared_order_and_collect_in_that_order(self) -> None:
        started: list[str] = []
        release = asyncio.Event()

        async def run_group(consumer_id: str):
            started.append(consumer_id)
            await release.wait()
            return ([{"consumer": consumer_id}], [{"fallback": consumer_id}])

        task = asyncio.create_task(
            _run_consumer_groups(("monitoring", "trading", "alpha"), run_group)
        )
        for _ in range(20):
            if len(started) == 3:
                break
            await asyncio.sleep(0)
        self.assertEqual(started, ["monitoring", "trading", "alpha"])
        release.set()
        self.assertEqual(
            await task,
            (
                ([{"consumer": "monitoring"}], [{"fallback": "monitoring"}]),
                ([{"consumer": "trading"}], [{"fallback": "trading"}]),
                ([{"consumer": "alpha"}], [{"fallback": "alpha"}]),
            ),
        )


if __name__ == "__main__":
    unittest.main()
