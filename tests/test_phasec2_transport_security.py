from __future__ import annotations

import ssl
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import grpc
import httpx

from qdl_sdk import GrpcStreamTransport, RestQueryTransport, WorkloadTlsConfig


class WorkloadTlsConfigTests(unittest.IsolatedAsyncioTestCase):
    def identity(self, root: Path) -> WorkloadTlsConfig:
        paths = []
        for name in ("ca.crt", "client.crt", "client.key"):
            path = root / name
            path.write_text("test-only", encoding="utf-8")
            paths.append(path)
        return WorkloadTlsConfig(*paths)

    async def test_missing_identity_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaisesRegex(ValueError, "TLS files are unavailable"):
                WorkloadTlsConfig(
                    root / "missing-ca",
                    root / "missing-cert",
                    root / "missing-key",
                )

    async def test_rest_and_grpc_share_one_workload_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            tls = self.identity(Path(temp))
            context = ssl.create_default_context()
            credentials = grpc.ssl_channel_credentials()
            with (
                mock.patch.object(
                    WorkloadTlsConfig, "ssl_context", return_value=context
                ) as rest_tls,
                mock.patch.object(
                    WorkloadTlsConfig,
                    "grpc_credentials",
                    return_value=credentials,
                ) as grpc_tls,
            ):
                rest = RestQueryTransport("https://qdl-v2-query:8200", tls=tls)
                stream = GrpcStreamTransport(
                    ("qdl-v2-stream-a:8210", "qdl-v2-stream-b:8210"),
                    tls=tls,
                )
                try:
                    self.assertEqual(
                        stream.targets,
                        ("qdl-v2-stream-a:8210", "qdl-v2-stream-b:8210"),
                    )
                    self.assertEqual(stream.target, "qdl-v2-stream-a:8210")
                    rest_tls.assert_called_once_with()
                    grpc_tls.assert_called_once_with()
                finally:
                    await rest.close()
                    await stream.close()


    async def test_rs256_token_is_cached_rotated_and_verified(self):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from qdl.security.policy import ServiceTokenVerifier
        from qdl_sdk import RotatingJwtCredentialProvider

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            private_path = root / "private.key"
            private_path.write_bytes(key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ))
            public_key = key.public_key().public_bytes(
                serialization.Encoding.PEM,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            now = [int(time.time())]
            provider = RotatingJwtCredentialProvider(
                private_key_file=private_path,
                key_id="workload-rs256-v1",
                algorithm="RS256",
                issuer="https://identity.qdl",
                audience="qdl-v2",
                subject="spiffe://qdl/paper/trading-system-stable",
                environment="paper",
                roles=("market_data_reader", "stream_consumer"),
                venues=("BINANCE", "OKX"),
                consumer_manifest_revision=1,
                lifetime_seconds=600,
                refresh_before_seconds=120,
                clock=lambda: now[0],
            )
            first = await provider.get_token()
            self.assertEqual(first, await provider.get_token())
            principal = ServiceTokenVerifier(
                issuer="https://identity.qdl",
                audience="qdl-v2",
                keys_by_id={"workload-rs256-v1": public_key},
                algorithms=("RS256",),
                max_lifetime_seconds=900,
            ).verify(first, expected_environment="paper")
            self.assertEqual(
                principal.subject,
                "spiffe://qdl/paper/trading-system-stable",
            )
            self.assertEqual(principal.venues, frozenset({"BINANCE", "OKX"}))
            now[0] += 500
            self.assertNotEqual(first, await provider.get_token())

    async def test_ambiguous_tls_clients_and_duplicate_targets_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            tls = self.identity(Path(temp))
            async with httpx.AsyncClient() as client:
                with self.assertRaisesRegex(ValueError, "either REST client"):
                    RestQueryTransport(
                        "https://qdl-v2-query:8200", client=client, tls=tls
                    )
            with self.assertRaisesRegex(ValueError, "non-empty and unique"):
                GrpcStreamTransport(
                    ("127.0.0.1:18220", "127.0.0.1:18220"),
                    allow_insecure_loopback=True,
                )
            with self.assertRaisesRegex(ValueError, "only for explicit loopback"):
                GrpcStreamTransport(
                    ("stream-a:8210", "stream-b:8210"),
                    allow_insecure_loopback=True,
                )


if __name__ == "__main__":
    unittest.main()
