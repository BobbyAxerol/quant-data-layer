from __future__ import annotations

import ssl
from dataclasses import dataclass
from pathlib import Path

import grpc


@dataclass(frozen=True, slots=True)
class WorkloadTlsConfig:
    """Mutual-TLS identity shared by the REST and gRPC transports."""

    ca_file: str | Path
    certificate_file: str | Path
    private_key_file: str | Path

    def __post_init__(self) -> None:
        paths = self.paths()
        missing = [name for name, path in paths.items() if not path.is_file()]
        if missing:
            raise ValueError(
                "Data Layer workload TLS files are unavailable: "
                + ",".join(sorted(missing))
            )

    def paths(self) -> dict[str, Path]:
        return {
            "ca_file": Path(self.ca_file).expanduser().resolve(),
            "certificate_file": Path(self.certificate_file).expanduser().resolve(),
            "private_key_file": Path(self.private_key_file).expanduser().resolve(),
        }

    def ssl_context(self) -> ssl.SSLContext:
        paths = self.paths()
        context = ssl.create_default_context(cafile=str(paths["ca_file"]))
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(
            certfile=str(paths["certificate_file"]),
            keyfile=str(paths["private_key_file"]),
        )
        return context

    def grpc_credentials(self) -> grpc.ChannelCredentials:
        paths = self.paths()
        return grpc.ssl_channel_credentials(
            root_certificates=paths["ca_file"].read_bytes(),
            private_key=paths["private_key_file"].read_bytes(),
            certificate_chain=paths["certificate_file"].read_bytes(),
        )
