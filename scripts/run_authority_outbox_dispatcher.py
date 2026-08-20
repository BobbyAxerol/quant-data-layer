#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket

from qdl.control.authority_outbox import (
    AsyncpgAuthorityOutboxRepository,
    AuthorityOutboxDispatcher,
    KafkaAuthorityPublisher,
)


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"required environment variable is missing: {name}")
    return value


async def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-seconds", type=float, default=0.5)
    parser.add_argument("--batch-size", type=int, default=20)
    args = parser.parse_args()
    if not 0.05 <= args.poll_seconds <= 60:
        raise ValueError("poll interval is outside bounds")
    repository = await AsyncpgAuthorityOutboxRepository.connect(required("QDL_CONTROL_DB_DSN"))
    publisher = KafkaAuthorityPublisher(
        {
            "bootstrap.servers": required("QDL_KAFKA_BOOTSTRAP_SERVERS"),
            "client.id": required("QDL_KAFKA_CLIENT_ID"),
            "security.protocol": "ssl",
            "ssl.ca.location": required("QDL_KAFKA_CA_LOCATION"),
            "ssl.certificate.location": required("QDL_KAFKA_CERT_LOCATION"),
            "ssl.key.location": required("QDL_KAFKA_KEY_LOCATION"),
        },
        topic=required("QDL_AUTHORITY_TOPIC"),
    )
    dispatcher = AuthorityOutboxDispatcher(
        repository=repository,
        publisher=publisher,
        lock_owner=f"{socket.gethostname()}:{os.getpid()}",
        batch_size=args.batch_size,
    )
    try:
        while True:
            count = await dispatcher.dispatch_once()
            print(json.dumps({"event": "qdl_authority_outbox_dispatch", "published": count}))
            if args.once:
                return
            await asyncio.sleep(args.poll_seconds if count == 0 else 0)
    finally:
        await repository.close()


if __name__ == "__main__":
    asyncio.run(run())
