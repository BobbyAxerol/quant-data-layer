#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import time
from pathlib import Path

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


def write_health(
    path: Path, *, status: str, published: int, error: str | None = None
) -> None:
    if status not in {"STARTING", "READY", "DEGRADED"} or published < 0:
        raise ValueError("authority dispatcher health payload is invalid")
    payload = {
        "schema": "qdl.authority-dispatcher-health.v1",
        "status": status,
        "heartbeat_ns": time.time_ns(),
        "published_last_cycle": published,
        "error": error[:1000] if error else None,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


async def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-seconds", type=float, default=0.5)
    parser.add_argument("--batch-size", type=int, default=20)
    args = parser.parse_args()
    if not 0.05 <= args.poll_seconds <= 60:
        raise ValueError("poll interval is outside bounds")
    health_path = Path(required("QDL_AUTHORITY_HEALTH_FILE"))
    write_health(health_path, status="STARTING", published=0)
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
            write_health(health_path, status="READY", published=count)
            print(json.dumps({"event": "qdl_authority_outbox_dispatch", "published": count}))
            if args.once:
                return
            await asyncio.sleep(args.poll_seconds if count == 0 else 0)
    except Exception as error:
        write_health(
            health_path, status="DEGRADED", published=0, error=str(error)
        )
        raise
    finally:
        await repository.close()


if __name__ == "__main__":
    asyncio.run(run())
