#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER_NAME="qdl-phase3-postgres-${$}"
POSTGRES_IMAGE="postgres:16-alpine"

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --rm --name "${CONTAINER_NAME}" \
  --tmpfs /var/lib/postgresql/data:rw,noexec,nosuid,size=256m \
  -e POSTGRES_PASSWORD=phase3 \
  -v "${ROOT_DIR}/migrations/postgres:/migrations:ro" \
  "${POSTGRES_IMAGE}" >/dev/null

for _ in $(seq 1 30); do
  if docker exec "${CONTAINER_NAME}" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "${CONTAINER_NAME}" pg_isready -U postgres -d postgres >/dev/null

for migration in "${ROOT_DIR}"/migrations/postgres/*.sql; do
  docker exec "${CONTAINER_NAME}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres \
    -f "/migrations/$(basename "${migration}")" >/dev/null
done

docker exec "${CONTAINER_NAME}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres <<'SQL' >/dev/null
INSERT INTO qdl_config_revisions(actor, reason, idempotency_key, payload_sha256)
VALUES ('phase3-test', 'lease smoke', 'phase3-lease-smoke', repeat('a', 64));

DO $$
DECLARE
  first_epoch BIGINT;
  rejected_count INTEGER;
  second_epoch BIGINT;
BEGIN
  SELECT lease_epoch INTO first_epoch
  FROM qdl_acquire_ingestion_lease('binance-usdm-trade-a', 'owner-a', 5, 1);
  IF first_epoch <> 1 THEN RAISE EXCEPTION 'unexpected first epoch'; END IF;

  SELECT count(*) INTO rejected_count
  FROM qdl_acquire_ingestion_lease('binance-usdm-trade-a', 'owner-b', 5, 1);
  IF rejected_count <> 0 THEN RAISE EXCEPTION 'concurrent owner was not rejected'; END IF;

  IF NOT qdl_renew_ingestion_lease('binance-usdm-trade-a', 'owner-a', 1, 5) THEN
    RAISE EXCEPTION 'valid owner failed renewal';
  END IF;
  IF qdl_renew_ingestion_lease('binance-usdm-trade-a', 'owner-b', 1, 5) THEN
    RAISE EXCEPTION 'wrong owner renewed lease';
  END IF;
  IF NOT qdl_release_ingestion_lease('binance-usdm-trade-a', 'owner-a', 1) THEN
    RAISE EXCEPTION 'valid owner failed release';
  END IF;

  SELECT lease_epoch INTO second_epoch
  FROM qdl_acquire_ingestion_lease('binance-usdm-trade-a', 'owner-b', 5, 1);
  IF second_epoch <> 2 THEN RAISE EXCEPTION 'takeover did not advance epoch'; END IF;
  IF qdl_renew_ingestion_lease('binance-usdm-trade-a', 'owner-a', 1, 5) THEN
    RAISE EXCEPTION 'stale owner renewed after takeover';
  END IF;
END;
$$;
SQL

echo 'phase3 lease smoke: PASS (exclusive owner, renewal, release, epoch fencing)'
