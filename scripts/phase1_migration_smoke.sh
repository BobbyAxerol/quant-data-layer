#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-timescale/timescaledb:latest-pg15}"
CONTAINER_NAME="qdl-phase1-postgres-${$}"
PASSWORD="phase1-disposable-only"

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d \
  --name "${CONTAINER_NAME}" \
  --tmpfs /var/lib/postgresql/data:rw,noexec,nosuid,size=512m \
  -e POSTGRES_PASSWORD="${PASSWORD}" \
  -v "${ROOT_DIR}/migrations/postgres:/migrations:ro" \
  "${POSTGRES_IMAGE}" >/dev/null

consecutive_ready=0
for _ in $(seq 1 90); do
  if docker exec "${CONTAINER_NAME}" psql -U postgres -d postgres -Atc "SELECT 1" >/dev/null 2>&1; then
    consecutive_ready=$((consecutive_ready + 1))
    if [[ "${consecutive_ready}" -ge 3 ]]; then
      break
    fi
  else
    consecutive_ready=0
  fi
  sleep 1
done
if [[ "${consecutive_ready}" -lt 3 ]]; then
  docker logs "${CONTAINER_NAME}" >&2
  exit 1
fi

schema_hashes=()
for database in qdl_phase1_clean qdl_phase1_existing; do
  docker exec "${CONTAINER_NAME}" createdb -U postgres "${database}"
done
docker exec "${CONTAINER_NAME}" psql -U postgres -d qdl_phase1_existing -v ON_ERROR_STOP=1 \
  -c "CREATE TABLE legacy_consumer_state (consumer_id text PRIMARY KEY); INSERT INTO legacy_consumer_state VALUES ('preserve-me');" \
  >/dev/null

for database in qdl_phase1_clean qdl_phase1_existing; do
  for _ in 1 2; do
    docker exec "${CONTAINER_NAME}" psql -U postgres -d "${database}" -v ON_ERROR_STOP=1 \
      -f /migrations/0001_phase1_control_plane.sql >/dev/null
    docker exec "${CONTAINER_NAME}" psql -U postgres -d "${database}" -v ON_ERROR_STOP=1 \
      -f /migrations/0002_phase1_seed_calendars.sql >/dev/null
  done
  table_count="$(docker exec "${CONTAINER_NAME}" psql -U postgres -d "${database}" -Atc \
    "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'qdl_%';")"
  calendar_count="$(docker exec "${CONTAINER_NAME}" psql -U postgres -d "${database}" -Atc \
    "SELECT count(*) FROM qdl_session_calendars;")"
  if [[ "${table_count}" != "11" || "${calendar_count}" != "2" ]]; then
    echo "migration validation failed database=${database} tables=${table_count} calendars=${calendar_count}" >&2
    exit 1
  fi
  schema_sha="$(docker exec "${CONTAINER_NAME}" pg_dump -U postgres -d "${database}" --schema-only --no-owner --no-privileges --table='public.qdl_*' \
    | sha256sum | awk '{print $1}')"
  schema_hashes+=("${schema_sha}")
  echo "database=${database} qdl_tables=${table_count} calendars=${calendar_count} schema_sha256=${schema_sha}"
done

if [[ "${schema_hashes[0]}" != "${schema_hashes[1]}" ]]; then
  echo "migration schema differs between clean and existing database cases" >&2
  exit 1
fi

legacy_count="$(docker exec "${CONTAINER_NAME}" psql -U postgres -d qdl_phase1_existing -Atc \
  "SELECT count(*) FROM legacy_consumer_state WHERE consumer_id='preserve-me';")"
if [[ "${legacy_count}" != "1" ]]; then
  echo "existing database compatibility failed: legacy row was not preserved" >&2
  exit 1
fi

echo "phase1 migration smoke: PASS (clean, existing, second-apply idempotence, legacy preservation)"
