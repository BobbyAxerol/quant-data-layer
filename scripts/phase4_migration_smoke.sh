#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="qdl-phase4-postgres-${$}"
password="phase4-disposable-only"

cleanup() {
  docker rm -f "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --name "${container}" \
  --tmpfs /var/lib/postgresql/data:rw,noexec,nosuid,size=512m \
  -e POSTGRES_PASSWORD="${password}" \
  -v "${root_dir}/migrations/postgres:/migrations:ro" \
  postgres:16-alpine >/dev/null

ready=false
for _ in $(seq 1 240); do
  if docker exec "${container}" pg_isready -U postgres >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 0.25
done
if [[ "${ready}" != "true" ]]; then
  echo "phase4 disposable PostgreSQL did not become ready within 60 seconds" >&2
  docker logs "${container}" >&2 || true
  exit 1
fi

for database in qdl_phase4_clean qdl_phase4_existing; do
  docker exec "${container}" createdb -U postgres "${database}"
done
docker exec "${container}" psql -U postgres -d qdl_phase4_existing -v ON_ERROR_STOP=1 \
  -c "CREATE TABLE legacy_history_state(id text primary key); INSERT INTO legacy_history_state VALUES ('preserve-me');" >/dev/null

for database in qdl_phase4_clean qdl_phase4_existing; do
  for _ in 1 2; do
    for migration in \
      0001_phase1_control_plane.sql \
      0002_phase1_seed_calendars.sql \
      0002_phase3_ingestion.sql \
      0003_phase4_quality_history.sql; do
      docker exec "${container}" psql -U postgres -d "${database}" \
        -v ON_ERROR_STOP=1 -f "/migrations/${migration}" >/dev/null
    done
  done
  tables="$(docker exec "${container}" psql -U postgres -d "${database}" -Atc \
    "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'qdl_%';")"
  functions="$(docker exec "${container}" psql -U postgres -d "${database}" -Atc \
    "SELECT count(*) FROM pg_proc WHERE proname LIKE 'qdl_%ingestion_lease';")"
  if [[ "${tables}" != "16" || "${functions}" != "3" ]]; then
    echo "phase4 migration mismatch database=${database} tables=${tables} functions=${functions}" >&2
    exit 1
  fi
done

legacy="$(docker exec "${container}" psql -U postgres -d qdl_phase4_existing -Atc \
  "SELECT count(*) FROM legacy_history_state WHERE id='preserve-me';")"
if [[ "${legacy}" != "1" ]]; then
  echo "phase4 migration changed legacy data" >&2
  exit 1
fi

echo "phase4 migration smoke: PASS (clean/existing, second apply, legacy preserved, 16 tables, 3 lease functions)"
