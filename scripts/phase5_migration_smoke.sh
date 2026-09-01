#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="qdl-phase5-postgres-${$}"
password="phase5-disposable-only"
postgres_image="${QDL_PHASE5_POSTGRES_IMAGE:-postgres:16-alpine}"

cleanup() {
  docker rm -f "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! docker image inspect "${postgres_image}" >/dev/null 2>&1; then
  image_ready=false
  for attempt in 1 2 3; do
    if docker pull "${postgres_image}"; then
      image_ready=true
      break
    fi
    sleep "${attempt}"
  done
  if [[ "${image_ready}" != "true" ]]; then
    echo "phase5 could not pull PostgreSQL image after 3 attempts" >&2
    exit 1
  fi
fi

container_started=false
for attempt in 1 2 3; do
  docker rm -f "${container}" >/dev/null 2>&1 || true
  if docker run -d --name "${container}" \
      --tmpfs /var/lib/postgresql/data:rw,noexec,nosuid,size=512m \
      -e POSTGRES_PASSWORD="${password}" \
      -v "${root_dir}/migrations/postgres:/migrations:ro" \
      "${postgres_image}" >/dev/null; then
    container_started=true
    break
  fi
  sleep "${attempt}"
done
if [[ "${container_started}" != "true" ]]; then
  echo "phase5 disposable PostgreSQL did not start after 3 attempts" >&2
  exit 1
fi

ready=false
consecutive_ready=0
for _ in $(seq 1 240); do
  if docker exec "${container}" psql -U postgres -d postgres -Atc "SELECT 1" \
      2>/dev/null | grep -qx "1"; then
    consecutive_ready=$((consecutive_ready + 1))
    if [[ "${consecutive_ready}" -ge 8 ]]; then
      ready=true
      break
    fi
  else
    consecutive_ready=0
  fi
  sleep 0.25
done
if [[ "${ready}" != "true" ]]; then
  echo "phase5 disposable PostgreSQL did not become ready within 60 seconds" >&2
  docker logs "${container}" >&2 || true
  exit 1
fi

for database in qdl_phase5_clean qdl_phase5_existing; do
  docker exec "${container}" createdb -U postgres "${database}"
done
docker exec "${container}" psql -U postgres -d qdl_phase5_existing -v ON_ERROR_STOP=1 \
  -c "CREATE TABLE legacy_v1_state(id text primary key); INSERT INTO legacy_v1_state VALUES ('preserve-me');" >/dev/null

for database in qdl_phase5_clean qdl_phase5_existing; do
  for _ in 1 2; do
    for migration in \
      0001_phase1_control_plane.sql \
      0002_phase1_seed_calendars.sql \
      0002_phase3_ingestion.sql \
      0003_phase4_quality_history.sql \
      0004_phase5_consumers.sql \
      0005_phase7_data_plane_identity.sql; do
      docker exec "${container}" psql -U postgres -d "${database}" \
        -v ON_ERROR_STOP=1 -f "/migrations/${migration}" >/dev/null
    done
  done
  tables="$(docker exec "${container}" psql -U postgres -d "${database}" -Atc \
    "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'qdl_%';")"
  functions="$(docker exec "${container}" psql -U postgres -d "${database}" -Atc \
    "SELECT count(*) FROM pg_proc WHERE proname LIKE 'qdl_%ingestion_lease';")"
  constraints="$(docker exec "${container}" psql -U postgres -d "${database}" -Atc \
    "SELECT count(*) FROM information_schema.table_constraints WHERE table_schema='public' AND table_name IN ('qdl_consumer_manifests','qdl_data_requirements','qdl_consumer_migrations','qdl_consumer_contract_usage_hourly','qdl_consumer_manifest_access');")"
  if [[ "${tables}" != "21" || "${functions}" != "3" || "${constraints}" -lt "15" ]]; then
    echo "phase5 migration mismatch database=${database} tables=${tables} functions=${functions} constraints=${constraints}" >&2
    exit 1
  fi
done

docker exec "${container}" psql -U postgres -d qdl_phase5_clean -v ON_ERROR_STOP=1 \
  -c "INSERT INTO qdl_consumer_manifests(consumer_id,manifest_sha256,owner,sdk_major,rollback_contract,manifest) VALUES ('phase7-smoke',repeat('a',64),'test',2,'V1','{}'); INSERT INTO qdl_consumer_manifest_access(consumer_id,manifest_sha256,subject,environment,manifest_revision,allowed_purposes,allowed_permissions,execution_dependency,quotas) VALUES ('phase7-smoke',repeat('a',64),'spiffe://qdl/test/phase7','paper',1,'[\"INTERNAL_ALPHA\"]','[\"snapshot:read\"]','FORBIDDEN','{\"requests_per_minute\":10}');" >/dev/null
access_rows="$(docker exec "${container}" psql -U postgres -d qdl_phase5_clean -Atc \
  "SELECT count(*) FROM qdl_consumer_manifest_access WHERE consumer_id='phase7-smoke' AND manifest_revision=1;")"
if [[ "${access_rows}" != "1" ]]; then
  echo "phase7 manifest access binding was not persisted" >&2
  exit 1
fi

legacy="$(docker exec "${container}" psql -U postgres -d qdl_phase5_existing -Atc \
  "SELECT count(*) FROM legacy_v1_state WHERE id='preserve-me';")"
if [[ "${legacy}" != "1" ]]; then
  echo "phase5 migration changed legacy V1 data" >&2
  exit 1
fi

echo "phase5+phase7 migration smoke: PASS (clean/existing, idempotent, legacy preserved, 21 tables, manifest access FK, 3 lease functions)"
