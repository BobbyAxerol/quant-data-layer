#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="qdl_c3_authority_db_smoke_$$"
IMAGE="postgres@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685"
OWNER_PASSWORD="c3-owner-test-only"
DISPATCHER_PASSWORD="c3-dispatcher-test-only"

cleanup() {
  docker stop "${NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm -d   --name "${NAME}"   --network none   --tmpfs /var/lib/postgresql/data:rw,nosuid,nodev,size=256m   --tmpfs /run/postgresql:rw,nosuid,nodev,size=8m   -e POSTGRES_DB=qdl_authority   -e POSTGRES_USER=qdl_authority   -e POSTGRES_PASSWORD="${OWNER_PASSWORD}"   -e QDL_STABLE_DISPATCHER_DB_PASSWORD="${DISPATCHER_PASSWORD}"   -e PGDATA=/var/lib/postgresql/data/pgdata   -v "${ROOT_DIR}/migrations/postgres:/docker-entrypoint-initdb.d:ro"   "${IMAGE}" >/dev/null

for _attempt in $(seq 1 60); do
  if docker exec "${NAME}" pg_isready       -U qdl_authority -d qdl_authority >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "${NAME}" pg_isready   -U qdl_authority -d qdl_authority >/dev/null

owner_query() {
  docker exec -e PGPASSWORD="${OWNER_PASSWORD}" "${NAME}"     psql --set=ON_ERROR_STOP=1 -At     -U qdl_authority -d qdl_authority -c "$1"
}

dispatcher_query() {
  docker exec -e PGPASSWORD="${DISPATCHER_PASSWORD}" "${NAME}"     psql --set=ON_ERROR_STOP=1 -At     -U qdl_authority_dispatcher -d qdl_authority -c "$1"
}

[[ "$(owner_query "SELECT count(*) FROM pg_proc WHERE proname IN ('qdl_claim_authority_outbox','qdl_complete_authority_outbox','qdl_retry_authority_outbox') AND prosecdef")" == "3" ]]
[[ "$(owner_query "SELECT count(*) FROM pg_proc WHERE proname IN ('qdl_claim_authority_outbox','qdl_complete_authority_outbox','qdl_retry_authority_outbox') AND 'search_path=pg_catalog, public'=ANY(proconfig)")" == "3" ]]
[[ "$(owner_query "SELECT rolsuper::int||':'||rolcreatedb::int||':'||rolcreaterole::int FROM pg_roles WHERE rolname='qdl_authority_dispatcher'")" == "0:0:0" ]]
[[ "$(owner_query "SELECT has_function_privilege('qdl_authority_dispatcher','qdl_claim_authority_outbox(text,integer,interval)','EXECUTE')::int")" == "1" ]]
[[ "$(owner_query "SELECT has_table_privilege('qdl_authority_dispatcher','qdl_authority_event_outbox','UPDATE')::int")" == "0" ]]
[[ "$(dispatcher_query "SELECT count(*) FROM qdl_claim_authority_outbox('c3-smoke',1)")" == "0" ]]

if dispatcher_query     "UPDATE qdl_authority_event_outbox SET status='BLOCKED' WHERE false"     >/dev/null 2>&1; then
  echo "dispatcher unexpectedly obtained direct table UPDATE" >&2
  exit 1
fi

owner_query "\\i /docker-entrypoint-initdb.d/0010_authority_dispatcher_security.sql"   >/dev/null

printf '%s\n' '{"schema":"qdl.c3.authority-db-smoke.v1","status":"PASS","production_mutations":0,"cleanup":"container-auto-remove"}'
