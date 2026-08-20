#!/usr/bin/env bash
set -euo pipefail

: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${QDL_STABLE_DISPATCHER_DB_PASSWORD:?QDL_STABLE_DISPATCHER_DB_PASSWORD is required}"

psql --set=ON_ERROR_STOP=1 \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB}" \
  --set=dispatcher_password="${QDL_STABLE_DISPATCHER_DB_PASSWORD}" <<'SQL'
SELECT format(
  'CREATE ROLE qdl_authority_dispatcher LOGIN PASSWORD %L',
  :'dispatcher_password'
)
WHERE NOT EXISTS (
  SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'qdl_authority_dispatcher'
)
\gexec
ALTER ROLE qdl_authority_dispatcher
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION
  PASSWORD :'dispatcher_password';
GRANT CONNECT ON DATABASE qdl_authority TO qdl_authority_dispatcher;
GRANT USAGE ON SCHEMA public TO qdl_authority_dispatcher;
GRANT EXECUTE ON FUNCTION qdl_claim_authority_outbox(TEXT, INTEGER, INTERVAL)
  TO qdl_authority_dispatcher;
GRANT EXECUTE ON FUNCTION qdl_complete_authority_outbox(UUID, TEXT, TEXT, INTEGER, BIGINT)
  TO qdl_authority_dispatcher;
GRANT EXECUTE ON FUNCTION qdl_retry_authority_outbox(UUID, TEXT, TEXT, INTERVAL)
  TO qdl_authority_dispatcher;
SQL
