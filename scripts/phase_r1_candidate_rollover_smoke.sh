#!/usr/bin/env bash
set -euo pipefail

# Disposable migration gate for the R1 BLOCKED-only candidate rollover. It
# never contacts the stable authority database or any Kafka/Redis service.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="qdl_r1_candidate_rollover_smoke_$$"
IMAGE="postgres@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685"
PASSWORD="r1-rollover-test-only"

cleanup() {
  docker rm -f "${NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --name "${NAME}" --network none \
  --security-opt no-new-privileges:true --pids-limit 256 --memory 512m --cpus 1.0 \
  --tmpfs /var/lib/postgresql/data:rw,nosuid,nodev,size=256m \
  --tmpfs /run/postgresql:rw,nosuid,nodev,size=8m \
  -e POSTGRES_DB=qdl_authority -e POSTGRES_USER=qdl_authority \
  -e POSTGRES_PASSWORD="${PASSWORD}" \
  -e QDL_STABLE_DISPATCHER_DB_PASSWORD=r1-rollover-dispatcher-test-only \
  -e PGDATA=/var/lib/postgresql/data/pgdata \
  -v "${ROOT_DIR}/migrations/postgres:/docker-entrypoint-initdb.d:ro" \
  "${IMAGE}" >/dev/null

for _ in $(seq 1 60); do
  if docker exec "${NAME}" pg_isready -U qdl_authority -d qdl_authority >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "${NAME}" pg_isready -U qdl_authority -d qdl_authority >/dev/null

# PostgreSQL accepts connections before docker-entrypoint has finished every
# mounted migration. Wait for the authority schema rather than treating
# pg_isready alone as migration readiness.
for _ in $(seq 1 60); do
  if docker exec -e PGPASSWORD="${PASSWORD}" "${NAME}" \
    psql -At -U qdl_authority -d qdl_authority -c \
      "SELECT to_regclass('public.qdl_authority_slices') IS NOT NULL" 2>/dev/null \
    | grep -qx 't'; then
    break
  fi
  sleep 1
done
[[ "$(docker exec -e PGPASSWORD="${PASSWORD}" "${NAME}" \
  psql -At -U qdl_authority -d qdl_authority -c \
    "SELECT to_regclass('public.qdl_authority_slices') IS NOT NULL")" == "t" ]]

query() {
  docker exec -e PGPASSWORD="${PASSWORD}" "${NAME}" \
    psql -v ON_ERROR_STOP=1 -At -U qdl_authority -d qdl_authority -c "$1"
}

query "INSERT INTO qdl_production_prerequisite_bundles (
  bundle_id,candidate_digest,policy_revision,decision,evidence,evidence_sha256,
  issued_by,issued_at,expires_at
) VALUES (
  '40000000-0000-4000-8000-000000000004',repeat('b',64),1,'GO','{}',
  repeat('9',64),'qdl-test',clock_timestamp(),clock_timestamp()+interval '1 hour'
);" >/dev/null

query "INSERT INTO qdl_authority_slices (
  slice_id,environment,venue,market,product_type,feed,partition_plan_epoch,
  partition_id,schema_major,state,authority_revision,owner_id,lease_epoch,
  terminal_watermark,candidate_digest,artifact_image_digest,sbom_digest,
  signature_identity,contract_digest,normalizer_version,adapter_version,
  config_revision,instrument_catalog_revision,source_policy_revision,
  partition_plan_digest,rollback_manifest_digest,prerequisite_bundle_id,
  approved_by,approved_at,hold_until
) VALUES (
  'production/binance/usdm/perpetual/trade/plan-1/btcusdt','production','BINANCE',
  'USDM','PERPETUAL','TRADE',1,'binance-usdm-btcusdt-trade',2,'BLOCKED',4,
  'qdl-v2-rust-canary',2,NULL,repeat('a',64),'sha256:'||repeat('c',64),
  repeat('d',64),'BobbyAxerol/quant-data-layer',repeat('e',64),'normalizer-v1',
  'adapter-v1','1','1','1',repeat('f',64),repeat('0',64),NULL,NULL,NULL,NULL
);" >/dev/null

rollover_sql="SELECT (qdl_rollover_authority_candidate(
  '50000000-0000-4000-8000-000000000005',
  'production/binance/usdm/perpetual/trade/plan-1/btcusdt',
  4,'qdl-v2-rust-canary',2,1,repeat('a',64),'sha256:'||repeat('c',64),
  'qdl-v2-rust-canary',3,
  jsonb_build_object(
    'candidate_digest',repeat('b',64),
    'artifact_image_digest','sha256:'||repeat('1',64),
    'sbom_digest',repeat('2',64),
    'signature_identity','BobbyAxerol/quant-data-layer',
    'contract_digest',repeat('e',64),
    'normalizer_version','normalizer-v1','adapter_version','adapter-v1',
    'config_revision','1','instrument_catalog_revision','1',
    'source_policy_revision','1','partition_plan_digest',repeat('f',64),
    'rollback_manifest_digest',repeat('3',64)
  ),
  '40000000-0000-4000-8000-000000000004','qdl-test','R1 test rollover'
)).state;"
[[ "$(query "${rollover_sql}")" == "BLOCKED" ]]
[[ "$(query "${rollover_sql}")" == "BLOCKED" ]]

if query "UPDATE qdl_authority_slices SET candidate_digest=repeat('7',64)" >/dev/null 2>&1; then
  echo "direct candidate provenance rewrite unexpectedly succeeded" >&2
  exit 1
fi

[[ "$(query "SELECT state||':'||authority_revision||':'||lease_epoch||':'||candidate_digest FROM qdl_authority_slices")" == "BLOCKED:5:3:$(printf 'b%.0s' {1..64})" ]]
[[ "$(query "SELECT count(*) FROM qdl_authority_candidate_rollovers")" == "1" ]]
[[ "$(query "SELECT count(*) FROM qdl_authority_transition_audit")" == "1" ]]
[[ "$(query "SELECT count(*) FROM qdl_authority_event_outbox")" == "1" ]]

printf '%s\n' '{"schema":"qdl.r1.candidate-rollover-smoke.v1","status":"PASS","idempotent":true,"direct_provenance_rewrite_rejected":true,"production_mutations":0,"cleanup":"container-removed"}'
