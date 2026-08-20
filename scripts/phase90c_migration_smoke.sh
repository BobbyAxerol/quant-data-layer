#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_IMAGE="${QDL_PHASE90C_POSTGRES_IMAGE:-timescale/timescaledb:latest-pg15}"
CONTAINER="${QDL_PHASE90C_POSTGRES_CONTAINER:-qdl_phase90c_postgres_$$}"
OUTPUT="${QDL_PHASE90C_MIGRATION_OUTPUT:-${ROOT_DIR}/upgrade/evidence/phase90c-authority-migration.json}"
CANDIDATE_DIGEST="${QDL_PHASE90C_CANDIDATE_DIGEST:-$(
  PYTHONPATH="${ROOT_DIR}" python3 -c     "from qdl.certification.prerequisites import CandidateSlice; print(CandidateSlice.load('${ROOT_DIR}/config/phase9/candidate-slice.yaml').digest)"
)}"

cleanup() {
  docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --name "${CONTAINER}" --network none \
  --security-opt no-new-privileges:true --pids-limit 256 --memory 768m --cpus 1.0 \
  --tmpfs /var/lib/postgresql/data:rw,nosuid,nodev,size=512m \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -v "${ROOT_DIR}/migrations/postgres:/migrations:ro" \
  "${POSTGRES_IMAGE}" >/dev/null

for _ in $(seq 1 60); do
  if docker exec "${CONTAINER}" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "${CONTAINER}" pg_isready -U postgres -d postgres >/dev/null

apply_migrations() {
  local migration
  for migration in "${ROOT_DIR}"/migrations/postgres/*.sql; do
    docker exec "${CONTAINER}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres \
      -f "/migrations/$(basename "${migration}")" >/dev/null
  done
}

apply_migrations

docker exec -i "${CONTAINER}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres <<SQL >/dev/null
INSERT INTO qdl_authority_slices (
  slice_id, environment, venue, market, product_type, feed,
  partition_plan_epoch, partition_id, schema_major, state,
  authority_revision, owner_id, lease_epoch, candidate_digest,
  artifact_image_digest, sbom_digest, signature_identity, contract_digest,
  normalizer_version, adapter_version, config_revision,
  instrument_catalog_revision, source_policy_revision,
  partition_plan_digest, rollback_manifest_digest
) VALUES (
  'production/binance/usdm/perpetual/trade/plan-1/btcusdt',
  'production', 'BINANCE', 'USDM', 'PERPETUAL', 'TRADE',
  1, 'rendezvous-sha256-v1:epoch-1:btcusdt', 2, 'RUST_SHADOW',
  1, 'rust-shadow-owner', 1, '${CANDIDATE_DIGEST}',
  'sha256:${CANDIDATE_DIGEST}', repeat('1',64), 'phase90c-test-signer', repeat('2',64),
  'qdl-rust-core/test', 'binance-usdm/test', 'phase90c-test-config',
  'phase90c-test-catalog', 'phase90c-test-source-policy',
  repeat('3',64), repeat('4',64)
);

SELECT (qdl_transition_authority(
  '11111111-1111-4111-8111-111111111111',
  'production/binance/usdm/perpetual/trade/plan-1/btcusdt',
  'RUST_SHADOW', 1, 'rust-shadow-owner', 1, 1,
  'VALIDATING', 'rust-shadow-owner', 1, NULL, NULL, NULL,
  'phase90c-test', 'enter isolated validation'
)).state;

INSERT INTO qdl_production_prerequisite_bundles (
  bundle_id, candidate_digest, policy_revision, decision, evidence,
  evidence_sha256, issued_by, issued_at, expires_at
) VALUES
  ('00000000-0000-4000-8000-000000000001', '${CANDIDATE_DIGEST}', 1,
   'NO_GO_EXTERNAL', '{}', repeat('5',64), 'phase90c-test', clock_timestamp(),
   clock_timestamp() + interval '1 day'),
  ('00000000-0000-4000-8000-000000000002', repeat('9',64), 1,
   'GO', '{}', repeat('6',64), 'phase90c-test', clock_timestamp(),
   clock_timestamp() + interval '1 day'),
  ('00000000-0000-4000-8000-000000000003', '${CANDIDATE_DIGEST}', 1,
   'GO', '{}', repeat('7',64), 'phase90c-test', clock_timestamp(),
   clock_timestamp() + interval '1 day');
SQL

expect_failure() {
  if docker exec "${CONTAINER}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres \
    -c "$1" >/dev/null 2>&1; then
    printf 'expected SQL failure but statement succeeded\n' >&2
    exit 1
  fi
}

expect_failure "SELECT qdl_transition_authority('31111111-1111-4111-8111-111111111111','production/binance/usdm/perpetual/trade/plan-1/btcusdt','RUST_SHADOW',1,'rust-shadow-owner',1,1,'VALIDATING','rust-shadow-owner',1,NULL,NULL,NULL,'phase90c-test','stale CAS');"
expect_failure "SELECT qdl_transition_authority('32222222-2222-4222-8222-222222222222','production/binance/usdm/perpetual/trade/plan-1/btcusdt','VALIDATING',2,'rust-shadow-owner',1,1,'RUST_CANARY','rust-canary-owner',2,123456,'00000000-0000-4000-8000-000000000001',clock_timestamp()+interval '1 hour','phase90c-test','blocked bundle');"
expect_failure "SELECT qdl_transition_authority('33333333-3333-4333-8333-333333333333','production/binance/usdm/perpetual/trade/plan-1/btcusdt','VALIDATING',2,'rust-shadow-owner',1,1,'RUST_CANARY','rust-canary-owner',2,123456,'00000000-0000-4000-8000-000000000002',clock_timestamp()+interval '1 hour','phase90c-test','wrong candidate');"

expect_failure "SELECT qdl_transition_authority('35555555-5555-4555-8555-555555555555','production/binance/usdm/perpetual/trade/plan-1/btcusdt','VALIDATING',2,'rust-shadow-owner',1,1,'RUST_CANARY','rust-canary-owner',2,NULL,'00000000-0000-4000-8000-000000000003',clock_timestamp()+interval '1 hour','phase90c-test','missing terminal watermark');"
expect_failure "SELECT qdl_transition_authority('36666666-6666-4666-8666-666666666666','production/binance/usdm/perpetual/trade/plan-1/btcusdt','VALIDATING',2,'rust-shadow-owner',1,1,'RUST_CANARY','rust-canary-owner',2,123456,'00000000-0000-4000-8000-000000000003',NULL,'phase90c-test','missing hold window');"
expect_failure "SELECT qdl_transition_authority('37777777-7777-4777-8777-777777777777','production/binance/usdm/perpetual/trade/plan-1/btcusdt','VALIDATING',2,'rust-shadow-owner',1,1,'RUST_CANARY','rust-canary-owner',2,123456,'00000000-0000-4000-8000-000000000003',clock_timestamp()+interval '2 days','phase90c-test','hold exceeds evidence expiry');"

expect_failure "SELECT qdl_transition_authority('38888888-8888-4888-8888-888888888888','production/binance/usdm/perpetual/trade/plan-1/btcusdt','VALIDATING',2,'rust-shadow-owner',1,1,'BLOCKED','rust-shadow-owner',1,NULL,'00000000-0000-4000-8000-000000000003',clock_timestamp()+interval '1 hour','phase90c-test','bundle on non-authority state');"

docker exec "${CONTAINER}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres \
  -c "SELECT qdl_transition_authority('22222222-2222-4222-8222-222222222222','production/binance/usdm/perpetual/trade/plan-1/btcusdt','VALIDATING',2,'rust-shadow-owner',1,1,'RUST_CANARY','rust-canary-owner',2,123456,'00000000-0000-4000-8000-000000000003',clock_timestamp()+interval '1 hour','phase90c-test','approved isolated CAS test');" >/dev/null

expect_failure "SELECT qdl_transition_authority('34444444-4444-4444-8444-444444444444','production/binance/usdm/perpetual/trade/plan-1/btcusdt','RUST_CANARY',3,'rust-shadow-owner',2,1,'BLOCKED','rust-shadow-owner',2,NULL,NULL,NULL,'phase90c-test','stale owner');"
expect_failure "UPDATE qdl_authority_transition_audit SET reason='mutated';"

apply_migrations

state="$(docker exec "${CONTAINER}" psql -U postgres -d postgres -Atc "SELECT state || ':' || authority_revision || ':' || owner_id || ':' || lease_epoch FROM qdl_authority_slices;")"
audit_count="$(docker exec "${CONTAINER}" psql -U postgres -d postgres -Atc "SELECT count(*) FROM qdl_authority_transition_audit;")"
bundle_count="$(docker exec "${CONTAINER}" psql -U postgres -d postgres -Atc "SELECT count(*) FROM qdl_production_prerequisite_bundles;")"
[[ "${state}" == "RUST_CANARY:3:rust-canary-owner:2" ]]
[[ "${audit_count}" == "2" ]]
[[ "${bundle_count}" == "3" ]]

python3 - "${OUTPUT}" "${state}" "${audit_count}" "${bundle_count}" <<'PY'
import json, pathlib, sys
output, state, audits, bundles = sys.argv[1:]
path = pathlib.Path(output); path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps({
    "schema": "qdl.phase9.0-c.authority-migration.v1",
    "status": "PASS",
    "final_test_state": state,
    "audit_records": int(audits),
    "prerequisite_bundles": int(bundles),
    "stale_cas_rejected": True,
    "no_go_bundle_rejected": True,
    "candidate_mismatch_rejected": True,
    "missing_terminal_watermark_rejected": True,
    "missing_hold_window_rejected": True,
    "hold_beyond_bundle_expiry_rejected": True,
    "bundle_on_non_authority_state_rejected": True,
    "stale_owner_rejected": True,
    "audit_mutation_rejected": True,
    "idempotent_migration": True,
    "production_mutations": 0,
}, indent=2, sort_keys=True) + "\n")
PY

cleanup
trap - EXIT
[[ -z "$(docker ps -aq --filter name=^/${CONTAINER}$)" ]]
printf '{"status":"PASS","state":"%s","audits":%s,"cleanup":true}\n' \
  "${state}" "${audit_count}"
