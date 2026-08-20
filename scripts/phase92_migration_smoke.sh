#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_IMAGE="${QDL_PHASE92_POSTGRES_IMAGE:-timescale/timescaledb:latest-pg15}"
CONTAINER="${QDL_PHASE92_POSTGRES_CONTAINER:-qdl_phase92_postgres_$$}"
OUTPUT="${QDL_PHASE92_MIGRATION_OUTPUT:-${ROOT_DIR}/upgrade/evidence/phase92-authority-migration.json}"
CANDIDATE_DIGEST="${QDL_PHASE92_CANDIDATE_DIGEST:-$(
  PYTHONPATH="${ROOT_DIR}" python3 -c "from qdl.certification.prerequisites import CandidateSlice; print(CandidateSlice.load('${ROOT_DIR}/config/phase9/candidate-slice.yaml').digest)"
)}"
SLICE="production/binance/usdm/perpetual/trade/plan-1/btcusdt"
BUNDLE="00000000-0000-4000-8000-000000000092"

cleanup() {
  docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --name "${CONTAINER}" --network none   --security-opt no-new-privileges:true --pids-limit 256 --memory 768m --cpus 1.0   --tmpfs /var/lib/postgresql/data:rw,nosuid,nodev,size=512m   -e POSTGRES_HOST_AUTH_METHOD=trust   -v "${ROOT_DIR}/migrations/postgres:/migrations:ro"   "${POSTGRES_IMAGE}" >/dev/null

for _ in $(seq 1 60); do
  docker exec "${CONTAINER}" pg_isready -U postgres -d postgres >/dev/null 2>&1 && break
  sleep 1
done
docker exec "${CONTAINER}" pg_isready -U postgres -d postgres >/dev/null

apply_migrations() {
  local migration
  for migration in "${ROOT_DIR}"/migrations/postgres/*.sql; do
    docker exec "${CONTAINER}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres       -f "/migrations/$(basename "${migration}")" >/dev/null
  done
}
apply_migrations

docker exec -i "${CONTAINER}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres <<SQL >/dev/null
INSERT INTO qdl_production_prerequisite_bundles (
  bundle_id, candidate_digest, policy_revision, decision, evidence,
  evidence_sha256, issued_by, issued_at, expires_at
) VALUES (
  '${BUNDLE}', '${CANDIDATE_DIGEST}', 1, 'GO', '{}', repeat('5',64),
  'phase92-test', clock_timestamp(), clock_timestamp() + interval '1 day'
);
INSERT INTO qdl_authority_slices (
  slice_id, environment, venue, market, product_type, feed,
  partition_plan_epoch, partition_id, schema_major, state,
  authority_revision, owner_id, lease_epoch, terminal_watermark,
  candidate_digest, artifact_image_digest, sbom_digest, signature_identity,
  contract_digest, normalizer_version, adapter_version, config_revision,
  instrument_catalog_revision, source_policy_revision, partition_plan_digest,
  rollback_manifest_digest, prerequisite_bundle_id, approved_by, approved_at,
  hold_until
) VALUES (
  '${SLICE}', 'production', 'BINANCE', 'USDM', 'PERPETUAL', 'TRADE',
  1, 'rendezvous-sha256-v1:epoch-1:btcusdt', 2, 'RUST_CANARY',
  3, 'python-primary', 1, 100,
  '${CANDIDATE_DIGEST}', 'sha256:${CANDIDATE_DIGEST}', repeat('1',64),
  'phase92-test-signer', repeat('2',64), 'qdl-rust-core/test',
  'binance-usdm/test', 'phase92-test-config', 'phase92-test-catalog',
  'phase92-test-source-policy', repeat('3',64), repeat('4',64),
  '${BUNDLE}', 'phase92-test', clock_timestamp(),
  clock_timestamp() + interval '2 hours'
);
SQL

expect_failure() {
  if docker exec "${CONTAINER}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres     -c "$1" >/dev/null 2>&1; then
    printf 'expected SQL failure but statement succeeded\n' >&2
    exit 1
  fi
}

expect_failure "SELECT qdl_transition_authority(
  '90000000-0000-4000-8000-000000000001','${SLICE}',
  'RUST_CANARY',3,'python-primary',1,1,'RUST_PRIMARY','rust-primary',2,
  100,'${BUNDLE}',clock_timestamp()+interval '1 hour',
  'phase92-test','direct primary bypass');"

docker exec -i "${CONTAINER}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres <<SQL >/dev/null
INSERT INTO qdl_terminal_owner_checkpoints (
  checkpoint_id, slice_id, owner_id, authority_revision, lease_epoch,
  partition_plan_epoch, source_session_id, connection_generation,
  terminal_watermark, terminal_event_id, terminal_payload_sha256,
  candidate_digest, committed_at
) VALUES (
  '91000000-0000-4000-8000-000000000001','${SLICE}','python-primary',
  3,1,1,'python-session-1',1,100,'event-100',repeat('6',64),
  '${CANDIDATE_DIGEST}',clock_timestamp()
);
INSERT INTO qdl_authority_handoffs (
  handoff_id, checkpoint_id, direction, slice_id, old_owner_id, new_owner_id,
  expected_state, new_state, expected_authority_revision,
  new_authority_revision, expected_lease_epoch, new_lease_epoch,
  partition_plan_epoch, terminal_watermark, first_new_watermark,
  overlap_start_watermark, overlap_end_watermark, old_event_count,
  new_event_count, semantic_mismatches, open_gaps, candidate_digest,
  prerequisite_bundle_id, handoff_sha256, approved_by, approved_at, expires_at
) VALUES (
  '92000000-0000-4000-8000-000000000001',
  '91000000-0000-4000-8000-000000000001','PYTHON_TO_RUST','${SLICE}',
  'python-primary','rust-primary','RUST_CANARY','RUST_PRIMARY',3,4,1,2,1,
  100,101,90,100,11,11,0,0,'${CANDIDATE_DIGEST}','${BUNDLE}',
  repeat('7',64),'phase92-test',clock_timestamp(),
  clock_timestamp()+interval '2 hours'
);
SELECT (qdl_transition_authority_v2(
  '92000000-0000-4000-8000-000000000001',
  '93000000-0000-4000-8000-000000000001','${SLICE}',
  'RUST_CANARY',3,'python-primary',1,1,'RUST_PRIMARY','rust-primary',2,
  100,'${BUNDLE}',clock_timestamp()+interval '1 hour',
  'phase92-test','accepted Python to Rust handoff'
)).state;
SQL

expect_failure "SELECT qdl_transition_authority_v2(
  '92000000-0000-4000-8000-000000000001',
  '93000000-0000-4000-8000-000000000009','${SLICE}',
  'RUST_CANARY',3,'python-primary',1,1,'RUST_PRIMARY','rust-primary',2,
  100,'${BUNDLE}',clock_timestamp()+interval '1 hour',
  'phase92-test','stale CAS replay');"
expect_failure "UPDATE qdl_authority_handoffs SET approved_by='mutated';"
expect_failure "DELETE FROM qdl_terminal_owner_checkpoints;"

docker exec -i "${CONTAINER}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres <<SQL >/dev/null
SELECT qdl_transition_authority(
  '93000000-0000-4000-8000-000000000002','${SLICE}',
  'RUST_PRIMARY',4,'rust-primary',2,1,'BLOCKED','rust-primary',2,
  120,NULL,NULL,'phase92-test','fence Rust before rollback');
SELECT qdl_transition_authority(
  '93000000-0000-4000-8000-000000000003','${SLICE}',
  'BLOCKED',5,'rust-primary',2,1,'ROLLBACK_PENDING','rust-primary',2,
  120,NULL,NULL,'phase92-test','prepare Python rollback');
INSERT INTO qdl_terminal_owner_checkpoints (
  checkpoint_id, slice_id, owner_id, authority_revision, lease_epoch,
  partition_plan_epoch, source_session_id, connection_generation,
  terminal_watermark, terminal_event_id, terminal_payload_sha256,
  candidate_digest, committed_at
) VALUES (
  '91000000-0000-4000-8000-000000000002','${SLICE}','rust-primary',
  6,2,1,'rust-session-1',1,120,'event-120',repeat('8',64),
  '${CANDIDATE_DIGEST}',clock_timestamp()
);
INSERT INTO qdl_authority_handoffs (
  handoff_id, checkpoint_id, direction, slice_id, old_owner_id, new_owner_id,
  expected_state, new_state, expected_authority_revision,
  new_authority_revision, expected_lease_epoch, new_lease_epoch,
  partition_plan_epoch, terminal_watermark, first_new_watermark,
  overlap_start_watermark, overlap_end_watermark, old_event_count,
  new_event_count, semantic_mismatches, open_gaps, candidate_digest,
  prerequisite_bundle_id, handoff_sha256, approved_by, approved_at, expires_at
) VALUES (
  '92000000-0000-4000-8000-000000000002',
  '91000000-0000-4000-8000-000000000002','RUST_TO_PYTHON','${SLICE}',
  'rust-primary','python-rollback','ROLLBACK_PENDING','PYTHON_PRIMARY',6,7,2,3,1,
  120,121,110,120,11,11,0,0,'${CANDIDATE_DIGEST}','${BUNDLE}',
  repeat('9',64),'phase92-test',clock_timestamp(),
  clock_timestamp()+interval '2 hours'
);
SELECT (qdl_transition_authority_v2(
  '92000000-0000-4000-8000-000000000002',
  '93000000-0000-4000-8000-000000000004','${SLICE}',
  'ROLLBACK_PENDING',6,'rust-primary',2,1,'PYTHON_PRIMARY','python-rollback',3,
  120,NULL,NULL,'phase92-test','accepted Rust to Python rollback'
)).state;
SQL

apply_migrations

state="$(docker exec "${CONTAINER}" psql -U postgres -d postgres -Atc   "SELECT state || ':' || authority_revision || ':' || owner_id || ':' || lease_epoch || ':' || terminal_watermark FROM qdl_authority_slices;")"
audit_count="$(docker exec "${CONTAINER}" psql -U postgres -d postgres -Atc   "SELECT count(*) FROM qdl_authority_transition_audit;")"
checkpoint_count="$(docker exec "${CONTAINER}" psql -U postgres -d postgres -Atc   "SELECT count(*) FROM qdl_terminal_owner_checkpoints;")"
handoff_count="$(docker exec "${CONTAINER}" psql -U postgres -d postgres -Atc   "SELECT count(*) FROM qdl_authority_handoffs;")"
[[ "${state}" == "PYTHON_PRIMARY:7:python-rollback:3:120" ]]
[[ "${audit_count}" == "4" ]]
[[ "${checkpoint_count}" == "2" ]]
[[ "${handoff_count}" == "2" ]]

python3 - "${OUTPUT}" "${state}" "${audit_count}" "${checkpoint_count}" "${handoff_count}" <<'PY'
import json
import pathlib
import sys

output, state, audits, checkpoints, handoffs = sys.argv[1:]
path = pathlib.Path(output)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps({
    "schema": "qdl.phase92.authority-migration.v1",
    "status": "PASS",
    "final_test_state": state,
    "audit_records": int(audits),
    "terminal_checkpoints": int(checkpoints),
    "accepted_handoffs": int(handoffs),
    "direct_primary_bypass_rejected": True,
    "stale_cas_rejected": True,
    "handoff_mutation_rejected": True,
    "checkpoint_delete_rejected": True,
    "python_to_rust_handoff_passed": True,
    "rust_to_python_rollback_passed": True,
    "idempotent_migration": True,
    "production_mutations": 0,
}, indent=2, sort_keys=True) + "\n")
PY

cleanup
trap - EXIT
[[ -z "$(docker ps -aq --filter name=^/${CONTAINER}$)" ]]
printf '{"status":"PASS","state":"%s","audits":%s,"checkpoints":%s,"handoffs":%s,"cleanup":true}\n'   "${state}" "${audit_count}" "${checkpoint_count}" "${handoff_count}"
