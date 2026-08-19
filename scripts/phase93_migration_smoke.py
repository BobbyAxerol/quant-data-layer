#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from qdl.certification.prerequisites import CandidateSlice  # noqa: E402


IMAGE = os.environ.get(
    "QDL_PHASE93_POSTGRES_IMAGE", "timescale/timescaledb:latest-pg15"
)
CONTAINER = os.environ.get(
    "QDL_PHASE93_POSTGRES_CONTAINER", f"qdl_phase93_postgres_{os.getpid()}"
)
OUTPUT = pathlib.Path(
    os.environ.get(
        "QDL_PHASE93_MIGRATION_OUTPUT",
        str(ROOT / "upgrade/evidence/phase93-hold-close-migration.json"),
    )
)
CANDIDATE = CandidateSlice.load(ROOT / "config/phase9/candidate-slice.yaml")
DIGEST = CANDIDATE.digest
SLICE = "production/binance/usdm/perpetual/trade/plan-1/btcusdt"
BUNDLE = "00000000-0000-4000-8000-000000000093"


def run(
    args: list[str],
    *,
    input_text: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        cwd=ROOT,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )
    if check and result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(args)}\n"
            f"stdout={result.stdout[-4000:]}\nstderr={result.stderr[-4000:]}"
        )
    return result


def psql(sql: str, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run(
        [
            "docker",
            "exec",
            "-i",
            CONTAINER,
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-d",
            "postgres",
        ],
        input_text=sql,
        check=check,
    )


def query(sql: str) -> str:
    return run(
        [
            "docker",
            "exec",
            CONTAINER,
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-Atc",
            sql,
        ]
    ).stdout.strip()


def expect_failure(sql: str) -> None:
    if psql(sql, check=False).returncode == 0:
        raise RuntimeError(f"expected SQL failure but statement succeeded: {sql}")


def apply_migrations() -> None:
    for path in sorted((ROOT / "migrations/postgres").glob("*.sql")):
        run(
            [
                "docker",
                "exec",
                CONTAINER,
                "psql",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-f",
                f"/migrations/{path.name}",
            ]
        )


def cleanup() -> None:
    run(["docker", "rm", "-f", CONTAINER], check=False)


def main() -> int:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    t = lambda value: value.isoformat()
    cleanup()
    run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            CONTAINER,
            "--network",
            "none",
            "--security-opt",
            "no-new-privileges:true",
            "--pids-limit",
            "256",
            "--memory",
            "768m",
            "--cpus",
            "1.0",
            "--tmpfs",
            "/var/lib/postgresql/data:rw,nosuid,nodev,size=512m",
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            "-v",
            f"{ROOT / 'migrations/postgres'}:/migrations:ro",
            IMAGE,
        ]
    )
    try:
        for _ in range(60):
            ready = run(
                [
                    "docker",
                    "exec",
                    CONTAINER,
                    "pg_isready",
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                ],
                check=False,
            )
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("Phase 9.3 PostgreSQL did not become ready")
        apply_migrations()

        psql(
            f"""
INSERT INTO qdl_production_prerequisite_bundles (
  bundle_id,candidate_digest,policy_revision,decision,evidence,evidence_sha256,
  issued_by,issued_at,expires_at
) VALUES (
  '{BUNDLE}','{DIGEST}',1,'GO','{{}}',repeat('5',64),'phase93-test',
  '{t(now - timedelta(minutes=5))}','{t(now + timedelta(days=1))}'
);
INSERT INTO qdl_authority_slices (
  slice_id,environment,venue,market,product_type,feed,partition_plan_epoch,
  partition_id,schema_major,state,authority_revision,owner_id,lease_epoch,
  terminal_watermark,candidate_digest,artifact_image_digest,sbom_digest,
  signature_identity,contract_digest,normalizer_version,adapter_version,
  config_revision,instrument_catalog_revision,source_policy_revision,
  partition_plan_digest,rollback_manifest_digest,prerequisite_bundle_id,
  approved_by,approved_at,hold_until
) VALUES (
  '{SLICE}','production','BINANCE','USDM','PERPETUAL','TRADE',1,
  'rendezvous-sha256-v1:epoch-1:btcusdt',2,'RUST_CANARY',3,
  'python-primary',1,100,'{DIGEST}','sha256:{DIGEST}',repeat('1',64),
  'phase93-test-signer',repeat('2',64),'qdl-rust-core/test',
  'binance-usdm/test','phase93-test-config','phase93-test-catalog',
  'phase93-test-source-policy',repeat('3',64),repeat('4',64),'{BUNDLE}',
  'phase93-test','{t(now)}','{t(now + timedelta(hours=2))}'
);
INSERT INTO qdl_terminal_owner_checkpoints (
  checkpoint_id,slice_id,owner_id,authority_revision,lease_epoch,
  partition_plan_epoch,source_session_id,connection_generation,
  terminal_watermark,terminal_event_id,terminal_payload_sha256,
  candidate_digest,committed_at
) VALUES (
  '91000000-0000-4000-8000-000000000093','{SLICE}','python-primary',
  3,1,1,'python-session-93',1,100,'event-100',repeat('6',64),
  '{DIGEST}','{t(now)}'
);
INSERT INTO qdl_authority_handoffs (
  handoff_id,checkpoint_id,direction,slice_id,old_owner_id,new_owner_id,
  expected_state,new_state,expected_authority_revision,new_authority_revision,
  expected_lease_epoch,new_lease_epoch,partition_plan_epoch,
  terminal_watermark,first_new_watermark,overlap_start_watermark,
  overlap_end_watermark,old_event_count,new_event_count,semantic_mismatches,
  open_gaps,candidate_digest,prerequisite_bundle_id,handoff_sha256,
  approved_by,approved_at,expires_at
) VALUES (
  '92000000-0000-4000-8000-000000000093',
  '91000000-0000-4000-8000-000000000093','PYTHON_TO_RUST','{SLICE}',
  'python-primary','rust-primary','RUST_CANARY','RUST_PRIMARY',3,4,1,2,1,
  100,101,90,100,11,11,0,0,'{DIGEST}','{BUNDLE}',repeat('7',64),
  'phase93-test','{t(now)}','{t(now + timedelta(hours=2))}'
);
SELECT (qdl_transition_authority_v2(
  '92000000-0000-4000-8000-000000000093',
  '93000000-0000-4000-8000-000000000093','{SLICE}',
  'RUST_CANARY',3,'python-primary',1,1,'RUST_PRIMARY','rust-primary',2,
  100,'{BUNDLE}','{t(now + timedelta(hours=1))}',
  'phase93-test','accepted primary handoff'
)).state;
"""
        )

        psql(
            f"""
INSERT INTO qdl_primary_holds (
  hold_id,slice_id,candidate_digest,prerequisite_bundle_id,owner_id,
  authority_revision,lease_epoch,partition_plan_epoch,started_at,
  required_until,policy_digest,minimum_duration_seconds,
  max_sample_gap_seconds,max_lag_ms,max_freshness_ms,max_queue_depth,
  max_spool_bytes,max_cpu_percent,max_rss_mb
) VALUES
(
  '94000000-0000-4000-8000-000000000001','{SLICE}','{DIGEST}','{BUNDLE}',
  'rust-primary',4,2,1,'{t(now - timedelta(minutes=2))}','{t(now)}',
  repeat('8',64),120,60,500,1000,1000,1000000,80,512
),
(
  '94000000-0000-4000-8000-000000000002','{SLICE}','{DIGEST}','{BUNDLE}',
  'rust-primary',4,2,1,'{t(now - timedelta(minutes=1))}','{t(now)}',
  repeat('9',64),60,60,500,1000,1000,1000000,80,512
);
INSERT INTO qdl_primary_hold_observations (
  observation_id,hold_id,slice_id,candidate_digest,owner_id,
  authority_revision,lease_epoch,partition_plan_epoch,sequence,observed_at,
  last_watermark,lag_ms,freshness_ms,queue_depth,spool_bytes,cpu_percent,
  rss_mb,registered_consumers,healthy_consumers,checkpoint_watermark
) VALUES
(
  '95000000-0000-4000-8000-000000000001',
  '94000000-0000-4000-8000-000000000001','{SLICE}','{DIGEST}',
  'rust-primary',4,2,1,1,'{t(now - timedelta(minutes=1))}',110,
  10,20,1,100,10,64,2,2,110
),
(
  '95000000-0000-4000-8000-000000000002',
  '94000000-0000-4000-8000-000000000001','{SLICE}','{DIGEST}',
  'rust-primary',4,2,1,2,'{t(now)}',120,
  10,20,1,100,10,64,2,2,120
);
INSERT INTO qdl_primary_hold_observations (
  observation_id,hold_id,slice_id,candidate_digest,owner_id,
  authority_revision,lease_epoch,partition_plan_epoch,sequence,observed_at,
  last_watermark,semantic_mismatches,lag_ms,freshness_ms,queue_depth,
  spool_bytes,cpu_percent,rss_mb,registered_consumers,healthy_consumers,
  checkpoint_watermark
) VALUES (
  '95000000-0000-4000-8000-000000000003',
  '94000000-0000-4000-8000-000000000002','{SLICE}','{DIGEST}',
  'rust-primary',4,2,1,1,'{t(now)}',120,1,10,20,1,100,10,64,2,2,120
);
INSERT INTO qdl_primary_hold_decisions (
  decision_id,hold_id,status,reason,scope,production_authorized,slice_id,
  candidate_digest,prerequisite_bundle_id,owner_id,authority_revision,
  lease_epoch,partition_plan_epoch,policy_digest,first_observed_at,
  last_observed_at,observation_count,terminal_watermark,decided_at,
  decision_sha256
) VALUES (
  '96000000-0000-4000-8000-000000000002',
  '94000000-0000-4000-8000-000000000002','BLOCKED','SEMANTIC_MISMATCH',
  'TEST_REHEARSAL',FALSE,'{SLICE}','{DIGEST}','{BUNDLE}','rust-primary',
  4,2,1,repeat('9',64),'{t(now)}','{t(now)}',1,120,'{t(now)}',
  repeat('b',64)
);
"""
        )

        expect_failure(
            f"""
INSERT INTO qdl_primary_hold_observations (
  observation_id,hold_id,slice_id,candidate_digest,owner_id,
  authority_revision,lease_epoch,partition_plan_epoch,sequence,observed_at,
  last_watermark,registered_consumers,healthy_consumers,checkpoint_watermark
) VALUES (
  '95000000-0000-4000-8000-000000000009',
  '94000000-0000-4000-8000-000000000001','{SLICE}','{DIGEST}',
  'rust-primary',4,2,1,4,'{t(now + timedelta(minutes=1))}',121,2,2,121
);
"""
        )
        expect_failure(
            f"""
INSERT INTO qdl_primary_hold_decisions (
  decision_id,hold_id,status,reason,scope,production_authorized,slice_id,
  candidate_digest,prerequisite_bundle_id,owner_id,authority_revision,
  lease_epoch,partition_plan_epoch,policy_digest,first_observed_at,
  last_observed_at,observation_count,terminal_watermark,decided_at,
  decision_sha256
) VALUES (
  '96000000-0000-4000-8000-000000000009',
  '94000000-0000-4000-8000-000000000002','PASSED','PASS','PRODUCTION',
  TRUE,'{SLICE}','{DIGEST}','{BUNDLE}','rust-primary',4,2,1,repeat('9',64),
  '{t(now)}','{t(now)}',1,120,'{t(now)}',repeat('c',64)
);
"""
        )

        psql(
            f"""
INSERT INTO qdl_primary_hold_decisions (
  decision_id,hold_id,status,reason,scope,production_authorized,slice_id,
  candidate_digest,prerequisite_bundle_id,owner_id,authority_revision,
  lease_epoch,partition_plan_epoch,policy_digest,first_observed_at,
  last_observed_at,observation_count,terminal_watermark,decided_at,
  decision_sha256
) VALUES (
  '96000000-0000-4000-8000-000000000001',
  '94000000-0000-4000-8000-000000000001','PASSED','PASS','PRODUCTION',
  TRUE,'{SLICE}','{DIGEST}','{BUNDLE}','rust-primary',4,2,1,repeat('8',64),
  '{t(now - timedelta(minutes=1))}','{t(now)}',2,120,'{t(now)}',
  repeat('a',64)
);
INSERT INTO qdl_consumer_registry_snapshots (
  snapshot_id,slice_id,authority_revision,checkpoint_count,
  ready_checkpoint_count,minimum_checkpoint_watermark,
  checkpoint_regressions,unresolved_migrations,rollback_ready,
  registry_sha256,details,observed_at
) VALUES (
  '97000000-0000-4000-8000-000000000001','{SLICE}',4,2,2,130,
  0,0,TRUE,repeat('c',64),'{{"consumers":["alpha","execution"]}}','{t(now)}'
);
INSERT INTO qdl_authority_registry_snapshots (
  snapshot_id,slice_id,state,owner_id,authority_revision,lease_epoch,
  partition_plan_epoch,candidate_digest,prerequisite_bundle_id,
  current_watermark,public_write_allowed,legacy_write_allowed,
  registry_sha256,observed_at
) VALUES (
  '97000000-0000-4000-8000-000000000002','{SLICE}','RUST_PRIMARY',
  'rust-primary',4,2,1,'{DIGEST}','{BUNDLE}',130,TRUE,TRUE,
  repeat('d',64),'{t(now)}'
);
INSERT INTO qdl_rollback_rehearsals (
  rehearsal_id,slice_id,candidate_digest,owner_id,authority_revision,
  lease_epoch,partition_plan_epoch,rollback_manifest_digest,
  reconciled_through_watermark,rto_ms,status,production_scope,
  rehearsal_sha256,observed_at,expires_at
) VALUES (
  '97000000-0000-4000-8000-000000000003','{SLICE}','{DIGEST}',
  'rust-primary',4,2,1,repeat('4',64),130,500,'PASS',TRUE,
  repeat('e',64),'{t(now)}','{t(now + timedelta(hours=1))}'
);
INSERT INTO qdl_closure_approvals (
  approval_id,slice_id,candidate_digest,prerequisite_bundle_id,hold_id,
  hold_policy_digest,decision,allow_close_rollback_window,
  repository_cleanup_approved,operator,change_ticket,approval_sha256,
  approved_at,expires_at
) VALUES (
  '97000000-0000-4000-8000-000000000004','{SLICE}','{DIGEST}','{BUNDLE}',
  '94000000-0000-4000-8000-000000000001',repeat('8',64),'APPROVE',
  TRUE,FALSE,'phase93-test','QDL-93',repeat('f',64),
  '{t(now - timedelta(minutes=1))}','{t(now + timedelta(hours=1))}'
);
SELECT (qdl_close_authority_window(
  '98000000-0000-4000-8000-000000000001',
  '96000000-0000-4000-8000-000000000001',
  '97000000-0000-4000-8000-000000000001',
  '97000000-0000-4000-8000-000000000002',
  '97000000-0000-4000-8000-000000000003',
  '97000000-0000-4000-8000-000000000004','{t(now)}'
)).closure_id;
"""
        )

        closure_state = query(
            f"SELECT state||':'||authority_revision||':'||owner_id||':'||"
            f"lease_epoch||':'||terminal_watermark FROM qdl_authority_slices "
            f"WHERE slice_id='{SLICE}';"
        )
        if closure_state != "RUST_PRIMARY:4:rust-primary:2:100":
            raise RuntimeError(f"closure mutated authority: {closure_state}")

        expect_failure(
            "UPDATE qdl_authority_closures SET change_ticket='mutated';"
        )
        expect_failure("DELETE FROM qdl_primary_hold_observations;")
        expect_failure(
            "UPDATE qdl_consumer_registry_snapshots SET checkpoint_count=3;"
        )
        expect_failure(
            f"""
INSERT INTO qdl_expansion_candidates (
  expansion_id,parent_closure_id,parent_slice_id,parent_candidate_digest,
  parent_closure_digest,expansion_type,candidate_digest,scope_digest,
  partition_plan_epoch,required_gates,status,transitive_evidence_allowed,
  public_write_allowed,legacy_write_allowed,created_at
) VALUES (
  '99000000-0000-4000-8000-000000000009',
  '98000000-0000-4000-8000-000000000001','{SLICE}','{DIGEST}',
  repeat('1',64),'BBO',repeat('2',64),repeat('3',64),1,
  ARRAY['rollback'],'INDEPENDENT_CERTIFICATION_REQUIRED',FALSE,FALSE,FALSE,
  '{t(now)}'
);
"""
        )

        expansion_rows = [
            (
                "99000000-0000-4000-8000-000000000001",
                "INSTRUMENT_PARTITION",
                "2",
                "3",
                2,
                [
                    "authority_handoff",
                    "capacity_headroom",
                    "exact_frame_parity",
                    "partition_churn",
                    "provider_authentic_source",
                    "rollback",
                    "source_capacity",
                ],
            ),
            (
                "99000000-0000-4000-8000-000000000002",
                "BBO",
                "3",
                "4",
                1,
                [
                    "authority_handoff",
                    "capacity_headroom",
                    "coalescing_policy",
                    "exact_frame_parity",
                    "freshness",
                    "ordering_reconnect",
                    "provider_authentic_source",
                    "quote_identity",
                    "rollback",
                ],
            ),
            (
                "99000000-0000-4000-8000-000000000003",
                "L2_BOOK",
                "4",
                "5",
                1,
                [
                    "authority_handoff",
                    "capacity_headroom",
                    "checksum",
                    "exact_frame_parity",
                    "lossless_backpressure",
                    "provider_authentic_source",
                    "resync",
                    "rollback",
                    "snapshot_delta_sequence",
                ],
            ),
            (
                "99000000-0000-4000-8000-000000000004",
                "BAR_LIFECYCLE",
                "5",
                "6",
                1,
                [
                    "authority_handoff",
                    "capacity_headroom",
                    "close_time_semantics",
                    "exact_frame_parity",
                    "final_revision_lineage",
                    "provider_authentic_source",
                    "replay",
                    "rollback",
                ],
            ),
            (
                "99000000-0000-4000-8000-000000000005",
                "VENUE_MARKET",
                "6",
                "7",
                1,
                [
                    "adapter_capability",
                    "authority_handoff",
                    "capacity_headroom",
                    "disaster_recovery",
                    "entitlement",
                    "exact_frame_parity",
                    "instrument_identity",
                    "provider_authentic_source",
                    "provider_semantics",
                    "rollback",
                ],
            ),
        ]
        for expansion_id, kind, candidate_char, scope_char, epoch, gates in expansion_rows:
            gate_sql = ",".join(f"'{value}'" for value in gates)
            psql(
                f"""
INSERT INTO qdl_expansion_candidates (
  expansion_id,parent_closure_id,parent_slice_id,parent_candidate_digest,
  parent_closure_digest,expansion_type,candidate_digest,scope_digest,
  partition_plan_epoch,required_gates,status,transitive_evidence_allowed,
  public_write_allowed,legacy_write_allowed,created_at
) VALUES (
  '{expansion_id}','98000000-0000-4000-8000-000000000001','{SLICE}',
  '{DIGEST}',repeat('1',64),'{kind}',repeat('{candidate_char}',64),
  repeat('{scope_char}',64),{epoch},ARRAY[{gate_sql}],
  'INDEPENDENT_CERTIFICATION_REQUIRED',FALSE,FALSE,FALSE,'{t(now)}'
);
"""
            )

        psql(
            f"""
INSERT INTO qdl_runtime_decommission_decisions (
  decision_id,runtime_id,owned_slice_count,rollback_reference_count,
  consumer_dependency_count,all_replacement_windows_closed,
  repository_cleanup_approved,shared_knowledge_retained,allowed,reason,
  decided_at
) VALUES
(
  '99000000-0000-4000-8000-000000000006','python-usdm-trade',
  1,1,1,FALSE,FALSE,TRUE,FALSE,'RUNTIME_STILL_OWNS_SLICES','{t(now)}'
),
(
  '99000000-0000-4000-8000-000000000007','retired-test-runtime',
  0,0,0,TRUE,TRUE,TRUE,TRUE,'AUTHORIZED','{t(now)}'
);
SELECT qdl_transition_authority(
  '99000000-0000-4000-8000-000000000008','{SLICE}',
  'RUST_PRIMARY',4,'rust-primary',2,1,'BLOCKED','rust-primary',2,
  130,NULL,NULL,'phase93-test','stale closure CAS test'
);
"""
        )
        expect_failure(
            f"""
SELECT qdl_close_authority_window(
  '98000000-0000-4000-8000-000000000009',
  '96000000-0000-4000-8000-000000000001',
  '97000000-0000-4000-8000-000000000001',
  '97000000-0000-4000-8000-000000000002',
  '97000000-0000-4000-8000-000000000003',
  '97000000-0000-4000-8000-000000000004','{t(now)}'
);
"""
        )

        apply_migrations()
        final_state = query(
            f"SELECT state||':'||authority_revision||':'||owner_id||':'||"
            f"lease_epoch||':'||terminal_watermark FROM qdl_authority_slices "
            f"WHERE slice_id='{SLICE}';"
        )
        counts = query(
            "SELECT "
            "(SELECT count(*) FROM qdl_primary_holds)||':'||"
            "(SELECT count(*) FROM qdl_primary_hold_observations)||':'||"
            "(SELECT count(*) FROM qdl_primary_hold_decisions)||':'||"
            "(SELECT count(*) FROM qdl_authority_closures)||':'||"
            "(SELECT count(*) FROM qdl_expansion_candidates)||':'||"
            "(SELECT count(*) FROM qdl_runtime_decommission_decisions);"
        )
        if final_state != "BLOCKED:5:rust-primary:2:130":
            raise RuntimeError(f"unexpected final test state: {final_state}")
        if counts != "2:3:2:1:5:2":
            raise RuntimeError(f"unexpected Phase 9.3 row counts: {counts}")

        values = [int(value) for value in counts.split(":")]
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_text(
            json.dumps(
                {
                    "schema": "qdl.phase93.hold-close-migration.v1",
                    "status": "PASS",
                    "authority_state_after_closure": closure_state,
                    "final_test_state_after_stale_cas_setup": final_state,
                    "hold_records": values[0],
                    "observation_records": values[1],
                    "decision_records": values[2],
                    "closure_records": values[3],
                    "expansion_records": values[4],
                    "decommission_records": values[5],
                    "dirty_hold_pass_rejected": True,
                    "out_of_order_observation_rejected": True,
                    "append_only_mutation_rejected": True,
                    "registry_mutation_rejected": True,
                    "closure_did_not_mutate_authority": True,
                    "stale_authority_closure_rejected": True,
                    "incomplete_expansion_gates_rejected": True,
                    "all_expansion_types_registered_independently": True,
                    "idempotent_migration": True,
                    "production_mutations": 0,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "closure_state": closure_state,
                    "final_test_state": final_state,
                    "counts": counts,
                    "cleanup": True,
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        cleanup()
        remaining = run(
            [
                "docker",
                "ps",
                "-aq",
                "--filter",
                f"name=^/{CONTAINER}$",
            ],
            check=False,
        ).stdout.strip()
        if remaining:
            raise RuntimeError("Phase 9.3 PostgreSQL cleanup failed")


if __name__ == "__main__":
    raise SystemExit(main())
