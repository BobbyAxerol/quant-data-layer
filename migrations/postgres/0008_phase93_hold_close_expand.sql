BEGIN;

CREATE TABLE IF NOT EXISTS qdl_primary_holds (
    hold_id UUID PRIMARY KEY,
    slice_id TEXT NOT NULL REFERENCES qdl_authority_slices(slice_id),
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    prerequisite_bundle_id UUID NOT NULL
        REFERENCES qdl_production_prerequisite_bundles(bundle_id),
    owner_id TEXT NOT NULL CHECK (btrim(owner_id) <> ''),
    authority_revision BIGINT NOT NULL CHECK (authority_revision > 0),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    partition_plan_epoch BIGINT NOT NULL CHECK (partition_plan_epoch > 0),
    started_at TIMESTAMPTZ NOT NULL,
    required_until TIMESTAMPTZ NOT NULL,
    policy_digest TEXT NOT NULL CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    minimum_duration_seconds BIGINT NOT NULL CHECK (minimum_duration_seconds > 0),
    max_sample_gap_seconds BIGINT NOT NULL CHECK (max_sample_gap_seconds > 0),
    max_lag_ms BIGINT NOT NULL CHECK (max_lag_ms > 0),
    max_freshness_ms BIGINT NOT NULL CHECK (max_freshness_ms > 0),
    max_queue_depth BIGINT NOT NULL CHECK (max_queue_depth > 0),
    max_spool_bytes BIGINT NOT NULL CHECK (max_spool_bytes > 0),
    max_cpu_percent DOUBLE PRECISION NOT NULL
        CHECK (max_cpu_percent > 0 AND max_cpu_percent <= 100),
    max_rss_mb DOUBLE PRECISION NOT NULL CHECK (max_rss_mb > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (required_until > started_at),
    CHECK (
        extract(epoch FROM (required_until - started_at))
        >= minimum_duration_seconds
    ),
    UNIQUE (
        slice_id, candidate_digest, owner_id, authority_revision,
        lease_epoch, partition_plan_epoch, hold_id
    )
);

CREATE TABLE IF NOT EXISTS qdl_primary_hold_observations (
    observation_id UUID PRIMARY KEY,
    hold_id UUID NOT NULL REFERENCES qdl_primary_holds(hold_id),
    slice_id TEXT NOT NULL,
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    owner_id TEXT NOT NULL CHECK (btrim(owner_id) <> ''),
    authority_revision BIGINT NOT NULL CHECK (authority_revision > 0),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    partition_plan_epoch BIGINT NOT NULL CHECK (partition_plan_epoch > 0),
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    observed_at TIMESTAMPTZ NOT NULL,
    last_watermark BIGINT NOT NULL CHECK (last_watermark >= 0),
    semantic_mismatches BIGINT NOT NULL DEFAULT 0 CHECK (semantic_mismatches >= 0),
    open_gaps BIGINT NOT NULL DEFAULT 0 CHECK (open_gaps >= 0),
    duplicate_external_writes BIGINT NOT NULL DEFAULT 0
        CHECK (duplicate_external_writes >= 0),
    accepted_stale_writer_writes BIGINT NOT NULL DEFAULT 0
        CHECK (accepted_stale_writer_writes >= 0),
    authority_ambiguities BIGINT NOT NULL DEFAULT 0
        CHECK (authority_ambiguities >= 0),
    durable_ack_failures BIGINT NOT NULL DEFAULT 0
        CHECK (durable_ack_failures >= 0),
    projection_mismatches BIGINT NOT NULL DEFAULT 0
        CHECK (projection_mismatches >= 0),
    consumer_checkpoint_regressions BIGINT NOT NULL DEFAULT 0
        CHECK (consumer_checkpoint_regressions >= 0),
    unexplained_quality_failures BIGINT NOT NULL DEFAULT 0
        CHECK (unexplained_quality_failures >= 0),
    lag_ms BIGINT NOT NULL DEFAULT 0 CHECK (lag_ms >= 0),
    freshness_ms BIGINT NOT NULL DEFAULT 0 CHECK (freshness_ms >= 0),
    queue_depth BIGINT NOT NULL DEFAULT 0 CHECK (queue_depth >= 0),
    spool_bytes BIGINT NOT NULL DEFAULT 0 CHECK (spool_bytes >= 0),
    cpu_percent DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (cpu_percent >= 0),
    rss_mb DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (rss_mb >= 0),
    registered_consumers BIGINT NOT NULL CHECK (registered_consumers > 0),
    healthy_consumers BIGINT NOT NULL CHECK (
        healthy_consumers >= 0 AND healthy_consumers <= registered_consumers
    ),
    checkpoint_watermark BIGINT NOT NULL CHECK (checkpoint_watermark >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (hold_id, sequence)
);

CREATE INDEX IF NOT EXISTS qdl_primary_hold_observation_time_idx
    ON qdl_primary_hold_observations (hold_id, observed_at);

CREATE OR REPLACE FUNCTION qdl_validate_primary_hold_observation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    hold_row qdl_primary_holds%ROWTYPE;
    previous_row qdl_primary_hold_observations%ROWTYPE;
    expected_sequence BIGINT;
BEGIN
    SELECT * INTO hold_row
    FROM qdl_primary_holds AS hold
    WHERE hold.hold_id = NEW.hold_id
    FOR SHARE;
    IF NOT FOUND
       OR NEW.slice_id <> hold_row.slice_id
       OR NEW.candidate_digest <> hold_row.candidate_digest
       OR NEW.owner_id <> hold_row.owner_id
       OR NEW.authority_revision <> hold_row.authority_revision
       OR NEW.lease_epoch <> hold_row.lease_epoch
       OR NEW.partition_plan_epoch <> hold_row.partition_plan_epoch THEN
        RAISE EXCEPTION 'Phase 9.3 hold observation identity mismatch';
    END IF;

    SELECT * INTO previous_row
    FROM qdl_primary_hold_observations AS observation
    WHERE observation.hold_id = NEW.hold_id
    ORDER BY observation.sequence DESC
    LIMIT 1
    FOR SHARE;

    expected_sequence := COALESCE(previous_row.sequence, 0) + 1;
    IF NEW.sequence <> expected_sequence THEN
        RAISE EXCEPTION 'Phase 9.3 hold observation sequence is not contiguous';
    END IF;
    IF NEW.observed_at <= COALESCE(previous_row.observed_at, hold_row.started_at) THEN
        RAISE EXCEPTION 'Phase 9.3 hold observation time is not monotonic';
    END IF;
    IF extract(epoch FROM (
        NEW.observed_at - COALESCE(previous_row.observed_at, hold_row.started_at)
    )) > hold_row.max_sample_gap_seconds THEN
        RAISE EXCEPTION 'Phase 9.3 hold observation gap exceeds policy';
    END IF;
    IF previous_row.observation_id IS NOT NULL
       AND NEW.last_watermark < previous_row.last_watermark THEN
        RAISE EXCEPTION 'Phase 9.3 hold watermark regressed';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS qdl_primary_hold_observation_guard
    ON qdl_primary_hold_observations;
CREATE TRIGGER qdl_primary_hold_observation_guard
BEFORE INSERT ON qdl_primary_hold_observations
FOR EACH ROW EXECUTE FUNCTION qdl_validate_primary_hold_observation();

CREATE TABLE IF NOT EXISTS qdl_primary_hold_decisions (
    decision_id UUID PRIMARY KEY,
    hold_id UUID NOT NULL REFERENCES qdl_primary_holds(hold_id),
    status TEXT NOT NULL CHECK (status IN ('IN_PROGRESS', 'PASSED', 'BLOCKED')),
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    scope TEXT NOT NULL CHECK (scope IN ('TEST_REHEARSAL', 'PRODUCTION')),
    production_authorized BOOLEAN NOT NULL,
    slice_id TEXT NOT NULL,
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    prerequisite_bundle_id UUID NOT NULL,
    owner_id TEXT NOT NULL CHECK (btrim(owner_id) <> ''),
    authority_revision BIGINT NOT NULL CHECK (authority_revision > 0),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    partition_plan_epoch BIGINT NOT NULL CHECK (partition_plan_epoch > 0),
    policy_digest TEXT NOT NULL CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    first_observed_at TIMESTAMPTZ,
    last_observed_at TIMESTAMPTZ,
    observation_count BIGINT NOT NULL CHECK (observation_count >= 0),
    terminal_watermark BIGINT CHECK (terminal_watermark >= 0),
    decided_at TIMESTAMPTZ NOT NULL,
    decision_sha256 TEXT NOT NULL CHECK (decision_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        production_authorized
        = (scope = 'PRODUCTION' AND status = 'PASSED')
    ),
    CHECK (
        (observation_count = 0 AND first_observed_at IS NULL
         AND last_observed_at IS NULL AND terminal_watermark IS NULL)
        OR
        (observation_count > 0 AND first_observed_at IS NOT NULL
         AND last_observed_at IS NOT NULL AND terminal_watermark IS NOT NULL)
    ),
    UNIQUE (hold_id, decision_sha256)
);

CREATE OR REPLACE FUNCTION qdl_validate_primary_hold_decision()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    hold_row qdl_primary_holds%ROWTYPE;
    summary RECORD;
BEGIN
    SELECT * INTO hold_row
    FROM qdl_primary_holds AS hold
    WHERE hold.hold_id = NEW.hold_id
    FOR SHARE;
    IF NOT FOUND
       OR NEW.slice_id <> hold_row.slice_id
       OR NEW.candidate_digest <> hold_row.candidate_digest
       OR NEW.prerequisite_bundle_id <> hold_row.prerequisite_bundle_id
       OR NEW.owner_id <> hold_row.owner_id
       OR NEW.authority_revision <> hold_row.authority_revision
       OR NEW.lease_epoch <> hold_row.lease_epoch
       OR NEW.partition_plan_epoch <> hold_row.partition_plan_epoch
       OR NEW.policy_digest <> hold_row.policy_digest THEN
        RAISE EXCEPTION 'Phase 9.3 hold decision identity mismatch';
    END IF;

    SELECT
        count(*) AS observation_count,
        min(observed_at) AS first_observed_at,
        max(observed_at) AS last_observed_at,
        (array_agg(last_watermark ORDER BY sequence DESC))[1]
            AS terminal_watermark,
        bool_or(
            semantic_mismatches <> 0
            OR open_gaps <> 0
            OR duplicate_external_writes <> 0
            OR accepted_stale_writer_writes <> 0
            OR authority_ambiguities <> 0
            OR durable_ack_failures <> 0
            OR projection_mismatches <> 0
            OR consumer_checkpoint_regressions <> 0
            OR unexplained_quality_failures <> 0
            OR lag_ms > hold_row.max_lag_ms
            OR freshness_ms > hold_row.max_freshness_ms
            OR queue_depth > hold_row.max_queue_depth
            OR spool_bytes > hold_row.max_spool_bytes
            OR cpu_percent > hold_row.max_cpu_percent
            OR rss_mb > hold_row.max_rss_mb
            OR healthy_consumers <> registered_consumers
            OR checkpoint_watermark < last_watermark
        ) AS breached
    INTO summary
    FROM qdl_primary_hold_observations AS observation
    WHERE observation.hold_id = NEW.hold_id;

    IF NEW.observation_count <> summary.observation_count
       OR NEW.first_observed_at IS DISTINCT FROM summary.first_observed_at
       OR NEW.last_observed_at IS DISTINCT FROM summary.last_observed_at
       OR NEW.terminal_watermark IS DISTINCT FROM summary.terminal_watermark THEN
        RAISE EXCEPTION 'Phase 9.3 hold decision summary mismatch';
    END IF;
    IF NEW.status = 'PASSED' AND (
        summary.observation_count = 0
        OR summary.breached
        OR summary.last_observed_at < hold_row.required_until
        OR NEW.decided_at < hold_row.required_until
        OR NEW.reason <> 'PASS'
    ) THEN
        RAISE EXCEPTION 'Phase 9.3 passing hold decision is not supported by evidence';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS qdl_primary_hold_decision_guard
    ON qdl_primary_hold_decisions;
CREATE TRIGGER qdl_primary_hold_decision_guard
BEFORE INSERT ON qdl_primary_hold_decisions
FOR EACH ROW EXECUTE FUNCTION qdl_validate_primary_hold_decision();

CREATE TABLE IF NOT EXISTS qdl_consumer_registry_snapshots (
    snapshot_id UUID PRIMARY KEY,
    slice_id TEXT NOT NULL REFERENCES qdl_authority_slices(slice_id),
    authority_revision BIGINT NOT NULL CHECK (authority_revision > 0),
    checkpoint_count BIGINT NOT NULL CHECK (checkpoint_count > 0),
    ready_checkpoint_count BIGINT NOT NULL CHECK (
        ready_checkpoint_count = checkpoint_count
    ),
    minimum_checkpoint_watermark BIGINT NOT NULL
        CHECK (minimum_checkpoint_watermark >= 0),
    checkpoint_regressions BIGINT NOT NULL
        CHECK (checkpoint_regressions = 0),
    unresolved_migrations BIGINT NOT NULL CHECK (unresolved_migrations = 0),
    rollback_ready BOOLEAN NOT NULL CHECK (rollback_ready),
    registry_sha256 TEXT NOT NULL CHECK (registry_sha256 ~ '^[0-9a-f]{64}$'),
    details JSONB NOT NULL CHECK (jsonb_typeof(details) = 'object'),
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (slice_id, authority_revision, registry_sha256)
);

CREATE TABLE IF NOT EXISTS qdl_authority_registry_snapshots (
    snapshot_id UUID PRIMARY KEY,
    slice_id TEXT NOT NULL REFERENCES qdl_authority_slices(slice_id),
    state TEXT NOT NULL CHECK (state = 'RUST_PRIMARY'),
    owner_id TEXT NOT NULL CHECK (btrim(owner_id) <> ''),
    authority_revision BIGINT NOT NULL CHECK (authority_revision > 0),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    partition_plan_epoch BIGINT NOT NULL CHECK (partition_plan_epoch > 0),
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    prerequisite_bundle_id UUID NOT NULL
        REFERENCES qdl_production_prerequisite_bundles(bundle_id),
    current_watermark BIGINT NOT NULL CHECK (current_watermark >= 0),
    public_write_allowed BOOLEAN NOT NULL CHECK (public_write_allowed),
    legacy_write_allowed BOOLEAN NOT NULL CHECK (legacy_write_allowed),
    registry_sha256 TEXT NOT NULL CHECK (registry_sha256 ~ '^[0-9a-f]{64}$'),
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (slice_id, authority_revision, registry_sha256)
);

CREATE TABLE IF NOT EXISTS qdl_rollback_rehearsals (
    rehearsal_id UUID PRIMARY KEY,
    slice_id TEXT NOT NULL REFERENCES qdl_authority_slices(slice_id),
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    owner_id TEXT NOT NULL CHECK (btrim(owner_id) <> ''),
    authority_revision BIGINT NOT NULL CHECK (authority_revision > 0),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    partition_plan_epoch BIGINT NOT NULL CHECK (partition_plan_epoch > 0),
    rollback_manifest_digest TEXT NOT NULL
        CHECK (rollback_manifest_digest ~ '^[0-9a-f]{64}$'),
    reconciled_through_watermark BIGINT NOT NULL
        CHECK (reconciled_through_watermark >= 0),
    rto_ms DOUBLE PRECISION NOT NULL CHECK (rto_ms > 0),
    status TEXT NOT NULL CHECK (status = 'PASS'),
    production_scope BOOLEAN NOT NULL CHECK (production_scope),
    rehearsal_sha256 TEXT NOT NULL CHECK (rehearsal_sha256 ~ '^[0-9a-f]{64}$'),
    observed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (expires_at > observed_at),
    UNIQUE (slice_id, authority_revision, rehearsal_sha256)
);

CREATE TABLE IF NOT EXISTS qdl_closure_approvals (
    approval_id UUID PRIMARY KEY,
    slice_id TEXT NOT NULL REFERENCES qdl_authority_slices(slice_id),
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    prerequisite_bundle_id UUID NOT NULL
        REFERENCES qdl_production_prerequisite_bundles(bundle_id),
    hold_id UUID NOT NULL REFERENCES qdl_primary_holds(hold_id),
    hold_policy_digest TEXT NOT NULL
        CHECK (hold_policy_digest ~ '^[0-9a-f]{64}$'),
    decision TEXT NOT NULL CHECK (decision = 'APPROVE'),
    allow_close_rollback_window BOOLEAN NOT NULL
        CHECK (allow_close_rollback_window),
    repository_cleanup_approved BOOLEAN NOT NULL
        CHECK (NOT repository_cleanup_approved),
    operator TEXT NOT NULL CHECK (btrim(operator) <> ''),
    change_ticket TEXT NOT NULL CHECK (btrim(change_ticket) <> ''),
    approval_sha256 TEXT NOT NULL CHECK (approval_sha256 ~ '^[0-9a-f]{64}$'),
    approved_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (expires_at > approved_at),
    UNIQUE (slice_id, hold_id, approval_sha256)
);

CREATE TABLE IF NOT EXISTS qdl_authority_closures (
    closure_id UUID PRIMARY KEY,
    slice_id TEXT NOT NULL REFERENCES qdl_authority_slices(slice_id),
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    prerequisite_bundle_id UUID NOT NULL,
    owner_id TEXT NOT NULL CHECK (btrim(owner_id) <> ''),
    authority_revision BIGINT NOT NULL CHECK (authority_revision > 0),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    partition_plan_epoch BIGINT NOT NULL CHECK (partition_plan_epoch > 0),
    hold_decision_id UUID NOT NULL
        REFERENCES qdl_primary_hold_decisions(decision_id),
    hold_decision_digest TEXT NOT NULL CHECK (hold_decision_digest ~ '^[0-9a-f]{64}$'),
    consumer_registry_snapshot_id UUID NOT NULL
        REFERENCES qdl_consumer_registry_snapshots(snapshot_id),
    consumer_registry_digest TEXT NOT NULL CHECK (consumer_registry_digest ~ '^[0-9a-f]{64}$'),
    authority_registry_snapshot_id UUID NOT NULL
        REFERENCES qdl_authority_registry_snapshots(snapshot_id),
    authority_registry_digest TEXT NOT NULL CHECK (authority_registry_digest ~ '^[0-9a-f]{64}$'),
    rollback_rehearsal_id UUID NOT NULL
        REFERENCES qdl_rollback_rehearsals(rehearsal_id),
    rollback_rehearsal_digest TEXT NOT NULL CHECK (rollback_rehearsal_digest ~ '^[0-9a-f]{64}$'),
    approval_id UUID NOT NULL REFERENCES qdl_closure_approvals(approval_id),
    approval_digest TEXT NOT NULL CHECK (approval_digest ~ '^[0-9a-f]{64}$'),
    approved_by TEXT NOT NULL CHECK (btrim(approved_by) <> ''),
    change_ticket TEXT NOT NULL CHECK (btrim(change_ticket) <> ''),
    approved_at TIMESTAMPTZ NOT NULL,
    approval_expires_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,
    production_authorized BOOLEAN NOT NULL CHECK (production_authorized),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (approval_expires_at > approved_at),
    CHECK (closed_at >= approved_at AND closed_at < approval_expires_at),
    UNIQUE (slice_id, authority_revision)
);

CREATE OR REPLACE FUNCTION qdl_close_authority_window(
    p_closure_id UUID,
    p_hold_decision_id UUID,
    p_consumer_registry_snapshot_id UUID,
    p_authority_registry_snapshot_id UUID,
    p_rollback_rehearsal_id UUID,
    p_approval_id UUID,
    p_closed_at TIMESTAMPTZ
)
RETURNS qdl_authority_closures
LANGUAGE plpgsql
AS $$
DECLARE
    current_row qdl_authority_slices%ROWTYPE;
    decision_row qdl_primary_hold_decisions%ROWTYPE;
    hold_row qdl_primary_holds%ROWTYPE;
    bundle_row qdl_production_prerequisite_bundles%ROWTYPE;
    consumer_row qdl_consumer_registry_snapshots%ROWTYPE;
    authority_row qdl_authority_registry_snapshots%ROWTYPE;
    rollback_row qdl_rollback_rehearsals%ROWTYPE;
    approval_row qdl_closure_approvals%ROWTYPE;
    closure_row qdl_authority_closures%ROWTYPE;
BEGIN
    SELECT * INTO decision_row
    FROM qdl_primary_hold_decisions AS decision
    WHERE decision.decision_id = p_hold_decision_id
    FOR SHARE;
    SELECT * INTO hold_row
    FROM qdl_primary_holds AS hold
    WHERE hold.hold_id = decision_row.hold_id
    FOR SHARE;
    SELECT * INTO current_row
    FROM qdl_authority_slices AS authority_slice
    WHERE authority_slice.slice_id = decision_row.slice_id
    FOR UPDATE;
    SELECT * INTO bundle_row
    FROM qdl_production_prerequisite_bundles AS bundle
    WHERE bundle.bundle_id = decision_row.prerequisite_bundle_id
    FOR SHARE;
    SELECT * INTO consumer_row
    FROM qdl_consumer_registry_snapshots AS snapshot
    WHERE snapshot.snapshot_id = p_consumer_registry_snapshot_id
    FOR SHARE;
    SELECT * INTO authority_row
    FROM qdl_authority_registry_snapshots AS snapshot
    WHERE snapshot.snapshot_id = p_authority_registry_snapshot_id
    FOR SHARE;
    SELECT * INTO rollback_row
    FROM qdl_rollback_rehearsals AS rehearsal
    WHERE rehearsal.rehearsal_id = p_rollback_rehearsal_id
    FOR SHARE;
    SELECT * INTO approval_row
    FROM qdl_closure_approvals AS approval
    WHERE approval.approval_id = p_approval_id
    FOR SHARE;

    IF decision_row.decision_id IS NULL
       OR hold_row.hold_id IS NULL
       OR decision_row.status <> 'PASSED'
       OR decision_row.scope <> 'PRODUCTION'
       OR NOT decision_row.production_authorized
       OR decision_row.reason <> 'PASS'
       OR decision_row.hold_id <> hold_row.hold_id
       OR decision_row.slice_id <> hold_row.slice_id
       OR decision_row.candidate_digest <> hold_row.candidate_digest
       OR decision_row.prerequisite_bundle_id <> hold_row.prerequisite_bundle_id
       OR decision_row.owner_id <> hold_row.owner_id
       OR decision_row.authority_revision <> hold_row.authority_revision
       OR decision_row.lease_epoch <> hold_row.lease_epoch
       OR decision_row.partition_plan_epoch <> hold_row.partition_plan_epoch
       OR decision_row.policy_digest <> hold_row.policy_digest THEN
        RAISE EXCEPTION 'Phase 9.3 passing production hold decision is required';
    END IF;
    IF current_row.slice_id IS NULL
       OR current_row.state <> 'RUST_PRIMARY'
       OR current_row.candidate_digest <> decision_row.candidate_digest
       OR current_row.prerequisite_bundle_id <> decision_row.prerequisite_bundle_id
       OR current_row.owner_id <> decision_row.owner_id
       OR current_row.authority_revision <> decision_row.authority_revision
       OR current_row.lease_epoch <> decision_row.lease_epoch
       OR current_row.partition_plan_epoch <> decision_row.partition_plan_epoch THEN
        RAISE EXCEPTION 'Phase 9.3 authority closure CAS mismatch';
    END IF;
    IF bundle_row.bundle_id IS NULL
       OR bundle_row.decision <> 'GO'
       OR bundle_row.candidate_digest <> current_row.candidate_digest
       OR bundle_row.expires_at <= p_closed_at THEN
        RAISE EXCEPTION 'Phase 9.3 closure prerequisite bundle is invalid';
    END IF;
    IF consumer_row.snapshot_id IS NULL
       OR consumer_row.slice_id <> current_row.slice_id
       OR consumer_row.authority_revision <> current_row.authority_revision
       OR consumer_row.minimum_checkpoint_watermark < authority_row.current_watermark
       OR consumer_row.observed_at > p_closed_at
       OR p_closed_at - consumer_row.observed_at > interval '5 minutes' THEN
        RAISE EXCEPTION 'Phase 9.3 consumer registry snapshot is invalid';
    END IF;
    IF authority_row.snapshot_id IS NULL
       OR authority_row.slice_id <> current_row.slice_id
       OR authority_row.state <> current_row.state
       OR authority_row.owner_id <> current_row.owner_id
       OR authority_row.authority_revision <> current_row.authority_revision
       OR authority_row.lease_epoch <> current_row.lease_epoch
       OR authority_row.partition_plan_epoch <> current_row.partition_plan_epoch
       OR authority_row.candidate_digest <> current_row.candidate_digest
       OR authority_row.prerequisite_bundle_id <> current_row.prerequisite_bundle_id
       OR authority_row.observed_at > p_closed_at
       OR p_closed_at - authority_row.observed_at > interval '5 minutes' THEN
        RAISE EXCEPTION 'Phase 9.3 authority registry snapshot is invalid';
    END IF;
    IF rollback_row.rehearsal_id IS NULL
       OR rollback_row.slice_id <> current_row.slice_id
       OR rollback_row.candidate_digest <> current_row.candidate_digest
       OR rollback_row.owner_id <> current_row.owner_id
       OR rollback_row.authority_revision <> current_row.authority_revision
       OR rollback_row.lease_epoch <> current_row.lease_epoch
       OR rollback_row.partition_plan_epoch <> current_row.partition_plan_epoch
       OR rollback_row.rollback_manifest_digest <> current_row.rollback_manifest_digest
       OR rollback_row.reconciled_through_watermark < authority_row.current_watermark
       OR rollback_row.expires_at <= p_closed_at THEN
        RAISE EXCEPTION 'Phase 9.3 rollback rehearsal is invalid';
    END IF;
    IF approval_row.approval_id IS NULL
       OR approval_row.slice_id <> current_row.slice_id
       OR approval_row.candidate_digest <> current_row.candidate_digest
       OR approval_row.prerequisite_bundle_id <> current_row.prerequisite_bundle_id
       OR approval_row.hold_id <> hold_row.hold_id
       OR approval_row.hold_policy_digest <> hold_row.policy_digest
       OR approval_row.approved_at > p_closed_at
       OR approval_row.expires_at <= p_closed_at THEN
        RAISE EXCEPTION 'Phase 9.3 closure approval is invalid or expired';
    END IF;

    INSERT INTO qdl_authority_closures (
        closure_id, slice_id, candidate_digest, prerequisite_bundle_id,
        owner_id, authority_revision, lease_epoch, partition_plan_epoch,
        hold_decision_id, hold_decision_digest,
        consumer_registry_snapshot_id, consumer_registry_digest,
        authority_registry_snapshot_id, authority_registry_digest,
        rollback_rehearsal_id, rollback_rehearsal_digest,
        approval_id, approval_digest, approved_by, change_ticket,
        approved_at, approval_expires_at, closed_at, production_authorized
    ) VALUES (
        p_closure_id, current_row.slice_id, current_row.candidate_digest,
        current_row.prerequisite_bundle_id, current_row.owner_id,
        current_row.authority_revision, current_row.lease_epoch,
        current_row.partition_plan_epoch, decision_row.decision_id,
        decision_row.decision_sha256, consumer_row.snapshot_id,
        consumer_row.registry_sha256, authority_row.snapshot_id,
        authority_row.registry_sha256, rollback_row.rehearsal_id,
        rollback_row.rehearsal_sha256, approval_row.approval_id,
        approval_row.approval_sha256, approval_row.operator,
        approval_row.change_ticket, approval_row.approved_at,
        approval_row.expires_at, p_closed_at, TRUE
    )
    RETURNING * INTO closure_row;
    RETURN closure_row;
END;
$$;

CREATE TABLE IF NOT EXISTS qdl_expansion_candidates (
    expansion_id UUID PRIMARY KEY,
    parent_closure_id UUID NOT NULL
        REFERENCES qdl_authority_closures(closure_id),
    parent_slice_id TEXT NOT NULL,
    parent_candidate_digest TEXT NOT NULL CHECK (parent_candidate_digest ~ '^[0-9a-f]{64}$'),
    parent_closure_digest TEXT NOT NULL CHECK (parent_closure_digest ~ '^[0-9a-f]{64}$'),
    expansion_type TEXT NOT NULL CHECK (expansion_type IN (
        'INSTRUMENT_PARTITION', 'BBO', 'L2_BOOK', 'BAR_LIFECYCLE',
        'VENUE_MARKET'
    )),
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    scope_digest TEXT NOT NULL CHECK (scope_digest ~ '^[0-9a-f]{64}$'),
    partition_plan_epoch BIGINT NOT NULL CHECK (partition_plan_epoch > 0),
    required_gates TEXT[] NOT NULL,
    status TEXT NOT NULL CHECK (status = 'INDEPENDENT_CERTIFICATION_REQUIRED'),
    transitive_evidence_allowed BOOLEAN NOT NULL
        CHECK (NOT transitive_evidence_allowed),
    public_write_allowed BOOLEAN NOT NULL CHECK (NOT public_write_allowed),
    legacy_write_allowed BOOLEAN NOT NULL CHECK (NOT legacy_write_allowed),
    created_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (candidate_digest <> parent_candidate_digest),
    CHECK (cardinality(required_gates) > 0),
    UNIQUE (parent_closure_id, expansion_type, candidate_digest, scope_digest)
);

CREATE OR REPLACE FUNCTION qdl_validate_expansion_candidate()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    closure_row qdl_authority_closures%ROWTYPE;
    expected_gates TEXT[];
BEGIN
    SELECT * INTO closure_row
    FROM qdl_authority_closures AS closure
    WHERE closure.closure_id = NEW.parent_closure_id
    FOR SHARE;
    IF NOT FOUND
       OR NEW.parent_slice_id <> closure_row.slice_id
       OR NEW.parent_candidate_digest <> closure_row.candidate_digest
       OR NOT closure_row.production_authorized THEN
        RAISE EXCEPTION 'Phase 9.3 expansion parent closure mismatch';
    END IF;
    expected_gates := CASE NEW.expansion_type
        WHEN 'INSTRUMENT_PARTITION' THEN ARRAY[
            'authority_handoff', 'capacity_headroom', 'exact_frame_parity',
            'partition_churn', 'provider_authentic_source', 'rollback',
            'source_capacity'
        ]
        WHEN 'BBO' THEN ARRAY[
            'authority_handoff', 'capacity_headroom', 'coalescing_policy',
            'exact_frame_parity', 'freshness', 'ordering_reconnect',
            'provider_authentic_source', 'quote_identity', 'rollback'
        ]
        WHEN 'L2_BOOK' THEN ARRAY[
            'authority_handoff', 'capacity_headroom', 'checksum',
            'exact_frame_parity', 'lossless_backpressure',
            'provider_authentic_source', 'resync', 'rollback',
            'snapshot_delta_sequence'
        ]
        WHEN 'BAR_LIFECYCLE' THEN ARRAY[
            'authority_handoff', 'capacity_headroom', 'close_time_semantics',
            'exact_frame_parity', 'final_revision_lineage',
            'provider_authentic_source', 'replay', 'rollback'
        ]
        WHEN 'VENUE_MARKET' THEN ARRAY[
            'adapter_capability', 'authority_handoff', 'capacity_headroom',
            'disaster_recovery', 'entitlement', 'exact_frame_parity',
            'instrument_identity', 'provider_authentic_source',
            'provider_semantics', 'rollback'
        ]
    END;
    IF NEW.required_gates <> expected_gates THEN
        RAISE EXCEPTION 'Phase 9.3 expansion requires independent capability gates';
    END IF;
    IF NEW.expansion_type = 'INSTRUMENT_PARTITION'
       AND NEW.partition_plan_epoch <= closure_row.partition_plan_epoch THEN
        RAISE EXCEPTION 'Phase 9.3 instrument expansion requires a new partition epoch';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS qdl_expansion_candidate_guard
    ON qdl_expansion_candidates;
CREATE TRIGGER qdl_expansion_candidate_guard
BEFORE INSERT ON qdl_expansion_candidates
FOR EACH ROW EXECUTE FUNCTION qdl_validate_expansion_candidate();

CREATE TABLE IF NOT EXISTS qdl_runtime_decommission_decisions (
    decision_id UUID PRIMARY KEY,
    runtime_id TEXT NOT NULL CHECK (btrim(runtime_id) <> ''),
    owned_slice_count BIGINT NOT NULL CHECK (owned_slice_count >= 0),
    rollback_reference_count BIGINT NOT NULL CHECK (rollback_reference_count >= 0),
    consumer_dependency_count BIGINT NOT NULL CHECK (consumer_dependency_count >= 0),
    all_replacement_windows_closed BOOLEAN NOT NULL,
    repository_cleanup_approved BOOLEAN NOT NULL,
    shared_knowledge_retained BOOLEAN NOT NULL,
    allowed BOOLEAN NOT NULL,
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    decided_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        NOT allowed OR (
            owned_slice_count = 0
            AND rollback_reference_count = 0
            AND consumer_dependency_count = 0
            AND all_replacement_windows_closed
            AND repository_cleanup_approved
            AND shared_knowledge_retained
        )
    )
);

CREATE OR REPLACE FUNCTION qdl_reject_phase93_registry_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Phase 9.3 hold/closure/expansion evidence is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qdl_primary_hold_immutable ON qdl_primary_holds;
CREATE TRIGGER qdl_primary_hold_immutable
BEFORE UPDATE OR DELETE ON qdl_primary_holds
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase93_registry_mutation();

DROP TRIGGER IF EXISTS qdl_primary_hold_observation_immutable
    ON qdl_primary_hold_observations;
CREATE TRIGGER qdl_primary_hold_observation_immutable
BEFORE UPDATE OR DELETE ON qdl_primary_hold_observations
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase93_registry_mutation();

DROP TRIGGER IF EXISTS qdl_primary_hold_decision_immutable
    ON qdl_primary_hold_decisions;
CREATE TRIGGER qdl_primary_hold_decision_immutable
BEFORE UPDATE OR DELETE ON qdl_primary_hold_decisions
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase93_registry_mutation();

DROP TRIGGER IF EXISTS qdl_consumer_registry_snapshot_immutable
    ON qdl_consumer_registry_snapshots;
CREATE TRIGGER qdl_consumer_registry_snapshot_immutable
BEFORE UPDATE OR DELETE ON qdl_consumer_registry_snapshots
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase93_registry_mutation();

DROP TRIGGER IF EXISTS qdl_authority_registry_snapshot_immutable
    ON qdl_authority_registry_snapshots;
CREATE TRIGGER qdl_authority_registry_snapshot_immutable
BEFORE UPDATE OR DELETE ON qdl_authority_registry_snapshots
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase93_registry_mutation();

DROP TRIGGER IF EXISTS qdl_rollback_rehearsal_immutable
    ON qdl_rollback_rehearsals;
CREATE TRIGGER qdl_rollback_rehearsal_immutable
BEFORE UPDATE OR DELETE ON qdl_rollback_rehearsals
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase93_registry_mutation();

DROP TRIGGER IF EXISTS qdl_closure_approval_immutable
    ON qdl_closure_approvals;
CREATE TRIGGER qdl_closure_approval_immutable
BEFORE UPDATE OR DELETE ON qdl_closure_approvals
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase93_registry_mutation();

DROP TRIGGER IF EXISTS qdl_authority_closure_immutable
    ON qdl_authority_closures;
CREATE TRIGGER qdl_authority_closure_immutable
BEFORE UPDATE OR DELETE ON qdl_authority_closures
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase93_registry_mutation();

DROP TRIGGER IF EXISTS qdl_expansion_candidate_immutable
    ON qdl_expansion_candidates;
CREATE TRIGGER qdl_expansion_candidate_immutable
BEFORE UPDATE OR DELETE ON qdl_expansion_candidates
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase93_registry_mutation();

DROP TRIGGER IF EXISTS qdl_runtime_decommission_decision_immutable
    ON qdl_runtime_decommission_decisions;
CREATE TRIGGER qdl_runtime_decommission_decision_immutable
BEFORE UPDATE OR DELETE ON qdl_runtime_decommission_decisions
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase93_registry_mutation();

REVOKE EXECUTE ON FUNCTION qdl_close_authority_window(
    UUID, UUID, UUID, UUID, UUID, UUID, TIMESTAMPTZ
) FROM PUBLIC;

COMMIT;
