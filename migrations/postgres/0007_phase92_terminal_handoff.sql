BEGIN;

CREATE TABLE IF NOT EXISTS qdl_terminal_owner_checkpoints (
    checkpoint_id UUID PRIMARY KEY,
    slice_id TEXT NOT NULL REFERENCES qdl_authority_slices(slice_id),
    owner_id TEXT NOT NULL CHECK (btrim(owner_id) <> ''),
    authority_revision BIGINT NOT NULL CHECK (authority_revision > 0),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    partition_plan_epoch BIGINT NOT NULL CHECK (partition_plan_epoch > 0),
    source_session_id TEXT NOT NULL CHECK (btrim(source_session_id) <> ''),
    connection_generation BIGINT NOT NULL CHECK (connection_generation > 0),
    terminal_watermark BIGINT NOT NULL CHECK (terminal_watermark >= 0),
    terminal_event_id TEXT NOT NULL CHECK (btrim(terminal_event_id) <> ''),
    terminal_payload_sha256 TEXT NOT NULL CHECK (terminal_payload_sha256 ~ '^[0-9a-f]{64}$'),
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    committed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        slice_id, owner_id, authority_revision, lease_epoch,
        partition_plan_epoch, terminal_watermark
    )
);

CREATE TABLE IF NOT EXISTS qdl_authority_handoffs (
    handoff_id UUID PRIMARY KEY,
    checkpoint_id UUID NOT NULL REFERENCES qdl_terminal_owner_checkpoints(checkpoint_id),
    direction TEXT NOT NULL CHECK (direction IN ('PYTHON_TO_RUST', 'RUST_TO_PYTHON')),
    slice_id TEXT NOT NULL REFERENCES qdl_authority_slices(slice_id),
    old_owner_id TEXT NOT NULL CHECK (btrim(old_owner_id) <> ''),
    new_owner_id TEXT NOT NULL CHECK (btrim(new_owner_id) <> ''),
    expected_state TEXT NOT NULL,
    new_state TEXT NOT NULL,
    expected_authority_revision BIGINT NOT NULL CHECK (expected_authority_revision > 0),
    new_authority_revision BIGINT NOT NULL CHECK (new_authority_revision > 0),
    expected_lease_epoch BIGINT NOT NULL CHECK (expected_lease_epoch > 0),
    new_lease_epoch BIGINT NOT NULL CHECK (new_lease_epoch > 0),
    partition_plan_epoch BIGINT NOT NULL CHECK (partition_plan_epoch > 0),
    terminal_watermark BIGINT NOT NULL CHECK (terminal_watermark >= 0),
    first_new_watermark BIGINT NOT NULL CHECK (first_new_watermark >= 1),
    overlap_start_watermark BIGINT NOT NULL CHECK (overlap_start_watermark >= 0),
    overlap_end_watermark BIGINT NOT NULL CHECK (overlap_end_watermark >= 0),
    old_event_count BIGINT NOT NULL CHECK (old_event_count > 0),
    new_event_count BIGINT NOT NULL CHECK (new_event_count > 0),
    semantic_mismatches BIGINT NOT NULL CHECK (semantic_mismatches = 0),
    open_gaps BIGINT NOT NULL CHECK (open_gaps = 0),
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    prerequisite_bundle_id UUID NOT NULL REFERENCES qdl_production_prerequisite_bundles(bundle_id),
    handoff_sha256 TEXT NOT NULL CHECK (handoff_sha256 ~ '^[0-9a-f]{64}$'),
    approved_by TEXT NOT NULL CHECK (btrim(approved_by) <> ''),
    approved_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (old_owner_id <> new_owner_id),
    CHECK (new_authority_revision = expected_authority_revision + 1),
    CHECK (new_lease_epoch > expected_lease_epoch),
    CHECK (first_new_watermark = terminal_watermark + 1),
    CHECK (overlap_start_watermark <= overlap_end_watermark),
    CHECK (overlap_end_watermark = terminal_watermark),
    CHECK (old_event_count = new_event_count),
    CHECK (expires_at > approved_at),
    CHECK (
        (direction = 'PYTHON_TO_RUST' AND expected_state = 'RUST_CANARY' AND new_state = 'RUST_PRIMARY')
        OR
        (direction = 'RUST_TO_PYTHON' AND expected_state = 'ROLLBACK_PENDING' AND new_state = 'PYTHON_PRIMARY')
    )
);

CREATE INDEX IF NOT EXISTS qdl_authority_handoff_transition_idx
    ON qdl_authority_handoffs (
        slice_id, expected_authority_revision, new_authority_revision, new_state
    );

CREATE OR REPLACE FUNCTION qdl_reject_phase92_evidence_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Phase 9.2 checkpoint/handoff evidence is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qdl_terminal_checkpoint_immutable
    ON qdl_terminal_owner_checkpoints;
CREATE TRIGGER qdl_terminal_checkpoint_immutable
BEFORE UPDATE OR DELETE ON qdl_terminal_owner_checkpoints
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase92_evidence_mutation();

DROP TRIGGER IF EXISTS qdl_authority_handoff_immutable
    ON qdl_authority_handoffs;
CREATE TRIGGER qdl_authority_handoff_immutable
BEFORE UPDATE OR DELETE ON qdl_authority_handoffs
FOR EACH ROW EXECUTE FUNCTION qdl_reject_phase92_evidence_mutation();

CREATE OR REPLACE FUNCTION qdl_require_accepted_primary_handoff()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    matching_handoffs BIGINT;
BEGIN
    IF (OLD.state <> 'RUST_PRIMARY' AND NEW.state = 'RUST_PRIMARY')
       OR (OLD.state = 'ROLLBACK_PENDING' AND NEW.state = 'PYTHON_PRIMARY') THEN
        SELECT count(*) INTO matching_handoffs
        FROM qdl_authority_handoffs AS handoff
        JOIN qdl_terminal_owner_checkpoints AS checkpoint
          ON checkpoint.checkpoint_id = handoff.checkpoint_id
        WHERE handoff.slice_id = NEW.slice_id
          AND handoff.old_owner_id = OLD.owner_id
          AND handoff.new_owner_id = NEW.owner_id
          AND handoff.expected_state = OLD.state
          AND handoff.new_state = NEW.state
          AND handoff.expected_authority_revision = OLD.authority_revision
          AND handoff.new_authority_revision = NEW.authority_revision
          AND handoff.expected_lease_epoch = OLD.lease_epoch
          AND handoff.new_lease_epoch = NEW.lease_epoch
          AND handoff.partition_plan_epoch = NEW.partition_plan_epoch
          AND handoff.terminal_watermark = NEW.terminal_watermark
          AND handoff.candidate_digest = NEW.candidate_digest
          AND handoff.prerequisite_bundle_id = COALESCE(
              NEW.prerequisite_bundle_id, handoff.prerequisite_bundle_id
          )
          AND handoff.expires_at > clock_timestamp()
          AND checkpoint.slice_id = handoff.slice_id
          AND checkpoint.owner_id = handoff.old_owner_id
          AND checkpoint.authority_revision = handoff.expected_authority_revision
          AND checkpoint.lease_epoch = handoff.expected_lease_epoch
          AND checkpoint.partition_plan_epoch = handoff.partition_plan_epoch
          AND checkpoint.terminal_watermark = handoff.terminal_watermark
          AND checkpoint.candidate_digest = handoff.candidate_digest;
        IF matching_handoffs <> 1 THEN
            RAISE EXCEPTION 'accepted exact Phase 9.2 handoff is required';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS qdl_authority_primary_handoff_guard
    ON qdl_authority_slices;
CREATE TRIGGER qdl_authority_primary_handoff_guard
BEFORE UPDATE ON qdl_authority_slices
FOR EACH ROW EXECUTE FUNCTION qdl_require_accepted_primary_handoff();

CREATE OR REPLACE FUNCTION qdl_transition_authority_v2(
    p_handoff_id UUID,
    p_transition_id UUID,
    p_slice_id TEXT,
    p_expected_state TEXT,
    p_expected_revision BIGINT,
    p_expected_owner_id TEXT,
    p_expected_lease_epoch BIGINT,
    p_expected_partition_plan_epoch BIGINT,
    p_new_state TEXT,
    p_new_owner_id TEXT,
    p_new_lease_epoch BIGINT,
    p_terminal_watermark BIGINT,
    p_prerequisite_bundle_id UUID,
    p_hold_until TIMESTAMPTZ,
    p_actor TEXT,
    p_reason TEXT
)
RETURNS qdl_authority_slices
LANGUAGE plpgsql
AS $$
DECLARE
    handoff qdl_authority_handoffs%ROWTYPE;
    transitioned qdl_authority_slices%ROWTYPE;
BEGIN
    SELECT * INTO handoff
    FROM qdl_authority_handoffs AS accepted
    WHERE accepted.handoff_id = p_handoff_id
    FOR SHARE;
    IF NOT FOUND
       OR handoff.slice_id <> p_slice_id
       OR handoff.expected_state <> p_expected_state
       OR handoff.expected_authority_revision <> p_expected_revision
       OR handoff.old_owner_id <> p_expected_owner_id
       OR handoff.expected_lease_epoch <> p_expected_lease_epoch
       OR handoff.partition_plan_epoch <> p_expected_partition_plan_epoch
       OR handoff.new_state <> p_new_state
       OR handoff.new_owner_id <> p_new_owner_id
       OR handoff.new_lease_epoch <> p_new_lease_epoch
       OR handoff.terminal_watermark <> p_terminal_watermark
       OR handoff.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'Phase 9.2 handoff/CAS request mismatch or expired';
    END IF;
    IF p_new_state = 'RUST_PRIMARY'
       AND handoff.prerequisite_bundle_id <> p_prerequisite_bundle_id THEN
        RAISE EXCEPTION 'Phase 9.2 prerequisite bundle mismatch';
    END IF;

    SELECT * INTO transitioned
    FROM qdl_transition_authority(
        p_transition_id, p_slice_id, p_expected_state, p_expected_revision,
        p_expected_owner_id, p_expected_lease_epoch,
        p_expected_partition_plan_epoch, p_new_state, p_new_owner_id,
        p_new_lease_epoch, p_terminal_watermark, p_prerequisite_bundle_id,
        p_hold_until, p_actor, p_reason
    );
    RETURN transitioned;
END;
$$;

REVOKE EXECUTE ON FUNCTION qdl_transition_authority_v2(
    UUID, UUID, TEXT, TEXT, BIGINT, TEXT, BIGINT, BIGINT, TEXT, TEXT,
    BIGINT, BIGINT, UUID, TIMESTAMPTZ, TEXT, TEXT
) FROM PUBLIC;

COMMIT;
