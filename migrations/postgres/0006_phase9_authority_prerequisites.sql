BEGIN;

CREATE TABLE IF NOT EXISTS qdl_production_prerequisite_bundles (
    bundle_id UUID PRIMARY KEY,
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    policy_revision BIGINT NOT NULL CHECK (policy_revision > 0),
    decision TEXT NOT NULL CHECK (decision IN ('GO', 'NO_GO_EXTERNAL')),
    evidence JSONB NOT NULL,
    evidence_sha256 TEXT NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    issued_by TEXT NOT NULL CHECK (btrim(issued_by) <> ''),
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (jsonb_typeof(evidence) = 'object'),
    CHECK (expires_at > issued_at)
);

CREATE INDEX IF NOT EXISTS qdl_prerequisite_bundle_candidate_idx
    ON qdl_production_prerequisite_bundles (candidate_digest, expires_at DESC);

CREATE TABLE IF NOT EXISTS qdl_authority_slices (
    slice_id TEXT PRIMARY KEY CHECK (btrim(slice_id) <> ''),
    environment TEXT NOT NULL,
    venue TEXT NOT NULL,
    market TEXT NOT NULL,
    product_type TEXT NOT NULL,
    feed TEXT NOT NULL,
    partition_plan_epoch BIGINT NOT NULL CHECK (partition_plan_epoch > 0),
    partition_id TEXT NOT NULL CHECK (btrim(partition_id) <> ''),
    schema_major INTEGER NOT NULL CHECK (schema_major > 0),
    state TEXT NOT NULL CHECK (state IN (
        'PYTHON_PRIMARY', 'RUST_SHADOW', 'VALIDATING', 'RUST_CANARY',
        'RUST_PRIMARY', 'BLOCKED', 'ROLLBACK_PENDING'
    )),
    authority_revision BIGINT NOT NULL CHECK (authority_revision > 0),
    owner_id TEXT NOT NULL CHECK (btrim(owner_id) <> ''),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    terminal_watermark BIGINT,
    candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    artifact_image_digest TEXT NOT NULL CHECK (artifact_image_digest ~ '^sha256:[0-9a-f]{64}$'),
    sbom_digest TEXT NOT NULL CHECK (sbom_digest ~ '^[0-9a-f]{64}$'),
    signature_identity TEXT NOT NULL CHECK (btrim(signature_identity) <> ''),
    contract_digest TEXT NOT NULL CHECK (contract_digest ~ '^[0-9a-f]{64}$'),
    normalizer_version TEXT NOT NULL CHECK (btrim(normalizer_version) <> ''),
    adapter_version TEXT NOT NULL CHECK (btrim(adapter_version) <> ''),
    config_revision TEXT NOT NULL CHECK (btrim(config_revision) <> ''),
    instrument_catalog_revision TEXT NOT NULL CHECK (btrim(instrument_catalog_revision) <> ''),
    source_policy_revision TEXT NOT NULL CHECK (btrim(source_policy_revision) <> ''),
    partition_plan_digest TEXT NOT NULL CHECK (partition_plan_digest ~ '^[0-9a-f]{64}$'),
    rollback_manifest_digest TEXT NOT NULL CHECK (rollback_manifest_digest ~ '^[0-9a-f]{64}$'),
    prerequisite_bundle_id UUID REFERENCES qdl_production_prerequisite_bundles(bundle_id),
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    hold_until TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (environment, venue, market, product_type, feed, partition_plan_epoch, partition_id, schema_major),
    CHECK ((approved_by IS NULL) = (approved_at IS NULL))
);

CREATE TABLE IF NOT EXISTS qdl_authority_transition_audit (
    transition_id UUID PRIMARY KEY,
    slice_id TEXT NOT NULL REFERENCES qdl_authority_slices(slice_id),
    previous_state TEXT NOT NULL,
    new_state TEXT NOT NULL,
    previous_revision BIGINT NOT NULL,
    new_revision BIGINT NOT NULL,
    previous_owner_id TEXT NOT NULL,
    new_owner_id TEXT NOT NULL,
    previous_lease_epoch BIGINT NOT NULL,
    new_lease_epoch BIGINT NOT NULL,
    partition_plan_epoch BIGINT NOT NULL,
    terminal_watermark BIGINT,
    prerequisite_bundle_id UUID REFERENCES qdl_production_prerequisite_bundles(bundle_id),
    hold_until TIMESTAMPTZ,
    actor TEXT NOT NULL CHECK (btrim(actor) <> ''),
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (new_revision = previous_revision + 1)
);

CREATE INDEX IF NOT EXISTS qdl_authority_transition_slice_idx
    ON qdl_authority_transition_audit (slice_id, new_revision);

CREATE OR REPLACE FUNCTION qdl_reject_authority_audit_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'qdl_authority_transition_audit is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qdl_authority_audit_immutable
    ON qdl_authority_transition_audit;
CREATE TRIGGER qdl_authority_audit_immutable
BEFORE UPDATE OR DELETE ON qdl_authority_transition_audit
FOR EACH ROW EXECUTE FUNCTION qdl_reject_authority_audit_mutation();

CREATE OR REPLACE FUNCTION qdl_transition_authority(
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
    current_row qdl_authority_slices%ROWTYPE;
    updated_row qdl_authority_slices%ROWTYPE;
    bundle_row qdl_production_prerequisite_bundles%ROWTYPE;
    transition_allowed BOOLEAN := FALSE;
BEGIN
    IF btrim(p_actor) = '' OR btrim(p_reason) = '' OR btrim(p_new_owner_id) = '' THEN
        RAISE EXCEPTION 'actor, reason and new owner are required';
    END IF;

    SELECT * INTO current_row
    FROM qdl_authority_slices AS authority_slice
    WHERE authority_slice.slice_id = p_slice_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'authority slice not found: %', p_slice_id;
    END IF;

    IF current_row.state <> p_expected_state
       OR current_row.authority_revision <> p_expected_revision
       OR current_row.owner_id <> p_expected_owner_id
       OR current_row.lease_epoch <> p_expected_lease_epoch
       OR current_row.partition_plan_epoch <> p_expected_partition_plan_epoch THEN
        RAISE EXCEPTION 'authority compare-and-swap precondition failed';
    END IF;

    transition_allowed := CASE current_row.state
        WHEN 'PYTHON_PRIMARY' THEN p_new_state IN ('RUST_SHADOW', 'BLOCKED')
        WHEN 'RUST_SHADOW' THEN p_new_state IN ('VALIDATING', 'BLOCKED')
        WHEN 'VALIDATING' THEN p_new_state IN ('RUST_CANARY', 'BLOCKED', 'ROLLBACK_PENDING')
        WHEN 'RUST_CANARY' THEN p_new_state IN ('RUST_PRIMARY', 'BLOCKED', 'ROLLBACK_PENDING')
        WHEN 'RUST_PRIMARY' THEN p_new_state IN ('BLOCKED', 'ROLLBACK_PENDING')
        WHEN 'BLOCKED' THEN p_new_state IN ('VALIDATING', 'ROLLBACK_PENDING')
        WHEN 'ROLLBACK_PENDING' THEN p_new_state = 'PYTHON_PRIMARY'
        ELSE FALSE
    END;
    IF NOT transition_allowed THEN
        RAISE EXCEPTION 'invalid authority transition: % -> %', current_row.state, p_new_state;
    END IF;

    IF p_new_lease_epoch < current_row.lease_epoch
       OR (p_new_owner_id <> current_row.owner_id AND p_new_lease_epoch <= current_row.lease_epoch) THEN
        RAISE EXCEPTION 'new owner requires a strictly newer lease epoch';
    END IF;

    IF p_new_state IN ('RUST_CANARY', 'RUST_PRIMARY') THEN
        IF p_prerequisite_bundle_id IS NULL THEN
            RAISE EXCEPTION 'canary/primary requires a prerequisite bundle';
        END IF;
        IF p_terminal_watermark IS NULL OR p_terminal_watermark < 0 THEN
            RAISE EXCEPTION 'canary/primary requires a non-negative terminal watermark';
        END IF;
        IF p_hold_until IS NULL OR p_hold_until <= clock_timestamp() THEN
            RAISE EXCEPTION 'canary/primary requires a future approval hold window';
        END IF;
        SELECT * INTO bundle_row
        FROM qdl_production_prerequisite_bundles AS bundle
        WHERE bundle.bundle_id = p_prerequisite_bundle_id;
        IF NOT FOUND
           OR bundle_row.decision <> 'GO'
           OR bundle_row.candidate_digest <> current_row.candidate_digest
           OR bundle_row.expires_at <= clock_timestamp()
           OR bundle_row.expires_at < p_hold_until THEN
            RAISE EXCEPTION 'production prerequisite bundle is absent, blocked, mismatched or expired';
        END IF;
    ELSIF p_prerequisite_bundle_id IS NOT NULL OR p_hold_until IS NOT NULL THEN
        RAISE EXCEPTION 'prerequisite bundle and hold window are valid only for canary/primary';
    END IF;

    UPDATE qdl_authority_slices AS authority_slice
    SET state = p_new_state,
        authority_revision = current_row.authority_revision + 1,
        owner_id = p_new_owner_id,
        lease_epoch = p_new_lease_epoch,
        terminal_watermark = p_terminal_watermark,
        prerequisite_bundle_id = CASE WHEN p_new_state IN ('RUST_CANARY', 'RUST_PRIMARY') THEN p_prerequisite_bundle_id ELSE NULL END,
        approved_by = CASE WHEN p_new_state IN ('RUST_CANARY', 'RUST_PRIMARY') THEN p_actor ELSE NULL END,
        approved_at = CASE WHEN p_new_state IN ('RUST_CANARY', 'RUST_PRIMARY') THEN clock_timestamp() ELSE NULL END,
        hold_until = CASE WHEN p_new_state IN ('RUST_CANARY', 'RUST_PRIMARY') THEN p_hold_until ELSE NULL END,
        updated_at = clock_timestamp()
    WHERE authority_slice.slice_id = p_slice_id
    RETURNING * INTO updated_row;

    INSERT INTO qdl_authority_transition_audit (
        transition_id, slice_id, previous_state, new_state,
        previous_revision, new_revision, previous_owner_id, new_owner_id,
        previous_lease_epoch, new_lease_epoch, partition_plan_epoch,
        terminal_watermark, prerequisite_bundle_id, hold_until, actor, reason
    ) VALUES (
        p_transition_id, p_slice_id, current_row.state, updated_row.state,
        current_row.authority_revision, updated_row.authority_revision,
        current_row.owner_id, updated_row.owner_id,
        current_row.lease_epoch, updated_row.lease_epoch,
        current_row.partition_plan_epoch, p_terminal_watermark,
        p_prerequisite_bundle_id, p_hold_until, p_actor, p_reason
    );

    RETURN updated_row;
END;
$$;

COMMIT;
