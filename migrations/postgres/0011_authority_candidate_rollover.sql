BEGIN;

CREATE TABLE IF NOT EXISTS qdl_authority_candidate_rollovers (
    rollover_id UUID PRIMARY KEY,
    slice_id TEXT NOT NULL REFERENCES qdl_authority_slices(slice_id),
    previous_authority_revision BIGINT NOT NULL CHECK (previous_authority_revision > 0),
    new_authority_revision BIGINT NOT NULL CHECK (new_authority_revision > 0),
    previous_owner_id TEXT NOT NULL CHECK (btrim(previous_owner_id) <> ''),
    new_owner_id TEXT NOT NULL CHECK (btrim(new_owner_id) <> ''),
    previous_lease_epoch BIGINT NOT NULL CHECK (previous_lease_epoch > 0),
    new_lease_epoch BIGINT NOT NULL CHECK (new_lease_epoch > previous_lease_epoch),
    previous_candidate_digest TEXT NOT NULL
        CHECK (previous_candidate_digest ~ '^[0-9a-f]{64}$'),
    new_candidate_digest TEXT NOT NULL
        CHECK (new_candidate_digest ~ '^[0-9a-f]{64}$'),
    previous_artifact_image_digest TEXT NOT NULL
        CHECK (previous_artifact_image_digest ~ '^sha256:[0-9a-f]{64}$'),
    new_artifact_image_digest TEXT NOT NULL
        CHECK (new_artifact_image_digest ~ '^sha256:[0-9a-f]{64}$'),
    candidate_bundle_id UUID NOT NULL
        REFERENCES qdl_production_prerequisite_bundles(bundle_id),
    previous_authority JSONB NOT NULL,
    new_authority JSONB NOT NULL,
    actor TEXT NOT NULL CHECK (btrim(actor) <> ''),
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (new_authority_revision = previous_authority_revision + 1),
    CHECK (new_candidate_digest <> previous_candidate_digest),
    CHECK (new_artifact_image_digest <> previous_artifact_image_digest),
    CHECK (jsonb_typeof(previous_authority) = 'object'),
    CHECK (jsonb_typeof(new_authority) = 'object'),
    UNIQUE (slice_id, new_authority_revision)
);

CREATE OR REPLACE FUNCTION qdl_reject_authority_candidate_rollover_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'qdl_authority_candidate_rollovers is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qdl_authority_candidate_rollover_immutable
    ON qdl_authority_candidate_rollovers;
CREATE TRIGGER qdl_authority_candidate_rollover_immutable
BEFORE UPDATE OR DELETE ON qdl_authority_candidate_rollovers
FOR EACH ROW EXECUTE FUNCTION qdl_reject_authority_candidate_rollover_mutation();

CREATE OR REPLACE FUNCTION qdl_guard_authority_candidate_provenance()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    requested_rollover UUID;
BEGIN
    IF NEW.candidate_digest IS DISTINCT FROM OLD.candidate_digest
       OR NEW.artifact_image_digest IS DISTINCT FROM OLD.artifact_image_digest
       OR NEW.sbom_digest IS DISTINCT FROM OLD.sbom_digest
       OR NEW.signature_identity IS DISTINCT FROM OLD.signature_identity
       OR NEW.contract_digest IS DISTINCT FROM OLD.contract_digest
       OR NEW.normalizer_version IS DISTINCT FROM OLD.normalizer_version
       OR NEW.adapter_version IS DISTINCT FROM OLD.adapter_version
       OR NEW.config_revision IS DISTINCT FROM OLD.config_revision
       OR NEW.instrument_catalog_revision IS DISTINCT FROM OLD.instrument_catalog_revision
       OR NEW.source_policy_revision IS DISTINCT FROM OLD.source_policy_revision
       OR NEW.partition_plan_digest IS DISTINCT FROM OLD.partition_plan_digest
       OR NEW.rollback_manifest_digest IS DISTINCT FROM OLD.rollback_manifest_digest THEN
        BEGIN
            requested_rollover := NULLIF(
                current_setting('qdl.authority_candidate_rollover_id', TRUE), ''
            )::UUID;
        EXCEPTION WHEN invalid_text_representation THEN
            RAISE EXCEPTION 'authority candidate provenance rewrite lacks a valid rollover context';
        END;
        IF requested_rollover IS NULL OR NOT EXISTS (
            SELECT 1
            FROM qdl_authority_candidate_rollovers AS rollover
            WHERE rollover.rollover_id = requested_rollover
              AND rollover.slice_id = OLD.slice_id
              AND rollover.previous_authority_revision = OLD.authority_revision
              AND rollover.new_authority_revision = NEW.authority_revision
              AND rollover.previous_lease_epoch = OLD.lease_epoch
              AND rollover.new_lease_epoch = NEW.lease_epoch
              AND rollover.previous_candidate_digest = OLD.candidate_digest
              AND rollover.new_candidate_digest = NEW.candidate_digest
              AND rollover.previous_artifact_image_digest = OLD.artifact_image_digest
              AND rollover.new_artifact_image_digest = NEW.artifact_image_digest
        ) THEN
            RAISE EXCEPTION 'authority candidate provenance rewrite requires an approved append-only rollover';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS qdl_authority_candidate_provenance_guard
    ON qdl_authority_slices;
CREATE TRIGGER qdl_authority_candidate_provenance_guard
BEFORE UPDATE ON qdl_authority_slices
FOR EACH ROW EXECUTE FUNCTION qdl_guard_authority_candidate_provenance();

CREATE OR REPLACE FUNCTION qdl_rollover_authority_candidate(
    p_rollover_id UUID,
    p_slice_id TEXT,
    p_expected_revision BIGINT,
    p_expected_owner_id TEXT,
    p_expected_lease_epoch BIGINT,
    p_expected_partition_plan_epoch BIGINT,
    p_expected_candidate_digest TEXT,
    p_expected_artifact_image_digest TEXT,
    p_new_owner_id TEXT,
    p_new_lease_epoch BIGINT,
    p_new_provenance JSONB,
    p_candidate_bundle_id UUID,
    p_actor TEXT,
    p_reason TEXT
)
RETURNS qdl_authority_slices
LANGUAGE plpgsql
AS $$
DECLARE
    current_row qdl_authority_slices%ROWTYPE;
    updated_row qdl_authority_slices%ROWTYPE;
    existing_rollover qdl_authority_candidate_rollovers%ROWTYPE;
    bundle_row qdl_production_prerequisite_bundles%ROWTYPE;
    actual_keys TEXT[];
    expected_keys TEXT[] := ARRAY[
        'adapter_version', 'artifact_image_digest', 'candidate_digest',
        'config_revision', 'contract_digest', 'instrument_catalog_revision',
        'normalizer_version', 'partition_plan_digest', 'rollback_manifest_digest',
        'sbom_digest', 'signature_identity', 'source_policy_revision'
    ];
    new_candidate_digest TEXT;
    new_artifact_image_digest TEXT;
    new_sbom_digest TEXT;
    new_signature_identity TEXT;
    new_contract_digest TEXT;
    new_normalizer_version TEXT;
    new_adapter_version TEXT;
    new_config_revision TEXT;
    new_instrument_catalog_revision TEXT;
    new_source_policy_revision TEXT;
    new_partition_plan_digest TEXT;
    new_rollback_manifest_digest TEXT;
    previous_snapshot JSONB;
    new_snapshot JSONB;
BEGIN
    IF p_rollover_id IS NULL
       OR btrim(p_slice_id) = ''
       OR p_expected_revision <= 0
       OR p_expected_lease_epoch <= 0
       OR p_expected_partition_plan_epoch <= 0
       OR p_expected_candidate_digest !~ '^[0-9a-f]{64}$'
       OR p_expected_artifact_image_digest !~ '^sha256:[0-9a-f]{64}$'
       OR btrim(p_expected_owner_id) = ''
       OR btrim(p_new_owner_id) = ''
       OR p_new_lease_epoch <= p_expected_lease_epoch
       OR p_candidate_bundle_id IS NULL
       OR btrim(p_actor) = ''
       OR btrim(p_reason) = '' THEN
        RAISE EXCEPTION 'authority candidate rollover arguments are invalid';
    END IF;

    SELECT * INTO existing_rollover
    FROM qdl_authority_candidate_rollovers
    WHERE rollover_id = p_rollover_id;
    IF FOUND THEN
        IF existing_rollover.slice_id <> p_slice_id
           OR existing_rollover.previous_authority_revision <> p_expected_revision
           OR existing_rollover.previous_lease_epoch <> p_expected_lease_epoch
           OR existing_rollover.previous_candidate_digest <> p_expected_candidate_digest
           OR existing_rollover.previous_artifact_image_digest <> p_expected_artifact_image_digest
           OR existing_rollover.candidate_bundle_id <> p_candidate_bundle_id
           OR existing_rollover.new_lease_epoch <> p_new_lease_epoch
           OR existing_rollover.previous_owner_id <> p_expected_owner_id
           OR existing_rollover.new_owner_id <> p_new_owner_id
           OR (existing_rollover.previous_authority ->> 'partition_plan_epoch')
                <> p_expected_partition_plan_epoch::TEXT
           OR (existing_rollover.new_authority - ARRAY[
                'slice_id', 'environment', 'venue', 'market', 'product_type',
                'feed', 'partition_plan_epoch', 'partition_id', 'schema_major',
                'state', 'authority_revision', 'owner_id', 'lease_epoch',
                'terminal_watermark', 'prerequisite_bundle_id', 'approved_by',
                'approved_at', 'hold_until'
           ]) IS DISTINCT FROM p_new_provenance THEN
            RAISE EXCEPTION 'existing authority candidate rollover conflicts with request';
        END IF;
        SELECT * INTO updated_row
        FROM qdl_authority_slices
        WHERE slice_id = p_slice_id
        FOR SHARE;
        IF NOT FOUND
           OR updated_row.state <> 'BLOCKED'
           OR updated_row.authority_revision <> existing_rollover.new_authority_revision
           OR updated_row.lease_epoch <> existing_rollover.new_lease_epoch
           OR updated_row.candidate_digest <> existing_rollover.new_candidate_digest
           OR updated_row.artifact_image_digest <> existing_rollover.new_artifact_image_digest THEN
            RAISE EXCEPTION 'existing authority candidate rollover does not match current authority';
        END IF;
        RETURN updated_row;
    END IF;

    SELECT * INTO current_row
    FROM qdl_authority_slices AS authority_slice
    WHERE authority_slice.slice_id = p_slice_id
    FOR UPDATE;
    IF NOT FOUND
       OR current_row.state <> 'BLOCKED'
       OR current_row.authority_revision <> p_expected_revision
       OR current_row.owner_id <> p_expected_owner_id
       OR current_row.lease_epoch <> p_expected_lease_epoch
       OR current_row.partition_plan_epoch <> p_expected_partition_plan_epoch
       OR current_row.candidate_digest <> p_expected_candidate_digest
       OR current_row.artifact_image_digest <> p_expected_artifact_image_digest THEN
        RAISE EXCEPTION 'authority candidate rollover compare-and-swap precondition failed';
    END IF;

    SELECT array_agg(key ORDER BY key) INTO actual_keys
    FROM jsonb_object_keys(p_new_provenance) AS keys(key);
    IF jsonb_typeof(p_new_provenance) <> 'object'
       OR actual_keys IS DISTINCT FROM expected_keys THEN
        RAISE EXCEPTION 'authority candidate rollover provenance fields are incomplete or unknown';
    END IF;

    new_candidate_digest := p_new_provenance ->> 'candidate_digest';
    new_artifact_image_digest := p_new_provenance ->> 'artifact_image_digest';
    new_sbom_digest := p_new_provenance ->> 'sbom_digest';
    new_signature_identity := p_new_provenance ->> 'signature_identity';
    new_contract_digest := p_new_provenance ->> 'contract_digest';
    new_normalizer_version := p_new_provenance ->> 'normalizer_version';
    new_adapter_version := p_new_provenance ->> 'adapter_version';
    new_config_revision := p_new_provenance ->> 'config_revision';
    new_instrument_catalog_revision := p_new_provenance ->> 'instrument_catalog_revision';
    new_source_policy_revision := p_new_provenance ->> 'source_policy_revision';
    new_partition_plan_digest := p_new_provenance ->> 'partition_plan_digest';
    new_rollback_manifest_digest := p_new_provenance ->> 'rollback_manifest_digest';

    IF new_candidate_digest !~ '^[0-9a-f]{64}$'
       OR new_artifact_image_digest !~ '^sha256:[0-9a-f]{64}$'
       OR new_sbom_digest !~ '^[0-9a-f]{64}$'
       OR new_contract_digest !~ '^[0-9a-f]{64}$'
       OR new_partition_plan_digest !~ '^[0-9a-f]{64}$'
       OR new_rollback_manifest_digest !~ '^[0-9a-f]{64}$'
       OR btrim(new_signature_identity) = ''
       OR btrim(new_normalizer_version) = ''
       OR btrim(new_adapter_version) = ''
       OR btrim(new_config_revision) = ''
       OR btrim(new_instrument_catalog_revision) = ''
       OR btrim(new_source_policy_revision) = ''
       OR new_candidate_digest = current_row.candidate_digest
       OR new_artifact_image_digest = current_row.artifact_image_digest THEN
        RAISE EXCEPTION 'authority candidate rollover provenance is invalid or not new';
    END IF;

    IF new_contract_digest <> current_row.contract_digest
       OR new_normalizer_version <> current_row.normalizer_version
       OR new_adapter_version <> current_row.adapter_version
       OR new_config_revision <> current_row.config_revision
       OR new_instrument_catalog_revision <> current_row.instrument_catalog_revision
       OR new_source_policy_revision <> current_row.source_policy_revision
       OR new_partition_plan_digest <> current_row.partition_plan_digest
       OR new_signature_identity <> current_row.signature_identity THEN
        RAISE EXCEPTION 'authority candidate rollover cannot change contract, plan or source semantics';
    END IF;

    SELECT * INTO bundle_row
    FROM qdl_production_prerequisite_bundles AS bundle
    WHERE bundle.bundle_id = p_candidate_bundle_id
    FOR SHARE;
    IF NOT FOUND
       OR bundle_row.decision <> 'GO'
       OR bundle_row.candidate_digest <> new_candidate_digest
       OR bundle_row.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'authority candidate rollover prerequisite bundle is invalid';
    END IF;

    previous_snapshot := to_jsonb(current_row) - 'updated_at';
    new_snapshot := jsonb_build_object(
        'slice_id', current_row.slice_id,
        'environment', current_row.environment,
        'venue', current_row.venue,
        'market', current_row.market,
        'product_type', current_row.product_type,
        'feed', current_row.feed,
        'partition_plan_epoch', current_row.partition_plan_epoch,
        'partition_id', current_row.partition_id,
        'schema_major', current_row.schema_major,
        'state', 'BLOCKED',
        'authority_revision', current_row.authority_revision + 1,
        'owner_id', p_new_owner_id,
        'lease_epoch', p_new_lease_epoch,
        'terminal_watermark', NULL,
        'candidate_digest', new_candidate_digest,
        'artifact_image_digest', new_artifact_image_digest,
        'sbom_digest', new_sbom_digest,
        'signature_identity', new_signature_identity,
        'contract_digest', new_contract_digest,
        'normalizer_version', new_normalizer_version,
        'adapter_version', new_adapter_version,
        'config_revision', new_config_revision,
        'instrument_catalog_revision', new_instrument_catalog_revision,
        'source_policy_revision', new_source_policy_revision,
        'partition_plan_digest', new_partition_plan_digest,
        'rollback_manifest_digest', new_rollback_manifest_digest,
        'prerequisite_bundle_id', NULL,
        'approved_by', NULL,
        'approved_at', NULL,
        'hold_until', NULL
    );

    INSERT INTO qdl_authority_candidate_rollovers (
        rollover_id, slice_id, previous_authority_revision, new_authority_revision,
        previous_owner_id, new_owner_id, previous_lease_epoch, new_lease_epoch,
        previous_candidate_digest, new_candidate_digest,
        previous_artifact_image_digest, new_artifact_image_digest,
        candidate_bundle_id, previous_authority, new_authority, actor, reason
    ) VALUES (
        p_rollover_id, current_row.slice_id, current_row.authority_revision,
        current_row.authority_revision + 1, current_row.owner_id, p_new_owner_id,
        current_row.lease_epoch, p_new_lease_epoch, current_row.candidate_digest,
        new_candidate_digest, current_row.artifact_image_digest,
        new_artifact_image_digest, p_candidate_bundle_id, previous_snapshot,
        new_snapshot, p_actor, p_reason
    );

    PERFORM set_config(
        'qdl.authority_candidate_rollover_id', p_rollover_id::TEXT, TRUE
    );
    UPDATE qdl_authority_slices AS authority_slice
    SET state = 'BLOCKED',
        authority_revision = current_row.authority_revision + 1,
        owner_id = p_new_owner_id,
        lease_epoch = p_new_lease_epoch,
        terminal_watermark = NULL,
        candidate_digest = new_candidate_digest,
        artifact_image_digest = new_artifact_image_digest,
        sbom_digest = new_sbom_digest,
        signature_identity = new_signature_identity,
        contract_digest = new_contract_digest,
        normalizer_version = new_normalizer_version,
        adapter_version = new_adapter_version,
        config_revision = new_config_revision,
        instrument_catalog_revision = new_instrument_catalog_revision,
        source_policy_revision = new_source_policy_revision,
        partition_plan_digest = new_partition_plan_digest,
        rollback_manifest_digest = new_rollback_manifest_digest,
        prerequisite_bundle_id = NULL,
        approved_by = NULL,
        approved_at = NULL,
        hold_until = NULL,
        updated_at = clock_timestamp()
    WHERE authority_slice.slice_id = current_row.slice_id
    RETURNING * INTO updated_row;

    IF (to_jsonb(updated_row) - 'updated_at') IS DISTINCT FROM new_snapshot THEN
        RAISE EXCEPTION 'authority candidate rollover postcondition differs from immutable snapshot';
    END IF;

    INSERT INTO qdl_authority_transition_audit (
        transition_id, slice_id, previous_state, new_state, previous_revision,
        new_revision, previous_owner_id, new_owner_id, previous_lease_epoch,
        new_lease_epoch, partition_plan_epoch, terminal_watermark,
        prerequisite_bundle_id, hold_until, actor, reason
    ) VALUES (
        p_rollover_id, current_row.slice_id, 'BLOCKED', 'BLOCKED',
        current_row.authority_revision, updated_row.authority_revision,
        current_row.owner_id, updated_row.owner_id, current_row.lease_epoch,
        updated_row.lease_epoch, current_row.partition_plan_epoch, NULL, NULL,
        NULL, p_actor, p_reason
    );

    RETURN updated_row;
END;
$$;

REVOKE ALL ON FUNCTION qdl_rollover_authority_candidate(
    UUID, TEXT, BIGINT, TEXT, BIGINT, BIGINT, TEXT, TEXT, TEXT, BIGINT, JSONB,
    UUID, TEXT, TEXT
) FROM PUBLIC;

COMMIT;
