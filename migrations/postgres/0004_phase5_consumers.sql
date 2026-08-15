BEGIN;

CREATE TABLE IF NOT EXISTS qdl_consumer_manifests (
    consumer_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    owner TEXT NOT NULL,
    sdk_major INTEGER NOT NULL CHECK (sdk_major = 2),
    rollback_contract TEXT NOT NULL CHECK (rollback_contract IN ('V1', 'V2')),
    manifest JSONB NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    retired_at TIMESTAMPTZ,
    PRIMARY KEY (consumer_id, manifest_sha256),
    CHECK (retired_at IS NULL OR retired_at >= registered_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS qdl_consumer_manifest_active_idx
    ON qdl_consumer_manifests (consumer_id) WHERE retired_at IS NULL;

CREATE TABLE IF NOT EXISTS qdl_data_requirements (
    consumer_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    requirement_index INTEGER NOT NULL CHECK (requirement_index >= 0),
    instrument_uid UUID NOT NULL,
    feed_type TEXT NOT NULL,
    interval TEXT NOT NULL DEFAULT '',
    consumer_grade TEXT NOT NULL CHECK (consumer_grade IN ('EXECUTION', 'ALPHA', 'RESEARCH')),
    source_policy_id TEXT NOT NULL,
    requirement JSONB NOT NULL,
    PRIMARY KEY (consumer_id, manifest_sha256, requirement_index),
    FOREIGN KEY (consumer_id, manifest_sha256)
        REFERENCES qdl_consumer_manifests (consumer_id, manifest_sha256),
    FOREIGN KEY (instrument_uid) REFERENCES qdl_instruments (instrument_uid)
);

CREATE TABLE IF NOT EXISTS qdl_consumer_migrations (
    migration_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    consumer_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    previous_state TEXT,
    state TEXT NOT NULL CHECK (state IN ('REGISTERED', 'SHADOW', 'ACCEPTED', 'ACTIVE', 'ROLLED_BACK')),
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (consumer_id, manifest_sha256)
        REFERENCES qdl_consumer_manifests (consumer_id, manifest_sha256)
);

CREATE INDEX IF NOT EXISTS qdl_consumer_migration_latest_idx
    ON qdl_consumer_migrations (consumer_id, changed_at DESC);

CREATE TABLE IF NOT EXISTS qdl_consumer_contract_usage_hourly (
    bucket_at TIMESTAMPTZ NOT NULL,
    consumer_id TEXT NOT NULL,
    sdk_major INTEGER NOT NULL CHECK (sdk_major IN (1, 2)),
    contract TEXT NOT NULL,
    request_count BIGINT NOT NULL CHECK (request_count >= 0),
    error_count BIGINT NOT NULL CHECK (error_count >= 0),
    last_cursor_offset BIGINT NOT NULL CHECK (last_cursor_offset >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bucket_at, consumer_id, sdk_major, contract)
);

COMMIT;
