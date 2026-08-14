BEGIN;

CREATE TABLE IF NOT EXISTS qdl_consumer_manifest_access (
    consumer_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    subject TEXT NOT NULL,
    environment TEXT NOT NULL,
    manifest_revision BIGINT NOT NULL CHECK (manifest_revision > 0),
    allowed_purposes JSONB NOT NULL,
    allowed_permissions JSONB NOT NULL,
    execution_dependency TEXT NOT NULL
        CHECK (execution_dependency IN ('FORBIDDEN', 'PAPER_ONLY', 'ALLOWED')),
    quotas JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_id, manifest_sha256),
    FOREIGN KEY (consumer_id, manifest_sha256)
        REFERENCES qdl_consumer_manifests (consumer_id, manifest_sha256),
    UNIQUE (environment, subject, manifest_revision),
    CHECK (jsonb_typeof(allowed_purposes) = 'array'),
    CHECK (jsonb_array_length(allowed_purposes) > 0),
    CHECK (jsonb_typeof(allowed_permissions) = 'array'),
    CHECK (jsonb_array_length(allowed_permissions) > 0),
    CHECK (jsonb_typeof(quotas) = 'object')
);

CREATE INDEX IF NOT EXISTS qdl_consumer_manifest_subject_idx
    ON qdl_consumer_manifest_access (environment, subject);

COMMIT;
