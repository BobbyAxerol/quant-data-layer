BEGIN;

CREATE TABLE IF NOT EXISTS qdl_feed_quality_state (
    source_id TEXT NOT NULL,
    instrument_uid UUID NOT NULL,
    feed_type TEXT NOT NULL,
    state TEXT NOT NULL,
    last_sequence TEXT,
    last_event_id BYTEA,
    last_source_time_ns BIGINT,
    last_received_time_ns BIGINT,
    expected_next_sequence TEXT,
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    normalizer_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_id, instrument_uid, feed_type),
    FOREIGN KEY (instrument_uid) REFERENCES qdl_instruments (instrument_uid)
);

CREATE TABLE IF NOT EXISTS qdl_sequence_gaps (
    gap_id UUID PRIMARY KEY,
    source_id TEXT NOT NULL,
    instrument_uid UUID NOT NULL,
    feed_type TEXT NOT NULL,
    expected_sequence TEXT NOT NULL,
    observed_sequence TEXT NOT NULL,
    state TEXT NOT NULL,
    detected_at_ns BIGINT NOT NULL,
    resolved_at_ns BIGINT,
    resolution_type TEXT,
    snapshot_reference TEXT,
    FOREIGN KEY (instrument_uid) REFERENCES qdl_instruments (instrument_uid),
    CHECK (resolved_at_ns IS NULL OR resolved_at_ns >= detected_at_ns)
);

CREATE INDEX IF NOT EXISTS qdl_sequence_gaps_open_idx
    ON qdl_sequence_gaps (source_id, instrument_uid, feed_type)
    WHERE resolved_at_ns IS NULL;

CREATE TABLE IF NOT EXISTS qdl_source_authority_events (
    authority_event_id UUID PRIMARY KEY,
    source_policy_id TEXT NOT NULL,
    instrument_uid UUID NOT NULL,
    feed_type TEXT NOT NULL,
    previous_source_id TEXT,
    selected_source_id TEXT,
    previous_state TEXT NOT NULL,
    state TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    config_revision BIGINT NOT NULL,
    occurred_at_ns BIGINT NOT NULL,
    FOREIGN KEY (instrument_uid) REFERENCES qdl_instruments (instrument_uid),
    FOREIGN KEY (source_policy_id) REFERENCES qdl_source_policies (source_policy_id),
    FOREIGN KEY (config_revision) REFERENCES qdl_config_revisions (config_revision)
);

CREATE TABLE IF NOT EXISTS qdl_materialization_snapshots (
    snapshot_id UUID PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    parent_snapshot_id UUID,
    state TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    data_checksum TEXT NOT NULL CHECK (data_checksum ~ '^[0-9a-f]{64}$'),
    manifest_uri TEXT NOT NULL,
    source_cursor_start TEXT NOT NULL,
    source_cursor_end TEXT NOT NULL,
    normalizer_version TEXT NOT NULL,
    config_revision BIGINT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (dataset_id, source_cursor_start, source_cursor_end, normalizer_version),
    FOREIGN KEY (parent_snapshot_id) REFERENCES qdl_materialization_snapshots (snapshot_id),
    FOREIGN KEY (config_revision) REFERENCES qdl_config_revisions (config_revision)
);

CREATE INDEX IF NOT EXISTS qdl_materialization_dataset_idx
    ON qdl_materialization_snapshots (dataset_id, committed_at DESC);

CREATE TABLE IF NOT EXISTS qdl_handoff_checkpoints (
    consumer_id TEXT NOT NULL,
    instrument_uid UUID NOT NULL,
    feed_type TEXT NOT NULL,
    interval TEXT NOT NULL DEFAULT '',
    snapshot_id UUID NOT NULL,
    cursor_token_sha256 TEXT NOT NULL CHECK (cursor_token_sha256 ~ '^[0-9a-f]{64}$'),
    confirmed_offset BIGINT NOT NULL CHECK (confirmed_offset >= 0),
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_id, instrument_uid, feed_type, interval),
    FOREIGN KEY (instrument_uid) REFERENCES qdl_instruments (instrument_uid),
    FOREIGN KEY (snapshot_id) REFERENCES qdl_materialization_snapshots (snapshot_id)
);

COMMIT;
