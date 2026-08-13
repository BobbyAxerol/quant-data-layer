BEGIN;

CREATE TABLE IF NOT EXISTS qdl_config_revisions (
    config_revision BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS qdl_session_calendars (
    calendar_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    timezone_iana TEXT NOT NULL,
    continuous BOOLEAN NOT NULL DEFAULT FALSE,
    definition JSONB NOT NULL,
    valid_from_ns BIGINT NOT NULL,
    valid_to_ns BIGINT,
    PRIMARY KEY (calendar_id, revision),
    CHECK (valid_to_ns IS NULL OR valid_to_ns > valid_from_ns)
);

CREATE TABLE IF NOT EXISTS qdl_instruments (
    instrument_uid UUID PRIMARY KEY,
    instrument_id TEXT NOT NULL UNIQUE,
    venue TEXT NOT NULL,
    market TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    product_type TEXT NOT NULL,
    current_metadata_revision BIGINT NOT NULL CHECK (current_metadata_revision > 0),
    current_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS qdl_instrument_revisions (
    instrument_uid UUID NOT NULL,
    metadata_revision BIGINT NOT NULL CHECK (metadata_revision > 0),
    product_type TEXT NOT NULL,
    native_symbol TEXT NOT NULL,
    base_asset TEXT NOT NULL DEFAULT '',
    quote_asset TEXT NOT NULL DEFAULT '',
    settlement_asset TEXT NOT NULL DEFAULT '',
    price_tick TEXT NOT NULL,
    quantity_step TEXT NOT NULL,
    contract_multiplier TEXT NOT NULL,
    expiry_time_ns BIGINT,
    strike_price TEXT,
    option_type TEXT,
    underlying_instrument_uid UUID,
    session_calendar_id TEXT NOT NULL,
    status TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    valid_from_ns BIGINT NOT NULL,
    valid_to_ns BIGINT,
    PRIMARY KEY (instrument_uid, metadata_revision),
    FOREIGN KEY (instrument_uid) REFERENCES qdl_instruments (instrument_uid),
    CHECK (valid_to_ns IS NULL OR valid_to_ns > valid_from_ns),
    CHECK (product_type <> 'OPTION' OR (expiry_time_ns IS NOT NULL AND strike_price IS NOT NULL AND option_type IS NOT NULL)),
    CHECK (product_type <> 'FUTURE' OR expiry_time_ns IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS qdl_instrument_revisions_current_idx
    ON qdl_instrument_revisions (instrument_uid) WHERE valid_to_ns IS NULL;

CREATE TABLE IF NOT EXISTS qdl_instrument_aliases (
    provider TEXT NOT NULL,
    market TEXT NOT NULL,
    native_symbol TEXT NOT NULL,
    valid_from_ns BIGINT NOT NULL,
    valid_to_ns BIGINT,
    instrument_uid UUID NOT NULL,
    instrument_revision BIGINT NOT NULL,
    PRIMARY KEY (provider, market, native_symbol, valid_from_ns),
    FOREIGN KEY (instrument_uid, instrument_revision)
        REFERENCES qdl_instrument_revisions (instrument_uid, metadata_revision),
    CHECK (valid_to_ns IS NULL OR valid_to_ns > valid_from_ns)
);

CREATE INDEX IF NOT EXISTS qdl_alias_resolve_idx
    ON qdl_instrument_aliases (provider, market, native_symbol, valid_from_ns, valid_to_ns);

CREATE TABLE IF NOT EXISTS qdl_source_profiles (
    source_profile_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    venue TEXT NOT NULL,
    market TEXT NOT NULL,
    region_profile TEXT NOT NULL,
    legal_entity TEXT NOT NULL,
    account_tier TEXT NOT NULL,
    capability_manifest JSONB NOT NULL,
    verified_at TIMESTAMPTZ,
    valid_from_revision BIGINT NOT NULL,
    valid_to_revision BIGINT
);

CREATE TABLE IF NOT EXISTS qdl_source_policies (
    source_policy_id TEXT PRIMARY KEY,
    instrument_pattern TEXT NOT NULL,
    feed_type TEXT NOT NULL,
    allowed_source_roles TEXT[] NOT NULL,
    max_freshness_ms BIGINT NOT NULL CHECK (max_freshness_ms >= 0),
    allow_cross_venue_reference BOOLEAN NOT NULL DEFAULT FALSE,
    on_gap TEXT NOT NULL,
    on_stale TEXT NOT NULL,
    on_fallback TEXT NOT NULL,
    config_revision BIGINT NOT NULL,
    FOREIGN KEY (config_revision) REFERENCES qdl_config_revisions (config_revision)
);

CREATE TABLE IF NOT EXISTS qdl_subscription_specs (
    subscription_id UUID PRIMARY KEY,
    instrument_uid UUID NOT NULL,
    feed_type TEXT NOT NULL,
    interval TEXT,
    source_policy_id TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    desired_state TEXT NOT NULL,
    config_revision BIGINT NOT NULL,
    requested_by TEXT NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    valid_to TIMESTAMPTZ,
    UNIQUE NULLS NOT DISTINCT (instrument_uid, feed_type, interval, source_policy_id, valid_from),
    FOREIGN KEY (instrument_uid) REFERENCES qdl_instruments (instrument_uid),
    FOREIGN KEY (source_policy_id) REFERENCES qdl_source_policies (source_policy_id),
    FOREIGN KEY (config_revision) REFERENCES qdl_config_revisions (config_revision),
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE TABLE IF NOT EXISTS qdl_ingestion_leases (
    shard_id TEXT PRIMARY KEY,
    owner_instance_id TEXT NOT NULL,
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    config_revision BIGINT NOT NULL,
    FOREIGN KEY (config_revision) REFERENCES qdl_config_revisions (config_revision)
);

CREATE TABLE IF NOT EXISTS qdl_job_states (
    job_id UUID PRIMARY KEY,
    job_type TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL,
    lease_owner TEXT,
    lease_epoch BIGINT,
    attempt INTEGER NOT NULL DEFAULT 0,
    request JSONB NOT NULL,
    result JSONB,
    error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS qdl_control_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    config_revision BIGINT,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    before_state JSONB,
    after_state JSONB,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (config_revision) REFERENCES qdl_config_revisions (config_revision)
);

COMMIT;
