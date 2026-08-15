BEGIN;

CREATE OR REPLACE FUNCTION qdl_acquire_ingestion_lease(
    p_shard_id TEXT,
    p_owner_instance_id TEXT,
    p_ttl_seconds INTEGER,
    p_config_revision BIGINT
)
RETURNS TABLE(lease_epoch BIGINT, lease_expires_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    current_lease qdl_ingestion_leases%ROWTYPE;
    now_at TIMESTAMPTZ := clock_timestamp();
BEGIN
    IF btrim(p_shard_id) = '' OR btrim(p_owner_instance_id) = '' THEN
        RAISE EXCEPTION 'shard_id and owner_instance_id are required';
    END IF;
    IF p_ttl_seconds < 5 OR p_ttl_seconds > 300 THEN
        RAISE EXCEPTION 'lease TTL must be between 5 and 300 seconds';
    END IF;

    SELECT * INTO current_lease
    FROM qdl_ingestion_leases
    WHERE shard_id = p_shard_id
    FOR UPDATE;

    IF NOT FOUND THEN
        INSERT INTO qdl_ingestion_leases(
            shard_id, owner_instance_id, lease_epoch, lease_expires_at,
            heartbeat_at, config_revision
        ) VALUES (
            p_shard_id, p_owner_instance_id, 1,
            now_at + make_interval(secs => p_ttl_seconds), now_at,
            p_config_revision
        );
        RETURN QUERY SELECT 1::BIGINT, now_at + make_interval(secs => p_ttl_seconds);
        RETURN;
    END IF;

    IF current_lease.lease_expires_at > now_at
       AND current_lease.owner_instance_id <> p_owner_instance_id THEN
        RETURN;
    END IF;

    UPDATE qdl_ingestion_leases
    SET owner_instance_id = p_owner_instance_id,
        lease_epoch = CASE
            WHEN current_lease.lease_expires_at <= now_at THEN current_lease.lease_epoch + 1
            ELSE current_lease.lease_epoch
        END,
        lease_expires_at = now_at + make_interval(secs => p_ttl_seconds),
        heartbeat_at = now_at,
        config_revision = p_config_revision
    WHERE shard_id = p_shard_id
    RETURNING qdl_ingestion_leases.lease_epoch,
              qdl_ingestion_leases.lease_expires_at
    INTO lease_epoch, lease_expires_at;
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION qdl_renew_ingestion_lease(
    p_shard_id TEXT,
    p_owner_instance_id TEXT,
    p_lease_epoch BIGINT,
    p_ttl_seconds INTEGER
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    changed INTEGER;
    now_at TIMESTAMPTZ := clock_timestamp();
BEGIN
    IF p_ttl_seconds < 5 OR p_ttl_seconds > 300 THEN
        RAISE EXCEPTION 'lease TTL must be between 5 and 300 seconds';
    END IF;
    UPDATE qdl_ingestion_leases
    SET lease_expires_at = now_at + make_interval(secs => p_ttl_seconds),
        heartbeat_at = now_at
    WHERE shard_id = p_shard_id
      AND owner_instance_id = p_owner_instance_id
      AND lease_epoch = p_lease_epoch
      AND lease_expires_at > now_at;
    GET DIAGNOSTICS changed = ROW_COUNT;
    RETURN changed = 1;
END;
$$;

CREATE OR REPLACE FUNCTION qdl_release_ingestion_lease(
    p_shard_id TEXT,
    p_owner_instance_id TEXT,
    p_lease_epoch BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    changed INTEGER;
BEGIN
    UPDATE qdl_ingestion_leases
    SET lease_expires_at = clock_timestamp(), heartbeat_at = clock_timestamp()
    WHERE shard_id = p_shard_id
      AND owner_instance_id = p_owner_instance_id
      AND lease_epoch = p_lease_epoch;
    GET DIAGNOSTICS changed = ROW_COUNT;
    RETURN changed = 1;
END;
$$;

COMMIT;
