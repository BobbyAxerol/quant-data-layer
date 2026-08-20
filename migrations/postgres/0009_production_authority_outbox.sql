BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS qdl_authority_event_outbox (
    event_id UUID PRIMARY KEY,
    transition_id UUID NOT NULL UNIQUE
        REFERENCES qdl_authority_transition_audit(transition_id),
    slice_id TEXT NOT NULL REFERENCES qdl_authority_slices(slice_id),
    authority_revision BIGINT NOT NULL CHECK (authority_revision > 0),
    event_kind TEXT NOT NULL CHECK (event_kind = 'AUTHORITY_TRANSITION'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'DISPATCHING', 'PUBLISHED', 'BLOCKED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    locked_at TIMESTAMPTZ,
    lock_owner TEXT,
    last_error TEXT,
    topic TEXT,
    topic_partition INTEGER,
    topic_offset BIGINT CHECK (topic_offset IS NULL OR topic_offset >= 0),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (slice_id, authority_revision, event_kind),
    CHECK ((locked_at IS NULL) = (lock_owner IS NULL)),
    CHECK (
        (status = 'PUBLISHED' AND topic IS NOT NULL
         AND topic_partition IS NOT NULL AND topic_offset IS NOT NULL
         AND published_at IS NOT NULL)
        OR status <> 'PUBLISHED'
    )
);

CREATE INDEX IF NOT EXISTS qdl_authority_outbox_dispatch_idx
    ON qdl_authority_event_outbox (status, available_at, created_at)
    WHERE status IN ('PENDING', 'DISPATCHING');

CREATE OR REPLACE FUNCTION qdl_reject_authority_outbox_payload_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_id <> OLD.event_id
       OR NEW.transition_id <> OLD.transition_id
       OR NEW.slice_id <> OLD.slice_id
       OR NEW.authority_revision <> OLD.authority_revision
       OR NEW.event_kind <> OLD.event_kind
       OR NEW.payload <> OLD.payload
       OR NEW.payload_sha256 <> OLD.payload_sha256
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'authority outbox identity/payload is immutable';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS qdl_authority_outbox_payload_immutable
    ON qdl_authority_event_outbox;
CREATE TRIGGER qdl_authority_outbox_payload_immutable
BEFORE UPDATE ON qdl_authority_event_outbox
FOR EACH ROW EXECUTE FUNCTION qdl_reject_authority_outbox_payload_mutation();

CREATE OR REPLACE FUNCTION qdl_enqueue_authority_transition()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    authority_row qdl_authority_slices%ROWTYPE;
    handoff_row qdl_authority_handoffs%ROWTYPE;
    checkpoint_row qdl_terminal_owner_checkpoints%ROWTYPE;
    event_payload JSONB;
BEGIN
    SELECT * INTO authority_row
    FROM qdl_authority_slices AS authority_slice
    WHERE authority_slice.slice_id = NEW.slice_id;
    IF NOT FOUND OR authority_row.authority_revision <> NEW.new_revision THEN
        RAISE EXCEPTION 'authority outbox cannot snapshot a divergent authority row';
    END IF;

    SELECT * INTO handoff_row
    FROM qdl_authority_handoffs AS handoff
    WHERE handoff.slice_id = NEW.slice_id
      AND handoff.expected_authority_revision = NEW.previous_revision
      AND handoff.new_authority_revision = NEW.new_revision
      AND handoff.old_owner_id = NEW.previous_owner_id
      AND handoff.new_owner_id = NEW.new_owner_id
    ORDER BY handoff.created_at DESC
    LIMIT 1;
    IF FOUND THEN
        SELECT * INTO checkpoint_row
        FROM qdl_terminal_owner_checkpoints AS checkpoint
        WHERE checkpoint.checkpoint_id = handoff_row.checkpoint_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'authority handoff checkpoint is missing';
        END IF;
    END IF;

    event_payload := jsonb_build_object(
        'schema', 'qdl.authority-outbox-event.v1',
        'event_id', NEW.transition_id,
        'transition', to_jsonb(NEW),
        'authority', to_jsonb(authority_row),
        'handoff', CASE WHEN handoff_row.handoff_id IS NULL THEN NULL ELSE to_jsonb(handoff_row) END,
        'checkpoint', CASE WHEN checkpoint_row.checkpoint_id IS NULL THEN NULL ELSE to_jsonb(checkpoint_row) END
    );

    INSERT INTO qdl_authority_event_outbox (
        event_id, transition_id, slice_id, authority_revision,
        event_kind, payload, payload_sha256
    ) VALUES (
        NEW.transition_id, NEW.transition_id, NEW.slice_id, NEW.new_revision,
        'AUTHORITY_TRANSITION', event_payload,
        encode(digest(convert_to(event_payload::text, 'UTF8'), 'sha256'), 'hex')
    )
    ON CONFLICT (event_id) DO NOTHING;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS qdl_authority_transition_outbox
    ON qdl_authority_transition_audit;
CREATE TRIGGER qdl_authority_transition_outbox
AFTER INSERT ON qdl_authority_transition_audit
FOR EACH ROW EXECUTE FUNCTION qdl_enqueue_authority_transition();

CREATE OR REPLACE FUNCTION qdl_claim_authority_outbox(
    p_lock_owner TEXT,
    p_limit INTEGER,
    p_lock_timeout INTERVAL DEFAULT INTERVAL '2 minutes'
)
RETURNS SETOF qdl_authority_event_outbox
LANGUAGE plpgsql
AS $$
BEGIN
    IF btrim(p_lock_owner) = '' OR p_limit < 1 OR p_limit > 100
       OR p_lock_timeout <= INTERVAL '0 seconds' THEN
        RAISE EXCEPTION 'authority outbox claim bounds are invalid';
    END IF;
    RETURN QUERY
    WITH candidates AS (
        SELECT event_id
        FROM qdl_authority_event_outbox
        WHERE available_at <= clock_timestamp()
          AND (
            status = 'PENDING'
            OR (
                status = 'DISPATCHING'
                AND locked_at < clock_timestamp() - p_lock_timeout
            )
          )
        ORDER BY created_at, event_id
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE qdl_authority_event_outbox AS outbox
    SET status = 'DISPATCHING',
        attempts = outbox.attempts + 1,
        locked_at = clock_timestamp(),
        lock_owner = p_lock_owner,
        last_error = NULL
    FROM candidates
    WHERE outbox.event_id = candidates.event_id
    RETURNING outbox.*;
END;
$$;

CREATE OR REPLACE FUNCTION qdl_complete_authority_outbox(
    p_event_id UUID,
    p_lock_owner TEXT,
    p_topic TEXT,
    p_partition INTEGER,
    p_offset BIGINT
)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    UPDATE qdl_authority_event_outbox
    SET status = 'PUBLISHED',
        topic = p_topic,
        topic_partition = p_partition,
        topic_offset = p_offset,
        published_at = clock_timestamp(),
        locked_at = NULL,
        lock_owner = NULL,
        last_error = NULL
    WHERE event_id = p_event_id
      AND status = 'DISPATCHING'
      AND lock_owner = p_lock_owner
      AND btrim(p_topic) <> ''
      AND p_partition >= 0
      AND p_offset >= 0;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'authority outbox completion lost ownership or has invalid ACK';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION qdl_retry_authority_outbox(
    p_event_id UUID,
    p_lock_owner TEXT,
    p_error TEXT,
    p_retry_after INTERVAL
)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    IF btrim(p_error) = '' OR p_retry_after <= INTERVAL '0 seconds' THEN
        RAISE EXCEPTION 'authority outbox retry requires bounded error/delay';
    END IF;
    UPDATE qdl_authority_event_outbox
    SET status = CASE WHEN attempts >= 20 THEN 'BLOCKED' ELSE 'PENDING' END,
        available_at = clock_timestamp() + p_retry_after,
        locked_at = NULL,
        lock_owner = NULL,
        last_error = left(p_error, 2000)
    WHERE event_id = p_event_id
      AND status = 'DISPATCHING'
      AND lock_owner = p_lock_owner;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'authority outbox retry lost ownership';
    END IF;
END;
$$;

REVOKE EXECUTE ON FUNCTION qdl_claim_authority_outbox(TEXT, INTEGER, INTERVAL)
    FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION qdl_complete_authority_outbox(UUID, TEXT, TEXT, INTEGER, BIGINT)
    FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION qdl_retry_authority_outbox(UUID, TEXT, TEXT, INTERVAL)
    FROM PUBLIC;

COMMIT;
