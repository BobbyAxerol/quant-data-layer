BEGIN;

ALTER FUNCTION qdl_claim_authority_outbox(TEXT, INTEGER, INTERVAL)
    SECURITY DEFINER
    SET search_path = pg_catalog, public;
ALTER FUNCTION qdl_complete_authority_outbox(UUID, TEXT, TEXT, INTEGER, BIGINT)
    SECURITY DEFINER
    SET search_path = pg_catalog, public;
ALTER FUNCTION qdl_retry_authority_outbox(UUID, TEXT, TEXT, INTERVAL)
    SECURITY DEFINER
    SET search_path = pg_catalog, public;

REVOKE ALL ON FUNCTION qdl_claim_authority_outbox(TEXT, INTEGER, INTERVAL)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION qdl_complete_authority_outbox(UUID, TEXT, TEXT, INTEGER, BIGINT)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION qdl_retry_authority_outbox(UUID, TEXT, TEXT, INTERVAL)
    FROM PUBLIC;

COMMIT;
