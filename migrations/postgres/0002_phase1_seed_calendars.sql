BEGIN;

INSERT INTO qdl_session_calendars (
    calendar_id,
    revision,
    timezone_iana,
    continuous,
    definition,
    valid_from_ns
) VALUES
    (
        'CRYPTO_24X7',
        1,
        'UTC',
        TRUE,
        '{"sessions":[{"name":"continuous","start":"00:00","end":"24:00"}],"holidays":[]}'::jsonb,
        0
    ),
    (
        'VN_MARKET_V1',
        1,
        'Asia/Ho_Chi_Minh',
        FALSE,
        '{"sessions":[{"name":"morning","start":"09:00","end":"11:30"},{"name":"afternoon","start":"13:00","end":"14:30"}],"holiday_source":"controlled_calendar_revision"}'::jsonb,
        0
    )
ON CONFLICT (calendar_id, revision) DO NOTHING;

COMMIT;

