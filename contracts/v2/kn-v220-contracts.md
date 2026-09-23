# KN V2.2.0 Frozen Contracts (KN-1 K1.3)

Normative spec for the Kafka-native Rust-first read plane. Implementations:
`qdl/replay/cursor_v3.py`, `qdl/projection/state_contract.py` (Python) and
`rust/qdl-contracts/src/{cursor_v3,state_contract}.rs` (Rust). Oracle:
`contracts/golden/kn_v220/*.json`, read by `tests/test_kn_v220_contracts.py`
and the Rust unit tests. Design context: guide section 18.4-18.5
(`upgrade/DATA_LAYER_V2_KAFKA_NATIVE_ARCHITECTURE_REVIEW.md#kn-contracts-and-correctness`).
Public HTTP/gRPC field shapes do not change; the token stays opaque.

## 1. Cursor v3

**Encoding.** `token = b64url(body) "." b64url(HMAC-SHA256(key[key_id], body))`,
base64url without padding and with canonical trailing bits. `body` is a JSON
object: keys sorted by byte value, no whitespace, `,` and `:` separators;
every value is either a string matching `[A-Za-z0-9._:/@|+=-]{1,256}` (never
escaped) or a decimal unsigned integer `0..2^63-1` (never a float or bool).
Verification uses the received body bytes; a validly signed body that does
not re-encode byte-for-byte is `NON_CANONICAL`.

**Claims** (all required, no others): `schema="qdl.handoff-cursor.v3"`,
`key_id`, `environment`, `consumer_id`, `requirement_digest` (64 lowercase
hex), `schema_major`, `stream`, `product_key` (section 3), `snapshot_id`,
`source_topic_id`, `source_partition`, `source_offset` (last canonical offset
applied to the snapshot view; replay starts strictly after it),
`partition_plan_epoch`, `source_policy_revision`, `catalog_revision`,
`route_generation`, `issued_at_ns`, `expires_at_ns` (> issued).

**Verification order and outcomes** (first failure wins, identical in both
languages):

| Step | Failure | Outcome |
|---|---|---|
| shape: one `.`, <= 4096 bytes, both parts canonical base64url, UTF-8 JSON object, no NaN/Infinity | `ENCODING` | INVALID |
| `schema` is v1/v2 (spool offsets) | `LEGACY_SCHEMA` | **EXPIRED** |
| `schema` is anything else but v3 | `SCHEMA` | INVALID |
| `key_id` unknown | `UNKNOWN_KEY` | INVALID |
| HMAC mismatch | `SIGNATURE` | INVALID |
| field set not exact | `FIELDS` | INVALID |
| per field, dataclass order: integer type/range, string charset; digest hex; expiry > issue | `FIELD_RANGE` / `FIELD_CHARSET` | INVALID |
| body not canonical | `NON_CANONICAL` | INVALID |
| consumer != authenticated consumer | `CONSUMER` | INVALID |
| environment != request and replica | `ENVIRONMENT` | INVALID |
| digest != request requirement digest | `REQUIREMENT` | INVALID |
| schema major, stream, topic id, plan epoch, source-policy, catalog, route generation (in this order) | `SCHEMA_MAJOR` ... `ROUTE_GENERATION` | **EXPIRED** |
| `now >= expires_at_ns` | `EXPIRED` | **EXPIRED** |

EXPIRED is gRPC `OUT_OF_RANGE` and the SDK answers it with a fresh snapshot
(`qdl_sdk/client.py` `CursorExpiredError`). INVALID is `INVALID_ARGUMENT`, on
which the SDK raises; a normal migration or rollback must never produce it.
Only the active key signs; any configured key verifies (rotation).

**Requirement digest.** SHA-256 over UTF-8 lines joined by `\n`:
`qdl.requirement-digest.v1`, then `name=value` for `instrument_uid`, `feed`,
`interval`, `consumer_grade`, `source_policy_id`, `max_freshness_ms`,
`event_recency_policy`, `max_session_liveness_ms`, `require_full_coverage`,
`require_final_bars`, `stale_policy`, `gap_policy`, `recovery`,
`bar_revision_policy`. Enum names drop the proto prefix; absent/zero optional
values are empty; booleans are `true`/`false`. The warmup horizon is excluded.
Rust derives it from the proto exactly as `requirement_from_proto` does.

## 2. Coordinates

- **Source coordinate** `(topic_id, partition, offset)`: the committed
  canonical record a state derives from. Only this appears in cursors and
  watermarks.
- **Changelog coordinate** `(topic, partition, offset, materializer_epoch)`:
  where a derived state record sits. Delivery metadata, never a cursor.
- Offsets compare only inside one `(topic_id, partition)`; across topic
  identities or partitions the result is `NOT_COMPARABLE` and the sink fence
  decides. Offset gaps from filtering or transaction markers are valid; source
  gaps are proven from source sequence, never from `offset == previous + 1`.

## 3. Logical product key

`lpk1|environment|VENUE|MARKET|instrument_uid|FEED|qualifier`; fields match
`[A-Za-z0-9._:-]{1,96}`; venue, market and feed are upper-case enum names;
qualifier is the interval, or `-`. BOOK_SNAPSHOT and BOOK_DELTA are distinct
products even though they share one physical Kafka key.

## 4. State rules

**Latest apply (stage B):** empty -> APPLY; higher offset -> APPLY; equal ->
DUPLICATE; lower -> STALE; other topic/partition -> NOT_COMPARABLE.

**BAR revision** (per product and open time): empty -> APPLY; final then
in-progress -> STALE_IN_PROGRESS; in-progress then final -> APPLY; two
in-progress -> latest-apply by source coordinate; two final: higher revision
-> APPLY, lower -> STALE_REVISION, equal and same canonical payload hash ->
DUPLICATE, equal and different -> CONFLICT (never last-write-wins). Revision
facts are append-only; the current index holds one row per open time.

**Cache read state:** no published ready generation ->
NOT_READY_NO_GENERATION; entry missing -> NOT_READY_MISSING; entry from
another generation -> NOT_READY_OTHER_GENERATION; else READY. Missing data is
typed, never a default value.

## 5. Rebuild epoch and read consistency (binding on KN-3/KN-4)

- A cache rebuild allocates a new generation, captures the readable committed
  canonical boundaries, restores latest state and BAR retention floors from
  the state topics, tails to the boundary, verifies coverage and publishes the
  ready generation atomically. The cache generation is not the public cursor
  generation: a cache rebuild does not reset consumers whose cursor contract
  is still valid.
- Payload, quality, watermark and generation of one product are read from one
  versioned view. A multi-page warmup pins the generation or detects a change
  and retries within a bound. `require_all` batches keep per-item watermarks;
  they are not an atomic global snapshot.
- Readiness is per product: a global flag never hides a missing key.
