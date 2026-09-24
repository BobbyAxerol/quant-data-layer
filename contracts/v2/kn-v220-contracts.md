# KN V2.2.0 Frozen Contracts (KN-1 K1.3)

Normative spec for the Kafka-native Rust-first read plane. Implementations:
`qdl/replay/cursor_v3.py`, `qdl/projection/state_contract.py` (Python) and
`rust/qdl-contracts/src/{cursor_v3,state_contract,requirement}.rs` (Rust). Oracle:
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
not re-encode byte-for-byte is `NON_CANONICAL`. Every pattern in this document is a
**full match** of the whole string (Python `fullmatch`, never `match` with
`$`, which also accepts a trailing `\n`).

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
| `schema` is anything else but v3, including a non-string (array, object, number) | `SCHEMA` | INVALID |
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

**Replay (no requirement).** `ReplayRequest` carries no requirement, so
Replay verifies with the same order minus the digest comparison
(`verify_scope`) and then binds the claims' `product_key` to a catalog binding
and the consumer's feed scope (`require_feed_scope`), as the Python Replay
does; Subscribe always compares the digest.

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
Rust derives it from the proto exactly as `requirement_from_proto` does,
and only for a requirement that passed the validation below.

**Requirement validation.** A requirement is validated before it is digested
or served, in the Python order: proto enums (unknown wire number ->
`ENUM_UNKNOWN`, then UNSPECIFIED for feed, grade, stale, gap, recovery,
revision; `event_recency_policy` UNSPECIFIED means absent), warmup horizon and
interval-source policy, `WarmupTimeRange`, `WarmupSpecification` bounds, then
`DataRequirement.__post_init__` (blank = Python `str.strip`, which includes
U+001C..U+001F). Each refusal has a stable rule code;
`contracts/golden/kn_v220/requirement_validation.json` holds every rule,
numeric edges on both sides and multi-violation `order_*` cases that pin which
rule is reported first, produced by the Python server path; both languages
assert the rule and the message prefix (`rule_messages`). Refusal is
`INVALID_ARGUMENT` with the Python message.

## 2. Coordinates

- **Source coordinate** `(topic_id, partition, offset)`: the committed
  canonical record a state derives from. Only this appears in cursors and
  watermarks.
- **Changelog coordinate** `(topic, partition, offset, materializer_epoch)`:
  where a derived state record sits. Delivery metadata, never a cursor.
- Decoding is strict in both languages: exactly the named fields; integers
  are JSON integers (never `true`, never `1.0`, never a string); partition
  `0..2^31-1`, offsets `0..2^63-1`, `materializer_epoch >= 1`; topic names are
  non-empty strings. A BAR state's `is_final` is a boolean, `revision` a
  uint32 and `content_sha256` 64 lowercase hex.
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

**BAR cache row** (current index, one row per open time): the canonical
EventEnvelope bytes without the LPK-derived fields (instrument uid, venue,
market, bar interval), behind a 48-byte trailer (source offset u64,
materializer epoch u64, SHA-256 of the canonical bytes). Decoding restores
those four fields from the product's LPK and must reproduce the canonical
bytes whose hash is stored; any other field (schema version, provider, source
id/role, symbol, instrument id/revision, provenance) may differ between rows of
one product and is always kept in the row. A row whose uid/venue/market/
interval differ from its key is refused.

**Cache read state:** no published ready generation ->
NOT_READY_NO_GENERATION; entry missing -> NOT_READY_MISSING; entry from
another generation -> NOT_READY_OTHER_GENERATION; else READY. Missing data is
typed, never a default value.

## 5. Rebuild epoch and read consistency (binding on KN-3/KN-4)

Frozen after Astra review R2 (2026-09-24): the ready generation is **per
product**, not global. Guide 18.4.4 allows it because every public read is
coherent per product and no read promises a cross-product snapshot.

- **Per-product ready pointer and fence.** Each logical product key has one
  ready pointer `(generation, fence)`. A rebuild of a product allocates a new
  generation, captures that product's committed canonical boundary, restores
  latest state / BAR retention floor from the state topics, tails to the
  boundary, verifies coverage, then publishes the pointer atomically (one
  compare-and-set on the pointer; the fence increases monotonically). A whole
  cache rebuild is a sequence of product swaps, one product in staging at a
  time.
- **Stale writers are rejected.** Every cache write carries the product's
  generation and fence; a write whose fence is lower than the pointer's, or
  whose generation is not the pointer's staging/ready generation, is refused
  (typed, counted). A writer never re-publishes a pointer it did not hold.
- **No mixed view.** Payload, quality, watermark and generation of one product
  are read from one versioned view. A multi-page warmup pins the product's
  generation; if the pointer changes, it detects the change and retries
  within a bound (at most the warmup deadline, <= 120 s), else fails typed. It
  never merges rows of two generations. `require_all` batches keep per-item
  watermarks; they are not an atomic global snapshot, and during a full
  rebuild different products may be at different generations.
- **Memory is counted until reclaimed.** A superseded product generation is
  deleted after its swap and counted against the cache budget until its
  memory is physically reclaimed; the next product's staging starts only
  after that. Peak = steady + two largest products (budget `market_cache`).
- **Cache generation is not the cursor generation.** A cache generation or
  fence never enters a public cursor; a rebuild does not reset consumers whose
  cursor contract is still valid, and cursor v3 validity never depends on it.
- Readiness is per product: a global flag never hides a missing key.

## 6. Stream delivery policy (binding on KN-2 and every materializer)

`contracts/golden/kn_v220/delivery_policy.json`, produced from
`qdl.ingestion.contracts.delivery_policy`: TRADE, BOOK_SNAPSHOT and
BOOK_DELTA are LOSSLESS; BAR is LOSSLESS when FINAL, REVISED or CANCELLED and
LIFECYCLE_COALESCE when IN_PROGRESS (a BAR without a lifecycle is refused by the
domain and treated as LOSSLESS); every other public feed is LATEST_STATE.
Invariant 27 adds: a record may be dropped only when a later record with the
same lifecycle key (BAR open time; the product for latest-state feeds) and the
same lifecycle signature (sorted quality flags, authority revision, source
id/role, provider, source session, connection generation, lease epoch)
supersedes it, and only under buffer pressure. A quality-state or
source-authority transition changes the signature and is never coalesced
away. The earlier Python stream's coalescing of BOOK_SNAPSHOT is not ported.
