# Changelog

## 3.0.2

- **Fix**: a Thrift-protocol query returning zero rows (e.g. a `WHERE 1=0`
  filter) left `cursor.description` empty instead of the real column
  names/types. `execute_lazy_thrift` populated `schema`/`columns` only once
  a batch had actually been decoded, which never happens for a zero-row
  result -- SEA didn't have this gap, since its columns come from the
  manifest at submit time regardless of row count. Now decodes the schema
  directly out of `TGetResultSetMetadataResp.arrowSchema` (already
  captured, previously discarded) up front. Along the way, found and
  worked around a real `arrow_ipc::StreamDecoder` gotcha: its internal
  state machine only finalizes a message on the *next* message's arrival,
  so a buffer ending exactly at a bare schema message's last byte silently
  never sets the schema -- fixed by appending the same 8-byte end-of-stream
  marker `StreamWriter::finish()` itself writes.
- **Perf**: `fetchone()`/`async for row in cursor` read row-at-a-time
  through a fixed ~110us PyO3/asyncio round trip per row, measured at
  226-320x slower than `fetchall()`. Added a Python-side read-ahead row
  buffer (1000 rows per underlying `fetchmany_arrow` pull) so row iteration
  no longer pays that cost per row -- measured after the fix: ~3x slower
  than `fetchall()`, not 226-320x.
- **Perf**: merged two pairs of back-to-back `Python::attach` calls in
  `PyTokenProvider::get_token` (runs on every authenticated request for a
  custom `token_provider`) that had no `.await` between them, cutting
  redundant GIL-attach overhead on that hot path.
- **Cleanup**: removed the JSON_ARRAY pipeline (`run_json_pipeline` et al.),
  confirmed dead since `prefer_inline`'s fallback path was the only
  possible caller and never actually used it; merged `SessionPool`/
  `ThriftSessionPool`'s ~90 lines of duplicated checkout/checkin logic into
  one generic `Pool<T>`; trimmed comments across the Rust core and Python
  facade that restated information already stated elsewhere, while leaving
  every comment citing a real incident, measured benchmark, or non-obvious
  protocol/library quirk untouched.

No behavior change beyond the fixes above -- still measurably faster than
`databricks-sql-connector` on a real warehouse after this release (~1.5-2x
on a 200k-row query in repeated runs).

## 3.0.1

Three follow-up fixes found via a deliberate real-warehouse audit pass and
a variety battery covering arrays, structs, maps, VARIANT, decimals,
timestamps, GROUP BY, and DISTINCT across both protocols:

- **Fix**: `protocol="thrift"` (the default) now calls `ensure_warehouse_running`
  before submitting a statement, same as `protocol="sea"` has always done --
  a stopped warehouse previously got no proactive wake-up on the Thrift path.
- **Fix**: SEA's own chunk fetch (`fetch_chunks_with_backpressure`) and the
  JSON_ARRAY path (`stream_query_json`, `decode_json_chunk`) now truncate a
  chunk to its manifest-declared row count, closing the same
  "server can over-deliver past its declared count" gap already fixed for
  Thrift in 3.0.0 (`SELECT ... LIMIT n` returning slightly more than `n`
  rows). Not observed to actually trigger on SEA/JSON in this session's
  testing, unlike the Thrift case -- fixed defensively since the gap was
  real and the fix costs nothing when it doesn't apply.

See AGENTS.md's design-invariant entries for the full detail, including a
real A/B that briefly looked like a performance regression from these
fixes and turned out to be network noise (documented so it isn't mistaken
for signal again).

## 3.0.0

`protocol="thrift"` is now the default on `Client`/`DatabricksClient`/`connect()` --
`protocol="sea"` remains fully supported as an explicit opt-in. Thrift was
benchmarked directly against SEA on a real production warehouse and found
never slower on any query shape tested, and roughly 2x faster for small
queries (`TExecuteStatementReq`'s `getDirectResults` returns a small result
inline in the same RPC that submits the statement, where SEA always needs a
separate poll/fetch round trip).

Flipping the default was blocked on test coverage, not the backend itself:
virtually every existing test constructed a client without `protocol=` at all
(SEA was always the implicit default), against mocks that only understood
SEA's REST/JSON shape. Built real Thrift-speaking mock infrastructure in both
languages first -- a custom `wiremock::Match` routing on the Thrift RPC name
for Rust (every RPC hits one shared HTTP path), and Python mocks built on
`databricks-sql-connector`'s own real, Thrift-compiler-generated
`TCLIService` module (literal ground truth for this crate's own field IDs,
not a second hand-rolled codec) -- then migrated every existing
SEA-testing call site to explicit `protocol="sea"` before flipping the
default. Verified against the real warehouse that `connect()` with no
`protocol` argument at all genuinely resolves to Thrift (confirmed via the
resulting `statement_id`'s format, hex with no dashes, distinct from SEA's
UUID shape).

Cross-checking against `databricks-sql-connector`'s real `ttypes.py` caught a
genuine latent bug for free: `OperationStatusResp::read` mapped
`displayMessage` to field 12, but the real field is 1281 -- fixed, with a
regression test.

A follow-up review pass found and fixed one more real gap: a link discovered
before compression was authoritatively confirmed could still be queued for
download against a stale guess (`resultSetMetadata` and `resultLinks` are
independent optional fields on `TFetchResultsResp`, so nothing guaranteed a
response carrying links also carried the metadata confirming their real
compression). `run_thrift_fetch_loop` now buffers a batch's links locally
until compression is confirmed at least once, instead of queueing
immediately. Also consolidated `DbClient::new`'s internal protocol default to
match the public-facing one, restored value-level assertions two truncation
tests lost when `decode_chunk_item` stopped returning re-encoded bytes, and
pinned `THRIFT_DIRECT_RESULTS_MAX_BYTES`'s exact value so an accidental
revert can't silently reintroduce the round-trip regression it fixes.

## 2.0.1

`v2.0.0`'s Thrift backend was measurably slower than SEA for a large,
multi-chunk result (~1.7x on a 500k-row real table) -- its `FetchResults`
loop fully awaited one batch's downloads before ever asking for the next
batch's links, capping effective download concurrency at whatever one
`FetchResults` response happened to contain. Fixed by splitting
`run_thrift_fetch_loop` into a sequential producer and a fixed pool of
`chunk_fetch_concurrency` workers downloading concurrently across every
batch discovered so far. Re-verified against the real warehouse: 500k rows,
Thrift now 11.6s mean vs SEA's 11.9s (was 20.5s); 2M rows, 33.3s vs SEA's
34.6s.

## 2.0.0

Opt-in Thrift/HiveServer2 backend (`protocol="thrift"`), closing the last
speed gap against the official connector for small queries. SEA
(`protocol="sea"`) remains the default in this release.

## 1.5.0

SEA session pooling closes the small-query latency gap.
