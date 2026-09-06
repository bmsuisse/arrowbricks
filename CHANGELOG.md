# Changelog

## 3.1.3 — 2026-09-06

- Anonymized internal catalog/table references in benchmark reports,
  source comments, test fixtures, and contributor notes.

- Cloud-fetch Range downloads start the remaining requests as soon as the
  first response's headers reveal the file size, overlapping its body
  transfer. The split budget is shared across links in each discovered
  batch, preventing the first files from claiming the slots their peers
  need. Failed probe attempts cancel and join their tail requests before
  retrying; dropped downloads cancel outstanding range tasks.
- Cached IPC replay (`ReplayableArrowChunk` / `_core.read_ipc_stream`) now
  shares immutable Python bytes with decoded Arrow arrays instead of copying
  every column on every read. The input stays alive until its last consumer
  releases it. Misaligned fixed-width buffers still use Arrow's safe copy
  fallback. Empty/corrupt streams and trailing bytes after EOS are rejected;
  schema-only empty results remain supported.
- Thrift downloads start immediately when direct-result metadata already
  confirms compression, overlapping subsequent `FetchResults` calls.
  A complete single-response cloud-fetch file avoids an extra buffer copy.
- Removed the Arrow umbrella dependency and three unused compute crates.
  Development builds keep backtrace line numbers without full debug type
  data; the Python/Arrow bridge uses size optimization separately from the
  IPC decoder.
- Corrected the benchmark's peak-RSS measurement, which previously sampled
  before imports and queries. Added reproducible cached-replay and
  interleaved, two-version warehouse benchmarks.
- Fixed a flaky parser property-test fixture that generated zero nested
  fields and then unwrapped a nonexistent first field; the nesting test
  now generates at least one field. The parser itself is unchanged.

## 3.1.2

- **Refactor**: `client.rs` (2,492 lines) and `pipeline.rs` (2,652 lines) split
  into cohesive submodules, each under 900 lines -- `client/{error,model,
  download,sea,thrift_rpc,volume}.rs` and `pipeline/{reorder,stats,sea,
  thrift_exec,ndjson}.rs`, with slim root files re-exporting everything via
  `pub use` so every existing `crate::client::X`/`crate::pipeline::X` path is
  unchanged. Pure code organization -- no behavior change, no public API
  change, same test suite passes unchanged. A code-review pass caught and
  fixed a handful of doc comments whose "above"/"below"/"this file"
  locality pointers went stale across the split (including one on
  `cancel_statement` documenting a real non-idempotent-SQL-retry safety
  invariant) and a duplicated test helper -- all corrected to point at the
  right file.

## 3.1.1

- **Refactor**: hand-rolled base64 decoder in `json_convert.rs` replaced with
  the `base64` crate. Zero `.so` growth -- `base64` (matching version) is
  already linked in unconditionally via `arrow-cast`, so this trades ~28
  lines of hand-rolled decode logic (and its own dedicated proptest) for a
  direct dependency on a crate already present in the compiled binary.
- **Cleanup**: `HeartbeatWait`/`HeartbeatStream`'s `Drop` impls and their
  `tick()` timeout-check logic were byte-for-byte duplicated between the two
  types. Extracted into two shared free functions
  (`check_deadline_or_wait`/`abort_on_drop` in `heartbeat.rs`) that both
  types now call -- one place to read/fix the abort-then-await-the-join
  reasoning instead of two copies that could silently drift apart. No
  behavior change; same tests cover both types before and after.

## 3.1.0

- **Fix (data safety)**: `prefer_inline=True` no longer silently re-runs a
  statement that already succeeded. Found in code review: when an INLINE
  result's JSON couldn't be converted to Arrow (an unsupported column type,
  or -- defensively -- a SUCCEEDED response with no `data_array` at all),
  arrowbricks used to transparently resubmit the identical SQL as a fresh
  statement to fetch it the normal way. That statement had already run and
  produced real rows server-side -- for non-idempotent SQL (INSERT/MERGE/
  UPDATE/DELETE), resubmitting it duplicated the write, with nothing
  surfaced to the caller. Both cases now raise `ArrowbricksError` naming the
  statement instead of re-executing anything; see README.md's `prefer_inline`
  entry for the corrected behavior (the *other* `prefer_inline` fallback --
  the result being too big for INLINE's byte cap -- is unaffected and still
  safely re-runs, since that statement fails server-side before ever really
  executing).
- **New**: typed exception hierarchy. Every `ApiError` crossing the PyO3
  boundary used to collapse to a plain `RuntimeError` (message only) --
  callers couldn't programmatically distinguish "safe to retry" from "don't
  retry" from "auth problem" without regex-parsing the message. Now raises
  one of `ArrowbricksError` (the base), `TransientError` (a network blip or
  5xx that survived every internal retry), `AuthError` (401/403, survived
  every internal retry too -- each one re-fetches a token -- *or* your own
  `token_provider` callable itself raised, e.g. its OAuth refresh came back
  unauthorized), or `StatementError` (the SQL statement itself failed/was
  canceled server-side) -- see README.md's new "Errors" section.
  **Backward compatible**: every new exception type subclasses
  `RuntimeError`, so an `except RuntimeError` written before this change
  keeps working unchanged. `QueryTimeout` now additionally subclasses
  `ArrowbricksError` (previously just `RuntimeError`) for a consistent
  catch-all -- its own behavior/call sites are otherwise untouched.
  Implemented via PyO3's `create_exception!` (real CPython exception types
  registered in the compiled `._core` submodule, not Python classes reached
  via a per-error module lookup) -- see
  `rust/arrowbricks_core/src/lib.rs`'s "Typed error taxonomy" section.
- **New**: `retry_attempts`/`retry_max_wait_s` constructor kwargs on
  `connect`/`DatabricksClient`/`Client`, defaulting to `6`/`20.0` (unchanged
  from the previous hardcoded behavior). Tunes the retry policy behind
  `TransientError`/`AuthError` above -- total attempts and the exponential-
  backoff ceiling, for every retryable request this client makes (statement
  submit/poll, chunk-index resolution, chunk download, volume file ops).
  `retry_attempts` must be at least 1, and always raises `ValueError` (not
  `OverflowError`) for an out-of-range value including negative ones -- the
  PyO3-facing parameter is a signed `i64`, not `u32`, specifically so a
  negative Python int reaches this crate's own validation instead of
  failing PyO3's own argument conversion first with the wrong exception
  type. Same "Rust constant -> PyO3 default -> Python kwarg default ->
  `.pyi` stub" threading pattern `chunk_fetch_concurrency` already uses --
  see AGENTS.md's own entry on that one's footgun for why each layer's
  default has to actually match, not just look like it does.
- **New** (Rust-internal, not user-facing): `proptest`-based property tests
  for the hand-rolled wire-protocol parsers (`thrift.rs`'s `Reader`,
  `json_convert.rs`'s STRUCT `type_text` tokenizer, `client.rs`'s
  `decompress_lz4_frame`) -- dev-only dependency, runs inside plain `cargo
  test`. Found and fixed a real bug: `thrift.rs`'s `skip()` (used to
  generically skip any field this crate doesn't care about) recursed with no
  depth bound into nested STRUCT/LIST/SET/MAP fields -- a small (~12KB),
  well-formed-*looking* buffer of a few thousand nested STRUCT field headers
  crashed the whole process with a hard stack-overflow abort, not a
  catchable exception. Fixed with a `MAX_SKIP_DEPTH` (64) ceiling, comfortably
  above any real struct this crate parses; a message nested past it now
  returns a clean `Err` instead. See `thrift.rs`'s own `MAX_SKIP_DEPTH` doc
  comment and its `skip_rejects_nesting_past_max_skip_depth_but_allows_shallow_nesting`
  regression test.
- **New**: server-side query cancellation. When a chunk download's
  `total_timeout_s` elapses, or the surrounding coroutine is cancelled
  (`task.cancel()`/`asyncio.wait_for`) while it's in flight, arrowbricks now
  fires a best-effort server-side cancel in the background -- Thrift's
  `CancelOperation` (already built and wire-tested, but never called before
  this), or a new SEA `POST /api/2.0/sql/statements/{id}/cancel` client call
  -- so Databricks stops running the query instead of finishing it for
  nobody. Fire-and-forget by design: the timeout/cancellation error still
  reaches the caller immediately, and the cancel call's own result is never
  awaited or surfaced. Plugs into the two points that already detect these
  triggers (`heartbeat.rs`'s `tick()` timeout branch and `Drop for
  HeartbeatWait`/`Drop for HeartbeatStream`) -- no new detection logic.
  `Cursor.fetchall_streamed()`/`fetchall_arrow_streamed()` now route through
  this same Rust-level heartbeat (they used to wrap `fetchall()`/
  `fetchall_arrow()` in a separate, Python-level timeout that never reached
  it), so a `total_timeout_s` timeout on either now fires the cancel too,
  not just `stream_query_json`/`stream_ndjson_lines` and the lower-level
  `._core.Client`/`ResultSet` streamed APIs. **Known gap, not addressed by
  this release**: a timeout/cancellation during the *initial* submit/poll
  wait (`Cursor.execute()`/`execute_streamed()`) isn't covered -- by the
  time any chunk is being fetched, the statement has normally already
  reached a terminal state server-side, so there's typically nothing left
  to cancel there in practice anyway.
- **New**: `on_event` observability hook. Pass a sync or async callable to
  `connect`/`DatabricksClient`/`Client` (same attachment point as
  `token_provider`) to receive a `QueryStats` snapshot once per query, at
  completion (`outcome` is `"success"`/`"cancelled"`/`"timeout"`/`"error"`):
  `statement_id`, `protocol`, `warehouse_wait_s`, `submit_to_ready_s`,
  `fetch_s`, `num_chunks`, `bytes_downloaded` (new counter), `retry_count`
  (new counter -- `retry_call`'s own retries were previously invisible),
  and `concurrency_used`. Strictly fire-and-forget: any exception the
  callback raises is caught and swallowed on the Rust side, and dispatching
  it (spawned, not awaited inline) never blocks or measurably slows the
  actual fetch. See README.md's new "Observability" section for the full
  field list and an example. A query whose result is never fully drained
  (e.g. a partial `fetchmany()`, then abandoned) never fires an event --
  same category as this package's existing documented GC-cleanup gap.
- Counters/timers are accumulated for every query unconditionally (a
  handful of atomic increments), not just when `on_event` is actually
  registered -- cheap enough not to bother gating; only the dispatch itself
  is skipped when there's no callback to receive it.

No behavior change for a caller who doesn't pass `on_event` and never hits
a `total_timeout_s`/cancellation during a chunk download -- both features
are purely additive. New Rust tests in `wiremock_pipeline.rs`/
`wiremock_thrift.rs` prove the cancel RPC/REST call actually fires (both
the `total_timeout_s` and bare-cancellation triggers, for both protocols);
new Python tests in `tests/test_observability.py` cover `QueryStats` for
each outcome and confirm a raising `on_event` callback never propagates or
delays the result.

## 3.0.4

- **Perf**: the Thrift inline-`arrowBatches` path (`build_inline_blob`) now pre-sizes its output buffer and decompresses LZ4-compressed batches directly into it, instead of decompressing into an intermediate buffer and copying that into the final one -- one fewer full copy per compressed inline batch. Pure internal refactor, no behavior change; existing LZ4/multi-batch test coverage passes unchanged.
- **No change, but worth recording**: an attempt to bump `chunk_fetch_concurrency`'s default from 64 to 96 (re-measured after 3.0.3's transport changes, on the theory that dropping `http2` or switching to `ring` could have moved the optimum) was investigated, benchmarked, and reverted after the benchmark that produced the "~13-16% faster" numbers turned out to have a real bug: it never passed `chunk_fetch_concurrency=` explicitly, so it was silently comparing the same real concurrency value against itself on every "level." A corrected, controlled, interleaved A/B (parameter passed explicitly, no rebuild needed) showed no consistent winner between 64 and 96 on the same real table. The default stays 64. See `client.rs`'s own doc comment on `DEFAULT_CHUNK_FETCH_CONCURRENCY` and AGENTS.md for the full story, including why this default is easy to accidentally half-update (it's hardcoded in four independent places) and how to avoid repeating the benchmarking mistake.

No behavior change beyond the `build_inline_blob` optimization above -- all 96 Rust tests and 81 Python tests pass; real-warehouse benchmark still shows arrowbricks ~2-3x faster than `databricks-sql-connector`.

## 3.0.3

Shipped `.so` shrunk from 13.1 MiB to 9.1 MiB (~31%), no measured speed cost -- each step re-measured before/after against a real workspace and/or a local CPU-bound decode benchmark (isolates decode cost from network noise):

- Dropped reqwest's `http2` feature -- every request this crate makes is an independent HTTPS call from its own connection pool, nothing gains from HTTP/2 multiplexing, and reqwest/hyper transparently fall back to HTTP/1.1. Confirmed both Thrift and SEA protocols still work end to end.
- `opt-level="s"` instead of the implicit default (`3`) -- a 400k-row/41-column local decode averaged 35.4ms at `3` vs 34.3ms at `"s"` (noise-level, not a real cost) vs 70.7ms at `"z"` (a real ~2x regression, rejected). This reverses a documented earlier decision that had never actually been measured against `"s"`/`"z"`.
- Switched the TLS crypto backend from `aws-lc-rs` to `ring` (reqwest's `rustls` feature is hardcoded to `aws-lc-rs`, so this needed `rustls-no-provider` plus two direct `rustls`/`hyper-rustls` deps with their own `ring` feature, and installing it as the default provider from `DbClient`'s own constructor). Verified real TLS handshakes and data transfer against a real workspace on both protocols; confirmed via `cargo tree -i aws-lc-sys` that it's fully gone from the dependency graph.
- Considered and rejected switching HTTP client libraries entirely (raw `hyper`, `ureq`): measured reqwest's own per-request overhead at ~54us (release build, local loopback) -- 1000x+ smaller than typical real-warehouse request latency, so there was no meaningful upside to chase, only risk to `client.rs`.

Also: dependencies bumped to latest within existing semver ranges (`arrow`/`arrow-json` 59.1.0->59.2.0, `pyo3` 0.29.1->0.29.2, several transitive patch bumps), and `lz4_flex` bumped 0.11->0.14 explicitly -- that range includes real security fixes for invalid-memory-reads decompressing untrusted input (0.12.0/0.12.1/0.13.0), directly relevant since this crate decompresses cloud-fetch data received over the network.

README now has a `Benchmarks` section with real, reproducible numbers (speed, memory, package size, the exact warehouse tier tested against) and a script (`examples/benchmark_vs_connector.py`) anyone can rerun against their own workspace.

No behavior change -- all 96 Rust tests and 81 Python tests pass; real-warehouse benchmark still shows arrowbricks ~1.8-2.3x faster than `databricks-sql-connector` across repeated runs.

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
