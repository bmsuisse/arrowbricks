# Changelog

## 5.2.1 — 2026-10-10

- Fix an async `token_provider` failing with `AuthError: RuntimeError: no
  running event loop` on a client that had not run any query yet.
  `stream_query_json` was the usual trigger: since 3.2.0 its statement is
  submitted from a background task, and the provider used to look for its
  event loop in whichever task first asked for a token. Once any
  `execute()` had run on the client, the loop it found then was reused,
  which is why it only hit cold clients.
- The event loop (and `contextvars`) is now captured when you call the
  method, where your loop is running, and travels with the query into
  every background task it starts: the submit, chunk downloads, the Thrift
  fetch loop, and best-effort cancels. Each token request therefore runs on
  the loop of the query that needs it, also when one client is used from
  several loops at once (one per thread) -- before, a token for one loop's
  query could be fetched on another loop.
- Sync `token_provider`s are unchanged. An async `on_event` callback picks
  its event loop the same way now, so it also fires for a cold client's
  first query when that query times out or is cancelled (it used to need an
  earlier query on the same client to have reported first).
- Tests: `tests/test_token_provider_event_loop.py` (both protocols; every
  public entry point on a cold client, two successive `asyncio.run()`s,
  `asyncio.run()` in a thread, two threads at once, and a timed-out stream
  whose cancel and async `on_event` must still run),
  `tests_py/test_token_provider.py`, and call-context tests in
  `call_context.rs`, `wiremock_pipeline.rs` and `wiremock_thrift.rs`. The
  cold-client streaming tests fail on 5.2.0.
- No API changes.
## 5.2.0 — 2026-10-09

- Cloud-fetch download errors no longer include the presigned URL (its
  `sig=` query string used to end up in exception messages and logs).
- Session pools: a call cancelled while a session is being created gives its
  pool slot back, `close()` frees the slots of the sessions it closes, and a
  session discarded after a failed statement is closed on the server
  instead of lingering until its idle TTL. Applies to SEA and Thrift.
- `ensure_warehouse_running` starts a warehouse it first sees STOPPING and
  then STOPPED (it used to wait out the whole start timeout), and fails fast
  on a deleted warehouse.
- A 401/403 with a static `token=` fails immediately instead of retrying for
  about 31 s; `token_provider` callables are still retried.
- A split download no longer trusts the server's `Content-Range` total for
  its buffer size (a bogus value could abort the process).
- Absurd constructor arguments (`http_timeout=1e30`,
  `chunk_fetch_concurrency=2**62`) raise `ValueError` instead of a panic;
  `chunk_fetch_concurrency` is capped at 4096.
- `row_limit`/`offset` accept only non-negative ints, and a query ending in
  `;` or a `--` comment can now be windowed.
- `stream_query_json` encodes NDJSON in pages of up to 8192 rows instead of
  one whole chunk at a time, cutting its peak memory by about a third (about
  130 MB to 87 MB on a 300k x 8 result, level with `fetchall_arrow`) with no
  change in speed or output.
- Thrift: a rejected `ExecuteStatement` (bad SQL, bad parameter) now raises
  `StatementError`, matching SEA, instead of the plain `ArrowbricksError`.
- Thrift: a timeout or cancellation while `ExecuteStatement` itself is still
  in flight (e.g. a cold connection) now cancels the statement once its
  operation handle arrives, instead of leaving it running on the warehouse.
- Thrift: a `None` named-parameter value is now bound as SQL NULL instead of
  the empty string (SEA already did this).
- SEA: pooled sessions are now created with the `catalog`/`schema` keys the
  sessions API reads; the old `catalog_name`/`schema_name` were silently
  ignored, so `schema=` had no effect whenever a session was in use.
- Materialize Python rows directly from chunked columns, avoiding native
  Arrow buffer concatenation before converting values to Python.
- Write IPC directly to the destination with Python writes capped at 1 MiB,
  avoiding a whole-result Rust buffer and Python copy. Handle short writes
  and preserve exceptions raised by the destination.
- Reject invalid streaming deadlines before starting work, with `ValueError`
  instead of a Rust panic. Invalid `fetchmany` sizes no longer change cursor position.
- Preserve schemas for schema-only IPC chunks and reconstruct empty SEA
  schemas from SQL manifest types, including nested ARRAY/MAP/STRUCT fields.
  Missing nullability metadata is treated conservatively as nullable;
  unsupported manifest types raise an explicit error instead of losing columns.
- Add opt-in real warehouse tests for both protocols, compression on/off,
  paging, Python values, nested NDJSON, empty results, concurrency, errors,
  and timeout recovery. Extend the interleaved benchmark to cover all fetch
  modes and IPC export.

## 5.1.2 — 2026-10-10

- Stop the background fetch as soon as a result is abandoned. Cancelling
  the task that awaits a fetch, closing a `stream_query_json` iterator
  (what a web framework does when the client disconnects), or dropping a
  `Cursor` before its result is drained used to leave the chunk downloads
  already in flight running to completion (up to `chunk_fetch_concurrency`
  of them), and workers could start new ones after the consumer was gone.
  In-flight downloads are now aborted right away, on both protocols.
- Thrift: closing the operation after an abandoned fetch no longer hangs.
  With more links left than the download workers and their queue could hold
  (about `2 * chunk_fetch_concurrency`; 128 by default), the fetch loop
  blocked forever, so `CloseOperation` (and `CloseSession` for a throwaway
  session) never went out, and the task, its client handle and its
  connection pool were never freed. The loop now also stops issuing
  `FetchResults` once nobody reads the result.
- Unchanged: server-side cancel (`CancelOperation` / SEA `POST .../cancel`)
  still fires when a still-running statement is abandoned, as since 3.2.0.
- Tests: `wiremock_thrift.rs`/`wiremock_pipeline.rs` drop a partly read
  result with a download in flight, and `test_thrift_pipeline.py` stops
  reading `stream_query_json` after the first rows. All three fail on 5.1.1.
- No API changes.

## 5.1.1 — 2026-10-07

- Fix a hole in 5.0.2's `read_body`: the up-front buffer reservation came from
  the untrusted `Content-Length` (capped at 1 GiB per download), and a failed
  reservation aborted the process, which the new allocator can turn into a
  crash under `RLIMIT_AS` or strict overcommit. It now reserves at most
  256 MiB, and falls back to `Response::bytes()` if even that fails.
- The allocator is behind a default-on `large-block-alloc` cargo feature and
  now requires Linux with glibc (it was only ever measured there; musl wheels
  are unaffected). A Rust crate that depends on `arrowbricks_core` with its
  own global allocator can opt out with `default-features = false`.
- Tests: a check that fails if the allocator is removed or mis-gated, `read_body`
  with and without `Content-Length` and with a short body, the Thrift inline
  LZ4 path with a zero-content frame, and `import arrowbricks` not loading
  `asyncio` (plus deferred submodules on 3.15).
- `AGENTS.md` and `rust/.../alloc.rs`: `// SAFETY:` comments with a lint to keep
  them, and design-invariant entries for the allocator, `read_body` and the
  lazy-import rule; the LZ4-loop entry now points at `lz4_frame_decode_into`.
- Removed the claim in 5.0.0 that the 3.15 suite passes with `-W error`: four
  tests fail intermittently under it from unclosed mock-server sockets.
- No API changes.

## 5.1.0 — 2026-10-07

- Linux (glibc): buffers of 1 MiB or more now come straight from `mmap` and go
  back to the OS when freed (`rust/arrowbricks_core/src/alloc.rs`, a global
  allocator that covers only this extension's Rust allocations; the host process's malloc is untouched, other platforms
  are unchanged). glibc raises its mmap threshold as large blocks are freed,
  so repeated queries in one long-lived process kept the memory: idle RSS
  grew from 200 MB to 1.4 GB over five queries of the same 500k-row table.
  Measured on a real warehouse (5 workloads, Thrift and SEA, LZ4-compressed
  results, interleaved against 5.0.3): peak RSS -41% to -64% on the larger
  results, idle RSS after repeated queries about -80%, time unchanged within
  noise. Tiny results are unaffected. No API changes.
- Correction to 5.0.2: its memory and speed figures were measured against a
  local mock warehouse serving uncompressed bodies. Against a real warehouse,
  where results arrive LZ4-compressed, 5.0.2 and 5.0.3 behave like 4.0.0 on
  fetch time and memory; the real-warehouse memory gain is in 5.1.0 above.
  Data and method: `benchmarks/2026-10-07-allocator.md`.

## 5.0.3 — 2026-10-06

- Internal cleanup, no behavior change: the 21 hand-built permanent
  `ApiError` literals now use `ApiError::permanent`; the multi-frame LZ4
  decode loop (with its past silent-truncation fix) lives in one helper
  shared by the chunk-download and Thrift inline-blob paths; `Cursor`
  shares one schema-refresh helper and no longer copies the row list when
  nothing is buffered in `fetchall()`/`fetchmany()`.

## 5.0.2 — 2026-10-06

- Read each single-request cloud-fetch download into a buffer sized from
  `Content-Length` instead of `Response::bytes()`, which kept collected frames alive while
  joining them, so every in-flight chunk peaked at about twice its size.
  Against a local mock warehouse (about 770 MB result) peak RSS fell from
  1302 MB to 707 MB and fetch time from 0.266 s to 0.208 s. Responses
  without `Content-Length` keep the previous path. Range-split parts still use
  `bytes()`. **These figures are mock-only (uncompressed bodies); on real LZ4
  results this release made no measurable difference, see the 5.1.0 entry.**
  No API changes.

## 5.0.1 — 2026-10-06

- Refresh all locked dependencies: Rust crates via `cargo update` (within
  the declared semver ranges, so the compiled extension picks up the latest
  patch releases of tokio, rustls, hyper, zerocopy and others) and the dev
  tooling via `uv lock --upgrade` (arro3 0.9, databricks-sql-connector 4.6,
  ruff 0.16.10, ty 0.0.84, ...). No API or behavior changes.

## 5.0.0 — 2026-10-06

- Python 3.15 support: the full suite passes on 3.15.0rc2. CI now tests 3.11-3.15. The abi3-py311 wheel is unchanged and
  works on every supported version.
- Faster import: `import arrowbricks` no longer imports `asyncio` (about
  27 ms of a 33 ms import); it loads on first use of `await_with_heartbeat`,
  by which point a running event loop has loaded it anyway. Measured on
  3.14: 33 ms -> 5 ms.
- Python 3.15 only: `__lazy_modules__` (PEP 810) defers loading the
  `_streaming`, `client` and `cursor` submodules until first use, so
  `import arrowbricks` costs about 1.3 ms there (just the compiled core).
  Older versions ignore the list and import eagerly, so behavior is the same.
- Set `asyncio_default_fixture_loop_scope` explicitly in the pytest config.
- Major version bump to mark the 3.15 support baseline; no API changes
  from 4.0.0.

## 4.0.0 — 2026-09-27

- Keep named-timezone support (`Etc/UTC`, as Databricks sends TIMESTAMP)
  in `stream_query_json`: enable arrow-array's `chrono-tz` feature directly,
  which pyo3-arrow previously enabled implicitly.
- Preserve batch schema validation in the smaller Arrow bridge so inconsistent
  result chunks fail before they can be exported under the wrong type.
- **Breaking:** `fetchall_arrow()`, `fetchmany_arrow()` and
  `read_ipc_stream()` return `arrowbricks._core.Table`, a minimal Arrow
  PyCapsule object, instead of pyo3-arrow's `Table`. It exposes `num_rows`,
  `num_columns`, `column_names`, `len()`, `__arrow_c_stream__` and
  `__arrow_c_schema__`; use `pyarrow.table(t)`, `polars.DataFrame(t)` or
  `arro3.core.Table.from_arrow(t)` for anything else. DuckDB reads it
  directly as before. `requested_schema` is ignored, as the PyCapsule
  interface permits. The compiled extension shrinks from 9.3 MB to 5.9 MB
  (wheel 3.4 MB to 2.4 MB on macOS arm64), with no measured speed change.
- `write_ipc_stream` accepts objects implementing `__arrow_c_stream__`;
  `__arrow_c_array__`-only objects are no longer accepted.
- Refresh the warehouse-running cache whenever a statement succeeds, so
  steady traffic with gaps under `warehouse_confirmed_running_ttl_s` no
  longer pays the warehouse-status GET. Measured on a real warehouse with
  queries 20 s apart: the extra 60-90 ms GET appeared on 4 of 9 queries
  before, 0 of 9 after. Back-to-back queries are unchanged.
- Thrift status polling sleeps 10/25/50/100 ms, then 200 ms steady, instead
  of a fixed 200 ms. The server holds each `GetOperationStatus` open for
  about 5 s in the measured workspace, so the ramp mainly helps when
  status calls return quickly.

## 3.2.0 — 2026-09-23

- Cancel a statement server-side when its submit/poll wait is abandoned --
  a `total_timeout_s` on `Cursor.execute()`/`execute_streamed()`, a
  Python-side `task.cancel()`/`asyncio.wait_for`, or a poll request that
  fails mid-wait. Previously only a timeout during the chunk download
  cancelled, so a long-running query kept running on the warehouse after
  the caller had given up. One gap remains on `protocol="sea"`: the submit
  POST itself can block server-side for up to `wait_timeout` (default 30s)
  before a statement id exists, and abandoning it inside that window has
  nothing to cancel. Thrift submits with `runAsync` and has no such window.
- Release the pooled session when a submit/poll wait is abandoned. It used
  to leak the pool reservation (after `MAX_SESSIONS_PER_KEY` abandonments
  every later query for that catalog/schema ran without a pooled session),
  and on Thrift left the session open until its server-side TTL.
- `stream_query_json(total_timeout_s=...)` now also bounds the submit/poll
  wait (and yields `HEARTBEAT` during it), not just the chunk downloads. A
  statement that never finished used to block it forever regardless of
  `total_timeout_s`.
- `stream_query_json` raises `QueryTimeout` on timeout, same as the cursor
  APIs, instead of a plain `ArrowbricksError`. Still a `RuntimeError`/
  `ArrowbricksError` subclass, so existing `except` clauses keep matching.


## 3.1.4 — 2026-09-07

- Return a conversion error for unsupported empty STRUCT arrays instead of
  panicking. The INLINE path reports the error without resubmitting an
  already-succeeded statement.
- Distribute spare cloud-fetch request slots across known files instead of
  leaving the division remainder unused. Keep the same global concurrency
  cap and a slot for each file when the batch fits within that cap. Repeated
  real-warehouse comparisons showed lower large-query latency, with higher
  peak process memory; see `benchmarks/2026-09-06-spare-slots.md`.
- Release the Python interpreter lock while decoding cached IPC streams,
  allowing independent replay calls to run concurrently while retaining
  ownership of the immutable input bytes.
- Collect NDJSON rows directly from the Arrow writer, avoiding a second
  full-chunk JSON buffer. Preserve nulls, Unicode, row order, and non-finite
  float handling. No new Python dependency or public API change.
- Add reproducible CPU benchmarks and anonymized live-warehouse validation
  in `benchmarks/2026-09-06-lowlevel.md`.
- Add `--verify-ipc` to the two-version benchmark for private, untimed checks
  of serialized Arrow results, including schema-only results. Suppress raw
  worker errors so benchmark output cannot expose warehouse error details;
  no result data or checksums are printed by the parent.

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
