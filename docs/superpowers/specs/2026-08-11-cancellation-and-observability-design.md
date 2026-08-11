# Server-side cancellation + observability hooks

## Context

Investigated as part of a broader "what's missing" review of arrowbricks. Two real, verified gaps:

1. **No server-side cancellation.** When a query times out (`total_timeout_s`) or the caller's coroutine is cancelled (`task.cancel()`, an SSE client disconnecting), arrowbricks gives up client-side only. Confirmed by tracing every relevant path:
   - Thrift's `CancelOperation` RPC is fully built and wire-tested (`thrift.rs`'s `build_cancel_operation`/`parse_cancel_operation`), but nothing calls it.
   - SEA's REST cancel endpoint (`POST /api/2.0/sql/statements/{id}/cancel`) has no client code at all.
   - `ResultStream` has no `Drop` impl, so abandoning a cursor early closes nothing server-side either.

   Net effect: every timeout or cancellation leaves the query running on the warehouse, consuming compute for a result nobody will read.

2. **No observability.** No way for a caller to see how a query's time was actually spent (warehouse-wait vs. submit vs. download), how many chunks/bytes moved, or whether/how a query failed — without instrumenting the wire themselves. Every "why is this query slow" question this session was answered by hand-tracing real requests; a caller integrating this into production has no equivalent tool.

## Goals

- Server-side cancel fires for the two triggers that are already detected client-side: `total_timeout_s` and explicit task cancellation. No new detection logic — plug into the exact points that already exist (`heartbeat.rs`'s `tick()` timeout branch and `Drop for HeartbeatWait`/`Drop for HeartbeatStream`).
- One typed `QueryStats` event per query, delivered via an optional callback, carrying enough detail to answer "where did the time go and how much data moved" without the caller instrumenting anything themselves.
- Neither feature may change behavior for a caller who doesn't opt in (no `on_event` passed → zero new work; a query that completes normally is unaffected by the cancellation wiring at all).

## Non-goals

- GC-based cleanup (a `Cursor` dropped with no explicit cancel/timeout/context-manager exit). Python cannot reliably await network I/O during garbage collection; this stays a documented limitation, same category as the existing Python-cancellation residual gap already noted in AGENTS.md.
- Live/streaming mid-query progress events. `QueryStats` fires once, at query completion (success, cancellation, timeout, or error) — not a stream of phase-transition events. A live progress feed is a bigger, separate feature if ever needed.
- Query-result caching (raised and explicitly deferred to a later discussion).

## Design: server-side cancellation

### Trigger points (exactly 2, both already detected)

| Trigger | Where it's already detected today |
|---|---|
| `total_timeout_s` elapses | `heartbeat.rs`'s `tick()`, timeout branch (both `HeartbeatWait` and `HeartbeatStream`) |
| `task.cancel()` / `asyncio.wait_for` timeout on the surrounding coroutine | `Drop for HeartbeatWait` / `Drop for HeartbeatStream` (already call `.abort()` on the local task handle; just don't tell the server) |

Both call sites already have the relevant operation/statement handle in scope at the moment they detect the timeout/cancel. The cancel call slots into those exact points — no new plumbing to carry the handle to a new location.

### What "cancel" means per protocol

- **Thrift**: call `CancelOperation` (already built, `thrift.rs::build_cancel_operation`/`parse_cancel_operation`) against the operation handle.
- **SEA**: call `POST /api/2.0/sql/statements/{id}/cancel` — new wire code, doesn't exist yet (SEA currently has no cancel-statement client code of any kind).

### Semantics

- **Fire-and-forget.** Same "best effort, ignore the result" pattern already used for `CloseOperation`/`CloseSession`/`delete_session` elsewhere in this codebase — a failed cancel call just leaves the query for Databricks' own eventual timeout/reaping. The `QueryTimeout`/`CancelledError` propagates to the caller immediately; the cancel RPC happens in the background, not awaited before the exception surfaces.
- **Open question to verify during implementation, not before:** does Thrift's `CloseOperation` on a still-running operation already implicitly cancel it server-side (common in HiveServer2-compatible implementations), making `CancelOperation` redundant in the cases where `CloseOperation` also fires? No prior investigation in this codebase answers this either way. Regardless of the answer, calling `CancelOperation` explicitly is the correct default: harmless if redundant, necessary if not. Verify against a real workspace (submit a long-running query, timeout it, poll `GetOperationStatus` afterward to confirm the operation actually stopped) as part of implementation, not as a design blocker.

### Explicitly out of scope

GC-based abandonment (see Non-goals). The two triggers above cover every case this codebase can currently detect; anything beyond that would need new detection machinery, which is a different, larger project.

## Design: observability hook

### Shape

`on_event` — an optional callback passed once to `DatabricksClient(...)`/`connect(...)`, at the same attachment point as `token_provider`. Applies to every query run through that client, not passed per-call.

### Granularity

One `QueryStats` event, fired once per query, at completion (success, cancellation, timeout, or error) — not a stream of phase-transition events.

### Payload

| Field | Type | Source |
|---|---|---|
| `statement_id` | `str` | already known |
| `protocol` | `Literal["thrift", "sea"]` | already known |
| `warehouse_wait_s` | `float` | time actually spent inside `ensure_warehouse_running` for this call (0.0 if the 30s-TTL cache already knew the warehouse was running) |
| `submit_to_ready_s` | `float` | statement submit → ready-to-fetch |
| `fetch_s` | `float` | download + decode phase |
| `num_chunks` | `int` | already known (manifest chunk count for SEA, discovered link/batch count for Thrift) |
| `bytes_downloaded` | `int` | **new counter** — not tracked anywhere today; needs adding in the download workers (`fetch_link_bytes`/`fetch_thrift_link` and their callers) |
| `retry_count` | `int` | **new counter** — `retry_call` (`client.rs`) currently retries silently with no count surfaced |
| `concurrency_used` | `int` | the `chunk_fetch_concurrency` value in effect for this client |
| `outcome` | `Literal["success", "cancelled", "timeout", "error"]` | derived from how the query actually ended |

### Callback contract

- **Sync or async**, detected and dispatched the same way `PyTokenProvider`/`TokenProvider` already handles both (`Callable[[QueryStats], None | Awaitable[None]]`) — reuses a pattern this codebase has already built, tested, and proven, rather than inventing a second convention.
- **Never allowed to affect the query.** Fire-and-forget: any exception raised inside the callback is caught and swallowed (logged at debug level, not raised), and firing the event never blocks or slows the actual fetch. A slow or broken `on_event` callback must be invisible to query correctness and latency, the same guarantee `token_provider` failures don't currently have to provide (a broken token provider *should* surface — this is different: telemetry must never gate correctness).

## Data flow

```
DbClient::new(..., on_event) ──┐
                                 │  (stored alongside token_provider)
Cursor.execute() / fetchall()   │
   │                            │
   ├─ ensure_warehouse_running()│  timed
   ├─ submit + poll             │  timed
   ├─ fetch_at_least() ─────────┤  timed, chunk/byte/retry counters incremented as work happens
   │                            │
   └─ on completion (any path: success / cancel / timeout / error)
        └─ build QueryStats from accumulated timers+counters
        └─ dispatch to on_event (fire-and-forget, errors swallowed)
```

Cancellation and observability share no code paths but do share timing: the moment a `QueryTimeout`/cancellation is detected (the same `tick()`/`Drop` call sites cancellation plugs into) is also the moment `outcome` becomes known for the `QueryStats` event on that same query.

## Testing

- **Cancellation**: mock-server tests (`wiremock_thrift.rs`/`wiremock_pipeline.rs`) asserting the cancel RPC/REST call actually fires when a timeout/cancellation is triggered against a mocked slow response — mirroring the existing pattern for `CloseOperation`'s own mock coverage. A real-workspace check (submit a genuinely long query, time it out, verify via `GetOperationStatus`/warehouse query history that it actually stopped) as manual verification, not CI (no live credentials in CI).
- **Observability**: unit tests asserting `QueryStats` fields are populated correctly for each `outcome` case (success, cancelled, timeout, error) against the mock server, plus a test confirming a callback that raises never propagates or delays the result.
