"""Tests for the `on_event` observability hook (`DatabricksClient(...,
on_event=...)` -> a `QueryStats` snapshot per query, at completion) and the
server-side cancellation feature it shares timing/outcome bookkeeping with
(see `docs/superpowers/specs/2026-08-11-cancellation-and-observability-design.md`).

Both features are wired at the Rust/PyO3 layer (`heartbeat.rs`'s two
existing timeout/cancellation-detection points, `pipeline.rs`'s
`StatsReporter`/`PoisonOnDrop`/`ReportOnDrop`) -- these tests exercise them
through the real, user-facing Python API (`DatabricksClient`/`Cursor`/
`stream_query_json`) against the local mock warehouse, not the lower-level
`._core` API `rust/arrowbricks_core/tests_py/` already covers directly.

The callback dispatch itself happens off a background Rust runtime thread,
fire-and-forget -- tests poll (`_wait_until`) for the callback to have run
rather than asserting on it immediately after the awaited call returns.
"""

from __future__ import annotations

import asyncio
import time

import pytest
from conftest import WAREHOUSE_ID, Request, Response

from arrowbricks import DatabricksClient, QueryStats
from arrowbricks.cursor import Cursor


async def _wait_until(predicate, *, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("timed out waiting for the on_event callback to fire")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_query_stats_success_via_sea(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=2, rows_per_chunk=3)
    received: list[QueryStats] = []
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", on_event=received.append)
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM whatever")
    rows = await cursor.fetchall()
    assert len(rows) == 6

    await _wait_until(lambda: len(received) == 1)
    stats = received[0]
    assert stats.outcome == "success"
    assert stats.protocol == "sea"
    assert stats.statement_id == "stmt-abc"
    assert stats.num_chunks == 2
    assert stats.bytes_downloaded > 0, "two real chunk downloads must count real bytes"
    assert stats.concurrency_used == 64  # DatabricksClient's own default
    assert stats.warehouse_wait_s >= 0.0
    assert stats.submit_to_ready_s >= 0.0
    assert stats.fetch_s >= 0.0
    assert stats.retry_count == 0


@pytest.mark.asyncio
async def test_query_stats_fires_exactly_once_across_multiple_fetchmany_calls(mock_warehouse):
    """`on_event` fires once per *query*, not once per fetch call -- draining
    a multi-chunk result via several `fetchmany()` calls must still produce
    exactly one `QueryStats`, dispatched only once the result is actually
    exhausted."""
    server, _route = mock_warehouse(n_chunks=3, rows_per_chunk=4)
    received: list[QueryStats] = []
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", on_event=received.append)
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM whatever")

    rows = []
    while True:
        batch = await cursor.fetchmany(5)
        if not batch:
            break
        rows.extend(batch)
    assert len(rows) == 12

    await _wait_until(lambda: len(received) == 1)
    # Give any (incorrect) second dispatch a moment to show up before asserting.
    await asyncio.sleep(0.05)
    assert len(received) == 1, "on_event must fire exactly once per query, not once per fetchmany() call"
    assert received[0].outcome == "success"
    assert received[0].num_chunks == 3


@pytest.mark.asyncio
async def test_query_stats_error_on_failed_statement(mock_server):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "stmt-failed-obs",
                "status": {"state": "FAILED", "error": {"error_code": "SYNTAX_ERROR", "message": "bad sql"}},
            }
        )
    )
    received: list[QueryStats] = []
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", on_event=received.append)
    cursor = Cursor(client)

    with pytest.raises(RuntimeError, match="SYNTAX_ERROR"):
        await cursor.execute("not valid sql")

    await _wait_until(lambda: len(received) == 1)
    assert received[0].outcome == "error"
    assert received[0].protocol == "sea"


@pytest.mark.asyncio
async def test_on_event_callback_that_raises_does_not_propagate_or_delay_result(mock_warehouse):
    """The callback contract is fire-and-forget: an exception it raises must
    be caught and swallowed on the Rust side, never surfaced to the caller,
    and must never delay the real result -- a slow test failure here would
    itself be evidence of the dispatch blocking the hot path."""
    server, _route = mock_warehouse(n_chunks=1, rows_per_chunk=3)

    def _raising_callback(_stats: QueryStats) -> None:
        raise RuntimeError("boom -- this must never reach the caller")

    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", on_event=_raising_callback)
    cursor = Cursor(client)

    started = time.monotonic()
    await cursor.execute("SELECT * FROM whatever")
    rows = await cursor.fetchall()
    elapsed = time.monotonic() - started

    assert [r[0] for r in rows] == [0, 1, 2]
    assert elapsed < 2.0, "a raising on_event callback must not measurably delay the real result"


@pytest.mark.asyncio
async def test_on_event_async_callback_is_awaited_and_receives_the_result(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=1, rows_per_chunk=3)
    received: list[QueryStats] = []

    async def _async_callback(stats: QueryStats) -> None:
        await asyncio.sleep(0)
        received.append(stats)

    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", on_event=_async_callback)
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM whatever")
    await cursor.fetchall()

    await _wait_until(lambda: len(received) == 1)
    assert received[0].outcome == "success"


@pytest.mark.asyncio
async def test_query_stats_cancelled_outcome_on_bare_asyncio_cancellation(mock_server, chunk_bytes_builder):
    """A caller cancelling a plain `cursor.fetchall()` directly (no
    `total_timeout_s`/`_streamed` variant, no `heartbeat.rs` wrapper involved
    at all) still gets a `QueryStats` with `outcome="cancelled"` -- reported
    from `pipeline.rs`'s `PoisonOnDrop`, the same guard that already exists
    to poison the stream against a silent truncation on retry. No
    server-side cancel RPC is expected here (that only fires through the two
    `heartbeat.rs`-wrapped paths) -- this only proves the *observability*
    side reaches this case too."""
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "stmt-cancel-obs",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 3}]},
            }
        )
    )
    server.get("/api/2.0/sql/statements/stmt-cancel-obs/result/chunks/0").mock(
        Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/slow-chunk"}]})
    )

    def _slow_chunk(_request: Request) -> Response:
        time.sleep(10)
        raise AssertionError("unreachable -- the test cancels before this returns")

    server.get("/_data/slow-chunk").mock(side_effect=_slow_chunk)

    received: list[QueryStats] = []
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", on_event=received.append)
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM whatever")

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(cursor.fetchall(), timeout=0.05)

    await _wait_until(lambda: len(received) == 1)
    assert received[0].outcome == "cancelled"


@pytest.mark.asyncio
async def test_stream_query_json_total_timeout_reports_timeout_and_fires_server_side_cancel(mock_server):
    """End-to-end proof of the full real wiring (Python -> `._core.Client.
    stream_ndjson_lines` -> `heartbeat::HeartbeatStream` ->
    `pipeline::cancel_hook` -> `POST .../cancel`), complementing the
    lower-level, PyO3-free proof in `rust/arrowbricks_core/tests/
    wiremock_pipeline.rs`. `stream_query_json`'s own `total_timeout_s` only
    covers the chunk-download phase (a preserved, documented quirk -- its
    submit/poll wait is never heartbeat-wrapped), which is exactly the phase
    this design's two cancellation triggers cover anyway."""
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "stmt-json-cancel",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 3}]},
            }
        )
    )
    server.get("/api/2.0/sql/statements/stmt-json-cancel/result/chunks/0").mock(
        Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/slow-chunk"}]})
    )

    def _slow_chunk(_request: Request) -> Response:
        time.sleep(10)
        raise AssertionError("unreachable -- total_timeout_s must fire first")

    server.get("/_data/slow-chunk").mock(side_effect=_slow_chunk)

    cancel_route = server.post("/api/2.0/sql/statements/stmt-json-cancel/cancel").mock(Response(json_body={}))

    received: list[QueryStats] = []
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", on_event=received.append)

    # `stream_query_json`/`._core.Client.stream_ndjson_lines` surfaces the
    # Rust-level `HeartbeatStream::tick()` timeout as a plain `RuntimeError`
    # (not the Python-level `QueryTimeout` `Cursor.fetchall_streamed` raises
    # -- a pre-existing distinction between the two heartbeat
    # implementations, not something this change alters).
    with pytest.raises(RuntimeError, match=r"exceeded 0\.05s timeout"):
        async for _line in client.stream_query_json("SELECT * FROM whatever", total_timeout_s=0.05):
            pass

    await _wait_until(lambda: cancel_route.call_count >= 1)
    await _wait_until(lambda: len(received) == 1)
    assert received[0].outcome == "timeout"
    assert cancel_route.call_count == 1


@pytest.mark.asyncio
async def test_cursor_fetchall_arrow_streamed_total_timeout_fires_server_side_cancel(mock_server):
    """Regression test for a real gap found in review: `Cursor.
    fetchall_streamed`/`fetchall_arrow_streamed` used to wrap `fetchall()`/
    `fetchall_arrow()` in this package's own Python-level
    `await_with_heartbeat`, which never touched `._core.ResultSet.
    fetchall_arrow_streamed` (the Rust-level, cancel-aware heartbeat) at
    all -- `total_timeout_s` here raised `QueryTimeout` correctly but never
    asked Databricks to actually stop the query. Both methods now delegate
    to the Rust-level heartbeat instead (see `cursor.py`'s own doc comment),
    so this must now fire the cancel RPC too, and still raise `QueryTimeout`
    (not the underlying `RuntimeError`) to preserve existing callers'
    contract."""
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "stmt-cursor-cancel",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 5}]},
            }
        )
    )
    server.get("/api/2.0/sql/statements/stmt-cursor-cancel/result/chunks/0").mock(
        Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/slow-chunk"}]})
    )

    def _slow_chunk(_request: Request) -> Response:
        time.sleep(10)
        raise AssertionError("unreachable -- total_timeout_s must fire first")

    server.get("/_data/slow-chunk").mock(side_effect=_slow_chunk)

    cancel_route = server.post("/api/2.0/sql/statements/stmt-cursor-cancel/cancel").mock(Response(json_body={}))

    received: list[QueryStats] = []
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", on_event=received.append)
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM whatever")

    from arrowbricks import QueryTimeout

    with pytest.raises(QueryTimeout):
        async for _item in cursor.fetchall_arrow_streamed(total_timeout_s=0.05):
            pass

    await _wait_until(lambda: cancel_route.call_count >= 1)
    await _wait_until(lambda: len(received) == 1)
    assert received[0].outcome == "timeout"
    assert cancel_route.call_count == 1


@pytest.mark.asyncio
async def test_cursor_fetchall_streamed_row_variant_also_raises_query_timeout(mock_server):
    """Same as above but through the row-tuple `fetchall_streamed` (built on
    top of `fetchall_arrow_streamed`, see `cursor.py`) -- confirms the
    `RuntimeError` -> `QueryTimeout` translation survives that extra layer
    too, and that it still needs `arro3-core` (this repo's dev environment
    has it) rather than silently returning Arrow tables."""
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "stmt-cursor-cancel-rows",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 5}]},
            }
        )
    )
    server.get("/api/2.0/sql/statements/stmt-cursor-cancel-rows/result/chunks/0").mock(
        Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/slow-chunk"}]})
    )

    def _slow_chunk(_request: Request) -> Response:
        time.sleep(10)
        raise AssertionError("unreachable -- total_timeout_s must fire first")

    server.get("/_data/slow-chunk").mock(side_effect=_slow_chunk)
    cancel_route = server.post("/api/2.0/sql/statements/stmt-cursor-cancel-rows/cancel").mock(Response(json_body={}))

    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM whatever")

    from arrowbricks import QueryTimeout

    with pytest.raises(QueryTimeout):
        async for _row in cursor.fetchall_streamed(total_timeout_s=0.05):
            pass

    await _wait_until(lambda: cancel_route.call_count >= 1)
    assert cancel_route.call_count == 1
