from __future__ import annotations

import json
import time

import pytest
from conftest import WAREHOUSE_ID, Request, Response

from arrowbricks import HEARTBEAT, DatabricksClient, QueryTimeout
from arrowbricks.cursor import Cursor


@pytest.mark.asyncio
async def test_failed_statement_raises(mock_server):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "stmt-failed",
                "status": {"state": "FAILED", "error": {"error_code": "SYNTAX_ERROR", "message": "bad sql"}},
            }
        )
    )
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    with pytest.raises(RuntimeError, match="SYNTAX_ERROR"):
        await cursor.execute("not valid sql")


@pytest.mark.asyncio
async def test_canceled_statement_raises(mock_server):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(json_body={"statement_id": "stmt-canceled", "status": {"state": "CANCELED"}})
    )
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    with pytest.raises(RuntimeError, match="canceled"):
        await cursor.execute("SELECT 1")


@pytest.mark.asyncio
async def test_failed_second_execute_clears_the_previous_result_set(mock_server):
    """Regression test for a bug found in code review: `_gen()` only
    assigned `self._result`/`self._schema`/`self._manifest_description` on
    the success path -- a second `execute()` call that fails (statement
    FAILED, here) left all three pointing at the *first* statement's result
    set, with no error of its own. A caller doing
    `try: await cur.execute(sql) / except: ...` and then reading the cursor
    anyway would silently get the previous query's rows and description
    instead of an error."""
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        side_effect=[
            Response(
                json_body={
                    "statement_id": "stmt-ok",
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {
                        "chunks": [],
                        "schema": {"columns": [{"name": "id", "type_name": "LONG"}]},
                    },
                }
            ),
            Response(
                json_body={
                    "statement_id": "stmt-failed",
                    "status": {"state": "FAILED", "error": {"error_code": "SYNTAX_ERROR", "message": "bad sql"}},
                }
            ),
        ]
    )
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT 1 AS id")
    assert cursor.description == [("id", "LONG", None, None, None, None, None)]
    assert await cursor.fetchall() == []

    with pytest.raises(RuntimeError, match="SYNTAX_ERROR"):
        await cursor.execute("not valid sql")

    assert cursor.description is None, "description must not still show the previous statement's schema"
    with pytest.raises(RuntimeError, match="no active result set"):
        await cursor.fetchall()


@pytest.mark.asyncio
async def test_prefer_inline_uses_embedded_data_array_with_no_further_requests(mock_server):
    """End-to-end proof (through the real public Cursor API, not just the
    Rust-level wiremock tests) that prefer_inline=True skips the chunk-fetch
    round trip entirely for a small, INLINE-eligible result."""
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))

    def submit(request: Request) -> Response:
        body = json.loads(request.body)
        assert body["disposition"] == "INLINE"
        assert body["format"] == "JSON_ARRAY"
        return Response(
            json_body={
                "statement_id": "stmt-inline",
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "chunks": [],
                    "schema": {
                        "columns": [{"name": "id", "type_name": "LONG"}, {"name": "label", "type_name": "STRING"}]
                    },
                },
                "result": {"data_array": [["0", "row_0"], ["1", "row_1"]]},
            }
        )

    submit_route = server.post("/api/2.0/sql/statements").mock(side_effect=submit)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM t", prefer_inline=True)
    table = await cursor.fetchall_arrow()
    assert table.num_rows == 2
    assert table.column("id").to_pylist() == [0, 1]
    assert submit_route.call_count == 1, "prefer_inline must not need a second statement execution when it succeeds"


@pytest.mark.asyncio
async def test_prefer_inline_falls_back_when_result_is_too_big_for_inline(mock_server, chunk_bytes_builder):
    """prefer_inline=True on a result that turns out too big for INLINE must
    transparently fall back to a second, normal EXTERNAL_LINKS execution and
    still return the complete, correct result -- not an error, not a
    truncated one. Built manually (not via the mock_warehouse fixture)
    since that fixture registers its own POST /statements route first, and
    routes are matched first-registered-wins -- a second route on the same
    path registered from the test body would never be selected."""
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))

    submit_calls: list[dict] = []
    statement_id = "stmt-fallback"

    def submit(request: Request) -> Response:
        body = json.loads(request.body)
        submit_calls.append(body)
        if body["disposition"] == "INLINE":
            return Response(
                json_body={
                    "statement_id": "stmt-inline-too-big",
                    "status": {
                        "state": "FAILED",
                        "error": {
                            "error_code": "BAD_REQUEST",
                            "message": (
                                "Inline byte limit exceeded. Statements executed with disposition=INLINE can "
                                "have a result size of at most 26214400 bytes. Please execute the statement "
                                "with disposition=EXTERNAL_LINKS if you want to download the full result."
                            ),
                        },
                    },
                }
            )
        return Response(
            json_body={
                "statement_id": statement_id,
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "chunks": [{"chunk_index": 0, "row_count": 5}, {"chunk_index": 1, "row_count": 5}],
                    "schema": {
                        "columns": [{"name": "id", "type_name": "LONG"}, {"name": "label", "type_name": "STRING"}]
                    },
                },
            }
        )

    server.post("/api/2.0/sql/statements").mock(side_effect=submit)

    chunk_bytes = {i: chunk_bytes_builder(i * 5, (i + 1) * 5) for i in range(2)}

    def resolve_chunk(request: Request) -> Response:
        idx = int(request.path.rsplit("/", 1)[-1])
        return Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/chunk-{idx}"}]})

    server.get(rf"^/api/2\.0/sql/statements/{statement_id}/result/chunks/\d+$", regex=True).mock(
        side_effect=resolve_chunk
    )

    def serve_chunk(request: Request) -> Response:
        idx = int(request.path.rsplit("-", 1)[-1])
        return Response(content=chunk_bytes[idx])

    server.get(r"^/_data/chunk-\d+$", regex=True).mock(side_effect=serve_chunk)

    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever", prefer_inline=True)
    rows = await cursor.fetchall()
    assert len(rows) == 10, "must fall back to the normal path and still return everything"
    assert len(submit_calls) == 2
    assert submit_calls[0]["disposition"] == "INLINE"
    assert submit_calls[1]["disposition"] == "EXTERNAL_LINKS"


@pytest.mark.asyncio
async def test_fetchall_returns_all_rows_in_order(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=3, rows_per_chunk=10)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    rows = await cursor.fetchall()

    assert [r[0] for r in rows] == list(range(30))
    assert cursor.description is not None
    assert cursor.description[0][0] == "id"


@pytest.mark.asyncio
async def test_fetchall_preserves_order_despite_out_of_order_chunks(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=4, rows_per_chunk=5, reverse_arrival=True)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever ORDER BY id")
    rows = await cursor.fetchall()

    assert [r[0] for r in rows] == list(range(20))


@pytest.mark.asyncio
async def test_fetchmany_pages_across_chunk_boundaries(mock_warehouse):
    """3 rows/chunk, fetchmany(2) repeatedly -- must page correctly across a
    chunk boundary (chunk 0 has 3 rows, so the 2nd fetchmany(2) needs 1 row
    left in chunk 0 plus 1 from chunk 1)."""
    server, _route = mock_warehouse(n_chunks=3, rows_per_chunk=3)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    all_ids: list[int] = []
    while True:
        rows = await cursor.fetchmany(2)
        if not rows:
            break
        all_ids.extend(r[0] for r in rows)

    assert all_ids == list(range(9))


@pytest.mark.asyncio
async def test_fetchone_then_fetchall_gets_remaining_rows(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=1, rows_per_chunk=5)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    first = await cursor.fetchone()
    rest = await cursor.fetchall()

    assert first is not None
    assert first[0] == 0
    assert [r[0] for r in rest] == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_fetchone_past_end_returns_none(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=1, rows_per_chunk=1)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    assert (await cursor.fetchone()) is not None
    assert (await cursor.fetchone()) is None


@pytest.mark.asyncio
async def test_fetchall_arrow_returns_a_table_with_all_rows(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=2, rows_per_chunk=4)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    table = await cursor.fetchall_arrow()

    assert table.num_rows == 8
    assert table.column_names == ["id", "label"]
    assert table.column(0).combine_chunks().to_pylist() == list(range(8))


@pytest.mark.asyncio
async def test_fetchmany_arrow_pages_across_chunk_boundaries(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=2, rows_per_chunk=3)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    first = await cursor.fetchmany_arrow(4)
    second = await cursor.fetchmany_arrow(4)

    assert first.num_rows == 4
    assert second.num_rows == 2
    assert first.column(0).combine_chunks().to_pylist() == [0, 1, 2, 3]
    assert second.column(0).combine_chunks().to_pylist() == [4, 5]


@pytest.mark.asyncio
async def test_aiter_yields_rows_one_at_a_time(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=1, rows_per_chunk=3)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    ids = [row[0] async for row in cursor]

    assert ids == [0, 1, 2]


@pytest.mark.asyncio
async def test_execute_streamed_emits_heartbeat_before_cursor_ready(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=1, rows_per_chunk=3)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    items = [item async for item in cursor.execute_streamed("SELECT * FROM whatever", total_timeout_s=5)]

    assert all(item is HEARTBEAT for item in items[:-1])
    assert items[-1] is cursor
    assert len(await cursor.fetchall()) == 3


@pytest.mark.asyncio
async def test_fetchall_streamed_times_out_on_a_slow_chunk_download(mock_server):
    """execute_streamed's own timeout only covers the wait for the statement
    to become ready -- it stops the moment chunks are available to fetch,
    before any chunk has actually been downloaded. A slow chunk download must
    still be bounded, which is exactly what fetchall_streamed adds."""
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "stmt-slow",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 3}]},
            }
        )
    )
    server.get("/api/2.0/sql/statements/stmt-slow/result/chunks/0").mock(
        Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/slow-chunk"}]})
    )

    def _slow_chunk(_request: object) -> Response:
        time.sleep(10)
        raise AssertionError("unreachable -- test should time out first")

    server.get("/_data/slow-chunk").mock(side_effect=_slow_chunk)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM whatever")

    with pytest.raises(QueryTimeout):
        async for _ in cursor.fetchall_streamed(total_timeout_s=0.05):
            pass


@pytest.mark.asyncio
async def test_retrying_fetchall_after_a_cancelled_fetch_errors_instead_of_silently_truncating(
    mock_server, chunk_bytes_builder
):
    """Regression test for a bug found in code review: the Rust core's
    `ResultStream::fetch_at_least` pulls chunks off its reorder buffer into a
    *local* decode list before recording them -- if the whole fetch is
    dropped mid-flight (exactly what `fetchall_streamed`'s `total_timeout_s`
    does via `QueryTimeout`, and what `asyncio.wait_for`/`task.cancel()` do
    to any coroutine), whatever chunk was already pulled for that call is
    lost, but the buffer's own bookkeeping has already moved past it. Before
    the fix, retrying `fetchall()` on the *same* cursor after a timeout
    silently returned a real but truncated row count (missing the lost
    chunk) with no error at all. Two chunks, sequential fetch
    (chunk_fetch_concurrency=1) so chunk 0 finishes and is pulled into the
    lost decode list before chunk 1's still-in-flight fetch is what the
    timeout actually catches."""
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "stmt-slow2",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 3}, {"chunk_index": 1, "row_count": 3}]},
            }
        )
    )
    server.get("/api/2.0/sql/statements/stmt-slow2/result/chunks/0").mock(
        Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/fast-chunk"}]})
    )
    server.get("/api/2.0/sql/statements/stmt-slow2/result/chunks/1").mock(
        Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/slow-chunk"}]})
    )
    server.get("/_data/fast-chunk").mock(Response(content=chunk_bytes_builder(0, 3)))

    def _slow_chunk(_request: object) -> Response:
        time.sleep(10)
        raise AssertionError("unreachable -- test should time out first")

    server.get("/_data/slow-chunk").mock(side_effect=_slow_chunk)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", chunk_fetch_concurrency=1)
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM whatever")

    with pytest.raises(QueryTimeout):
        async for _ in cursor.fetchall_streamed(total_timeout_s=0.2):
            pass

    with pytest.raises(RuntimeError, match="incomplete"):
        await cursor.fetchall()


@pytest.mark.asyncio
async def test_fetchall_streamed_yields_result_after_zero_or_more_heartbeats(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=1, rows_per_chunk=3)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM whatever")

    items = [item async for item in cursor.fetchall_streamed(total_timeout_s=5)]

    assert all(item is HEARTBEAT for item in items[:-1])
    assert [r[0] for r in items[-1]] == [0, 1, 2]


@pytest.mark.asyncio
async def test_fetch_before_execute_raises():
    client = DatabricksClient("http://fake", WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    with pytest.raises(RuntimeError, match="execute"):
        await cursor.fetchall()


@pytest.mark.asyncio
async def test_description_falls_back_to_real_arrow_schema_when_manifest_omits_it(mock_server, chunk_bytes_builder):
    """Not every manifest carries `schema.columns` -- description must still
    resolve correctly once a chunk has actually been fetched, from that
    chunk's real Arrow schema, rather than staying empty forever."""
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "stmt-no-schema",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 3}]},  # no "schema" key
            }
        )
    )
    server.get(r"^/api/2\.0/sql/statements/stmt-no-schema/result/chunks/\d+$", regex=True).mock(
        Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/chunk-0"}]})
    )
    server.get("/_data/chunk-0").mock(Response(content=chunk_bytes_builder(0, 3)))
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    assert cursor.description == []  # manifest had no schema, and nothing fetched yet

    rows = await cursor.fetchall()

    assert [r[0] for r in rows] == [0, 1, 2]
    assert cursor.description is not None
    assert cursor.description[0][0] == "id"
