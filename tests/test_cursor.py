from __future__ import annotations

import time

import pytest
from conftest import WAREHOUSE_ID, Response

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
