from __future__ import annotations

import pytest
import respx

from arrowbricks import HEARTBEAT, DatabricksClient
from arrowbricks.cursor import Cursor


@pytest.mark.asyncio
@respx.mock
async def test_fetchall_returns_all_rows_in_order(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=3, rows_per_chunk=10)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    rows = await cursor.fetchall()

    assert [r[0] for r in rows] == list(range(30))
    assert cursor.description is not None
    assert cursor.description[0][0] == "id"


@pytest.mark.asyncio
@respx.mock
async def test_fetchall_preserves_order_despite_out_of_order_chunks(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=4, rows_per_chunk=5, reverse_arrival=True)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever ORDER BY id")
    rows = await cursor.fetchall()

    assert [r[0] for r in rows] == list(range(20))


@pytest.mark.asyncio
@respx.mock
async def test_fetchmany_pages_across_chunk_boundaries(mock_warehouse, warehouse_host_id):
    """3 rows/chunk, fetchmany(2) repeatedly -- must page correctly across a
    chunk boundary (chunk 0 has 3 rows, so the 2nd fetchmany(2) needs 1 row
    left in chunk 0 plus 1 from chunk 1)."""
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=3, rows_per_chunk=3)
    client = DatabricksClient(host, warehouse_id, token="test-token")
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
@respx.mock
async def test_fetchone_then_fetchall_gets_remaining_rows(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=5)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    first = await cursor.fetchone()
    rest = await cursor.fetchall()

    assert first is not None
    assert first[0] == 0
    assert [r[0] for r in rest] == [1, 2, 3, 4]


@pytest.mark.asyncio
@respx.mock
async def test_fetchone_past_end_returns_none(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=1)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    assert (await cursor.fetchone()) is not None
    assert (await cursor.fetchone()) is None


@pytest.mark.asyncio
@respx.mock
async def test_fetchall_arrow_returns_a_table_with_all_rows(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=2, rows_per_chunk=4)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    table = await cursor.fetchall_arrow()

    assert table.num_rows == 8
    assert table.column_names == ["id", "label"]
    assert table.column(0).combine_chunks().to_pylist() == list(range(8))


@pytest.mark.asyncio
@respx.mock
async def test_fetchmany_arrow_pages_across_chunk_boundaries(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=2, rows_per_chunk=3)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    first = await cursor.fetchmany_arrow(4)
    second = await cursor.fetchmany_arrow(4)

    assert first.num_rows == 4
    assert second.num_rows == 2
    assert first.column(0).combine_chunks().to_pylist() == [0, 1, 2, 3]
    assert second.column(0).combine_chunks().to_pylist() == [4, 5]


@pytest.mark.asyncio
@respx.mock
async def test_aiter_yields_rows_one_at_a_time(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=3)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    cursor = Cursor(client)

    await cursor.execute("SELECT * FROM whatever")
    ids = [row[0] async for row in cursor]

    assert ids == [0, 1, 2]


@pytest.mark.asyncio
@respx.mock
async def test_execute_streamed_emits_heartbeat_before_cursor_ready(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=3)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    cursor = Cursor(client)

    items = [item async for item in cursor.execute_streamed("SELECT * FROM whatever", total_timeout_s=5)]

    assert all(item is HEARTBEAT for item in items[:-1])
    assert items[-1] is cursor
    assert len(await cursor.fetchall()) == 3


@pytest.mark.asyncio
async def test_fetch_before_execute_raises():
    client = DatabricksClient("https://fake", "wh", token="test-token")
    cursor = Cursor(client)

    with pytest.raises(RuntimeError, match="execute"):
        await cursor.fetchall()
