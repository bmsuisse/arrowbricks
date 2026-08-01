from __future__ import annotations

import json

import httpx
import pytest
import respx

from arrowbricks import DatabricksClient, stream_query_json


@pytest.mark.asyncio
@respx.mock
async def test_stream_query_json_preserves_order_despite_out_of_order_chunks(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    # Chunks resolve in REVERSE completion order (see conftest's
    # reverse_arrival) -- this is the scenario that silently breaks ORDER BY
    # if a caller doesn't reorder before emitting.
    mock_warehouse(respx.mock, n_chunks=4, rows_per_chunk=5, reverse_arrival=True)
    client = DatabricksClient(host, warehouse_id, token="test-token")

    rows = [json.loads(row) async for row in stream_query_json(client, "SELECT * FROM whatever ORDER BY id")]

    assert [r["id"] for r in rows] == list(range(20))


@pytest.mark.asyncio
@respx.mock
async def test_stream_query_json_row_shape(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=3)
    client = DatabricksClient(host, warehouse_id, token="test-token")

    rows = [json.loads(row) async for row in stream_query_json(client, "SELECT * FROM whatever")]

    assert rows == [
        {"id": 0, "label": "row_0"},
        {"id": 1, "label": "row_1"},
        {"id": 2, "label": "row_2"},
    ]


@pytest.mark.asyncio
@respx.mock
async def test_stream_query_json_survives_duplicate_index_and_a_gap(warehouse_host_id, chunk_bytes_builder):
    """chunk_index 0 has TWO external_links (a real, if uncommon, shape --
    DatabricksClient._fetch_chunk_index gathers over all of them), and index 1
    never shows up at all. Neither must lose rows -- see cursor.py's
    _pull_one_chunk_table / this module's stream_query_json docstrings."""
    host, warehouse_id = warehouse_host_id
    statement_id = "stmt-dup-gap"
    respx.mock.get(f"{host}/api/2.0/sql/warehouses/{warehouse_id}").mock(
        return_value=httpx.Response(200, json={"state": "RUNNING"})
    )
    respx.mock.post(f"{host}/api/2.0/sql/statements").mock(
        return_value=httpx.Response(
            200,
            json={
                "statement_id": statement_id,
                "status": {"state": "SUCCEEDED"},
                # index 1 deliberately absent -- a genuine gap.
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 4}, {"chunk_index": 2, "row_count": 2}]},
            },
        )
    )
    respx.mock.get(f"{host}/api/2.0/sql/statements/{statement_id}/result/chunks/0").mock(
        return_value=httpx.Response(
            200,
            json={"external_links": [{"external_link": f"{host}/_data/0a"}, {"external_link": f"{host}/_data/0b"}]},
        )
    )
    respx.mock.get(f"{host}/api/2.0/sql/statements/{statement_id}/result/chunks/2").mock(
        return_value=httpx.Response(200, json={"external_links": [{"external_link": f"{host}/_data/2"}]})
    )
    respx.mock.get(f"{host}/_data/0a").mock(return_value=httpx.Response(200, content=chunk_bytes_builder(0, 2)))
    respx.mock.get(f"{host}/_data/0b").mock(return_value=httpx.Response(200, content=chunk_bytes_builder(10, 12)))
    respx.mock.get(f"{host}/_data/2").mock(return_value=httpx.Response(200, content=chunk_bytes_builder(20, 22)))
    client = DatabricksClient(host, warehouse_id, token="test-token")

    rows = [json.loads(row) async for row in stream_query_json(client, "SELECT * FROM whatever")]

    assert sorted(r["id"] for r in rows) == [0, 1, 10, 11, 20, 21]
