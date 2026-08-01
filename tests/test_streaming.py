from __future__ import annotations

import json

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
