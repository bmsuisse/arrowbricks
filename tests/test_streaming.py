from __future__ import annotations

import json

import pytest
from conftest import WAREHOUSE_ID, Response

from arrowbricks import DatabricksClient, stream_query_json


@pytest.mark.asyncio
async def test_stream_query_json_preserves_order_despite_out_of_order_chunks(mock_warehouse):
    # Chunks resolve in REVERSE completion order (see conftest's
    # reverse_arrival) -- this is the scenario that silently breaks ORDER BY
    # if a caller doesn't reorder before emitting.
    server, _route = mock_warehouse(n_chunks=4, rows_per_chunk=5, reverse_arrival=True)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")

    rows = [json.loads(row) async for row in stream_query_json(client, "SELECT * FROM whatever ORDER BY id")]

    assert [r["id"] for r in rows] == list(range(20))


@pytest.mark.asyncio
async def test_stream_query_json_row_shape(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=1, rows_per_chunk=3)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")

    rows = [json.loads(row) async for row in stream_query_json(client, "SELECT * FROM whatever")]

    assert rows == [
        {"id": 0, "label": "row_0"},
        {"id": 1, "label": "row_1"},
        {"id": 2, "label": "row_2"},
    ]


@pytest.mark.asyncio
async def test_client_stream_query_json_method_matches_the_free_function(mock_warehouse):
    """DatabricksClient.stream_query_json(sql, ...) is a convenience method
    form of the free function above -- same behavior, client passed as
    `self` instead of the first positional argument. Must produce identical
    output, not just "also work"."""
    server, _route = mock_warehouse(n_chunks=1, rows_per_chunk=3)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")

    rows = [json.loads(row) async for row in client.stream_query_json("SELECT * FROM whatever")]

    assert rows == [
        {"id": 0, "label": "row_0"},
        {"id": 1, "label": "row_1"},
        {"id": 2, "label": "row_2"},
    ]


@pytest.mark.asyncio
async def test_stream_query_json_survives_duplicate_index_and_a_gap(mock_server, chunk_bytes_builder):
    """chunk_index 0 has TWO external_links (a real, if uncommon, shape --
    DatabricksClient._fetch_chunk_index gathers over all of them), and index 1
    never shows up at all. Neither must lose rows -- see this module's
    stream_query_json docstring / arrowbricks_core's own reorder-buffer
    tests."""
    server = mock_server()
    statement_id = "stmt-dup-gap"
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": statement_id,
                "status": {"state": "SUCCEEDED"},
                # index 1 deliberately absent -- a genuine gap.
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 4}, {"chunk_index": 2, "row_count": 2}]},
            }
        )
    )
    server.get(f"/api/2.0/sql/statements/{statement_id}/result/chunks/0").mock(
        Response(
            json_body={
                "external_links": [
                    {"external_link": f"{server.host}/_data/0a"},
                    {"external_link": f"{server.host}/_data/0b"},
                ]
            }
        )
    )
    server.get(f"/api/2.0/sql/statements/{statement_id}/result/chunks/2").mock(
        Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/2"}]})
    )
    server.get("/_data/0a").mock(Response(content=chunk_bytes_builder(0, 2)))
    server.get("/_data/0b").mock(Response(content=chunk_bytes_builder(10, 12)))
    server.get("/_data/2").mock(Response(content=chunk_bytes_builder(20, 22)))
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")

    rows = [json.loads(row) async for row in stream_query_json(client, "SELECT * FROM whatever")]

    assert sorted(r["id"] for r in rows) == [0, 1, 10, 11, 20, 21]


@pytest.mark.parametrize(
    ("sql", "limit", "offset", "expected"),
    [
        ("select 1", None, None, "select 1"),
        ("select 1;", 5, None, "SELECT * FROM (select 1\n) _q LIMIT 5"),
        ("select 1 ;  \n", 5, 2, "SELECT * FROM (select 1\n) _q LIMIT 5 OFFSET 2"),
        ("select 1 -- c", 5, None, "SELECT * FROM (select 1 -- c\n) _q LIMIT 5"),
        ("select 1", None, 3, "SELECT * FROM (select 1\n) _q OFFSET 3"),
    ],
)
def test_windowed_sql_survives_trailing_semicolons_and_line_comments(sql, limit, offset, expected):
    from arrowbricks._streaming import windowed_sql

    assert windowed_sql(sql, row_limit=limit, offset=offset) == expected


@pytest.mark.parametrize("bad", ["1; DROP TABLE x --", True, 1.5, -1])
def test_windowed_sql_rejects_values_that_are_not_non_negative_ints(bad):
    from arrowbricks._streaming import windowed_sql

    with pytest.raises((TypeError, ValueError)):
        windowed_sql("select 1", row_limit=bad, offset=None)
    with pytest.raises((TypeError, ValueError)):
        windowed_sql("select 1", row_limit=None, offset=bad)
