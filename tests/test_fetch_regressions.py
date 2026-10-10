"""Public API regressions through real HTTP and Arrow IPC."""

import io

import arro3.core as core
import arro3.io as arrow_io
import pytest
from conftest import WAREHOUSE_ID, Response

from arrowbricks import DatabricksClient, _core, await_with_heartbeat, stream_query_json
from arrowbricks.cursor import Cursor


@pytest.mark.parametrize("with_chunk", [True, False])
async def test_empty_sea_result_keeps_columns(mock_server, with_chunk):
    server = mock_server()
    schema = core.Schema([core.Field("id", core.DataType.int64())])
    output = io.BytesIO()
    arrow_io.write_ipc_stream(core.RecordBatchReader.from_batches(schema, []), output, compression=None)
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(json_body={
            "statement_id": "empty",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "chunks": [{"chunk_index": 0, "row_count": 0}] if with_chunk else [],
                "schema": {"columns": [{"name": "id", "type_name": "LONG"}]},
            },
            "result": {"external_links": [{"chunk_index": 0, "external_link": f"{server.host}/data"}]},
        })
    )
    server.get("/data").mock(Response(content=output.getvalue()))
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test", protocol="sea")
    cursor = Cursor(client)
    await cursor.execute("SELECT id FROM t WHERE false")
    table = core.Table.from_arrow(await cursor.fetchall_arrow())
    assert table.num_rows == 0
    assert table.column_names == ["id"]
    assert table.schema.field(0).type == core.DataType.int64()
    assert (await cursor.fetchall_arrow()).column_names == ["id"]


@pytest.mark.parametrize("size", [-1, -1000, 1.5])
async def test_invalid_fetchmany_does_not_change_position(mock_warehouse, size):
    server, _ = mock_warehouse(1, 5)
    cursor = Cursor(DatabricksClient(server.host, WAREHOUSE_ID, token="test", protocol="sea"))
    await cursor.execute("SELECT * FROM t")
    assert await cursor.fetchone() == (0, "row_0")
    with pytest.raises((ValueError, TypeError)):
        await cursor.fetchmany(size)
    assert await cursor.fetchall() == [(1, "row_1"), (2, "row_2"), (3, "row_3"), (4, "row_4")]


@pytest.mark.parametrize("timeout", [-1.0, float("nan"), float("inf"), 1e300])
@pytest.mark.parametrize("method", ["fetch", "ndjson", "execute"])
async def test_invalid_timeout_is_value_error_without_consuming_result(mock_warehouse, timeout, method):
    server, _ = mock_warehouse(1, 3)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test", protocol="sea")
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM t")
    with pytest.raises(ValueError):
        if method == "fetch":
            async for _ in cursor.fetchall_arrow_streamed(total_timeout_s=timeout):
                pass
        elif method == "ndjson":
            async for _ in stream_query_json(client, "SELECT * FROM t", total_timeout_s=timeout):
                pass
        else:
            await cursor.execute("SELECT * FROM t", total_timeout_s=timeout)
    assert (await cursor.fetchall_arrow()).num_rows == 3


async def test_ndjson_large_chunk_preserves_all_rows(mock_warehouse):
    import json

    server, _ = mock_warehouse(1, 10000)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test", protocol="sea")
    ids = []
    async for page in client._core_client.stream_ndjson_lines("SELECT * FROM t"):
        if page is _core.HEARTBEAT:
            continue
        assert isinstance(page, list)
        ids.extend(json.loads(line)["id"] for line in page)
    assert ids == list(range(10000))


async def test_invalid_heartbeat_timeout_cancels_the_supplied_awaitable():
    import asyncio

    pending = asyncio.get_running_loop().create_future()
    with pytest.raises(ValueError):
        async for _ in await_with_heartbeat(pending, total_timeout_s=-1):
            pass
    assert pending.cancelled()
