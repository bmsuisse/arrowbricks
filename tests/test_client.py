from __future__ import annotations

import pytest
from conftest import WAREHOUSE_ID, Response

from arrowbricks import Connection, DatabricksClient


@pytest.mark.asyncio
async def test_upload_volume_file_puts_bytes_with_overwrite(mock_volume_files):
    server, put_route, _delete_route = mock_volume_files()
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")

    await client.upload_volume_file("/Volumes/cat/schema/vol/some/file.parquet", b"parquet-bytes")

    assert put_route.call_count == 1


@pytest.mark.asyncio
async def test_upload_volume_file_accepts_any_contiguous_byte_buffer(mock_volume_files):
    import array

    payload = bytes(range(256)) * 1000
    strided = bytearray(payload + payload)  # every 2nd byte -> non-contiguous view
    inputs = {
        "bytes": payload,
        "bytearray": bytearray(payload),
        "memoryview": memoryview(payload),
        "memoryview_bytearray": memoryview(bytearray(payload)),
        "array": array.array("B", payload),
        "strided_memoryview": memoryview(strided)[::2],
    }
    expected = {name: payload for name in inputs}
    expected["strided_memoryview"] = bytes(strided[::2])

    server, put_route, _delete_route = mock_volume_files()
    seen: list[bytes] = []

    def _record(request):
        seen.append(request.body)
        return Response(status=204)

    put_route.mock(side_effect=_record)
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")
    for name, data in inputs.items():
        seen.clear()
        await client.upload_volume_file("/Volumes/cat/schema/vol/f.bin", data)
        assert seen == [expected[name]], name


@pytest.mark.asyncio
async def test_upload_volume_file_rejects_non_buffer(mock_volume_files):
    server, _put_route, _delete_route = mock_volume_files()
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")
    with pytest.raises(TypeError):
        await client.upload_volume_file("/Volumes/cat/schema/vol/f.bin", "text")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_delete_volume_file_treats_404_as_success(mock_volume_files):
    server, _put_route, delete_route = mock_volume_files()
    delete_route.mock(Response(status=404))
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")

    await client.delete_volume_file("/Volumes/cat/schema/vol/already-gone.parquet")  # must not raise

    assert delete_route.call_count == 1


@pytest.mark.asyncio
async def test_delete_volume_file_reraises_non_404_errors(mock_volume_files):
    # 400 rather than a transient status (401/403/408/429/5xx) -- those get
    # retried internally, which would just slow this test down without
    # testing anything different.
    server, _put_route, delete_route = mock_volume_files()
    delete_route.mock(Response(status=400))
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")

    # upload_volume_file/delete_volume_file delegate to the Rust core, which
    # raises arrowbricks.ArrowbricksError (message only, not
    # httpx.HTTPStatusError) -- still a RuntimeError subclass, so this
    # pre-existing `except RuntimeError` catch keeps working unchanged; see
    # test_errors.py for a dedicated test asserting the concrete type.
    with pytest.raises(RuntimeError, match="400"):
        await client.delete_volume_file("/Volumes/cat/schema/vol/forbidden.parquet")


@pytest.mark.asyncio
async def test_token_provider_sync_and_async(mock_warehouse):
    server, _route = mock_warehouse(n_chunks=1, rows_per_chunk=2)

    sync_conn = Connection(server.host, WAREHOUSE_ID, token_provider=lambda: "sync-token", protocol="sea")
    rows = await sync_conn.cursor().execute("SELECT * FROM whatever")
    assert len(await rows.fetchall()) == 2

    async def async_provider() -> str:
        return "async-token"

    async_conn = Connection(server.host, WAREHOUSE_ID, token_provider=async_provider, protocol="sea")
    rows = await async_conn.cursor().execute("SELECT * FROM whatever")
    assert len(await rows.fetchall()) == 2


def test_requires_token_or_provider():
    with pytest.raises(ValueError, match="token"):
        DatabricksClient("http://fake", WAREHOUSE_ID, protocol="sea")


def test_rejects_both_token_and_provider():
    with pytest.raises(ValueError, match="not both"):
        DatabricksClient("http://fake", WAREHOUSE_ID, token="fake", token_provider=lambda: "fake", protocol="sea")
