from __future__ import annotations

import httpx
import pytest
import respx

from arrowbricks import Connection, DatabricksClient


@pytest.mark.asyncio
@respx.mock
async def test_failed_statement_raises(warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    respx.mock.get(f"{host}/api/2.0/sql/warehouses/{warehouse_id}").mock(
        return_value=httpx.Response(200, json={"state": "RUNNING"})
    )
    respx.mock.post(f"{host}/api/2.0/sql/statements").mock(
        return_value=httpx.Response(
            200,
            json={
                "statement_id": "stmt-failed",
                "status": {"state": "FAILED", "error": {"error_code": "SYNTAX_ERROR", "message": "bad sql"}},
            },
        )
    )
    client = DatabricksClient(host, warehouse_id, token="test-token")

    with pytest.raises(RuntimeError, match="SYNTAX_ERROR"):
        await client.execute_json_statement("not valid sql")


@pytest.mark.asyncio
@respx.mock
async def test_canceled_statement_raises(warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    respx.mock.get(f"{host}/api/2.0/sql/warehouses/{warehouse_id}").mock(
        return_value=httpx.Response(200, json={"state": "RUNNING"})
    )
    respx.mock.post(f"{host}/api/2.0/sql/statements").mock(
        return_value=httpx.Response(200, json={"statement_id": "stmt-canceled", "status": {"state": "CANCELED"}})
    )
    client = DatabricksClient(host, warehouse_id, token="test-token")

    with pytest.raises(RuntimeError, match="canceled"):
        await client.execute_json_statement("SELECT 1")


@pytest.mark.asyncio
@respx.mock
async def test_transient_5xx_is_retried_then_succeeds(warehouse_host_id, chunk_bytes_builder):
    """A 503 on the warehouse-status check should be retried (see
    _is_transient_error), not surfaced to the caller, as long as a later
    attempt succeeds within the retry budget."""
    host, warehouse_id = warehouse_host_id
    respx.mock.get(f"{host}/api/2.0/sql/warehouses/{warehouse_id}").mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(200, json={"state": "RUNNING"}),
        ]
    )
    statement_id = "stmt-ok"
    respx.mock.post(f"{host}/api/2.0/sql/statements").mock(
        return_value=httpx.Response(
            200,
            json={
                "statement_id": statement_id,
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 3}]},
            },
        )
    )
    respx.mock.get(url__regex=rf"{host}/api/2\.0/sql/statements/{statement_id}/result/chunks/\d+").mock(
        return_value=httpx.Response(200, json={"external_links": [{"external_link": f"{host}/_data/chunk-0"}]})
    )
    respx.mock.get(f"{host}/_data/chunk-0").mock(return_value=httpx.Response(200, content=chunk_bytes_builder(0, 3)))

    client = DatabricksClient(host, warehouse_id, token="test-token")
    statement_id_returned, manifest = await client.execute_arrow_statement("SELECT * FROM whatever")

    assert statement_id_returned == statement_id
    assert manifest["chunks"] == [{"chunk_index": 0, "row_count": 3}]


@pytest.mark.asyncio
@respx.mock
async def test_upload_volume_file_puts_bytes_with_overwrite(mock_volume_files, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    put_route, _delete_route = mock_volume_files(respx.mock, host)
    client = DatabricksClient(host, warehouse_id, token="test-token")

    await client.upload_volume_file("/Volumes/cat/schema/vol/some/file.parquet", b"parquet-bytes")

    assert put_route.call_count == 1
    request = put_route.calls.last.request
    assert request.url.path == "/api/2.0/fs/files/Volumes/cat/schema/vol/some/file.parquet"
    assert request.url.params["overwrite"] == "true"
    assert request.content == b"parquet-bytes"
    assert request.headers["Content-Type"] == "application/octet-stream"


@pytest.mark.asyncio
@respx.mock
async def test_delete_volume_file_treats_404_as_success(mock_volume_files, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    _put_route, delete_route = mock_volume_files(respx.mock, host)
    delete_route.mock(return_value=httpx.Response(404))
    client = DatabricksClient(host, warehouse_id, token="test-token")

    await client.delete_volume_file("/Volumes/cat/schema/vol/already-gone.parquet")  # must not raise

    assert delete_route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_delete_volume_file_reraises_non_404_errors(mock_volume_files, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    # 400 rather than a transient status (401/403/408/429/5xx) -- those get
    # retried by _is_transient_error, which would just slow this test down
    # without testing anything different.
    _put_route, delete_route = mock_volume_files(respx.mock, host)
    delete_route.mock(return_value=httpx.Response(400))
    client = DatabricksClient(host, warehouse_id, token="test-token")

    with pytest.raises(httpx.HTTPStatusError):
        await client.delete_volume_file("/Volumes/cat/schema/vol/forbidden.parquet")


@pytest.mark.asyncio
@respx.mock
async def test_permanent_4xx_is_not_retried(warehouse_host_id):
    """A permanent 404 (e.g. a typo'd host/path) should fail immediately, not
    burn through the whole retry budget the way a transient 401/403/429/5xx
    does (see _is_transient_error)."""
    host, warehouse_id = warehouse_host_id
    route = respx.mock.get(f"{host}/api/2.0/sql/warehouses/{warehouse_id}").mock(return_value=httpx.Response(404))
    client = DatabricksClient(host, warehouse_id, token="test-token")

    with pytest.raises(httpx.HTTPStatusError):
        await client.execute_json_statement("SELECT 1")
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_token_provider_sync_and_async(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=2)

    sync_conn = Connection(host, warehouse_id, token_provider=lambda: "sync-token")
    rows = await sync_conn.cursor().execute("SELECT * FROM whatever")
    assert len(await rows.fetchall()) == 2

    async def async_provider() -> str:
        return "async-token"

    async_conn = Connection(host, warehouse_id, token_provider=async_provider)
    rows = await async_conn.cursor().execute("SELECT * FROM whatever")
    assert len(await rows.fetchall()) == 2


def test_requires_token_or_provider(warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    with pytest.raises(ValueError, match="token"):
        DatabricksClient(host, warehouse_id)


@pytest.mark.asyncio
@respx.mock
async def test_shares_one_http_client_across_calls(mock_warehouse, warehouse_host_id):
    """A DatabricksClient should reuse one httpx.AsyncClient (and its
    connection pool) across statements and chunk fetches, not open a fresh
    one per call -- see client.py's _get_http_client."""
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=2, rows_per_chunk=2)
    client = DatabricksClient(host, warehouse_id, token="test-token")

    assert client._http is None
    await client.execute_json_statement("SELECT 1")
    first = client._http
    assert first is not None
    await client.execute_json_statement("SELECT 2")
    assert client._http is first  # same instance reused, not recreated


@pytest.mark.asyncio
@respx.mock
async def test_aclose_closes_and_allows_reopening(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=1)
    client = DatabricksClient(host, warehouse_id, token="test-token")

    await client.execute_json_statement("SELECT 1")
    first = client._http
    assert first is not None
    await client.aclose()
    assert client._http is None
    assert first.is_closed

    await client.execute_json_statement("SELECT 1")  # still usable after close
    assert client._http is not None
    assert client._http is not first


@pytest.mark.asyncio
@respx.mock
async def test_async_context_manager_closes_on_exit(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=1)

    async with DatabricksClient(host, warehouse_id, token="test-token") as client:
        await client.execute_json_statement("SELECT 1")
        http_client = client._http

    assert http_client is not None
    assert http_client.is_closed
    assert client._http is None


@pytest.mark.asyncio
@respx.mock
async def test_warehouse_running_check_is_cached_across_statements(mock_warehouse, warehouse_host_id):
    """Once RUNNING is confirmed, a second statement within the TTL window
    shouldn't re-GET the warehouse status -- that round trip buys nothing on
    an already-known-warm warehouse (see _ensure_warehouse_running)."""
    host, warehouse_id = warehouse_host_id
    warehouse_route = mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=1)
    client = DatabricksClient(host, warehouse_id, token="test-token", warehouse_confirmed_running_ttl_s=60.0)

    await client.execute_json_statement("SELECT 1")
    assert warehouse_route.call_count == 1
    await client.execute_json_statement("SELECT 2")
    assert warehouse_route.call_count == 1  # still 1 -- cached, no second GET


@pytest.mark.asyncio
@respx.mock
async def test_warehouse_running_check_re_verifies_after_ttl_expires(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    warehouse_route = mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=1)
    client = DatabricksClient(host, warehouse_id, token="test-token", warehouse_confirmed_running_ttl_s=0.0)

    await client.execute_json_statement("SELECT 1")
    assert warehouse_route.call_count == 1
    await client.execute_json_statement("SELECT 2")
    assert warehouse_route.call_count == 2  # TTL is 0 -- re-verified every time
