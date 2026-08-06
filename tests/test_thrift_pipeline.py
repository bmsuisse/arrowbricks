"""Python-level (`DatabricksClient`/`Cursor`) coverage for `protocol="thrift"`
-- the Thrift-speaking analogue of `test_cursor.py`'s SEA-path tests, using
`thrift_mock.ThriftMockServer` (see that module's own doc comment for why
it's built on `databricks-sql-connector`'s real, installed
Apache-Thrift-compiler-generated `TCLIService` module instead of a
hand-rolled second Python Thrift codec). Aims for the same depth AGENTS.md
already documents for the SEA path: happy path, multi-chunk order
preservation, error propagation, LZ4 compression, and session reuse."""

from __future__ import annotations

import pytest
import thrift_mock as tm

from arrowbricks import DatabricksClient
from arrowbricks.cursor import Cursor

WAREHOUSE_ID = "wh-thrift-123"


def _install_open_session(server, prefix: bytes = b"sess"):
    calls = {"n": 0}

    def _open(_req):
        n = calls["n"]
        calls["n"] += 1
        return tm.ttypes.TOpenSessionResp(
            status=tm.ok_status(), sessionHandle=tm.session_handle(prefix + str(n).encode())
        )

    server.handler.open_session = _open
    return calls


@pytest.mark.asyncio
async def test_thrift_small_query_returns_data_inline_with_zero_fetch_calls(mock_thrift_server):
    server = mock_thrift_server(WAREHOUSE_ID)
    _install_open_session(server)

    schema_bytes, batches = tm.build_schema_and_batches([(0, 3)])

    fetch_calls = {"n": 0}

    def _fetch_should_not_be_called(_req):
        fetch_calls["n"] += 1
        raise AssertionError("small inline query must never call FetchResults")

    server.handler.fetch_results = _fetch_should_not_be_called

    poll_calls = {"n": 0}

    def _poll_should_not_be_called(_req):
        poll_calls["n"] += 1
        raise AssertionError("a query finished inside getDirectResults must never be polled")

    server.handler.get_operation_status = _poll_should_not_be_called

    server.handler.execute_statement = lambda req: tm.execute_statement_resp(
        op_guid=b"op-a",
        direct=tm.DirectResults(
            operation_state=tm.OperationState.FINISHED_STATE,
            lz4_compressed=False,
            arrow_schema=schema_bytes,
            fetch=tm.fetch_results_resp(has_more_rows=False, arrow_batches=[(batches[0], 3)]),
        ),
    )

    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="thrift")
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM t")
    rows = await cursor.fetchall()

    assert [r[0] for r in rows] == [0, 1, 2]
    assert fetch_calls["n"] == 0
    assert poll_calls["n"] == 0


@pytest.mark.asyncio
async def test_thrift_multi_chunk_preserves_order_despite_out_of_order_downloads(mock_thrift_server):
    server = mock_thrift_server(WAREHOUSE_ID)
    _install_open_session(server)

    server.handler.execute_statement = lambda req: tm.execute_statement_resp(op_guid=b"op-b", direct=None)
    server.handler.get_operation_status = lambda req: tm.operation_status_resp(tm.OperationState.FINISHED_STATE)

    n_batches, chunks_per_batch, rows_per_chunk = 3, 2, 5
    total_chunks = n_batches * chunks_per_batch
    total_rows = total_chunks * rows_per_chunk

    import time

    def serve_chunk(match):
        idx = int(match.group(1))
        # Reverse delay: later-indexed chunks finish first, forcing genuine
        # out-of-order completion across FetchResults batches, same trick
        # mock_warehouse's own reverse_arrival uses for the SEA path.
        time.sleep((total_chunks - idx) * 0.01)
        return tm.build_full_ipc_stream(idx * rows_per_chunk, (idx + 1) * rows_per_chunk)

    server.add_data_route(r"^/_data/chunk-(\d+)$", serve_chunk)

    fetch_calls = {"n": 0}

    def _fetch(_req):
        n = fetch_calls["n"]
        fetch_calls["n"] += 1
        base = n * chunks_per_batch
        links = [(f"{server.host}/_data/chunk-{base + j}", rows_per_chunk) for j in range(chunks_per_batch)]
        # lz4_compressed=False explicitly -- otherwise the client's own
        # default-true guess (before any real metadata confirms it) never
        # gets corrected, and the fetch workers try to LZ4-decompress plain
        # uncompressed test data.
        return tm.fetch_results_resp(has_more_rows=(n + 1 < n_batches), result_links=links, lz4_compressed=False)

    server.handler.fetch_results = _fetch

    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="thrift")
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM t ORDER BY id")
    rows = await cursor.fetchall()

    assert [r[0] for r in rows] == list(range(total_rows))
    assert fetch_calls["n"] == n_batches


@pytest.mark.asyncio
async def test_thrift_failed_statement_raises_via_direct_results(mock_thrift_server):
    server = mock_thrift_server(WAREHOUSE_ID)
    _install_open_session(server)

    server.handler.execute_statement = lambda req: tm.execute_statement_resp(
        op_guid=b"op-c",
        direct=tm.DirectResults(
            operation_state=tm.OperationState.ERROR_STATE,
            operation_error="SYNTAX_ERROR: bad sql",
        ),
    )

    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="thrift")
    cursor = Cursor(client)
    with pytest.raises(RuntimeError, match="SYNTAX_ERROR"):
        await cursor.execute("not valid sql")


@pytest.mark.asyncio
async def test_thrift_failed_statement_raises_via_polling(mock_thrift_server):
    server = mock_thrift_server(WAREHOUSE_ID)
    _install_open_session(server)

    server.handler.execute_statement = lambda req: tm.execute_statement_resp(op_guid=b"op-d", direct=None)
    server.handler.get_operation_status = lambda req: tm.operation_status_resp(
        tm.OperationState.ERROR_STATE, error_message="Query failed: table not found"
    )

    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="thrift")
    cursor = Cursor(client)
    with pytest.raises(RuntimeError, match="table not found"):
        await cursor.execute("SELECT * FROM missing")


@pytest.mark.asyncio
async def test_thrift_lz4_compressed_result_links_are_decompressed(mock_thrift_server):
    import lz4.frame as lz4_frame

    server = mock_thrift_server(WAREHOUSE_ID)
    _install_open_session(server)

    server.handler.execute_statement = lambda req: tm.execute_statement_resp(op_guid=b"op-e", direct=None)
    server.handler.get_operation_status = lambda req: tm.operation_status_resp(tm.OperationState.FINISHED_STATE)

    raw = tm.build_full_ipc_stream(0, 5)
    compressed = lz4_frame.compress(raw)

    def serve_chunk(_match) -> bytes:
        return compressed

    server.add_data_route(r"^/_data/chunk-0$", serve_chunk)

    fetch_calls = {"n": 0}

    def _fetch(_req):
        fetch_calls["n"] += 1
        return tm.fetch_results_resp(
            has_more_rows=False,
            result_links=[(f"{server.host}/_data/chunk-0", 5)],
            lz4_compressed=True,
        )

    server.handler.fetch_results = _fetch

    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="thrift")
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM t")
    rows = await cursor.fetchall()

    assert [r[0] for r in rows] == [0, 1, 2, 3, 4]
    assert fetch_calls["n"] == 1


@pytest.mark.asyncio
async def test_thrift_session_is_reused_across_sequential_executes(mock_thrift_server):
    server = mock_thrift_server(WAREHOUSE_ID)
    open_calls = _install_open_session(server)

    schema_bytes, batches = tm.build_schema_and_batches([(0, 1)])

    def _execute(_req):
        return tm.execute_statement_resp(
            op_guid=b"op-f",
            direct=tm.DirectResults(
                operation_state=tm.OperationState.FINISHED_STATE,
                lz4_compressed=False,
                arrow_schema=schema_bytes,
                fetch=tm.fetch_results_resp(has_more_rows=False, arrow_batches=[(batches[0], 1)]),
            ),
        )

    server.handler.execute_statement = _execute

    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="thrift")
    cursor = Cursor(client)
    await cursor.execute("SELECT 1")
    await cursor.fetchall()
    await cursor.execute("SELECT 2")
    await cursor.fetchall()

    assert open_calls["n"] == 1, "a second statement on the same client must reuse the pooled Thrift session"


@pytest.mark.asyncio
async def test_thrift_is_the_default_protocol_when_omitted(mock_thrift_server):
    """The actual regression this whole default-flip depends on: a caller
    who never passes `protocol=` at all must still speak Thrift, not just a
    caller who explicitly passes `protocol="thrift"` (every other test in
    this file does that explicitly). `ThriftMockServer` only understands
    Thrift wire bytes -- if `DatabricksClient`'s own `protocol` kwarg
    default ever silently reverted to `"sea"`, this would fail with a
    protocol-mismatch error instead of returning rows, not pass by
    accident. Confirmed additionally against a real production warehouse
    (not just this mock) as part of this same change -- see the PR
    description/AGENTS.md for that verification, not reproducible in CI."""
    server = mock_thrift_server(WAREHOUSE_ID)
    _install_open_session(server)

    schema_bytes, batches = tm.build_schema_and_batches([(0, 2)])
    server.handler.execute_statement = lambda req: tm.execute_statement_resp(
        op_guid=b"op-g",
        direct=tm.DirectResults(
            operation_state=tm.OperationState.FINISHED_STATE,
            lz4_compressed=False,
            arrow_schema=schema_bytes,
            fetch=tm.fetch_results_resp(has_more_rows=False, arrow_batches=[(batches[0], 2)]),
        ),
    )

    # Deliberately no `protocol=` kwarg at all.
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token")
    cursor = Cursor(client)
    await cursor.execute("SELECT * FROM t")
    rows = await cursor.fetchall()

    assert [r[0] for r in rows] == [0, 1]
