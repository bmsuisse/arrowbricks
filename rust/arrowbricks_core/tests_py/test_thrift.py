"""PyO3-level (`arrowbricks_core.Client`) coverage for `protocol="thrift"` --
the lower-level analogue of `tests/test_thrift_pipeline.py`, exercising
`arrowbricks._core.Client` directly (no `Cursor`/`DatabricksClient` facade),
same shape as this directory's existing SEA-path tests
(`test_parameters.py`, `test_streaming.py`). See `thrift_mock.py`'s own doc
comment for why this is built on `databricks-sql-connector`'s real,
installed Apache-Thrift-compiler-generated `TCLIService` module instead of a
hand-rolled second Python Thrift codec."""

from __future__ import annotations

import pytest
import thrift_mock as tm

from arrowbricks import _core as arrowbricks_core

WAREHOUSE_ID = "wh-thrift-456"


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
async def test_thrift_execute_returns_data_inline_via_direct_results(mock_thrift_server):
    server = mock_thrift_server(WAREHOUSE_ID)
    _install_open_session(server)

    schema_bytes, batches = tm.build_schema_and_batches([(0, 3)])
    server.handler.execute_statement = lambda req: tm.execute_statement_resp(
        op_guid=b"op-a",
        direct=tm.DirectResults(
            operation_state=tm.OperationState.FINISHED_STATE,
            lz4_compressed=False,
            arrow_schema=schema_bytes,
            fetch=tm.fetch_results_resp(has_more_rows=False, arrow_batches=[(batches[0], 3)]),
        ),
    )

    client = arrowbricks_core.Client(host=server.host, warehouse_id=WAREHOUSE_ID, token="fake", protocol="thrift")
    result = await client.execute("SELECT * FROM t")
    table = await result.fetchall_arrow()
    assert table.num_rows == 3
    assert table.column("id").to_pylist() == [0, 1, 2]


@pytest.mark.asyncio
async def test_thrift_multi_batch_fetch_preserves_order(mock_thrift_server):
    server = mock_thrift_server(WAREHOUSE_ID)
    _install_open_session(server)

    server.handler.execute_statement = lambda req: tm.execute_statement_resp(op_guid=b"op-b", direct=None)
    server.handler.get_operation_status = lambda req: tm.operation_status_resp(tm.OperationState.FINISHED_STATE)

    n_batches, chunks_per_batch, rows_per_chunk = 3, 2, 4
    total_chunks = n_batches * chunks_per_batch
    total_rows = total_chunks * rows_per_chunk

    import time

    def serve_chunk(match):
        idx = int(match.group(1))
        time.sleep((total_chunks - idx) * 0.01)
        return tm.build_full_ipc_stream(idx * rows_per_chunk, (idx + 1) * rows_per_chunk)

    server.add_data_route(r"^/_data/chunk-(\d+)$", serve_chunk)

    def _fetch(_req, _state={"n": 0}):  # noqa: B006 -- deliberate mutable default as a call counter
        n = _state["n"]
        _state["n"] += 1
        base = n * chunks_per_batch
        links = [(f"{server.host}/_data/chunk-{base + j}", rows_per_chunk) for j in range(chunks_per_batch)]
        return tm.fetch_results_resp(has_more_rows=(n + 1 < n_batches), result_links=links, lz4_compressed=False)

    server.handler.fetch_results = _fetch

    client = arrowbricks_core.Client(host=server.host, warehouse_id=WAREHOUSE_ID, token="fake", protocol="thrift")
    result = await client.execute("SELECT * FROM t ORDER BY id")
    table = await result.fetchall_arrow()
    assert table.num_rows == total_rows
    assert table.column("id").to_pylist() == list(range(total_rows))


@pytest.mark.asyncio
async def test_thrift_failed_statement_raises(mock_thrift_server):
    server = mock_thrift_server(WAREHOUSE_ID)
    _install_open_session(server)

    server.handler.execute_statement = lambda req: tm.execute_statement_resp(
        op_guid=b"op-c",
        direct=tm.DirectResults(
            operation_state=tm.OperationState.ERROR_STATE,
            operation_error="SYNTAX_ERROR: bad sql",
        ),
    )

    client = arrowbricks_core.Client(host=server.host, warehouse_id=WAREHOUSE_ID, token="fake", protocol="thrift")
    with pytest.raises(RuntimeError, match="SYNTAX_ERROR"):
        await client.execute("not valid sql")


@pytest.mark.asyncio
async def test_thrift_lz4_compressed_result_links_are_decompressed(mock_thrift_server):
    import lz4.frame as lz4_frame

    server = mock_thrift_server(WAREHOUSE_ID)
    _install_open_session(server)

    server.handler.execute_statement = lambda req: tm.execute_statement_resp(op_guid=b"op-d", direct=None)
    server.handler.get_operation_status = lambda req: tm.operation_status_resp(tm.OperationState.FINISHED_STATE)

    raw = tm.build_full_ipc_stream(0, 4)
    compressed = lz4_frame.compress(raw)
    server.add_data_route(r"^/_data/chunk-0$", lambda _m: compressed)

    server.handler.fetch_results = lambda req: tm.fetch_results_resp(
        has_more_rows=False,
        result_links=[(f"{server.host}/_data/chunk-0", 4)],
        lz4_compressed=True,
    )

    client = arrowbricks_core.Client(host=server.host, warehouse_id=WAREHOUSE_ID, token="fake", protocol="thrift")
    result = await client.execute("SELECT * FROM t")
    table = await result.fetchall_arrow()
    assert table.num_rows == 4
    assert table.column("id").to_pylist() == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_thrift_execute_forwards_parameters_to_the_request(mock_thrift_server):
    """The Thrift-path analogue of `test_parameters.py`'s SEA-level test --
    proves `parameters` reaches `TExecuteStatementReq.parameters`, not just
    that it's silently dropped. Unlike the Rust wiremock mock (which has to
    hand-parse the request body to check this), `Handler.ExecuteStatement`
    already receives the real, fully-parsed `TExecuteStatementReq` object
    for free -- one of the concrete advantages of building this mock on the
    real generated Thrift code."""
    server = mock_thrift_server(WAREHOUSE_ID)
    _install_open_session(server)

    captured: list = []

    def _execute(req):
        captured.append(req.parameters)
        return tm.execute_statement_resp(
            op_guid=b"op-e",
            direct=tm.DirectResults(operation_state=tm.OperationState.FINISHED_STATE),
        )

    server.handler.execute_statement = _execute

    client = arrowbricks_core.Client(host=server.host, warehouse_id=WAREHOUSE_ID, token="fake", protocol="thrift")
    params = [{"name": "min_id", "value": "5", "type": "INT"}]
    await client.execute("SELECT * FROM t WHERE id > :min_id", parameters=params)

    assert len(captured) == 1
    assert captured[0] is not None
    assert len(captured[0]) == 1
    assert captured[0][0].name == "min_id"
    assert captured[0][0].type == "INT"
    assert captured[0][0].value.stringValue == "5"


@pytest.mark.asyncio
async def test_thrift_session_is_reused_across_sequential_executes(mock_thrift_server):
    server = mock_thrift_server(WAREHOUSE_ID)
    open_calls = _install_open_session(server)

    schema_bytes, batches = tm.build_schema_and_batches([(0, 1)])
    server.handler.execute_statement = lambda req: tm.execute_statement_resp(
        op_guid=b"op-f",
        direct=tm.DirectResults(
            operation_state=tm.OperationState.FINISHED_STATE,
            lz4_compressed=False,
            arrow_schema=schema_bytes,
            fetch=tm.fetch_results_resp(has_more_rows=False, arrow_batches=[(batches[0], 1)]),
        ),
    )

    client = arrowbricks_core.Client(host=server.host, warehouse_id=WAREHOUSE_ID, token="fake", protocol="thrift")
    r1 = await client.execute("SELECT 1")
    await r1.fetchall_arrow()
    r2 = await client.execute("SELECT 2")
    await r2.fetchall_arrow()

    assert open_calls["n"] == 1, "a second statement on the same client must reuse the pooled Thrift session"


@pytest.mark.asyncio
async def test_thrift_is_the_default_protocol_when_omitted(mock_thrift_server):
    """The lower-level analogue of `tests/test_thrift_pipeline.py`'s own
    same-named test -- `arrowbricks_core.Client` with no `protocol=` kwarg
    at all must still speak Thrift, not fall back to SEA. `ThriftMockServer`
    only understands Thrift wire bytes, so this fails loudly (a
    protocol-mismatch error, not a silently wrong result) if the PyO3
    `#[pyo3(signature = ...)]` default in `lib.rs` ever reverts."""
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
    client = arrowbricks_core.Client(host=server.host, warehouse_id=WAREHOUSE_ID, token="fake")
    result = await client.execute("SELECT * FROM t")
    table = await result.fetchall_arrow()
    assert table.column("id").to_pylist() == [0, 1]
