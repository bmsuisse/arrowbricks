"""Agent API contracts over real SEA and Thrift sockets."""

from __future__ import annotations

import asyncio
import io
import json
import threading
import time
from typing import Any

import pytest
import thrift_mock as tm
from conftest import WAREHOUSE_ID, Response

from arrowbricks import DatabricksClient


def ipc(columns, names=None):
    import arro3.core as core
    import arro3.io as aio

    arrays = [
        core.Array(values) if isinstance(values, core.Array) else core.Array(values, type=kind)
        for _, values, kind in columns
    ]
    schema = core.Schema([core.Field(name, kind) for name, _, kind in columns])
    if names is not None:
        schema = core.Schema([core.Field(name, kind) for name, (_, _, kind) in zip(names, columns, strict=True)])
    table = core.Table.from_arrays(arrays, schema=schema)
    out = io.BytesIO()
    aio.write_ipc_stream(table, out, compression=None)
    return out.getvalue()


@pytest.fixture(params=["sea", "thrift"])
def agent_server(request, mock_server, mock_thrift_server):
    protocol = request.param
    server = mock_server() if protocol == "sea" else mock_thrift_server()
    calls: dict[str, Any] = {"submit": [], "open": [], "cancel": threading.Event(), "download": []}
    statement_id = "agent-stmt"
    if protocol == "sea":
        server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
        cancel = server.post(f"/api/2.0/sql/statements/{statement_id}/cancel")
        cancel.mock(side_effect=lambda req: (calls["cancel"].set(), Response(json_body={}))[1])
    else:

        def open_session(req):
            calls["open"].append(req)
            return tm.ttypes.TOpenSessionResp(status=tm.ok_status(), sessionHandle=tm.session_handle(b"agent-session"))

        server.handler.open_session = open_session
        server.handler.cancel_operation = lambda req: (
            calls["cancel"].set(),
            tm.ttypes.TCancelOperationResp(status=tm.ok_status()),
        )[1]

    def install(blobs, rows, *, delay=0, error=None, columns=None, concurrency=1, on_event=None):
        if protocol == "sea":

            def submit(req):
                calls["submit"].append(json.loads(req.body))
                status = (
                    {"state": "SUCCEEDED"}
                    if error is None
                    else {"state": "FAILED", "error": {"error_code": "STATEMENT_ERROR", "message": error}}
                )
                return Response(
                    json_body={
                        "statement_id": statement_id,
                        "status": status,
                        "manifest": {
                            "chunks": [{"chunk_index": i, "row_count": n} for i, n in enumerate(rows)],
                            "schema": {"columns": columns or []},
                        },
                    }
                )

            server.post("/api/2.0/sql/statements").mock(side_effect=submit)
            server.get(r"^/api/2\.0/sql/statements/agent-stmt/result/chunks/\d+$", regex=True).mock(
                side_effect=lambda req: Response(
                    json_body={
                        "external_links": [{"external_link": f"{server.host}/data/{req.path.rsplit('/', 1)[-1]}"}]
                    }
                )
            )

            def download(req):
                i = int(req.path.rsplit("/", 1)[-1])
                calls["download"].append(i)
                time.sleep(delay * (len(blobs) - i))
                return Response(content=blobs[i])

            server.get(r"^/data/\d+$", regex=True).mock(side_effect=download)
        else:

            def submit(req):
                calls["submit"].append(req)
                return tm.execute_statement_resp(
                    direct=tm.DirectResults(
                        operation_state=tm.OperationState.FINISHED_STATE
                        if error is None
                        else tm.OperationState.ERROR_STATE,
                        operation_error=error,
                        lz4_compressed=False,
                        fetch=tm.fetch_results_resp(
                            result_links=[(f"{server.host}/data/{i}", n) for i, n in enumerate(rows)],
                            lz4_compressed=False,
                        ),
                    )
                )

            server.handler.execute_statement = submit

            def download(match):
                i = int(match.group(1))
                calls["download"].append(i)
                time.sleep(delay * (len(blobs) - i))
                return blobs[i]

            server.add_data_route(r"^/data/(\d+)$", download)
        return DatabricksClient(
            server.host,
            WAREHOUSE_ID,
            token="test-token",
            protocol=protocol,
            retry_attempts=1,
            chunk_fetch_concurrency=concurrency,
            on_event=on_event,
        )

    return install, calls, protocol, server


async def test_typed_ordered_rows(agent_server):
    import arro3.core as core

    install, calls, _, _ = agent_server
    blobs = [
        ipc(
            [("id", [i, i + 1], core.DataType.int64()), ("label", [f"row_{i}", f"row_{i + 1}"], core.DataType.string())]
        )
        for i in (0, 2)
    ]
    client = install(blobs, [2, 2], delay=0.03, concurrency=4)
    text = await client.query_agent("SELECT id, label FROM t ORDER BY id")
    assert text.startswith("|")
    assert "id" in text and "Int64" in text and "Utf8" in text
    assert [text.index(f"row\\_{i}") for i in range(4)] == sorted(text.index(f"row\\_{i}") for i in range(4))
    assert "complete=true" in text and "rows_shown=4" in text
    assert len(calls["submit"]) == 1


async def test_empty_preserves_schema(agent_server):
    import arro3.core as core

    install, _, _, _ = agent_server
    client = install([ipc([("empty", [], core.DataType.int64())])], [0])
    text = await client.query_agent("SELECT 1 AS empty WHERE false")
    assert "empty" in text and "Int64" in text
    assert "rows_shown=0" in text and "complete=true" in text


async def test_unicode_null_and_markdown_are_distinct(agent_server):
    import arro3.core as core

    install, _, _, _ = agent_server
    values = [None, "NULL", "雪🙂|\\<b>*x*\nline"]
    client = install([ipc([("v", values, core.DataType.string())])], [3])
    text = await client.query_agent("SELECT v FROM t")
    assert "雪🙂" in text
    assert "\\|" in text and "&lt;b&gt;" in text
    assert "\\*x\\*" in text
    assert "| NULL |" in text and '| "NULL" |' in text
    assert "complete=true" in text


async def test_duplicate_column_names_remain_in_position(agent_server):
    import arro3.core as core

    install, _, _, _ = agent_server
    client = install([ipc([("dup", [17], core.DataType.int64()), ("dup", [29], core.DataType.int64())])], [1])
    text = await client.query_agent("SELECT 17 AS dup, 29 AS dup")
    assert text.splitlines()[0].count("dup") == 2
    assert "17" in text and "29" in text
    assert text.index("17") < text.index("29")


@pytest.mark.parametrize("budget", [256, 300, 1000])
async def test_whole_utf8_output_fits_byte_budget(agent_server, budget):
    import arro3.core as core

    install, _, _, _ = agent_server
    client = install([ipc([("v", ["雪🙂" * 100] * 30, core.DataType.string())])], [30])
    text = await client.query_agent("SELECT v FROM t", max_bytes=budget)
    assert len(text.encode()) <= budget
    assert "complete=false" in text


async def test_row_cap_stops_without_draining(agent_server):
    import arro3.core as core

    install, calls, _, _ = agent_server
    client = install(
        [ipc([("id", list(range(i, i + 10)), core.DataType.int64())]) for i in range(0, 100, 10)], [10] * 10
    )
    text = await client.query_agent("SELECT id FROM t", max_rows=3)
    assert "rows_shown=3" in text and "complete=false" in text
    assert len(calls["download"]) < 10
    assert "total_rows=100" not in text


async def test_cell_clipping_marks_incomplete(agent_server):
    import arro3.core as core

    install, _, _, _ = agent_server
    client = install([ipc([("v", ["雪🙂abcdefgh"], core.DataType.string())])], [1])
    text = await client.query_agent("SELECT v FROM t", max_cell_chars=3)
    assert "abcdefgh" not in text and "雪🙂" in text
    assert "…" in text and "complete=false" in text


@pytest.mark.parametrize(
    "message,category",
    [
        ("[PARSE_SYNTAX_ERROR] Syntax error at or near 'SELEC'. SQLSTATE: 42601", "syntax_error"),
        ("[UNRESOLVED_COLUMN.WITH_SUGGESTION] Cannot resolve `naem`. Did you mean [`name`, `id`]?", "missing_column"),
        ("[TABLE_OR_VIEW_NOT_FOUND] The table `missing` cannot be found. Verify schema and catalog.", "missing_table"),
        ("[UNRESOLVED_FIELD.WITH_SUGGESTION] No struct field `naem`. Did you mean [`name`]?", "missing_column"),
        (
            "[UNRESOLVED_ROUTINE] Cannot resolve function `missing_fn` on search path [`system`, `default`].",
            "missing_function",
        ),
        ("[SCHEMA_NOT_FOUND] Schema `missing_schema` cannot be found.", "missing_namespace"),
        ("[CATALOG_NOT_FOUND] Catalog `missing_catalog` cannot be found.", "missing_namespace"),
    ],
)
async def test_actionable_database_errors_preserve_diagnostics(agent_server, message, category):
    install, calls, _, _ = agent_server
    client = install([], [], error=message)
    result = json.loads(await client.query_agent("SELEC naem FROM missing"))
    assert result["category"] == category
    assert message in result["message"]
    assert result["next_step"]
    assert len(calls["submit"]) == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_rows": 0},
        {"max_rows": -1},
        {"max_bytes": 255},
        {"max_cell_chars": 0},
        {"total_timeout_s": -1},
        {"total_timeout_s": float("nan")},
        {"total_timeout_s": float("inf")},
    ],
)
async def test_invalid_budgets_rejected_before_sql(agent_server, kwargs):
    install, calls, _, _ = agent_server
    client = install([], [])
    with pytest.raises((ValueError, OverflowError)):
        await client.query_agent("SELECT 1", **kwargs)
    assert calls["submit"] == []


async def test_timeout_cancels_server(agent_server):
    import arro3.core as core

    install, calls, _, _ = agent_server
    client = install([ipc([("id", [1], core.DataType.int64())])], [1], delay=0.5)
    result = json.loads(await client.query_agent("SELECT id FROM t", total_timeout_s=0.1))
    assert result["category"] == "timeout" and result["next_step"]
    assert await asyncio.to_thread(calls["cancel"].wait, 2)


async def test_asyncio_cancellation_is_preserved_and_cancels_server(agent_server):
    import arro3.core as core

    install, calls, _, _ = agent_server
    client = install([ipc([("id", [1], core.DataType.int64())])], [1], delay=0.5)
    task = asyncio.create_task(client.query_agent("SELECT id FROM t"))
    deadline = asyncio.get_running_loop().time() + 2
    while not calls["download"] and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    assert calls["download"]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await asyncio.to_thread(calls["cancel"].wait, 2)


async def test_failure_after_rows_returns_error_not_success(agent_server):
    import arro3.core as core

    install, _, _, _ = agent_server
    client = install([ipc([("id", list(range(256)), core.DataType.int64())]), b"corrupt arrow"], [256, 1])
    text = await client.query_agent("SELECT id FROM t")
    result = json.loads(text)
    assert result["message"] and result["next_step"]
    assert "complete=true" not in text


@pytest.mark.parametrize("status,category", [(401, "authentication"), (403, "authentication"), (429, "transient")])
async def test_transport_error_classifier(mock_server, status, category):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(Response(status=status, content=b"test diagnostic"))
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea", retry_attempts=1)
    result = json.loads(await client.query_agent("SELECT 1"))
    assert result["category"] == category
    assert "test diagnostic" in result["message"] and result["next_step"]


async def test_nested_values_are_rendered_without_python_materialization(agent_server):
    import arro3.core as core

    install, _, _, _ = agent_server
    values = core.list_array(
        core.Array([0, 2, 2, 2], type=core.DataType.int32()),
        core.Array([1, 2], type=core.DataType.int64()),
        mask=core.Array([False, True, False], type=core.DataType.bool()),
    )
    client = install([ipc([("items", values, values.type)])], [3])
    text = await client.query_agent("SELECT items FROM t")
    assert "List" in text and r"\[1, 2\]" in text and r"\[\]" in text
    assert "NULL" in text and "complete=true" in text


async def test_catalog_schema_and_parameters_forwarded_without_sql_rewrite(agent_server):
    import arro3.core as core

    install, calls, protocol, _ = agent_server
    client = install([ipc([("answer", [42], core.DataType.int64())])], [1])
    params = [{"name": "value", "value": "42", "type": "INT"}]
    sql = "SELECT :value AS answer"
    text = await client.query_agent(sql, catalog="test_catalog", schema="test_schema", params=params)
    assert "42" in text
    submitted = calls["submit"][0]
    if protocol == "sea":
        assert submitted["statement"] == sql
        assert submitted["catalog"] == "test_catalog" and submitted["schema"] == "test_schema"
        assert submitted["parameters"] == params
    else:
        assert submitted.statement == sql
        assert submitted.parameters
        parameter = submitted.parameters[0]
        assert parameter.name == "value"
        assert calls["open"][0].initialNamespace.catalogName == "test_catalog"
        assert calls["open"][0].initialNamespace.schemaName == "test_schema"


async def test_empty_sea_manifest_preserves_column_metadata(mock_server):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "empty-stmt",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [], "schema": {"columns": [{"name": "empty_column", "type_name": "LONG"}]}},
            }
        )
    )
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")
    text = await client.query_agent("SELECT 1 AS empty_column WHERE false")
    assert r"empty\_column" in text
    assert "rows_shown=0" in text and "complete=true" in text


async def test_long_error_json_stays_valid_inside_byte_budget(mock_server):
    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "failed-stmt",
                "status": {
                    "state": "FAILED",
                    "error": {"error_code": "PARSE_SYNTAX_ERROR", "message": "雪🙂 syntax " * 1000},
                },
            }
        )
    )
    client = DatabricksClient(server.host, WAREHOUSE_ID, token="test-token", protocol="sea")
    text = await client.query_agent("SELECT broken", max_bytes=256)
    assert len(text.encode()) <= 256
    result = json.loads(text)
    assert result["category"] == "syntax_error" and result["message"] and result["next_step"]


async def test_boolean_float_and_null_scalar_values(agent_server):
    import arro3.core as core

    install, _, _, _ = agent_server
    client = install(
        [
            ipc(
                [
                    ("flag", [True, False, None], core.DataType.bool()),
                    ("amount", [1.25, -2.5, None], core.DataType.float64()),
                ]
            )
        ],
        [3],
    )
    text = await client.query_agent("SELECT flag, amount FROM t")
    assert "Boolean" in text and "Float64" in text
    assert "true" in text and "false" in text
    assert "1.25" in text and "-2.5" in text and "NULL" in text
    assert "complete=true" in text


async def test_exact_row_boundary_is_complete_only_after_exhaustion(agent_server):
    import arro3.core as core

    install, _, _, _ = agent_server
    client = install([ipc([("id", [1, 2, 3], core.DataType.int64())])], [3])
    text = await client.query_agent("SELECT id FROM t", max_rows=3)
    assert "rows_shown=3" in text and "total_rows=3" in text and "complete=true" in text


async def test_schema_failure_reports_exactly_one_error_event(agent_server):
    import arro3.core as core

    install, _, _, _ = agent_server
    events = []
    client = install(
        [
            ipc([("first", [1], core.DataType.int64())]),
            ipc([("different", [2], core.DataType.int64())]),
        ],
        [1, 1],
        on_event=events.append,
    )
    result = json.loads(await client.query_agent("SELECT * FROM t"))
    assert result["category"] == "execution_error"
    deadline = asyncio.get_running_loop().time() + 2
    while not events and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)
    assert len(events) == 1
    assert events[0].outcome == "error"
