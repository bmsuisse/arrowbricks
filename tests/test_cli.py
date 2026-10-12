"""Installed CLI contracts over real mock warehouse sockets."""

from __future__ import annotations

import asyncio
import io
import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest
import thrift_mock as tm
from conftest import WAREHOUSE_ID, Response


@pytest.fixture(params=["sea", "thrift"])
def cli_warehouse(request, mock_warehouse, mock_thrift_server):
    protocol = request.param
    submitted = []
    if protocol == "sea":
        server, _ = mock_warehouse(n_chunks=1, rows_per_chunk=5)
        route = next(
            route
            for method, pattern, route in server._routes
            if method == "POST" and pattern.match("/api/2.0/sql/statements")
        )
        original = route._static

        def submit(req):
            submitted.append(json.loads(req.body)["statement"])
            return original

        route.mock(side_effect=submit)
    else:
        server = mock_thrift_server()
        server.handler.open_session = lambda req: tm.ttypes.TOpenSessionResp(
            status=tm.ok_status(), sessionHandle=tm.session_handle(b"cli-session")
        )
        schema, batches = tm.build_schema_and_batches([(0, 5)])

        def submit(req):
            submitted.append(req.statement)
            return tm.execute_statement_resp(
                direct=tm.DirectResults(
                    operation_state=tm.OperationState.FINISHED_STATE,
                    lz4_compressed=False,
                    arrow_schema=schema,
                    fetch=tm.fetch_results_resp(arrow_batches=[(batches[0], 5)]),
                )
            )

        server.handler.execute_statement = submit
    env = cli_env()
    env.update(
        DATABRICKS_HOST=server.host, DATABRICKS_WAREHOUSE_ID=WAREHOUSE_ID, DATABRICKS_TOKEN="fake-test-credential"
    )
    return protocol, env, submitted, server


def cli_env():
    keys = ("PATH", "PYTHONPATH", "HOME", "TMPDIR", "TEMP", "TMP", "SYSTEMROOT")
    return {key: os.environ[key] for key in keys if key in os.environ}


def run_cli(*args, env=None, stdin="", module=True):
    command = [sys.executable, "-m", "arrowbricks"] if module else [str(Path(sys.executable).with_name("arrowbricks"))]
    return subprocess.run(  # noqa: S603 -- controlled executable/arguments, no shell
        command + list(args), input=stdin, text=True, capture_output=True, env=env or cli_env(), timeout=15, check=False
    )


def test_cli_help():
    result = run_cli("--help")
    assert result.returncode == 0 and result.stderr == ""
    assert "--sql" in result.stdout and "--file" in result.stdout and "--max-bytes" in result.stdout
    assert "fake-test-credential" not in result.stdout


@pytest.mark.parametrize(
    "result,exit_code",
    [
        ("| 雪🙂 |\nrows_shown=1", 0),
        ('{"message":"雪🙂"}', 1),
        ("a" * 255 + "\n", 0),
        ('{"message":"' + "x" * 241 + '"}\n', 1),
    ],
    ids=["unicode-markdown", "unicode-json", "crlf-markdown-budget", "crlf-json-budget"],
)
async def test_cli_query_output_preserves_exact_utf8_wire_bytes(monkeypatch, result, exit_code):
    from arrowbricks import cli, client

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def query_agent(self, *args, **kwargs):
            return result

    monkeypatch.setattr(client, "DatabricksClient", FakeClient)
    args = cli._parser().parse_args(
        ["--host", "host", "--warehouse-id", "warehouse", "--token", "test", "--max-bytes", "256"]
    )
    stdout_bytes, stderr_bytes = io.BytesIO(), io.BytesIO()
    stdout = io.TextIOWrapper(stdout_bytes, encoding="ascii", newline="\r\n")
    stderr = io.TextIOWrapper(stderr_bytes, encoding="ascii", newline="\r\n")
    with monkeypatch.context() as patch:
        patch.setattr(sys, "stdout", stdout)
        patch.setattr(sys, "stderr", stderr)
        actual_exit_code = await cli._run(args, "SELECT 1")
        stdout.flush()
        stderr.flush()
    expected = result.encode("utf-8")
    assert actual_exit_code == exit_code
    assert stdout_bytes.getvalue() == (expected if exit_code == 0 else b"")
    assert stderr_bytes.getvalue() == (expected if exit_code == 1 else b"")
    if "雪" not in result:
        assert len(expected) == 256


async def test_cli_default_agent_and_explicit_aliases(cli_warehouse):
    protocol, env, submitted, _ = cli_warehouse
    for alias in ([], ["--agent"], ["-agent"]):
        result = run_cli(*alias, "--protocol", protocol, "-c", "SELECT id, label FROM t", env=env)
        assert result.returncode == 0 and result.stderr == ""
        assert result.stdout.startswith("id:Int64\tlabel:Utf8\n")
        assert "row_0" in result.stdout and "complete=true" in result.stdout
    assert submitted == ["SELECT id, label FROM t"] * 3


async def test_cli_format_flag_selects_markdown(cli_warehouse):
    protocol, env, _, _ = cli_warehouse
    result = run_cli("--format", "markdown", "--protocol", protocol, "-c", "SELECT id, label FROM t", env=env)
    assert result.returncode == 0 and result.stderr == ""
    assert result.stdout.startswith("|id (Int64)|label (Utf8)|\n|-|-|\n|0|")
    assert r"row\_0" in result.stdout
    assert run_cli("--format", "csv", "-c", "SELECT 1", env=env).returncode == 2


def test_error_json_detection_never_mistakes_a_tsv_header_for_an_error():
    from arrowbricks import cli

    assert cli._is_error_json('{"category":"syntax_error","message":"m","next_step":"n"}')
    assert not cli._is_error_json("{braces}:Utf8\tb:Int64\n1\t2\n\nrows_shown=1; complete=true; total_rows=1")
    assert not cli._is_error_json("id:Int64\n1")


@pytest.mark.parametrize("flag", ["-c", "--sql", "--command"])
async def test_cli_sql_is_not_rewritten(cli_warehouse, flag):
    protocol, env, submitted, _ = cli_warehouse
    sql = "SELECT * FROM t; -- exact SQL\n"
    result = run_cli("--protocol", protocol, flag, sql, env=env)
    assert result.returncode == 0 and submitted == [sql]


async def test_cli_reads_utf8_file(cli_warehouse, tmp_path):
    protocol, env, submitted, _ = cli_warehouse
    sql = "SELECT '雪🙂' AS label;\r\n"
    path = tmp_path / "query.sql"
    path.write_bytes(sql.encode("utf-8"))
    result = run_cli("--protocol", protocol, "--file", str(path), env=env)
    assert result.returncode == 0 and submitted == [sql]


@pytest.mark.parametrize("args", [[], ["-f", "-"]])
async def test_cli_reads_piped_sql(cli_warehouse, args):
    protocol, env, submitted, _ = cli_warehouse
    sql = "SELECT id FROM t\r\n"
    result = run_cli("--protocol", protocol, *args, env=env, stdin=sql)
    assert result.returncode == 0 and submitted == [sql]


async def test_cli_caps_entire_stdout_and_marks_preview(cli_warehouse):
    protocol, env, _, _ = cli_warehouse
    result = run_cli("--protocol", protocol, "-c", "SELECT * FROM t", "--max-rows", "2", "--max-bytes", "256", env=env)
    assert result.returncode == 0 and result.stderr == ""
    assert len(result.stdout.encode()) <= 256
    assert "rows_shown=2" in result.stdout and "complete=false" in result.stdout


@pytest.mark.parametrize(
    "args",
    [
        ["-c", "SELECT 1"],
        ["-c", "SELECT 1", "--max-rows", "0"],
        ["-c", "SELECT 1", "--max-bytes", "255"],
        ["--unknown"],
        ["-c", "a", "-f", "b"],
        ["-f", "/nonexistent/arrowbricks-query.sql"],
        [],
    ],
)
def test_cli_configuration_errors_are_json_on_stderr(args):
    result = run_cli(*args)
    assert result.returncode == 2 and result.stdout == ""
    error = json.loads(result.stderr)
    assert error["category"] == "configuration" and error["message"] and error["next_step"]


async def test_cli_invalid_budget_rejected_without_sql(cli_warehouse):
    protocol, env, submitted, _ = cli_warehouse
    result = run_cli("--protocol", protocol, "-c", "SELECT * FROM t", "--max-bytes", "255", env=env)
    assert result.returncode == 2 and result.stdout == ""
    assert json.loads(result.stderr)["category"] == "configuration"
    assert submitted == []


async def test_cli_flags_override_environment(cli_warehouse):
    protocol, env, _, server = cli_warehouse
    env.update(DATABRICKS_HOST="http://127.0.0.1:1", DATABRICKS_WAREHOUSE_ID="wrong", DATABRICKS_TOKEN="wrong")
    result = run_cli(
        "--host",
        server.host,
        "--warehouse-id",
        WAREHOUSE_ID,
        "--token",
        "fake-test-credential",
        "--protocol",
        protocol,
        "-c",
        "SELECT * FROM t",
        env=env,
    )
    assert result.returncode == 0 and result.stderr == ""
    assert "fake-test-credential" not in result.stdout


async def test_cli_sql_error_is_json_only_on_stderr(cli_warehouse):
    protocol, env, _, server = cli_warehouse
    message = "[PARSE_SYNTAX_ERROR] Syntax error near 'SELEC'"
    if protocol == "sea":
        route = next(
            route
            for method, pattern, route in server._routes
            if method == "POST" and pattern.match("/api/2.0/sql/statements")
        )
        route.mock(
            side_effect=lambda req: Response(
                json_body={
                    "statement_id": "failed-cli",
                    "status": {"state": "FAILED", "error": {"error_code": "PARSE_SYNTAX_ERROR", "message": message}},
                }
            )
        )
    else:
        server.handler.execute_statement = lambda req: tm.execute_statement_resp(
            direct=tm.DirectResults(
                operation_state=tm.OperationState.ERROR_STATE,
                operation_error=message,
            )
        )
    result = run_cli("--protocol", protocol, "-c", "SELEC * FROM t", env=env)
    assert result.returncode == 1 and result.stdout == ""
    error = json.loads(result.stderr)
    assert error["category"] == "syntax_error" and message in error["message"]
    assert error["next_step"] and "fake-test-credential" not in result.stderr


async def test_cli_query_error_respects_entire_stderr_byte_budget(cli_warehouse):
    protocol, env, _, server = cli_warehouse
    message = "[PARSE_SYNTAX_ERROR] " + "x" * 10000
    if protocol == "sea":
        route = next(
            route
            for method, pattern, route in server._routes
            if method == "POST" and pattern.match("/api/2.0/sql/statements")
        )
        route.mock(
            side_effect=lambda req: Response(
                json_body={
                    "statement_id": "failed-cli",
                    "status": {"state": "FAILED", "error": {"error_code": "PARSE_SYNTAX_ERROR", "message": message}},
                }
            )
        )
    else:
        server.handler.execute_statement = lambda req: tm.execute_statement_resp(
            direct=tm.DirectResults(operation_state=tm.OperationState.ERROR_STATE, operation_error=message)
        )
    result = run_cli("--protocol", protocol, "-c", "SELEC 1", "--max-bytes", "256", env=env)
    assert result.returncode == 1 and result.stdout == ""
    assert len(result.stderr.encode()) <= 256
    error = json.loads(result.stderr)
    assert error["category"] == "syntax_error" and error["truncated"]


async def test_installed_console_script(cli_warehouse):
    protocol, env, _, _ = cli_warehouse
    result = run_cli("--protocol", protocol, "-c", "SELECT * FROM t", env=env, module=False)
    assert result.returncode == 0 and result.stdout.startswith("id:Int64") and result.stderr == ""


@pytest.mark.parametrize(
    "args",
    [
        ["-c", "SELECT 1", "--max-rows", "0"],
        ["-c", "SELECT 1", "--max-cell-chars", "0"],
        ["-c", "SELECT 1", "--timeout", "nan"],
        ["-f", "/nonexistent/arrowbricks-query.sql"],
        [],
    ],
)
async def test_cli_invalid_input_with_credentials_never_submits(cli_warehouse, args):
    protocol, env, submitted, _ = cli_warehouse
    result = run_cli("--protocol", protocol, *args, env=env)
    assert result.returncode == 2 and result.stdout == ""
    assert json.loads(result.stderr)["category"] == "configuration"
    assert submitted == []


def test_cli_no_query_on_tty_is_actionable(monkeypatch, capsys):
    from arrowbricks.cli import main

    monkeypatch.setenv("DATABRICKS_HOST", "http://127.0.0.1:1")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "fake-warehouse")
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-test-credential")
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    with pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "pipe" in json.loads(captured.err)["message"]


async def test_cli_deadline_error_is_json_on_stderr(cli_warehouse):
    protocol, env, _, _ = cli_warehouse
    result = run_cli("--protocol", protocol, "-c", "SELECT * FROM t", "--total-timeout-s", "0", env=env)
    assert result.returncode == 1 and result.stdout == ""
    error = json.loads(result.stderr)
    assert error["category"] == "timeout" and error["next_step"]


@pytest.mark.skipif(os.name == "nt", reason="POSIX SIGINT process delivery")
async def test_cli_interrupt_is_actionable_json_without_traceback(cli_warehouse):
    protocol, env, submitted, server = cli_warehouse
    if protocol == "sea":
        route = next(
            route
            for method, pattern, route in server._routes
            if method == "POST" and pattern.match("/api/2.0/sql/statements")
        )
        original = route._callable

        def pending(req):
            original(req)
            return Response(json_body={"statement_id": "cli-pending", "status": {"state": "RUNNING"}})

        route.mock(side_effect=pending)
        server.get("/api/2.0/sql/statements/cli-pending").mock(
            Response(json_body={"statement_id": "cli-pending", "status": {"state": "RUNNING"}})
        )
        server.post("/api/2.0/sql/statements/cli-pending/cancel").mock(Response(json_body={}))
    else:
        original = server.handler.execute_statement

        def pending(req):
            original(req)
            return tm.execute_statement_resp(direct=tm.DirectResults(operation_state=tm.OperationState.RUNNING_STATE))

        server.handler.execute_statement = pending
        server.handler.get_operation_status = lambda req: tm.operation_status_resp(tm.OperationState.RUNNING_STATE)
    command = [sys.executable, "-m", "arrowbricks", "--protocol", protocol, "-c", "SELECT * FROM t"]
    process = subprocess.Popen(  # noqa: S603 -- controlled arguments, no shell
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
    )
    try:
        deadline = asyncio.get_running_loop().time() + 5
        while not submitted and process.poll() is None and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        assert submitted
        process.send_signal(signal.SIGINT)
        stdout, stderr = process.communicate(timeout=10)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
    assert process.returncode == 130 and stdout == ""
    error = json.loads(stderr)
    assert error["category"] == "interrupted" and error["next_step"]
    assert "Traceback" not in stderr
