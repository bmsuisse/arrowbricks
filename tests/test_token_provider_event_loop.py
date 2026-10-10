"""An async `token_provider` must work on a cold `DatabricksClient` from every
public entry point, on both protocols.

Regression tests for 5.2.1: `stream_query_json` (and every other path that
submits its statement from a task the Rust core spawns, such as
`fetchall_arrow_streamed`) failed with `AuthError: RuntimeError: no running
event loop` when the client had never run a plain `execute()` before. The
token provider used to find its asyncio event loop lazily, from whichever
task first asked for a token; on those paths that is a background task with
no loop. The loop is now captured when the Python awaitable is created and
travels with the statement into every task it spawns.

The provider here records which event loop actually ran each call, so the
tests also check that a token is fetched on the caller's own loop -- not on
some other loop that happened to use the client earlier -- when the client
is used from several loops in turn, from a thread, or from two threads at
the same time.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest
import thrift_mock as tm
from conftest import WAREHOUSE_ID

from arrowbricks import HEARTBEAT, DatabricksClient
from arrowbricks.cursor import Cursor

N_CHUNKS = 4
ROWS_PER_CHUNK = 5
TOTAL_ROWS = N_CHUNKS * ROWS_PER_CHUNK

# Which caller a token request belongs to -- set by a test before it starts
# a query, read back inside the provider's coroutine. The provider runs with
# the caller's context, so this tells the two threads' requests apart.
CALLER: contextvars.ContextVar[str] = contextvars.ContextVar("CALLER", default="main")


@dataclass
class RecordingProvider:
    """An async `token_provider` that records, for every call, which caller
    asked and which event loop ran the coroutine. `expected_loops` maps a
    caller to the loop its query runs on; a call on any other loop is
    recorded as a mismatch."""

    calls: list[tuple[str, asyncio.AbstractEventLoop]] = field(default_factory=list)
    expected_loops: dict[str, asyncio.AbstractEventLoop] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    async def __call__(self) -> str:
        loop = asyncio.get_running_loop()
        await asyncio.sleep(0)  # really suspend on the loop, not just return
        caller = CALLER.get()
        with self.lock:
            self.calls.append((caller, loop))
        return f"tok-{caller}-{len(self.calls)}"

    def mismatches(self) -> list[tuple[str, asyncio.AbstractEventLoop]]:
        with self.lock:
            return [(c, lp) for c, lp in self.calls if c in self.expected_loops and self.expected_loops[c] is not lp]


def _install_thrift(mock_thrift_server: Callable[..., Any]) -> str:
    """A multi-chunk Thrift result that is polled (no direct results), so
    `FetchResults` and the downloads run in tasks the core spawns."""
    server = mock_thrift_server(WAREHOUSE_ID)
    sessions = {"n": 0}

    def _open(_req: Any) -> Any:
        sessions["n"] += 1
        return tm.ttypes.TOpenSessionResp(
            status=tm.ok_status(), sessionHandle=tm.session_handle(b"sess" + str(sessions["n"]).encode())
        )

    server.handler.open_session = _open
    lock = threading.Lock()
    ops = {"n": 0}
    fetches: dict[bytes, int] = {}

    def _execute(_req: Any) -> Any:
        with lock:
            ops["n"] += 1
            guid = b"op-" + str(ops["n"]).encode()
        return tm.execute_statement_resp(op_guid=guid, direct=None)

    server.handler.execute_statement = _execute
    server.handler.get_operation_status = lambda req: tm.operation_status_resp(tm.OperationState.FINISHED_STATE)
    server.add_data_route(
        r"^/_data/chunk-(\d+)$",
        lambda m: tm.build_full_ipc_stream(int(m.group(1)) * ROWS_PER_CHUNK, (int(m.group(1)) + 1) * ROWS_PER_CHUNK),
    )
    per_fetch = 2

    def _fetch(req: Any) -> Any:
        # `FetchResults` batches are counted per operation, so concurrent
        # statements each get the whole result.
        guid = req.operationHandle.operationId.guid
        with lock:
            n = fetches.get(guid, 0)
            fetches[guid] = n + 1
        base = n * per_fetch
        links = [
            (f"{server.host}/_data/chunk-{i}", ROWS_PER_CHUNK) for i in range(base, min(base + per_fetch, N_CHUNKS))
        ]
        return tm.fetch_results_resp(
            has_more_rows=base + per_fetch < N_CHUNKS, result_links=links, lz4_compressed=False
        )

    server.handler.fetch_results = _fetch
    return server.host


@pytest.fixture(params=["sea", "thrift"])
def make_client(request, mock_warehouse, mock_thrift_server) -> Callable[..., DatabricksClient]:
    """Returns a factory for a fresh (cold) client against a mock warehouse
    with a `N_CHUNKS`-chunk result, for the parametrized protocol."""
    protocol = request.param
    if protocol == "sea":
        server, _route = mock_warehouse(n_chunks=N_CHUNKS, rows_per_chunk=ROWS_PER_CHUNK)
        host = server.host
    else:
        host = _install_thrift(mock_thrift_server)

    def _make(token_provider: Any, **kwargs: Any) -> DatabricksClient:
        return DatabricksClient(
            host,
            WAREHOUSE_ID,
            token_provider=token_provider,
            protocol=protocol,
            chunk_fetch_concurrency=2,
            **kwargs,
        )

    return _make


async def _stream_ids(client: DatabricksClient) -> list[int]:
    return [
        json.loads(line)["id"]
        async for line in client.stream_query_json("SELECT * FROM t ORDER BY id")
        if line is not HEARTBEAT
    ]


def _ids(table: Any) -> list[int]:
    from arro3.core import Table

    return list(Table.from_arrow(table).column("id").combine_chunks().to_pylist())


# ---- cold client, one entry point each ---------------------------------------


async def test_cold_client_stream_query_json(make_client):
    provider = RecordingProvider()
    provider.expected_loops["main"] = asyncio.get_running_loop()
    client = make_client(provider)

    assert await _stream_ids(client) == list(range(TOTAL_ROWS))
    assert provider.calls, "the async token_provider was never called"
    assert provider.mismatches() == []


async def test_cold_client_cursor_execute_and_fetchall(make_client):
    provider = RecordingProvider()
    provider.expected_loops["main"] = asyncio.get_running_loop()
    cursor = Cursor(make_client(provider))

    await cursor.execute("SELECT * FROM t ORDER BY id")
    rows = await cursor.fetchall()

    assert [r[0] for r in rows] == list(range(TOTAL_ROWS))
    assert provider.mismatches() == []


async def test_cold_client_cursor_fetchmany_and_fetchall_arrow(make_client):
    provider = RecordingProvider()
    provider.expected_loops["main"] = asyncio.get_running_loop()
    cursor = Cursor(make_client(provider))

    await cursor.execute("SELECT * FROM t ORDER BY id")
    first = await cursor.fetchmany(7)
    rest = await cursor.fetchall_arrow()

    assert [r[0] for r in first] + _ids(rest) == list(range(TOTAL_ROWS))
    assert provider.mismatches() == []


async def test_cold_client_execute_streamed_and_fetchall_arrow_streamed(make_client):
    provider = RecordingProvider()
    provider.expected_loops["main"] = asyncio.get_running_loop()
    cursor = Cursor(make_client(provider))

    async for item in cursor.execute_streamed("SELECT * FROM t ORDER BY id"):
        assert item is HEARTBEAT or item is cursor
    tables = [item async for item in cursor.fetchall_arrow_streamed() if item is not HEARTBEAT]

    assert len(tables) == 1
    assert _ids(tables[0]) == list(range(TOTAL_ROWS))
    assert provider.mismatches() == []


async def test_cold_client_fetchall_streamed(make_client):
    provider = RecordingProvider()
    provider.expected_loops["main"] = asyncio.get_running_loop()
    cursor = Cursor(make_client(provider))

    await cursor.execute("SELECT * FROM t ORDER BY id")
    batches = [item async for item in cursor.fetchall_streamed() if item is not HEARTBEAT]

    assert [r[0] for batch in batches for r in batch] == list(range(TOTAL_ROWS))
    assert provider.mismatches() == []


async def test_cold_client_volume_files(mock_volume_files):
    server, put_route, delete_route = mock_volume_files()
    provider = RecordingProvider()
    provider.expected_loops["main"] = asyncio.get_running_loop()
    client = DatabricksClient(server.host, WAREHOUSE_ID, token_provider=provider)

    await client.upload_volume_file("/Volumes/c/s/v/f.bin", b"abc")
    await client.delete_volume_file("/Volumes/c/s/v/f.bin")

    assert put_route.call_count == 1
    assert delete_route.call_count == 1
    assert len(provider.calls) == 2
    assert provider.mismatches() == []


async def test_cold_client_sync_token_provider_stream_query_json(make_client):
    calls: list[str] = []

    def provider() -> str:
        calls.append(threading.current_thread().name)
        return f"sync-{len(calls)}"

    assert await _stream_ids(make_client(provider)) == list(range(TOTAL_ROWS))
    assert calls


# ---- one client, several event loops -----------------------------------------


def test_stream_query_json_on_a_new_event_loop_each_time(make_client):
    """A client created outside any event loop, then streamed from two
    separate `asyncio.run()` calls: each run's tokens must come from that
    run's own loop (the first loop is closed by the time the second runs)."""
    provider = RecordingProvider()
    client = make_client(provider)

    async def _run(caller: str) -> list[int]:
        CALLER.set(caller)
        provider.expected_loops[caller] = asyncio.get_running_loop()
        return await _stream_ids(client)

    assert asyncio.run(_run("first")) == list(range(TOTAL_ROWS))
    assert asyncio.run(_run("second")) == list(range(TOTAL_ROWS))
    assert {c for c, _ in provider.calls} == {"first", "second"}
    assert provider.mismatches() == []


def test_stream_query_json_from_asyncio_run_in_a_thread(make_client):
    provider = RecordingProvider()
    client = make_client(provider)
    result: dict[str, Any] = {}

    async def _run() -> list[int]:
        CALLER.set("thread")
        provider.expected_loops["thread"] = asyncio.get_running_loop()
        return await _stream_ids(client)

    def _thread_main() -> None:
        try:
            result["ids"] = asyncio.run(_run())
        except BaseException as exc:  # noqa: BLE001 -- re-raised in the test thread below
            result["error"] = exc

    t = threading.Thread(target=_thread_main, name="loop-thread")
    t.start()
    t.join(timeout=30)
    assert not t.is_alive(), "streaming from a thread's own event loop hung"
    if "error" in result:
        raise result["error"]
    assert result["ids"] == list(range(TOTAL_ROWS))
    assert provider.mismatches() == []


def test_two_threads_stream_from_their_own_loops_at_the_same_time(make_client):
    """Two threads, each with its own event loop, stream from one shared
    client at the same time. Every token request must run on the loop of the
    thread whose query asked for it."""
    provider = RecordingProvider()
    client = make_client(provider)
    barrier = threading.Barrier(2)
    results: dict[str, Any] = {}

    async def _run(caller: str) -> list[int]:
        CALLER.set(caller)
        provider.expected_loops[caller] = asyncio.get_running_loop()
        barrier.wait(timeout=10)
        out: list[int] = []
        for _ in range(3):
            out = await _stream_ids(client)
        return out

    def _thread_main(caller: str) -> None:
        try:
            results[caller] = asyncio.run(_run(caller))
        except BaseException as exc:  # noqa: BLE001 -- re-raised in the test thread below
            results[caller] = exc

    threads = [threading.Thread(target=_thread_main, args=(c,), name=c) for c in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in threads), "concurrent streaming from two loops hung"
    for caller in ("a", "b"):
        if isinstance(results[caller], BaseException):
            raise results[caller]
        assert results[caller] == list(range(TOTAL_ROWS))
    assert {c for c, _ in provider.calls} == {"a", "b"}
    assert provider.mismatches() == []


# ---- cancel and on_event, both async, on a cold client ------------------------


async def test_cold_client_timeout_cancels_and_reports_with_async_callbacks(mock_server):
    """`stream_query_json` timing out mid-download on a cold client: the
    best-effort cancel (fired from a hook, in a task of its own) still needs
    a token from the async provider, and an async `on_event` still needs a
    loop to run on. Both must use the caller's loop."""
    from conftest import Request, Response

    from arrowbricks import QueryTimeout

    server = mock_server()
    server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(Response(json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            json_body={
                "statement_id": "stmt-tp-cancel",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 3}]},
            }
        )
    )
    server.get("/api/2.0/sql/statements/stmt-tp-cancel/result/chunks/0").mock(
        Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/slow-chunk"}]})
    )

    def _slow_chunk(_request: Request) -> Response:
        threading.Event().wait(5)
        return Response(status=500)

    server.get("/_data/slow-chunk").mock(side_effect=_slow_chunk)
    cancel_route = server.post("/api/2.0/sql/statements/stmt-tp-cancel/cancel").mock(Response(json_body={}))

    loop = asyncio.get_running_loop()
    provider = RecordingProvider()
    provider.expected_loops["main"] = loop
    events: list[tuple[str, asyncio.AbstractEventLoop]] = []

    async def on_event(stats: Any) -> None:
        events.append((stats.outcome, asyncio.get_running_loop()))

    client = DatabricksClient(server.host, WAREHOUSE_ID, token_provider=provider, protocol="sea", on_event=on_event)
    with pytest.raises(QueryTimeout):
        async for _line in client.stream_query_json("SELECT * FROM t", total_timeout_s=0.3):
            pass

    for _ in range(200):
        if cancel_route.call_count and events:
            break
        await asyncio.sleep(0.01)
    assert cancel_route.call_count == 1, "the cancel request never got a token"
    assert events == [("timeout", loop)]
    assert provider.mismatches() == []
