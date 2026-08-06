"""Python-level tests for ResultSet.fetchall_arrow_streamed -- the HEARTBEAT
identity check and the async-iterator protocol (__aiter__/__anext__,
StopAsyncIteration) can only be proven against the real built extension and a
real asyncio event loop.

The heartbeat interval itself is hardcoded at 15s in production code (no
Python-facing knob) -- tests don't wait through that. Instead they exploit
that the total_timeout_s deadline is checked at the *start* of each tick,
so a very short total_timeout_s (e.g. 0.01s) naturally shortens the first
wait too (min(interval, deadline - now)), making the heartbeat-then-timeout
path fast and deterministic without needing virtual time."""

import http.server
import json
import threading

import pytest

from arrowbricks import _core as arrowbricks_core

WAREHOUSE_ID = "wh-test-123"
STATEMENT_ID = "stmt-abc"


def _start_fast_server(n_chunks: int, rows_per_chunk: int):
    """Statement is SUCCEEDED immediately -- no heartbeats expected."""
    port_holder: dict = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _json(self, payload, code=200):
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}":
                return self._json({"state": "RUNNING"})
            if self.path == f"/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/0":
                link = f"http://127.0.0.1:{port_holder['port']}/_data/chunk-0"
                return self._json({"external_links": [{"external_link": link}]})
            if self.path == "/_data/chunk-0":
                import io

                import arro3.core as core
                import arro3.io as aio

                ids = list(range(n_chunks * rows_per_chunk))
                table = core.Table.from_pydict({"id": core.Array(ids, type=core.DataType.int64())})
                buf = io.BytesIO()
                aio.write_ipc_stream(table, buf, compression=None)
                body = buf.getvalue()
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):
            # Drain the body on every branch, even the 404 fallback -- an
            # unread body desyncs the next request on a kept-alive
            # connection, and no Content-Length on a bodyless response
            # leaves the client hanging until its own timeout.
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            if self.path == "/api/2.0/sql/statements":
                del body
                return self._json(
                    {
                        "statement_id": STATEMENT_ID,
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {"chunks": [{"chunk_index": 0, "row_count": n_chunks * rows_per_chunk}]},
                    }
                )
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    port_holder["port"] = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, port_holder["port"]


def _start_stalls_on_chunk_fetch_server():
    """Statement submission succeeds immediately (one chunk), but that
    chunk's own link-resolution GET never responds -- forces
    fetchall_arrow_streamed's heartbeat wrapper to keep ticking (waiting on
    the chunk download, not the statement) until total_timeout_s fires.
    time.sleep in the handler only blocks that one request's own thread
    (ThreadingHTTPServer) -- the client-side timeout still fires quickly
    since it's the Rust reqwest future being aborted, not the server."""
    port_holder: dict = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _json(self, payload, code=200):
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}":
                return self._json({"state": "RUNNING"})
            if self.path == f"/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/0":
                import time

                time.sleep(30)  # far longer than any total_timeout_s used below
                return self._json({"external_links": []})
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):
            # Drain the body on every branch, even the 404 fallback -- an
            # unread body desyncs the next request on a kept-alive
            # connection, and no Content-Length on a bodyless response
            # leaves the client hanging until its own timeout.
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            if self.path == "/api/2.0/sql/statements":
                del body
                return self._json(
                    {
                        "statement_id": STATEMENT_ID,
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {"chunks": [{"chunk_index": 0, "row_count": 5}]},
                    }
                )
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    port_holder["port"] = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, port_holder["port"]


@pytest.mark.asyncio
async def test_fetchall_arrow_streamed_yields_one_table_no_heartbeats():
    server, port = _start_fast_server(n_chunks=6, rows_per_chunk=5)
    try:
        client = arrowbricks_core.Client(
            host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake", protocol="sea"
        )
        result_set = await client.execute("SELECT * FROM t")
        items = [item async for item in result_set.fetchall_arrow_streamed()]
        assert len(items) == 1
        assert items[0].num_rows == 30
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_fetchall_arrow_streamed_heartbeat_identity_and_timeout():
    """total_timeout_s's deadline is checked at the *start* of each tick, so
    a very short deadline (0.01s) also shortens the first wait
    (min(interval, deadline - now)) -- makes the heartbeat-then-timeout path
    fast without waiting through the real 15s interval."""
    server, port = _start_stalls_on_chunk_fetch_server()
    try:
        client = arrowbricks_core.Client(
            host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake", protocol="sea"
        )
        result_set = await client.execute("SELECT * FROM t")
        seen_heartbeat = False
        with pytest.raises(RuntimeError, match="timeout"):
            async for item in result_set.fetchall_arrow_streamed(total_timeout_s=0.01):
                assert item is arrowbricks_core.HEARTBEAT
                seen_heartbeat = True
        assert seen_heartbeat, "expected at least one HEARTBEAT before the timeout fired"
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_heartbeat_is_a_true_singleton():
    # Two independently-triggered heartbeats (from two separate iterators)
    # must be the *same* object -- callers do `item is HEARTBEAT`.
    server, port = _start_stalls_on_chunk_fetch_server()
    try:
        client = arrowbricks_core.Client(
            host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake", protocol="sea"
        )
        result_set = await client.execute("SELECT * FROM t")
        first = await result_set.fetchall_arrow_streamed(total_timeout_s=0.01).__anext__()
        second_result_set = await client.execute("SELECT * FROM t")
        second = await second_result_set.fetchall_arrow_streamed(total_timeout_s=0.01).__anext__()
        assert first is arrowbricks_core.HEARTBEAT
        assert second is arrowbricks_core.HEARTBEAT
        assert first is second, "HEARTBEAT must be one singleton object, not a fresh instance per tick"
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_streamed_iterator_stops_after_yielding_once():
    server, port = _start_fast_server(n_chunks=1, rows_per_chunk=3)
    try:
        client = arrowbricks_core.Client(
            host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake", protocol="sea"
        )
        result_set = await client.execute("SELECT * FROM t")
        it = result_set.fetchall_arrow_streamed()
        first = await it.__anext__()
        assert first is not arrowbricks_core.HEARTBEAT
        with pytest.raises(StopAsyncIteration):
            await it.__anext__()
    finally:
        server.shutdown()
