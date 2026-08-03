"""Python-level tests for Client.stream_chunks_arrow -- the chunk-at-a-time
streaming primitive backing stream_query_json. Proves: one Table per chunk,
in logical (chunk_index) order even when a later chunk resolves first,
StopAsyncIteration once exhausted, and heartbeat/timeout during a slow
chunk (not just a slow initial submit/poll)."""

import http.server
import io
import json
import threading
import time

import arro3.core as core
import arro3.io as aio
import arrowbricks_core
import pytest

WAREHOUSE_ID = "wh-test-123"
STATEMENT_ID = "stmt-abc"


def _chunk_bytes(values: list[int]) -> bytes:
    table = core.Table.from_pydict({"id": core.Array(values, type=core.DataType.int64())})
    buf = io.BytesIO()
    aio.write_ipc_stream(table, buf, compression=None)
    return buf.getvalue()


def _start_server(chunk_values: list[list[int]], *, chunk_delay_s: dict[int, float] | None = None):
    """Serves `len(chunk_values)` chunks, each its own external link. An
    entry in `chunk_delay_s` makes that chunk_index's byte download stall
    before responding, to force genuine out-of-order arrival."""
    port_holder: dict = {}
    chunk_delay_s = chunk_delay_s or {}

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
            for idx in range(len(chunk_values)):
                if self.path == f"/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/{idx}":
                    link = f"http://127.0.0.1:{port_holder['port']}/_data/chunk-{idx}"
                    return self._json({"external_links": [{"external_link": link}]})
                if self.path == f"/_data/chunk-{idx}":
                    time.sleep(chunk_delay_s.get(idx, 0.0))
                    body = _chunk_bytes(chunk_values[idx])
                    self.send_response(200)
                    self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
            self.send_response(404)
            self.end_headers()

        def do_POST(self):
            if self.path == "/api/2.0/sql/statements":
                length = int(self.headers.get("Content-Length", 0))
                self.rfile.read(length)
                chunks = [{"chunk_index": i, "row_count": len(v)} for i, v in enumerate(chunk_values)]
                return self._json(
                    {
                        "statement_id": STATEMENT_ID,
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {"chunks": chunks},
                    }
                )
            self.send_response(404)
            self.end_headers()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    port_holder["port"] = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, port_holder["port"]


@pytest.mark.asyncio
async def test_stream_chunks_arrow_yields_one_table_per_chunk_in_order():
    server, port = _start_server([[1, 2], [3, 4, 5], [6]])
    try:
        client = arrowbricks_core.Client(host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake")
        tables = [item async for item in client.stream_chunks_arrow("SELECT * FROM t")]
        assert all(t is not arrowbricks_core.HEARTBEAT for t in tables)
        assert [t.num_rows for t in tables] == [2, 3, 1]
        all_ids = [v for t in tables for v in core.Table.from_arrow(t)["id"].to_pylist()]
        assert all_ids == [1, 2, 3, 4, 5, 6], "chunks must come out in logical order"
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_stream_chunks_arrow_preserves_order_despite_out_of_order_arrival():
    # Chunk 0 is slow, chunk 1 resolves first -- must still be yielded 0, 1.
    server, port = _start_server([[1], [2]], chunk_delay_s={0: 0.2})
    try:
        client = arrowbricks_core.Client(host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake")
        tables = [item async for item in client.stream_chunks_arrow("SELECT * FROM t")]
        all_ids = [v for t in tables for v in core.Table.from_arrow(t)["id"].to_pylist()]
        assert all_ids == [1, 2]
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_stream_chunks_arrow_stops_after_exhausted():
    server, port = _start_server([[1]])
    try:
        client = arrowbricks_core.Client(host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake")
        it = client.stream_chunks_arrow("SELECT * FROM t")
        first = await it.__anext__()
        assert first.num_rows == 1
        with pytest.raises(StopAsyncIteration):
            await it.__anext__()
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_stream_chunks_arrow_heartbeats_on_slow_chunk_then_times_out():
    """total_timeout_s covers each chunk pull, not just the initial
    submit/poll -- a chunk that never arrives must still heartbeat then
    time out, matching stream_query_json's heartbeat_over_stream wrapping."""
    server, port = _start_server([[1]], chunk_delay_s={0: 60.0})
    try:
        client = arrowbricks_core.Client(host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake")
        seen_heartbeat = False
        with pytest.raises(RuntimeError, match="timeout"):
            async for item in client.stream_chunks_arrow("SELECT * FROM t", total_timeout_s=0.01):
                assert item is arrowbricks_core.HEARTBEAT
                seen_heartbeat = True
        assert seen_heartbeat, "expected at least one HEARTBEAT before the timeout fired"
    finally:
        server.shutdown()
