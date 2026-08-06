"""Python-level tests for Client.stream_ndjson_lines -- the chunk-at-a-time
streaming primitive backing stream_query_json end to end (decode + NDJSON
encode both happen in Rust). Proves: one list[str] of NDJSON lines per
chunk, in logical (chunk_index) order even when a later chunk resolves
first, StopAsyncIteration once exhausted, and heartbeat/timeout during a
slow chunk (not just a slow initial submit/poll)."""

import http.server
import io
import json
import threading
import time

import arro3.core as core
import arro3.io as aio
import pytest

from arrowbricks import _core as arrowbricks_core

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
                chunks = [{"chunk_index": i, "row_count": len(v)} for i, v in enumerate(chunk_values)]
                return self._json(
                    {
                        "statement_id": STATEMENT_ID,
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {"chunks": chunks},
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
async def test_stream_ndjson_lines_yields_one_chunk_of_lines_in_order():
    server, port = _start_server([[1, 2], [3, 4, 5], [6]])
    try:
        client = arrowbricks_core.Client(host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake")
        chunks = [item async for item in client.stream_ndjson_lines("SELECT * FROM t")]
        assert all(c is not arrowbricks_core.HEARTBEAT for c in chunks)
        assert [len(c) for c in chunks] == [2, 3, 1]
        all_ids = [json.loads(line)["id"] for chunk in chunks for line in chunk]
        assert all_ids == [1, 2, 3, 4, 5, 6], "chunks must come out in logical order"
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_stream_ndjson_lines_preserves_order_despite_out_of_order_arrival():
    # Chunk 0 is slow, chunk 1 resolves first -- must still be yielded 0, 1.
    server, port = _start_server([[1], [2]], chunk_delay_s={0: 0.2})
    try:
        client = arrowbricks_core.Client(host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake")
        chunks = [item async for item in client.stream_ndjson_lines("SELECT * FROM t")]
        all_ids = [json.loads(line)["id"] for chunk in chunks for line in chunk]
        assert all_ids == [1, 2]
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_stream_ndjson_lines_stops_after_exhausted():
    server, port = _start_server([[1]])
    try:
        client = arrowbricks_core.Client(host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake")
        it = client.stream_ndjson_lines("SELECT * FROM t")
        first = await it.__anext__()
        assert first == ['{"id":1}']
        with pytest.raises(StopAsyncIteration):
            await it.__anext__()
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_stream_ndjson_lines_heartbeats_on_slow_chunk_then_times_out():
    """total_timeout_s covers each chunk pull, not just the initial
    submit/poll -- a chunk that never arrives must still heartbeat then
    time out, matching stream_query_json's pre-cutover heartbeat_over_stream
    wrapping."""
    server, port = _start_server([[1]], chunk_delay_s={0: 60.0})
    try:
        client = arrowbricks_core.Client(host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake")
        seen_heartbeat = False
        with pytest.raises(RuntimeError, match="timeout"):
            async for item in client.stream_ndjson_lines("SELECT * FROM t", total_timeout_s=0.01):
                assert item is arrowbricks_core.HEARTBEAT
                seen_heartbeat = True
        assert seen_heartbeat, "expected at least one HEARTBEAT before the timeout fired"
    finally:
        server.shutdown()
