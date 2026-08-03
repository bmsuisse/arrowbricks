"""Python-level regression test for token_provider bridging.

This exists specifically because the bug it guards against is invisible to
`cargo test`: calling a Python `token_provider` from inside a nested
`tokio::spawn`-ed chunk-fetch worker task requires the right asyncio
event-loop context to be propagated into that task (see lib.rs's
PyTokenProvider doc comment). `cargo test` never runs inside a real asyncio
event loop with real nested task contexts, so this class of bug can only be
caught by actually running the built extension against a real Python
asyncio loop with a multi-chunk result (forcing worker tasks to be spawned).
A single-chunk test wouldn't exercise it either -- the first token call
(warehouse status) always succeeds from the outer task; only chunk-index
resolution inside a spawned worker triggers the missing-event-loop case.
"""

import asyncio
import http.server
import io
import json
import re
import threading

import arro3.core as core
import arro3.io as aio
import arrowbricks_core
import pytest

WAREHOUSE_ID = "wh-test-123"
STATEMENT_ID = "stmt-abc"


def _build_chunk_bytes(lo: int, hi: int) -> bytes:
    ids = list(range(lo, hi))
    id_col = core.Array(ids, type=core.DataType.int64())
    table = core.Table.from_pydict({"id": id_col})
    buf = io.BytesIO()
    aio.write_ipc_stream(table, buf, compression=None)
    return buf.getvalue()


def _start_server(n_chunks: int, rows_per_chunk: int, seen_tokens: list):
    chunk_bytes = {i: _build_chunk_bytes(i * rows_per_chunk, (i + 1) * rows_per_chunk) for i in range(n_chunks)}
    port_holder: dict = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _json(self, payload, code=200):
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                seen_tokens.append(auth.removeprefix("Bearer "))
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}":
                return self._json({"state": "RUNNING"})
            m = re.match(rf"^/api/2\.0/sql/statements/{STATEMENT_ID}/result/chunks/(\d+)$", self.path)
            if m:
                idx = int(m.group(1))
                link = f"http://127.0.0.1:{port_holder['port']}/_data/chunk-{idx}"
                return self._json({"external_links": [{"external_link": link}]})
            m = re.match(r"^/_data/chunk-(\d+)$", self.path)
            if m:
                body = chunk_bytes[int(m.group(1))]
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
                chunks = [{"chunk_index": i, "row_count": rows_per_chunk} for i in range(n_chunks)]
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
async def test_static_token_still_works():
    seen: list = []
    server, port = _start_server(n_chunks=3, rows_per_chunk=5, seen_tokens=seen)
    try:
        client = arrowbricks_core.Client(
            host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fixed-token"
        )
        table = await client.execute_arrow("SELECT * FROM t")
        assert table.num_rows == 15
        assert seen and all(t == "fixed-token" for t in seen)
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_sync_token_provider_called_fresh_every_request():
    seen: list = []
    calls = {"n": 0}

    def provider() -> str:
        calls["n"] += 1
        return f"sync-{calls['n']}"

    server, port = _start_server(n_chunks=6, rows_per_chunk=5, seen_tokens=seen)
    try:
        client = arrowbricks_core.Client(
            host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token_provider=provider
        )
        table = await client.execute_arrow("SELECT * FROM t")
        assert table.num_rows == 30
        # warehouse status + submit + one chunk-index resolve per chunk
        assert len(seen) == 2 + 6
        assert len(set(seen)) == len(seen), f"token_provider must not be cached: {seen}"
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_async_token_provider_works_from_nested_worker_tasks():
    """Regression test for the actual bug found during development: calling
    into_future from inside a tokio::spawn-ed chunk-fetch worker task failed
    with "no running event loop" because those tasks don't inherit the outer
    task's asyncio-loop context. Needs multiple chunks (so at least one
    chunk-index resolution happens inside a spawned worker, not the outer
    task) -- a single-chunk result wouldn't exercise this at all."""
    seen: list = []
    calls = {"n": 0}

    async def provider() -> str:
        calls["n"] += 1
        n = calls["n"]
        await asyncio.sleep(0)  # actually suspend, not just call-and-return
        return f"async-{n}"

    server, port = _start_server(n_chunks=8, rows_per_chunk=5, seen_tokens=seen)
    try:
        client = arrowbricks_core.Client(
            host=f"http://127.0.0.1:{port}",
            warehouse_id=WAREHOUSE_ID,
            token_provider=provider,
            chunk_fetch_concurrency=4,
        )
        result_set = await client.execute("SELECT * FROM t")
        table = await result_set.fetchall_arrow()
        assert table.num_rows == 40
        assert calls["n"] == len(seen), "coroutine leak: provider calls vs delivered tokens mismatch"
        assert len(set(seen)) == len(seen), f"token_provider must not be cached: {seen}"
    finally:
        server.shutdown()


def test_client_requires_token_or_token_provider():
    with pytest.raises(ValueError, match="token"):
        arrowbricks_core.Client(host="https://example.com", warehouse_id=WAREHOUSE_ID)
