"""Python-level test for Client.execute_json -- the reorder buffer's effect
on real out-of-order JSON chunk arrival can only be proven end to end
(unit-level reorder correctness is already covered by pipeline.rs's own
Rust tests); this checks the actual PyO3-exposed API surface instead."""

import http.server
import json
import re
import threading

import pytest

from arrowbricks import _core as arrowbricks_core

WAREHOUSE_ID = "wh-test-123"
STATEMENT_ID = "stmt-abc"


def _start_server(n_chunks: int, rows_per_chunk: int, reverse_arrival: bool = False):
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
            m = re.match(rf"^/api/2\.0/sql/statements/{STATEMENT_ID}/result/chunks/(\d+)$", self.path)
            if m:
                idx = int(m.group(1))
                link = f"http://127.0.0.1:{port_holder['port']}/_data/chunk-{idx}"
                return self._json({"external_links": [{"external_link": link}]})
            m = re.match(r"^/_data/chunk-(\d+)$", self.path)
            if m:
                idx = int(m.group(1))
                if reverse_arrival:
                    import time

                    time.sleep((n_chunks - idx) * 0.01)
                # JSON_ARRAY's own contract: every non-null value is a
                # string regardless of real column type.
                lo = idx * rows_per_chunk
                rows = [[str(lo + i), f"row_{lo + i}"] for i in range(rows_per_chunk)]
                return self._json(rows)
            self.send_response(404)
            self.end_headers()

        def do_POST(self):
            if self.path == "/api/2.0/sql/statements":
                length = int(self.headers.get("Content-Length", 0))
                self.rfile.read(length)
                chunks = [{"chunk_index": i, "row_count": rows_per_chunk} for i in range(n_chunks)]
                return self._json(
                    {"statement_id": STATEMENT_ID, "status": {"state": "SUCCEEDED"}, "manifest": {"chunks": chunks}}
                )
            self.send_response(404)
            self.end_headers()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    port_holder["port"] = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, port_holder["port"]


@pytest.mark.asyncio
async def test_execute_json_returns_ordered_string_rows():
    server, port = _start_server(n_chunks=6, rows_per_chunk=5)
    try:
        client = arrowbricks_core.Client(host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake")
        rows = await client.execute_json("SELECT * FROM t")
        assert len(rows) == 30
        assert [r[0] for r in rows] == [str(i) for i in range(30)]
        assert all(isinstance(v, str) for row in rows for v in row), "JSON_ARRAY values must stay strings"
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_execute_json_survives_reverse_chunk_arrival():
    server, port = _start_server(n_chunks=5, rows_per_chunk=4, reverse_arrival=True)
    try:
        client = arrowbricks_core.Client(
            host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake", chunk_fetch_concurrency=4
        )
        rows = await client.execute_json("SELECT * FROM t")
        assert len(rows) == 20
        assert [r[0] for r in rows] == [str(i) for i in range(20)], "row order must survive out-of-order arrival"
    finally:
        server.shutdown()
