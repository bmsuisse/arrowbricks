"""Python-level test proving `parameters` (Databricks' named-parameter
format) actually reaches the statement-submit request body -- a silent drop
here would mean a caller's parameterized query returns wrong (unfiltered)
results, not just a missing feature."""

import http.server
import json
import threading

import pytest

from arrowbricks import _core as arrowbricks_core

WAREHOUSE_ID = "wh-test-123"
STATEMENT_ID = "stmt-abc"


def _start_server(captured_bodies: list):
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
            # Content-Length: 0 explicit -- HTTP/1.1 keep-alive with no body
            # and no Content-Length leaves the client hanging until its own
            # timeout, since it can't tell the response is complete.
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):
            # Drain the body on every branch, even the 404 fallback -- an
            # unread body desyncs the next request on a kept-alive connection.
            length = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(length)
            if self.path == "/api/2.0/sql/statements":
                body = json.loads(raw_body or b"{}")
                captured_bodies.append(body)
                return self._json(
                    {
                        "statement_id": STATEMENT_ID,
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {"chunks": []},
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
async def test_execute_forwards_parameters_to_request_body():
    captured: list = []
    server, port = _start_server(captured)
    try:
        client = arrowbricks_core.Client(
            host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake", protocol="sea"
        )
        params = [{"name": "min_id", "value": "5", "type": "INT"}]
        await client.execute("SELECT * FROM t WHERE id > :min_id", parameters=params)
        assert len(captured) == 1
        assert captured[0]["parameters"] == params
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_execute_omits_parameters_key_when_not_given():
    captured: list = []
    server, port = _start_server(captured)
    try:
        client = arrowbricks_core.Client(
            host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake", protocol="sea"
        )
        await client.execute("SELECT * FROM t")
        assert "parameters" not in captured[0]
    finally:
        server.shutdown()
