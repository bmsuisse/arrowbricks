"""Python-level test for Client.upload_volume_file/delete_volume_file.
Wiremock-based Rust tests already cover the retry/status-code logic; this
just proves the actual PyO3-exposed async methods work end to end."""

import http.server
import threading

import pytest

from arrowbricks import _core as arrowbricks_core

WAREHOUSE_ID = "wh-test-123"


def _start_server(*, delete_status: int = 204):
    seen: dict = {}
    port_holder: dict = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def do_PUT(self):
            seen["path"] = self.path
            seen["content_type"] = self.headers.get("Content-Type")
            length = int(self.headers.get("Content-Length", 0))
            seen["body"] = self.rfile.read(length)
            self.send_response(204)
            self.end_headers()

        def do_DELETE(self):
            self.send_response(delete_status)
            self.end_headers()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    port_holder["port"] = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, port_holder["port"], seen


@pytest.mark.asyncio
async def test_upload_volume_file_sends_overwrite_and_body():
    server, port, seen = _start_server()
    try:
        client = arrowbricks_core.Client(host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake")
        await client.upload_volume_file("/Volumes/cat/schema/vol/f.bin", b"hello world")
        assert "overwrite=true" in seen["path"]
        assert seen["content_type"] == "application/octet-stream"
        assert seen["body"] == b"hello world"
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_delete_volume_file_treats_404_as_success():
    server, port, _seen = _start_server(delete_status=404)
    try:
        client = arrowbricks_core.Client(host=f"http://127.0.0.1:{port}", warehouse_id=WAREHOUSE_ID, token="fake")
        await client.delete_volume_file("/Volumes/cat/schema/vol/already-gone.bin")  # must not raise
    finally:
        server.shutdown()
