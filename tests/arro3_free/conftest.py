"""Fixtures for the arro3-free regression suite (see .github/workflows/ci.yml's
`test-arro3-free` job). pytest imports every `conftest.py` from rootdir down
to a test's own directory, so nesting here under `tests/` means
`tests/conftest.py` gets imported too -- that's fine, its own arro3 import is
deliberately lazy (inside `build_chunk_bytes`, see its docstring), so it
imports cleanly with arro3 absent as long as nothing here ever calls that
function or the fixtures built on it (`mock_warehouse`, `chunk_bytes_builder`,
etc.). This file, and everything else under this directory, must never import
arro3 itself -- the CI job that runs this suite installs arrowbricks with no
extras at all."""

from __future__ import annotations

import http.server
import json
import threading
from pathlib import Path
from typing import Any

import pytest

WAREHOUSE_ID = "wh-test-123"
STATEMENT_ID = "stmt-abc"

# Pre-built via arro3 once (see this file's own generation note below) and
# committed as bytes -- so this whole suite never needs arro3 installed to
# get real Arrow-IPC data to serve.
SAMPLE_CHUNK_BYTES = (Path(__file__).parent / "fixtures" / "sample_chunk.arrow").read_bytes()


class _DispatchingServer(http.server.ThreadingHTTPServer):
    owner: OneChunkServer


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: _DispatchingServer  # narrows socketserver's untyped `self.server`

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def _json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _bytes(self, body: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}":
            return self._json({"state": "RUNNING"})
        if self.path == f"/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/0":
            return self._json({"external_links": [{"external_link": f"{self.server.owner.host}/_data/chunk-0"}]})
        if self.path == "/_data/chunk-0":
            return self._bytes(SAMPLE_CHUNK_BYTES, "application/vnd.apache.arrow.stream")
        # Content-Length: 0 explicit -- see do_POST's fallback for why.
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        # Every branch drains the request body first, even the 404 fallback
        # -- with HTTP/1.1 keep-alive, leaving unread bytes in the socket
        # desyncs the next request on the same connection.
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        if self.path == "/api/2.0/sql/statements":
            del body  # unused -- fixed one-chunk response regardless of the SQL sent
            return self._json(
                {
                    "statement_id": STATEMENT_ID,
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"chunks": [{"chunk_index": 0, "row_count": 5}]},
                }
            )
        # Explicit Content-Length: 0 -- with no body and no Content-Length,
        # HTTP/1.1 keep-alive leaves the client unable to tell the response
        # is actually complete (nothing here closes the connection either),
        # so it just hangs waiting for more until its own timeout fires.
        # Found via the SEA session pool, whose session-creation POST is the
        # first thing to ever hit this fallback branch on this fixture.
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()


class OneChunkServer:
    """A real local HTTP server standing in for a Databricks SQL warehouse --
    one statement, one chunk, always the same `SAMPLE_CHUNK_BYTES` (id/label
    columns, 5 rows) -- enough to exercise the whole submit -> poll -> resolve
    chunk link -> fetch -> decode path without needing arro3 to build varied
    fixture data per test."""

    def __init__(self) -> None:
        self._server = _DispatchingServer(("127.0.0.1", 0), _Handler)
        self._server.owner = self
        self._server.daemon_threads = True
        self.host = f"http://127.0.0.1:{self._server.server_address[1]}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def one_chunk_server():
    server = OneChunkServer()
    yield server
    server.shutdown()


@pytest.fixture
def warehouse_id() -> str:
    return WAREHOUSE_ID


@pytest.fixture
def sample_chunk_bytes() -> bytes:
    return SAMPLE_CHUNK_BYTES
