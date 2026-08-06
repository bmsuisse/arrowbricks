"""Shared fixtures: a real local HTTP server standing in for a Databricks SQL
warehouse. respx (which patches httpx's transport) can't intercept the Rust
core's own reqwest-based requests at all -- everything in this package
delegates its actual network I/O to it -- so this is a real socket instead.

arro3 here is a test-only dependency (building synthetic Arrow-IPC chunk
bytes) -- the package itself has none; see rust/arrowbricks_core's own
Arrow/NDJSON encoding. The import is deliberately lazy (inside
`build_chunk_bytes`, not at module level) so this file stays importable with
arro3 absent -- `tests/arro3_free/` nests under this directory and would
otherwise fail collection entirely just from pytest loading this conftest.py
on the way down, regardless of whether any of its own tests actually call
`build_chunk_bytes`.
"""

from __future__ import annotations

import http.server
import io
import json
import re
import threading
import time
from dataclasses import dataclass
from typing import Any

import pytest

WAREHOUSE_ID = "wh-test-123"


def build_chunk_bytes(lo: int, hi: int) -> bytes:
    """Synthesizes one chunk's Arrow-IPC bytes via arro3 directly -- an id
    column running lo..hi-1 plus a label column -- so tests exercise the real
    Arrow IPC round trip without a live Databricks connection."""
    import arro3.core as core
    import arro3.io as aio

    ids = list(range(lo, hi))
    id_col = core.Array(ids, type=core.DataType.int64())
    label_col = core.Array([f"row_{i}" for i in ids], type=core.DataType.string())
    table = core.Table.from_pydict({"id": id_col, "label": label_col})
    buf = io.BytesIO()
    aio.write_ipc_stream(table, buf, compression=None)
    return buf.getvalue()


@dataclass
class Request:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


@dataclass
class Response:
    status: int = 200
    json_body: Any | None = None
    content: bytes | None = None
    content_type: str | None = None


class Route:
    """One registered (method, path) match -- tracks call_count like a respx
    Route, and supports either a single static Response, a list of Responses
    consumed in order (last one repeats, for a retry-then-succeed test), or a
    per-request callable."""

    def __init__(self) -> None:
        self.call_count = 0
        self._sequence: list[Response] | None = None
        self._callable = None
        self._static: Response | None = None

    def mock(self, response: Response | None = None, *, side_effect: Any = None) -> Route:
        if isinstance(side_effect, list):
            self._sequence = side_effect
        elif callable(side_effect):
            self._callable = side_effect
        else:
            self._static = response
        return self

    def respond(self, request: Request) -> Response:
        self.call_count += 1
        if self._sequence is not None:
            idx = min(self.call_count - 1, len(self._sequence) - 1)
            return self._sequence[idx]
        if self._callable is not None:
            return self._callable(request)
        return self._static or Response(status=404)


class MockServer:
    """A real local HTTP server -- routes register in respx style
    (`server.get(path).mock(...)`) but requests are handled by a genuine
    `ThreadingHTTPServer`, so both httpx and reqwest see real responses over a
    real socket rather than an intercepted transport."""

    def __init__(self) -> None:
        self._routes: list[tuple[str, re.Pattern[str], Route]] = []
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: object) -> None:  # noqa: A002
                pass

            def _handle(self, method: str) -> None:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b""
                request = Request(method=method, path=self.path, headers=dict(self.headers), body=body)
                route = outer._match(method, self.path)
                response = route.respond(request) if route is not None else Response(status=404)
                self._send(response)

            def _send(self, response: Response) -> None:
                if response.json_body is not None:
                    body = json.dumps(response.json_body).encode()
                    content_type = "application/json"
                elif response.content is not None:
                    body = response.content
                    content_type = "application/octet-stream"
                else:
                    body = b""
                    content_type = "application/octet-stream"
                self.send_response(response.status)
                self.send_header("Content-Type", response.content_type or content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if body:
                    self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802
                self._handle("GET")

            def do_POST(self) -> None:  # noqa: N802
                self._handle("POST")

            def do_PUT(self) -> None:  # noqa: N802
                self._handle("PUT")

            def do_DELETE(self) -> None:  # noqa: N802
                self._handle("DELETE")

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]
        self.host = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def _match(self, method: str, path: str) -> Route | None:
        for m, pattern, route in self._routes:
            if m == method and pattern.match(path):
                return route
        return None

    def _register(self, method: str, path: str, *, regex: bool) -> Route:
        pattern = re.compile(path) if regex else re.compile(f"^{re.escape(path)}$")
        route = Route()
        self._routes.append((method, pattern, route))
        return route

    def get(self, path: str, *, regex: bool = False) -> Route:
        return self._register("GET", path, regex=regex)

    def post(self, path: str, *, regex: bool = False) -> Route:
        return self._register("POST", path, regex=regex)

    def put(self, path: str, *, regex: bool = False) -> Route:
        return self._register("PUT", path, regex=regex)

    def delete(self, path: str, *, regex: bool = False) -> Route:
        return self._register("DELETE", path, regex=regex)

    def shutdown(self) -> None:
        self._server.shutdown()


@pytest.fixture
def mock_server():
    servers: list[MockServer] = []

    def _make() -> MockServer:
        server = MockServer()
        servers.append(server)
        return server

    yield _make
    for server in servers:
        server.shutdown()


@pytest.fixture
def chunk_bytes_builder():
    return build_chunk_bytes


@pytest.fixture
def mock_warehouse(mock_server):
    """Spins up a real local warehouse with `n_chunks` chunks of
    `rows_per_chunk` rows each (id column running 0..n_chunks*rows_per_chunk).
    Returns (server, warehouse_status_route) -- `server.host`/`WAREHOUSE_ID`
    for constructing a client, `warehouse_status_route.call_count` for TTL-
    caching assertions."""

    def _install(n_chunks: int, rows_per_chunk: int, *, reverse_arrival: bool = False, statement_id: str = "stmt-abc"):
        server = mock_server()
        chunks = [{"chunk_index": i, "row_count": rows_per_chunk} for i in range(n_chunks)]
        chunk_bytes = {i: build_chunk_bytes(i * rows_per_chunk, (i + 1) * rows_per_chunk) for i in range(n_chunks)}

        warehouse_route = server.get(f"/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(
            Response(json_body={"state": "RUNNING"})
        )
        server.post("/api/2.0/sql/statements").mock(
            Response(
                json_body={
                    "statement_id": statement_id,
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {
                        "chunks": chunks,
                        "schema": {
                            "columns": [
                                {"name": "id", "type_name": "LONG"},
                                {"name": "label", "type_name": "STRING"},
                            ]
                        },
                    },
                }
            )
        )

        def resolve_chunk(request: Request) -> Response:
            idx = int(request.path.rsplit("/", 1)[-1])
            return Response(json_body={"external_links": [{"external_link": f"{server.host}/_data/chunk-{idx}"}]})

        server.get(rf"^/api/2\.0/sql/statements/{statement_id}/result/chunks/\d+$", regex=True).mock(
            side_effect=resolve_chunk
        )

        def serve_chunk_bytes(request: Request) -> Response:
            idx = int(request.path.rsplit("-", 1)[-1])
            if reverse_arrival:
                time.sleep((n_chunks - idx) * 0.01)
            return Response(content=chunk_bytes[idx])

        server.get(r"^/_data/chunk-\d+$", regex=True).mock(side_effect=serve_chunk_bytes)
        return server, warehouse_route

    return _install


@pytest.fixture
def mock_thrift_server():
    """The Thrift-speaking analogue of `mock_server` -- a real local
    `ThreadingHTTPServer` (see `thrift_mock.ThriftMockServer`) for
    `protocol="thrift"` tests. Not imported at module level (like
    `chunk_bytes_builder`'s own lazy arro3 import) so this file stays
    importable with `databricks-sql-connector` absent -- it's a dev-only
    dependency (see pyproject.toml's `dependency-groups.dev` comment), used
    only to build/parse real Thrift wire bytes in tests, never shipped."""
    from thrift_mock import ThriftMockServer

    servers: list[ThriftMockServer] = []

    def _make(warehouse_id: str = WAREHOUSE_ID) -> ThriftMockServer:
        server = ThriftMockServer(warehouse_id)
        servers.append(server)
        return server

    yield _make
    for server in servers:
        server.shutdown()


@pytest.fixture
def mock_volume_files(mock_server):
    """Spins up a real local server for the Files API's PUT (upload)/DELETE
    endpoints -- returns (server, put_route, delete_route)."""

    def _install():
        server = mock_server()
        put_route = server.put(r"^/api/2\.0/fs/files/.*$", regex=True).mock(Response(status=204))
        delete_route = server.delete(r"^/api/2\.0/fs/files/.*$", regex=True).mock(Response(status=204))
        return server, put_route, delete_route

    return _install
