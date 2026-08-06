"""Thrift-over-HTTP mock server support for `protocol="thrift"` tests --
built on `databricks-sql-connector`'s own installed, real
Apache-Thrift-compiler-generated `databricks.sql.thrift_api.TCLIService`
module (`ttypes.py`'s structs + `TCLIService.py`'s real `Processor`
dispatch), not a hand-rolled second Python Thrift codec.

Why: `rust/arrowbricks_core/src/thrift.rs`'s own module doc comment says its
field IDs were read directly out of this exact installed package's
`ttypes.py` -- i.e. this *is* the ground truth the Rust implementation was
built against, not just "a" Thrift library. Building mock responses against
it directly (real `TBinaryProtocol` read/write, real generated structs) is
strictly more trustworthy than a second hand-written parser/encoder that
could carry its own independent bugs -- and it's a test-only dependency
(see pyproject.toml's `dependency-groups.dev` comment), never imported by
the shipped package, so it doesn't touch this project's "zero required
runtime dependencies" invariant at all.

`rust/arrowbricks_core/tests/wiremock_thrift.rs` (the Rust-side sibling of
this module) *does* hand-roll its response bytes via `thrift.rs`'s own
`Writer` -- there is no equivalent "already correct, ground truth" crate
available there the way there is here, so that's a deliberate, asymmetric
choice between the two languages' mocks, not an oversight.

Every RPC arrowbricks speaks (`OpenSession`/`ExecuteStatement`/
`GetOperationStatus`/`FetchResults`/`CloseOperation`/`CloseSession`/
`CancelOperation`) hits one shared HTTP path (`/sql/1.0/warehouses/{id}`,
POST only -- see `client.rs`'s `thrift_url`) -- `TCLIService.Processor.process`
reads the Thrift message name straight out of the request body and
dispatches to the matching `Handler` method itself, so (unlike the Rust
mock's own `IsThriftRpc` custom `wiremock::Match`) no extra "which RPC is
this" routing code is needed here at all.
"""

from __future__ import annotations

import http.server
import io
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from databricks.sql.thrift_api.TCLIService import TCLIService, ttypes
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

StatusCode = ttypes.TStatusCode
OperationState = ttypes.TOperationState


def ok_status() -> ttypes.TStatus:
    return ttypes.TStatus(statusCode=StatusCode.SUCCESS_STATUS)


def error_status(message: str) -> ttypes.TStatus:
    """A `TStatus`-level failure -- read correctly by `thrift.rs`'s
    `Status::read`/`Status::error()` (field 6, `displayMessage`, matches
    real `ttypes.TStatus.displayMessage`'s own field id). Note this is a
    *different* struct from `TGetOperationStatusResp`'s own separate
    `displayMessage` field (see `operation_status_resp`'s own doc comment
    below for why that one is deliberately not used the same way here)."""
    return ttypes.TStatus(statusCode=StatusCode.ERROR_STATUS, displayMessage=message)


def handle_id(guid: bytes, secret: bytes = b"secret") -> ttypes.THandleIdentifier:
    return ttypes.THandleIdentifier(guid=guid, secret=secret)


def session_handle(guid: bytes, secret: bytes = b"secret") -> ttypes.TSessionHandle:
    return ttypes.TSessionHandle(sessionId=handle_id(guid, secret))


def operation_handle(guid: bytes, secret: bytes = b"opsecret") -> ttypes.TOperationHandle:
    return ttypes.TOperationHandle(operationId=handle_id(guid, secret), operationType=0, hasResultSet=True)


def operation_status_resp(
    state: int, *, error_message: str | None = None, display_message: str | None = None
) -> ttypes.TGetOperationStatusResp:
    """Builds a `TGetOperationStatusResp` for a `GetOperationStatus` reply
    (standalone or nested inside `TSparkDirectResults.operationStatus`).

    Building this mock against the real, installed `ttypes.py` caught a
    real field-id bug in `thrift.rs`'s own `OperationStatusResp::read`: it
    read this struct's "display message" at field id 12, but the real
    `ttypes.TGetOperationStatusResp.thrift_spec` has no field 12 at all --
    `displayMessage` is field **1281**. Fixed in `thrift.rs` this same
    session (see its own comment on the `(1281, ttype::STRING)` arm) --
    `display_message` set here now round-trips correctly end to end.
    """
    return ttypes.TGetOperationStatusResp(
        status=ok_status(),
        operationState=state,
        errorMessage=error_message,
        displayMessage=display_message,
    )


def result_set_metadata(
    *, lz4_compressed: bool = False, arrow_schema: bytes | None = None
) -> ttypes.TGetResultSetMetadataResp:
    return ttypes.TGetResultSetMetadataResp(status=ok_status(), lz4Compressed=lz4_compressed, arrowSchema=arrow_schema)


def row_set(
    *,
    arrow_batches: list[tuple[bytes, int]] | None = None,
    result_links: list[tuple[str, int]] | None = None,
) -> ttypes.TRowSet:
    batches = [ttypes.TSparkArrowBatch(batch=b, rowCount=rc) for b, rc in (arrow_batches or [])]
    links = [ttypes.TSparkArrowResultLink(fileLink=link, rowCount=rc) for link, rc in (result_links or [])]
    return ttypes.TRowSet(arrowBatches=batches or None, resultLinks=links or None)


def fetch_results_resp(
    *,
    has_more_rows: bool = False,
    arrow_batches: list[tuple[bytes, int]] | None = None,
    result_links: list[tuple[str, int]] | None = None,
    lz4_compressed: bool | None = None,
    arrow_schema: bytes | None = None,
    status: ttypes.TStatus | None = None,
) -> ttypes.TFetchResultsResp:
    results = (
        row_set(arrow_batches=arrow_batches, result_links=result_links) if (arrow_batches or result_links) else None
    )
    metadata = (
        result_set_metadata(lz4_compressed=lz4_compressed or False, arrow_schema=arrow_schema)
        if lz4_compressed is not None
        else None
    )
    return ttypes.TFetchResultsResp(
        status=status or ok_status(),
        hasMoreRows=has_more_rows,
        results=results,
        resultSetMetadata=metadata,
    )


@dataclass
class DirectResults:
    operation_state: int | None = None
    operation_error: str | None = None
    lz4_compressed: bool | None = None
    arrow_schema: bytes | None = None
    fetch: ttypes.TFetchResultsResp | None = None


def execute_statement_resp(
    *,
    op_guid: bytes = b"op",
    status: ttypes.TStatus | None = None,
    direct: DirectResults | None = None,
) -> ttypes.TExecuteStatementResp:
    direct_results = None
    if direct is not None:
        op_status = (
            operation_status_resp(direct.operation_state, error_message=direct.operation_error)
            if direct.operation_state is not None
            else None
        )
        metadata = (
            result_set_metadata(lz4_compressed=direct.lz4_compressed or False, arrow_schema=direct.arrow_schema)
            if direct.lz4_compressed is not None
            else None
        )
        direct_results = ttypes.TSparkDirectResults(
            operationStatus=op_status,
            resultSetMetadata=metadata,
            resultSet=direct.fetch,
        )
    return ttypes.TExecuteStatementResp(
        status=status or ok_status(),
        operationHandle=operation_handle(op_guid) if status is None else None,
        directResults=direct_results,
    )


# ---- Arrow-IPC byte helpers (arro3 -- test-only, see tests/conftest.py's own reasoning) ----


def build_full_ipc_stream(lo: int, hi: int) -> bytes:
    """A full, standalone, valid Arrow-IPC stream (schema + one record batch
    + EOS) -- what a cloud-fetch `resultLinks` download must be (decoded
    directly, same shape SEA's own external-link chunks use). Same shape as
    `conftest.py`'s own `build_chunk_bytes` -- duplicated here rather than
    imported so this module has no import-order dependency on `conftest.py`."""
    import arro3.core as core
    import arro3.io as aio

    ids = list(range(lo, hi))
    id_col = core.Array(ids, type=core.DataType.int64())
    label_col = core.Array([f"row_{i}" for i in ids], type=core.DataType.string())
    table = core.Table.from_pydict({"id": id_col, "label": label_col})
    buf = io.BytesIO()
    aio.write_ipc_stream(table, buf, compression=None)
    return buf.getvalue()


def build_schema_and_batches(ranges: list[tuple[int, int]]) -> tuple[bytes, list[bytes]]:
    """Splits N full IPC streams (sharing one schema) into
    (schema-message-only bytes, one batch-message-only-bytes per range) --
    exactly what `pipeline.rs`'s `build_inline_blob` expects to concatenate
    for the `arrowBatches` (direct-results / inline `FetchResults`) path:
    the schema message travels once, separately
    (`TGetResultSetMetadataResp.arrowSchema`), and no EOS marker belongs in
    the mix at all (`arrow_ipc::reader::StreamDecoder::finish()` accepts
    ending cleanly right after the last full message, confirmed directly
    against its own source -- see `wiremock_thrift.rs`'s own matching
    helper for the full reasoning).

    Isolating the schema message: two full streams sharing an identical
    schema are byte-identical up to (and only up to) the point their actual
    row data diverges -- confirmed empirically (a 3-row stream and a 0-row
    stream of the same schema share exactly a 196-byte prefix, which is
    exactly the schema message; arro3 still emits a zero-row *batch*
    message for the empty case, so this is a real divergence point, not a
    coincidence). Each per-range batch's own trailing 8-byte EOS marker
    (`b"\\xff\\xff\\xff\\xff\\x00\\x00\\x00\\x00"`, confirmed by inspecting
    the trailing bytes directly) is stripped before concatenation for the
    same reason.
    """
    streams = [build_full_ipc_stream(lo, hi) for lo, hi in ranges]
    schema_len = _common_prefix_len(streams[0], build_full_ipc_stream(ranges[0][0], ranges[0][0]))
    schema_bytes = streams[0][:schema_len]
    eos = b"\xff\xff\xff\xff\x00\x00\x00\x00"
    batch_bytes = []
    for s in streams:
        assert s.endswith(eos), "expected the standard 8-byte Arrow-IPC EOS marker"
        batch_bytes.append(s[schema_len : len(s) - len(eos)])
    return schema_bytes, batch_bytes


def _common_prefix_len(a: bytes, b: bytes) -> int:
    n = 0
    limit = min(len(a), len(b))
    while n < limit and a[n] == b[n]:
        n += 1
    return n


# ---- RPC dispatch -----------------------------------------------------------


@dataclass
class Handler(TCLIService.Iface):
    """One Thrift RPC handler -- each of the 7 methods arrowbricks speaks is
    a plain callable taking the parsed request object and returning the
    matching response object; a test assigns only the ones it needs
    (mirroring `wiremock_thrift.rs`'s own per-RPC `Mock::given(...)`
    mounts). `CloseOperation`/`CloseSession`/`CancelOperation` default to a
    bare success response if left unassigned, since most tests don't care
    about their bodies, only (at most) that they were called."""

    open_session: Callable[[Any], ttypes.TOpenSessionResp] | None = None
    execute_statement: Callable[[Any], ttypes.TExecuteStatementResp] | None = None
    get_operation_status: Callable[[Any], ttypes.TGetOperationStatusResp] | None = None
    fetch_results: Callable[[Any], ttypes.TFetchResultsResp] | None = None
    close_operation: Callable[[Any], ttypes.TCloseOperationResp] | None = None
    close_session: Callable[[Any], ttypes.TCloseSessionResp] | None = None
    cancel_operation: Callable[[Any], ttypes.TCancelOperationResp] | None = None

    def OpenSession(self, req: Any) -> ttypes.TOpenSessionResp:  # noqa: N802
        assert self.open_session is not None, "OpenSession not mocked for this test"
        return self.open_session(req)

    def ExecuteStatement(self, req: Any) -> ttypes.TExecuteStatementResp:  # noqa: N802
        assert self.execute_statement is not None, "ExecuteStatement not mocked for this test"
        return self.execute_statement(req)

    def GetOperationStatus(self, req: Any) -> ttypes.TGetOperationStatusResp:  # noqa: N802
        assert self.get_operation_status is not None, "GetOperationStatus not mocked for this test"
        return self.get_operation_status(req)

    def FetchResults(self, req: Any) -> ttypes.TFetchResultsResp:  # noqa: N802
        assert self.fetch_results is not None, "FetchResults not mocked for this test"
        return self.fetch_results(req)

    def CloseOperation(self, req: Any) -> ttypes.TCloseOperationResp:  # noqa: N802
        if self.close_operation is None:
            return ttypes.TCloseOperationResp(status=ok_status())
        return self.close_operation(req)

    def CloseSession(self, req: Any) -> ttypes.TCloseSessionResp:  # noqa: N802
        if self.close_session is None:
            return ttypes.TCloseSessionResp(status=ok_status())
        return self.close_session(req)

    def CancelOperation(self, req: Any) -> ttypes.TCancelOperationResp:  # noqa: N802
        if self.cancel_operation is None:
            return ttypes.TCancelOperationResp(status=ok_status())
        return self.cancel_operation(req)


def handle_request(body: bytes, handler: TCLIService.Iface) -> bytes:
    """Runs one Thrift RPC request through the real generated `Processor`
    dispatch -- reads the RPC name + args out of `body` (`TBinaryProtocol`,
    the exact wire format `thrift.rs` speaks), calls the matching `handler`
    method, and returns the serialized reply bytes."""
    itrans = TTransport.TMemoryBuffer(body)
    otrans = TTransport.TMemoryBuffer()
    iprot = TBinaryProtocol.TBinaryProtocol(itrans)
    oprot = TBinaryProtocol.TBinaryProtocol(otrans)
    TCLIService.Processor(handler).process(iprot, oprot)
    return otrans.getvalue()


# ---- HTTP server ------------------------------------------------------------


class ThriftMockServer:
    """A real local `ThreadingHTTPServer` speaking Thrift-over-HTTP on the
    single shared path every RPC uses (`/sql/1.0/warehouses/{warehouse_id}`,
    POST only -- see `client.rs`'s `thrift_url`). `self.handler` is a
    `Handler` whose per-RPC callables a test assigns directly.

    `GET` requests are routed separately (regex path -> raw bytes) for
    cloud-fetch `resultLinks` downloads -- the same `/_data/chunk-N`
    convention `conftest.py`'s own SEA `mock_warehouse` fixture uses."""

    def __init__(self, warehouse_id: str) -> None:
        self.warehouse_id = warehouse_id
        self.handler = Handler()
        self._get_routes: list[tuple[re.Pattern[str], Callable[[re.Match[str]], bytes]]] = []
        outer = self

        class HttpHandler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: object) -> None:  # noqa: A002
                pass

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b""
                if self.path != f"/sql/1.0/warehouses/{outer.warehouse_id}":
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                try:
                    resp_bytes = handle_request(body, outer.handler)
                except Exception as exc:  # noqa: BLE001 -- surfaced as a 500, same as a real crash would be
                    msg = str(exc).encode()
                    self.send_response(500)
                    self.send_header("Content-Length", str(len(msg)))
                    self.end_headers()
                    self.wfile.write(msg)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/x-thrift")
                self.send_header("Content-Length", str(len(resp_bytes)))
                self.end_headers()
                self.wfile.write(resp_bytes)

            def do_GET(self) -> None:  # noqa: N802
                for pattern, responder in outer._get_routes:
                    m = pattern.match(self.path)
                    if m:
                        body = responder(m)
                        self.send_response(200)
                        self.send_header("Content-Type", "application/octet-stream")
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                        return
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), HttpHandler)
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]
        self.host = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def add_data_route(self, pattern: str, responder: Callable[[re.Match[str]], bytes]) -> None:
        """Registers a `GET` route (regex-matched path -> raw response
        bytes) -- used for cloud-fetch `resultLinks` file downloads."""
        self._get_routes.append((re.compile(pattern), responder))

    def shutdown(self) -> None:
        self._server.shutdown()
