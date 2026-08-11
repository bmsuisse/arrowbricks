from collections.abc import Awaitable, Callable
from typing import Any, BinaryIO, Literal

def write_ipc_stream(stream: Any, buf: BinaryIO) -> None: ...  # stream: anything implementing __arrow_c_stream__
def read_ipc_stream(data: bytes) -> Any: ...  # Arrow table (__arrow_c_stream__)

class _Heartbeat:
    def __repr__(self) -> str: ...

HEARTBEAT: _Heartbeat

class ArrowbricksError(RuntimeError):
    """Base class for every exception arrowbricks raises itself. Subclasses
    RuntimeError, not Exception, so an `except RuntimeError` written before
    this hierarchy existed keeps working unchanged -- see README.md's
    "Errors" section."""

class TransientError(ArrowbricksError):
    """A retryable failure (network blip, connection reset, or a 5xx) that
    survived every internal retry (`retry_attempts`) before reaching Python."""

class AuthError(ArrowbricksError):
    """HTTP 401/403, even after every internal retry re-fetched a token."""

class StatementError(ArrowbricksError):
    """The SQL statement itself failed or was canceled server-side."""

class QueryStats:
    """One query's timing/counters, handed to `on_event` exactly once, at
    completion -- see `README.md`'s "Observability" section for the
    field-by-field description."""

    statement_id: str
    protocol: Literal["thrift", "sea"]
    warehouse_wait_s: float
    submit_to_ready_s: float
    fetch_s: float
    num_chunks: int
    bytes_downloaded: int
    retry_count: int
    concurrency_used: int
    outcome: Literal["success", "cancelled", "timeout", "error"]
    def __repr__(self) -> str: ...

class ResultSet:
    statement_id: str
    num_chunks: int
    # (name, type_name) pairs from the manifest -- pre-fetch estimate only,
    # for Cursor.description-style compatibility.
    columns: list[tuple[str, str | None]]

    async def fetchmany_arrow(self, n: int) -> Any: ...  # Arrow table (__arrow_c_stream__)
    async def fetchall_arrow(self) -> Any: ...  # Arrow table (__arrow_c_stream__)
    def fetchall_arrow_streamed(self, total_timeout_s: float | None = None) -> FetchallArrowStreamedIter: ...
    async def schema(self) -> list[tuple[str, str]] | None: ...  # real schema, known after >=1 fetch

class FetchallArrowStreamedIter:
    def __aiter__(self) -> FetchallArrowStreamedIter: ...
    async def __anext__(self) -> Any: ...  # Arrow table (__arrow_c_stream__) | _Heartbeat

class NdjsonStreamIter:
    def __aiter__(self) -> NdjsonStreamIter: ...
    async def __anext__(self) -> list[str] | _Heartbeat: ...  # NDJSON lines for one chunk

class Client:
    def __init__(
        self,
        host: str,
        warehouse_id: str,
        token: str | None = None,
        token_provider: Callable[[], str | Awaitable[str]] | None = None,
        chunk_fetch_concurrency: int = 64,
        http_timeout: float = 60.0,
        wait_timeout: str = "30s",
        warehouse_start_timeout: float = 300.0,
        warehouse_confirmed_running_ttl_s: float = 30.0,
        compress_results: bool = True,
        protocol: str = "thrift",
        on_event: Callable[[QueryStats], None | Awaitable[None]] | None = None,
        retry_attempts: int = 6,
        retry_max_wait_s: float = 20.0,
    ) -> None: ...
    async def execute(
        self,
        statement: str,
        catalog: str | None = None,
        schema: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
        prefer_inline: bool = False,
    ) -> ResultSet: ...
    async def upload_volume_file(self, volume_path: str, data: bytes) -> None: ...
    async def delete_volume_file(self, volume_path: str) -> None: ...
    async def close_sessions(self) -> None: ...
    def stream_ndjson_lines(
        self,
        statement: str,
        catalog: str | None = None,
        schema: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
        total_timeout_s: float | None = None,
        non_finite_as_string: bool = False,
    ) -> NdjsonStreamIter: ...
