from collections.abc import Awaitable, Callable
from typing import Any, BinaryIO

def ping() -> str: ...
def write_ipc_stream(stream: Any, buf: BinaryIO) -> None: ...  # stream: anything implementing __arrow_c_stream__
def read_ipc_stream(data: bytes) -> Any: ...  # Arrow table (__arrow_c_stream__)

class _Heartbeat:
    def __repr__(self) -> str: ...

HEARTBEAT: _Heartbeat

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

class ExecuteStreamedIter:
    def __aiter__(self) -> ExecuteStreamedIter: ...
    async def __anext__(self) -> ResultSet | _Heartbeat: ...

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
        chunk_fetch_concurrency: int = 32,
        http_timeout: float = 60.0,
        wait_timeout: str = "30s",
        warehouse_start_timeout: float = 300.0,
        warehouse_confirmed_running_ttl_s: float = 30.0,
        compress_results: bool = True,
    ) -> None: ...
    async def execute_arrow(
        self,
        statement: str,
        catalog: str | None = None,
        schema: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
    ) -> Any: ...  # Arrow table (__arrow_c_stream__)
    async def execute(
        self,
        statement: str,
        catalog: str | None = None,
        schema: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
    ) -> ResultSet: ...
    def execute_streamed(
        self,
        statement: str,
        catalog: str | None = None,
        schema: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
        total_timeout_s: float | None = None,
    ) -> ExecuteStreamedIter: ...
    async def execute_json(
        self,
        statement: str,
        catalog: str | None = None,
        schema: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
    ) -> list[list[Any]]: ...
    async def upload_volume_file(self, volume_path: str, data: bytes) -> None: ...
    async def delete_volume_file(self, volume_path: str) -> None: ...
    def stream_ndjson_lines(
        self,
        statement: str,
        catalog: str | None = None,
        schema: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
        total_timeout_s: float | None = None,
        non_finite_as_string: bool = False,
    ) -> NdjsonStreamIter: ...
