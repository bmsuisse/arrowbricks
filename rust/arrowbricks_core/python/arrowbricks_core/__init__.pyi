from collections.abc import Awaitable, Callable
from typing import Any

def ping() -> str: ...

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

class ExecuteStreamedIter:
    def __aiter__(self) -> ExecuteStreamedIter: ...
    async def __anext__(self) -> ResultSet | _Heartbeat: ...

class FetchallArrowStreamedIter:
    def __aiter__(self) -> FetchallArrowStreamedIter: ...
    async def __anext__(self) -> Any: ...  # Arrow table (__arrow_c_stream__) | _Heartbeat

class Client:
    def __init__(
        self,
        host: str,
        warehouse_id: str,
        token: str | None = None,
        token_provider: Callable[[], str | Awaitable[str]] | None = None,
        chunk_fetch_concurrency: int = 32,
    ) -> None: ...
    async def execute_arrow(
        self, statement: str, catalog: str | None = None, schema: str | None = None
    ) -> Any: ...  # Arrow table (__arrow_c_stream__)
    async def execute(self, statement: str, catalog: str | None = None, schema: str | None = None) -> ResultSet: ...
    def execute_streamed(
        self,
        statement: str,
        catalog: str | None = None,
        schema: str | None = None,
        total_timeout_s: float | None = None,
    ) -> ExecuteStreamedIter: ...
    async def execute_json(
        self, statement: str, catalog: str | None = None, schema: str | None = None
    ) -> list[list[Any]]: ...
    async def upload_volume_file(self, volume_path: str, data: bytes) -> None: ...
    async def delete_volume_file(self, volume_path: str) -> None: ...
