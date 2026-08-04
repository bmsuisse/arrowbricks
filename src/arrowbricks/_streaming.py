"""Heartbeats, plus the two functions that still need a raw Arrow-C-Data-
Interface-compatible object at the Python boundary (`write_ipc_stream`,
`stream_query_json`) -- both delegate to `._core` (this package's compiled
Rust/PyO3 submodule) for the actual Arrow/NDJSON work, so nothing here needs
arro3 or any other Arrow library installed. `stream_query_json` also
delegates its submit/poll/fetch/reorder/decode to `._core` through
`DatabricksClient._core_client`, via `._core.Client.stream_ndjson_lines` --
which returns already-formatted NDJSON lines per chunk, so there's no
Python-side Arrow-to-JSON conversion step at all.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable
from typing import Any, BinaryIO, TypeVar, cast

from . import _core
from .client import DatabricksClient

__all__ = [
    "HEARTBEAT",
    "QueryTimeout",
    "ReplayableArrowChunk",
    "await_with_heartbeat",
    "stream_query_json",
    "write_ipc_stream",
]

# How often a caller waiting on a slow Databricks round-trip (warehouse cold
# start, a long-running statement) gets a HEARTBEAT -- pick something well
# under whatever idle-connection ceiling sits between your server and its
# client (e.g. many PaaS load balancers cut an idle SSE connection around
# ~230s) if you're forwarding these as keep-alive pings.
_HEARTBEAT_INTERVAL_S = 15.0


class QueryTimeout(RuntimeError):
    """Raised when a query exceeds its `total_timeout_s`."""


class _Heartbeat:
    __slots__ = ()

    def __repr__(self) -> str:
        return "HEARTBEAT"


HEARTBEAT = _Heartbeat()

T = TypeVar("T")


def write_ipc_stream(stream: Any, buf: BinaryIO) -> None:
    """Writes `stream` (anything implementing `__arrow_c_stream__`, e.g. a
    Table/RecordBatchReader from this package, arro3, or pyarrow) as
    Arrow-IPC stream bytes -- always uncompressed. A compressed body
    (arro3's own default is `compression="LZ4"`) is transparently
    decompressed by some Arrow readers (e.g. DuckDB's) but not all --
    `duckdb-wasm`'s browser-side decoder silently fails to parse it. Plain,
    uncompressed bodies are the safe default for bytes that might end up
    read by anything."""
    _core.write_ipc_stream(stream, buf)


class ReplayableArrowChunk:
    """Wraps one Arrow IPC-stream byte chunk so it can be handed to something
    that calls `__arrow_c_stream__` more than once per relation (a schema
    peek, then the actual scan -- DuckDB's registration path does this, for
    one). A plain parsed stream is single-use and raises on the second call,
    so this re-parses from the cached bytes every call instead. The bytes are
    already fully in memory, so re-parsing costs a cheap second pass, not a
    second network fetch. No extra dependency needed -- `._core.read_ipc_stream`
    handles the actual parse."""

    __slots__ = ("_data", "chunk_index", "declared_row_count")

    def __init__(self, data: bytes, chunk_index: int, declared_row_count: int | None = None) -> None:
        self._data = data
        self.chunk_index = chunk_index
        self.declared_row_count = declared_row_count

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return _core.read_ipc_stream(self._data).__arrow_c_stream__(requested_schema)

    def nbytes(self) -> int:
        return len(self._data)

    def to_table(self) -> Any:
        """Parses this chunk's bytes into an Arrow table (implements
        `__arrow_c_stream__` -- arro3/pyarrow/DuckDB-compatible)."""
        return _core.read_ipc_stream(self._data)


async def await_with_heartbeat(
    aw: Awaitable[T], *, interval_s: float = _HEARTBEAT_INTERVAL_S, total_timeout_s: float | None = None
) -> AsyncIterator[Any]:
    """Wraps a single slow awaitable with periodic HEARTBEAT yields, so a
    caller streaming this over e.g. SSE never goes silent for the whole wait.
    Yields HEARTBEAT zero or more times, then yields the awaitable's real
    result exactly once. Re-raises whatever `aw` raised, or QueryTimeout if
    `total_timeout_s` elapses first."""
    task: asyncio.Task[T] = asyncio.ensure_future(aw)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + total_timeout_s if total_timeout_s is not None else None
    try:
        while not task.done():
            wait_for = interval_s if deadline is None else min(interval_s, max(deadline - loop.time(), 0.0))
            done, _ = await asyncio.wait({task}, timeout=wait_for)
            if not done:
                if deadline is not None and loop.time() >= deadline:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                    raise QueryTimeout(f"Query exceeded {total_timeout_s}s timeout")
                yield HEARTBEAT
        yield await task
    finally:
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


def windowed_sql(sql: str, *, row_limit: int | None, offset: int | None) -> str:
    """Pushes LIMIT/OFFSET into the SQL submitted to Databricks -- a query
    should never fetch more rows from the warehouse than the caller wants."""
    if row_limit is None and not offset:
        return sql
    if row_limit is None:
        return f"SELECT * FROM ({sql}) _q OFFSET {offset}"  # noqa: S608
    if offset:
        return f"SELECT * FROM ({sql}) _q LIMIT {row_limit} OFFSET {offset}"  # noqa: S608
    return f"SELECT * FROM ({sql}) _q LIMIT {row_limit}"  # noqa: S608


async def stream_query_json(
    client: DatabricksClient,
    sql: str,
    *,
    params: list[dict[str, Any]] | None = None,
    row_limit: int | None = None,
    offset: int | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    total_timeout_s: float | None = None,
    non_finite_floats: str = "null",
) -> AsyncIterator[Any]:
    """Yields HEARTBEAT while waiting on Databricks (and between slow chunks
    mid-download), then each result row as a ready-to-send JSON string --
    one per SSE frame, say.

    Unlike cursor.py's fetch methods, this registers and emits each Databricks
    chunk AS IT ARRIVES rather than buffering the full result first, via
    `._core.Client.stream_ndjson_lines` -- the first row reaches the caller
    after ~one chunk's fetch/decode/reorder/encode time, not the whole
    statement's; chunk order is restored there too (chunks can complete out
    of order over the network). Decode and NDJSON encoding both happen in
    Rust -- this function only forwards already-formatted lines.

    `non_finite_floats` -- `"null"` (default) or `"string"`. A NaN/Infinity/
    -Infinity float column comes back from Databricks like any other value,
    but JSON itself has no literal for it; the underlying JSON writer's own
    fixed behavior is to emit `null` -- valid JSON, but indistinguishable
    from a real SQL NULL in the same column once it's out (found by testing
    against a live warehouse: a NaN and a NULL in the same float column both
    came back as `null`). `"string"` recovers the distinction by emitting
    the JSON strings `"NaN"`/`"Infinity"`/`"-Infinity"` for those specific
    cells instead -- still valid JSON, but the caller must special-case
    those three string values if it needs the actual float back. Only
    top-level float columns are covered; one nested inside a STRUCT/ARRAY/MAP
    still comes back as `null` either way.

    Note this yields a whole chunk's rows at once -- Databricks' own chunk
    sizing already bounds how much that is."""
    if non_finite_floats not in ("null", "string"):
        raise ValueError(f"non_finite_floats must be 'null' or 'string', got {non_finite_floats!r}")
    sql = windowed_sql(sql, row_limit=row_limit, offset=offset)
    core_client = client._core_client  # noqa: SLF001 -- same package, see client.py

    async for item in core_client.stream_ndjson_lines(
        sql,
        catalog=catalog,
        schema=schema,
        parameters=params,
        total_timeout_s=total_timeout_s,
        non_finite_as_string=(non_finite_floats == "string"),
    ):
        if item is _core.HEARTBEAT:
            yield HEARTBEAT
            continue
        for line in cast("list[str]", item):
            yield line
