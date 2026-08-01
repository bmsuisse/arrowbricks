"""Chunk fetching, heartbeats, and Arrow (de)serialization -- everything that
turns a DatabricksClient's raw chunk bytes into Arrow, via arro3 directly.
Single Arrow engine, no pluggable backend: arrowbricks' whole reason to exist
is "Databricks to Arrow via arro3", so there's nothing to make pluggable.

`write_ipc_stream` always passes `compression=None` -- arro3's own default
(`compression="LZ4"`) body-compresses every record batch, which DuckDB's own
Arrow C Data Interface reader decompresses transparently but which other
Arrow IPC readers may not support at all (observed: duckdb-wasm's browser-side
decoder silently fails to parse it). Plain, uncompressed bodies are the safe
default for a library whose bytes might end up read by anything."""

from __future__ import annotations

import asyncio
import contextlib
import io
from collections.abc import AsyncIterator, Awaitable
from typing import Any, BinaryIO, TypeVar

import arro3.core as core
import arro3.io as aio

from .client import DatabricksClient

__all__ = [
    "HEARTBEAT",
    "QueryTimeout",
    "ReplayableArrowChunk",
    "await_with_heartbeat",
    "fetch_arrow_chunks_for_statement",
    "fetch_arrow_chunks_with_manifest",
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
    """Writes `stream` (anything implementing `__arrow_c_stream__`, e.g. an
    arro3 Table/RecordBatchReader or a ReplayableArrowChunk) as Arrow-IPC
    stream bytes -- always uncompressed, see module docstring."""
    aio.write_ipc_stream(stream, buf, compression=None)


class ReplayableArrowChunk:
    """Wraps one Arrow IPC-stream byte chunk so it can be handed to something
    that calls `__arrow_c_stream__` more than once per relation (a schema
    peek, then the actual scan -- DuckDB's registration path does this, for
    one). A plain parsed stream is single-use and raises on the second call,
    so this re-parses from the cached bytes every call instead. The bytes are
    already fully in memory (just downloaded), so re-parsing costs a cheap
    second pass, not a second network fetch."""

    __slots__ = ("_data", "chunk_index", "declared_row_count")

    def __init__(self, data: bytes, chunk_index: int, declared_row_count: int | None = None) -> None:
        self._data = data
        self.chunk_index = chunk_index
        self.declared_row_count = declared_row_count

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        reader = aio.read_ipc_stream(io.BytesIO(self._data))
        return reader.__arrow_c_stream__(requested_schema)

    def nbytes(self) -> int:
        return len(self._data)

    def to_table(self) -> core.Table:
        """Parses this chunk's bytes into an arro3 Table -- the entry point
        `cursor.py`'s result-set buffering uses to work with chunks at the
        Arrow level (concat/slice) rather than re-parsing bytes per access."""
        return core.Table.from_arrow(self)


async def fetch_arrow_chunks_with_manifest(
    client: DatabricksClient,
    sql: str,
    *,
    catalog: str | None = None,
    schema: str | None = None,
    parameters: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, Any], AsyncIterator[ReplayableArrowChunk]]:
    """Submits `sql` and returns (statement_id, manifest, chunk_iterator).
    total_row_count/total_chunk_count are already in the manifest once the
    statement succeeds, so callers needing a progress-bar target don't need a
    separate preflight COUNT(*)."""
    statement_id, manifest = await client.execute_arrow_statement(
        sql, catalog=catalog, schema=schema, parameters=parameters
    )
    chunk_metas = manifest.get("chunks") or []
    return statement_id, manifest, fetch_arrow_chunks_for_statement(client, statement_id, chunk_metas)


async def fetch_arrow_chunks_for_statement(
    client: DatabricksClient, statement_id: str, chunk_metas: list[dict[str, Any]]
) -> AsyncIterator[ReplayableArrowChunk]:
    async for chunk_bytes, row_count, chunk_index in client.stream_chunks_by_index(statement_id, chunk_metas):
        if chunk_bytes:
            yield ReplayableArrowChunk(chunk_bytes, chunk_index, declared_row_count=row_count)


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


async def heartbeat_over_stream(
    aiter: AsyncIterator[T], *, interval_s: float = _HEARTBEAT_INTERVAL_S, total_timeout_s: float | None = None
) -> AsyncIterator[Any]:
    """Like await_with_heartbeat, but for a stream of items rather than one
    final result: yields HEARTBEAT whenever the wait for the *next* item
    exceeds interval_s, and otherwise passes each item through as it arrives.
    Used by stream_query_json (and cursor.py's execute_streamed) so a slow
    chunk mid-stream -- not just a cold warehouse start before the first one
    -- never lets a downstream SSE connection go silent for the whole wait."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + total_timeout_s if total_timeout_s is not None else None
    it = aiter.__aiter__()
    task: asyncio.Task[Any] | None = None
    try:
        while True:
            if task is None:
                task = asyncio.ensure_future(it.__anext__())
            wait_for = interval_s if deadline is None else min(interval_s, max(deadline - loop.time(), 0.0))
            done, _ = await asyncio.wait({task}, timeout=wait_for)
            if not done:
                if deadline is not None and loop.time() >= deadline:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                    raise QueryTimeout(f"Query exceeded {total_timeout_s}s timeout")
                yield HEARTBEAT
                continue
            try:
                yield task.result()
            except StopAsyncIteration:
                return
            finally:
                task = None
    finally:
        if task is not None and not task.done():
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


def _write_ndjson(chunk: ReplayableArrowChunk) -> bytes:
    buf = io.BytesIO()
    # explicit_nulls=True: arro3 omits null-valued keys by default -- without
    # this a row's JSON shape would vary by which columns happen to be null
    # in it.
    aio.write_ndjson(chunk, buf, explicit_nulls=True)
    return buf.getvalue()


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
) -> AsyncIterator[Any]:
    """Yields HEARTBEAT while waiting on Databricks, then each result row as a
    ready-to-send JSON string (arro3's native `write_ndjson`) -- one per SSE
    frame, say.

    Unlike cursor.py's fetch methods, this registers and emits each Databricks
    chunk AS IT ARRIVES rather than buffering the full result first -- the
    first row reaches the caller after ~one chunk's fetch time, not the whole
    statement's, and at most a handful of chunks' bytes (bounded by the
    client's own chunk_fetch_concurrency) are ever held in memory at once, not
    O(whole result). Chunks can arrive out of order, so out-of-order arrivals
    sit in a small `pending` buffer until the next expected chunk_index shows
    up -- that buffer stays bounded by concurrency, it never grows to the full
    result. `pending` holds a *list* per index, not a single chunk, since a
    chunk_index can have more than one blob (DatabricksClient._fetch_chunk_index
    gathers over possibly-multiple `external_links` per chunk); once the
    source is exhausted, any index that never showed up at all is skipped
    rather than stranding everything buffered past it forever.

    Note this yields a whole chunk's rows at once (write_ndjson has no
    incremental/row-at-a-time mode) -- Databricks' own chunk sizing already
    bounds how much that is, so the overall memory-bounded-across-chunks
    guarantee above still holds, just at chunk granularity."""
    sql = windowed_sql(sql, row_limit=row_limit, offset=offset)
    _statement_id, _manifest, chunk_iter = await fetch_arrow_chunks_with_manifest(
        client, sql, catalog=catalog, schema=schema, parameters=params
    )

    pending: dict[int, list[ReplayableArrowChunk]] = {}
    next_idx = 0
    loop = asyncio.get_running_loop()

    async def _emit_lines(chunk: ReplayableArrowChunk) -> AsyncIterator[str]:
        blob = await loop.run_in_executor(None, _write_ndjson, chunk)
        for line in blob.splitlines():
            yield line.decode()

    async for item in heartbeat_over_stream(chunk_iter, total_timeout_s=total_timeout_s):
        if item is HEARTBEAT:
            yield HEARTBEAT
            continue
        pending.setdefault(item.chunk_index, []).append(item)
        while next_idx in pending:
            chunk = pending[next_idx].pop(0)
            if not pending[next_idx]:
                del pending[next_idx]
                next_idx += 1
            async for line in _emit_lines(chunk):
                yield line

    # A genuine gap (an index that never arrived) must not strand chunks
    # buffered past it -- drain whatever's left, in ascending index order.
    for idx in sorted(pending):
        for chunk in pending[idx]:
            async for line in _emit_lines(chunk):
                yield line
