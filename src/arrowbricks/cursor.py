"""A Cursor/Connection surface shaped like databricks-sql-python's (execute,
fetchone/fetchmany/fetchall, fetchall_arrow/fetchmany_arrow) -- familiar DB-API
ergonomics on top of the Statement Execution API + arro3, for callers who want
to pull rows/Arrow incrementally rather than get everything back at once.

Unlike a real DB-API cursor, this is async throughout (`execute`, `fetchone`,
etc. are all coroutines) since the underlying client is -- there's no
synchronous escape hatch, matching every other function in this package.

Chunks are fetched from Databricks lazily, only as fetchone/fetchmany/fetchall
actually need more rows -- a `fetchmany(100)` loop over a 700k-row result
never pulls more chunks than it's consumed. Chunks can arrive out of order
over the network (see client.py); a `pending` buffer holds early arrivals
until the next expected chunk_index shows up, same reasoning as
_streaming.py's stream_query_json."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import arro3.core as core

from ._streaming import (
    HEARTBEAT,
    ReplayableArrowChunk,
    await_with_heartbeat,
    fetch_arrow_chunks_with_manifest,
    windowed_sql,
)
from .client import DatabricksClient

__all__ = ["Connection", "Cursor", "connect"]

Row = tuple[Any, ...]
Description = tuple[str, str | None, None, None, None, None, None]


def _empty_table(schema: core.Schema | None) -> core.Table:
    return core.Table.from_batches([], schema=schema) if schema is not None else core.Table.from_pydict({})


def _description_from_manifest(manifest: dict[str, Any]) -> list[Description]:
    columns = manifest.get("schema", {}).get("columns") or []
    return [(c.get("name"), c.get("type_name"), None, None, None, None, None) for c in columns]


class _ResultSet:
    """Buffers already-fetched-but-not-yet-returned rows at the Arrow level
    (an arro3 Table), so fetchall_arrow/fetchmany_arrow stay zero-copy and
    fetchone/fetchmany/fetchall just materialize whatever slice they need.
    Not part of the public API -- reached only via Cursor."""

    def __init__(self, schema: core.Schema | None, chunk_aiter: AsyncIterator[ReplayableArrowChunk]) -> None:
        self.schema = schema
        self._chunk_aiter = chunk_aiter
        self._pending: dict[int, ReplayableArrowChunk] = {}
        self._next_idx = 0
        self._exhausted = False
        self._buffer: core.Table | None = None
        self.rownumber = 0

    async def _pull_one_chunk_table(self) -> core.Table | None:
        """Returns the next expected chunk's Table, in order -- checking
        `_pending` FIRST, since a single earlier call can have pulled several
        chunks off `_chunk_aiter` before the one it actually needed showed up
        (arrival order is completion order, not chunk_index order), leaving
        the rest already-fetched-and-buffered here. Only touches the network
        (`_chunk_aiter.__anext__()`) once `_pending` has nothing more to give."""
        while True:
            if self._next_idx in self._pending:
                ready = self._pending.pop(self._next_idx)
                self._next_idx += 1
                return ready.to_table()
            if self._exhausted:
                return None
            try:
                chunk = await self._chunk_aiter.__anext__()
            except StopAsyncIteration:
                self._exhausted = True
                continue
            self._pending[chunk.chunk_index] = chunk

    async def _ensure_buffer(self, want: int) -> None:
        while (self._buffer is None or self._buffer.num_rows < want) and not self._exhausted:
            table = await self._pull_one_chunk_table()
            if table is None:
                break
            if self.schema is None:
                self.schema = table.schema
            self._buffer = (
                table
                if self._buffer is None
                else core.Table.from_batches(self._buffer.to_batches() + table.to_batches(), schema=self._buffer.schema)
            )

    async def fetchmany_arrow(self, size: int) -> core.Table:
        await self._ensure_buffer(size)
        if self._buffer is None or self._buffer.num_rows == 0:
            return _empty_table(self.schema)
        n = min(size, self._buffer.num_rows)
        out = self._buffer.slice(0, n)
        rest = self._buffer.slice(n)
        self._buffer = rest if rest.num_rows else None
        self.rownumber += n
        return out

    async def fetchall_arrow(self) -> core.Table:
        while not self._exhausted:
            await self._ensure_buffer((self._buffer.num_rows if self._buffer is not None else 0) + 1)
        out = self._buffer if self._buffer is not None else _empty_table(self.schema)
        self._buffer = None
        self.rownumber += out.num_rows
        return out

    @staticmethod
    def _table_to_rows(table: core.Table) -> list[Row]:
        if table.num_rows == 0:
            return []
        columns = [table.column(i).combine_chunks().to_pylist() for i in range(table.num_columns)]
        return list(zip(*columns, strict=True))

    async def fetchone(self) -> Row | None:
        rows = self._table_to_rows(await self.fetchmany_arrow(1))
        return rows[0] if rows else None

    async def fetchmany(self, size: int) -> list[Row]:
        return self._table_to_rows(await self.fetchmany_arrow(size))

    async def fetchall(self) -> list[Row]:
        return self._table_to_rows(await self.fetchall_arrow())


class Cursor:
    """One statement's worth of state: `execute()` (or `execute_streamed()`)
    submits and waits for it, then fetchone/fetchmany/fetchall/fetchall_arrow/
    fetchmany_arrow pull its result. Reusable across statements -- each
    `execute()` call replaces the previous result set."""

    def __init__(self, client: DatabricksClient) -> None:
        self._client = client
        self._result: _ResultSet | None = None
        self.description: list[Description] | None = None

    def execute_streamed(
        self,
        sql: str,
        parameters: list[dict[str, Any]] | None = None,
        *,
        row_limit: int | None = None,
        offset: int | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        total_timeout_s: float | None = None,
    ) -> AsyncIterator[Any]:
        """Like execute(), but yields HEARTBEAT while waiting on Databricks
        instead of blocking silently -- for bridging e.g. an SSE connection
        during a possible multi-minute cold warehouse start. Yields HEARTBEAT
        zero or more times, then this same Cursor once ready to fetch."""
        sql = windowed_sql(sql, row_limit=row_limit, offset=offset)

        async def _gen() -> AsyncIterator[Any]:
            fetch = fetch_arrow_chunks_with_manifest(
                self._client, sql, catalog=catalog, schema=schema, parameters=parameters
            )
            async for item in await_with_heartbeat(fetch, total_timeout_s=total_timeout_s):
                if item is HEARTBEAT:
                    yield HEARTBEAT
                    continue
                _statement_id, manifest, chunk_iter = item
                self.description = _description_from_manifest(manifest)
                self._result = _ResultSet(schema=None, chunk_aiter=chunk_iter.__aiter__())
                yield self

        return _gen()

    async def execute(
        self,
        sql: str,
        parameters: list[dict[str, Any]] | None = None,
        *,
        row_limit: int | None = None,
        offset: int | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        total_timeout_s: float | None = None,
    ) -> Cursor:
        """Submits `sql` and waits for it to complete -- a plain blocking
        await, like a real DB-API cursor's execute(). `parameters`, if given,
        is Databricks' own named-parameter format --
        [{"name": ..., "value": ..., "type": ...}] bound against `:name`
        markers in `sql`, not DB-API's `?`/`%s` placeholder style."""
        async for _ in self.execute_streamed(
            sql,
            parameters,
            row_limit=row_limit,
            offset=offset,
            catalog=catalog,
            schema=schema,
            total_timeout_s=total_timeout_s,
        ):
            pass
        return self

    def _require_result(self) -> _ResultSet:
        if self._result is None:
            raise RuntimeError("no active result set -- call execute() first")
        return self._result

    async def fetchone(self) -> Row | None:
        return await self._require_result().fetchone()

    async def fetchmany(self, size: int) -> list[Row]:
        return await self._require_result().fetchmany(size)

    async def fetchall(self) -> list[Row]:
        return await self._require_result().fetchall()

    async def fetchmany_arrow(self, size: int) -> core.Table:
        return await self._require_result().fetchmany_arrow(size)

    async def fetchall_arrow(self) -> core.Table:
        return await self._require_result().fetchall_arrow()

    def __aiter__(self) -> Cursor:
        return self

    async def __anext__(self) -> Row:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row


class Connection:
    """One Databricks SQL warehouse endpoint -- see DatabricksClient for the
    constructor args (auth, timeouts, chunk_fetch_concurrency). Statements are
    independent REST calls with no server-side session state, so `cursor()`
    can be called as many times as you like; `close()` is a no-op kept for
    context-manager parity with a real DB-API connection."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.client = DatabricksClient(*args, **kwargs)

    def cursor(self) -> Cursor:
        return Cursor(self.client)

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()


def connect(*args: Any, **kwargs: Any) -> Connection:
    """Same signature as DatabricksClient -- `connect(host, warehouse_id,
    token=..., ...)`."""
    return Connection(*args, **kwargs)
