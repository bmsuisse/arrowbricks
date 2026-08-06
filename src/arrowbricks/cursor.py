"""A Cursor/Connection surface shaped like databricks-sql-python's (execute,
fetchone/fetchmany/fetchall, fetchall_arrow/fetchmany_arrow) -- familiar DB-API
ergonomics on top of the Statement Execution API. The actual submit/poll/
fetch/reorder/decode work is delegated to `arrowbricks_core` (Rust/PyO3) via
`DatabricksClient._core_client` -- this module just adapts that result to the
same public shape as before the cutover (`Cursor.fetchone`/`fetchmany`/
`fetchall`, `Cursor.description`, etc.), so existing callers see no change.

Unlike a real DB-API cursor, this is async throughout (`execute`, `fetchone`,
etc. are all coroutines) since the underlying client is -- there's no
synchronous escape hatch, matching every other function in this package."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from ._streaming import HEARTBEAT, await_with_heartbeat, windowed_sql
from .client import DatabricksClient

if TYPE_CHECKING:
    # Type-only: the objects returned by `._core` (this package's compiled
    # Rust submodule) are already arro3-compatible Tables at runtime (no
    # import needed for that) -- arro3-core is an *optional* runtime
    # dependency (`pip install arrowbricks[arro3]`), needed only by
    # `_table_to_rows` below, so this import must never execute.
    import arro3.core as core

__all__ = ["Connection", "Cursor", "connect"]

Row = tuple[Any, ...]
Description = tuple[str, str | None, None, None, None, None, None]


def _table_to_rows(table: Any) -> list[Row]:
    if table.num_rows == 0:
        return []
    try:
        columns = [table.column(i).combine_chunks().to_pylist() for i in range(table.num_columns)]
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "fetchone/fetchmany/fetchall need arro3-core installed -- "
            "`pip install arrowbricks[arro3]`, or use fetchall_arrow/fetchmany_arrow instead, "
            "which don't need it"
        ) from exc
    return list(zip(*columns, strict=True))


class Cursor:
    """One statement's worth of state: `execute()` (or `execute_streamed()`)
    submits and waits for it, then fetchone/fetchmany/fetchall/fetchall_arrow/
    fetchmany_arrow pull its result. Reusable across statements -- each
    `execute()` call replaces the previous result set."""

    def __init__(self, client: DatabricksClient) -> None:
        self._client = client
        self._result: Any = None  # arrowbricks_core.ResultSet once execute() has run
        self._manifest_description: list[Description] | None = None
        # Prefers the *actual* Arrow schema off the first fetched chunk once
        # one has been pulled -- the manifest's own columns (used for
        # `_manifest_description` above) is only a pre-fetch estimate.
        # (name, type_name) pairs from `._core.ResultSet.schema()` -- not a
        # `core.Table`'s own `.schema` property, which specifically requires
        # a real arro3 install to construct its return value (see
        # cursor.py's `fetchmany_arrow`/`fetchall_arrow`).
        self._schema: list[tuple[str, str]] | None = None

    @property
    def description(self) -> list[Description] | None:
        if self._schema is not None:
            return [(name, type_name, None, None, None, None, None) for name, type_name in self._schema]
        return self._manifest_description

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
        prefer_inline: bool = False,
    ) -> AsyncIterator[Any]:
        """Like execute(), but yields HEARTBEAT while waiting on Databricks
        instead of blocking silently -- for bridging e.g. an SSE connection
        during a possible multi-minute cold warehouse start. Yields HEARTBEAT
        zero or more times, then this same Cursor once ready to fetch.

        `prefer_inline` -- if you expect this query's result to be small
        (a handful of rows, well under Databricks' own 25MiB INLINE result
        cap), setting this tries fetching it inline in the same round trip
        as the statement submission itself, skipping the separate chunk-fetch
        entirely. If the result turns out too big, or has a column type this
        can't convert inline (nested ARRAY/MAP/STRUCT, VARIANT), it
        transparently re-runs the query the normal way instead -- meaning a
        caller who sets this without actually expecting a small result pays
        for the query twice. Leave this off unless you know the result is
        small; it changes latency, not correctness, either way."""
        sql = windowed_sql(sql, row_limit=row_limit, offset=offset)

        async def _gen() -> AsyncIterator[Any]:
            # Cleared up front, not just on success -- found in code review
            # that a failed submit (statement FAILED/CANCELED, or
            # QueryTimeout) left these pointing at the *previous* statement's
            # result set with no error of its own, so a caller doing
            # `try: await cur.execute(sql) / except: ...` and later reading
            # the cursor anyway would silently get a different query's rows
            # and description. Clearing here means a failed execute() leaves
            # the cursor in the same "no active result set" state execute()
            # was never called at all -- fetchone/fetchmany/fetchall/
            # description below already raise/return None for that.
            self._result = None
            self._schema = None
            self._manifest_description = None
            core_client = self._client._core_client  # noqa: SLF001 -- same package, see client.py
            submit = core_client.execute(
                sql, catalog=catalog, schema=schema, parameters=parameters, prefer_inline=prefer_inline
            )
            async for item in await_with_heartbeat(submit, total_timeout_s=total_timeout_s):
                if item is HEARTBEAT:
                    yield HEARTBEAT
                    continue
                self._result = item
                self._schema = None
                self._manifest_description = [
                    (name, type_name, None, None, None, None, None) for name, type_name in item.columns
                ]
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
        prefer_inline: bool = False,
    ) -> Cursor:
        """Submits `sql` and waits for it to complete -- a plain blocking
        await, like a real DB-API cursor's execute(). `parameters`, if given,
        is Databricks' own named-parameter format --
        [{"name": ..., "value": ..., "type": ...}] bound against `:name`
        markers in `sql`, not DB-API's `?`/`%s` placeholder style. See
        `execute_streamed`'s own docstring for what `prefer_inline` does."""
        async for _ in self.execute_streamed(
            sql,
            parameters,
            row_limit=row_limit,
            offset=offset,
            catalog=catalog,
            schema=schema,
            total_timeout_s=total_timeout_s,
            prefer_inline=prefer_inline,
        ):
            pass
        return self

    def _require_result(self) -> Any:
        if self._result is None:
            raise RuntimeError("no active result set -- call execute() first")
        return self._result

    async def fetchone(self) -> Row | None:
        rows = _table_to_rows(await self.fetchmany_arrow(1))
        return rows[0] if rows else None

    async def fetchmany(self, size: int) -> list[Row]:
        return _table_to_rows(await self.fetchmany_arrow(size))

    async def fetchall(self) -> list[Row]:
        return _table_to_rows(await self.fetchall_arrow())

    async def fetchmany_arrow(self, size: int) -> core.Table:
        result = self._require_result()
        table = await result.fetchmany_arrow(size)
        if self._schema is None:
            self._schema = await result.schema()
        return table

    async def fetchall_arrow(self) -> core.Table:
        result = self._require_result()
        table = await result.fetchall_arrow()
        if self._schema is None:
            self._schema = await result.schema()
        return table

    def fetchall_streamed(self, *, total_timeout_s: float | None = None) -> AsyncIterator[Any]:
        """Like fetchall(), but yields HEARTBEAT while pulling chunks instead
        of blocking silently -- for a caller bridging e.g. an SSE connection
        through the full download, not just the initial `execute_streamed`
        wait for the statement to complete. Downloading many chunks for a
        large result can itself take a while; `execute_streamed`'s own
        heartbeats stop the moment the statement is ready, before any chunk
        has actually been fetched. Yields HEARTBEAT zero or more times, then
        the final `list[Row]`."""

        async def _gen() -> AsyncIterator[Any]:
            async for item in await_with_heartbeat(self.fetchall(), total_timeout_s=total_timeout_s):
                yield item

        return _gen()

    def fetchall_arrow_streamed(self, *, total_timeout_s: float | None = None) -> AsyncIterator[Any]:
        """Arrow-`Table` counterpart to fetchall_streamed -- see its docstring."""

        async def _gen() -> AsyncIterator[Any]:
            async for item in await_with_heartbeat(self.fetchall_arrow(), total_timeout_s=total_timeout_s):
                yield item

        return _gen()

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
