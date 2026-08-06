"""Thin Python facade over `._core.Client` (this package's compiled Rust/PyO3
submodule, see rust/arrowbricks_core) -- `DatabricksClient` validates
constructor args (auth, timeouts) the same way it always has, then delegates
every actual network call to the Rust core. `cursor.py`/`_streaming.py` use
`._core_client` directly for the hot path (bounded-concurrency chunk fetch,
reorder, Arrow/JSON/NDJSON decode)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from . import _core

TokenProvider = Callable[[], "str | Awaitable[str]"]


class DatabricksClient:
    """One Databricks SQL warehouse endpoint. Auth is entirely bring-your-own:
    pass a static `token`, or a `token_provider` callable (sync or async) that
    returns one -- this client has no opinion on *how* you get a token (a
    personal access token, an OAuth M2M flow, a cloud-provider credential
    chain) and no cloud-SDK dependency baked in. If your provider is expensive
    to call (e.g. shells out to a CLI), cache/refresh inside it -- this client
    calls it on every request and does no caching of its own."""

    def __init__(
        self,
        host: str,
        warehouse_id: str,
        *,
        token: str | None = None,
        token_provider: TokenProvider | None = None,
        http_timeout: float = 60.0,
        wait_timeout: str = "30s",
        chunk_fetch_concurrency: int = 64,
        warehouse_start_timeout: float = 300.0,
        warehouse_confirmed_running_ttl_s: float = 30.0,
        compress_results: bool = True,
        protocol: str = "sea",
    ) -> None:
        if not token and not token_provider:
            raise ValueError("DatabricksClient needs either `token` or `token_provider`")
        if token and token_provider:
            raise ValueError("DatabricksClient needs exactly one of `token` or `token_provider`, not both")
        # All of these (and warehouse_id) are only ever needed by the Rust
        # core's own Client below, which holds the authoritative copies --
        # nothing here reads them back, so they aren't kept on self too.
        # chunk_fetch_concurrency: each concurrent fetch slot holds a whole
        # chunk's raw bytes in memory. 64 -- tuned for the Rust core's real
        # OS-thread parallelism, which scales well past what asyncio+GIL
        # concurrency used to buy; measured against a real 400-chunk/
        # 5.6M-row table (see client.rs's own comment for the numbers).
        # compress_results: requests LZ4-compressed cloud-fetch chunks
        # (matches databricks-sql-connector's own
        # enable_query_result_lz4_compression default) -- measured ~2x
        # faster chunk-fetch time against a real 120-column/100k-row table,
        # since network transfer, not local Arrow-IPC decode, is the
        # bottleneck for a result that size. Set False if your link to the
        # warehouse is fast/low-latency enough that decompression CPU time
        # stops paying for itself.
        # protocol: "sea" (default, unchanged) submits statements via the
        # REST Statement Execution API. "thrift" instead speaks the same
        # HiveServer2-compatible Thrift-over-HTTPS protocol
        # databricks-sql-connector uses by *default* (when its own
        # `use_sea` isn't set) -- measurably faster for small queries,
        # since its ExecuteStatement RPC can return a small result inline
        # in the same call that submits the statement (`getDirectResults`),
        # where SEA always needs at least one further poll/fetch round
        # trip. `prefer_inline` has no effect under `protocol="thrift"`
        # (silently ignored, not an error) -- Thrift has no
        # INLINE-disposition equivalent, and doesn't need one.
        if protocol not in ("sea", "thrift"):
            raise ValueError(f'DatabricksClient protocol must be "sea" or "thrift", got {protocol!r}')
        self._core_client = _core.Client(
            host,
            warehouse_id,
            token=token,
            token_provider=token_provider,
            chunk_fetch_concurrency=chunk_fetch_concurrency,
            http_timeout=http_timeout,
            wait_timeout=wait_timeout,
            warehouse_start_timeout=warehouse_start_timeout,
            warehouse_confirmed_running_ttl_s=warehouse_confirmed_running_ttl_s,
            compress_results=compress_results,
            protocol=protocol,
        )

    async def aclose(self) -> None:
        """Closes every currently-idle pooled session this client opened
        (see `execute`'s own use of one per (catalog, schema) pair) -- both
        the SEA session pool and, if `protocol="thrift"` was used, the
        Thrift session pool; best-effort, never raises; anything left open
        just falls to Databricks' own server-side session TTL instead. HTTP
        connections themselves still need no explicit close -- the Rust
        core's connection pool is cleaned up on garbage collection."""
        await self._core_client.close_sessions()

    async def __aenter__(self) -> DatabricksClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def upload_volume_file(self, volume_path: str, data: bytes) -> None:
        """Uploads `data` to a Unity Catalog volume path via the Files API,
        overwriting anything already there. `volume_path` is caller-supplied
        in full (e.g. `/Volumes/my_catalog/my_schema/my_volume/some/file.parquet`)
        -- this package has no knowledge of any specific catalog/schema/volume.
        Raises `RuntimeError` (message only) on failure."""
        await self._core_client.upload_volume_file(volume_path, data)

    async def delete_volume_file(self, volume_path: str) -> None:
        """Deletes a file at `volume_path` (see upload_volume_file). A 404 is
        treated as success -- the file is already gone, which is fine for
        idempotent staging cleanup. Raises `RuntimeError` (message only) on
        any other failure."""
        await self._core_client.delete_volume_file(volume_path)

    def stream_query_json(
        self,
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
        """Convenience method form of the free function `stream_query_json(client,
        sql, ...)` -- `client.stream_query_json(sql, ...)` reads more naturally at
        a call site than passing `client` as the first positional argument of a
        module-level function. Same behavior, same arguments minus `client`
        itself; see `arrowbricks.stream_query_json`'s own docstring for what each
        one does. The free function still exists and isn't going away -- this
        just delegates to it (a lazy import here avoids a circular import
        between this module and `_streaming.py`, which imports `DatabricksClient`
        from here for its own type hint)."""
        from ._streaming import stream_query_json as _stream_query_json

        return _stream_query_json(
            self,
            sql,
            params=params,
            row_limit=row_limit,
            offset=offset,
            catalog=catalog,
            schema=schema,
            total_timeout_s=total_timeout_s,
            non_finite_floats=non_finite_floats,
        )
