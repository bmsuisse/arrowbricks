"""Async client for the Databricks SQL Statement Execution API, tuned for
pulling large results out as Arrow-IPC chunks rather than running a single
small query. No dependency on Arrow at all -- that lives in `_streaming.py`
and `cursor.py`, so this client is reusable by any caller regardless of what
they do with the raw chunk bytes.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, TypeVar, cast

import httpx

_POLL_INTERVAL_S = 2.0
_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}
_RETRY_ATTEMPTS = 6
_RETRY_MAX_WAIT_S = 20.0

TokenProvider = Callable[[], "str | Awaitable[str]"]

_T = TypeVar("_T")


def _is_transient_error(exc: BaseException) -> bool:
    """401/403/429/5xx are treated as transient and retried -- these have been
    observed in practice to resolve themselves (e.g. a just-granted permission
    or a just-refreshed token not yet visible to the request that raced it)
    rather than indicating a persistent problem. httpx.TimeoutException/
    NetworkError are deliberately excluded: a genuinely stalled connection
    should fail fast on the caller's own timeout, not be retried here."""
    return isinstance(exc, httpx.HTTPStatusError) and (
        exc.response.status_code in (401, 403, 408, 429) or exc.response.status_code >= 500
    )


async def _retry_call(fn: Callable[[], Awaitable[_T]]) -> _T:
    """Retry fn() with exponential backoff (1s, 2s, 4s, ... capped at
    _RETRY_MAX_WAIT_S) on transient errors, up to _RETRY_ATTEMPTS total tries.
    A hand-rolled loop instead of tenacity -- this is the only retry pattern
    in the whole client, so a dependency for it isn't worth it."""
    attempt = 0
    while True:
        try:
            return await fn()
        except Exception as exc:
            if attempt == _RETRY_ATTEMPTS - 1 or not _is_transient_error(exc):
                raise
            await asyncio.sleep(min(_RETRY_MAX_WAIT_S, 2.0**attempt))
            attempt += 1


def _raise_for_failed(status: dict[str, Any]) -> None:
    state = status.get("state")
    if state == "FAILED":
        err = status.get("error", {})
        raise RuntimeError(f"Databricks statement failed [{err.get('error_code')}]: {err.get('message')}")
    if state == "CANCELED":
        raise RuntimeError("Databricks statement was canceled")


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
        chunk_fetch_concurrency: int = 6,
        warehouse_start_timeout: float = 300.0,
        warehouse_confirmed_running_ttl_s: float = 30.0,
    ) -> None:
        if not token and not token_provider:
            raise ValueError("DatabricksClient needs either `token` or `token_provider`")
        self._host = host.rstrip("/")
        if not self._host.startswith("https://"):
            self._host = f"https://{self._host}"
        self.warehouse_id = warehouse_id
        self._token = token
        self._token_provider = token_provider
        self.http_timeout = http_timeout
        self.wait_timeout = wait_timeout
        # Each concurrent fetch slot (plus its matching queued-but-unconsumed
        # slot, see _fetch_chunks_with_backpressure) holds a whole chunk's raw
        # bytes in plain Python memory. Keep this modest by default -- it's
        # I/O-bound network fetching, so parallelism past single digits buys
        # little, while every extra slot is more memory held hostage behind a
        # slow consumer.
        self.chunk_fetch_concurrency = chunk_fetch_concurrency
        self.warehouse_start_timeout = warehouse_start_timeout
        # See _ensure_warehouse_running: a warm warehouse doesn't need its
        # RUNNING state re-verified on every single statement -- one round
        # trip saved per query once confirmed within this window.
        self._warehouse_confirmed_running_ttl_s = warehouse_confirmed_running_ttl_s
        self._warehouse_confirmed_running_at: float | None = None
        # One shared connection pool for this client's whole lifetime instead
        # of a fresh httpx.AsyncClient() (and its own TCP+TLS handshake) per
        # call -- every method below hits the same Databricks host repeatedly,
        # so keep-alive/pooling actually pays off across calls. Presigned
        # external-link downloads (a different host: blob storage) still each
        # get their own connection from this same pool as needed; httpx pools
        # per-host internally, so sharing one client across hosts is safe.
        self._http: httpx.AsyncClient | None = None
        self._http_lock = asyncio.Lock()

    async def _get_http_client(self) -> httpx.AsyncClient:
        if self._http is None:
            async with self._http_lock:
                if self._http is None:
                    self._http = httpx.AsyncClient()
        return self._http

    async def aclose(self) -> None:
        """Closes the shared connection pool. Safe to call even if no request
        was ever made (no-op) or more than once."""
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def __aenter__(self) -> DatabricksClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def _bearer_token(self) -> str:
        if self._token is not None:
            return self._token
        assert self._token_provider is not None  # noqa: S101 -- enforced in __init__, not a test assertion
        result = self._token_provider()
        if inspect.isawaitable(result):
            return await cast("Awaitable[str]", result)
        return cast(str, result)

    async def _headers(self, content_type: str = "application/json") -> dict[str, str]:
        return {
            "Authorization": f"Bearer {await self._bearer_token()}",
            "Content-Type": content_type,
        }

    async def _authed_request(
        self, client: httpx.AsyncClient, method: str, url: str, *, content_type: str = "application/json", **kwargs: Any
    ) -> httpx.Response:
        async def _do() -> httpx.Response:
            resp = await client.request(
                method, url, headers=await self._headers(content_type), timeout=self.http_timeout, **kwargs
            )
            resp.raise_for_status()
            return resp

        return await _retry_call(_do)

    async def _ensure_warehouse_running(self, client: httpx.AsyncClient) -> None:
        """Explicitly wait for the warehouse to reach RUNNING before submitting
        a statement, rather than relying on the Statement Execution API's own
        implicit auto-start. A cold warehouse's catalog credential cache needs
        a moment to catch up right after startup -- submitting straight into
        that window is a common source of transient, identity-scoped 403s.

        Skips the check entirely if RUNNING was already confirmed within
        `_warehouse_confirmed_running_ttl_s` -- a warm, always-on warehouse
        doesn't need re-verifying on every single statement; that GET is a
        full round trip that buys nothing once already known-good."""
        now = time.monotonic()
        if (
            self._warehouse_confirmed_running_at is not None
            and now - self._warehouse_confirmed_running_at < self._warehouse_confirmed_running_ttl_s
        ):
            return

        url = f"{self._host}/api/2.0/sql/warehouses/{self.warehouse_id}"
        resp = await self._authed_request(client, "GET", url)
        state = resp.json().get("state")
        if state == "RUNNING":
            self._warehouse_confirmed_running_at = time.monotonic()
            return

        if state == "STOPPED":
            await self._authed_request(client, "POST", f"{url}/start")

        deadline = time.monotonic() + self.warehouse_start_timeout
        while time.monotonic() < deadline:
            await asyncio.sleep(_POLL_INTERVAL_S)
            resp = await self._authed_request(client, "GET", url)
            state = resp.json().get("state")
            if state == "RUNNING":
                self._warehouse_confirmed_running_at = time.monotonic()
                return
        # Falls through and lets the statement submission itself surface
        # whatever's actually wrong -- proceeding anyway rather than raising
        # here avoids a false negative if the warehouse comes up moments later.

    async def _execute_statement(
        self,
        statement: str,
        *,
        format: str,
        catalog: str | None,
        schema: str | None,
        wait_timeout: str | None,
        parameters: list[dict[str, Any]] | None,
    ) -> tuple[str, dict[str, Any]]:
        """Submit an EXTERNAL_LINKS statement in the given result format and
        poll until terminal. Returns (statement_id, manifest). `parameters`,
        if given, is Databricks' own named-parameter format --
        [{"name": ..., "value": ..., "type": ...}] bound against `:name`
        markers in `statement`."""
        body: dict[str, Any] = {
            "warehouse_id": self.warehouse_id,
            "statement": statement,
            "disposition": "EXTERNAL_LINKS",
            "format": format,
            "wait_timeout": wait_timeout or self.wait_timeout,
            "on_wait_timeout": "CONTINUE",
        }
        if catalog:
            body["catalog"] = catalog
        if schema:
            body["schema"] = schema
        if parameters:
            body["parameters"] = parameters

        client = await self._get_http_client()
        await self._ensure_warehouse_running(client)
        resp = await self._authed_request(client, "POST", f"{self._host}/api/2.0/sql/statements", json=body)
        data = resp.json()

        status = data.get("status", {})
        while status.get("state") not in _TERMINAL_STATES:
            statement_id = data["statement_id"]
            await asyncio.sleep(_POLL_INTERVAL_S)
            resp = await self._authed_request(client, "GET", f"{self._host}/api/2.0/sql/statements/{statement_id}")
            data = resp.json()
            status = data.get("status", {})

        _raise_for_failed(status)
        return data["statement_id"], data.get("manifest") or {}

    async def execute_arrow_statement(
        self,
        statement: str,
        *,
        catalog: str | None = None,
        schema: str | None = None,
        wait_timeout: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Like _execute_statement, fixed to ARROW_STREAM -- for callers that
        ingest the result as Arrow (see _streaming.py/cursor.py)."""
        return await self._execute_statement(
            statement,
            format="ARROW_STREAM",
            catalog=catalog,
            schema=schema,
            wait_timeout=wait_timeout,
            parameters=parameters,
        )

    async def execute_json_statement(
        self,
        statement: str,
        *,
        catalog: str | None = None,
        schema: str | None = None,
        wait_timeout: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Like _execute_statement, fixed to JSON_ARRAY -- each fetched
        chunk's bytes (see stream_chunks_by_index) are then a plain JSON array
        of rows, for callers that want Python values without an Arrow parse."""
        return await self._execute_statement(
            statement,
            format="JSON_ARRAY",
            catalog=catalog,
            schema=schema,
            wait_timeout=wait_timeout,
            parameters=parameters,
        )

    async def upload_volume_file(self, volume_path: str, data: bytes) -> None:
        """Uploads `data` to a Unity Catalog volume path via the Files API,
        overwriting anything already there. `volume_path` is caller-supplied
        in full (e.g. `/Volumes/my_catalog/my_schema/my_volume/some/file.parquet`)
        -- this package has no knowledge of any specific catalog/schema/volume."""
        client = await self._get_http_client()
        await self._authed_request(
            client,
            "PUT",
            f"{self._host}/api/2.0/fs/files{volume_path}",
            params={"overwrite": "true"},
            content_type="application/octet-stream",
            content=data,
        )

    async def delete_volume_file(self, volume_path: str) -> None:
        """Deletes a file at `volume_path` (see upload_volume_file). A 404 is
        treated as success -- the file is already gone, which is fine for
        idempotent staging cleanup."""
        client = await self._get_http_client()
        try:
            await self._authed_request(client, "DELETE", f"{self._host}/api/2.0/fs/files{volume_path}")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise

    async def stream_chunks_by_index(
        self, statement_id: str, chunk_metas: list[dict[str, Any]]
    ) -> AsyncIterator[tuple[bytes, int | None, int]]:
        """Fetch already-known chunks for a completed statement (see
        execute_arrow_statement/execute_json_statement), with real
        backpressure -- see _fetch_chunks_with_backpressure. Each yielded
        (blob, row_count, chunk_index) is an independent, self-contained
        result blob plus its row count from the manifest (`None` if the
        manifest didn't carry one) and its own chunk_index, so a caller that
        cares about the original row order (e.g. a query with ORDER BY) can
        restore it even though chunks can complete out of order."""
        client = await self._get_http_client()
        async for blob, row_count, chunk_index in self._fetch_chunks_with_backpressure(
            client, statement_id, chunk_metas
        ):
            yield blob, row_count, chunk_index

    async def _fetch_link_bytes(self, client: httpx.AsyncClient, url: str) -> bytes:
        async def _do() -> bytes:
            resp = await client.get(url, timeout=self.http_timeout)
            resp.raise_for_status()
            return resp.content

        return await _retry_call(_do)

    async def _fetch_chunk_index(self, client: httpx.AsyncClient, statement_id: str, chunk_index: int) -> list[bytes]:
        """Resolve one chunk index to its external link(s) (usually exactly
        one) and download the bytes. Each chunk index can be requested
        independently -- that's what makes concurrent fetching possible."""
        resp = await self._authed_request(
            client, "GET", f"{self._host}/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}"
        )
        links = resp.json().get("external_links") or []
        return await asyncio.gather(*(self._fetch_link_bytes(client, link["external_link"]) for link in links))

    async def _fetch_chunks_with_backpressure(
        self,
        client: httpx.AsyncClient,
        statement_id: str,
        chunk_metas: list[dict[str, Any]],
    ) -> AsyncIterator[tuple[bytes, int | None, int]]:
        """Fetch every chunk concurrently (bounded), instead of one round trip
        at a time -- for a result with hundreds of chunks, that sequential
        chain dominates total latency. Chunks complete out of order; ordering
        (if the caller's SQL needs it) is restored downstream, not here.

        This is a bounded *worker pool*, not `asyncio.as_completed` over every
        chunk task scheduled up front -- that shape bounds how many downloads
        run *simultaneously* but not how many completed-and-downloaded chunks
        pile up waiting for a slow consumer, since a slot frees the instant
        its own download finishes, whether or not anything downstream has
        consumed that chunk yet. If the consumer is slower than the network --
        plausible, one async task processing vs. several concurrent
        downloads -- unconsumed bytes still pile up to O(whole result)
        regardless of the concurrency bound. Here, a worker blocks on
        `out.put()` (a bounded queue) until the consumer actually takes the
        previous chunk, so a fetch slot is only freed once its chunk has been
        handed off -- backpressure reaches all the way to the download, and
        peak resident chunks stay at ~concurrency + queue size, not
        O(whole result)."""
        concurrency = self.chunk_fetch_concurrency
        work: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        for meta in chunk_metas:
            work.put_nowait(meta)
        out: asyncio.Queue[tuple[bytes, int | None, int] | None] = asyncio.Queue(maxsize=concurrency)

        async def worker() -> None:
            while True:
                try:
                    meta = work.get_nowait()
                except asyncio.QueueEmpty:
                    return
                for blob in await self._fetch_chunk_index(client, statement_id, meta["chunk_index"]):
                    await out.put((blob, meta.get("row_count"), meta["chunk_index"]))  # blocks here = backpressure

        workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(chunk_metas) or 1))]

        async def close_when_done() -> None:
            # return_exceptions=True, not the default False: with the default,
            # a failing worker makes gather raise *immediately* and this
            # coroutine never reaches `out.put(None)` -- the consumer's
            # `await out.get()` below then blocks forever. Collect every
            # worker's outcome first, unblock the consumer unconditionally,
            # then re-raise whichever failed.
            results = await asyncio.gather(*workers, return_exceptions=True)
            await out.put(None)
            for result in results:
                if isinstance(result, BaseException):
                    raise result

        closer = asyncio.create_task(close_when_done())
        try:
            while True:
                item = await out.get()
                if item is None:
                    break
                yield item
            await closer  # propagate a worker failure that only surfaced after the last successful yield
        finally:
            closer.cancel()
            for w in workers:
                w.cancel()
