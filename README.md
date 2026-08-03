# arrowbricks

Runs SQL against a Databricks SQL warehouse via the Statement Execution API and hands you the result as Arrow -- a `Cursor` shaped like [`databricks-sql-python`](https://github.com/databricks/databricks-sql-python)'s (`execute`, `fetchone`/`fetchmany`/`fetchall`, `fetchall_arrow`/`fetchmany_arrow`), or `stream_query_json` for streaming NDJSON. One Arrow engine ([arro3](https://github.com/kylebarron/arro3)), no DuckDB, no pandas/pyarrow.

- Single responsibility: Databricks to Arrow via arro3. No embedded query engine -- that's [duckbricks](https://github.com/bmsuisse/duckbricks), built on top of this.
- Bring-your-own-auth -- a static token or your own token-refresh callable. No cloud-SDK dependency baked in.
- Result-order preserved even though chunks can complete out of order over the network.
- Chunks are fetched lazily as `fetchone`/`fetchmany`/`fetchall` actually need them, not all upfront.
- Heartbeats between slow chunks (`execute_streamed`/`stream_query_json`), so a caller streaming this over e.g. SSE never goes silent during a cold warehouse start.

## Install

```bash
pip install arrowbricks
```

Dependencies: `httpx` + `arro3-core` + `arro3-io`. That's the whole tree.

## Quickstart

```python
import asyncio
from arrowbricks import connect


async def main():
    conn = connect(
        host="adb-1234567890.1.azuredatabricks.net",
        warehouse_id="abcd1234efgh5678",
        token="dapi...",  # or token_provider=... -- see Auth below
    )
    cursor = conn.cursor()

    await cursor.execute("SELECT * FROM my_catalog.my_schema.my_table LIMIT 100")
    async for row in cursor:
        print(row)

    await cursor.execute("SELECT * FROM my_catalog.my_schema.my_table LIMIT 100")
    table = await cursor.fetchall_arrow()  # an arro3 Table


asyncio.run(main())
```

For streaming NDJSON (e.g. a FastAPI SSE endpoint, first row out as soon as its chunk arrives):

```python
from arrowbricks import HEARTBEAT, DatabricksClient, stream_query_json

client = DatabricksClient(host=..., warehouse_id=..., token=...)

async for item in stream_query_json(client, "SELECT * FROM my_catalog.my_schema.big_table"):
    if item is HEARTBEAT:
        continue  # forward as an SSE keep-alive comment, e.g.
    print(item)  # one ready-to-send JSON string per row
```

See [`examples/basic.py`](examples/basic.py) for a runnable version,
[`examples/cursor_paging.py`](examples/cursor_paging.py) for paging a large
result with `fetchmany`/`fetchmany_arrow` without buffering it all upfront,
[`examples/fastapi_sse.py`](examples/fastapi_sse.py) for streaming a query to
a client as Server-Sent Events, [`examples/fastapi_sse_pivot.py`](examples/fastapi_sse_pivot.py)
for the same over a buffered `Cursor.fetchall_streamed` result with one
combined heartbeat/timeout budget across both the wait and the download, or
[`examples/azure_auth.py`](examples/azure_auth.py) for a caching
`token_provider` built on Azure AD (`DefaultAzureCredential`).

## Rust core (replacing this package's implementation)

A [PyO3](https://pyo3.rs)-based reimplementation of the hot path -- statement
submit/poll, bounded-concurrency chunk fetch, the reorder buffer, and
Arrow-IPC decode via [arrow-rs](https://github.com/apache/arrow-rs) -- is
being built in [`rust/arrowbricks_core`](rust/arrowbricks_core). This is the
committed direction, not a side experiment: once it reaches feature parity
with the pure-Python implementation below, `arrowbricks`'s build switches to
it and the Python implementation is deleted, not kept as a permanent
alternative. Benchmarked so far at 1.3x-8x over the pure-Python path,
depending on concurrency and chunk count: Python's asyncio+GIL plateaus past
a handful of concurrent chunk fetches, while the Rust core keeps scaling on
real OS threads.

**Nothing above changes because of this yet.** `pip install arrowbricks`
still gets you exactly the pure-Python/arro3 implementation described in
this README until parity is reached -- the Rust core is a separate,
unpublished crate you'd have to clone this repo and build yourself
(`uvx maturin develop --uv --release` inside `rust/arrowbricks_core`). Ported
so far: lazy `fetchmany`, `token_provider` callback, JSON result format,
volume-file operations. One gap left: `execute_streamed` heartbeats during a
slow warehouse cold start. See [its own README](rust/arrowbricks_core/README.md)
for what's actually there today, including DuckDB and FastAPI SSE usage.

## Why not `databricks-sql-connector`?

The [official driver](https://github.com/databricks/databricks-sql-python) is the right choice if you need full DB-API 2.0 compatibility over Databricks' Thrift/ODBC-style protocol. If you just want a query result as Arrow/JSON in your own async app, it drags in a lot for that: `pandas`, `thrift`, `openpyxl`, `pybreaker`, `pyjwt`, `oauthlib`, `lz4`, `requests`, `urllib3` as hard dependencies. arrowbricks talks to the plain REST Statement Execution API instead, and its whole dependency tree is `httpx` + `arro3-core` + `arro3-io`. The `Cursor` API is deliberately shaped like the official driver's so switching between them is mostly a constructor change, but arrowbricks is async throughout (`execute`, `fetchone`, etc. are all coroutines) -- there's no sync escape hatch.

## Why not `duckbricks`?

[duckbricks](https://github.com/bmsuisse/duckbricks) does the same Databricks-to-Arrow work, then goes further: it uses a real embedded DuckDB engine to materialize results into your own DuckDB connection/table (`feed_select_to_duckdb_table`), or push a DuckDB query's result *up* to Databricks (`feed_duckdb_table_to_databricks`). If you need that -- a real local SQL engine sitting on top, not just "run this query, get Arrow/JSON back" -- use duckbricks; it depends on arrowbricks for the Databricks/Arrow half. If you don't need DuckDB at all, arrowbricks alone is the smaller, single-responsibility half.

## Auth

`connect`/`DatabricksClient` take either:

- `token: str` -- a static personal access token or pre-issued OAuth token, or
- `token_provider` -- a callable (sync or async) returning a token string, called on every request.

arrowbricks has no opinion on *how* you get a token and no cloud-SDK dependency of its own. If your provider is expensive to call, cache/refresh inside it -- arrowbricks does no caching on your behalf.

```python
conn = connect(host=..., warehouse_id=..., token_provider=my_token_provider)
```

## API

- `connect(host, warehouse_id, *, token=None, token_provider=None, ...) -> Connection`
- `Connection.cursor() -> Cursor`
- `Connection.client -> DatabricksClient` -- the same client `cursor()` uses, for lower-level access (e.g. `stream_query_json`, `execute_json_statement`, `upload_volume_file`).
- `Cursor.execute(sql, parameters=None, *, row_limit=None, offset=None, catalog=None, schema=None, total_timeout_s=None) -> Cursor` -- submits and waits for the statement, like a real DB-API cursor. `parameters`, if given, is Databricks' own named-parameter format -- `[{"name": ..., "value": ..., "type": ...}]` bound against `:name` markers in `sql`.
- `Cursor.execute_streamed(...)` -- same args, but an async generator yielding `HEARTBEAT` while waiting on a slow cold start, then the ready `Cursor` -- for bridging e.g. an SSE connection. Its timeout/heartbeats stop the moment the statement is ready, *before* any chunk has been downloaded -- see `fetchall_streamed` below for the download phase itself.
- `Cursor.fetchone() -> tuple | None`, `Cursor.fetchmany(size) -> list[tuple]`, `Cursor.fetchall() -> list[tuple]`
- `Cursor.fetchmany_arrow(size) -> arro3.core.Table`, `Cursor.fetchall_arrow() -> arro3.core.Table`
- `Cursor.fetchall_streamed(*, total_timeout_s=None)` / `Cursor.fetchall_arrow_streamed(*, total_timeout_s=None)` -- like `fetchall()`/`fetchall_arrow()`, but yield `HEARTBEAT` while pulling chunks instead of blocking silently, then the final rows/Table -- for a caller downloading a large result over SSE who needs heartbeats (and a timeout) through the *download*, not just the initial wait. Compose with `execute_streamed` and a shared deadline if you want one combined budget across both phases (see `examples/fastapi_sse_pivot.py`).
- `Cursor` is an async iterator, yielding one row (tuple) at a time.
- `Cursor.description` -- DB-API-style `[(name, type_name, None, None, None, None, None), ...]` after `execute()`.
- `stream_query_json(client, sql, **kwargs)` -- yields `HEARTBEAT`, then each row as a JSON string, as soon as its chunk arrives. Timestamps come out as full ISO-8601, every column key is always present (`"col":null` for a null value, never an omitted key).
- `DatabricksClient(host, warehouse_id, *, token=None, token_provider=None, ...)` -- the lower-level client `Connection` wraps. `client.execute_json_statement(sql, ...)` for plain JSON rows with no Arrow parse at all; `client.upload_volume_file(volume_path, data)`/`client.delete_volume_file(volume_path)` for the Files API.
- `write_ipc_stream(table_or_chunk, buf)` -- thin wrapper around `arro3.io.write_ipc_stream` that always writes uncompressed bodies (see below).

`Cursor.execute`/`execute_streamed`/`stream_query_json` all accept `catalog`, `schema`, `row_limit`, `offset`, and `total_timeout_s`.

## A note on Arrow IPC compression

`write_ipc_stream` (and everything in this package that serializes Arrow-IPC bytes) always writes **uncompressed** bodies. arro3's own default (`compression="LZ4"`) is transparently decompressed by some Arrow readers (e.g. DuckDB's) but not necessarily by every other Arrow IPC reader -- notably, `duckdb-wasm`'s browser-side decoder silently fails to parse LZ4-compressed bodies. Since arrowbricks' bytes might end up read by anything, plain uncompressed is the safe default.

## License

MIT