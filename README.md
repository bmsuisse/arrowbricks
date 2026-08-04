# arrowbricks

Runs SQL against a Databricks SQL warehouse via the Statement Execution API and hands you the result as Arrow -- a `Cursor` shaped like [`databricks-sql-python`](https://github.com/databricks/databricks-sql-python)'s (`execute`, `fetchone`/`fetchmany`/`fetchall`, `fetchall_arrow`/`fetchmany_arrow`), or `stream_query_json` for streaming NDJSON. The hot path (statement submit/poll, bounded-concurrency chunk fetch, the reorder buffer, Arrow-IPC decode) is a [PyO3](https://pyo3.rs)/[arrow-rs](https://github.com/apache/arrow-rs) extension bundled in this same package -- no DuckDB, no pandas/pyarrow.

- Single responsibility: Databricks to Arrow. No embedded query engine -- that's [duckbricks](https://github.com/bmsuisse/duckbricks), built on top of this.
- Bring-your-own-auth -- a static token or your own token-refresh callable. No cloud-SDK dependency baked in.
- Result-order preserved even though chunks can complete out of order over the network.
- Chunks are fetched lazily as `fetchone`/`fetchmany`/`fetchall` actually need them, not all upfront.
- Heartbeats between slow chunks (`execute_streamed`/`stream_query_json`), so a caller streaming this over e.g. SSE never goes silent during a cold warehouse start.
- Rust core, real OS-thread concurrency: 1.6x-2.5x faster than a pure-Python/asyncio client at fetching/decoding a multi-chunk result, scaling further with chunk count and concurrency where asyncio+GIL plateaus.
- Zero required runtime dependencies.

## Install

```bash
pip install arrowbricks
```

Ships as platform wheels (Linux/macOS/Windows) with the Rust extension precompiled -- no Rust toolchain needed to install, and no required dependencies. `fetchone`/`fetchmany`/`fetchall` (row-tuple materialization) need `pip install arrowbricks[arro3]`; everything else (`fetchall_arrow`/`fetchmany_arrow`, `execute_arrow`, `stream_query_json`, `upload_volume_file`/`delete_volume_file`, `Cursor.description`) works with nothing installed.

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
    table = await cursor.fetchall_arrow()  # an Arrow table (arro3/pyarrow/DuckDB-compatible)


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

## Rust core

`rust/arrowbricks_core` is a [PyO3](https://pyo3.rs)/[arrow-rs](https://github.com/apache/arrow-rs)
crate implementing the actual hot path -- statement submit/poll,
bounded-concurrency chunk fetch, the `chunk_index` reorder buffer,
Arrow-IPC decode/write, and NDJSON encode -- built into this same
`arrowbricks` wheel as a compiled submodule, not a separate PyPI package.
`Cursor`, `stream_query_json`, `DatabricksClient` all delegate to it
directly -- there's no separate Python-level HTTP client or Arrow library
on the hot path at all. See [its own README](rust/arrowbricks_core/README.md)
for the crate-level design, including standalone DuckDB and FastAPI SSE
usage examples against the compiled extension directly.

## Why not `databricks-sql-connector`?

The [official driver](https://github.com/databricks/databricks-sql-python) is the right choice if you need full DB-API 2.0 compatibility over Databricks' Thrift/ODBC-style protocol. If you just want a query result as Arrow/JSON in your own async app, it drags in a lot for that: `pandas`, `thrift`, `openpyxl`, `pybreaker`, `pyjwt`, `oauthlib`, `lz4`, `requests`, `urllib3` as hard dependencies. arrowbricks talks to the plain REST Statement Execution API instead, with a Rust core and zero required runtime dependencies of its own. The `Cursor` API is deliberately shaped like the official driver's so switching between them is mostly a constructor change, but arrowbricks is async throughout (`execute`, `fetchone`, etc. are all coroutines) -- there's no sync escape hatch.

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
- `Connection.client -> DatabricksClient` -- the same client `cursor()` uses, for lower-level access (e.g. `stream_query_json`, `upload_volume_file`).
- `Cursor.execute(sql, parameters=None, *, row_limit=None, offset=None, catalog=None, schema=None, total_timeout_s=None) -> Cursor` -- submits and waits for the statement, like a real DB-API cursor. `parameters`, if given, is Databricks' own named-parameter format -- `[{"name": ..., "value": ..., "type": ...}]` bound against `:name` markers in `sql`.
- `Cursor.execute_streamed(...)` -- same args, but an async generator yielding `HEARTBEAT` while waiting on a slow cold start, then the ready `Cursor` -- for bridging e.g. an SSE connection. Its timeout/heartbeats stop the moment the statement is ready, *before* any chunk has been downloaded -- see `fetchall_streamed` below for the download phase itself.
- `Cursor.fetchone() -> tuple | None`, `Cursor.fetchmany(size) -> list[tuple]`, `Cursor.fetchall() -> list[tuple]` -- needs `arrowbricks[arro3]` (see "Arrow vs. row-tuple fetches" below).
- `Cursor.fetchmany_arrow(size) -> Table`, `Cursor.fetchall_arrow() -> Table` -- an Arrow table (implements `__arrow_c_stream__`, so arro3/pyarrow/DuckDB can all consume it directly, zero-copy). No extra dependency needed.
- `Cursor.fetchall_streamed(*, total_timeout_s=None)` / `Cursor.fetchall_arrow_streamed(*, total_timeout_s=None)` -- like `fetchall()`/`fetchall_arrow()`, but yield `HEARTBEAT` while pulling chunks instead of blocking silently, then the final rows/Table -- for a caller downloading a large result over SSE who needs heartbeats (and a timeout) through the *download*, not just the initial wait. Compose with `execute_streamed` and a shared deadline if you want one combined budget across both phases (see `examples/fastapi_sse_pivot.py`).
- `Cursor` is an async iterator, yielding one row (tuple) at a time -- needs `arrowbricks[arro3]`, same as `fetchone`/`fetchmany`/`fetchall`.
- `Cursor.description` -- DB-API-style `[(name, type_name, None, None, None, None, None), ...]` after `execute()`. No extra dependency needed.
- `stream_query_json(client, sql, **kwargs)` -- yields `HEARTBEAT`, then each row as a JSON string, as soon as its chunk arrives. Timestamps come out as full ISO-8601, every column key is always present (`"col":null` for a null value, never an omitted key). No extra dependency needed.
- `DatabricksClient(host, warehouse_id, *, token=None, token_provider=None, ...)` -- the lower-level client `Connection` wraps. `client.upload_volume_file(volume_path, data)`/`client.delete_volume_file(volume_path)` for the Files API.
- `write_ipc_stream(table_or_chunk, buf)` -- writes any Arrow-C-Data-Interface-compatible object as an uncompressed Arrow-IPC stream (see below). No extra dependency needed.

`Cursor.execute`/`execute_streamed`/`stream_query_json` all accept `catalog`, `schema`, `row_limit`, `offset`, and `total_timeout_s`.

## Arrow vs. row-tuple fetches -- when you need `arrowbricks[arro3]`

`fetchall_arrow`/`fetchmany_arrow` return an Arrow table backed entirely by the Rust core -- no extra install needed, and it's the faster path if your code can consume Arrow directly (DuckDB, pyarrow, polars, a Parquet writer, ...):

```python
import duckdb

table = await cursor.fetchall_arrow()
duckdb.sql("SELECT count(*) FROM table").show()  # DuckDB reads it zero-copy
```

`fetchone`/`fetchmany`/`fetchall` (and iterating a `Cursor` directly) materialize actual Python tuples instead -- `("id", "label")`-style rows you can index into, print, or pass to code that doesn't know about Arrow at all. That conversion is handled by `arro3-core` (`pip install arrowbricks[arro3]`), not this package itself:

```python
await cursor.execute("SELECT id, label FROM my_catalog.my_schema.my_table")
async for row in cursor:  # or: rows = await cursor.fetchall()
    print(row[0], row[1])
```

Calling a row-tuple method without `arro3-core` installed raises a `ModuleNotFoundError` naming the exact install command, rather than failing silently or with a confusing traceback.

## A note on Arrow IPC compression

`write_ipc_stream` (and everything in this package that serializes Arrow-IPC bytes) always writes **uncompressed** bodies. A compressed body (arro3's own default is `compression="LZ4"`) is transparently decompressed by some Arrow readers (e.g. DuckDB's) but not necessarily by every other Arrow IPC reader -- notably, `duckdb-wasm`'s browser-side decoder silently fails to parse LZ4-compressed bodies. Since arrowbricks' bytes might end up read by anything, plain uncompressed is the safe default.

## License

MIT