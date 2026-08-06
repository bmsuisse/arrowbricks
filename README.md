<p align="center">
  <img src="assets/logo.svg" width="72" height="72" alt="arrowbricks logo">
</p>

<h1 align="center">arrowbricks</h1>
<p align="center">Databricks SQL to Arrow, with a Rust core.</p>

Runs SQL against a Databricks SQL warehouse via the Statement Execution API and hands you the result as Arrow -- a `Cursor` shaped like [`databricks-sql-python`](https://github.com/databricks/databricks-sql-python)'s (`execute`, `fetchone`/`fetchmany`/`fetchall`, `fetchall_arrow`/`fetchmany_arrow`), or `stream_query_json` for streaming NDJSON.

- **Rust core.** Statement submit/poll, bounded-concurrency chunk fetch, the reorder buffer, and Arrow-IPC decode all run in a [PyO3](https://pyo3.rs)/[arrow-rs](https://github.com/apache/arrow-rs) extension bundled in this same package -- 1.6x-2.5x faster than a pure-Python/asyncio client on a multi-chunk result, scaling further with chunk count and concurrency where asyncio+GIL plateaus.
- **Compressed cloud-fetch transport by default.** Every statement requests LZ4-compressed chunk downloads (same default as the official `databricks-sql-connector`) and decompresses them in Rust before you ever see the bytes -- less data over the wire, which matters more than local decode speed for a large result. Measured ~2x faster chunk-fetch time against a real 120-column/100k-row table. Disable per-client with `compress_results=False` (`connect()`/`DatabricksClient(...)`) if your link to the warehouse is fast enough that decompression CPU time stops paying for itself.
- **Zero required dependencies.** `pip install arrowbricks` and go.
- **Bring-your-own-auth** -- a static token or your own token-refresh callable. No cloud-SDK dependency baked in.
- **Result order preserved** even though chunks can complete out of order over the network.
- **Lazy fetching** -- chunks are pulled only as `fetchone`/`fetchmany`/`fetchall` actually need them, not all upfront.
- **Heartbeats** between slow chunks (`execute_streamed`/`stream_query_json`), so a caller streaming this over e.g. SSE never goes silent during a cold warehouse start.

## Install

```bash
pip install arrowbricks
```

Ships as precompiled platform wheels (Linux/macOS/Windows) -- no Rust toolchain needed, and nothing else to install for most of the API. Row-tuple fetches (`fetchone`/`fetchmany`/`fetchall`) need one optional extra: `pip install arrowbricks[arro3]` -- see [Arrow vs. row-tuple fetches](#arrow-vs-row-tuple-fetches) below.

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
from arrowbricks import HEARTBEAT, DatabricksClient

client = DatabricksClient(host=..., warehouse_id=..., token=...)

async for item in client.stream_query_json("SELECT * FROM my_catalog.my_schema.big_table"):
    if item is HEARTBEAT:
        continue  # forward as an SSE keep-alive comment, e.g.
    print(item)  # one ready-to-send JSON string per row
```

See [`examples/basic.py`](examples/basic.py) for a runnable version,
[`examples/cursor_paging.py`](examples/cursor_paging.py) for paging a large
result with `fetchmany`/`fetchmany_arrow` without buffering it all upfront, or
[`examples/azure_auth.py`](examples/azure_auth.py) for a caching
`token_provider` built on Azure AD (`DefaultAzureCredential`).

## FastAPI SSE example

`stream_query_json` is the full-speed way to serve a query over HTTP: the first row reaches the client after roughly one chunk's fetch/decode time, not the whole query's -- the Rust core is fetching, decoding, and reordering chunks concurrently the entire time, and the `HEARTBEAT`s keep the connection alive through a slow cold warehouse start instead of the client just seeing dead air:

```python
import os
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from arrowbricks import HEARTBEAT, DatabricksClient

app = FastAPI()
client = DatabricksClient(
    host=os.environ["DATABRICKS_HOST"],
    warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
    token=os.environ["DATABRICKS_TOKEN"],
)


async def _sse(sql: str) -> AsyncIterator[str]:
    async for item in client.stream_query_json(sql, total_timeout_s=300):
        if item is HEARTBEAT:
            yield ": keep-alive\n\n"  # SSE comment line -- clients ignore it, it just keeps the connection open
        else:
            yield f"data: {item}\n\n"


@app.get("/query")
async def query(sql: str) -> StreamingResponse:
    return StreamingResponse(_sse(sql), media_type="text/event-stream")
```

```bash
uvicorn app:app --reload
curl -N "http://localhost:8000/query?sql=SELECT+*+FROM+range(1000000)"
```

This example takes `sql` straight from the request for brevity -- arrowbricks does no SQL validation by design, so a real deployment must validate/allowlist it (or accept fixed query names + params) before exposing a route like this publicly. See [`examples/fastapi_sse.py`](examples/fastapi_sse.py) for the runnable version, [`examples/fastapi_sse_pivot.py`](examples/fastapi_sse_pivot.py) for the same over a buffered `Cursor.fetchall_streamed` result with one combined heartbeat/timeout budget across both the wait and the download, or [`examples/fastapi_sse_validated.py`](examples/fastapi_sse_validated.py) for one way to do that validation, using [sqlglot](https://github.com/tobymao/sqlglot) to require a single read-only `SELECT` against an allowlist of fully-qualified tables.

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
- `Cursor.fetchone() -> tuple | None`, `Cursor.fetchmany(size) -> list[tuple]`, `Cursor.fetchall() -> list[tuple]`, and iterating a `Cursor` directly -- row tuples; needs the `arro3` extra.
- `Cursor.fetchmany_arrow(size) -> Table`, `Cursor.fetchall_arrow() -> Table` -- an Arrow table (implements `__arrow_c_stream__`, so arro3/pyarrow/DuckDB can all consume it directly, zero-copy).
- `Cursor.fetchall_streamed(*, total_timeout_s=None)` / `Cursor.fetchall_arrow_streamed(*, total_timeout_s=None)` -- like `fetchall()`/`fetchall_arrow()`, but yield `HEARTBEAT` while pulling chunks instead of blocking silently, then the final rows/Table -- for a caller downloading a large result over SSE who needs heartbeats (and a timeout) through the *download*, not just the initial wait. Compose with `execute_streamed` and a shared deadline if you want one combined budget across both phases (see `examples/fastapi_sse_pivot.py`).
- `Cursor.description` -- DB-API-style `[(name, type_name, None, None, None, None, None), ...]` after `execute()`.
- `client.stream_query_json(sql, **kwargs)` (or the equivalent free function `stream_query_json(client, sql, **kwargs)`) -- yields `HEARTBEAT`, then each row as a JSON string, as soon as its chunk arrives. Timestamps come out as full ISO-8601, every column key is always present (`"col":null` for a null value, never an omitted key). JSON has no literal for NaN/Infinity/-Infinity, so those come back as `"col":null` by default -- pass `non_finite_floats="string"` to get `"col":"NaN"`/`"col":"Infinity"`/`"col":"-Infinity"` instead if you need to tell them apart from a real NULL.
- `DatabricksClient(host, warehouse_id, *, token=None, token_provider=None, ...)` -- the lower-level client `Connection` wraps. `client.upload_volume_file(volume_path, data)`/`client.delete_volume_file(volume_path)` for the Files API.
- `write_ipc_stream(table, buf)` -- writes any Arrow-C-Data-Interface-compatible object as an uncompressed Arrow-IPC stream (see below).
- `ReplayableArrowChunk(data: bytes, chunk_index, declared_row_count=None)` -- wraps raw Arrow-IPC stream bytes (e.g. previously downloaded and stored) so they can be read more than once via `__arrow_c_stream__` (a schema peek, then the actual scan -- DuckDB's registration path does this), and `.to_table()` for a one-shot parse. No extra dependency needed.

`Cursor.execute`/`execute_streamed`/`stream_query_json` all accept `catalog`, `schema`, `row_limit`, `offset`, and `total_timeout_s`.

## Arrow vs. row-tuple fetches

Everything above works with zero dependencies installed *except* row-tuple fetches. `fetchall_arrow`/`fetchmany_arrow` return an Arrow table straight from the Rust core -- the faster path if your code can consume Arrow directly (DuckDB, pyarrow, polars, a Parquet writer, ...):

```python
import duckdb

table = await cursor.fetchall_arrow()
duckdb.sql("SELECT count(*) FROM table").show()  # DuckDB reads it zero-copy
```

`fetchone`/`fetchmany`/`fetchall` (and iterating a `Cursor` directly) materialize actual Python tuples instead -- `("id", "label")`-style rows you can index into, print, or pass to code that doesn't know about Arrow at all. That conversion needs `arro3-core` (`pip install arrowbricks[arro3]`):

```python
await cursor.execute("SELECT id, label FROM my_catalog.my_schema.my_table")
async for row in cursor:  # or: rows = await cursor.fetchall()
    print(row[0], row[1])
```

Calling a row-tuple method without `arro3-core` installed raises a `ModuleNotFoundError` naming the exact install command, rather than failing silently or with a confusing traceback.

## Using with DuckDB

Anything arrowbricks hands back as Arrow (`fetchall_arrow`/`fetchmany_arrow`, `ReplayableArrowChunk`) implements `__arrow_c_stream__`, so DuckDB can register and query it directly -- zero-copy, no intermediate materialization, and no `arro3-core`/`pyarrow` install needed on top:

```python
import duckdb
from arrowbricks import connect

conn = connect(host=..., warehouse_id=..., token=...)
cursor = conn.cursor()
await cursor.execute("SELECT * FROM my_catalog.my_schema.my_table LIMIT 100")
table = await cursor.fetchall_arrow()

con = duckdb.connect()
con.register("my_table", table)
con.sql("SELECT count(*) FROM my_table").show()
```

`ReplayableArrowChunk` works the same way for Arrow-IPC bytes you fetched and stored earlier (e.g. a raw chunk's bytes, cached in Redis/a file/wherever) -- DuckDB's registration path calls `__arrow_c_stream__` twice (a schema peek, then the actual scan), which is exactly what `ReplayableArrowChunk` exists to support:

```python
from arrowbricks import ReplayableArrowChunk

chunk = ReplayableArrowChunk(stored_bytes, chunk_index=0)
con.register("my_table", chunk)
con.sql("SELECT * FROM my_table WHERE id = 42").show()
```

## Rust core

`rust/arrowbricks_core` is the crate implementing the hot path above, built into this same `arrowbricks` wheel as a compiled submodule -- not a separate PyPI package. See [its own README](rust/arrowbricks_core/README.md) for the crate-level design, plus standalone DuckDB and FastAPI SSE examples against the compiled extension directly.

## Why not `databricks-sql-connector`?

The [official driver](https://github.com/databricks/databricks-sql-python) is the right choice if you need full DB-API 2.0 compatibility over Databricks' Thrift/ODBC-style protocol. If you just want a query result as Arrow/JSON in your own async app, it drags in a lot for that: `pandas`, `thrift`, `openpyxl`, `pybreaker`, `pyjwt`, `oauthlib`, `lz4`, `requests`, `urllib3` as hard dependencies. arrowbricks talks to the plain REST Statement Execution API instead, with a Rust core and zero required dependencies of its own. The `Cursor` API is deliberately shaped like the official driver's so switching between them is mostly a constructor change, but arrowbricks is async throughout (`execute`, `fetchone`, etc. are all coroutines) -- there's no sync escape hatch.

## A note on Arrow IPC compression

`write_ipc_stream` (and everything in this package that serializes Arrow-IPC bytes) always writes **uncompressed** bodies. A compressed body (arro3's own default is `compression="LZ4"`) is transparently decompressed by some Arrow readers (e.g. DuckDB's) but not necessarily by every other Arrow IPC reader -- notably, `duckdb-wasm`'s browser-side decoder silently fails to parse LZ4-compressed bodies. Since arrowbricks' bytes might end up read by anything, plain uncompressed is the safe default.

## License

MIT
