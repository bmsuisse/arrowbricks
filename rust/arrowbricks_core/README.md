# arrowbricks_core

Rust reimplementation of [arrowbricks](https://github.com/bmsuisse/arrowbricks)'
hot path -- statement submit/poll, bounded-concurrency chunk fetch, the
`chunk_index` reorder buffer, and Arrow-IPC decode via
[arrow-rs](https://github.com/apache/arrow-rs) -- exposed to Python as an
async `Client` via [PyO3](https://pyo3.rs). This is the committed direction
for arrowbricks itself: once this crate reaches feature parity with the
current pure-Python/arro3 implementation, `arrowbricks`'s build switches to
this crate and the Python implementation is deleted -- not kept as a
permanent second option.

**Feature parity with the pure-Python implementation is done.** Ported: lazy
`fetchmany_arrow`/`fetchall_arrow` (`execute()` + `ResultSet`), `token_provider`
callback (sync or async), JSON result format (`execute_json`), volume-file
operations (`upload_volume_file`/`delete_volume_file`), and
`execute_streamed`/`fetchall_arrow_streamed` heartbeats during a slow
warehouse cold start. Not wired into `arrowbricks` itself yet and not
published to PyPI -- that cutover (swap `arrowbricks`'s build to this crate
behind the same public API, delete the Python implementation) is the next
step. See the parent repo's [AGENTS.md](../../AGENTS.md) for the state of
the migration.

## Build

```bash
uvx maturin develop --uv --release
```

`maturin develop` builds this as a mixed Python/Rust project (see
`pyproject.toml`'s `python-source = "python"`): the compiled extension lands
in `python/arrowbricks_core/`, alongside a hand-written `__init__.pyi` and a
`py.typed` marker (PEP 561), so `Client`/`ResultSet`/etc. get real
autocomplete and type-checking in editors and under `mypy`/`pyright`/`ty` --
a compiled PyO3 extension has no type info of its own without them.

## Quickstart

```python
import asyncio
import arrowbricks_core


async def main():
    client = arrowbricks_core.Client(
        host="adb-1234567890.1.azuredatabricks.net",
        warehouse_id="abcd1234efgh5678",
        token="dapi...",
    )
    table = await client.execute_arrow("SELECT * FROM my_catalog.my_schema.my_table LIMIT 100")
    print(table.num_rows, table.schema)


asyncio.run(main())
```

`table` implements the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
(`__arrow_c_stream__`/`__arrow_c_array__`) -- any consumer that speaks that
protocol (DuckDB, pyarrow, arro3) can import it with zero copies.

## API

- `Client(host, warehouse_id, *, token=None, token_provider=None, chunk_fetch_concurrency=32)` -- exactly one of `token`/`token_provider`. `token_provider` is a callable (sync or async) returning a token string, called fresh on every request, no caching.
- `Client.execute_arrow(statement, *, catalog=None, schema=None) -> Table` -- eager: fetches and assembles the whole result before returning.
- `Client.execute(statement, *, catalog=None, schema=None) -> ResultSet` -- submits and starts background chunk fetching without pulling anything yet.
  - `ResultSet.fetchmany_arrow(n) -> Table` -- pulls/decodes only as many chunks as needed for `n` rows, buffering the rest; may return fewer than `n` once exhausted.
  - `ResultSet.fetchall_arrow() -> Table` -- drains everything remaining.
  - `ResultSet.statement_id`, `ResultSet.num_chunks`.
- `Client.execute_json(statement, *, catalog=None, schema=None) -> list[list[str | None]]` -- JSON_ARRAY format, no Arrow parse. Every non-null value comes back as a string regardless of real column type (Databricks' own contract) -- cast by the manifest's column type yourself if you want native Python types.
- `Client.upload_volume_file(volume_path, data: bytes)` / `Client.delete_volume_file(volume_path)` -- Unity Catalog volume files via the Files API. Delete treats a 404 as success (idempotent).
- `Client.execute_streamed(statement, *, catalog=None, schema=None, total_timeout_s=None)` -- like `execute()`, but an async iterator yielding the `HEARTBEAT` singleton while waiting on Databricks instead of blocking silently (bridge e.g. an SSE connection through a slow warehouse cold start), then a `ResultSet` exactly once. Raises if `total_timeout_s` elapses first.
  - `ResultSet.fetchall_arrow_streamed(*, total_timeout_s=None)` -- same idea for the chunk-download phase: yields `HEARTBEAT` while pulling chunks, then a `Table` exactly once.
- `HEARTBEAT` -- module-level singleton; compare with `is`, e.g. `if item is arrowbricks_core.HEARTBEAT: ...`.

## With DuckDB

DuckDB's `duckdb.sql()` resolves an unrecognized table name against local
Python variables via a replacement scan -- point it at the Arrow object
directly, no pandas/pyarrow conversion step:

```python
import duckdb

result = await client.execute_arrow("SELECT * FROM my_catalog.my_schema.my_table")
print(duckdb.sql("SELECT count(*) FROM result").fetchall())
```

Runnable version: [`examples/duckdb_query.py`](examples/duckdb_query.py).

## With FastAPI (Server-Sent Events)

`execute_arrow` fetches the whole result eagerly by design (it's the
one-call convenience method) -- streaming its result means sending the
*already-fetched* table to the HTTP client in per-row chunks, bounding
client-side memory/parse latency on a large result, not hiding a slow
Databricks warehouse cold start:

```python
import json
from fastapi.responses import StreamingResponse

def _rows(table):
    names = [f.name for f in table.schema]
    columns = [table.column(i).combine_chunks().to_pylist() for i in range(table.num_columns)]
    return [dict(zip(names, row, strict=True)) for row in zip(*columns, strict=True)]

async def _sse(sql: str):
    table = await client.execute_arrow(sql)
    for row in _rows(table):
        yield f"data: {json.dumps(row)}\n\n"
```

Runnable version: [`examples/fastapi_sse.py`](examples/fastapi_sse.py).

For a connection that also needs to survive a slow warehouse cold start
without going silent, use `execute_streamed` and forward `HEARTBEAT` as an
SSE keep-alive comment:

```python
from arrowbricks_core import HEARTBEAT

async def _sse_with_heartbeats(sql: str):
    async for item in client.execute_streamed(sql, total_timeout_s=300):
        if item is HEARTBEAT:
            yield ": keep-alive\n\n"
            continue
        table = await item.fetchall_arrow()  # item is a ResultSet here
        for row in _rows(table):
            yield f"data: {json.dumps(row)}\n\n"
```

## Testing

```bash
cargo test --no-default-features
```

(`--no-default-features` disables `extension-module`, which otherwise skips
linking against libpython -- correct for the cdylib maturin ships, wrong for
a plain `cargo test` binary. See `Cargo.toml`'s `[features]` comment.)

## License

MIT
