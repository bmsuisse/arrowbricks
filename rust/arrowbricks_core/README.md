# arrowbricks_core

Experimental Rust reimplementation of [arrowbricks](https://github.com/bmsuisse/arrowbricks)'
hot path -- statement submit/poll, bounded-concurrency chunk fetch, the
`chunk_index` reorder buffer, and Arrow-IPC decode via
[arrow-rs](https://github.com/apache/arrow-rs) -- exposed to Python as an
async `Client` via [PyO3](https://pyo3.rs).

**Not yet wired into `arrowbricks` itself, and not yet published to PyPI.**
This is only the eager full-table-fetch path: it has no lazy `fetchmany`
(the whole result is fetched and assembled before `execute_arrow` returns),
no `token_provider` callback, no JSON result format, no `execute_streamed`
heartbeats, and no volume-file operations. See the parent repo's
[AGENTS.md](../../AGENTS.md) for the state of the wider migration.

## Build

```bash
uvx maturin develop --uv --release
```

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

Because `execute_arrow` fetches the whole result eagerly (no lazy per-chunk
pull yet, unlike arrowbricks' own `stream_query_json`), streaming here means
sending the *already-fetched* result to the HTTP client in per-row chunks --
bounding client-side memory/parse latency on a large result -- not hiding a
slow Databricks warehouse cold start the way arrowbricks' `HEARTBEAT`-based
streaming does:

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

## Testing

```bash
cargo test --no-default-features
```

(`--no-default-features` disables `extension-module`, which otherwise skips
linking against libpython -- correct for the cdylib maturin ships, wrong for
a plain `cargo test` binary. See `Cargo.toml`'s `[features]` comment.)

## License

MIT
