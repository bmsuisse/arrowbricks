# arrowbricks_core

Rust implementation of [arrowbricks](https://github.com/bmsuisse/arrowbricks)'
hot path -- statement submit/poll, bounded-concurrency chunk fetch, the
`chunk_index` reorder buffer, Arrow-IPC decode/write, and NDJSON encode via
[arrow-rs](https://github.com/apache/arrow-rs) -- exposed to Python via
[PyO3](https://pyo3.rs). This crate builds into the same wheel as the
top-level `arrowbricks` package, as its compiled submodule `arrowbricks._core`
(see the repo root `pyproject.toml`'s `[tool.maturin]`) -- it is not published
to PyPI on its own. `arrowbricks`'s own `Cursor`/`DatabricksClient`/
`stream_query_json` all delegate to it directly; this README is for working
with the compiled extension on its own terms, or on the crate itself.

## Build

From the repo root (this crate has no `pyproject.toml` of its own -- the root
one's `manifest-path` points back at this directory's `Cargo.toml`):

```bash
uvx maturin develop --uv --release
```

This builds a mixed Python/Rust project: the compiled extension lands at
`src/arrowbricks/_core.<abi3>.so`, alongside `src/arrowbricks/_core.pyi` and
`src/arrowbricks/py.typed` (PEP 561) for editor autocomplete and
`mypy`/`pyright`/`ty` type-checking -- a compiled PyO3 extension has no type
info of its own without them.

## Quickstart

Importable directly (bypassing the `arrowbricks` Python wrapper) as
`arrowbricks._core`:

```python
import asyncio
from arrowbricks import _core


async def main():
    client = _core.Client(
        host="adb-1234567890.1.azuredatabricks.net",
        warehouse_id="abcd1234efgh5678",
        token="dapi...",
    )
    table = await client.execute_arrow("SELECT * FROM my_catalog.my_schema.my_table LIMIT 100")
    print(table.num_rows)


asyncio.run(main())
```

`table` implements the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
(`__arrow_c_stream__`/`__arrow_c_array__`) -- any consumer that speaks that
protocol (DuckDB, pyarrow, arro3) can import it with zero copies, no extra
dependency needed. Note: `table.column(i)`/`table.schema` (materializing
native Python values) are `pyo3-arrow` methods designed to hand back the
caller's own *real* `arro3.core` objects -- they need `arro3-core` installed,
unlike the table object itself.

## API

- `Client(host, warehouse_id, *, token=None, token_provider=None, chunk_fetch_concurrency=32, http_timeout=60.0, wait_timeout="30s", warehouse_start_timeout=300.0, warehouse_confirmed_running_ttl_s=30.0)` -- exactly one of `token`/`token_provider`. `token_provider` is a callable (sync or async) returning a token string, called fresh on every request, no caching.
- `Client.execute_arrow(statement, *, catalog=None, schema=None, parameters=None) -> Table` -- eager: fetches and assembles the whole result before returning. `parameters` is Databricks' own named-parameter format (`[{"name":..., "value":..., "type":...}]`), passed straight through.
- `Client.execute(statement, *, catalog=None, schema=None, parameters=None) -> ResultSet` -- submits and starts background chunk fetching without pulling anything yet.
  - `ResultSet.fetchmany_arrow(n) -> Table` -- pulls/decodes only as many chunks as needed for `n` rows, buffering the rest; may return fewer than `n` once exhausted.
  - `ResultSet.fetchall_arrow() -> Table` -- drains everything remaining.
  - `ResultSet.schema() -> list[tuple[str, str]] | None` -- the real decoded Arrow schema as `(name, type_name)` string pairs, once known (after >=1 fetch); `None` before that. Computed directly in Rust -- unlike a `Table`'s own `.schema`, this needs no arro3 install.
  - `ResultSet.statement_id`, `ResultSet.num_chunks`, `ResultSet.columns` (manifest-based pre-fetch schema estimate, also arro3-free).
- `Client.execute_json(statement, *, catalog=None, schema=None, parameters=None) -> list[list[str | None]]` -- JSON_ARRAY format, no Arrow parse. Every non-null value comes back as a string regardless of real column type (Databricks' own contract) -- cast by the manifest's column type yourself if you want native Python types.
- `Client.stream_ndjson_lines(statement, *, catalog=None, schema=None, parameters=None, total_timeout_s=None)` -- chunk-at-a-time: an async iterator yielding the `HEARTBEAT` singleton while waiting on the statement or any individual chunk, then a `list[str]` of NDJSON lines (one per row, explicit nulls, ISO-8601 timestamps) per chunk in logical order. Decode and JSON encoding both happen in Rust -- backs `stream_query_json` end to end.
- `Client.upload_volume_file(volume_path, data: bytes)` / `Client.delete_volume_file(volume_path)` -- Unity Catalog volume files via the Files API. Delete treats a 404 as success (idempotent). Both raise a plain `RuntimeError` (message only) on failure.
- `Client.execute_streamed(statement, *, catalog=None, schema=None, parameters=None, total_timeout_s=None)` -- like `execute()`, but an async iterator yielding the `HEARTBEAT` singleton while waiting on Databricks instead of blocking silently (bridge e.g. an SSE connection through a slow warehouse cold start), then a `ResultSet` exactly once. Raises if `total_timeout_s` elapses first.
  - `ResultSet.fetchall_arrow_streamed(*, total_timeout_s=None)` -- same idea for the chunk-download phase: yields `HEARTBEAT` while pulling chunks, then a `Table` exactly once.
- `write_ipc_stream(stream, buf)` -- free function; writes any object implementing `__arrow_c_stream__` (a `Table` from this crate, arro3, pyarrow, ...) as uncompressed Arrow-IPC stream bytes to a Python file-like object. No dependency needed regardless of the input's origin.
- `read_ipc_stream(data: bytes) -> Table` -- free function; the exact inverse of `write_ipc_stream`, parsing raw Arrow-IPC stream bytes back into a `Table`. No dependency needed regardless of where the bytes came from -- backs `arrowbricks.ReplayableArrowChunk`, which needs to re-parse the same cached bytes on every `__arrow_c_stream__` call.
- `HEARTBEAT` -- module-level singleton; compare with `is`, e.g. `if item is _core.HEARTBEAT: ...`.

## With DuckDB

DuckDB's `duckdb.sql()` resolves an unrecognized table name against local
Python variables via a replacement scan -- point it at the Arrow object
directly, no pandas/pyarrow conversion step, no extra dependency:

```python
import duckdb

result = await client.execute_arrow("SELECT * FROM my_catalog.my_schema.my_table")
print(duckdb.sql("SELECT count(*) FROM result").fetchall())
```

Runnable version: [`examples/duckdb_query.py`](examples/duckdb_query.py).

## With FastAPI (Server-Sent Events)

The simplest streaming path is `stream_ndjson_lines`, which yields
ready-to-send NDJSON lines directly (see `arrowbricks.stream_query_json`,
built on exactly this):

```python
import json
from arrowbricks import _core

async def _sse(sql: str):
    async for item in client.stream_ndjson_lines(sql, total_timeout_s=300):
        if item is _core.HEARTBEAT:
            yield ": keep-alive\n\n"
            continue
        for line in item:
            yield f"data: {line}\n\n"
```

Runnable version: [`examples/fastapi_sse.py`](examples/fastapi_sse.py).

## Testing

```bash
cargo test --no-default-features
```

(`--no-default-features` disables `extension-module`, which otherwise skips
linking against libpython -- correct for the cdylib maturin ships, wrong for
a plain `cargo test` binary. See `Cargo.toml`'s `[features]` comment.)

`tests_py/` is this crate's own PyO3-level Python test suite (against the
actually-built extension) -- run explicitly by path (`uv run pytest
rust/arrowbricks_core/tests_py -v` from the repo root), not auto-discovered
by a bare `pytest`.

## License

MIT
