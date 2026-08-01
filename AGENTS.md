# arrowbricks

Runs SQL against a Databricks SQL warehouse via the Statement Execution API and hands the result back as Arrow (a `Cursor` shaped like `databricks-sql-python`'s) or streaming NDJSON. See `README.md` for the user-facing API; this file is about working *on* the package.

## Layout

- `src/arrowbricks/client.py` -- pure REST client (auth, statement submission/polling, backpressure-bounded concurrent chunk download). No Arrow dependency at all, intentionally: someone who only wants `execute_json_statement` or `upload_volume_file`/`delete_volume_file` shouldn't need arro3 pulled in conceptually either, even though it's a hard dependency of the package as a whole. Retries are a small hand-rolled `_retry_call` loop, not a dependency (see "Design invariants" below).
- `src/arrowbricks/_streaming.py` -- Arrow (de)serialization via arro3, always: `ReplayableArrowChunk`, `write_ipc_stream` (always uncompressed, see below), heartbeat helpers, chunk fetching, and `stream_query_json` (arro3's `write_ndjson`). One Arrow engine, no pluggable backend -- unlike duckbricks, there's nothing here to make pluggable; arro3 *is* the whole point.
- `src/arrowbricks/cursor.py` -- `Connection`/`Cursor`, the DB-API-ish surface (`execute`/`execute_streamed`, `fetchone`/`fetchmany`/`fetchall`, `fetchall_arrow`/`fetchmany_arrow`). `_ResultSet` buffers at the Arrow-Table level (not materialized Python rows) so the Arrow-native fetch methods stay zero-copy; row-based fetches materialize lazily off that buffer.
- `tests/` -- respx mocks the Databricks REST endpoints (warehouse status, statement submit, chunk-link resolution, external-link byte download); no real warehouse or credentials needed to run the suite.

## Commands

```bash
uv sync --all-extras
uv run pytest -q
uv run ruff check .
uv run ty check src tests
```

One-time setup per clone: `prek install` (needs `uv tool install prek` first if not already on PATH).

## Design invariants -- don't casually undo these

- **No cloud-SDK dependency.** Auth is `token: str` or `token_provider: Callable[[], str | Awaitable[str]]`. Do not add `azure-identity`/`boto3`/etc. as a real dependency -- that belongs in the caller's app.
- **No hardcoded catalog/schema.** `catalog`/`schema` default to `None` everywhere. This package has zero knowledge of any specific Databricks workspace's naming.
- **Chunk order is not fetch order.** `DatabricksClient` fetches chunks concurrently (bounded, with backpressure) and they can complete out of order. `_ResultSet`/`stream_query_json` both hold a `pending: dict[int, chunk]` reorder buffer keyed by `chunk_index`, releasing in order as the next expected index shows up. If you touch either, keep a test proving order survives out-of-order arrival (see `test_fetchall_preserves_order_despite_out_of_order_chunks`, `test_stream_query_json_preserves_order_despite_out_of_order_chunks`).
- **Chunks are fetched lazily, not all upfront.** `_ResultSet` only pulls the next chunk from `_chunk_aiter` when the caller's `fetchone`/`fetchmany`/`fetchall` actually needs more rows than are already buffered. Don't "simplify" this into draining the whole chunk iterator inside `execute()` -- that defeats the point of a paginated cursor.
- **No silent row caps.** There's no `ABSOLUTE_ROW_LIMIT`-style ceiling baked in. If a caller wants one, that's `row_limit`, which they pass explicitly.
- **No retry dependency.** `client.py`'s `_retry_call` is a ~10-line hand-rolled exponential-backoff loop, replacing tenacity on purpose -- it's the only retry pattern in the whole client, so a dependency for it wasn't worth it.
- **One Arrow engine, no pluggable backend.** Unlike duckbricks (which supports nanoarrow *or* arro3 *or* bring-your-own), arrowbricks is arro3-only by design -- that's the entire "single responsibility" pitch. Don't add a backend-abstraction layer back in; if a caller needs a different Arrow engine, that's duckbricks' `set_arrow_backend()`, not this package.
- **`write_ipc_stream` (and everything built on it) always writes uncompressed Arrow-IPC bodies.** `aio.write_ipc_stream(..., compression=None)` explicitly, everywhere. arro3's own default (`compression="LZ4"`) is transparently decompressed by DuckDB's Arrow reader but not necessarily by other Arrow IPC readers -- `duckdb-wasm`'s browser-side decoder silently fails to parse LZ4-compressed bodies (this was a real bug in duckbricks 0.3.0, fixed in 0.3.1 -- see its CHANGELOG/git history). Never remove the explicit `compression=None`.

## Testing

Mock the Databricks endpoints with `respx` (see `tests/conftest.py`'s `mock_warehouse` fixture) rather than hitting a real warehouse. The fixture builds real Arrow-IPC chunk bytes via arro3 directly, so tests exercise the actual Arrow IPC round trip, not a stand-in. Pass `reverse_arrival=True` to force genuine out-of-order chunk completion when a test needs to prove ordering survives it.

## Releasing

1. Bump `version` in `pyproject.toml`.
2. `git tag vX.Y.Z && git push origin vX.Y.Z`.
3. `.github/workflows/release.yml` runs the test job, then builds and publishes to PyPI via trusted publishing (OIDC) -- no stored token.

One-time, outside this repo: register this GitHub repo + `release.yml` workflow as a **trusted publisher** on the `arrowbricks` PyPI project (PyPI project settings -> Publishing). Without that, the `publish` job's OIDC exchange fails even though tests pass.
