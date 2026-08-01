"""Shared fixtures: a fake Databricks SQL warehouse mocked via respx, so the
suite runs with zero real network calls or credentials.
"""

from __future__ import annotations

import asyncio
import io

import arro3.core as core
import arro3.io as aio
import httpx
import pytest
import respx

HOST = "https://fake-workspace.cloud.databricks.com"
WAREHOUSE_ID = "wh-test-123"


def build_chunk_bytes(lo: int, hi: int) -> bytes:
    """Synthesizes one chunk's Arrow-IPC bytes via arro3 directly -- an id
    column running lo..hi-1 plus a label column -- so tests exercise the real
    Arrow IPC round trip without a live Databricks connection or DuckDB."""
    ids = list(range(lo, hi))
    id_col = core.Array(ids, type=core.DataType.int64())
    label_col = core.Array([f"row_{i}" for i in ids], type=core.DataType.string())
    table = core.Table.from_pydict({"id": id_col, "label": label_col})
    buf = io.BytesIO()
    aio.write_ipc_stream(table, buf, compression=None)
    return buf.getvalue()


@pytest.fixture
def warehouse_host_id() -> tuple[str, str]:
    return HOST, WAREHOUSE_ID


@pytest.fixture
def chunk_bytes_builder():
    return build_chunk_bytes


@pytest.fixture
def mock_warehouse():
    """Registers respx routes for a fake warehouse with `n_chunks` chunks of
    `rows_per_chunk` rows each (id column running 0..n_chunks*rows_per_chunk).
    Callers configure it via mock_warehouse(...) inside a `with respx.mock:`
    block (or use the `respx_router` param)."""

    def _install(
        router: respx.Router, n_chunks: int, rows_per_chunk: int, *, reverse_arrival: bool = False
    ) -> respx.Route:
        statement_id = "stmt-abc"
        chunks = [{"chunk_index": i, "row_count": rows_per_chunk} for i in range(n_chunks)]
        chunk_bytes = {i: build_chunk_bytes(i * rows_per_chunk, (i + 1) * rows_per_chunk) for i in range(n_chunks)}

        warehouse_route = router.get(f"{HOST}/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(
            return_value=httpx.Response(200, json={"state": "RUNNING"})
        )
        router.post(f"{HOST}/api/2.0/sql/statements").mock(
            return_value=httpx.Response(
                200,
                json={
                    "statement_id": statement_id,
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {
                        "chunks": chunks,
                        "schema": {
                            "columns": [
                                {"name": "id", "type_name": "LONG"},
                                {"name": "label", "type_name": "STRING"},
                            ]
                        },
                    },
                },
            )
        )

        def resolve_chunk(request: httpx.Request) -> httpx.Response:
            idx = int(str(request.url).rsplit("/", 1)[-1])
            return httpx.Response(200, json={"external_links": [{"external_link": f"{HOST}/_data/chunk-{idx}"}]})

        router.get(url__regex=rf"{HOST}/api/2\.0/sql/statements/{statement_id}/result/chunks/\d+").mock(
            side_effect=resolve_chunk
        )

        async def serve_chunk_bytes(request: httpx.Request) -> httpx.Response:
            idx = int(str(request.url).rsplit("-", 1)[-1])
            if reverse_arrival:
                await asyncio.sleep((n_chunks - idx) * 0.01)
            return httpx.Response(200, content=chunk_bytes[idx])

        router.get(url__regex=rf"{HOST}/_data/chunk-\d+").mock(side_effect=serve_chunk_bytes)
        return warehouse_route

    return _install


@pytest.fixture
def mock_volume_files():
    """Registers respx routes for the Files API's PUT (upload)/DELETE
    endpoints (DatabricksClient.upload_volume_file/delete_volume_file) --
    kept separate from mock_warehouse since it's a different API surface
    entirely (no statement/manifest/chunk shape to it). Returns the
    (put_route, delete_route) respx.Route objects so a test can assert on
    call_count/request bodies."""

    def _install(router: respx.Router, host: str) -> tuple[respx.Route, respx.Route]:
        put_route = router.put(url__regex=rf"{host}/api/2\.0/fs/files/.*").mock(return_value=httpx.Response(204))
        delete_route = router.delete(url__regex=rf"{host}/api/2\.0/fs/files/.*").mock(return_value=httpx.Response(204))
        return put_route, delete_route

    return _install
