"""Opt-in, read-only end-to-end tests against the configured real warehouse.

ARROWBRICKS_LIVE=1 uv run pytest tests/live -q
Uses DATABRICKS_HOST / WAREHOUSE_ID / TOKEN from the environment or root .env.
All SQL generates synthetic data; no existing tables are read or modified.
"""

import asyncio
import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import arro3.core as core
import pytest

from arrowbricks import HEARTBEAT, ArrowbricksError, QueryTimeout, connect, stream_query_json

pytestmark = pytest.mark.skipif(os.environ.get("ARROWBRICKS_LIVE") != "1", reason="real warehouse opt-in")


@pytest.fixture(params=[("thrift", True), ("sea", True), ("thrift", False), ("sea", False)])
async def connection(request):
    path = Path(__file__).resolve().parents[2] / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
    async with connect(
        os.environ["DATABRICKS_HOST"],
        os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
        protocol=request.param[0],
        compress_results=request.param[1],
        chunk_fetch_concurrency=8,
        retry_attempts=1,
    ) as conn:
        yield conn


async def test_live_paging_and_rows_preserve_every_value(connection):
    cursor = connection.cursor()
    sql = "SELECT id, id * 2 AS doubled, CAST(id AS STRING) AS label FROM range(12000) ORDER BY id"
    await cursor.execute(sql)
    offset = 0
    while True:
        page = core.Table.from_arrow(await cursor.fetchmany_arrow(777))
        if not page.num_rows:
            break
        assert page["id"].to_pylist() == list(range(offset, offset + page.num_rows))
        assert page["doubled"].to_pylist() == [i * 2 for i in range(offset, offset + page.num_rows)]
        offset += page.num_rows
    assert offset == 12000
    await cursor.execute(sql)
    assert await cursor.fetchone() == (0, 0, "0")
    with pytest.raises(ValueError):
        await cursor.fetchmany(-1)
    rows = await cursor.fetchall()
    assert rows == [(i, i * 2, str(i)) for i in range(1, 12000)]


async def test_live_scalar_values_survive_row_conversion(connection):
    cursor = connection.cursor()
    await cursor.execute("""SELECT CAST(-7 AS TINYINT) tiny, CAST(1.25 AS DECIMAL(10,2)) amount,
        DATE '2026-01-02' day, TIMESTAMP_NTZ '2026-01-02 03:04:05.123456' ts,
        unhex('00ff') binary_value, true flag, CAST(NULL AS BIGINT) missing""")
    assert await cursor.fetchall() == [
        (-7, Decimal("1.25"), date(2026, 1, 2), datetime(2026, 1, 2, 3, 4, 5, 123456), b"\x00\xff", True, None)
    ]


async def test_live_empty_result_retains_schema(connection):
    cursor = connection.cursor()
    projection = """CAST(1 AS BIGINT) id, CAST(1.25 AS DECIMAL(10,2)) amount, 'text' label,
        CAST(NULL AS ARRAY<INT>) items,
        CAST(NULL AS STRUCT<`x,y`: ARRAY<DECIMAL(10,2)>>) nested,
        CAST(NULL AS MAP<STRING, BIGINT>) mapping, CAST(NULL AS VOID) nothing"""
    await cursor.execute(f"SELECT {projection}")
    schema = core.Table.from_arrow(await cursor.fetchall_arrow()).schema
    await cursor.execute(f"SELECT {projection} WHERE false")
    empty = core.Table.from_arrow(await cursor.fetchall_arrow())
    assert empty.num_rows == 0
    assert empty.column_names == ["id", "amount", "label", "items", "nested", "mapping", "nothing"]
    assert [field.type for field in empty.schema] == [field.type for field in schema]


async def test_live_ndjson_unicode_nulls_nested_and_nonfinite(connection):
    sql = """SELECT id, 'café 🦀' label, CAST(NULL AS STRING) missing,
        array(id, id + 1) items, named_struct('x', id) nested,
        CASE WHEN id % 2 = 0 THEN CAST('NaN' AS DOUBLE) ELSE 1.5 END value
        FROM range(10001) ORDER BY id"""
    count = 0
    async for line in stream_query_json(connection.client, sql, non_finite_floats="string", total_timeout_s=60):
        if line is HEARTBEAT:
            continue
        row = json.loads(line)
        assert row["id"] == count
        assert row["label"] == "café 🦀"
        assert row["missing"] is None
        # Some protocols serialize complex SQL values as JSON strings in Arrow.
        items = json.loads(row["items"]) if isinstance(row["items"], str) else row["items"]
        nested = json.loads(row["nested"]) if isinstance(row["nested"], str) else row["nested"]
        assert items == [count, count + 1]
        assert nested == {"x": count}
        assert row["value"] == ("NaN" if count % 2 == 0 else 1.5)
        count += 1
    assert count == 10001


async def test_live_concurrent_queries_and_error_recovery(connection):
    async def query(n):
        cursor = connection.cursor()
        await cursor.execute("SELECT :value AS value", parameters=[{"name": "value", "value": str(n), "type": "INT"}])
        assert await cursor.fetchall() == [(n,)]

    await asyncio.gather(*(query(n) for n in range(10)))
    cursor = connection.cursor()
    with pytest.raises(ArrowbricksError):
        await cursor.execute("SELECT * FROM range(1) WHERE definitely_missing_column = 1")
    assert cursor.description is None
    await query(42)


async def test_live_fetch_deadline_and_recovery(connection):
    cursor = connection.cursor()
    await cursor.execute("SELECT id, repeat('x', 256) payload FROM range(200000)")
    with pytest.raises(QueryTimeout):
        async for _ in cursor.fetchall_arrow_streamed(total_timeout_s=0):
            pass
    await cursor.execute("SELECT 42 AS value")
    assert await cursor.fetchall() == [(42,)]
