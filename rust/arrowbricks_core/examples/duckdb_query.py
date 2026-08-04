"""Query Databricks via the Rust core, then hand the result straight to
DuckDB -- zero-copy, no intermediate row materialization. The Table
`arrowbricks._core` returns implements the Arrow C Data Interface
(`__arrow_c_stream__`), the same protocol pyarrow/arro3 use, so DuckDB's own
replacement scan can read it directly by variable name. No extra dependency
beyond `arrowbricks` itself.

Requires `pip install duckdb` on top of arrowbricks.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    python examples/duckdb_query.py
"""

import asyncio
import os

import duckdb

from arrowbricks import _core


async def main() -> None:
    client = _core.Client(
        host=os.environ["DATABRICKS_HOST"],
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
    result = await client.execute_arrow("SELECT * FROM my_catalog.my_schema.my_table LIMIT 1000000")  # noqa: F841 -- read by DuckDB's replacement scan via local variable name, not a literal Python reference

    # `result` is queryable by variable name -- DuckDB imports it zero-copy,
    # no pandas/pyarrow conversion step in between.
    print(duckdb.sql("SELECT count(*) FROM result").fetchall())


asyncio.run(main())
