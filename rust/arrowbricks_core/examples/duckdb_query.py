"""Query Databricks via the Rust core, then hand the result straight to
DuckDB -- zero-copy, no intermediate row materialization. The Table
arrowbricks_core returns implements the Arrow C Data Interface
(`__arrow_c_stream__`), the same protocol pyarrow/arro3 use, so DuckDB's own
replacement scan can read it directly by variable name.

Requires `pip install duckdb` on top of arrowbricks_core.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    python examples/duckdb_query.py
"""

import asyncio
import os

import duckdb

import arrowbricks_core


async def main() -> None:
    client = arrowbricks_core.Client(
        host=os.environ["DATABRICKS_HOST"],
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
    result = await client.execute_arrow("SELECT * FROM my_catalog.my_schema.my_table LIMIT 1000000")

    # `result` is queryable by variable name -- DuckDB imports it zero-copy,
    # no pandas/pyarrow conversion step in between.
    print(duckdb.sql("SELECT count(*) FROM result").fetchall())


asyncio.run(main())
