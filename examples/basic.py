"""Run a query against a Databricks SQL warehouse using a static personal
access token (or any other pre-issued OAuth token).

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXX \\
    python examples/basic.py
"""

import asyncio
import os

from arrowbricks import connect


async def main() -> None:
    conn = connect(
        host=os.environ["DATABRICKS_HOST"],
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
    cursor = conn.cursor()

    await cursor.execute("SELECT 1 AS n, 'hello' AS greeting")
    print(await cursor.fetchall())  # [(1, "hello")]

    await cursor.execute("SELECT * FROM range(5)")
    async for row in cursor:
        print(row)

    # stream_query_json works off the lower-level DatabricksClient directly --
    # no Cursor needed, since it streams rows as they arrive rather than
    # buffering a result set to page through. `conn.client` is the same
    # client `cursor()` uses.
    async for row_json in conn.client.stream_query_json("SELECT * FROM range(5)"):
        print(row_json)


if __name__ == "__main__":
    asyncio.run(main())
