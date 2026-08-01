"""Page through a large result with the Cursor's fetchmany/fetchmany_arrow --
chunks are only fetched from Databricks as you actually consume them, not all
upfront, so this never buffers more than a few chunks' worth of rows at once
regardless of how big the underlying result is.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    python examples/cursor_paging.py
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
    await cursor.execute("SELECT * FROM range(1000000) AS t(id)")

    seen = 0
    while True:
        rows = await cursor.fetchmany(50_000)
        if not rows:
            break
        seen += len(rows)
        print(f"...{seen} rows so far, last id in this page: {rows[-1][0]}")

    # fetchall_arrow/fetchmany_arrow return an arro3 Table -- zero-copy, no
    # Python-tuple materialization, if that's all you need downstream (e.g.
    # writing Parquet, or handing it to another Arrow-aware library).
    await cursor.execute("SELECT * FROM range(3) AS t(id)")
    table = await cursor.fetchall_arrow()
    print(table.column_names, table.num_rows)


if __name__ == "__main__":
    asyncio.run(main())
