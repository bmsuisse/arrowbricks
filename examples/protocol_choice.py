"""Pick a protocol explicitly, and see why `protocol="thrift"` is the
default: `SELECT ... LIMIT n` for a few values of `n`, once per protocol,
timed. Thrift's `getDirectResults` returns a small result inline in the
same RPC that submits the statement -- SEA always needs a separate
poll/fetch round trip -- so thrift wins small queries most clearly; both
converge for larger ones. See AGENTS.md's "Thrift is now the default
protocol" entry for the full real-warehouse numbers this default was
based on.

`protocol="sea"` remains a fully-supported, explicit opt-in -- reach for
it if your deployment specifically needs Databricks' newer REST-based
Statement Execution API surface (e.g. a policy requiring only that API,
independent of which one is faster here).

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    python examples/protocol_choice.py
"""

import asyncio
import os
import time

from arrowbricks import connect


async def timed_query(protocol: str, limit: int) -> float:
    conn = connect(
        host=os.environ["DATABRICKS_HOST"],
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
        protocol=protocol,  # "thrift" (the default -- shown explicitly here) or "sea"
    )
    cursor = conn.cursor()
    t0 = time.perf_counter()
    await cursor.execute(f"SELECT * FROM range({limit})")  # noqa: S608 -- limit is always a hardcoded int from main()'s own tuple, never external input
    await cursor.fetchall_arrow()
    elapsed = time.perf_counter() - t0
    await conn.close()
    return elapsed


async def main() -> None:
    for limit in (10, 10_000, 500_000):
        thrift_s = await timed_query("thrift", limit)
        sea_s = await timed_query("sea", limit)
        print(f"LIMIT {limit:>7,}: thrift={thrift_s:.3f}s  sea={sea_s:.3f}s")


if __name__ == "__main__":
    asyncio.run(main())
