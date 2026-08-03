"""Fetch a Databricks query via the Rust core, then stream the result to a
client as Server-Sent Events.

Note the difference from arrowbricks' own examples/fastapi_sse.py:
Client.execute_arrow fetches the *entire* result eagerly -- there's no lazy
per-chunk pull yet (see rust/arrowbricks_core's current-limitations note in
the PR/README), so the first byte reaches the client only after the whole
query has finished fetching from Databricks, not after the first chunk.
What streams here is the already-materialized result being sent to the HTTP
client in per-row chunks instead of one giant JSON response -- useful for
bounding client-side memory/parse latency on a big result, not for hiding a
slow Databricks warehouse cold start.

Requires `pip install fastapi uvicorn` on top of arrowbricks_core.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    uvicorn examples.fastapi_sse:app --reload

Then, in another terminal:

    curl -N "http://localhost:8000/query?sql=SELECT+*+FROM+range(1000000)"

This example takes `sql` straight from the request for brevity -- validate/
allowlist it before exposing a route like this publicly.
"""

import asyncio
import json
import os
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI
from fastapi.responses import StreamingResponse

import arrowbricks_core

app = FastAPI()

client = arrowbricks_core.Client(
    host=os.environ["DATABRICKS_HOST"],
    warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
    token=os.environ["DATABRICKS_TOKEN"],
)


def _rows(table: Any) -> list[dict[str, Any]]:
    names = [f.name for f in table.schema]
    columns = [table.column(i).combine_chunks().to_pylist() for i in range(table.num_columns)]
    return [dict(zip(names, row, strict=True)) for row in zip(*columns, strict=True)]


async def _sse(sql: str) -> AsyncIterator[str]:
    table = await client.execute_arrow(sql)
    for row in _rows(table):
        yield f"data: {json.dumps(row)}\n\n"
        await asyncio.sleep(0)  # yield to the event loop between rows


@app.get("/query")
async def query(sql: str) -> StreamingResponse:
    return StreamingResponse(_sse(sql), media_type="text/event-stream")
