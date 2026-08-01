"""Stream a Databricks query to a client as Server-Sent Events via FastAPI --
the fastest way to get rows out over HTTP: the first row reaches the client
after roughly one chunk's fetch time, not the whole query's, and a HEARTBEAT
comment keeps the connection alive during a slow warehouse cold start.
Requires `pip install arrowbricks fastapi uvicorn`.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    uvicorn examples.fastapi_sse:app --reload

Then, in another terminal:

    curl -N "http://localhost:8000/query?sql=SELECT+*+FROM+range(1000000)"

This example takes `sql` straight from the request for brevity -- arrowbricks
does no SQL validation by design (see AGENTS.md), so a real deployment must
validate/allowlist it (or accept fixed query names + params) before exposing
a route like this publicly.
"""

import os
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from arrowbricks import HEARTBEAT, DatabricksClient, stream_query_json

app = FastAPI()

client = DatabricksClient(
    host=os.environ["DATABRICKS_HOST"],
    warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
    token=os.environ["DATABRICKS_TOKEN"],
)


async def _sse(sql: str) -> AsyncIterator[str]:
    async for item in stream_query_json(client, sql, total_timeout_s=300):
        if item is HEARTBEAT:
            yield ": keep-alive\n\n"  # SSE comment line -- clients ignore it, it just keeps the connection open
        else:
            yield f"data: {item}\n\n"


@app.get("/query")
async def query(sql: str) -> StreamingResponse:
    return StreamingResponse(_sse(sql), media_type="text/event-stream")
