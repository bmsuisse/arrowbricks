"""Fetch a Databricks query via the Rust core, then stream the result to a
client as Server-Sent Events -- as soon as each chunk's rows are ready, not
after the whole result is materialized. `Client.stream_ndjson_lines` yields
the `HEARTBEAT` singleton while waiting on the statement or any individual
chunk (bridging a possible multi-minute cold warehouse start without going
silent), then a `list[str]` of already-formatted NDJSON lines per chunk in
logical order -- decode and JSON encoding both happen in Rust, so there's no
Arrow-to-Python conversion step, and no extra dependency beyond arrowbricks
itself.

Requires `pip install fastapi uvicorn` on top of arrowbricks.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    uvicorn examples.fastapi_sse:app --reload

Then, in another terminal:

    curl -N "http://localhost:8000/query?sql=SELECT+*+FROM+range(1000000)"

This example takes `sql` straight from the request for brevity -- validate/
allowlist it before exposing a route like this publicly.
"""

import os
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from arrowbricks import _core

app = FastAPI()

client = _core.Client(
    host=os.environ["DATABRICKS_HOST"],
    warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
    token=os.environ["DATABRICKS_TOKEN"],
)


async def _sse(sql: str) -> AsyncIterator[str]:
    async for item in client.stream_ndjson_lines(sql, total_timeout_s=300):
        if item is _core.HEARTBEAT:
            yield ": keep-alive\n\n"
            continue
        for line in item:
            yield f"data: {line}\n\n"


@app.get("/query")
async def query(sql: str) -> StreamingResponse:
    return StreamingResponse(_sse(sql), media_type="text/event-stream")
