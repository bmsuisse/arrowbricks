"""Stream a large result to a client over SSE with ONE combined heartbeat/
timeout budget spanning both phases: waiting for the statement to complete
(execute_streamed) AND downloading every chunk afterwards (fetchall_streamed)
-- the two-phase composition `fastapi_sse.py`'s simpler stream_query_json
example doesn't need, since that one streams rows as chunks arrive rather
than buffering a full result first.

Splitting the timeout into two separately-clocked halves (each given the
same total_timeout_s) would let a pathological case run up to 2x the
intended ceiling -- tracking one deadline and passing the *remaining* budget
into the second phase keeps it as one real ceiling across both.

Requires `pip install arrowbricks fastapi uvicorn`.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    uvicorn examples.fastapi_sse_pivot:app --reload

Then, in another terminal:

    curl -N "http://localhost:8000/pivot?sql=SELECT+*+FROM+range(1000000)"

Same caveat as fastapi_sse.py: `sql` comes straight from the request for
brevity -- validate/allowlist it in a real deployment.
"""

import asyncio
import os
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from arrowbricks import HEARTBEAT, connect
from arrowbricks.cursor import Cursor

app = FastAPI()

conn = connect(
    host=os.environ["DATABRICKS_HOST"],
    warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
    token=os.environ["DATABRICKS_TOKEN"],
)

_TOTAL_TIMEOUT_S = 300.0


async def _fetch_all_rows_with_heartbeat(cursor: Cursor, deadline: float) -> AsyncIterator[object]:
    """Yields HEARTBEAT while downloading, then the final list[Row] -- bounded
    by whatever's left of the shared deadline, not a fresh full timeout."""
    loop = asyncio.get_running_loop()
    remaining = max(deadline - loop.time(), 0.0)
    async for item in cursor.fetchall_streamed(total_timeout_s=remaining):
        yield item


async def _sse(sql: str) -> AsyncIterator[str]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _TOTAL_TIMEOUT_S
    cursor = conn.cursor()

    async for item in cursor.execute_streamed(sql, total_timeout_s=_TOTAL_TIMEOUT_S):
        if item is HEARTBEAT:
            yield ": keep-alive\n\n"
            continue
        async for fetch_item in _fetch_all_rows_with_heartbeat(cursor, deadline):
            if fetch_item is HEARTBEAT:
                yield ": keep-alive\n\n"
            else:
                for row in fetch_item:
                    yield f"data: {row}\n\n"
    yield "event: end\ndata: {}\n\n"


@app.get("/pivot")
async def pivot(sql: str) -> StreamingResponse:
    return StreamingResponse(_sse(sql), media_type="text/event-stream")
