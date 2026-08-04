"""Same as fastapi_sse.py, but validates the caller-supplied SQL with
sqlglot before it ever reaches Databricks -- fastapi_sse.py's own docstring
says plainly not to expose that example's route publicly without doing this.
This is one way to do it, not the only one -- a fixed-query-name allowlist
(no arbitrary SQL accepted at all) is simpler and safer if your use case
allows it. Requires `pip install arrowbricks fastapi uvicorn sqlglot`.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    uvicorn examples.fastapi_sse_validated:app --reload

Then, in another terminal:

    curl -N "http://localhost:8000/query?sql=SELECT+*+FROM+my_catalog.my_schema.my_table+LIMIT+10"
    curl -N "http://localhost:8000/query?sql=DROP+TABLE+my_catalog.my_schema.my_table"        # 422: not a SELECT
    curl -N "http://localhost:8000/query?sql=SELECT+*+FROM+my_catalog.my_schema.secret_table"  # 422: table not allowed
"""

import os
from collections.abc import AsyncIterator

import sqlglot
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from sqlglot import exp

from arrowbricks import HEARTBEAT, DatabricksClient

app = FastAPI()

client = DatabricksClient(
    host=os.environ["DATABRICKS_HOST"],
    warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
    token=os.environ["DATABRICKS_TOKEN"],
)

# Fully-qualified (catalog.schema.table) names this endpoint may read from.
# Adjust to your own workspace -- this allowlist is the actual security
# boundary, not the "is it a SELECT" check below (that alone doesn't stop
# someone reading a table they shouldn't).
ALLOWED_TABLES = {
    "my_catalog.my_schema.my_table",
    "my_catalog.my_schema.big_table",
}


def validate_sql(sql: str) -> None:
    """Raises HTTPException(422) unless `sql` is exactly one read-only
    SELECT touching only allowlisted, fully-qualified tables. Parses with
    Databricks' own SQL dialect (not ANSI) -- catches real syntax Databricks
    accepts that a generic parser might reject or mis-scope tables for."""
    try:
        statements = sqlglot.parse(sql, dialect="databricks")
    except sqlglot.errors.ParseError as e:
        raise HTTPException(422, f"could not parse SQL: {e}") from e

    if len(statements) != 1 or statements[0] is None:
        raise HTTPException(422, "exactly one statement is allowed")

    statement = statements[0]
    if not isinstance(statement, exp.Select):
        raise HTTPException(422, "only SELECT statements are allowed")

    # Positional (found via find_all, so this also catches tables named
    # inside a subquery or JOIN -- not just the top-level FROM) -- a
    # bare table name with no catalog/schema resolves against Databricks'
    # own session catalog/schema context, so it's rejected rather than
    # guessing which workspace default it'd actually hit.
    tables = {t.sql(dialect="databricks", identify=False).lower() for t in statement.find_all(exp.Table)}
    unqualified = {t for t in tables if t.count(".") != 2}
    if unqualified:
        raise HTTPException(422, f"table(s) must be fully qualified (catalog.schema.table): {unqualified}")
    disallowed = tables - ALLOWED_TABLES
    if disallowed:
        raise HTTPException(422, f"table(s) not allowed: {disallowed}")


async def _sse(sql: str) -> AsyncIterator[str]:
    async for item in client.stream_query_json(sql, total_timeout_s=300):
        if item is HEARTBEAT:
            yield ": keep-alive\n\n"
        else:
            yield f"data: {item}\n\n"


@app.get("/query")
async def query(sql: str) -> StreamingResponse:
    validate_sql(sql)
    return StreamingResponse(_sse(sql), media_type="text/event-stream")
