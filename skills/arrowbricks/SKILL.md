---
name: arrowbricks
description: Use when querying a Databricks SQL warehouse with Arrowbricks, preparing bounded agent previews, or interpreting its Markdown results and JSON errors.
---

# Arrowbricks

Use Arrowbricks 6.0+ for a bounded SQL preview. The CLI and async Python API
format Arrow directly in Rust; no optional Python Arrow packages are needed.

## Execute

Use the existing `DATABRICKS_HOST`, `DATABRICKS_WAREHOUSE_ID` and
`DATABRICKS_TOKEN` environment settings. Verify presence without printing
values; obtain missing configuration through the user's credential mechanism.
Keep tokens out of commands, logs and generated artifacts.

```bash
arrowbricks --agent -c 'SELECT 1 AS answer' \
  --max-rows 20 --max-bytes 8000 --max-cell-chars 500 --total-timeout-s 60
```

`python -m arrowbricks` is equivalent. Agent output is the default; `--agent`
and `-agent` are optional. `-c`/`--sql`/`--command` sends SQL unchanged;
`-f`/`--file` reads a UTF-8 SQL file (`-` reads stdin). Piped stdin supplies SQL
when neither is given. `--catalog` and `--schema` set the namespace; use
`--protocol sea` only when REST is desired (default: `thrift`). Check `--help`
for the installed CLI. Use a 6.0+ wheel for an unreleased checkout; once a
compatible release is published, install with `pip install 'arrowbricks>=6.0'`.

Preserve supplied SQL. Set preview budgets through arguments, without adding
`LIMIT`, wrapping SQL, removing comments or changing predicates. Execute only
statements within the user's authorized task.

The Python equivalent returns one string, including JSON failures:

```python
import asyncio
import os
from arrowbricks import DatabricksClient

async def preview(sql):
    async with DatabricksClient(
        os.environ["DATABRICKS_HOST"], os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
    ) as client:
        return await client.query_agent(
            sql, max_rows=20, max_bytes=8000, max_cell_chars=500,
            total_timeout_s=60, catalog=None, schema=None, params=None,
        )

result = asyncio.run(preview("SELECT 1 AS answer"))
```

`params` accepts named `{"name": ..., "value": ..., "type": ...}` objects.
Never f-string values into SQL: on Python 3.14+ put them in a t-string
(`t"... WHERE id = {user_id}"`, bound as parameters, `:i` for identifiers) or use `params`.
`arrowbricks.unsafe_sql(text)` is only for SQL you vouch for.
Defaults are 1000 rows, 10000 UTF-8 output bytes, 500 characters per cell and
60 seconds. Row/cell limits are positive; bytes are at least 256; timeout is
finite and non-negative. The deadline spans submission, fetching and rendering. Stopping
requests best-effort server cancellation; Python task cancellation stays
`asyncio.CancelledError`.

## Interpret

| Signal | Meaning |
| --- | --- |
| CLI exit 0, Markdown stdout | Query succeeded; inspect its footer. |
| `complete=false` | Preview is incomplete, including cell clipping. |
| `total_rows=N` / `total_rows>=N` | Exact exhausted total / fetched lower bound. |
| `cells_truncated=true`, `…[truncated]` | Values were clipped; do not treat them as full values. |
| CLI exit 1, JSON stderr | Query failed; inspect `category`, `message`, `next_step`. |
| CLI exit 2 / 130, JSON stderr | Configuration error / interruption. |

Typed headers retain column order and duplicate names. SQL NULL is unquoted;
the string `"NULL"` is quoted. Markdown/HTML escaping is presentation, not
changed data. An error after fetching rows is failure, not a successful preview.

A preview cap does not limit warehouse scanning or cost. Pre-execution cost
estimates and percentage progress are unavailable; do not invent them or drain
the result to count/hash it. Persistent result paging and last-row retrieval
are not offered by this API.

## Recover

Preserve database diagnostics and suggestions when reporting a failure. For
approved exact SQL, propose a correction instead of substituting it silently.
Within authorized read-only repair scope, allow at most one evidence-backed
correction and one retry; state the changed SQL. Resolve names from diagnostics
or explicitly requested metadata, not guessed schema/catalog values. Stop if
the correction fails or needs information or access outside the task.

Authentication/permission errors need credentials/access resolution.
Transient/timeouts permit at most one deliberate retry of an authorized
read-only query. Replaying a write after an ambiguous failure requires resolving
whether it executed first. Output limits alone do not authorize broader queries.
