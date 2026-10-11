<p align="center">
  <img src="assets/logo.svg" width="72" height="72" alt="arrowbricks logo">
</p>

<h1 align="center">arrowbricks</h1>
<p align="center">Fast Databricks SQL for Python applications and AI agents.</p>

- **Zero required Python dependencies.** One precompiled wheel. No Rust toolchain or cloud SDK needed.
- **Fast Rust core.** Concurrent chunk downloads, lazy Arrow decoding, and streaming NDJSON, with result order preserved.
- **Agent mode, new in 6.0.** A CLI and Python API that return compact typed Markdown, explicit truncation, and actionable JSON errors. Formatting stays in Rust.

Use Thrift by default or the Statement Execution API with `protocol="sea"`.
Bring a token or your own token-refresh callable.

## Install

```bash
pip install arrowbricks
```

Wheels cover Linux, macOS, and Windows; Python 3.11+.
Arrow, NDJSON, and agent mode work without extras. Python row tuples need
`pip install 'arrowbricks[arro3]'`.

## Query from an agent or terminal

```bash
export DATABRICKS_HOST='your-workspace.azuredatabricks.net'
export DATABRICKS_WAREHOUSE_ID='your-warehouse-id'
export DATABRICKS_TOKEN='your-access-token'

arrowbricks -c 'SELECT 1 AS answer'
```

```markdown
| answer (Int32) |
| --- |
| 1 |

rows_shown=1; complete=true; total_rows=1
```

Agent mode is the CLI default. SQL files and piped stdin work too:

```bash
arrowbricks --file query.sql --max-rows 20 --max-bytes 4000
printf 'SELECT 1 AS answer' | arrowbricks
```

Previews have row, byte, cell, and deadline limits. The footer tells you when
results are incomplete. Query errors go to stderr as JSON, preserving the
warehouse's diagnostic and suggestions with a recovery hint; the CLI exits 1.
Preview limits bound output and fetching, not warehouse scan cost.

For Python agents:

```python
from arrowbricks import DatabricksClient

async with DatabricksClient(host=..., warehouse_id=..., token=...) as client:
    text = await client.query_agent("SELECT 1 AS answer", max_bytes=4000)
    print(text)  # Markdown on success; JSON on query failure
```

Give your agent the reusable [Arrowbricks skill](skills/arrowbricks/SKILL.md).
See [CLI options and skill installation](docs/usage.md#cli-quickstart-60) and
[agent budgets, completeness, and cancellation](docs/usage.md#agent-results).

## Query from an application

The async cursor offers familiar `execute` and `fetch*` methods.
Arrow results work directly with DuckDB, PyArrow, and Polars:

```python
from arrowbricks import connect

async with connect(host=..., warehouse_id=..., token=...) as conn:
    cursor = conn.cursor()
    await cursor.execute("SELECT * FROM my_catalog.my_schema.my_table LIMIT 100")
    table = await cursor.fetchall_arrow()
```

Or consume ready-to-send NDJSON without building Python rows:

```python
from arrowbricks import HEARTBEAT, DatabricksClient

async with DatabricksClient(host=..., warehouse_id=..., token=...) as client:
    async for item in client.stream_query_json("SELECT * FROM range(100)"):
        if item is not HEARTBEAT:
            print(item)
```

## Performance

The query and formatting work runs in Rust. Agent previews render directly from
Arrow, avoiding intermediate Python rows or JSON. In a real warehouse test,
a 100-row preview used **44% fewer bytes and 24% fewer tokens** than NDJSON.
See [agent measurements and methodology](docs/research/agent-rendering.md) and
[client benchmarks](docs/benchmarks.md) for workloads, comparisons, and limits.

## Learn more

- [Usage and API reference](docs/usage.md): authentication, errors, cancellation, observability, and Arrow interop.
- [Runnable examples](examples/): cursors, agents, OAuth, and FastAPI streaming.
- [Rust core](rust/arrowbricks_core/README.md): implementation and native APIs.
- [Real warehouse verification](docs/usage.md#real-warehouse-verification).
- [Changelog](CHANGELOG.md).

## License

MIT
