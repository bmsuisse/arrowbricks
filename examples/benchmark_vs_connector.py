"""Reproduces the speed/memory numbers in README.md: arrowbricks vs
`databricks-sql-connector`, same query, same warehouse, a few warm runs each.
Each library runs in its own subprocess so peak memory (`ru_maxrss`) reflects
that library alone, not the other's imports.

Needs both packages installed: `pip install arrowbricks databricks-sql-connector`.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXX \\
    python examples/benchmark_vs_connector.py

Or drop those three into a `.env` file (same directory you run this from, or
any parent directory) and just run `python examples/benchmark_vs_connector.py` --
loaded automatically, real environment variables still take priority. Never
commit a `.env` with a real token in it.

Optional: BENCHMARK_SQL to override the default query, BENCHMARK_RUNS
(default 3) for how many timed runs to average (plus one untimed warm-up).
"""

import json
import os
import statistics
import subprocess
import sys
import textwrap
from pathlib import Path


def load_dotenv() -> None:
    """Minimal `.env` loader (KEY=VALUE per line, `#` comments, optional
    quotes) -- no dependency needed for one optional convenience. Searches
    the current directory upward so this works whether you run it from the
    repo root or from `examples/`. Existing environment variables always
    win, matching every real dotenv library's own precedence."""
    for directory in [Path.cwd(), *Path.cwd().parents]:
        env_file = directory / ".env"
        if not env_file.is_file():
            continue
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
        return


DEFAULT_SQL = "SELECT id, id * 2 AS doubled, CAST(id AS STRING) AS label FROM range(200000)"

# `ru_maxrss` is bytes on macOS/BSD but kilobytes on Linux -- normalize to KB.
RSS_KB = """
import resource, sys
_r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
_RSS_KB = _r // 1024 if sys.platform == "darwin" else _r
"""

ARROWBRICKS_CHILD = (
    RSS_KB
    + """
import asyncio, json, os, time
from arrowbricks import connect

async def main():
    host = os.environ["DATABRICKS_HOST"]
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    token = os.environ["DATABRICKS_TOKEN"]
    sql = os.environ["BENCHMARK_SQL"]
    runs = int(os.environ["BENCHMARK_RUNS"])
    times = []
    async with connect(host, warehouse_id, token=token) as conn:
        for _ in range(runs + 1):
            cur = conn.cursor()
            t0 = time.perf_counter()
            await cur.execute(sql)
            table = await cur.fetchall_arrow()
            times.append(time.perf_counter() - t0)
            assert table.num_rows > 0
    print(json.dumps({"times": times[1:], "peak_rss_kb": _RSS_KB}))

asyncio.run(main())
"""
)

CONNECTOR_CHILD = (
    RSS_KB
    + """
import json, os, time
from databricks import sql

host = os.environ["DATABRICKS_HOST"]
warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
token = os.environ["DATABRICKS_TOKEN"]
sql_text = os.environ["BENCHMARK_SQL"]
runs = int(os.environ["BENCHMARK_RUNS"])
times = []
conn = sql.connect(
    server_hostname=host,
    http_path=f"/sql/1.0/warehouses/{warehouse_id}",
    access_token=token,
)
try:
    for _ in range(runs + 1):
        cur = conn.cursor()
        t0 = time.perf_counter()
        cur.execute(sql_text)
        rows = cur.fetchall()
        times.append(time.perf_counter() - t0)
        assert len(rows) > 0
        cur.close()
finally:
    conn.close()
print(json.dumps({"times": times[1:], "peak_rss_kb": _RSS_KB}))
"""
)


def run_child(label: str, code: str) -> dict:
    # `code` is always one of this file's own CONNECTOR_CHILD/ARROWBRICKS_CHILD
    # constants, never external input.
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", textwrap.dedent(code)], capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        print(f"--- {label} failed ---\n{result.stderr}", file=sys.stderr)
        raise SystemExit(1)
    return json.loads(result.stdout.strip().splitlines()[-1])


def summarize(label: str, result: dict) -> dict:
    times = result["times"]
    avg = statistics.mean(times)
    stdev = statistics.stdev(times) if len(times) > 1 else 0.0
    summary = {
        "label": label,
        "avg": avg,
        "stdev": stdev,
        "min": min(times),
        "max": max(times),
        "peak_rss_mb": result["peak_rss_kb"] / 1024,
    }
    print(
        f"{label:<28} {[f'{t:.2f}s' for t in times]}  "
        f"avg={avg:.2f}s  stdev={stdev:.2f}s  range=[{summary['min']:.2f}s, {summary['max']:.2f}s]  "
        f"peak_rss={summary['peak_rss_mb']:.0f} MB"
    )
    return summary


def main() -> None:
    load_dotenv()
    missing = [v for v in ("DATABRICKS_HOST", "DATABRICKS_WAREHOUSE_ID", "DATABRICKS_TOKEN") if v not in os.environ]
    if missing:
        raise SystemExit(
            f"missing required environment variable(s): {', '.join(missing)} "
            "-- set them directly or put them in a .env file (see this script's own docstring)"
        )
    os.environ.setdefault("BENCHMARK_SQL", DEFAULT_SQL)
    os.environ.setdefault("BENCHMARK_RUNS", "3")
    sql_text = os.environ["BENCHMARK_SQL"]
    runs = int(os.environ["BENCHMARK_RUNS"])

    print(f"query: {sql_text}")
    print(f"runs: {runs} (plus 1 discarded warm-up)\n")

    connector = summarize("databricks-sql-connector:", run_child("databricks-sql-connector", CONNECTOR_CHILD))
    arrowbricks = summarize("arrowbricks:", run_child("arrowbricks", ARROWBRICKS_CHILD))

    print(f"\narrowbricks is {connector['avg'] / arrowbricks['avg']:.2f}x the connector's speed")
    print(f"arrowbricks peak RSS is {arrowbricks['peak_rss_mb'] / connector['peak_rss_mb']:.2f}x the connector's")


if __name__ == "__main__":
    main()
