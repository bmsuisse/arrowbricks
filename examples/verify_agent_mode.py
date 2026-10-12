"""Read-only live verification and interleaved timings for both protocols.

    python examples/verify_agent_mode.py --env-file /path/to/.env --runs 5

Only generated SQL values are recorded; credentials and hostnames are omitted.
"""

# ruff: noqa: S101 -- executable live verification, not production validation

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from pathlib import Path

from benchmark_vs_connector import load_dotenv

from arrowbricks import DatabricksClient


async def verify(protocol: str, runs: int) -> dict:
    client = DatabricksClient(
        os.environ["DATABRICKS_HOST"],
        os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
        protocol=protocol,
        chunk_fetch_concurrency=1,
    )
    cases = []

    async def check(name: str, sql: str, *, contains=(), **kwargs) -> str:
        start = time.perf_counter()
        kwargs.setdefault("output_format", "markdown")
        text = await client.query_agent(sql, **kwargs)
        assert len(text.encode()) <= kwargs.get("max_bytes", 10000), name
        for expected in contains:
            assert expected in text, (name, expected, text[:300])
        cases.append(
            {"name": name, "elapsed_ms": (time.perf_counter() - start) * 1000, "output_bytes": len(text.encode())}
        )
        print(f"{protocol}: {name} passed ({cases[-1]['elapsed_ms']:.1f} ms)", file=sys.stderr, flush=True)
        return text

    try:
        await check("scalar", "SELECT 42 AS n, 'hello' AS greeting", contains=("42", '"hello"', "complete=true"))
        await check(
            "empty",
            "SELECT CAST(NULL AS BIGINT) AS empty_id WHERE false",
            contains=("empty\\_id", "rows_shown=0", "complete=true"),
        )
        await check(
            "null_and_string", "SELECT CAST(NULL AS STRING) AS absent, 'NULL' AS literal", contains=("NULL", '"NULL"')
        )
        text = await check(
            "tsv_tab_and_newline",
            "SELECT concat('a', char(9), 'b', char(10), 'c') AS v, CAST(NULL AS STRING) AS n",
            output_format="tsv",
            contains=("v:", "a\\tb\\nc", "\\N", "complete=true"),
        )
        assert len(text.split("\n\n")[0].splitlines()) == 2, text
        await check("unicode", "SELECT 'Grüezi 🦆 東京' AS greeting", contains=("Grüezi", "🦆", "東京"))
        text = await check("escaped", "SELECT concat('a|b', char(10), '<script>', char(92), '`x`') AS value")
        assert "<script>" not in text and "a|b" not in text, text
        await check(
            "decimal_date_timestamp",
            "SELECT CAST(123.45 AS DECIMAL(12,2)) AS amount, DATE '2026-10-11' AS day, "
            "TIMESTAMP '2026-10-11 10:00:00' AS stamp",
            contains=("123.45", "2026-10-11"),
        )
        text = await check(
            "nested",
            "SELECT array(1,2,3) AS numbers, named_struct('a', 42, 'b', 'hi') AS details, map('x', 7) AS mapping",
        )
        assert not text.startswith("{"), text
        await check("duplicate_names", "SELECT 1 AS duplicate, 2 AS duplicate", contains=("1", "2"))
        await check("row_cap", "SELECT id FROM range(10000)", max_rows=5, contains=("rows_shown=5", "complete=false"))
        await check(
            "byte_cap",
            "SELECT id, repeat('abcd', 30) AS value FROM range(1000)",
            max_bytes=512,
            contains=("complete=false",),
        )
        await check(
            "cell_cap",
            "SELECT repeat('🦆', 100000) AS value",
            max_cell_chars=20,
            contains=("truncated", "complete=false"),
        )
        await check(
            "parameter", "SELECT :n AS number", params=[{"name": "n", "value": "7", "type": "INT"}], contains=("7",)
        )
        for name, sql, error_class in [
            ("bad_syntax", "SELEC 42", "PARSE_SYNTAX_ERROR"),
            ("missing_column", "SELECT arrowbricks_missing_column_7a59 FROM range(3)", "UNRESOLVED_COLUMN"),
            ("missing_table", "SELECT * FROM arrowbricks_missing_table_7a59", "TABLE_OR_VIEW_NOT_FOUND"),
        ]:
            text = await check(name, sql)
            error = json.loads(text)
            assert error_class in error["message"], (name, error)
            assert error["next_step"], (name, error)
        text = await check("deadline", "SELECT sum(sin(id)) AS n FROM range(1000000)", total_timeout_s=0.005)
        error = json.loads(text)
        assert "timeout" in json.dumps(error).lower(), error
        await asyncio.sleep(0.2)
        await check("usable_after_timeout", "SELECT 1 AS n", contains=("complete=true",))

        sql = "SELECT id, id * 2 AS doubled, CAST(id AS STRING) AS label FROM range(100)"
        agent_times, json_times = [], []
        agent_bytes = json_bytes = 0
        for i in range(runs + 1):
            for mode in ["agent", "json"] if i % 2 else ["json", "agent"]:
                start = time.perf_counter()
                if mode == "agent":
                    text = await client.query_agent(sql, max_rows=100, max_bytes=20000)
                    assert "rows_shown=100" in text and "complete=true" in text
                    agent_bytes = len(text.encode())
                else:
                    rows = [row async for row in client.stream_query_json(sql) if isinstance(row, str)]
                    assert len(rows) == 100
                    json_bytes = sum(len(row.encode()) + 1 for row in rows)
                elapsed = (time.perf_counter() - start) * 1000
                if i:
                    (agent_times if mode == "agent" else json_times).append(elapsed)
        return {
            "protocol": protocol,
            "cases": cases,
            "benchmark": {
                "runs": runs,
                "agent_median_ms": statistics.median(agent_times),
                "ndjson_median_ms": statistics.median(json_times),
                "agent_times_ms": agent_times,
                "ndjson_times_ms": json_times,
                "agent_output_bytes": agent_bytes,
                "ndjson_output_bytes": json_bytes,
            },
        }
    finally:
        await client.aclose()


async def main(args: argparse.Namespace) -> None:
    if args.env_file:
        load_dotenv(args.env_file)
    results = [await verify(protocol, args.runs) for protocol in args.protocol]
    text = json.dumps(results, indent=2)
    if args.output:
        args.output.write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--protocol", nargs="+", choices=["thrift", "sea"], default=["thrift", "sea"])
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    asyncio.run(main(args))
