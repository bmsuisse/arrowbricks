"""Before/after benchmark for the persistent-client + warehouse-check-cache
optimizations (see client.py's _get_http_client/_ensure_warehouse_running).

Runs the same small query N times two ways against a REAL Databricks
warehouse:
  - "cold": a fresh DatabricksClient per query (simulates the old
    per-call-httpx.AsyncClient() + per-call warehouse-status-GET behavior).
  - "warm": one DatabricksClient reused across all N queries (the new
    default behavior).

Usage:
    export DATABRICKS_HOST=...           # or falls back to `databricks auth env`
    export DATABRICKS_WAREHOUSE_ID=...
    export DATABRICKS_TOKEN=...          # a personal access token
    uv run python scripts/benchmark_client_reuse.py [--n 10] [--sql "SELECT 1"]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import statistics
import subprocess
import sys
import time

sys.path.insert(0, "src")

from arrowbricks import DatabricksClient  # noqa: E402


def _databricks_auth_env() -> dict[str, str]:
    """Falls back to `databricks auth env` (the CLI's own resolved
    credentials) if DATABRICKS_HOST/TOKEN aren't set directly -- lets this
    script work whether the user set env vars or ran `databricks configure`."""
    if os.environ.get("DATABRICKS_HOST") and (
        os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_CLIENT_ID")
    ):
        return dict(os.environ)
    databricks_cli = shutil.which("databricks")
    if not databricks_cli:
        return dict(os.environ)
    try:
        out = subprocess.run(  # noqa: S603 -- fixed args, no untrusted input
            [databricks_cli, "auth", "env"], capture_output=True, text=True, check=True
        ).stdout
    except Exception:
        return dict(os.environ)
    env = dict(os.environ)
    for line in out.splitlines():
        line = line.strip()
        if line.startswith("export "):
            line = line[len("export ") :]
        if "=" in line:
            k, _, v = line.partition("=")
            env.setdefault(k.strip(), v.strip().strip('"'))
    return env


async def _run_n_cold(host: str, warehouse_id: str, token: str, sql: str, n: int) -> list[float]:
    """Old behavior: a fresh client (and fresh http connection pool + a
    fresh warehouse-status check) per query."""
    times = []
    for _ in range(n):
        client = DatabricksClient(host, warehouse_id, token=token)
        start = time.perf_counter()
        await client.execute_json_statement(sql)
        times.append(time.perf_counter() - start)
        await client.aclose()
    return times


async def _run_n_warm(host: str, warehouse_id: str, token: str, sql: str, n: int) -> list[float]:
    """New behavior: one client, one connection pool, warehouse status
    checked once then cached for the TTL window."""
    times = []
    async with DatabricksClient(host, warehouse_id, token=token) as client:
        for _ in range(n):
            start = time.perf_counter()
            await client.execute_json_statement(sql)
            times.append(time.perf_counter() - start)
    return times


def _report(label: str, times: list[float]) -> None:
    print(f"{label}: n={len(times)} mean={statistics.mean(times):.3f}s median={statistics.median(times):.3f}s "
          f"min={min(times):.3f}s max={max(times):.3f}s total={sum(times):.3f}s")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=10)
    parser.add_argument("--sql", default="SELECT 1")
    args = parser.parse_args()

    env = _databricks_auth_env()
    host = env.get("DATABRICKS_HOST", "adb-8956277663194228.8.azuredatabricks.net")
    warehouse_id = env.get("DATABRICKS_WAREHOUSE_ID", "c397040753b46093")
    token = env.get("DATABRICKS_TOKEN")
    if not token:
        raise SystemExit(
            "No DATABRICKS_TOKEN found (env var or `databricks auth env`). "
            "Set one and re-run -- this script needs a real, working credential."
        )

    print(f"host={host} warehouse_id={warehouse_id} n={args.n} sql={args.sql!r}\n")

    # Warm the warehouse first (cold-start time shouldn't pollute either measurement).
    warmup = DatabricksClient(host, warehouse_id, token=token)
    await warmup.execute_json_statement(args.sql)
    await warmup.aclose()

    cold = await _run_n_cold(host, warehouse_id, token, args.sql, args.n)
    _report("cold (fresh client per query)", cold)

    warm = await _run_n_warm(host, warehouse_id, token, args.sql, args.n)
    _report("warm (one reused client)     ", warm)

    speedup = statistics.mean(cold) / statistics.mean(warm) if statistics.mean(warm) > 0 else float("inf")
    print(f"\nmean speedup: {speedup:.2f}x")


if __name__ == "__main__":
    asyncio.run(main())
