"""SIMULATED before/after benchmark for the persistent-client +
warehouse-check-cache optimizations (client.py's _get_http_client /
_ensure_warehouse_running) -- runs entirely against a respx-mocked
transport, no real Databricks warehouse or credentials needed.

This is NOT a measurement against a real network -- respx intercepts at the
transport layer, so there's no actual socket, TCP handshake, or TLS
negotiation happening. To make the comparison meaningful anyway, this script
injects realistic artificial latency for exactly the two things the
optimizations remove:

  - `_HANDSHAKE_COST_S`: paid once per NEW httpx.AsyncClient (simulates the
    TCP+TLS handshake a brand-new connection to the Databricks host pays).
    "cold" mode creates a fresh DatabricksClient (and thus a fresh
    httpx.AsyncClient) per query, so it pays this every time. "warm" mode
    reuses one DatabricksClient across all queries, paying it exactly once.
  - `_WAREHOUSE_CHECK_COST_S`: the warehouse-status GET's round-trip time.
    This one isn't injected by this script at all -- it falls out of the
    REAL client.py code: "cold" mode's fresh client has no cached
    confirmation, so _ensure_warehouse_running does the GET every time;
    "warm" mode's cache (see warehouse_confirmed_running_ttl_s) skips it
    after the first call. The respx route itself sleeps
    _WAREHOUSE_CHECK_COST_S before responding, same for both modes -- the
    difference in total time comes only from how many times each mode hits
    that route.

Every other mocked endpoint (statement submission, chunk resolve/fetch) has
zero added delay in both modes, since that cost doesn't differ between them
and would only dilute the comparison.

Usage:
    uv run python scripts/benchmark_simulated.py [--n 20]
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
import time

sys.path.insert(0, "src")
sys.path.insert(0, "tests")

import httpx  # noqa: E402
import respx  # noqa: E402
from conftest import HOST, WAREHOUSE_ID, build_chunk_bytes  # noqa: E402

from arrowbricks import DatabricksClient  # noqa: E402

_HANDSHAKE_COST_S = 0.06  # a fresh HTTPS connection's TCP+TLS setup, typical for a cross-region call
_WAREHOUSE_CHECK_COST_S = 0.04  # one small API round-trip


def _install_routes(router: respx.Router, statement_id: str) -> None:
    router.get(f"{HOST}/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(
        side_effect=_slow_response(_WAREHOUSE_CHECK_COST_S, {"state": "RUNNING"})
    )
    router.post(f"{HOST}/api/2.0/sql/statements").mock(
        return_value=httpx.Response(
            200,
            json={
                "statement_id": statement_id,
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 1}]},
            },
        )
    )
    router.get(url__regex=rf"{HOST}/api/2\.0/sql/statements/{statement_id}/result/chunks/\d+").mock(
        return_value=httpx.Response(200, json={"external_links": [{"external_link": f"{HOST}/_data/chunk-0"}]})
    )
    router.get(url__regex=rf"{HOST}/_data/chunk-\d+").mock(
        return_value=httpx.Response(200, content=build_chunk_bytes(0, 1))
    )


def _slow_response(delay_s: float, body: dict) -> object:
    async def _handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(delay_s)
        return httpx.Response(200, json=body)

    return _handler


async def _timed_query(client: DatabricksClient, sql: str) -> float:
    """Charges _HANDSHAKE_COST_S the first time this client's shared
    httpx.AsyncClient gets its first real use, simulating a fresh
    connection's TCP+TLS setup -- a real socket would pay this on its own,
    respx's in-memory transport doesn't, so it's added explicitly here."""
    is_first_use = client._http is None
    start = time.perf_counter()
    if is_first_use:
        await asyncio.sleep(_HANDSHAKE_COST_S)
    await client.execute_json_statement(sql)
    return time.perf_counter() - start


async def _run_cold(n: int, sql: str) -> list[float]:
    times = []
    with respx.mock:
        _install_routes(respx.mock, "stmt-cold")
        for _ in range(n):
            token = "fake-token"  # noqa: S105 -- fake token, mocked transport only
            client = DatabricksClient(HOST, WAREHOUSE_ID, token=token)
            times.append(await _timed_query(client, sql))
            await client.aclose()
    return times


async def _run_warm(n: int, sql: str) -> list[float]:
    times = []
    with respx.mock:
        _install_routes(respx.mock, "stmt-warm")
        token = "fake-token"  # noqa: S105 -- fake token, mocked transport only
        async with DatabricksClient(HOST, WAREHOUSE_ID, token=token) as client:
            for _ in range(n):
                times.append(await _timed_query(client, sql))
    return times


def _report(label: str, times: list[float]) -> None:
    print(
        f"{label}: n={len(times)} mean={statistics.mean(times) * 1000:.1f}ms "
        f"median={statistics.median(times) * 1000:.1f}ms total={sum(times):.3f}s"
    )


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=20)
    args = parser.parse_args()

    print("*** SIMULATED benchmark -- respx-mocked transport, no real Databricks call. ***")
    print(
        f"Injected costs: fresh-connection handshake={_HANDSHAKE_COST_S * 1000:.0f}ms, "
        f"warehouse-status round-trip={_WAREHOUSE_CHECK_COST_S * 1000:.0f}ms\n"
    )

    cold = await _run_cold(args.n, "SELECT 1")
    _report("cold (old behavior: fresh client + warehouse-check per query)", cold)

    warm = await _run_warm(args.n, "SELECT 1")
    _report("warm (new behavior: one reused client)                      ", warm)

    saved_per_query_ms = (statistics.mean(cold) - statistics.mean(warm)) * 1000
    speedup = statistics.mean(cold) / statistics.mean(warm) if statistics.mean(warm) > 0 else float("inf")
    print(f"\nmean saved per query (after the first): ~{saved_per_query_ms:.1f}ms")
    print(f"mean speedup: {speedup:.2f}x")
    print(
        "\nNote: this isolates exactly the two mechanisms changed (connection reuse, "
        "warehouse-check caching) with realistic but assumed latency values -- it does "
        "not measure real Databricks/network round-trip times, statement execution time, "
        "or chunk-fetch time, none of which changed."
    )


if __name__ == "__main__":
    asyncio.run(main())
