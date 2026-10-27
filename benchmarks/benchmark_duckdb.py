"""arrowbricks -> DuckDB end-to-end benchmark against a local mock warehouse; never reads warehouse credentials.

Models a fact_sales-style export (order_id, sku, qty, price, status, ts) split
into N cloud-fetch chunks, then runs the result into DuckDB the ways a service
would. Each `--pipeline` runs in its own process so `peak_rss_mib` is per
pipeline. Select the DuckDB version by choosing the venv it is installed in.

    python benchmark_duckdb.py --pipeline arrow_zero_copy --chunks 20 --rows-per-chunk 100000

Live mode (`--live-env .env --table cat.schema.table [--where ...] [--limit N]`)
runs the same pipelines against a real warehouse instead of the mock: read-only
`SELECT * ... LIMIT`, credentials read from the env file and never printed.
Each result carries a phase split (server execute / download+decode / DuckDB).

Needs `pyarrow` (to synthesize the chunk bytes) and this repository's tests
directory (for the local MockServer), same as `benchmark_lowlevel.py ndjson`.
"""

import argparse
import asyncio
import hashlib
import io
import json
import resource
import statistics
import sys
import tempfile
import time
from pathlib import Path

AGG = "SELECT status, count(*) AS n, round(sum(qty * price), 2) AS revenue, count(DISTINCT sku) AS skus FROM t GROUP BY ALL ORDER BY ALL"


def build_chunk(rows):
    import pyarrow as pa

    statuses = ["open", "picked", "packed", "shipped", "returned"]
    table = pa.table(
        {
            "order_id": pa.array(range(rows), pa.int64()),
            "sku": pa.array([f"SKU-{i % 50_000:06d}" for i in range(rows)]),
            "qty": pa.array([1 + i % 12 for i in range(rows)], pa.int32()),
            "price": pa.array([(i % 997) / 7 for i in range(rows)], pa.float64()),
            "status": pa.array([statuses[i % 5] for i in range(rows)]),
            "ts": pa.array([1_700_000_000_000_000 + i * 1000 for i in range(rows)], pa.timestamp("us")),
        }
    )
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def start_server(chunks, blob):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
    from conftest import MockServer, Response

    server = MockServer()
    server.get("/api/2.0/sql/warehouses/bench").mock(Response(200, json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            200,
            json_body={
                "statement_id": "bench",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": i, "row_count": None} for i in range(chunks)]},
            },
        )
    )
    server.get(r"^/api/2\.0/sql/statements/bench/result/chunks/\d+$", regex=True).mock(
        side_effect=lambda req: Response(
            200, json_body={"external_links": [{"external_link": f"{server.host}/blob"}]}
        )
    )
    server.get("/blob").mock(Response(200, content=blob))
    return server


def digest(rows):
    return hashlib.sha256(repr(rows).encode()).hexdigest()[:16]


LIVE_AGG = (
    'SELECT count(*) AS n, sum("Value") AS value, sum("Quantity (Stock Unit)") AS qty, '
    'count(DISTINCT article_sk) AS articles, min("Valid From") AS first_from, '
    'count(*) FILTER (WHERE "Is Current") AS current_rows FROM t'
)


def read_env(path):
    env = {}
    for line in Path(path).read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip("'\"")
    return env


async def run_pipeline(name, server, blob, trials, live=None):
    import duckdb

    from arrowbricks import ReplayableArrowChunk, connect

    global AGG
    if live:
        env = read_env(live["env"])
        conn = connect(
            host=env["DATABRICKS_HOST"],
            warehouse_id=env["DATABRICKS_WAREHOUSE_ID"],
            token=env["DATABRICKS_TOKEN"],
            **({"protocol": live["protocol"]} if live["protocol"] else {}),
        )
        sql = f"SELECT * FROM {live['table']} WHERE {live['where']} LIMIT {live['limit']}"
        AGG = LIVE_AGG
    else:
        conn = connect(host=server.host, warehouse_id="bench", token="fake", protocol="sea", compress_results=False)  # noqa: S106
        sql = "SELECT * FROM fact_sales"
    phases = []

    async def fetch_arrow():
        cursor = conn.cursor()
        t0 = time.perf_counter()
        await cursor.execute(sql)
        t1 = time.perf_counter()
        table = await cursor.fetchall_arrow()
        phases.append((t1 - t0, time.perf_counter() - t1))
        return table

    async def arrow_zero_copy():
        table = await fetch_arrow()
        con = duckdb.connect()
        con.register("t", table)
        return con.sql(AGG).fetchall()

    async def arrow_via_pyarrow():
        import pyarrow as pa

        table = pa.table(await fetch_arrow())
        con = duckdb.connect()
        con.register("t", table)
        return con.sql(AGG).fetchall()

    async def to_parquet():
        table = await fetch_arrow()
        con = duckdb.connect()
        con.register("t", table)
        with tempfile.TemporaryDirectory() as tmp:
            con.execute(f"COPY t TO '{tmp}/out.parquet' (FORMAT parquet)")
            return con.sql(f"SELECT count(*) FROM read_parquet('{tmp}/out.parquet')").fetchall()

    async def load_table():
        table = await fetch_arrow()
        con = duckdb.connect()
        con.register("src", table)
        con.execute("CREATE TABLE t AS SELECT * FROM src")
        return con.sql(AGG).fetchall()

    async def replayable():
        # Cached-IPC-bytes path: DuckDB 2.x calls __arrow_c_stream__ for the
        # schema peek and again for the scan, which ReplayableArrowChunk supports.
        con = duckdb.connect()
        con.register("t", ReplayableArrowChunk(blob, chunk_index=0))
        return con.sql(AGG).fetchall()

    async def legacy_rows():
        # Row tuples, then a bulk load: the best case for the path the Arrow
        # handoff replaces. (`executemany` is far slower still -- ~0.7 ms/row
        # on DuckDB 2.0.0 -- so it is not benchmarked here.) One chunk only.
        import pyarrow as pa

        cursor = conn.cursor()
        await cursor.execute(sql)
        rows = await cursor.fetchall()
        names = [d[0] for d in cursor.description] if live else ["order_id", "sku", "qty", "price", "status", "ts"]
        con = duckdb.connect()
        con.register("src", pa.Table.from_pylist([dict(zip(names, r, strict=True)) for r in rows]))
        con.execute("CREATE TABLE t AS SELECT * FROM src")
        return con.sql(AGG).fetchall()

    pipelines = {f.__name__: f for f in (arrow_zero_copy, arrow_via_pyarrow, to_parquet, load_table, replayable, legacy_rows)}
    fn = pipelines[name]
    times, result = [], None
    for trial in range(trials):
        start = time.perf_counter()
        result = await fn()
        elapsed = time.perf_counter() - start
        if trial:  # first trial is warmup
            times.append(elapsed)
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

    execute_s = statistics.median(p[0] for p in phases[1:]) if len(phases) > 1 else None
    fetch_s = statistics.median(p[1] for p in phases[1:]) if len(phases) > 1 else None
    return {
        "execute_s": execute_s,
        "fetch_s": fetch_s,
        "pipeline": name,
        "duckdb": duckdb.__version__,
        "median_s": statistics.median(times),
        "min_s": min(times),
        "seconds": times,
        "peak_rss_mib": rss,
        "result_sha256": digest(result),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline", required=True)
    parser.add_argument("--chunks", type=int, default=20)
    parser.add_argument("--rows-per-chunk", type=int, default=100_000)
    parser.add_argument("--trials", type=int, default=6)
    parser.add_argument("--live-env", help="env file with DATABRICKS_HOST/_TOKEN/_WAREHOUSE_ID; enables live mode")
    parser.add_argument("--table")
    parser.add_argument("--where", default="true")
    parser.add_argument("--limit", type=int, default=2_000_000)
    parser.add_argument("--protocol", choices=["thrift", "sea"])
    args = parser.parse_args()
    if args.trials < 2:
        parser.error("--trials must be >= 2 (the first trial is a discarded warmup)")
    if args.live_env:
        if not args.table:
            parser.error("--live-env requires --table")
        if args.pipeline == "replayable":
            parser.error("--pipeline replayable replays cached IPC bytes and is not available in live mode")
        live = {"env": args.live_env, "table": args.table, "where": args.where, "limit": args.limit, "protocol": args.protocol}
        out = asyncio.run(run_pipeline(args.pipeline, None, None, args.trials, live))
        out.update(table=args.table, where=args.where, limit=args.limit, protocol=args.protocol or "default")
        print(json.dumps(out), flush=True)
        sys.exit(0)
    chunks = 1 if args.pipeline in ("replayable", "legacy_rows") else args.chunks
    blob = build_chunk(args.rows_per_chunk)
    server = start_server(chunks, blob)
    try:
        out = asyncio.run(run_pipeline(args.pipeline, server, blob, args.trials))
        out.update(chunks=chunks, rows=chunks * args.rows_per_chunk, wire_mib=0.0 if args.pipeline == "replayable" else len(blob) * chunks / 2**20)
        print(json.dumps(out), flush=True)
    finally:
        server.shutdown()
