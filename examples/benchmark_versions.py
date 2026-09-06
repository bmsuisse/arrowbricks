"""Interleave two unpacked wheels against the same real warehouse.

Needs DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID, DATABRICKS_TOKEN (or .env).
Both workers keep one connection, discard a warm-up, and alternate A/B and
B/A order. All runtime parameters under test are passed explicitly. Query
results stay local; output contains timings, row counts and peak process RSS.
Use --verify-ipc to compare serialized Arrow batches in memory after timing
(requires arro3-core). This requires stable query results, row order and batch
boundaries; it is stricter than comparing logical values alone. Checksums are
never printed. Verification contributes to subsequent process peak RSS.

    python examples/benchmark_versions.py --baseline /tmp/old --candidate /tmp/new
"""

import argparse
import asyncio
import hashlib
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

from benchmark_vs_connector import DEFAULT_SQL, load_dotenv


def ipc_checksum(table) -> str:
    from arro3.core import Table

    from arrowbricks import write_ipc_stream

    class Sink:
        def __init__(self):
            self.digest = hashlib.sha256()

        def write(self, data):
            self.digest.update(data)
            return len(data)

    sink = Sink()
    # Serialize one batch at a time instead of creating another whole-result
    # allocation. No query data is written to disk.
    arrow_table = Table.from_arrow(table)
    batches = arrow_table.to_batches()
    if not batches:
        write_ipc_stream(arrow_table, sink)
    for batch in batches:
        write_ipc_stream(Table.from_batches([batch]), sink)
    return sink.digest.hexdigest()


async def worker(args) -> None:
    import resource

    sys.path.insert(0, str(Path(args.worker).resolve()))
    import arrowbricks
    from arrowbricks import connect

    package = Path(arrowbricks.__file__).resolve()
    if not package.is_relative_to(Path(args.worker).resolve()):
        raise RuntimeError(f"wrong package imported: {package}")
    events = []
    async with connect(
        os.environ["DATABRICKS_HOST"],
        os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
        protocol=args.protocol,
        chunk_fetch_concurrency=args.concurrency,
        compress_results=True,
        retry_attempts=1,
        http_timeout=60,
        on_event=events.append,
    ) as conn:
        while await asyncio.to_thread(sys.stdin.readline):
            events.clear()
            cursor = conn.cursor()
            start = time.perf_counter()
            await cursor.execute(args.sql)
            ready = time.perf_counter()
            table = await cursor.fetchall_arrow()
            done = time.perf_counter()
            rows = table.num_rows
            if args.expected_rows is not None and rows != args.expected_rows:
                raise RuntimeError(f"expected {args.expected_rows} rows, received {rows}")
            checksum = ipc_checksum(table) if args.verify_ipc else None
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            print(
                json.dumps(
                    {
                        "total_s": done - start,
                        "execute_s": ready - start,
                        "fetch_s": done - ready,
                        "rows": rows,
                        "chunks": events[-1].num_chunks if events else None,
                        "bytes_downloaded": events[-1].bytes_downloaded if events else None,
                        "peak_rss_mib": rss / (1024 * 1024 if sys.platform == "darwin" else 1024),
                        "checksum": checksum,
                    }
                ),
                flush=True,
            )
            del table, cursor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline")
    parser.add_argument("--candidate")
    parser.add_argument("--worker", help=argparse.SUPPRESS)
    parser.add_argument("--sql", default=DEFAULT_SQL)
    parser.add_argument("--expected-rows", type=int)
    parser.add_argument("--runs", type=int, default=6)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--protocol", choices=["thrift", "sea"], default="thrift")
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--verify-ipc", action="store_true", help="compare IPC checksums privately after timing")
    args = parser.parse_args()
    load_dotenv()
    if args.worker:
        try:
            asyncio.run(worker(args))
        except Exception as error:
            # Warehouse errors can include SQL, values or signed URLs.
            print(f"benchmark worker failed ({type(error).__name__})", file=sys.stderr)
            raise SystemExit(1) from None
        return
    if not args.baseline or not args.candidate or min(args.runs, args.warmups, args.concurrency) < 1:
        parser.error("baseline, candidate, positive runs, warmups and concurrency are required")
    children = {}
    results = {"baseline": [], "candidate": []}
    try:
        for label in results:
            cmd = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker",
                getattr(args, label),
                "--sql",
                args.sql,
                "--protocol",
                args.protocol,
                "--concurrency",
                str(args.concurrency),
            ]
            if args.expected_rows is not None:
                cmd.extend(["--expected-rows", str(args.expected_rows)])
            if args.verify_ipc:
                cmd.append("--verify-ipc")
            children[label] = subprocess.Popen(  # noqa: S603
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
            )
        for round_index in range(args.runs + args.warmups):
            order = list(results) if round_index % 2 == 0 else list(reversed(results))
            round_results = []
            checksums = []
            samples_to_record = []
            for label in order:
                child = children[label]
                child.stdin.write("query\n")
                child.stdin.flush()
                line = child.stdout.readline()
                if not line:
                    raise RuntimeError(f"{label} worker exited without a result")
                sample = json.loads(line)
                checksums.append(sample.pop("checksum", None))
                round_results.append(sample)
                if round_index >= args.warmups:
                    samples_to_record.append((label, sample))
            if round_results[0]["rows"] != round_results[1]["rows"]:
                raise RuntimeError("baseline and candidate returned different row counts")
            if args.verify_ipc and (None in checksums or checksums[0] != checksums[1]):
                raise RuntimeError("baseline and candidate IPC checksums differ; values suppressed")
            for label, sample in samples_to_record:
                results[label].append(sample)
                print(json.dumps({"round": round_index - args.warmups + 1, "version": label, **sample}), flush=True)
        for label, samples in results.items():
            print(
                json.dumps(
                    {
                        "version": label,
                        "median_s": statistics.median(s["total_s"] for s in samples),
                        "max_peak_rss_mib": max(s["peak_rss_mib"] for s in samples),
                    }
                )
            )
    finally:
        for child in children.values():
            if child.stdin:
                child.stdin.close()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()


if __name__ == "__main__":
    main()
