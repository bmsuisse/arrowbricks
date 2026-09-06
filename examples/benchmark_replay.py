"""Measure cached IPC replay CPU time and memory without a warehouse.

Run with the dev dependencies installed. Each measurement uses a fresh
subprocess; fixture construction is excluded from its peak RSS. Set
PYTHONPATH to an unpacked wheel to compare builds using the same interpreter.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def measure(path: str, repeats: int) -> None:
    import resource

    from arrowbricks import ReplayableArrowChunk

    data = Path(path).read_bytes()
    chunk = ReplayableArrowChunk(data, chunk_index=0)
    tables = []
    elapsed = []
    for _ in range(repeats):
        start = time.perf_counter()
        tables.append(chunk.to_table())
        elapsed.append(time.perf_counter() - start)
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(
        json.dumps(
            {
                "median_ms": statistics.median(elapsed) * 1000,
                "times_ms": [t * 1000 for t in elapsed],
                "peak_rss_mib": rss / (1024 * 1024 if sys.platform == "darwin" else 1024),
                "ipc_bytes": len(data),
                "rows": tables[0].num_rows,
                "retained_replays": len(tables),
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--columns", type=int, default=16)
    parser.add_argument("--repeats", type=int, default=8)
    parser.add_argument("--input", help="Internal subprocess input")
    args = parser.parse_args()
    if min(args.rows, args.columns, args.repeats) < 1:
        parser.error("rows, columns, and repeats must be positive")
    if args.input:
        measure(args.input, args.repeats)
        return

    import arro3.core as core

    from arrowbricks import write_ipc_stream

    array = core.Array(list(range(args.rows)), type=core.DataType.int64())
    table = core.Table.from_pydict({f"c{i}": array for i in range(args.columns)})
    with tempfile.TemporaryDirectory(prefix="arrowbricks-replay-") as directory:
        path = Path(directory) / "input.arrow"
        with path.open("wb") as output:
            write_ipc_stream(table, output)
        result = subprocess.run(  # noqa: S603
            [sys.executable, os.path.abspath(__file__), "--input", str(path), "--repeats", str(args.repeats)],
            check=True,
            capture_output=True,
            text=True,
        )
        print(result.stdout.strip())


if __name__ == "__main__":
    main()
