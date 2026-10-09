"""Synthetic IPC replay and local NDJSON benchmarks; never reads warehouse credentials.

Run `prepare /tmp/synthetic.arrow` once, then `replay` or `ndjson` with the
same path. Set PYTHONPATH to an unpacked release wheel to select a version.
The NDJSON mode requires this repository's test dependencies.
"""

import argparse
import asyncio
import concurrent.futures
import hashlib
import json
import resource
import statistics
import sys
import threading
import time
from pathlib import Path

ROWS = 100_000


def prepare(path):
    import arro3.core as core

    from arrowbricks import write_ipc_stream

    array = core.Array(["synthetic café " + "x" * 114] * ROWS, type=core.DataType.string())
    table = core.Table.from_pydict({f"c{i}": array for i in range(8)})
    with path.open("wb") as output:
        write_ipc_stream(table, output)


def replay(path):
    from arrowbricks import _core

    data = path.read_bytes()
    for workers in (1, 4):
        values = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            for trial in range(9):
                barrier = threading.Barrier(workers + 1)

                def work(start_barrier=barrier):
                    start_barrier.wait()
                    for _ in range(10):
                        table = _core.read_ipc_stream(data)
                        if table.num_rows != ROWS:
                            raise RuntimeError("unexpected replay row count")

                futures = [pool.submit(work) for _ in range(workers)]
                start = time.perf_counter()
                barrier.wait()
                for future in futures:
                    future.result()
                elapsed = time.perf_counter() - start
                if trial >= 2:
                    values.append(elapsed)
        print(
            json.dumps(
                {"workers": workers, "decodes": workers * 10, "seconds": values, "median_s": statistics.median(values)}
            ),
            flush=True,
        )


def ndjson(path):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
    from conftest import MockServer, Response

    from arrowbricks import _core

    blob = path.read_bytes()
    server = MockServer()
    server.get("/api/2.0/sql/warehouses/synthetic").mock(Response(200, json_body={"state": "RUNNING"}))
    server.post("/api/2.0/sql/statements").mock(
        Response(
            200,
            json_body={
                "statement_id": "synthetic",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": ROWS}]},
                "result": {"external_links": [{"chunk_index": 0, "external_link": server.host + "/blob"}]},
            },
        )
    )
    server.get("/blob").mock(Response(200, content=blob))

    async def measure():
        client = _core.Client(
            host=server.host,
            warehouse_id="synthetic",
            token="fake",  # noqa: S106
            protocol="sea",
            compress_results=False,
        )
        for trial in range(5):
            start = time.perf_counter()
            cpu_start = time.process_time()
            rows = 0
            pages = 0
            digest = hashlib.sha256()
            async for page in client.stream_ndjson_lines("SELECT synthetic"):
                if page is _core.HEARTBEAT:
                    continue
                rows += len(page)
                pages += 1
                for line in page:
                    digest.update(line.encode())
                    digest.update(b"\n")
            elapsed = time.perf_counter() - start
            cpu_s = time.process_time() - cpu_start
            if rows != ROWS:
                raise RuntimeError("unexpected NDJSON row count")
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            rss /= 1024 * 1024 if sys.platform == "darwin" else 1024
            if trial:
                print(
                    json.dumps(
                        {
                            "trial": trial,
                            "seconds": elapsed,
                            "cpu_s": cpu_s,
                            "pages": pages,
                            "peak_rss_mib": rss,
                            "rows": rows,
                            "sha256": digest.hexdigest(),
                        }
                    ),
                    flush=True,
                )

    try:
        asyncio.run(measure())
    finally:
        server.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["prepare", "replay", "ndjson"])
    parser.add_argument("input", type=Path)
    args = parser.parse_args()
    {"prepare": prepare, "replay": replay, "ndjson": ndjson}[args.mode](args.input)
