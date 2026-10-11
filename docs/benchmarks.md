# Arrowbricks benchmarks

[Back to the quickstart](../README.md)

Measured against a real Databricks SQL warehouse (Azure Databricks, `2X-Small` **Pro** serverless warehouse, Photon on, 1-4 auto-scaling clusters -- the smallest/cheapest warehouse tier, deliberately: a bigger warehouse would narrow the gap by making the query itself slower and the client-side overhead this compares proportionally smaller). Query: `SELECT id, id * 2 AS doubled, CAST(id AS STRING) AS label FROM range(200000)` (200k rows, 3 columns), 3 timed runs after 1 discarded warm-up run, one connection reused per library:

| | avg | stdev | range |
|---|---|---|---|
| `databricks-sql-connector` | 0.88s | 0.06s | 0.83s - 0.95s |
| arrowbricks | 0.50s | 0.02s | 0.48s - 0.52s |

These historical latency measurements compare the connector's `fetchall()`
(Python rows) with arrowbricks' `fetchall_arrow()` (Arrow), which do different
amounts of materialization. The previously published 16 MB memory figures
were invalid: the script sampled peak RSS before importing the libraries or
running queries. It now measures after the workload; rerun it for actual
peak memory on your machine. The installed-footprint measurements were:

| | installed size (package + all required deps) |
|---|---|
| `databricks-sql-connector` | 71 MB (pulls in `pandas`, `numpy`, `thrift`, `oauthlib`, `lz4`, `requests`, `urllib3`, `openpyxl`, ... as hard dependencies -- `pandas`+`numpy` alone are 61 MB of that) |
| arrowbricks | 9 MB (zero required runtime dependencies -- the whole thing is one compiled Rust extension, built with `opt-level="s"`, `ring` instead of the heavier default TLS crypto backend, and no unused HTTP/2 support -- see `rust/arrowbricks_core/Cargo.toml`'s own `[profile.release]` and `rustls`/`hyper-rustls` comments for the measurements behind those choices) |

Run it yourself: [`examples/benchmark_vs_connector.py`](../examples/benchmark_vs_connector.py) (needs both packages installed: `pip install arrowbricks databricks-sql-connector`). Reads `DATABRICKS_HOST`/`DATABRICKS_WAREHOUSE_ID`/`DATABRICKS_TOKEN` from the environment or a `.env` file (never commit one with a real token in it) and runs each library in its own subprocess so peak memory reflects that library alone:

```bash
DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \
DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
python examples/benchmark_vs_connector.py
```

For comparing arrowbricks versions with identical APIs, use
[`examples/benchmark_versions.py`](../examples/benchmark_versions.py). It keeps
one connection per version, discards warm-ups, alternates execution order,
passes concurrency explicitly, and checks row counts. Add `--verify-ipc`
(requires `arro3-core`) to compare serialized Arrow results in memory after
timing. Results must have stable values, schema metadata, row order, and batch boundaries;
verification contributes to peak process memory, and checksums are not printed. See
[`benchmarks/2026-09-06.md`](../benchmarks/2026-09-06.md) for replay/cache measurements
and [`benchmarks/2026-09-06-downloads.md`](../benchmarks/2026-09-06-downloads.md)
for the subsequent cloud-fetch scheduling measurements and limits.

Follow-up experiments cover [spare request slots](../benchmarks/2026-09-06-spare-slots.md),
[concurrent replay and NDJSON encoding](../benchmarks/2026-09-06-lowlevel.md),
and the rejected [LZ4 capacity](../benchmarks/2026-09-06-lz4-capacity.md) and
[direct-fill buffer](../benchmarks/2026-09-06-direct-fill.md) changes.

Cached IPC replay can be measured without a warehouse:

```bash
uv run python examples/benchmark_replay.py --rows 200000 --columns 16 --repeats 8
```

Replays now share immutable input bytes across decoded tables, reducing
repeated copies and memory use. Arrow may still copy misaligned fixed-width
buffers or decompress IPC-compressed bodies. Keeping a small slice of a
decoded array can retain the full source allocation until that slice is released.
Decoding releases the Python interpreter lock, so independent replay calls
can run concurrently on separate Python threads.

`BENCHMARK_SQL` overrides the query, `BENCHMARK_RUNS` (default 3) controls how many timed runs to average.
