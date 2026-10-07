# Allocator benchmark, 2026-10-07 (arrowbricks 5.0.3 vs 5.1.0)

Why this exists: the 5.0.2 changelog figures came from a local mock server serving uncompressed
bodies and did not carry over to a real warehouse. These numbers are from a real Databricks SQL
warehouse with LZ4-compressed results (`compress_results=True`). The raw data is in
`2026-10-07-allocator.json`; workloads are described by shape, not by table name.

## Method

- One Linux x86_64 host (24 cores, 30 GB), Python 3.14, glibc, one warehouse, one day. The
  numbers are not portable to other hosts or to musl/macOS/Windows (the allocator is gated to
  Linux + glibc).
- Workloads: read-only `SELECT * ... LIMIT n` over five existing fact tables, from a 1,000-row
  result to about 1M rows x 104 columns. Result sizes are in the tables (compressed MB downloaded).
- **A/B** (`examples/benchmark_versions.py`): 5.0.3 and the allocator build alternate in the order
  A/B, B/A, ...; one discarded warm-up, 8 timed runs each, one persistent process per version.
  Peak RSS is the process high-water mark over those runs.
- **Repeat** (`repeat.py`-style loop): the same query 5 times in one process per version, RSS read
  from `/proc/self/statm` after each result is freed ("idle") plus the `ru_maxrss` peak.
- Checks: the Arrow bytes of both versions were compared (`--verify-ipc`) on a deterministic
  3M-row synthetic query (not warehouse data). 53 comparisons across both protocols were
  identical. One earlier Thrift comparison (5.0.3 vs the allocator build) reported a difference
  that never reproduced, including in 8 runs of 5.0.3 against itself; the cause is unknown.

## Interleaved A/B (8 timed runs per version)

| protocol | workload | downloaded MB | 5.0.3 median s | 5.1.0 median s | time | 5.0.3 peak MiB | 5.1.0 peak MiB | peak |
|---|---|---|---|---|---|---|---|---|
| sea | calldata: 1M rows, 104 cols (string/long) | 28.5 | 2.05 | 1.92 | -6% | 1468 | 660 | -55% |
| sea | ledger: 1M rows, 36 cols (numeric-heavy) | 101.4 | 5.25 | 4.41 | -16% | 576 | 268 | -53% |
| sea | strings: 1M rows, 37 cols (string-heavy) | 148.7 | 6.08 | 5.5 | -10% | 1514 | 552 | -64% |
| sea | tiny: 1000 rows, 27 cols | 0.1 | 0.38 | 0.38 | +0% | 38 | 38 | +0% |
| sea | wide: 500k rows, 137 cols (mixed types) | 92.2 | 4.1 | 4.44 | +8% | 1653 | 604 | -63% |
| thrift | calldata: 1M rows, 104 cols (string/long) | 29.2 | 1.9 | 1.86 | -2% | 765 | 661 | -14% |
| thrift | ledger: 1M rows, 36 cols (numeric-heavy) | 101.4 | 4.44 | 4.54 | +2% | 667 | 394 | -41% |
| thrift | strings: 1M rows, 37 cols (string-heavy) | 155.0 | 6.23 | 6.71 | +8% | 1245 | 717 | -42% |
| thrift | tiny: 1000 rows, 27 cols | 0.0 | 0.24 | 0.23 | -4% | 37 | 38 | +3% |
| thrift | wide: 500k rows, 137 cols (mixed types) | 99.0 | 4.11 | 4.25 | +3% | 1319 | 687 | -48% |

Time is within run-to-run noise: the same query on the same version varied by up to 2x between
runs. The largest individual differences are +8% (sea, wide) and -16% (sea, ledger) in both
directions, with no consistent sign. Tiny results are unaffected.

## Repeated queries in one process (5 runs)

| protocol | workload | 5.0.3 idle MB after run 5 | 5.1.0 idle MB | idle | 5.0.3 peak MB | 5.1.0 peak MB | peak |
|---|---|---|---|---|---|---|---|
| thrift | ledger | 478 | 181 | -62% | 670 | 396 | -41% |
| thrift | wide | 1363 | 152 | -89% | 1490 | 676 | -55% |
| thrift | strings | 1237 | 214 | -83% | 1416 | 693 | -51% |
| thrift | calldata | 167 | 92 | -45% | 765 | 658 | -14% |
| sea | ledger | 312 | 44 | -86% | 501 | 263 | -48% |
| sea | wide | 1487 | 66 | -96% | 1498 | 594 | -60% |
| sea | strings | 1105 | 52 | -95% | 1164 | 540 | -54% |
| sea | calldata | 1015 | 77 | -92% | 1343 | 645 | -52% |

Mean change in idle RSS after the fifth run: -81%. In 5.0.3, idle RSS keeps
growing with every query (one example: 201 MB after run 1, 1440 MB after run 5) because glibc's
dynamic mmap threshold sends multi-MiB buffers to the heap, where they fragment and are never
returned. Setting `MALLOC_MMAP_THRESHOLD_` or `MALLOC_TRIM_THRESHOLD_` to 1 MiB gave the same
result as the allocator build (idle about 100-160 MB, peak about 620-680 MB on the 500k-row wide
query), which is how the cause was identified.

## Caveats

- Fresh-process peak memory did not change: a 541 MB Arrow result peaked at about 740 MB for both
  4.0.0 and 5.0.3. The gain is for processes that run many queries.
- Not measured: very high download parallelism (each 1 MiB+ buffer is now an `mmap`/`munmap`
  pair), other platforms, and other allocators.
