# Distributing spare cloud-fetch request slots

This local candidate builds on the unreleased IPC/NDJSON changes. Its download
scheduler distributes the remainder of the per-batch request budget instead of
discarding it. With 35 known files and concurrency 64, 29 files may use two
requests and six may use one. The previous calculation allowed only one per
file, leaving 29 slots unused. The shared semaphore still caps total requests,
including queries sharing a client. The default remains 64; the per-file cap
remains eight. Files queue normally when there are more files than slots.

## Real warehouse comparisons

Baseline: the locally built 3.1.3 wheel. Candidate: the same release build
configuration plus the current changes. IPC replay and NDJSON optimizations
are included but neither path is called by the timed `fetchall_arrow` workload.
The machine and toolchain match [the earlier report](2026-09-06-lowlevel.md).

Every pair compares the complete returned Arrow result using SHA-256 over
serialized batches in memory, after timing. All comparisons matched. Row counts,
chunk counts and downloaded byte totals also matched. No query text, credentials,
workspace/table identifiers, result values or result checksums appear in the
[raw measurements](2026-09-06-spare-slots.json). The hashes in its `artifacts`
section identify compiled libraries only.

| Workload | Timed pairs | Baseline median | Candidate median | Interpretation |
| --- | ---: | ---: | ---: | --- |
| 500k rows, 35 chunks: first comparison | 4 | 12.478 s | 10.886 s | 12.8% lower latency; 4/4 wins |
| 500k rows, 35 chunks: fresh comparison | 6 | 10.377 s | 9.877 s | 4.8% lower latency; 6/6 wins |
| 500k rows: same-baseline control | 4 | 11.503 s | 11.063 s | 3.8% difference with identical code |
| 10k rows, one chunk: first comparison | 8 | 619 ms | 694 ms | Candidate 12.1% slower |
| 10k rows, one chunk: fresh comparison | 8 | 641 ms | 644 ms | Effectively unchanged, 0.4% slower |
| 10k rows: same-baseline control | 8 | 640 ms | 622 ms | 2.7% difference with identical code |

The large-query candidate won all ten measured pairs across two independently
started comparisons. Its second median advantage is modest relative to the
control variation, so these observations do not establish a fixed percentage
gain on other workloads. The one-file scheduling policy is unchanged, and the
initial small-query slowdown did not reproduce. No small-query gain is claimed.

Large queries downloaded 310,614,409 bytes each; small queries downloaded
6,074,427 bytes each. All queries returned 120 columns. They were read-only,
with Thrift, LZ4 enabled and concurrency explicitly set to 64. Each comparison
uses two persistent worker processes, discards two warmup pairs and alternates
A/B and B/A order. Only one benchmark runs at a time, with builds stopped.
The small-query first comparison also exercises the public `--verify-ipc`
benchmark option. Other comparisons use the equivalent private harness.

## Memory and size tradeoffs

Large-query peak process RSS rose from 1,016.6 to 1,087.1 MiB in the first
comparison and from 1,023.3 to 1,112.8 MiB in the second. These high-water marks
include untimed verification and allocator retention across queries. The change
does **not** demonstrate lower large-query memory use. More split downloads can
hold both part buffers and their assembled copy; removing that copy is a
separate hypothesis requiring validation.

The compiled extension grew from 9,288,784 to 9,288,800 bytes: 16 bytes. No new
Python dependency or public query option was added. Persisted cache sizes are
unchanged.

## Correctness and reproduction

A new integration test gives two known files three slots and verifies that the
first file uses two range requests, the second retains its slot, and all 120k
synthetic rows survive in order. It fails against the previous scheduler and
passes against the candidate. Existing tests continue to cover shared-budget
contention, truncation, ordering, cancellation and failed range retries.

The final Rust suite also found an unrelated existing panic for `STRUCT<>` in
INLINE conversion. Replacing Arrow's panicking constructor with its fallible
counterpart returns the normal conversion error without resubmitting a succeeded
statement. Regression cases cover zero rows, an empty object and a null struct;
the generated property-test seed is retained. The artifact hashes above identify
the measured scheduling candidate, before this subsequent conversion fix.

After that fix, 121 Rust tests, 65 Python tests and 48 PyO3 tests passed
(234 total; three manual Rust benchmarks ignored and one Python test skipped).
The Python suites used the rebuilt release wheel. Ruff, type checking,
formatting and Clippy passed with the existing `type_complexity` allowance.
The rebuilt extension remains 9,288,800 bytes; its hash is recorded separately
as `candidate_after_struct_fix`.

The rebuilt wheel also passed a final 500k-row warehouse check: two warmup
pairs and one timed pair, all IPC checksums matching. The timed pair was
14.350 versus 11.939 seconds with identical 35 chunks and 310,614,409 downloaded
bytes. This final-build check is recorded separately as `spare-final-smoke`;
one timed pair is not an additional repeatability claim.

To reproduce with your own stable query, install the benchmark dependencies
(including `arro3-core` for verification) and unpack each wheel separately:

```bash
python examples/benchmark_versions.py \
  --baseline /tmp/baseline --candidate /tmp/candidate \
  --sql 'SELECT * FROM your_catalog.your_schema.your_table LIMIT 500000' \
  --expected-rows 500000 --protocol thrift --concurrency 64 \
  --warmups 2 --runs 6 --verify-ipc
```

Use a stable snapshot and row order: IPC verification is stricter than logical
value equality and also checks batch boundaries and serialization. Repeat with
both paths pointing to the baseline for the control. Verification hashes remain
inside the worker/parent pipes and are removed before metrics are printed.
