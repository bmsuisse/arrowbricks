# Cloud-fetch scheduling measurements, 2026-09-06

This follow-up compares against the **first-round optimized wheel**, already
including the replay/cache changes in [the earlier report](2026-09-06.md),
not against the original `e681224` release. Both versions are release builds
on macOS arm64, Python 3.14.3, Rust 1.93.1. Exact extension hashes, artifact
sizes, and all raw samples are in [the JSON report](2026-09-06-downloads.json).

## What changed

Range downloads now start their remaining requests when the probe's headers
arrive, overlapping its first MiB of body transfer. A failed probe cancels
and joins its tail requests before retrying. Dropping the download aborts
outstanding range tasks; completed parts are assembled in their original order.

Each discovered batch also limits ranges per file to its share of the
configured concurrency budget. Previously, early files could grab eight
slots before other known files acquired one. One-file results still split;
35 known files under a 64-slot budget each use one request. The existing
shared semaphore still enforces the global cap, including concurrent queries.
This is a per-batch allocation rule, not global knowledge of future batches.
The public default remains 64 and fetching remains lazy.

## Final build: real warehouse results

Both persistent workers used the same actual warehouse and
`SELECT * FROM your_catalog.your_schema.your_table LIMIT ...` (120 columns), Thrift,
compression enabled, and concurrency **explicitly set to 64**. Two warm-ups
per worker were discarded, then A/B and B/A order alternated. No other
benchmark or build ran concurrently. Timings cover execute plus fetchall_arrow.

| Result | Pairs | Before median | After median | Lower latency | Candidate wins |
|---|---:|---:|---:|---:|---:|
| 10,000 rows, one chunk | 12 | 738.6 ms | 653.7 ms | 11.5% | 10/12 |
| 500,000 rows, 35 chunks | 6 | 13.213 s | 12.520 s | 5.2% | 4/6 |
| Same baseline in both workers, 10,000 rows | 12 | 781.3 ms | 781.2 ms | 0.02% | 6/12 |

The small-query fetch phase fell from 447.7 to 377.9 ms (median).
The unchanged-code control supports the small-query improvement. The large
result's 5.2% difference is within the 7.6% same-code variation previously
observed on this warehouse; **a general large-query throughput gain is not
established**. These are workload-specific measurements, not universal promises.

Every measured run checked its full expected row count. Both versions
reported identical chunk counts and downloaded bytes: 6,074,427 for 10,000
rows and 310,614,409 for 500,000 rows. This checks volume, not cell-by-cell
equality of the independently executed live queries. Local regression tests
check exact downloaded bytes and decoded values.

Peak process RSS was 70.7 versus 75.9 MiB for the small query and 869.2
versus 908.5 MiB for the large query. This round does not reduce query peak
memory. The final extension is 9,288,784 bytes (48 bytes above this round's
baseline); the wheel is 3,377,404 bytes, 3,485 bytes larger than that baseline
but still below the original release wheel's 3,382,093 bytes.

## Exploratory result and regression caught

Headers overlap alone first reduced small-query medians from 967 to 711 ms,
winning all 12 pairs, but increased the 500,000-row median from 12.908 to
14.070 seconds. That prompted the batch allocation fix. Those exploratory
samples are retained as `small` and `large` in the JSON; only `small-final`
and `large-final` measure the final combined build. `small-control` loads
the unchanged baseline in both workers despite its generic worker labels.

## Validation and reproduction

229 tests passed: 117 Rust, 65 Python cursor-level, and 47 PyO3-level;
one Rust benchmark was ignored and one Python test skipped. New regressions
prove that tails start before the probe body finishes, a truncated probe
retries without corrupting the result, and two known files with two slots
start together. The scheduling tests failed against the old implementation.
A pre-existing property-test fixture was corrected to generate at least one
nested field before unwrapping that field; parser behavior is unchanged.
Ruff, ty, formatting, and diff checks passed. Clippy passed with the existing
`type_complexity` lint allowed.

Unpack each release wheel into a separate directory containing `arrowbricks/`:

```bash
python examples/benchmark_versions.py \
  --baseline /tmp/old-wheel --candidate /tmp/new-wheel \
  --sql 'SELECT * FROM your_catalog.your_schema.your_table LIMIT 10000' \
  --expected-rows 10000 --protocol thrift --concurrency 64 \
  --warmups 2 --runs 12
```

For the large case, use 500000 rows and six runs. For the A/A control, pass
the same baseline directory to both version arguments. Credentials come
from the environment or `.env`; none are included in these reports.
