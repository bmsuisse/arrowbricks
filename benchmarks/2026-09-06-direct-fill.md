# Direct filling of split-download buffers — rejected

This prototype was **rejected and removed** after the completed real-warehouse
comparison showed slower downloads without a meaningful memory reduction.

## Hypothesis and implementation

The split downloader previously collected each HTTP response into a separate
buffer and copied the parts into another allocation. The prototype partitions
one `BytesMut` allocation into disjoint regions, fills each region from HTTP
frames, and rejoins completed regions in range order. Successful regions rejoin
without copying. A retried tail receives a replacement allocation and uses
`unsplit`'s safe copying fallback. Probe retries still cancel and join their old
tail tasks. Whole-file responses retain the existing path.

The allocation-sharing test verifies the original pointer survives HTTP filling
and rejoining. Other new tests exercise a replacement allocation after a failed
tail request and exact reassembly of an 11 MiB synthetic payload across eight
uneven ranges. No unsafe code, new dependencies or concurrency changes were
introduced. Individual range lengths are checked before accepting their bytes.

## Prototype validation

- 124 Rust, 65 Python and 48 PyO3 tests passed: 237 total, with three manual Rust
  benchmarks ignored and one Python test skipped. Python tests used the release
  wheel. Ruff, type checking, formatting and Clippy passed with the existing
  `type_complexity` allowance.
- The extension grew from 9,288,800 to 9,305,312 bytes: 16,512 bytes. A runtime
  benefit must justify that cost before retaining the change.
- The first unpinned live comparison failed the full-result IPC checksum check
  before producing any valid measurement. Its cause is unresolved; it is not
  counted as evidence of correctness or speed.
- A subsequent 500k-row control used the same unchanged candidate on both sides
  and a common table version. All three pairs matched IPC checksums. Its single
  timed pair was 10.863 versus 10.865 seconds, 35 chunks and 310,614,409 bytes
  per worker. This validates the control setup, not the new implementation.
- Subsequent attempts encountered request failures. A diagnostic identified
  the unchanged baseline as the failing worker (`error sending request`). Two
  independent warehouse endpoint checks also timed out. Those failed attempts
  provide no performance samples.
- A later bounded old/new check completed against the same table version:
  all three 10k-row pairs matched full IPC checksums.
- The bounded 500k-row old/new run then matched full IPC checksums in both
  warmup pairs. Its first timed pair was interrupted by a `TransientError`
  from the candidate, so it produced no accepted performance sample. A complete
  timed comparison was subsequently completed, as recorded below.

Both workers use the same read-only table version selected from history.
Databricks documents [table history and time travel](https://docs.databricks.com/aws/en/tables/history).
The version, identifiers, credentials, data and result checksums remain private.
All workload queries still request the same real 120-column data; using a
snapshot does not replace it with synthetic data.

## Completed comparison and decision

After connectivity recovered, the comparison completed with two warmup pairs
and four timed pairs, alternating execution order. Both implementations returned
500k rows, 120 columns, 35 chunks and 310,614,409 downloaded bytes. All six pairs
matched the complete IPC result. No builds or other benchmarks ran concurrently.

| Metric | Retained candidate | Direct-fill prototype |
| --- | ---: | ---: |
| Median query time | 11.150 s | 12.584 s |
| Median peak process RSS | 1,007.94 MiB | 1,010.80 MiB |
| Compiled extension | 9,288,800 bytes | 9,305,312 bytes |

The prototype was **12.9% slower**, used essentially the same peak memory, and
added 16,512 bytes to the extension. It and its prototype-only tests were removed.
Peak RSS includes the untimed result verification and allocator retention.
These measurements do not establish why direct filling was slower; they are
sufficient evidence against retaining it for this workload.

[Raw measurements and compiled-artifact hashes](2026-09-06-direct-fill.json)
contain no warehouse identifiers, SQL, credentials, result values or result
checksums. Earlier failed attempts contributed no performance samples.

The retained implementation and its 234 passing tests remain documented in
[the spare-slot report](2026-09-06-spare-slots.md). The 237-test count above
belongs to the rejected prototype, not the release candidate.
