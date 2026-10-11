# Agent rendering: design and measurement

The DuckDB v2.0 CLI switches to compact Markdown with types, explicit limits,
JSON errors and query feedback. It is a CLI behavior, not an Arrow/Python API
setting. Source: https://duckdb.org/2026/10/09/agent-mode and
https://github.com/duckdb/duckdb/pull/26167.

For Arrowbricks, the target path stays Arrow-native until one final Python
string: existing lazy fetch/decode -> bounded Markdown in Rust -> PyO3.
It does not construct Python row objects, serialize whole chunks to NDJSON,
or parse JSON back into rows to render a table.

Arrow-rs supplies `ArrayFormatter`, which caches a type-specific formatter per
column and writes values through `fmt::Write`. Reuse it for native scalar and
nested values rather than maintaining a second Arrow conversion implementation.
Source: https://arrow.apache.org/rust/arrow_cast/display/struct.ArrayFormatter.html.

A reusable buffer reserves capacity once, writes delimiters directly, and escapes
only the cell contents. Format work and Unicode-safe cell clipping belong in a
bounded sink so a very long value cannot cause a full-size intermediate string.
Do not scan every row to compute column widths: compact Markdown needs none.
`String::with_capacity` avoids reallocations until its capacity is exceeded:
https://doc.rust-lang.org/std/string/struct.String.html#method.with_capacity.

Benchmark the complete output path as well as the renderer, using identical
queries, schemas, limits, repeated interleaved runs, and a warm-up. A large
query's network/download time must not be attributed to the renderer.
Record output bytes, correctness, local encoding timings and live end-to-end
latency. Do not infer token savings from bytes without a tokenizer measurement.

Databricks result row/byte caps do not guarantee bounded scan cost. Its REST
`byte_limit` describes internal representation, not final rendered text:
https://docs.databricks.com/api/statement-execution/v1/execute-statement.
Current Arrowbricks pipelines do not expose DuckDB-like pre-execution scan
estimates or numeric progress. The agent API must not invent those.

## Direct DuckDB source inspection

Read the merged PR's pinned `ModeMarkdownRenderer` implementation and its tests:
https://github.com/duckdb/duckdb/blob/9f1b73003de37db9041cc769b3b63ad0eafaff4e/tools/shell/shell_renderer.cpp
and
https://github.com/duckdb/duckdb/blob/9f1b73003de37db9041cc769b3b63ad0eafaff4e/tools/shell/tests/test_agent_mode.py.

- Compact mode skips column-width analysis and streams results; it buffers only
  a bounded head and a small tail ring before choosing full output or a sample.
- It deliberately allows the first row beyond the byte budget, and its row
  budget excludes the header/footer. Arrowbricks instead promises a strict
  complete-output byte ceiling, including headers, escaped values and footer.
- DuckDB counts/hashes up to 100000 additional rows before stopping. A remote
  Databricks preview avoids that extra network/download work and does not claim
  an exact full count or fingerprint for a prefix.
- It checks stream failure before writing a success footer. Arrowbricks tests
  corrupt/failed downloads after earlier valid rows to enforce the same invariant.
- Cell truncation walks UTF-8 character boundaries and marks the shortened value.
  Arrowbricks bounds formatting itself, including nested strings, before producing
  a whole-cell temporary value.
- Shell `ErrorToJSON` reuses engine JSON, preserving the helpful diagnostic
  message while removing its duplicate `candidates` field. Arrowbricks similarly
  retains Databricks suggestions in the original error message, adding recovery
  categories rather than auto-executing repaired SQL.

No DuckDB code was copied into the implementation.

## Measured output size

For the same 100-row query `(id, doubled, label)`, complete typed Markdown
was 2039 UTF-8 bytes versus 3625 bytes of NDJSON (43.8% smaller). The same
strings measured 1139 versus 1500 tokens with `o200k_base` (24.1% fewer).
This is one query shape and tokenizer, not a promise for every result.

Five interleaved live runs on the final 6.0.0 release wheel measured Thrift
227 ms agent versus 285 ms NDJSON, and SEA 393 ms versus 375 ms. Earlier
debug runs were nearly equal. Network variance dominates these small samples;
they do not establish a general latency improvement. All 34 live correctness
cases passed, including cancellation, Unicode, nested values and SQL errors.
See [the recorded release results](2026-10-11-agent-mode-live.json).

The installed CLI also passed ten live checks in an environment containing only
Arrowbricks: success, capped stdin previews, syntax errors, missing columns and
SQL files, through both protocols. No optional Python Arrow package was needed.

## Local release renderer benchmark

Run `cargo run --manifest-path rust/arrowbricks_core/Cargo.toml --release
--no-default-features --example agent_render_bench`. Local average microseconds
per iteration on Apple Silicon:

| Input | Direct Arrow, 10 KB | JSON then Markdown, 10 KB |
|---|---:|---:|
| 1000 numeric rows | 92.32 | 484.08 |
| 1000 Unicode/nested rows | 41.80 | 7024.60 |
| One 4 MB string | 14.56 | 9630.11 |
| One 10 MB nested string | 11.51 | 32414.88 |

The comparator materializes the entire batch as JSON before rendering a bounded
preview; it omits typed header/footer construction. These timings measure that
specific conversion strategy, not every possible JSON implementation. With a
2 MB output allowance, Unicode/nested Markdown took 7.77 ms versus 6.89 ms for
JSON serialization alone. Bounded preview rendering is the optimization target.

During review, an existing empty SEA INTERVAL schema conversion failure was
reproduced against the released package and filed as
https://github.com/bmsuisse/arrowbricks/issues/18. The same zero-row query
works through Thrift. It is outside the new agent/CLI behavior.

## Autoresearch before release

Two committed experiments were measured before the requested release. The first
cached null checks and avoided failed string downcasts for nonstring cells. Its
short samples produced contradictory workload results, so it was discarded.

The second replaced per-control-character temporary strings with a ten-byte
stack buffer. An exhaustive test compares every Unicode control character with
the previous escaping contract, including short tab/newline/CR escapes. It was
retained: control-heavy previews measured median 262.82 µs before and 154.62 µs
after (41.2% lower). The six-workload geometric mean ratio was 0.89245.

The experiment harness compiled the actual renderer source with the same
`opt-level="s"`, LTO and single-codegen-unit settings. It included production
deadline/atomic-cancellation checks, 10000 iterations per bounded workload,
and five alternating baseline/candidate runs. Fixtures included numeric,
Unicode/nested, ordinary ASCII, control strings and oversized scalar/nested
strings. Output budgets and fixtures were unchanged between each pair.
The primary metric was the geometric mean of candidate/baseline median times
for 10 KB previews; full-output timings were recorded separately.
The committed benchmark reproduces these fixtures and iteration counts with
`cargo run --manifest-path rust/arrowbricks_core/Cargo.toml --release
--no-default-features --example agent_render_bench -- --agent-only`.

Machine contention caused substantial variation on other workloads, so these
measurements establish a focused control-escaping improvement, not a general
latency claim. See [the raw timings](2026-10-11-agent-autoresearch.json). The
loop stopped after these two experiments when release was requested. Results:

| Experiment | Commit | Score ratio | Decision |
|---|---|---:|---|
| Avoid nonstring downcasts | 30323a3 | 0.91271 | Discard: inconclusive samples |
| Stack control escaping | bcfbf10 | 0.89245 | Keep: affected workload improved |
