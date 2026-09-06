# LZ4 allocation hypotheses

Follow-up to the [IPC and NDJSON experiments](2026-09-06-lowlevel.md).
No production allocation policy changed. The proposed use of warehouse size
metadata did not pass this synthetic screening, so it was not deployed or tested
against the warehouse. No credentials or warehouse data were used in these runs.

Five initial capacities were compared: compressed size times one, two and four
(the current implementation), exact decoded size, and decoded size minus 64 bytes.
Each input is 8 MiB of deterministic bytes with a varying proportion of random
bytes, compressed as 16 concatenated LZ4 frames. Every decompression is checked
byte-for-byte against the original input.

Two independent release-mode runs used ten rounds per shape, rotating variant
order and discarding two warmups. There are 320 recorded timings in total.
The following are median milliseconds from the second run; the first run gives
the same broad tradeoffs. [Raw synthetic results](2026-09-06-lz4-capacity.json)
include both runs and retained capacities.

| Decoded/compressed ratio | 1x | 2x | Current 4x | Exact hint | Hint short by 64 bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 251.82 | 12.298 | 12.297 | 12.382 | 11.846 | 11.826 |
| 3.37 | 2.475 | 3.491 | 2.190 | 2.649 | 1.690 |
| 1.81 | 2.722 | 2.158 | 1.421 | 2.633 | 1.679 |
| 1.00 | 0.636 | 0.629 | 0.629 | 2.273 | 0.918 |

An exact capacity retains 8 MiB in every case, while a hint short by just 64
bytes grows to 16,777,088 bytes (almost 16 MiB). The current heuristic retains
8–32 MiB across these cases. Reducing the multiplier saves capacity for some
inputs, but slows other shapes; exact preallocation also regresses several
shapes. These are allocator- and workload-dependent measurements, not universal
claims about LZ4 speed. Capacity measures reserved buffer space, not resident
process memory or persisted cache size.

The decision is to keep the current heuristic. There is no demonstrated strategy
here that improves both speed and allocation across the tested inputs. A future
adaptive implementation needs representative size-metadata validation and live
E2E measurement before adoption. The benchmark remains as a reproducible way to
evaluate that work.

Run on the same macOS arm64 / Rust 1.93.1 machine as the earlier report:

```bash
CARGO_BUILD_JOBS=1 cargo test --manifest-path rust/arrowbricks_core/Cargo.toml --release --no-default-features --lib benchmark_lz4_capacity_hypotheses -- --ignored --nocapture
```

Both benchmark runs passed exact equality for every decoded input. Clippy and
formatting checks passed. The only production-source edits in this follow-up
correct comments: compressed size is not a lower bound on decoded size, and the
multi-frame loop terminates on reader exhaustion, not lack of output growth.
