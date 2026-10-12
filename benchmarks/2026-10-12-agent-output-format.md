# Agent output format: TSV vs Markdown

Question: which layout should `query_agent` return by default? Measured with
the shipped Rust renderer on ten result sets: token cost, lossless decoding,
and whether Claude reads the result correctly. Reproduction scripts, data,
answers and the answer key are in
[`agent-output-format/`](agent-output-format/).

## Verdict

- **TSV is much cheaper:** 28% fewer `o200k_base` tokens (26% fewer
  `cl100k_base`) than 6.1's Markdown table, and 12% (9%) fewer than the new
  unpadded Markdown.
- **TSV is lossless and robust:** every one of 14,810 cells decoded back to the
  source value, every one of 2,150 rows had exactly the right number of tabs,
  and a test over every Unicode scalar value found no character that can
  break a row. A naive `split("|")` on the Markdown layouts gets the column
  count wrong for 16 of 2,160 table lines (cells containing a pipe); `split("\t")`
  on TSV was right for all 2,150.
- **Readability is a tie, not a win:** across 12 blind reader runs (Haiku and
  Sonnet, 82 questions each) TSV scored 97.6%, new Markdown 97.6% and 6.1
  Markdown 98.8%. Paired, TSV won 3 questions and 6.1 Markdown won 7 (sign
  test p = 0.34), so no real difference. Every miss was a hand count or
  "largest of many numbers" question; exact-string and escaping questions
  (tabs, newlines, pipes, backslashes, `\N`, NULL vs `'NULL'` vs empty) were
  100% for all layouts. So TSV costs less without costing accuracy; this
  benchmark does not show it improves accuracy.

## Method

- **Data:** ten deterministic result sets (2,150 rows): orders, events (UUIDs,
  timestamps), wide metrics (25 columns), nested (struct, list),
  text with tabs/newlines/pipes/quotes/backslashes/`\N`/`NULL`/empty/Unicode
  and a 600-character cell that gets clipped (plus column names `a|b` and
  `{x}`), paths and URLs full of `_ * & <`, a 70%-NULL table, a 1,000-row
  table, extreme numerics, and multi-line log messages.
- **Renderers:** this branch for TSV and the new Markdown; commit `5243cf6`
  (6.1.0) built unmodified for the old Markdown. Typed Arrow schemas, byte
  limits lifted so nothing but the 500-character cell clip applies.
- **Tokens:** `tiktoken` `o200k_base` and `cl100k_base`. These are not the
  Claude tokenizer; ratios, not absolute counts, are what carry over.
- **Reader test:** 82 questions (lookups, filtered counts, extremes, NULL
  handling, column types, exact strings with escaping, clip detection).
  Twelve agents (Claude Haiku and Sonnet, three layouts, two runs each, the
  second with the datasets in reverse order) got only the printed files for
  one layout, labelled X/Y/Z so the layout was not named, and only the Read
  tool (no code). They were told to answer with the original value, undoing
  whatever escaping the layout used. Answers were graded against a key
  computed from the source data, which they could not see. Six runs ran one
  `ls` on their folder despite the rules; it listed file names only.

## Tokens (shipped renderer)

| Result set | TSV | Markdown (new) | Markdown (6.1) |
| --- | ---: | ---: | ---: |
| big (1000 rows) | 16,725 | 18,894 | 25,904 |
| events | 15,944 | 18,226 | 20,911 |
| orders | 5,177 | 6,222 | 7,834 |
| wide metrics | 7,164 | 7,129 | 9,088 |
| paths and URLs | 4,588 | 6,027 | 6,623 |
| numeric | 4,102 | 4,128 | 4,886 |
| logs | 3,580 | 4,626 | 5,234 |
| nested | 2,095 | 2,337 | 2,643 |
| nulls heavy | 1,983 | 2,032 | 2,390 |
| text, messy | 585 | 724 | 914 |
| **total** | **61,943** | **70,345** | **86,427** |
| vs Markdown (6.1) | **-28.3%** | -18.6% | baseline |

TSV is not cheaper on every set: on the 25-column all-numbers table it is a
tie with the new Markdown (7,164 against 7,129), and on `numeric` nearly one.
Bytes show the same order (121.9 kB, 140.5 kB, 170.5 kB). 6.1's table also
escaped `_` (including in column names) and quoted every string, which the
other two layouts mostly avoid.

## Reader accuracy (82 questions x 4 runs per layout)

| | TSV | Markdown (new) | Markdown (6.1) |
| --- | ---: | ---: | ---: |
| Haiku | 158/164 (96.3%) | 157/164 (95.7%) | 160/164 (97.6%) |
| Sonnet | 162/164 (98.8%) | 163/164 (99.4%) | 164/164 (100%) |
| all | 320/328 (97.6%) | 320/328 (97.6%) | 324/328 (98.8%) |
| exact-string / escaping questions | 100% | 100% | 100% |
| count / extreme questions | 93.8% | 93.8% | 96.9% |

Twelve of the 82 questions were missed at least once (listed by `grade_eval.py`): counting rows (some
over 1,000) and picking the largest of nearly equal 13-digit numbers.
Sonnet missed the largest-ratio question under TSV in both runs
(answering id 146, 8.63e12, instead of 103, 8.82e12). With four runs per
cell this cannot be told from chance, but it is the one place TSV did worse
in a way that repeated.

## Earlier survey of other formats

Before this, 12 other encodings (CSV, JSON, NDJSON, YAML, columnar JSON,
TOON, padded Markdown, spaced compact Markdown and three invented variants:
dictionary-encoded, run-length columnar, trimmed Markdown) were encoded with
prototype Python encoders and read by Haiku over 24 questions on five sets.
Token totals (o200k): TSV 25.8k, CSV 26.2k, trimmed Markdown 26.2k, run-length
columnar 26.1k, dictionary 26.6k, columnar JSON 27.2k, TOON 28.8k, spaced
Markdown 31.7k, padded Markdown 39.1k, JSON 42.4k, NDJSON 43.6k, YAML 54.3k.
Reader scores were 23-24 of 24 for all. TOON beat JSON and YAML but lost to
TSV/CSV on tables and fell back to a verbose list form for nested data (2,430
tokens against about 1,500 for columnar JSON); the invented variants saved at
most 4-5% over TSV. gzip+base64 (unreadable) was 17.7k, only 31% under TSV.
This survey used the model under test to write its encoders, so only its token
ordering is relied on; the shipped-renderer numbers above are the evidence.

## Why TSV needs the escaping it has

Tabs and newlines are the only characters TSV cannot hold raw. Values are
written PostgreSQL-`COPY`-style: `\\`, `\t`, `\n`, `\r`, other control
characters as `\uXXXX`, NULL as `\N`, empty string as an empty field. Tests:
a round-trip property test over random strings, an exhaustive test over every
Unicode scalar value (each must decode back and never add a tab or line), and
the 14,810-cell comparison above.

## Limits

Four runs per layout and model; OpenAI tokenizers, not Claude's; one task type
(read a printed result); nested values keep Arrow's display text, so a NULL
inside a list prints as `NULL` in both layouts. The 6.1 Markdown was measured
from the real 6.1 code, not reconstructed.
