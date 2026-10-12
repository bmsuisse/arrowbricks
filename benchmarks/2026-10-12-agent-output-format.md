# Agent output format: TSV vs Markdown vs others

Question: which text layout should `query_agent` return? Measured on tokens
(cost) and on whether a model can read the result correctly.

## Method

- Five deterministic result sets: `orders` (100 rows, 7 typed columns, 30% NULL
  notes), `wide` (50 rows x 30 numeric columns), `nested` (40 rows with a
  struct, an array and a map), `big` (1,000 rows x 5), and `escapes` (12 rows
  holding commas, pipes, quotes, newlines, `NULL` vs SQL NULL, empty string,
  Unicode, leading/trailing spaces, and a string that looks like a table).
- **Tokens:** `tiktoken` `o200k_base` (also `cl100k_base`, same order). This is
  not the Claude tokenizer; treat ratios, not absolute counts, as meaningful.
- **Readability:** one Claude Haiku agent per format wrote its own encoder,
  then answered 24 questions (lookups, filtered counts, sums, NULL and
  escaping checks, nested access) from the encoded text only. Answers were
  graded against ground truth the agents could not see. 12 formats/variants
  were tried, including TOON, JSON, NDJSON, YAML, CSV, columnar JSON and three
  invented variants (dictionary-encoded, run-length columnar, trimmed Markdown).
- **Real renderer:** the table below re-measures the shipped Rust renderer on
  `orders`, `wide`, `big`, `escapes`, `nested` (schemas inferred from NDJSON).
  The pre-change padded Markdown is reconstructed from the new Markdown output
  by re-adding the spaces (same escaping), not run from the old build.

## Result: shipped renderer, `o200k_base` tokens

| Result set | TSV | Markdown (new) | Markdown (6.1) | TSV vs 6.1 |
| --- | ---: | ---: | ---: | ---: |
| orders | 2,510 | 3,164 | 3,941 | -36% |
| wide | 6,062 | 6,175 | 7,734 | -22% |
| big | 15,903 | 17,915 | 23,924 | -34% |
| escapes | 109 | 156 | 194 | -44% |
| nested | 1,835 | 1,879 | 2,085 | -12% |
| **total** | **26,419** | **29,289** | **37,878** | **-30%** |

New Markdown (no padding, `|-|` separator) is -23% against 6.1. 6.1's table
also escaped `_` and quoted every string, which the TSV layout does not need.

## Result: prototype encoders, all formats

Token totals over the five sets (prototype encoders, so slightly different
from the table above), and Haiku reader score out of 24:

| Format | Tokens | vs padded Markdown | Reader score |
| --- | ---: | ---: | ---: |
| TSV | 25.8k | -34% | 24 |
| CSV | 26.2k | -33% | 24 (b) |
| trimmed Markdown, run-length columnar, dictionary (invented) | 26.1k-26.6k | -32% to -33% | 23 each |
| columnar JSON | 27.2k | -30% | 23 |
| TOON | 28.8k | -26% | 23 |
| compact Markdown, spaced pipes | 31.7k | -19% | 24 |
| padded Markdown | 39.1k | baseline | 24 |
| JSON | 42.4k | +8% | 23 |
| NDJSON | 43.6k | +12% | 23 |
| YAML | 54.3k | +39% | 24 |

Scores exclude one ambiguous question, `escapes-7` (a literal backslash-n versus
a real newline), counted correct for every format. (b) The CSV agent had seen some of the data before
answering, so its run was not fully blind.

## Reading the results

- Every miss was hand-counting or hand-adding over the 50-row `wide` table
  (plus one nested count), not a format-specific failure. 24 questions cannot
  separate the formats on accuracy; they show nothing is unreadable.
- TSV and CSV are cheapest because they state each column name once and add
  almost no punctuation. A pipe table pays for a `|` and spacing per cell.
- TOON beat JSON and YAML, but lost to TSV/CSV on flat tables and, on the
  `nested` set, fell back to a verbose list form (its tabular form needs flat
  values): 2,430 tokens against about 1,500 for columnar JSON. Its paper
  (arXiv 2605.29676) compares against JSON only. It has no explicit delimiters,
  and the crate's streaming parses the whole input first, which conflicts with
  the bounded renderer.
- The invented variants saved at most 4-5% over TSV and cost readability.
- gzip+base64 (unreadable control) came to 17.7k tokens: only 31% under TSV,
  so no readable format is near the floor.

## Why TSV needs the escaping it has

Tabs and newlines are the only characters TSV can't hold raw. Values are
written PostgreSQL-`COPY`-style: `\\`, `\t`, `\n`, `\r`, other control
characters as `\uXXXX`, NULL as `\N`, empty string as an empty field. A
property test round-trips random strings (tabs, newlines, backslashes, `\N`,
control characters) and checks every row is one line with exactly
`columns - 1` tabs.

## Limits

Small question set; OpenAI tokenizers, not Claude's; encoders written by the
model under test (except TOON, which used `@toon-format/toon` 4.4.0); the 6.1
column is reconstructed. Nested values still use Arrow's display text, so a
NULL inside a list prints as `NULL` in both layouts.
