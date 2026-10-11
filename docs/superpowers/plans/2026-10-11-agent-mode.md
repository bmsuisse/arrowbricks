# Agent Mode Implementation Plan

> For agentic workers: execute inline or use subagent-driven-development for independent renderer work.

**Goal:** Fast, zero-Python-dependency agent results: bounded typed Markdown on success and actionable JSON on database failure.

**Architecture:** An explicit `DatabricksClient.query_agent` calls a Rust/PyO3 entry point. Rust uses the existing lazy Arrow pipelines for SEA and Thrift, fetches bounded pages, renders directly from Arrow, and returns one string to Python. No SQL rewriting, automatic statement resubmission, full-result hashing or implicit schema queries. Existing APIs retain their behavior.

**Tech Stack:** Rust, arrow-rs, Tokio, PyO3, Python thin wrapper.

**Spec:** The user-approved interface and behavior in this conversation.

## Constraints

- No required Python dependencies, no import-time asyncio, no hardcoded schema/catalog.
- Default explicit agent budgets: 1000 rows, 10000 output bytes, 500 characters per cell, 60 seconds total.
- Whole UTF-8 output must fit max_bytes, including headers and completeness footer; reject budgets under 256 bytes before querying.
- A clipped cell is marked and makes rendered output incomplete even when every row is present.
- Count only fetched rows; report exact totals only after source exhaustion. Do not drain all data to count.
- Abort pending downloads on stopping, request best-effort server cancellation, and preserve external asyncio cancellation.
- Render NULL distinctly from a string containing NULL; escape pipes, backslashes, HTML/Markdown and line breaks.
- Structured errors preserve database diagnostic text, classify recovery, and never automatically execute corrected SQL.
- Estimates/percentage progress unavailable from these pipelines must not be fabricated.

## Tasks

1. Renderer: test-first Rust tests for typed compact Markdown, Unicode, escaping, null/string distinction, duplicate names, nested/decimal/timestamps, exact row and byte caps. Implement a renderer over Arrow batches with cached per-column formatters and bounded reusable string buffers. Benchmark direct formatting against JSON materialization; keep evidence of the winner.
2. Execution: add Rust agent entry point using existing lazy pipelines and a single total deadline. Fetch in bounded pages, stop when rendered output fills, preserve empty schema, and return honest exact/lower-bound footer. JSON errors have category/message/next_step; extract known error classes without discarding the original suggestion text.
3. Public interface: thin `query_agent` method plus stubs; mock e2e tests using both real HTTP and Thrift servers. Cover errors, timeout, cancellation, schema/parameters, early stopping, no required optional extras, and existing API regressions.
4. Documentation and evidence: README section, runnable environment-based example, renderer benchmark and read-only live warehouse tests on both protocols covering scalar/nested/Unicode/null values, empty results, malformed SQL, missing columns/tables, oversized results and cancellations. Version/changelog update for the feature.
5. Verification/review: run Rust unit/integration tests, Python and PyO3 tests, lint/type checks, release build and benchmark. Review whole diff before creating a ready PR; do not merge.
