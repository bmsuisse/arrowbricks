# Agent output format benchmark (TSV vs Markdown)

Results and interpretation: [`../2026-10-12-agent-output-format.md`](../2026-10-12-agent-output-format.md).

Pipeline:

1. `python gen_eval.py eval_key.json` writes `data/*.ndjson`, `data/*.schema.tsv`, `data/questions_blind.json` and the answer key (seeded, deterministic).
2. Build `render_nd.rs` twice: copy it to `rust/arrowbricks_core/examples/` on this branch for `render_new` (TSV and the unpadded Markdown), and on commit `5243cf6` (6.1.0, with the `format` argument and `OutputFormat` import removed) for `render_old`. `cargo build --release --example render_nd`.
3. `./render_all.sh` renders every dataset in the three layouts into `rendered/`.
4. `python verify_tsv.py` decodes the TSV back against the source data, checks naive column splitting and counts tokens (`pip install tiktoken`).
5. Blind reader test: `rendered/` files were copied into folders X, Y, Z (the format-to-letter map is `eval_map.json`), and each Claude Haiku / Sonnet agent got `READER_TASK.md` and one folder (Read tool only). Answers are in `answers/`. `python grade_eval.py` grades them against `eval_key.json`.
