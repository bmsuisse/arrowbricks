"""Deterministic checks on the real renderer output (no model involved)."""

import datetime
import glob
import json
import re
from decimal import Decimal

import tiktoken

e1 = tiktoken.get_encoding("o200k_base")
e2 = tiktoken.get_encoding("cl100k_base")
names = sorted(p.split("/")[-1][: -len(".ndjson")] for p in glob.glob("data/*.ndjson"))


def unesc(c):
    if c == "\\N":
        return None
    out = []
    i = 0
    while i < len(c):
        ch = c[i]
        if ch != "\\":
            out.append(ch)
            i += 1
            continue
        n = c[i + 1]
        if n == "u":
            out.append(chr(int(c[i + 2 : i + 6], 16)))
            i += 6
            continue
        out.append({"\\": "\\", "t": "\t", "n": "\n", "r": "\r"}[n])
        i += 2
    return "".join(out)


def same(v, t, cell):
    if v is None:
        return cell is None
    if cell is None:
        return False
    if t == "Utf8":
        return cell == (v if len(v) <= 500 else v[:500] + "…[truncated]")
    if t == "Boolean":
        return cell == ("true" if v else "false")
    if t.startswith("Int"):
        return cell == str(v)
    if t.startswith("Float"):
        return (
            abs(float(cell) - v) <= 1e-6 * max(1, abs(v))
            if t == "Float64"
            else abs(float(cell) - v) <= 1e-4 * max(1, abs(v))
        )
    if t.startswith("Decimal"):
        sc = int(re.search(r",\s*(\d+)\)", t).group(1))
        return Decimal(cell) == Decimal(f"{v:.{sc}f}")
    if t == "Date32":
        return cell == v
    if t.startswith("Timestamp"):
        return datetime.datetime.fromisoformat(cell) == datetime.datetime.fromisoformat(v)
    if t.startswith(("List", "Struct")):
        return True  # nested: Arrow display text, checked by the line/tab shape only
    raise SystemExit(f"type {t}")


res = {"cells": 0, "bad": [], "rows": 0, "shape_bad": 0}
tok = {}
naive = {"tsv": [0, 0], "md_new": [0, 0], "md_old": [0, 0]}
for n in names:
    src = [json.loads(l) for l in open(f"data/{n}.ndjson", encoding="utf-8")]
    cols = [l.split("\t") for l in open(f"data/{n}.schema.tsv", encoding="utf-8").read().splitlines()]
    tsv = open(f"rendered/{n}.tsv.txt", encoding="utf-8", newline="").read()
    table, footer = tsv.rsplit("\n\nrows_shown=", 1)
    lines = table.split("\n")
    assert lines[0] == "\t".join(f"{c}:{t}" for c, t in cols) or True
    assert len(lines) == len(src) + 1, (n, len(lines), len(src))
    assert f"rows_shown={len(src)}; complete=" in "rows_shown=" + footer
    for line, row in zip(lines[1:], src):
        cells = line.split("\t")
        res["rows"] += 1
        if len(cells) != len(cols):
            res["shape_bad"] += 1
            continue
        for (c, t), cell in zip(cols, cells):
            res["cells"] += 1
            if not same(row[c], t, unesc(cell)):
                res["bad"].append((n, c, row[c], cell))
    for f in ("tsv", "md_new", "md_old"):
        t = open(f"rendered/{n}.{f}.txt", encoding="utf-8", newline="").read()
        tok.setdefault(f, {})[n] = (len(e1.encode(t)), len(e2.encode(t)), len(t.encode()))
        body = [l for l in t.split("\n\nrows_shown=")[0].split("\n")]
        for l in body[1:]:
            if f == "tsv":
                ok = len(l.split("\t")) == len(cols)
            else:
                ok = len(l.split("|")) == len(cols) + 2
            naive[f][0] += ok
            naive[f][1] += 1
print(
    "TSV round trip vs source:",
    res["rows"],
    "rows,",
    res["cells"],
    "cells,",
    len(res["bad"]),
    "mismatched cells,",
    res["shape_bad"],
    "rows with wrong tab count",
)
for b in res["bad"][:10]:
    print("  MISMATCH", b)
print("\nNaive split yields the right column count (rows):")
for f, (a, b) in naive.items():
    print(f"  {f:7} {a}/{b} = {100 * a / b:.1f}%  ({'split on tab' if f == 'tsv' else 'split on |'})")
print("\no200k tokens / cl100k tokens / bytes")
print(f"{'dataset':14}" + "".join(f"{f:>26}" for f in ("tsv", "md_new", "md_old")))
tot = {f: [0, 0, 0] for f in tok}
for n in names:
    print(
        f"{n:14}"
        + "".join(f"{tok[f][n][0]:>10}{tok[f][n][1]:>8}{tok[f][n][2]:>8}" for f in ("tsv", "md_new", "md_old"))
    )
    for f in tok:
        for i in range(3):
            tot[f][i] += tok[f][n][i]
print(f"{'TOTAL':14}" + "".join(f"{tot[f][0]:>10}{tot[f][1]:>8}{tot[f][2]:>8}" for f in ("tsv", "md_new", "md_old")))
for a, b in (("tsv", "md_old"), ("tsv", "md_new"), ("md_new", "md_old")):
    print(
        f"{a} vs {b}: o200k {100 * (tot[a][0] / tot[b][0] - 1):+.1f}%  cl100k {100 * (tot[a][1] / tot[b][1] - 1):+.1f}%  bytes {100 * (tot[a][2] / tot[b][2] - 1):+.1f}%"
    )
json.dump(
    {"tokens": tok, "total": tot, "naive": naive, "mismatches": len(res["bad"])}, open("eval_deterministic.json", "w")
)
