import collections
import glob
import json
import math
import os
import re

S = os.path.dirname(os.path.abspath(__file__))
key = {q["id"]: q for q in json.load(open(f"{S}/eval_key.json"))}
mp = json.load(open(f"{S}/eval_map.json"))


def norm(x):
    s = str(x).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        s = s[1:-1]
    return s


def ok(ans, exp):
    if ans is None:
        return False
    a = str(ans).strip()
    e = str(exp)
    if a == e or norm(a) == norm(e):
        return True
    if norm(a).lower() == norm(e).lower() and not re.search(r"[\\\t]|\s{2}", e):
        return True
    try:
        fa, fe = float(norm(a).replace(",", "")), float(norm(e))
        return math.isclose(fa, fe, rel_tol=1e-9, abs_tol=1e-9)
    except ValueError:
        return False


runs = {}
for p in sorted(glob.glob(S + "/answers/*.json")):
    m, L, r = os.path.basename(p)[:-5].split("_")
    try:
        runs[(m, mp[L], r)] = json.load(open(p))
    except Exception as ex:
        print("BAD JSON", p, ex)
fmts = ["tsv", "md_new", "md_old"]


def cat(qid):
    q = key[qid]
    t = q["question"]
    if q["dataset"] == "text_messy" or "exact message" in t or "exact text" in t:
        return "exact/escaping"
    if t.startswith("How many") or "largest" in t or "highest" in t:
        return "count/extreme"
    return "lookup/other"


print("runs:", sorted(runs))
tot = collections.defaultdict(lambda: [0, 0])
bycat = collections.defaultdict(lambda: [0, 0])
byds = collections.defaultdict(lambda: [0, 0])
wrong = collections.defaultdict(list)
for (m, f, r), ans in runs.items():
    for qid, q in key.items():
        good = ok(ans.get(qid), q["answer"])
        for d, k in ((tot, (f, m)), (tot, (f, "all")), (bycat, (f, cat(qid))), (byds, (f, q["dataset"]))):
            d[k][0] += good
            d[k][1] += 1
        if not good:
            wrong[(f, qid)].append((m, r, ans.get(qid)))
print("\nAccuracy (correct/total across runs)")
for m in ("haiku", "sonnet", "all"):
    print(
        f" {m:7}"
        + "".join(
            f"  {f}: {tot[(f, m)][0]}/{tot[(f, m)][1]} = {100 * tot[(f, m)][0] / max(1, tot[(f, m)][1]):.1f}%"
            for f in fmts
        )
    )
print("\nBy question type")
for c in ("exact/escaping", "count/extreme", "lookup/other"):
    print(
        f" {c:15}"
        + "".join(
            f"  {f}: {100 * bycat[(f, c)][0] / max(1, bycat[(f, c)][1]):.1f}% (n={bycat[(f, c)][1]})" for f in fmts
        )
    )
print("\nBy dataset (accuracy %)")
for d in sorted({q["dataset"] for q in key.values()}):
    print(f" {d:13}" + "".join(f"  {f}: {100 * byds[(f, d)][0] / max(1, byds[(f, d)][1]):5.1f}" for f in fmts))
print("\nPaired differences (same question, same model+rep): wins for A / wins for B")
for A, B in (("tsv", "md_old"), ("tsv", "md_new"), ("md_new", "md_old")):
    aw = bw = 0
    for (m, f, r), ans in runs.items():
        if f != A or (m, B, r) not in runs:
            continue
        for qid, q in key.items():
            ga = ok(ans.get(qid), q["answer"])
            gb = ok(runs[(m, B, r)].get(qid), q["answer"])
            aw += ga and not gb
            bw += gb and not ga
    n = aw + bw
    p = 1.0 if n == 0 else min(1.0, 2 * sum(math.comb(n, k) for k in range(0, min(aw, bw) + 1)) / 2**n)
    print(f" {A} vs {B}: {aw} / {bw}  (exact sign test p={p:.3f})")
print("\nQuestions missed (by format), showing model/rep and the answer given")
for (f, qid), lst in sorted(wrong.items(), key=lambda x: (x[0][1], x[0][0])):
    print(
        f" {qid:16} {f:7} x{len(lst)}  exp={str(key[qid]['answer'])[:40]!r}  got={[(m[0] + r, str(a)[:30]) for m, r, a in lst][:4]}"
    )
