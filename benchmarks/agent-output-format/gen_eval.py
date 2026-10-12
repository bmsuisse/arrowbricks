"""Ten realistic result sets (NDJSON + Arrow schema) and graded questions. Answers go to the key file only."""

import datetime
import json
import random
import sys

R = random.Random(7)
OUT = "data"
KEY = sys.argv[1]


def pick(xs):
    return R.choice(xs)


def d(i):
    return (datetime.date(2025, 1, 1) + datetime.timedelta(days=(i * 7) % 700)).isoformat()


def ts(i):
    return (
        datetime.datetime(2026, 3, 1, 8, 0, 0)
        + datetime.timedelta(seconds=i * 37 + R.randint(0, 36), microseconds=R.randint(0, 999999))
    ).strftime("%Y-%m-%dT%H:%M:%S.%f")


def uuid():
    return "-".join("".join(R.choice("0123456789abcdef") for _ in range(n)) for n in (8, 4, 4, 4, 12))


FIRST = ["anna", "ben", "carla", "dev", "elif", "finn", "gina", "hugo"]
LAST = ["meier", "rossi", "nguyen", "silva", "khan", "tanaka"]
DS = {}
Q = []


def add(ds, q, a):
    Q.append({"id": f"{ds}-{sum(1 for x in Q if x['dataset'] == ds) + 1}", "dataset": ds, "question": q, "answer": a})


def esc(s):
    return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


# 1 orders
rows = []
for i in range(1, 201):
    rows.append(
        {
            "id": i,
            "customer_id": f"cust_{R.randint(1, 60):04d}",
            "order_date": d(i),
            "amount": round(R.uniform(5, 3000), 2),
            "currency": pick(["CHF", "EUR", "USD"]),
            "status": pick(["shipped", "pending", "cancelled", "returned"]),
            "note": None if R.random() < 0.3 else pick(["gift_wrap", "rush_delivery", "B2B", "fragile"]),
        }
    )
DS["orders"] = (
    rows,
    [
        ("id", "Int64"),
        ("customer_id", "Utf8"),
        ("order_date", "Date32"),
        ("amount", "Decimal128(10, 2)"),
        ("currency", "Utf8"),
        ("status", "Utf8"),
        ("note", "Utf8"),
    ],
)
add("orders", "What is customer_id of the row with id 117?", rows[116]["customer_id"])
add("orders", "How many rows have status 'returned'?", sum(r["status"] == "returned" for r in rows))
add(
    "orders",
    "How many rows have currency 'EUR' AND status 'shipped'?",
    sum(r["currency"] == "EUR" and r["status"] == "shipped" for r in rows),
)
add("orders", "What is the id of the row with the highest amount?", max(rows, key=lambda r: r["amount"])["id"])
add("orders", "How many rows have a SQL NULL note?", sum(r["note"] is None for r in rows))
add("orders", "What is order_date of the row with id 58?", rows[57]["order_date"])
add(
    "orders",
    "What is note of the row with id 33? (NULL if SQL NULL)",
    rows[32]["note"] if rows[32]["note"] is not None else "NULL",
)
add("orders", "Which column has the type Date32? Give the column name.", "order_date")

# 2 events
rows = []
for i in range(1, 301):
    rows.append(
        {
            "seq": i,
            "event_id": uuid(),
            "ts": ts(i),
            "user_name": f"{pick(FIRST)}_{pick(LAST)}",
            "event_type": pick(["page_view", "click", "purchase", "error"]),
            "duration_ms": None if R.random() < 0.15 else R.randint(1, 5000),
            "success": R.random() < 0.8,
            "country_code": pick(["CH", "DE", "US", "JP", "BR"]),
        }
    )
DS["events"] = (
    rows,
    [
        ("seq", "Int64"),
        ("event_id", "Utf8"),
        ("ts", "Timestamp(µs)"),
        ("user_name", "Utf8"),
        ("event_type", "Utf8"),
        ("duration_ms", "Int32"),
        ("success", "Boolean"),
        ("country_code", "Utf8"),
    ],
)
add("events", "What is user_name where seq is 211?", rows[210]["user_name"])
add("events", "How many rows have event_type 'purchase'?", sum(r["event_type"] == "purchase" for r in rows))
add("events", "How many rows have success false?", sum(not r["success"] for r in rows))
add(
    "events",
    "What is seq of the row with the largest duration_ms?",
    max((r for r in rows if r["duration_ms"] is not None), key=lambda r: r["duration_ms"])["seq"],
)
add("events", "How many rows have a SQL NULL duration_ms?", sum(r["duration_ms"] is None for r in rows))
add("events", "What is ts where seq is 75, exactly as it appears in the result?", rows[74]["ts"])
add(
    "events",
    "How many rows have event_type 'error' AND country_code 'CH'?",
    sum(r["event_type"] == "error" and r["country_code"] == "CH" for r in rows),
)
add("events", "What is event_id where seq is 140?", rows[139]["event_id"])

# 3 wide metrics
cols = (
    [("id", "Int64"), ("region", "Utf8")]
    + [(f"m{c:02d}", "Float64") for c in range(1, 21)]
    + [(f"c{c}", "Int64") for c in range(1, 4)]
)
rows = []
for i in range(1, 81):
    r = {"id": i, "region": pick(["north", "south", "east", "west"])}
    for c in range(1, 21):
        r[f"m{c:02d}"] = None if R.random() < 0.1 else round(R.uniform(0, 100), 2)
    for c in range(1, 4):
        r[f"c{c}"] = R.randint(0, 1000)
    rows.append(r)
DS["wide_metrics"] = (rows, cols)
add("wide_metrics", "What is m07 where id is 33?", rows[32]["m07"] if rows[32]["m07"] is not None else "NULL")
add("wide_metrics", "What is m19 where id is 60?", rows[59]["m19"] if rows[59]["m19"] is not None else "NULL")
add(
    "wide_metrics", "How many rows have m05 greater than 90?", sum(r["m05"] is not None and r["m05"] > 90 for r in rows)
)
add(
    "wide_metrics",
    "What is the id of the row with the largest m12?",
    max((r for r in rows if r["m12"] is not None), key=lambda r: r["m12"])["id"],
)
add("wide_metrics", "How many rows have a SQL NULL m03?", sum(r["m03"] is None for r in rows))
add("wide_metrics", "What is region where id is 45?", rows[44]["region"])
add("wide_metrics", "How many rows have region 'north'?", sum(r["region"] == "north" for r in rows))
add("wide_metrics", "What is c2 where id is 71?", rows[70]["c2"])

# 4 nested
rows = []
for i in range(1, 61):
    rows.append(
        {
            "id": i,
            "customer": {"name": f"{pick(FIRST).title()} {pick(LAST).title()}", "vip": R.random() < 0.25},
            "tags": R.sample(["a", "b", "c", "d", "e"], R.randint(0, 4)),
            "scores": [R.randint(1, 9) for _ in range(R.randint(1, 4))],
            "total": round(R.uniform(10, 900), 2),
        }
    )
DS["nested"] = (
    rows,
    [
        ("id", "Int64"),
        ("customer", 'Struct("name": Utf8, "vip": Boolean)'),
        ("tags", "List(Utf8)"),
        ("scores", "List(Int32)"),
        ("total", "Decimal128(10, 2)"),
    ],
)
add("nested", "What is customer.name where id is 13?", rows[12]["customer"]["name"])
add("nested", "How many rows have customer.vip true?", sum(r["customer"]["vip"] for r in rows))
add("nested", "How many rows have an empty tags list?", sum(len(r["tags"]) == 0 for r in rows))
add("nested", "What is the first element of scores where id is 7?", rows[6]["scores"][0])
add("nested", "How many rows have the tag 'c' in tags?", sum("c" in r["tags"] for r in rows))
add("nested", "How many elements does tags have where id is 31?", len(rows[30]["tags"]))
add("nested", "What is total where id is 40?", rows[39]["total"])
add("nested", "What is the sum of the scores list where id is 22?", sum(rows[21]["scores"]))

# 5 text_messy
tricky = {
    3: "tab\there",
    5: "line1\nline2\nline3",
    7: "pipe|inside|cell",
    9: "back\\slash and \\n literal",
    11: "\\N",
    13: "NULL",
    15: "",
    17: "Zürich — 東京 🚀 café",
    19: "  padded both sides  ",
    21: "quote \"x\" and 'y'",
    23: "| a | b |\n|---|---|",
    25: "crlf\r\nend",
    27: "a_b_c *star* [link](u) <b>x</b> &amp; `code` ~s~",
    29: "true",
    31: "x" * 600,
}
rows = []
for i in range(1, 41):
    t = tricky.get(i, f"plain text {i}")
    rows.append({"id": i, "text": t, "a|b": i * 3, "{x}": None if i % 4 == 0 else f"v{i}"})
DS["text_messy"] = (rows, [("id", "Int64"), ("text", "Utf8"), ("a|b", "Int32"), ("{x}", "Utf8")])
add("text_messy", "Give the exact text where id is 3. Write a real tab as \\t.", esc(tricky[3]))
add("text_messy", "Give the exact text where id is 5. Write each real newline as \\n.", esc(tricky[5]))
add("text_messy", "Give the exact text where id is 9. Write a real backslash as \\\\ (two characters).", esc(tricky[9]))
add(
    "text_messy",
    "Is text where id is 11 the two-character string backslash-N, or SQL NULL? Answer 'string' or 'null'.",
    "string",
)
add(
    "text_messy",
    "Is text where id is 13 the four-letter string NULL, or SQL NULL? Answer 'string' or 'null'.",
    "string",
)
add(
    "text_messy",
    "Is text where id is 15 an empty string, SQL NULL, or something else? Answer 'empty', 'null' or 'other'.",
    "empty",
)
add("text_messy", "Give the exact text where id is 23. Write each real newline as \\n.", esc(tricky[23]))
add("text_messy", "Give the exact text where id is 27 (the original characters, not any escaped form).", tricky[27])
add(
    "text_messy",
    "Give the exact text where id is 19, wrapped in single quotes so spaces are visible.",
    "'" + tricky[19] + "'",
)
add(
    "text_messy",
    "Does the result show the full text where id is 31 (600 characters), or is it clipped? Answer 'full' or 'clipped'.",
    "clipped",
)
add("text_messy", "What is the value of the column named 'a|b' where id is 5?", 15)
add("text_messy", "What is {x} where id is 8? (NULL if SQL NULL)", "NULL")

# 6 paths_urls
rows = []
for i in range(1, 101):
    rows.append(
        {
            "id": i,
            "url": f"https://api.example.com/v{R.randint(1, 3)}/items_{R.randint(1, 99)}?a={R.randint(1, 9)}&b_c={R.randint(1, 9)}"
            + ("&q=<x>*" if R.random() < 0.2 else ""),
            "file_path": f"/data/raw_files/{pick(['sales', 'user_logs', 'events'])}_{R.randint(2020, 2026)}/part_{R.randint(0, 99):05d}."
            + pick(["parquet", "csv", "json"]),
            "owner_name": f"team_{pick(['alpha', 'beta', 'gamma'])}_{R.randint(1, 3)}",
            "size_bytes": R.randint(1, 10**9),
        }
    )
DS["paths_urls"] = (
    rows,
    [("id", "Int64"), ("url", "Utf8"), ("file_path", "Utf8"), ("owner_name", "Utf8"), ("size_bytes", "Int64")],
)
add("paths_urls", "What is url where id is 14?", rows[13]["url"])
add("paths_urls", "What is file_path where id is 77?", rows[76]["file_path"])
add("paths_urls", "What is owner_name where id is 5?", rows[4]["owner_name"])
add("paths_urls", "How many rows have owner_name 'team_beta_2'?", sum(r["owner_name"] == "team_beta_2" for r in rows))
add(
    "paths_urls",
    "What is the id of the row with the largest size_bytes?",
    max(rows, key=lambda r: r["size_bytes"])["id"],
)
add("paths_urls", "How many rows have a url containing the text '<x>'?", sum("<x>" in r["url"] for r in rows))
add(
    "paths_urls",
    "How many rows have a file_path ending in '.parquet'?",
    sum(r["file_path"].endswith(".parquet") for r in rows),
)
add("paths_urls", "What is size_bytes where id is 60?", rows[59]["size_bytes"])

# 7 nulls_heavy
rows = []
for i in range(1, 101):
    n = lambda v: None if R.random() < 0.6 else v
    rows.append(
        {
            "id": i,
            "a": n(f"val_{i}"),
            "b": n(R.randint(1, 99)),
            "c": n(round(R.uniform(0, 1), 3)),
            "d": n(d(i)),
            "e": n(R.random() < 0.5),
            "f": pick([None, "", f"s{i}"]),
        }
    )
DS["nulls_heavy"] = (
    rows,
    [
        ("id", "Int64"),
        ("a", "Utf8"),
        ("b", "Int64"),
        ("c", "Float64"),
        ("d", "Date32"),
        ("e", "Boolean"),
        ("f", "Utf8"),
    ],
)
add("nulls_heavy", "How many rows have a SQL NULL a?", sum(r["a"] is None for r in rows))
add("nulls_heavy", "How many rows have f equal to the empty string (not NULL)?", sum(r["f"] == "" for r in rows))
add("nulls_heavy", "How many rows have both a and b SQL NULL?", sum(r["a"] is None and r["b"] is None for r in rows))
add(
    "nulls_heavy",
    "What is c where id is 20? (NULL if SQL NULL)",
    rows[19]["c"] if rows[19]["c"] is not None else "NULL",
)
add("nulls_heavy", "How many rows have e true?", sum(r["e"] is True for r in rows))
add(
    "nulls_heavy",
    "What is d where id is 64? (NULL if SQL NULL)",
    rows[63]["d"] if rows[63]["d"] is not None else "NULL",
)
fid = next(r["id"] for r in rows if r["f"] == "")
add(
    "nulls_heavy",
    f"For id {fid}, is f an empty string, SQL NULL, or a non-empty value? Answer 'empty', 'null' or 'value'.",
    "empty",
)
nid = next(r["id"] for r in rows if r["f"] is None)
add(
    "nulls_heavy",
    f"For id {nid}, is f an empty string, SQL NULL, or a non-empty value? Answer 'empty', 'null' or 'value'.",
    "null",
)

# 8 big
rows = []
for i in range(1, 1001):
    rows.append(
        {
            "seq": i,
            "sku": f"SKU-{R.randint(1000, 9999)}",
            "qty": R.randint(1, 50),
            "price": round(R.uniform(1, 999), 2),
            "city": pick(["Oslo", "Lima", "Bern", "Kyoto", "Accra", "Porto"]),
            "flag": R.random() < 0.5,
        }
    )
DS["big"] = (
    rows,
    [
        ("seq", "Int64"),
        ("sku", "Utf8"),
        ("qty", "Int32"),
        ("price", "Decimal128(10, 2)"),
        ("city", "Utf8"),
        ("flag", "Boolean"),
    ],
)
add("big", "What is sku where seq is 777?", rows[776]["sku"])
add("big", "What is price where seq is 500?", rows[499]["price"])
add("big", "What is sku where seq is 123?", rows[122]["sku"])
add("big", "What is qty where seq is 999?", rows[998]["qty"])
add("big", "What is city where seq is 250?", rows[249]["city"])
add("big", "What is the seq of the row with the largest price?", max(rows, key=lambda r: r["price"])["seq"])

# 9 numeric
rows = []
for i in range(1, 151):
    rows.append(
        {
            "id": i,
            "big": R.randint(-(10**12), 10**12),
            "ratio": R.choice([R.uniform(0, 1), R.uniform(0, 1) * 1e-7, R.uniform(1, 9) * 1e12]),
            "neg": round(R.uniform(-5000, 5000), 4),
            "pct": round(R.uniform(0, 100), 1),
        }
    )
rows[6]["big"] = 9223372036854775807
rows[89]["ratio"] = 1.5e12
DS["numeric"] = (
    rows,
    [("id", "Int64"), ("big", "Int64"), ("ratio", "Float64"), ("neg", "Decimal128(18, 4)"), ("pct", "Float32")],
)
add("numeric", "What is big where id is 7?", rows[6]["big"])
add("numeric", "What is neg where id is 33?", rows[32]["neg"])
add("numeric", "How many rows have big less than 0?", sum(r["big"] < 0 for r in rows))
add(
    "numeric",
    "What is the id of the row with the largest ratio? (if tied give the lowest id)",
    min((r for r in rows if r["ratio"] == max(x["ratio"] for x in rows)), key=lambda r: r["id"])["id"],
)
add("numeric", "What is pct where id is 12?", rows[11]["pct"])
add("numeric", "Which column has type Float32? Give the column name.", "pct")
add("numeric", "How many rows have ratio less than 0.000001?", sum(r["ratio"] < 1e-6 for r in rows))
add("numeric", "What is the precision and scale of the neg column's decimal type, as 'p,s'?", "18,4")

# 10 logs
rows = []
for i in range(1, 121):
    msg = pick(
        [
            "login ok",
            "cache miss key=user_42",
            'payload {"user":"x","ok":true,"n":[1,2]}',
            "retry 3|5 failed",
            "timeout after 30s",
        ]
    )
    if i % 17 == 0:
        msg = "Unhandled exception\n\tat svc.Handler.run(Handler.java:42)\n\tat svc.Main.main(Main.java:7)"
    rows.append(
        {
            "seq": i,
            "ts": ts(i),
            "level": pick(["INFO", "WARN", "ERROR", "DEBUG"]),
            "service": pick(["auth_service", "billing_api", "search_worker"]),
            "message": msg,
        }
    )
DS["logs"] = (
    rows,
    [("seq", "Int64"), ("ts", "Timestamp(µs)"), ("level", "Utf8"), ("service", "Utf8"), ("message", "Utf8")],
)
add(
    "logs",
    "Give the exact message where seq is 10. Write real newlines as \\n and tabs as \\t.",
    esc(rows[9]["message"]),
)
add("logs", "How many rows have level 'ERROR'?", sum(r["level"] == "ERROR" for r in rows))
add("logs", "What is service where seq is 55?", rows[54]["service"])
add("logs", "How many rows have a message containing the pipe character '|'?", sum("|" in r["message"] for r in rows))
add(
    "logs",
    "Give the exact message where seq is 85. Write real newlines as \\n and tabs as \\t.",
    esc(rows[84]["message"]),
)
add("logs", "What is ts where seq is 33, exactly as it appears in the result?", rows[32]["ts"])
add("logs", "How many rows have service 'auth_service'?", sum(r["service"] == "auth_service" for r in rows))
add(
    "logs",
    "Give the exact message where seq is 99. Write real newlines as \\n and tabs as \\t.",
    esc(rows[98]["message"]),
)

for name, (rows, cols) in DS.items():
    with open(f"{OUT}/{name}.ndjson", "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    open(f"{OUT}/{name}.schema.tsv", "w", encoding="utf-8").write("\n".join(f"{n}\t{t}" for n, t in cols) + "\n")
json.dump(Q, open(KEY, "w"), ensure_ascii=False, indent=1)
json.dump(
    [{k: v for k, v in q.items() if k != "answer"} for q in Q],
    open("data/questions_blind.json", "w"),
    ensure_ascii=False,
    indent=1,
)
print(len(DS), "datasets", len(Q), "questions")
