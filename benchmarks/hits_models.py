"""hits_models.py — the model-design half of the hits head-to-head, with the API key
read directly from the file (robust to env non-propagation). Reuses the existing
benchb_src source + bench_blind helpers. Blind designs only by default (fast); pass
--adv to also run the Advisor-correction passes."""
from __future__ import annotations
import json
import os
import sys

import advisor_common as ac
import bench_blind as bb

MODEL = "claude-opus-4-8"
SRC_TABLE = "default.hits_sample"
WITH_ADV = "--adv" in sys.argv
KEY = open(os.path.expanduser("~/.config/anthropic-api-key")).read().strip()

ch = ac.client()
COLS = [(n, t) for n, t in ch.query(
    "SELECT name, type FROM system.columns WHERE database='default' AND table='hits_sample' "
    "ORDER BY position").result_rows]
NROWS = ch.query(f"SELECT count() FROM {SRC_TABLE}").result_rows[0][0]

HELD_OUT = [
    "SELECT count() FROM {T} WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31'",
    "SELECT count() FROM {T} WHERE UserID = 435090932899640449",
    "SELECT URL, count() AS c FROM {T} WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY c DESC LIMIT 10",
]
TASK = {
    "domain": ("a web-analytics clickstream 'hits' table (page views / user events, one row per "
               "hit) for a general analytics warehouse — the Yandex.Metrica / ClickBench schema"),
    "columns": COLS, "held_out": HELD_OUT,
    "reference": {"columns": {}, "order_by": ["CounterID", "EventDate", "UserID", "EventTime", "WatchID"],
                  "partition_by": "tuple()", "skip_indexes": []},
    "naive": {"columns": {}, "order_by": ["EventTime"], "partition_by": "toYYYYMM(EventDate)",
              "skip_indexes": []},
}
bb.TASKS["hits"] = TASK

# source already built by the prior run; recreate only if missing
src = f"{bb.SRC_DB}.hits"
if not ch.query("SELECT count() FROM system.tables WHERE database=%(d)s AND name='hits'",
                parameters={"d": bb.SRC_DB}).result_rows[0][0]:
    ch.command(f"CREATE DATABASE IF NOT EXISTS {bb.SRC_DB}")
    ch.command(f"CREATE TABLE {src} AS {SRC_TABLE}")
    ch.command(f"INSERT INTO {src} SELECT * FROM {SRC_TABLE}",
               settings={"max_insert_threads": 4, "log_queries": 0})


def design(skills):
    import anthropic
    cols = "\n".join(f"  - {n} ({t})" for n, t in COLS)
    sb = (bb.SKILLS_RULES + "\n\n") if skills else ""
    sys_p = (
        "You are a ClickHouse physical-design expert designing a table at creation time. " + sb +
        "Given a domain and columns, design the best MergeTree schema for a general analytics "
        "workload. Choose per-column storage types, per-column codecs, ORDER BY, PARTITION BY, "
        "and any data-skipping indexes. In \"columns\", ONLY list columns whose type or codec "
        "you would CHANGE from the given type; omit unchanged columns. Reply with ONLY JSON: "
        "{\"columns\": {\"col\": \"TYPE [CODEC(...)]\"}, \"order_by\": [\"col\"], "
        "\"partition_by\": \"<expr or tuple()>\", \"skip_indexes\": [{\"column\": \"col\", "
        "\"type\": \"minmax\"}]}.")
    usr = f"Domain: {TASK['domain']}\n\nColumns:\n{cols}\n\nReturn JSON only."
    r = anthropic.Anthropic(api_key=KEY).messages.create(
        model=MODEL, max_tokens=6000, system=sys_p, messages=[{"role": "user", "content": usr}])
    text = "".join(b.text for b in r.content if getattr(b, "type", "") == "text")
    return bb.parse_schema(text, [n for n, _ in COLS]), text


rows = [("reference", 98305, "OB(CounterID,EventDate,UserID,EventTime,WatchID)"),
        ("naive", 614400, "OB(EventTime) PART(toYYYYMM(EventDate))"),
        ("naive+adv", 172032, "applied C5,C7")]
for skills in (False, True):
    tag = "opus+skills" if skills else "opus-blind"
    s, raw = design(skills)
    if not s:
        rows.append((tag, None, "no proposal; raw=" + raw[:120])); continue
    tbl = bb.materialize(ch, "hits", TASK, tag, s)
    rr, by = bb.measure(ch, TASK, tbl)
    rows.append((tag, rr, bb.design_brief(s)))
    print(f"[{tag}] designed OB={s.get('order_by')} parts={s.get('partition_by')} "
          f"lowcard/codec cols={len(s.get('columns') or {})} skipidx={len(s.get('skip_indexes') or [])}", flush=True)
    if WITH_ADV:
        corr, ap = bb.apply_advisor(ch, tbl, s)
        tbl2 = bb.materialize(ch, "hits", TASK, tag + "_adv", corr)
        rr2, by2 = bb.measure(ch, TASK, tbl2)
        rows.append((tag + "+adv", rr2, "applied " + ",".join(ap)))

rr0 = 98305
print(f"\n=== Agent-Skills head-to-head · ClickBench hits · {NROWS:,} rows · {MODEL} ===")
print(f"{'treatment':<16}{'read_rows':>12}{'vs_ref':>9}  design / applied")
for tag, rr, d in rows:
    if rr is None:
        print(f"{tag:<16}{'ERR':>12}{'':>9}  {d}"); continue
    print(f"{tag:<16}{rr:>12,}{rr/rr0:>8.1f}x  {d}")
print(json.dumps([(t, r, d) for t, r, d in rows], default=str), file=sys.stderr)
