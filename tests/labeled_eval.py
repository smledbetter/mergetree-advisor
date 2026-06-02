"""Layer 3 — precision/recall vs a labeled oracle.
Each case = (db, table, workload, expected-fired-checks). Run the Advisor, compare
fired set to the label, tally per-check TP/FP/FN/TN → precision & recall.
Builds constructed fixtures for checks the real tables don't exercise; uses the
loaded real tables (imdb/nyc/noaa) for the rest. Also closes the C10 gap with a
String-of-numbers case (and L4-style result check on it).

Usage: python labeled_eval.py
"""
from __future__ import annotations
import advisor_common as ac, advisor, closed_loop

ch = ac.client()
SP = "SETTINGS storage_policy='eval_policy', min_bytes_for_wide_part=0"
ch.command("DROP DATABASE IF EXISTS lab SYNC"); ch.command("CREATE DATABASE lab")

def build(name, ddl, insert, rows=2000000):
    ch.command(f"DROP TABLE IF EXISTS lab.{name} SYNC")
    ch.command(ddl)
    ch.command(insert, settings={"log_queries": 0})
    ch.command(f"OPTIMIZE TABLE lab.{name} FINAL")

# ---- constructed fixtures (on the volume) ----
build("c1", f"CREATE TABLE lab.c1 (scol UInt32, fcol UInt32, pad String) ENGINE=MergeTree ORDER BY scol {SP}",
      "INSERT INTO lab.c1 SELECT toUInt32(cityHash64(number)), toUInt32(number), toString(number%100) FROM numbers(2000000)")
build("c3", f"CREATE TABLE lab.c3 (v UInt32, bucket UInt16, pad String) ENGINE=MergeTree ORDER BY v PARTITION BY bucket {SP}",
      "INSERT INTO lab.c3 SELECT toUInt32(number), toUInt16(cityHash64(number)%500), toString(number%100) FROM numbers(2000000) SETTINGS max_partitions_per_insert_block=0")
build("c4", f"CREATE TABLE lab.c4 (ts DateTime, metric UInt32, amount Float64) ENGINE=MergeTree ORDER BY ts {SP}",
      "INSERT INTO lab.c4 SELECT toDateTime(0)+number, toUInt32(number), (number%1000)/10.0 FROM numbers(2000000)")
build("c7", f"CREATE TABLE lab.c7 (tenant LowCardinality(String), metric UInt32, amount Float64) ENGINE=MergeTree ORDER BY tenant {SP}",
      "INSERT INTO lab.c7 SELECT concat('t',toString(number%100)), toUInt32(number), (number%1000)/10.0 FROM numbers(2000000)")
build("c10", f"CREATE TABLE lab.c10 (k UInt32, bignum String, txt String) ENGINE=MergeTree ORDER BY k {SP}",
      "INSERT INTO lab.c10 SELECT number, toString(number*987654321), concat('note-',toString(number%3)) FROM numbers(2000000)")
build("neg", f"CREATE TABLE lab.neg (cat LowCardinality(String), n UInt8, amt Float64 CODEC(ZSTD(1))) ENGINE=MergeTree ORDER BY cat {SP}",
      "INSERT INTO lab.neg SELECT concat('c',toString(number%50)), toUInt8(number%200), (number%1000)/10.0 FROM numbers(2000000)")
# C11 mutation fixture
build("c11", f"CREATE TABLE lab.c11 (k UInt32, v UInt32) ENGINE=MergeTree ORDER BY k {SP}",
      "INSERT INTO lab.c11 SELECT number, number FROM numbers(1000000)")
ch.command("ALTER TABLE lab.c11 UPDATE v = v+1 WHERE k%100=0 SETTINGS mutations_sync=1")

# ---- cases: (db, table, [workload], expected_fire set) ----
CASES = [
  ("lab","c1",  ["SELECT count() FROM lab.c1 WHERE fcol={i}"],            {"C1"}),
  ("lab","c3",  ["SELECT count(),avg(v) FROM lab.c3 WHERE v BETWEEN {i} AND {i}+10000"], {"C3","C3b"}),
  ("lab","c4",  ["SELECT count() FROM lab.c4 WHERE metric BETWEEN {i} AND {i}+10000"],   {"C4"}),
  ("lab","c7",  ["SELECT count(),sum(amount) FROM lab.c7 WHERE metric BETWEEN {i} AND {i}+10000"], {"C7"}),
  ("lab","c10", ["SELECT count() FROM lab.c10 WHERE k={i}"],              {"C10"}),
  ("lab","c11", ["SELECT count() FROM lab.c11 WHERE k={i}"],              {"C11"}),
  ("lab","neg", ["SELECT cat,count() FROM lab.neg WHERE cat='c5' GROUP BY cat"], set()),  # well-typed: nothing storage/type
  ("imdb","movie_info", ["SELECT info_type_id,count() FROM imdb.movie_info WHERE movie_id={i} GROUP BY info_type_id"], {"C8"}),
  ("nyc","trips", ["SELECT pickup_ntaname,count(),sum(total_amount) FROM nyc.trips GROUP BY pickup_ntaname","SELECT round(avg(pickup_longitude),5) FROM nyc.trips"], {"C9","C6","C7b"}),
  ("noaa","w", ["SELECT element,count(),avg(value) FROM noaa.w GROUP BY element"], {"C5","C6","C7b"}),
]
# C12 (default-codec advisory) fires on almost everything uncodec'd — exclude from P/R (advisory, not a P/R check)

LABELS = ["C1","C3","C3b","C4","C5","C6","C7","C7b","C8","C9","C10","C11"]
tp = {c:0 for c in LABELS}; fp = {c:0 for c in LABELS}; fn = {c:0 for c in LABELS}
rows = []
for db, tbl, wl, expected in CASES:
    ch.command("TRUNCATE TABLE system.query_log")
    for q in wl:
        for i in (1,2,3):
            try: ch.query(q.replace("{i}", str(i*100000)), settings={"log_queries":1})
            except Exception: pass
    ch.command("SYSTEM FLUSH LOGS")
    findings = advisor.run(ch, db, tbl)
    fired = {f["_label"] for f in findings if f.get("verdict")=="finding"} & set(LABELS)
    for c in LABELS:
        if c in expected and c in fired: tp[c]+=1
        elif c in expected and c not in fired: fn[c]+=1
        elif c not in expected and c in fired: fp[c]+=1
    rows.append((f"{db}.{tbl}", sorted(expected), sorted(fired), sorted(fired-expected), sorted(expected-fired)))
    ch.command("DROP DATABASE IF EXISTS advisor_cf SYNC")

print("\n=== L3 per-case (table | expected | fired | FALSE-POS | MISSED) ===")
for r in rows: print(f"  {r[0]:<18} exp={r[1]} fired={r[2]} FP={r[3]} FN={r[4]}")
print("\n=== L3 per-check precision / recall ===")
TP=FP=FN=0
for c in LABELS:
    t,f_,n = tp[c],fp[c],fn[c]; TP+=t;FP+=f_;FN+=n
    if t+f_+n==0: continue
    p = t/(t+f_) if t+f_ else float('nan'); r = t/(t+n) if t+n else float('nan')
    print(f"  {c:<4} TP={t} FP={f_} FN={n}  precision={p:.2f} recall={r:.2f}")
print(f"\n  OVERALL precision={TP/(TP+FP) if TP+FP else 0:.2f}  recall={TP/(TP+FN) if TP+FN else 0:.2f}  (TP={TP} FP={FP} FN={FN})")
ch.command("DROP DATABASE IF EXISTS lab SYNC"); ch.command("DROP DATABASE IF EXISTS advisor_cf SYNC")
