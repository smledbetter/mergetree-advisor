"""Layer 1 — Advisor robustness battery. Asserts NO CRASH on diverse column types,
adversarial SQL, and edge inputs. Run: python test_robustness.py"""
import advisor_common as ac, advisor

ch = ac.client()
PASS = FAIL = 0
fails = []
def ok(name, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1
    else: FAIL += 1; fails.append(f"FAIL {name}: {detail}")

ch.command("DROP DATABASE IF EXISTS rob SYNC"); ch.command("CREATE DATABASE rob")

# ---- 1. TYPE MATRIX: every common column type → run all 13 checks, assert no crash ----
ch.command("""CREATE TABLE rob.types (
  i8 Int8, i32 Int32, i64 Int64, u8 UInt8, u64 UInt64, f64 Float64, dec Decimal(18,4),
  s String, fs FixedString(8), d Date, dt DateTime, dt64 DateTime64(3),
  e8 Enum8('a'=1,'b'=2), lc LowCardinality(String), nu Nullable(Int32),
  arr Array(UInt32), mp Map(String,UInt32), tup Tuple(UInt8,String),
  uid UUID, ip4 IPv4, ip6 IPv6, bl Bool
) ENGINE=MergeTree ORDER BY i64 SETTINGS storage_policy='eval_policy', min_bytes_for_wide_part=0""")
ch.command("""INSERT INTO rob.types SELECT
  number%127, number, number, number%256, number, number/3, toDecimal64(number/11,4),
  toString(number), leftPad(toString(number%99999999),8,'0'), toDate('2026-01-01')+(number%100),
  toDateTime('2026-01-01 00:00:00')+number, toDateTime64('2026-01-01 00:00:00',3)+number,
  (number%2)+1, toString(number%40), if(number%5=0,NULL,toInt32(number)),
  [toUInt32(number%10)], map('k',toUInt32(number%10)), (toUInt8(number%256),toString(number)),
  generateUUIDv4(), toIPv4('1.2.3.4'), toIPv6('::1'), toBool(number%2)
  FROM numbers(200000)""", settings={"log_queries":0})
ch.command("OPTIMIZE TABLE rob.types FINAL")
# seed a workload touching several types so checks engage
ch.command("SELECT e8, count() FROM rob.types WHERE lc='5' GROUP BY e8", settings={"log_queries":1})
ch.command("SELECT count() FROM rob.types WHERE i32 BETWEEN 100 AND 200", settings={"log_queries":1})
ch.command("SYSTEM FLUSH LOGS")
findings = advisor.run(ch, "rob", "types")
errs = [(f["_label"], f.get("reason","")) for f in findings if f.get("verdict") == "error"]
ok("type-matrix: no check crashed", not errs, f"errors={errs}")
print(f"  type-matrix findings: {[(f['_label'],f['verdict']) for f in findings]}")

# ---- 2. ADVERSARIAL SQL battery → filter_columns must not crash; attribution correct ----
cols = ["region", "metric", "ts", "user_id"]
W = lambda q: [{"query": q, "executions": 1}]
cases = [
  ("simple-eq",        "SELECT * FROM t WHERE region = 'x'",                              {"region"}),
  ("aliased",          "SELECT * FROM t AS a WHERE a.region = 'x'",                       {"region"}),
  ("join-key-excluded","SELECT * FROM a JOIN b ON a.user_id = b.user_id",                 set()),
  ("join+real-filter", "SELECT * FROM a JOIN b ON a.user_id=b.user_id WHERE a.region='y'",{"region"}),
  ("IN-list",          "SELECT * FROM t WHERE metric IN (1,2,3)",                         {"metric"}),
  ("BETWEEN",          "SELECT * FROM t WHERE metric BETWEEN 1 AND 9",                    {"metric"}),
  ("CTE",              "WITH x AS (SELECT 1) SELECT * FROM t WHERE region = 'a'",         {"region"}),
  ("comment+multiline","SELECT *\n-- c\nFROM t\nWHERE region = 'a' /* z */",             {"region"}),
  ("OR",               "SELECT * FROM t WHERE region='a' OR metric=5",                    {"region","metric"}),
  ("ge-le",            "SELECT * FROM t WHERE metric >= 5 AND metric <= 9",              {"metric"}),
  # known-hard (assert NO CRASH; attribution is best-effort/limitation):
  ("func-predicate",   "SELECT * FROM t WHERE toYear(ts) = 2023",                         None),
  ("subquery",         "SELECT * FROM t WHERE region IN (SELECT region FROM s)",          None),
  ("window-fn",        "SELECT region, row_number() OVER (ORDER BY metric) FROM t",       None),
]
for name, q, expected in cases:
    try:
        got = set(ac.filter_columns(W(q), cols))
        if expected is None:
            ok(f"sql:{name}(no-crash)", True)          # limitation cases: only assert no crash
        else:
            ok(f"sql:{name}", got == expected, f"got {got} want {expected}")
    except Exception as e:
        ok(f"sql:{name}", False, f"CRASH {type(e).__name__}: {e}")

# ---- 3. EDGE inputs → no crash ----
ch.command("CREATE TABLE rob.empty (a UInt32, b String) ENGINE=MergeTree ORDER BY a SETTINGS storage_policy='eval_policy'")
try:
    f = advisor.run(ch, "rob", "empty")
    ok("edge:empty-table", all(x.get("verdict") != "error" for x in f),
       f"{[(x['_label'],x.get('reason')) for x in f if x.get('verdict')=='error']}")
except Exception as e:
    ok("edge:empty-table", False, f"CRASH {e}")

ch.command("CREATE TABLE rob.allnull (a UInt32, n Nullable(String)) ENGINE=MergeTree ORDER BY a SETTINGS storage_policy='eval_policy', min_bytes_for_wide_part=0")
ch.command("INSERT INTO rob.allnull SELECT number, NULL FROM numbers(50000)", settings={"log_queries":0})
try:
    f = advisor.run(ch, "rob", "allnull")
    ok("edge:all-null-column", all(x.get("verdict") != "error" for x in f),
       f"{[(x['_label'],x.get('reason')) for x in f if x.get('verdict')=='error']}")
except Exception as e:
    ok("edge:all-null-column", False, f"CRASH {e}")

ch.command("DROP DATABASE IF EXISTS rob SYNC")
print(f"\n=== Layer 1 robustness: PASS={PASS} FAIL={FAIL} ===")
for r in fails: print(r)
