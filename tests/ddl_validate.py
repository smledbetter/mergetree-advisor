"""ddl_validate.py — prove the Advisor's EMITTED recommendation DDL is valid, idiomatic
ClickHouse by EXECUTING it against fresh copies of the real eval tables.

For each (db, table): seed a small workload (so workload-gated checks fire), run all
checks, pull every recommendation that contains `ALTER TABLE`, extract the ordered ALTER
statements, copy the table to ddlval.*, retarget the statements, and execute them in
order with mutations_sync=1 (so MATERIALIZE/MODIFY mutations actually run). Report pass/fail.
"""
from __future__ import annotations
import re
import sys

import advisor_common as ac
import c1_detector, c3_detector
import checks_ext as ce

VAL_DB = "ddlval"
ALTER_RE = re.compile(r"ALTER TABLE .+?(?=(?:;\s)|(?:\.\s)|\Z)", re.IGNORECASE | re.DOTALL)

# (db, table, [seed queries to populate query_log so C4/C7/C7b have evidence])
TARGETS = [
    ("noaa", "w", [
        "SELECT count() FROM noaa.w WHERE element = 'PRCP'",
        "SELECT element, count() FROM noaa.w GROUP BY element",
        "SELECT count() FROM noaa.w WHERE station_id = 'USW00094728'",
    ]),
    ("nyc", "trips", [
        "SELECT count() FROM nyc.trips WHERE payment_type = 2",
        "SELECT passenger_count, avg(trip_distance) FROM nyc.trips GROUP BY passenger_count",
    ]),
    ("imdb", "movie_info", [
        "SELECT count() FROM imdb.movie_info WHERE info_type_id = 3",
        "SELECT info_type_id, count() FROM imdb.movie_info GROUP BY info_type_id",
    ]),
    ("ddlfix", "c4", []),   # synthetic: forces C4 (skip index) to fire
]


def seed(ch, queries):
    for q in queries:
        try:
            ch.query(q, settings={"log_queries": 1})
        except Exception as e:
            print(f"   (seed skip: {e})")
    ch.command("SYSTEM FLUSH LOGS")


def run_checks(ch, db, table):
    findings = []
    for fn, name in [(c1_detector.detect, "C1"), (c3_detector.detect, "C3")]:
        try:
            findings.append(fn(ch, db, table))
        except Exception as e:
            print(f"   ({name} error: {e})")
    for chk in ce.ALL_CHECKS:
        try:
            findings.append(chk(ch, db, table))
        except Exception as e:
            print(f"   ({chk.__name__} error: {e})")
    return findings


def setup_fixtures(ch):
    """A synthetic table that FORCES C4 to fire (region correlated with sort key id,
    so a skip index prunes) — exercises the ADD INDEX + MATERIALIZE INDEX construct."""
    ch.command("CREATE DATABASE IF NOT EXISTS ddlfix")
    ch.command("DROP TABLE IF EXISTS ddlfix.c4 SYNC")
    ch.command(
        "CREATE TABLE ddlfix.c4 (id UInt32, region UInt16, payload UInt32) "
        "ENGINE = MergeTree ORDER BY id")
    ch.command(
        "INSERT INTO ddlfix.c4 SELECT number AS id, toUInt16(intDiv(number, 6000)) AS region, "
        "rand() AS payload FROM numbers(300000)", settings={"log_queries": 0})
    for q in ["SELECT count() FROM ddlfix.c4 WHERE region = 7",
              "SELECT sum(payload) FROM ddlfix.c4 WHERE region = 12"]:
        ch.query(q, settings={"log_queries": 1})
    ch.command("SYSTEM FLUSH LOGS")


def construct_smoke(ch):
    """Directly execute the emitted-DDL CONSTRUCTS for checks that did not fire on the
    real substrates (C10 String->native MODIFY; C7 normal projection ORDER BY variant),
    confirming ClickHouse accepts the exact syntax the Advisor would emit."""
    print("\n=== construct smoke (un-fired checks' emitted DDL) ===")
    results = []
    ch.command(f"CREATE DATABASE IF NOT EXISTS {VAL_DB}")
    # C10: String column of integer-strings -> Int64
    ch.command(f"DROP TABLE IF EXISTS {VAL_DB}.c10 SYNC")
    ch.command(f"CREATE TABLE {VAL_DB}.c10 (id UInt32, num String) ENGINE = MergeTree ORDER BY id")
    ch.command(f"INSERT INTO {VAL_DB}.c10 SELECT number, toString(number*7) FROM numbers(100000)",
               settings={"log_queries": 0})
    for s in [f"ALTER TABLE {VAL_DB}.c10 MODIFY COLUMN `num` Int64"]:
        results.append(_try(ch, "C10-native-types", s))
    # C7: normal projection, ORDER BY variant + MATERIALIZE
    ch.command(f"DROP TABLE IF EXISTS {VAL_DB}.c7 SYNC")
    ch.command(f"CREATE TABLE {VAL_DB}.c7 (id UInt32, k UInt32, v UInt32) ENGINE = MergeTree ORDER BY id")
    ch.command(f"INSERT INTO {VAL_DB}.c7 SELECT number, number%1000, rand() FROM numbers(100000)",
               settings={"log_queries": 0})
    for s in [f"ALTER TABLE {VAL_DB}.c7 ADD PROJECTION adv_proj_k (SELECT `id`, `k`, `v` ORDER BY `k`)",
              f"ALTER TABLE {VAL_DB}.c7 MATERIALIZE PROJECTION adv_proj_k"]:
        results.append(_try(ch, "C7-projection", s))
    ch.command(f"DROP TABLE IF EXISTS {VAL_DB}.c10 SYNC")
    ch.command(f"DROP TABLE IF EXISTS {VAL_DB}.c7 SYNC")
    return results


def _try(ch, label, s):
    try:
        ch.command(s, settings={"mutations_sync": 1, "log_queries": 0})
        print(f"   ✅ [{label}] {s[:100]}")
        return True
    except Exception as e:
        print(f"   ❌ [{label}] {s[:100]}\n      -> {str(e)[:160]}")
        return False


def main():
    ch = ac.client()
    ch.command(f"CREATE DATABASE IF NOT EXISTS {VAL_DB}")
    setup_fixtures(ch)
    total, ok = 0, 0
    for db, table, seeds in TARGETS:
        print(f"\n=== {db}.{table} ===")
        seed(ch, seeds)
        findings = run_checks(ch, db, table)
        for f in findings:
            if f.get("verdict") != "finding":
                continue
            rec = f.get("recommendation") or ""
            stmts = [s.strip() for s in ALTER_RE.findall(rec)]
            if not stmts:
                continue
            chk = f["check"]
            copy = f"{table}__{chk.split('-')[0].lower()}"
            ch.command(f"DROP TABLE IF EXISTS {VAL_DB}.{copy} SYNC")
            ch.command(f"CREATE TABLE {VAL_DB}.{copy} AS {db}.{table}")
            ch.command(f"INSERT INTO {VAL_DB}.{copy} SELECT * FROM {db}.{table} LIMIT 300000",
                       settings={"log_queries": 0})
            # retarget: `ALTER TABLE <table> ` -> `ALTER TABLE ddlval.<copy> `
            retgt = [re.sub(rf"^ALTER TABLE {re.escape(table)}\b",
                            f"ALTER TABLE {VAL_DB}.{copy}", s) for s in stmts]
            print(f"\n [{chk}] {len(retgt)} statement(s):")
            all_ok = True
            for s in retgt:
                total += 1
                try:
                    ch.command(s, settings={"mutations_sync": 1, "log_queries": 0})
                    ok += 1
                    print(f"   ✅ {s[:110]}")
                except Exception as e:
                    all_ok = False
                    print(f"   ❌ {s[:110]}\n      -> {str(e)[:160]}")
            # sanity: table still queryable after the changes
            if all_ok:
                cnt = ch.query(f"SELECT count() FROM {VAL_DB}.{copy}",
                               settings={"log_queries": 0}).result_rows[0][0]
                print(f"   (table healthy after changes: {cnt:,} rows)")
            ch.command(f"DROP TABLE IF EXISTS {VAL_DB}.{copy} SYNC")
    smoke = construct_smoke(ch)
    s_ok, s_tot = sum(1 for x in smoke if x), len(smoke)
    total += s_tot; ok += s_ok
    print(f"\n========================================\n"
          f"EMITTED-DDL VALIDATION: {ok}/{total} statements accepted & executed by ClickHouse "
          f"(incl. {s_ok}/{s_tot} construct-smoke for un-fired checks)")
    ch.command(f"DROP DATABASE IF EXISTS {VAL_DB} SYNC")
    ch.command("DROP DATABASE IF EXISTS ddlfix SYNC")
    sys.exit(0 if ok == total else 1)


if __name__ == "__main__":
    main()
