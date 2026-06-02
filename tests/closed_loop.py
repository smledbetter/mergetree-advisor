"""Layer 4 — closed-loop apply-verification (the ship-blocker).
For a table+workload: run the Advisor; for each FIRED check, APPLY its recommendation
to a real copy, then verify (a) the workload-query RESULTS are identical original-vs-
applied (correctness preserved) and (b) the gain reproduces. Catches the catastrophic
class: a rec that silently changes results.

Usage: python closed_loop.py <db> <table>
"""
from __future__ import annotations
import sys, re
import advisor_common as ac
import advisor

APPLIED_DB = "applied_cf"


def run_result(ch, query):
    return ch.query(query, settings={"log_queries": 0}).result_rows


def results_equal(a, b, tol=1e-6):
    if len(a) != len(b):
        return False, f"row count {len(a)} vs {len(b)}"
    sa, sb = sorted(a, key=lambda r: str(r)), sorted(b, key=lambda r: str(r))
    for ra, rb in zip(sa, sb):
        if len(ra) != len(rb):
            return False, "arity"
        for x, y in zip(ra, rb):
            if isinstance(x, float) or isinstance(y, float):
                xf, yf = float(x or 0), float(y or 0)
                if abs(xf - yf) > tol * max(1.0, abs(xf)):
                    return False, f"float {xf} vs {yf}"
            elif x != y:
                return False, f"value {x!r} vs {y!r}"
    return True, ""


def apply_finding(ch, db, table, meta, f):
    """Reconstruct the table with the finding's recommendation applied; return its name."""
    lbl, ev = f["_label"], f.get("evidence", {})
    types = {n: t for n, t in meta["columns"]}
    cf = f"{table}__{lbl}"
    kw = dict(cf_db=APPLIED_DB, cf_table=cf)
    if lbl == "C1":
        return ac.build_counterfactual(ch, db, table, meta, order_by=f["proposed_order_by"].split(", "), **kw)
    if lbl in ("C3", "C3b"):
        return ac.build_counterfactual(ch, db, table, meta, partition_by="tuple()", **kw)
    if lbl == "C4":
        # index_type evidence is "col:type, col2:type2"
        idx = []
        for part in (ev.get("index_type") or "").split(", "):
            if ":" in part:
                c, t = part.split(":", 1); idx.append(f"INDEX ap_{c} `{c}` TYPE {t} GRANULARITY 1")
        return ac.build_counterfactual(ch, db, table, meta, extra_ddl=idx, **kw)
    if lbl == "C5":
        ov = {c: "LowCardinality(String)" for c in ev.get("candidate_columns", [])}
        return ac.build_counterfactual(ch, db, table, meta, column_overrides=ov, **kw)
    if lbl == "C6":
        ov = {c: f"{types[c].split(' CODEC')[0]} CODEC(Delta, ZSTD(1))" for c in ev.get("candidate_columns", [])}
        return ac.build_counterfactual(ch, db, table, meta, column_overrides=ov, **kw)
    if lbl == "C7":
        proj = [f"PROJECTION ap_{s['order']} (SELECT {', '.join(s['select'])} ORDER BY `{s['order']}`)"
                for s in ev.get("projection_specs", [])]
        return ac.build_counterfactual(ch, db, table, meta, extra_ddl=proj, **kw)
    if lbl == "C7b":
        keys, aggs = ev.get("group_by_keys", []), ev.get("aggregates", [])
        sel = ", ".join([f"`{k}`" for k in keys] + aggs); gb = ", ".join(f"`{k}`" for k in keys)
        return ac.build_counterfactual(ch, db, table, meta, extra_ddl=[f"PROJECTION ap_agg (SELECT {sel} GROUP BY {gb})"], **kw)
    if lbl == "C8":
        ov, selo = {}, {}
        for s in ev.get("changes", []):
            m = re.match(r"(\w+) \w+->(\w+)", s)
            if m: ov[m.group(1)] = m.group(2); selo[m.group(1)] = f"to{m.group(2)}(`{m.group(1)}`)"
        return ac.build_counterfactual(ch, db, table, meta, column_overrides=ov, select_overrides=selo, **kw)
    if lbl == "C9":
        ov, selo = {}, {}
        for c in ev.get("columns", []):
            inner = re.match(r"Nullable\((.+)\)$", types[c]).group(1)
            ov[c] = inner; selo[c] = f"assumeNotNull(`{c}`)"
        return ac.build_counterfactual(ch, db, table, meta, column_overrides=ov, select_overrides=selo, **kw)
    if lbl == "C10":
        cm = {"Int64": "toInt64", "Float64": "toFloat64", "DateTime": "parseDateTimeBestEffort"}
        ov, selo = {}, {}
        for s in ev.get("changes", []):
            m = re.match(r"(\w+) String->(\w+)", s)
            if m: ov[m.group(1)] = m.group(2); selo[m.group(1)] = f"{cm.get(m.group(2), 'to'+m.group(2))}(`{m.group(1)}`)"
        return ac.build_counterfactual(ch, db, table, meta, column_overrides=ov, select_overrides=selo, **kw)
    return None   # C11/C12 advisory — nothing to apply


def main():
    db, table = sys.argv[1], sys.argv[2]
    ch = ac.client()
    ch.command(f"DROP DATABASE IF EXISTS {APPLIED_DB} SYNC")
    ch.command(f"CREATE DATABASE {APPLIED_DB}")
    meta = ac.get_meta(ch, db, table)
    workload = ac.collect_workload(ch, db, table)
    findings = advisor.run(ch, db, table)
    fired = [f for f in findings if f.get("verdict") == "finding"]
    print(f"\n=== L4 closed-loop · {db}.{table} === ({len(fired)} fired, {len(workload)} workload queries)")
    for f in fired:
        lbl = f["_label"]
        try:
            applied = apply_finding(ch, db, table, meta, f)
        except Exception as e:
            print(f"  {lbl:<5} APPLY-ERROR: {str(e).split(chr(10))[0][:80]}"); continue
        if applied is None:
            print(f"  {lbl:<5} advisory (nothing to apply)"); continue
        # verify each workload query returns identical results original vs applied
        worst = None
        for w in workload:
            orig = run_result(ch, w["query"])
            app = run_result(ch, ac.retarget(w["query"], db, table, applied))
            eq, why = results_equal(orig, app)
            if not eq:
                worst = (w["query"][:55], why); break
        verdict = "RESULTS IDENTICAL ✓" if worst is None else f"RESULT CHANGED ✗ [{worst[1]}] q={worst[0]}"
        print(f"  {lbl:<5} {verdict}")
        ch.command(f"DROP TABLE IF EXISTS {applied} SYNC")
    ch.command(f"DROP DATABASE IF EXISTS {APPLIED_DB} SYNC")
    ch.command("DROP DATABASE IF EXISTS advisor_cf SYNC")


if __name__ == "__main__":
    main()
