"""advisor.py — the MergeTree Advisor: run all checks on a table, emit a report.

Runs the full check suite (C1, C3 from the original detectors; C3b, C4, C5, C6
from checks_ext) against a real ClickHouse table, reading its workload from
system.query_log and measuring each recommendation's impact via a counterfactual.

Usage: python advisor.py <database> <table> [--host H] [--json]
"""
from __future__ import annotations
import argparse
import json
import sys

import advisor_common as ac
import c1_detector
import c3_detector
import checks_ext

# (label, callable) in display order
CHECKS = [
    ("C1", lambda ch, db, t: c1_detector.detect(ch, db, t)),
    ("C3", lambda ch, db, t: c3_detector.detect(ch, db, t)),
    ("C3b", lambda ch, db, t: checks_ext.check_c3b(ch, db, t)),
    ("C4", lambda ch, db, t: checks_ext.check_c4(ch, db, t)),
    ("C5", lambda ch, db, t: checks_ext.check_c5(ch, db, t)),
    ("C6", lambda ch, db, t: checks_ext.check_c6(ch, db, t)),
    ("C7", lambda ch, db, t: checks_ext.check_c7(ch, db, t)),
    ("C7b", lambda ch, db, t: checks_ext.check_c7b(ch, db, t)),
    ("C8", lambda ch, db, t: checks_ext.check_c8(ch, db, t)),
    ("C9", lambda ch, db, t: checks_ext.check_c9(ch, db, t)),
    ("C10", lambda ch, db, t: checks_ext.check_c10(ch, db, t)),
    ("C11", lambda ch, db, t: checks_ext.check_c11(ch, db, t)),
    ("C12", lambda ch, db, t: checks_ext.check_c12(ch, db, t)),
]


def run(ch, db, table) -> list[dict]:
    findings = []
    for label, fn in CHECKS:
        try:
            f = fn(ch, db, table)
        except Exception as e:
            msg = str(e)
            if "code: 241" in msg or "MEMORY_LIMIT" in msg or "memory limit" in msg.lower():
                # the counterfactual rebuild exceeds this host's memory at full scale
                # (e.g. re-sorting a very wide table). Degrade gracefully rather than
                # hard-error: the measured-counterfactual method is sampling-validated.
                f = {"check": label, "verdict": "abstain",
                     "reason": "counterfactual build exceeds this host's memory at full "
                               "scale — re-run on a sample (the method is sampling-validated)"}
            else:
                f = {"check": label, "verdict": "error",
                     "reason": msg.split("Stack trace")[0][:140]}
        f["_label"] = label
        findings.append(f)
        try:                       # bound peak disk: drop each check's counterfactuals
            ch.command("DROP DATABASE IF EXISTS advisor_cf SYNC")
        except Exception:
            pass
    return findings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("database")
    ap.add_argument("table")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    ch = ac.client(args.host)
    findings = run(ch, args.database, args.table)

    if args.json:
        print(json.dumps(findings, indent=2)); return

    fired = [f for f in findings if f.get("verdict") == "finding"]
    # cost-weighted priority: rank fired read-path findings by estimated rows saved
    # (per-query saved × execution frequency from query_log)
    def _impact(f):
        return f.get("evidence", {}).get("estimated_rows_saved") or 0
    fired.sort(key=_impact, reverse=True)
    print(f"\n=== MergeTree Advisor · {args.database}.{args.table} ===")
    print(f"{len(fired)} finding(s) of {len(findings)} checks (ranked by est. rows saved/period)\n")
    for f in fired:
        ev = f.get("evidence", {})
        saved = ev.get("estimated_rows_saved")
        tag = f"  [~{saved:,} rows saved/period]" if saved else ""
        print(f"⚠ {f['_label']:<4}{f.get('severity') or '':<8}{f.get('check','')}{tag}")
        print(f"      → {f['recommendation']}")
    print("\n--- all checks ---")
    for f in findings:
        v = f.get("verdict")
        mark = {"finding": "⚠ ", "no-finding": "✓ ", "abstain": "· ", "error": "✗ "}.get(v, "  ")
        sev = f" [{f.get('severity')}]" if f.get("severity") else ""
        print(f"{mark}{f['_label']:<4}{v:<11}{sev:<10}{f.get('check','')}")
    print(json.dumps(findings, indent=2), file=sys.stderr)


if __name__ == "__main__":
    main()
