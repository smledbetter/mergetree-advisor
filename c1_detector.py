"""c1_detector.py — Advisor check C1: ORDER BY prefix doesn't match the workload's filters.

One-shot analyzer. Flags when the leading ORDER BY column is not among the
workload's filter columns (the sparse primary index then can't prune granules),
and QUANTIFIES the harm with a measured counterfactual: rebuild with ORDER BY
(filter cols low->high cardinality, then the rest), preserving PARTITION BY,
replay the workload, compare read_rows. Abstains when evidence is thin.

Usage: python c1_detector.py <database> <table> [--workload-limit N] [--host H]
"""
from __future__ import annotations
import argparse
import json
import re

import advisor_common as ac

REDUCTION_FLAG_X = 2.0


def detect(ch, db: str, table: str, workload_limit: int = 200) -> dict:
    meta = ac.get_meta(ch, db, table)
    sk_cols = ac.key_columns(meta["sorting_key"], meta["column_names"])
    leading = sk_cols[0] if sk_cols else None
    ac.log(f"[C1] {db}.{table}  ORDER BY ({meta['sorting_key']})  leading={leading}")

    if not ac.structural_safe(meta):
        return {"check": "C1-order-by-suboptimal", "table": f"{db}.{table}", "verdict": "abstain",
                "reason": f"engine {meta['engine']}: ORDER BY is the dedup/rollup key — "
                          f"reordering changes results (out of scope; storage checks still apply)"}
    workload = ac.collect_workload(ch, db, table, workload_limit)
    if not workload:
        ac.log("[C1] ABSTAIN — no workload.")
        return {"check": "C1-order-by-suboptimal", "table": f"{db}.{table}",
                "verdict": "abstain", "reason": "no workload queries in system.query_log"}

    fcols = ac.filter_columns(workload, meta["column_names"])
    if not fcols:
        ac.log("[C1] ABSTAIN — no filter columns detected.")
        return {"check": "C1-order-by-suboptimal", "table": f"{db}.{table}",
                "verdict": "abstain", "reason": "no filter-position columns detected in workload"}

    cards = ac.cardinalities(ch, db, table, fcols)
    # WHOLE-ORDER-BY proposal: order the WORKLOAD FILTER columns low->high cardinality
    # and drop non-filter columns from the key (they add no pruning and bury filter
    # dimensions). Generalizes beyond "leading col unfiltered" to "the key doesn't
    # serve all filters" — e.g. a correlated range column buried behind incidental
    # categoricals the workload never filters.
    # COST-AWARE ordering: lead with the most-frequently-filtered column (Σ executions
    # of queries that filter it), tie-break low->high cardinality. A pure low->high-card
    # order can bury the workload's dominant filter behind a rarely-filtered low-card col.
    def _wt(c):
        return sum(int(w.get("executions") or 1) for w in workload
                   if re.search(rf"(?:\b\w+\.)?\b{re.escape(c)}\b\s*{ac.FILTER_OPS}\s*{ac._LITERAL_RHS}",
                                w["query"], re.IGNORECASE))
    proposed = sorted(fcols, key=lambda c: (-_wt(c), cards.get(c, 1 << 62)))
    # filtered columns NOT covered by the current key's leading prefix (informational)
    buried = [c for c in fcols if c not in sk_cols[:len(proposed)]]
    ac.log(f"[C1] workload: {len(workload)} queries; filter cols {fcols} (cards {cards}); "
           f"current ORDER BY {sk_cols}; proposed {proposed}; buried {buried}")

    if proposed == sk_cols:
        ac.log("[C1] no-finding — ORDER BY already matches workload filter columns.")
        return {"check": "C1-order-by-suboptimal", "table": f"{db}.{table}",
                "verdict": "no-finding",
                "reason": "ORDER BY already equals the workload filter columns (low->high card)"}

    dst = ac.build_counterfactual(ch, db, table, meta, order_by=proposed)
    m = ac.replay(ch, workload, db, table, dst)
    reduction = (m["read_rows_current"] / m["read_rows_counterfactual"]
                 if m["read_rows_counterfactual"] else float("inf"))
    fires = m.get("weighted_reduction", reduction) >= REDUCTION_FLAG_X  # per-query (anti-dilution)
    ac.log(f"[C1] verdict={'finding' if fires else 'no-finding'} reduction={round(reduction,1)}x "
           f"({m['read_rows_current']:,} -> {m['read_rows_counterfactual']:,})")
    return {
        "check": "C1-order-by-suboptimal",
        "table": f"{db}.{table}",
        "verdict": "finding" if fires else "no-finding",
        "severity": "high" if fires and reduction >= 10 else ("medium" if fires else None),
        "current_order_by": meta["sorting_key"],
        "proposed_order_by": ", ".join(proposed),
        "evidence": {
            "current_sort_cols": sk_cols,
            "workload_filter_cols": fcols,
            "buried_filter_cols": buried,     # filtered but not in the current leading prefix
            "workload_queries_analyzed": len(workload),
            "read_rows_current": m["read_rows_current"],
            "read_rows_counterfactual": m["read_rows_counterfactual"],
            "read_rows_reduction_x": round(reduction, 1),
            "estimated_rows_saved": m.get("estimated_rows_saved_weighted"),
            "latency_ms_current": m["latency_ms_current"],
            "latency_ms_counterfactual": m["latency_ms_counterfactual"],
        },
        "recommendation": (
            f"The workload filters {fcols}, but the current ORDER BY {sk_cols} does not serve "
            f"them all" + (f" (filter column(s) {buried} are buried behind non-filter columns, so "
            f"the sparse primary index can't prune them)" if buried else "") + f". Rebuild as "
            f"ORDER BY ({', '.join(proposed)}) — filter columns only, low->high cardinality. NOTE: "
            f"ORDER BY is immutable — requires a rebuild/backfill (also re-check column compression, "
            f"which depends on sort order). Measured: cuts rows read {round(reduction,1)}x "
            f"({m['read_rows_current']:,} -> {m['read_rows_counterfactual']:,}), latency "
            f"{m['latency_ms_current']}ms -> {m['latency_ms_counterfactual']}ms."
        ) if fires else (
            "reordering to the workload filter columns does not materially reduce rows read "
            "(no single ORDER BY serves the filters better — consider a projection) — no recommendation."
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("database")
    ap.add_argument("table")
    ap.add_argument("--workload-limit", type=int, default=200)
    ap.add_argument("--host", default="localhost")
    args = ap.parse_args()
    ch = ac.client(args.host)
    print(json.dumps(detect(ch, args.database, args.table, args.workload_limit), indent=2))


if __name__ == "__main__":
    main()
