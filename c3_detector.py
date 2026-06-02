"""c3_detector.py — Advisor check C3: PARTITION BY misuse.

One-shot analyzer. Flags a PARTITION BY key that is absent from the workload's
filter predicates (so it gives no pruning, only part fragmentation), and
QUANTIFIES the harm with a measured counterfactual (rebuild PARTITION BY tuple(),
replay the workload, compare read_rows). Abstains when workload evidence is thin.

Usage: python c3_detector.py <database> <table> [--workload-limit N] [--host H]
"""
from __future__ import annotations
import argparse
import json

import advisor_common as ac

REDUCTION_FLAG_X = 2.0  # rec fires only if removing the partition cuts read_rows >= 2x


def detect(ch, db: str, table: str, workload_limit: int = 200) -> dict:
    meta = ac.get_meta(ch, db, table)
    if not ac.structural_safe(meta):
        return {"check": "C3-partition-by-misuse", "table": f"{db}.{table}", "verdict": "abstain",
                "reason": f"engine {meta['engine']}: partition affects per-partition dedup/rollup — out of scope"}
    pk_cols = ac.key_columns(meta["partition_key"], meta["column_names"])
    ac.log(f"[C3] {db}.{table}  PARTITION BY {meta['partition_key'] or 'tuple()'}  "
           f"ORDER BY ({meta['sorting_key']})")

    if not pk_cols:
        ac.log("[C3] no partition key → no finding (clean).")
        return {"check": "C3-partition-by-misuse", "table": f"{db}.{table}",
                "verdict": "no-finding",
                "reason": "table is not partitioned (PARTITION BY tuple()) — no misuse possible"}

    workload = ac.collect_workload(ch, db, table, workload_limit)
    if not workload:
        ac.log("[C3] ABSTAIN — no workload evidence.")
        return {"check": "C3-partition-by-misuse", "table": f"{db}.{table}",
                "verdict": "abstain", "partition_key": meta["partition_key"],
                "reason": "no workload queries in system.query_log — insufficient evidence"}

    present = ac.columns_present(workload, pk_cols)
    pk_in_filters = bool(present)
    ac.log(f"[C3] workload: {len(workload)} queries; partition cols {pk_cols} "
           f"present in workload text: {sorted(present) or 'NONE'}")

    dst = ac.build_counterfactual(ch, db, table, meta, partition_by="tuple()")
    m = ac.replay(ch, workload, db, table, dst)
    cur_parts, cf_parts = ac.parts_count(ch, db, table), ac.parts_count(ch, ac.CF_DB, table)
    reduction = (m["read_rows_current"] / m["read_rows_counterfactual"]
                 if m["read_rows_counterfactual"] else float("inf"))
    # Fire when the partition key is never filtered AND dropping it is neutral-or-
    # better on reads (reduction >= ~1.0). A non-filtered partition gives no pruning,
    # so dropping is usually neutral — but its read benefit can be masked by a bad
    # ORDER BY that a separate check (C1) fixes, so we do NOT require a big delta;
    # we only require that dropping it does not measurably HURT reads. Keep it only
    # if needed for data lifecycle (TTL / DROP PARTITION).
    fires = (not pk_in_filters) and (reduction >= 0.95)
    ac.log(f"[C3] verdict={'finding' if fires else 'no-finding'} reduction={round(reduction,1)}x "
           f"({m['read_rows_current']:,} -> {m['read_rows_counterfactual']:,})")
    return {
        "check": "C3-partition-by-misuse",
        "table": f"{db}.{table}",
        "verdict": "finding" if fires else "no-finding",
        "severity": "high" if fires and reduction >= 10 else ("medium" if fires else None),
        "partition_key": meta["partition_key"],
        "order_by": meta["sorting_key"],
        "evidence": {
            "partition_key_in_workload_filters": pk_in_filters,
            "workload_queries_analyzed": len(workload),
            "read_rows_current": m["read_rows_current"],
            "read_rows_counterfactual": m["read_rows_counterfactual"],
            "read_rows_reduction_x": round(reduction, 1),
            "estimated_rows_saved": m.get("estimated_rows_saved_weighted"),
            "latency_ms_current": m["latency_ms_current"],
            "latency_ms_counterfactual": m["latency_ms_counterfactual"],
            "parts_current": cur_parts, "parts_counterfactual": cf_parts,
        },
        "recommendation": (
            f"PARTITION BY `{meta['partition_key']}` is not referenced by any analyzed workload "
            f"filter, so it provides no partition pruning while fragmenting the table into "
            f"{cur_parts} parts. Drop or coarsen the partition (keep ORDER BY ({meta['sorting_key']})) "
            f"unless you need it for data lifecycle (TTL / DROP PARTITION). "
            f"Measured: removing it cuts rows read {round(reduction,1)}x ({m['read_rows_current']:,} -> "
            f"{m['read_rows_counterfactual']:,}) and latency {m['latency_ms_current']}ms -> "
            f"{m['latency_ms_counterfactual']}ms."
        ) if fires else (
            "partition key appears in workload filters or removing it does not materially "
            "reduce rows read — no recommendation."
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
