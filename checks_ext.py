"""checks_ext.py — additional MergeTree Advisor checks (C3b, C4, C5, C6).

Each check(ch, db, table) -> finding dict with the same contract as c1/c3:
read facts from system tables, MEASURE a counterfactual, emit a grounded rec.
Grounded in ClickHouse's own agent-skills rules + core docs.

  C3b  too many / over-fragmented partitions   schema-partition-low-cardinality   (parts)
  C4   selective non-key filter wants skip idx  docs/optimize/skipping-indexes     (read_rows)
  C5   String <10K distinct -> LowCardinality   schema-types-lowcardinality        (compressed bytes)
  C6   timestamp col without Delta,ZSTD         data-compression docs              (compressed bytes)
"""
from __future__ import annotations
import re

import advisor_common as ac

PARTITION_HARD_MAX = 1000        # SKILL.md schema-partition-low-cardinality: keep 100-1000
READ_ROWS_FLAG_X = 2.0
BYTES_FLAG_X = 1.2               # >=20% smaller column on disk


# ── C3b: too many / over-fragmented partitions ──────────────────────────
def check_c3b(ch, db, table) -> dict:
    meta = ac.get_meta(ch, db, table)
    out = {"check": "C3b-too-many-partitions", "table": f"{db}.{table}", "metric": "parts"}
    if not ac.structural_safe(meta):
        return {**out, "verdict": "abstain", "reason": f"engine {meta['engine']}: partition affects dedup/rollup — out of scope"}
    if not ac.key_columns(meta["partition_key"], meta["column_names"]):
        return {**out, "verdict": "no-finding", "reason": "table is not partitioned"}
    pc = ac.partition_count(ch, db, table)
    total = ch.query(f"SELECT count() FROM {db}.{table}",
                     settings={"log_queries": 0}).result_rows[0][0]
    rows_per = total / pc if pc else total
    over_band = pc > PARTITION_HARD_MAX
    over_frag = pc > 100 and rows_per < 10 * meta["granularity"]   # < ~10 granules/partition
    if not (over_band or over_frag):
        return {**out, "verdict": "no-finding",
                "reason": f"{pc} partitions, {int(rows_per):,} rows each — within healthy range"}

    # measure: coarsen to tuple(); report parts + (if workload) the read_rows trade-off
    dst = ac.build_counterfactual(ch, db, table, meta, partition_by="tuple()")
    cf_parts = ac.partition_count(ch, ac.CF_DB, table)
    workload = ac.collect_workload(ch, db, table)
    rr = ac.replay(ch, workload, db, table, dst) if workload else None
    return {
        **out, "verdict": "finding",
        "severity": "high" if over_band else "medium",
        "partition_key": meta["partition_key"],
        "evidence": {
            "partitions_current": pc, "partitions_coarsened": cf_parts,
            "rows_per_partition": int(rows_per),
            "rule": "keep partition count 100-1000 (schema-partition-low-cardinality)",
            "read_rows_current": rr["read_rows_current"] if rr else None,
            "read_rows_coarsened": rr["read_rows_counterfactual"] if rr else None,
            "estimated_rows_saved": rr.get("estimated_rows_saved_weighted") if rr else None,
        },
        "recommendation": (
            f"{pc} partitions ({int(rows_per):,} rows each) over-fragments the table "
            f"(rule: keep 100-1000). Coarsen `PARTITION BY {meta['partition_key']}` toward "
            f"`tuple()` or a coarser bucket. "
            + (f"The read-pruning benefit is marginal: coarsening reads "
               f"{rr['read_rows_counterfactual']:,} vs {rr['read_rows_current']:,} rows but cuts "
               f"parts {pc} -> {cf_parts}." if rr else f"Reduces parts {pc} -> {cf_parts}.")
        ),
    }


# ── C4: skip-index opportunity for a selective non-key filter ────────────
def check_c4(ch, db, table) -> dict:
    meta = ac.get_meta(ch, db, table)
    out = {"check": "C4-skip-index-opportunity", "table": f"{db}.{table}", "metric": "read_rows"}
    sk = ac.key_columns(meta["sorting_key"], meta["column_names"])
    workload = ac.collect_workload(ch, db, table)
    if not workload:
        return {**out, "verdict": "abstain", "reason": "no workload in query_log"}
    fcols = ac.filter_columns(workload, meta["column_names"])
    candidates = [c for c in fcols if c not in sk]           # not prunable by the primary index
    if not candidates:
        return {**out, "verdict": "no-finding",
                "reason": "all workload filter columns are already in ORDER BY"}

    # smart index-type per candidate: equality+high-card -> bloom_filter, equality+
    # low-card -> set, range/correlated -> minmax (skipping-indexes docs). One build.
    blob = " ".join(re.sub(r"\s+", " ", w["query"]) for w in workload)
    eq = [c for c in candidates
          if re.search(rf"\b{re.escape(c)}\b\s*(?:=|!=|\bIN\b)", blob, re.IGNORECASE)]
    cards = ac.cardinalities(ch, db, table, eq) if eq else {}
    def _itype(c):
        if c in eq:
            return "bloom_filter(0.01)" if cards.get(c, 0) > 10000 else "set(1000)"
        return "minmax"
    types = {c: _itype(c) for c in candidates}
    idx = [f"INDEX {ac.skip_index_name(c)} `{c}` TYPE {types[c]} GRANULARITY 1" for c in candidates]
    dst = ac.build_counterfactual(ch, db, table, meta, extra_ddl=idx)
    m = ac.replay(ch, workload, db, table, dst)
    red = (m["read_rows_current"] / m["read_rows_counterfactual"]
           if m["read_rows_counterfactual"] else float("inf"))
    itype = ", ".join(f"{c}:{types[c]}" for c in candidates)
    fires = m.get("weighted_reduction", red) >= READ_ROWS_FLAG_X  # per-query (anti-dilution)
    return {
        **out, "verdict": "finding" if fires else "no-finding",
        "severity": "high" if fires and red >= 10 else ("medium" if fires else None),
        "evidence": {
            "candidate_columns": candidates, "index_type": itype if fires else None,
            "read_rows_current": m["read_rows_current"],
            "read_rows_with_index": m["read_rows_counterfactual"],
            "read_rows_reduction_x": round(red, 1),
            "estimated_rows_saved": m.get("estimated_rows_saved_weighted"),
        },
        "recommendation": (
            f"Workload filters {candidates}, which are not in ORDER BY and so cannot be pruned by "
            f"the primary index. A data-skipping index helps because the data is correlated with "
            f"the sort key. Add the index AND materialize it (ADD INDEX alone only covers new "
            f"inserts/merges — MATERIALIZE INDEX builds it over existing parts): "
            + ac.ddl_join(*[ac.add_skip_index(table, c, types[c]) for c in candidates])
            + f". Measured: cuts rows read {round(red,1)}x ({m['read_rows_current']:,} -> "
              f"{m['read_rows_counterfactual']:,})."
        ) if fires else (
            "no skip index materially reduced rows read (columns likely uncorrelated with the "
            "sort key — a skip index would not skip granules). No recommendation."
        ),
    }


# ── C5: LowCardinality for low-cardinality String columns ────────────────
def check_c5(ch, db, table) -> dict:
    meta = ac.get_meta(ch, db, table)
    out = {"check": "C5-lowcardinality", "table": f"{db}.{table}", "metric": "compressed_bytes"}
    string_cols = [n for n, t in meta["columns"] if t == "String"]
    if not string_cols:
        return {**out, "verdict": "no-finding", "reason": "no plain String columns"}
    cards = ac.cardinalities(ch, db, table, string_cols)
    # LowCardinality is effective up to ~100K distinct (docs); <10K is the safe zone,
    # 10K-100K still helps storage. Candidate up to 100K; the measured-bytes gate decides.
    cand = [c for c in string_cols if 0 <= cards[c] < 100000]
    if not cand:
        return {**out, "verdict": "no-finding",
                "reason": "no String column under the 10K-distinct LowCardinality threshold"}

    # measure on forced-wide rebuilds of baseline vs LowCardinality (same data,
    # same keys) so per-column bytes are readable and the change is isolated.
    base = ac.build_counterfactual(ch, db, table, meta, cf_table=f"{table}__c5base", force_wide=True)
    mod = ac.build_counterfactual(ch, db, table, meta, force_wide=True, cf_table=f"{table}__c5mod",
                                  column_overrides={c: "LowCardinality(String)" for c in cand})
    bdb, btbl = base.split("."); mdb, mtbl = mod.split(".")
    cur = sum(ac.column_compressed_bytes(ch, bdb, btbl, c) for c in cand)
    cf = sum(ac.column_compressed_bytes(ch, mdb, mtbl, c) for c in cand)
    if cur == 0 or cf == 0:
        return {**out, "verdict": "abstain",
                "reason": "could not read per-column compressed bytes", "candidates": cand}
    red = cur / cf
    fires = red >= BYTES_FLAG_X
    return {
        **out, "verdict": "finding" if fires else "no-finding",
        "severity": "medium" if fires else None,
        "evidence": {
            "candidate_columns": cand, "distinct_values": {c: cards[c] for c in cand},
            "compressed_bytes_current": cur, "compressed_bytes_lowcardinality": cf,
            "compressed_bytes_reduction_x": round(red, 2),
        },
        "recommendation": (
            f"String columns {cand} have <100K distinct values (LowCardinality is effective to "
            f"~100K); LowCardinality(String) dictionary-encodes them. Apply: "
            + "; ".join(ac.modify_column(table, c, "LowCardinality(String)") for c in cand)
            + f". Measured: shrinks these columns on disk {round(red,2)}x ({cur:,} -> {cf:,} bytes)."
        ) if fires else (
            f"LowCardinality did not materially shrink {cand} on disk — no recommendation."
        ),
    }


# ── C6: Delta,ZSTD codec for timestamp / date columns ────────────────────
def check_c6(ch, db, table) -> dict:
    meta = ac.get_meta(ch, db, table)
    out = {"check": "C6-codec-delta-zstd", "table": f"{db}.{table}", "metric": "compressed_bytes"}
    rows = ch.query(
        "SELECT name, type, compression_codec FROM system.columns "
        "WHERE database=%(d)s AND table=%(t)s", parameters={"d": db, "t": table}).result_rows
    typ = {n: t for n, t, _ in rows}
    cand = [n for n, t, codec in rows
            if re.match(r"(Date|DateTime)", t) and "Delta" not in (codec or "")]
    if not cand:
        return {**out, "verdict": "no-finding",
                "reason": "no Date/DateTime column lacking a Delta codec"}

    base = ac.build_counterfactual(ch, db, table, meta, cf_table=f"{table}__c6base", force_wide=True)
    mod = ac.build_counterfactual(ch, db, table, meta, force_wide=True, cf_table=f"{table}__c6mod",
                                  column_overrides={c: f"{typ[c]} CODEC(Delta, ZSTD(1))" for c in cand})
    bdb, btbl = base.split("."); mdb, mtbl = mod.split(".")
    cur = sum(ac.column_compressed_bytes(ch, bdb, btbl, c) for c in cand)
    cf = sum(ac.column_compressed_bytes(ch, mdb, mtbl, c) for c in cand)
    if cur == 0 or cf == 0:
        return {**out, "verdict": "abstain",
                "reason": "could not read per-column compressed bytes", "candidates": cand}
    red = cur / cf
    fires = red >= BYTES_FLAG_X
    return {
        **out, "verdict": "finding" if fires else "no-finding",
        "severity": "medium" if fires else None,
        "evidence": {
            "candidate_columns": cand,
            "compressed_bytes_current": cur, "compressed_bytes_delta_zstd": cf,
            "compressed_bytes_reduction_x": round(red, 2),
        },
        "recommendation": (
            f"Date/DateTime columns {cand} compress far better with delta encoding (monotonic "
            f"increments). Apply (restate the column type, then the codec — encoding codec Delta "
            f"first, general-purpose ZSTD last, per CH convention): "
            + "; ".join(ac.modify_column(table, c, f"{typ[c]} CODEC(Delta, ZSTD(1))") for c in cand)
            + f". Measured: shrinks these columns on disk {round(red,2)}x ({cur:,} -> {cf:,} bytes)."
        ) if fires else (
            f"Delta,ZSTD did not materially shrink {cand} — no recommendation."
        ),
    }


# ── C7: projection for a filter dimension no single ORDER BY can serve ───
def _projection_select_cols(workload, candidate, all_cols) -> list:
    """Columns a narrow projection on `candidate` must carry = every schema column
    referenced by the queries that FILTER `candidate` (a projection only serves a
    query if it contains all the query's columns). Returns stable schema order."""
    needed = {candidate}
    for w in workload:
        q = re.sub(r"\s+", " ", w["query"])
        if re.search(rf"\b{re.escape(candidate)}\b\s*{ac.FILTER_OPS}", q, re.IGNORECASE):
            for c in all_cols:
                if re.search(rf"\b{re.escape(c)}\b", q):
                    needed.add(c)
    return [c for c in all_cols if c in needed]


def check_c7(ch, db, table) -> dict:
    """When the workload filters multiple INDEPENDENT dimensions, one ORDER BY can
    only serve the leading one and a skip index can't help the rest (ranges overlap
    across the leading key). A `normal` projection = a second physical ordering by
    the unserved column, which ClickHouse auto-selects for queries filtering it.
    Measurement-gated; flags the storage cost (a SELECT * projection ~doubles the
    table's data)."""
    meta = ac.get_meta(ch, db, table)
    out = {"check": "C7-projection-opportunity", "table": f"{db}.{table}", "metric": "read_rows"}
    if not ac.structural_safe(meta):
        return {**out, "verdict": "abstain", "reason": f"engine {meta['engine']}: projection may not reflect dedup/rollup (FINAL) — out of scope"}
    sk = ac.key_columns(meta["sorting_key"], meta["column_names"])
    leading = sk[0] if sk else None
    workload = ac.collect_workload(ch, db, table)
    if not workload:
        return {**out, "verdict": "abstain", "reason": "no workload in query_log"}
    fcols = ac.filter_columns(workload, meta["column_names"])
    # dimensions the base key's leading column does not serve (can't prune via prefix)
    candidates = [c for c in fcols if c != leading]
    if not candidates:
        return {**out, "verdict": "no-finding",
                "reason": "base ORDER BY leading column serves the workload filters"}

    # NARROW projection: carry only the columns the queries that filter `c` actually
    # reference (filter + select/aggregate cols), not SELECT * — this drops the
    # scattered low-cardinality columns that blow up a full-copy projection's size.
    all_cols = meta["column_names"]
    specs, proj_ddl = [], []
    for c in candidates:
        sel = _projection_select_cols(workload, c, all_cols)
        specs.append({"order": c, "select": sel})
        proj_ddl.append(f"PROJECTION {ac.projection_name(c)} (SELECT {ac.qcols(sel)} ORDER BY `{c}`)")
    dst = ac.build_counterfactual(ch, db, table, meta, extra_ddl=proj_ddl)
    cf_db, cf_tbl = dst.split(".")
    m = ac.replay(ch, workload, db, table, dst)
    red = (m["read_rows_current"] / m["read_rows_counterfactual"]
           if m["read_rows_counterfactual"] else float("inf"))
    base_bytes = ac.column_compressed_bytes(ch, cf_db, cf_tbl)  # base columns only
    proj_bytes = int(ch.query(
        "SELECT sum(data_compressed_bytes) FROM system.projection_parts "
        "WHERE database=%(d)s AND table=%(t)s AND active",
        parameters={"d": cf_db, "t": cf_tbl}).result_rows[0][0] or 0)
    fires = m.get("weighted_reduction", red) >= READ_ROWS_FLAG_X  # per-query (anti-dilution)
    return {
        **out, "verdict": "finding" if fires else "no-finding",
        "severity": "high" if fires and red >= 10 else ("medium" if fires else None),
        "evidence": {
            "leading_sort_col": leading, "candidate_columns": candidates,
            "projection_specs": specs,
            "read_rows_current": m["read_rows_current"],
            "read_rows_with_projection": m["read_rows_counterfactual"],
            "read_rows_reduction_x": round(red, 1),
            "estimated_rows_saved": m.get("estimated_rows_saved_weighted"),
            "projection_bytes": proj_bytes, "base_bytes": base_bytes,
            "projection_storage_overhead_pct": round(100 * proj_bytes / base_bytes, 1) if base_bytes else None,
        },
        "recommendation": (
            f"The workload filters {candidates} independently of the leading sort column "
            f"`{leading}`, so neither a single ORDER BY nor a skip index can prune them. Add a "
            f"NARROW projection per unserved dimension (only the queried columns): "
            + ac.ddl_join(*[ac.add_projection(table, s['select'], s['order']) for s in specs])
            + f". Measured: cuts rows read {round(red,1)}x "
              f"({m['read_rows_current']:,} -> {m['read_rows_counterfactual']:,}); projection adds "
              f"{proj_bytes:,} bytes ({round(100*proj_bytes/base_bytes,1) if base_bytes else '?'}% of base). "
              f"NOTE: projection columns cannot be CODEC-compressed (CH <=26.5, open issue #74234) — "
              f"if this storage cost is too high, use a codec'd MV-to-table instead (a separate "
              f"MergeTree ORDER BY {candidates[0]} with per-column CODECs, fed by a materialized view): "
              f"same pruning at materially less storage (e.g. ~6x smaller when the carried column is a "
              f"compressible float), but queries must target that table explicitly (no auto-routing)."
        ) if fires else (
            "a projection on the unserved filter column(s) does not materially reduce rows read "
            "— no recommendation."
        ),
    }


# ── C8: minimize numeric bit-width ───────────────────────────────────────
_UINT = [("UInt8", 255), ("UInt16", 65535), ("UInt32", 4294967295)]
_INT = [("Int8", -128, 127), ("Int16", -32768, 32767), ("Int32", -2147483648, 2147483647)]


def _smallest_numeric(t, mn, mx):
    if t.startswith("UInt"):
        for name, hi in _UINT:
            if mx is not None and mx <= hi:
                return name
        return "UInt64"
    for name, lo, hi in _INT:
        if mn is not None and mx is not None and mn >= lo and mx <= hi:
            return name
    return "Int64"


def _num_width(t):
    m = re.search(r"(\d+)", t)
    return int(m.group(1)) if m else 64


def check_c8(ch, db, table) -> dict:
    meta = ac.get_meta(ch, db, table)
    out = {"check": "C8-numeric-bitwidth", "table": f"{db}.{table}", "metric": "compressed_bytes"}
    cands = []  # (col, cur_type, new_type)
    for n, t in meta["columns"]:
        if re.fullmatch(r"U?Int(8|16|32|64)", t):
            mn, mx = ac.numeric_minmax(ch, db, table, n)
            nt = _smallest_numeric(t, mn, mx)
            if _num_width(nt) < _num_width(t):
                cands.append((n, t, nt))
    if not cands:
        return {**out, "verdict": "no-finding", "reason": "no oversized integer columns"}
    overrides = {c: nt for c, _, nt in cands}
    selo = {c: f"to{nt}(`{c}`)" for c, _, nt in cands}
    base = ac.build_counterfactual(ch, db, table, meta, cf_table=f"{table}__c8base", force_wide=True)
    mod = ac.build_counterfactual(ch, db, table, meta, force_wide=True, cf_table=f"{table}__c8mod",
                                  column_overrides=overrides, select_overrides=selo)
    bdb, btbl = base.split("."); mdb, mtbl = mod.split(".")
    cur = sum(ac.column_compressed_bytes(ch, bdb, btbl, c) for c, _, _ in cands)
    cf = sum(ac.column_compressed_bytes(ch, mdb, mtbl, c) for c, _, _ in cands)
    if cur == 0 or cf == 0:
        return {**out, "verdict": "abstain", "reason": "could not read per-column bytes"}
    red = cur / cf
    fires = red >= BYTES_FLAG_X
    changes = [f"{c} {ct}->{nt}" for c, ct, nt in cands]
    return {
        **out, "verdict": "finding" if fires else "no-finding",
        "severity": "medium" if fires else None,
        "evidence": {"changes": changes, "compressed_bytes_current": cur,
                     "compressed_bytes_narrowed": cf, "compressed_bytes_reduction_x": round(red, 2)},
        "recommendation": (
            f"Oversized integer columns (actual value range fits a smaller type): "
            + "; ".join(f"`{c}` is {ct} but fits {nt}" for c, ct, nt in cands)
            + ". Narrow them: "
            + "; ".join(ac.modify_column(table, c, nt) for c, ct, nt in cands)
            + f". Measured: shrinks these columns on disk {round(red,2)}x "
              f"({cur:,} -> {cf:,} bytes)."
        ) if fires else (f"narrowing {changes} does not materially reduce compressed disk (unused "
                         f"high-order bytes compress away); the bit-width benefit is mainly query-time "
                         f"memory/CPU, which this disk gate does not measure — no recommendation."),
    }


# ── C9: avoid Nullable (declared Nullable but never NULL) ────────────────
def check_c9(ch, db, table) -> dict:
    meta = ac.get_meta(ch, db, table)
    out = {"check": "C9-avoid-nullable", "table": f"{db}.{table}", "metric": "nulls"}
    nullable = [(n, t) for n, t in meta["columns"] if t.startswith("Nullable(")]
    if not nullable:
        return {**out, "verdict": "no-finding", "reason": "no Nullable columns"}
    flagged = []
    for n, t in nullable:
        nulls = ch.query(f"SELECT countIf(`{n}` IS NULL) FROM {db}.{table}",
                         settings={"log_queries": 0}).result_rows[0][0]
        if nulls == 0:
            flagged.append((n, re.match(r"Nullable\((.+)\)$", t).group(1)))
    if not flagged:
        return {**out, "verdict": "no-finding", "reason": "Nullable columns do contain NULLs"}
    cur = sum(ac.column_compressed_bytes(ch, db, table, n) for n, _ in flagged)
    return {
        **out, "verdict": "finding", "severity": "low",
        "evidence": {"columns": [n for n, _ in flagged], "nulls": 0,
                     "current_compressed_bytes": cur},
        "recommendation": (
            f"Columns {[n for n,_ in flagged]} are Nullable but contain ZERO NULLs. Drop Nullable "
            f"(CH requires a DEFAULT for the Nullable->bare conversion; with no NULLs it never "
            f"materializes): "
            + "; ".join(ac.modify_column(table, n, inner, default=ac.default_for(inner))
                        for n, inner in flagged)
            + f". Removes the per-row null-map subcolumn and unblocks optimizations the Nullable "
              f"wrapper prevents."
        ),
    }


# ── C7b: aggregating projection for recurring GROUP-BY aggregate workloads ──
_AGG = (r"(?:count|sum|avg|min|max|any|anyLast|uniq|uniqExact|uniqCombined|argMax|argMin|"
        r"quantile\w*|median|stddev\w*|var\w*|groupArray|topK|sumIf|countIf|avgIf)")


def check_c7b(ch, db, table) -> dict:
    meta = ac.get_meta(ch, db, table)
    out = {"check": "C7b-aggregating-projection", "table": f"{db}.{table}", "metric": "read_rows"}
    if not ac.structural_safe(meta):
        return {**out, "verdict": "abstain", "reason": f"engine {meta['engine']}: aggregating projection may double-count vs {meta['engine']} rollup — out of scope"}
    all_cols = meta["column_names"]
    workload = ac.collect_workload(ch, db, table)
    gb = [w for w in workload if re.search(r"\bGROUP\s+BY\b", w["query"], re.IGNORECASE)]
    if not gb:
        return {**out, "verdict": "no-finding", "reason": "no GROUP BY queries in workload"}
    w = gb[0]   # highest-cost (workload sorted by total_read_bytes)
    q = re.sub(r"\s+", " ", w["query"])
    mk = re.search(r"GROUP\s+BY\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s+HAVING|\s+SETTINGS|$)", q, re.IGNORECASE)
    msel = re.search(r"SELECT\s+(.+?)\s+FROM\b", q, re.IGNORECASE)
    if not mk or not msel:
        return {**out, "verdict": "abstain", "reason": "could not parse GROUP BY / SELECT"}
    keys = [k.strip().strip("`").split(".")[-1] for k in mk.group(1).split(",")]  # strip alias mi.col->col
    keys = [k for k in keys if k in all_cols]            # only plain-column keys
    aggs = list(dict.fromkeys(re.findall(rf"{_AGG}\s*\([^()]*\)", msel.group(1), re.IGNORECASE)))
    aggs = [re.sub(r"\b\w+\.", "", a) for a in aggs]     # strip table aliases inside aggregates
    if not keys:
        return {**out, "verdict": "abstain", "reason": "GROUP BY keys are not plain columns"}
    if not aggs:
        return {**out, "verdict": "no-finding", "reason": "no aggregate functions found"}

    proj_sel = ac.qcols(keys) + ((", " + ", ".join(aggs)) if aggs else "")
    gb_keys = ac.qcols(keys)
    proj = [f"PROJECTION {ac.AGG_PROJECTION_NAME} (SELECT {proj_sel} GROUP BY {gb_keys})"]
    dst = ac.build_counterfactual(ch, db, table, meta, extra_ddl=proj)
    cf_db, cf_tbl = dst.split(".")
    cur, _ = ac.measure(ch, w["query"])
    cf, _ = ac.measure(ch, ac.retarget(w["query"], db, table, dst))
    red = cur / cf if cf else float("inf")
    proj_rows = int(ch.query(
        "SELECT sum(rows) FROM system.projection_parts WHERE database=%(d)s AND table=%(t)s AND active",
        parameters={"d": cf_db, "t": cf_tbl}).result_rows[0][0] or 0)
    fires = red >= READ_ROWS_FLAG_X
    ex = int(w.get("executions") or 1)
    return {
        **out, "verdict": "finding" if fires else "no-finding",
        "severity": "high" if fires and red >= 10 else ("medium" if fires else None),
        "evidence": {
            "group_by_keys": keys, "aggregates": aggs,
            "read_rows_current": cur, "read_rows_with_projection": cf,
            "read_rows_reduction_x": round(red, 1),
            "projection_stored_rows": proj_rows,
            "estimated_rows_saved": max(0, cur - cf) * ex,
        },
        "recommendation": (
            f"The workload runs recurring GROUP BY {keys} aggregations ({aggs}). Add an aggregating "
            f"projection — its hidden engine becomes AggregatingMergeTree storing one pre-aggregated "
            f"row per key-combination (~{proj_rows:,} rows vs {cur:,} scanned): "
            + ac.ddl_join(ac.add_agg_projection(table, keys, aggs))
            + f". Measured: cuts rows read {round(red,1)}x ({cur:,} -> {cf:,}); "
              f"aggregating projections are typically ~1-5% of source size (rows, not column codecs)."
        ) if fires else (
            "an aggregating projection did not materially reduce rows read for the GROUP BY query "
            "(it may not match the query's exact aggregates) — no recommendation."
        ),
    }


# ── C10: native types instead of String ─────────────────────────────────
def check_c10(ch, db, table) -> dict:
    meta = ac.get_meta(ch, db, table)
    out = {"check": "C10-native-types", "table": f"{db}.{table}", "metric": "compressed_bytes"}
    string_cols = [n for n, t in meta["columns"] if t == "String"]
    if not string_cols:
        return {**out, "verdict": "no-finding", "reason": "no plain String columns"}
    probes = [("Int64", "toInt64OrNull", "toInt64"),
              ("Float64", "toFloat64OrNull", "toFloat64"),
              ("DateTime", "parseDateTimeBestEffortOrNull", "parseDateTimeBestEffort")]
    cands = []  # (col, native_type, cast_fn)
    for n in string_cols:
        for nt, orfn, castfn in probes:
            # bad if any value fails to parse OR is empty — empties can't cast to a
            # non-Nullable native type (toInt64('') errors), so require all non-empty.
            bad = ch.query(
                f"SELECT countIf({orfn}(`{n}`) IS NULL OR `{n}` = '') FROM {db}.{table}",
                settings={"log_queries": 0}).result_rows[0][0]
            if bad == 0:                       # every value is non-empty AND parses as this native type
                cands.append((n, nt, castfn)); break
    if not cands:
        return {**out, "verdict": "no-finding", "reason": "no String column is fully native-parseable"}
    overrides = {c: nt for c, nt, _ in cands}
    selo = {c: f"{cf}(`{c}`)" for c, _, cf in cands}
    base = ac.build_counterfactual(ch, db, table, meta, cf_table=f"{table}__c10base", force_wide=True)
    mod = ac.build_counterfactual(ch, db, table, meta, force_wide=True, cf_table=f"{table}__c10mod",
                                  column_overrides=overrides, select_overrides=selo)
    bdb, btbl = base.split("."); mdb, mtbl = mod.split(".")
    cur = sum(ac.column_compressed_bytes(ch, bdb, btbl, c) for c, _, _ in cands)
    cf = sum(ac.column_compressed_bytes(ch, mdb, mtbl, c) for c, _, _ in cands)
    if cur == 0 or cf == 0:
        return {**out, "verdict": "abstain", "reason": "could not read per-column bytes"}
    red = cur / cf
    fires = red >= BYTES_FLAG_X
    changes = [f"{c} String->{nt}" for c, nt, _ in cands]
    return {
        **out, "verdict": "finding" if fires else "no-finding",
        "severity": "medium" if fires else None,
        "evidence": {"changes": changes, "compressed_bytes_current": cur,
                     "compressed_bytes_native": cf, "compressed_bytes_reduction_x": round(red, 2)},
        "recommendation": (
            f"String columns hold only native-typed values: " + "; ".join(changes)
            + ". Use native types (smaller, faster, enable codecs/range pruning): "
            + "; ".join(ac.modify_column(table, c, nt) for c, nt, _ in cands)
            + f". Measured: shrinks these columns on disk {round(red,2)}x ({cur:,} -> {cf:,} bytes)."
        ) if fires else (f"native typing {changes} does not materially reduce compressed disk "
                         f"(digit-strings compress well); the benefit is mainly query-time memory + "
                         f"enabling range pruning/codecs, not measured here — no recommendation."),
    }


# ── C11: mutation anti-pattern (ALTER UPDATE/DELETE rewrites whole parts) ──
def check_c11(ch, db, table) -> dict:
    out = {"check": "C11-mutation-antipattern", "table": f"{db}.{table}", "metric": "mutations"}
    total, pending = ch.query(
        "SELECT count(), countIf(NOT is_done) FROM system.mutations "
        "WHERE database=%(d)s AND table=%(t)s", parameters={"d": db, "t": table}).result_rows[0]
    if total == 0:
        return {**out, "verdict": "no-finding", "reason": "no mutations on this table"}
    cmds = [r[0] for r in ch.query(
        "SELECT command FROM system.mutations WHERE database=%(d)s AND table=%(t)s "
        "ORDER BY create_time DESC LIMIT 3", parameters={"d": db, "t": table}).result_rows]
    return {
        **out, "verdict": "finding",
        "severity": "high" if pending else "medium",
        "evidence": {"mutations_total": int(total), "mutations_pending": int(pending),
                     "recent_commands": cmds},
        "recommendation": (
            f"{total} mutation(s) ({pending} pending) on this table. ALTER UPDATE/DELETE mutations "
            f"rewrite entire data parts (column-oriented immutable storage) — expensive and async. "
            f"Recent: {cmds[:2]}. Prefer: ReplacingMergeTree (versioned upserts), lightweight DELETE, "
            f"or DROP PARTITION for bulk removal, instead of recurring mutations."
        ),
    }


# ── C12: default-compression advisory (reaches even projection columns) ───
def check_c12(ch, db, table) -> dict:
    out = {"check": "C12-default-compression", "table": f"{db}.{table}", "metric": "advisory"}
    rows = ch.query("SELECT name, compression_codec FROM system.columns "
                    "WHERE database=%(d)s AND table=%(t)s",
                    parameters={"d": db, "t": table}).result_rows
    uncodec = [n for n, c in rows if not c]
    if len(uncodec) < max(1, len(rows) // 2):
        return {**out, "verdict": "no-finding",
                "reason": f"{len(uncodec)}/{len(rows)} columns use default compression"}
    return {
        **out, "verdict": "finding", "severity": "low",
        "evidence": {"uncodec_columns": uncodec, "total_columns": len(rows)},
        "recommendation": (
            f"{len(uncodec)}/{len(rows)} columns have no explicit CODEC and fall back to the server "
            f"default compression (LZ4 on self-managed; ZSTD on ClickHouse Cloud). If self-managed, "
            f"set ZSTD as the cluster default in config.xml `<compression>` for a broad storage win — "
            f"this is also the ONLY lever that compresses projection columns, which cannot take "
            f"per-column CODECs (issue #74234). Or add explicit CODECs to the heavy columns."
        ),
    }


ALL_CHECKS = [check_c3b, check_c4, check_c5, check_c6, check_c7, check_c7b,
              check_c8, check_c9, check_c10, check_c11, check_c12]
