"""advisor_common.py — shared helpers for the ClickHouse MergeTree Advisor checks.

Engine-grounded, parse-light building blocks used by every Advisor detector
(c1_detector, c3_detector, ...) and by the benchmark harness. The design rule:
read facts from `system` tables, measure impact via real query execution, and
never rely on a full CH-dialect SQL parser (the ecosystem research showed sqlglot
/ sqlfluff break on CH syntax: sqlglot #2711/#4310, sqlfluff #7434).
"""
from __future__ import annotations
import logging
import re
import sys

import clickhouse_connect
from clickhouse_connect.driver.client import Client

CF_DB = "advisor_cf"  # shared counterfactual scratch database (tables dropped per run)

# operators that mark a column as being in FILTER position
FILTER_OPS = r"(?:=|!=|<>|<=|>=|<|>|\bBETWEEN\b|\bIN\b|\bLIKE\b|\bGLOBAL\s+IN\b)"

logger = logging.getLogger("ch_advisor")
if not logger.handlers:                      # configure once on import
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


def log(msg: str) -> None:
    logger.info(msg)


def client(host: str = "localhost") -> Client:
    """Official clickhouse-connect client (HTTP) — matches the ClickHouse MCP server
    and clickhouse-connect ecosystem convention."""
    return clickhouse_connect.get_client(host=host)


# ── Schema / metadata ───────────────────────────────────────────────────
def get_meta(ch: Client, db: str, table: str) -> dict:
    row = ch.query(
        "SELECT partition_key, sorting_key, engine FROM system.tables "
        "WHERE database = %(db)s AND name = %(t)s",
        parameters={"db": db, "t": table},
    ).result_rows
    if not row:
        sys.exit(f"[advisor] table {db}.{table} not found")
    partition_key, sorting_key, engine = row[0]
    cols = ch.query(
        "SELECT name, type FROM system.columns "
        "WHERE database = %(db)s AND table = %(t)s ORDER BY position",
        parameters={"db": db, "t": table},
    ).result_rows
    create = ch.query(f"SHOW CREATE TABLE {db}.{table}").result_rows[0][0]
    m = re.search(r"index_granularity\s*=\s*(\d+)", create)
    return {
        "partition_key": (partition_key or "").strip(),
        "sorting_key": (sorting_key or "").strip(),
        "engine": engine,
        "columns": [(c[0], c[1]) for c in cols],
        "column_names": [c[0] for c in cols],
        "granularity": int(m.group(1)) if m else 8192,
    }


def key_columns(key_expr: str, all_cols: list[str]) -> list[str]:
    """Columns referenced by an ORDER BY / PARTITION BY expression, in order."""
    if not key_expr or key_expr in ("tuple()", "()"):
        return []
    out = []
    for tok in re.split(r"\s*,\s*", key_expr):
        for c in all_cols:
            if re.search(rf"\b{re.escape(c)}\b", tok) and c not in out:
                out.append(c)
                break
    return out


# ── Workload (from system.query_log) ────────────────────────────────────
def _strip_format(query: str) -> str:
    """Drop a trailing FORMAT clause / semicolon. clickhouse-connect manages the
    wire format itself (and appends `FORMAT Native`), so a query that carries its
    own FORMAT — e.g. one logged by an HTTP client — must be cleaned before it can
    be re-executed for measurement."""
    return re.sub(r"\s+FORMAT\s+\w+\s*$", "",
                  query.strip().rstrip(";").strip(), flags=re.IGNORECASE)


def collect_workload(ch: Client, db: str, table: str, limit: int = 200) -> list[dict]:
    """Distinct recent successful SELECTs against the table (excludes introspection)."""
    # Aggregate by query SHAPE (normalizeQuery folds literals -> ?), so the same
    # query run with different constants collapses to one entry carrying its true
    # execution count + total bytes — the frequency/cost signal only a workload-
    # grounded tool has (the model and Agent Skills are frequency-blind).
    rows = ch.query(
        """
        SELECT any(query) AS q, count() AS executions,
               sum(read_rows) AS total_read_rows, sum(read_bytes) AS total_read_bytes
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind = 'Select'
          AND has(tables, %(qual)s) AND read_rows > 0
          AND query NOT ILIKE '%%system.%%'
        GROUP BY normalizeQuery(query)
        ORDER BY total_read_bytes DESC
        LIMIT %(lim)s
        """,
        parameters={"qual": f"{db}.{table}", "lim": limit},
    ).result_rows
    return [{"query": _strip_format(q), "executions": int(ex),
             "read_rows": int(trr), "read_bytes": int(trb)}
            for q, ex, trr, trb in rows]


def columns_present(workload: list[dict], cols: list[str]) -> set[str]:
    """Which of `cols` appear anywhere in the workload text (bare presence)."""
    blob = " ".join(re.sub(r"\s+", " ", w["query"]) for w in workload).lower()
    return {c for c in cols if re.search(rf"\b{re.escape(c.lower())}\b", blob)}


# RHS must look like a literal/list (not another identifier), so JOIN keys
# (a.x = b.y) are NOT counted as filters.
_LITERAL_RHS = r"(?:'|[-0-9]|\(|NULL\b|today\b|now\b|yesterday\b|to[A-Z])"


def filter_columns(workload: list[dict], cols: list[str]) -> list[str]:
    """Columns in FILTER position (`[alias.]col <op> <literal>`). Strips table
    aliases (mi.col -> col) and excludes JOIN keys (col = col) by requiring a
    literal/list RHS — so the regex survives real aliased/JOIN SQL."""
    blob = " ".join(re.sub(r"\s+", " ", w["query"]) for w in workload)
    out = []
    for c in cols:
        pat = rf"(?:\b\w+\.)?\b{re.escape(c)}\b\s*{FILTER_OPS}\s*{_LITERAL_RHS}"
        if re.search(pat, blob, re.IGNORECASE):
            out.append(c)
    return out


def cardinalities(ch: Client, db: str, table: str, cols: list[str]) -> dict:
    # log_queries=0: the Advisor must not pollute the very query_log it reads as
    # the workload (an unlogged uniqExact full-scan would otherwise reappear as
    # "workload" on the next run and wash out the measured delta).
    out = {}
    for c in cols:
        try:
            out[c] = ch.query(f"SELECT uniqExact(`{c}`) FROM {db}.{table}",
                              settings={"log_queries": 0}).result_rows[0][0]
        except Exception:
            out[c] = -1
    return out


# ── Counterfactual build + measured replay ──────────────────────────────
def build_counterfactual(ch: Client, db: str, table: str, meta: dict, *,
                         order_by: list[str] | None = None,
                         partition_by: str | None = None,
                         column_overrides: dict | None = None,
                         select_overrides: dict | None = None,
                         extra_ddl: list[str] | None = None,
                         cf_db: str = CF_DB,
                         cf_table: str | None = None,
                         force_wide: bool = False) -> str:
    """Materialize an identical table (same data) with optionally overridden
    ORDER BY / PARTITION BY / per-column type+codec / extra DDL (e.g. skip
    indexes). Unspecified keys are preserved. Returns the counterfactual name.

    column_overrides: {col: "TYPE [CODEC(...)]"} replaces that column's spec.
    extra_ddl: list of DDL fragments appended to the column list (e.g.
               "INDEX idx `col` TYPE minmax GRANULARITY 1")."""
    dst = cf_table or table
    ch.command(f"CREATE DATABASE IF NOT EXISTS {cf_db}")
    ch.command(f"DROP TABLE IF EXISTS {cf_db}.{dst}")
    ov = column_overrides or {}
    parts = [f"`{n}` {ov.get(n, t)}" for n, t in meta["columns"]]
    if extra_ddl:
        parts += list(extra_ddl)
    cols_ddl = ", ".join(parts)
    ob_cols = order_by if order_by is not None else key_columns(meta["sorting_key"], meta["column_names"])
    ob = ", ".join(f"`{c}`" for c in ob_cols) or "tuple()"
    part = partition_by if partition_by is not None else (meta["partition_key"] or "tuple()")
    # force_wide: per-column compressed bytes (system.parts_columns) are only
    # populated for WIDE parts — compact parts report 0. Storage checks force
    # wide so the per-column measurement is readable.
    wide = ", min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0" if force_wide else ""
    ch.command(
        f"CREATE TABLE {cf_db}.{dst} ({cols_ddl}) ENGINE = MergeTree "
        f"ORDER BY ({ob}) PARTITION BY {part} "
        f"SETTINGS index_granularity = {meta['granularity']}{wide}"
    )
    # select_overrides: {col: expr} applies a per-column SELECT expression (e.g. a
    # type cast toUInt32(`c`)) on insert; other columns pass through unchanged.
    if select_overrides:
        sel = ", ".join(select_overrides.get(n, f"`{n}`") for n, _ in meta["columns"])
    else:
        sel = "*"
    ch.command(
        f"INSERT INTO {cf_db}.{dst} SELECT {sel} FROM {db}.{table}",
        settings={"max_insert_threads": 4, "max_partitions_per_insert_block": 0, "log_queries": 0},
    )
    ch.command(f"OPTIMIZE TABLE {cf_db}.{dst} FINAL")
    return f"{cf_db}.{dst}"


def structural_safe(meta: dict) -> bool:
    """True iff structural changes (ORDER BY / PARTITION / projections) are SAFE.
    On engines where ORDER BY is the dedup/rollup key — ReplacingMergeTree,
    SummingMergeTree, AggregatingMergeTree, CollapsingMergeTree,
    VersionedCollapsingMergeTree — changing ORDER BY/partition alters which rows
    survive dedup/rollup → changes results. Only plain MergeTree is safe to restructure.
    (Storage checks C5/C6/C8/C9/C10 and additive skip indexes C4 stay safe everywhere.)"""
    return (meta.get("engine") or "").rstrip() == "MergeTree"


# ── DDL generators: ONE home for ClickHouse DDL conventions ──────────────
# Every emitted recommendation flows its DDL through these, so the engine rules
# live in a single place instead of being copy-pasted per check: identifiers are
# backticked; ADD INDEX / ADD PROJECTION are PAIRED with MATERIALIZE so the change
# covers EXISTING parts (not just new inserts/merges); a Nullable->bare conversion
# carries the DEFAULT ClickHouse requires; codecs put the encoding codec before the
# general-purpose one (e.g. CODEC(Delta, ZSTD(1))).
def qcols(cols: list[str]) -> str:
    """Backtick-quote a list of identifiers: ['a', 'b'] -> '`a`, `b`'."""
    return ", ".join(f"`{c}`" for c in cols)


def default_for(inner: str) -> str:
    """ClickHouse requires a DEFAULT when converting Nullable(T) -> T (even with
    zero NULLs). Type-appropriate literal; with no NULLs it never materializes."""
    return "''" if inner.split("(")[0] in ("String", "FixedString") else "0"


def skip_index_name(col: str) -> str:
    return f"adv_skip_{col}"


def projection_name(key: str) -> str:
    return f"adv_proj_{key}"


AGG_PROJECTION_NAME = "adv_agg"


def modify_column(table: str, col: str, type_spec: str, default: str | None = None) -> str:
    """ALTER TABLE t MODIFY COLUMN `c` <type [CODEC(...)]> [DEFAULT <d>]."""
    ddl = f"ALTER TABLE {table} MODIFY COLUMN `{col}` {type_spec}"
    return ddl + (f" DEFAULT {default}" if default is not None else "")


def add_skip_index(table: str, col: str, index_type: str, granularity: int = 1) -> tuple[str, str]:
    """(ADD INDEX, MATERIALIZE INDEX) — MATERIALIZE builds it over existing parts."""
    name = skip_index_name(col)
    return (f"ALTER TABLE {table} ADD INDEX {name} `{col}` TYPE {index_type} GRANULARITY {granularity}",
            f"ALTER TABLE {table} MATERIALIZE INDEX {name}")


def add_projection(table: str, select_cols: list[str], order_col: str) -> tuple[str, str]:
    """(ADD PROJECTION, MATERIALIZE PROJECTION) for a normal ordered projection."""
    name = projection_name(order_col)
    return (f"ALTER TABLE {table} ADD PROJECTION {name} (SELECT {qcols(select_cols)} ORDER BY `{order_col}`)",
            f"ALTER TABLE {table} MATERIALIZE PROJECTION {name}")


def add_agg_projection(table: str, group_cols: list[str], agg_exprs: list[str]) -> tuple[str, str]:
    """(ADD PROJECTION, MATERIALIZE PROJECTION) for a GROUP-BY aggregating projection."""
    sel = qcols(group_cols) + ((", " + ", ".join(agg_exprs)) if agg_exprs else "")
    return (f"ALTER TABLE {table} ADD PROJECTION {AGG_PROJECTION_NAME} "
            f"(SELECT {sel} GROUP BY {qcols(group_cols)})",
            f"ALTER TABLE {table} MATERIALIZE PROJECTION {AGG_PROJECTION_NAME}")


def ddl_join(*items) -> str:
    """Flatten ALTER statements and/or (add, materialize) pairs into one
    '; '-joined statement string ready to paste into clickhouse-client."""
    out = []
    for it in items:
        out.extend(it if isinstance(it, (tuple, list)) else [it])
    return "; ".join(out)


def numeric_minmax(ch: Client, db: str, table: str, col: str) -> tuple:
    r = ch.query(f"SELECT min(`{col}`), max(`{col}`) FROM {db}.{table}",
                 settings={"log_queries": 0}).result_rows[0]
    return r[0], r[1]


def partition_count(ch: Client, db: str, table: str) -> int:
    return ch.query(
        "SELECT uniqExact(partition) FROM system.parts "
        "WHERE database=%(d)s AND table=%(t)s AND active",
        parameters={"d": db, "t": table}).result_rows[0][0]


def column_compressed_bytes(ch: Client, db: str, table: str, col: str | None = None) -> int:
    """Compressed on-disk bytes for one column (or the whole table if col is None),
    from system.parts_columns (per-column; populated for WIDE parts only)."""
    q = ("SELECT sum(column_data_compressed_bytes) FROM system.parts_columns "
         "WHERE database=%(d)s AND table=%(t)s AND active")
    params = {"d": db, "t": table}
    if col is not None:
        q += " AND column=%(c)s"
        params["c"] = col
    return int(ch.query(q, parameters=params).result_rows[0][0] or 0)


def column_types(ch: Client, db: str, table: str) -> dict:
    return {n: t for n, t in ch.query(
        "SELECT name, type FROM system.columns WHERE database=%(d)s AND table=%(t)s",
        parameters={"d": db, "t": table}).result_rows}


def retarget(query: str, src_db: str, src_table: str, dst_qual: str) -> str:
    """Swap the analyzed table reference for the counterfactual one (qualified
    first, bare name as boundary-guarded fallback)."""
    q = re.sub(rf"\b{re.escape(src_db)}\.{re.escape(src_table)}\b", dst_qual, query)
    q = re.sub(rf"(?<![\w.]){re.escape(src_table)}\b", dst_qual, q)
    return q


def measure(ch: Client, query: str, *, log: bool = False) -> tuple[int, float]:
    """Run a query; return (rows_read, elapsed_seconds) from the query summary
    (clickhouse-connect's QueryResult.summary, populated from the
    X-ClickHouse-Summary header — read_rows + elapsed_ns; no query_log round-trip).
    log=False (default) keeps replayed measurements out of query_log so the
    Advisor never re-pollutes the workload signal it reads."""
    summary = ch.query(_strip_format(query),
                       settings={"log_queries": 1 if log else 0}).summary
    rows = int(summary.get("read_rows") or 0)
    elapsed = int(summary.get("elapsed_ns") or 0) / 1e9
    return rows, elapsed


def replay(ch: Client, workload: list[dict], src_db: str, src_table: str, dst_qual: str) -> dict:
    """Re-run each workload query against current + counterfactual; measured
    read_rows/latency from the query profile."""
    cur_rows = cf_rows = 0
    cur_s = cf_s = 0.0
    saved_weighted = 0          # Σ (rows saved per query) × executions
    cur_w = cf_w = 0            # frequency-weighted totals (the firing metric)
    for w in workload:
        ex = int(w.get("executions") or 1)
        r, s = measure(ch, w["query"]); cur_rows += r; cur_s += s
        r2, s2 = measure(ch, retarget(w["query"], src_db, src_table, dst_qual)); cf_rows += r2; cf_s += s2
        saved_weighted += max(0, r - r2) * ex
        cur_w += r * ex; cf_w += r2 * ex
    # cost-weighted aggregate reduction: nets help AND harm across queries, weighted
    # by frequency. Fixes dilution (a frequent helped query dominates a rare full scan)
    # WITHOUT over-firing (a change that hurts other frequent queries nets out).
    weighted_reduction = (cur_w / cf_w) if cf_w else float("inf")
    return {"read_rows_current": cur_rows, "read_rows_counterfactual": cf_rows,
            "estimated_rows_saved_weighted": saved_weighted,
            "weighted_reduction": round(weighted_reduction, 1),
            "latency_ms_current": round(cur_s * 1000), "latency_ms_counterfactual": round(cf_s * 1000)}


def parts_count(ch: Client, db: str, table: str) -> int:
    return ch.query(
        "SELECT count() FROM system.parts WHERE database=%(db)s AND table=%(t)s AND active",
        parameters={"db": db, "t": table},
    ).result_rows[0][0]
