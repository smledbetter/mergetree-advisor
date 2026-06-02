"""bench_blind.py — CH-Agent-Bench, NO-WORKLOAD / held-out-workload variant.

The realistic regime: the model designs a schema BLIND — given the domain + columns
but NOT the queries (as an agent does at table-creation time) — then the schema is
scored against a HELD-OUT workload it never saw. The Advisor reads that held-out
workload from system.query_log (the information the model structurally lacked) and
applies its fixes. This tests the gap the research documented (AI-generated schemas
over-partition / pick generic ORDER BY) and where the Advisor's t0-vs-t1 workload
asymmetry is the whole point.

Usage: python bench_blind.py [--models m1,m2,..] [--rows N] [--host H]
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys

import advisor_common as ac
import advisor

SRC_DB = "benchb_src"
RUN_DB = "benchb"
DEFAULT_MODELS = "claude-haiku-4-5,claude-sonnet-4-6,claude-opus-4-8"

# ClickHouse's own "Agent Skills" (clickhouse-best-practices) schema rules, verbatim
# from ClickHouse/agent-skills SKILL.md. This is the passive context-injection package
# CH ships as its answer to "agents design bad schemas" — the +skills treatment.
SKILLS_RULES = """ClickHouse schema-design best practices (follow these):
ORDER BY / primary key:
- Plan ORDER BY before table creation (it is immutable).
- Order ORDER BY columns low-to-high cardinality.
- Include frequently filtered columns in ORDER BY.
- Query filters must use the ORDER BY prefix to prune.
Data types:
- Use native types, not String for everything.
- Use the smallest numeric type that fits.
- Use LowCardinality for string columns with <10K unique values.
- Use Enum for finite value sets.
- Avoid Nullable; use DEFAULT instead.
Partitioning:
- Keep partition count 100-1,000.
- Use partitioning for data lifecycle, not for query speed.
- Understand partition pruning trade-offs.
- Consider starting without partitioning."""

TASKS = {
    "ecom_blind": {
        "domain": ("a high-volume e-commerce user-events table (clickstream: product "
                   "views, cart adds, purchases) for a general analytics warehouse"),
        "columns": [("event_id", "UInt64"), ("user_id", "UInt64"), ("product_id", "UInt32"),
                    ("category", "String"), ("event_type", "String"), ("ts", "DateTime"),
                    ("price", "Float64"), ("quantity", "UInt16")],
        "data_select": (
            "SELECT number AS event_id, cityHash64(number) % 1000000 AS user_id, "
            "toUInt32(number % 50000) AS product_id, concat('cat', toString(number % 100)) AS category, "
            "['view','cart','purchase','wishlist','return','review','share','compare'][(number % 8) + 1] AS event_type, "
            "toDateTime('2026-01-01 00:00:00') + number AS ts, "
            "(number % 10000) / 100.0 AS price, toUInt16(number % 10 + 1) AS quantity "
            "FROM numbers({N})"
        ),
        # HELD-OUT: never shown to the model. Filters category (a dimension a blind
        # "events" design — ts/user-ordered, date-partitioned — won't prune).
        "held_out": [
            "SELECT count() AS c, sum(price) AS s FROM {T} WHERE category = 'cat42'",
            "SELECT count() AS c FROM {T} WHERE category = 'cat77'",
        ],
        # oracle for the held-out workload
        "reference": {
            "columns": {"category": "LowCardinality(String)", "event_type": "LowCardinality(String)",
                        "ts": "DateTime CODEC(Delta, ZSTD(1))"},
            "order_by": ["category"], "partition_by": "tuple()", "skip_indexes": [],
        },
        # deterministic AI-naive default: generic ORDER BY + reflexive date partition
        "naive": {
            "columns": {}, "order_by": ["event_id"], "partition_by": "toDate(ts)", "skip_indexes": [],
        },
    },
    # MULTI-FILTER: the held-out workload filters TWO independent dimensions —
    # tenant (equality, wants the ORDER BY prefix) AND metric (range, monotonic,
    # wants a skip index). ORDER BY can only lead with one, so the oracle needs
    # ORDER BY (tenant) + a minmax skip index on metric. Tests whether the Advisor
    # COMPOSES C1 (ordering) and C4 (skip index) correctly — the open question.
    "multi_blind": {
        "domain": ("a high-volume multi-tenant e-commerce events table for an "
                   "analytics platform serving many tenants"),
        "columns": [("event_id", "UInt64"), ("tenant", "String"), ("category", "String"),
                    ("event_type", "String"), ("ts", "DateTime"), ("amount", "Float64"),
                    ("metric", "UInt32")],
        "data_select": (
            "SELECT number AS event_id, concat('tenant', toString(number % 100)) AS tenant, "
            "concat('cat', toString(number % 50)) AS category, "
            "['view','cart','purchase','wishlist','return','review','share','compare'][(number % 8) + 1] AS event_type, "
            "toDateTime('2026-01-01 00:00:00') + number AS ts, "
            "(number % 10000) / 100.0 AS amount, toUInt32(number) AS metric "
            "FROM numbers({N})"
        ),
        # HELD-OUT: never shown. Q1 = tenant equality; Q2 = metric range.
        "held_out": [
            "SELECT count() AS c, sum(amount) AS s FROM {T} WHERE tenant = 'tenant42'",
            "SELECT count() AS c, sum(amount) AS s FROM {T} WHERE metric BETWEEN 1500000 AND 1510000",
        ],
        "reference": {
            "columns": {"tenant": "LowCardinality(String)", "category": "LowCardinality(String)",
                        "event_type": "LowCardinality(String)", "ts": "DateTime CODEC(Delta, ZSTD(1))"},
            "order_by": ["tenant"], "partition_by": "tuple()",
            "skip_indexes": [{"column": "metric", "type": "minmax"}],
        },
        "naive": {
            "columns": {}, "order_by": ["event_id"], "partition_by": "toDate(ts)", "skip_indexes": [],
        },
    },
}


def ensure_source(ch, name, task, rows):
    src = f"{SRC_DB}.{name}"
    ch.command(f"CREATE DATABASE IF NOT EXISTS {SRC_DB}")
    exists = ch.query("SELECT count() FROM system.tables WHERE database=%(d)s AND name=%(t)s",
                        parameters={"d": SRC_DB, "t": name}).result_rows[0][0]
    if not exists:
        cols = ", ".join(f"`{n}` {t}" for n, t in task["columns"])
        ch.command(f"CREATE TABLE {src} ({cols}) ENGINE = MergeTree ORDER BY tuple()")
    if ch.query(f"SELECT count() FROM {src}").result_rows[0][0] != rows:
        ch.command(f"TRUNCATE TABLE {src}")
        ch.command(f"INSERT INTO {src} {task['data_select'].format(N=rows)}",
                   settings={"max_insert_threads": 4, "log_queries": 0})
    return src


def materialize(ch, name, task, suffix, schema):
    tbl = f"{name}__{re.sub(r'[^0-9A-Za-z_]', '_', suffix)}"
    qual = f"{RUN_DB}.{tbl}"
    ch.command(f"CREATE DATABASE IF NOT EXISTS {RUN_DB}")
    ch.command(f"DROP TABLE IF EXISTS {qual}")
    ov = schema.get("columns", {})
    parts = [f"`{n}` {ov.get(n, t)}" for n, t in task["columns"]]
    for ix in schema.get("skip_indexes", []) or []:
        parts.append(f"INDEX bidx_{ix['column']} `{ix['column']}` TYPE {ix['type']} GRANULARITY 1")
    for pj in schema.get("projections", []) or []:
        cols = ", ".join(f"`{x}`" for x in pj["select"])
        parts.append(f"PROJECTION bproj_{pj['order']} (SELECT {cols} ORDER BY `{pj['order']}`)")
    ob = ", ".join(f"`{c}`" for c in (schema.get("order_by") or [])) or "tuple()"
    part = schema.get("partition_by") or "tuple()"
    ch.command(f"CREATE TABLE {qual} ({', '.join(parts)}) ENGINE = MergeTree "
               f"ORDER BY ({ob}) PARTITION BY {part} "
               f"SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0")
    ch.command(f"INSERT INTO {qual} SELECT * FROM {SRC_DB}.{name}",
               settings={"max_insert_threads": 4, "max_partitions_per_insert_block": 0, "log_queries": 0})
    ch.command(f"OPTIMIZE TABLE {qual} FINAL")
    return tbl


def measure(ch, task, tbl):
    rr = 0
    for q in task["held_out"]:
        query = q.format(T=f"{RUN_DB}.{tbl}")
        ac.measure(ch, query, log=True)        # seed held-out workload into query_log
        rows, _ = ac.measure(ch, query)
        rr += rows
    ch.command("SYSTEM FLUSH LOGS")
    by = ch.query("SELECT sum(data_compressed_bytes) FROM system.parts "
                    "WHERE database=%(d)s AND table=%(t)s AND active", parameters={"d": RUN_DB, "t": tbl}).result_rows[0][0] or 0
    # include projection storage (a SELECT * projection ~doubles the data on disk)
    pby = ch.query("SELECT sum(data_compressed_bytes) FROM system.projection_parts "
                     "WHERE database=%(d)s AND table=%(t)s AND active", parameters={"d": RUN_DB, "t": tbl}).result_rows[0][0] or 0
    return rr, int(by) + int(pby)


def apply_advisor(ch, tbl, base_schema):
    findings = advisor.run(ch, RUN_DB, tbl)
    schema = {"columns": dict(base_schema.get("columns", {})),
              "order_by": list(base_schema.get("order_by") or []),
              "partition_by": base_schema.get("partition_by") or "tuple()",
              "skip_indexes": list(base_schema.get("skip_indexes") or []),
              "projections": list(base_schema.get("projections") or [])}
    applied = []
    types = ac.column_types(ch, RUN_DB, tbl)
    for f in findings:
        if f.get("verdict") != "finding":
            continue
        lbl, ev = f["_label"], f.get("evidence", {})
        if lbl == "C1":
            schema["order_by"] = f["proposed_order_by"].split(", "); applied.append("C1")
        elif lbl in ("C3", "C3b"):
            schema["partition_by"] = "tuple()"; applied.append(lbl)
        elif lbl == "C4":
            for c in ev.get("candidate_columns", []):
                schema["skip_indexes"].append({"column": c, "type": ev.get("index_type") or "minmax"})
            applied.append("C4")
        elif lbl == "C5":
            for c in ev.get("candidate_columns", []):
                schema["columns"][c] = "LowCardinality(String)"
            applied.append("C5")
        elif lbl == "C6":
            for c in ev.get("candidate_columns", []):
                base_t = types.get(c, "DateTime").split(" CODEC")[0]
                schema["columns"][c] = f"{base_t} CODEC(Delta, ZSTD(1))"
            applied.append("C6")
        elif lbl == "C7":
            for s in ev.get("projection_specs", []):
                schema["projections"].append(s)
            applied.append("C7")
    # compose: don't project the column the (corrected) base ORDER BY already serves; dedup by order col
    lead = schema["order_by"][0] if schema["order_by"] else None
    dedup = {p["order"]: p for p in schema["projections"] if p["order"] != lead}
    schema["projections"] = list(dedup.values())
    ch.command("DROP DATABASE IF EXISTS advisor_cf SYNC")   # bound disk across the matrix
    return schema, sorted(set(applied))


def parse_schema(text, valid_cols):
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return None
    try:
        d = json.loads(m.group(0))
    except Exception:
        return None
    cols = {k: v for k, v in (d.get("columns") or {}).items() if k in valid_cols}
    ob = [c for c in (d.get("order_by") or []) if c in valid_cols]
    six = [ix for ix in (d.get("skip_indexes") or [])
           if isinstance(ix, dict) and ix.get("column") in valid_cols and ix.get("type")]
    return {"columns": cols, "order_by": ob,
            "partition_by": (d.get("partition_by") or "tuple()").strip() or "tuple()", "skip_indexes": six}


def llm_schema(task, model, skills=False):
    """BLIND design — domain + columns only, NO workload. If skills=True, inject
    ClickHouse's Agent Skills best-practice rules into the prompt (+skills treatment)."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return None
    import anthropic
    cols = "\n".join(f"  - {n} ({t})" for n, t in task["columns"])
    skills_block = (SKILLS_RULES + "\n\n") if skills else ""
    sys_p = (
        "You are a ClickHouse physical-design expert designing a table at creation time. "
        + skills_block +
        "Given a domain and columns, design the best MergeTree schema for a general analytics "
        "workload. Choose per-column storage types, per-column codecs, ORDER BY, PARTITION BY, "
        "and any data-skipping indexes. Reply with ONLY JSON: {\"columns\": {\"col\": \"TYPE "
        "[CODEC(...)]\"}, \"order_by\": [\"col\"], \"partition_by\": \"<expr or tuple()>\", "
        "\"skip_indexes\": [{\"column\": \"col\", \"type\": \"minmax\"}]}.")
    usr = f"Domain: {task['domain']}\n\nColumns:\n{cols}\n\nReturn JSON only."
    resp = anthropic.Anthropic(api_key=key).messages.create(
        model=model, max_tokens=600, system=sys_p, messages=[{"role": "user", "content": usr}])
    text = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")
    return parse_schema(text, [n for n, _ in task["columns"]])


def short(m):
    return m.replace("claude-", "")


def design_brief(s):
    ob = ",".join(s.get("order_by") or ["tuple()"])
    return f"OB({ob}) PART({s.get('partition_by','tuple()')})"


def run(ch, name, models, rows):
    task = TASKS[name]
    ensure_source(ch, name, task, rows)
    out = []

    def treat(label, schema, applied=None, design=None):
        note = ""
        try:
            tbl = materialize(ch, name, task, label, schema)
        except Exception as e:
            # invalid model schema (hallucinated codec OR malformed skip index) —
            # fall back to keys-only so the pruning design is still measured.
            if schema.get("columns") or schema.get("skip_indexes"):
                note = " [invalid schema→keys-only]"
                schema = {**schema, "columns": {}, "skip_indexes": []}
                try:
                    tbl = materialize(ch, name, task, label, schema)
                except Exception as e2:
                    out.append({"t": label, "read_rows": None, "error": str(e2).split("Stack trace")[0][:70]})
                    return None
            else:
                out.append({"t": label, "read_rows": None, "error": str(e).split("Stack trace")[0][:70]})
                return None
        rr, by = measure(ch, task, tbl)
        out.append({"t": label, "read_rows": rr, "bytes": by, "applied": applied,
                    "design": (design if design else design_brief(schema)) + note})
        return tbl

    treat("reference", task["reference"])
    nt = treat("naive", task["naive"])
    if nt:
        corr, ap = apply_advisor(ch, nt, task["naive"]); treat("naive+adv", corr, ap)
    for model in models:
        for skills in (False, True):
            tag = short(model) + ("+skills" if skills else "")
            s = llm_schema(task, model, skills=skills)
            if not s:
                out.append({"t": tag, "read_rows": None, "error": "no proposal"}); continue
            tbl = treat(tag, s)
            if tbl:
                corr, ap = apply_advisor(ch, tbl, s); treat(f"{tag}+adv", corr, ap)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", default="ecom_blind")
    ap.add_argument("--models", default=DEFAULT_MODELS)
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--host", default="localhost")
    args = ap.parse_args()
    ch = ac.client(args.host)
    models = [m.strip() for m in args.models.split(",") if m.strip()]
    rows = run(ch, args.task, models, args.rows)

    ref = next((r for r in rows if r["t"] == "reference" and r.get("read_rows") is not None), None)
    rr0, by0 = (ref["read_rows"], ref["bytes"]) if ref else (None, None)
    print(f"\n=== CH-Agent-Bench · BLIND design / held-out workload · task={args.task} · {args.rows:,} rows ===")
    print(f"{'treatment':<18}{'read_rows':>12}{'MB':>9}{'vs_ref_rd':>11}  {'design / applied':<32}")
    for r in rows:
        if r.get("read_rows") is None:
            print(f"{r['t']:<18}{'ERR':>12}{'':>9}{'':>11}  {r.get('error','')}"); continue
        vrd = f"{r['read_rows']/rr0:.1f}x" if rr0 else "-"
        tag = (("applied " + ",".join(r["applied"])) if r.get("applied") else r.get("design", ""))
        print(f"{r['t']:<18}{r['read_rows']:>12,}{r['bytes']/1e6:>9.1f}{vrd:>11}  {tag:<32}")
    print(json.dumps({"task": args.task, "rows": args.rows, "results": rows}, indent=2, default=str),
          file=sys.stderr)


if __name__ == "__main__":
    main()
