# MergeTree Advisor

Your ClickHouse table is probably slower and larger than it needs to be. This finds the fix. It measures the fix on your data. It proves the fix does not change your results.

Point it at a table that has a query history. It reads the workload from `system.query_log`. It reads the layout from `system.parts`. It builds a counterfactual rebuild. It measures the read-rows and compressed-bytes delta. Then it hands you copy-paste `ALTER` DDL. Every recommendation carries a number. Every recommendation preserves your query results byte-for-byte.

## What it is not

It is not "ask an LLM to design your schema." I tested that. It does not hold up. See [Honest positioning](#honest-positioning).

It is the closed loop the ClickHouse agent ecosystem is missing. The official `mcp-clickhouse` server is read-only by design. Agent Skills is passive best-practice text. Neither one measures anything on your data. Neither one proves a change is safe. This does both.

## 60-second demo

```bash
ADVISOR_PYTHON=/path/to/venv/bin/python ./demo.sh
```

The demo builds a 3M-row `events` table with a deliberately mediocre design. It is ordered by `ts`, but the workload filters `status` and `country`. Three `String` columns should be low-cardinality. One integer is oversized. The timestamp has no codec. The demo runs a realistic workload. Then it asks the Advisor. Real output:

```text
=== MergeTree Advisor · advisor_demo.events ===
6 finding(s) of 13 checks (ranked by est. rows saved/period)

⚠ C1  high    ORDER BY (ts) doesn't serve the workload's status/country filters.
      → Rebuild as ORDER BY (status, country).
        Measured: 14.3x fewer rows read, 9,000,000 to 630,786. Latency 559ms to 65ms.

⚠ C7  medium  status/country are filtered independently of the leading sort column.
      → ADD PROJECTION adv_proj_status ... MATERIALIZE PROJECTION adv_proj_status.
        Measured: 5.2x fewer rows read. Projection adds 1.5% storage.

⚠ C5  medium  status/country/device are String but low-cardinality.
      → ALTER TABLE events MODIFY COLUMN `status` LowCardinality(String) ...
        Measured: 9.95x smaller on disk, 473 KB to 48 KB.

⚠ C6  medium  ts has no codec.
      → ALTER TABLE events MODIFY COLUMN `ts` DateTime CODEC(Delta, ZSTD(1)).
        Measured: 3.4x smaller.

⚠ C8  medium  event_id/user_id/amount_cents are wider than their values need.
      → MODIFY COLUMN ... UInt32 / UInt16. Measured: 1.26x smaller.
```

Watch what does not fire. `C4` stays silent. `status` is not correlated with the sort key. A skip index would not prune granules. The Advisor fires only what it can measure a win for. It does not invent recommendations.

## How it works

Every check runs the same loop. The loop is measurement-gated. It never recommends a change it cannot show helps.

1. **Read facts.** The workload comes from `system.query_log`. It is aggregated by query shape and weighted by execution frequency. The layout comes from `system.parts`. Reading uses cheap regex. There is no SQL parser. CH-dialect parsers break. The reader takes filter columns and `GROUP BY` keys. It skips what it cannot read.
2. **Build a counterfactual.** This is an identical-data table with the one change applied.
3. **Measure.** Replay the workload. Read `read_rows` from the query summary. Read per-column compressed bytes from `system.parts_columns`.
4. **Emit DDL.** The DDL is idiomatic and ready to paste. It uses `ALTER ... MODIFY COLUMN`, `ADD INDEX` paired with `MATERIALIZE INDEX`, and `ADD PROJECTION` paired with `MATERIALIZE PROJECTION`. Codecs list the encoding codec first and the general-purpose codec last.
5. **Close the loop.** Apply the recommendation. Re-run the workload. The results come back identical. This is verified, not assumed.

## Verified on real data

The canonical ClickBench `hits` table exists to be benchmarked. It is already a careful design. The Advisor still finds storage it leaves on the table.

| Recommendation | Columns | Measured |
|---|---|---|
| LowCardinality | 23 String cols | 2.54x smaller |
| Delta codec | 4 timestamps | 2.05x smaller |
| narrower integers | 45 oversized ints | 1.39x smaller |

The changes are result-preserving. I applied them and re-ran the workload. The results were byte-identical. This was verified on NYC-taxi, NOAA, and IMDb.

The testing is thorough. Every emitted statement executes against the engine. The score is 28 of 28. The crash battery passes across 22 column types with zero failures. The suite also found and fixed real correctness bugs. Changing `ORDER BY` on a `ReplacingMergeTree` silently changes results, so that is now engine-guarded. `ADD INDEX` needs `MATERIALIZE` to cover existing parts. A `Nullable` to bare conversion needs a `DEFAULT`.

## Honest positioning

A frontier model already designs a well-known schema well. I gave Claude Opus 4.8 the `hits` columns with no workload. I ran it with ClickHouse's own Agent Skills and without them. It reproduced the expert `ORDER BY` exactly. It scored identical to the reference. So this tool does not win by out-guessing a model on an obvious schema.

It wins two other ways.

First, measurement over assertion. A model guesses. The Advisor measures. You do not deploy a guess to a 100 TB table. You deploy a change with a measured delta and a result-preservation guarantee.

Second, workload-grounding for the non-obvious. Some access patterns cannot be read off the column names. Secondary filter dimensions. Multi-tenant tables. Drifting machine-generated workloads. There the query log is the only signal.

## Scope and limits

The Advisor recommends for plain `MergeTree`. It abstains on Replacing, Summing, Aggregating, and Collapsing engines for the structural checks. On those engines `ORDER BY` is the dedup key. Changing it changes results.

The workload reader is regex-based. It reads filters and `GROUP BY` keys. It skips function-in-predicate and deep subqueries. It does not guess.

Full-table counterfactuals are memory-bound. For very large tables the method is validated on a sample.

## Install

You need a reachable ClickHouse over HTTP. The default is `localhost:8123`. You need Python 3.10 or newer.

```bash
pip install -r requirements.txt
```

## Usage

```bash
python advisor.py <database> <table> [--host HOST]
```

You get a ranked list of measured findings. The highest estimated rows saved comes first. Each finding has the exact copy-paste DDL and the measured before and after. The Advisor reads only `system.*` and `query_log`. It never touches your table.

Run the demo end-to-end:

```bash
ADVISOR_PYTHON=$(which python) ./demo.sh
```

## Repository layout

```
advisor.py          orchestrator: runs the 13 checks and ranks findings by est. rows saved
advisor_common.py   shared helpers: workload reader, counterfactual build, measure, DDL generators
c1_detector.py      C1: ORDER BY vs. workload filters
c3_detector.py      C3: PARTITION BY misuse
checks_ext.py       C3b, C4, C5, C6, C7, C7b, C8, C9, C10, C11, C12
demo.sh             reproducible 60-second demo
tests/              rigor suite: crash battery, result-preservation, precision/recall, DDL validator
benchmarks/         CH-Agent-Bench: blind model design vs. the workload-grounded Advisor
```

Run anything under `tests/` or `benchmarks/` from the repo root. Put the root on the path.

```bash
PYTHONPATH=. python tests/test_robustness.py
PYTHONPATH=. python benchmarks/bench_blind.py --task multi_blind
```

## License

MIT. See [LICENSE](LICENSE).
