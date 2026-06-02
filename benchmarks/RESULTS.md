# CH-Agent-Bench results

This benchmark asks one question. Can a model design a ClickHouse table as well as a tool that reads the real workload?

The setup is the same each time. A model designs the table blind. It sees the columns and the domain. It does not see the queries. This is what an agent does at table-creation time. We run the model twice. Once on its own. Once with ClickHouse's own Agent Skills rules added to the prompt. Then we score each design against a held-out workload by rows read. Lower is better. The Advisor then reads that same workload from `system.query_log` and corrects the design.

The model is Claude Opus 4.8. The harness is `bench_blind.py`.

## Result 1: the model wins on an obvious schema

The table is the canonical ClickBench `hits` table. It has 1,031,895 rows. The access pattern is easy to read off the column names.

| treatment | read_rows | vs reference | design |
|---|---|---|---|
| reference | 98,305 | 1.0x | expert ORDER BY |
| naive | 614,400 | 6.2x | ORDER BY EventTime |
| opus blind | 98,305 | 1.0x | ORDER BY CounterID, EventDate, UserID, EventTime, WatchID |
| opus + Agent Skills | 98,305 | 1.0x | same |

Opus reproduced the expert ORDER BY exactly. It did this with no workload. Agent Skills made no difference. On a schema like this the Advisor does not out-design the model. This is an honest loss for the workload-grounding idea. We report it because it is true.

## Result 2: the model loses when the workload is hidden

The table is a multi-tenant events table. It has 3,000,000 rows. The held-out workload filters two independent columns. One is `tenant` with an equality filter. The other is `metric` with a range filter. A single ORDER BY cannot serve both. The correct design needs ORDER BY `tenant` plus a data-skipping index on `metric`.

| treatment | read_rows | vs reference | design / applied |
|---|---|---|---|
| reference | 1,679,360 | 1.0x | ORDER BY tenant + skip index on metric |
| naive | 3,065,536 | 1.8x | ORDER BY event_id |
| naive + Advisor | 589,824 | 0.35x | applied C1, C5, C6, C7 |
| opus blind | 2,727,552 | 1.6x | ORDER BY tenant, event_type, category, ts |
| opus + Advisor | 65,536 | 0.04x | applied C7 |
| opus + Agent Skills | 2,727,552 | 1.6x | same as opus blind |
| opus + Skills + Advisor | 65,536 | 0.04x | applied C7 |

Opus ordered by `tenant`. That serves the equality filter. It missed the `metric` range filter. It added no index and no projection. So the range query scanned wide. It read 1.6x the rows of the expert design. Agent Skills did not change this. The two designs are identical.

Then the Advisor read the query log. It saw both filters. It added the missing projection. The corrected design read 65,536 rows. That is 41x fewer than Opus alone. It also beats the expert reference by 25x.

## What the two results mean together

A frontier model is good at design. When the access pattern is visible in the schema, the model gets it right. You do not need this tool for the ORDER BY there.

The Advisor still earns its place two ways. It measures every change on your data and proves your results do not change. And it wins outright when the workload hides a dimension the schema does not show. That second case is common. Multi-tenant tables. Secondary filters. Drifting machine-generated workloads. There the query log is the only signal. The Advisor is the thing that reads it.

## Reproduce

```bash
PYTHONPATH=. python benchmarks/bench_blind.py --task multi_blind --models claude-opus-4-8
```

The model call needs an Anthropic API key in `ANTHROPIC_API_KEY`. Row counts and exact numbers vary a little with the sample size and the model.
