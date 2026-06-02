#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 60-second demo: point the Advisor at a table that has a real query history
# and get MEASURED, copy-paste, result-preserving ClickHouse DDL back.
#
# Requires: a local ClickHouse (clickhouse-client on PATH) and a Python with
# clickhouse-connect installed. Set ADVISOR_PYTHON to that interpreter.
#   ADVISOR_PYTHON=/path/to/venv/bin/python ./demo.sh
# ---------------------------------------------------------------------------
set -euo pipefail
CH="${CLICKHOUSE_CLIENT:-clickhouse-client}"
PY="${ADVISOR_PYTHON:-python3}"
DB="${DEMO_DB:-advisor_demo}"

echo "▸ Building a demo 'events' table (3M rows) with a deliberately mediocre design:"
echo "  ORDER BY ts — but the workload filters status/country; String low-card columns;"
echo "  an oversized integer; an un-codec'd timestamp."
$CH --query "DROP DATABASE IF EXISTS $DB SYNC"
$CH --query "CREATE DATABASE $DB"
$CH --query "CREATE TABLE $DB.events (
  event_id    UInt64,
  user_id     UInt64,
  status      String,
  country     String,
  device      String,
  amount_cents UInt64,
  ts          DateTime
) ENGINE = MergeTree ORDER BY ts"
$CH --query "INSERT INTO $DB.events
  SELECT number,
         rand() % 1000000,
         ['ok','ok','ok','failed','pending'][(number % 5) + 1],
         ['US','GB','DE','FR','JP','BR'][(number % 6) + 1],
         ['ios','android','web'][(number % 3) + 1],
         number % 50000,
         toDateTime('2026-01-01 00:00:00') + intDiv(number, 40)
  FROM numbers(3000000)"
$CH --query "OPTIMIZE TABLE $DB.events FINAL"

echo "▸ Running the real workload (so it lands in system.query_log — the signal the Advisor reads):"
$CH --query "SELECT count() FROM $DB.events WHERE status = 'failed'"
$CH --query "SELECT country, count() FROM $DB.events WHERE status = 'failed' GROUP BY country ORDER BY count() DESC"
$CH --query "SELECT count() FROM $DB.events WHERE status = 'pending' AND country = 'DE'"
$CH --query "SYSTEM FLUSH LOGS"

echo
echo "▸ Advisor:"
"$PY" advisor.py "$DB" events

$CH --query "DROP DATABASE IF EXISTS $DB SYNC"
