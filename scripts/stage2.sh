#!/bin/bash
set -euo pipefail

source scripts/load_env.sh

# Idempotency: ensure output dir exists and clear stale CSVs from prior runs.
# JPGs are produced manually in Superset; do NOT delete them.
mkdir -p output
rm -f output/q*.csv

BEELINE_URL="jdbc:hive2://${HIVE__HOST}:${HIVE__PORT}"

echo "[Stage 2] Cleaning Hive database..."
beeline -u "$BEELINE_URL" \
    -n "$PG__USER" -p "$PG__PASSWORD" \
    --hivevar dbname="$PG__DBNAME" \
    -e "DROP DATABASE IF EXISTS ${PG__DBNAME} CASCADE;"

echo "[Stage 2] Creating Hive tables (chess_moves with PARTITION + BUCKETING)..."
beeline -u "$BEELINE_URL" \
    -n "$PG__USER" -p "$PG__PASSWORD" \
    --hivevar dbname="$PG__DBNAME" \
    -f sql/db.hql > output/hive_results.txt

echo "[Stage 2] Running EDA queries q1..q8..."
for q in q1 q2 q3 q4 q5 q6 q7 q8; do
    echo "[Stage 2]   - ${q}: building ${q}_results table"
    beeline -u "$BEELINE_URL" \
        -n "$PG__USER" -p "$PG__PASSWORD" \
        --hivevar dbname="$PG__DBNAME" \
        -f "sql/${q}.hql"

    echo "[Stage 2]   - ${q}: exporting CSV to output/${q}.csv"
    beeline -u "$BEELINE_URL" \
        -n "$PG__USER" -p "$PG__PASSWORD" \
        --outputformat=csv2 --silent=true \
        -e "USE ${PG__DBNAME}; SELECT * FROM ${q}_results;" \
        > "output/${q}.csv"
done

echo "[Stage 2] Done! CSVs in output/qN.csv. Charts (output/qN.jpg) are produced manually in Superset."
