#!/bin/bash

source scripts/load_env.sh

echo "[Stage 2] Cleaning Hive database..."
beeline -u jdbc:hive2://${HIVE__HOST}:${HIVE__PORT} \
    -n "$PG__USER" -p "$PG__PASSWORD" \
    --hivevar dbname="$PG__DBNAME" \
    -e "DROP DATABASE IF EXISTS ${PG__DBNAME} CASCADE;"

echo "[Stage 2] Creating HIVE tables..."

beeline -u jdbc:hive2://${HIVE__HOST}:${HIVE__PORT} \
    -n "$PG__USER" -p "$PG__PASSWORD" \
    --hivevar dbname="$PG__DBNAME" \
    -f sql/db.hql > output/hive_results.txt

echo "[Stage 2] Done!"
