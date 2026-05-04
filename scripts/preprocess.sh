#!/bin/bash

source scripts/load_env.sh

# install dependencies
echo "[Preprocess] Installing dependencies..."

# install uv
if command -v uv &> /dev/null; then
    echo "[Preprocess] uv installed. Skipping..."
else
    echo "[Preprocess] Downloadig uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi

uv sync

echo "[Preprocess] Dropping existing tables..."
.venv/bin/alembic downgrade base

echo "[Preprocess] Cleaning HDFS warehouse..."
hdfs dfs -rm -r -skipTrash \
    hdfs://${HDFS__WAREHOUSE_HOST}:${HDFS__WAREHOUSE_PORT}/user/${PG__USER}/project/warehouse

echo "[Preprocess] Cleaning Hive database..."
beeline -u jdbc:hive2://${HIVE__HOST}:${HIVE__PORT} \
    -n "$PG__USER" -p "$PG__PASSWORD" \
    --hivevar dbname="$PG__DBNAME" \
    -e "DROP DATABASE IF EXISTS ${PG__DBNAME} CASCADE;"
