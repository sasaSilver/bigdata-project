#!/bin/bash

source scripts/load_env.sh

# install dependencies
echo "[Preprocess] Installing dependencies..."
# install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync

echo "[Preprocess] Dropping existing tables..."
.venv/bin/alembic downgrade base

echo "[Preprocess] Cleaning up HDFS warehouse..."
hdfs dfs -rm -r -skipTrash hdfs://${HDFS__WAREHOUSE_HOST}:${HDFS__WAREHOUSE_PORT}/user/${PG__USER}/project/warehouse
