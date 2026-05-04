#!/bin/bash

source scripts/load_env.sh

echo "[Stage 1] Creating database schema..."
.venv/bin/alembic upgrade head

echo "[Stage 1] Downloading the dataset..."

DATASET_FILE="data/chess_moves_dataset.csv"

if [[ -f "$DATASET_FILE" ]]; then
    echo "[Stage 1] Dataset file already exists. Skipping download."
else
    echo "[Stage 1] Downloading dataset file..."
    .venv/bin/gdown "$GD__DATASET_FILE_ID" -O "$DATASET_FILE"
fi

echo "[Stage 1] Ingesting data into postgres..."
uv run python -m scripts.insert_data

echo "[Stage 1] Importing the database into hdfs..."

sqoop import \
  --connect jdbc:postgresql://${PG__HOST}/${PG__DBNAME} \
  --username "$PG__USER" --password "$PG__PASSWORD" \
  --compression-codec=snappy --compress \
  --as-avrodatafile \
  --warehouse-dir=project/warehouse \
  --m 1 \
  --table chess_moves

echo "[Stage 1] Done!"
