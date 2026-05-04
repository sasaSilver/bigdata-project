#!/bin/bash

echo "[Stage 1] Creating database schema..."
.venv/bin/alembic upgrade head

echo "[Stage 1] Downloading the dataset..."
gdown "1_TRU4n9Jcs-fSxSYb5tEbFcR9rfLO7rJ" -O data/chess_moves_dataset.csv

echo "[Stage 1] Ingesting data into postgres..."
uv run python -m scripts.insert_data
