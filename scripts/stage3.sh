#!/bin/bash

echo "[Stage 3] Running model training..."
python notebooks/chess_modeling.py > output/model_results.txt

echo "[Stage 3] Ingesting ml metrics into postgres..."
uv run python -m scripts.insert_metrics > output/metrics_results.txt
