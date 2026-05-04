#!/bin/bash

# install dependencies
echo "[Preprocess] Installing dependencies..."
# install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync

echo "[Preprocess] Dropping existing tables..."
# drop existing tables
.venv/bin/alembic downgrade base
