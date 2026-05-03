#!/bin/bash

# insstall uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# install dependencies
echo "Installing dependencies..."
uv sync

# load environment variables
export $(grep -v '^#' .env | xargs)

echo "Dropping existing tables..."

# drop existing tables
.venv/bin/alembic downgrade base