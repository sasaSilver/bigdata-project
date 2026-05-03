#!/bin/bash

# insstall uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# install dependencies
uv sync

# load environment variables
export $(grep -v '^#' .env | xargs)



# drop existing tables
.venv/bin/alembic downgrade base