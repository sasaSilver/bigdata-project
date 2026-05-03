#!/bin/bash

# insstall uv
# pip install uv

# install dependencies
uv sync

# load environment variables
export $(grep -v '^#' .env | xargs)



# drop existing tables
.venv/bin/alembic downgrade base