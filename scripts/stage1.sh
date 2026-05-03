#!/bin/bash

# Create database schema
.venv/bin/alembic upgrade head

# Load data into PostgreSQL
psql -h localhost -U user -d chess_db \
    -c "\COPY chess_moves FROM 'data/output/chess_moves_dataset.csv' CSV HEADER"
