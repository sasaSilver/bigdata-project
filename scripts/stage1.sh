#!/bin/bash

# Create database schema
.venv/bin/alembic upgrade head

echo $PG__HOST

# Load data into PostgreSQL
psql -h $PG__HOST -U $PG__USER -d $PG__DBNAME \
    -c "\COPY chess_moves FROM 'data/output/chess_moves_dataset.csv' CSV HEADER"
