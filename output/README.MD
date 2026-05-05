# `output/` — Pipeline artifacts

Stage outputs land here. The directory is committed; large intermediates that don't need review should stay out of git via `data/.gitignore` patterns instead.

## Stage 1 artifacts

- `postgres_results.txt` — Postgres ingestion log (Stage 1).
- `*.java` — Sqoop-generated Avro/Parquet schema sources (Stage 1).

## Stage 2 — EDA

The Stage 2 pipeline (`scripts/stage2.sh`) writes one CSV per insight. Charts are added manually.

- `hive_results.txt` — Beeline output of `sql/db.hql` (table-creation log).
- `qN.csv` — auto-generated CSV result of `sql/qN.hql`. Has a header row, comma-separated, no quoting unless a value needs it (beeline `--outputformat=csv2 --silent=true`).
- `qN.jpg` — **manual deliverable.** Chart exported from Apache Superset and dropped into this folder. The pipeline does not produce these because Superset is interactive.

### Per-insight checklist

| File | Granularity | Source query | Chart type | x-axis | y-axis / series |
|------|-------------|--------------|------------|--------|-----------------|
| `q1.csv` / `q1.jpg` | game | `sql/q1.hql` | stacked bar | `time_class` | `games` (color = `final_result_class`) |
| `q2.csv` / `q2.jpg` | game | `sql/q2.hql` | horizontal bar | `eco_code` (top 15) | `white_win_pct` |
| `q3.csv` / `q3.jpg` | game | `sql/q3.hql` | line (3 series) | `rating_gap_bucket` | `white_win_pct`, `draw_pct`, `black_win_pct` |
| `q4.csv` / `q4.jpg` | game | `sql/q4.hql` | bar | `time_class` | `avg_plies` (label `median_plies` if shown) |
| `q5.csv` / `q5.jpg` | ply | `sql/q5.hql` | pie | — | `checkmate_plies` per `phase` |
| `q6.csv` / `q6.jpg` | game | `sql/q6.hql` | line (3 series) | `archive_month` | `white_win_pct`, `black_win_pct`, `draw_pct` |
| `q7.csv` / `q7.jpg` | game | `sql/q7.hql` | stacked bar | `termination` | win-rate columns (color = outcome) |
| `q8.csv` / `q8.jpg` | game (mid-ply) | `sql/q8.hql` | line | `material_bucket` | `white_win_pct` |

### Producing the JPGs in Apache Superset

1. In Superset, **Data → Datasets → + Dataset**, point at the Hive `${PG__DBNAME}.qN_results` table (or upload `output/qN.csv` if Hive is not connected).
2. Build the chart per the table above.
3. Add a descriptive title (e.g. `Q1 — Outcome by time_class`) and save.
4. Open the chart, then **... menu → Download → Image (.jpg)**. Save the file as `output/qN.jpg`.
5. Commit the JPG to the repo.

### Fallback (no Superset)

If Superset is not available for the team, generate the JPGs from `output/qN.csv` with pandas + matplotlib in `notebooks/` and save with the same `output/qN.jpg` filename. The Stage IV deliverable still expects a Superset dashboard, so this is a backup, not the primary path.
