This repository is a template for the final project of big data course. It contains the following directories:

- `data/` contains the dataset files.
- `models/` contains the Spark ML models.
- `notebooks/` has the Jupyter or Zeppelin notebooks of your project and used just for learning purposes.
- `output/` represents the output directory for storing the results of the project. It can contain `csv` files, text files. images and any other materials you returned as an ouput of the pipeline.
- `scripts/` is a place for storing `.sh` scripts and `.py` scripts of the pipeline.
- `sql/` is a folder for keeping all `.sql` and `.hql` files.

`requirements.txt` lists the Python packages needed for running your Python scripts. Feel free to add more packages when necessary.

`main.sh` is the main script that will run all scripts of the pipeline stages which will execute the full pipeline and store the results in `output/` folder. During checking your project repo, the grader will run only the main script and check the results in `output/` folder.

**Important Note:** You cannot change the content of the script `main.sh` since it will be used for assessment purposes.

**Another Note:** The notebooks in `notebooks/` folder are used only for learning purposes since you need to put all Python scripts of the pipeline in `scripts/` folder. During the assessment, the grader can delete the folder `notebooks/` to check that your pipeline does not depend on its content.

## Stage 2 — EDA

Stage 2 lives in `sql/db.hql` (schema) and `sql/q1.hql` … `sql/q8.hql` (insights), driven by `scripts/stage2.sh`.

### Hive layout

The fact table `chess_moves` is **partitioned by `archive_month` and bucketed by `game_uuid` into 8 buckets** — satisfies the Stage II "at least one Hive table with both partitioning and bucketing" requirement on a single source-of-truth table that Stage 3 and Stage 4 will reuse. The bucket key is also the join key in `q8`, exercising the bucket map join.

### Insights

The BS track requires at least 6 insights (2 points each: query + chart + story). We produce **8** to leave headroom:

| # | Question | Granularity |
|---|----------|-------------|
| q1 | Outcome distribution by `time_class` (bullet/blitz/rapid/daily) | game |
| q2 | Top 15 ECO openings by white win rate | game |
| q3 | Win rate vs `rating_diff` bucket | game |
| q4 | Game length (avg/median plies) by `time_class` | game |
| q5 | Phase distribution of checkmating plies | ply |
| q6 | White/black/draw share over `archive_month` | game |
| q7 | Outcome by `termination` type (top 15) | game |
| q8 | Mid-game material advantage → outcome | game (1 mid-ply per game) |

**Granularity convention:** the dataset is ply-level (~6.7 plies/game), so naive `COUNT(*) GROUP BY game_attribute` weights long games. Game-level insights filter `WHERE ply_index = 1` (one row per game). q5 is genuinely per-ply.

### Outputs

Each insight `qN`:

- **SQL** — `sql/qN.hql` creates an external `qN_results` table at `project/hive/warehouse/qN` and populates it via `INSERT OVERWRITE`.
- **CSV** — `output/qN.csv`, auto-exported by `scripts/stage2.sh` via `beeline --outputformat=csv2 --silent=true`.
- **Chart** — `output/qN.jpg`, **produced manually in Apache Superset** (see `output/README.MD` for the per-query checklist and Superset workflow).

### Re-running

`bash scripts/stage2.sh` is idempotent — drops the Hive database, recreates `chess_moves_raw`, `chess_moves` (partitioned + bucketed), `qN_results` for each insight, and rewrites every `output/qN.csv`. JPGs in `output/` are preserved across runs.
