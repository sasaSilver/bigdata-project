# Chess Move Database Schema

PostgreSQL database schema for chess move-level dataset analysis using SQLAlchemy ORM.

## Overview

This schema is designed for a Big Data/Spark final project focusing on **multiclass classification** of chess game outcomes. Each row represents one ply (half-move) from one Chess.com game, making the dataset suitable for large-scale sequence-to-tabular modeling.

## Key Design Decisions

### 1. Denormalized Schema (Single Table)

**Rationale:** 
- Optimized for ML training and Spark operations
- No joins required for model training data
- Better parallelism and performance in distributed computing
- Point-in-time data preservation (ratings at game time)

**Trade-offs:**
- Storage overhead (~20-30% larger)
- Data duplication (player names, game metadata per ply)

### 2. Enum Types

Categorical fields use PostgreSQL enums for type safety:
- `ResultClass` - Game outcome (white_win, black_win, draw) - **primary ML target**
- `TimeClass` - Time control categories (bullet, blitz, rapid, daily)
- `SideToMove` - Current player (white, black)
- `PieceType` - Chess pieces (pawn, knight, bishop, rook, queen, king)
- `PromotionPiece` - Promotion options (queen, rook, bishop, knight)
- `Weekday` - Day of week (Monday through Sunday)


## Table: `chess_moves`

### Primary Key
- `id` - Auto-generated UUID for each move record

### Metadata Columns
- `game_uuid` - Stable game identifier (indexed)
- `game_url` - Chess.com game URL for auditability
- `archive_month` - Archive partition (YYYY-MM)
- `game_year`, `game_month` - Temporal fields for partitioning
- `game_end_timestamp`, `game_end_datetime_utc` - Game end time
- `end_hour_utc`, `end_weekday_utc` - Derived temporal features

### Game Metadata
- `rated` - Whether the game was rated
- `rules` - Ruleset (e.g., "chess")
- `time_class` - Time bucket (bullet, blitz, rapid, daily) - **indexed**
- `time_control_raw` - Original time-control string
- `time_control_base_seconds`, `time_control_increment_seconds` - Parsed time control

### Opening/ECO Information
- `eco_code` - ECO opening classification code - **indexed**
- `opening_name` - Human-readable opening name
- `eco_url` - Chess.com opening reference URL

### Player Information
- `white_username`, `black_username` - Player usernames
- `white_rating`, `black_rating` - Player ratings - **indexed**
- `rating_diff` - White rating - black rating
- `white_accuracy`, `black_accuracy` - Post-game accuracy scores

### Move Context
- `ply_index` - 1-based ply number within game - **indexed**
- `fullmove_number` - Human-readable move number
- `side_to_move` - 'w' or 'b' before move
- `side_to_move_name` - 'white' or 'black' (enum)
- `san_move` - Standard algebraic notation
- `uci_move` - UCI notation (machine-friendly)
- `piece_moved` - Piece type moved (enum)
- `from_square`, `to_square` - Move coordinates

### Move Flags
- `is_capture`, `is_check`, `is_checkmate` - Position flags
- `is_castling`, `is_promotion` - Special moves
- `promotion_piece` - Promoted piece type (enum, nullable)

### Board State (FEN)
- `board_fen_before`, `board_fen_after` - Full FEN strings
- `active_color_before`, `castling_rights_before` - FEN components
- `en_passant_square_before` - En passant target square
- `halfmove_clock_before`, `fullmove_number_before` - FEN counters
- `legal_moves_count_before`, `total_piece_count_before` - Position analysis

### Material Analysis
- `material_white/black_before` - Material counts (piece weights)
- `material_diff_before` - White material - black material
- `non_pawn_material_white/black_before` - Non-pawn material
- Piece counts by type for each side (pawns, knights, bishops, rooks, queens)

### Pawn Structure
- `white/black_doubled_pawns_before` - Doubled pawn counts
- `white/black_isolated_pawns_before` - Isolated pawn counts
- `white/black_passed_pawns_before` - Passed pawn counts
- Difference fields for all above
- `white/black_pawn_shield_score_before` - Pawn shield scores

### King Safety
- `white/black_king_tropism_before` - Enemy piece closeness to king
- `king_tropism_diff_before` - White tropism - black tropism

### Castling & Bishop Analysis
- Castling rights for both sides (kingside, queenside)
- `bishops_pair_white/black_before` - Bishop pair flags
- `in_check_before` - Pre-move check flag

### Game Phase Classification
- `is_opening_phase`, `is_middlegame_phase`, `is_endgame_phase`
- `ply_bucket` - Ply range bucket (e.g., "0001-0010")

### Clock/Time Data
- `white/black_clock_seconds_after` - Clock after move
- `side_to_move_clock_before` - Reconstructed pre-move clock
- `time_spent_on_move_seconds` - Seconds spent on current move
- `clock_remaining_pct` - Fraction of base time remaining
- `avg_time_spent_per_move_so_far` - Average time spent on previous moves
- `is_in_time_trouble_30s` - Time pressure flag

### Player Rating Context
- `side_to_move_rating` - Rating of player about to move
- `opponent_rating` - Rating of waiting player
- `side_to_move_rating_diff` - Rating gap

### Audit-Only Columns
**Do not use these as modeling features** - they reveal the final outcome:
- `result_raw` - Raw result signals (PGN + API)
- `termination` - Termination method (resignation, checkmate, etc.)

### Target Columns
**Primary ML target:**
- `final_result_class` - White win, black win, or draw (enum) - **indexed**

**Derived target flags:**
- `white_won_flag` - 1 if white won, else 0
- `black_won_flag` - 1 if black won, else 0
- `draw_flag` - 1 if draw, else 0

### Metadata
- `created_at`, `updated_at` - Record timestamps

## Indexes

### Composite Indexes for Optimized Querying

**Game-Level Queries:**
- `ix_chess_moves_game_ply` → `(game_uuid, ply_index)` - Retrieve all moves for a game in order

**Rating-Based Analysis:**
- `ix_chess_moves_white_rating_result` → `(white_rating, final_result_class)`
- `ix_chess_moves_black_rating_result` → `(black_rating, final_result_class)`
- `ix_chess_moves_side_rating_diff` → `(side_to_move_rating, side_to_move_rating_diff)`

**Time Control Performance:**
- `ix_chess_moves_time_class_result` → `(time_class, final_result_class)`
- `ix_chess_moves_time_control_base_result` → `(time_control_base_seconds, final_result_class)`

**Opening/ECO Analysis:**
- `ix_chess_moves_eco_code_result` → `(eco_code, final_result_class)`
- `ix_chess_moves_eco_ply` → `(eco_code, ply_index)`

**Temporal Analysis:**
- `ix_chess_moves_year_month_result` → `(game_year, game_month, final_result_class)`
- `ix_chess_moves_timestamp` → `(game_end_timestamp)`

**Material Advantage:**
- `ix_chess_moves_material_diff_result` → `(material_diff_before, final_result_class)`
- `ix_chess_moves_material_counts` → `(material_white_before, material_black_before, final_result_class)`

**Game Phase:**
- `ix_chess_moves_phase_result` → `(is_opening_phase, is_middlegame_phase, is_endgame_phase)`
- `ix_chess_moves_ply_bucket_result` → `(ply_bucket, final_result_class)`

**Position-Specific:**
- `ix_chess_moves_check_result` → `(is_check, final_result_class)`
- `ix_chess_moves_capture_result` → `(is_capture, final_result_class)`

**Time Trouble:**
- `ix_chess_moves_time_trouble_result` → `(is_in_time_trouble_30s, final_result_class)`

**Rating Matchups:**
- `ix_chess_moves_ratings_matchup` → `(white_rating, black_rating, final_result_class)`

**Piece Count:**
- `ix_chess_moves_piece_count_result` → `(total_piece_count_before, final_result_class)`

**Advanced Positional:**
- `ix_chess_moves_castling_result` → Castling rights vs result
- `ix_chess_moves_bishop_pair_result` → Bishop pair vs result

### Loading into PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ChessML").getOrCreate()

# Read from PostgreSQL
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/chess_db") \
    .option("dbtable", "chess_moves") \
    .option("user", "user") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Filter for training data
training_data = df.filter(
    (df.is_middlegame_phase == True) &
    (df.time_class == "rapid")
)
```

## Data Loading

### Bulk Import from CSV

For large datasets (660K+ rows), use PostgreSQL bulk copy:

```bash
# Create table first
python -c "from sql.schema import Base, engine; Base.metadata.create_all(engine)"

# Copy data using psql
psql -h localhost -U user -d chess_db \
    -c "\COPY chess_moves FROM 'data/output/moves.csv' CSV HEADER"
```

## Performance Considerations

### For OLTP (Transaction Processing):
- Current denormalized schema is **not optimal**
- Consider star/fact schema for analytical queries
- Add triggers for data consistency

### For OLAP (Analytics/ML):
- Current schema is **highly optimized**
- Single table enables fast full scans
- Complementary indexes support common query patterns
- Easy to export to Parquet/Avro for Spark

### Storage Optimization:
- Consider partitioning by `archive_month` for time-series queries
- Use TOAST compression for large text fields (FEN strings)
- Consider columnar storage for analytics workloads
