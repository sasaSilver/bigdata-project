-- q7: game-level. One row per game via ply_index = 1.
-- Outcome by termination type (top 15, gated to >= 50 games).
-- Note: termination is audit-only for ML, but legitimate for descriptive EDA.

USE ${dbname};

DROP TABLE IF EXISTS q7_results;

CREATE EXTERNAL TABLE q7_results (
    termination STRING,
    games BIGINT,
    white_win_pct DOUBLE,
    black_win_pct DOUBLE,
    draw_pct DOUBLE
)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/q7'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE q7_results
SELECT
    termination,
    COUNT(*)                            AS games,
    ROUND(AVG(white_won_flag) * 100, 2) AS white_win_pct,
    ROUND(AVG(black_won_flag) * 100, 2) AS black_win_pct,
    ROUND(AVG(draw_flag)      * 100, 2) AS draw_pct
FROM chess_moves
WHERE ply_index = 1
GROUP BY termination
HAVING COUNT(*) >= 50
ORDER BY games DESC
LIMIT 15;
