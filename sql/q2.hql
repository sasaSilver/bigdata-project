-- q2: game-level. One row per game via ply_index = 1.
-- Top 15 ECO openings by white win rate, gated to >= 200 games to suppress noise.

USE ${dbname};

DROP TABLE IF EXISTS q2_results;

CREATE EXTERNAL TABLE q2_results (
    eco_code STRING,
    opening_name STRING,
    games BIGINT,
    white_win_pct DOUBLE,
    black_win_pct DOUBLE,
    draw_pct DOUBLE
)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/q2'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE q2_results
SELECT
    eco_code,
    MAX(opening_name) AS opening_name,
    COUNT(*) AS games,
    ROUND(AVG(white_won_flag) * 100, 2) AS white_win_pct,
    ROUND(AVG(black_won_flag) * 100, 2) AS black_win_pct,
    ROUND(AVG(draw_flag)      * 100, 2) AS draw_pct
FROM chess_moves
WHERE ply_index = 1
GROUP BY eco_code
HAVING COUNT(*) >= 200
ORDER BY white_win_pct DESC
LIMIT 15;
