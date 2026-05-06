-- q6: game-level. One row per game via ply_index = 1.
-- White/black/draw share over the partition column archive_month.

USE ${dbname};

DROP TABLE IF EXISTS q6_results;

CREATE EXTERNAL TABLE q6_results (
    archive_month STRING,
    games BIGINT,
    white_win_pct DOUBLE,
    black_win_pct DOUBLE,
    draw_pct DOUBLE
)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/q6'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE q6_results
SELECT
    archive_month,
    COUNT(*) AS games,
    ROUND(AVG(white_won_flag) * 100, 2) AS white_win_pct,
    ROUND(AVG(black_won_flag) * 100, 2) AS black_win_pct,
    ROUND(AVG(draw_flag)      * 100, 2) AS draw_pct
FROM chess_moves
WHERE ply_index = 1
GROUP BY archive_month
ORDER BY archive_month;
