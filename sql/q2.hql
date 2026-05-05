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
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q2';

INSERT OVERWRITE TABLE q2_results
SELECT
    eco_code,
    -- TEXTFILE delimiter is ',' and has no quoting; opening_name often contains
    -- commas (e.g., "King's Indian Defense, Classical"). Replace to keep rows aligned.
    regexp_replace(MAX(opening_name), ',', ';') AS opening_name,
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
