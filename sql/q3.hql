-- q3: game-level. One row per game via ply_index = 1.
-- Win rate by white-perspective rating gap bucket (rating_diff = white - black).

USE ${dbname};

DROP TABLE IF EXISTS q3_results;

CREATE EXTERNAL TABLE q3_results (
    rating_gap_bucket STRING,
    games BIGINT,
    white_win_pct DOUBLE,
    draw_pct DOUBLE,
    black_win_pct DOUBLE
)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/q3'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE q3_results
SELECT
    CASE
        WHEN rating_diff <= -300 THEN 'gap<=-300'
        WHEN rating_diff <= -200 THEN '-300<gap<=-200'
        WHEN rating_diff <= -100 THEN '-200<gap<=-100'
        WHEN rating_diff <=  -25 THEN '-100<gap<=-25'
        WHEN rating_diff <   25  THEN '-25<gap<25'
        WHEN rating_diff <  100  THEN '25<=gap<100'
        WHEN rating_diff <  200  THEN '100<=gap<200'
        WHEN rating_diff <  300  THEN '200<=gap<300'
        ELSE 'gap>=300'
    END AS rating_gap_bucket,
    COUNT(*) AS games,
    ROUND(AVG(white_won_flag) * 100, 2) AS white_win_pct,
    ROUND(AVG(draw_flag)      * 100, 2) AS draw_pct,
    ROUND(AVG(black_won_flag) * 100, 2) AS black_win_pct
FROM chess_moves
WHERE ply_index = 1
GROUP BY
    CASE
        WHEN rating_diff <= -300 THEN 'gap<=-300'
        WHEN rating_diff <= -200 THEN '-300<gap<=-200'
        WHEN rating_diff <= -100 THEN '-200<gap<=-100'
        WHEN rating_diff <=  -25 THEN '-100<gap<=-25'
        WHEN rating_diff <   25  THEN '-25<gap<25'
        WHEN rating_diff <  100  THEN '25<=gap<100'
        WHEN rating_diff <  200  THEN '100<=gap<200'
        WHEN rating_diff <  300  THEN '200<=gap<300'
        ELSE 'gap>=300'
    END
ORDER BY MIN(rating_diff);
