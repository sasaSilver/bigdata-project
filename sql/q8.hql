-- q8: game-level sampled to one mid-game ply per game.
-- Picks the first middlegame ply per game_uuid, buckets material_diff_before,
-- and computes outcome rates per bucket. The JOIN is on game_uuid (the bucket
-- key on chess_moves) so the bucket map join can fire.

USE ${dbname};

DROP TABLE IF EXISTS q8_results;

CREATE EXTERNAL TABLE q8_results (
    material_bucket STRING,
    games BIGINT,
    white_win_pct DOUBLE,
    draw_pct DOUBLE,
    black_win_pct DOUBLE
)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/q8'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

WITH mid AS (
    SELECT
        game_uuid,
        MIN(ply_index) AS mid_ply_index
    FROM chess_moves
    WHERE is_middlegame_phase = TRUE
    GROUP BY game_uuid
)
INSERT OVERWRITE TABLE q8_results
SELECT
    CASE
        WHEN c.material_diff_before <= -5 THEN 'white_down>=5'
        WHEN c.material_diff_before <= -2 THEN 'white_down_2_4'
        WHEN c.material_diff_before <   0 THEN 'white_down_1'
        WHEN c.material_diff_before =  0 THEN 'equal'
        WHEN c.material_diff_before <   2 THEN 'white_up_1'
        WHEN c.material_diff_before <=  4 THEN 'white_up_2_4'
        ELSE 'white_up>=5'
    END AS material_bucket,
    COUNT(*) AS games,
    ROUND(AVG(c.white_won_flag) * 100, 2) AS white_win_pct,
    ROUND(AVG(c.draw_flag)      * 100, 2) AS draw_pct,
    ROUND(AVG(c.black_won_flag) * 100, 2) AS black_win_pct
FROM chess_moves c
JOIN mid
  ON c.game_uuid = mid.game_uuid
 AND c.ply_index = mid.mid_ply_index
GROUP BY
    CASE
        WHEN c.material_diff_before <= -5 THEN 'white_down>=5'
        WHEN c.material_diff_before <= -2 THEN 'white_down_2_4'
        WHEN c.material_diff_before <   0 THEN 'white_down_1'
        WHEN c.material_diff_before =  0 THEN 'equal'
        WHEN c.material_diff_before <   2 THEN 'white_up_1'
        WHEN c.material_diff_before <=  4 THEN 'white_up_2_4'
        ELSE 'white_up>=5'
    END
ORDER BY MIN(c.material_diff_before);
