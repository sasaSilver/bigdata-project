-- q5: ply-level. is_checkmate = TRUE gives one ply per decisive game.
-- Phase distribution of checkmating plies.

USE ${dbname};

DROP TABLE IF EXISTS q5_results;

CREATE EXTERNAL TABLE q5_results (
    phase STRING,
    checkmate_plies BIGINT,
    avg_ply_index DOUBLE
)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/q5'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE q5_results
SELECT
    CASE
        WHEN is_endgame_phase    THEN 'endgame'
        WHEN is_middlegame_phase THEN 'middlegame'
        WHEN is_opening_phase    THEN 'opening'
        ELSE 'unclassified'
    END AS phase,
    COUNT(*)                  AS checkmate_plies,
    ROUND(AVG(ply_index), 2)  AS avg_ply_index
FROM chess_moves
WHERE is_checkmate = TRUE
GROUP BY
    CASE
        WHEN is_endgame_phase    THEN 'endgame'
        WHEN is_middlegame_phase THEN 'middlegame'
        WHEN is_opening_phase    THEN 'opening'
        ELSE 'unclassified'
    END
ORDER BY checkmate_plies DESC;
