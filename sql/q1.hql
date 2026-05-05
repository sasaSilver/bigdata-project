-- q1: game-level. One row per game via ply_index = 1.
-- Outcome distribution by Chess.com time_class (bullet/blitz/rapid/daily).

USE ${dbname};

DROP TABLE IF EXISTS q1_results;

CREATE EXTERNAL TABLE q1_results (
    time_class STRING,
    final_result_class STRING,
    games BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/q1';

INSERT OVERWRITE TABLE q1_results
SELECT
    time_class,
    final_result_class,
    COUNT(*) AS games
FROM chess_moves
WHERE ply_index = 1
GROUP BY time_class, final_result_class
ORDER BY time_class, final_result_class;
