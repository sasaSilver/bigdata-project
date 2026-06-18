-- q4: game-level. Game length = MAX(ply_index) per game_uuid.
-- Aggregate per game first, then per time_class.

USE ${dbname};

DROP TABLE IF EXISTS q4_results;

CREATE EXTERNAL TABLE q4_results (
    time_class STRING,
    games BIGINT,
    avg_plies DOUBLE,
    median_plies DOUBLE,
    min_plies INT,
    max_plies INT
)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/q4'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE q4_results
SELECT
    time_class,
    COUNT(*)                          AS games,
    ROUND(AVG(ply_count), 2)          AS avg_plies,
    PERCENTILE_APPROX(ply_count, 0.5) AS median_plies,
    MIN(ply_count)                    AS min_plies,
    MAX(ply_count)                    AS max_plies
FROM (
    SELECT
        game_uuid,
        time_class,
        MAX(ply_index) AS ply_count
    FROM chess_moves
    GROUP BY game_uuid, time_class
) g
GROUP BY time_class
ORDER BY avg_plies DESC;
