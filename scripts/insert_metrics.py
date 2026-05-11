"""
This script inserts the .csv ml metrics into the postgres ml_metrics table using COPY.
"""

import psycopg2 as pg

from settings import settings

if __name__ == "__main__":
    with pg.connect(settings.pg.conn_dsn) as conn:
        with conn.cursor() as cur:
            with open("output/model_metrics.csv", "r", encoding="utf-8") as f:
                cur.copy_expert("""
COPY ml_metrics (
    model, accuracy, f1
) FROM STDIN WITH (
    FORMAT CSV,
    HEADER,
    DELIMITER ',',
    NULL '',
    FORCE_NULL (promotion_piece)
)
""",
                    f,
                )
            cur.execute("SELECT * FROM ml_meytrics LIMIT 2;")
