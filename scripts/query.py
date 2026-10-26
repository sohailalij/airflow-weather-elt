"""Print the latest mart rows. Run via `make query`."""

import os

import duckdb

path = os.path.join(os.getenv("PIPELINE_DATA_DIR", "data"), "warehouse.duckdb")
con = duckdb.connect(path, read_only=True)
con.sql(
    """
    SELECT obs_date, city, avg_temp_c, max_temp_c, total_precip_mm, avg_pm2_5, aqi_category
    FROM marts.daily_city_summary
    ORDER BY obs_date DESC, avg_temp_c DESC
    LIMIT 20
    """
).show()
