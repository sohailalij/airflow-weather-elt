"""Print the latest mart rows. Run via `make query` or the docker compose command in the README."""

import os

import duckdb

path = os.path.join(os.getenv("PIPELINE_DATA_DIR", "data"), "warehouse.duckdb")
con = duckdb.connect(path, read_only=True)

print("\n== City daily summary ==")
con.sql(
    """
    SELECT obs_date, city, avg_temp_c, max_temp_c, total_precip_mm, avg_pm2_5, aqi_category
    FROM marts.daily_city_summary
    ORDER BY obs_date DESC, avg_temp_c DESC
    LIMIT 12
    """
).show(max_width=250)

print("\n== Entity resolution: how each raw site name was resolved ==")
con.sql(
    """
    SELECT site_id, raw_location, resolved_city, match_method, match_score, status, owner_team
    FROM ref.site_resolution
    ORDER BY site_id
    """
).show(max_width=250, max_rows=20)

print("\n== Site attribution mart: latest day, highest-criticality sites first ==")
con.sql(
    """
    SELECT obs_date, site_id, city, business_unit, owner_team, criticality_tier,
           avg_pm2_5, aqi_category
    FROM marts.site_environment_daily
    WHERE obs_date = (SELECT max(obs_date) FROM marts.site_environment_daily)
    ORDER BY criticality_tier, avg_pm2_5 DESC NULLS LAST
    """
).show(max_width=250, max_rows=20)
