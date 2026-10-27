"""Load + model + validate inside DuckDB (free, embedded, zero setup).

DuckDB allows one writer at a time, so loading is deliberately a single task
rather than one task per city.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from weather_pipeline import config

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS ref;

CREATE TABLE IF NOT EXISTS raw.hourly_observations (
    city             VARCHAR   NOT NULL,
    observed_at      TIMESTAMP NOT NULL,
    temperature_c    DOUBLE,
    precipitation_mm DOUBLE,
    wind_speed_kmh   DOUBLE,
    pm2_5            DOUBLE,
    us_aqi           INTEGER,
    loaded_at        TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS marts.daily_city_summary (
    obs_date         DATE    NOT NULL,
    city             VARCHAR NOT NULL,
    avg_temp_c       DOUBLE,
    min_temp_c       DOUBLE,
    max_temp_c       DOUBLE,
    total_precip_mm  DOUBLE,
    max_wind_kmh     DOUBLE,
    avg_pm2_5        DOUBLE,
    max_us_aqi       INTEGER,
    aqi_category     VARCHAR,
    hours_observed   INTEGER
);

CREATE TABLE IF NOT EXISTS ref.site_resolution (
    site_id          VARCHAR NOT NULL,
    raw_location     VARCHAR,
    site_type        VARCHAR,
    cost_center      VARCHAR,
    resolved_city    VARCHAR,
    match_method     VARCHAR,
    match_score      DOUBLE,
    status           VARCHAR,
    business_unit    VARCHAR,
    owner_team       VARCHAR,
    criticality_tier INTEGER,
    matched_rules    VARCHAR,
    resolved_at      TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS marts.site_environment_daily (
    obs_date         DATE    NOT NULL,
    site_id          VARCHAR NOT NULL,
    city             VARCHAR,
    business_unit    VARCHAR,
    owner_team       VARCHAR,
    criticality_tier INTEGER,
    avg_temp_c       DOUBLE,
    avg_pm2_5        DOUBLE,
    max_us_aqi       INTEGER,
    aqi_category     VARCHAR
);

CREATE OR REPLACE VIEW marts.city_rolling_7d AS
SELECT
    obs_date,
    city,
    avg_temp_c,
    round(avg(avg_temp_c) OVER w, 2) AS avg_temp_7d,
    round(avg(avg_pm2_5)  OVER w, 2) AS avg_pm2_5_7d
FROM marts.daily_city_summary
WINDOW w AS (PARTITION BY city ORDER BY obs_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);
"""

EXPECTED_HOURS = 24
MAX_NULL_TEMP_RATIO = 0.05
TEMP_RANGE_C = (-90.0, 60.0)
AQI_RANGE = (0, 500)


@contextmanager
def connect(db_path: Path | str | None = None) -> Iterator[duckdb.DuckDBPyConnection]:
    path = Path(db_path or config.WAREHOUSE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    try:
        con.execute(DDL)
        yield con
    finally:
        con.close()


def load_rows(rows: list[dict], ds: str, db_path: Path | str | None = None) -> int:
    """Idempotent load: replace the whole day, atomically."""
    with connect(db_path) as con:
        con.execute("BEGIN")
        try:
            con.execute(
                "DELETE FROM raw.hourly_observations "
                "WHERE CAST(observed_at AS DATE) = CAST(? AS DATE)",
                [ds],
            )
            con.executemany(
                "INSERT INTO raw.hourly_observations "
                "(city, observed_at, temperature_c, precipitation_mm, "
                " wind_speed_kmh, pm2_5, us_aqi) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        r["city"],
                        r["observed_at"],
                        r["temperature_c"],
                        r["precipitation_mm"],
                        r["wind_speed_kmh"],
                        r["pm2_5"],
                        r["us_aqi"],
                    )
                    for r in rows
                ],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return len(rows)


def build_daily_summary(ds: str, db_path: Path | str | None = None) -> int:
    with connect(db_path) as con:
        con.execute("BEGIN")
        try:
            con.execute(
                "DELETE FROM marts.daily_city_summary WHERE obs_date = CAST(? AS DATE)",
                [ds],
            )
            con.execute(
                """
                INSERT INTO marts.daily_city_summary
                SELECT
                    CAST(observed_at AS DATE)         AS obs_date,
                    city,
                    round(avg(temperature_c), 2),
                    min(temperature_c),
                    max(temperature_c),
                    round(sum(precipitation_mm), 2),
                    max(wind_speed_kmh),
                    round(avg(pm2_5), 2),
                    max(us_aqi),
                    CASE
                        WHEN max(us_aqi) IS NULL THEN 'unknown'
                        WHEN max(us_aqi) <= 50   THEN 'good'
                        WHEN max(us_aqi) <= 100  THEN 'moderate'
                        WHEN max(us_aqi) <= 150  THEN 'unhealthy_for_sensitive'
                        WHEN max(us_aqi) <= 200  THEN 'unhealthy'
                        ELSE 'very_unhealthy_or_worse'
                    END,
                    count(*)
                FROM raw.hourly_observations
                WHERE CAST(observed_at AS DATE) = CAST(? AS DATE)
                GROUP BY 1, 2
                """,
                [ds],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return con.execute(
            "SELECT count(*) FROM marts.daily_city_summary WHERE obs_date = CAST(? AS DATE)",
            [ds],
        ).fetchone()[0]


def load_site_resolutions(records: list[dict], db_path: Path | str | None = None) -> int:
    """Replace the site resolution snapshot (the inventory is a full snapshot each run)."""
    with connect(db_path) as con:
        con.execute("BEGIN")
        try:
            con.execute("DELETE FROM ref.site_resolution")
            con.executemany(
                "INSERT INTO ref.site_resolution (site_id, raw_location, site_type, cost_center, "
                "resolved_city, match_method, match_score, status, business_unit, owner_team, "
                "criticality_tier, matched_rules) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        r["site_id"],
                        r["raw_location"],
                        r["site_type"],
                        r["cost_center"],
                        r["resolved_city"],
                        r["match_method"],
                        r["match_score"],
                        r["status"],
                        r["business_unit"],
                        r["owner_team"],
                        r["criticality_tier"],
                        r["matched_rules"],
                    )
                    for r in records
                ],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return len(records)


def build_site_environment(ds: str, db_path: Path | str | None = None) -> int:
    """Attribute each day's city metrics to resolved sites, their owners and tiers."""
    with connect(db_path) as con:
        con.execute("BEGIN")
        try:
            con.execute(
                "DELETE FROM marts.site_environment_daily WHERE obs_date = CAST(? AS DATE)",
                [ds],
            )
            con.execute(
                """
                INSERT INTO marts.site_environment_daily
                SELECT d.obs_date, s.site_id, s.resolved_city, s.business_unit, s.owner_team,
                       s.criticality_tier, d.avg_temp_c, d.avg_pm2_5, d.max_us_aqi, d.aqi_category
                FROM ref.site_resolution s
                JOIN marts.daily_city_summary d
                  ON d.city = s.resolved_city AND d.obs_date = CAST(? AS DATE)
                WHERE s.status = 'resolved'
                """,
                [ds],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return con.execute(
            "SELECT count(*) FROM marts.site_environment_daily WHERE obs_date = CAST(? AS DATE)",
            [ds],
        ).fetchone()[0]


def find_resolution_failures(
    ds: str,
    min_resolution_rate: float = config.MIN_RESOLUTION_RATE,
    db_path: Path | str | None = None,
) -> list[str]:
    """Monitor the attribution layer: resolution rate, ownership coverage, join completeness."""
    failures: list[str] = []
    with connect(db_path) as con:
        total, resolved, unowned = con.execute(
            """
            SELECT count(*),
                   count(*) FILTER (WHERE status = 'resolved'),
                   count(*) FILTER (WHERE owner_team = 'unassigned')
            FROM ref.site_resolution
            """
        ).fetchone()
        if total == 0:
            return ["ref.site_resolution is empty"]
        rate = resolved / total
        if rate < min_resolution_rate:
            failures.append(
                f"site resolution rate {rate:.0%} is below the {min_resolution_rate:.0%} minimum"
            )
        if unowned:
            failures.append(f"{unowned} site(s) have no owner assigned")
        attributed = con.execute(
            "SELECT count(*) FROM marts.site_environment_daily WHERE obs_date = CAST(? AS DATE)",
            [ds],
        ).fetchone()[0]
        if attributed != resolved:
            failures.append(
                f"site_environment_daily has {attributed} rows for {ds}, expected {resolved}"
            )
    return failures


def find_quality_failures(
    ds: str, expected_cities: list[str], db_path: Path | str | None = None
) -> list[str]:
    """Return a list of human-readable failures (empty list == all checks pass)."""
    failures: list[str] = []
    with connect(db_path) as con:
        per_city = {
            row[0]: row
            for row in con.execute(
                """
                SELECT city,
                       count(*)                      AS n_rows,
                       count(DISTINCT observed_at)   AS n_distinct,
                       count(temperature_c)          AS n_temp,
                       min(temperature_c), max(temperature_c),
                       min(us_aqi), max(us_aqi)
                FROM raw.hourly_observations
                WHERE CAST(observed_at AS DATE) = CAST(? AS DATE)
                GROUP BY city
                """,
                [ds],
            ).fetchall()
        }

        for city in expected_cities:
            if city not in per_city:
                failures.append(f"{city}: no rows loaded for {ds}")
                continue
            _, n_rows, n_distinct, n_temp, t_min, t_max, aqi_min, aqi_max = per_city[city]
            if n_rows != EXPECTED_HOURS:
                failures.append(f"{city}: expected {EXPECTED_HOURS} rows, got {n_rows}")
            if n_distinct != n_rows:
                failures.append(f"{city}: duplicate hourly timestamps")
            if n_rows and (n_rows - n_temp) / n_rows > MAX_NULL_TEMP_RATIO:
                failures.append(f"{city}: too many null temperatures")
            if t_min is not None and not (TEMP_RANGE_C[0] <= t_min and t_max <= TEMP_RANGE_C[1]):
                failures.append(f"{city}: temperature outside {TEMP_RANGE_C} ({t_min}..{t_max})")
            if aqi_min is not None and not (AQI_RANGE[0] <= aqi_min and aqi_max <= AQI_RANGE[1]):
                failures.append(f"{city}: us_aqi outside {AQI_RANGE} ({aqi_min}..{aqi_max})")

        mart_cities = con.execute(
            "SELECT count(*) FROM marts.daily_city_summary WHERE obs_date = CAST(? AS DATE)",
            [ds],
        ).fetchone()[0]
        if mart_cities != len(expected_cities):
            failures.append(
                f"marts.daily_city_summary has {mart_cities} rows, expected {len(expected_cities)}"
            )
    return failures
