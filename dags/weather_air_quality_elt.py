"""
### Weather & Air Quality ELT

Daily pipeline that pulls hourly weather and air-quality data for several cities
from the free Open-Meteo APIs, lands raw JSON, loads it into DuckDB, builds an
analytics mart, and validates the result.

Flow: `list_cities` feeds two parallel branches, `extract` (one mapped task per city)
and `resolve_sites` (entity resolution). Both must finish before
`load_raw -> build_marts -> quality_checks`.

`resolve_sites` maps messy site-inventory locations ("Londn", "Delhi NCR", "PHX") to
canonical cities with alias + fuzzy matching, then assigns owner, business unit and
criticality tier from version-controlled YAML rules. Unresolvable records are flagged,
never guessed.

* Idempotent: every task can be re-run for any date without creating duplicates.
* Backfill: `airflow dags backfill -s 2026-10-01 -e 2026-10-07 weather_air_quality_elt`
  (the forecast API serves roughly the last 92 days).
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from weather_pipeline.config import CITIES
from weather_pipeline.entity_resolution import load_inventory, load_yaml, resolve_inventory
from weather_pipeline.extract import extract_city, read_raw
from weather_pipeline.transform import flatten_payload
from weather_pipeline.warehouse import (
    build_daily_summary,
    build_site_environment,
    find_quality_failures,
    find_resolution_failures,
    load_rows,
    load_site_resolutions,
)

log = logging.getLogger(__name__)


def notify_failure(context: dict) -> None:
    """Hook for Slack/email alerts. Logging keeps the project zero-cost."""
    ti = context["task_instance"]
    log.error("Task failed: dag=%s task=%s run=%s", ti.dag_id, ti.task_id, context["run_id"])


@dag(
    dag_id="weather_air_quality_elt",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "on_failure_callback": notify_failure,
    },
    tags=["portfolio", "elt", "duckdb", "open-meteo"],
    doc_md=__doc__,
)
def weather_air_quality_elt():
    @task
    def list_cities() -> list[dict]:
        return CITIES

    @task(max_active_tis_per_dag=3)  # be polite to the free API
    def extract(city: dict, ds: str | None = None) -> str:
        return extract_city(city, ds)

    @task
    def resolve_sites(cities: list[dict]) -> int:
        records = resolve_inventory(
            load_inventory(),
            [c["name"] for c in cities],
            load_yaml("city_aliases.yaml"),
            load_yaml("ownership_rules.yaml"),
        )
        for r in records:
            if r["status"] != "resolved":
                log.warning(
                    "Site %s %s: raw_location=%r (best score %.1f)",
                    r["site_id"],
                    r["status"],
                    r["raw_location"],
                    r["match_score"],
                )
        return load_site_resolutions(records)

    @task
    def load_raw(paths, _sites_loaded: int, ds: str | None = None) -> int:
        rows = []
        for path in paths:
            rows.extend(flatten_payload(read_raw(path)))
        loaded = load_rows(rows, ds)
        log.info("Loaded %d rows for %s", loaded, ds)
        return loaded

    @task
    def build_marts(_loaded: int, ds: str | None = None) -> int:
        city_rows = build_daily_summary(ds)
        site_rows = build_site_environment(ds)
        log.info("Built %d city rows and %d site rows for %s", city_rows, site_rows, ds)
        return city_rows

    @task(retries=0)  # data problems are not transient; fail fast
    def quality_checks(_mart_rows: int, ds: str | None = None) -> None:
        failures = find_quality_failures(ds, [c["name"] for c in CITIES])
        failures += find_resolution_failures(ds)
        if failures:
            raise AirflowFailException("Data quality failed:\n" + "\n".join(failures))
        log.info("All data quality checks passed for %s", ds)

    cities = list_cities()
    raw_paths = extract.expand(city=cities)
    sites = resolve_sites(cities)
    loaded = load_raw(raw_paths, sites)
    mart_rows = build_marts(loaded)
    quality_checks(mart_rows)


weather_air_quality_elt()
