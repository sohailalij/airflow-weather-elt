"""
### Weather & Air Quality ELT

Daily pipeline that pulls hourly weather and air-quality data for several cities
from the free Open-Meteo APIs, lands raw JSON, loads it into DuckDB, builds an
analytics mart, and validates the result.

`list_cities -> extract (mapped per city) -> load_raw -> build_marts -> quality_checks`

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
from weather_pipeline.extract import extract_city, read_raw
from weather_pipeline.transform import flatten_payload
from weather_pipeline.warehouse import build_daily_summary, find_quality_failures, load_rows

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
    def load_raw(paths, ds: str | None = None) -> int:
        rows = []
        for path in paths:
            rows.extend(flatten_payload(read_raw(path)))
        loaded = load_rows(rows, ds)
        log.info("Loaded %d rows for %s", loaded, ds)
        return loaded

    @task
    def build_marts(_loaded: int, ds: str | None = None) -> int:
        return build_daily_summary(ds)

    @task(retries=0)  # data problems are not transient; fail fast
    def quality_checks(_mart_rows: int, ds: str | None = None) -> None:
        failures = find_quality_failures(ds, [c["name"] for c in CITIES])
        if failures:
            raise AirflowFailException("Data quality failed:\n" + "\n".join(failures))
        log.info("All data quality checks passed for %s", ds)

    raw_paths = extract.expand(city=list_cities())
    loaded = load_raw(raw_paths)
    mart_rows = build_marts(loaded)
    quality_checks(mart_rows)


weather_air_quality_elt()
