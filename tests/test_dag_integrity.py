"""Requires apache-airflow (installed in CI and inside the Docker image)."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_structure(dagbag):
    dag = dagbag.dags.get("weather_air_quality_elt")
    assert dag is not None
    assert set(dag.task_ids) == {
        "list_cities",
        "extract",
        "load_raw",
        "build_marts",
        "quality_checks",
    }
    assert dag.get_task("load_raw").upstream_task_ids == {"extract"}
    assert dag.get_task("quality_checks").upstream_task_ids == {"build_marts"}
    assert dag.catchup is False and dag.max_active_runs == 1
