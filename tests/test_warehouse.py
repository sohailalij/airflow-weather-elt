from conftest import make_payload
from weather_pipeline import warehouse
from weather_pipeline.transform import flatten_payload

DS = "2026-10-20"
CITIES = ["london", "tokyo"]


def _rows(cities=CITIES, **kwargs):
    out = []
    for c in cities:
        out.extend(flatten_payload(make_payload(c, DS, **kwargs)))
    return out


def test_load_is_idempotent(tmp_path):
    db = tmp_path / "wh.duckdb"
    warehouse.load_rows(_rows(), DS, db)
    warehouse.load_rows(_rows(), DS, db)  # second run must not duplicate
    with warehouse.connect(db) as con:
        assert con.execute("SELECT count(*) FROM raw.hourly_observations").fetchone()[0] == 48


def test_mart_and_quality_pass(tmp_path):
    db = tmp_path / "wh.duckdb"
    warehouse.load_rows(_rows(), DS, db)
    assert warehouse.build_daily_summary(DS, db) == 2
    assert warehouse.find_quality_failures(DS, CITIES, db) == []
    with warehouse.connect(db) as con:
        cat = con.execute("SELECT aqi_category FROM marts.daily_city_summary LIMIT 1").fetchone()[0]
    assert cat == "good"


def test_quality_catches_missing_city_and_bad_values(tmp_path):
    db = tmp_path / "wh.duckdb"
    rows = _rows(["london"], temp=200.0)  # impossible temperature
    warehouse.load_rows(rows, DS, db)
    warehouse.build_daily_summary(DS, db)
    failures = warehouse.find_quality_failures(DS, CITIES, db)
    assert any("tokyo: no rows" in f for f in failures)
    assert any("london: temperature outside" in f for f in failures)
