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


def _site_records():
    from weather_pipeline import entity_resolution as er

    return er.resolve_inventory(
        er.load_inventory(),
        ["london", "tokyo", "phoenix", "delhi", "sao_paulo", "nairobi"],
        er.load_yaml("city_aliases.yaml"),
        er.load_yaml("ownership_rules.yaml"),
    )


def test_site_attribution_end_to_end(tmp_path):
    db = tmp_path / "wh.duckdb"
    cities = ["phoenix", "london", "delhi", "sao_paulo", "tokyo", "nairobi"]
    warehouse.load_rows(_rows(cities), DS, db)
    warehouse.build_daily_summary(DS, db)
    warehouse.load_site_resolutions(_site_records(), db)
    assert warehouse.build_site_environment(DS, db) == 11  # 13 sites, 2 not resolvable
    assert warehouse.find_resolution_failures(DS, db_path=db) == []
    with warehouse.connect(db) as con:
        row = con.execute(
            "SELECT city, business_unit, owner_team, criticality_tier "
            "FROM marts.site_environment_daily WHERE site_id = 'S004'"
        ).fetchone()
    assert row == ("london", "Operations", "site-reliability", 1)  # 'Londn' typo resolved


def test_site_attribution_is_idempotent(tmp_path):
    db = tmp_path / "wh.duckdb"
    cities = ["phoenix", "london", "delhi", "sao_paulo", "tokyo", "nairobi"]
    warehouse.load_rows(_rows(cities), DS, db)
    warehouse.build_daily_summary(DS, db)
    for _ in range(3):
        warehouse.load_site_resolutions(_site_records(), db)
        warehouse.build_site_environment(DS, db)
    with warehouse.connect(db) as con:
        assert con.execute("SELECT count(*) FROM ref.site_resolution").fetchone()[0] == 13
        assert con.execute("SELECT count(*) FROM marts.site_environment_daily").fetchone()[0] == 11


def test_resolution_gate_fails_when_rate_drops(tmp_path):
    db = tmp_path / "wh.duckdb"
    records = _site_records()
    for r in records[:6]:  # simulate a broken upstream feed: most sites stop resolving
        r.update(resolved_city=None, status="unresolved", match_method="none", match_score=0.0)
    warehouse.load_site_resolutions(records, db)
    failures = warehouse.find_resolution_failures(DS, db_path=db)
    assert any("resolution rate" in f for f in failures)
