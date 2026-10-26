import pytest
from conftest import make_payload
from weather_pipeline.transform import flatten_payload


def test_flatten_produces_24_typed_rows():
    rows = flatten_payload(make_payload("london", "2026-10-20"))
    assert len(rows) == 24
    assert rows[0]["city"] == "london"
    assert rows[0]["observed_at"].hour == 0
    assert isinstance(rows[0]["us_aqi"], int)


def test_missing_air_quality_becomes_null():
    payload = make_payload("delhi", "2026-10-20", aqi=None)
    rows = flatten_payload(payload)
    assert all(r["us_aqi"] is None for r in rows)


def test_length_mismatch_raises():
    payload = make_payload("tokyo", "2026-10-20")
    payload["weather"]["hourly"]["precipitation"].pop()
    with pytest.raises(ValueError):
        flatten_payload(payload)
