"""Extract: call the public APIs and land the untouched JSON in a raw zone."""

from __future__ import annotations

import json
import os
from pathlib import Path

import requests

from weather_pipeline import config


def fetch_json(url: str, params: dict) -> dict:
    response = requests.get(url, params=params, timeout=config.HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def extract_city(city: dict, ds: str, raw_dir: Path | None = None) -> str:
    """Fetch one city-day of weather + air quality and write it to the raw zone.

    The write is atomic (tmp file + rename) and the path is deterministic, so
    re-running a task for the same date simply overwrites the same file.
    """
    raw_dir = raw_dir or config.RAW_DIR
    common = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "start_date": ds,
        "end_date": ds,
        "timezone": "UTC",
    }
    weather = fetch_json(
        config.WEATHER_URL,
        {**common, "hourly": "temperature_2m,precipitation,wind_speed_10m"},
    )
    air_quality = fetch_json(
        config.AIR_QUALITY_URL,
        {**common, "hourly": "pm2_5,us_aqi"},
    )
    payload = {
        "city": city["name"],
        "ds": ds,
        "weather": weather,
        "air_quality": air_quality,
    }

    target = raw_dir / ds / f"{city['name']}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload))
    os.replace(tmp, target)
    return str(target)


def read_raw(path: str) -> dict:
    return json.loads(Path(path).read_text())
