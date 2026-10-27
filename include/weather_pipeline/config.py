"""Central configuration. Everything is overridable via environment variables."""

from __future__ import annotations

import os
from pathlib import Path

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
RAW_DIR = DATA_DIR / "raw"
WAREHOUSE_PATH = DATA_DIR / "warehouse.duckdb"

# Open-Meteo is free and needs no API key.
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
HTTP_TIMEOUT_SECONDS = 30

CITIES = [
    {"name": "phoenix", "lat": 33.4484, "lon": -112.0740},
    {"name": "london", "lat": 51.5074, "lon": -0.1278},
    {"name": "delhi", "lat": 28.6139, "lon": 77.2090},
    {"name": "sao_paulo", "lat": -23.5505, "lon": -46.6333},
    {"name": "tokyo", "lat": 35.6762, "lon": 139.6503},
    {"name": "nairobi", "lat": -1.2921, "lon": 36.8219},
]

# Entity resolution
REFERENCE_DIR = Path(__file__).parent / "reference"
FUZZY_THRESHOLD = 85.0  # minimum similarity (0-100) to accept a fuzzy match
AMBIGUITY_MARGIN = 5.0  # runner-up closer than this to the winner => ambiguous, not resolved
MIN_RESOLUTION_RATE = 0.8  # quality gate: fail the run if fewer sites resolve
