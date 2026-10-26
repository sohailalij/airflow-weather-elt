import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))


def make_payload(city: str, ds: str, temp: float = 20.0, aqi: int | None = 40) -> dict:
    times = [f"{ds}T{h:02d}:00" for h in range(24)]
    return {
        "city": city,
        "ds": ds,
        "weather": {
            "hourly": {
                "time": times,
                "temperature_2m": [temp + h * 0.1 for h in range(24)],
                "precipitation": [0.0] * 24,
                "wind_speed_10m": [10.0] * 24,
            }
        },
        "air_quality": {
            "hourly": {"time": times, "pm2_5": [8.0] * 24, "us_aqi": [aqi] * 24},
        },
    }
