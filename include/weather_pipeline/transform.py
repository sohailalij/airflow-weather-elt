"""Transform: flatten the nested API payload into typed, row-oriented records."""

from __future__ import annotations

from datetime import datetime


def flatten_payload(payload: dict) -> list[dict]:
    weather = payload["weather"]["hourly"]
    air = payload["air_quality"]["hourly"]

    times = weather["time"]
    for key in ("temperature_2m", "precipitation", "wind_speed_10m"):
        if len(weather[key]) != len(times):
            raise ValueError(f"{payload['city']}: '{key}' length does not match 'time'")

    air_by_time = {
        t: (pm, aqi) for t, pm, aqi in zip(air["time"], air["pm2_5"], air["us_aqi"], strict=True)
    }

    rows = []
    for i, t in enumerate(times):
        pm2_5, us_aqi = air_by_time.get(t, (None, None))
        rows.append(
            {
                "city": payload["city"],
                "observed_at": datetime.fromisoformat(t),
                "temperature_c": weather["temperature_2m"][i],
                "precipitation_mm": weather["precipitation"][i],
                "wind_speed_kmh": weather["wind_speed_10m"][i],
                "pm2_5": pm2_5,
                "us_aqi": int(us_aqi) if us_aqi is not None else None,
            }
        )
    return rows
