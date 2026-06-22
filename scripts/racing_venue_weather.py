"""Fetch and store daily weather data for every horse racing venue.

Usage:
    # Backfill historical data for all venues
    uv run python racing_venue_weather.py --mode backfill --start 2020-01-01 --end 2025-12-31

    # Fetch today's weather for all venues (daily cron)
    uv run python racing_venue_weather.py --mode daily

    # Custom DuckDB path
    uv run python racing_venue_weather.py --mode daily --db-path /data/venue_weather.duckdb
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TypedDict
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import duckdb

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path.home() / ".horse-racing" / "venue_weather.duckdb"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

_DAILY_VARS = (
    "weather_code,"
    "temperature_2m_max,"
    "temperature_2m_min,"
    "precipitation_sum,"
    "wind_speed_10m_max,"
    "wind_gusts_10m_max"
)
_TIMEZONE = "Asia/Tokyo"
_TIMEOUT_SEC = 30
_CHUNK_DAYS = 365
_SLEEP_SEC = 0.2

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS venue_weather (
    keibajo_code VARCHAR NOT NULL,
    weather_date  DATE    NOT NULL,
    venue_name    VARCHAR NOT NULL,
    latitude      DOUBLE  NOT NULL,
    longitude     DOUBLE  NOT NULL,
    weather_code      INTEGER,
    temperature_max   DOUBLE,
    temperature_min   DOUBLE,
    precipitation_sum DOUBLE,
    wind_speed_max    DOUBLE,
    wind_gusts_max    DOUBLE,
    fetched_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (keibajo_code, weather_date)
)
"""

_UPSERT_SQL = """
INSERT INTO venue_weather
    (keibajo_code, weather_date, venue_name, latitude, longitude,
     weather_code, temperature_max, temperature_min,
     precipitation_sum, wind_speed_max, wind_gusts_max, fetched_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (keibajo_code, weather_date) DO UPDATE SET
    venue_name        = excluded.venue_name,
    latitude          = excluded.latitude,
    longitude         = excluded.longitude,
    weather_code      = excluded.weather_code,
    temperature_max   = excluded.temperature_max,
    temperature_min   = excluded.temperature_min,
    precipitation_sum = excluded.precipitation_sum,
    wind_speed_max    = excluded.wind_speed_max,
    wind_gusts_max    = excluded.wind_gusts_max,
    fetched_at        = excluded.fetched_at
"""


class VenueInfo(TypedDict):
    name: str
    lat: float
    lon: float


VENUE_COORDS: dict[str, VenueInfo] = {
    # JRA venues (keibajo_code 01–10)
    "01": {"name": "札幌", "lat": 43.0437, "lon": 141.4041},
    "02": {"name": "函館", "lat": 41.7684, "lon": 140.7288},
    "03": {"name": "福島", "lat": 37.7501, "lon": 140.4677},
    "04": {"name": "新潟", "lat": 37.9161, "lon": 139.0556},
    "05": {"name": "東京", "lat": 35.6894, "lon": 139.4990},
    "06": {"name": "中山", "lat": 35.7738, "lon": 139.9280},
    "07": {"name": "中京", "lat": 35.0825, "lon": 136.9987},
    "08": {"name": "京都", "lat": 34.9420, "lon": 135.6893},
    "09": {"name": "阪神", "lat": 34.7271, "lon": 135.3709},
    "10": {"name": "小倉", "lat": 33.8594, "lon": 130.8745},
    # NAR venues
    "30": {"name": "門別", "lat": 42.3838, "lon": 141.9897},
    "35": {"name": "盛岡", "lat": 39.7038, "lon": 141.1648},
    "36": {"name": "水沢", "lat": 39.2614, "lon": 141.1301},
    "42": {"name": "浦和", "lat": 35.8641, "lon": 139.6476},
    "43": {"name": "船橋", "lat": 35.6916, "lon": 140.0025},
    "44": {"name": "大井", "lat": 35.6128, "lon": 139.7378},
    "45": {"name": "川崎", "lat": 35.4956, "lon": 139.7211},
    "46": {"name": "金沢", "lat": 36.6063, "lon": 136.6232},
    "47": {"name": "笠松", "lat": 35.3791, "lon": 136.7652},
    "48": {"name": "名古屋", "lat": 35.1600, "lon": 136.9120},
    "50": {"name": "園田", "lat": 34.7561, "lon": 135.4012},
    "51": {"name": "姫路", "lat": 34.8278, "lon": 134.6892},
    "54": {"name": "高知", "lat": 33.5561, "lon": 133.5308},
    "55": {"name": "佐賀", "lat": 33.2685, "lon": 130.2967},
    "83": {"name": "帯広", "lat": 42.9204, "lon": 143.1968},  # Ban'ei
}


def build_url(lat: float, lon: float, start: date, end: date, *, archive: bool) -> str:
    base = OPEN_METEO_ARCHIVE_URL if archive else OPEN_METEO_FORECAST_URL
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": _DAILY_VARS,
        "timezone": _TIMEZONE,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }
    return f"{base}?{urlencode(params)}"


def fetch_raw(url: str) -> bytes:
    with urlopen(url, timeout=_TIMEOUT_SEC) as resp:
        return resp.read()


def _nth(values: object, i: int) -> object | None:
    if not isinstance(values, list) or i >= len(values):
        return None
    return values[i]


def parse_daily(raw: bytes) -> list[dict[str, object]]:
    outer: dict[str, object] = json.loads(raw)
    daily = outer.get("daily")
    if not isinstance(daily, dict):
        return []
    times = daily.get("time")
    if not isinstance(times, list):
        return []
    return [
        {
            "date": t,
            "weather_code": _nth(daily.get("weather_code"), i),
            "temperature_max": _nth(daily.get("temperature_2m_max"), i),
            "temperature_min": _nth(daily.get("temperature_2m_min"), i),
            "precipitation_sum": _nth(daily.get("precipitation_sum"), i),
            "wind_speed_max": _nth(daily.get("wind_speed_10m_max"), i),
            "wind_gusts_max": _nth(daily.get("wind_gusts_10m_max"), i),
        }
        for i, t in enumerate(times)
    ]


def build_records(
    keibajo_code: str,
    venue: VenueInfo,
    rows: list[dict[str, object]],
    fetched_at: str,
) -> list[tuple[object, ...]]:
    return [
        (
            keibajo_code,
            row["date"],
            venue["name"],
            venue["lat"],
            venue["lon"],
            row.get("weather_code"),
            row.get("temperature_max"),
            row.get("temperature_min"),
            row.get("precipitation_sum"),
            row.get("wind_speed_max"),
            row.get("wind_gusts_max"),
            fetched_at,
        )
        for row in rows
    ]


def open_db(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute(_CREATE_SQL)
    return conn


def upsert_records(
    conn: duckdb.DuckDBPyConnection,
    records: list[tuple[object, ...]],
) -> int:
    if not records:
        return 0
    conn.executemany(_UPSERT_SQL, records)
    return len(records)


def fetch_venue(
    keibajo_code: str,
    venue: VenueInfo,
    start: date,
    end: date,
    *,
    archive: bool,
    fetched_at: str,
) -> list[tuple[object, ...]]:
    url = build_url(venue["lat"], venue["lon"], start, end, archive=archive)
    raw = fetch_raw(url)
    rows = parse_daily(raw)
    return build_records(keibajo_code, venue, rows, fetched_at)


def _sync_all_venues(
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    *,
    archive: bool,
    fetched_at: str,
) -> None:
    for keibajo_code, venue in VENUE_COORDS.items():
        try:
            records = fetch_venue(
                keibajo_code,
                venue,
                start,
                end,
                archive=archive,
                fetched_at=fetched_at,
            )
            n = upsert_records(conn, records)
            logger.info("Saved %d rows: %s (%s–%s)", n, venue["name"], start, end)
        except (URLError, OSError, KeyError, ValueError):
            logger.exception("Failed: %s (%s–%s)", venue["name"], start, end)
        time.sleep(_SLEEP_SEC)


def run_backfill(start: date, end: date, db_path: Path) -> None:
    conn = open_db(db_path)
    try:
        fetched_at = datetime.now(timezone.utc).isoformat()
        current = start
        while current <= end:
            chunk_end = min(current + timedelta(days=_CHUNK_DAYS - 1), end)
            _sync_all_venues(
                conn, current, chunk_end, archive=True, fetched_at=fetched_at
            )
            current = chunk_end + timedelta(days=1)
    finally:
        conn.close()


def run_daily(db_path: Path, *, today: date | None = None) -> None:
    actual_today = today if today is not None else date.today()
    conn = open_db(db_path)
    try:
        fetched_at = datetime.now(timezone.utc).isoformat()
        _sync_all_venues(
            conn, actual_today, actual_today, archive=False, fetched_at=fetched_at
        )
    finally:
        conn.close()


def _parse_date_range(
    start_str: str | None, end_str: str | None
) -> tuple[date, date] | None:
    if not start_str or not end_str:
        logger.error("--start and --end are required for backfill mode")
        return None
    try:
        start = date.fromisoformat(start_str)
        end = date.fromisoformat(end_str)
    except ValueError as exc:
        logger.error("Invalid date: %s", exc)
        return None
    if start > end:
        logger.error("--start must be <= --end")
        return None
    return start, end


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch horse racing venue weather data (Open-Meteo)"
    )
    parser.add_argument(
        "--mode",
        choices=["backfill", "daily"],
        required=True,
        help="backfill: historical range; daily: today's forecast",
    )
    parser.add_argument("--start", metavar="YYYY-MM-DD", help="Backfill start date")
    parser.add_argument("--end", metavar="YYYY-MM-DD", help="Backfill end date")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        metavar="PATH",
        help=f"DuckDB file path (default: {DEFAULT_DB_PATH})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = _parse_args(argv)
    db_path = Path(args.db_path)
    if args.mode == "daily":
        run_daily(db_path)
        return 0
    dates = _parse_date_range(args.start, args.end)
    if dates is None:
        return 1
    run_backfill(dates[0], dates[1], db_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
