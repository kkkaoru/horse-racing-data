"""Fetch and store hourly weather data for every horse racing venue.

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

_HOURLY_VARS = (
    "weather_code,"
    "temperature_2m,"
    "precipitation,"
    "wind_speed_10m,"
    "wind_gusts_10m"
)
_TIMEZONE = "Asia/Tokyo"
_TIMEOUT_SEC = 30
_CHUNK_DAYS = 365
_SLEEP_SEC = 0.2

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS venue_weather (
    keibajo_code  VARCHAR NOT NULL,
    weather_date  DATE    NOT NULL,
    weather_hour  INTEGER NOT NULL,
    venue_name    VARCHAR NOT NULL,
    latitude      DOUBLE  NOT NULL,
    longitude     DOUBLE  NOT NULL,
    weather_code  INTEGER,
    temperature   DOUBLE,
    precipitation DOUBLE,
    wind_speed    DOUBLE,
    wind_gusts    DOUBLE,
    fetched_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (keibajo_code, weather_date, weather_hour)
)
"""

_UPSERT_SQL = """
INSERT INTO venue_weather
    (keibajo_code, weather_date, weather_hour, venue_name, latitude, longitude,
     weather_code, temperature, precipitation, wind_speed, wind_gusts, fetched_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (keibajo_code, weather_date, weather_hour) DO UPDATE SET
    venue_name    = excluded.venue_name,
    latitude      = excluded.latitude,
    longitude     = excluded.longitude,
    weather_code  = excluded.weather_code,
    temperature   = excluded.temperature,
    precipitation = excluded.precipitation,
    wind_speed    = excluded.wind_speed,
    wind_gusts    = excluded.wind_gusts,
    fetched_at    = excluded.fetched_at
"""


class VenueInfo(TypedDict):
    name: str
    lat: float
    lon: float


VENUE_COORDS: dict[str, VenueInfo] = {
    # JRA venues (keibajo_code 01–10) — racetrack coordinates
    "01": {"name": "札幌", "lat": 43.0776, "lon": 141.3240},
    "02": {"name": "函館", "lat": 41.7829, "lon": 140.7755},
    "03": {"name": "福島", "lat": 37.7651, "lon": 140.4804},
    "04": {"name": "新潟", "lat": 37.9489, "lon": 139.1858},
    "05": {"name": "東京", "lat": 35.6652, "lon": 139.4856},
    "06": {"name": "中山", "lat": 35.7266, "lon": 139.9594},
    "07": {"name": "中京", "lat": 35.0665, "lon": 136.9895},
    "08": {"name": "京都", "lat": 34.9070, "lon": 135.7246},
    "09": {"name": "阪神", "lat": 34.7811, "lon": 135.3630},
    "10": {"name": "小倉", "lat": 33.8432, "lon": 130.8746},
    # NAR venues — racetrack coordinates
    "30": {"name": "門別", "lat": 42.5380, "lon": 142.0046},
    "35": {"name": "盛岡", "lat": 39.6924, "lon": 141.2191},
    "36": {"name": "水沢", "lat": 39.1295, "lon": 141.1689},
    "42": {"name": "浦和", "lat": 35.8570, "lon": 139.6695},
    "43": {"name": "船橋", "lat": 35.6842, "lon": 139.9920},
    "44": {"name": "大井", "lat": 35.5935, "lon": 139.7434},
    "45": {"name": "川崎", "lat": 35.5335, "lon": 139.7103},
    "46": {"name": "金沢", "lat": 36.6368, "lon": 136.6729},
    "47": {"name": "笠松", "lat": 35.3728, "lon": 136.7663},
    "48": {"name": "名古屋", "lat": 35.0538, "lon": 136.7838},
    "50": {"name": "園田", "lat": 34.7655, "lon": 135.4449},
    "51": {"name": "姫路", "lat": 34.8551, "lon": 134.7011},
    "54": {"name": "高知", "lat": 33.5046, "lon": 133.5301},
    "55": {"name": "佐賀", "lat": 33.2683, "lon": 130.2852},
    "83": {"name": "帯広", "lat": 42.9211, "lon": 143.1822},  # Ban'ei
}


def build_url(lat: float, lon: float, start: date, end: date, *, archive: bool) -> str:
    base = OPEN_METEO_ARCHIVE_URL if archive else OPEN_METEO_FORECAST_URL
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": _HOURLY_VARS,
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


def _split_time(t: str) -> tuple[str, int]:
    dt = datetime.fromisoformat(t)
    return dt.date().isoformat(), dt.hour


def parse_hourly(raw: bytes) -> list[dict[str, object]]:
    outer: dict[str, object] = json.loads(raw)
    hourly = outer.get("hourly")
    if not isinstance(hourly, dict):
        return []
    times = hourly.get("time")
    if not isinstance(times, list):
        return []
    rows: list[dict[str, object]] = []
    for i, t in enumerate(times):
        d, h = _split_time(str(t))
        rows.append(
            {
                "date": d,
                "hour": h,
                "weather_code": _nth(hourly.get("weather_code"), i),
                "temperature": _nth(hourly.get("temperature_2m"), i),
                "precipitation": _nth(hourly.get("precipitation"), i),
                "wind_speed": _nth(hourly.get("wind_speed_10m"), i),
                "wind_gusts": _nth(hourly.get("wind_gusts_10m"), i),
            }
        )
    return rows


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
            row["hour"],
            venue["name"],
            venue["lat"],
            venue["lon"],
            row.get("weather_code"),
            row.get("temperature"),
            row.get("precipitation"),
            row.get("wind_speed"),
            row.get("wind_gusts"),
            fetched_at,
        )
        for row in rows
    ]


def _needs_migration(conn: duckdb.DuckDBPyConnection) -> bool:
    try:
        cols = {row[0] for row in conn.execute("DESCRIBE venue_weather").fetchall()}
        return "weather_hour" not in cols
    except duckdb.Error:
        return False


def open_db(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    if _needs_migration(conn):
        conn.execute("DROP TABLE IF EXISTS venue_weather")
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
    rows = parse_hourly(raw)
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
