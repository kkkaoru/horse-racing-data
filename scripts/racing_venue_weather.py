"""Fetch and store hourly weather data for every horse racing venue.

Usage:
    # Backfill historical data for all venues
    uv run python racing_venue_weather.py --mode backfill --start 2013-01-01 --end 2026-12-31

    # Fetch today's weather for all venues (daily cron)
    uv run python racing_venue_weather.py --mode daily

    # Custom storage directory (year-sharded files are created inside)
    uv run python racing_venue_weather.py --mode daily --db-dir /data/weather

Data is stored in year-sharded DuckDB files:
    <db-dir>/venue_weather_2013.duckdb
    <db-dir>/venue_weather_2014.duckdb
    ...
Each file holds one calendar year of hourly data (~9 MB), well under
GitHub's 100 MB per-file limit.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from collections.abc import Sequence
from typing import TypedDict
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import duckdb

logger = logging.getLogger(__name__)

DEFAULT_DB_DIR = Path.home() / ".horse-racing"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_VENUE_COORDS_PATH = Path(__file__).parent / "venue_coords.json"

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


def _load_venue_coords() -> dict[str, VenueInfo]:
    raw: dict[str, VenueInfo] = json.loads(
        _VENUE_COORDS_PATH.read_text(encoding="utf-8")
    )
    return raw


VENUE_COORDS: dict[str, VenueInfo] = _load_venue_coords()


def _year_db_path(year: int, db_dir: Path) -> Path:
    return db_dir / f"venue_weather_{year}.duckdb"


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
    records: Sequence[tuple[object, ...]],
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


def run_backfill(start: date, end: date, db_dir: Path) -> None:
    fetched_at = datetime.now(timezone.utc).isoformat()
    year = start.year
    while year <= end.year:
        year_start = max(start, date(year, 1, 1))
        year_end = min(end, date(year, 12, 31))
        conn = open_db(_year_db_path(year, db_dir))
        try:
            current = year_start
            while current <= year_end:
                chunk_end = min(current + timedelta(days=_CHUNK_DAYS - 1), year_end)
                _sync_all_venues(
                    conn, current, chunk_end, archive=True, fetched_at=fetched_at
                )
                current = chunk_end + timedelta(days=1)
        finally:
            conn.close()
        year += 1


def run_daily(db_dir: Path, *, today: date | None = None) -> None:
    actual_today = today if today is not None else date.today()
    conn = open_db(_year_db_path(actual_today.year, db_dir))
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
        "--db-dir",
        default=str(DEFAULT_DB_DIR),
        metavar="DIR",
        help=f"Directory for year-sharded DuckDB files (default: {DEFAULT_DB_DIR})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = _parse_args(argv)
    db_dir = Path(args.db_dir)
    if args.mode == "daily":
        run_daily(db_dir)
        return 0
    dates = _parse_date_range(args.start, args.end)
    if dates is None:
        return 1
    run_backfill(dates[0], dates[1], db_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
