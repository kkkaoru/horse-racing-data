"""Synchronize Cloudflare R2 SQL venue weather into local yearly DuckDB files."""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final, TypeGuard, TypeVar

import duckdb

RowT = TypeVar("RowT")

ACCOUNT_ID: Final[str] = "78109ec18c7c85b194b19fb32e3bb149"
BUCKET_NAME: Final[str] = "pc-keiba-venue-weather-archive"
WAREHOUSE: Final[str] = f"{ACCOUNT_ID}_{BUCKET_NAME}"
TABLE_NAME: Final[str] = "weather.venue_weather_hourly"
TABLE_NAME_V2: Final[str] = "weather.venue_weather_hourly_v2"
R2_SQL_ENDPOINT: Final[str] = (
    "https://api.sql.cloudflarestorage.com/api/v1/accounts/"
    f"{ACCOUNT_ID}/r2-sql/query/{BUCKET_NAME}"
)
DEFAULT_LOOKBACK_DAYS: Final[int] = 7
DEFAULT_RETRIES: Final[int] = 3
REQUEST_TIMEOUT_SECONDS: Final[float] = 60.0
RETRY_BASE_SECONDS: Final[float] = 1.0

CREATE_TABLE_SQL: Final[str] = """
create table if not exists venue_weather (
  keibajo_code varchar not null,
  weather_date date not null,
  weather_hour integer not null,
  venue_name varchar not null,
  latitude double not null,
  longitude double not null,
  weather_code integer,
  temperature double,
  precipitation double,
  wind_speed double,
  wind_gusts double,
  fetched_at timestamptz not null,
  primary key (keibajo_code, weather_date, weather_hour)
)
"""

CREATE_TABLE_V2_SQL: Final[str] = """
create table if not exists venue_weather_v2 (
  keibajo_code varchar not null,
  weather_date date not null,
  weather_hour integer not null,
  venue_name varchar not null,
  latitude double not null,
  longitude double not null,
  weather_code integer,
  temperature double,
  precipitation double,
  wind_speed double,
  wind_gusts double,
  relative_humidity double not null,
  dew_point double not null,
  wet_bulb_temperature double not null,
  shortwave_radiation double not null,
  fetched_at timestamptz not null,
  primary key (keibajo_code, weather_date, weather_hour)
)
"""


@dataclass(frozen=True, slots=True)
class WeatherRow:
    """Validated latest Cloudflare weather value for one venue-hour key."""

    fetched_at: str
    keibajo_code: str
    latitude: float
    longitude: float
    precipitation: float | None
    temperature: float | None
    venue_name: str
    weather_code: int | None
    weather_date: str
    weather_hour: int
    wind_gusts: float | None
    wind_speed: float | None

    def as_sql_parameters(self) -> tuple[object, ...]:
        return (
            self.keibajo_code,
            self.weather_date,
            self.weather_hour,
            self.venue_name,
            self.latitude,
            self.longitude,
            self.weather_code,
            self.temperature,
            self.precipitation,
            self.wind_speed,
            self.wind_gusts,
            self.fetched_at,
        )


@dataclass(frozen=True, slots=True)
class WeatherV2Row:
    """Validated latest Cloudflare v2 value for one venue-hour key."""

    dew_point: float
    fetched_at: str
    keibajo_code: str
    latitude: float
    longitude: float
    precipitation: float | None
    relative_humidity: float
    shortwave_radiation: float
    temperature: float | None
    venue_name: str
    weather_code: int | None
    weather_date: str
    weather_hour: int
    wet_bulb_temperature: float
    wind_gusts: float | None
    wind_speed: float | None

    def as_sql_parameters(self) -> tuple[object, ...]:
        return (
            self.keibajo_code,
            self.weather_date,
            self.weather_hour,
            self.venue_name,
            self.latitude,
            self.longitude,
            self.weather_code,
            self.temperature,
            self.precipitation,
            self.wind_speed,
            self.wind_gusts,
            self.relative_humidity,
            self.dew_point,
            self.wet_bulb_temperature,
            self.shortwave_radiation,
            self.fetched_at,
        )


@dataclass(frozen=True, slots=True)
class SyncSummary:
    """Observed local changes produced by one synchronization run."""

    dates: int
    inserted: int
    rows_from_cloudflare: int
    unchanged: int
    updated: int


@dataclass(frozen=True, slots=True)
class DualSyncSummary:
    """Observed local changes for the stable v1 and additive v2 schemas."""

    v1: SyncSummary
    v2: SyncSummary


def parse_date(value: str) -> date:
    """Parse an ISO calendar date or raise an explicit argument error."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def date_range(start: date, end: date) -> Iterator[date]:
    """Yield every date in the inclusive ordered range."""
    if end < start:
        raise ValueError("to-date must be on or after from-date")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def build_latest_weather_query(target_date: date) -> str:
    """Return a bounded query selecting one preferred row per venue-hour."""
    value = target_date.isoformat()
    return f"""WITH ranked AS (
  SELECT
    race_date,
    keibajo_code,
    weather_hour,
    venue_name,
    latitude,
    longitude,
    weather_code,
    temperature,
    precipitation,
    wind_speed,
    wind_gusts,
    cast(fetched_at AS VARCHAR) AS fetched_at,
    row_number() OVER (
      PARTITION BY race_date, keibajo_code, weather_hour
      ORDER BY CASE WHEN weather_data_type = 'actual' THEN 0 ELSE 1 END,
               fetched_at DESC
    ) AS row_number
  FROM {TABLE_NAME}
  WHERE race_date = '{value}'
)
SELECT
  race_date,
  keibajo_code,
  weather_hour,
  venue_name,
  latitude,
  longitude,
  weather_code,
  temperature,
  precipitation,
  wind_speed,
  wind_gusts,
  fetched_at
FROM ranked
WHERE row_number = 1
ORDER BY keibajo_code, weather_hour"""


def build_latest_weather_v2_query(target_date: date) -> str:
    """Return a bounded v2 query selecting one preferred row per venue-hour."""
    value = target_date.isoformat()
    return f"""WITH ranked AS (
  SELECT
    race_date,
    keibajo_code,
    weather_hour,
    venue_name,
    latitude,
    longitude,
    weather_code,
    temperature,
    precipitation,
    wind_speed,
    wind_gusts,
    relative_humidity,
    dew_point,
    wet_bulb_temperature,
    shortwave_radiation,
    cast(fetched_at AS VARCHAR) AS fetched_at,
    row_number() OVER (
      PARTITION BY race_date, keibajo_code, weather_hour
      ORDER BY CASE WHEN weather_data_type = 'actual' THEN 0 ELSE 1 END,
               fetched_at DESC
    ) AS row_number
  FROM {TABLE_NAME_V2}
  WHERE race_date = '{value}'
)
SELECT
  race_date,
  keibajo_code,
  weather_hour,
  venue_name,
  latitude,
  longitude,
  weather_code,
  temperature,
  precipitation,
  wind_speed,
  wind_gusts,
  relative_humidity,
  dew_point,
  wet_bulb_temperature,
  shortwave_radiation,
  fetched_at
FROM ranked
WHERE row_number = 1
ORDER BY keibajo_code, weather_hour"""


def is_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def required_string(row: Mapping[object, object], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"R2 SQL row is missing {key}")
    return value


def required_float(row: Mapping[object, object], key: str) -> float:
    value = row.get(key)
    if not isinstance(value, int | float):
        raise TypeError(f"R2 SQL row is missing {key}")
    return float(value)


def optional_float(row: Mapping[object, object], key: str) -> float | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, int | float):
        raise TypeError(f"R2 SQL row has invalid {key}")
    return float(value)


def optional_int(row: Mapping[object, object], key: str) -> int | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"R2 SQL row has invalid {key}")
    return value


def parse_weather_row(value: object) -> WeatherRow:
    """Validate one untrusted R2 SQL JSON row."""
    if not is_mapping(value):
        raise ValueError("R2 SQL row is not an object")
    hour = value.get("weather_hour")
    if not isinstance(hour, int) or isinstance(hour, bool) or hour not in range(24):
        raise ValueError("R2 SQL row has invalid weather_hour")
    weather_date = required_string(value, "race_date")
    parse_date(weather_date)
    return WeatherRow(
        fetched_at=required_string(value, "fetched_at"),
        keibajo_code=required_string(value, "keibajo_code"),
        latitude=required_float(value, "latitude"),
        longitude=required_float(value, "longitude"),
        precipitation=optional_float(value, "precipitation"),
        temperature=optional_float(value, "temperature"),
        venue_name=required_string(value, "venue_name"),
        weather_code=optional_int(value, "weather_code"),
        weather_date=weather_date,
        weather_hour=hour,
        wind_gusts=optional_float(value, "wind_gusts"),
        wind_speed=optional_float(value, "wind_speed"),
    )


def parse_weather_v2_row(value: object) -> WeatherV2Row:
    """Validate one untrusted R2 SQL v2 JSON row without filling metrics."""
    if not is_mapping(value):
        raise ValueError("R2 SQL v2 row is not an object")
    hour = value.get("weather_hour")
    if not isinstance(hour, int) or isinstance(hour, bool) or hour not in range(24):
        raise ValueError("R2 SQL v2 row has invalid weather_hour")
    weather_date = required_string(value, "race_date")
    parse_date(weather_date)
    return WeatherV2Row(
        dew_point=required_float(value, "dew_point"),
        fetched_at=required_string(value, "fetched_at"),
        keibajo_code=required_string(value, "keibajo_code"),
        latitude=required_float(value, "latitude"),
        longitude=required_float(value, "longitude"),
        precipitation=optional_float(value, "precipitation"),
        relative_humidity=required_float(value, "relative_humidity"),
        shortwave_radiation=required_float(value, "shortwave_radiation"),
        temperature=optional_float(value, "temperature"),
        venue_name=required_string(value, "venue_name"),
        weather_code=optional_int(value, "weather_code"),
        weather_date=weather_date,
        weather_hour=hour,
        wet_bulb_temperature=required_float(value, "wet_bulb_temperature"),
        wind_gusts=optional_float(value, "wind_gusts"),
        wind_speed=optional_float(value, "wind_speed"),
    )


def parse_response_rows(payload: object) -> list[WeatherRow]:
    """Validate the Cloudflare envelope and return its typed rows."""
    if not is_mapping(payload) or payload.get("success") is not True:
        raise ValueError("R2 SQL query failed")
    result = payload.get("result")
    if not is_mapping(result):
        raise ValueError("R2 SQL response has no result object")
    raw_rows = result.get("rows")
    if not isinstance(raw_rows, list):
        raise TypeError("R2 SQL response has no rows list")
    return [parse_weather_row(row) for row in raw_rows]


def parse_v2_response_rows(payload: object) -> list[WeatherV2Row]:
    """Validate the Cloudflare v2 envelope and return its typed rows."""
    if not is_mapping(payload) or payload.get("success") is not True:
        raise ValueError("R2 SQL v2 query failed")
    result = payload.get("result")
    if not is_mapping(result):
        raise ValueError("R2 SQL v2 response has no result object")
    raw_rows = result.get("rows")
    if not isinstance(raw_rows, list):
        raise TypeError("R2 SQL v2 response has no rows list")
    return [parse_weather_v2_row(row) for row in raw_rows]


def query_cloudflare_rows(
    query: str,
    token: str,
    parser: Callable[[object], list[RowT]],
    error_label: str,
    *,
    retries: int = DEFAULT_RETRIES,
) -> list[RowT]:
    """Execute one typed retried R2 SQL request."""
    body = json.dumps({"query": query, "warehouse": WAREHOUSE}).encode("utf-8")
    request = urllib.request.Request(
        R2_SQL_ENDPOINT,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "horse-racing-data-weather-sync/1.0",
        },
        method="POST",
    )
    last_error: BaseException | None = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(
                request, timeout=REQUEST_TIMEOUT_SECONDS
            ) as response:
                raw = response.read().decode("utf-8")
            return parser(json.loads(raw))
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError,
        ) as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(RETRY_BASE_SECONDS * (2**attempt))
    raise RuntimeError(f"{error_label} after {retries} attempts") from last_error


def query_cloudflare(
    query: str,
    token: str,
    *,
    retries: int = DEFAULT_RETRIES,
) -> list[WeatherRow]:
    """Execute a retried R2 SQL v1 request and validate the response."""
    return query_cloudflare_rows(
        query,
        token,
        parse_response_rows,
        "R2 SQL request failed",
        retries=retries,
    )


def query_cloudflare_v2(
    query: str,
    token: str,
    *,
    retries: int = DEFAULT_RETRIES,
) -> list[WeatherV2Row]:
    """Execute a retried R2 SQL v2 request and validate the response."""
    return query_cloudflare_rows(
        query,
        token,
        parse_v2_response_rows,
        "R2 SQL v2 request failed",
        retries=retries,
    )


def count_value(connection: duckdb.DuckDBPyConnection, query: str) -> int:
    row = connection.execute(query).fetchone()
    if row is None or not isinstance(row[0], int):
        raise RuntimeError("DuckDB count query returned no integer")
    return row[0]


def upsert_weather_rows(
    connection: duckdb.DuckDBPyConnection, rows: Sequence[WeatherRow]
) -> tuple[int, int, int]:
    """Upsert validated rows and return inserted, updated, unchanged counts."""
    connection.execute(CREATE_TABLE_SQL)
    if not rows:
        return 0, 0, 0
    connection.execute(
        "create or replace temp table incoming as select * from venue_weather limit 0"
    )
    connection.executemany(
        "insert into incoming values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [row.as_sql_parameters() for row in rows],
    )
    join = """
      existing.keibajo_code = incoming.keibajo_code
      and existing.weather_date = incoming.weather_date
      and existing.weather_hour = incoming.weather_hour
    """
    same = """
      existing.venue_name is not distinct from incoming.venue_name
      and existing.latitude is not distinct from incoming.latitude
      and existing.longitude is not distinct from incoming.longitude
      and existing.weather_code is not distinct from incoming.weather_code
      and existing.temperature is not distinct from incoming.temperature
      and existing.precipitation is not distinct from incoming.precipitation
      and existing.wind_speed is not distinct from incoming.wind_speed
      and existing.wind_gusts is not distinct from incoming.wind_gusts
      and existing.fetched_at is not distinct from incoming.fetched_at
    """
    inserted = count_value(
        connection,
        f"""select count(*) from incoming
        left join venue_weather existing on {join}
        where existing.keibajo_code is null""",
    )
    unchanged = count_value(
        connection,
        f"""select count(*) from incoming
        join venue_weather existing on {join}
        where {same}""",
    )
    updated = len(rows) - inserted - unchanged
    connection.execute("begin transaction")
    try:
        connection.execute(
            "insert or replace into venue_weather select * from incoming"
        )
        connection.execute("commit")
    except duckdb.Error:
        connection.execute("rollback")
        raise
    return inserted, updated, unchanged


def upsert_weather_v2_rows(
    connection: duckdb.DuckDBPyConnection, rows: Sequence[WeatherV2Row]
) -> tuple[int, int, int]:
    """Upsert validated v2 rows and return inserted, updated, unchanged counts."""
    connection.execute(CREATE_TABLE_V2_SQL)
    if not rows:
        return 0, 0, 0
    connection.execute(
        "create or replace temp table incoming as select * from venue_weather_v2 limit 0"
    )
    connection.executemany(
        "insert into incoming values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [row.as_sql_parameters() for row in rows],
    )
    join = """
      existing.keibajo_code = incoming.keibajo_code
      and existing.weather_date = incoming.weather_date
      and existing.weather_hour = incoming.weather_hour
    """
    same = """
      existing.venue_name is not distinct from incoming.venue_name
      and existing.latitude is not distinct from incoming.latitude
      and existing.longitude is not distinct from incoming.longitude
      and existing.weather_code is not distinct from incoming.weather_code
      and existing.temperature is not distinct from incoming.temperature
      and existing.precipitation is not distinct from incoming.precipitation
      and existing.wind_speed is not distinct from incoming.wind_speed
      and existing.wind_gusts is not distinct from incoming.wind_gusts
      and existing.relative_humidity is not distinct from incoming.relative_humidity
      and existing.dew_point is not distinct from incoming.dew_point
      and existing.wet_bulb_temperature is not distinct from incoming.wet_bulb_temperature
      and existing.shortwave_radiation is not distinct from incoming.shortwave_radiation
      and existing.fetched_at is not distinct from incoming.fetched_at
    """
    inserted = count_value(
        connection,
        f"""select count(*) from incoming
        left join venue_weather_v2 existing on {join}
        where existing.keibajo_code is null""",
    )
    unchanged = count_value(
        connection,
        f"""select count(*) from incoming
        join venue_weather_v2 existing on {join}
        where {same}""",
    )
    updated = len(rows) - inserted - unchanged
    connection.execute("begin transaction")
    try:
        connection.execute(
            "insert or replace into venue_weather_v2 select * from incoming"
        )
        connection.execute("commit")
    except duckdb.Error:
        connection.execute("rollback")
        raise
    return inserted, updated, unchanged


def connect_duckdb(path: Path) -> duckdb.DuckDBPyConnection:
    """Open the configured DuckDB database."""
    return duckdb.connect(str(path))


def sync_weather(
    *,
    data_dir: Path,
    start: date,
    end: date,
    token: str,
    query_impl: Callable[[str, str], list[WeatherRow]] = query_cloudflare,
    connect_impl: Callable[[Path], duckdb.DuckDBPyConnection] = connect_duckdb,
) -> SyncSummary:
    """Synchronize an inclusive date range into year-partitioned DuckDB files."""
    data_dir.mkdir(parents=True, exist_ok=True)
    inserted = 0
    updated = 0
    unchanged = 0
    remote_rows = 0
    dates = list(date_range(start, end))
    for target_date in dates:
        rows = query_impl(build_latest_weather_query(target_date), token)
        wrong_dates = [
            row.weather_date
            for row in rows
            if row.weather_date != target_date.isoformat()
        ]
        if wrong_dates:
            raise ValueError(
                f"Cloudflare returned rows outside requested date: {wrong_dates[0]}"
            )
        path = data_dir / f"venue_weather_{target_date.year:04d}.duckdb"
        connection = connect_impl(path)
        try:
            date_inserted, date_updated, date_unchanged = upsert_weather_rows(
                connection, rows
            )
        finally:
            connection.close()
        inserted += date_inserted
        updated += date_updated
        unchanged += date_unchanged
        remote_rows += len(rows)
    return SyncSummary(
        dates=len(dates),
        inserted=inserted,
        rows_from_cloudflare=remote_rows,
        unchanged=unchanged,
        updated=updated,
    )


def sync_weather_v2(
    *,
    data_dir: Path,
    start: date,
    end: date,
    token: str,
    query_impl: Callable[[str, str], list[WeatherV2Row]] = query_cloudflare_v2,
    connect_impl: Callable[[Path], duckdb.DuckDBPyConnection] = connect_duckdb,
) -> SyncSummary:
    """Synchronize v2 metrics into separate year-partitioned DuckDB files."""
    data_dir.mkdir(parents=True, exist_ok=True)
    inserted = 0
    updated = 0
    unchanged = 0
    remote_rows = 0
    dates = list(date_range(start, end))
    for target_date in dates:
        rows = query_impl(build_latest_weather_v2_query(target_date), token)
        wrong_dates = [
            row.weather_date
            for row in rows
            if row.weather_date != target_date.isoformat()
        ]
        if wrong_dates:
            raise ValueError(
                f"Cloudflare v2 returned rows outside requested date: {wrong_dates[0]}"
            )
        path = data_dir / f"venue_weather_v2_{target_date.year:04d}.duckdb"
        connection = connect_impl(path)
        try:
            date_inserted, date_updated, date_unchanged = upsert_weather_v2_rows(
                connection, rows
            )
        finally:
            connection.close()
        inserted += date_inserted
        updated += date_updated
        unchanged += date_unchanged
        remote_rows += len(rows)
    return SyncSummary(
        dates=len(dates),
        inserted=inserted,
        rows_from_cloudflare=remote_rows,
        unchanged=unchanged,
        updated=updated,
    )


def sync_all_weather(
    *, data_dir: Path, start: date, end: date, token: str
) -> DualSyncSummary:
    """Synchronize stable v1 first, then the additive v2 schema."""
    v1 = sync_weather(data_dir=data_dir, start=start, end=end, token=token)
    v2 = sync_weather_v2(data_dir=data_dir, start=start, end=end, token=token)
    return DualSyncSummary(v1=v1, v2=v2)


def resolve_token(environment: Mapping[str, str]) -> str:
    """Resolve an R2 SQL token without accepting an empty value."""
    token = environment.get("WRANGLER_R2_SQL_AUTH_TOKEN") or environment.get(
        "CLOUDFLARE_DEBUG_TOKEN"
    )
    if not token:
        raise RuntimeError(
            "missing WRANGLER_R2_SQL_AUTH_TOKEN or CLOUDFLARE_DEBUG_TOKEN"
        )
    return token


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--from-date", type=parse_date)
    parser.add_argument(
        "--to-date", type=parse_date, default=datetime.now(timezone.utc).date()
    )
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    args = parser.parse_args(argv)
    if args.lookback_days < 1:
        parser.error("--lookback-days must be positive")
    if args.from_date is None:
        args.from_date = args.to_date - timedelta(days=args.lookback_days - 1)
    if args.to_date < args.from_date:
        parser.error("--to-date must be on or after --from-date")
    return args


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    summary = sync_all_weather(
        data_dir=args.data_dir,
        start=args.from_date,
        end=args.to_date,
        token=resolve_token(os.environ),
    )
    print(json.dumps(asdict(summary), sort_keys=True))


if __name__ == "__main__":
    main()
