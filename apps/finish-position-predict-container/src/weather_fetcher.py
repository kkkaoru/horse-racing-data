"""Fetch per-venue hourly weather rows from the venue-weather Cloudflare worker.

This module is excluded from the coverage gate (only ``predict_lib`` is
measured) because it performs live HTTP I/O against the public venue-weather
Cloudflare worker and writes a DuckDB sidecar file. It is verified at deploy
time, not in CI unit tests — same framing as ``realtime_odds_fetcher``.

Flow per category run:
  1. ``GET {VENUE_WEATHER_BASE_URL}/weather?race_date=YYYYMMDD`` (no auth) and
     parse the JSON body's ``rows`` list. Failure of the request (URLError,
     timeout, JSON decode, missing/non-list ``rows``) is logged to stderr and
     treated as "no weather" — the function returns ``[]`` so the build falls
     back to the existing NULL-weather path gracefully.
  2. Write the collected rows to a per-year DuckDB file
     ``{work_dir}/venue-weather/venue_weather_{year}.duckdb`` with a single
     ``venue_weather`` table whose schema and column names exactly match what
     the DuckDB feature builder probes (``keibajo_code``, ``weather_date``,
     ``weather_hour``, ``temperature``, ``precipitation``, ``wind_speed``,
     ``wind_gusts``). Returns the ENCLOSING directory (the builder's
     ``--venue-weather-dir`` wants the directory that holds the per-year files).

Worker row shape note:
  The worker emits each row with the key ``race_date`` (a ``YYYY-MM-DD``
  string) which maps to the table column ``weather_date``. Numeric fields may
  be absent for sparse hours; missing optional values become SQL ``NULL``.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Final

from predict_lib.debug_log import debug_log

VENUE_WEATHER_BASE_URL: Final[str] = os.environ.get(
    "VENUE_WEATHER_URL", "https://venue-weather.kaoru.workers.dev"
)
FETCH_TIMEOUT_SECONDS: Final[float] = 10.0
FETCH_ATTEMPTS: Final[int] = 3
RETRY_BASE_SECONDS: Final[float] = 0.5
HOURS_PER_DAY: Final[int] = 24
EXPECTED_VENUE_CODES: Final[frozenset[str]] = frozenset(
    {
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "30",
        "35",
        "36",
        "42",
        "43",
        "44",
        "45",
        "46",
        "47",
        "48",
        "50",
        "51",
        "54",
        "55",
        "83",
    }
)
EXPECTED_ROW_COUNT: Final[int] = len(EXPECTED_VENUE_CODES) * HOURS_PER_DAY

# Cloudflare WAF rejects Python's default empty User-Agent with HTTP 403; an
# explicit, descriptive UA passes and keeps worker-side logs legible. Same
# rationale as realtime_odds_fetcher._REQUEST_HEADERS.
_REQUEST_HEADERS: Final[dict[str, str]] = {
    "Accept": "application/json",
    "User-Agent": "horse-racing-data-predict/1.0",
}

# Column order for the parameterized insert — mirrors the table schema and the
# SELECT the DuckDB feature builder runs against ``venue_weather``.
_ROW_KEYS: Final[tuple[str, ...]] = (
    "keibajo_code",
    "race_date",
    "weather_hour",
    "temperature",
    "precipitation",
    "wind_speed",
    "wind_gusts",
)
# Keys whose absence makes a row unusable (no venue/date/hour to key on).
_REQUIRED_KEYS: Final[tuple[str, ...]] = ("keibajo_code", "race_date", "weather_hour")

_CREATE_TABLE_SQL: Final[str] = (
    "create table if not exists venue_weather ("
    "keibajo_code VARCHAR, "
    "weather_date DATE, "
    "weather_hour INTEGER, "
    "temperature DOUBLE, "
    "precipitation DOUBLE, "
    "wind_speed DOUBLE, "
    "wind_gusts DOUBLE, "
    "primary key (keibajo_code, weather_date, weather_hour))"
)
_INSERT_SQL: Final[str] = "insert or replace into venue_weather values (?,?,?,?,?,?,?)"
_V2_ROW_KEYS: Final[tuple[str, ...]] = (
    "keibajo_code",
    "race_date",
    "weather_hour",
    "temperature",
    "relative_humidity",
    "dew_point",
    "wet_bulb_temperature",
    "shortwave_radiation",
)
_V2_METRIC_KEYS: Final[tuple[str, ...]] = (
    "relative_humidity",
    "dew_point",
    "wet_bulb_temperature",
    "shortwave_radiation",
)
_CREATE_V2_TABLE_SQL: Final[str] = (
    "create table if not exists venue_weather_v2 ("
    "keibajo_code VARCHAR, "
    "weather_date DATE, "
    "weather_hour INTEGER, "
    "temperature DOUBLE, "
    "relative_humidity DOUBLE, "
    "dew_point DOUBLE, "
    "wet_bulb_temperature DOUBLE, "
    "shortwave_radiation DOUBLE, "
    "primary key (keibajo_code, weather_date, weather_hour))"
)
_V2_INSERT_SQL: Final[str] = "insert or replace into venue_weather_v2 values (?,?,?,?,?,?,?,?)"


def build_weather_url(target_date: str) -> str:
    """Return the venue-weather worker URL for ``target_date`` (YYYYMMDD)."""
    return f"{VENUE_WEATHER_BASE_URL}/weather?race_date={target_date}"


def parse_weather_response(parsed: object, target_date: str) -> tuple[list[dict[str, object]], str]:
    """Validate a complete 25-venue hourly Cloudflare weather response."""
    if not isinstance(parsed, dict):
        raise ValueError("response is not an object")
    source = parsed.get("source")
    if source not in ("kv", "r2"):
        raise ValueError("response has invalid source")
    raw_rows = parsed.get("rows")
    if not isinstance(raw_rows, list):
        raise ValueError("response has no rows list")
    expected_date = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    rows: list[dict[str, object]] = []
    keys: set[tuple[str, int]] = set()
    venue_counts: Counter[str] = Counter()
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict):
            raise ValueError("response row is not an object")
        venue = raw_row.get("keibajo_code")
        race_date = raw_row.get("race_date")
        hour = raw_row.get("weather_hour")
        if not isinstance(venue, str) or venue not in EXPECTED_VENUE_CODES:
            raise ValueError("response row has invalid keibajo_code")
        if race_date != expected_date:
            raise ValueError("response row has invalid race_date")
        if not isinstance(hour, int) or isinstance(hour, bool) or hour not in range(24):
            raise ValueError("response row has invalid weather_hour")
        key = (venue, hour)
        if key in keys:
            raise ValueError("response contains a duplicate venue-hour")
        keys.add(key)
        venue_counts[venue] += 1
        rows.append(raw_row)
    if len(rows) != EXPECTED_ROW_COUNT or frozenset(venue_counts) != EXPECTED_VENUE_CODES:
        raise ValueError(f"response is incomplete rows={len(rows)} venues={len(venue_counts)}")
    if set(venue_counts.values()) != {HOURS_PER_DAY}:
        raise ValueError("response has incomplete hourly venue coverage")
    return rows, source


def fetch_weather_json(target_date: str) -> list[dict[str, object]]:
    """Fetch the worker's ``rows`` list for ``target_date``; ``[]`` on any error.

    Performs a plain ``urllib.request`` GET with explicit headers (Cloudflare
    WAF rejects the default UA) and parses the JSON body. Any failure — URL
    error, timeout, JSON decode, or a missing / non-list ``rows`` field — is
    logged to stderr and yields an empty list so the caller falls back to the
    NULL-weather path gracefully.
    """
    url = build_weather_url(target_date)
    req = urllib.request.Request(url, headers=_REQUEST_HEADERS)
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT_SECONDS) as resp:
                raw = resp.read().decode("utf-8")
            rows, source = parse_weather_response(json.loads(raw), target_date)
            debug_log(
                f"[venue-weather] fetch complete target_date={target_date} "
                f"source={source} rows={len(rows)} attempt={attempt}"
            )
            return rows
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            ValueError,
        ) as exc:
            debug_log(
                f"[venue-weather] fetch attempt failed target_date={target_date} "
                f"attempt={attempt}/{FETCH_ATTEMPTS} error={exc!r}"
            )
            if attempt < FETCH_ATTEMPTS:
                time.sleep(RETRY_BASE_SECONDS * (2 ** (attempt - 1)))
    debug_log(
        f"[venue-weather] fetch exhausted target_date={target_date} attempts={FETCH_ATTEMPTS}"
    )
    return []


def _write_v2_duckdb(
    rows: list[dict[str, object]],
    target_date: str,
    weather_dir: Path,
) -> None:
    import duckdb

    if not all(all(row.get(key) is not None for key in _V2_METRIC_KEYS) for row in rows):
        return
    params = [tuple(row.get(key) for key in _V2_ROW_KEYS) for row in rows]
    db_path = weather_dir / f"venue_weather_v2_{int(target_date[:4]):04d}.duckdb"
    try:
        con = duckdb.connect(str(db_path))
        try:
            con.execute(_CREATE_V2_TABLE_SQL)
            con.executemany(_V2_INSERT_SQL, params)
        finally:
            con.close()
    except (OSError, duckdb.Error) as exc:
        debug_log(
            f"[venue-weather] optional v2 DuckDB write failed "
            f"target_date={target_date} error={exc!r}"
        )


def write_weather_duckdb(
    rows: list[dict[str, object]],
    target_date: str,
    work_dir: Path,
) -> Path | None:
    """Write ``rows`` to a per-year DuckDB sidecar; return its enclosing dir.

    Returns ``None`` when ``rows`` is empty or when every row is dropped by the
    required-key filter (no file written). Otherwise creates
    ``{work_dir}/venue-weather/venue_weather_{year}.duckdb`` with the
    ``venue_weather`` table the feature builder probes and returns the
    ``venue-weather`` directory (the builder's ``--venue-weather-dir`` wants the
    directory that holds the per-year files). DuckDB is imported lazily so the
    module imports cleanly in the unit-test venv where duckdb is absent. Any
    DuckDB-side failure is logged to stderr and yields ``None``.
    """
    if not rows:
        return None
    params: list[tuple[object, ...]] = []
    for row in rows:
        if any(row.get(key) is None for key in _REQUIRED_KEYS):
            continue
        params.append(tuple(row.get(key) for key in _ROW_KEYS))
    if not params:
        return None
    year = int(target_date[:4])
    weather_dir = work_dir / "venue-weather"
    try:
        import duckdb
    except ImportError as exc:
        debug_log(f"[venue-weather] duckdb import failed target_date={target_date} error={exc!r}")
        return None
    try:
        weather_dir.mkdir(parents=True, exist_ok=True)
        db_path = weather_dir / f"venue_weather_{year:04d}.duckdb"
        con = duckdb.connect(str(db_path))
        try:
            con.execute(_CREATE_TABLE_SQL)
            con.executemany(_INSERT_SQL, params)
        finally:
            con.close()
        _write_v2_duckdb(rows, target_date, weather_dir)
    except (OSError, duckdb.Error) as exc:
        debug_log(f"[venue-weather] duckdb write failed target_date={target_date} error={exc!r}")
        return None
    debug_log(f"[venue-weather] wrote {len(params)} rows to {db_path} target_date={target_date}")
    return weather_dir


def fetch_venue_weather_dir(target_date: str, work_dir: Path) -> Path | None:
    """Fetch weather for ``target_date`` and materialize it as a DuckDB dir.

    Orchestrates ``fetch_weather_json`` -> ``write_weather_duckdb``. Returns the
    ``venue-weather`` directory on success, or ``None`` when the worker returned
    no usable rows (the build then falls back to the NULL-weather path).
    """
    rows = fetch_weather_json(target_date)
    if not rows:
        debug_log(f"[venue-weather] no rows for target_date={target_date} — skipping weather")
        return None
    return write_weather_duckdb(rows, target_date, work_dir)
