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
import sys
import urllib.request
from pathlib import Path
from typing import Final

VENUE_WEATHER_BASE_URL: Final[str] = os.environ.get(
    "VENUE_WEATHER_URL", "https://venue-weather.kaoru.workers.dev"
)
FETCH_TIMEOUT_SECONDS: Final[float] = 10.0

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
    "wind_gusts DOUBLE)"
)
_INSERT_SQL: Final[str] = "insert into venue_weather values (?,?,?,?,?,?,?)"


def build_weather_url(target_date: str) -> str:
    """Return the venue-weather worker URL for ``target_date`` (YYYYMMDD)."""
    return f"{VENUE_WEATHER_BASE_URL}/weather?race_date={target_date}"


def fetch_weather_json(target_date: str) -> list[dict[str, object]]:
    """Fetch the worker's ``rows`` list for ``target_date``; ``[]`` on any error.

    Performs a plain ``urllib.request`` GET with explicit headers (Cloudflare
    WAF rejects the default UA) and parses the JSON body. Any failure — URL
    error, timeout, JSON decode, or a missing / non-list ``rows`` field — is
    logged to stderr and yields an empty list so the caller falls back to the
    NULL-weather path gracefully.
    """
    url = build_weather_url(target_date)
    try:
        req = urllib.request.Request(url, headers=_REQUEST_HEADERS)
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT_SECONDS) as resp:
            raw = resp.read().decode("utf-8")
        parsed: object = json.loads(raw)
    except Exception as exc:
        print(
            f"[venue-weather] fetch failed target_date={target_date} error={exc!r}",
            file=sys.stderr,
        )
        return []
    if not isinstance(parsed, dict):
        print(
            f"[venue-weather] response is not an object target_date={target_date}",
            file=sys.stderr,
        )
        return []
    rows: object = parsed.get("rows")
    if not isinstance(rows, list):
        print(
            f"[venue-weather] response has no rows list target_date={target_date}",
            file=sys.stderr,
        )
        return []
    return [row for row in rows if isinstance(row, dict)]


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

        weather_dir.mkdir(parents=True, exist_ok=True)
        db_path = weather_dir / f"venue_weather_{year:04d}.duckdb"
        con = duckdb.connect(str(db_path))
        try:
            con.execute(_CREATE_TABLE_SQL)
            con.executemany(_INSERT_SQL, params)
        finally:
            con.close()
    except Exception as exc:
        print(
            f"[venue-weather] duckdb write failed target_date={target_date} error={exc!r}",
            file=sys.stderr,
        )
        return None
    print(
        f"[venue-weather] wrote {len(params)} rows to {db_path} target_date={target_date}",
        file=sys.stderr,
    )
    return weather_dir


def fetch_venue_weather_dir(target_date: str, work_dir: Path) -> Path | None:
    """Fetch weather for ``target_date`` and materialize it as a DuckDB dir.

    Orchestrates ``fetch_weather_json`` -> ``write_weather_duckdb``. Returns the
    ``venue-weather`` directory on success, or ``None`` when the worker returned
    no usable rows (the build then falls back to the NULL-weather path).
    """
    rows = fetch_weather_json(target_date)
    if not rows:
        print(
            f"[venue-weather] no rows for target_date={target_date} — skipping weather",
            file=sys.stderr,
        )
        return None
    return write_weather_duckdb(rows, target_date, work_dir)
