"""Tests for the venue-weather HTTP fetcher + DuckDB sidecar writer.

``weather_fetcher`` is top-level I/O glue (NOT in the coverage gate — only
``predict_lib`` is measured), so these tests exist to PASS and prove each branch
works, not to lift coverage. ``urllib.request.urlopen`` and ``duckdb.connect``
are mocked (duckdb is not importable in the unit-test venv).
"""

from __future__ import annotations

import json
import sys
import urllib.error
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from weather_fetcher import (
    build_weather_url,
    fetch_venue_weather_dir,
    fetch_weather_json,
    write_weather_duckdb,
)

_ONE_ROW: dict[str, object] = {
    "keibajo_code": "01",
    "race_date": "2026-06-24",
    "weather_hour": 9,
    "temperature": 20.5,
    "precipitation": 0,
    "wind_speed": 5.2,
    "wind_gusts": 8.1,
    "weather_type": "actual",
}


def _make_urlopen_returning(body: bytes) -> MagicMock:
    """Build a urlopen mock whose context manager yields a resp with ``body``."""
    resp = MagicMock()
    resp.read.return_value = body
    urlopen = MagicMock()
    urlopen.return_value.__enter__.return_value = resp
    return urlopen


def test_build_weather_url_format() -> None:
    url = build_weather_url("20260624")
    assert url.endswith("/weather?race_date=20260624")


def test_build_weather_url_subdomain() -> None:
    url = build_weather_url("20260624")
    assert url.startswith("https://venue-weather.kaoru.workers.dev")


def test_fetch_weather_json_success() -> None:
    body = json.dumps({"rows": [_ONE_ROW], "source": "kv"}).encode("utf-8")
    with patch("weather_fetcher.urllib.request.urlopen", _make_urlopen_returning(body)):
        rows = fetch_weather_json("20260624")
    assert len(rows) == 1
    assert rows[0]["keibajo_code"] == "01"


def test_fetch_weather_json_http_error_returns_empty() -> None:
    urlopen = MagicMock(side_effect=urllib.error.URLError("boom"))
    with patch("weather_fetcher.urllib.request.urlopen", urlopen):
        rows = fetch_weather_json("20260624")
    assert rows == []


def test_fetch_weather_json_malformed_rows_returns_empty() -> None:
    body = json.dumps({"source": "kv"}).encode("utf-8")
    with patch("weather_fetcher.urllib.request.urlopen", _make_urlopen_returning(body)):
        rows = fetch_weather_json("20260624")
    assert rows == []


def test_fetch_weather_json_non_object_returns_empty() -> None:
    body = json.dumps([_ONE_ROW]).encode("utf-8")
    with patch("weather_fetcher.urllib.request.urlopen", _make_urlopen_returning(body)):
        rows = fetch_weather_json("20260624")
    assert rows == []


def test_write_weather_duckdb_empty_rows_returns_none(tmp_path: Path) -> None:
    assert write_weather_duckdb([], "20260624", tmp_path) is None


def test_write_weather_duckdb_success(tmp_path: Path) -> None:
    fake_duckdb = MagicMock()
    fake_con = MagicMock()
    fake_duckdb.connect.return_value = fake_con
    with patch.dict(sys.modules, {"duckdb": fake_duckdb}):
        result = write_weather_duckdb([dict(_ONE_ROW)], "20260624", tmp_path)
    assert result == tmp_path / "venue-weather"
    assert fake_duckdb.connect.called
    create_sql = fake_con.execute.call_args[0][0]
    assert "venue_weather" in create_sql
    insert_sql = fake_con.executemany.call_args[0][0]
    assert "venue_weather" in insert_sql
    inserted_params = fake_con.executemany.call_args[0][1]
    assert len(inserted_params) == 1
    assert inserted_params[0][0] == "01"
    assert inserted_params[0][1] == "2026-06-24"


def test_write_weather_duckdb_rows_all_filtered_returns_none(tmp_path: Path) -> None:
    fake_duckdb = MagicMock()
    bad_rows: list[dict[str, object]] = [
        {"keibajo_code": "01", "weather_hour": 9, "temperature": 20.0}
    ]
    with patch.dict(sys.modules, {"duckdb": fake_duckdb}):
        result = write_weather_duckdb(bad_rows, "20260624", tmp_path)
    assert result is None
    assert not fake_duckdb.connect.called


def test_write_weather_duckdb_connect_failure_returns_none(tmp_path: Path) -> None:
    fake_duckdb = MagicMock()
    fake_duckdb.connect.side_effect = RuntimeError("disk full")
    with patch.dict(sys.modules, {"duckdb": fake_duckdb}):
        result = write_weather_duckdb([dict(_ONE_ROW)], "20260624", tmp_path)
    assert result is None


def test_fetch_venue_weather_dir_empty_returns_none(tmp_path: Path) -> None:
    with patch("weather_fetcher.fetch_weather_json", return_value=[]):
        assert fetch_venue_weather_dir("20260624", tmp_path) is None


def test_fetch_venue_weather_dir_writes_and_returns_dir(tmp_path: Path) -> None:
    sentinel = tmp_path / "venue-weather"
    with (
        patch("weather_fetcher.fetch_weather_json", return_value=[dict(_ONE_ROW)]),
        patch("weather_fetcher.write_weather_duckdb", return_value=sentinel) as writer,
    ):
        result = fetch_venue_weather_dir("20260624", tmp_path)
    assert result == sentinel
    assert writer.called
