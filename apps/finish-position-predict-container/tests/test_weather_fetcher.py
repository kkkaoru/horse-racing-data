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
from itertools import product
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from weather_fetcher import (
    build_weather_url,
    fetch_venue_weather_dir,
    fetch_weather_json,
    parse_weather_response,
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
_VENUE_CODES: tuple[str, ...] = (
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
)


def _weather_row(key: tuple[str, int]) -> dict[str, object]:
    venue, hour = key
    return {**_ONE_ROW, "keibajo_code": venue, "weather_hour": hour}


def _complete_rows() -> list[dict[str, object]]:
    return list(map(_weather_row, product(_VENUE_CODES, range(24))))


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
    body = json.dumps({"rows": _complete_rows(), "source": "kv"}).encode("utf-8")
    with patch("weather_fetcher.urllib.request.urlopen", _make_urlopen_returning(body)):
        rows = fetch_weather_json("20260624")
    assert len(rows) == 600
    assert rows[0]["keibajo_code"] == "01"


def test_fetch_weather_json_http_error_returns_empty() -> None:
    urlopen = MagicMock(side_effect=urllib.error.URLError("boom"))
    with (
        patch("weather_fetcher.urllib.request.urlopen", urlopen),
        patch("weather_fetcher.time.sleep") as sleep,
    ):
        rows = fetch_weather_json("20260624")
    assert rows == []
    assert urlopen.call_count == 3
    assert sleep.call_count == 2


def test_fetch_weather_json_malformed_rows_returns_empty() -> None:
    body = json.dumps({"source": "kv"}).encode("utf-8")
    with (
        patch("weather_fetcher.urllib.request.urlopen", _make_urlopen_returning(body)),
        patch("weather_fetcher.time.sleep"),
    ):
        rows = fetch_weather_json("20260624")
    assert rows == []


def test_fetch_weather_json_non_object_returns_empty() -> None:
    body = json.dumps([_ONE_ROW]).encode("utf-8")
    with (
        patch("weather_fetcher.urllib.request.urlopen", _make_urlopen_returning(body)),
        patch("weather_fetcher.time.sleep"),
    ):
        rows = fetch_weather_json("20260624")
    assert rows == []


def test_fetch_weather_json_retries_then_succeeds() -> None:
    body = json.dumps({"rows": _complete_rows(), "source": "r2"}).encode("utf-8")
    success = _make_urlopen_returning(body).return_value
    urlopen = MagicMock(side_effect=[urllib.error.URLError("temporary"), success])
    with (
        patch("weather_fetcher.urllib.request.urlopen", urlopen),
        patch("weather_fetcher.time.sleep") as sleep,
    ):
        rows = fetch_weather_json("20260624")
    assert len(rows) == 600
    assert urlopen.call_count == 2
    sleep.assert_called_once_with(0.5)


def test_parse_weather_response_rejects_partial_cloudflare_data() -> None:
    with pytest.raises(ValueError, match="response is incomplete rows=1 venues=1"):
        parse_weather_response({"rows": [_ONE_ROW], "source": "r2"}, "20260624")


def test_write_weather_duckdb_empty_rows_returns_none(tmp_path: Path) -> None:
    assert write_weather_duckdb([], "20260624", tmp_path) is None


def test_write_weather_duckdb_success(tmp_path: Path) -> None:
    fake_duckdb = MagicMock()
    fake_duckdb.Error = RuntimeError
    fake_con = MagicMock()
    fake_duckdb.connect.return_value = fake_con
    with patch.dict(sys.modules, {"duckdb": fake_duckdb}):
        result = write_weather_duckdb([dict(_ONE_ROW)], "20260624", tmp_path)
    assert result == tmp_path / "venue-weather"
    assert fake_duckdb.connect.called
    create_sql = fake_con.execute.call_args[0][0]
    assert "venue_weather" in create_sql
    assert "primary key" in create_sql
    insert_sql = fake_con.executemany.call_args[0][0]
    assert insert_sql == "insert or replace into venue_weather values (?,?,?,?,?,?,?)"
    inserted_params = fake_con.executemany.call_args[0][1]
    assert len(inserted_params) == 1
    assert inserted_params[0][0] == "01"
    assert inserted_params[0][1] == "2026-06-24"


def test_write_weather_duckdb_writes_complete_v2_sidecar(tmp_path: Path) -> None:
    fake_duckdb = MagicMock()
    fake_duckdb.Error = RuntimeError
    v1_con = MagicMock()
    v2_con = MagicMock()
    fake_duckdb.connect.side_effect = [v1_con, v2_con]
    row = {
        **_ONE_ROW,
        "relative_humidity": 80.0,
        "dew_point": 18.0,
        "wet_bulb_temperature": 20.0,
        "shortwave_radiation": 300.0,
    }
    with patch.dict(sys.modules, {"duckdb": fake_duckdb}):
        result = write_weather_duckdb([row], "20260624", tmp_path)
    assert result == tmp_path / "venue-weather"
    assert fake_duckdb.connect.call_count == 2
    assert (
        fake_duckdb.connect.call_args_list[1]
        .args[0]
        .endswith("venue-weather/venue_weather_v2_2026.duckdb")
    )
    assert v2_con.executemany.call_args.args[0] == (
        "insert or replace into venue_weather_v2 values (?,?,?,?,?,?,?,?)"
    )
    assert v2_con.executemany.call_args.args[1][0] == (
        "01",
        "2026-06-24",
        9,
        20.5,
        80.0,
        18.0,
        20.0,
        300.0,
    )


def test_write_weather_duckdb_keeps_v1_when_optional_v2_write_fails(
    tmp_path: Path,
) -> None:
    fake_duckdb = MagicMock()
    fake_duckdb.Error = RuntimeError
    v1_con = MagicMock()
    fake_duckdb.connect.side_effect = [v1_con, RuntimeError("v2 disk full")]
    row = {
        **_ONE_ROW,
        "relative_humidity": 80.0,
        "dew_point": 18.0,
        "wet_bulb_temperature": 20.0,
        "shortwave_radiation": 300.0,
    }
    with patch.dict(sys.modules, {"duckdb": fake_duckdb}):
        result = write_weather_duckdb([row], "20260624", tmp_path)
    assert result == tmp_path / "venue-weather"
    assert fake_duckdb.connect.call_count == 2


def test_write_weather_duckdb_rows_all_filtered_returns_none(tmp_path: Path) -> None:
    fake_duckdb = MagicMock()
    fake_duckdb.Error = RuntimeError
    bad_rows: list[dict[str, object]] = [
        {"keibajo_code": "01", "weather_hour": 9, "temperature": 20.0}
    ]
    with patch.dict(sys.modules, {"duckdb": fake_duckdb}):
        result = write_weather_duckdb(bad_rows, "20260624", tmp_path)
    assert result is None
    assert not fake_duckdb.connect.called


def test_write_weather_duckdb_connect_failure_returns_none(tmp_path: Path) -> None:
    fake_duckdb = MagicMock()
    fake_duckdb.Error = RuntimeError
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
