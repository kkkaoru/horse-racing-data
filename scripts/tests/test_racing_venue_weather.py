from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.error import URLError

import duckdb

import racing_venue_weather as rw


# ---------------------------------------------------------------------------
# _load_venue_coords / VENUE_COORDS
# ---------------------------------------------------------------------------


class TestLoadVenueCoords:
    def test_loads_from_json_file(self, tmp_path: Path) -> None:
        data = {"01": {"name": "札幌", "lat": 43.0775, "lon": 141.3269}}
        f = tmp_path / "venue_coords.json"
        f.write_text(json.dumps(data), encoding="utf-8")
        with patch.object(rw, "_VENUE_COORDS_PATH", f):
            result = rw._load_venue_coords()
        assert result == data

    def test_returns_dict_with_venue_info_shape(self) -> None:
        result = rw._load_venue_coords()
        for code, venue in result.items():
            assert "name" in venue, f"{code} missing name"
            assert "lat" in venue, f"{code} missing lat"
            assert "lon" in venue, f"{code} missing lon"


class TestVenueCoords:
    def test_has_all_jra_venues(self) -> None:
        for code in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"]:
            assert code in rw.VENUE_COORDS, f"JRA code {code} missing"

    def test_has_all_nar_venues(self) -> None:
        for code in [
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
        ]:
            assert code in rw.VENUE_COORDS, f"NAR code {code} missing"

    def test_each_venue_has_required_keys(self) -> None:
        for code, venue in rw.VENUE_COORDS.items():
            assert "name" in venue, f"{code} missing name"
            assert "lat" in venue, f"{code} missing lat"
            assert "lon" in venue, f"{code} missing lon"

    def test_total_venue_count(self) -> None:
        assert len(rw.VENUE_COORDS) == 25


# ---------------------------------------------------------------------------
# build_url
# ---------------------------------------------------------------------------


class TestBuildUrl:
    def test_archive_uses_archive_base(self) -> None:
        url = rw.build_url(
            35.69, 139.49, date(2023, 1, 1), date(2023, 12, 31), archive=True
        )
        assert url.startswith(rw.OPEN_METEO_ARCHIVE_URL)

    def test_forecast_uses_forecast_base(self) -> None:
        url = rw.build_url(
            35.69, 139.49, date(2024, 1, 1), date(2024, 1, 7), archive=False
        )
        assert url.startswith(rw.OPEN_METEO_FORECAST_URL)

    def test_contains_date_range(self) -> None:
        url = rw.build_url(
            43.04, 141.40, date(2023, 6, 1), date(2023, 6, 30), archive=True
        )
        assert "2023-06-01" in url
        assert "2023-06-30" in url

    def test_contains_hourly_variables(self) -> None:
        url = rw.build_url(
            35.69, 139.49, date(2023, 1, 1), date(2023, 1, 31), archive=True
        )
        assert "weather_code" in url
        assert "temperature_2m" in url
        assert "precipitation" in url

    def test_contains_timezone(self) -> None:
        url = rw.build_url(
            35.69, 139.49, date(2023, 1, 1), date(2023, 1, 31), archive=True
        )
        assert "Asia" in url

    def test_uses_hourly_param_key(self) -> None:
        url = rw.build_url(
            35.69, 139.49, date(2023, 1, 1), date(2023, 1, 31), archive=True
        )
        assert "hourly=" in url


# ---------------------------------------------------------------------------
# fetch_raw
# ---------------------------------------------------------------------------


class TestFetchRaw:
    def _make_mock_response(self, body: bytes) -> MagicMock:
        mock_resp = MagicMock()
        mock_resp.read.return_value = body
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    def test_returns_response_body(self) -> None:
        body = b'{"hourly": {}}'
        with patch(
            "racing_venue_weather.urlopen", return_value=self._make_mock_response(body)
        ):
            result = rw.fetch_raw("http://example.com")
        assert result == body

    def test_passes_timeout(self) -> None:
        with patch(
            "racing_venue_weather.urlopen", return_value=self._make_mock_response(b"{}")
        ) as mock_open:
            rw.fetch_raw("http://example.com")
        mock_open.assert_called_once_with("http://example.com", timeout=rw._TIMEOUT_SEC)


# ---------------------------------------------------------------------------
# _nth
# ---------------------------------------------------------------------------


class TestNth:
    def test_returns_element_at_index(self) -> None:
        assert rw._nth([10, 20, 30], 1) == 20

    def test_returns_first_element(self) -> None:
        assert rw._nth([99], 0) == 99

    def test_returns_none_when_out_of_bounds(self) -> None:
        assert rw._nth([10], 5) is None

    def test_returns_none_for_non_list(self) -> None:
        assert rw._nth("not-a-list", 0) is None

    def test_returns_none_for_none_input(self) -> None:
        assert rw._nth(None, 0) is None

    def test_returns_none_for_empty_list(self) -> None:
        assert rw._nth([], 0) is None


# ---------------------------------------------------------------------------
# _split_time
# ---------------------------------------------------------------------------


class TestSplitTime:
    def test_midnight(self) -> None:
        assert rw._split_time("2024-01-01T00:00") == ("2024-01-01", 0)

    def test_noon(self) -> None:
        assert rw._split_time("2024-06-15T12:00") == ("2024-06-15", 12)

    def test_last_hour(self) -> None:
        assert rw._split_time("2024-12-31T23:00") == ("2024-12-31", 23)

    def test_single_digit_hour(self) -> None:
        assert rw._split_time("2024-01-01T09:00") == ("2024-01-01", 9)


# ---------------------------------------------------------------------------
# parse_hourly
# ---------------------------------------------------------------------------


class TestParseHourly:
    def test_empty_object_returns_empty(self) -> None:
        assert rw.parse_hourly(b"{}") == []

    def test_null_hourly_returns_empty(self) -> None:
        assert rw.parse_hourly(b'{"hourly": null}') == []

    def test_non_dict_hourly_returns_empty(self) -> None:
        assert rw.parse_hourly(b'{"hourly": []}') == []

    def test_missing_time_returns_empty(self) -> None:
        assert rw.parse_hourly(b'{"hourly": {"weather_code": [1]}}') == []

    def test_null_time_returns_empty(self) -> None:
        assert rw.parse_hourly(b'{"hourly": {"time": null}}') == []

    def test_full_response_parses_all_fields(self) -> None:
        data = {
            "hourly": {
                "time": ["2023-01-01T00:00", "2023-01-01T01:00"],
                "weather_code": [3, 61],
                "temperature_2m": [10.5, 8.2],
                "precipitation": [0.0, 12.3],
                "wind_speed_10m": [15.2, 20.1],
                "wind_gusts_10m": [25.3, 32.1],
            }
        }
        rows = rw.parse_hourly(json.dumps(data).encode())
        assert len(rows) == 2
        assert rows[0]["date"] == "2023-01-01"
        assert rows[0]["hour"] == 0
        assert rows[0]["weather_code"] == 3
        assert rows[0]["temperature"] == 10.5
        assert rows[0]["precipitation"] == 0.0
        assert rows[0]["wind_speed"] == 15.2
        assert rows[0]["wind_gusts"] == 25.3
        assert rows[1]["hour"] == 1
        assert rows[1]["temperature"] == 8.2

    def test_missing_optional_keys_produce_none(self) -> None:
        data = {"hourly": {"time": ["2023-01-01T00:00"]}}
        rows = rw.parse_hourly(json.dumps(data).encode())
        assert len(rows) == 1
        assert rows[0]["weather_code"] is None
        assert rows[0]["temperature"] is None
        assert rows[0]["precipitation"] is None

    def test_single_hour(self) -> None:
        data = {
            "hourly": {
                "time": ["2024-06-01T09:00"],
                "weather_code": [0],
                "temperature_2m": [28.0],
                "precipitation": [0.0],
                "wind_speed_10m": [12.0],
                "wind_gusts_10m": [20.0],
            }
        }
        rows = rw.parse_hourly(json.dumps(data).encode())
        assert len(rows) == 1
        assert rows[0]["date"] == "2024-06-01"
        assert rows[0]["hour"] == 9


# ---------------------------------------------------------------------------
# build_records
# ---------------------------------------------------------------------------


class TestBuildRecords:
    _VENUE: rw.VenueInfo = {"name": "東京", "lat": 35.6894, "lon": 139.4990}

    def test_builds_correct_tuples(self) -> None:
        rows: list[dict[str, object]] = [
            {
                "date": "2023-01-01",
                "hour": 9,
                "weather_code": 3,
                "temperature": 10.5,
                "precipitation": 0.0,
                "wind_speed": 15.2,
                "wind_gusts": 25.3,
            }
        ]
        records = rw.build_records("05", self._VENUE, rows, "ts")
        assert len(records) == 1
        rec = records[0]
        assert rec[0] == "05"
        assert rec[1] == "2023-01-01"
        assert rec[2] == 9
        assert rec[3] == "東京"
        assert rec[4] == 35.6894
        assert rec[5] == 139.4990
        assert rec[6] == 3
        assert rec[11] == "ts"

    def test_empty_rows_returns_empty_list(self) -> None:
        assert rw.build_records("05", self._VENUE, [], "ts") == []

    def test_multiple_rows(self) -> None:
        rows: list[dict[str, object]] = [
            {
                "date": "2023-01-01",
                "hour": 0,
                "weather_code": 1,
                "temperature": None,
                "precipitation": None,
                "wind_speed": None,
                "wind_gusts": None,
            },
            {
                "date": "2023-01-01",
                "hour": 1,
                "weather_code": 2,
                "temperature": None,
                "precipitation": None,
                "wind_speed": None,
                "wind_gusts": None,
            },
        ]
        records = rw.build_records("05", self._VENUE, rows, "ts")
        assert len(records) == 2
        assert records[0][2] == 0
        assert records[1][2] == 1


# ---------------------------------------------------------------------------
# _needs_migration
# ---------------------------------------------------------------------------


class TestNeedsMigration:
    def test_returns_false_when_no_table(self, tmp_path: Path) -> None:
        conn = duckdb.connect(str(tmp_path / "w.duckdb"))
        assert rw._needs_migration(conn) is False
        conn.close()

    def test_returns_false_for_current_schema(self, tmp_path: Path) -> None:
        conn = duckdb.connect(str(tmp_path / "w.duckdb"))
        conn.execute(rw._CREATE_SQL)
        assert rw._needs_migration(conn) is False
        conn.close()

    def test_returns_true_for_old_schema_without_weather_hour(
        self, tmp_path: Path
    ) -> None:
        conn = duckdb.connect(str(tmp_path / "w.duckdb"))
        conn.execute(
            """
            CREATE TABLE venue_weather (
                keibajo_code VARCHAR NOT NULL,
                weather_date DATE NOT NULL,
                venue_name VARCHAR NOT NULL,
                PRIMARY KEY (keibajo_code, weather_date)
            )
        """
        )
        assert rw._needs_migration(conn) is True
        conn.close()


# ---------------------------------------------------------------------------
# open_db
# ---------------------------------------------------------------------------


class TestOpenDb:
    def test_creates_duckdb_file(self, tmp_path: Path) -> None:
        db_path = tmp_path / "weather.duckdb"
        conn = rw.open_db(db_path)
        assert db_path.exists()
        conn.close()

    def test_creates_venue_weather_table(self, tmp_path: Path) -> None:
        conn = rw.open_db(tmp_path / "w.duckdb")
        result = conn.execute("SELECT * FROM venue_weather LIMIT 0").fetchall()
        assert result == []
        conn.close()

    def test_creates_missing_parent_directories(self, tmp_path: Path) -> None:
        db_path = tmp_path / "nested" / "dirs" / "weather.duckdb"
        conn = rw.open_db(db_path)
        assert db_path.exists()
        conn.close()

    def test_idempotent_on_second_open(self, tmp_path: Path) -> None:
        db_path = tmp_path / "w.duckdb"
        c1 = rw.open_db(db_path)
        c1.close()
        c2 = rw.open_db(db_path)
        c2.close()

    def test_migration_drops_old_schema_and_recreates(self, tmp_path: Path) -> None:
        db_path = tmp_path / "w.duckdb"
        old_conn = duckdb.connect(str(db_path))
        old_conn.execute(
            """
            CREATE TABLE venue_weather (
                keibajo_code VARCHAR NOT NULL,
                weather_date DATE NOT NULL,
                venue_name VARCHAR NOT NULL,
                PRIMARY KEY (keibajo_code, weather_date)
            )
        """
        )
        old_conn.close()

        conn = rw.open_db(db_path)
        cols = {row[0] for row in conn.execute("DESCRIBE venue_weather").fetchall()}
        assert "weather_hour" in cols
        conn.close()


# ---------------------------------------------------------------------------
# upsert_records
# ---------------------------------------------------------------------------


class TestUpsertRecords:
    _TS = "2023-01-02T00:00:00+00:00"

    def test_empty_list_returns_zero(self, tmp_path: Path) -> None:
        conn = rw.open_db(tmp_path / "w.duckdb")
        assert rw.upsert_records(conn, []) == 0
        conn.close()

    def test_inserts_single_record(self, tmp_path: Path) -> None:
        conn = rw.open_db(tmp_path / "w.duckdb")
        rec = [
            (
                "05",
                "2023-01-01",
                9,
                "東京",
                35.69,
                139.49,
                3,
                10.5,
                0.0,
                15.2,
                25.3,
                self._TS,
            )
        ]
        n = rw.upsert_records(conn, rec)
        assert n == 1
        rows = conn.execute("SELECT keibajo_code FROM venue_weather").fetchall()
        assert len(rows) == 1
        conn.close()

    def test_returns_record_count(self, tmp_path: Path) -> None:
        conn = rw.open_db(tmp_path / "w.duckdb")
        recs = [
            (
                "05",
                "2023-01-01",
                9,
                "東京",
                35.69,
                139.49,
                3,
                10.5,
                0.0,
                15.2,
                25.3,
                self._TS,
            ),
            (
                "09",
                "2023-01-01",
                9,
                "阪神",
                34.73,
                135.37,
                0,
                20.0,
                0.0,
                8.0,
                12.0,
                self._TS,
            ),
        ]
        assert rw.upsert_records(conn, recs) == 2
        conn.close()

    def test_upsert_updates_existing_row(self, tmp_path: Path) -> None:
        conn = rw.open_db(tmp_path / "w.duckdb")
        r1 = [
            (
                "05",
                "2023-01-01",
                9,
                "東京",
                35.69,
                139.49,
                3,
                10.5,
                0.0,
                15.2,
                25.3,
                self._TS,
            )
        ]
        r2 = [
            (
                "05",
                "2023-01-01",
                9,
                "東京",
                35.69,
                139.49,
                61,
                8.2,
                12.3,
                20.1,
                32.1,
                self._TS,
            )
        ]
        rw.upsert_records(conn, r1)
        rw.upsert_records(conn, r2)
        rows = conn.execute(
            "SELECT weather_code FROM venue_weather"
            " WHERE keibajo_code='05' AND weather_date='2023-01-01' AND weather_hour=9"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 61
        conn.close()

    def test_null_values_accepted(self, tmp_path: Path) -> None:
        conn = rw.open_db(tmp_path / "w.duckdb")
        rec = [
            (
                "01",
                "2023-01-01",
                0,
                "札幌",
                43.04,
                141.40,
                None,
                None,
                None,
                None,
                None,
                self._TS,
            )
        ]
        rw.upsert_records(conn, rec)
        rows = conn.execute(
            "SELECT weather_code FROM venue_weather WHERE keibajo_code='01'"
        ).fetchall()
        assert rows[0][0] is None
        conn.close()


# ---------------------------------------------------------------------------
# fetch_venue
# ---------------------------------------------------------------------------


class TestFetchVenue:
    _VENUE: rw.VenueInfo = {"name": "東京", "lat": 35.6894, "lon": 139.4990}

    def _mock_response(self, data: dict[str, object]) -> MagicMock:
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(data).encode()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    def test_returns_records_from_api(self) -> None:
        data = {
            "hourly": {
                "time": ["2023-01-01T00:00"],
                "weather_code": [3],
                "temperature_2m": [10.5],
                "precipitation": [0.0],
                "wind_speed_10m": [15.2],
                "wind_gusts_10m": [25.3],
            }
        }
        with patch(
            "racing_venue_weather.urlopen", return_value=self._mock_response(data)
        ):
            records = rw.fetch_venue(
                "05",
                self._VENUE,
                date(2023, 1, 1),
                date(2023, 1, 1),
                archive=True,
                fetched_at="ts",
            )
        assert len(records) == 1
        assert records[0][0] == "05"
        assert records[0][2] == 0
        assert records[0][3] == "東京"

    def test_empty_hourly_returns_empty_records(self) -> None:
        with patch(
            "racing_venue_weather.urlopen", return_value=self._mock_response({})
        ):
            records = rw.fetch_venue(
                "05",
                self._VENUE,
                date(2023, 1, 1),
                date(2023, 1, 1),
                archive=False,
                fetched_at="ts",
            )
        assert records == []


# ---------------------------------------------------------------------------
# _sync_all_venues
# ---------------------------------------------------------------------------


class TestSyncAllVenues:
    def test_saves_all_venues(self, tmp_path: Path) -> None:
        conn = rw.open_db(tmp_path / "w.duckdb")

        sample_rec = [
            (
                "01",
                "2023-01-01",
                0,
                "札幌",
                43.04,
                141.40,
                0,
                5.0,
                0.0,
                10.0,
                15.0,
                "2023-01-02T00:00:00+00:00",
            )
        ]

        def side_effect(
            keibajo_code: str, *_args: object, **_kwargs: object
        ) -> list[tuple[object, ...]]:
            return sample_rec if keibajo_code == "01" else []

        with patch("racing_venue_weather.fetch_venue", side_effect=side_effect):
            with patch("time.sleep"):
                rw._sync_all_venues(
                    conn,
                    date(2023, 1, 1),
                    date(2023, 1, 1),
                    archive=True,
                    fetched_at="ts",
                )

        rows = conn.execute(
            "SELECT keibajo_code FROM venue_weather WHERE keibajo_code='01'"
        ).fetchall()
        assert len(rows) == 1
        conn.close()

    def test_continues_on_url_error(self, tmp_path: Path) -> None:
        conn = rw.open_db(tmp_path / "w.duckdb")
        call_count = 0

        def raise_on_first(
            *_args: object, **_kwargs: object
        ) -> list[tuple[object, ...]]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise URLError("connection refused")
            return []

        with patch("racing_venue_weather.fetch_venue", side_effect=raise_on_first):
            with patch("time.sleep"):
                rw._sync_all_venues(
                    conn,
                    date(2023, 1, 1),
                    date(2023, 1, 1),
                    archive=True,
                    fetched_at="ts",
                )

        assert call_count == len(rw.VENUE_COORDS)
        conn.close()

    def test_sleeps_between_venues(self, tmp_path: Path) -> None:
        conn = rw.open_db(tmp_path / "w.duckdb")
        with patch("racing_venue_weather.fetch_venue", return_value=[]):
            with patch("time.sleep") as mock_sleep:
                rw._sync_all_venues(
                    conn,
                    date(2023, 1, 1),
                    date(2023, 1, 1),
                    archive=True,
                    fetched_at="ts",
                )
        assert mock_sleep.call_count == len(rw.VENUE_COORDS)
        conn.close()

    def test_continues_on_os_error(self, tmp_path: Path) -> None:
        conn = rw.open_db(tmp_path / "w.duckdb")
        call_count = 0

        def raise_os(*_args: object, **_kwargs: object) -> list[tuple[object, ...]]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("io error")
            return []

        with patch("racing_venue_weather.fetch_venue", side_effect=raise_os):
            with patch("time.sleep"):
                rw._sync_all_venues(
                    conn,
                    date(2023, 1, 1),
                    date(2023, 1, 1),
                    archive=True,
                    fetched_at="ts",
                )

        assert call_count == len(rw.VENUE_COORDS)
        conn.close()


# ---------------------------------------------------------------------------
# run_backfill
# ---------------------------------------------------------------------------


class TestRunBackfill:
    def test_single_chunk_calls_sync_once(self, tmp_path: Path) -> None:
        with patch("racing_venue_weather._sync_all_venues") as mock_sync:
            rw.run_backfill(date(2023, 1, 1), date(2023, 12, 31), tmp_path / "w.duckdb")
        assert mock_sync.call_count == 1
        _, kwargs = mock_sync.call_args
        assert kwargs["archive"] is True

    def test_multi_year_calls_sync_multiple_times(self, tmp_path: Path) -> None:
        with patch("racing_venue_weather._sync_all_venues") as mock_sync:
            rw.run_backfill(date(2022, 1, 1), date(2023, 12, 31), tmp_path / "w.duckdb")
        assert mock_sync.call_count == 2

    def test_single_day_range(self, tmp_path: Path) -> None:
        with patch("racing_venue_weather._sync_all_venues") as mock_sync:
            rw.run_backfill(date(2023, 6, 1), date(2023, 6, 1), tmp_path / "w.duckdb")
        assert mock_sync.call_count == 1
        _args, _kwargs = mock_sync.call_args
        assert _args[1] == date(2023, 6, 1)
        assert _args[2] == date(2023, 6, 1)


# ---------------------------------------------------------------------------
# run_daily
# ---------------------------------------------------------------------------


class TestRunDaily:
    def test_uses_provided_today(self, tmp_path: Path) -> None:
        with patch("racing_venue_weather._sync_all_venues") as mock_sync:
            rw.run_daily(tmp_path / "w.duckdb", today=date(2024, 6, 1))
        mock_sync.assert_called_once()
        _args, kwargs = mock_sync.call_args
        assert kwargs["archive"] is False
        assert _args[1] == date(2024, 6, 1)
        assert _args[2] == date(2024, 6, 1)

    def test_uses_date_today_when_not_provided(self, tmp_path: Path) -> None:
        with patch("racing_venue_weather._sync_all_venues") as mock_sync:
            rw.run_daily(tmp_path / "w.duckdb")
        mock_sync.assert_called_once()
        _, kwargs = mock_sync.call_args
        assert kwargs["archive"] is False


# ---------------------------------------------------------------------------
# _parse_date_range
# ---------------------------------------------------------------------------


class TestParseDateRange:
    def test_valid_range_returns_tuple(self) -> None:
        result = rw._parse_date_range("2023-01-01", "2023-12-31")
        assert result == (date(2023, 1, 1), date(2023, 12, 31))

    def test_same_start_and_end(self) -> None:
        result = rw._parse_date_range("2023-06-01", "2023-06-01")
        assert result == (date(2023, 6, 1), date(2023, 6, 1))

    def test_missing_start_returns_none(self) -> None:
        assert rw._parse_date_range(None, "2023-12-31") is None

    def test_missing_end_returns_none(self) -> None:
        assert rw._parse_date_range("2023-01-01", None) is None

    def test_both_missing_returns_none(self) -> None:
        assert rw._parse_date_range(None, None) is None

    def test_invalid_start_returns_none(self) -> None:
        assert rw._parse_date_range("not-a-date", "2023-12-31") is None

    def test_invalid_end_returns_none(self) -> None:
        assert rw._parse_date_range("2023-01-01", "bad") is None

    def test_start_after_end_returns_none(self) -> None:
        assert rw._parse_date_range("2023-12-31", "2023-01-01") is None


# ---------------------------------------------------------------------------
# _parse_args
# ---------------------------------------------------------------------------


class TestParseArgs:
    def test_daily_mode(self) -> None:
        args = rw._parse_args(["--mode", "daily"])
        assert args.mode == "daily"

    def test_backfill_mode_with_dates(self) -> None:
        args = rw._parse_args(
            ["--mode", "backfill", "--start", "2020-01-01", "--end", "2020-12-31"]
        )
        assert args.mode == "backfill"
        assert args.start == "2020-01-01"
        assert args.end == "2020-12-31"

    def test_custom_db_path(self) -> None:
        args = rw._parse_args(["--mode", "daily", "--db-path", "/tmp/custom.duckdb"])
        assert args.db_path == "/tmp/custom.duckdb"

    def test_default_db_path(self) -> None:
        args = rw._parse_args(["--mode", "daily"])
        assert args.db_path == str(rw.DEFAULT_DB_PATH)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


class TestMain:
    def test_daily_mode_returns_zero(self, tmp_path: Path) -> None:
        with patch("racing_venue_weather.run_daily") as mock_daily:
            rc = rw.main(["--mode", "daily", "--db-path", str(tmp_path / "w.duckdb")])
        assert rc == 0
        mock_daily.assert_called_once()

    def test_backfill_mode_returns_zero(self, tmp_path: Path) -> None:
        with patch("racing_venue_weather.run_backfill") as mock_backfill:
            rc = rw.main(
                [
                    "--mode",
                    "backfill",
                    "--start",
                    "2023-01-01",
                    "--end",
                    "2023-12-31",
                    "--db-path",
                    str(tmp_path / "w.duckdb"),
                ]
            )
        assert rc == 0
        mock_backfill.assert_called_once()

    def test_backfill_missing_dates_returns_one(self, tmp_path: Path) -> None:
        rc = rw.main(["--mode", "backfill", "--db-path", str(tmp_path / "w.duckdb")])
        assert rc == 1

    def test_backfill_invalid_start_returns_one(self, tmp_path: Path) -> None:
        rc = rw.main(
            [
                "--mode",
                "backfill",
                "--start",
                "INVALID",
                "--end",
                "2023-12-31",
                "--db-path",
                str(tmp_path / "w.duckdb"),
            ]
        )
        assert rc == 1

    def test_backfill_start_after_end_returns_one(self, tmp_path: Path) -> None:
        rc = rw.main(
            [
                "--mode",
                "backfill",
                "--start",
                "2023-12-31",
                "--end",
                "2023-01-01",
                "--db-path",
                str(tmp_path / "w.duckdb"),
            ]
        )
        assert rc == 1
