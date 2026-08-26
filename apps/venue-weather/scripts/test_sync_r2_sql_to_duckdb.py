"""Tests for the Cloudflare R2 SQL to local DuckDB synchronizer."""

from __future__ import annotations

import argparse
import tempfile
import unittest
import urllib.error
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import sync_r2_sql_to_duckdb as subject
from sync_r2_sql_to_duckdb import (
    DualSyncSummary,
    SyncSummary,
    WeatherRow,
    WeatherV2Row,
    build_latest_weather_query,
    build_latest_weather_v2_query,
    date_range,
    parse_args,
    parse_date,
    parse_response_rows,
    parse_v2_response_rows,
    query_cloudflare,
    query_cloudflare_v2,
    resolve_token,
    sync_all_weather,
    sync_weather,
    sync_weather_v2,
    upsert_weather_rows,
    upsert_weather_v2_rows,
)


class SyncR2SqlToDuckDbTest(unittest.TestCase):
    def test_date_and_value_validators_reject_invalid_inputs(self) -> None:
        with self.assertRaisesRegex(argparse.ArgumentTypeError, "invalid ISO date"):
            parse_date("2026-13-40")
        with self.assertRaisesRegex(ValueError, "on or after"):
            list(date_range(date(2026, 8, 26), date(2026, 8, 25)))
        with self.assertRaisesRegex(ValueError, "row is not an object"):
            subject.parse_weather_row("invalid")
        with self.assertRaisesRegex(ValueError, "v2 row is not an object"):
            subject.parse_weather_v2_row("invalid")
        with self.assertRaisesRegex(TypeError, "invalid temperature"):
            subject.optional_float({"temperature": "hot"}, "temperature")
        with self.assertRaisesRegex(TypeError, "invalid weather_code"):
            subject.optional_int({"weather_code": True}, "weather_code")

    def test_build_latest_weather_query_is_bounded_and_prefers_actual(self) -> None:
        query = build_latest_weather_query(date(2025, 8, 23))

        self.assertIn("race_date = '2025-08-23'", query)
        self.assertIn("weather_data_type = 'actual'", query)
        self.assertIn("PARTITION BY race_date, keibajo_code, weather_hour", query)
        self.assertIn("WHERE row_number = 1", query)

    def test_build_latest_weather_v2_query_selects_new_metrics(self) -> None:
        query = build_latest_weather_v2_query(date(2026, 8, 25))

        self.assertIn("weather.venue_weather_hourly_v2", query)
        self.assertIn("race_date = '2026-08-25'", query)
        self.assertIn("weather_data_type = 'actual'", query)
        self.assertIn("relative_humidity", query)
        self.assertIn("wet_bulb_temperature", query)

    def test_parse_response_rows_validates_and_normalizes_numbers(self) -> None:
        rows = parse_response_rows(
            {
                "success": True,
                "result": {
                    "rows": [
                        {
                            "fetched_at": "2026-06-23 20:36:14.107002+09",
                            "keibajo_code": "01",
                            "latitude": 43,
                            "longitude": 141.3269,
                            "precipitation": 2.5,
                            "race_date": "2025-08-23",
                            "temperature": 20.7,
                            "venue_name": "札幌",
                            "weather_code": 61,
                            "weather_hour": 0,
                            "wind_gusts": 47.9,
                            "wind_speed": 23.9,
                        }
                    ]
                },
            }
        )

        self.assertEqual(
            rows,
            [
                WeatherRow(
                    fetched_at="2026-06-23 20:36:14.107002+09",
                    keibajo_code="01",
                    latitude=43.0,
                    longitude=141.3269,
                    precipitation=2.5,
                    temperature=20.7,
                    venue_name="札幌",
                    weather_code=61,
                    weather_date="2025-08-23",
                    weather_hour=0,
                    wind_gusts=47.9,
                    wind_speed=23.9,
                )
            ],
        )

    def test_parse_response_rows_rejects_invalid_hour(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid weather_hour"):
            parse_response_rows(
                {
                    "success": True,
                    "result": {
                        "rows": [
                            {
                                "fetched_at": "2026-06-23 20:36:14.107002+09",
                                "keibajo_code": "01",
                                "latitude": 43.0,
                                "longitude": 141.0,
                                "race_date": "2025-08-23",
                                "venue_name": "札幌",
                                "weather_hour": 24,
                            }
                        ]
                    },
                }
            )

    def test_parse_v2_response_rows_requires_all_new_metrics(self) -> None:
        payload = {
            "success": True,
            "result": {
                "rows": [
                    {
                        "dew_point": 21.0,
                        "fetched_at": "2026-08-25 02:00:00+09",
                        "keibajo_code": "83",
                        "latitude": 42.89,
                        "longitude": 143.18,
                        "precipitation": 0.0,
                        "race_date": "2026-08-25",
                        "relative_humidity": 72.0,
                        "shortwave_radiation": 120.0,
                        "temperature": 26.0,
                        "venue_name": "帯広",
                        "weather_code": 1,
                        "weather_hour": 12,
                        "wet_bulb_temperature": 22.0,
                        "wind_gusts": 12.0,
                        "wind_speed": 5.0,
                    }
                ]
            },
        }

        rows = parse_v2_response_rows(payload)

        self.assertEqual(
            rows,
            [
                WeatherV2Row(
                    dew_point=21.0,
                    fetched_at="2026-08-25 02:00:00+09",
                    keibajo_code="83",
                    latitude=42.89,
                    longitude=143.18,
                    precipitation=0.0,
                    relative_humidity=72.0,
                    shortwave_radiation=120.0,
                    temperature=26.0,
                    venue_name="帯広",
                    weather_code=1,
                    weather_date="2026-08-25",
                    weather_hour=12,
                    wet_bulb_temperature=22.0,
                    wind_gusts=12.0,
                    wind_speed=5.0,
                )
            ],
        )
        with self.assertRaisesRegex(TypeError, "missing relative_humidity"):
            parse_v2_response_rows(
                {
                    "success": True,
                    "result": {
                        "rows": [
                            {
                                "dew_point": 21.0,
                                "fetched_at": "2026-08-25 02:00:00+09",
                                "keibajo_code": "83",
                                "latitude": 42.89,
                                "longitude": 143.18,
                                "race_date": "2026-08-25",
                                "shortwave_radiation": 120.0,
                                "venue_name": "帯広",
                                "weather_hour": 12,
                                "wet_bulb_temperature": 22.0,
                            }
                        ]
                    },
                }
            )

    def test_query_cloudflare_v2_builds_request_and_parses_response(self) -> None:
        response = MagicMock()
        response.__enter__.return_value.read.return_value = (
            b'{"success":true,"result":{"rows":[{'
            b'"dew_point":21,"fetched_at":"2026-08-25 02:00:00+09",'
            b'"keibajo_code":"83","latitude":42.89,"longitude":143.18,'
            b'"race_date":"2026-08-25","relative_humidity":72,'
            b'"shortwave_radiation":120,"venue_name":"Obihiro",'
            b'"weather_hour":12,"wet_bulb_temperature":22}]}}'
        )

        with patch("urllib.request.urlopen", return_value=response) as urlopen:
            rows = query_cloudflare_v2("select v2", "secret", retries=1)

        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Authorization"), "Bearer secret")
        self.assertIn(b'"query": "select v2"', request.data)
        self.assertEqual(rows[0].relative_humidity, 72.0)
        self.assertEqual(rows[0].wet_bulb_temperature, 22.0)

    def test_query_cloudflare_retries_then_raises(self) -> None:
        with (
            patch(
                "urllib.request.urlopen",
                side_effect=urllib.error.URLError("offline"),
            ) as urlopen,
            patch("time.sleep") as sleep,
            self.assertRaisesRegex(RuntimeError, "after 2 attempts"),
        ):
            query_cloudflare("select v1", "secret", retries=2)

        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(1.0)

    def test_response_envelopes_reject_missing_structures(self) -> None:
        with self.assertRaisesRegex(ValueError, "query failed"):
            parse_response_rows({"success": False})
        with self.assertRaisesRegex(ValueError, "no result object"):
            parse_response_rows({"success": True})
        with self.assertRaisesRegex(TypeError, "no rows list"):
            parse_response_rows({"success": True, "result": {}})
        with self.assertRaisesRegex(ValueError, "v2 query failed"):
            parse_v2_response_rows({"success": False})
        with self.assertRaisesRegex(ValueError, "v2 response has no result"):
            parse_v2_response_rows({"success": True})
        with self.assertRaisesRegex(TypeError, "v2 response has no rows"):
            parse_v2_response_rows({"success": True, "result": {}})

    def test_cli_parsing_and_token_resolution(self) -> None:
        args = parse_args(
            [
                "--to-date",
                "2026-08-25",
                "--lookback-days",
                "2",
                "--data-dir",
                "weather-data",
            ]
        )

        self.assertEqual(args.from_date, date(2026, 8, 24))
        self.assertEqual(args.to_date, date(2026, 8, 25))
        self.assertEqual(args.data_dir, Path("weather-data"))
        self.assertEqual(
            resolve_token({"WRANGLER_R2_SQL_AUTH_TOKEN": "primary"}), "primary"
        )
        self.assertEqual(
            resolve_token({"CLOUDFLARE_DEBUG_TOKEN": "fallback"}), "fallback"
        )
        with self.assertRaisesRegex(RuntimeError, "missing WRANGLER"):
            resolve_token({})

    def test_sync_all_weather_preserves_v1_then_v2_summaries(self) -> None:
        v1 = SyncSummary(
            dates=1, inserted=600, rows_from_cloudflare=600, unchanged=0, updated=0
        )
        v2 = SyncSummary(
            dates=1, inserted=0, rows_from_cloudflare=0, unchanged=0, updated=0
        )
        with (
            patch.object(subject, "sync_weather", return_value=v1) as sync_v1,
            patch.object(subject, "sync_weather_v2", return_value=v2) as sync_v2,
        ):
            result = sync_all_weather(
                data_dir=Path("data"),
                start=date(2026, 8, 25),
                end=date(2026, 8, 25),
                token="token",
            )

        self.assertEqual(result, DualSyncSummary(v1=v1, v2=v2))
        sync_v1.assert_called_once_with(
            data_dir=Path("data"),
            start=date(2026, 8, 25),
            end=date(2026, 8, 25),
            token="token",
        )
        sync_v2.assert_called_once_with(
            data_dir=Path("data"),
            start=date(2026, 8, 25),
            end=date(2026, 8, 25),
            token="token",
        )

    def test_upsert_weather_rows_reports_insert_update_and_unchanged(self) -> None:
        original = WeatherRow(
            fetched_at="2026-06-23 20:36:14.107002+09",
            keibajo_code="01",
            latitude=43.0775,
            longitude=141.3269,
            precipitation=0.0,
            temperature=20.0,
            venue_name="札幌",
            weather_code=0,
            weather_date="2025-08-23",
            weather_hour=9,
            wind_gusts=8.0,
            wind_speed=4.0,
        )
        changed = WeatherRow(
            fetched_at="2026-08-24 17:00:00+09",
            keibajo_code="01",
            latitude=43.0775,
            longitude=141.3269,
            precipitation=1.0,
            temperature=21.0,
            venue_name="札幌",
            weather_code=61,
            weather_date="2025-08-23",
            weather_hour=9,
            wind_gusts=9.0,
            wind_speed=5.0,
        )
        with tempfile.TemporaryDirectory() as directory:
            connection = duckdb.connect(str(Path(directory) / "weather.duckdb"))
            try:
                first = upsert_weather_rows(connection, [original])
                second = upsert_weather_rows(connection, [original])
                third = upsert_weather_rows(connection, [changed])
                stored = connection.execute(
                    "select temperature, precipitation, weather_code from venue_weather"
                ).fetchone()
            finally:
                connection.close()

        self.assertEqual(first, (1, 0, 0))
        self.assertEqual(second, (0, 0, 1))
        self.assertEqual(third, (0, 1, 0))
        self.assertEqual(stored, (21.0, 1.0, 61))

    def test_upsert_weather_v2_rows_reports_insert_update_and_unchanged(self) -> None:
        original = WeatherV2Row(
            dew_point=18.0,
            fetched_at="2026-08-25 02:00:00+09",
            keibajo_code="83",
            latitude=42.89,
            longitude=143.18,
            precipitation=0.0,
            relative_humidity=70.0,
            shortwave_radiation=100.0,
            temperature=24.0,
            venue_name="帯広",
            weather_code=1,
            weather_date="2026-08-25",
            weather_hour=12,
            wet_bulb_temperature=20.0,
            wind_gusts=10.0,
            wind_speed=4.0,
        )
        changed = WeatherV2Row(
            dew_point=19.0,
            fetched_at="2026-08-25 20:00:00+09",
            keibajo_code="83",
            latitude=42.89,
            longitude=143.18,
            precipitation=1.0,
            relative_humidity=75.0,
            shortwave_radiation=80.0,
            temperature=23.0,
            venue_name="帯広",
            weather_code=61,
            weather_date="2026-08-25",
            weather_hour=12,
            wet_bulb_temperature=20.5,
            wind_gusts=11.0,
            wind_speed=5.0,
        )
        with tempfile.TemporaryDirectory() as directory:
            connection = duckdb.connect(str(Path(directory) / "weather-v2.duckdb"))
            try:
                first = upsert_weather_v2_rows(connection, [original])
                second = upsert_weather_v2_rows(connection, [original])
                third = upsert_weather_v2_rows(connection, [changed])
                stored = connection.execute(
                    "select relative_humidity, wet_bulb_temperature, weather_code "
                    "from venue_weather_v2"
                ).fetchone()
            finally:
                connection.close()

        self.assertEqual(first, (1, 0, 0))
        self.assertEqual(second, (0, 0, 1))
        self.assertEqual(third, (0, 1, 0))
        self.assertEqual(stored, (75.0, 20.5, 61))

    def test_sync_weather_writes_year_partition_and_summary(self) -> None:
        def query_impl(query: str, token: str) -> list[WeatherRow]:
            self.assertEqual(token, "token")
            self.assertIn("race_date = '2025-08-23'", query)
            return [
                WeatherRow(
                    fetched_at="2026-06-23 20:36:14.107002+09",
                    keibajo_code="01",
                    latitude=43.0775,
                    longitude=141.3269,
                    precipitation=0.0,
                    temperature=20.0,
                    venue_name="札幌",
                    weather_code=0,
                    weather_date="2025-08-23",
                    weather_hour=9,
                    wind_gusts=8.0,
                    wind_speed=4.0,
                )
            ]

        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            summary = sync_weather(
                data_dir=data_dir,
                start=date(2025, 8, 23),
                end=date(2025, 8, 23),
                token="token",
                query_impl=query_impl,
            )
            database_exists = (data_dir / "venue_weather_2025.duckdb").exists()

        self.assertEqual(summary.dates, 1)
        self.assertEqual(summary.rows_from_cloudflare, 1)
        self.assertEqual(summary.inserted, 1)
        self.assertEqual(summary.updated, 0)
        self.assertEqual(summary.unchanged, 0)
        self.assertTrue(database_exists)

    def test_sync_weather_v2_writes_separate_year_partition(self) -> None:
        def query_impl(query: str, token: str) -> list[WeatherV2Row]:
            self.assertEqual(token, "token")
            self.assertIn("race_date = '2026-08-25'", query)
            return [
                WeatherV2Row(
                    dew_point=18.0,
                    fetched_at="2026-08-25 20:00:00+09",
                    keibajo_code="83",
                    latitude=42.89,
                    longitude=143.18,
                    precipitation=0.0,
                    relative_humidity=70.0,
                    shortwave_radiation=100.0,
                    temperature=24.0,
                    venue_name="帯広",
                    weather_code=1,
                    weather_date="2026-08-25",
                    weather_hour=12,
                    wet_bulb_temperature=20.0,
                    wind_gusts=10.0,
                    wind_speed=4.0,
                )
            ]

        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            summary = sync_weather_v2(
                data_dir=data_dir,
                start=date(2026, 8, 25),
                end=date(2026, 8, 25),
                token="token",
                query_impl=query_impl,
            )
            database_exists = (data_dir / "venue_weather_v2_2026.duckdb").exists()

        self.assertEqual(summary.dates, 1)
        self.assertEqual(summary.rows_from_cloudflare, 1)
        self.assertEqual(summary.inserted, 1)
        self.assertEqual(summary.updated, 0)
        self.assertEqual(summary.unchanged, 0)
        self.assertTrue(database_exists)


if __name__ == "__main__":
    unittest.main()
