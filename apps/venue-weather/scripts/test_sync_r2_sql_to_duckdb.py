"""Tests for the Cloudflare R2 SQL to local DuckDB synchronizer."""

from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb
from sync_r2_sql_to_duckdb import (
    WeatherRow,
    build_latest_weather_query,
    parse_response_rows,
    sync_weather,
    upsert_weather_rows,
)


class SyncR2SqlToDuckDbTest(unittest.TestCase):
    def test_build_latest_weather_query_is_bounded_and_prefers_actual(self) -> None:
        query = build_latest_weather_query(date(2025, 8, 23))

        self.assertIn("race_date = '2025-08-23'", query)
        self.assertIn("weather_data_type = 'actual'", query)
        self.assertIn("PARTITION BY race_date, keibajo_code, weather_hour", query)
        self.assertIn("WHERE row_number = 1", query)

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


if __name__ == "__main__":
    unittest.main()
