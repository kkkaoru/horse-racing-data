from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import backfill_r2_catalog_v2 as subject
import duckdb


def create_v2_file(path: Path, hours: range = range(24)) -> None:
    connection = duckdb.connect(str(path))
    connection.execute(
        "create table venue_weather_v2 ("
        "keibajo_code varchar, weather_date date, weather_hour integer, "
        "venue_name varchar, latitude double, longitude double, temperature double, "
        "relative_humidity double, dew_point double, wet_bulb_temperature double, "
        "shortwave_radiation double)"
    )
    connection.executemany(
        "insert into venue_weather_v2 values (?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("05", "2025-01-01", hour, "東京", 35.0, 139.0, 10.0, 70.0, 5.0, 8.0, 100.0)
            for hour in hours
        ],
    )
    connection.close()


class BackfillV2Test(unittest.TestCase):
    def test_discover_files_uses_requested_years(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "venue_weather_v2_2024.duckdb").touch()
            (root / "venue_weather_v2_2025.duckdb").touch()
            self.assertEqual(
                subject.discover_files(root, date(2025, 1, 1), date(2025, 1, 1)),
                [root / "venue_weather_v2_2025.duckdb"],
            )

    def test_validate_and_iter_complete_day(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "venue_weather_v2_2025.duckdb"
            create_v2_file(path)
            self.assertEqual(
                subject.validate_file(path, date(2025, 1, 1), date(2025, 1, 1)), 24
            )
            events = list(
                subject.iter_events(
                    path,
                    date(2025, 1, 1),
                    date(2025, 1, 1),
                    "2026-08-25T00:00:00Z",
                )
            )
            self.assertEqual(len(events), 24)
            self.assertEqual(events[0].key, "2025-01-01|05|00")
            self.assertEqual(events[0].payload["relative_humidity"], 70.0)
            self.assertEqual(events[0].payload["weather_data_type"], "actual")

    def test_validate_rejects_incomplete_day(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "venue_weather_v2_2025.duckdb"
            create_v2_file(path, range(23))
            with self.assertRaisesRegex(ValueError, "incomplete v2 venue-date groups"):
                subject.validate_file(path, date(2025, 1, 1), date(2025, 1, 1))

    def test_backfill_checkpoints_only_after_successful_batches(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "venue_weather_v2_2025.duckdb"
            checkpoint = root / "checkpoint.json"
            create_v2_file(path)
            with patch.object(subject, "post_batch") as post:
                summary = subject.backfill(
                    files=[path],
                    start=date(2025, 1, 1),
                    end=date(2025, 1, 1),
                    pipeline_url="https://example.test",
                    token="token",
                    batch_size=10,
                    checkpoint_path=checkpoint,
                    dry_run=False,
                    fetched_at="2026-08-25T00:00:00Z",
                )
            self.assertEqual(summary, subject.Summary(events=24, requests=3, skipped=0))
            self.assertEqual(post.call_count, 3)
            self.assertEqual(
                json.loads(checkpoint.read_text())["last_key"], "2025-01-01|05|23"
            )
            with patch.object(subject, "post_batch") as resumed_post:
                resumed = subject.backfill(
                    files=[path],
                    start=date(2025, 1, 1),
                    end=date(2025, 1, 1),
                    pipeline_url="https://example.test",
                    token="token",
                    batch_size=10,
                    checkpoint_path=checkpoint,
                    dry_run=False,
                    fetched_at="2026-08-25T00:00:00Z",
                )
            self.assertEqual(resumed, subject.Summary(events=0, requests=0, skipped=24))
            resumed_post.assert_not_called()

    def test_post_batch_uses_authenticated_json_request(self) -> None:
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"ok"
        with patch.object(
            subject.urllib.request, "urlopen", return_value=response
        ) as urlopen:
            subject.post_batch(
                "https://example.test", "secret", [{"race_date": "2025-01-01"}]
            )
        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(
            request.get_header("X-venue-weather-v2-backfill-token"), "secret"
        )

    def test_empty_selected_slice_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "venue_weather_v2_2025.duckdb"
            create_v2_file(path)
            with self.assertRaisesRegex(
                ValueError, "selected v2 backfill slice has no rows"
            ):
                subject.backfill(
                    files=[path],
                    start=date(2025, 1, 2),
                    end=date(2025, 1, 2),
                    pipeline_url="https://example.test",
                    token="token",
                    batch_size=10,
                    checkpoint_path=None,
                    dry_run=True,
                    fetched_at="2026-08-25T00:00:00Z",
                )


if __name__ == "__main__":
    unittest.main()
