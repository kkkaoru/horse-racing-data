#!/usr/bin/env python3
"""Tests for the fail-fast entity-history serving refresh orchestrator."""

from __future__ import annotations

import argparse
import subprocess
import unittest
from pathlib import Path
from unittest.mock import call, patch

from refresh_entity_history_serving import commands, parse_args, run


class RefreshEntityHistoryServingTest(unittest.TestCase):
    def test_commands_publish_catalog_before_manifest(self) -> None:
        args = argparse.Namespace(
            concurrency=4,
            output=Path("/tmp/objects"),
            skip_upload=False,
            year=2026,
        )
        self.assertEqual(
            commands(args),
            [
                ["uv", "run", "scripts/sync_entity_history.py", "--year", "2026"],
                [
                    "uv",
                    "run",
                    "scripts/build_entity_history_objects.py",
                    "--year",
                    "2026",
                    "--output",
                    "/tmp/objects",
                ],
                [
                    "uv",
                    "run",
                    "scripts/pack_entity_history_objects.py",
                    "/tmp/objects",
                    "--year",
                    "2026",
                ],
                [
                    "uv",
                    "run",
                    "scripts/upload_entity_history_objects.py",
                    "/tmp/objects",
                    "--year",
                    "2026",
                    "--concurrency",
                    "4",
                ],
            ],
        )

    def test_skip_upload_stops_before_publication(self) -> None:
        args = parse_args(["--year", "2026", "--skip-upload"])
        self.assertEqual(len(commands(args)), 3)

    def test_run_is_fail_fast(self) -> None:
        args = parse_args(["--year", "2026", "--skip-upload"])
        with (
            patch(
                "refresh_entity_history_serving.subprocess.run",
                side_effect=[None, subprocess.CalledProcessError(1, "build")],
            ) as run_mock,
            self.assertRaises(subprocess.CalledProcessError),
        ):
            run(args)
        self.assertEqual(
            run_mock.call_args_list,
            [
                call(
                    [
                        "uv",
                        "run",
                        "scripts/sync_entity_history.py",
                        "--year",
                        "2026",
                    ],
                    check=True,
                ),
                call(
                    [
                        "uv",
                        "run",
                        "scripts/build_entity_history_objects.py",
                        "--year",
                        "2026",
                        "--output",
                        "tmp/entity-history-objects",
                    ],
                    check=True,
                ),
            ],
        )

    def test_parse_args_validates_bounds(self) -> None:
        with self.assertRaises(SystemExit):
            parse_args(["--year", "1985"])
        with self.assertRaises(SystemExit):
            parse_args(["--year", "2026", "--concurrency", "0"])


if __name__ == "__main__":
    unittest.main()
