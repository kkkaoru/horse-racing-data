#!/usr/bin/env python3
"""Tests for the fail-fast direct-Catalog serving refresh orchestrator."""

from __future__ import annotations

import argparse
import subprocess
import unittest
from pathlib import Path
from unittest.mock import call, patch

from refresh_entity_history_serving import commands, parse_args, run


class RefreshEntityHistoryServingTest(unittest.TestCase):
    def test_commands_refresh_raw_and_history_before_manifest(self) -> None:
        args = argparse.Namespace(
            output=Path("/tmp/entity-catalog-manifest.json"),
            skip_upload=False,
            year=2026,
        )
        self.assertEqual(
            commands(args),
            [
                ["uv", "run", "scripts/sync_r2_catalog.py"],
                ["uv", "run", "scripts/sync_entity_history.py", "--year", "2026"],
                [
                    "uv",
                    "run",
                    "scripts/publish_entity_catalog_manifest.py",
                    "--output",
                    "/tmp/entity-catalog-manifest.json",
                ],
            ],
        )

    def test_skip_upload_builds_manifest_without_publication(self) -> None:
        args = parse_args(["--year", "2026", "--skip-upload"])
        self.assertEqual(
            commands(args)[2],
            [
                "uv",
                "run",
                "scripts/publish_entity_catalog_manifest.py",
                "--output",
                "tmp/entity-catalog-manifest.json",
                "--skip-upload",
            ],
        )

    def test_run_is_fail_fast(self) -> None:
        args = parse_args(["--year", "2026", "--skip-upload"])
        with (
            patch(
                "refresh_entity_history_serving.subprocess.run",
                side_effect=[None, subprocess.CalledProcessError(1, "history")],
            ) as run_mock,
            self.assertRaises(subprocess.CalledProcessError),
        ):
            run(args)
        self.assertEqual(
            run_mock.call_args_list,
            [
                call(["uv", "run", "scripts/sync_r2_catalog.py"], check=True),
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
            ],
        )

    def test_parse_args_validates_year_bounds(self) -> None:
        with self.assertRaises(SystemExit):
            parse_args(["--year", "1985"])
        with self.assertRaises(SystemExit):
            parse_args(["--year", "9999"])


if __name__ == "__main__":
    unittest.main()
