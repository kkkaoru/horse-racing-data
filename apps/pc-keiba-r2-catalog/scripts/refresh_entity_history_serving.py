#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# ///
"""Refresh one Catalog year and atomically publish its serving projection."""

from __future__ import annotations

import argparse
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Final

DEFAULT_OUTPUT: Final[Path] = Path("tmp/entity-history-objects")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args(argv)
    if args.year < 1986 or args.year > 9998:
        parser.error("--year must be between 1986 and 9998")
    if args.concurrency < 1 or args.concurrency > 100:
        parser.error("--concurrency must be between 1 and 100")
    return args


def commands(args: argparse.Namespace) -> list[list[str]]:
    year = str(args.year)
    output = str(args.output)
    stages = [
        ["uv", "run", "scripts/sync_entity_history.py", "--year", year],
        [
            "uv",
            "run",
            "scripts/build_entity_history_objects.py",
            "--year",
            year,
            "--output",
            output,
        ],
        [
            "uv",
            "run",
            "scripts/pack_entity_history_objects.py",
            output,
            "--year",
            year,
        ],
    ]
    if not args.skip_upload:
        stages.append(
            [
                "uv",
                "run",
                "scripts/upload_entity_history_objects.py",
                output,
                "--year",
                year,
                "--concurrency",
                str(args.concurrency),
            ]
        )
    return stages


def run(args: argparse.Namespace) -> None:
    for command in commands(args):
        subprocess.run(command, check=True)


def main() -> int:
    run(parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
