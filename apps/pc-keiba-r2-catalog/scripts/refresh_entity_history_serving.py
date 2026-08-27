#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# ///
"""Refresh one Catalog year and atomically publish its direct-serving index."""

from __future__ import annotations

import argparse
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Final

DEFAULT_OUTPUT: Final[Path] = Path("tmp/entity-catalog-manifest.json")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args(argv)
    if args.year < 1986 or args.year > 9998:
        parser.error("--year must be between 1986 and 9998")
    return args


def commands(args: argparse.Namespace) -> list[list[str]]:
    year = str(args.year)
    publish = [
        "uv",
        "run",
        "scripts/publish_entity_catalog_manifest.py",
        "--output",
        str(args.output),
    ]
    if args.skip_upload:
        publish.append("--skip-upload")
    return [
        ["uv", "run", "scripts/sync_r2_catalog.py"],
        ["uv", "run", "scripts/sync_entity_history.py", "--year", year],
        publish,
    ]


def run(args: argparse.Namespace) -> None:
    for command in commands(args):
        subprocess.run(command, check=True)


def main() -> int:
    run(parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
