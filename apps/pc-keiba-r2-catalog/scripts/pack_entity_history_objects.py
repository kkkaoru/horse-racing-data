#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# ///
"""Pack generated gzip members into range-readable yearly R2 objects."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Final

DEFAULT_OUTPUT: Final[Path] = Path("tmp/entity-history-objects")
OBJECT_VERSION: Final[int] = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--year", action="append", type=int)
    return parser.parse_args()


def pack_members(source: Path, destination: Path) -> dict[str, list[int]]:
    index: dict[str, list[int]] = {}
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    offset = 0
    with temporary.open("wb") as packed:
        for path in sorted(
            item for item in source.rglob("*.json.gz") if item.is_file()
        ):
            body = path.read_bytes()
            key = path.relative_to(source).as_posix().removesuffix(".json.gz")
            packed.write(body)
            index[key] = [offset, len(body)]
            offset += len(body)
    os.replace(temporary, destination)
    return index


def compact_json(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def pack_generation(
    output: Path, year: str, generation: str, destination: Path
) -> None:
    source = output / "data" / year / generation
    history = pack_members(source / "history", destination / "history.pack")
    target = pack_members(source / "target", destination / "target.pack")
    (destination / "index.json").write_bytes(
        compact_json({"history": history, "target": target, "version": OBJECT_VERSION})
    )
    print(
        json.dumps(
            {
                "event": "entity_history_year_packed",
                "history_members": len(history),
                "target_members": len(target),
                "year": year,
            },
            sort_keys=True,
        ),
        flush=True,
    )


def pack(output: Path, selected_years: set[str] | None = None) -> int:
    manifest = json.loads((output / "generations.json").read_text())
    years = manifest.get("years") if isinstance(manifest, dict) else None
    if not isinstance(years, dict):
        raise TypeError("generations.json is malformed")
    selected = {
        year: generation
        for year, generation in years.items()
        if selected_years is None or year in selected_years
    }
    if selected_years is not None and set(selected) != selected_years:
        missing = ", ".join(sorted(selected_years - set(selected)))
        raise ValueError(f"years are absent from generations.json: {missing}")
    if not all(
        isinstance(year, str) and isinstance(value, str)
        for year, value in selected.items()
    ):
        raise TypeError("generation mapping is malformed")

    packed_root = output / "packed"
    temporary_root = output / f"packed_build_{uuid.uuid4().hex[:8]}"
    shutil.rmtree(temporary_root, ignore_errors=True)
    try:
        for year, generation in sorted(selected.items()):
            pack_generation(
                output, year, generation, temporary_root / year / generation
            )
        if selected_years is None:
            shutil.rmtree(packed_root, ignore_errors=True)
            os.replace(temporary_root, packed_root)
        else:
            for year, generation in sorted(selected.items()):
                destination = packed_root / year / generation
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.rmtree(destination, ignore_errors=True)
                os.replace(temporary_root / year / generation, destination)
            shutil.rmtree(temporary_root, ignore_errors=True)
    except Exception:
        shutil.rmtree(temporary_root, ignore_errors=True)
        raise
    object_count = len(selected) * 3
    print(
        json.dumps(
            {"event": "entity_history_objects_packed", "objects": object_count},
            sort_keys=True,
        ),
        flush=True,
    )
    return object_count


def main() -> int:
    args = parse_args()
    years = None if args.year is None else {str(year) for year in args.year}
    pack(args.output, years)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
