#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.5,<1.6",
#   "pyarrow>=19",
#   "pyiceberg[pyarrow,s3fs]>=0.11,<0.12",
# ]
# ///
"""Build immutable R2 objects for low-latency race entity history reads."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import uuid
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Final

import duckdb
from sync_entity_history import (
    MIN_HISTORY_YEAR,
    connect_source,
    extract_target_year,
    extract_year,
    source_years,
)
from sync_r2_catalog import emit, load_settings

OBJECT_VERSION: Final[int] = 1
DEFAULT_OUTPUT: Final[Path] = Path("tmp/entity-history-objects")
ENTITY_TYPES: Final[frozenset[str]] = frozenset({"horse", "jockey", "trainer", "owner"})
SOURCES: Final[frozenset[str]] = frozenset({"jra", "nar"})
BUCKETS: Final[frozenset[str]] = frozenset("0123456789abcdef")


def parse_year(value: str) -> str:
    if (
        len(value) != 4
        or not value.isdigit()
        or not MIN_HISTORY_YEAR <= value <= "9998"
    ):
        raise argparse.ArgumentTypeError("year must use YYYY and be at least 1986")
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--full", action="store_true")
    mode.add_argument("--year", type=parse_year)
    mode.add_argument("--targets-only", action="store_true")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args(argv)


def compact_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def write_gzip_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with (
        temporary.open("wb") as raw_output,
        gzip.GzipFile(
            fileobj=raw_output, mode="wb", compresslevel=6, mtime=0
        ) as output,
    ):
        output.write(compact_json(value))
    os.replace(temporary, path)


def required_text(row: dict[str, object], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"entity history object row is missing {name}")
    return value


def validate_partition(row: dict[str, object], year: str) -> tuple[str, str, str]:
    entity_type = required_text(row, "entity_type")
    source = required_text(row, "source")
    bucket = required_text(row, "entity_bucket")
    if entity_type not in ENTITY_TYPES:
        raise ValueError(f"unsupported entity type: {entity_type}")
    if source not in SOURCES:
        raise ValueError(f"unsupported source: {source}")
    if bucket not in BUCKETS:
        raise ValueError(f"unsupported entity bucket: {bucket}")
    if required_text(row, "kaisai_nen") != year:
        raise ValueError("object row year does not match the requested year")
    return entity_type, source, bucket


def grouped_rows(
    rows: Iterable[dict[str, object]], year: str
) -> tuple[
    dict[tuple[str, str, str, str], list[dict[str, object]]],
    dict[tuple[str, str], list[dict[str, object]]],
]:
    history: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(
        list
    )
    targets: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        entity_type, source, bucket = validate_partition(row, year)
        entity_id = required_text(row, "entity_id")
        shard = entity_id[-1]
        if not shard.isdigit():
            raise ValueError("entity ID must end in a decimal shard")
        history[(entity_type, source, bucket, shard)].append(row)
        targets[(source, required_text(row, "kaisai_tsukihi"))].append(row)
    return dict(history), dict(targets)


def write_target_objects(
    root: Path,
    targets: dict[tuple[str, str], list[dict[str, object]]],
) -> int:
    for (source, month_day), target_rows in targets.items():
        write_gzip_json(
            root / "target" / source / f"{month_day}.json.gz",
            {"rows": target_rows, "version": OBJECT_VERSION},
        )
    return len(targets)


def write_year_objects(
    output: Path,
    year: str,
    generation: str,
    rows: Iterable[dict[str, object]],
    target_rows: Iterable[dict[str, object]] | None = None,
) -> tuple[int, int]:
    history, default_targets = grouped_rows(rows, year)
    targets = (
        default_targets if target_rows is None else grouped_rows(target_rows, year)[1]
    )
    root = output / "data" / year / generation
    for (entity_type, source, bucket, shard), partition_rows in history.items():
        write_gzip_json(
            root / "history" / entity_type / source / f"{bucket}-{shard}.json.gz",
            {"rows": partition_rows, "version": OBJECT_VERSION},
        )
    return len(history), write_target_objects(root, targets)


def read_generations(output: Path) -> dict[str, str]:
    path = output / "generations.json"
    if not path.exists():
        return {}
    value = json.loads(path.read_text())
    years = value.get("years") if isinstance(value, dict) else None
    if not isinstance(years, dict) or not all(
        isinstance(year, str) and isinstance(generation, str)
        for year, generation in years.items()
    ):
        raise ValueError("existing generations.json is malformed")
    return dict(years)


def write_generations(output: Path, generations: dict[str, str]) -> None:
    path = output / "generations.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_bytes(
        compact_json({"version": OBJECT_VERSION, "years": generations})
    )
    os.replace(temporary, path)


def build_year(
    connection: duckdb.DuckDBPyConnection,
    output: Path,
    year: str,
    *,
    include_unfinished_targets: bool,
) -> tuple[str, int]:
    generation = uuid.uuid4().hex[:16]
    data = extract_year(connection, year)
    target_data = (
        extract_target_year(connection, year) if include_unfinished_targets else data
    )
    history_count, target_count = write_year_objects(
        output,
        year,
        generation,
        data.to_pylist(),
        target_data.to_pylist(),
    )
    emit(
        "entity_history_objects_year_built",
        year=year,
        generation=generation,
        rows=data.num_rows,
        target_rows=target_data.num_rows,
        history_objects=history_count,
        target_objects=target_count,
    )
    return generation, data.num_rows


def build_full(connection: duckdb.DuckDBPyConnection, output: Path) -> int:
    years = source_years(connection)
    if not years:
        raise RuntimeError("entity history source contains no supported years")
    temporary = output.with_name(f"{output.name}_build_{uuid.uuid4().hex[:8]}")
    shutil.rmtree(temporary, ignore_errors=True)
    generations: dict[str, str] = {}
    total_rows = 0
    try:
        latest_year = years[-1]
        for year in years:
            generation, rows = build_year(
                connection,
                temporary,
                year,
                include_unfinished_targets=year == latest_year,
            )
            generations[year] = generation
            total_rows += rows
        write_generations(temporary, generations)
        if output.exists():
            shutil.rmtree(output)
        os.replace(temporary, output)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    emit("entity_history_objects_full_complete", rows=total_rows, years=len(years))
    return total_rows


def refresh_year(connection: duckdb.DuckDBPyConnection, output: Path, year: str) -> int:
    generations = read_generations(output)
    if not generations:
        raise RuntimeError("generations.json is missing; run --full first")
    generation, rows = build_year(
        connection, output, year, include_unfinished_targets=True
    )
    generations[year] = generation
    write_generations(output, generations)
    emit(
        "entity_history_objects_year_complete",
        year=year,
        generation=generation,
        rows=rows,
    )
    return rows


def refresh_all_targets(connection: duckdb.DuckDBPyConnection, output: Path) -> int:
    generations = read_generations(output)
    if not generations:
        raise RuntimeError("generations.json is missing; run --full first")
    total_rows = 0
    latest_year = max(generations)
    for year, generation in sorted(generations.items()):
        data = (
            extract_target_year(connection, year)
            if year == latest_year
            else extract_year(connection, year)
        )
        targets = grouped_rows(data.to_pylist(), year)[1]
        count = write_target_objects(output / "data" / year / generation, targets)
        total_rows += data.num_rows
        emit(
            "entity_history_object_targets_built",
            year=year,
            generation=generation,
            rows=data.num_rows,
            target_objects=count,
        )
    emit("entity_history_object_targets_complete", rows=total_rows)
    return total_rows


def run(args: argparse.Namespace) -> int:
    settings = load_settings()
    connection = connect_source(settings)
    try:
        if args.full:
            return build_full(connection, args.output)
        if args.targets_only:
            return refresh_all_targets(connection, args.output)
        return refresh_year(connection, args.output, args.year)
    finally:
        connection.close()


def main() -> int:
    run(parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
