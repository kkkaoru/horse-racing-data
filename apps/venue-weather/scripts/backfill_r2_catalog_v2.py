#!/usr/bin/env python3
"""Fail-closed, resumable backfill for the isolated venue-weather v2 Pipeline."""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

DEFAULT_PIPELINE_URL: Final[str] = (
    "https://venue-weather.kaoru.workers.dev/api/internal/backfill-r2-catalog-v2"
)
DEFAULT_BATCH_SIZE: Final[int] = 500


@dataclass(frozen=True, slots=True)
class Event:
    key: str
    payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class Summary:
    events: int
    requests: int
    skipped: int


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def discover_files(data_dir: Path, start: date, end: date) -> list[Path]:
    return [
        data_dir / f"venue_weather_v2_{year}.duckdb"
        for year in range(start.year, end.year + 1)
        if (data_dir / f"venue_weather_v2_{year}.duckdb").exists()
    ]


def validate_file(path: Path, start: date, end: date) -> int:
    """Reject the whole selected slice if any venue-date is not 24 complete rows."""
    connection = duckdb.connect(str(path), read_only=True)
    try:
        result = connection.execute(
            """
            with selected as (
              select * from venue_weather_v2
              where weather_date between ? and ?
            ), invalid as (
              select keibajo_code, weather_date
              from selected
              group by keibajo_code, weather_date
              having count(*) <> 24 or count(distinct weather_hour) <> 24
                or min(weather_hour) <> 0 or max(weather_hour) <> 23
                or count(relative_humidity) <> 24 or count(dew_point) <> 24
                or count(wet_bulb_temperature) <> 24
                or count(shortwave_radiation) <> 24
            )
            select (select count(*) from selected), (select count(*) from invalid)
            """,
            [start, end],
        ).fetchone()
    finally:
        connection.close()
    if result is None:
        raise RuntimeError(f"validation returned no result: {path}")
    rows, invalid = int(result[0]), int(result[1])
    if invalid:
        raise ValueError(f"incomplete v2 venue-date groups in {path}: {invalid}")
    return rows


def iter_events(path: Path, start: date, end: date, fetched_at: str) -> Iterator[Event]:
    connection = duckdb.connect(str(path), read_only=True)
    try:
        cursor = connection.execute(
            """
            select cast(weather_date as varchar), keibajo_code, weather_hour,
                   venue_name, latitude, longitude, temperature,
                   relative_humidity, dew_point, wet_bulb_temperature,
                   shortwave_radiation
            from venue_weather_v2
            where weather_date between ? and ?
            order by weather_date, keibajo_code, weather_hour
            """,
            [start, end],
        )
        while rows := cursor.fetchmany(DEFAULT_BATCH_SIZE):
            for row in rows:
                key = f"{row[0]}|{row[1]}|{int(row[2]):02d}"
                yield Event(
                    key=key,
                    payload={
                        "race_date": str(row[0]),
                        "keibajo_code": str(row[1]),
                        "weather_hour": int(row[2]),
                        "weather_data_type": "actual",
                        "venue_name": str(row[3]),
                        "latitude": float(row[4]),
                        "longitude": float(row[5]),
                        "weather_code": None,
                        "temperature": float(row[6]),
                        "precipitation": None,
                        "wind_speed": None,
                        "wind_gusts": None,
                        "relative_humidity": float(row[7]),
                        "dew_point": float(row[8]),
                        "wet_bulb_temperature": float(row[9]),
                        "shortwave_radiation": float(row[10]),
                        "fetched_at": fetched_at,
                    },
                )
    finally:
        connection.close()


def load_checkpoint(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    payload: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping) or not isinstance(payload.get("last_key"), str):
        raise TypeError("invalid v2 backfill checkpoint")
    return payload["last_key"]


def write_checkpoint(path: Path | None, key: str) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(json.dumps({"last_key": key}) + "\n", encoding="utf-8")
    temporary.replace(path)


def post_batch(url: str, token: str, payloads: Sequence[dict[str, object]]) -> None:
    request = urllib.request.Request(
        url,
        data=json.dumps(payloads).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-venue-weather-v2-backfill-token": token,
            "User-Agent": "horse-racing-data-weather-v2-backfill/1.0",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        response.read()


def backfill(
    *,
    files: Sequence[Path],
    start: date,
    end: date,
    pipeline_url: str,
    token: str,
    batch_size: int,
    checkpoint_path: Path | None,
    dry_run: bool,
    fetched_at: str,
) -> Summary:
    if end < start:
        raise ValueError("to-date must be on or after from-date")
    if batch_size < 1:
        raise ValueError("batch-size must be positive")
    expected_rows = sum(validate_file(path, start, end) for path in files)
    if expected_rows == 0:
        raise ValueError("selected v2 backfill slice has no rows")
    checkpoint = load_checkpoint(checkpoint_path)
    pending: list[Event] = []
    events = requests = skipped = 0

    def flush() -> None:
        nonlocal events, requests
        if not pending:
            return
        if not dry_run:
            post_batch(pipeline_url, token, [event.payload for event in pending])
            write_checkpoint(checkpoint_path, pending[-1].key)
        events += len(pending)
        requests += 1
        pending.clear()

    for path in files:
        for event in iter_events(path, start, end, fetched_at):
            if checkpoint is not None and event.key <= checkpoint:
                skipped += 1
                continue
            pending.append(event)
            if len(pending) == batch_size:
                flush()
    flush()
    return Summary(events=events, requests=requests, skipped=skipped)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--from-date", type=parse_date, required=True)
    parser.add_argument("--to-date", type=parse_date, required=True)
    parser.add_argument("--pipeline-url", default=DEFAULT_PIPELINE_URL)
    parser.add_argument(
        "--token", default=os.environ.get("VENUE_WEATHER_V2_BACKFILL_TOKEN")
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    files = discover_files(args.data_dir, args.from_date, args.to_date)
    if not files:
        raise RuntimeError("no v2 DuckDB file covers the requested dates")
    if not args.dry_run and not args.token:
        raise RuntimeError("VENUE_WEATHER_V2_BACKFILL_TOKEN or --token is required")
    summary = backfill(
        files=files,
        start=args.from_date,
        end=args.to_date,
        pipeline_url=args.pipeline_url,
        token=args.token or "dry-run",
        batch_size=args.batch_size,
        checkpoint_path=args.checkpoint,
        dry_run=args.dry_run,
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )
    print(json.dumps(asdict(summary), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
