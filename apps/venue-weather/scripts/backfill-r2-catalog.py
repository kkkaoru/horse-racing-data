#!/usr/bin/env python3
"""Backfill venue-weather DuckDB files through the Worker R2 Catalog path."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_WORKER_URL = "https://venue-weather.kaoru.workers.dev"
DEFAULT_PIPELINE_URL = "https://9171219290d34ec1b67e17371a726ff0.ingest.cloudflare.com"
DEFAULT_BATCH_SIZE = 50
INTERNAL_PATH = "/api/internal/backfill-r2-catalog"


@dataclass(frozen=True)
class WeatherPayload:
    fetched_at: str
    keibajo_code: str
    race_date: str
    rows: list[dict[str, Any]]
    venue: dict[str, Any]
    weather_type: str

    def to_worker_json(self) -> dict[str, Any]:
        return {
            "fetchedAt": self.fetched_at,
            "keibajoCode": self.keibajo_code,
            "raceDate": self.race_date,
            "rows": self.rows,
            "venue": self.venue,
            "weatherType": self.weather_type,
        }

    def to_pipeline_events(self) -> list[dict[str, Any]]:
        return [
            {
                "fetched_at": self.fetched_at,
                "keibajo_code": self.keibajo_code,
                "latitude": self.venue["lat"],
                "longitude": self.venue["lon"],
                "precipitation": row["precipitation"],
                "race_date": self.race_date,
                "temperature": row["temperature"],
                "venue_name": self.venue["name"],
                "weather_code": row["weatherCode"],
                "weather_data_type": self.weather_type,
                "weather_hour": row["hour"],
                "wind_gusts": row["windGusts"],
                "wind_speed": row["windSpeed"],
            }
            for row in self.rows
        ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="data", type=Path)
    parser.add_argument("--worker-url", default=os.environ.get("VENUE_WEATHER_URL", DEFAULT_WORKER_URL))
    parser.add_argument("--token", default=os.environ.get("VENUE_WEATHER_INTERNAL_TOKEN"))
    parser.add_argument(
        "--pipeline-url",
        default=os.environ.get("VENUE_WEATHER_PIPELINE_URL", DEFAULT_PIPELINE_URL),
    )
    parser.add_argument(
        "--pipeline-token",
        default=os.environ.get("VENUE_WEATHER_PIPELINE_TOKEN")
        or os.environ.get("CLOUDFLARE_DEBUG_TOKEN"),
    )
    parser.add_argument("--pipeline-no-auth", action="store_true")
    parser.add_argument("--batch-size", default=DEFAULT_BATCH_SIZE, type=int)
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--from-year", type=int)
    parser.add_argument("--to-year", type=int)
    parser.add_argument("--write-runtime-objects", action="store_true")
    parser.add_argument("--runtime-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def discover_files(data_dir: Path, from_year: int | None, to_year: int | None) -> list[Path]:
    files = sorted(data_dir.glob("venue_weather_*.duckdb"))
    selected: list[Path] = []
    for path in files:
        year = int(path.stem.removeprefix("venue_weather_"))
        if from_year is not None and year < from_year:
            continue
        if to_year is not None and year > to_year:
            continue
        selected.append(path)
    return selected


def fetch_payloads(
    path: Path, from_date: str | None = None, to_date: str | None = None
) -> Iterator[WeatherPayload]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        rows = con.execute(
            """
            select
              keibajo_code,
              cast(weather_date as varchar) as race_date,
              weather_hour,
              venue_name,
              latitude,
              longitude,
              weather_code,
              temperature,
              precipitation,
              wind_speed,
              wind_gusts,
              cast(fetched_at as varchar) as fetched_at
            from venue_weather
            where (? is null or cast(weather_date as varchar) >= ?)
              and (? is null or cast(weather_date as varchar) <= ?)
            order by race_date, keibajo_code, weather_hour
            """,
            [from_date, from_date, to_date, to_date],
        ).fetchall()
    finally:
        con.close()

    current_key: tuple[str, str] | None = None
    current_rows: list[dict[str, Any]] = []
    venue: dict[str, Any] = {}
    fetched_at = datetime.now(timezone.utc).isoformat()

    def flush() -> WeatherPayload | None:
        if current_key is None or not current_rows:
            return None
        race_date, keibajo_code = current_key
        return WeatherPayload(
            fetched_at=fetched_at,
            keibajo_code=keibajo_code,
            race_date=race_date,
            rows=current_rows.copy(),
            venue=venue.copy(),
            weather_type="actual",
        )

    for row in rows:
        (
            keibajo_code,
            race_date,
            weather_hour,
            venue_name,
            latitude,
            longitude,
            weather_code,
            temperature,
            precipitation,
            wind_speed,
            wind_gusts,
            row_fetched_at,
        ) = row
        next_key = (str(race_date), str(keibajo_code))
        if current_key is not None and next_key != current_key:
            payload = flush()
            if payload is not None:
                yield payload
            current_rows = []
        current_key = next_key
        fetched_at = str(row_fetched_at)
        venue = {"lat": latitude, "lon": longitude, "name": venue_name}
        current_rows.append(
            {
                "date": race_date,
                "hour": weather_hour,
                "precipitation": precipitation,
                "temperature": temperature,
                "weatherCode": weather_code,
                "windGusts": wind_gusts,
                "windSpeed": wind_speed,
            }
        )
    payload = flush()
    if payload is not None:
        yield payload


def post_batch(
    worker_url: str,
    token: str,
    batch: list[WeatherPayload],
    *,
    catalog_only: bool,
    runtime_only: bool,
) -> dict[str, Any]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    body = json.dumps(
        {
            "catalogOnly": catalog_only,
            "fetchedAt": fetched_at,
            "payloads": [payload.to_worker_json() for payload in batch],
            "runtimeOnly": runtime_only,
        }
    ).encode()
    request = urllib.request.Request(
        f"{worker_url.rstrip('/')}{INTERNAL_PATH}",
        data=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "horse-racing-data-venue-weather-backfill/1.0",
            "x-venue-weather-internal-token": token,
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def post_pipeline_batch(
    pipeline_url: str,
    pipeline_token: str | None,
    batch: list[WeatherPayload],
    *,
    no_auth: bool,
) -> dict[str, Any]:
    events = [event for payload in batch for event in payload.to_pipeline_events()]
    body = json.dumps(events).encode()
    auth_headers = {} if no_auth else {"Authorization": f"Bearer {pipeline_token}"}
    request = urllib.request.Request(
        pipeline_url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "horse-racing-data-venue-weather-backfill/1.0",
            **auth_headers,
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        raw = response.read().decode("utf-8")
    return {"events": len(events), "response": raw}


def main() -> int:
    args = parse_args()
    files = discover_files(args.data_dir, args.from_year, args.to_year)
    if not files:
        print(f"no venue_weather_*.duckdb files under {args.data_dir}", file=sys.stderr)
        return 1
    if not args.dry_run and args.write_runtime_objects and not args.token:
        print(
            "VENUE_WEATHER_INTERNAL_TOKEN is required with --write-runtime-objects",
            file=sys.stderr,
        )
        return 1
    if args.runtime_only and not args.write_runtime_objects:
        print("--runtime-only requires --write-runtime-objects", file=sys.stderr)
        return 1
    if (
        not args.dry_run
        and not args.write_runtime_objects
        and not args.pipeline_no_auth
        and not args.pipeline_token
    ):
        print(
            "VENUE_WEATHER_PIPELINE_TOKEN or CLOUDFLARE_DEBUG_TOKEN is required unless --dry-run is set",
            file=sys.stderr,
        )
        return 1

    total_payloads = 0
    total_rows = 0
    batch: list[WeatherPayload] = []
    for path in files:
        print(f"reading {path}", file=sys.stderr)
        for payload in fetch_payloads(path, args.from_date, args.to_date):
            batch.append(payload)
            total_payloads += 1
            total_rows += len(payload.rows)
            if len(batch) >= args.batch_size:
                if args.dry_run:
                    print(f"dry-run batch payloads={len(batch)} rows={sum(len(p.rows) for p in batch)}")
                else:
                    result = (
                        post_batch(
                            args.worker_url,
                            args.token,
                            batch,
                            catalog_only=False,
                            runtime_only=args.runtime_only,
                        )
                        if args.write_runtime_objects
                        else post_pipeline_batch(
                            args.pipeline_url,
                            args.pipeline_token,
                            batch,
                            no_auth=args.pipeline_no_auth,
                        )
                    )
                    print(json.dumps(result, ensure_ascii=False), file=sys.stderr)
                batch = []
    if batch:
        if args.dry_run:
            print(f"dry-run batch payloads={len(batch)} rows={sum(len(p.rows) for p in batch)}")
        else:
            result = (
                post_batch(
                    args.worker_url,
                    args.token,
                    batch,
                    catalog_only=False,
                    runtime_only=args.runtime_only,
                )
                if args.write_runtime_objects
                else post_pipeline_batch(
                    args.pipeline_url,
                    args.pipeline_token,
                    batch,
                    no_auth=args.pipeline_no_auth,
                )
            )
            print(json.dumps(result, ensure_ascii=False), file=sys.stderr)
    print(f"done payloads={total_payloads} rows={total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
