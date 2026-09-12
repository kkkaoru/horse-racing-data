"""Export the canonical JRA cell and 20-year PIT training-scope manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Final, LiteralString, cast

import psycopg
from psycopg.rows import tuple_row

from predict_lib.jra_cell_scope import (
    EVALUATION_YEAR_FROM,
    EVALUATION_YEAR_TO,
    MINIMUM_DIVERSE_TRAINING_RACES,
    OPEN_CLASS_CODE,
    JraCellKey,
    JraRace,
    JraRaceIndex,
    group_observed_cells,
)

MANIFEST_VERSION: Final[str] = "jra-cell-v2"
SCOPE_SELECTOR_VERSION: Final[str] = "cell-cohort-entrant-history-20y-v6"
PRIORITY_SCOPE_OVERRIDES: Final[dict[str, tuple[str, int, float]]] = {
    "jra:2026:0905:06:01": ("venue-surface-track-bias", 6, 0.05),
    "jra:2026:0905:06:11": ("target-entrant-history", 6, 0.05),
    "jra:2026:0905:09:11": ("venue-surface-track-bias", 8, 0.05),
    "jra:2026:0905:01:11": ("related-distance-track-bias", 6, 0.05),
}
DEFAULT_FROM_DATE: Final[str] = "20000101"
DEFAULT_TO_DATE: Final[str] = "20260905"
RACE_QUERY: Final[LiteralString] = """
select
  ra.kaisai_nen,
  ra.kaisai_tsukihi,
  ra.keibajo_code,
  ra.race_bango,
  ra.kyori,
  ra.track_code,
  ra.kyoso_joken_code,
  ra.grade_code,
  ra.kyosomei_hondai,
  ra.kyoso_shubetsu_code,
  ra.juryo_shubetsu_code,
  array_agg(distinct nullif(trim(se.ketto_toroku_bango), ''))
    filter (where nullif(trim(se.ketto_toroku_bango), '') is not null)
from jvd_ra ra
join jvd_se se
  using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
where ra.kaisai_nen || ra.kaisai_tsukihi between %s and %s
  and coalesce(trim(ra.data_kubun), '') not in ('0', '9')
  and coalesce(trim(se.ijo_kubun_code), '0') not in ('1', '2', '3')
group by
  ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango,
  ra.kyori, ra.track_code, ra.kyoso_joken_code, ra.grade_code,
  ra.kyosomei_hondai, ra.kyoso_shubetsu_code, ra.juryo_shubetsu_code
order by ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango
"""

RaceRow = tuple[
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    list[str],
]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="build_jra_cell_manifest")
    parser.add_argument("--pg-url", default=os.environ.get("DATABASE_URL_LOCAL"))
    parser.add_argument("--from-date", default=DEFAULT_FROM_DATE)
    parser.add_argument("--to-date", default=DEFAULT_TO_DATE)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--production-target-date", default=None)
    args = parser.parse_args(argv)
    if not args.pg_url:
        parser.error("--pg-url or DATABASE_URL_LOCAL is required")
    return args


def _race_from_row(row: RaceRow) -> JraRace:
    (
        year,
        month_day,
        venue,
        race_number,
        distance,
        track_code,
        condition_code,
        grade_code,
        race_name,
        race_type_code,
        weight_type_code,
        horse_ids,
    ) = row
    return JraRace(
        race_id=f"jra:{year}:{month_day}:{venue}:{race_number}",
        race_date=datetime.strptime(f"{year}{month_day}", "%Y%m%d").date(),
        venue=venue,
        distance=int(distance),
        track_code=track_code,
        condition_code=condition_code,
        grade_code=grade_code,
        race_name=race_name,
        race_type_code=race_type_code,
        weight_type_code=weight_type_code,
        horse_ids=tuple(sorted(horse_ids)),
    )


def load_races(pg_url: str, from_date: str, to_date: str) -> list[JraRace]:
    with (
        psycopg.connect(pg_url, row_factory=tuple_row) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(RACE_QUERY, (from_date, to_date))
        return [_race_from_row(cast(RaceRow, row)) for row in cursor]


def _digest_ids(values: Sequence[str]) -> str:
    payload = "\n".join(values).encode()
    return hashlib.sha256(payload).hexdigest()


def _cell_payload(cell: JraCellKey) -> dict[str, object]:
    return {
        "cell_id": cell.cell_id,
        "canonical": cell.canonical,
        "venue": cell.venue,
        "distance": cell.distance,
        "season": cell.season,
        "surface": cell.surface,
        "condition_code": cell.condition_code,
        "race_identity": cell.race_identity,
        "model_version": f"{MANIFEST_VERSION}-{cell.cell_id.removeprefix('jra-cell-')}",
    }


def build_manifest(races: Sequence[JraRace], generated_at: str) -> dict[str, object]:
    observed = group_observed_cells(races)
    index = JraRaceIndex(races)
    entries: list[dict[str, object]] = []
    used_ids: dict[str, str] = {}
    insufficient_scopes = 0
    for cell, target_races in sorted(observed.items(), key=lambda item: item[0].canonical):
        previous = used_ids.setdefault(cell.cell_id, cell.canonical)
        if previous != cell.canonical:
            raise ValueError(f"cell id collision: {cell.cell_id}")
        folds: list[dict[str, object]] = []
        for year in range(EVALUATION_YEAR_FROM, EVALUATION_YEAR_TO + 1):
            scope = index.build_fold_scope(cell, year)
            if not scope.evaluation_race_ids:
                continue
            insufficient_scopes += int(not scope.covers_evaluation_size)
            folds.append(
                {
                    "evaluation_year": year,
                    "cutoff": scope.cutoff.isoformat(),
                    "history_start": scope.history_start.isoformat(),
                    "seed_race_count": len(scope.seed_race_ids),
                    "seed_horse_count": len(scope.seed_horse_ids),
                    "cell_history_seed_race_count": len(scope.cell_history_seed_race_ids),
                    "cell_history_seed_race_sha256": _digest_ids(scope.cell_history_seed_race_ids),
                    "training_race_count": len(scope.training_race_ids),
                    "training_runner_count": scope.training_horse_rows,
                    "evaluation_race_count": len(scope.evaluation_race_ids),
                    "evaluation_runner_count": scope.evaluation_horse_rows,
                    "covers_evaluation_size": scope.covers_evaluation_size,
                    "training_scope_mode": scope.training_scope_mode,
                    "related_seed_race_count": scope.related_seed_race_count,
                    "training_target_cell_race_count": scope.training_target_cell_race_count,
                    "training_cross_cell_race_count": scope.training_cross_cell_race_count,
                    "training_scope_is_diverse": scope.is_diverse,
                    "seed_horse_sha256": _digest_ids(scope.seed_horse_ids),
                    "training_race_sha256": _digest_ids(scope.training_race_ids),
                    "evaluation_race_sha256": _digest_ids(scope.evaluation_race_ids),
                }
            )
        payload = _cell_payload(cell)
        payload.update(
            {
                "target_race_count": len(target_races),
                "target_year_from": min(race.race_date.year for race in target_races),
                "target_year_to": max(race.race_date.year for race in target_races),
                "folds": folds,
                "training_scope": {
                    "selector_version": SCOPE_SELECTOR_VERSION,
                    "lookback_years": 20,
                    "excluded_race_data_kubun": ["0", "9"],
                    "excluded_nonstarter_codes": ["1", "2", "3"],
                    "seed": (
                        "entrants of every target-cell race in the prior 20 years plus "
                        "evaluation entrants; related-course cohorts only expand this population"
                    ),
                    "expansion": (
                        "all prior races of every seed horse across every venue/cell; "
                        "target-cell-only training is forbidden"
                    ),
                    "related_distance_meters": 400,
                    "target_cell_only_training_allowed": False,
                    "future_outcomes_allowed": False,
                    "target_day_outcomes_allowed": False,
                },
            }
        )
        entries.append(payload)
    open_entries = [entry for entry in entries if entry["condition_code"] == OPEN_CLASS_CODE]
    generic_open = [entry for entry in open_entries if entry["race_identity"] is None]
    if generic_open:
        raise ValueError("generic JRA 999 cells are forbidden")
    return {
        "version": MANIFEST_VERSION,
        "generated_at": generated_at,
        "category": "jra",
        "evaluation_year_from": EVALUATION_YEAR_FROM,
        "evaluation_year_to": EVALUATION_YEAR_TO,
        "training_lookback_years": 20,
        "cell_dimensions": ["venue", "distance", "season", "surface", "condition_code"],
        "condition_999_additional_dimension": "race_identity",
        "summary": {
            "source_race_count": len(races),
            "cell_count": len(entries),
            "open_999_cell_count": len(open_entries),
            "generic_999_cell_count": len(generic_open),
            "insufficient_fold_scope_count": insufficient_scopes,
        },
        "cells": entries,
    }


def build_production_plan(races: Sequence[JraRace], target_date: date) -> dict[str, object]:
    index = JraRaceIndex(races)
    grouped = group_observed_cells(
        (race for race in races if race.race_date == target_date),
        year_from=target_date.year,
        year_to=target_date.year,
    )
    cells: list[dict[str, object]] = []
    for cell, target_races in sorted(grouped.items(), key=lambda item: item[0].canonical):
        override_items = [
            PRIORITY_SCOPE_OVERRIDES[race.race_id]
            for race in target_races
            if race.race_id in PRIORITY_SCOPE_OVERRIDES
        ]
        if len(set(override_items)) > 1:
            raise ValueError(f"conflicting JRA priority scope overrides: {cell.cell_id}")
        override = override_items[0] if override_items else None
        if override is None:
            scope = index.build_target_date_scope(cell, target_date)
            training_parameters = {
                "iterations": 250,
                "depth": 8,
                "learning_rate": 0.05,
            }
        else:
            strategy, depth, learning_rate = override
            if strategy == "target-entrant-history":
                scope = index.build_entrant_scope(cell, target_date, target_races)
            elif strategy == "related-distance-track-bias":
                scope = index.build_related_distance_scope(cell, target_date, target_races)
            elif strategy == "venue-surface-track-bias":
                scope = index.build_venue_surface_scope(cell, target_date, target_races)
            else:
                raise ValueError(f"unsupported JRA priority scope override: {strategy}")
            training_parameters = {
                "iterations": 100,
                "depth": depth,
                "learning_rate": learning_rate,
            }
        if len(scope.training_race_ids) < MINIMUM_DIVERSE_TRAINING_RACES:
            raise ValueError(
                f"JRA production cell has fewer than {MINIMUM_DIVERSE_TRAINING_RACES} "
                f"training races after related-course expansion: {cell.cell_id}"
            )
        if not scope.is_diverse:
            raise ValueError(
                f"JRA production training scope matches its target cell: {cell.cell_id}"
            )
        payload = _cell_payload(cell)
        payload.update(
            {
                "model_version": f"{payload['model_version']}-asof-{target_date:%Y%m%d}",
                "target_race_ids": [race.race_id for race in target_races],
                "target_runner_count": scope.evaluation_horse_rows,
                "seed_race_count": len(scope.seed_race_ids),
                "seed_horse_count": len(scope.seed_horse_ids),
                "scope_selector_version": SCOPE_SELECTOR_VERSION,
                "seed_race_ids": list(scope.seed_race_ids),
                "seed_horse_ids": list(scope.seed_horse_ids),
                "cell_history_seed_race_ids": list(scope.cell_history_seed_race_ids),
                "training_race_count": len(scope.training_race_ids),
                "training_runner_count": scope.training_horse_rows,
                "training_scope_mode": scope.training_scope_mode,
                "related_seed_race_count": scope.related_seed_race_count,
                "training_target_cell_race_count": scope.training_target_cell_race_count,
                "training_cross_cell_race_count": scope.training_cross_cell_race_count,
                "training_scope_is_diverse": scope.is_diverse,
                "training_parameters": training_parameters,
                "covers_target_size": scope.covers_evaluation_size,
                "training_race_ids": list(scope.training_race_ids),
                "training_race_sha256": _digest_ids(scope.training_race_ids),
                "cutoff": scope.cutoff.isoformat(),
                "history_start": scope.history_start.isoformat(),
            }
        )
        cells.append(payload)
    return {
        "version": MANIFEST_VERSION,
        "category": "jra",
        "target_date": target_date.isoformat(),
        "generic_999_model_allowed": False,
        "cells": cells,
    }


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        Path(temporary_name).unlink(missing_ok=True)
        raise


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    races = load_races(args.pg_url, args.from_date, args.to_date)
    manifest = build_manifest(races, generated_at=datetime.now().astimezone().isoformat())
    encoded = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    output = args.output_dir / "manifest.json"
    atomic_write(output, encoded)
    production_output: str | None = None
    if args.production_target_date is not None:
        target_date = datetime.strptime(args.production_target_date, "%Y%m%d").date()
        production_plan = build_production_plan(races, target_date)
        production_encoded = (
            json.dumps(production_plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        )
        production_path = args.output_dir / f"production-{args.production_target_date}.json"
        atomic_write(production_path, production_encoded)
        production_output = str(production_path)
    print(
        json.dumps(
            {
                "output": str(output),
                "production_output": production_output,
                "sha256": hashlib.sha256(encoded.encode()).hexdigest(),
                **cast(dict[str, object], manifest["summary"]),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
