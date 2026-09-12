"""Optimize four priority JRA cells without ever training on target-cell-only data."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Final, cast

import pyarrow.dataset as ds

from build_jra_cell_manifest import atomic_write, load_races
from evaluate_jra_cell_models import aggregate_evaluations, evaluate_fold
from predict_lib.jra_cell_scope import (
    MINIMUM_DIVERSE_TRAINING_RACES,
    JraCellKey,
    JraFoldScope,
    JraRace,
    JraRaceIndex,
    cell_for_race,
    derive_surface,
    group_observed_cells,
)
from predict_lib.rank_relevance import DEFAULT_RELEVANCE_MODE, RELEVANCE_MODES
from train_jra_cell_models import DatasetLike, numeric_feature_names

REPORT_VERSION: Final[str] = "jra-priority-cell-optimization-v6"
TARGET_DATE: Final[date] = date(2026, 9, 5)
PRIORITY_RACES: Final[dict[str, str]] = {
    "nakayama-1r-obstacle-open": "jra:2026:0905:06:01",
    "nakayama-11r-keisei-hai-ah": "jra:2026:0905:06:11",
    "hanshin-11r-enif-stakes": "jra:2026:0905:09:11",
    "sapporo-11r-sapporo-2yo-stakes": "jra:2026:0905:01:11",
}
DEVELOPMENT_YEARS: Final[tuple[int, ...]] = (2020, 2021, 2022, 2023)
HOLDOUT_YEARS: Final[tuple[int, ...]] = (2024, 2025, 2026)
MODEL_CONFIGS: Final[tuple[tuple[int, float], ...]] = ((6, 0.05), (8, 0.05))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="optimize_priority_jra_cells")
    parser.add_argument("--pg-url", default=os.environ.get("DATABASE_URL_LOCAL"))
    parser.add_argument("--features-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checkpoint-dir", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--thread-count", type=int, default=6)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--development-only", action="store_true")
    parser.add_argument("--development-year-from", type=int, default=DEVELOPMENT_YEARS[0])
    parser.add_argument("--development-year-to", type=int, default=DEVELOPMENT_YEARS[-1])
    parser.add_argument("--relevance-mode", choices=RELEVANCE_MODES, default=DEFAULT_RELEVANCE_MODE)
    parser.add_argument("--target-date", type=date.fromisoformat, default=TARGET_DATE)
    parser.add_argument("--production-plan", type=Path)
    parser.add_argument("--priority-race", action="append", default=None)
    parser.add_argument(
        "--input-revision",
        required=True,
        help="Immutable revision covering the PostgreSQL export and feature snapshot",
    )
    args = parser.parse_args(argv)
    if not args.pg_url:
        parser.error("--pg-url or DATABASE_URL_LOCAL is required")
    if not 2000 <= args.development_year_from <= args.development_year_to < min(HOLDOUT_YEARS):
        parser.error("development years must be ordered and strictly before holdout years")
    args.development_years = tuple(range(args.development_year_from, args.development_year_to + 1))
    if not args.input_revision.strip():
        parser.error("--input-revision must not be empty")
    if args.production_plan is not None and args.priority_race is not None:
        parser.error("--production-plan and --priority-race are mutually exclusive")
    args.priority_races = (
        priority_races_from_plan(args.production_plan, target_date=args.target_date)
        if args.production_plan is not None
        else parse_priority_races(args.priority_race, target_date=args.target_date)
    )
    return args


def priority_races_from_plan(path: Path, *, target_date: date) -> dict[str, str]:
    """Use one representative target race for every unique production cell."""
    payload: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("target_date") != target_date.isoformat():
        raise ValueError("production plan target date does not match --target-date")
    cells = payload.get("cells")
    if not isinstance(cells, list):
        raise ValueError("production plan cells must be a list")
    values: list[str] = []
    for cell in cells:
        if not isinstance(cell, dict):
            raise ValueError("production plan cell must be an object")
        cell_id = cell.get("cell_id")
        target_race_ids = cell.get("target_race_ids")
        if (
            not isinstance(cell_id, str)
            or not isinstance(target_race_ids, list)
            or not target_race_ids
            or not isinstance(target_race_ids[0], str)
        ):
            raise ValueError("production plan cell identity or target races are invalid")
        values.append(f"{cell_id}={target_race_ids[0]}")
    return parse_priority_races(values, target_date=target_date)


def parse_priority_races(
    values: Sequence[str] | None,
    *,
    target_date: date,
) -> dict[str, str]:
    if values is None:
        if target_date != TARGET_DATE:
            raise ValueError("explicit --priority-race values are required for a new target date")
        return dict(PRIORITY_RACES)
    result: dict[str, str] = {}
    expected_prefix = f"jra:{target_date:%Y:%m%d}:"
    for value in values:
        label, separator, race_id = value.partition("=")
        parts = race_id.split(":")
        if (
            not separator
            or not label.strip()
            or label in result
            or race_id in result.values()
            or not race_id.startswith(expected_prefix)
            or len(parts) != 5
            or parts[3] not in {f"{v:02d}" for v in range(1, 11)}
            or parts[4] not in {f"{v:02d}" for v in range(1, 13)}
        ):
            raise ValueError(
                "priority race must be unique label=jra:YYYY:MMDD:VV:RR for target date"
            )
        result[label] = race_id
    if not result:
        raise ValueError("at least one priority race is required")
    return result


def resolve_priority_cells(
    races: Sequence[JraRace],
    *,
    priority_races: Mapping[str, str] = PRIORITY_RACES,
) -> dict[str, JraCellKey]:
    by_id = {race.race_id: race for race in races}
    missing = sorted(set(priority_races.values()) - by_id.keys())
    if missing:
        raise ValueError(f"priority JRA races are missing: {','.join(missing)}")
    return {label: cell_for_race(by_id[race_id]) for label, race_id in priority_races.items()}


def evaluation_races_for_year(
    all_races: Sequence[JraRace],
    exact_races: Sequence[JraRace],
    cell: JraCellKey,
    year: int,
    *,
    target_date: date = TARGET_DATE,
) -> tuple[tuple[JraRace, ...], str]:
    observed_exact = tuple(
        race for race in exact_races if race.race_date.year == year and race.race_date < target_date
    )
    if observed_exact:
        return observed_exact, "exact-cell"
    maximum_delta = 800 if cell.surface == "obstacle" else 400
    proxies = [
        race
        for race in all_races
        if race.race_date.year == year
        and race.race_date < target_date
        and race.venue.strip() == cell.venue
        and derive_surface(race.track_code) == cell.surface
        and abs(race.distance - cell.distance) <= maximum_delta
    ]
    proxies.sort(key=lambda race: (race.race_date, race.race_id), reverse=True)
    return tuple(proxies[:32]), "related-course-evaluation-proxy"


def scope_for_strategy(
    index: JraRaceIndex,
    cell: JraCellKey,
    cutoff: date,
    evaluation_races: Sequence[JraRace],
    strategy: str,
) -> JraFoldScope:
    builders: dict[str, Callable[[JraCellKey, date, Sequence[JraRace]], JraFoldScope]] = {
        "entrant-history": index.build_entrant_scope,
        "related-distance-track-bias": index.build_related_distance_scope,
        "venue-surface-track-bias": index.build_venue_surface_scope,
    }
    try:
        return builders[strategy](cell, cutoff, evaluation_races)
    except KeyError as error:
        raise ValueError(f"unknown JRA scope strategy: {strategy}") from error


def _candidate_key(strategy: str, depth: int, learning_rate: float) -> str:
    return f"{strategy}__depth-{depth}__lr-{learning_rate:g}"


def build_checkpoint_path(
    root: Path,
    cell: JraCellKey,
    candidate_key: str,
    year: int,
    *,
    args: argparse.Namespace,
) -> Path:
    production_plan = getattr(args, "production_plan", None)
    contract = {
        "version": REPORT_VERSION,
        "candidate": candidate_key,
        "target_date": args.target_date.isoformat(),
        "iterations": args.iterations,
        "relevance_mode": getattr(args, "relevance_mode", DEFAULT_RELEVANCE_MODE),
        "thread_count": args.thread_count,
        "input_revision": args.input_revision,
        "production_plan_sha256": (
            hashlib.sha256(production_plan.read_bytes()).hexdigest()
            if isinstance(production_plan, Path)
            else None
        ),
        "features_root": str(args.features_root.resolve()),
    }
    digest = hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()
    return root / cell.cell_id / f"{digest}-{year}.json"


def _write_json(path: Path, payload: object) -> None:
    atomic_write(path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _annual_guard_passes(aggregate: dict[str, object]) -> bool:
    by_year = cast(dict[str, dict[str, object]], aggregate["by_year"])
    return all(
        int(cast(dict[str, int], year_result["delta_hits"])[f"top{depth}"]) >= 0
        for year_result in by_year.values()
        for depth in range(2, 6)
    )


def _candidate_sort_key(candidate: dict[str, object]) -> tuple[int, int, int, int, int]:
    delta = cast(
        dict[str, int], cast(dict[str, object], candidate["aggregate"])["market_delta_hits"]
    )
    return (
        delta["top1"],
        delta["top2"],
        delta["top3"],
        delta["top4"],
        delta["top5"],
    )


def select_development_candidate(
    candidates: Sequence[dict[str, object]],
) -> tuple[dict[str, object], bool]:
    if not candidates:
        raise ValueError("priority JRA cell has no evaluated candidates")
    complete = [
        candidate
        for candidate in candidates
        if bool(candidate.get("all_requested_years_evaluated", True))
    ]
    if not complete:
        raise ValueError("priority JRA cell has no complete development candidates")
    eligible = [
        candidate
        for candidate in complete
        if bool(candidate["annual_top2_top5_guard_passed"])
        and int(
            cast(
                dict[str, int],
                cast(dict[str, object], candidate["aggregate"])["market_delta_hits"],
            )["top1"]
        )
        > 0
    ]
    pool = eligible or complete
    return max(pool, key=_candidate_sort_key), bool(eligible)


def evaluate_candidate(
    dataset: DatasetLike,
    feature_names: Sequence[str],
    index: JraRaceIndex,
    cell: JraCellKey,
    all_races: Sequence[JraRace],
    target_races: Sequence[JraRace],
    years: Sequence[int],
    strategy: str,
    depth: int,
    learning_rate: float,
    args: argparse.Namespace,
    *,
    include_predictions: bool = False,
) -> dict[str, object]:
    candidate_key = _candidate_key(strategy, depth, learning_rate)
    folds: list[dict[str, object]] = []
    for year in years:
        evaluation_races, evaluation_scope_mode = evaluation_races_for_year(
            all_races, target_races, cell, year, target_date=args.target_date
        )
        if not evaluation_races:
            continue
        checkpoint = build_checkpoint_path(
            args.checkpoint_dir, cell, candidate_key, year, args=args
        )
        fold: dict[str, object] = {}
        if args.resume and checkpoint.exists():
            cached = cast(dict[str, object], json.loads(checkpoint.read_text(encoding="utf-8")))
            if not include_predictions or cached.get("predictions") is not None:
                fold = cached
        if not fold:
            scope = scope_for_strategy(index, cell, date(year, 1, 1), evaluation_races, strategy)
            if (
                len(scope.training_race_ids) < MINIMUM_DIVERSE_TRAINING_RACES
                or not scope.is_diverse
            ):
                fold = {
                    "evaluation_year": year,
                    "status": "insufficient-diverse-training-scope",
                    "training_race_count": len(scope.training_race_ids),
                    "training_cross_cell_race_count": scope.training_cross_cell_race_count,
                    "metrics": None,
                }
            else:
                fold_args = argparse.Namespace(
                    iterations=args.iterations,
                    depth=depth,
                    learning_rate=learning_rate,
                    thread_count=args.thread_count,
                    relevance_mode=args.relevance_mode,
                )
                fold = evaluate_fold(
                    dataset,
                    feature_names,
                    scope,
                    fold_args,
                    include_predictions=include_predictions,
                )
            fold["training_scope"] = {
                "cell_id": cell.cell_id,
                "cutoff": scope.cutoff.isoformat(),
                "history_start": scope.history_start.isoformat(),
                "mode": scope.training_scope_mode,
                "cell_history_seed_race_ids": list(scope.cell_history_seed_race_ids),
                "seed_race_ids": list(scope.seed_race_ids),
                "seed_horse_ids": list(scope.seed_horse_ids),
                "training_race_ids": list(scope.training_race_ids),
                "evaluation_race_ids": list(scope.evaluation_race_ids),
            }
            fold["evaluation_scope_mode"] = evaluation_scope_mode
            fold["evaluation_proxy_race_count"] = (
                len(evaluation_races)
                if evaluation_scope_mode == "related-course-evaluation-proxy"
                else 0
            )
            _write_json(checkpoint, fold)
        folds.append(fold)
    evaluated = [fold for fold in folds if fold.get("metrics") is not None]
    aggregate = aggregate_evaluations([{"folds": evaluated}])
    return {
        "candidate_key": candidate_key,
        "relevance_mode": args.relevance_mode,
        "strategy": strategy,
        "depth": depth,
        "learning_rate": learning_rate,
        "iterations": args.iterations,
        "all_requested_years_evaluated": len(evaluated) == len(years),
        "annual_top2_top5_guard_passed": bool(evaluated) and _annual_guard_passes(aggregate),
        "aggregate": aggregate,
        "exact_aggregate": aggregate_evaluations(
            [
                {
                    "folds": [
                        fold for fold in evaluated if fold["evaluation_scope_mode"] == "exact-cell"
                    ]
                }
            ]
        ),
        "proxy_aggregate": aggregate_evaluations(
            [
                {
                    "folds": [
                        fold for fold in evaluated if fold["evaluation_scope_mode"] != "exact-cell"
                    ]
                }
            ]
        ),
        "folds": folds,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    races = load_races(args.pg_url, "20000101", args.target_date.strftime("%Y%m%d"))
    priority_cells = resolve_priority_cells(races, priority_races=args.priority_races)
    grouped = group_observed_cells(
        races, year_from=args.development_year_from, year_to=args.target_date.year
    )
    index = JraRaceIndex(races)
    dataset = cast(
        DatasetLike, ds.dataset(args.features_root, format="parquet", partitioning="hive")
    )
    feature_names = numeric_feature_names(dataset.schema)
    if not feature_names:
        raise ValueError("feature dataset has no numeric model features")
    cell_results: list[dict[str, object]] = []
    for label, cell in priority_cells.items():
        target_races = grouped.get(cell, ())
        development = [
            evaluate_candidate(
                dataset,
                feature_names,
                index,
                cell,
                races,
                target_races,
                args.development_years,
                strategy,
                depth,
                learning_rate,
                args,
            )
            for strategy in (
                "entrant-history",
                "related-distance-track-bias",
                "venue-surface-track-bias",
            )
            for depth, learning_rate in MODEL_CONFIGS
        ]
        if not any(item.get("all_requested_years_evaluated", True) for item in development):
            cell_results.append(
                {
                    "label": label,
                    "target_race_id": args.priority_races[label],
                    "cell_id": cell.cell_id,
                    "canonical": cell.canonical,
                    "development_candidates": development,
                    "selected_candidate_key": None,
                    "development_eligible": False,
                    "production_eligible": False,
                    "production_eligibility_reason": "no-complete-development-candidates",
                }
            )
            continue
        selected, development_eligible = select_development_candidate(development)
        if args.development_only:
            cell_results.append(
                {
                    "label": label,
                    "target_race_id": args.priority_races[label],
                    "cell_id": cell.cell_id,
                    "canonical": cell.canonical,
                    "development_candidates": development,
                    "selected_candidate_key": selected["candidate_key"],
                    "development_eligible": development_eligible,
                    "production_eligible": False,
                    "production_eligibility_reason": "development-only-no-holdout-evaluated",
                }
            )
            continue
        selected_strategy = selected["strategy"]
        if not isinstance(selected_strategy, str):
            raise ValueError("selected scope strategy must be a string")
        holdout = evaluate_candidate(
            dataset,
            feature_names,
            index,
            cell,
            races,
            target_races,
            HOLDOUT_YEARS,
            selected_strategy,
            cast(int, selected["depth"]),
            cast(float, selected["learning_rate"]),
            args,
            include_predictions=True,
        )
        production_scope = scope_for_strategy(
            index,
            cell,
            args.target_date,
            [race for race in races if race.race_id == args.priority_races[label]],
            selected_strategy,
        )
        market_guard_eligible = (
            development_eligible
            and bool(holdout["all_requested_years_evaluated"])
            and bool(holdout["annual_top2_top5_guard_passed"])
            and production_scope.is_diverse
            and len(production_scope.training_race_ids) >= MINIMUM_DIVERSE_TRAINING_RACES
        )
        cell_results.append(
            {
                "label": label,
                "target_race_id": args.priority_races[label],
                "cell_id": cell.cell_id,
                "canonical": cell.canonical,
                "development_candidates": development,
                "selected_candidate_key": selected["candidate_key"],
                "development_eligible": development_eligible,
                "holdout": holdout,
                "production_scope": {
                    "mode": production_scope.training_scope_mode,
                    "training_race_count": len(production_scope.training_race_ids),
                    "training_target_cell_race_count": (
                        production_scope.training_target_cell_race_count
                    ),
                    "training_cross_cell_race_count": (
                        production_scope.training_cross_cell_race_count
                    ),
                    "related_seed_race_count": production_scope.related_seed_race_count,
                    "target_cell_only_training_allowed": False,
                },
                "market_guard_eligible": market_guard_eligible,
                "production_eligible": False,
                "production_eligibility_reason": (
                    "requires-current-prophet-v6.1-same-identity-comparison"
                ),
            }
        )
    report = {
        "version": REPORT_VERSION,
        "generated_at": datetime.now().astimezone().isoformat(),
        "target_date": args.target_date.isoformat(),
        "input_revision": args.input_revision,
        "production_plan_sha256": (
            hashlib.sha256(args.production_plan.read_bytes()).hexdigest()
            if args.production_plan is not None
            else None
        ),
        "minimum_diverse_training_races": MINIMUM_DIVERSE_TRAINING_RACES,
        "target_cell_only_training_allowed": False,
        "development_years": list(args.development_years),
        "holdout_years": [] if args.development_only else list(HOLDOUT_YEARS),
        "development_only": args.development_only,
        "relevance_mode": args.relevance_mode,
        "feature_count": len(feature_names),
        "all_priority_cells_production_eligible": all(
            bool(result["production_eligible"]) for result in cell_results
        ),
        "cells": cell_results,
    }
    _write_json(args.output, report)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "cell_count": len(cell_results),
                "all_priority_cells_production_eligible": report[
                    "all_priority_cells_production_eligible"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
