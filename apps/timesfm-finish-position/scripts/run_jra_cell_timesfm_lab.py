#!/usr/bin/env python3
"""Research-only, cell-custom TimesFM-3 walk-forward evaluation for one JRA plan."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import cast

import numpy as np
import pyarrow.parquet as pq
from build_jra_cell_manifest import load_races
from optimize_priority_jra_cells import (
    evaluation_races_for_year,
    priority_races_from_plan,
    resolve_priority_cells,
    scope_for_strategy,
)
from predict_lib.jra_cell_scope import JraFoldScope, JraRaceIndex, group_observed_cells

from timesfm_finish_position.cell_adaptation import (
    CellTemporalProfile,
    build_entrant_history_scope,
    scope_strategies_for_cell,
    temporal_profiles_for_cell,
)
from timesfm_finish_position.forecasting import TimesFm3Forecaster, resolve_timesfm_device
from timesfm_finish_position.horse_tsfm import (
    build_horse_rolling_queries,
    build_horse_year_queries,
    forecast_horse_year,
)
from timesfm_finish_position.policy_report import require_float, require_int
from timesfm_finish_position.rustuna_optimization import (
    FoldForecastSurface,
    evaluate_surface,
    optimize_cell_surfaces,
)

DEVELOPMENT_YEARS = (2020, 2021, 2022, 2023)
HOLDOUT_YEARS = (2024, 2025, 2026)
BLEND_WEIGHTS = (0.5,)
VALUE_COLUMNS = ("performance_rating", "speed_figure", "final_3f_rating", "pace_rating")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--production-plan", type=Path, required=True)
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--target-date", type=lambda value: date.fromisoformat(value), required=True
    )
    parser.add_argument("--pg-url", default=os.environ.get("DATABASE_URL_LOCAL"))
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--rustuna-trials", type=int, default=10_000)
    parser.add_argument("--rustuna-storage-dir", type=Path)
    parser.add_argument("--max-cells", type=int)
    parser.add_argument("--cell-id", action="append", dest="cell_ids")
    parser.add_argument("--failed-cells-from", type=Path)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--device")
    parser.add_argument("--accept-non-commercial-license", action="store_true")
    args = parser.parse_args(argv)
    if not args.pg_url:
        parser.error("--pg-url or DATABASE_URL_LOCAL is required")
    if args.max_cells is not None and args.max_cells < 1:
        parser.error("--max-cells must be positive")
    if not args.accept_non_commercial_license:
        parser.error("official TimesFM-3 weights require explicit non-commercial acceptance")
    return args


def _market_percentile_rating(
    race_ids: np.ndarray, horse_ids: np.ndarray, decimal_odds: np.ndarray
) -> np.ndarray:
    ratings = np.zeros(len(race_ids), dtype=np.float64)
    boundaries = np.flatnonzero(race_ids[1:] != race_ids[:-1]) + 1
    starts = np.concatenate((np.asarray([0]), boundaries))
    ends = np.concatenate((boundaries, np.asarray([len(race_ids)])))
    for start, end in zip(starts, ends, strict=True):
        indices = np.arange(start, end, dtype=np.int64)
        ranked = sorted(
            indices,
            key=lambda index: (
                float(np.nan_to_num(decimal_odds[index], nan=np.inf, posinf=np.inf)),
                str(horse_ids[index]),
            ),
        )
        denominator = max(len(ranked) - 1, 1)
        for rank, index in enumerate(ranked):
            ratings[index] = 1.0 - rank / denominator
    return ratings


def _load_columns(path: Path) -> dict[str, np.ndarray]:
    columns = (
        "race_id",
        "race_date",
        "horse_id",
        "finish_position",
        "decimal_odds",
        *VALUE_COLUMNS,
    )
    table = pq.read_table(path, columns=list(columns))
    source = {name: np.asarray(table.column(name).to_pylist()) for name in columns}
    order = np.lexsort(
        (
            source["horse_id"].astype(np.str_),
            source["race_id"].astype(np.str_),
            source["race_date"].astype(np.str_),
        )
    )
    ordered = {name: values[order] for name, values in source.items()}
    race_ids = ordered["race_id"].astype(np.str_)
    horse_ids = ordered["horse_id"].astype(np.str_)
    odds = ordered["decimal_odds"].astype(np.float64)
    market_rating = _market_percentile_rating(race_ids, horse_ids, odds)
    ordered["performance_over_market"] = (
        ordered["performance_rating"].astype(np.float64) - market_rating
    )
    return ordered


def _indices_by_race(race_ids: np.ndarray) -> dict[str, np.ndarray]:
    grouped: dict[str, list[int]] = {}
    for index, race_id in enumerate(race_ids.astype(np.str_)):
        grouped.setdefault(str(race_id), []).append(index)
    return {key: np.asarray(value, dtype=np.int64) for key, value in grouped.items()}


def _indices_for_races(index: Mapping[str, np.ndarray], race_ids: Sequence[str]) -> np.ndarray:
    blocks = [index[race_id] for race_id in race_ids if race_id in index]
    return np.sort(np.concatenate(blocks)) if blocks else np.asarray([], dtype=np.int64)


def _finite_values(values: np.ndarray, *, fitting_mask: np.ndarray) -> np.ndarray:
    if fitting_mask.shape != (len(values),):
        raise ValueError("imputation fitting mask must align")
    result = values.astype(np.float64, copy=True)
    for column in range(result.shape[1]):
        finite_training = fitting_mask & np.isfinite(result[:, column])
        replacement = (
            float(np.median(result[finite_training, column]))
            if bool(finite_training.any())
            else 0.0
        )
        result[~np.isfinite(result[:, column]), column] = replacement
    return result


def _normalize_horse_histories(
    values: np.ndarray, horse_ids: np.ndarray, *, fitting_mask: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if fitting_mask.shape != (len(values),) or horse_ids.shape != (len(values),):
        raise ValueError("horse normalization columns must align")
    if not bool(np.any(fitting_mask)):
        raise ValueError("horse normalization requires fitting rows")
    training: dict[str, list[int]] = {}
    for index in np.flatnonzero(fitting_mask):
        training.setdefault(str(horse_ids[index]), []).append(int(index))
    locations: dict[str, np.ndarray] = {}
    scales: dict[str, np.ndarray] = {}
    for horse_id, indices in training.items():
        history = values[indices]
        locations[horse_id] = np.mean(history, axis=0)
        scales[horse_id] = np.maximum(np.std(history, axis=0), 1e-6)
    default_location = np.mean(values[fitting_mask], axis=0)
    default_scale = np.maximum(np.std(values[fitting_mask], axis=0), 1e-6)
    row_locations = np.vstack(
        [locations.get(str(horse_id), default_location) for horse_id in horse_ids]
    )
    row_scales = np.vstack([scales.get(str(horse_id), default_scale) for horse_id in horse_ids])
    return (values - row_locations) / row_scales, row_locations, row_scales


def _race_standardize(scores: np.ndarray, race_ids: np.ndarray) -> np.ndarray:
    standardized = np.zeros_like(scores, dtype=np.float64)
    for race_id in sorted(set(race_ids.astype(np.str_))):
        mask = race_ids == race_id
        values = scores[mask]
        std = float(np.std(values))
        standardized[mask] = (values - float(np.mean(values))) / std if std > 0 else 0.0
    return standardized


def _ranks(scores: np.ndarray, race_ids: np.ndarray, horse_ids: np.ndarray) -> np.ndarray:
    result = np.zeros(len(scores), dtype=np.int64)
    for race_id in sorted(set(race_ids.astype(np.str_))):
        indices = np.flatnonzero(race_ids == race_id)
        order = sorted(indices, key=lambda index: (-float(scores[index]), str(horse_ids[index])))
        for rank, index in enumerate(order, start=1):
            result[index] = rank
    return result


def _metrics(ranks: np.ndarray, finishes: np.ndarray, race_ids: np.ndarray) -> dict[str, int]:
    winners = finishes.astype(np.int64) == 1
    races = sorted(set(race_ids.astype(np.str_)))
    result = {"race_count": len(races)}
    for depth in range(1, 6):
        result[f"top{depth}_hits"] = int(np.sum(winners & (ranks <= depth)))
    return result


def _delta(model: Mapping[str, int], baseline: Mapping[str, int]) -> dict[str, int]:
    return {
        f"top{depth}": model[f"top{depth}_hits"] - baseline[f"top{depth}_hits"]
        for depth in range(1, 6)
    }


def _candidate_key(profile: str, strategy: str, weight: float) -> str:
    return f"{profile}__{strategy}__weight-{weight:g}"


def _annual_place_guard(folds: Sequence[Mapping[str, object]]) -> bool:
    return all(
        cast(Mapping[str, int], fold["market_delta_hits"])[f"top{depth}"] >= 0
        for fold in folds
        for depth in range(2, 6)
    )


def _aggregate_folds(folds: Sequence[Mapping[str, object]]) -> dict[str, object]:
    metrics = {"race_count": 0, **{f"top{depth}_hits": 0 for depth in range(1, 6)}}
    market = metrics.copy()
    for fold in folds:
        for key, value in cast(Mapping[str, int], fold["metrics"]).items():
            metrics[key] += value
        for key, value in cast(Mapping[str, int], fold["market_metrics"]).items():
            market[key] += value
    return {
        "metrics": metrics,
        "market_metrics": market,
        "market_delta_hits": _delta(metrics, market),
    }


def _fold_candidates(
    *,
    source: Mapping[str, np.ndarray],
    row_index: Mapping[str, np.ndarray],
    scope: JraFoldScope,
    profiles: Sequence[CellTemporalProfile],
    year: int,
    forecaster: TimesFm3Forecaster,
    strategy: str,
) -> list[dict[str, object]]:
    training_race_ids = scope.training_race_ids
    evaluation_race_ids = scope.evaluation_race_ids
    evaluation_indices = _indices_for_races(row_index, evaluation_race_ids)
    additional_indices = _indices_for_races(row_index, training_race_ids)
    target_cell_history = frozenset(scope.cell_history_seed_race_ids)
    audited = build_entrant_history_scope(
        horse_ids=source["horse_id"].astype(np.str_),
        race_dates=source["race_date"].astype(np.str_),
        race_ids=source["race_id"].astype(np.str_),
        evaluation_indices=evaluation_indices,
        cutoff=f"{year}0101",
        history_start=scope.history_start.strftime("%Y%m%d"),
        target_cell_race_ids=target_cell_history,
        additional_training_indices=additional_indices,
    )
    source_horses = source["horse_id"].astype(np.str_)
    source_dates = source["race_date"].astype(np.str_)
    evaluation_horses = frozenset(str(source_horses[index]) for index in evaluation_indices)
    latest_evaluation_date = max(str(source_dates[index]) for index in evaluation_indices)
    entrant_history_pool = np.flatnonzero(
        np.isin(source_horses, tuple(evaluation_horses))
        & (source_dates >= scope.history_start.strftime("%Y%m%d"))
        & (source_dates < latest_evaluation_date)
    )
    selected = np.unique(
        np.concatenate((audited.training_indices, entrant_history_pool, evaluation_indices))
    )
    local_evaluation_indices = np.searchsorted(selected, evaluation_indices).astype(np.int64)
    if not np.array_equal(selected[local_evaluation_indices], evaluation_indices):
        raise RuntimeError("evaluation rows were not preserved in the local history scope")
    local_horses = source_horses[selected]
    local_dates = source["race_date"][selected].astype(np.str_)
    local_races = source["race_id"][selected].astype(np.str_)
    evaluation_set = set(evaluation_race_ids)
    observed_evaluation = {str(value) for value in local_races if str(value) in evaluation_set}
    if observed_evaluation != evaluation_set:
        missing = sorted(evaluation_set - observed_evaluation)
        raise ValueError(f"history export omitted evaluation races: {missing}")
    profile_results: list[dict[str, object]] = []
    for profile in profiles:
        profile_name = profile.name
        columns = profile.value_columns
        prior = local_dates < f"{year}0101"
        history_values = _finite_values(
            np.column_stack([source[column][selected].astype(np.float64) for column in columns]),
            fitting_mask=prior,
        )
        row_locations: np.ndarray | None = None
        row_scales: np.ndarray | None = None
        if profile.horse_normalize:
            history_values, row_locations, row_scales = _normalize_horse_histories(
                history_values, local_horses, fitting_mask=prior
            )
        fallback = np.mean(history_values[prior], axis=0)
        if profile.rolling_origin:
            queries = build_horse_rolling_queries(
                horse_ids=local_horses,
                race_dates=local_dates,
                history_values=history_values,
                evaluation_indices=local_evaluation_indices,
                max_history=None,
            )
        else:
            queries = build_horse_year_queries(
                horse_ids=local_horses,
                race_dates=local_dates,
                history_values=history_values,
                year=year,
                max_history=None,
                evaluation_indices=local_evaluation_indices,
            )
        forecast = forecast_horse_year(queries, forecaster, fallback=fallback)
        target = forecast.target_indices
        forecast_values = forecast.values
        if row_locations is not None and row_scales is not None:
            forecast_values = forecast_values * row_scales[target] + row_locations[target]
        target_races = local_races[target]
        target_horses = local_horses[target]
        components = np.column_stack(
            [
                _race_standardize(forecast_values[:, column], target_races)
                for column in range(forecast_values.shape[1])
            ]
        )
        timesfm_scores = np.mean(components, axis=1)
        odds = source["decimal_odds"][selected][target].astype(np.float64)
        market_scores = _race_standardize(-np.nan_to_num(odds, nan=9999.0), target_races)
        finishes = source["finish_position"][selected][target].astype(np.int64)
        market_metrics = _metrics(
            _ranks(market_scores, target_races, target_horses), finishes, target_races
        )
        for weight in BLEND_WEIGHTS:
            scores = weight * timesfm_scores + (1.0 - weight) * market_scores
            ranks = _ranks(scores, target_races, target_horses)
            metrics = _metrics(ranks, finishes, target_races)
            profile_results.append(
                {
                    "candidate_key": _candidate_key(profile_name, strategy, weight),
                    "profile": profile_name,
                    "strategy": strategy,
                    "weight": weight,
                    "year": year,
                    "metrics": metrics,
                    "market_metrics": market_metrics,
                    "market_delta_hits": _delta(metrics, market_metrics),
                    "history_available_rate": float(np.mean(forecast.history_available)),
                    "_surface": FoldForecastSurface(
                        profile=profile_name,
                        strategy=strategy,
                        year=year,
                        value_columns=columns,
                        timesfm_components=components,
                        market_scores=market_scores,
                        race_ids=target_races,
                        horse_ids=target_horses,
                        finish_positions=finishes,
                        history_counts=forecast.history_counts,
                    ),
                    "training_scope": {
                        "entrant_horse_count": len(audited.entrant_horse_ids),
                        "entrant_history_row_count": len(entrant_history_pool),
                        "pre_year_entrant_history_row_count": len(audited.entrant_history_indices),
                        "training_row_count": len(selected) - len(evaluation_indices),
                        "additional_training_row_count": audited.additional_training_row_count,
                        "target_cell_training_race_count": audited.target_cell_training_race_count,
                        "cross_cell_training_race_count": audited.cross_cell_training_race_count,
                    },
                    "predictions": [
                        {
                            "race_id": str(race_id),
                            "horse_id": str(horse_id),
                            "finish_position": int(finish),
                            "predicted_rank": int(rank),
                        }
                        for race_id, horse_id, finish, rank in zip(
                            target_races, target_horses, finishes, ranks, strict=True
                        )
                    ],
                }
            )
    return profile_results


def _apply_rustuna_params(
    fold: Mapping[str, object], params: Mapping[str, float | int | str | bool | None]
) -> dict[str, object]:
    surface = cast(FoldForecastSurface, fold["_surface"])
    component_weights = {
        column: require_float(params.get(f"weight_{column}", 0.0))
        for column in surface.value_columns
    }
    market_weight = (
        1.0 if params.get("market_only") is True else require_float(params["market_weight"])
    )
    minimum_history_count = require_int(params.get("minimum_history_count", 1))
    evaluated = evaluate_surface(
        surface,
        component_weights=component_weights,
        market_weight=market_weight,
        minimum_history_count=minimum_history_count,
    )
    weights = np.asarray([component_weights[column] for column in surface.value_columns])
    if not bool(np.any(weights > 0)):
        weights[:] = 1.0
    weights /= np.sum(weights)
    temporal = surface.timesfm_components @ weights
    scores = surface.market_scores.copy()
    eligible = surface.history_counts >= minimum_history_count
    scores[eligible] = (1.0 - market_weight) * temporal[
        eligible
    ] + market_weight * surface.market_scores[eligible]
    ranks = _ranks(scores, surface.race_ids, surface.horse_ids)
    metrics = {
        "race_count": evaluated.race_count,
        **{f"top{depth}_hits": evaluated.model_hits[depth - 1] for depth in range(1, 6)},
    }
    market_metrics = {
        "race_count": evaluated.race_count,
        **{f"top{depth}_hits": evaluated.market_hits[depth - 1] for depth in range(1, 6)},
    }
    return {
        **{key: value for key, value in fold.items() if not key.startswith("_")},
        "candidate_key": str(params["profile_scope"]),
        "weight": None,
        "metrics": metrics,
        "market_metrics": market_metrics,
        "market_delta_hits": _delta(metrics, market_metrics),
        "predictions": [
            {
                "race_id": str(race_id),
                "horse_id": str(horse_id),
                "finish_position": int(finish),
                "predicted_rank": int(rank),
            }
            for race_id, horse_id, finish, rank in zip(
                surface.race_ids,
                surface.horse_ids,
                surface.finish_positions,
                ranks,
                strict=True,
            )
        ],
    }


def _race_date_from_id(race_id: str) -> str:
    parts = race_id.split(":")
    if len(parts) != 5 or len(parts[1]) != 4 or len(parts[2]) != 4:
        raise ValueError(f"unsupported canonical race ID: {race_id}")
    return parts[1] + parts[2]


def _current_metrics(
    predictions: Sequence[Mapping[str, object]],
    current: Mapping[tuple[str, str], int],
    *,
    current_through: str,
) -> tuple[dict[str, int] | None, dict[str, int] | None, bool]:
    comparable = [
        row for row in predictions if _race_date_from_id(str(row["race_id"])) <= current_through
    ]
    if not comparable:
        return None, None, True
    identities = {(str(row["race_id"]), str(row["horse_id"])) for row in comparable}
    if not identities.issubset(current):
        return None, None, False
    races = np.asarray([str(row["race_id"]) for row in comparable], dtype=np.str_)
    horses = np.asarray([str(row["horse_id"]) for row in comparable], dtype=np.str_)
    finishes = np.asarray(
        [require_int(row["finish_position"]) for row in comparable], dtype=np.int64
    )
    model_ranks = np.asarray(
        [require_int(row["predicted_rank"]) for row in comparable], dtype=np.int64
    )
    current_ranks = np.asarray(
        [current[(race_id, horse_id)] for race_id, horse_id in zip(races, horses, strict=True)],
        dtype=np.int64,
    )
    return _metrics(model_ranks, finishes, races), _metrics(current_ranks, finishes, races), True


def _write_cell_checkpoint(path: Path, cells: Sequence[Mapping[str, object]]) -> None:
    pending = path.with_suffix(path.suffix + ".next")
    pending.write_text(
        json.dumps(
            {"schema": "jra-cell-timesfm3-checkpoint-v1", "cells": cells}, ensure_ascii=False
        )
        + "\n"
    )
    pending.replace(path)


def run(args: argparse.Namespace) -> dict[str, object]:
    source = _load_columns(args.history)
    row_index = _indices_by_race(source["race_id"])
    current_table = pq.read_table(
        args.current, columns=["race_id", "ketto_toroku_bango", "predicted_rank"]
    )
    current_race_ids = [str(value) for value in current_table.column("race_id").to_pylist()]
    current_through = max(_race_date_from_id(race_id) for race_id in current_race_ids)
    current = {
        (str(race_id), str(horse_id)): int(rank)
        for race_id, horse_id, rank in zip(
            current_table.column("race_id").to_pylist(),
            current_table.column("ketto_toroku_bango").to_pylist(),
            current_table.column("predicted_rank").to_pylist(),
            strict=True,
        )
    }
    races = load_races(args.pg_url, "20000101", args.target_date.strftime("%Y%m%d"))
    priority_races = priority_races_from_plan(args.production_plan, target_date=args.target_date)
    priority_cells = resolve_priority_cells(races, priority_races=priority_races)
    grouped = group_observed_cells(races, year_from=2020, year_to=args.target_date.year)
    index = JraRaceIndex(races)
    forecaster = TimesFm3Forecaster(
        checkpoint="google/timesfm-3.0-pytorch",
        batch_size=args.batch_size,
        device=resolve_timesfm_device(args.device),
    )
    checkpoint_path = args.output.with_suffix(args.output.suffix + ".partial")
    completed: dict[str, dict[str, object]] = {}
    if args.resume and checkpoint_path.exists():
        checkpoint = cast(dict[str, object], json.loads(checkpoint_path.read_text()))
        completed = {
            str(cell["cell_id"]): cell
            for cell in cast(Sequence[dict[str, object]], checkpoint["cells"])
        }
    cell_results: list[dict[str, object]] = []
    selected_cells = list(priority_cells.items())
    requested = set(cast(Sequence[str], args.cell_ids or ()))
    if args.failed_cells_from is not None:
        prior_report = cast(dict[str, object], json.loads(args.failed_cells_from.read_text()))
        requested.update(
            str(cell["cell_id"])
            for cell in cast(Sequence[dict[str, object]], prior_report["cells"])
            if not bool(cell["production_eligible"])
        )
    if requested:
        selected_cells = [item for item in selected_cells if item[1].cell_id in requested]
        observed = {cell.cell_id for _, cell in selected_cells}
        if observed != requested:
            raise ValueError(f"unknown requested cell IDs: {sorted(requested - observed)}")
    if args.max_cells is not None:
        selected_cells = selected_cells[: args.max_cells]
    for label, cell in selected_cells:
        if cell.cell_id in completed:
            cell_results.append(completed[cell.cell_id])
            continue
        profiles = temporal_profiles_for_cell(
            distance=cell.distance,
            surface=cell.surface,
            condition_code=cell.condition_code,
            race_identity=cell.race_identity or "",
        )
        scope_strategies = scope_strategies_for_cell(
            distance=cell.distance,
            surface=cell.surface,
            condition_code=cell.condition_code,
            race_identity=cell.race_identity or "",
        )
        target_races = grouped.get(cell, ())
        by_candidate: dict[str, list[dict[str, object]]] = {}
        for year in (*DEVELOPMENT_YEARS, *HOLDOUT_YEARS):
            evaluation_races, evaluation_mode = evaluation_races_for_year(
                races, target_races, cell, year, target_date=args.target_date
            )
            if not evaluation_races:
                continue
            for strategy in scope_strategies:
                scope = scope_for_strategy(
                    index, cell, date(year, 1, 1), evaluation_races, strategy
                )
                for fold in _fold_candidates(
                    source=source,
                    row_index=row_index,
                    scope=scope,
                    profiles=profiles,
                    year=year,
                    forecaster=forecaster,
                    strategy=strategy,
                ):
                    fold["evaluation_scope_mode"] = evaluation_mode
                    by_candidate.setdefault(cast(str, fold["candidate_key"]), []).append(fold)
        unique_folds: dict[tuple[str, int], dict[str, object]] = {}
        for folds in by_candidate.values():
            for fold in folds:
                surface = cast(FoldForecastSurface, fold["_surface"])
                unique_folds.setdefault((surface.key, surface.year), fold)
        development_surfaces = [
            cast(FoldForecastSurface, fold["_surface"])
            for (_, year), fold in unique_folds.items()
            if year in DEVELOPMENT_YEARS
        ]
        storage_path = None
        if args.rustuna_storage_dir is not None:
            args.rustuna_storage_dir.mkdir(parents=True, exist_ok=True)
            storage_path = args.rustuna_storage_dir / f"{cell.cell_id}.sqlite3"
        rustuna_result = optimize_cell_surfaces(
            development_surfaces,
            n_trials=args.rustuna_trials,
            seed=20260912,
            study_name=f"jra-timesfm3-{cell.cell_id}",
            storage_path=storage_path,
        )
        params = rustuna_result.best_params
        selected_key = str(params["profile_scope"])
        selected_raw_folds = [
            fold for (key, _year), fold in unique_folds.items() if key == selected_key
        ]
        development_folds = [
            _apply_rustuna_params(fold, params)
            for fold in selected_raw_folds
            if require_int(fold["year"]) in DEVELOPMENT_YEARS
        ]
        development = _aggregate_folds(development_folds)
        development_delta = cast(Mapping[str, int], development["market_delta_hits"])
        development_eligible = (
            _annual_place_guard(development_folds) and development_delta["top1"] > 0
        )
        holdout_folds = [
            _apply_rustuna_params(fold, params)
            for fold in selected_raw_folds
            if require_int(fold["year"]) in HOLDOUT_YEARS
        ]
        holdout = _aggregate_folds(holdout_folds)
        holdout_delta = cast(Mapping[str, int], holdout["market_delta_hits"])
        market_guard = (
            development_eligible
            and _annual_place_guard(holdout_folds)
            and holdout_delta["top1"] > 0
        )
        current_complete = True
        current_folds: list[dict[str, object]] = []
        model_current = {"race_count": 0, **{f"top{depth}_hits": 0 for depth in range(1, 6)}}
        model_comparable = {"race_count": 0, **{f"top{depth}_hits": 0 for depth in range(1, 6)}}
        for fold in holdout_folds:
            comparable_metrics, current_metrics, complete = _current_metrics(
                cast(Sequence[Mapping[str, object]], fold["predictions"]),
                current,
                current_through=current_through,
            )
            current_complete &= complete
            current_folds.append(
                {
                    "year": fold["year"],
                    "complete": complete,
                    "delta_hits": (
                        _delta(comparable_metrics, current_metrics)
                        if comparable_metrics is not None and current_metrics is not None
                        else None
                    ),
                }
            )
            if comparable_metrics is not None and current_metrics is not None:
                for key, value in current_metrics.items():
                    model_current[key] += value
                for key, value in comparable_metrics.items():
                    model_comparable[key] += value
        current_delta = _delta(model_comparable, model_current) if current_complete else None
        current_annual_place_guard = current_complete and all(
            cast(Mapping[str, int], fold["delta_hits"])[f"top{depth}"] >= 0
            for fold in current_folds
            if fold["delta_hits"] is not None
            for depth in range(2, 6)
        )
        current_guard = (
            market_guard
            and current_annual_place_guard
            and current_delta is not None
            and current_delta["top1"] > 0
        )
        cell_results.append(
            {
                "label": label,
                "target_race_id": priority_races[label],
                "cell_id": cell.cell_id,
                "canonical": cell.canonical,
                "profiles": [asdict(profile) for profile in profiles],
                "scope_strategies": list(scope_strategies),
                "rustuna": asdict(rustuna_result),
                "development": {**development, "folds": development_folds},
                "selected_candidate_key": selected_key,
                "development_eligible": development_eligible,
                "holdout": {**holdout, "folds": holdout_folds},
                "market_guard_eligible": market_guard,
                "current_comparison_complete": current_complete,
                "current_comparison_folds": current_folds,
                "current_annual_top2_top5_guard_passed": current_annual_place_guard,
                "current_delta_hits": current_delta,
                "production_eligible": current_guard,
                "research_only": True,
            }
        )
        _write_cell_checkpoint(checkpoint_path, cell_results)
    return {
        "schema": "jra-cell-timesfm3-walk-forward-v1",
        "research_only": True,
        "production_integration": False,
        "license_acceptance": "user-confirmed-self-only-non-commercial-cloudflare-use",
        "checkpoint": forecaster.checkpoint,
        "checkpoint_revision": forecaster.checkpoint_revision,
        "history_sha256": hashlib.sha256(args.history.read_bytes()).hexdigest(),
        "production_plan_sha256": hashlib.sha256(args.production_plan.read_bytes()).hexdigest(),
        "current_sha256": hashlib.sha256(args.current.read_bytes()).hexdigest(),
        "current_comparison_through": current_through,
        "cell_count": len(cell_results),
        "production_eligible_cell_count": sum(
            bool(cell["production_eligible"]) for cell in cell_results
        ),
        "cells": cell_results,
    }


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = run(args)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    args.output.with_suffix(args.output.suffix + ".partial").unlink(missing_ok=True)
    print(json.dumps({"cell_count": report["cell_count"], "output": str(args.output)}))


if __name__ == "__main__":
    main()
