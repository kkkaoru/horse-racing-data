"""Walk-forward evaluation for every observed canonical JRA cell.

Each fold is strictly point-in-time: evaluation-year entrants seed the cohort,
training rows are only those entrants' races in the preceding 20-year window,
and the evaluation labels never enter fitting.  Cell checkpoint files make the
large all-cell run resumable without weakening any fold guard.
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Final, cast

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from catboost import CatBoostRanker

from build_jra_cell_manifest import atomic_write, load_races
from predict_lib.jra_cell_scope import (
    EVALUATION_YEAR_FROM,
    EVALUATION_YEAR_TO,
    MINIMUM_DIVERSE_TRAINING_RACES,
    JraCellKey,
    JraFoldScope,
    JraRace,
    JraRaceIndex,
    group_observed_cells,
)
from predict_lib.rank_relevance import (
    DEFAULT_RELEVANCE_MODE,
    RELEVANCE_MODES,
    parse_relevance_mode,
)
from train_jra_cell_models import (
    DatasetLike,
    build_rank_pool,
    load_feature_rows,
    numeric_feature_names,
    rank_predictions,
    winner_topk_metrics,
)

REPORT_VERSION: Final[str] = "jra-cell-walk-forward-v2"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="evaluate_jra_cell_models")
    parser.add_argument("--pg-url", default=os.environ.get("DATABASE_URL_LOCAL"))
    parser.add_argument("--features-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--target-plan", type=Path)
    parser.add_argument("--from-date", default="20000101")
    parser.add_argument("--to-date", default="20260905")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--depth", type=int, default=8)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--relevance-mode", choices=RELEVANCE_MODES, default=DEFAULT_RELEVANCE_MODE)
    parser.add_argument("--thread-count", type=int, default=6)
    parser.add_argument("--max-cells", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    if not args.pg_url:
        parser.error("--pg-url or DATABASE_URL_LOCAL is required")
    if args.shard_count <= 0 or not 0 <= args.shard_index < args.shard_count:
        parser.error("--shard-index must be within --shard-count")
    return args


def _complete_rows(frame: pd.DataFrame) -> pd.DataFrame:
    finish = cast(pd.Series, pd.to_numeric(frame["finish_position"], errors="coerce"))
    return cast(pd.DataFrame, frame[finish > 0]).copy()


def evaluate_fold(
    dataset: DatasetLike,
    feature_names: Sequence[str],
    scope: JraFoldScope,
    args: argparse.Namespace,
    *,
    include_predictions: bool = False,
) -> dict[str, object]:
    relevance_mode = parse_relevance_mode(getattr(args, "relevance_mode", DEFAULT_RELEVANCE_MODE))
    base = {
        "relevance_mode": relevance_mode,
        "evaluation_year": scope.evaluation_year,
        "cutoff": scope.cutoff.isoformat(),
        "history_start": scope.history_start.isoformat(),
        "seed_race_count": len(scope.seed_race_ids),
        "seed_horse_count": len(scope.seed_horse_ids),
        "training_scope_race_count": len(scope.training_race_ids),
        "evaluation_scope_race_count": len(scope.evaluation_race_ids),
        "training_scope_mode": scope.training_scope_mode,
        "related_seed_race_count": scope.related_seed_race_count,
        "training_target_cell_race_count": scope.training_target_cell_race_count,
        "training_cross_cell_race_count": scope.training_cross_cell_race_count,
        "training_scope_is_diverse": scope.is_diverse,
    }
    historical = _complete_rows(load_feature_rows(dataset, scope.training_race_ids, feature_names))
    evaluation = _complete_rows(
        load_feature_rows(dataset, scope.evaluation_race_ids, feature_names)
    )
    training_race_count = len(set(cast(pd.Series, historical["race_id"]).astype(str)))
    evaluation_race_count = len(set(cast(pd.Series, evaluation["race_id"]).astype(str)))
    base.update(
        {
            "training_race_count": training_race_count,
            "training_runner_count": len(historical),
            "evaluation_race_count": evaluation_race_count,
            "evaluation_runner_count": len(evaluation),
        }
    )
    if training_race_count < MINIMUM_DIVERSE_TRAINING_RACES:
        return {**base, "status": "insufficient-training-races", "metrics": None}
    if not scope.is_diverse:
        return {**base, "status": "target-cell-only-training-forbidden", "metrics": None}
    if evaluation_race_count == 0:
        return {**base, "status": "no-complete-evaluation-races", "metrics": None}
    training_ids = set(cast(pd.Series, historical["race_id"]).astype(str))
    evaluation_ids = set(cast(pd.Series, evaluation["race_id"]).astype(str))
    if training_ids & evaluation_ids:
        raise ValueError("walk-forward training and evaluation races overlap")
    training_dates = cast(pd.Series, historical["race_date"]).astype(str)
    evaluation_dates = cast(pd.Series, evaluation["race_date"]).astype(str)
    cutoff_token = scope.cutoff.strftime("%Y%m%d")
    if bool((training_dates >= cutoff_token).any()):
        raise ValueError("walk-forward training row is not before cutoff")
    if bool((evaluation_dates < cutoff_token).any()):
        raise ValueError("walk-forward evaluation row is before cutoff")
    model = CatBoostRanker(
        loss_function="YetiRank",
        iterations=args.iterations,
        learning_rate=args.learning_rate,
        depth=args.depth,
        l2_leaf_reg=3.0,
        random_seed=20260905,
        task_type="CPU",
        thread_count=args.thread_count,
        verbose=False,
    )
    model.fit(
        build_rank_pool(historical, feature_names, with_labels=True, relevance_mode=relevance_mode)
    )
    ordered = evaluation.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    scores = np.asarray(
        model.predict(build_rank_pool(ordered, feature_names, with_labels=False)),
        dtype=np.float64,
    )
    if not bool(np.isfinite(scores).all()):
        raise ValueError("walk-forward model produced a non-finite score")
    race_ids = cast(pd.Series, ordered["race_id"]).astype(str).tolist()
    horse_ids = cast(pd.Series, ordered["ketto_toroku_bango"]).astype(str).tolist()
    ordered["predicted_score"] = scores
    ordered["predicted_rank"] = rank_predictions(race_ids, horse_ids, scores.tolist())
    metrics = winner_topk_metrics(ordered)
    market_scores = cast(pd.Series, pd.to_numeric(ordered["odds_score"], errors="coerce"))
    market_scores = -market_scores.fillna(1.0)
    market_frame = ordered.copy()
    market_frame["predicted_rank"] = rank_predictions(
        race_ids, horse_ids, market_scores.astype(float).tolist()
    )
    market_metrics = winner_topk_metrics(market_frame)
    result: dict[str, object] = {
        **base,
        "status": "evaluated",
        "metrics": metrics,
        "market_baseline_metrics": market_metrics,
        "market_delta_hits": {
            f"top{depth}": int(metrics[f"top{depth}_hits"])
            - int(market_metrics[f"top{depth}_hits"])
            for depth in range(1, 6)
        },
    }
    if include_predictions:
        result["predictions"] = ordered.loc[
            :,
            [
                "race_date",
                "race_id",
                "ketto_toroku_bango",
                "umaban",
                "finish_position",
                "odds_score",
                "predicted_score",
                "predicted_rank",
            ],
        ].to_dict(orient="records")
    return result


def evaluate_cell(
    dataset: DatasetLike,
    feature_names: Sequence[str],
    index: JraRaceIndex,
    cell: JraCellKey,
    target_races: Sequence[JraRace],
    args: argparse.Namespace,
) -> dict[str, object]:
    folds: list[dict[str, object]] = []
    for year in range(EVALUATION_YEAR_FROM, EVALUATION_YEAR_TO + 1):
        year_races = [race for race in target_races if race.race_date.year == year]
        if not year_races:
            continue
        scope = index.build_fold_scope(cell, year)
        folds.append(evaluate_fold(dataset, feature_names, scope, args))
    return {
        "cell_id": cell.cell_id,
        "canonical": cell.canonical,
        "venue": cell.venue,
        "distance": cell.distance,
        "season": cell.season,
        "surface": cell.surface,
        "condition_code": cell.condition_code,
        "race_identity": cell.race_identity,
        "folds": folds,
    }


def _checkpoint_path(output_dir: Path, cell: JraCellKey) -> Path:
    return output_dir / "cells" / f"{cell.cell_id}.json"


def _write_json(path: Path, payload: object) -> None:
    atomic_write(path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def aggregate_evaluations(cells: Sequence[dict[str, object]]) -> dict[str, object]:
    by_year: dict[int, dict[str, int]] = {}
    market_by_year: dict[int, dict[str, int]] = {}
    aggregate = {"race_count": 0, **{f"top{depth}_hits": 0 for depth in range(1, 6)}}
    market_aggregate = {
        "race_count": 0,
        **{f"top{depth}_hits": 0 for depth in range(1, 6)},
    }
    status_counts: dict[str, int] = {}
    for cell in cells:
        for fold in cast(list[dict[str, object]], cell["folds"]):
            status = cast(str, fold["status"])
            status_counts[status] = status_counts.get(status, 0) + 1
            metrics = cast(dict[str, float | int] | None, fold["metrics"])
            if metrics is None:
                continue
            market_metrics = cast(dict[str, float | int], fold["market_baseline_metrics"])
            year = cast(int, fold["evaluation_year"])
            empty_counts = {
                "race_count": 0,
                **{f"top{depth}_hits": 0 for depth in range(1, 6)},
            }
            year_totals = by_year.setdefault(year, empty_counts.copy())
            market_year_totals = market_by_year.setdefault(year, empty_counts.copy())
            for field in year_totals:
                value = int(metrics[field])
                year_totals[field] += value
                aggregate[field] += value
                market_value = int(market_metrics[field])
                market_aggregate[field] += market_value
                market_year_totals[field] += market_value

    def with_accuracy(counts: dict[str, int]) -> dict[str, float | int]:
        races = counts["race_count"]
        return {
            **counts,
            **{
                f"top{depth}_accuracy": counts[f"top{depth}_hits"] / races if races else 0.0
                for depth in range(1, 6)
            },
        }

    return {
        "status_counts": status_counts,
        "overall": with_accuracy(aggregate),
        "market_baseline_overall": with_accuracy(market_aggregate),
        "market_delta_hits": {
            f"top{depth}": aggregate[f"top{depth}_hits"] - market_aggregate[f"top{depth}_hits"]
            for depth in range(1, 6)
        },
        "by_year": {
            str(year): {
                "model": with_accuracy(counts),
                "market_baseline": with_accuracy(market_by_year[year]),
                "delta_hits": {
                    f"top{depth}": counts[f"top{depth}_hits"]
                    - market_by_year[year][f"top{depth}_hits"]
                    for depth in range(1, 6)
                },
            }
            for year, counts in sorted(by_year.items())
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    races = load_races(args.pg_url, args.from_date, args.to_date)
    all_observed = sorted(group_observed_cells(races).items(), key=lambda item: item[0].canonical)
    selected_observed = all_observed
    if args.target_plan is not None:
        target_plan = json.loads(args.target_plan.read_text(encoding="utf-8"))
        target_canonicals = {str(cell["canonical"]) for cell in target_plan["cells"]}
        selected_observed = [
            item for item in all_observed if item[0].canonical in target_canonicals
        ]
        observed_canonicals = {cell.canonical for cell, _target_races in selected_observed}
        if observed_canonicals != target_canonicals:
            missing = sorted(target_canonicals - observed_canonicals)
            raise ValueError(f"target plan cells are absent from observed history: {missing}")
    observed = [
        item
        for position, item in enumerate(selected_observed)
        if position % args.shard_count == args.shard_index
    ]
    if args.max_cells is not None:
        observed = observed[: args.max_cells]
    index = JraRaceIndex(races)
    dataset = cast(
        DatasetLike, ds.dataset(args.features_root, format="parquet", partitioning="hive")
    )
    if not isinstance(dataset.schema, pa.Schema):
        raise TypeError("feature dataset schema is unavailable")
    feature_names = numeric_feature_names(dataset.schema)
    if not feature_names:
        raise ValueError("feature dataset has no numeric model features")
    cells: list[dict[str, object]] = []
    for position, (cell, target_races) in enumerate(observed, start=1):
        checkpoint = _checkpoint_path(args.output_dir, cell)
        if args.resume and checkpoint.exists():
            payload = json.loads(checkpoint.read_text(encoding="utf-8"))
        else:
            payload = evaluate_cell(dataset, feature_names, index, cell, target_races, args)
            _write_json(checkpoint, payload)
        cells.append(cast(dict[str, object], payload))
        print(
            json.dumps(
                {"cell": position, "cell_count": len(observed), "cell_id": cell.cell_id},
                sort_keys=True,
            ),
            flush=True,
        )
    report = {
        "version": REPORT_VERSION,
        "generated_at": datetime.now().astimezone().isoformat(),
        "evaluation_year_from": EVALUATION_YEAR_FROM,
        "evaluation_year_to": EVALUATION_YEAR_TO,
        "training_lookback_years": 20,
        "feature_count": len(feature_names),
        "cell_count": len(cells),
        "total_cell_count": len(all_observed),
        "shard_count": args.shard_count,
        "shard_index": args.shard_index,
        "training_scope_policy": {
            "entrant_history_across_all_venues_and_cells": True,
            "related_distance_track_bias_expansion": True,
            "venue_surface_track_bias_expansion": True,
            "minimum_diverse_training_races": 100,
            "target_cell_only_training_allowed": False,
        },
        "leakage_guards": {
            "evaluation_entrants_seed_training_scope": True,
            "training_before_fold_cutoff": True,
            "training_evaluation_race_disjoint": True,
            "random_kfold_used": False,
        },
        "aggregate": aggregate_evaluations(cells),
        "cells": cells,
    }
    report_path = args.output_dir / "report.json"
    _write_json(report_path, report)
    print(
        json.dumps({"report": str(report_path), "aggregate": report["aggregate"]}, sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
