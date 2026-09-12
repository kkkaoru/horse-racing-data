"""Fixed-budget, out-of-time ranker ablations; never a production promotion gate."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final, Literal

import numpy as np
import polars as pl
from catboost import CatBoostRanker, Pool

from learning.cell_training_scope import CellScopeConfig, build_cell_training_scope
from learning.relative_history_features import (
    RELATIVE_SPEED_FEATURES,
    add_relative_history_features,
)

BASE_FEATURES: Final[tuple[str, ...]] = (
    "venue",
    "distance_m",
    "field_size",
    "age",
    "track_code",
    "sex_code",
    "going",
    "month",
    "past_runs",
    "past_finish_mean",
    "past_win_rate",
    "days_since",
    "jockey_past_win_rate",
    "trainer_past_win_rate",
)
SPEED_FEATURES: Final[tuple[str, ...]] = ("past_speed_mean", "past_speed_365d")
OBJECTIVES: Final[dict[str, str]] = {
    "winner": "QuerySoftMax",
    "top5": "YetiRank:mode=NDCG;top=5;dcg_type=Base;dcg_denominator=LogPosition",
}


@dataclass(frozen=True)
class AblationConfig:
    include_speed: bool
    iterations: int = 200
    depth: int = 6
    learning_rate: float = 0.05
    seed: int = 20260912
    threads: int = 4
    objective: Literal["winner", "top5"] = "winner"
    include_relative_speed: bool = False


@dataclass(frozen=True)
class AblationResult:
    model: CatBoostRanker
    predictions: pl.DataFrame
    metrics: dict[str, float]
    features: tuple[str, ...]


def exact_rank_metrics(predictions: pl.DataFrame) -> dict[str, float]:
    """Report per-race Top1 and exact rank2–5 accuracy, including short fields."""
    races = predictions["race_id"].n_unique()
    if races == 0:
        raise ValueError("Cannot evaluate an empty race set")
    return {
        f"rank{rank}": predictions.filter(
            (pl.col("predicted_rank") == rank) & (pl.col("finish") == rank)
        )["race_id"].n_unique()
        / races
        for rank in range(1, 6)
    }


def train_ablation_fold(
    training: pl.DataFrame, evaluation: pl.DataFrame, *, config: AblationConfig
) -> AblationResult:
    """Fit without eval_set/early stopping: held-out outcomes cannot select trees.

    Compare this compact experimental model only to its matched ablation arm.
    Production adoption additionally requires a matched current-production
    baseline, independent confirmation, inference parity and cell release gates.
    """
    if training.is_empty() or evaluation.is_empty():
        raise ValueError("Both training and evaluation rows are required")
    train_end = training["race_date"].max()
    evaluation_start = evaluation["race_date"].min()
    if not isinstance(train_end, str) or not isinstance(evaluation_start, str):
        raise TypeError("Race dates must be non-null YYYYMMDD strings")
    if train_end >= evaluation_start:
        raise ValueError("Training must strictly precede evaluation")
    features = BASE_FEATURES + SPEED_FEATURES if config.include_speed else BASE_FEATURES
    if config.include_relative_speed:
        features += RELATIVE_SPEED_FEATURES
    train = training.filter(pl.col("finish").is_not_null()).sort(
        "race_id", "horse_number"
    )
    valid = evaluation.filter(pl.col("finish").is_not_null()).sort(
        "race_id", "horse_number"
    )
    if train.is_empty() or valid.is_empty():
        raise ValueError("Both splits require observed finish labels")
    train_pool = Pool(
        data=train.select(features).cast(pl.Float32).to_numpy(),
        label=(
            (train["finish"] == 1).cast(pl.Int32).to_numpy()
            if config.objective == "winner"
            else (6 - train["finish"]).clip(lower_bound=0).to_numpy()
        ),
        group_id=train["race_id"].cast(pl.Categorical).to_physical().to_numpy(),
        feature_names=list(features),
    )
    model = CatBoostRanker(
        loss_function=OBJECTIVES[config.objective],
        iterations=config.iterations,
        depth=config.depth,
        learning_rate=config.learning_rate,
        random_seed=config.seed,
        thread_count=config.threads,
        verbose=False,
        allow_writing_files=False,
    )
    model.fit(train_pool)
    return predict_ablation(model, valid, features=features)


def predict_ablation(
    model: CatBoostRanker,
    evaluation: pl.DataFrame,
    *,
    features: tuple[str, ...],
) -> AblationResult:
    """Preserve the research model's Float32/NaN projection when re-scoring."""
    if list(features) != model.feature_names_:
        raise ValueError(
            "Saved model feature order does not match the declared features"
        )
    valid = evaluation.filter(pl.col("finish").is_not_null()).sort(
        "race_id", "horse_number"
    )
    if valid.is_empty():
        raise ValueError("Evaluation requires observed finish labels")
    scores = np.asarray(
        model.predict(valid.select(features).cast(pl.Float32).to_numpy())
    )
    if scores.shape != (valid.height,) or not np.isfinite(scores).all():
        raise ValueError("Expected one finite research score per runner")
    predictions = (
        valid.select("race_id", "race_date", "horse_id", "horse_number", "finish")
        .with_columns(pl.Series("score", scores))
        .with_columns(
            pl.col("score")
            .rank(method="ordinal", descending=True)
            .over("race_id")
            .alias("predicted_rank")
        )
    )
    return AblationResult(
        model=model,
        predictions=predictions,
        metrics=exact_rank_metrics(predictions),
        features=features,
    )


def run_ablation_experiment(
    feature_path: Path,
    output_dir: Path,
    *,
    scope_config: CellScopeConfig,
    config: AblationConfig,
) -> dict[str, object]:
    """Persist a reproducible local-PG experiment in a caller-owned directory."""
    history = pl.read_parquet(feature_path)
    if config.include_relative_speed:
        history = add_relative_history_features(history)
    scope = build_cell_training_scope(history, config=scope_config)
    result = train_ablation_fold(
        scope.training_rows, scope.evaluation_rows, config=config
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    result.model.save_model(str(output_dir / "model.cbm"))
    result.predictions.write_parquet(output_dir / "predictions.parquet")
    scope.seed_race_ids.write_parquet(output_dir / "seed-races.parquet")
    scope.race_universe.write_parquet(output_dir / "scope-races.parquet")
    report: dict[str, object] = {
        "arm": "speed" if config.include_speed else "no-speed",
        "cell_id": scope_config.cell_id,
        "category": scope_config.category,
        "dimensions": dict(scope_config.dimensions),
        "year": scope_config.training_cutoff.year,
        "seed_years": scope_config.seed_years,
        "training_cutoff": scope_config.training_cutoff.isoformat(),
        "evaluation_end": scope_config.evaluation_end.isoformat(),
        "training_rows": scope.training_rows.height,
        "evaluation_rows": scope.evaluation_rows.height,
        "training_start": scope.training_rows["race_date"].min(),
        "training_end": scope.training_rows["race_date"].max(),
        "metrics": result.metrics,
        "features": result.features,
        "relative_speed": config.include_relative_speed,
        "iterations": config.iterations,
        "objective": OBJECTIVES[config.objective],
        "label_contract": "winner-indicator"
        if config.objective == "winner"
        else "max(6-finish,0)",
        "depth": config.depth,
        "learning_rate": config.learning_rate,
        "seed": config.seed,
        "source": "local-pg",
        "feature_path": str(feature_path),
        "promotion_eligible": False,
    }
    (output_dir / "report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    return report


def parse_ablation_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--venue", choices=("54", "55", "83"), required=True)
    parser.add_argument("--year", type=int, choices=range(2020, 2027), required=True)
    parser.add_argument("--include-speed", action="store_true")
    parser.add_argument("--include-relative-speed", action="store_true")
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--objective", choices=("winner", "top5"), default="winner")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_ablation_args(argv)
    category = "ban-ei" if args.venue == "83" else "nar"
    scope = CellScopeConfig(
        cell_id=f"{category}-{args.venue}",
        category=category,
        dimensions=(("venue", args.venue),),
        training_cutoff=date(args.year, 1, 1),
        evaluation_end=min(date(args.year, 12, 31), date(2026, 9, 11)),
    )
    config = AblationConfig(
        include_speed=args.include_speed,
        iterations=args.iterations,
        threads=args.threads,
        objective=args.objective,
        include_relative_speed=args.include_relative_speed,
    )
    report = run_ablation_experiment(
        args.features, args.output, scope_config=scope, config=config
    )
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
