"""Wide-history native-feature CatBoost controls and corrected-history candidates."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from catboost import CatBoostRanker, Pool
from numpy.typing import NDArray
from predict_lib.model_meta import assert_no_within_race_leak_columns

from learning.cell_training_scope import CellScopeConfig, build_cell_training_scope
from learning.history_ablation import BASE_FEATURES, SPEED_FEATURES, exact_rank_metrics
from learning.relative_history_features import (
    RELATIVE_SPEED_FEATURES,
    add_relative_history_features,
)

# Conservative early-card policy: no final odds, weigh-in, observed weather,
# observed going, or lookups conditioned on that unavailable going. Historical
# odds summaries and past weight averages remain available predictors.
EARLY_UNAVAILABLE_FEATURES = frozenset(
    {
        "popularity_score",
        "odds_score",
        "weight_diff_from_avg",
        "weather_normalized",
        "track_condition_normalized",
        "current_baba_condition",
        "horse_baba_career_starts",
        "horse_baba_win_rate",
        "sire_baba_career_starts",
        "sire_baba_win_rate",
        "damsire_baba_career_starts",
        "damsire_baba_win_rate",
        "sire_horse_baba_combined_score",
        "going",
    }
)


@dataclass(frozen=True)
class RichConfig:
    native_features: tuple[str, ...]
    include_history: bool
    early_only: bool = True
    iterations: int = 300
    depth: int = 8
    threads: int = 2
    seed: int = 20260519


@dataclass(frozen=True)
class RichRows:
    training: pl.DataFrame
    evaluation: pl.DataFrame
    race_universe: pl.DataFrame
    seed_races: pl.DataFrame
    audit: dict[str, int]


def native_identity(frame: pl.DataFrame, *, context: str) -> pl.DataFrame:
    return frame.with_columns(
        pl.concat_str(
            "source", "race_date", "keibajo_code", "race_bango", separator="-"
        ).alias("race_id"),
        pl.col("ketto_toroku_bango").alias("horse_id"),
        pl.col("finish_position").alias("native_finish"),
        pl.lit(context).alias("native_context"),
    )


def prepare_rich_rows(
    history: pl.DataFrame,
    native: pl.DataFrame,
    *,
    scope_config: CellScopeConfig,
    native_features: tuple[str, ...],
) -> RichRows:
    """Left-join every required observation, rejecting lost labelled history."""
    if not native_features or len(set(native_features)) != len(native_features):
        raise ValueError("Native feature names must be nonempty and unique")
    assert_no_within_race_leak_columns(
        native_features, context="rich-history experiment"
    )
    if set(native_features).intersection(history.columns):
        raise ValueError("Native and research feature names must not collide")
    if native.select("race_id", "horse_id").is_duplicated().any():
        raise ValueError("Duplicate native race/horse keys")
    scope = build_cell_training_scope(history, config=scope_config)
    required = pl.concat([scope.training_rows, scope.evaluation_rows])
    columns = [
        "race_id",
        "horse_id",
        "native_finish",
        "native_context",
        *native_features,
    ]
    joined = required.join(
        native.select(columns), on=["race_id", "horse_id"], how="left", validate="m:1"
    )
    missing = joined.filter(pl.col("native_context").is_null())
    if missing.filter(pl.col("finish") > 0).height:
        raise ValueError("Native rows are missing for required labelled history")
    if joined.filter(
        pl.col("native_context").is_not_null()
        & ~pl.col("finish").eq_missing(pl.col("native_finish"))
    ).height:
        raise ValueError("Native and history finish labels disagree")
    cutoff = scope_config.training_cutoff.strftime("%Y%m%d")
    training = joined.filter(pl.col("race_date") < cutoff).sort(
        "race_id", "horse_number"
    )
    evaluation = joined.filter(pl.col("race_date") >= cutoff).sort(
        "race_id", "horse_number"
    )
    return RichRows(
        training=training,
        evaluation=evaluation,
        race_universe=scope.race_universe,
        seed_races=scope.seed_race_ids,
        audit={
            "required_rows": required.height,
            "retained_rows": joined.height,
            "missing_native_unlabelled": missing.height,
            "retired_base_rows": joined.filter(
                pl.col("native_context") == "retired-base"
            ).height,
            "training_rows": training.height,
            "evaluation_rows": evaluation.height,
        },
    )


def rich_matrix(frame: pl.DataFrame, features: tuple[str, ...]) -> NDArray[np.float64]:
    """Numeric parquet projection: serving-equivalent null->0, preserving NaN."""
    return frame.select(features).cast(pl.Float64).fill_null(0.0).to_numpy()


def rich_feature_names(config: RichConfig) -> tuple[str, ...]:
    names = config.native_features
    if config.include_history:
        names += BASE_FEATURES + SPEED_FEATURES + RELATIVE_SPEED_FEATURES
    return tuple(
        name
        for name in names
        if not config.early_only or name not in EARLY_UNAVAILABLE_FEATURES
    )


def train_rich_fold(
    rows: RichRows, *, config: RichConfig
) -> tuple[CatBoostRanker, pl.DataFrame, tuple[str, ...]]:
    features = rich_feature_names(config)
    training = rows.training.filter(pl.col("finish") > 0)
    evaluation = rows.evaluation.filter(pl.col("finish") > 0)
    if training.is_empty() or evaluation.is_empty():
        raise ValueError("Both rich-model splits require observed finishers")
    train_end, eval_start = training["race_date"].max(), evaluation["race_date"].min()
    if (
        not isinstance(train_end, str)
        or not isinstance(eval_start, str)
        or train_end >= eval_start
    ):
        raise ValueError("Rich-model training must strictly precede evaluation")
    model = CatBoostRanker(
        loss_function="YetiRank",
        iterations=config.iterations,
        depth=config.depth,
        learning_rate=0.05,
        l2_leaf_reg=3.0,
        random_seed=config.seed,
        thread_count=config.threads,
        verbose=False,
        allow_writing_files=False,
    )
    pool = Pool(
        rich_matrix(training, features),
        label=(4 - training["finish"]).clip(lower_bound=0).to_numpy(),
        group_id=training["race_id"].cast(pl.Categorical).to_physical().to_numpy(),
        feature_names=list(features),
    )
    model.fit(pool)
    scores = model.predict(rich_matrix(evaluation, features))
    predictions = (
        evaluation.select("race_id", "race_date", "horse_id", "horse_number", "finish")
        .with_columns(
            pl.Series("score", scores),
        )
        .with_columns(
            pl.col("score")
            .rank(method="ordinal", descending=True)
            .over("race_id")
            .alias("predicted_rank")
        )
    )
    return model, predictions, features


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--native", required=True)
    parser.add_argument("--retired", required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--year", type=int, choices=range(2020, 2027), required=True)
    parser.add_argument("--include-history", action="store_true")
    parser.add_argument("--retrospective-context", action="store_true")
    parser.add_argument("--iterations", type=int, default=300)
    args = parser.parse_args(argv)
    metadata = json.loads(args.metadata.read_text(encoding="utf-8"))
    names = tuple(str(name) for name in metadata["feature_names"])
    history = add_relative_history_features(pl.read_parquet(args.history))
    native = pl.concat(
        [
            native_identity(pl.read_parquet(args.native), context="full"),
            native_identity(pl.read_parquet(args.retired), context="retired-base"),
        ],
        how="diagonal_relaxed",
    )
    scope_config = CellScopeConfig(
        cell_id="ban-ei-83",
        category="ban-ei",
        dimensions=(("venue", "83"),),
        training_cutoff=date(args.year, 1, 1),
        evaluation_end=min(date(args.year, 12, 31), date(2026, 9, 11)),
    )
    rows = prepare_rich_rows(
        history, native, scope_config=scope_config, native_features=names
    )
    config = RichConfig(
        native_features=names,
        include_history=args.include_history,
        early_only=not args.retrospective_context,
        iterations=args.iterations,
    )
    model, predictions, features = train_rich_fold(rows, config=config)
    args.output.mkdir(parents=True, exist_ok=True)
    model.save_model(args.output / "model.cbm")
    predictions.write_parquet(args.output / "predictions.parquet")
    rows.race_universe.write_parquet(args.output / "scope-races.parquet")
    rows.seed_races.write_parquet(args.output / "seed-races.parquet")
    report = {
        "year": args.year,
        "venue": "83",
        "source": "local-pg",
        "seed_years": 20,
        "features": features,
        "include_history": config.include_history,
        "early_only": config.early_only,
        "excluded_unavailable_features": sorted(EARLY_UNAVAILABLE_FEATURES)
        if config.early_only
        else [],
        "iterations": config.iterations,
        "depth": config.depth,
        "seed": config.seed,
        "objective": "YetiRank",
        "relevance": "max(4-finish,0)",
        "training_end": rows.training["race_date"].max(),
        "scope_audit": rows.audit,
        "metrics": exact_rank_metrics(predictions),
        "promotion_eligible": False,
        "retired_context_caveat": "Real base features retained; 83-only extension columns explicitly null before serving-compatible projection",
    }
    (args.output / "report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
