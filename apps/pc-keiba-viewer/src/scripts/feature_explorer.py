"""Feature subset exploration using Optuna + Walk-Forward evaluation."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Final, TypedDict

import optuna
import pandas as pd

from feature_registry import FeatureRegistry
from finish_position_lightgbm import (
    META_COLUMNS,
    OBJECTIVE_LAMBDARANK,
    TrainingParams,
    resolve_feature_columns,
    run_walk_forward_fold,
    split_walk_forward,
)

optuna.logging.set_verbosity(optuna.logging.WARNING)

DEFAULT_TRAIN_START: Final[str] = "20160101"
DEFAULT_VALIDATION_YEARS: Final[list[int]] = [2023, 2024]
MIN_FEATURES: Final[int] = 5

DEFAULT_PARAMS: Final[TrainingParams] = {
    "lambda_l2": 1.0,
    "learning_rate": 0.05,
    "min_child_samples": 20,
    "num_iterations": 300,
    "num_leaves": 63,
    "objective": OBJECTIVE_LAMBDARANK,
}


class ExplorationResult(TypedDict):
    trial_id: str
    ndcg_at_3: float
    feature_names: list[str]
    promoted: bool


def select_features(
    df: pd.DataFrame,
    feature_mask: dict[str, bool],
) -> pd.DataFrame:
    meta_and_label = set(META_COLUMNS) | {
        "finish_position",
        "finish_norm",
        "target_corner_1_norm",
        "target_corner_3_norm",
        "target_corner_4_norm",
        "target_running_style_class",
        "race_id",
    }
    selected = [col for col, keep in feature_mask.items() if keep]
    keep_cols = [c for c in df.columns if c in meta_and_label or c in selected]
    return df[keep_cols].copy()


def evaluate_feature_set(
    df: pd.DataFrame,
    feature_names: list[str],
    validation_years: list[int],
    train_start: str,
    params: TrainingParams,
) -> float:
    meta_and_label = set(META_COLUMNS) | {
        "finish_position",
        "finish_norm",
        "target_corner_1_norm",
        "target_corner_3_norm",
        "target_corner_4_norm",
        "target_running_style_class",
        "race_id",
    }
    keep_cols = [c for c in df.columns if c in meta_and_label or c in feature_names]
    subset_df = df[keep_cols].copy()
    ndcg_scores: list[float] = []
    for year in validation_years:
        fold = split_walk_forward(subset_df, train_start, year)
        if fold["train_df"].empty or fold["valid_df"].empty:
            continue
        _, _, metrics = run_walk_forward_fold(fold, params)
        ndcg_scores.append(metrics["ndcg_at_3"])
    if not ndcg_scores:
        return 0.0
    return sum(ndcg_scores) / len(ndcg_scores)


def build_objective(
    df: pd.DataFrame,
    candidate_features: list[str],
    validation_years: list[int],
    train_start: str,
    params: TrainingParams,
    registry: FeatureRegistry,
    study_name: str,
) -> Callable[[optuna.Trial], float]:
    def objective(trial: optuna.Trial) -> float:
        feature_mask = {
            col: trial.suggest_categorical(f"use_{col}", [True, False])
            for col in candidate_features
        }
        selected = [col for col, keep in feature_mask.items() if keep]
        if len(selected) < MIN_FEATURES:
            return 0.0
        ndcg = evaluate_feature_set(df, selected, validation_years, train_start, params)
        trial_id = f"{study_name}_trial_{trial.number}"
        definition = json.dumps({"features": selected, "trial": trial.number})
        registry.maybe_promote(trial_id, ndcg, selected, definition)
        return ndcg

    return objective


def run_exploration(
    df: pd.DataFrame,
    registry: FeatureRegistry,
    n_trials: int = 50,
    validation_years: list[int] = DEFAULT_VALIDATION_YEARS,
    train_start: str = DEFAULT_TRAIN_START,
    params: TrainingParams = DEFAULT_PARAMS,
    study_name: str = "feature_exploration",
    storage: str | None = None,
) -> list[ExplorationResult]:
    candidate_features = resolve_feature_columns(list(df.columns))
    objective = build_objective(
        df,
        candidate_features,
        validation_years,
        train_start,
        params,
        registry,
        study_name,
    )
    study = optuna.create_study(
        direction="maximize",
        study_name=study_name,
        storage=storage,
        load_if_exists=True,
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    results: list[ExplorationResult] = []
    for trial in study.trials:
        if trial.value is None:
            continue
        selected = [
            col
            for col in candidate_features
            if trial.params.get(f"use_{col}", False)
        ]
        results.append(
            ExplorationResult(
                trial_id=f"{study_name}_trial_{trial.number}",
                ndcg_at_3=float(trial.value),
                feature_names=selected,
                promoted=False,
            )
        )
    return results
