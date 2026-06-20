"""Feature subset exploration using Optuna + Walk-Forward evaluation."""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Callable
from pathlib import Path
from typing import Final, Literal, TypedDict, cast

import optuna
import pandas as pd

from feature_registry import FeatureRegistry
from finish_position_catboost import train_catboost_ranker
from finish_position_lightgbm import (
    LABEL_COLUMNS,
    META_COLUMNS,
    OBJECTIVE_LAMBDARANK,
    FoldSplit,
    TrainingParams,
    resolve_feature_columns,
    run_walk_forward_fold,
    split_walk_forward,
)
from finish_position_xgboost import train_xgboost_ranker

optuna.logging.set_verbosity(optuna.logging.WARNING)

ModelBackend = Literal["lightgbm", "xgboost", "catboost"]

DEFAULT_TRAIN_START: Final[str] = "20160101"
DEFAULT_VALIDATION_YEARS: Final[list[int]] = [2023, 2024]
MIN_FEATURES: Final[int] = 5
DEFAULT_BACKENDS: Final[tuple[ModelBackend, ...]] = ("lightgbm", "xgboost", "catboost")

DEFAULT_PARAMS: Final[TrainingParams] = {
    "lambda_l2": 1.0,
    "learning_rate": 0.05,
    "min_child_samples": 20,
    "num_iterations": 300,
    "num_leaves": 63,
    "objective": OBJECTIVE_LAMBDARANK,
}

_XGB_ARGS: Final[argparse.Namespace] = argparse.Namespace(
    learning_rate=0.05,
    max_depth=6,
    min_child_weight=1,
    reg_lambda=1.0,
    seed=42,
    num_rounds=300,
    early_stopping_rounds=50,
    relevance_rank1=3,
    relevance_rank2=2,
    relevance_rank3=1,
)

_CB_ARGS: Final[argparse.Namespace] = argparse.Namespace(
    learning_rate=0.05,
    depth=6,
    l2_leaf_reg=3.0,
    seed=42,
    iterations=300,
    early_stopping_rounds=50,
    relevance_rank1=3,
    relevance_rank2=2,
    relevance_rank3=1,
    no_cat_features=False,
)

_LABEL_COLS: Final[frozenset[str]] = frozenset(LABEL_COLUMNS)

_RELEVANCE_MAP: Final[dict[int, float]] = {1: 3.0, 2: 2.0, 3: 1.0}


class ExplorationResult(TypedDict):
    trial_id: str
    ndcg_at_3: float
    feature_names: list[str]
    promoted: bool


def _ndcg_at_3_from_valid_df(valid_df: pd.DataFrame) -> float:
    ndcg_scores: list[float] = []
    for _, group in valid_df.groupby("race_id"):
        sorted_group = group.sort_values("predicted_rank")
        dcg = 0.0
        for rank_idx, finish_pos in enumerate(
            sorted_group["finish_position"].tolist()[:3], start=1
        ):
            dcg += _RELEVANCE_MAP.get(int(finish_pos), 0.0) / math.log2(rank_idx + 1)
        ideal_relevances = sorted(
            (_RELEVANCE_MAP.get(int(fp), 0.0) for fp in group["finish_position"]),
            reverse=True,
        )[:3]
        ideal_dcg = sum(
            rel / math.log2(i + 2)
            for i, rel in enumerate(ideal_relevances)
            if rel > 0.0
        )
        if ideal_dcg > 0.0:
            ndcg_scores.append(dcg / ideal_dcg)
    return sum(ndcg_scores) / len(ndcg_scores) if ndcg_scores else 0.0


def _xgb_numeric_features(df: pd.DataFrame, feature_names: list[str]) -> list[str]:
    excluded = set(META_COLUMNS) | _LABEL_COLS
    return [
        c for c in feature_names
        if c not in excluded and pd.api.types.is_numeric_dtype(df[c])
    ]


def _run_fold_lightgbm(fold: FoldSplit, params: TrainingParams) -> float:
    _, _, metrics = run_walk_forward_fold(fold, params)
    return metrics["ndcg_at_3"]


def _run_fold_xgboost(fold: FoldSplit) -> float | None:
    feature_cols = _xgb_numeric_features(fold["train_df"], list(fold["train_df"].columns))
    if not feature_cols:
        return None
    _, result = train_xgboost_ranker(
        fold["train_df"], fold["valid_df"], feature_cols, _XGB_ARGS,
    )
    valid_df = cast(pd.DataFrame, result["valid_predictions"])
    return _ndcg_at_3_from_valid_df(valid_df)


def _run_fold_catboost(fold: FoldSplit) -> float | None:
    excluded = set(META_COLUMNS) | _LABEL_COLS
    feature_cols = [c for c in fold["train_df"].columns if c not in excluded]
    if not feature_cols:
        return None
    result = train_catboost_ranker(
        fold["train_df"], fold["valid_df"], feature_cols, _CB_ARGS,
    )
    valid_df = cast(pd.DataFrame, result["valid_predictions"])
    return _ndcg_at_3_from_valid_df(valid_df)


def run_fold_with_backend(
    fold: FoldSplit,
    backend: ModelBackend,
    lgb_params: TrainingParams,
) -> float | None:
    if backend == "lightgbm":
        return _run_fold_lightgbm(fold, lgb_params)
    if backend == "xgboost":
        return _run_fold_xgboost(fold)
    return _run_fold_catboost(fold)


def select_features(
    df: pd.DataFrame,
    feature_mask: dict[str, bool],
) -> pd.DataFrame:
    meta_and_label = set(META_COLUMNS) | _LABEL_COLS | {"race_id"}
    selected = [col for col, keep in feature_mask.items() if keep]
    keep_cols = [c for c in df.columns if c in meta_and_label or c in selected]
    return df[keep_cols].copy()


def evaluate_feature_set(
    df: pd.DataFrame,
    feature_names: list[str],
    validation_years: list[int],
    train_start: str,
    params: TrainingParams,
    backends: tuple[ModelBackend, ...] = DEFAULT_BACKENDS,
) -> float:
    feature_mask = {col: col in set(feature_names) for col in df.columns}
    subset_df = select_features(df, feature_mask)
    ndcg_scores: list[float] = []
    for year in validation_years:
        fold = split_walk_forward(subset_df, train_start, year)
        if fold["train_df"].empty or fold["valid_df"].empty:
            continue
        for backend in backends:
            score = run_fold_with_backend(fold, backend, params)
            if score is not None:
                ndcg_scores.append(score)
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
    backends: tuple[ModelBackend, ...] = DEFAULT_BACKENDS,
) -> Callable[[optuna.Trial], float]:
    def objective(trial: optuna.Trial) -> float:
        feature_mask = {
            col: trial.suggest_categorical(f"use_{col}", [True, False])
            for col in candidate_features
        }
        selected = [col for col, keep in feature_mask.items() if keep]
        if len(selected) < MIN_FEATURES:
            return 0.0
        ndcg = evaluate_feature_set(
            df, selected, validation_years, train_start, params, backends,
        )
        trial_id = f"{study_name}_trial_{trial.number}"
        definition = json.dumps({"features": selected, "trial": trial.number})
        registry.maybe_promote(trial_id, ndcg, selected, definition)
        return ndcg

    return objective


def run_exploration(
    df: pd.DataFrame,
    registry: FeatureRegistry,
    n_trials: int = 50,
    validation_years: list[int] | None = None,
    train_start: str = DEFAULT_TRAIN_START,
    params: TrainingParams = DEFAULT_PARAMS,
    study_name: str = "feature_exploration",
    storage: str | None = None,
    backends: tuple[ModelBackend, ...] = DEFAULT_BACKENDS,
) -> list[ExplorationResult]:
    effective_years = list(validation_years) if validation_years is not None else list(DEFAULT_VALIDATION_YEARS)
    candidate_features = resolve_feature_columns(list(df.columns))
    objective = build_objective(
        df,
        candidate_features,
        effective_years,
        train_start,
        params,
        registry,
        study_name,
        backends,
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
