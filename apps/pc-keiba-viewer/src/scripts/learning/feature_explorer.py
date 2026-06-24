"""Feature subset exploration using Optuna + Walk-Forward evaluation."""

from __future__ import annotations

import argparse
import json
import math
import random
from collections.abc import Callable
from typing import Final, Literal, TypedDict, cast

import optuna
import pandas as pd

from learning.feature_registry import FeatureRegistry
from finish_position_catboost import (
    CATEGORICAL_FEATURE_NAMES as CB_CATEGORICAL_FEATURE_NAMES,
    train_catboost_ranker,
)
from finish_position_lightgbm import (
    CATEGORICAL_FEATURE_COLUMNS,
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
VALIDATION_YEAR_POOL: Final[list[int]] = [2021, 2022, 2023, 2024, 2025]
DEFAULT_VALIDATION_YEARS_PER_ROUND: Final[int] = 2
MIN_FEATURES: Final[int] = 5
DEFAULT_BACKENDS: Final[tuple[ModelBackend, ...]] = ("lightgbm", "xgboost", "catboost")
CATEGORY_BACKENDS: Final[dict[str, tuple[ModelBackend, ...]]] = {
    "jra": ("catboost",),
    "nar": ("xgboost",),
    "ban-ei": ("catboost",),
}

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

_ALLOWED_CATEGORICAL: Final[frozenset[str]] = frozenset(CATEGORICAL_FEATURE_COLUMNS) | frozenset(
    CB_CATEGORICAL_FEATURE_NAMES
)

_RELEVANCE_MAP: Final[dict[int, float]] = {1: 3.0, 2: 2.0, 3: 1.0}


def _is_model_safe_feature(df: pd.DataFrame, col: str) -> bool:
    return col in _ALLOWED_CATEGORICAL or pd.api.types.is_numeric_dtype(df[col])


class ExplorationResult(TypedDict):
    trial_id: str
    ndcg_at_3: float
    feature_names: list[str]
    promoted: bool


def select_round_validation_years(
    round_num: int,
    pool: list[int],
    blind_holdout_year: int,
    k: int = DEFAULT_VALIDATION_YEARS_PER_ROUND,
) -> list[int]:
    """Pick k validation years for one round, excluding the blind holdout year.

    Seeded by round_num so each round is reproducible yet rounds differ, which
    stops Optuna from overfitting a single fixed eval set (selection bias). The
    blind holdout year is never returned, keeping it untouched for the final
    promotion decision.
    """
    eligible = sorted(y for y in pool if y != blind_holdout_year)
    if not eligible:
        raise ValueError(
            "validation year pool has no eligible years after excluding "
            "the blind holdout year"
        )
    rng = random.Random(round_num)
    count = min(k, len(eligible))
    return sorted(rng.sample(eligible, count))


def _ndcg_at_3_from_valid_df(valid_df: pd.DataFrame) -> float:
    ndcg_scores: list[float] = []
    for _, group in valid_df.groupby("race_id"):
        valid_group = group.dropna(subset=["predicted_rank", "finish_position"])
        sorted_group = valid_group.sort_values("predicted_rank")
        dcg = sum(
            _RELEVANCE_MAP.get(int(finish_pos), 0.0) / math.log2(rank_idx + 1)
            for rank_idx, finish_pos in enumerate(
                sorted_group["finish_position"].tolist()[:3], start=1
            )
        )
        ideal_relevances = sorted(
            (_RELEVANCE_MAP.get(int(fp), 0.0) for fp in group["finish_position"] if pd.notna(fp)),
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
    _, predictions, _ = run_walk_forward_fold(fold, params)
    valid_with_pos = fold["valid_df"][["race_id", "ketto_toroku_bango", "finish_position"]].merge(
        predictions,
        on=["race_id", "ketto_toroku_bango"],
        how="left",
    )
    return _ndcg_at_3_from_valid_df(valid_with_pos)


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
    feature_cols = [
        c
        for c in fold["train_df"].columns
        if c not in excluded and _is_model_safe_feature(fold["train_df"], c)
    ]
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


def _predict_fold_lightgbm(fold: FoldSplit, params: TrainingParams) -> pd.DataFrame:
    _, predictions, _ = run_walk_forward_fold(fold, params)
    return fold["valid_df"][["race_id", "ketto_toroku_bango", "finish_position"]].merge(
        predictions,
        on=["race_id", "ketto_toroku_bango"],
        how="left",
    )


def _predict_fold_xgboost(fold: FoldSplit) -> pd.DataFrame | None:
    feature_cols = _xgb_numeric_features(fold["train_df"], list(fold["train_df"].columns))
    if not feature_cols:
        return None
    _, result = train_xgboost_ranker(
        fold["train_df"], fold["valid_df"], feature_cols, _XGB_ARGS,
    )
    return cast(pd.DataFrame, result["valid_predictions"])


def _predict_fold_catboost(fold: FoldSplit) -> pd.DataFrame | None:
    excluded = set(META_COLUMNS) | _LABEL_COLS
    feature_cols = [
        c
        for c in fold["train_df"].columns
        if c not in excluded and _is_model_safe_feature(fold["train_df"], c)
    ]
    if not feature_cols:
        return None
    result = train_catboost_ranker(
        fold["train_df"], fold["valid_df"], feature_cols, _CB_ARGS,
    )
    return cast(pd.DataFrame, result["valid_predictions"])


def predict_fold_with_backend(
    fold: FoldSplit,
    backend: ModelBackend,
    lgb_params: TrainingParams,
) -> pd.DataFrame | None:
    """Train one fold and return its valid predictions with a ``predicted_rank`` column.

    Mirrors :func:`run_fold_with_backend` but yields the per-row prediction frame
    (race_id, ketto_toroku_bango, predicted_rank, finish_position) instead of a scalar
    NDCG, so callers can break the score down by subgroup.
    """
    if backend == "lightgbm":
        return _predict_fold_lightgbm(fold, lgb_params)
    if backend == "xgboost":
        return _predict_fold_xgboost(fold)
    return _predict_fold_catboost(fold)


def select_features(
    df: pd.DataFrame,
    feature_mask: dict[str, bool],
) -> pd.DataFrame:
    meta_and_label = set(META_COLUMNS) | _LABEL_COLS | {"race_id"}
    selected = [col for col, keep in feature_mask.items() if keep]
    keep_cols = [c for c in df.columns if c in meta_and_label or c in selected]
    return df[keep_cols]


def _select_fold_features(fold: FoldSplit, feature_set: set[str]) -> FoldSplit:
    """Select only the needed features from a pre-split fold."""
    meta_and_label = set(META_COLUMNS) | _LABEL_COLS | {"race_id"}
    keep_cols = [
        c
        for c in fold["train_df"].columns
        if c in meta_and_label
        or (c in feature_set and _is_model_safe_feature(fold["train_df"], c))
    ]
    return {
        "train_df": fold["train_df"][keep_cols],
        "valid_df": fold["valid_df"][keep_cols],
        "valid_year": fold["valid_year"],
    }


def select_fold_features(fold: FoldSplit, feature_set: set[str]) -> FoldSplit:
    """Public alias for :func:`_select_fold_features` used by external callers."""
    return _select_fold_features(fold, feature_set)


def evaluate_feature_set(
    df: pd.DataFrame,
    feature_names: list[str],
    validation_years: list[int],
    train_start: str,
    params: TrainingParams,
    backends: tuple[ModelBackend, ...] = DEFAULT_BACKENDS,
) -> float:
    feature_set = set(feature_names)
    feature_mask = {col: col in feature_set for col in df.columns}
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
    pre_split_folds: dict[int, FoldSplit] = {}
    for year in validation_years:
        fold = split_walk_forward(df, train_start, year)
        if not fold["train_df"].empty and not fold["valid_df"].empty:
            pre_split_folds[year] = fold

    def objective(trial: optuna.Trial) -> float:
        feature_mask = {
            col: trial.suggest_categorical(f"use_{col}", [True, False])
            for col in candidate_features
        }
        selected = [col for col, keep in feature_mask.items() if keep]
        if len(selected) < MIN_FEATURES:
            return 0.0
        feature_set = set(selected)
        ndcg_scores: list[float] = []
        for step, fold in enumerate(pre_split_folds.values()):
            fold_with_features = _select_fold_features(fold, feature_set)
            fold_scores: list[float] = []
            for backend in backends:
                score = run_fold_with_backend(fold_with_features, backend, params)
                if score is not None:
                    fold_scores.append(score)
            if fold_scores:
                ndcg_scores.extend(fold_scores)
                intermediate = sum(ndcg_scores) / len(ndcg_scores)
                trial.report(intermediate, step)
                if trial.should_prune():
                    raise optuna.TrialPruned()
        ndcg = sum(ndcg_scores) / len(ndcg_scores) if ndcg_scores else 0.0
        trial_id = f"{study_name}_trial_{trial.number}"
        active_entry = registry.get_active_entry()
        active_ndcg = active_entry["ndcg_at_3"] if active_entry is not None else 0.0
        delta_pp = (ndcg - active_ndcg) * 100
        definition = json.dumps(
            {"features": selected, "trial": trial.number, "delta_pp": delta_pp}
        )
        promoted = registry.maybe_promote(trial_id, ndcg, selected, definition)
        trial.set_user_attr("promoted", promoted)
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
        pruner=optuna.pruners.MedianPruner(n_startup_trials=3, n_warmup_steps=0),
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    results: list[ExplorationResult] = []
    for trial in study.trials:
        if trial.value is None:
            continue
        selected = [
            col[len("use_"):]
            for col, val in trial.params.items()
            if col.startswith("use_") and val
        ]
        results.append(
            ExplorationResult(
                trial_id=f"{study_name}_trial_{trial.number}",
                ndcg_at_3=float(trial.value),
                feature_names=selected,
                promoted=bool(trial.user_attrs.get("promoted", False)),
            )
        )
    return results
