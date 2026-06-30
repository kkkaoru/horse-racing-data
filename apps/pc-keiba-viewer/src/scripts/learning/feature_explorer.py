"""Feature subset exploration using Optuna + Walk-Forward evaluation."""

from __future__ import annotations

import argparse
import json
import math
import random
from collections.abc import Callable, Sequence
from typing import Final, Literal, Protocol, TypedDict, cast, final

import optuna
import polars as pl
from optuna.distributions import BaseDistribution, CategoricalDistribution
from optuna.samplers import BaseSampler, TPESampler
from optuna.trial import FrozenTrial, TrialState, create_trial

from learning.feature_registry import NDCG_IMPROVEMENT_THRESHOLD, FeatureRegistry
from learning.feature_selection_policy import (
    compute_feature_set_hash as compute_normalized_feature_set_hash,
)
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
from learning.subgroup_diagnostics import SubgroupMetrics, assign_subgroup_keys

optuna.logging.set_verbosity(optuna.logging.WARNING)

ModelBackend = Literal["lightgbm", "xgboost", "catboost"]

DEFAULT_TRAIN_START: Final[str] = "20160101"
DEFAULT_VALIDATION_YEARS: Final[list[int]] = [2023, 2024]
VALIDATION_YEAR_POOL: Final[list[int]] = [2021, 2022, 2023, 2024, 2025]
DEFAULT_VALIDATION_YEARS_PER_ROUND: Final[int] = 2
MIN_FEATURES: Final[int] = 5
# Random startup trials before TPE begins modelling. With a tiny per-round trial
# budget a large startup count would degenerate the search to pure random sampling,
# so it is kept small and capped below the round's n_trials at study-build time.
TPE_N_STARTUP_TRIALS: Final[int] = 5
# Sampler seed keeps a round's feature-selection draws reproducible across reruns.
SAMPLER_SEED: Final[int] = 42
# How many of the registry's best prior trials to feed back into a resumed study so
# TPE models the known-good region instead of relearning it from random draws.
DEFAULT_WARM_START_TOP_K: Final[int] = 20
# Method label recorded with a deduplicated trial so the block-TPE objective's
# cached scores never collide with forward/backward stepwise entries in a store
# keyed by (feature_set_hash, method).
BLOCK_METHOD: Final[str] = "block"
DEFAULT_STEPWISE_MAX_FEATURES: Final[int] = 50
DEFAULT_BACKWARD_MIN_FEATURES: Final[int] = 10
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
    presorted=True,
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
    presorted=True,
)

_SCREEN_XGB_ARGS: Final[argparse.Namespace] = argparse.Namespace(
    learning_rate=0.05,
    max_depth=6,
    min_child_weight=1,
    reg_lambda=1.0,
    seed=42,
    num_rounds=150,
    early_stopping_rounds=30,
    relevance_rank1=3,
    relevance_rank2=2,
    relevance_rank3=1,
    presorted=True,
)

_SCREEN_CB_ARGS: Final[argparse.Namespace] = argparse.Namespace(
    learning_rate=0.05,
    depth=6,
    l2_leaf_reg=3.0,
    seed=42,
    iterations=150,
    early_stopping_rounds=30,
    relevance_rank1=3,
    relevance_rank2=2,
    relevance_rank3=1,
    no_cat_features=False,
    presorted=True,
)

_LABEL_COLS: Final[frozenset[str]] = frozenset(LABEL_COLUMNS)

_EXCLUDED_COLS: Final[frozenset[str]] = frozenset(META_COLUMNS) | _LABEL_COLS

_META_AND_LABEL: Final[frozenset[str]] = frozenset(META_COLUMNS) | _LABEL_COLS | frozenset({"race_id"})

_ALLOWED_CATEGORICAL: Final[frozenset[str]] = frozenset(CATEGORICAL_FEATURE_COLUMNS) | frozenset(
    CB_CATEGORICAL_FEATURE_NAMES
)

_RELEVANCE_MAP: Final[dict[int, float]] = {1: 3.0, 2: 2.0, 3: 1.0}

FEATURE_GROUPS: Final[dict[str, tuple[str, ...]]] = {
    "odds": ("tansho_odds", "tansho_ninkijun", "odds_score", "popularity_score"),
    "jockey": ("jockey_",),
    "pedigree": ("pedigree_", "dam_sire_", "damsire_", "sire_"),
    "running_style": ("past_nige_", "past_senkou_", "past_sashi_", "past_oikomi_", "past_dominant_"),
    "corner": ("corner_pass_", "past_corner_", "horse_distance_corner_", "horse_grade_corner_", "horse_keibajo_corner_", "horse_track_corner_", "last_race_corner_"),
    "speed": ("speed_index_", "field_strength_"),
    "similar_race": ("sim_",),
    "weather": ("venue_precipitation_", "venue_temperature", "venue_wind_", "weather_normalized", "rain_x_", "cold_x_", "wind_x_"),
    "weight": ("weight_", "zogen_sa", "bataiju"),
    "race_condition": ("kyori", "kyori_band", "track_code", "grade_code", "track_condition_", "track_bias_", "shusso_tosu", "field_size_", "season_band", "kaisai_month", "babajotai_"),
    "recent_form": ("recent_", "last_race_", "last_3_", "finish_trend_", "consecutive_race_count", "days_since_last_race", "kohan3f_avg_5"),
    "career": ("career_", "same_distance_", "same_grade_", "same_keibajo_", "same_track_", "avg_finish", "experience_", "is_grade_race", "place_count_in_grade", "top1_count_in_grade", "rival_count_"),
    "trainer": ("trainer_",),
    "horse_identity": ("seibetsu_code", "is_newcomer_race", "umaban_norm", "nar_subclass", "kyoso_joken_code"),
}


def assign_feature_group(col: str) -> str:
    """Return the group name for a column, or 'other' if no match."""
    for group_name, prefixes in FEATURE_GROUPS.items():
        for prefix in prefixes:
            if col == prefix or col.startswith(prefix):
                return group_name
    return "other"


def group_features(candidate_features: list[str]) -> dict[str, list[str]]:
    """Group all candidate features by their semantic group."""
    groups: dict[str, list[str]] = {}
    for col in candidate_features:
        group = assign_feature_group(col)
        groups.setdefault(group, []).append(col)
    return groups


_IMPORTANCE_ACCUMULATOR: dict[str, list[float]] = {}


def compute_feature_importance(
    df: pl.DataFrame,
    feature_names: list[str],
    validation_years: list[int],
    train_start: str,
    backends: tuple[ModelBackend, ...],
) -> dict[str, float]:
    """Train a model with the given features and return per-feature importance scores.

    Uses CatBoost's get_feature_importance() or XGBoost's get_score().
    Returns a dict of feature_name -> importance_score (higher = more important).
    If training fails or no importance data is available, returns an empty dict.
    """
    importances: dict[str, float] = {}
    count = 0
    for year in validation_years:
        fold = split_walk_forward(df, train_start, year)
        if fold["train_df"].is_empty() or fold["valid_df"].is_empty():
            continue
        for backend in backends:
            fold_importances = _extract_fold_importance(fold, feature_names, backend)
            if fold_importances:
                for feat, score in fold_importances.items():
                    importances[feat] = importances.get(feat, 0.0) + score
                count += 1
    if count > 0:
        for feat in importances:
            importances[feat] /= count
    return importances


def _extract_fold_importance(
    fold: FoldSplit,
    feature_names: list[str],
    backend: ModelBackend,
) -> dict[str, float]:
    """Train one fold and extract feature importances from the model."""
    if backend == "catboost":
        safe = _model_safe_columns(fold["train_df"])
        feature_cols = [
            c for c in feature_names
            if c not in _EXCLUDED_COLS and c in safe
        ]
        if not feature_cols:
            return {}
        result = train_catboost_ranker(
            fold["train_df"], fold["valid_df"], feature_cols, _CB_ARGS,
        )
        model = result.get("model")
        get_importance = getattr(model, "get_feature_importance", None)
        if get_importance is None:
            return {}
        try:
            raw_importance = get_importance()
            return dict(zip(feature_cols, (float(v) for v in raw_importance), strict=False))
        except Exception:
            return {}
    if backend == "xgboost":
        numeric = _numeric_columns(fold["train_df"])
        feature_cols = [c for c in feature_names if c in numeric]
        if not feature_cols:
            return {}
        model, _ = train_xgboost_ranker(
            fold["train_df"], fold["valid_df"], feature_cols, _XGB_ARGS,
        )
        get_score = getattr(model, "get_score", None)
        if get_score is None:
            return {}
        try:
            scores: dict[str, float] = get_score(importance_type="gain")
            return {k: float(v) for k, v in scores.items() if k in feature_names}
        except Exception:
            return {}
    return {}


def get_importance_ranked_features(candidate_features: list[str]) -> list[str]:
    """Return features sorted by accumulated importance (descending).

    Features not in the accumulator are sorted to the end.
    """
    def sort_key(col: str) -> float:
        scores = _IMPORTANCE_ACCUMULATOR.get(col, [])
        if not scores:
            return 0.0
        return sum(scores) / len(scores)
    return sorted(candidate_features, key=sort_key, reverse=True)


def compute_cell_weights_from_accuracy(
    cell_metrics: list[SubgroupMetrics],
) -> dict[str, float]:
    """Inverse-accuracy weighting: underperforming cells get higher weight.

    Cell keys use the same canonical subgroup key as diagnostics and adoption:
    f"{category}_{surface}_{distance_band}_{class_label}_{season}_{venue}".
    Weight = 1 / max(accuracy, 0.01) normalized so mean weight = 1.0.
    """
    if not cell_metrics:
        return {}
    cell_weights: dict[str, float] = {}
    for m in cell_metrics:
        key = m["subgroup"]
        accuracy = m["top1_accuracy"]
        cell_weights[key] = 1.0 / max(accuracy, 0.01)
    mean_weight = sum(cell_weights.values()) / len(cell_weights)
    for key in cell_weights:
        cell_weights[key] /= mean_weight
    return cell_weights


def weighted_ndcg_at_3(
    valid_df: pl.DataFrame, cell_weights: dict[str, float]
) -> float:
    """NDCG@3 with per-race cell-based weighting."""
    if valid_df.is_empty():
        return 0.0

    required_cols = {"source", "keibajo_code", "track_code", "kyori"}
    available = set(valid_df.columns)
    if not required_cols.issubset(available):
        return _ndcg_at_3_from_valid_df(valid_df)

    dcg = (
        valid_df.drop_nulls(subset=["predicted_rank", "finish_position"])
        .with_columns(
            _slot=pl.col("predicted_rank").rank("ordinal").over("race_id")
        )
        .filter(pl.col("_slot") <= 3)
        .with_columns(
            _contrib=_relevance_expr(pl.col("finish_position"))
            * _discount_expr(pl.col("_slot"))
        )
        .group_by("race_id")
        .agg(pl.col("_contrib").sum().alias("dcg"))
    )
    ideal = (
        valid_df.drop_nulls(subset=["finish_position"])
        .with_columns(_rel=_relevance_expr(pl.col("finish_position")))
        .with_columns(
            _slot=pl.col("_rel").rank("ordinal", descending=True).over("race_id")
        )
        .filter(pl.col("_slot") <= 3)
        .with_columns(_contrib=pl.col("_rel") * _discount_expr(pl.col("_slot")))
        .group_by("race_id")
        .agg(pl.col("_contrib").sum().alias("ideal_dcg"))
    )

    race_cell_cols = ["race_id", "source", "keibajo_code", "track_code", "kyori"]
    race_cell_cols.extend(
        col for col in ("grade_code", "kaisai_nengappi", "race_date") if col in valid_df.columns
    )
    race_cell_context = valid_df.select(race_cell_cols).unique(subset=["race_id"])
    race_cells = race_cell_context.with_columns(
        _cell_key=assign_subgroup_keys(race_cell_context)
    ).select(["race_id", "_cell_key"])

    per_race = (
        ideal.join(dcg, on="race_id", how="left")
        .filter(pl.col("ideal_dcg") > 0.0)
        .with_columns(_ndcg=pl.col("dcg").fill_null(0.0) / pl.col("ideal_dcg"))
        .join(race_cells, on="race_id", how="left")
    )
    if per_race.is_empty():
        return 0.0

    keys = per_race["_cell_key"].to_list()
    ndcg_values = per_race["_ndcg"].to_list()
    weights = [cell_weights.get(str(k), 1.0) for k in keys]
    total_weight = sum(weights)
    if total_weight <= 0.0:
        return _ndcg_at_3_from_valid_df(valid_df)
    weighted_sum = sum(n * w for n, w in zip(ndcg_values, weights, strict=False))
    return weighted_sum / total_weight


# DCG@3 position discounts 1/log2(rank+1) for ranks 1, 2, 3 — constant per race,
# so precompute once instead of recomputing log2 per element on every race.
_DISCOUNT_AT_3: Final[tuple[float, float, float]] = (
    1.0 / math.log2(2),
    1.0 / math.log2(3),
    1.0 / math.log2(4),
)


_DtypeSignature = tuple[tuple[str, str], ...]

# Per-dataframe memo of the model-safe column set. ``build_objective`` pre-splits
# folds once and reuses the SAME train/valid frames across every trial of a round,
# so the (id(df), dtypes) key hits on every trial after the first, turning a hot
# per-(trial x fold x column) numeric-dtype scan into a single classification.
_MODEL_SAFE_CACHE: dict[int, tuple[_DtypeSignature, frozenset[str]]] = {}
_XGB_NUMERIC_CACHE: dict[int, tuple[_DtypeSignature, frozenset[str]]] = {}


def _dtype_signature(df: pl.DataFrame) -> _DtypeSignature:
    return tuple(zip(df.columns, (str(dt) for dt in df.dtypes), strict=True))


def _model_safe_columns(df: pl.DataFrame) -> frozenset[str]:
    """Cached frozenset of columns that are model-safe for the given dataframe.

    A column is model-safe iff it is an allowed categorical (kept regardless of
    dtype) or has a numeric dtype — identical to the inline check it replaces. The
    cache is keyed by ``id(df)`` plus the dtypes signature so a reused id whose
    schema changed (after GC) is treated as a miss and reclassified.
    """
    signature = _dtype_signature(df)
    cached = _MODEL_SAFE_CACHE.get(id(df))
    if cached is not None and cached[0] == signature:
        return cached[1]
    schema = df.schema
    safe = frozenset(
        col
        for col in df.columns
        if col in _ALLOWED_CATEGORICAL or schema[col].is_numeric()
    )
    _MODEL_SAFE_CACHE[id(df)] = (signature, safe)
    return safe


def _numeric_columns(df: pl.DataFrame) -> frozenset[str]:
    """Cached frozenset of numeric, non-meta/label columns for the given dataframe.

    Identical classification to the inline scan it replaces (a column qualifies
    iff it is not a meta/label column and has a numeric dtype); only the order is
    dropped here — callers re-impose ``feature_names`` order by filtering against
    this set. Keyed by ``id(df)`` plus dtypes signature like the model-safe cache.
    """
    signature = _dtype_signature(df)
    cached = _XGB_NUMERIC_CACHE.get(id(df))
    if cached is not None and cached[0] == signature:
        return cached[1]
    schema = df.schema
    numeric = frozenset(
        col
        for col in df.columns
        if col not in _EXCLUDED_COLS and schema[col].is_numeric()
    )
    _XGB_NUMERIC_CACHE[id(df)] = (signature, numeric)
    return numeric


def _is_model_safe_feature(df: pl.DataFrame, col: str) -> bool:
    return col in _model_safe_columns(df)


class ExplorationResult(TypedDict):
    trial_id: str
    ndcg_at_3: float
    feature_names: list[str]
    promoted: bool


# (trial_id, ndcg_at_3, feature_names_json, definition_json) — exactly the columns
# bulk_record_trials writes, so non-promoting trials defer to one flush per round.
DeferredTrial = tuple[str, float, str, str]


class FeatureObjective:
    """Callable Optuna objective that also accumulates non-promoting trial rows.

    Wrapping the closure in a callable object (instead of attaching an attribute to a
    plain function) lets ``run_exploration`` read ``deferred_trials`` after the study
    finishes while keeping the value a callable the existing tests and ``study.optimize``
    can invoke directly, and without tripping the type checkers on dynamic attributes.
    """

    def __init__(
        self,
        fn: Callable[[optuna.Trial], float],
        deferred_trials: list[DeferredTrial],
    ) -> None:
        self._fn: Callable[[optuna.Trial], float] = fn
        self.deferred_trials: list[DeferredTrial] = deferred_trials

    def __call__(self, trial: optuna.Trial) -> float:
        return self._fn(trial)


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


def _relevance_expr(fp: pl.Expr) -> pl.Expr:
    """Map a finish-position column to DCG relevance (1->3, 2->2, 3->1, else 0)."""
    return (
        pl.when(fp == 1)
        .then(_RELEVANCE_MAP[1])
        .when(fp == 2)
        .then(_RELEVANCE_MAP[2])
        .when(fp == 3)
        .then(_RELEVANCE_MAP[3])
        .otherwise(0.0)
    )


def _discount_expr(position: pl.Expr) -> pl.Expr:
    """Map a 1-based slot position to its DCG@3 discount (positions > 3 -> 0)."""
    return (
        pl.when(position == 1)
        .then(_DISCOUNT_AT_3[0])
        .when(position == 2)
        .then(_DISCOUNT_AT_3[1])
        .when(position == 3)
        .then(_DISCOUNT_AT_3[2])
        .otherwise(0.0)
    )


def _ndcg_at_3_from_valid_df(valid_df: pl.DataFrame) -> float:
    if valid_df.is_empty():
        return 0.0
    # DCG: drop rows missing either field, rank predictions within each race (ordinal
    # rank reproduces the stable predicted_rank sort), take the top 3 slots, weight the
    # finish-position relevance by that slot's discount, and sum per race.
    dcg = (
        valid_df.drop_nulls(subset=["predicted_rank", "finish_position"])
        .with_columns(
            _slot=pl.col("predicted_rank").rank("ordinal").over("race_id")
        )
        .filter(pl.col("_slot") <= 3)
        .with_columns(
            _contrib=_relevance_expr(pl.col("finish_position"))
            * _discount_expr(pl.col("_slot"))
        )
        .group_by("race_id")
        .agg(pl.col("_contrib").sum().alias("dcg"))
    )
    # Ideal DCG: over every scored finisher in the race (predicted_rank may be null),
    # rank relevances descending, take the top 3, weight by the same slot discounts.
    ideal = (
        valid_df.drop_nulls(subset=["finish_position"])
        .with_columns(_rel=_relevance_expr(pl.col("finish_position")))
        .with_columns(
            _slot=pl.col("_rel").rank("ordinal", descending=True).over("race_id")
        )
        .filter(pl.col("_slot") <= 3)
        .with_columns(_contrib=pl.col("_rel") * _discount_expr(pl.col("_slot")))
        .group_by("race_id")
        .agg(pl.col("_contrib").sum().alias("ideal_dcg"))
    )
    per_race = (
        ideal.join(dcg, on="race_id", how="left")
        .filter(pl.col("ideal_dcg") > 0.0)
        .with_columns(
            _ndcg=pl.col("dcg").fill_null(0.0) / pl.col("ideal_dcg")
        )
    )
    if per_race.is_empty():
        return 0.0
    return cast(float, per_race["_ndcg"].mean())


def _xgb_numeric_features(df: pl.DataFrame, feature_names: list[str]) -> list[str]:
    numeric = _numeric_columns(df)
    return [c for c in feature_names if c in numeric]


def _run_fold_lightgbm(fold: FoldSplit, params: TrainingParams) -> float:
    _, predictions, _ = run_walk_forward_fold(fold, params)
    valid_with_pos = fold["valid_df"].select(
        ["race_id", "ketto_toroku_bango", "finish_position"]
    ).join(
        predictions,
        on=["race_id", "ketto_toroku_bango"],
        how="left",
    )
    return _ndcg_at_3_from_valid_df(valid_with_pos)


def _run_fold_xgboost(
    fold: FoldSplit,
    xgb_args: argparse.Namespace | None = None,
) -> float | None:
    feature_cols = _xgb_numeric_features(fold["train_df"], list(fold["train_df"].columns))
    if not feature_cols:
        return None
    _, result = train_xgboost_ranker(
        fold["train_df"], fold["valid_df"], feature_cols, xgb_args if xgb_args is not None else _XGB_ARGS,
    )
    valid_df = cast(pl.DataFrame, result["valid_predictions"])
    return _ndcg_at_3_from_valid_df(valid_df)


def _run_fold_catboost(
    fold: FoldSplit,
    cb_args: argparse.Namespace | None = None,
) -> float | None:
    feature_cols = [
        c
        for c in fold["train_df"].columns
        if c not in _EXCLUDED_COLS and _is_model_safe_feature(fold["train_df"], c)
    ]
    if not feature_cols:
        return None
    result = train_catboost_ranker(
        fold["train_df"], fold["valid_df"], feature_cols, cb_args if cb_args is not None else _CB_ARGS,
    )
    valid_df = cast(pl.DataFrame, result["valid_predictions"])
    return _ndcg_at_3_from_valid_df(valid_df)


def run_fold_with_backend(
    fold: FoldSplit,
    backend: ModelBackend,
    lgb_params: TrainingParams,
    xgb_args: argparse.Namespace | None = None,
    cb_args: argparse.Namespace | None = None,
) -> float | None:
    if backend == "lightgbm":
        return _run_fold_lightgbm(fold, lgb_params)
    if backend == "xgboost":
        return _run_fold_xgboost(fold, xgb_args)
    return _run_fold_catboost(fold, cb_args)


def _predict_fold_lightgbm(fold: FoldSplit, params: TrainingParams) -> pl.DataFrame:
    _, predictions, _ = run_walk_forward_fold(fold, params)
    return fold["valid_df"].select(
        ["race_id", "ketto_toroku_bango", "finish_position"]
    ).join(
        predictions,
        on=["race_id", "ketto_toroku_bango"],
        how="left",
    )


def _predict_fold_xgboost(fold: FoldSplit) -> pl.DataFrame | None:
    feature_cols = _xgb_numeric_features(fold["train_df"], list(fold["train_df"].columns))
    if not feature_cols:
        return None
    _, result = train_xgboost_ranker(
        fold["train_df"], fold["valid_df"], feature_cols, _XGB_ARGS,
    )
    return cast(pl.DataFrame, result["valid_predictions"])


def _predict_fold_catboost(fold: FoldSplit) -> pl.DataFrame | None:
    feature_cols = [
        c
        for c in fold["train_df"].columns
        if c not in _EXCLUDED_COLS and _is_model_safe_feature(fold["train_df"], c)
    ]
    if not feature_cols:
        return None
    result = train_catboost_ranker(
        fold["train_df"], fold["valid_df"], feature_cols, _CB_ARGS,
    )
    return cast(pl.DataFrame, result["valid_predictions"])


def predict_fold_with_backend(
    fold: FoldSplit,
    backend: ModelBackend,
    lgb_params: TrainingParams,
) -> pl.DataFrame | None:
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
    df: pl.DataFrame,
    feature_mask: dict[str, bool],
) -> pl.DataFrame:
    selected = [col for col, keep in feature_mask.items() if keep]
    keep_cols = [c for c in df.columns if c in _META_AND_LABEL or c in selected]
    return df.select(keep_cols)


def _select_fold_features(fold: FoldSplit, feature_set: set[str]) -> FoldSplit:
    """Select only the needed features from a pre-split fold."""
    safe = _model_safe_columns(fold["train_df"])
    keep_cols = [
        c
        for c in fold["train_df"].columns
        if c in _META_AND_LABEL or (c in feature_set and c in safe)
    ]
    return {
        "train_df": fold["train_df"].select(keep_cols),
        "valid_df": fold["valid_df"].select(keep_cols),
        "valid_year": fold["valid_year"],
    }


def select_fold_features(fold: FoldSplit, feature_set: set[str]) -> FoldSplit:
    """Public alias for :func:`_select_fold_features` used by external callers."""
    return _select_fold_features(fold, feature_set)


@final
class _FoldPlan:
    """Per-fold static selection plan, computed once and reused across every trial.

    ``build_objective`` evaluates thousands of feature masks against the SAME
    pre-split folds. The column order and the model-safe classification of those
    columns never change within a round, so they are resolved once here. Each trial
    then only intersects its ``feature_set`` against ``selectable`` — a cheap set
    membership pass — instead of re-deriving ``_model_safe_columns`` (and its
    dtype-signature scan) on every fold of every trial.
    """

    __slots__ = ("fold", "columns", "always_keep", "selectable")

    def __init__(self, fold: FoldSplit) -> None:
        safe = _model_safe_columns(fold["train_df"])
        columns = list(fold["train_df"].columns)
        self.fold: FoldSplit = fold
        self.columns: list[str] = columns
        # Meta/label columns are kept regardless of the mask; ``selectable`` is the
        # subset of feature columns a trial may opt into (model-safe and not meta).
        self.always_keep: frozenset[str] = frozenset(
            c for c in columns if c in _META_AND_LABEL
        )
        self.selectable: frozenset[str] = frozenset(
            c for c in columns if c not in _META_AND_LABEL and c in safe
        )

    def select(self, feature_set: set[str]) -> FoldSplit:
        chosen = self.selectable & feature_set
        keep_cols = [c for c in self.columns if c in self.always_keep or c in chosen]
        return {
            "train_df": self.fold["train_df"].select(keep_cols),
            "valid_df": self.fold["valid_df"].select(keep_cols),
            "valid_year": self.fold["valid_year"],
        }


def evaluate_feature_set(
    df: pl.DataFrame,
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
        if fold["train_df"].is_empty() or fold["valid_df"].is_empty():
            continue
        for backend in backends:
            score = run_fold_with_backend(fold, backend, params)
            if score is not None:
                ndcg_scores.append(score)
    if not ndcg_scores:
        return 0.0
    return sum(ndcg_scores) / len(ndcg_scores)


def run_forward_selection(
    df: pl.DataFrame,
    candidate_features: list[str],
    validation_years: list[int],
    train_start: str,
    params: TrainingParams,
    backends: tuple[ModelBackend, ...],
    max_features: int = DEFAULT_STEPWISE_MAX_FEATURES,
    importance_scores: dict[str, float] | None = None,
) -> list[str]:
    """Greedily grow a feature set, keeping each candidate only if it lifts NDCG@3.

    Candidates are visited in descending importance order when ``importance_scores``
    is given, so the strongest columns are admitted first; otherwise the input order
    is used. The set stops growing at ``max_features`` columns.
    """
    ordered = (
        sorted(candidate_features, key=lambda c: importance_scores.get(c, 0.0), reverse=True)
        if importance_scores is not None
        else list(candidate_features)
    )
    selected: list[str] = []
    best_ndcg = 0.0
    for candidate in ordered:
        if len(selected) >= max_features:
            break
        trial_set = [*selected, candidate]
        ndcg = evaluate_feature_set(
            df, trial_set, validation_years, train_start, params, backends
        )
        if ndcg > best_ndcg:
            selected = trial_set
            best_ndcg = ndcg
    return selected


def run_backward_elimination(
    df: pl.DataFrame,
    feature_names: list[str],
    validation_years: list[int],
    train_start: str,
    params: TrainingParams,
    backends: tuple[ModelBackend, ...],
    min_features: int = DEFAULT_BACKWARD_MIN_FEATURES,
    importance_scores: dict[str, float] | None = None,
) -> list[str]:
    """Drop features one at a time while NDCG@3 holds, pruning redundant columns.

    Features are tried lowest-importance first when ``importance_scores`` is given so
    the weakest are removed before the rest, and a removal is kept whenever the score
    does not fall. Elimination stops once ``min_features`` columns remain.
    """
    selected = list(feature_names)
    best_ndcg = evaluate_feature_set(
        df, selected, validation_years, train_start, params, backends
    )
    order = (
        sorted(feature_names, key=lambda c: importance_scores.get(c, 0.0))
        if importance_scores is not None
        else list(feature_names)
    )
    for feature in order:
        if len(selected) <= min_features:
            break
        trial_set = [c for c in selected if c != feature]
        ndcg = evaluate_feature_set(
            df, trial_set, validation_years, train_start, params, backends
        )
        if ndcg >= best_ndcg:
            selected = trial_set
            best_ndcg = ndcg
    return selected


def _feature_set_hash(feature_names: list[str]) -> str:
    """Order-independent content hash of a feature set, used as a dedup cache key.

    Sorting before serialising makes the hash invariant to selection order, so two
    trials that pick the same columns in a different sequence collide and share one
    cached NDCG instead of retraining.
    """
    return compute_normalized_feature_set_hash(feature_names)


class TrialDeduplicator(Protocol):
    """Cache of already-evaluated feature sets, keyed by (hash, method).

    A store implementing this lets the objective skip retraining a feature set it
    has already scored under the same exploration method — the dominant cost in a
    feature search is the GBDT fit, so a hit returns in microseconds.
    """

    def get_cached_ndcg(self, feature_set_hash: str, method: str) -> float | None: ...
    def record_trial(
        self, feature_set_hash: str, method: str, ndcg: float, feature_names: list[str]
    ) -> None: ...


def importance_to_vector(
    importance: dict[str, float],
    all_features: list[str],
) -> list[float]:
    """Convert sparse importance dict to dense vector aligned with all_features order."""
    return [importance.get(f, 0.0) for f in sorted(all_features)]


def feature_mask_to_vector(
    selected: list[str],
    all_features: list[str],
) -> list[bool]:
    """Convert feature selection to boolean vector aligned with all_features order."""
    selected_set = set(selected)
    return [f in selected_set for f in sorted(all_features)]


def build_objective(
    df: pl.DataFrame,
    candidate_features: list[str],
    validation_years: list[int],
    train_start: str,
    params: TrainingParams,
    registry: FeatureRegistry,
    study_name: str,
    backends: tuple[ModelBackend, ...] = DEFAULT_BACKENDS,
    xgb_args: argparse.Namespace | None = None,
    cb_args: argparse.Namespace | None = None,
    co_optimize_hp: bool = False,
    trial_dedup: TrialDeduplicator | None = None,
) -> FeatureObjective:
    fold_plans: list[_FoldPlan] = []
    for year in validation_years:
        fold = split_walk_forward(df, train_start, year)
        if not fold["train_df"].is_empty() and not fold["valid_df"].is_empty():
            fold_plans.append(_FoldPlan(fold))

    deferred_trials: list[DeferredTrial] = []

    # The active entry only changes when a trial in *this* study promotes (via
    # maybe_promote below), so its NDCG is read from the registry once here and
    # tracked locally — saving one get_active_entry() SELECT on every trial. The
    # buffered (non-promoting) and warm-start/enqueued trials never activate, so
    # nothing else can move the active row mid-round.
    active_entry = registry.get_active_entry()
    active_state = {
        "ndcg": active_entry["ndcg_at_3"] if active_entry is not None else 0.0
    }

    def objective(trial: optuna.Trial) -> float:
        grouped = group_features(candidate_features)
        feature_mask: dict[str, bool] = {}
        for group_name, group_cols in sorted(grouped.items()):
            group_active = trial.suggest_categorical(f"group_{group_name}", [True, False])
            if group_active:
                for col in group_cols:
                    feature_mask[col] = trial.suggest_categorical(f"use_{col}", [True, False])
            else:
                for col in group_cols:
                    feature_mask[col] = False
        selected = [col for col, keep in feature_mask.items() if keep]
        if len(selected) < MIN_FEATURES:
            return 0.0
        feature_set_hash: str | None = None
        if trial_dedup is not None:
            feature_set_hash = _feature_set_hash(selected)
            cached = trial_dedup.get_cached_ndcg(feature_set_hash, BLOCK_METHOD)
            if cached is not None:
                trial.set_user_attr("promoted", False)
                return cached
        feature_set = set(selected)
        if co_optimize_hp:
            hp_learning_rate = trial.suggest_float("hp_learning_rate", 0.01, 0.1, log=True)
            hp_depth = trial.suggest_int("hp_depth", 4, 8)
            hp_l2 = trial.suggest_float("hp_l2_reg", 0.1, 10.0, log=True)
            hp_iterations = trial.suggest_categorical("hp_iterations", [150, 300, 500])
            trial_xgb_args = argparse.Namespace(
                learning_rate=hp_learning_rate,
                max_depth=hp_depth,
                min_child_weight=1,
                reg_lambda=hp_l2,
                seed=42,
                num_rounds=hp_iterations,
                early_stopping_rounds=50,
                relevance_rank1=3,
                relevance_rank2=2,
                relevance_rank3=1,
                presorted=True,
            )
            trial_cb_args = argparse.Namespace(
                learning_rate=hp_learning_rate,
                depth=hp_depth,
                l2_leaf_reg=hp_l2,
                seed=42,
                iterations=hp_iterations,
                early_stopping_rounds=50,
                relevance_rank1=3,
                relevance_rank2=2,
                relevance_rank3=1,
                no_cat_features=False,
                presorted=True,
            )
        else:
            trial_xgb_args = xgb_args
            trial_cb_args = cb_args
        ndcg_scores: list[float] = []
        for step, plan in enumerate(fold_plans):
            fold_with_features = plan.select(feature_set)
            fold_scores: list[float] = []
            for backend in backends:
                score = run_fold_with_backend(fold_with_features, backend, params, trial_xgb_args, trial_cb_args)
                if score is not None:
                    fold_scores.append(score)
            if fold_scores:
                ndcg_scores.extend(fold_scores)
                intermediate = sum(ndcg_scores) / len(ndcg_scores)
                trial.report(intermediate, step)
                if trial.should_prune():
                    raise optuna.TrialPruned()
            # Free this fold's column subset before the next iteration allocates
            # its own, so the final fold's frame isn't pinned through the registry
            # maybe_promote call below.
            del fold_with_features
        ndcg = sum(ndcg_scores) / len(ndcg_scores) if ndcg_scores else 0.0
        if trial_dedup is not None and feature_set_hash is not None:
            trial_dedup.record_trial(feature_set_hash, BLOCK_METHOD, ndcg, selected)
        trial_id = f"{study_name}_trial_{trial.number}"
        active_ndcg = active_state["ndcg"]
        delta_pp = (ndcg - active_ndcg) * 100
        definition = json.dumps(
            {"features": selected, "trial": trial.number, "delta_pp": delta_pp}
        )
        # Mirror maybe_promote's internal decision (strict ``>`` against the same
        # threshold constant) so promote-vs-defer routing is byte-identical to the
        # old per-trial path: promoting rows insert+activate immediately, while
        # non-promoting rows are buffered and flushed in one bulk write afterward.
        if ndcg > active_ndcg + NDCG_IMPROVEMENT_THRESHOLD:
            promoted = registry.maybe_promote(
                trial_id, ndcg, selected, definition, active_ndcg=active_ndcg
            )
            if promoted:
                active_state["ndcg"] = ndcg
            trial.set_user_attr("promoted", promoted)
        else:
            deferred_trials.append((trial_id, ndcg, json.dumps(selected), definition))
            trial.set_user_attr("promoted", False)
        return ndcg

    return FeatureObjective(objective, deferred_trials)


def build_feature_sampler(n_trials: int, seed: int = SAMPLER_SEED) -> BaseSampler:
    """Return a TPE sampler tuned for binary feature-selection search.

    ``multivariate`` lets TPE model joint feature interactions rather than treating
    each ``use_<feature>`` flag independently, which matters because feature value
    comes from combinations, not isolated columns. ``constant_liar`` is disabled
    because ``run_exploration`` runs with ``n_jobs=1`` (no concurrent trials), so
    there is never a pending trial to penalise; leaving it off keeps suggestions
    driven purely by completed observations.

    ``BruteForceSampler`` is infeasible here (2^n candidate space) and ``CmaEsSampler``
    only handles continuous spaces, so neither fits the boolean mask search.

    The random startup count is capped just below ``n_trials`` so a small per-round
    budget never collapses into pure random sampling before TPE engages.
    """
    n_startup = max(1, min(TPE_N_STARTUP_TRIALS, n_trials - 1))
    return TPESampler(
        seed=seed,
        n_startup_trials=n_startup,
        multivariate=True,
        constant_liar=False,
    )


def _mask_to_params(
    candidate_features: Sequence[str], selected: set[str]
) -> tuple[dict[str, bool], dict[str, BaseDistribution]]:
    """Build Optuna params/distributions for a known feature mask over the candidates."""
    distribution = CategoricalDistribution([True, False])
    params: dict[str, bool] = {}
    distributions: dict[str, BaseDistribution] = {}
    grouped = group_features(list(candidate_features))
    for group_name, group_cols in sorted(grouped.items()):
        group_key = f"group_{group_name}"
        group_has_selected = any(col in selected for col in group_cols)
        params[group_key] = group_has_selected
        distributions[group_key] = distribution
        if group_has_selected:
            for col in group_cols:
                key = f"use_{col}"
                params[key] = col in selected
                distributions[key] = distribution
    return params, distributions


def build_warm_start_trials(
    registry: FeatureRegistry,
    candidate_features: Sequence[str],
    top_k: int = DEFAULT_WARM_START_TOP_K,
) -> list[FrozenTrial]:
    """Reconstruct the registry's best prior trials as Optuna trials over current candidates.

    Each prior trial's stored feature set is re-expressed as a boolean mask over the
    *current* candidate columns and paired with its recorded NDCG, so a resumed study
    can seed TPE with proven points instead of relearning them. Features that no longer
    exist as candidates are dropped from the mask (they cannot be suggested anyway), and
    prior trials whose surviving mask falls below ``MIN_FEATURES`` are skipped because the
    objective would have scored them 0.0 — feeding those back would bias the prior.
    """
    candidate_set = set(candidate_features)
    warm: list[FrozenTrial] = []
    for entry in registry.list_trials(limit=top_k):
        surviving = set(entry["feature_names"]) & candidate_set
        if len(surviving) < MIN_FEATURES:
            continue
        params, distributions = _mask_to_params(candidate_features, surviving)
        warm.append(
            create_trial(
                state=TrialState.COMPLETE,
                value=entry["ndcg_at_3"],
                params=params,
                distributions=distributions,
            )
        )
    return warm


def enqueue_feature_subsets(
    study: optuna.Study,
    candidate_features: Sequence[str],
    subsets: Sequence[set[str]],
) -> None:
    """Queue concrete feature subsets so the next trials evaluate them verbatim.

    Used to force-evaluate the active feature set and enrichment candidates up front,
    guaranteeing a resumed round spends budget on the most promising masks first rather
    than waiting for the sampler to rediscover them. Subsets below ``MIN_FEATURES`` are
    dropped since the objective would short-circuit them to 0.0.
    """
    candidate_set = set(candidate_features)
    for subset in subsets:
        surviving = subset & candidate_set
        if len(surviving) < MIN_FEATURES:
            continue
        params, _ = _mask_to_params(candidate_features, surviving)
        study.enqueue_trial(params, skip_if_exists=True)


def make_per_trial_timeout_callback(
    per_trial_timeout_s: float,
) -> Callable[[optuna.Study, FrozenTrial], None]:
    """Return a callback that stops the study once a trial exceeds the wall-clock budget.

    Optuna cannot interrupt a trial mid-``run_fold_with_backend`` (the GBDT train call is
    not cooperative), so this is a *soft* guard: it lets the offending trial finish, then
    calls ``study.stop()`` so no further long trials are launched. This bounds total
    wasted time to roughly one over-budget trial instead of an unbounded tail.
    """

    def _callback(study: optuna.Study, trial: FrozenTrial) -> None:
        if trial.datetime_start is None or trial.datetime_complete is None:
            return
        elapsed = (trial.datetime_complete - trial.datetime_start).total_seconds()
        if elapsed > per_trial_timeout_s:
            study.stop()

    return _callback


def run_exploration(
    df: pl.DataFrame,
    registry: FeatureRegistry,
    n_trials: int = 50,
    validation_years: list[int] | None = None,
    train_start: str = DEFAULT_TRAIN_START,
    params: TrainingParams = DEFAULT_PARAMS,
    study_name: str = "feature_exploration",
    storage: str | None = None,
    backends: tuple[ModelBackend, ...] = DEFAULT_BACKENDS,
    n_jobs: int = 1,
    study_timeout_s: float | None = None,
    per_trial_timeout_s: float | None = None,
    warm_start: bool = True,
    enqueue_subsets: Sequence[set[str]] | None = None,
    co_optimize_hp: bool = False,
    screening: bool = False,
    trial_dedup: TrialDeduplicator | None = None,
) -> list[ExplorationResult]:
    """Run one Optuna feature-selection study and return its scored trials.

    ``warm_start`` seeds the sampler with the registry's best prior masks so a resumed
    round starts from proven regions. ``enqueue_subsets`` force-evaluates specific masks
    (e.g. the active set + enrichment candidates) before the sampler takes over.
    ``per_trial_timeout_s`` bounds the long-trial tail and ``study_timeout_s`` bounds the
    whole round. ``n_jobs`` defaults to 1 because the shared DuckDB registry write in the
    objective is not safe under concurrent threads; raise it only with a thread-safe
    registry.
    """
    effective_years = list(validation_years) if validation_years is not None else list(DEFAULT_VALIDATION_YEARS)
    candidate_features = resolve_feature_columns(list(df.columns))
    screen_xgb = _SCREEN_XGB_ARGS if screening else None
    screen_cb = _SCREEN_CB_ARGS if screening else None
    objective = build_objective(
        df,
        candidate_features,
        effective_years,
        train_start,
        params,
        registry,
        study_name,
        backends,
        xgb_args=screen_xgb,
        cb_args=screen_cb,
        co_optimize_hp=co_optimize_hp,
        trial_dedup=trial_dedup,
    )
    study = optuna.create_study(
        direction="maximize",
        study_name=study_name,
        storage=storage,
        load_if_exists=True,
        sampler=build_feature_sampler(n_trials),
        pruner=optuna.pruners.MedianPruner(n_startup_trials=3, n_warmup_steps=0),
    )
    if warm_start:
        for warm_trial in build_warm_start_trials(registry, candidate_features):
            study.add_trial(warm_trial)
    if enqueue_subsets:
        enqueue_feature_subsets(study, candidate_features, enqueue_subsets)
    callbacks: list[Callable[[optuna.Study, FrozenTrial], None]] = (
        [make_per_trial_timeout_callback(per_trial_timeout_s)]
        if per_trial_timeout_s is not None
        else []
    )
    study.optimize(
        objective,
        n_trials=n_trials,
        timeout=study_timeout_s,
        n_jobs=n_jobs,
        callbacks=callbacks,
        show_progress_bar=False,
    )
    # Flush every non-promoting trial buffered during the study in one bulk write,
    # so by the time this returns each scored trial exists as a feature_trials row
    # (the contract continuous_learner depends on when it reads the registry back).
    registry.bulk_record_trials(objective.deferred_trials)
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


def run_combined_exploration(
    df: pl.DataFrame,
    registry: FeatureRegistry,
    n_trials: int,
    validation_years: list[int],
    train_start: str,
    params: TrainingParams,
    study_name: str,
    backends: tuple[ModelBackend, ...] = DEFAULT_BACKENDS,
    stepwise_max_features: int = DEFAULT_STEPWISE_MAX_FEATURES,
    trial_store: object | None = None,
    co_optimize_hp: bool = False,
    storage: str | None = None,
    n_jobs: int = 1,
    study_timeout_s: float | None = None,
    per_trial_timeout_s: float | None = None,
    warm_start: bool = True,
    screening: bool = False,
) -> list[ExplorationResult]:
    """Stepwise warm-start followed by block TPE exploration.

    Importance is read off the registry's current best (only when one exists, since a
    cold registry has no model to rank columns by). Forward selection grows an
    importance-guided set, backward elimination prunes its redundant columns, and the
    survivors are enqueued so the TPE block search spends its budget refining that
    proven mask first. ``trial_store`` is threaded through as the dedup cache.
    """
    candidate_features = resolve_feature_columns(list(df.columns))
    active = registry.get_active_entry()
    importance_scores = (
        compute_feature_importance(
            df, candidate_features, validation_years, train_start, backends
        )
        if active is not None
        else None
    )
    forward = run_forward_selection(
        df,
        candidate_features,
        validation_years,
        train_start,
        params,
        backends,
        max_features=stepwise_max_features,
        importance_scores=importance_scores,
    )
    pruned = run_backward_elimination(
        df,
        forward,
        validation_years,
        train_start,
        params,
        backends,
        importance_scores=importance_scores,
    )
    enqueue = [set(pruned)] if pruned else None
    return run_exploration(
        df,
        registry,
        n_trials=n_trials,
        validation_years=validation_years,
        train_start=train_start,
        params=params,
        study_name=study_name,
        storage=storage,
        backends=backends,
        n_jobs=n_jobs,
        study_timeout_s=study_timeout_s,
        per_trial_timeout_s=per_trial_timeout_s,
        warm_start=warm_start,
        enqueue_subsets=enqueue,
        co_optimize_hp=co_optimize_hp,
        screening=screening,
        trial_dedup=cast("TrialDeduplicator | None", trial_store),
    )
