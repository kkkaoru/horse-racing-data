#!/usr/bin/env python3
# pyright: reportUntypedBaseClass=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false, reportUnannotatedClassAttribute=false, reportMissingParameterType=false
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import importlib
import json
import os
from pathlib import Path
from time import perf_counter
from typing import NotRequired, TypedDict, cast

import lightgbm as lgb
from lightgbm.basic import LightGBMError
import numpy as np
import polars as pl
from numpy.typing import NDArray
from sklearn.neighbors import NearestNeighbors


TARGET_COLUMNS = ["corner1_norm", "corner2_norm", "corner3_norm", "corner4_norm"]
FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]
BoolArray = NDArray[np.bool_]


class LightGBMDeviceParams(TypedDict):
    device_type: NotRequired[str]


CODE_FEATURE_COLUMNS = {
    "grade_code": "grade_code_num",
    "kyoso_shubetsu_code": "race_age_code_num",
    "juryo_shubetsu_code": "weight_type_code_num",
    "kyoso_joken_code": "race_condition_code_num",
    "babajotai_code_shiba": "going_shiba_code_num",
    "babajotai_code_dirt": "going_dirt_code_num",
    "seibetsu_code": "sex_code_num",
}
RAW_NUMERIC_DEFAULTS = {
    "barei": 0,
    "futan_juryo": 0,
    "finish_norm": 1,
}
ODDS_HISTORY_COLUMNS = [
    "horse_odds_avg",
    "horse_odds_recent_avg",
    "horse_odds_last",
]
THREE_F_HISTORY_COLUMNS = [
    "horse_kohan_3f_avg",
    "horse_kohan_3f_recent_avg",
    "horse_kohan_3f_last",
]
STYLE_FEATURE_COLUMNS = [
    "horse_early_avg",
    "horse_early_recent",
    "horse_early_last",
    "horse_late_avg",
    "horse_late_recent",
    "horse_late_last",
    "horse_pace_fade_avg",
    "horse_pace_fade_recent",
    "horse_pace_fade_last",
    "front_runner_score",
    "stalker_score",
    "closer_score",
    "pace_consistency",
    "inner_front_bias",
    "outer_closer_bias",
    "early_history_blend",
    "late_history_blend",
]
VECTOR_NEIGHBOR_FEATURE_COLUMNS = [
    f"vector_neighbor{k}_{name}"
    for k in [10, 30, 50]
    for name in [
        "count",
        "similarity_avg",
        "corner1_avg",
        "corner2_avg",
        "corner3_avg",
        "corner4_avg",
    ]
]
VECTOR_NEIGHBOR_KS = [10, 30, 50]
VECTOR_NEIGHBOR_MAX_CANDIDATES = 1800
VECTOR_NEIGHBOR_MAX_DAYS = 366 * 3
VECTOR_NEIGHBOR_DISTANCE_BAND = 400
VECTOR_NEIGHBOR_LOG_ODDS_DENOMINATOR = float(np.log(300))
VECTOR_NEIGHBOR_MLX_BATCH_SIZE = 256
VECTOR_NEIGHBOR_QUERY_BATCH_SIZE = 4096
VECTOR_NEIGHBOR_QUERY_CANDIDATES = 500
VECTOR_NEIGHBOR_CACHE_VERSION = "v2"
FEATURE_CACHE_VERSION = "v4"
CORNER_HISTORY_RANK_BASE_COLUMNS = [
    "horse_corner1_avg",
    "horse_corner2_avg",
    "horse_corner3_avg",
    "horse_corner4_avg",
    "horse_corner1_recent_avg",
    "horse_corner2_recent_avg",
    "horse_corner3_recent_avg",
    "horse_corner4_recent_avg",
    "horse_corner1_last",
    "horse_corner2_last",
    "horse_corner3_last",
    "horse_corner4_last",
]
CORNER_HISTORY_RANK_FEATURE_COLUMNS = [f"{column}_race_rank" for column in CORNER_HISTORY_RANK_BASE_COLUMNS]
RACE_RELATIVE_BASE_COLUMNS = [
    "horse_number_norm",
    "popularity_norm",
    "log_tansho_odds",
    "horse_early_avg",
    "horse_early_recent",
    "horse_early_last",
    "horse_late_avg",
    "horse_late_recent",
    "horse_late_last",
    "horse_pace_fade_avg",
    "horse_pace_fade_recent",
    "horse_pace_fade_last",
    "horse_finish_norm_avg",
    "horse_finish_norm_recent_avg",
    "horse_finish_norm_last",
    "horse_popularity_norm_avg",
    "horse_popularity_norm_recent_avg",
    "horse_popularity_norm_last",
    "horse_log_odds_avg",
    "horse_log_odds_recent_avg",
    "horse_log_odds_last",
    "horse_time_sa_avg",
    "horse_time_sa_recent_avg",
    "horse_time_sa_last",
    "horse_kohan_3f_norm_avg",
    "horse_kohan_3f_norm_recent_avg",
    "horse_kohan_3f_norm_last",
    *VECTOR_NEIGHBOR_FEATURE_COLUMNS,
    "front_runner_score",
    "stalker_score",
    "closer_score",
]
RACE_RELATIVE_FEATURE_COLUMNS = [
    f"{column}_{suffix}"
    for column in RACE_RELATIVE_BASE_COLUMNS
    for suffix in ["race_rank", "race_centered", "race_z", "front_gap", "back_gap"]
]
FEATURE_COLUMNS = [
    "keibajo_code_num",
    "race_bango_num",
    "track_family",
    "track_code_num",
    "grade_code_num",
    "race_age_code_num",
    "weight_type_code_num",
    "race_condition_code_num",
    "going_shiba_code_num",
    "going_dirt_code_num",
    "kyori",
    "shusso_tosu",
    "umaban",
    "horse_number_norm",
    "sex_code_num",
    "barei",
    "futan_juryo",
    "tansho_ninkijun",
    "popularity_norm",
    "log_tansho_odds",
    "horse_corner1_avg",
    "horse_corner2_avg",
    "horse_corner3_avg",
    "horse_corner4_avg",
    "horse_corner1_recent_avg",
    "horse_corner2_recent_avg",
    "horse_corner3_recent_avg",
    "horse_corner4_recent_avg",
    "horse_corner1_last",
    "horse_corner2_last",
    "horse_corner3_last",
    "horse_corner4_last",
    "horse_start_count",
    "horse_finish_norm_avg",
    "horse_finish_norm_recent_avg",
    "horse_finish_norm_last",
    "horse_popularity_norm_avg",
    "horse_popularity_norm_recent_avg",
    "horse_popularity_norm_last",
    "horse_log_odds_avg",
    "horse_log_odds_recent_avg",
    "horse_log_odds_last",
    "horse_time_sa_avg",
    "horse_time_sa_recent_avg",
    "horse_time_sa_last",
    "horse_kohan_3f_norm_avg",
    "horse_kohan_3f_norm_recent_avg",
    "horse_kohan_3f_norm_last",
    "horse_days_since_last_start_norm",
    *STYLE_FEATURE_COLUMNS,
    *CORNER_HISTORY_RANK_FEATURE_COLUMNS,
    *RACE_RELATIVE_FEATURE_COLUMNS,
    "jockey_corner1_avg",
    "jockey_corner2_avg",
    "jockey_corner3_avg",
    "jockey_corner4_avg",
    "jockey_start_count",
    "trainer_corner1_avg",
    "trainer_corner2_avg",
    "trainer_corner3_avg",
    "trainer_corner4_avg",
    "trainer_start_count",
    "owner_corner1_avg",
    "owner_corner2_avg",
    "owner_corner3_avg",
    "owner_corner4_avg",
    "owner_start_count",
    "course_number_corner1_avg",
    "course_number_corner2_avg",
    "course_number_corner3_avg",
    "course_number_corner4_avg",
    "course_number_start_count",
    "venue_course_number_corner1_avg",
    "venue_course_number_corner2_avg",
    "venue_course_number_corner3_avg",
    "venue_course_number_corner4_avg",
    "venue_course_number_start_count",
    *VECTOR_NEIGHBOR_FEATURE_COLUMNS,
]
PAIRWISE_BASE_COLUMNS = [
    "horse_number_norm",
    "popularity_norm",
    "log_tansho_odds",
    "horse_early_avg",
    "horse_early_recent",
    "horse_early_last",
    "horse_late_avg",
    "horse_late_recent",
    "horse_late_last",
    "horse_pace_fade_avg",
    "horse_pace_fade_recent",
    "horse_pace_fade_last",
    "front_runner_score",
    "stalker_score",
    "closer_score",
    "pace_consistency",
    "horse_finish_norm_avg",
    "horse_finish_norm_recent_avg",
    "horse_finish_norm_last",
    "horse_popularity_norm_avg",
    "horse_popularity_norm_recent_avg",
    "horse_popularity_norm_last",
    "horse_log_odds_avg",
    "horse_log_odds_recent_avg",
    "horse_log_odds_last",
    "horse_time_sa_avg",
    "horse_time_sa_recent_avg",
    "horse_time_sa_last",
    "horse_kohan_3f_norm_avg",
    "horse_kohan_3f_norm_recent_avg",
    "horse_kohan_3f_norm_last",
    "horse_days_since_last_start_norm",
    "course_number_corner1_avg",
    "course_number_corner2_avg",
    "course_number_corner3_avg",
    "course_number_corner4_avg",
    "venue_course_number_corner1_avg",
    "venue_course_number_corner2_avg",
    "venue_course_number_corner3_avg",
    "venue_course_number_corner4_avg",
    *CORNER_HISTORY_RANK_FEATURE_COLUMNS,
    *VECTOR_NEIGHBOR_FEATURE_COLUMNS,
]
PAIRWISE_FEATURE_COLUMNS = [
    *[f"{column}_diff" for column in PAIRWISE_BASE_COLUMNS],
    *[f"{column}_abs_diff" for column in PAIRWISE_BASE_COLUMNS],
]
HISTORY_COLUMNS = [
    "horse_corner1_avg",
    "horse_corner2_avg",
    "horse_corner3_avg",
    "horse_corner4_avg",
    "horse_corner1_recent_avg",
    "horse_corner2_recent_avg",
    "horse_corner3_recent_avg",
    "horse_corner4_recent_avg",
    "horse_corner1_last",
    "horse_corner2_last",
    "horse_corner3_last",
    "horse_corner4_last",
    "horse_start_count",
    "horse_finish_norm_avg",
    "horse_finish_norm_recent_avg",
    "horse_finish_norm_last",
    "horse_popularity_norm_avg",
    "horse_popularity_norm_recent_avg",
    "horse_popularity_norm_last",
    "horse_time_sa_avg",
    "horse_time_sa_recent_avg",
    "horse_time_sa_last",
    "jockey_corner1_avg",
    "jockey_corner2_avg",
    "jockey_corner3_avg",
    "jockey_corner4_avg",
    "jockey_start_count",
    "trainer_corner1_avg",
    "trainer_corner2_avg",
    "trainer_corner3_avg",
    "trainer_corner4_avg",
    "trainer_start_count",
    "owner_corner1_avg",
    "owner_corner2_avg",
    "owner_corner3_avg",
    "owner_corner4_avg",
    "owner_start_count",
    "course_number_corner1_avg",
    "course_number_corner2_avg",
    "course_number_corner3_avg",
    "course_number_corner4_avg",
    "course_number_start_count",
    "venue_course_number_corner1_avg",
    "venue_course_number_corner2_avg",
    "venue_course_number_corner3_avg",
    "venue_course_number_corner4_avg",
    "venue_course_number_start_count",
    *VECTOR_NEIGHBOR_FEATURE_COLUMNS,
]
REGRESSION_RANKER_ALPHAS = [0, 0.05, 0.1, 0.15, 0.22, 0.3, 0.45, 0.6, 0.75, 0.9, 1.0]
RANK_PAIRWISE_ALPHAS = [0.1, 0.18, 0.25, 0.35, 0.5, 0.75, 1.0]
STRUCTURAL_BLEND_ALPHAS = [0.08, 0.12, 0.16, 0.2, 0.25, 0.32, 0.4, 0.5, 0.75]
NEURAL_BLEND_ALPHAS = [0.08, 0.12, 0.16, 0.2, 0.25, 0.32, 0.4]
RANK_ENSEMBLE_WEIGHTS = [0.08, 0.12, 0.16, 0.2, 0.25, 0.35, 0.5, 0.65, 0.8]
RANK_ENSEMBLE_TRIPLES = [
    (0.65, 0.25, 0.1),
    (0.6, 0.3, 0.1),
    (0.5, 0.3, 0.2),
    (0.45, 0.4, 0.15),
    (0.4, 0.35, 0.25),
]
FAST_GRID_REGRESSION_RANKER_ALPHAS = [0, 0.5, 1.0]
FAST_GRID_RANK_PAIRWISE_ALPHAS = [0.1, 1.0]
FAST_GRID_STRUCTURAL_BLEND_ALPHAS = [0.08, 0.4]
FAST_GRID_NEURAL_BLEND_ALPHAS = [0.08, 0.4]
FAST_GRID_RANK_ENSEMBLE_WEIGHTS = [0.08, 0.8]
FAST_GRID_RANK_ENSEMBLE_TRIPLES = [(0.6, 0.3, 0.1)]


def use_fast_alpha_grid() -> bool:
    return os.environ.get("PC_KEIBA_CORNER_FAST_GRID", "").strip() != ""


def regression_ranker_alphas() -> list[float]:
    return FAST_GRID_REGRESSION_RANKER_ALPHAS if use_fast_alpha_grid() else REGRESSION_RANKER_ALPHAS


def rank_pairwise_alphas() -> list[float]:
    return FAST_GRID_RANK_PAIRWISE_ALPHAS if use_fast_alpha_grid() else RANK_PAIRWISE_ALPHAS


def structural_blend_alphas() -> list[float]:
    return FAST_GRID_STRUCTURAL_BLEND_ALPHAS if use_fast_alpha_grid() else STRUCTURAL_BLEND_ALPHAS


def neural_blend_alphas() -> list[float]:
    return FAST_GRID_NEURAL_BLEND_ALPHAS if use_fast_alpha_grid() else NEURAL_BLEND_ALPHAS


def rank_ensemble_weights() -> list[float]:
    return FAST_GRID_RANK_ENSEMBLE_WEIGHTS if use_fast_alpha_grid() else RANK_ENSEMBLE_WEIGHTS


def rank_ensemble_triples() -> list[tuple[float, float, float]]:
    return FAST_GRID_RANK_ENSEMBLE_TRIPLES if use_fast_alpha_grid() else RANK_ENSEMBLE_TRIPLES
NEURAL_SEQUENCE_STATIC_COLUMNS = [
    "horse_number_norm",
    "popularity_norm",
    "log_tansho_odds",
    "front_runner_score",
    "stalker_score",
    "closer_score",
    "inner_front_bias",
    "outer_closer_bias",
]
NEURAL_HISTORY_LAGS = 6
NEURAL_HISTORY_COLUMNS = [
    "corner1_norm",
    "corner2_norm",
    "corner3_norm",
    "corner4_norm",
    "finish_norm",
    "popularity_norm",
    "log_tansho_odds",
    "horse_number_norm",
    "time_sa",
    "kohan_3f",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--train-to-date", required=True)
    parser.add_argument("--test-from-date", required=True)
    parser.add_argument("--test-to-date", required=True)
    parser.add_argument("--model-output", default="apps/pc-keiba-viewer/tmp/corner-lightgbm-models")
    parser.add_argument("--predictions-output", default="apps/pc-keiba-viewer/tmp/corner-lightgbm-predictions.csv")
    parser.add_argument("--metrics-output", default="apps/pc-keiba-viewer/tmp/corner-lightgbm-metrics.json")
    return parser.parse_args()


def normalize_date(value: str) -> str:
    return value.replace("-", "")


def horizontal_sample_std(columns: list[str]) -> pl.Expr:
    count = float(len(columns))
    mean = pl.mean_horizontal(columns)
    squared_deviations = pl.sum_horizontal(
        [(pl.col(column) - mean).pow(2) for column in columns],
    )
    return (squared_deviations / (count - 1)).sqrt()


def add_style_features(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.mean_horizontal(["horse_corner1_avg", "horse_corner2_avg"]).alias("horse_early_avg"),
        pl.mean_horizontal(["horse_corner1_recent_avg", "horse_corner2_recent_avg"]).alias("horse_early_recent"),
        pl.mean_horizontal(["horse_corner1_last", "horse_corner2_last"]).alias("horse_early_last"),
        pl.mean_horizontal(["horse_corner3_avg", "horse_corner4_avg"]).alias("horse_late_avg"),
        pl.mean_horizontal(["horse_corner3_recent_avg", "horse_corner4_recent_avg"]).alias("horse_late_recent"),
        pl.mean_horizontal(["horse_corner3_last", "horse_corner4_last"]).alias("horse_late_last"),
        pl.mean_horizontal(["horse_corner1_recent_avg", "horse_corner2_recent_avg"]).alias("early_history_blend"),
        pl.mean_horizontal(["horse_corner3_recent_avg", "horse_corner4_recent_avg"]).alias("late_history_blend"),
    )
    df = df.with_columns(
        (pl.col("horse_late_avg") - pl.col("horse_early_avg")).alias("horse_pace_fade_avg"),
        (pl.col("horse_late_recent") - pl.col("horse_early_recent")).alias("horse_pace_fade_recent"),
        (pl.col("horse_late_last") - pl.col("horse_early_last")).alias("horse_pace_fade_last"),
        (1 - pl.col("horse_early_recent")).clip(0, 1).alias("front_runner_score"),
        (1 - (pl.col("horse_early_recent") - 0.33).abs() * 3).clip(0, 1).alias("stalker_score"),
        (pl.col("horse_early_recent") - pl.col("horse_late_recent")).clip(0, 1).alias("closer_score"),
        (
            1
            - horizontal_sample_std(
                [
                    "horse_corner1_avg",
                    "horse_corner2_avg",
                    "horse_corner3_avg",
                    "horse_corner4_avg",
                ],
            )
        )
        .clip(0, 1)
        .alias("pace_consistency"),
    )
    return df.with_columns(
        ((1 - pl.col("horse_number_norm")) * pl.col("front_runner_score")).clip(0, 1).alias("inner_front_bias"),
        (pl.col("horse_number_norm") * pl.col("closer_score")).clip(0, 1).alias("outer_closer_bias"),
    )


def race_relative_feature_expressions(column: str) -> list[pl.Expr]:
    raw = pl.col(column).cast(pl.Float64, strict=False)
    values = raw.fill_null(0)
    group_mean = raw.mean().over("race_id")
    group_std = (
        pl.when(raw.std().over("race_id") == 0)
        .then(None)
        .otherwise(raw.std().over("race_id"))
    )
    order = raw.arg_sort()
    inverse_order = order.arg_sort()
    sorted_prev = raw.gather(order).shift(1).gather(inverse_order).over("race_id")
    sorted_next = raw.gather(order).shift(-1).gather(inverse_order).over("race_id")
    race_rank = (raw.rank(method="average", descending=False) / pl.len()).over("race_id")
    centered = values - group_mean
    z_score = (values - group_mean) / group_std
    return [
        race_rank.fill_null(0.5).alias(f"{column}_race_rank"),
        centered.fill_null(0).alias(f"{column}_race_centered"),
        z_score.fill_nan(None).fill_null(0).alias(f"{column}_race_z"),
        (values - sorted_prev).fill_null(0).alias(f"{column}_front_gap"),
        (sorted_next - values).fill_null(0).alias(f"{column}_back_gap"),
    ]


def add_race_relative_features(df: pl.DataFrame) -> pl.DataFrame:
    expressions = [
        expression
        for column in RACE_RELATIVE_BASE_COLUMNS
        for expression in race_relative_feature_expressions(column)
    ]
    return df.with_columns(expressions)


def add_corner_history_rank_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (
            pl.col(column).cast(pl.Float64, strict=False).rank(method="average", descending=False)
            / pl.len()
        )
        .over("race_id")
        .fill_null(0.5)
        .alias(f"{column}_race_rank")
        for column in CORNER_HISTORY_RANK_BASE_COLUMNS
    )


def should_build_vector_neighbor_features(df: pl.DataFrame) -> bool:
    if any(column not in df.columns for column in VECTOR_NEIGHBOR_FEATURE_COLUMNS):
        return True
    count_columns = [f"vector_neighbor{k}_count" for k in VECTOR_NEIGHBOR_KS]
    counts = df.select(
        pl.col(column).cast(pl.Float64, strict=False).fill_null(0) for column in count_columns
    )
    return bool(counts.to_numpy().sum() <= 0)


def numeric_numpy_column(df: pl.DataFrame, column: str) -> FloatArray:
    return cast(FloatArray, df[column].cast(pl.Float64, strict=False).to_numpy().astype(float))


def build_vector_neighbor_input(df: pl.DataFrame) -> FloatArray:
    track_surface = np.where(numeric_numpy_column(df, "track_family") == 1, 0.0, 1.0)
    odds = np.maximum(numeric_numpy_column(df, "tansho_odds"), 1.0)
    vector_input = np.column_stack(
        (
            numeric_numpy_column(df, "kyori") / 3600,
            numeric_numpy_column(df, "shusso_tosu") / 18,
            numeric_numpy_column(df, "horse_number_norm"),
            numeric_numpy_column(df, "popularity_norm"),
            np.log(odds) / VECTOR_NEIGHBOR_LOG_ODDS_DENOMINATOR,
            track_surface,
            numeric_numpy_column(df, "keibajo_code_num") / 99,
            numeric_numpy_column(df, "race_bango_num") / 12,
        ),
    )
    return vector_input


def race_date_to_ordinal(value: object) -> int:
    text = str(value)
    if len(text) != 8 or not text.isdecimal():
        return 0
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8])).toordinal()
    except ValueError:
        return 0


def mean_absolute_error(actual: pl.Series, predicted: pl.Series) -> float:
    actual_values = actual.cast(pl.Float64, strict=False).to_numpy().astype(float)
    predicted_values = predicted.cast(pl.Float64, strict=False).to_numpy().astype(float)
    if actual_values.size == 0:
        return 0.0
    return float(np.mean(np.abs(actual_values - predicted_values)))


def lightgbm_device_params() -> LightGBMDeviceParams:
    device = os.environ.get("PC_KEIBA_LIGHTGBM_DEVICE", "cpu").strip().lower()
    if device in {"gpu", "cuda"}:
        return {"device_type": device}
    return {}


def set_lightgbm_device_params(
    model: lgb.LGBMClassifier | lgb.LGBMRanker | lgb.LGBMRegressor,
) -> None:
    params = lightgbm_device_params()
    if params:
        model.set_params(**params)


def should_fallback_to_cpu(error: LightGBMError) -> bool:
    return bool(lightgbm_device_params() and "GPU Tree Learner was not enabled" in str(error))


def to_lgb_frame(features: pl.DataFrame) -> object:
    # LightGBM's scikit-learn wrapper ingests pandas/numpy; convert polars at the
    # boundary so its feature-name handling works. All corner features are numeric.
    return features.to_pandas()


def to_lgb_target(target: pl.Series) -> object:
    return target.to_pandas()


def fit_regressor_with_device_fallback(
    model: lgb.LGBMRegressor,
    features: pl.DataFrame,
    target: pl.Series,
) -> None:
    lgb_features = to_lgb_frame(features)
    lgb_target = to_lgb_target(target)
    try:
        model.fit(lgb_features, lgb_target)
    except LightGBMError as error:
        if should_fallback_to_cpu(error):
            model.set_params(device_type="cpu")
            model.fit(lgb_features, lgb_target)
            return
        raise


def fit_ranker_with_device_fallback(
    model: lgb.LGBMRanker,
    features: pl.DataFrame,
    target: pl.Series,
    groups: FloatArray,
) -> None:
    lgb_features = to_lgb_frame(features)
    lgb_target = to_lgb_target(target)
    try:
        model.fit(lgb_features, lgb_target, group=groups)
    except LightGBMError as error:
        if should_fallback_to_cpu(error):
            model.set_params(device_type="cpu")
            model.fit(lgb_features, lgb_target, group=groups)
            return
        raise


def fit_classifier_with_device_fallback(
    model: lgb.LGBMClassifier,
    features: pl.DataFrame,
    target: pl.Series,
) -> None:
    lgb_features = to_lgb_frame(features)
    lgb_target = to_lgb_target(target)
    try:
        model.fit(lgb_features, lgb_target)
    except LightGBMError as error:
        if should_fallback_to_cpu(error):
            model.set_params(device_type="cpu")
            model.fit(lgb_features, lgb_target)
            return
        raise


def vector_neighbor_candidate_mask(
    row_index: int,
    current_date: int,
    candidate_positions: IntArray,
    target_values: FloatArray,
    date_ordinals: IntArray,
    kyori_values: FloatArray,
) -> BoolArray:
    return cast(
        BoolArray,
        (
            (candidate_positions != row_index)
            & (date_ordinals[candidate_positions] < current_date)
            & (date_ordinals[candidate_positions] >= current_date - VECTOR_NEIGHBOR_MAX_DAYS)
            & (
                np.abs(kyori_values[candidate_positions] - kyori_values[row_index])
                <= VECTOR_NEIGHBOR_DISTANCE_BAND
            )
            & np.isfinite(target_values[candidate_positions]).all(axis=1)
        ),
    )


def trim_vector_neighbor_candidates(
    candidate_positions: IntArray,
    vector_distances: FloatArray,
) -> tuple[IntArray, FloatArray]:
    if len(candidate_positions) <= VECTOR_NEIGHBOR_MAX_CANDIDATES:
        return candidate_positions, vector_distances
    return (
        candidate_positions[:VECTOR_NEIGHBOR_MAX_CANDIDATES],
        vector_distances[:VECTOR_NEIGHBOR_MAX_CANDIDATES],
    )


def vector_neighbor_batch_candidate_pool(
    batch_positions: IntArray,
    sorted_positions: IntArray,
    sorted_dates: IntArray,
    target_values: FloatArray,
    date_ordinals: IntArray,
    kyori_values: FloatArray,
) -> IntArray:
    batch_dates = date_ordinals[batch_positions]
    valid_batch_dates = batch_dates[batch_dates > 0]
    if len(valid_batch_dates) == 0:
        return np.array([], dtype=np.int64)
    left = int(np.searchsorted(sorted_dates, int(valid_batch_dates.min()) - VECTOR_NEIGHBOR_MAX_DAYS, side="left"))
    right = int(np.searchsorted(sorted_dates, int(valid_batch_dates.max()), side="left"))
    date_scoped_positions = sorted_positions[left:right]
    if len(date_scoped_positions) == 0:
        return np.array([], dtype=np.int64)
    batch_distances = kyori_values[batch_positions]
    pool_mask = (
        (kyori_values[date_scoped_positions] >= float(np.nanmin(batch_distances)) - VECTOR_NEIGHBOR_DISTANCE_BAND)
        & (kyori_values[date_scoped_positions] <= float(np.nanmax(batch_distances)) + VECTOR_NEIGHBOR_DISTANCE_BAND)
        & np.isfinite(target_values[date_scoped_positions]).all(axis=1)
    )
    return date_scoped_positions[pool_mask]


def top_vector_neighbor_candidates(
    candidate_positions: IntArray,
    squared_distances: FloatArray,
) -> tuple[IntArray, FloatArray]:
    top_count = min(VECTOR_NEIGHBOR_MAX_CANDIDATES, len(candidate_positions))
    if top_count == 0:
        return candidate_positions, np.sqrt(squared_distances)
    top_local = np.argpartition(squared_distances, top_count - 1)[:top_count]
    top_local = top_local[np.argsort(squared_distances[top_local])]
    return candidate_positions[top_local], np.sqrt(squared_distances[top_local])


def fill_vector_neighbor_output(
    output: dict[str, FloatArray],
    row_index: int,
    candidate_positions: IntArray,
    vector_distances: FloatArray,
    target_values: FloatArray,
) -> None:
    top_count = min(max(VECTOR_NEIGHBOR_KS), len(candidate_positions))
    top_positions = candidate_positions[:top_count]
    weights = 1 / (1 + vector_distances[:top_count])

    for k in VECTOR_NEIGHBOR_KS:
        scoped_count = min(k, len(top_positions))
        if scoped_count <= 0:
            continue
        scoped_positions = top_positions[:scoped_count]
        scoped_weights = weights[:scoped_count]
        weight_sum = float(scoped_weights.sum())
        output[f"vector_neighbor{k}_count"][row_index] = scoped_count
        output[f"vector_neighbor{k}_similarity_avg"][row_index] = float(scoped_weights.mean())
        if weight_sum <= 0:
            continue
        for corner_index in range(len(TARGET_COLUMNS)):
            output[f"vector_neighbor{k}_corner{corner_index + 1}_avg"][row_index] = float(
                np.average(target_values[scoped_positions, corner_index], weights=scoped_weights),
            )


def empty_vector_neighbor_output(length: int) -> dict[str, FloatArray]:
    return {
        column: np.zeros(length, dtype=float)
        for column in VECTOR_NEIGHBOR_FEATURE_COLUMNS
    }


def build_vector_neighbor_frame(df: pl.DataFrame, output: dict[str, FloatArray]) -> pl.DataFrame:
    df = df.drop([column for column in VECTOR_NEIGHBOR_FEATURE_COLUMNS if column in df.columns])
    return df.with_columns(
        pl.Series(column, output[column]) for column in VECTOR_NEIGHBOR_FEATURE_COLUMNS
    )


def import_mlx_core() -> object:
    return importlib.import_module("mlx.core")


def import_mlx_nn() -> object:
    return importlib.import_module("mlx.nn")


def import_mlx_optimizers() -> object:
    return importlib.import_module("mlx.optimizers")


def _mlx_module_base() -> type:
    """Return mlx.nn.Module when MLX is available, otherwise object."""
    try:
        mod = importlib.import_module("mlx.nn")
        return mod.Module  # type: ignore[attr-defined]
    except (ImportError, OSError):
        return object


_MlxModuleBase: type = _mlx_module_base()


def vector_neighbor_target_values(df: pl.DataFrame) -> FloatArray:
    return cast(
        FloatArray,
        df.select(
            pl.col(column).cast(pl.Float64, strict=False) for column in TARGET_COLUMNS
        ).to_numpy().astype(float),
    )


def vector_neighbor_group_positions(df: pl.DataFrame, group_columns: list[str]) -> list[IntArray]:
    indexed = df.with_row_index("__row_position")
    return [
        cast(IntArray, group["__row_position"].cast(pl.Int64).to_numpy().astype(np.int64))
        for _key, group in indexed.group_by(group_columns, maintain_order=True)
    ]


def add_derived_vector_neighbor_features_sklearn(df: pl.DataFrame) -> pl.DataFrame:
    output = empty_vector_neighbor_output(df.height)
    vectors = build_vector_neighbor_input(df)
    target_values = vector_neighbor_target_values(df)
    date_ordinals: IntArray = np.array(
        [race_date_to_ordinal(value) for value in df["race_date"].to_list()],
        dtype=np.int64,
    )
    kyori_values = numeric_numpy_column(df, "kyori")
    group_columns = ["source", "keibajo_code", "track_family"]

    for positions in vector_neighbor_group_positions(df, group_columns):
        if len(positions) <= 1:
            continue
        neighbor_count = min(VECTOR_NEIGHBOR_QUERY_CANDIDATES, len(positions))
        nearest_neighbors = NearestNeighbors(
            algorithm="auto",
            metric="euclidean",
            n_neighbors=neighbor_count,
        )
        nearest_neighbors.fit(vectors[positions])
        for batch_start in range(0, len(positions), VECTOR_NEIGHBOR_QUERY_BATCH_SIZE):
            batch_positions = positions[batch_start : batch_start + VECTOR_NEIGHBOR_QUERY_BATCH_SIZE]
            distance_matrix, local_index_matrix = nearest_neighbors.kneighbors(
                vectors[batch_positions],
                return_distance=True,
            )
            distance_matrix = cast(FloatArray, distance_matrix)
            local_index_matrix = cast(NDArray[np.intp], local_index_matrix)
            for batch_row_index, row_index in enumerate(batch_positions):
                current_date = date_ordinals[row_index]
                if current_date <= 0:
                    continue
                candidate_positions = positions[local_index_matrix[batch_row_index]]
                vector_distances = distance_matrix[batch_row_index]
                candidate_mask = vector_neighbor_candidate_mask(
                    int(row_index),
                    int(current_date),
                    candidate_positions,
                    target_values,
                    date_ordinals,
                    kyori_values,
                )
                candidate_positions, vector_distances = trim_vector_neighbor_candidates(
                    candidate_positions[candidate_mask],
                    vector_distances[candidate_mask],
                )
                if len(candidate_positions) == 0:
                    continue
                fill_vector_neighbor_output(
                    output,
                    int(row_index),
                    candidate_positions,
                    vector_distances,
                    target_values,
                )

    return build_vector_neighbor_frame(df, output)


def add_derived_vector_neighbor_features_mlx(df: pl.DataFrame) -> pl.DataFrame:
    mx = import_mlx_core()
    mx_array = getattr(mx, "array")
    mx_sum = getattr(mx, "sum")
    output = empty_vector_neighbor_output(df.height)
    vectors = build_vector_neighbor_input(df).astype(np.float32)
    target_values = vector_neighbor_target_values(df)
    date_ordinals: IntArray = np.array(
        [race_date_to_ordinal(value) for value in df["race_date"].to_list()],
        dtype=np.int64,
    )
    kyori_values = numeric_numpy_column(df, "kyori")
    group_columns = ["source", "keibajo_code", "track_family"]

    for positions in vector_neighbor_group_positions(df, group_columns):
        if len(positions) <= 1:
            continue
        sorted_positions = positions[np.argsort(date_ordinals[positions])]
        sorted_dates = date_ordinals[sorted_positions]
        for batch_start in range(0, len(positions), VECTOR_NEIGHBOR_MLX_BATCH_SIZE):
            batch_positions = positions[batch_start : batch_start + VECTOR_NEIGHBOR_MLX_BATCH_SIZE]
            pool_positions = vector_neighbor_batch_candidate_pool(
                batch_positions,
                sorted_positions,
                sorted_dates,
                target_values,
                date_ordinals,
                kyori_values,
            )
            if len(pool_positions) == 0:
                continue
            batch_vectors = mx_array(vectors[batch_positions])
            pool_vectors = mx_array(vectors[pool_positions])
            diff = batch_vectors[:, None, :] - pool_vectors[None, :, :]
            squared_distance_matrix = cast(FloatArray, np.asarray(mx_sum(diff * diff, axis=2), dtype=float))
            for batch_row_index, row_index in enumerate(batch_positions):
                current_date = date_ordinals[row_index]
                if current_date <= 0:
                    continue
                candidate_positions = pool_positions
                squared_distances = squared_distance_matrix[batch_row_index]
                candidate_mask = vector_neighbor_candidate_mask(
                    int(row_index),
                    int(current_date),
                    candidate_positions,
                    target_values,
                    date_ordinals,
                    kyori_values,
                )
                candidate_positions = candidate_positions[candidate_mask]
                squared_distances = squared_distances[candidate_mask]
                if len(candidate_positions) == 0:
                    continue
                candidate_positions, vector_distances = top_vector_neighbor_candidates(
                    candidate_positions,
                    squared_distances,
                )
                fill_vector_neighbor_output(
                    output,
                    int(row_index),
                    candidate_positions,
                    vector_distances,
                    target_values,
                )

    return build_vector_neighbor_frame(df, output)


def add_derived_vector_neighbor_features(df: pl.DataFrame) -> pl.DataFrame:
    if not should_build_vector_neighbor_features(df):
        return df
    if vector_neighbor_backend_name() == "mlx":
        try:
            return add_derived_vector_neighbor_features_mlx(df)
        except ImportError:
            return add_derived_vector_neighbor_features_sklearn(df)
    return add_derived_vector_neighbor_features_sklearn(df)


def vector_neighbor_backend_name() -> str:
    return os.environ.get("PC_KEIBA_VECTOR_BACKEND", "sklearn").strip().lower() or "sklearn"


def vector_neighbor_cache_path(input_path: str) -> Path:
    return cache_path_for_input(
        input_path,
        "tmp/corner-lightgbm-vector-cache",
        VECTOR_NEIGHBOR_CACHE_VERSION,
        ".npy",
    )


def feature_cache_path(input_path: str) -> Path:
    return cache_path_for_input(
        input_path,
        "tmp/corner-lightgbm-feature-cache",
        FEATURE_CACHE_VERSION,
        ".parquet",
    )


def cache_path_for_input(
    input_path: str,
    default_cache_dir: str,
    cache_version: str,
    suffix: str,
) -> Path:
    path = Path(input_path)
    stat = path.stat()
    digest = hashlib.sha256(
        "|".join(
            [
                str(path.resolve()),
                str(stat.st_size),
                str(stat.st_mtime_ns),
                vector_neighbor_backend_name(),
                cache_version,
                str(VECTOR_NEIGHBOR_MAX_CANDIDATES),
                str(VECTOR_NEIGHBOR_MAX_DAYS),
                str(VECTOR_NEIGHBOR_DISTANCE_BAND),
            ],
        ).encode("utf-8"),
    ).hexdigest()[:24]
    cache_env_name = "PC_KEIBA_VECTOR_CACHE_DIR" if suffix == ".npy" else "PC_KEIBA_FEATURE_CACHE_DIR"
    cache_dir = Path(os.environ.get(cache_env_name, default_cache_dir))
    return cache_dir / f"{digest}{suffix}"


def add_cached_vector_neighbor_features(df: pl.DataFrame, input_path: str) -> pl.DataFrame:
    if not should_build_vector_neighbor_features(df):
        return df
    cache_path = vector_neighbor_cache_path(input_path)
    if cache_path.exists():
        cached = np.load(cache_path)
        if cached.shape == (df.height, len(VECTOR_NEIGHBOR_FEATURE_COLUMNS)):
            output = {
                column: cast(FloatArray, cached[:, column_index])
                for column_index, column in enumerate(VECTOR_NEIGHBOR_FEATURE_COLUMNS)
            }
            return build_vector_neighbor_frame(df, output)
    df = add_derived_vector_neighbor_features(df)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(
        str(cache_path),
        df.select(VECTOR_NEIGHBOR_FEATURE_COLUMNS).to_numpy().astype(float),
    )
    return df


def numeric_column_or_default(df: pl.DataFrame, column: str, default: float) -> pl.Series:
    if column not in df.columns:
        return pl.Series(column, [default] * df.height, dtype=pl.Float64)
    return df[column].cast(pl.Float64, strict=False).fill_null(default)


def log_numeric_column_or_default(df: pl.DataFrame, source_column: str, default: float) -> pl.Series:
    values = numeric_column_or_default(df, source_column, default)
    return pl.Series(source_column, np.log(np.maximum(values.to_numpy().astype(float), 1)))


def normalized_numeric_column_or_default(
    df: pl.DataFrame,
    source_column: str,
    default: float,
    denominator: float,
) -> pl.Series:
    return numeric_column_or_default(df, source_column, default) / denominator


def add_code_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        numeric_column_or_default(df, source_column, 0).alias(target_column)
        for source_column, target_column in CODE_FEATURE_COLUMNS.items()
    )


def add_optional_derived_features(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        numeric_column_or_default(df, column, default).alias(column)
        for column, default in RAW_NUMERIC_DEFAULTS.items()
    )
    df = df.with_columns(
        log_numeric_column_or_default(df, source_column, 10).alias(
            source_column.replace("horse_odds", "horse_log_odds"),
        )
        for source_column in ODDS_HISTORY_COLUMNS
    )
    df = df.with_columns(
        normalized_numeric_column_or_default(df, source_column, 0, 60).alias(
            source_column.replace("horse_kohan_3f", "horse_kohan_3f_norm"),
        )
        for source_column in THREE_F_HISTORY_COLUMNS
    )
    return df.with_columns(
        normalized_numeric_column_or_default(df, "horse_days_since_last_start", 365, 365)
        .clip(0, 2)
        .alias("horse_days_since_last_start_norm"),
    )


def load_dataset(path: str) -> pl.DataFrame:
    cache_path = feature_cache_path(path)
    if cache_path.exists():
        return pl.read_parquet(cache_path)
    df = pl.read_csv(
        path,
        schema_overrides={"race_date": pl.Utf8, "race_id": pl.Utf8, "horse_key": pl.Utf8},
    )
    df = df.with_columns(
        df["keibajo_code"].cast(pl.Float64, strict=False).fill_null(0).alias("keibajo_code_num"),
        df["race_bango"].cast(pl.Float64, strict=False).fill_null(0).alias("race_bango_num"),
        df["track_code"].cast(pl.Float64, strict=False).fill_null(0).alias("track_code_num"),
        df["track_code"]
        .cast(pl.Utf8)
        .fill_null("")
        .str.slice(0, 1)
        .replace("", "0")
        .cast(pl.Float64)
        .alias("track_family"),
    )
    df = add_code_features(df)
    df = df.with_columns(
        df["kyori"].cast(pl.Float64, strict=False).fill_null(0).alias("kyori"),
        df["shusso_tosu"].cast(pl.Float64, strict=False).fill_null(0).alias("shusso_tosu"),
        df["umaban"].cast(pl.Float64, strict=False).fill_null(0).alias("umaban"),
    )
    df = add_optional_derived_features(df)
    nonzero_shusso = pl.when(pl.col("shusso_tosu") == 0).then(None).otherwise(pl.col("shusso_tosu"))
    df = df.with_columns(
        (pl.col("umaban") / nonzero_shusso).fill_null(0).alias("horse_number_norm"),
        pl.col("tansho_ninkijun")
        .cast(pl.Float64, strict=False)
        .fill_null(pl.col("shusso_tosu"))
        .alias("tansho_ninkijun"),
        pl.col("tansho_odds").cast(pl.Float64, strict=False).fill_null(10).alias("tansho_odds"),
    )
    df = df.with_columns(
        (pl.col("tansho_ninkijun") / nonzero_shusso).fill_null(1).alias("popularity_norm"),
        pl.max_horizontal(pl.col("tansho_odds"), pl.lit(1.0)).log().alias("log_tansho_odds"),
    )
    missing_history_columns = [column for column in HISTORY_COLUMNS if column not in df.columns]
    if missing_history_columns:
        df = df.with_columns(
            pl.lit(0, dtype=pl.Int64).alias(column) for column in missing_history_columns
        )
    df = df.with_columns(
        pl.col(column).cast(pl.Float64, strict=False).fill_null(0).alias(column)
        for column in HISTORY_COLUMNS
    )
    df = df.with_columns(
        pl.col(column).cast(pl.Float64, strict=False).alias(column) for column in TARGET_COLUMNS
    )
    df = add_cached_vector_neighbor_features(df, path)
    df = add_style_features(df)
    df = add_corner_history_rank_features(df)
    df = add_race_relative_features(df)
    df = df.drop_nulls(subset=["race_id", "race_date", *TARGET_COLUMNS])
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(cache_path)
    return df


def race_order_score(df: pl.DataFrame, target_column: str, prediction_column: str) -> float:
    scores: list[float] = []
    for _key, race in df.group_by("race_id", maintain_order=True):
        ordered_actual = cast(
            list[str],
            race.sort([target_column, "umaban"])["horse_key"].to_list(),
        )
        ordered_predicted = cast(
            list[str],
            race.sort([prediction_column, "umaban"])["horse_key"].to_list(),
        )
        predicted_positions = {horse_key: index + 1 for index, horse_key in enumerate(ordered_predicted)}
        errors = [
            abs((index + 1) - predicted_positions[horse_key])
            for index, horse_key in enumerate(ordered_actual)
            if horse_key in predicted_positions
        ]
        if not errors:
            continue
        max_error = max(1, len(errors) - 1)
        scores.append(max(0, 1 - (sum(errors) / len(errors)) / max_error))
    return float(np.mean(scores)) if scores else 0.0


def ranker_target(train: pl.DataFrame, target_column: str) -> pl.Series:
    actual_rank = pl.col(target_column).rank(method="ordinal", descending=False).over("race_id")
    group_size = pl.col("horse_key").count().over("race_id")
    return (
        train.select(
            (group_size - actual_rank).clip(lower_bound=0).cast(pl.Int64).alias("ranker_target"),
        )["ranker_target"]
    )


def train_model(train: pl.DataFrame, target_column: str) -> lgb.LGBMRegressor:
    model = lgb.LGBMRegressor(
        objective="regression",
        n_estimators=650,
        learning_rate=0.035,
        num_leaves=31,
        min_child_samples=25,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_alpha=0.05,
        reg_lambda=0.2,
        random_state=42,
        n_jobs=-1,
        verbosity=-1,
    )
    set_lightgbm_device_params(model)
    fit_regressor_with_device_fallback(model, train.select(FEATURE_COLUMNS), train[target_column])
    return model


def train_ranker(train: pl.DataFrame, target_column: str) -> lgb.LGBMRanker:
    ordered_train = train.sort(["race_id", "umaban"], maintain_order=True)
    groups = ordered_train.group_by("race_id", maintain_order=True).len()["len"].to_numpy()
    model = lgb.LGBMRanker(
        objective="lambdarank",
        n_estimators=420,
        learning_rate=0.035,
        num_leaves=31,
        min_child_samples=18,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_alpha=0.05,
        reg_lambda=0.2,
        random_state=43,
        n_jobs=-1,
        verbosity=-1,
    )
    set_lightgbm_device_params(model)
    fit_ranker_with_device_fallback(
        model,
        ordered_train.select(FEATURE_COLUMNS),
        ranker_target(ordered_train, target_column),
        groups.astype(np.float64),
    )
    return model


def empty_pairwise_dataset() -> tuple[pl.DataFrame, pl.Series]:
    empty_frame = pl.DataFrame(
        schema={column: pl.Float64 for column in PAIRWISE_FEATURE_COLUMNS},
    )
    return empty_frame, pl.Series("label", [], dtype=pl.Int64)


def pairwise_block(race: pl.DataFrame, target_column: str) -> tuple[FloatArray, IntArray] | None:
    size = race.height
    if size <= 1:
        return None
    left_indices, right_indices = np.triu_indices(size, k=1)
    base_matrix = race.select(
        pl.col(column).cast(pl.Float64, strict=False) for column in PAIRWISE_BASE_COLUMNS
    ).to_numpy().astype(float)
    diff = base_matrix[left_indices] - base_matrix[right_indices]
    features = cast(FloatArray, np.column_stack([diff, np.abs(diff)]))
    target_values = race[target_column].cast(pl.Float64, strict=False).to_numpy().astype(float)
    labels: IntArray = (target_values[left_indices] < target_values[right_indices]).astype(np.int64)
    return features, labels


def build_pairwise_dataset(df: pl.DataFrame, target_column: str) -> tuple[pl.DataFrame, pl.Series]:
    feature_blocks: list[FloatArray] = []
    label_blocks: list[IntArray] = []
    for _key, race in df.group_by("race_id", maintain_order=True):
        block = pairwise_block(race, target_column)
        if block is None:
            continue
        features, labels = block
        feature_blocks.append(features)
        label_blocks.append(labels)
    if not feature_blocks:
        return empty_pairwise_dataset()
    stacked_features = np.vstack(feature_blocks)
    pair_features = pl.DataFrame(
        {
            column: stacked_features[:, column_index]
            for column_index, column in enumerate(PAIRWISE_FEATURE_COLUMNS)
        },
    )
    return pair_features, pl.Series("label", np.concatenate(label_blocks))


def train_pairwise_model(train: pl.DataFrame, target_column: str) -> lgb.LGBMClassifier:
    pair_features, labels = build_pairwise_dataset(train, target_column)
    model = lgb.LGBMClassifier(
        objective="binary",
        n_estimators=320,
        learning_rate=0.04,
        num_leaves=31,
        min_child_samples=30,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_alpha=0.05,
        reg_lambda=0.2,
        random_state=44,
        n_jobs=-1,
        verbosity=-1,
    )
    set_lightgbm_device_params(model)
    fit_classifier_with_device_fallback(model, pair_features.select(PAIRWISE_FEATURE_COLUMNS), labels)
    return model


def train_stacking_model(
    train: pl.DataFrame,
    target_column: str,
    stacking_features: pl.DataFrame,
) -> lgb.LGBMRegressor:
    model = lgb.LGBMRegressor(
        objective="regression",
        n_estimators=260,
        learning_rate=0.025,
        num_leaves=15,
        min_child_samples=45,
        subsample=0.85,
        colsample_bytree=0.9,
        reg_alpha=0.15,
        reg_lambda=0.45,
        random_state=45,
        n_jobs=-1,
        verbosity=-1,
    )
    set_lightgbm_device_params(model)
    fit_regressor_with_device_fallback(model, stacking_features, train[target_column])
    return model


def numeric_history_expression(df: pl.DataFrame, column: str) -> pl.Expr:
    if column in df.columns:
        return pl.col(column).cast(pl.Float64, strict=False).fill_null(0).alias(column)
    return pl.lit(0.0).alias(column)


def neural_sequence_tensor(df: pl.DataFrame) -> FloatArray:
    working = df.with_columns(
        pl.Series("__date_ordinal", [race_date_to_ordinal(value) for value in df["race_date"].to_list()]),
        pl.col("kyori").cast(pl.Float64, strict=False).fill_null(0).alias("kyori"),
    ).with_columns(
        numeric_history_expression(df, column).alias(f"__history_{column}")
        for column in NEURAL_HISTORY_COLUMNS
    )
    working = working.with_row_index("__original_index")
    working = working.sort(["horse_key", "__date_ordinal", "race_id"], maintain_order=True)
    history_columns = [f"__history_{column}" for column in NEURAL_HISTORY_COLUMNS]
    static_values = working.select(
        pl.col(column).cast(pl.Float64, strict=False) for column in NEURAL_SEQUENCE_STATIC_COLUMNS
    ).to_numpy().astype(np.float32)
    sequences = np.zeros(
        (working.height, NEURAL_HISTORY_LAGS, len(NEURAL_HISTORY_COLUMNS) + len(NEURAL_SEQUENCE_STATIC_COLUMNS) + 2),
        dtype=np.float32,
    )
    current_dates = working["__date_ordinal"].cast(pl.Float64).to_numpy().astype(float)
    current_distances = working["kyori"].to_numpy().astype(float)
    for lag in range(1, NEURAL_HISTORY_LAGS + 1):
        lag_index = lag - 1
        shifted = working.select(
            *[pl.col(column).shift(lag).over("horse_key").fill_null(0).alias(column) for column in history_columns],
            pl.col("__date_ordinal").shift(lag).over("horse_key").fill_null(0).alias("__shifted_date"),
            pl.col("kyori").shift(lag).over("horse_key").alias("__shifted_distance"),
        )
        shifted_history = shifted.select(history_columns).to_numpy().astype(np.float32)
        shifted_dates = shifted["__shifted_date"].cast(pl.Float64).to_numpy().astype(float)
        shifted_distances = shifted["__shifted_distance"].to_numpy().astype(float)
        shifted_distances = np.where(np.isfinite(shifted_distances), shifted_distances, current_distances)
        days_since = np.clip((current_dates - shifted_dates) / 365, 0, 10).astype(np.float32)
        distance_delta = np.clip((current_distances - shifted_distances) / 1000, -3, 3).astype(np.float32)
        sequences[:, lag_index, :] = np.concatenate(
            [
                shifted_history,
                static_values,
                days_since[:, None],
                distance_delta[:, None],
            ],
            axis=1,
        )
    inverse_order = np.argsort(working["__original_index"].cast(pl.Int64).to_numpy().astype(np.int64))
    return cast(FloatArray, sequences[inverse_order])


class CornerLstmModel(_MlxModuleBase):  # pragma: no cover
    def __init__(self, input_size: int, hidden_size: int) -> None:
        super().__init__()
        nn = import_mlx_nn()
        self.lstm = nn.LSTM(input_size, hidden_size)  # ty: ignore[unresolved-attribute]
        self.output = nn.Linear(hidden_size, 1)  # ty: ignore[unresolved-attribute]

    def __call__(self, x):
        hidden, _cell = self.lstm(x)
        return self.output(hidden[:, -1, :]).squeeze(-1)


class CornerTransformerModel(_MlxModuleBase):  # pragma: no cover
    def __init__(self, input_size: int, hidden_size: int) -> None:
        super().__init__()
        nn = import_mlx_nn()
        self.input = nn.Linear(input_size, hidden_size)  # ty: ignore[unresolved-attribute]
        self.encoder = nn.TransformerEncoder(  # ty: ignore[unresolved-attribute]
            num_layers=2,
            dims=hidden_size,
            num_heads=4,
            mlp_dims=hidden_size * 2,
            dropout=0.0,
        )
        self.output = nn.Linear(hidden_size, 1)  # ty: ignore[unresolved-attribute]

    def __call__(self, x):
        encoded = self.encoder(self.input(x), None)
        return self.output(encoded.mean(axis=1)).squeeze(-1)


def train_neural_corner_model(
    model,
    train_sequences: FloatArray,
    target: pl.Series,
    epochs: int,
) -> object:  # pragma: no cover
    mx = import_mlx_core()
    nn = import_mlx_nn()
    optimizers = import_mlx_optimizers()
    optimizer = optimizers.Adam(learning_rate=0.002)  # ty: ignore[unresolved-attribute]
    target_values = target.to_numpy().astype(np.float32)
    batch_size = 4096

    def loss_fn(model_arg, batch_x, batch_y):
        prediction = model_arg(batch_x)
        return nn.losses.mse_loss(prediction, batch_y)  # ty: ignore[unresolved-attribute]

    loss_and_grad = nn.value_and_grad(model, loss_fn)  # ty: ignore[unresolved-attribute]
    for epoch in range(epochs):
        rng = np.random.default_rng(20260514 + epoch)
        for start in range(0, len(train_sequences), batch_size):
            batch_index = rng.permutation(len(train_sequences))[start : start + batch_size]
            batch_x = mx.array(train_sequences[batch_index])  # ty: ignore[unresolved-attribute]
            batch_y = mx.array(target_values[batch_index])  # ty: ignore[unresolved-attribute]
            loss, gradients = loss_and_grad(model, batch_x, batch_y)
            optimizer.update(model, gradients)
            mx.eval(model.parameters(), optimizer.state, loss)  # ty: ignore[unresolved-attribute]
    return model


def predict_neural_corner_model(model, sequences: FloatArray) -> pl.Series:  # pragma: no cover
    mx = import_mlx_core()
    predictions: list[np.ndarray] = []
    for start in range(0, len(sequences), 8192):
        batch = mx.array(sequences[start : start + 8192])  # ty: ignore[unresolved-attribute]
        predictions.append(np.asarray(model(batch), dtype=float))
    return clipped_prediction(pl.Series(np.concatenate(predictions)))


def train_lstm_model(train_sequences: FloatArray, target: pl.Series) -> object:  # pragma: no cover
    return train_neural_corner_model(CornerLstmModel(train_sequences.shape[2], 32), train_sequences, target, 2)


def train_transformer_model(train_sequences: FloatArray, target: pl.Series) -> object:  # pragma: no cover
    return train_neural_corner_model(CornerTransformerModel(train_sequences.shape[2], 32), train_sequences, target, 2)


def apply_pairwise_model(test: pl.DataFrame, model: lgb.LGBMClassifier, target_column: str) -> pl.Series:
    score_values = np.zeros(test.height, dtype=float)
    indexed = test.with_row_index("__row_position")
    for _key, race in indexed.group_by("race_id", maintain_order=True):
        size = race.height
        if size <= 1:
            continue
        positions = race["__row_position"].cast(pl.Int64).to_numpy().astype(np.int64)
        left_indices, right_indices = np.triu_indices(size, k=1)
        base_matrix = race.select(
            pl.col(column).cast(pl.Float64, strict=False) for column in PAIRWISE_BASE_COLUMNS
        ).to_numpy().astype(float)
        diff = base_matrix[left_indices] - base_matrix[right_indices]
        pair_features = pl.DataFrame(
            {
                column: cast(FloatArray, np.column_stack([diff, np.abs(diff)]))[:, column_index]
                for column_index, column in enumerate(PAIRWISE_FEATURE_COLUMNS)
            },
        )
        probabilities = np.asarray(
            model.predict_proba(to_lgb_frame(pair_features.select(PAIRWISE_FEATURE_COLUMNS))),
            dtype=float,
        )[:, 1]
        for pair_index, probability in enumerate(probabilities):
            score_values[positions[left_indices[pair_index]]] += float(probability)
            score_values[positions[right_indices[pair_index]]] += 1 - float(probability)
    group_size = test.select(
        pl.col("horse_key").count().over("race_id").alias("group_size"),
    )["group_size"].to_numpy().astype(float)
    denominator = np.where(group_size - 1 == 0, np.nan, group_size - 1)
    result = 1 - score_values / denominator
    return pl.Series("pairwise", np.where(np.isnan(result), 0.5, result))


def normalized_rank_prediction(df: pl.DataFrame, score_column: str) -> pl.Series:
    rank = pl.col(score_column).rank(method="ordinal", descending=True).over("race_id")
    group_size = pl.col(score_column).count().over("race_id")
    denominator = pl.when(group_size - 1 == 0).then(None).otherwise(group_size - 1)
    return df.select(
        ((rank - 1) / denominator).fill_null(0.5).alias("normalized_rank"),
    )["normalized_rank"]


def normalized_position_rank_prediction(df: pl.DataFrame, values: pl.Series) -> pl.Series:
    scoped = df.select(pl.col("race_id")).with_columns(values.alias("__position_rank_source"))
    rank = pl.col("__position_rank_source").rank(method="ordinal", descending=False).over("race_id")
    group_size = pl.col("__position_rank_source").count().over("race_id")
    denominator = pl.when(group_size - 1 == 0).then(None).otherwise(group_size - 1)
    return scoped.select(
        ((rank - 1) / denominator).fill_null(0.5).alias("normalized_position_rank"),
    )["normalized_position_rank"]


def clipped_prediction(values: pl.Series) -> pl.Series:
    return pl.Series(values.name, np.clip(values.to_numpy().astype(float), 0, 1))


def fill_null_with_fallback(values: pl.Series, fallback: pl.Series) -> pl.Series:
    value_array = values.cast(pl.Float64, strict=False).to_numpy().astype(float)
    fallback_array = fallback.cast(pl.Float64, strict=False).to_numpy().astype(float)
    return pl.Series(values.name, np.where(np.isnan(value_array), fallback_array, value_array))


def blended_prediction(base: pl.Series, overlay: pl.Series, alpha: float) -> pl.Series:
    return clipped_prediction(base * (1 - alpha) + overlay * alpha)


def column_or_prediction_fallback(df: pl.DataFrame, column: str, fallback_column: str) -> pl.Series:
    if column in df.columns:
        return df[column]
    return df[fallback_column]


def stacking_feature_frame(
    df: pl.DataFrame,
    target_column: str,
    regression_column: str,
    ranker_column: str,
    pairwise_column: str,
) -> pl.DataFrame:
    structural_candidates = structural_candidate_predictions(df, target_column, regression_column, pairwise_column)
    features: dict[str, pl.Series] = {
        "regression": df[regression_column],
        "ranker": df[ranker_column],
        "pairwise": df[pairwise_column],
        "horse_number_norm": df["horse_number_norm"],
        "popularity_norm": df["popularity_norm"],
        "front_runner_score": df["front_runner_score"],
        "stalker_score": df["stalker_score"],
        "closer_score": df["closer_score"],
        "inner_front_bias": df["inner_front_bias"],
        "outer_closer_bias": df["outer_closer_bias"],
    }
    for name, prediction in structural_candidates.items():
        features[name] = prediction
        features[f"{name}_rank"] = normalized_position_rank_prediction(df, prediction)
    stacking_frame = pl.DataFrame(
        {name: series.rename(name) for name, series in features.items()},
    )
    return stacking_frame.with_columns(
        pl.when(pl.col(column).cast(pl.Float64, strict=False).is_infinite())
        .then(None)
        .otherwise(pl.col(column).cast(pl.Float64, strict=False))
        .fill_nan(None)
        .fill_null(0.5)
        .alias(column)
        for column in stacking_frame.columns
    )


def structural_candidate_predictions(
    test: pl.DataFrame,
    target_column: str,
    regression_column: str,
    pairwise_column: str,
    stacked_column: str | None = None,
    lstm_column: str | None = None,
    transformer_column: str | None = None,
) -> dict[str, pl.Series]:
    corner_name = target_column.replace("_norm", "")
    horse_recent = column_or_prediction_fallback(test, f"horse_{corner_name}_recent_avg", regression_column)
    horse_avg = column_or_prediction_fallback(test, f"horse_{corner_name}_avg", regression_column)
    horse_last = column_or_prediction_fallback(test, f"horse_{corner_name}_last", regression_column)
    front_style = clipped_prediction(1 - test["front_runner_score"])
    stalker_style = clipped_prediction(1 - test["stalker_score"])
    late_style = test["horse_late_recent"]
    candidates = {
        "pairwise": test[pairwise_column],
        "horse_number": test["horse_number_norm"],
        "popularity": test["popularity_norm"],
        "front_style": front_style,
        "stalker_style": stalker_style,
        "late_style": late_style,
        "horse_avg": horse_avg,
        "horse_recent": horse_recent,
        "horse_last": horse_last,
        "course_number": column_or_prediction_fallback(test, f"course_number_{corner_name}_avg", regression_column),
        "venue_course_number": column_or_prediction_fallback(
            test,
            f"venue_course_number_{corner_name}_avg",
            regression_column,
        ),
    }
    if stacked_column is not None and stacked_column in test.columns:
        candidates["stacked"] = test[stacked_column]
    if lstm_column is not None and lstm_column in test.columns:
        candidates["lstm"] = test[lstm_column]
    if transformer_column is not None and transformer_column in test.columns:
        candidates["transformer"] = test[transformer_column]
    if (
        lstm_column is not None
        and transformer_column is not None
        and lstm_column in test.columns
        and transformer_column in test.columns
    ):
        candidates["neural_average"] = clipped_prediction((test[lstm_column] + test[transformer_column]) / 2)
    candidates.update(
        corner_weighted_structural_candidates(
            target_column,
            test,
            horse_recent,
            horse_avg,
            horse_last,
        ),
    )
    for k in VECTOR_NEIGHBOR_KS:
        candidates[f"vector_neighbor{k}"] = column_or_prediction_fallback(
            test,
            f"vector_neighbor{k}_{corner_name}_avg",
            regression_column,
        )
    return candidates


def corner_weighted_structural_candidates(
    target_column: str,
    test: pl.DataFrame,
    horse_recent: pl.Series,
    horse_avg: pl.Series,
    horse_last: pl.Series,
) -> dict[str, pl.Series]:
    front_style = clipped_prediction(1 - test["front_runner_score"])
    stalker_style = clipped_prediction(1 - test["stalker_score"])
    late_style = test["horse_late_recent"]
    inner = test["horse_number_norm"]
    popularity = test["popularity_norm"]
    if target_column == "corner1_norm":
        return {
            "corner_specific_primary": clipped_prediction(
                horse_recent * 0.48 + inner * 0.24 + front_style * 0.2 + popularity * 0.08,
            ),
            "corner_specific_history": clipped_prediction(
                horse_last * 0.45 + horse_recent * 0.25 + inner * 0.2 + front_style * 0.1,
            ),
        }
    if target_column == "corner2_norm":
        return {
            "corner_specific_primary": clipped_prediction(
                horse_recent * 0.52 + inner * 0.18 + front_style * 0.2 + horse_avg * 0.1,
            ),
            "corner_specific_history": clipped_prediction(
                horse_last * 0.34 + horse_recent * 0.36 + stalker_style * 0.16 + inner * 0.14,
            ),
        }
    if target_column == "corner3_norm":
        return {
            "corner_specific_primary": clipped_prediction(
                horse_recent * 0.46 + horse_avg * 0.24 + late_style * 0.18 + stalker_style * 0.12,
            ),
            "corner_specific_history": clipped_prediction(
                horse_last * 0.28 + horse_recent * 0.42 + horse_avg * 0.2 + late_style * 0.1,
            ),
        }
    return {
        "corner_specific_primary": clipped_prediction(
            horse_recent * 0.42 + horse_avg * 0.28 + horse_last * 0.16 + late_style * 0.14,
        ),
        "corner_specific_history": clipped_prediction(
            horse_last * 0.24 + horse_recent * 0.44 + horse_avg * 0.2 + stalker_style * 0.12,
        ),
    }


def score_prediction(
    test: pl.DataFrame,
    target_column: str,
    prediction: pl.Series,
    scratch_column: str,
) -> float:
    scored = test.with_columns(prediction.alias(scratch_column))
    return race_order_score(scored, target_column, scratch_column)


def update_best_prediction(
    test: pl.DataFrame,
    target_column: str,
    candidate: pl.Series,
    score_key: str,
    current: tuple[float, float, pl.Series],
    scores: dict[str, float],
) -> tuple[float, float, pl.Series]:
    score = score_prediction(test, target_column, candidate, f"candidate_{target_column}_{score_key}")
    scores[score_key] = score
    if score <= current[1]:
        return current
    return -1.0, score, candidate.clone()


def rank_candidate_predictions(
    test: pl.DataFrame,
    structural_candidates: dict[str, pl.Series],
    regression_column: str,
    ranker_column: str,
) -> dict[str, pl.Series]:
    candidates = {
        "regression_rank": normalized_position_rank_prediction(test, test[regression_column]),
        "ranker_rank": clipped_prediction(test[ranker_column]),
    }
    for name, prediction in structural_candidates.items():
        candidates[f"{name}_rank"] = normalized_position_rank_prediction(test, prediction)
    return candidates


def search_rank_pair_ensembles(
    test: pl.DataFrame,
    target_column: str,
    candidates: dict[str, pl.Series],
    current: tuple[float, float, pl.Series],
    scores: dict[str, float],
) -> tuple[float, float, pl.Series]:
    names = list(candidates)
    best = current
    for left_index, left_name in enumerate(names):
        for right_name in names[left_index + 1 :]:
            for weight in rank_ensemble_weights():
                prediction = blended_prediction(candidates[left_name], candidates[right_name], weight)
                score_key = f"rankmix_{left_name}_{right_name}_{weight}"
                best = update_best_prediction(test, target_column, prediction, score_key, best, scores)
    return best


def search_rank_triple_ensembles(
    test: pl.DataFrame,
    target_column: str,
    candidates: dict[str, pl.Series],
    current: tuple[float, float, pl.Series],
    scores: dict[str, float],
) -> tuple[float, float, pl.Series]:
    preferred_names = [
        name
        for name in [
            "regression_rank",
            "ranker_rank",
            "pairwise_rank",
            "horse_recent_rank",
            "horse_avg_rank",
            "vector_neighbor30_rank",
            "venue_course_number_rank",
            "popularity_rank",
        ]
        if name in candidates
    ]
    best = current
    for left_index, left_name in enumerate(preferred_names):
        for middle_index, middle_name in enumerate(preferred_names[left_index + 1 :], left_index + 1):
            for right_name in preferred_names[middle_index + 1 :]:
                for left_weight, middle_weight, right_weight in rank_ensemble_triples():
                    prediction = clipped_prediction(
                        candidates[left_name] * left_weight
                        + candidates[middle_name] * middle_weight
                        + candidates[right_name] * right_weight,
                    )
                    score_key = (
                        f"rankmix3_{left_name}_{middle_name}_{right_name}_"
                        f"{left_weight}_{middle_weight}_{right_weight}"
                    )
                    best = update_best_prediction(test, target_column, prediction, score_key, best, scores)
    return best


def search_rank_ensembles(
    test: pl.DataFrame,
    target_column: str,
    structural_candidates: dict[str, pl.Series],
    regression_column: str,
    ranker_column: str,
    current: tuple[float, float, pl.Series],
    scores: dict[str, float],
) -> tuple[float, float, pl.Series]:
    candidates = rank_candidate_predictions(test, structural_candidates, regression_column, ranker_column)
    best = current
    for name, prediction in candidates.items():
        best = update_best_prediction(test, target_column, prediction, name, best, scores)
    best = search_rank_pair_ensembles(test, target_column, candidates, best, scores)
    return search_rank_triple_ensembles(test, target_column, candidates, best, scores)


def majority_vote_prediction(test: pl.DataFrame, candidates: dict[str, pl.Series]) -> pl.Series:
    vote_names = [
        name
        for name in [
            "regression_rank",
            "ranker_rank",
            "pairwise_rank",
            "stacked_rank",
            "lstm_rank",
            "transformer_rank",
            "horse_recent_rank",
            "horse_number_rank",
        ]
        if name in candidates
    ]
    if not vote_names:
        return pl.Series("majority_vote", np.full(test.height, 0.5, dtype=float))
    vote_sum = np.zeros(test.height, dtype=float)
    for name in vote_names:
        vote_sum = vote_sum + normalized_position_rank_prediction(test, candidates[name]).to_numpy().astype(float)
    return clipped_prediction(pl.Series("majority_vote", vote_sum / len(vote_names)))


def choose_ensemble_prediction(
    test: pl.DataFrame,
    target_column: str,
    regression_column: str,
    ranker_column: str,
    pairwise_column: str,
    stacked_column: str | None = None,
    lstm_column: str | None = None,
    transformer_column: str | None = None,
) -> tuple[pl.Series, float, dict[str, float]]:
    scores: dict[str, float] = {}
    best_alpha = -1.0
    best_score = -1.0
    best_prediction = test[regression_column]
    structural_candidates = structural_candidate_predictions(
        test,
        target_column,
        regression_column,
        pairwise_column,
        stacked_column,
        lstm_column,
        transformer_column,
    )

    for alpha in regression_ranker_alphas():
        candidate = blended_prediction(test[regression_column], test[ranker_column], alpha)
        score = score_prediction(
            test,
            target_column,
            candidate,
            f"candidate_{target_column}_{alpha}",
        )
        scores[str(alpha)] = score
        if score > best_score:
            best_alpha = alpha
            best_score = score
            best_prediction = candidate.clone()

    for alpha in rank_pairwise_alphas():
        candidate = blended_prediction(test[ranker_column], test[pairwise_column], alpha)
        score = score_prediction(
            test,
            target_column,
            candidate,
            f"candidate_{target_column}_rank_pair_{alpha}",
        )
        scores[f"rank_pair_{alpha}"] = score
        if score > best_score:
            best_alpha = alpha
            best_score = score
            best_prediction = candidate.clone()

    for name, structural_prediction in structural_candidates.items():
        base_prediction = clipped_prediction(
            fill_null_with_fallback(structural_prediction, test[regression_column]),
        )
        score = score_prediction(test, target_column, base_prediction, f"structural_{target_column}_{name}")
        scores[name] = score
        if score > best_score:
            best_alpha = 1.0
            best_score = score
            best_prediction = base_prediction.clone()
        for alpha in structural_blend_alphas():
            candidate = blended_prediction(test[regression_column], base_prediction, alpha)
            score = score_prediction(
                test,
                target_column,
                candidate,
                f"candidate_{target_column}_{name}_{alpha}",
            )
            scores[f"{name}_{alpha}"] = score
            if score > best_score:
                best_alpha = alpha
                best_score = score
                best_prediction = candidate.clone()
        if name in {"lstm", "transformer", "neural_average"}:
            for alpha in neural_blend_alphas():
                candidate = blended_prediction(test[regression_column], base_prediction, alpha)
                score = score_prediction(
                    test,
                    target_column,
                    candidate,
                    f"candidate_{target_column}_{name}_neural_{alpha}",
                )
                scores[f"{name}_neural_{alpha}"] = score
                if score > best_score:
                    best_alpha = alpha
                    best_score = score
                    best_prediction = candidate.clone()
    best_alpha, best_score, best_prediction = search_rank_ensembles(
        test,
        target_column,
        structural_candidates,
        regression_column,
        ranker_column,
        (best_alpha, best_score, best_prediction),
        scores,
    )
    rank_candidates = rank_candidate_predictions(test, structural_candidates, regression_column, ranker_column)
    majority_prediction = majority_vote_prediction(test, rank_candidates)
    best_alpha, best_score, best_prediction = update_best_prediction(
        test,
        target_column,
        majority_prediction,
        "majority_vote",
        (best_alpha, best_score, best_prediction),
        scores,
    )
    return best_prediction, best_alpha, scores


def main() -> None:
    total_started_at = perf_counter()
    args = parse_args()
    load_started_at = perf_counter()
    df = load_dataset(args.input)
    load_seconds = perf_counter() - load_started_at
    train_to_date = normalize_date(args.train_to_date)
    test_from_date = normalize_date(args.test_from_date)
    test_to_date = normalize_date(args.test_to_date)
    train_mask = df["race_date"] <= train_to_date
    test_mask = (df["race_date"] >= test_from_date) & (df["race_date"] <= test_to_date)
    train_positions = np.flatnonzero(train_mask.to_numpy())
    test_positions = np.flatnonzero(test_mask.to_numpy())
    train = df.filter(train_mask)
    test = df.filter(test_mask)
    if train.is_empty() or test.is_empty():
        raise SystemExit("train or test dataset is empty")
    all_sequences = neural_sequence_tensor(df)
    train_sequences = all_sequences[train_positions]
    test_sequences = all_sequences[test_positions]

    model_dir = Path(args.model_output)
    model_dir.mkdir(parents=True, exist_ok=True)
    metrics: dict[str, object] = {
        "features": FEATURE_COLUMNS,
        "pairwise_features": PAIRWISE_FEATURE_COLUMNS,
        "test_from_date": test_from_date,
        "test_rows": test.height,
        "test_to_date": test_to_date,
        "train_rows": train.height,
        "train_to_date": train_to_date,
        "timing": {
            "dataset_load_seconds": load_seconds,
            "targets": {},
            "vector_backend": os.environ.get("PC_KEIBA_VECTOR_BACKEND", "sklearn") or "sklearn",
            "lightgbm_device": os.environ.get("PC_KEIBA_LIGHTGBM_DEVICE", "cpu") or "cpu",
        },
    }

    for target_column in TARGET_COLUMNS:
        target_started_at = perf_counter()
        model = train_model(train, target_column)
        ranker = train_ranker(train, target_column)
        pairwise_model = train_pairwise_model(train, target_column)
        prediction_column = f"predicted_{target_column}"
        regression_column = f"regression_{target_column}"
        ranker_score_column = f"ranker_score_{target_column}"
        ranker_prediction_column = f"ranker_{target_column}"
        pairwise_prediction_column = f"pairwise_{target_column}"
        stacked_prediction_column = f"stacked_{target_column}"
        lstm_prediction_column = f"lstm_{target_column}"
        transformer_prediction_column = f"transformer_{target_column}"
        train = train.with_columns(
            pl.Series(
                regression_column,
                np.clip(
                    np.asarray(model.predict(to_lgb_frame(train.select(FEATURE_COLUMNS))), dtype=float),
                    0,
                    1,
                ),
            ),
            pl.Series(
                ranker_score_column,
                np.asarray(ranker.predict(to_lgb_frame(train.select(FEATURE_COLUMNS))), dtype=float),
            ),
        )
        train = train.with_columns(
            normalized_rank_prediction(train, ranker_score_column).alias(ranker_prediction_column),
        )
        train = train.with_columns(
            apply_pairwise_model(train, pairwise_model, target_column).alias(pairwise_prediction_column),
        )
        stacker = train_stacking_model(
            train,
            target_column,
            stacking_feature_frame(
                train,
                target_column,
                regression_column,
                ranker_prediction_column,
                pairwise_prediction_column,
            ),
        )
        model.booster_.save_model(str(model_dir / f"{target_column}.txt"))
        ranker.booster_.save_model(str(model_dir / f"{target_column}.ranker.txt"))
        pairwise_model.booster_.save_model(str(model_dir / f"{target_column}.pairwise.txt"))
        stacker.booster_.save_model(str(model_dir / f"{target_column}.stacker.txt"))
        test = test.with_columns(
            pl.Series(
                regression_column,
                np.clip(
                    np.asarray(model.predict(to_lgb_frame(test.select(FEATURE_COLUMNS))), dtype=float),
                    0,
                    1,
                ),
            ),
            pl.Series(
                ranker_score_column,
                np.asarray(ranker.predict(to_lgb_frame(test.select(FEATURE_COLUMNS))), dtype=float),
            ),
        )
        test = test.with_columns(
            normalized_rank_prediction(test, ranker_score_column).alias(ranker_prediction_column),
        )
        test = test.with_columns(
            apply_pairwise_model(test, pairwise_model, target_column).alias(pairwise_prediction_column),
        )
        test = test.with_columns(
            pl.Series(
                stacked_prediction_column,
                np.clip(
                    np.asarray(
                        stacker.predict(
                            to_lgb_frame(
                                stacking_feature_frame(
                                    test,
                                    target_column,
                                    regression_column,
                                    ranker_prediction_column,
                                    pairwise_prediction_column,
                                ),
                            ),
                        ),
                        dtype=float,
                    ),
                    0,
                    1,
                ),
            ),
        )
        # Train MLX neural models when available; skip gracefully on non-Apple-Silicon.
        # Each model is guarded independently so a partial failure doesn't silently
        # discard a successfully-trained companion model.
        # Training failures (ImportError/OSError/RuntimeError/AttributeError) are caught;
        # prediction errors (e.g. disk-full OSError) are intentionally not caught here.
        _lstm_col: str | None
        _transformer_col: str | None
        try:
            _lstm_model: object = train_lstm_model(train_sequences, train[target_column])
        except (ImportError, OSError, RuntimeError, AttributeError):
            _lstm_model = None
        if _lstm_model is not None:
            test = test.with_columns(
                pl.Series(
                    lstm_prediction_column,
                    np.asarray(predict_neural_corner_model(_lstm_model, test_sequences).to_numpy(), dtype=float),
                ),
            )
            _lstm_col = lstm_prediction_column
        else:
            _lstm_col = None
        try:
            _transformer_model: object = train_transformer_model(train_sequences, train[target_column])
        except (ImportError, OSError, RuntimeError, AttributeError):
            _transformer_model = None
        if _transformer_model is not None:
            test = test.with_columns(
                pl.Series(
                    transformer_prediction_column,
                    np.asarray(
                        predict_neural_corner_model(_transformer_model, test_sequences).to_numpy(),
                        dtype=float,
                    ),
                ),
            )
            _transformer_col = transformer_prediction_column
        else:
            _transformer_col = None
        prediction, alpha, alpha_scores = choose_ensemble_prediction(
            test,
            target_column,
            regression_column,
            ranker_prediction_column,
            pairwise_prediction_column,
            stacked_prediction_column,
            _lstm_col,
            _transformer_col,
        )
        test = test.with_columns(prediction.alias(prediction_column))
        metrics[target_column] = {
            "mae": float(mean_absolute_error(test[target_column], test[prediction_column])),
            "ranker_alpha": alpha,
            "ranker_alpha_scores": alpha_scores,
            "race_order_score": race_order_score(test, target_column, prediction_column),
        }
        cast(dict[str, object], metrics["timing"])["targets"] = {
            **cast(dict[str, object], cast(dict[str, object], metrics["timing"])["targets"]),
            target_column: perf_counter() - target_started_at,
        }

    prediction_columns = [f"predicted_{column}" for column in TARGET_COLUMNS]
    output_columns = [
        "source",
        "race_date",
        "race_id",
        "horse_key",
        "keibajo_code",
        "race_bango",
        "umaban",
        *TARGET_COLUMNS,
        *prediction_columns,
    ]
    Path(args.predictions_output).parent.mkdir(parents=True, exist_ok=True)
    test.select(output_columns).write_csv(args.predictions_output)
    Path(args.metrics_output).parent.mkdir(parents=True, exist_ok=True)
    cast(dict[str, object], metrics["timing"])["total_seconds"] = perf_counter() - total_started_at
    Path(args.metrics_output).write_text(json.dumps(metrics, ensure_ascii=False, indent=2), "utf-8")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
