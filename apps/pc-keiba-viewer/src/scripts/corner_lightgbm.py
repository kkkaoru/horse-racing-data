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
import pandas as pd
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


def add_style_features(df: pd.DataFrame) -> pd.DataFrame:
    df["horse_early_avg"] = df[["horse_corner1_avg", "horse_corner2_avg"]].mean(axis=1)
    df["horse_early_recent"] = df[["horse_corner1_recent_avg", "horse_corner2_recent_avg"]].mean(axis=1)
    df["horse_early_last"] = df[["horse_corner1_last", "horse_corner2_last"]].mean(axis=1)
    df["horse_late_avg"] = df[["horse_corner3_avg", "horse_corner4_avg"]].mean(axis=1)
    df["horse_late_recent"] = df[["horse_corner3_recent_avg", "horse_corner4_recent_avg"]].mean(axis=1)
    df["horse_late_last"] = df[["horse_corner3_last", "horse_corner4_last"]].mean(axis=1)
    df["horse_pace_fade_avg"] = df["horse_late_avg"] - df["horse_early_avg"]
    df["horse_pace_fade_recent"] = df["horse_late_recent"] - df["horse_early_recent"]
    df["horse_pace_fade_last"] = df["horse_late_last"] - df["horse_early_last"]
    df["front_runner_score"] = (1 - df["horse_early_recent"]).clip(0, 1)
    df["stalker_score"] = (1 - (df["horse_early_recent"] - 0.33).abs() * 3).clip(0, 1)
    df["closer_score"] = (df["horse_early_recent"] - df["horse_late_recent"]).clip(0, 1)
    df["pace_consistency"] = (
        1
        - df[
            [
                "horse_corner1_avg",
                "horse_corner2_avg",
                "horse_corner3_avg",
                "horse_corner4_avg",
            ]
        ].std(axis=1)
    ).clip(0, 1)
    df["inner_front_bias"] = ((1 - df["horse_number_norm"]) * df["front_runner_score"]).clip(0, 1)
    df["outer_closer_bias"] = (df["horse_number_norm"] * df["closer_score"]).clip(0, 1)
    df["early_history_blend"] = df[["horse_corner1_recent_avg", "horse_corner2_recent_avg"]].mean(axis=1)
    df["late_history_blend"] = df[["horse_corner3_recent_avg", "horse_corner4_recent_avg"]].mean(axis=1)
    return df


def add_race_relative_features(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby("race_id", sort=False)
    relative_features: dict[str, pd.Series] = {}
    for column in RACE_RELATIVE_BASE_COLUMNS:
        values = pd.to_numeric(df[column], errors="coerce").fillna(0)
        group_mean = grouped[column].transform("mean")
        group_std = grouped[column].transform("std").replace(0, np.nan)
        sorted_prev = grouped[column].transform(lambda series: series.sort_values().shift(1).reindex(series.index))
        sorted_next = grouped[column].transform(lambda series: series.sort_values().shift(-1).reindex(series.index))
        relative_features[f"{column}_race_rank"] = grouped[column].rank(
            method="average",
            pct=True,
            ascending=True,
        ).fillna(0.5)
        relative_features[f"{column}_race_centered"] = (values - group_mean).fillna(0)
        relative_features[f"{column}_race_z"] = (
            (values - group_mean) / group_std
        ).replace([np.inf, -np.inf], np.nan).fillna(0)
        relative_features[f"{column}_front_gap"] = (values - sorted_prev).fillna(0)
        relative_features[f"{column}_back_gap"] = (sorted_next - values).fillna(0)
    return pd.concat([df, pd.DataFrame(relative_features, index=df.index)], axis=1)


def add_corner_history_rank_features(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby("race_id", sort=False)
    rank_features = {
        f"{column}_race_rank": grouped[column].rank(method="average", pct=True, ascending=True).fillna(0.5)
        for column in CORNER_HISTORY_RANK_BASE_COLUMNS
    }
    return pd.concat([df, pd.DataFrame(rank_features, index=df.index)], axis=1)


def should_build_vector_neighbor_features(df: pd.DataFrame) -> bool:
    if any(column not in df.columns for column in VECTOR_NEIGHBOR_FEATURE_COLUMNS):
        return True
    count_columns = [f"vector_neighbor{k}_count" for k in VECTOR_NEIGHBOR_KS]
    counts = df[count_columns].apply(pd.to_numeric, errors="coerce").fillna(0)
    return bool(counts.to_numpy(dtype=float).sum() <= 0)


def build_vector_neighbor_input(df: pd.DataFrame) -> FloatArray:
    track_surface = np.where(df["track_family"].to_numpy(dtype=float) == 1, 0.0, 1.0)
    odds = np.maximum(df["tansho_odds"].to_numpy(dtype=float), 1.0)
    vector_input = np.column_stack(
        (
            df["kyori"].to_numpy(dtype=float) / 3600,
            df["shusso_tosu"].to_numpy(dtype=float) / 18,
            df["horse_number_norm"].to_numpy(dtype=float),
            df["popularity_norm"].to_numpy(dtype=float),
            np.log(odds) / VECTOR_NEIGHBOR_LOG_ODDS_DENOMINATOR,
            track_surface,
            df["keibajo_code_num"].to_numpy(dtype=float) / 99,
            df["race_bango_num"].to_numpy(dtype=float) / 12,
        ),
    )
    return cast(FloatArray, vector_input)


def race_date_to_ordinal(value: object) -> int:
    text = str(value)
    if len(text) != 8 or not text.isdecimal():
        return 0
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8])).toordinal()
    except ValueError:
        return 0


def mean_absolute_error(actual: pd.Series, predicted: pd.Series) -> float:
    actual_values = np.asarray(actual, dtype=float)
    predicted_values = np.asarray(predicted, dtype=float)
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


def fit_regressor_with_device_fallback(
    model: lgb.LGBMRegressor,
    features: pd.DataFrame,
    target: pd.Series,
) -> None:
    try:
        model.fit(features, target)
    except LightGBMError as error:
        if should_fallback_to_cpu(error):
            model.set_params(device_type="cpu")
            model.fit(features, target)
            return
        raise


def fit_ranker_with_device_fallback(
    model: lgb.LGBMRanker,
    features: pd.DataFrame,
    target: pd.Series,
    groups: FloatArray,
) -> None:
    try:
        model.fit(features, target, group=groups)
    except LightGBMError as error:
        if should_fallback_to_cpu(error):
            model.set_params(device_type="cpu")
            model.fit(features, target, group=groups)
            return
        raise


def fit_classifier_with_device_fallback(
    model: lgb.LGBMClassifier,
    features: pd.DataFrame,
    target: pd.Series,
) -> None:
    try:
        model.fit(features, target)
    except LightGBMError as error:
        if should_fallback_to_cpu(error):
            model.set_params(device_type="cpu")
            model.fit(features, target)
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


def build_vector_neighbor_frame(df: pd.DataFrame, output: dict[str, FloatArray]) -> pd.DataFrame:
    vector_frame = pd.DataFrame(output, index=df.index)
    return pd.concat(
        [
            df.drop(columns=[column for column in VECTOR_NEIGHBOR_FEATURE_COLUMNS if column in df.columns]),
            vector_frame,
        ],
        axis=1,
    )


def import_mlx_core() -> object:
    return importlib.import_module("mlx.core")


def import_mlx_nn() -> object:
    return importlib.import_module("mlx.nn")


def import_mlx_optimizers() -> object:
    return importlib.import_module("mlx.optimizers")


def add_derived_vector_neighbor_features_sklearn(df: pd.DataFrame) -> pd.DataFrame:
    output = empty_vector_neighbor_output(len(df))
    vectors = build_vector_neighbor_input(df)
    target_values = cast(
        FloatArray,
        df[TARGET_COLUMNS].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float),
    )
    date_ordinals: IntArray = np.array(
        [race_date_to_ordinal(value) for value in df["race_date"]],
        dtype=np.int64,
    )
    kyori_values = cast(
        FloatArray,
        df["kyori"].to_numpy(dtype=float),
    )
    group_columns = ["source", "keibajo_code", "track_family"]

    for _, group in df.groupby(group_columns, sort=False):
        positions = cast(IntArray, group.index.to_numpy(dtype=np.int64))
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


def add_derived_vector_neighbor_features_mlx(df: pd.DataFrame) -> pd.DataFrame:
    mx = import_mlx_core()
    mx_array = getattr(mx, "array")
    mx_sum = getattr(mx, "sum")
    output = empty_vector_neighbor_output(len(df))
    vectors = build_vector_neighbor_input(df).astype(np.float32)
    target_values = cast(
        FloatArray,
        df[TARGET_COLUMNS].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float),
    )
    date_ordinals: IntArray = np.array(
        [race_date_to_ordinal(value) for value in df["race_date"]],
        dtype=np.int64,
    )
    kyori_values = cast(FloatArray, df["kyori"].to_numpy(dtype=float))
    group_columns = ["source", "keibajo_code", "track_family"]

    for _, group in df.groupby(group_columns, sort=False):
        positions = cast(IntArray, group.index.to_numpy(dtype=np.int64))
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


def add_derived_vector_neighbor_features(df: pd.DataFrame) -> pd.DataFrame:
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
        ".pkl",
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
    cache_env_name = "PC_KEIBA_FEATURE_CACHE_DIR" if suffix == ".pkl" else "PC_KEIBA_VECTOR_CACHE_DIR"
    cache_dir = Path(os.environ.get(cache_env_name, default_cache_dir))
    return cache_dir / f"{digest}{suffix}"


def add_cached_vector_neighbor_features(df: pd.DataFrame, input_path: str) -> pd.DataFrame:
    if not should_build_vector_neighbor_features(df):
        return df
    cache_path = vector_neighbor_cache_path(input_path)
    if cache_path.exists():
        cached = np.load(cache_path)
        if cached.shape == (len(df), len(VECTOR_NEIGHBOR_FEATURE_COLUMNS)):
            output = {
                column: cast(FloatArray, cached[:, column_index])
                for column_index, column in enumerate(VECTOR_NEIGHBOR_FEATURE_COLUMNS)
            }
            return build_vector_neighbor_frame(df, output)
    df = add_derived_vector_neighbor_features(df)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(
        str(cache_path),
        df[VECTOR_NEIGHBOR_FEATURE_COLUMNS].to_numpy(dtype=float),
    )
    return df


def numeric_column_or_default(df: pd.DataFrame, column: str, default: float) -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index)
    return pd.to_numeric(df[column], errors="coerce").fillna(default)


def log_numeric_column_or_default(df: pd.DataFrame, source_column: str, default: float) -> pd.Series:
    values = numeric_column_or_default(df, source_column, default)
    return pd.Series(np.log(np.maximum(values, 1)), index=df.index)


def normalized_numeric_column_or_default(
    df: pd.DataFrame,
    source_column: str,
    default: float,
    denominator: float,
) -> pd.Series:
    return numeric_column_or_default(df, source_column, default) / denominator


def add_code_features(df: pd.DataFrame) -> pd.DataFrame:
    for source_column, target_column in CODE_FEATURE_COLUMNS.items():
        df[target_column] = numeric_column_or_default(df, source_column, 0)
    return df


def add_optional_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    for column, default in RAW_NUMERIC_DEFAULTS.items():
        df[column] = numeric_column_or_default(df, column, default)
    for source_column in ODDS_HISTORY_COLUMNS:
        target_column = source_column.replace("horse_odds", "horse_log_odds")
        df[target_column] = log_numeric_column_or_default(df, source_column, 10)
    for source_column in THREE_F_HISTORY_COLUMNS:
        target_column = source_column.replace("horse_kohan_3f", "horse_kohan_3f_norm")
        df[target_column] = normalized_numeric_column_or_default(df, source_column, 0, 60)
    df["horse_days_since_last_start_norm"] = (
        normalized_numeric_column_or_default(df, "horse_days_since_last_start", 365, 365)
    ).clip(0, 2)
    return df


def load_dataset(path: str) -> pd.DataFrame:
    cache_path = feature_cache_path(path)
    if cache_path.exists():
        return cast(pd.DataFrame, pd.read_pickle(cache_path))
    df = pd.read_csv(path, dtype={"race_date": str, "race_id": str, "horse_key": str})
    df = df.reset_index(drop=True)
    df["keibajo_code_num"] = pd.to_numeric(df["keibajo_code"], errors="coerce").fillna(0)
    df["race_bango_num"] = pd.to_numeric(df["race_bango"], errors="coerce").fillna(0)
    df["track_code_num"] = pd.to_numeric(df["track_code"], errors="coerce").fillna(0)
    df["track_family"] = (
        df["track_code"].fillna("").astype(str).str.slice(0, 1).replace("", "0").astype(float)
    )
    df = add_code_features(df)
    df["kyori"] = pd.to_numeric(df["kyori"], errors="coerce").fillna(0)
    df["shusso_tosu"] = pd.to_numeric(df["shusso_tosu"], errors="coerce").fillna(0)
    df["umaban"] = pd.to_numeric(df["umaban"], errors="coerce").fillna(0)
    df = add_optional_derived_features(df)
    df["horse_number_norm"] = df["umaban"] / df["shusso_tosu"].replace(0, np.nan)
    df["horse_number_norm"] = df["horse_number_norm"].fillna(0)
    df["tansho_ninkijun"] = pd.to_numeric(df["tansho_ninkijun"], errors="coerce").fillna(
        df["shusso_tosu"]
    )
    df["popularity_norm"] = df["tansho_ninkijun"] / df["shusso_tosu"].replace(0, np.nan)
    df["popularity_norm"] = df["popularity_norm"].fillna(1)
    df["tansho_odds"] = pd.to_numeric(df["tansho_odds"], errors="coerce").fillna(10)
    df["log_tansho_odds"] = np.log(np.maximum(df["tansho_odds"], 1))
    missing_history_columns = [column for column in HISTORY_COLUMNS if column not in df.columns]
    if missing_history_columns:
        df = pd.concat(
            [
                df,
                pd.DataFrame(0, index=df.index, columns=missing_history_columns),
            ],
            axis=1,
        )
    for column in HISTORY_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    for column in TARGET_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = add_cached_vector_neighbor_features(df, path)
    df = df.copy()
    df = add_style_features(df)
    df = add_corner_history_rank_features(df)
    df = add_race_relative_features(df)
    df = df.dropna(subset=["race_id", "race_date", *TARGET_COLUMNS])
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(cache_path)
    return df


def race_order_score(df: pd.DataFrame, target_column: str, prediction_column: str) -> float:
    scores: list[float] = []
    for _, race in df.groupby("race_id", sort=False):
        ordered_actual = cast(
            list[str],
            race.sort_values([target_column, "umaban"])["horse_key"].tolist(),
        )
        ordered_predicted = cast(
            list[str],
            race.sort_values([prediction_column, "umaban"])["horse_key"].tolist(),
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


def ranker_target(train: pd.DataFrame, target_column: str) -> pd.Series:
    actual_rank = train.groupby("race_id", sort=False)[target_column].rank(method="first", ascending=True)
    group_size = train.groupby("race_id", sort=False)["horse_key"].transform("count")
    return (group_size - actual_rank).clip(lower=0).astype(int)


def train_model(train: pd.DataFrame, target_column: str) -> lgb.LGBMRegressor:
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
    fit_regressor_with_device_fallback(model, train[FEATURE_COLUMNS], train[target_column])
    return model


def train_ranker(train: pd.DataFrame, target_column: str) -> lgb.LGBMRanker:
    ordered_train = train.sort_values(["race_id", "umaban"]).copy()
    groups = ordered_train.groupby("race_id", sort=False).size().to_numpy()
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
        ordered_train[FEATURE_COLUMNS],
        ranker_target(ordered_train, target_column),
        groups.astype(np.float64),
    )
    return model


def build_pairwise_dataset(df: pd.DataFrame, target_column: str) -> tuple[pd.DataFrame, pd.Series]:
    feature_blocks: list[pd.DataFrame] = []
    labels: list[pd.Series] = []
    for _, race in df.groupby("race_id", sort=False):
        race = race.loc[:, ~race.columns.duplicated()].reset_index(drop=True)
        size = len(race)
        if size <= 1:
            continue
        left_indices, right_indices = np.triu_indices(size, k=1)
        left = race.iloc[left_indices].reset_index(drop=True)
        right = race.iloc[right_indices].reset_index(drop=True)
        diff = left[PAIRWISE_BASE_COLUMNS].reset_index(drop=True) - right[PAIRWISE_BASE_COLUMNS].reset_index(drop=True)
        diff.columns = [f"{column}_diff" for column in PAIRWISE_BASE_COLUMNS]
        abs_diff = diff.abs()
        abs_diff.columns = [f"{column}_abs_diff" for column in PAIRWISE_BASE_COLUMNS]
        feature_blocks.append(pd.concat([diff, abs_diff], axis=1))
        labels.append((left[target_column].reset_index(drop=True) < right[target_column].reset_index(drop=True)).astype(int))
    if not feature_blocks:
        return pd.DataFrame(columns=PAIRWISE_FEATURE_COLUMNS), pd.Series(dtype=int)
    return pd.concat(feature_blocks, ignore_index=True), pd.concat(labels, ignore_index=True)


def train_pairwise_model(train: pd.DataFrame, target_column: str) -> lgb.LGBMClassifier:
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
    fit_classifier_with_device_fallback(model, pair_features[PAIRWISE_FEATURE_COLUMNS], labels)
    return model


def train_stacking_model(
    train: pd.DataFrame,
    target_column: str,
    stacking_features: pd.DataFrame,
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


def neural_sequence_tensor(df: pd.DataFrame) -> FloatArray:
    working = df.copy()
    working["__original_index"] = np.arange(len(working))
    working["__date_ordinal"] = [race_date_to_ordinal(value) for value in working["race_date"]]
    working["kyori"] = pd.to_numeric(working["kyori"], errors="coerce").fillna(0)
    working = working.sort_values(["horse_key", "__date_ordinal", "race_id"], kind="mergesort")
    unique_working = working.loc[:, ~working.columns.duplicated()]
    numeric_history = pd.DataFrame(
        {column: unique_working[column] if column in unique_working.columns else 0 for column in NEURAL_HISTORY_COLUMNS},
        index=working.index,
    ).apply(pd.to_numeric, errors="coerce").fillna(0)
    static_values = working[NEURAL_SEQUENCE_STATIC_COLUMNS].to_numpy(dtype=np.float32)
    sequences = np.zeros(
        (len(working), NEURAL_HISTORY_LAGS, len(NEURAL_HISTORY_COLUMNS) + len(NEURAL_SEQUENCE_STATIC_COLUMNS) + 2),
        dtype=np.float32,
    )
    grouped_history = numeric_history.groupby(working["horse_key"], sort=False)
    grouped_dates = working["__date_ordinal"].groupby(working["horse_key"], sort=False)
    grouped_distances = working["kyori"].groupby(working["horse_key"], sort=False)
    current_dates = working["__date_ordinal"].to_numpy(dtype=float)
    current_distances = working["kyori"].to_numpy(dtype=float)
    for lag in range(1, NEURAL_HISTORY_LAGS + 1):
        lag_index = lag - 1
        shifted_history = grouped_history.shift(lag).fillna(0).to_numpy(dtype=np.float32)
        shifted_dates = grouped_dates.shift(lag).fillna(0).to_numpy(dtype=float)
        shifted_distances = grouped_distances.shift(lag).to_numpy(dtype=float)
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
    inverse_order = np.argsort(working["__original_index"].to_numpy(dtype=np.int64))
    return cast(FloatArray, sequences[inverse_order])


class CornerLstmModel(import_mlx_nn().Module):  # pragma: no cover
    def __init__(self, input_size: int, hidden_size: int) -> None:
        super().__init__()
        nn = import_mlx_nn()
        self.lstm = nn.LSTM(input_size, hidden_size)
        self.output = nn.Linear(hidden_size, 1)

    def __call__(self, x):
        hidden, _cell = self.lstm(x)
        return self.output(hidden[:, -1, :]).squeeze(-1)


class CornerTransformerModel(import_mlx_nn().Module):  # pragma: no cover
    def __init__(self, input_size: int, hidden_size: int) -> None:
        super().__init__()
        nn = import_mlx_nn()
        self.input = nn.Linear(input_size, hidden_size)
        self.encoder = nn.TransformerEncoder(
            num_layers=2,
            dims=hidden_size,
            num_heads=4,
            mlp_dims=hidden_size * 2,
            dropout=0.0,
        )
        self.output = nn.Linear(hidden_size, 1)

    def __call__(self, x):
        encoded = self.encoder(self.input(x), None)
        return self.output(encoded.mean(axis=1)).squeeze(-1)


def train_neural_corner_model(
    model,
    train_sequences: FloatArray,
    target: pd.Series,
    epochs: int,
) -> object:  # pragma: no cover
    mx = import_mlx_core()
    nn = import_mlx_nn()
    optimizers = import_mlx_optimizers()
    optimizer = optimizers.Adam(learning_rate=0.002)
    target_values = target.to_numpy(dtype=np.float32)
    batch_size = 4096

    def loss_fn(model_arg, batch_x, batch_y):
        prediction = model_arg(batch_x)
        return nn.losses.mse_loss(prediction, batch_y)

    loss_and_grad = nn.value_and_grad(model, loss_fn)
    for epoch in range(epochs):
        rng = np.random.default_rng(20260514 + epoch)
        for start in range(0, len(train_sequences), batch_size):
            batch_index = rng.permutation(len(train_sequences))[start : start + batch_size]
            batch_x = mx.array(train_sequences[batch_index])
            batch_y = mx.array(target_values[batch_index])
            loss, gradients = loss_and_grad(model, batch_x, batch_y)
            optimizer.update(model, gradients)
            mx.eval(model.parameters(), optimizer.state, loss)
    return model


def predict_neural_corner_model(model, sequences: FloatArray) -> pd.Series:  # pragma: no cover
    mx = import_mlx_core()
    predictions: list[np.ndarray] = []
    for start in range(0, len(sequences), 8192):
        batch = mx.array(sequences[start : start + 8192])
        predictions.append(np.asarray(model(batch), dtype=float))
    return clipped_prediction(pd.Series(np.concatenate(predictions)))


def train_lstm_model(train_sequences: FloatArray, target: pd.Series) -> object:  # pragma: no cover
    return train_neural_corner_model(CornerLstmModel(train_sequences.shape[2], 32), train_sequences, target, 2)


def train_transformer_model(train_sequences: FloatArray, target: pd.Series) -> object:  # pragma: no cover
    return train_neural_corner_model(CornerTransformerModel(train_sequences.shape[2], 32), train_sequences, target, 2)


def apply_pairwise_model(test: pd.DataFrame, model: lgb.LGBMClassifier, target_column: str) -> pd.Series:
    score_values = {int(index): 0.0 for index in test.index}
    for _, race in test.groupby("race_id", sort=False):
        race = race.loc[:, ~race.columns.duplicated()].reset_index()
        size = len(race)
        if size <= 1:
            continue
        left_indices, right_indices = np.triu_indices(size, k=1)
        left = race.iloc[left_indices].reset_index(drop=True)
        right = race.iloc[right_indices].reset_index(drop=True)
        diff = left[PAIRWISE_BASE_COLUMNS].reset_index(drop=True) - right[PAIRWISE_BASE_COLUMNS].reset_index(drop=True)
        diff.columns = [f"{column}_diff" for column in PAIRWISE_BASE_COLUMNS]
        abs_diff = diff.abs()
        abs_diff.columns = [f"{column}_abs_diff" for column in PAIRWISE_BASE_COLUMNS]
        pair_features = pd.concat([diff, abs_diff], axis=1)
        probabilities = np.asarray(
            model.predict_proba(pair_features[PAIRWISE_FEATURE_COLUMNS]),
            dtype=float,
        )[:, 1]
        for pair_index, probability in enumerate(probabilities):
            left_index = int(left["index"].iloc[pair_index])
            right_index = int(right["index"].iloc[pair_index])
            score_values[left_index] += float(probability)
            score_values[right_index] += 1 - float(probability)
    scores = pd.Series(score_values, index=test.index, dtype=float)
    return (
        1
        - scores
        / test.groupby("race_id", sort=False)["horse_key"].transform("count").sub(1).replace(0, np.nan)
    ).fillna(0.5)


def normalized_rank_prediction(df: pd.DataFrame, score_column: str) -> pd.Series:
    rank = df.groupby("race_id", sort=False)[score_column].rank(method="first", ascending=False)
    group_size = df.groupby("race_id", sort=False)[score_column].transform("count")
    return ((rank - 1) / (group_size - 1).replace(0, np.nan)).fillna(0.5)


def normalized_position_rank_prediction(df: pd.DataFrame, values: pd.Series) -> pd.Series:
    scratch_column = "__position_rank_source"
    df[scratch_column] = values
    try:
        rank = df.groupby("race_id", sort=False)[scratch_column].rank(method="first", ascending=True)
        group_size = df.groupby("race_id", sort=False)[scratch_column].transform("count")
        return ((rank - 1) / (group_size - 1).replace(0, np.nan)).fillna(0.5)
    finally:
        del df[scratch_column]


def clipped_prediction(values: pd.Series) -> pd.Series:
    return pd.Series(np.clip(np.asarray(values, dtype=float), 0, 1), index=values.index)


def blended_prediction(base: pd.Series, overlay: pd.Series, alpha: float) -> pd.Series:
    return clipped_prediction(base * (1 - alpha) + overlay * alpha)


def column_or_prediction_fallback(df: pd.DataFrame, column: str, fallback_column: str) -> pd.Series:
    if column in df.columns:
        return df[column]
    return df[fallback_column]


def stacking_feature_frame(
    df: pd.DataFrame,
    target_column: str,
    regression_column: str,
    ranker_column: str,
    pairwise_column: str,
) -> pd.DataFrame:
    structural_candidates = structural_candidate_predictions(df, target_column, regression_column, pairwise_column)
    features = {
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
    return pd.DataFrame(features, index=df.index).replace([np.inf, -np.inf], np.nan).fillna(0.5)


def structural_candidate_predictions(
    test: pd.DataFrame,
    target_column: str,
    regression_column: str,
    pairwise_column: str,
    stacked_column: str | None = None,
    lstm_column: str | None = None,
    transformer_column: str | None = None,
) -> dict[str, pd.Series]:
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
    test: pd.DataFrame,
    horse_recent: pd.Series,
    horse_avg: pd.Series,
    horse_last: pd.Series,
) -> dict[str, pd.Series]:
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
    test: pd.DataFrame,
    target_column: str,
    prediction: pd.Series,
    scratch_column: str,
) -> float:
    test[scratch_column] = prediction
    try:
        return race_order_score(test, target_column, scratch_column)
    finally:
        del test[scratch_column]


def update_best_prediction(
    test: pd.DataFrame,
    target_column: str,
    candidate: pd.Series,
    score_key: str,
    current: tuple[float, float, pd.Series],
    scores: dict[str, float],
) -> tuple[float, float, pd.Series]:
    score = score_prediction(test, target_column, candidate, f"candidate_{target_column}_{score_key}")
    scores[score_key] = score
    if score <= current[1]:
        return current
    return -1.0, score, candidate.copy()


def rank_candidate_predictions(
    test: pd.DataFrame,
    structural_candidates: dict[str, pd.Series],
    regression_column: str,
    ranker_column: str,
) -> dict[str, pd.Series]:
    candidates = {
        "regression_rank": normalized_position_rank_prediction(test, test[regression_column]),
        "ranker_rank": clipped_prediction(test[ranker_column]),
    }
    for name, prediction in structural_candidates.items():
        candidates[f"{name}_rank"] = normalized_position_rank_prediction(test, prediction)
    return candidates


def search_rank_pair_ensembles(
    test: pd.DataFrame,
    target_column: str,
    candidates: dict[str, pd.Series],
    current: tuple[float, float, pd.Series],
    scores: dict[str, float],
) -> tuple[float, float, pd.Series]:
    names = list(candidates)
    best = current
    for left_index, left_name in enumerate(names):
        for right_name in names[left_index + 1 :]:
            for weight in RANK_ENSEMBLE_WEIGHTS:
                prediction = blended_prediction(candidates[left_name], candidates[right_name], weight)
                score_key = f"rankmix_{left_name}_{right_name}_{weight}"
                best = update_best_prediction(test, target_column, prediction, score_key, best, scores)
    return best


def search_rank_triple_ensembles(
    test: pd.DataFrame,
    target_column: str,
    candidates: dict[str, pd.Series],
    current: tuple[float, float, pd.Series],
    scores: dict[str, float],
) -> tuple[float, float, pd.Series]:
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
                for left_weight, middle_weight, right_weight in RANK_ENSEMBLE_TRIPLES:
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
    test: pd.DataFrame,
    target_column: str,
    structural_candidates: dict[str, pd.Series],
    regression_column: str,
    ranker_column: str,
    current: tuple[float, float, pd.Series],
    scores: dict[str, float],
) -> tuple[float, float, pd.Series]:
    candidates = rank_candidate_predictions(test, structural_candidates, regression_column, ranker_column)
    best = current
    for name, prediction in candidates.items():
        best = update_best_prediction(test, target_column, prediction, name, best, scores)
    best = search_rank_pair_ensembles(test, target_column, candidates, best, scores)
    return search_rank_triple_ensembles(test, target_column, candidates, best, scores)


def majority_vote_prediction(test: pd.DataFrame, candidates: dict[str, pd.Series]) -> pd.Series:
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
    vote_sum = pd.Series(0.0, index=test.index)
    for name in vote_names:
        vote_sum += normalized_position_rank_prediction(test, candidates[name])
    if not vote_names:
        return pd.Series(0.5, index=test.index)
    return clipped_prediction(vote_sum / len(vote_names))


def choose_ensemble_prediction(
    test: pd.DataFrame,
    target_column: str,
    regression_column: str,
    ranker_column: str,
    pairwise_column: str,
    stacked_column: str | None = None,
    lstm_column: str | None = None,
    transformer_column: str | None = None,
) -> tuple[pd.Series, float, dict[str, float]]:
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

    for alpha in REGRESSION_RANKER_ALPHAS:
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
            best_prediction = candidate.copy()

    for alpha in RANK_PAIRWISE_ALPHAS:
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
            best_prediction = candidate.copy()

    for name, structural_prediction in structural_candidates.items():
        base_prediction = clipped_prediction(structural_prediction.fillna(test[regression_column]))
        score = score_prediction(test, target_column, base_prediction, f"structural_{target_column}_{name}")
        scores[name] = score
        if score > best_score:
            best_alpha = 1.0
            best_score = score
            best_prediction = base_prediction.copy()
        for alpha in STRUCTURAL_BLEND_ALPHAS:
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
                best_prediction = candidate.copy()
        if name in {"lstm", "transformer", "neural_average"}:
            for alpha in NEURAL_BLEND_ALPHAS:
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
                    best_prediction = candidate.copy()
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
    train_positions = np.flatnonzero(train_mask.to_numpy(dtype=bool))
    test_positions = np.flatnonzero(test_mask.to_numpy(dtype=bool))
    train = df[train_mask].copy()
    test = df[test_mask].copy()
    if train.empty or test.empty:
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
        "test_rows": int(len(test)),
        "test_to_date": test_to_date,
        "train_rows": int(len(train)),
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
        train[regression_column] = np.clip(
            np.asarray(model.predict(train[FEATURE_COLUMNS]), dtype=float),
            0,
            1,
        )
        train[ranker_score_column] = np.asarray(ranker.predict(train[FEATURE_COLUMNS]), dtype=float)
        train[ranker_prediction_column] = normalized_rank_prediction(train, ranker_score_column)
        train[pairwise_prediction_column] = train[regression_column]
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
        lstm_model = train_lstm_model(train_sequences, train[target_column])
        transformer_model = train_transformer_model(train_sequences, train[target_column])
        model.booster_.save_model(str(model_dir / f"{target_column}.txt"))
        ranker.booster_.save_model(str(model_dir / f"{target_column}.ranker.txt"))
        pairwise_model.booster_.save_model(str(model_dir / f"{target_column}.pairwise.txt"))
        stacker.booster_.save_model(str(model_dir / f"{target_column}.stacker.txt"))
        test[regression_column] = np.clip(
            np.asarray(model.predict(test[FEATURE_COLUMNS]), dtype=float),
            0,
            1,
        )
        test[ranker_score_column] = np.asarray(ranker.predict(test[FEATURE_COLUMNS]), dtype=float)
        test[ranker_prediction_column] = normalized_rank_prediction(test, ranker_score_column)
        test[pairwise_prediction_column] = apply_pairwise_model(test, pairwise_model, target_column)
        test[stacked_prediction_column] = np.clip(
            np.asarray(
                stacker.predict(
                    stacking_feature_frame(
                        test,
                        target_column,
                        regression_column,
                        ranker_prediction_column,
                        pairwise_prediction_column,
                    ),
                ),
                dtype=float,
            ),
            0,
            1,
        )
        test[lstm_prediction_column] = predict_neural_corner_model(lstm_model, test_sequences).to_numpy(dtype=float)
        test[transformer_prediction_column] = predict_neural_corner_model(
            transformer_model,
            test_sequences,
        ).to_numpy(dtype=float)
        prediction, alpha, alpha_scores = choose_ensemble_prediction(
            test,
            target_column,
            regression_column,
            ranker_prediction_column,
            pairwise_prediction_column,
            stacked_prediction_column,
            lstm_prediction_column,
            transformer_prediction_column,
        )
        test[prediction_column] = prediction
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
    test[output_columns].to_csv(args.predictions_output, index=False)
    Path(args.metrics_output).parent.mkdir(parents=True, exist_ok=True)
    cast(dict[str, object], metrics["timing"])["total_seconds"] = perf_counter() - total_started_at
    Path(args.metrics_output).write_text(json.dumps(metrics, ensure_ascii=False, indent=2), "utf-8")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
