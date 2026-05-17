#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Race-grouped padded tensor builder for the Race Set Transformer.

Reads the parquet dataset produced by finish_position_features_duckdb.py,
groups rows by race_id, pads each race up to MAX_RUNNERS, and emits numeric/
categorical arrays plus mask and label arrays in a shape suitable for the
MLX transformer model.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from finish_position_lightgbm import (
    CATEGORICAL_FEATURE_COLUMNS,
    LABEL_COLUMNS,
    META_COLUMNS,
    resolve_feature_columns,
)

MAX_RUNNERS = 18
PAD_CATEGORICAL_INDEX = 0
UNKNOWN_CATEGORICAL_INDEX = 0
PAD_FINISH_POSITION = 0
DEFAULT_UMABAN_PAD = 0
TRANSFORMER_FEATURE_SCHEMA_VERSION = "v1"

FloatArray = NDArray[np.float32]
IntArray = NDArray[np.int32]
BoolArray = NDArray[np.bool_]


class NumericStat(TypedDict):
    mean: float
    std: float


class NormalizationStats(TypedDict):
    numeric_columns: list[str]
    numeric_mean: list[float]
    numeric_std: list[float]
    categorical_columns: list[str]
    categorical_vocab: dict[str, list[str]]


class RaceBatchArrays(TypedDict):
    numeric_features: FloatArray
    categorical_indices: IntArray
    umaban: IntArray
    finish_position: FloatArray
    mask: BoolArray
    race_ids: list[str]
    ketto_toroku_bango: list[list[str]]


@dataclass(frozen=True)
class FeatureColumns:
    numeric: list[str]
    categorical: list[str]


def resolve_transformer_feature_columns(df_columns: list[str]) -> FeatureColumns:
    all_features = resolve_feature_columns(df_columns)
    categorical = [c for c in all_features if c in CATEGORICAL_FEATURE_COLUMNS]
    numeric = [c for c in all_features if c not in CATEGORICAL_FEATURE_COLUMNS]
    return FeatureColumns(numeric=numeric, categorical=categorical)


def fit_normalization_stats(df: pd.DataFrame, columns: FeatureColumns) -> NormalizationStats:
    numeric_mean: list[float] = []
    numeric_std: list[float] = []
    for column in columns.numeric:
        series = pd.to_numeric(df[column], errors="coerce")
        mean = float(series.mean()) if series.notna().any() else 0.0
        std = float(series.std(ddof=0)) if series.notna().any() else 1.0
        if not np.isfinite(std) or std < 1e-6:
            std = 1.0
        if not np.isfinite(mean):
            mean = 0.0
        numeric_mean.append(mean)
        numeric_std.append(std)
    vocab: dict[str, list[str]] = {}
    for column in columns.categorical:
        unique_values = sorted({str(v) for v in df[column].dropna().unique() if str(v).strip() != ""})
        vocab[column] = unique_values
    return {
        "numeric_columns": columns.numeric,
        "numeric_mean": numeric_mean,
        "numeric_std": numeric_std,
        "categorical_columns": columns.categorical,
        "categorical_vocab": vocab,
    }


def categorical_vocab_size(stats: NormalizationStats, column: str) -> int:
    return len(stats["categorical_vocab"][column]) + 1


def _encode_categorical_value(stats: NormalizationStats, column: str, value: object) -> int:
    if value is None:
        return UNKNOWN_CATEGORICAL_INDEX
    raw = str(value).strip()
    if raw == "" or raw == "nan":
        return UNKNOWN_CATEGORICAL_INDEX
    vocab = stats["categorical_vocab"][column]
    try:
        return vocab.index(raw) + 1
    except ValueError:
        return UNKNOWN_CATEGORICAL_INDEX


def _normalize_numeric(stats: NormalizationStats, df: pd.DataFrame) -> FloatArray:
    out = np.zeros((len(df), len(stats["numeric_columns"])), dtype=np.float32)
    for col_idx, column in enumerate(stats["numeric_columns"]):
        series = pd.to_numeric(df[column], errors="coerce")
        mean = stats["numeric_mean"][col_idx]
        std = stats["numeric_std"][col_idx]
        normalized = ((series - mean) / std).to_numpy(dtype=np.float32, na_value=0.0)
        out[:, col_idx] = normalized
    return out


def _encode_categorical_frame(stats: NormalizationStats, df: pd.DataFrame) -> IntArray:
    out = np.zeros((len(df), len(stats["categorical_columns"])), dtype=np.int32)
    for col_idx, column in enumerate(stats["categorical_columns"]):
        values = df[column].tolist()
        out[:, col_idx] = [_encode_categorical_value(stats, column, value) for value in values]
    return out


def _read_umaban_int(df: pd.DataFrame) -> IntArray:
    series = pd.to_numeric(df["umaban"], errors="coerce")
    return series.to_numpy(dtype=np.int32, na_value=DEFAULT_UMABAN_PAD)


def _read_finish_position_float(df: pd.DataFrame) -> FloatArray:
    series = pd.to_numeric(df["finish_position"], errors="coerce")
    return series.to_numpy(dtype=np.float32, na_value=float(PAD_FINISH_POSITION))


def build_race_batches(df: pd.DataFrame, stats: NormalizationStats) -> RaceBatchArrays:
    sorted_df = df.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    sorted_df = sorted_df[sorted_df["race_id"].notna()].copy()
    race_ids: list[str] = sorted_df["race_id"].astype(str).unique().tolist()
    num_races = len(race_ids)
    numeric_cols = stats["numeric_columns"]
    categorical_cols = stats["categorical_columns"]
    numeric_features = np.zeros((num_races, MAX_RUNNERS, len(numeric_cols)), dtype=np.float32)
    categorical_indices = np.zeros((num_races, MAX_RUNNERS, len(categorical_cols)), dtype=np.int32)
    umaban_arr = np.zeros((num_races, MAX_RUNNERS), dtype=np.int32)
    finish_position_arr = np.zeros((num_races, MAX_RUNNERS), dtype=np.float32)
    mask = np.zeros((num_races, MAX_RUNNERS), dtype=np.bool_)
    ketto_per_race: list[list[str]] = [["" for _ in range(MAX_RUNNERS)] for _ in range(num_races)]
    race_index = {race_id: idx for idx, race_id in enumerate(race_ids)}
    numeric_block = _normalize_numeric(stats, sorted_df)
    categorical_block = _encode_categorical_frame(stats, sorted_df)
    umaban_block = _read_umaban_int(sorted_df)
    finish_block = _read_finish_position_float(sorted_df)
    horse_ids = sorted_df["ketto_toroku_bango"].astype(str).tolist()
    race_id_values = sorted_df["race_id"].astype(str).tolist()
    counters = np.zeros(num_races, dtype=np.int32)
    for row_idx, raw_race_id in enumerate(race_id_values):
        race_idx = race_index[raw_race_id]
        slot = int(counters[race_idx])
        if slot >= MAX_RUNNERS:
            continue
        numeric_features[race_idx, slot] = numeric_block[row_idx]
        categorical_indices[race_idx, slot] = categorical_block[row_idx]
        umaban_arr[race_idx, slot] = umaban_block[row_idx]
        finish_position_arr[race_idx, slot] = finish_block[row_idx]
        mask[race_idx, slot] = True
        ketto_per_race[race_idx][slot] = horse_ids[row_idx]
        counters[race_idx] = slot + 1
    return {
        "numeric_features": numeric_features,
        "categorical_indices": categorical_indices,
        "umaban": umaban_arr,
        "finish_position": finish_position_arr,
        "mask": mask,
        "race_ids": race_ids,
        "ketto_toroku_bango": ketto_per_race,
    }


def iter_race_batches(
    arrays: RaceBatchArrays,
    batch_size: int,
    shuffle: bool,
    rng: np.random.Generator | None = None,
) -> list[RaceBatchArrays]:
    num_races = len(arrays["race_ids"])
    indices = np.arange(num_races)
    if shuffle:
        generator = rng if rng is not None else np.random.default_rng()
        generator.shuffle(indices)
    batches: list[RaceBatchArrays] = []
    for start in range(0, num_races, batch_size):
        slice_idx = indices[start : start + batch_size]
        batches.append(_select_indices(arrays, slice_idx))
    return batches


def _select_indices(arrays: RaceBatchArrays, indices: NDArray[np.int_]) -> RaceBatchArrays:
    return {
        "numeric_features": arrays["numeric_features"][indices],
        "categorical_indices": arrays["categorical_indices"][indices],
        "umaban": arrays["umaban"][indices],
        "finish_position": arrays["finish_position"][indices],
        "mask": arrays["mask"][indices],
        "race_ids": [arrays["race_ids"][int(i)] for i in indices],
        "ketto_toroku_bango": [arrays["ketto_toroku_bango"][int(i)] for i in indices],
    }


__all__ = (
    "MAX_RUNNERS",
    "PAD_CATEGORICAL_INDEX",
    "UNKNOWN_CATEGORICAL_INDEX",
    "TRANSFORMER_FEATURE_SCHEMA_VERSION",
    "CATEGORICAL_FEATURE_COLUMNS",
    "LABEL_COLUMNS",
    "META_COLUMNS",
    "FeatureColumns",
    "NormalizationStats",
    "NumericStat",
    "RaceBatchArrays",
    "build_race_batches",
    "categorical_vocab_size",
    "fit_normalization_stats",
    "iter_race_batches",
    "resolve_transformer_feature_columns",
)
