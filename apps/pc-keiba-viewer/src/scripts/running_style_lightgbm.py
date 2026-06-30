#!/usr/bin/env python3
# pyright: reportUnknownParameterType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportMissingParameterType=false
"""Multiclass softmax LightGBM for running-style prediction (nige/senkou/sashi/oikomi).

Trains a single 4-class softmax head on target_running_style_class with
inverse-frequency sample weights (class imbalance correction). Reads the
Phase A parquet produced by finish_position_features_duckdb.py.

Training range default is 21 years (2005-2026 for NAR, 2006-2026 for JRA),
matching the bucket eval range. The 2026 partial year is held out as the
production early-stopping validation slice via --valid-start-date.

Run with:
  cd src/scripts && ../../.venv/bin/python -m running_style_lightgbm walk-forward \\
    --csv ../../tmp/finish-position-features-parquet-jra-v4 \\
    --train-start-date 20050101 \\
    --validation-years 2024,2025 \\
    --output-predictions-dir ../../tmp/finish-position-eval/predictions-jra/running-style-lgbm
"""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, TypedDict

import lightgbm as lgb
import numpy as np
import polars as pl

from learning.feature_selection_policy import (
    compute_feature_set_hash,
    normalize_feature_names,
    resolve_feature_columns_for_target,
)
from running_style_field_features import (
    FIELD_FEATURE_COLUMNS,
    enrich_dataframe_with_field_features,
)

if TYPE_CHECKING:
    import pandas as pd

META_COLUMNS: tuple[str, ...] = (
    "source",
    "race_date",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
    "bamei",
    "umaban",
    "category",
    "race_id",
    "race_year",
    "feature_schema_version",
)
LABEL_COLUMNS: tuple[str, ...] = (
    "finish_position",
    "finish_norm",
    "target_corner_1_norm",
    "target_corner_3_norm",
    "target_corner_4_norm",
    "target_running_style_class",
)
CATEGORICAL_FEATURE_COLUMNS: tuple[str, ...] = (
    "track_code",
    "grade_code",
    "keibajo_code",
    "kyori_band",
    "season_band",
    "is_newcomer_race",
    "tenko_code",
    "babajotai_code_shiba",
    "babajotai_code_dirt",
    "seibetsu_code",
)

TARGET_COLUMN = "target_running_style_class"
NUM_CLASSES = 4
CLASS_LABELS: tuple[str, str, str, str] = ("nige", "senkou", "sashi", "oikomi")
PROBABILITY_COLUMNS: tuple[str, str, str, str] = (
    "p_nige",
    "p_senkou",
    "p_sashi",
    "p_oikomi",
)

# rs_p_* columns are running-style probability predictions derived from the target —
# including them causes label leakage. Exclude explicitly from all training feature sets.
LEAK_COLUMNS: tuple[str, str, str, str] = (
    "rs_p_nige",
    "rs_p_senkou",
    "rs_p_sashi",
    "rs_p_oikomi",
)

CELL_DERIVED_COLUMNS: tuple[str, ...] = (
    "__rs_cell_category",
    "__rs_cell_surface",
    "__rs_cell_distance_band",
    "__rs_cell_season",
    "__rs_cell_class",
    "__rs_cell_venue",
    "__rs_cell_subgroup",
)
RUNNING_STYLE_DEFAULT_VARIANT_ID = "latest"
BAN_EI_RUNNING_STYLE_KEIBAJO_CODES: frozenset[str] = frozenset({"65", "83"})
DISTANCE_SPRINT_MAX = 1200
DISTANCE_MILE_MAX = 1600
DISTANCE_INTERMEDIATE_MAX = 2000
DISTANCE_LONG_MAX = 2400
DEFAULT_CELL_MIN_TRAIN_ROWS = 200
DEFAULT_CELL_MIN_VALID_ROWS = 50
DEFAULT_CELL_MIN_CLASSES = 2
DEFAULT_CELL_ROUTING_DIMENSIONS: tuple[str, ...] = (
    "class",
    "distance_band",
    "season",
    "surface",
    "venue",
    "subgroup",
)
CELL_MODEL_KEY_ROOT = "running-style/models"
CELL_VARIANT_ID_PATTERN = re.compile(r"[^0-9A-Za-z_.-]+")

DEFAULT_NUM_LEAVES = 63
DEFAULT_LEARNING_RATE = 0.05
DEFAULT_MIN_CHILD_SAMPLES = 30
DEFAULT_LAMBDA_L1 = 0.1
DEFAULT_LAMBDA_L2 = 0.1
DEFAULT_FEATURE_FRACTION = 0.8
DEFAULT_BAGGING_FRACTION = 0.8
DEFAULT_BAGGING_FREQ = 1
DEFAULT_NUM_ITERATIONS = 2000
DEFAULT_EARLY_STOPPING_ROUNDS = 100
DEFAULT_VERBOSE_EVAL = 0

# 21-year training range: 2005-2026 inclusive. NAR feature parquet starts
# in 2005, JRA starts in 2006 (filtered at category level). 2026 is the
# latest backfilled year and is sliced off as the early-stop holdout via
# DEFAULT_VALID_START_DATE so the production model still sees all 21 years.
DEFAULT_TRAIN_START_DATE = "20050101"
DEFAULT_TRAIN_END_DATE = "20261231"
DEFAULT_VALID_START_DATE = "20260101"


class TrainingParams(TypedDict):
    num_leaves: int
    learning_rate: float
    min_child_samples: int
    lambda_l1: float
    lambda_l2: float
    feature_fraction: float
    bagging_fraction: float
    bagging_freq: int
    num_iterations: int
    early_stopping_rounds: int


class FoldMetrics(TypedDict):
    validation_year: int
    train_rows: int
    valid_rows: int
    accuracy: float
    macro_f1: float
    per_class_precision: dict[str, float]
    per_class_recall: dict[str, float]
    per_class_support: dict[str, int]


class WalkForwardEvalMetrics(TypedDict):
    holdout_year: int
    train_start_date: str
    train_end_date: str
    train_rows: int
    valid_rows: int
    precision_nige: float
    recall_nige: float
    log_loss_nige: float
    multi_log_loss: float
    accuracy: float


class RunningStyleCellMetrics(TypedDict):
    prediction_count: int
    top2_hit_count: int
    accuracy: float
    macro_f1: float
    multi_log_loss: float
    top2_accuracy: float
    race_level: RunningStyleRaceLevelMetrics
    per_class_accuracy: dict[str, float]
    per_class_f1: dict[str, float]
    per_class_precision: dict[str, float]
    per_class_recall: dict[str, float]
    per_class_support: dict[str, int]
    predicted_class_support: dict[str, int]
    confusion_matrix: dict[str, dict[str, int]]
    per_class_log_loss_sum: dict[str, float]
    per_class_log_loss_count: dict[str, int]
    per_class_log_loss: dict[str, float]


class RunningStyleRaceLevelMetrics(TypedDict):
    race_count: int
    style_distribution_mae: float
    style_count_mae: dict[str, float]
    style_count_bias: dict[str, float]
    nige_count_mae: float
    front_group_count_mae: float
    corner_rank_spearman: float
    finish_weighted_accuracy: float
    top1_finish_style_accuracy: float
    top3_finish_style_accuracy: float


class RunningStyleCellAdoptionMetrics(TypedDict):
    prediction_target: str
    feature_set_hash: str
    category: str
    surface: str
    distance_band: str
    class_label: str
    season: str
    venue: str
    subgroup: str | None
    race_count: int
    prediction_count: int
    ndcg_at_3: float
    top1_accuracy: float
    place2_accuracy: float
    place3_accuracy: float
    place4_accuracy: float
    place5_accuracy: float
    place6_accuracy: float
    top3_box_accuracy: float
    accuracy_vector: list[float]
    cell_vector: list[str]
    metric_mapping: dict[str, str]


@dataclass(frozen=True, order=True)
class RunningStyleCellKey:
    category: str
    class_label: str
    distance_band: str | None
    season: str | None
    surface: str | None
    venue: str
    subgroup: str | None


@dataclass(frozen=True)
class CellFeatureSelectionRule:
    category: str
    conditions: tuple[tuple[str, tuple[str, ...]], ...]
    feature_names: tuple[str, ...]
    feature_set_hash: str


DEFAULT_WALK_FORWARD_WINDOWS = "2023,2024,2025,2026"
WALK_FORWARD_PRECISION_GAP_THRESHOLD_PP = 30.0
LOG_LOSS_EPS = 1e-15

# Per-class multipliers for the "balanced2" weight scheme.
# Applied on top of inverse-frequency base weights.
# nige×0.65 slightly under-weights the front-runner class;
# oikomi×0.85 slightly under-weights the closer class.
BALANCED2_WEIGHT_MULTIPLIERS: tuple[float, float, float, float] = (0.65, 1.0, 1.0, 0.85)


def default_training_params() -> TrainingParams:
    return {
        "num_leaves": DEFAULT_NUM_LEAVES,
        "learning_rate": DEFAULT_LEARNING_RATE,
        "min_child_samples": DEFAULT_MIN_CHILD_SAMPLES,
        "lambda_l1": DEFAULT_LAMBDA_L1,
        "lambda_l2": DEFAULT_LAMBDA_L2,
        "feature_fraction": DEFAULT_FEATURE_FRACTION,
        "bagging_fraction": DEFAULT_BAGGING_FRACTION,
        "bagging_freq": DEFAULT_BAGGING_FREQ,
        "num_iterations": DEFAULT_NUM_ITERATIONS,
        "early_stopping_rounds": DEFAULT_EARLY_STOPPING_ROUNDS,
    }


def resolve_feature_columns(df_columns: list[str]) -> list[str]:
    return resolve_feature_columns_for_target(df_columns, "running_style")


def detect_categorical_features(feature_columns: list[str]) -> list[str]:
    return [
        column for column in feature_columns if column in CATEGORICAL_FEATURE_COLUMNS
    ]


def _read_partitioned_parquet(child: Path) -> pl.DataFrame:
    frame = pl.read_parquet(child)
    if "race_year" not in frame.columns:
        year_token = child.parent.name
        if year_token.startswith("race_year="):
            frame = frame.with_columns(
                pl.lit(int(year_token.split("=", 1)[1])).alias("race_year")
            )
    return frame


def load_dataset_parquet(path: Path) -> pl.DataFrame:
    if path.is_dir():
        partitioned = sorted(path.glob("race_year=*/*.parquet"))
        if partitioned:
            return pl.concat(
                [_read_partitioned_parquet(child) for child in partitioned],
                how="diagonal_relaxed",
            )
        flat = sorted(path.glob("*.parquet"))
        if flat:
            return pl.concat(
                [pl.read_parquet(child) for child in flat], how="diagonal_relaxed"
            )
        raise ValueError(f"No parquet files found under {path}")
    return pl.read_parquet(path)


def filter_labeled_rows(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col(TARGET_COLUMN).is_not_null())


def split_by_year(
    df: pl.DataFrame, train_start: str, valid_year: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    race_date = pl.col("race_date").str.to_datetime("%Y%m%d")
    train_mask = (race_date >= pl.lit(train_start).str.to_datetime("%Y%m%d")) & (
        pl.col("race_year") < valid_year
    )
    valid_mask = pl.col("race_year") == valid_year
    return df.filter(train_mask), df.filter(valid_mask)


def compute_inverse_frequency_weights(labels: pl.Series) -> np.ndarray:
    label_array = labels.to_numpy().astype(np.int64)
    class_counts = np.bincount(label_array, minlength=NUM_CLASSES).astype(np.float64)
    safe_counts = np.where(class_counts == 0, 1.0, class_counts)
    inverse_frequencies = label_array.size / (NUM_CLASSES * safe_counts)
    return inverse_frequencies[label_array]


def compute_weighted_sample_weights(
    labels: pl.Series, class_multipliers: tuple[float, float, float, float]
) -> np.ndarray:
    """Apply per-class multipliers on top of inverse-frequency base weights."""
    base_weights = compute_inverse_frequency_weights(labels)
    label_array = labels.to_numpy().astype(np.int64)
    multiplier_array = np.array(class_multipliers, dtype=np.float64)
    return base_weights * multiplier_array[label_array]


def resolve_sample_weights(labels: pl.Series, class_weight_scheme: str) -> np.ndarray:
    """Resolve sample weights by scheme name. 'balanced2' applies BALANCED2_WEIGHT_MULTIPLIERS."""
    if class_weight_scheme == "balanced2":
        return compute_weighted_sample_weights(labels, BALANCED2_WEIGHT_MULTIPLIERS)
    return compute_inverse_frequency_weights(labels)


def normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return None
    return text


def normalize_optional_number(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, str | int | float | np.integer | np.floating):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(numeric):
        return None
    return numeric


def normalize_keibajo_code(value: object) -> str:
    normalized = normalize_optional_string(value)
    if normalized is None:
        return ""
    return normalized.zfill(2)


def derive_running_style_category(source: object, keibajo_code: object) -> str:
    source_text = normalize_optional_string(source)
    source_key = source_text.lower() if source_text is not None else None
    if source_key == "jra":
        return "jra"
    if source_key == "ban-ei":
        return "ban-ei"
    normalized_keibajo = normalize_keibajo_code(keibajo_code)
    if normalized_keibajo in BAN_EI_RUNNING_STYLE_KEIBAJO_CODES:
        return "ban-ei"
    return "nar"


def derive_running_style_surface(track_code: object, category: str) -> str | None:
    if category != "jra":
        return "dirt"
    normalized = normalize_optional_string(track_code)
    if normalized is None:
        return None
    if normalized.startswith("1"):
        return "turf"
    if normalized.startswith("2"):
        return "dirt"
    return "other"


def derive_running_style_distance_band(kyori: object) -> str | None:
    numeric = normalize_optional_number(kyori)
    if numeric is None:
        return None
    if numeric < DISTANCE_SPRINT_MAX:
        return "sprint"
    if numeric < DISTANCE_MILE_MAX:
        return "mile"
    if numeric < DISTANCE_INTERMEDIATE_MAX:
        return "intermediate"
    if numeric < DISTANCE_LONG_MAX:
        return "long"
    return "extended"


def derive_running_style_season(kaisai_tsukihi: object) -> str | None:
    normalized = normalize_optional_string(kaisai_tsukihi)
    if normalized is None or len(normalized) < 2:
        return None
    month_text = normalized[:2]
    if not month_text.isdigit():
        return None
    month = int(month_text)
    if 3 <= month <= 5:
        return "spring"
    if 6 <= month <= 8:
        return "summer"
    if 9 <= month <= 11:
        return "autumn"
    return "winter"


def _first_present_record_value(
    record: dict[str, object], names: tuple[str, ...]
) -> object:
    for name in names:
        if name in record:
            return record[name]
    return None


def derive_running_style_cell_key(
    record: dict[str, object],
    *,
    allow_target_class_label: bool = False,
) -> RunningStyleCellKey:
    source = _first_present_record_value(record, ("source", "category"))
    keibajo_code = _first_present_record_value(record, ("keibajo_code",))
    category = derive_running_style_category(source, keibajo_code)
    grade_code = normalize_optional_string(
        _first_present_record_value(record, ("grade_code",))
    )
    subgroup_source = (
        _first_present_record_value(record, ("nar_subclass", "condition_key"))
        if category == "nar"
        else _first_present_record_value(record, ("kyoso_joken_code", "class_code"))
    )
    return RunningStyleCellKey(
        category=category,
        class_label=grade_code
        if grade_code is not None
        else derive_class_label_from_record(record)
        if allow_target_class_label
        else "unknown",
        distance_band=derive_running_style_distance_band(
            _first_present_record_value(record, ("kyori",))
        )
        or normalize_optional_string(
            _first_present_record_value(record, ("kyori_band",))
        ),
        season=derive_running_style_season(
            _first_present_record_value(record, ("kaisai_tsukihi",))
        )
        or normalize_optional_string(
            _first_present_record_value(record, ("season_band",))
        ),
        surface=derive_running_style_surface(
            _first_present_record_value(record, ("track_code",)),
            category,
        ),
        venue=normalize_keibajo_code(keibajo_code),
        subgroup=normalize_optional_string(subgroup_source),
    )


def derive_class_label_from_record(record: dict[str, object]) -> str:
    numeric = normalize_optional_number(
        _first_present_record_value(record, (TARGET_COLUMN,))
    )
    if numeric is None:
        return "unknown"
    class_idx = int(numeric)
    if 0 <= class_idx < len(CLASS_LABELS):
        return CLASS_LABELS[class_idx]
    return "unknown"


def derive_running_style_cell(record: dict[str, object]) -> dict[str, object]:
    cell = derive_running_style_cell_key(record, allow_target_class_label=True)
    derived: dict[str, object] = {
        "category": cell.category,
        "class_label": cell.class_label,
        "distance_band": cell.distance_band,
        "season": cell.season,
        "surface": cell.surface,
        "venue": cell.venue,
    }
    if cell.subgroup is not None:
        derived["subgroup"] = cell.subgroup
    return derived


def attach_running_style_cell_columns(
    df: pl.DataFrame,
    *,
    allow_target_class_label: bool = False,
) -> pl.DataFrame:
    cells = [
        derive_running_style_cell_key(
            dict(row), allow_target_class_label=allow_target_class_label
        )
        for row in df.iter_rows(named=True)
    ]
    return df.with_columns(
        pl.Series(CELL_DERIVED_COLUMNS[0], [cell.category for cell in cells]),
        pl.Series(CELL_DERIVED_COLUMNS[1], [cell.surface for cell in cells]),
        pl.Series(CELL_DERIVED_COLUMNS[2], [cell.distance_band for cell in cells]),
        pl.Series(CELL_DERIVED_COLUMNS[3], [cell.season for cell in cells]),
        pl.Series(CELL_DERIVED_COLUMNS[4], [cell.class_label for cell in cells]),
        pl.Series(CELL_DERIVED_COLUMNS[5], [cell.venue for cell in cells]),
        pl.Series(CELL_DERIVED_COLUMNS[6], [cell.subgroup for cell in cells]),
    )


def cell_key_from_derived_record(record: dict[str, object]) -> RunningStyleCellKey:
    category = normalize_optional_string(record[CELL_DERIVED_COLUMNS[0]]) or "nar"
    class_label = (
        normalize_optional_string(record[CELL_DERIVED_COLUMNS[4]]) or "unknown"
    )
    venue = normalize_optional_string(record[CELL_DERIVED_COLUMNS[5]]) or ""
    return RunningStyleCellKey(
        category=category,
        class_label=class_label,
        distance_band=normalize_optional_string(record[CELL_DERIVED_COLUMNS[2]]),
        season=normalize_optional_string(record[CELL_DERIVED_COLUMNS[3]]),
        surface=normalize_optional_string(record[CELL_DERIVED_COLUMNS[1]]),
        venue=venue,
        subgroup=normalize_optional_string(record[CELL_DERIVED_COLUMNS[6]]),
    )


def build_cell_filter_expr(cell: RunningStyleCellKey) -> pl.Expr:
    comparisons: list[pl.Expr] = [
        pl.col(CELL_DERIVED_COLUMNS[0]) == cell.category,
        pl.col(CELL_DERIVED_COLUMNS[4]) == cell.class_label,
        pl.col(CELL_DERIVED_COLUMNS[5]) == cell.venue,
    ]
    optional_pairs = (
        (CELL_DERIVED_COLUMNS[2], cell.distance_band),
        (CELL_DERIVED_COLUMNS[3], cell.season),
        (CELL_DERIVED_COLUMNS[1], cell.surface),
        (CELL_DERIVED_COLUMNS[6], cell.subgroup),
    )
    for column, value in optional_pairs:
        comparisons.append(
            pl.col(column).is_null() if value is None else pl.col(column) == value
        )
    expr = comparisons[0]
    for comparison in comparisons[1:]:
        expr = expr & comparison
    return expr


def source_for_running_style_category(category: str) -> str:
    if category == "jra":
        return "jra"
    return "nar"


def build_default_running_style_model_key(category: str) -> str:
    source = source_for_running_style_category(category)
    return f"{CELL_MODEL_KEY_ROOT}/{source}/latest.flatbin"


def sanitize_variant_token(value: str) -> str:
    token = CELL_VARIANT_ID_PATTERN.sub("-", value.strip()).strip("-").lower()
    return token if token else "none"


def build_cell_variant_id(cell: RunningStyleCellKey) -> str:
    parts = [
        cell.category,
        f"class-{cell.class_label}",
        f"dist-{cell.distance_band or 'none'}",
        f"season-{cell.season or 'none'}",
        f"surface-{cell.surface or 'none'}",
        f"venue-{cell.venue or 'none'}",
        f"subgroup-{cell.subgroup or 'none'}",
    ]
    return "cell-" + "-".join(sanitize_variant_token(part) for part in parts)


def build_cell_model_key(category: str, model_version: str, variant_id: str) -> str:
    source = source_for_running_style_category(category)
    version_token = sanitize_variant_token(model_version)
    return f"{CELL_MODEL_KEY_ROOT}/{source}/cells/{version_token}-{variant_id}.flatbin"


def cell_conditions(cell: RunningStyleCellKey) -> list[dict[str, object]]:
    values_by_dimension: dict[str, str | None] = {
        "class": cell.class_label,
        "distance_band": cell.distance_band,
        "season": cell.season,
        "surface": cell.surface,
        "venue": cell.venue,
        "subgroup": cell.subgroup,
    }
    return [
        {"dimension": dimension, "values": [value]}
        for dimension in DEFAULT_CELL_ROUTING_DIMENSIONS
        if (value := values_by_dimension[dimension]) is not None and value != ""
    ]


def compute_topk_accuracy(probabilities: np.ndarray, actual: np.ndarray, k: int) -> float:
    topk = compute_topk_classes(probabilities, actual, k)
    if topk.size == 0:
        return float("nan")
    hits = np.any(topk == actual.reshape(-1, 1), axis=1)
    return float(hits.mean())


def compute_topk_classes(
    probabilities: np.ndarray, actual: np.ndarray, k: int
) -> np.ndarray:
    if actual.size == 0:
        return np.empty((0, 0), dtype=np.int64)
    if probabilities.ndim != 2 or probabilities.shape[0] != actual.size:
        raise ValueError("probabilities must be a 2D array with one row per label")
    if probabilities.shape[1] == 0:
        return np.empty((actual.size, 0), dtype=np.int64)
    effective_k = min(k, probabilities.shape[1])
    class_indices = np.arange(probabilities.shape[1])
    return np.asarray(
        [
            np.lexsort((class_indices, -row))[:effective_k]
            for row in probabilities
        ],
        dtype=np.int64,
    )


def compute_top2_accuracy(probabilities: np.ndarray, actual: np.ndarray) -> float:
    return compute_topk_accuracy(probabilities, actual, 2)


def compute_predicted_front_score(probabilities: np.ndarray) -> np.ndarray:
    """Expected running-style ordinal: lower means closer to the lead."""
    if probabilities.ndim != 2 or probabilities.shape[1] != NUM_CLASSES:
        raise ValueError("probabilities must have one column per running-style class")
    class_ordinals = np.arange(NUM_CLASSES, dtype=np.float64)
    return probabilities @ class_ordinals


def average_ranks(values: np.ndarray) -> np.ndarray:
    if values.size == 0:
        return np.empty(0, dtype=np.float64)
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty(values.size, dtype=np.float64)
    start = 0
    while start < values.size:
        end = start + 1
        while end < values.size and values[order[end]] == values[order[start]]:
            end += 1
        average_rank = (start + end - 1) / 2.0
        ranks[order[start:end]] = average_rank
        start = end
    return ranks


def spearman_corr(left: np.ndarray, right: np.ndarray) -> float:
    if left.size < 2 or right.size != left.size:
        return float("nan")
    left_ranks = average_ranks(left)
    right_ranks = average_ranks(right)
    left_centered = left_ranks - left_ranks.mean()
    right_centered = right_ranks - right_ranks.mean()
    denominator = float(
        np.sqrt(np.sum(left_centered * left_centered) * np.sum(right_centered * right_centered))
    )
    if denominator == 0.0:
        return float("nan")
    return float(np.sum(left_centered * right_centered) / denominator)


def _race_ids_for_metrics(
    actual: np.ndarray, race_ids: Sequence[object] | np.ndarray | None
) -> np.ndarray:
    if race_ids is None:
        return np.repeat("__all__", actual.size).astype(object)
    values = np.asarray(race_ids, dtype=object)
    if values.shape[0] != actual.size:
        raise ValueError("race_ids must have one value per label")
    return values


def _optional_float_array(
    values: Sequence[object] | np.ndarray | None, expected_size: int, name: str
) -> np.ndarray | None:
    if values is None:
        return None
    array = np.asarray(values, dtype=np.float64)
    if array.shape[0] != expected_size:
        raise ValueError(f"{name} must have one value per label")
    return array


def compute_race_level_running_style_metrics(
    probabilities: np.ndarray,
    actual: np.ndarray,
    *,
    race_ids: Sequence[object] | np.ndarray | None = None,
    corner1_norm: Sequence[object] | np.ndarray | None = None,
    finish_positions: Sequence[object] | np.ndarray | None = None,
) -> RunningStyleRaceLevelMetrics:
    predicted = compute_predicted_labels(probabilities)
    pred_front_score = compute_predicted_front_score(probabilities)
    race_id_values = _race_ids_for_metrics(actual, race_ids)
    corner_values = _optional_float_array(corner1_norm, actual.size, "corner1_norm")
    finish_values = _optional_float_array(finish_positions, actual.size, "finish_positions")

    distribution_errors: list[float] = []
    style_count_abs_errors: dict[str, list[float]] = {
        class_name: [] for class_name in CLASS_LABELS
    }
    style_count_biases: dict[str, list[float]] = {class_name: [] for class_name in CLASS_LABELS}
    nige_count_errors: list[float] = []
    front_group_count_errors: list[float] = []
    corner_spearman_values: list[float] = []
    finish_weighted_accuracy_values: list[float] = []
    top1_finish_style_accuracy_values: list[float] = []
    top3_finish_style_accuracy_values: list[float] = []
    unique_race_ids = list(dict.fromkeys(race_id_values.tolist()))

    for race_id in unique_race_ids:
        mask = race_id_values == race_id
        race_actual = actual[mask]
        race_predicted = predicted[mask]
        if race_actual.size == 0:
            continue
        actual_counts = np.bincount(race_actual, minlength=NUM_CLASSES).astype(np.float64)
        predicted_counts = np.bincount(race_predicted, minlength=NUM_CLASSES).astype(np.float64)
        distribution_errors.append(
            float(np.mean(np.abs((predicted_counts - actual_counts) / race_actual.size)))
        )
        for class_idx, class_name in enumerate(CLASS_LABELS):
            count_delta = predicted_counts[class_idx] - actual_counts[class_idx]
            style_count_abs_errors[class_name].append(float(abs(count_delta)))
            style_count_biases[class_name].append(float(count_delta))
        nige_count_errors.append(float(abs(predicted_counts[0] - actual_counts[0])))
        front_group_count_errors.append(
            float(abs(predicted_counts[0:2].sum() - actual_counts[0:2].sum()))
        )
        if corner_values is not None:
            race_corner = corner_values[mask]
            valid_corner = np.isfinite(race_corner)
            if int(valid_corner.sum()) >= 2:
                corr = spearman_corr(pred_front_score[mask][valid_corner], race_corner[valid_corner])
                if np.isfinite(corr):
                    corner_spearman_values.append(corr)
        if finish_values is not None:
            race_finish = finish_values[mask]
            valid_finish = np.isfinite(race_finish) & (race_finish > 0)
            if np.any(valid_finish):
                weights = 1.0 / race_finish[valid_finish]
                hits = (race_predicted[valid_finish] == race_actual[valid_finish]).astype(np.float64)
                finish_weighted_accuracy_values.append(float(np.average(hits, weights=weights)))
            top1_finish = valid_finish & (race_finish == 1)
            if np.any(top1_finish):
                top1_finish_style_accuracy_values.append(
                    float((race_predicted[top1_finish] == race_actual[top1_finish]).mean())
                )
            top3_finish = valid_finish & (race_finish <= 3)
            if np.any(top3_finish):
                top3_finish_style_accuracy_values.append(
                    float((race_predicted[top3_finish] == race_actual[top3_finish]).mean())
                )

    return {
        "race_count": len(unique_race_ids),
        "style_distribution_mae": float(np.mean(distribution_errors))
        if distribution_errors
        else float("nan"),
        "style_count_mae": {
            class_name: (
                float(np.mean(style_count_abs_errors[class_name]))
                if style_count_abs_errors[class_name]
                else float("nan")
            )
            for class_name in CLASS_LABELS
        },
        "style_count_bias": {
            class_name: (
                float(np.mean(style_count_biases[class_name]))
                if style_count_biases[class_name]
                else float("nan")
            )
            for class_name in CLASS_LABELS
        },
        "nige_count_mae": float(np.mean(nige_count_errors)) if nige_count_errors else float("nan"),
        "front_group_count_mae": float(np.mean(front_group_count_errors))
        if front_group_count_errors
        else float("nan"),
        "corner_rank_spearman": float(np.mean(corner_spearman_values))
        if corner_spearman_values
        else float("nan"),
        "finish_weighted_accuracy": float(np.mean(finish_weighted_accuracy_values))
        if finish_weighted_accuracy_values
        else float("nan"),
        "top1_finish_style_accuracy": float(np.mean(top1_finish_style_accuracy_values))
        if top1_finish_style_accuracy_values
        else float("nan"),
        "top3_finish_style_accuracy": float(np.mean(top3_finish_style_accuracy_values))
        if top3_finish_style_accuracy_values
        else float("nan"),
    }


def compute_per_class_log_loss(
    probabilities: np.ndarray, actual: np.ndarray
) -> dict[str, float]:
    sums, counts = compute_per_class_log_loss_sums(probabilities, actual)
    return {
        class_name: (
            float(sums[class_name] / counts[class_name])
            if counts[class_name] > 0
            else float("nan")
        )
        for class_name in CLASS_LABELS
    }


def compute_per_class_log_loss_sums(
    probabilities: np.ndarray, actual: np.ndarray
) -> tuple[dict[str, float], dict[str, int]]:
    sums: dict[str, float] = {}
    counts: dict[str, int] = {}
    clipped = np.clip(probabilities, LOG_LOSS_EPS, 1.0 - LOG_LOSS_EPS)
    for class_idx, class_name in enumerate(CLASS_LABELS):
        class_mask = actual == class_idx
        if not np.any(class_mask):
            sums[class_name] = 0.0
            counts[class_name] = 0
            continue
        selected = clipped[class_mask, class_idx]
        sums[class_name] = float(-np.log(selected).sum())
        counts[class_name] = int(class_mask.sum())
    return sums, counts


def compute_confusion_matrix(
    predicted: np.ndarray, actual: np.ndarray
) -> dict[str, dict[str, int]]:
    return {
        actual_name: {
            predicted_name: int(
                ((actual == actual_idx) & (predicted == predicted_idx)).sum()
            )
            for predicted_idx, predicted_name in enumerate(CLASS_LABELS)
        }
        for actual_idx, actual_name in enumerate(CLASS_LABELS)
    }


def compute_running_style_metrics(
    probabilities: np.ndarray,
    actual: np.ndarray,
    *,
    race_ids: Sequence[object] | np.ndarray | None = None,
    corner1_norm: Sequence[object] | np.ndarray | None = None,
    finish_positions: Sequence[object] | np.ndarray | None = None,
) -> RunningStyleCellMetrics:
    predicted = compute_predicted_labels(probabilities)
    precision, recall, support = compute_per_class_precision_recall(predicted, actual)
    per_class_f1 = compute_per_class_f1(precision, recall)
    predicted_support = {
        class_name: int((predicted == class_idx).sum())
        for class_idx, class_name in enumerate(CLASS_LABELS)
    }
    top2_classes = compute_topk_classes(probabilities, actual, 2)
    top2_hits = np.any(top2_classes == actual.reshape(-1, 1), axis=1)
    log_loss_sums, log_loss_counts = compute_per_class_log_loss_sums(probabilities, actual)
    return {
        "prediction_count": int(actual.size),
        "top2_hit_count": int(top2_hits.sum()),
        "accuracy": compute_accuracy(predicted, actual),
        "macro_f1": macro_f1_from_precision_recall(precision, recall),
        "multi_log_loss": compute_multi_log_loss(probabilities, actual),
        "top2_accuracy": float(top2_hits.mean()) if actual.size > 0 else float("nan"),
        "race_level": compute_race_level_running_style_metrics(
            probabilities,
            actual,
            race_ids=race_ids,
            corner1_norm=corner1_norm,
            finish_positions=finish_positions,
        ),
        "per_class_accuracy": compute_per_class_accuracy(predicted, actual),
        "per_class_f1": per_class_f1,
        "per_class_precision": precision,
        "per_class_recall": recall,
        "per_class_support": support,
        "predicted_class_support": predicted_support,
        "confusion_matrix": compute_confusion_matrix(predicted, actual),
        "per_class_log_loss_sum": log_loss_sums,
        "per_class_log_loss_count": log_loss_counts,
        "per_class_log_loss": {
            class_name: (
                float(log_loss_sums[class_name] / log_loss_counts[class_name])
                if log_loss_counts[class_name] > 0
                else float("nan")
            )
            for class_name in CLASS_LABELS
        },
    }


def running_style_cell_metrics_for_adoption(
    cell: RunningStyleCellKey,
    metrics: RunningStyleCellMetrics,
    *,
    feature_set_hash: str,
    race_count: int,
) -> RunningStyleCellAdoptionMetrics:
    surface = cell.surface or "unknown"
    distance_band = cell.distance_band or "unknown"
    season = cell.season or "unknown"
    adoption_vector = [
        metrics["accuracy"],
        metrics["top2_accuracy"],
        metrics["macro_f1"],
        0.0,
        0.0,
        0.0,
    ]
    return {
        "prediction_target": "running_style",
        "feature_set_hash": feature_set_hash,
        "category": cell.category,
        "surface": surface,
        "distance_band": distance_band,
        "class_label": cell.class_label,
        "season": season,
        "venue": cell.venue,
        "subgroup": cell.subgroup,
        "race_count": race_count,
        "prediction_count": metrics["prediction_count"],
        "ndcg_at_3": metrics["accuracy"],
        "top1_accuracy": metrics["accuracy"],
        "place2_accuracy": metrics["top2_accuracy"],
        "place3_accuracy": metrics["macro_f1"],
        "place4_accuracy": 0.0,
        "place5_accuracy": 0.0,
        "place6_accuracy": 0.0,
        "top3_box_accuracy": 0.0,
        "accuracy_vector": adoption_vector,
        "cell_vector": [
            cell.category,
            surface,
            distance_band,
            cell.class_label,
            season,
            cell.venue,
        ],
        "metric_mapping": {
            "top1_accuracy": "accuracy",
            "place2_accuracy": "top2_accuracy",
            "place3_accuracy": "macro_f1",
        },
    }


def json_ready(value: object) -> object:
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [json_ready(item) for item in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        numeric = float(value)
        return numeric if np.isfinite(numeric) else None
    if isinstance(value, float):
        return value if np.isfinite(value) else None
    return value


def build_running_style_cell_routing_config(
    trained_cells: list[dict[str, object]],
) -> dict[str, object]:
    categories = sorted({str(cell["category"]) for cell in trained_cells})
    config: dict[str, object] = {}
    for category in categories:
        variants: dict[str, object] = {
            RUNNING_STYLE_DEFAULT_VARIANT_ID: {
                "modelKey": build_default_running_style_model_key(category)
            }
        }
        rules: list[dict[str, object]] = []
        category_cells = [
            cell for cell in trained_cells if str(cell["category"]) == category
        ]
        for cell_record in sorted(
            category_cells, key=lambda record: str(record["variant_id"])
        ):
            variant_id = str(cell_record["variant_id"])
            variants[variant_id] = {"modelKey": str(cell_record["model_key"])}
            rules.append(
                {
                    "conditions": cell_record["conditions"],
                    "variantId": variant_id,
                }
            )
        config[category] = {
            "defaultVariantId": RUNNING_STYLE_DEFAULT_VARIANT_ID,
            "rules": rules,
            "variants": variants,
        }
    return config


def lgb_params_for_multiclass(params: TrainingParams) -> dict[str, object]:
    return {
        "objective": "multiclass",
        "num_class": NUM_CLASSES,
        "metric": "multi_logloss",
        "num_leaves": params["num_leaves"],
        "learning_rate": params["learning_rate"],
        "min_child_samples": params["min_child_samples"],
        "lambda_l1": params["lambda_l1"],
        "lambda_l2": params["lambda_l2"],
        "feature_fraction": params["feature_fraction"],
        "bagging_fraction": params["bagging_fraction"],
        "bagging_freq": params["bagging_freq"],
        "verbose": -1,
    }


def encode_categoricals(
    frame: pl.DataFrame, categorical_features: list[str]
) -> pl.DataFrame:
    present = [column for column in categorical_features if column in frame.columns]
    return frame.with_columns(pl.col(column).cast(pl.Categorical) for column in present)


def to_lgb_frame(frame: pl.DataFrame) -> "pd.DataFrame":
    # LightGBM 4.x ingests only pandas/numpy; its Arrow path rejects dictionary
    # (categorical) columns. Convert at the boundary so polars Categorical maps to
    # pandas "category" dtype, which LightGBM consumes natively for categorical splits.
    return frame.to_pandas()


def build_lgb_dataset(
    frame: pl.DataFrame,
    labels: pl.Series,
    sample_weights: np.ndarray,
    feature_columns: list[str],
    categorical_features: list[str],
    reference: lgb.Dataset | None = None,
) -> lgb.Dataset:
    feature_frame = encode_categoricals(
        frame.select(feature_columns), categorical_features
    )
    return lgb.Dataset(
        to_lgb_frame(feature_frame),
        label=labels.to_numpy().astype(np.int64),
        weight=sample_weights,
        categorical_feature=categorical_features if categorical_features else "auto",
        free_raw_data=False,
        reference=reference,
    )


def predict_softmax(
    booster: lgb.Booster,
    frame: pl.DataFrame,
    feature_columns: list[str],
    categorical_features: list[str],
) -> np.ndarray:
    feature_frame = encode_categoricals(
        frame.select(feature_columns), categorical_features
    )
    raw = booster.predict(
        to_lgb_frame(feature_frame), num_iteration=booster.best_iteration
    )
    return np.asarray(raw, dtype=np.float64)


def compute_predicted_labels(probabilities: np.ndarray) -> np.ndarray:
    return np.argmax(probabilities, axis=1)


def compute_accuracy(predicted: np.ndarray, actual: np.ndarray) -> float:
    if actual.size == 0:
        return float("nan")
    return float((predicted == actual).mean())


def compute_per_class_precision_recall(
    predicted: np.ndarray, actual: np.ndarray
) -> tuple[dict[str, float], dict[str, float], dict[str, int]]:
    precision: dict[str, float] = {}
    recall: dict[str, float] = {}
    support: dict[str, int] = {}
    for class_idx, class_name in enumerate(CLASS_LABELS):
        actual_mask = actual == class_idx
        predicted_mask = predicted == class_idx
        tp = int((predicted_mask & actual_mask).sum())
        predicted_count = int(predicted_mask.sum())
        actual_count = int(actual_mask.sum())
        precision[class_name] = (
            float(tp / predicted_count) if predicted_count > 0 else float("nan")
        )
        recall[class_name] = (
            float(tp / actual_count) if actual_count > 0 else float("nan")
        )
        support[class_name] = actual_count
    return precision, recall, support


def compute_per_class_accuracy(
    predicted: np.ndarray, actual: np.ndarray
) -> dict[str, float]:
    accuracy: dict[str, float] = {}
    for class_idx, class_name in enumerate(CLASS_LABELS):
        actual_mask = actual == class_idx
        actual_count = int(actual_mask.sum())
        accuracy[class_name] = (
            float((predicted[actual_mask] == actual[actual_mask]).mean())
            if actual_count > 0
            else float("nan")
        )
    return accuracy


def compute_per_class_f1(
    precision: dict[str, float], recall: dict[str, float]
) -> dict[str, float]:
    f1: dict[str, float] = {}
    for class_name in CLASS_LABELS:
        p = precision[class_name]
        r = recall[class_name]
        f1[class_name] = (
            0.0
            if np.isnan(p) or np.isnan(r) or (p + r) == 0
            else float(2.0 * p * r / (p + r))
        )
    return f1


def macro_f1_from_precision_recall(
    precision: dict[str, float], recall: dict[str, float]
) -> float:
    return float(np.mean(list(compute_per_class_f1(precision, recall).values())))


def compute_binary_log_loss_nige(
    probabilities: np.ndarray, actual: np.ndarray
) -> float:
    if actual.size == 0:
        return float("nan")
    p_nige = np.clip(probabilities[:, 0], LOG_LOSS_EPS, 1.0 - LOG_LOSS_EPS)
    is_nige = (actual == 0).astype(np.float64)
    losses = -(is_nige * np.log(p_nige) + (1.0 - is_nige) * np.log(1.0 - p_nige))
    return float(losses.mean())


def compute_multi_log_loss(probabilities: np.ndarray, actual: np.ndarray) -> float:
    if actual.size == 0:
        return float("nan")
    clipped = np.clip(probabilities, LOG_LOSS_EPS, 1.0 - LOG_LOSS_EPS)
    selected = clipped[np.arange(actual.size), actual.astype(np.int64)]
    return float(-np.log(selected).mean())


def compute_walk_forward_eval_metrics(
    probabilities: np.ndarray,
    actual: np.ndarray,
    holdout_year: int,
    train_start_date: str,
    train_end_date: str,
    train_rows: int,
) -> WalkForwardEvalMetrics:
    predicted = compute_predicted_labels(probabilities)
    precision, recall, _ = compute_per_class_precision_recall(predicted, actual)
    return {
        "holdout_year": holdout_year,
        "train_start_date": train_start_date,
        "train_end_date": train_end_date,
        "train_rows": train_rows,
        "valid_rows": int(actual.size),
        "precision_nige": precision["nige"],
        "recall_nige": recall["nige"],
        "log_loss_nige": compute_binary_log_loss_nige(probabilities, actual),
        "multi_log_loss": compute_multi_log_loss(probabilities, actual),
        "accuracy": compute_accuracy(predicted, actual),
    }


def parse_walk_forward_windows(value: str) -> list[int]:
    return [int(token.strip()) for token in value.split(",") if token.strip()]


def derive_walk_forward_train_end(holdout_year: int) -> str:
    return f"{holdout_year - 1}1231"


def derive_walk_forward_valid_range(holdout_year: int) -> tuple[str, str]:
    return f"{holdout_year}0101", f"{holdout_year}1231"


def detect_walk_forward_precision_regression(
    walk_forward_precision_nige: float,
    production_precision_nige: float,
    gap_threshold_pp: float = WALK_FORWARD_PRECISION_GAP_THRESHOLD_PP,
) -> bool:
    if np.isnan(walk_forward_precision_nige) or np.isnan(production_precision_nige):
        return False
    gap_pp = (production_precision_nige - walk_forward_precision_nige) * 100.0
    return gap_pp >= gap_threshold_pp


def maybe_enrich_with_field_features(df: pl.DataFrame, enabled: bool) -> pl.DataFrame:
    if not enabled:
        return df
    return enrich_dataframe_with_field_features(df)


def extend_feature_columns(
    feature_columns: list[str], with_field_features: bool
) -> list[str]:
    if not with_field_features:
        return feature_columns
    merged = list(feature_columns)
    for column in FIELD_FEATURE_COLUMNS:
        if column not in merged:
            merged.append(column)
    return merged


def _cell_dimension_value(cell: RunningStyleCellKey, dimension: str) -> str:
    values = {
        "category": cell.category,
        "class": cell.class_label,
        "class_label": cell.class_label,
        "distance_band": cell.distance_band or "",
        "season": cell.season or "",
        "surface": cell.surface or "",
        "venue": cell.venue,
        "racetrack": cell.venue,
        "subgroup": cell.subgroup or "",
    }
    return values.get(dimension, "")


def _parse_feature_selection_conditions(
    raw_conditions: object,
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if not isinstance(raw_conditions, list):
        return ()
    parsed: list[tuple[str, tuple[str, ...]]] = []
    for condition in raw_conditions:
        if not isinstance(condition, dict):
            continue
        dimension = condition.get("dimension")
        values = condition.get("values")
        if not isinstance(dimension, str) or not isinstance(values, list):
            continue
        parsed.append((dimension, tuple(str(value) for value in values)))
    return tuple(parsed)


def _build_cell_feature_selection_rule(
    *,
    category: str,
    raw_conditions: object,
    raw_features: object,
    raw_feature_set_hash: object,
) -> CellFeatureSelectionRule | None:
    if not isinstance(raw_features, list):
        return None
    feature_names = tuple(normalize_feature_names([str(f) for f in raw_features]))
    if not feature_names:
        return None
    return CellFeatureSelectionRule(
        category=category,
        conditions=_parse_feature_selection_conditions(raw_conditions),
        feature_names=feature_names,
        feature_set_hash=(
            str(raw_feature_set_hash)
            if isinstance(raw_feature_set_hash, str) and raw_feature_set_hash
            else compute_feature_set_hash(feature_names)
        ),
    )


def _extract_variant_id_from_rule(raw_rule: object) -> str | None:
    if not isinstance(raw_rule, Mapping):
        return None
    variant_id = raw_rule.get("variant")
    if isinstance(variant_id, str):
        return variant_id
    variant_id = raw_rule.get("variantId")
    return variant_id if isinstance(variant_id, str) else None


def _extract_explicit_feature_selection_rules(
    raw_rules: Sequence[object], default_category: str | None
) -> list[CellFeatureSelectionRule]:
    rules: list[CellFeatureSelectionRule] = []
    for raw_rule in raw_rules:
        if not isinstance(raw_rule, dict):
            continue
        category = raw_rule.get("category")
        if not isinstance(category, str):
            category = default_category
        if category is None:
            continue
        rule = _build_cell_feature_selection_rule(
            category=category,
            raw_conditions=raw_rule.get("conditions"),
            raw_features=raw_rule.get("feature_names"),
            raw_feature_set_hash=raw_rule.get("feature_set_hash"),
        )
        if rule is not None:
            rules.append(rule)
    return rules


def _extract_feature_selection_rules_from_routing(
    payload: dict[str, object],
) -> list[CellFeatureSelectionRule]:
    rules: list[CellFeatureSelectionRule] = []
    raw_top_level_rules = payload.get("rules")
    if isinstance(raw_top_level_rules, list):
        rules.extend(
            _extract_explicit_feature_selection_rules(raw_top_level_rules, None)
        )
    for category, raw_config in payload.items():
        if not isinstance(raw_config, dict):
            continue
        raw_rules = raw_config.get("rules")
        if not isinstance(raw_rules, list):
            continue
        rules.extend(_extract_explicit_feature_selection_rules(raw_rules, category))
        raw_variants = raw_config.get("variants")
        if not isinstance(raw_variants, dict):
            continue
        for raw_rule in raw_rules:
            if not isinstance(raw_rule, dict):
                continue
            if "feature_names" in raw_rule:
                continue
            variant_id = _extract_variant_id_from_rule(raw_rule)
            if variant_id is None:
                continue
            variant = raw_variants.get(variant_id)
            if not isinstance(variant, dict):
                continue
            rule = _build_cell_feature_selection_rule(
                category=category,
                raw_conditions=raw_rule.get("conditions"),
                raw_features=variant.get("feature_names"),
                raw_feature_set_hash=variant.get("feature_set_hash"),
            )
            if rule is not None:
                rules.append(rule)
    return rules


def _payload_has_feature_selection_rule_candidates(payload: dict[str, object]) -> bool:
    raw_top_level_rules = payload.get("rules")
    if isinstance(raw_top_level_rules, list) and len(raw_top_level_rules) > 0:
        return True
    for raw_config in payload.values():
        if not isinstance(raw_config, dict):
            continue
        raw_rules = raw_config.get("rules")
        if isinstance(raw_rules, list) and len(raw_rules) > 0:
            return True
    return False


def load_cell_feature_selection_rules(path: Path | None) -> list[CellFeatureSelectionRule]:
    if path is None:
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"cell feature selection JSON must be an object: {path}")
    rules = _extract_feature_selection_rules_from_routing(payload)
    if not rules and _payload_has_feature_selection_rule_candidates(payload):
        raise ValueError(
            "cell feature selection JSON contains rules but no usable "
            "feature-selection rules; provide rules with feature_names or "
            "route variants referenced by variant/variantId that include feature_names"
        )
    return rules


def _rule_matches_cell(
    rule: CellFeatureSelectionRule, cell: RunningStyleCellKey
) -> bool:
    if rule.category != cell.category:
        return False
    return all(
        _cell_dimension_value(cell, dimension) in values
        for dimension, values in rule.conditions
    )


def resolve_cell_feature_selection(
    cell: RunningStyleCellKey,
    default_feature_columns: list[str],
    rules: list[CellFeatureSelectionRule],
) -> tuple[list[str], str]:
    matching = [rule for rule in rules if _rule_matches_cell(rule, cell)]
    if not matching:
        return default_feature_columns, compute_feature_set_hash(default_feature_columns)
    selected = list(matching[0].feature_names)
    available = set(default_feature_columns)
    missing = [feature for feature in selected if feature not in available]
    if missing:
        raise ValueError(
            "cell feature selection contains features not present in training data: "
            + ", ".join(missing[:10])
        )
    return selected, matching[0].feature_set_hash


def train_running_style_head(
    train_df: pl.DataFrame,
    valid_df: pl.DataFrame,
    feature_columns: list[str],
    categorical_features: list[str],
    params: TrainingParams,
    *,
    class_weight_scheme: str = "inverse_freq",
) -> tuple[lgb.Booster, np.ndarray]:
    train_subset = filter_labeled_rows(train_df)
    valid_subset = filter_labeled_rows(valid_df)
    train_weights = resolve_sample_weights(
        train_subset[TARGET_COLUMN], class_weight_scheme
    )
    valid_weights = np.ones(len(valid_subset), dtype=np.float64)
    train_dataset = build_lgb_dataset(
        train_subset,
        train_subset[TARGET_COLUMN],
        train_weights,
        feature_columns,
        categorical_features,
    )
    valid_dataset = build_lgb_dataset(
        valid_subset,
        valid_subset[TARGET_COLUMN],
        valid_weights,
        feature_columns,
        categorical_features,
        reference=train_dataset,
    )
    booster = lgb.train(
        lgb_params_for_multiclass(params),
        train_dataset,
        num_boost_round=params["num_iterations"],
        valid_sets=[valid_dataset],
        callbacks=[
            lgb.early_stopping(stopping_rounds=params["early_stopping_rounds"]),
            lgb.log_evaluation(period=DEFAULT_VERBOSE_EVAL),
        ],
    )
    probabilities = predict_softmax(
        booster, valid_df, feature_columns, categorical_features
    )
    return booster, probabilities


def build_predictions_df(
    valid_df: pl.DataFrame, probabilities: np.ndarray
) -> pl.DataFrame:
    output = valid_df.select(
        ["race_id", "ketto_toroku_bango", "umaban", "race_year", TARGET_COLUMN]
    )
    predicted_indices = compute_predicted_labels(probabilities)
    probability_columns = [
        pl.Series(column_name, probabilities[:, class_idx])
        for class_idx, column_name in enumerate(PROBABILITY_COLUMNS)
    ]
    return output.with_columns(
        *probability_columns,
        pl.Series(
            "predicted_label", [CLASS_LABELS[int(idx)] for idx in predicted_indices]
        ),
        pl.Series("predicted_class", predicted_indices.astype(int)),
    )


def run_walk_forward_for_year(
    df: pl.DataFrame,
    valid_year: int,
    train_start: str,
    feature_columns: list[str],
    categorical_features: list[str],
    params: TrainingParams,
    *,
    with_field_features: bool,
) -> tuple[pl.DataFrame, FoldMetrics]:
    train_df, valid_df = split_by_year(df, train_start, valid_year)
    train_df = maybe_enrich_with_field_features(train_df, with_field_features)
    valid_df = maybe_enrich_with_field_features(valid_df, with_field_features)
    _booster, probabilities = train_running_style_head(
        train_df,
        valid_df,
        feature_columns,
        categorical_features,
        params,
    )
    predictions_df = build_predictions_df(valid_df, probabilities)
    evaluation_subset = predictions_df.drop_nulls(subset=[TARGET_COLUMN])
    predicted = evaluation_subset["predicted_class"].to_numpy().astype(np.int64)
    actual = evaluation_subset[TARGET_COLUMN].to_numpy().astype(np.int64)
    precision, recall, support = compute_per_class_precision_recall(predicted, actual)
    metrics: FoldMetrics = {
        "validation_year": valid_year,
        "train_rows": int(len(train_df)),
        "valid_rows": int(len(valid_df)),
        "accuracy": compute_accuracy(predicted, actual),
        "macro_f1": macro_f1_from_precision_recall(precision, recall),
        "per_class_precision": precision,
        "per_class_recall": recall,
        "per_class_support": support,
    }
    return predictions_df, metrics


def _sanitize_record_for_json(record: dict[str, object]) -> dict[str, object]:
    sanitized: dict[str, object] = {}
    for key, value in record.items():
        if isinstance(value, float) and not np.isfinite(value):
            sanitized[key] = None
        else:
            sanitized[key] = value
    return sanitized


def write_predictions_jsonl(predictions: pl.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for raw_record in predictions.iter_rows(named=True):
            record = _sanitize_record_for_json(
                {str(k): v for k, v in raw_record.items()}
            )
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_walk_forward_report(
    metrics_per_fold: list[FoldMetrics], output_path: Path
) -> None:
    aggregate = {
        "accuracy_mean": float(
            np.nanmean([fold["accuracy"] for fold in metrics_per_fold])
        ),
        "macro_f1_mean": float(
            np.nanmean([fold["macro_f1"] for fold in metrics_per_fold])
        ),
    }
    payload = {"folds": metrics_per_fold, "aggregate": aggregate}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def _add_lgbm_hyperparam_arguments(subparser: argparse.ArgumentParser) -> None:
    subparser.add_argument("--num-leaves", type=int, default=DEFAULT_NUM_LEAVES)
    subparser.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    subparser.add_argument(
        "--min-child-samples", type=int, default=DEFAULT_MIN_CHILD_SAMPLES
    )
    subparser.add_argument("--num-iterations", type=int, default=DEFAULT_NUM_ITERATIONS)
    subparser.add_argument(
        "--early-stopping-rounds",
        type=int,
        default=DEFAULT_EARLY_STOPPING_ROUNDS,
    )
    subparser.add_argument(
        "--bagging-fraction",
        type=float,
        default=DEFAULT_BAGGING_FRACTION,
        help="Subsample ratio of training data per bagging round",
    )
    subparser.add_argument(
        "--bagging-freq",
        type=int,
        default=DEFAULT_BAGGING_FREQ,
        help="Bagging frequency (every N iters perform bagging)",
    )
    subparser.add_argument(
        "--feature-fraction",
        type=float,
        default=DEFAULT_FEATURE_FRACTION,
        help="Subsample ratio of columns per tree",
    )
    subparser.add_argument(
        "--reg-alpha",
        type=float,
        default=DEFAULT_LAMBDA_L1,
        help="L1 regularization (lambda_l1)",
    )
    subparser.add_argument(
        "--reg-lambda",
        type=float,
        default=DEFAULT_LAMBDA_L2,
        help="L2 regularization (lambda_l2)",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="running_style_lightgbm")
    subparsers = parser.add_subparsers(dest="command", required=True)
    walk = subparsers.add_parser("walk-forward")
    walk.add_argument(
        "--csv", type=Path, required=True, help="parquet directory or file"
    )
    walk.add_argument("--train-start-date", type=str, default=DEFAULT_TRAIN_START_DATE)
    walk.add_argument("--validation-years", type=str, default="2024,2025")
    walk.add_argument("--output-predictions-dir", type=Path, required=True)
    walk.add_argument("--output-report", type=Path, default=None)
    _add_lgbm_hyperparam_arguments(walk)
    walk.add_argument(
        "--with-field-features",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Compute race-internal field_* features before training/prediction",
    )
    train_prod = subparsers.add_parser("train-production")
    train_prod.add_argument(
        "--csv", type=Path, required=True, help="parquet directory or file"
    )
    train_prod.add_argument(
        "--train-start-date",
        type=str,
        default=DEFAULT_TRAIN_START_DATE,
        help="YYYYMMDD inclusive (21-year default starts at NAR 2005-01-01)",
    )
    train_prod.add_argument(
        "--train-end-date",
        type=str,
        default=DEFAULT_TRAIN_END_DATE,
        help="YYYYMMDD inclusive (21-year default ends at backfilled 2026-12-31)",
    )
    train_prod.add_argument("--model-version", type=str, required=True)
    train_prod.add_argument("--output-model-dir", type=Path, required=True)
    _add_lgbm_hyperparam_arguments(train_prod)
    train_prod.add_argument(
        "--valid-start-date",
        type=str,
        default=DEFAULT_VALID_START_DATE,
        help="Hold out races from this date onward for production early stopping",
    )
    train_prod.add_argument(
        "--with-field-features",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    train_prod.add_argument(
        "--enable-walk-forward-eval",
        action="store_true",
        help="Run walk-forward CV evaluation alongside production fit (slower, +20-30%% wall clock)",
    )
    train_prod.add_argument(
        "--walk-forward-windows",
        type=str,
        default=DEFAULT_WALK_FORWARD_WINDOWS,
        help="Comma-separated holdout years for walk-forward eval",
    )
    train_prod.add_argument(
        "--class-weight-scheme",
        type=str,
        default="inverse_freq",
        choices=["inverse_freq", "balanced2"],
        help="Sample weight scheme: inverse_freq (default) or balanced2 (nige×0.65, senkou×1.0, sashi×1.0, oikomi×0.85)",
    )
    train_cells = subparsers.add_parser("train-cells")
    train_cells.add_argument(
        "--csv", type=Path, required=True, help="parquet directory or file"
    )
    train_cells.add_argument(
        "--train-start-date",
        type=str,
        default=DEFAULT_TRAIN_START_DATE,
        help="YYYYMMDD inclusive",
    )
    train_cells.add_argument(
        "--train-end-date",
        type=str,
        default=DEFAULT_TRAIN_END_DATE,
        help="YYYYMMDD inclusive",
    )
    train_cells.add_argument(
        "--valid-start-date",
        type=str,
        default=DEFAULT_VALID_START_DATE,
        help="Hold out races from this date onward for per-cell evaluation",
    )
    train_cells.add_argument("--model-version", type=str, required=True)
    train_cells.add_argument("--output-root", type=Path, default=None)
    train_cells.add_argument(
        "--output-model-dir", type=Path, dest="output_root", default=None
    )
    train_cells.add_argument("--output-routing-json", type=Path, required=True)
    train_cells.add_argument("--output-metrics-json", type=Path, default=None)
    train_cells.add_argument(
        "--cell-feature-selection-json",
        type=Path,
        default=None,
        help=(
            "cell-routing JSON containing variants[].feature_names; matching cells "
            "train with the adopted feature set instead of the global default"
        ),
    )
    train_cells.add_argument(
        "--min-train-rows", type=int, default=DEFAULT_CELL_MIN_TRAIN_ROWS
    )
    train_cells.add_argument(
        "--min-valid-rows", type=int, default=DEFAULT_CELL_MIN_VALID_ROWS
    )
    train_cells.add_argument(
        "--min-classes", type=int, default=DEFAULT_CELL_MIN_CLASSES
    )
    train_cells.add_argument(
        "--with-field-features",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    train_cells.add_argument(
        "--class-weight-scheme",
        type=str,
        default="inverse_freq",
        choices=["inverse_freq", "balanced2"],
    )
    _add_lgbm_hyperparam_arguments(train_cells)
    return parser.parse_args(argv)


def training_params_from_args(args: argparse.Namespace) -> TrainingParams:
    base = default_training_params()
    base["num_leaves"] = args.num_leaves
    base["learning_rate"] = args.learning_rate
    base["min_child_samples"] = args.min_child_samples
    base["num_iterations"] = args.num_iterations
    base["early_stopping_rounds"] = getattr(
        args, "early_stopping_rounds", DEFAULT_EARLY_STOPPING_ROUNDS
    )
    base["bagging_fraction"] = args.bagging_fraction
    base["bagging_freq"] = args.bagging_freq
    base["feature_fraction"] = args.feature_fraction
    base["lambda_l1"] = args.reg_alpha
    base["lambda_l2"] = args.reg_lambda
    return base


def parse_validation_years(value: str) -> list[int]:
    return [int(token.strip()) for token in value.split(",") if token.strip()]


def run_walk_forward_command(args: argparse.Namespace) -> None:
    started = perf_counter()
    df = load_dataset_parquet(args.csv)
    base_feature_columns = resolve_feature_columns(list(df.columns))
    feature_columns = extend_feature_columns(
        base_feature_columns, args.with_field_features
    )
    categorical_features = detect_categorical_features(feature_columns)
    params = training_params_from_args(args)
    validation_years = parse_validation_years(args.validation_years)
    metrics_per_fold: list[FoldMetrics] = []
    all_predictions: list[pl.DataFrame] = []
    for valid_year in validation_years:
        predictions_df, metrics = run_walk_forward_for_year(
            df,
            valid_year,
            args.train_start_date,
            feature_columns,
            categorical_features,
            params,
            with_field_features=args.with_field_features,
        )
        metrics_per_fold.append(metrics)
        all_predictions.append(predictions_df)
        print(json.dumps({"fold": metrics}, ensure_ascii=False))
    combined = pl.concat(all_predictions, how="diagonal_relaxed")
    range_label = f"{validation_years[0]}-{validation_years[-1]}"
    output_jsonl = args.output_predictions_dir / f"{range_label}.jsonl"
    write_predictions_jsonl(combined, output_jsonl)
    if args.output_report is not None:
        write_walk_forward_report(metrics_per_fold, args.output_report)
    elapsed = perf_counter() - started
    print(
        json.dumps(
            {
                "elapsed_seconds": elapsed,
                "predictions_jsonl": str(output_jsonl),
                "rows": len(combined),
            }
        )
    )


def filter_by_date_range(df: pl.DataFrame, start: str, end: str) -> pl.DataFrame:
    return df.filter((pl.col("race_date") >= start) & (pl.col("race_date") <= end))


def split_production_train_valid(
    train_df: pl.DataFrame,
    valid_start_date: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    race_date = pl.col("race_date").str.to_datetime("%Y%m%d")
    valid_start = pl.lit(valid_start_date).str.to_datetime("%Y%m%d")
    return (
        train_df.filter(race_date < valid_start),
        train_df.filter(race_date >= valid_start),
    )


def train_full_dataset(
    train_df: pl.DataFrame,
    feature_columns: list[str],
    categorical_features: list[str],
    params: TrainingParams,
    *,
    valid_start_date: str | None = None,
    class_weight_scheme: str = "inverse_freq",
) -> lgb.Booster:
    train_subset = filter_labeled_rows(train_df)
    if valid_start_date is not None:
        fit_df, valid_df = split_production_train_valid(train_subset, valid_start_date)
        if len(valid_df) == 0:
            raise ValueError(
                f"production validation split is empty from {valid_start_date}"
            )
        train_weights = resolve_sample_weights(
            fit_df[TARGET_COLUMN], class_weight_scheme
        )
        valid_weights = np.ones(len(valid_df), dtype=np.float64)
        train_dataset = build_lgb_dataset(
            fit_df,
            fit_df[TARGET_COLUMN],
            train_weights,
            feature_columns,
            categorical_features,
        )
        valid_dataset = build_lgb_dataset(
            valid_df,
            valid_df[TARGET_COLUMN],
            valid_weights,
            feature_columns,
            categorical_features,
            reference=train_dataset,
        )
        return lgb.train(
            lgb_params_for_multiclass(params),
            train_dataset,
            num_boost_round=params["num_iterations"],
            valid_sets=[valid_dataset],
            callbacks=[
                lgb.early_stopping(stopping_rounds=params["early_stopping_rounds"]),
                lgb.log_evaluation(period=DEFAULT_VERBOSE_EVAL),
            ],
        )

    train_weights = resolve_sample_weights(
        train_subset[TARGET_COLUMN], class_weight_scheme
    )
    train_dataset = build_lgb_dataset(
        train_subset,
        train_subset[TARGET_COLUMN],
        train_weights,
        feature_columns,
        categorical_features,
    )
    return lgb.train(
        lgb_params_for_multiclass(params),
        train_dataset,
        num_boost_round=params["num_iterations"],
        callbacks=[lgb.log_evaluation(period=DEFAULT_VERBOSE_EVAL)],
    )


def run_walk_forward_eval_for_year(
    df: pl.DataFrame,
    holdout_year: int,
    train_start_date: str,
    feature_columns: list[str],
    categorical_features: list[str],
    params: TrainingParams,
) -> WalkForwardEvalMetrics:
    train_end_date = derive_walk_forward_train_end(holdout_year)
    valid_start, valid_end = derive_walk_forward_valid_range(holdout_year)
    train_df = filter_by_date_range(df, train_start_date, train_end_date)
    valid_df = filter_by_date_range(df, valid_start, valid_end)
    train_labeled = filter_labeled_rows(train_df)
    valid_labeled = filter_labeled_rows(valid_df)
    if len(train_labeled) == 0 or len(valid_labeled) == 0:
        return {
            "holdout_year": holdout_year,
            "train_start_date": train_start_date,
            "train_end_date": train_end_date,
            "train_rows": int(len(train_labeled)),
            "valid_rows": int(len(valid_labeled)),
            "precision_nige": float("nan"),
            "recall_nige": float("nan"),
            "log_loss_nige": float("nan"),
            "multi_log_loss": float("nan"),
            "accuracy": float("nan"),
        }
    _booster, probabilities = train_running_style_head(
        train_labeled,
        valid_labeled,
        feature_columns,
        categorical_features,
        params,
    )
    actual = valid_labeled[TARGET_COLUMN].to_numpy().astype(np.int64)
    return compute_walk_forward_eval_metrics(
        probabilities,
        actual,
        holdout_year,
        train_start_date,
        train_end_date,
        int(len(train_labeled)),
    )


def run_walk_forward_eval_for_windows(
    df: pl.DataFrame,
    windows: list[int],
    train_start_date: str,
    feature_columns: list[str],
    categorical_features: list[str],
    params: TrainingParams,
) -> dict[str, WalkForwardEvalMetrics]:
    results: dict[str, WalkForwardEvalMetrics] = {}
    for year in windows:
        metrics = run_walk_forward_eval_for_year(
            df,
            year,
            train_start_date,
            feature_columns,
            categorical_features,
            params,
        )
        results[str(year)] = metrics
        print(json.dumps({"walk_forward_eval": metrics}, ensure_ascii=False))
    return results


def compute_production_precision_nige(
    booster: lgb.Booster,
    train_subset_full: pl.DataFrame,
    feature_columns: list[str],
    categorical_features: list[str],
    valid_start_date: str,
) -> float:
    labeled = filter_labeled_rows(train_subset_full)
    _, valid_df = split_production_train_valid(labeled, valid_start_date)
    if len(valid_df) == 0:
        return float("nan")
    probabilities = predict_softmax(
        booster, valid_df, feature_columns, categorical_features
    )
    predicted = compute_predicted_labels(probabilities)
    actual = valid_df[TARGET_COLUMN].to_numpy().astype(np.int64)
    precision, _, _ = compute_per_class_precision_recall(predicted, actual)
    return precision["nige"]


def emit_walk_forward_warnings(
    walk_forward_results: dict[str, WalkForwardEvalMetrics],
    production_precision_nige: float,
) -> list[str]:
    warnings: list[str] = []
    for year_key, metrics in walk_forward_results.items():
        if detect_walk_forward_precision_regression(
            metrics["precision_nige"],
            production_precision_nige,
        ):
            gap_pp = (production_precision_nige - metrics["precision_nige"]) * 100.0
            message = (
                f"WARNING: walk-forward {year_key} precision_nige="
                f"{metrics['precision_nige']:.2f} vs production fit "
                f"{production_precision_nige:.2f} (-{gap_pp:.0f}pp). "
                f"Train leakage suspected."
            )
            warnings.append(message)
            print(message)
    return warnings


def write_model_metadata(
    output_dir: Path,
    model_version: str,
    feature_columns: list[str],
    categorical_features: list[str],
    train_rows: int,
    train_start: str,
    train_end: str,
    *,
    with_field_features: bool,
    walk_forward_results: dict[str, WalkForwardEvalMetrics] | None = None,
    production_precision_nige: float | None = None,
    hyperparameters: TrainingParams | None = None,
    class_weight_scheme: str | None = None,
) -> None:
    metadata: dict[str, object] = {
        "model_version": model_version,
        "num_classes": NUM_CLASSES,
        "class_labels": list(CLASS_LABELS),
        "feature_columns": feature_columns,
        "categorical_features": categorical_features,
        "train_rows": train_rows,
        "train_start_date": train_start,
        "train_end_date": train_end,
        "feature_schema_version": "v2" if with_field_features else "v1",
    }
    if hyperparameters is not None:
        metadata["hyperparameters"] = dict(hyperparameters)
    if class_weight_scheme is not None:
        metadata["class_weight_scheme"] = class_weight_scheme
    if walk_forward_results is not None:
        metadata["walk_forward_results"] = walk_forward_results
    if production_precision_nige is not None and not np.isnan(
        production_precision_nige
    ):
        metadata["production_precision_nige"] = production_precision_nige
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def run_train_production_command(args: argparse.Namespace) -> None:
    started = perf_counter()
    df = load_dataset_parquet(args.csv)
    df = maybe_enrich_with_field_features(df, args.with_field_features)
    base_feature_columns = resolve_feature_columns(list(df.columns))
    feature_columns = extend_feature_columns(
        base_feature_columns, args.with_field_features
    )
    categorical_features = detect_categorical_features(feature_columns)
    params = training_params_from_args(args)
    train_subset_full = filter_by_date_range(
        df, args.train_start_date, args.train_end_date
    )
    walk_forward_results: dict[str, WalkForwardEvalMetrics] | None = None
    if args.enable_walk_forward_eval:
        windows = parse_walk_forward_windows(args.walk_forward_windows)
        walk_forward_results = run_walk_forward_eval_for_windows(
            df,
            windows,
            args.train_start_date,
            feature_columns,
            categorical_features,
            params,
        )
    booster = train_full_dataset(
        train_subset_full,
        feature_columns,
        categorical_features,
        params,
        valid_start_date=args.valid_start_date,
        class_weight_scheme=args.class_weight_scheme,
    )
    production_precision_nige: float | None = None
    if walk_forward_results is not None:
        production_precision_nige = compute_production_precision_nige(
            booster,
            train_subset_full,
            feature_columns,
            categorical_features,
            args.valid_start_date,
        )
        emit_walk_forward_warnings(walk_forward_results, production_precision_nige)
    args.output_model_dir.mkdir(parents=True, exist_ok=True)
    model_path = args.output_model_dir / "model.txt"
    booster.save_model(str(model_path))
    write_model_metadata(
        args.output_model_dir,
        args.model_version,
        feature_columns,
        categorical_features,
        int(len(filter_labeled_rows(train_subset_full))),
        args.train_start_date,
        args.train_end_date,
        with_field_features=args.with_field_features,
        walk_forward_results=walk_forward_results,
        production_precision_nige=production_precision_nige,
        hyperparameters=params,
        class_weight_scheme=args.class_weight_scheme,
    )
    elapsed = perf_counter() - started
    print(
        json.dumps(
            {
                "elapsed_seconds": elapsed,
                "model_path": str(model_path),
                "rows": int(len(train_subset_full)),
            }
        )
    )


def unique_running_style_cells(df: pl.DataFrame) -> list[RunningStyleCellKey]:
    records = df.select(CELL_DERIVED_COLUMNS).unique().iter_rows(named=True)
    cells = [cell_key_from_derived_record(dict(record)) for record in records]
    return sorted(cells)


def eligibility_rejections(
    train_df: pl.DataFrame,
    valid_df: pl.DataFrame,
    *,
    min_train_rows: int,
    min_valid_rows: int,
    min_classes: int,
) -> list[str]:
    reasons: list[str] = []
    if len(train_df) < min_train_rows:
        reasons.append(f"train_rows {len(train_df)} < {min_train_rows}")
    if len(valid_df) < min_valid_rows:
        reasons.append(f"valid_rows {len(valid_df)} < {min_valid_rows}")
    train_classes = int(train_df[TARGET_COLUMN].n_unique()) if len(train_df) > 0 else 0
    if train_classes < min_classes:
        reasons.append(f"train_classes {train_classes} < {min_classes}")
    valid_classes = int(valid_df[TARGET_COLUMN].n_unique()) if len(valid_df) > 0 else 0
    if valid_classes < min_classes:
        reasons.append(f"valid_classes {valid_classes} < {min_classes}")
    return reasons


def write_cell_model_metadata(
    output_dir: Path,
    *,
    model_version: str,
    cell: RunningStyleCellKey,
    variant_id: str,
    model_key: str,
    conditions: list[dict[str, object]],
    feature_columns: list[str],
    feature_set_hash: str,
    categorical_features: list[str],
    train_rows: int,
    valid_rows: int,
    train_start_date: str,
    train_end_date: str,
    valid_start_date: str,
    with_field_features: bool,
    metrics: RunningStyleCellMetrics,
    hyperparameters: TrainingParams,
    class_weight_scheme: str,
) -> None:
    metadata = {
        "model_version": model_version,
        "variant_id": variant_id,
        "model_key": model_key,
        "cell": asdict(cell),
        "conditions": conditions,
        "num_classes": NUM_CLASSES,
        "class_labels": list(CLASS_LABELS),
        "feature_columns": feature_columns,
        "feature_set_hash": feature_set_hash,
        "categorical_features": categorical_features,
        "train_rows": train_rows,
        "valid_rows": valid_rows,
        "train_start_date": train_start_date,
        "train_end_date": train_end_date,
        "valid_start_date": valid_start_date,
        "feature_schema_version": "v2" if with_field_features else "v1",
        "metrics": metrics,
        "hyperparameters": dict(hyperparameters),
        "class_weight_scheme": class_weight_scheme,
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(json_ready(metadata), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def train_one_running_style_cell(
    cell: RunningStyleCellKey,
    fit_df: pl.DataFrame,
    valid_df: pl.DataFrame,
    feature_columns: list[str],
    feature_set_hash: str,
    categorical_features: list[str],
    params: TrainingParams,
    args: argparse.Namespace,
    variant_id: str,
) -> dict[str, object]:
    booster, probabilities = train_running_style_head(
        fit_df,
        valid_df,
        feature_columns,
        categorical_features,
        params,
        class_weight_scheme=args.class_weight_scheme,
    )
    actual = valid_df[TARGET_COLUMN].to_numpy().astype(np.int64)
    metrics = compute_running_style_metrics(
        probabilities,
        actual,
        race_ids=valid_df["race_id"].to_numpy() if "race_id" in valid_df.columns else None,
        corner1_norm=(
            valid_df["target_corner_1_norm"].to_numpy()
            if "target_corner_1_norm" in valid_df.columns
            else None
        ),
        finish_positions=(
            valid_df["finish_position"].to_numpy()
            if "finish_position" in valid_df.columns
            else None
        ),
    )
    race_count = (
        int(valid_df["race_id"].n_unique())
        if "race_id" in valid_df.columns
        else int(len(valid_df))
    )
    adoption_metrics = running_style_cell_metrics_for_adoption(
        cell,
        metrics,
        feature_set_hash=feature_set_hash,
        race_count=race_count,
    )
    conditions = cell_conditions(cell)
    model_key = build_cell_model_key(cell.category, args.model_version, variant_id)
    output_dir = args.output_root / cell.category / variant_id
    output_dir.mkdir(parents=True, exist_ok=True)
    model_path = output_dir / "model.txt"
    booster.save_model(str(model_path))
    write_cell_model_metadata(
        output_dir,
        model_version=args.model_version,
        cell=cell,
        variant_id=variant_id,
        model_key=model_key,
        conditions=conditions,
        feature_columns=feature_columns,
        feature_set_hash=feature_set_hash,
        categorical_features=categorical_features,
        train_rows=int(len(fit_df)),
        valid_rows=int(len(valid_df)),
        train_start_date=args.train_start_date,
        train_end_date=args.train_end_date,
        valid_start_date=args.valid_start_date,
        with_field_features=args.with_field_features,
        metrics=metrics,
        hyperparameters=params,
        class_weight_scheme=args.class_weight_scheme,
    )
    return {
        "category": cell.category,
        "variant_id": variant_id,
        "model_key": model_key,
        "model_path": str(model_path),
        "metadata_path": str(output_dir / "metadata.json"),
        "cell": asdict(cell),
        "conditions": conditions,
        "feature_set_hash": feature_set_hash,
        "feature_columns": feature_columns,
        "feature_count": len(feature_columns),
        "train_rows": int(len(fit_df)),
        "valid_rows": int(len(valid_df)),
        "valid_race_count": race_count,
        "metrics": metrics,
        "cell_training_evaluation": adoption_metrics,
    }


def reserve_variant_id(cell: RunningStyleCellKey, used: set[str]) -> str:
    base = build_cell_variant_id(cell)
    variant_id = base
    suffix = 2
    while variant_id in used:
        variant_id = f"{base}-{suffix}"
        suffix += 1
    used.add(variant_id)
    return variant_id


def run_train_cells_command(args: argparse.Namespace) -> None:
    started = perf_counter()
    if args.output_root is None:
        raise ValueError("train-cells requires --output-root or --output-model-dir")
    output_metrics_json = (
        args.output_metrics_json
        if args.output_metrics_json is not None
        else args.output_root / "cell_metrics.json"
    )
    df = load_dataset_parquet(args.csv)
    df = maybe_enrich_with_field_features(df, args.with_field_features)
    base_feature_columns = resolve_feature_columns(list(df.columns))
    feature_columns = extend_feature_columns(
        base_feature_columns, args.with_field_features
    )
    default_feature_set_hash = compute_feature_set_hash(feature_columns)
    feature_selection_rules = load_cell_feature_selection_rules(
        args.cell_feature_selection_json
    )
    params = training_params_from_args(args)
    train_subset_full = filter_by_date_range(
        df, args.train_start_date, args.train_end_date
    )
    labeled = filter_labeled_rows(train_subset_full)
    with_cells = attach_running_style_cell_columns(labeled)
    fit_df, valid_df = split_production_train_valid(with_cells, args.valid_start_date)

    trained_cells: list[dict[str, object]] = []
    skipped_cells: list[dict[str, object]] = []
    used_variant_ids: set[str] = set()
    for cell in unique_running_style_cells(with_cells):
        cell_expr = build_cell_filter_expr(cell)
        cell_fit_df = fit_df.filter(cell_expr)
        cell_valid_df = valid_df.filter(cell_expr)
        rejections = eligibility_rejections(
            cell_fit_df,
            cell_valid_df,
            min_train_rows=args.min_train_rows,
            min_valid_rows=args.min_valid_rows,
            min_classes=args.min_classes,
        )
        if rejections:
            skipped_cells.append(
                {
                    "cell": asdict(cell),
                    "train_rows": int(len(cell_fit_df)),
                    "valid_rows": int(len(cell_valid_df)),
                    "rejections": rejections,
                }
            )
            continue
        variant_id = reserve_variant_id(cell, used_variant_ids)
        cell_feature_columns, cell_feature_set_hash = resolve_cell_feature_selection(
            cell, feature_columns, feature_selection_rules
        )
        cell_categorical_features = detect_categorical_features(cell_feature_columns)
        trained = train_one_running_style_cell(
            cell,
            cell_fit_df,
            cell_valid_df,
            cell_feature_columns,
            cell_feature_set_hash,
            cell_categorical_features,
            params,
            args,
            variant_id,
        )
        trained_cells.append(trained)
        print(json.dumps({"trained_cell": json_ready(trained)}, ensure_ascii=False))

    routing = build_running_style_cell_routing_config(trained_cells)
    args.output_routing_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_routing_json.write_text(
        json.dumps(json_ready(routing), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    metrics_payload = {
        "model_version": args.model_version,
        "train_start_date": args.train_start_date,
        "train_end_date": args.train_end_date,
        "valid_start_date": args.valid_start_date,
        "min_train_rows": args.min_train_rows,
        "min_valid_rows": args.min_valid_rows,
        "min_classes": args.min_classes,
        "feature_columns": feature_columns,
        "default_feature_set_hash": default_feature_set_hash,
        "cell_feature_selection_json": (
            str(args.cell_feature_selection_json)
            if args.cell_feature_selection_json is not None
            else None
        ),
        "cell_feature_selection_rule_count": len(feature_selection_rules),
        "categorical_features": detect_categorical_features(feature_columns),
        "hyperparameters": dict(params),
        "class_weight_scheme": args.class_weight_scheme,
        "trained_cells": trained_cells,
        "skipped_cells": skipped_cells,
        "aggregate": {
            "trained_count": len(trained_cells),
            "skipped_count": len(skipped_cells),
        },
    }
    output_metrics_json.parent.mkdir(parents=True, exist_ok=True)
    output_metrics_json.write_text(
        json.dumps(json_ready(metrics_payload), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    elapsed = perf_counter() - started
    print(
        json.dumps(
            {
                "elapsed_seconds": elapsed,
                "trained_cells": len(trained_cells),
                "skipped_cells": len(skipped_cells),
                "routing_json": str(args.output_routing_json),
                "metrics_json": str(output_metrics_json),
            }
        )
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "walk-forward":
        run_walk_forward_command(args)
    if args.command == "train-production":
        run_train_production_command(args)
    if args.command == "train-cells":
        run_train_cells_command(args)


if __name__ == "__main__":
    main()
