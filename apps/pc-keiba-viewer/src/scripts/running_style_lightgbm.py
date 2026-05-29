#!/usr/bin/env python3
# pyright: reportUnknownParameterType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportMissingParameterType=false
"""Multiclass softmax LightGBM for running-style prediction (nige/senkou/sashi/oikomi).

Trains a single 4-class softmax head on target_running_style_class with
inverse-frequency sample weights (class imbalance correction). Reads the
Phase A parquet produced by finish_position_features_duckdb.py.

Run with:
  cd src/scripts && ../../.venv/bin/python -m running_style_lightgbm walk-forward \\
    --csv ../../tmp/finish-position-features-parquet-jra-v4 \\
    --train-start-date 20160101 \\
    --validation-years 2024,2025 \\
    --output-predictions-dir ../../tmp/finish-position-eval/predictions-jra/running-style-lgbm
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter
from typing import TypedDict

import lightgbm as lgb
import numpy as np
import pandas as pd

from running_style_field_features import FIELD_FEATURE_COLUMNS, enrich_dataframe_with_field_features

META_COLUMNS: tuple[str, ...] = (
    "source",
    "race_date",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
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
PROBABILITY_COLUMNS: tuple[str, str, str, str] = ("p_nige", "p_senkou", "p_sashi", "p_oikomi")

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
    excluded = set(META_COLUMNS) | set(LABEL_COLUMNS)
    return [column for column in df_columns if column not in excluded]


def detect_categorical_features(feature_columns: list[str]) -> list[str]:
    return [column for column in feature_columns if column in CATEGORICAL_FEATURE_COLUMNS]


def _read_partitioned_parquet(child: Path) -> pd.DataFrame:
    frame = pd.read_parquet(child)
    if "race_year" not in frame.columns:
        year_token = child.parent.name
        if year_token.startswith("race_year="):
            frame["race_year"] = int(year_token.split("=", 1)[1])
    return frame


def load_dataset_parquet(path: Path) -> pd.DataFrame:
    if path.is_dir():
        partitioned = sorted(path.glob("race_year=*/*.parquet"))
        if partitioned:
            return pd.concat(
                [_read_partitioned_parquet(child) for child in partitioned], ignore_index=True
            )
        flat = sorted(path.glob("*.parquet"))
        if flat:
            return pd.concat([pd.read_parquet(child) for child in flat], ignore_index=True)
        raise ValueError(f"No parquet files found under {path}")
    return pd.read_parquet(path)


def encode_categoricals(frame: pd.DataFrame, categorical_features: list[str]) -> pd.DataFrame:
    encoded = frame.copy()
    for column in categorical_features:
        if column in encoded.columns:
            encoded[column] = encoded[column].astype("category")
    return encoded


def filter_labeled_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df[df[TARGET_COLUMN].notna()].reset_index(drop=True)


def split_by_year(df: pd.DataFrame, train_start: str, valid_year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    train_start_date = pd.to_datetime(train_start)
    race_date = pd.to_datetime(df["race_date"], format="%Y%m%d")
    train_mask = (race_date >= train_start_date) & (df["race_year"] < valid_year)
    valid_mask = df["race_year"] == valid_year
    return df[train_mask].reset_index(drop=True), df[valid_mask].reset_index(drop=True)


def compute_inverse_frequency_weights(labels: pd.Series) -> np.ndarray:
    label_array = labels.to_numpy(dtype=np.int64)
    class_counts = np.bincount(label_array, minlength=NUM_CLASSES).astype(np.float64)
    safe_counts = np.where(class_counts == 0, 1.0, class_counts)
    inverse_frequencies = label_array.size / (NUM_CLASSES * safe_counts)
    return inverse_frequencies[label_array]


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


def build_lgb_dataset(
    frame: pd.DataFrame,
    labels: pd.Series,
    sample_weights: np.ndarray,
    feature_columns: list[str],
    categorical_features: list[str],
    reference: lgb.Dataset | None = None,
) -> lgb.Dataset:
    feature_frame = encode_categoricals(frame[feature_columns], categorical_features)
    return lgb.Dataset(
        feature_frame,
        label=labels.to_numpy(dtype=np.int64),
        weight=sample_weights,
        categorical_feature=categorical_features if categorical_features else "auto",
        free_raw_data=False,
        reference=reference,
    )


def predict_softmax(
    booster: lgb.Booster,
    frame: pd.DataFrame,
    feature_columns: list[str],
    categorical_features: list[str],
) -> np.ndarray:
    feature_frame = encode_categoricals(frame[feature_columns], categorical_features)
    raw = booster.predict(feature_frame, num_iteration=booster.best_iteration)
    return np.asarray(raw, dtype=np.float64)


def compute_predicted_labels(probabilities: np.ndarray) -> np.ndarray:
    return np.argmax(probabilities, axis=1)


def apply_race_level_nige_constraint(
    df: pd.DataFrame,
    probabilities: np.ndarray,
    *,
    nige_class_index: int = 0,
    min_nige_probability: float = 0.18,
) -> np.ndarray:
    """Ensure at most one horse per race is labeled nige by suppressing weaker nige picks."""
    adjusted = probabilities.copy()
    if "race_id" not in df.columns:
        return adjusted

    for _, race_df in df.groupby("race_id", sort=False):
        indices = race_df.index.to_numpy()
        if indices.size <= 1:
            continue
        race_probs = adjusted[indices]
        nige_probs = race_probs[:, nige_class_index]
        top_index = int(np.argmax(nige_probs))
        if nige_probs[top_index] < min_nige_probability:
            race_probs[:, nige_class_index] = 0.0
        else:
            for row_index in range(indices.size):
                if row_index != top_index:
                    race_probs[row_index, nige_class_index] = 0.0
        row_sums = race_probs.sum(axis=1, keepdims=True)
        safe_sums = np.where(row_sums <= 0, 1.0, row_sums)
        adjusted[indices] = race_probs / safe_sums
    return adjusted


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
        precision[class_name] = float(tp / predicted_count) if predicted_count > 0 else float("nan")
        recall[class_name] = float(tp / actual_count) if actual_count > 0 else float("nan")
        support[class_name] = actual_count
    return precision, recall, support


def macro_f1_from_precision_recall(precision: dict[str, float], recall: dict[str, float]) -> float:
    f1_scores: list[float] = []
    for class_name in CLASS_LABELS:
        p = precision[class_name]
        r = recall[class_name]
        if np.isnan(p) or np.isnan(r) or (p + r) == 0:
            continue
        f1_scores.append(2.0 * p * r / (p + r))
    if not f1_scores:
        return float("nan")
    return float(np.mean(f1_scores))


def maybe_enrich_with_field_features(df: pd.DataFrame, enabled: bool) -> pd.DataFrame:
    if not enabled:
        return df
    return enrich_dataframe_with_field_features(df)


def extend_feature_columns(feature_columns: list[str], with_field_features: bool) -> list[str]:
    if not with_field_features:
        return feature_columns
    merged = list(feature_columns)
    for column in FIELD_FEATURE_COLUMNS:
        if column not in merged:
            merged.append(column)
    return merged


def train_running_style_head(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_columns: list[str],
    categorical_features: list[str],
    params: TrainingParams,
    *,
    apply_nige_constraint: bool,
) -> tuple[lgb.Booster, np.ndarray]:
    train_subset = filter_labeled_rows(train_df)
    valid_subset = filter_labeled_rows(valid_df)
    train_weights = compute_inverse_frequency_weights(train_subset[TARGET_COLUMN])
    valid_weights = np.ones(len(valid_subset), dtype=np.float64)
    train_dataset = build_lgb_dataset(
        train_subset, train_subset[TARGET_COLUMN], train_weights,
        feature_columns, categorical_features,
    )
    valid_dataset = build_lgb_dataset(
        valid_subset, valid_subset[TARGET_COLUMN], valid_weights,
        feature_columns, categorical_features, reference=train_dataset,
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
    probabilities = predict_softmax(booster, valid_df, feature_columns, categorical_features)
    if apply_nige_constraint:
        probabilities = apply_race_level_nige_constraint(valid_df, probabilities)
    return booster, probabilities


def build_predictions_df(valid_df: pd.DataFrame, probabilities: np.ndarray) -> pd.DataFrame:
    output = valid_df[
        ["race_id", "ketto_toroku_bango", "umaban", "race_year", TARGET_COLUMN]
    ].copy()
    for class_idx, column_name in enumerate(PROBABILITY_COLUMNS):
        output[column_name] = probabilities[:, class_idx]
    predicted_indices = compute_predicted_labels(probabilities)
    output["predicted_label"] = [CLASS_LABELS[int(idx)] for idx in predicted_indices]
    output["predicted_class"] = predicted_indices.astype(int)
    return output


def run_walk_forward_for_year(
    df: pd.DataFrame,
    valid_year: int,
    train_start: str,
    feature_columns: list[str],
    categorical_features: list[str],
    params: TrainingParams,
    *,
    with_field_features: bool,
    apply_nige_constraint: bool,
) -> tuple[pd.DataFrame, FoldMetrics]:
    train_df, valid_df = split_by_year(df, train_start, valid_year)
    train_df = maybe_enrich_with_field_features(train_df, with_field_features)
    valid_df = maybe_enrich_with_field_features(valid_df, with_field_features)
    _booster, probabilities = train_running_style_head(
        train_df,
        valid_df,
        feature_columns,
        categorical_features,
        params,
        apply_nige_constraint=apply_nige_constraint,
    )
    predictions_df = build_predictions_df(valid_df, probabilities)
    evaluation_subset = predictions_df.dropna(subset=[TARGET_COLUMN])
    predicted = evaluation_subset["predicted_class"].to_numpy(dtype=np.int64)
    actual = evaluation_subset[TARGET_COLUMN].to_numpy(dtype=np.int64)
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


def write_predictions_jsonl(predictions: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for raw_record in predictions.to_dict(orient="records"):
            record = _sanitize_record_for_json({str(k): v for k, v in raw_record.items()})
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_walk_forward_report(metrics_per_fold: list[FoldMetrics], output_path: Path) -> None:
    aggregate = {
        "accuracy_mean": float(np.nanmean([fold["accuracy"] for fold in metrics_per_fold])),
        "macro_f1_mean": float(np.nanmean([fold["macro_f1"] for fold in metrics_per_fold])),
    }
    payload = {"folds": metrics_per_fold, "aggregate": aggregate}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="running_style_lightgbm")
    subparsers = parser.add_subparsers(dest="command", required=True)
    walk = subparsers.add_parser("walk-forward")
    walk.add_argument("--csv", type=Path, required=True, help="parquet directory or file")
    walk.add_argument("--train-start-date", type=str, default="20160101")
    walk.add_argument("--validation-years", type=str, default="2024,2025")
    walk.add_argument("--output-predictions-dir", type=Path, required=True)
    walk.add_argument("--output-report", type=Path, default=None)
    walk.add_argument("--num-leaves", type=int, default=DEFAULT_NUM_LEAVES)
    walk.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    walk.add_argument("--min-child-samples", type=int, default=DEFAULT_MIN_CHILD_SAMPLES)
    walk.add_argument("--num-iterations", type=int, default=DEFAULT_NUM_ITERATIONS)
    walk.add_argument("--early-stopping-rounds", type=int, default=DEFAULT_EARLY_STOPPING_ROUNDS)
    walk.add_argument(
        "--with-field-features",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Compute race-internal field_* features before training/prediction",
    )
    walk.add_argument(
        "--race-level-nige-constraint",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Allow at most one nige label per race at inference time",
    )
    train_prod = subparsers.add_parser("train-production")
    train_prod.add_argument("--csv", type=Path, required=True, help="parquet directory or file")
    train_prod.add_argument("--train-start-date", type=str, default="20160101")
    train_prod.add_argument("--train-end-date", type=str, required=True, help="YYYYMMDD inclusive")
    train_prod.add_argument("--model-version", type=str, required=True)
    train_prod.add_argument("--output-model-dir", type=Path, required=True)
    train_prod.add_argument("--num-leaves", type=int, default=DEFAULT_NUM_LEAVES)
    train_prod.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    train_prod.add_argument("--min-child-samples", type=int, default=DEFAULT_MIN_CHILD_SAMPLES)
    train_prod.add_argument("--num-iterations", type=int, default=DEFAULT_NUM_ITERATIONS)
    train_prod.add_argument("--early-stopping-rounds", type=int, default=DEFAULT_EARLY_STOPPING_ROUNDS)
    train_prod.add_argument(
        "--valid-start-date",
        type=str,
        default="20250101",
        help="Hold out races from this date onward for production early stopping",
    )
    train_prod.add_argument(
        "--with-field-features",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    train_prod.add_argument(
        "--race-level-nige-constraint",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    return parser.parse_args(argv)


def training_params_from_args(args: argparse.Namespace) -> TrainingParams:
    base = default_training_params()
    base["num_leaves"] = args.num_leaves
    base["learning_rate"] = args.learning_rate
    base["min_child_samples"] = args.min_child_samples
    base["num_iterations"] = args.num_iterations
    base["early_stopping_rounds"] = getattr(args, "early_stopping_rounds", DEFAULT_EARLY_STOPPING_ROUNDS)
    return base


def parse_validation_years(value: str) -> list[int]:
    return [int(token.strip()) for token in value.split(",") if token.strip()]


def run_walk_forward_command(args: argparse.Namespace) -> None:
    started = perf_counter()
    df = load_dataset_parquet(args.csv)
    base_feature_columns = resolve_feature_columns(list(df.columns))
    feature_columns = extend_feature_columns(base_feature_columns, args.with_field_features)
    categorical_features = detect_categorical_features(feature_columns)
    params = training_params_from_args(args)
    validation_years = parse_validation_years(args.validation_years)
    metrics_per_fold: list[FoldMetrics] = []
    all_predictions: list[pd.DataFrame] = []
    for valid_year in validation_years:
        predictions_df, metrics = run_walk_forward_for_year(
            df,
            valid_year,
            args.train_start_date,
            feature_columns,
            categorical_features,
            params,
            with_field_features=args.with_field_features,
            apply_nige_constraint=args.race_level_nige_constraint,
        )
        metrics_per_fold.append(metrics)
        all_predictions.append(predictions_df)
        print(json.dumps({"fold": metrics}, ensure_ascii=False))
    combined = pd.concat(all_predictions, ignore_index=True)
    range_label = f"{validation_years[0]}-{validation_years[-1]}"
    output_jsonl = args.output_predictions_dir / f"{range_label}.jsonl"
    write_predictions_jsonl(combined, output_jsonl)
    if args.output_report is not None:
        write_walk_forward_report(metrics_per_fold, args.output_report)
    elapsed = perf_counter() - started
    print(json.dumps({"elapsed_seconds": elapsed, "predictions_jsonl": str(output_jsonl), "rows": len(combined)}))


def filter_by_date_range(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    return df[(df["race_date"] >= start) & (df["race_date"] <= end)].copy()


def split_production_train_valid(
    train_df: pd.DataFrame,
    valid_start_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid_start = pd.to_datetime(valid_start_date)
    race_date = pd.to_datetime(train_df["race_date"], format="%Y%m%d")
    return (
        train_df[race_date < valid_start].reset_index(drop=True),
        train_df[race_date >= valid_start].reset_index(drop=True),
    )


def train_full_dataset(
    train_df: pd.DataFrame,
    feature_columns: list[str],
    categorical_features: list[str],
    params: TrainingParams,
    *,
    valid_start_date: str | None = None,
) -> lgb.Booster:
    train_subset = filter_labeled_rows(train_df)
    if valid_start_date is not None:
        fit_df, valid_df = split_production_train_valid(train_subset, valid_start_date)
        if len(valid_df) == 0:
            raise ValueError(f"production validation split is empty from {valid_start_date}")
        train_weights = compute_inverse_frequency_weights(fit_df[TARGET_COLUMN])
        valid_weights = np.ones(len(valid_df), dtype=np.float64)
        train_dataset = build_lgb_dataset(
            fit_df, fit_df[TARGET_COLUMN], train_weights,
            feature_columns, categorical_features,
        )
        valid_dataset = build_lgb_dataset(
            valid_df, valid_df[TARGET_COLUMN], valid_weights,
            feature_columns, categorical_features, reference=train_dataset,
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

    train_weights = compute_inverse_frequency_weights(train_subset[TARGET_COLUMN])
    train_dataset = build_lgb_dataset(
        train_subset, train_subset[TARGET_COLUMN], train_weights,
        feature_columns, categorical_features,
    )
    return lgb.train(
        lgb_params_for_multiclass(params),
        train_dataset,
        num_boost_round=params["num_iterations"],
        callbacks=[lgb.log_evaluation(period=DEFAULT_VERBOSE_EVAL)],
    )


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
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8",
    )


def run_train_production_command(args: argparse.Namespace) -> None:
    started = perf_counter()
    df = load_dataset_parquet(args.csv)
    df = maybe_enrich_with_field_features(df, args.with_field_features)
    base_feature_columns = resolve_feature_columns(list(df.columns))
    feature_columns = extend_feature_columns(base_feature_columns, args.with_field_features)
    categorical_features = detect_categorical_features(feature_columns)
    params = training_params_from_args(args)
    train_subset_full = filter_by_date_range(df, args.train_start_date, args.train_end_date)
    booster = train_full_dataset(
        train_subset_full,
        feature_columns,
        categorical_features,
        params,
        valid_start_date=args.valid_start_date,
    )
    args.output_model_dir.mkdir(parents=True, exist_ok=True)
    model_path = args.output_model_dir / "model.txt"
    booster.save_model(str(model_path))
    write_model_metadata(
        args.output_model_dir, args.model_version, feature_columns, categorical_features,
        int(len(filter_labeled_rows(train_subset_full))),
        args.train_start_date, args.train_end_date,
        with_field_features=args.with_field_features,
    )
    elapsed = perf_counter() - started
    print(json.dumps({
        "elapsed_seconds": elapsed,
        "model_path": str(model_path),
        "rows": int(len(train_subset_full)),
    }))


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "walk-forward":
        run_walk_forward_command(args)
    if args.command == "train-production":
        run_train_production_command(args)


if __name__ == "__main__":
    main()
