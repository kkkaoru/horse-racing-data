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


def load_dataset_parquet(path: Path) -> pd.DataFrame:
    if path.is_dir():
        partitioned = sorted(path.glob("race_year=*/*.parquet"))
        if partitioned:
            return pd.concat([pd.read_parquet(child) for child in partitioned], ignore_index=True)
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


def train_running_style_head(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_columns: list[str],
    categorical_features: list[str],
    params: TrainingParams,
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
) -> tuple[pd.DataFrame, FoldMetrics]:
    train_df, valid_df = split_by_year(df, train_start, valid_year)
    _booster, probabilities = train_running_style_head(
        train_df, valid_df, feature_columns, categorical_features, params
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


def write_predictions_jsonl(predictions: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in predictions.to_dict(orient="records"):
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
    return parser.parse_args(argv)


def training_params_from_args(args: argparse.Namespace) -> TrainingParams:
    base = default_training_params()
    base["num_leaves"] = args.num_leaves
    base["learning_rate"] = args.learning_rate
    base["min_child_samples"] = args.min_child_samples
    base["num_iterations"] = args.num_iterations
    base["early_stopping_rounds"] = args.early_stopping_rounds
    return base


def parse_validation_years(value: str) -> list[int]:
    return [int(token.strip()) for token in value.split(",") if token.strip()]


def run_walk_forward_command(args: argparse.Namespace) -> None:
    started = perf_counter()
    df = load_dataset_parquet(args.csv)
    feature_columns = resolve_feature_columns(list(df.columns))
    categorical_features = detect_categorical_features(feature_columns)
    params = training_params_from_args(args)
    validation_years = parse_validation_years(args.validation_years)
    metrics_per_fold: list[FoldMetrics] = []
    all_predictions: list[pd.DataFrame] = []
    for valid_year in validation_years:
        predictions_df, metrics = run_walk_forward_for_year(
            df, valid_year, args.train_start_date, feature_columns, categorical_features, params
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


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "walk-forward":
        run_walk_forward_command(args)


if __name__ == "__main__":
    main()
