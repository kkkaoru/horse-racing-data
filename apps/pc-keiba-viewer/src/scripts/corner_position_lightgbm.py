#!/usr/bin/env python3
# pyright: reportUnknownParameterType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportMissingParameterType=false
"""3-head regression for corner-position prediction (corner_1/3/4).

Trains independent LightGBM regressors per corner head with native
categorical features and walk-forward CV. Reads the Phase A parquet
produced by finish_position_features_duckdb.py.

Run with:
  cd src/scripts && ../../.venv/bin/python -m corner_position_lightgbm walk-forward \\
    --csv ../../tmp/finish-position-features-parquet-jra-v4 \\
    --train-start-date 20160101 \\
    --validation-years 2024,2025 \\
    --output-predictions-dir ../../tmp/finish-position-eval/predictions-jra/corner-lgbm
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

CORNER_HEAD_TARGETS: dict[str, str] = {
    "corner_1": "target_corner_1_norm",
    "corner_3": "target_corner_3_norm",
    "corner_4": "target_corner_4_norm",
}

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
TOP_K_AGREEMENT = 3


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
    per_head_mae: dict[str, float]
    corner_1_top3_agreement: float


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


def filter_target_rows(df: pd.DataFrame, target_column: str) -> pd.DataFrame:
    return df[df[target_column].notna()].reset_index(drop=True)


def split_by_year(df: pd.DataFrame, train_start: str, valid_year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    train_start_date = pd.to_datetime(train_start)
    race_date = pd.to_datetime(df["race_date"], format="%Y%m%d")
    train_mask = (race_date >= train_start_date) & (df["race_year"] < valid_year)
    valid_mask = df["race_year"] == valid_year
    return df[train_mask].reset_index(drop=True), df[valid_mask].reset_index(drop=True)


def build_lgb_dataset(
    frame: pd.DataFrame,
    label: pd.Series,
    feature_columns: list[str],
    categorical_features: list[str],
    reference: lgb.Dataset | None = None,
) -> lgb.Dataset:
    feature_frame = encode_categoricals(frame[feature_columns], categorical_features)
    return lgb.Dataset(
        feature_frame,
        label=label.to_numpy(dtype=np.float64),
        categorical_feature=categorical_features if categorical_features else "auto",
        free_raw_data=False,
        reference=reference,
    )


def lgb_params_for_regression(params: TrainingParams) -> dict[str, object]:
    return {
        "objective": "regression",
        "metric": "l1",
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


def train_head(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    target_column: str,
    feature_columns: list[str],
    categorical_features: list[str],
    params: TrainingParams,
) -> tuple[lgb.Booster, np.ndarray]:
    train_subset = filter_target_rows(train_df, target_column)
    valid_subset = filter_target_rows(valid_df, target_column)
    train_dataset = build_lgb_dataset(
        train_subset, train_subset[target_column], feature_columns, categorical_features
    )
    valid_dataset = build_lgb_dataset(
        valid_subset, valid_subset[target_column], feature_columns, categorical_features,
        reference=train_dataset,
    )
    booster = lgb.train(
        lgb_params_for_regression(params),
        train_dataset,
        num_boost_round=params["num_iterations"],
        valid_sets=[valid_dataset],
        callbacks=[
            lgb.early_stopping(stopping_rounds=params["early_stopping_rounds"]),
            lgb.log_evaluation(period=DEFAULT_VERBOSE_EVAL),
        ],
    )
    predictions = booster.predict(
        encode_categoricals(valid_df[feature_columns], categorical_features),
        num_iteration=booster.best_iteration,
    )
    return booster, np.asarray(predictions, dtype=np.float64)


def mae_for_head(predictions: np.ndarray, target_values: pd.Series) -> float:
    mask = target_values.notna().to_numpy()
    if mask.sum() == 0:
        return float("nan")
    diffs = np.abs(predictions[mask] - target_values[mask].to_numpy(dtype=np.float64))
    return float(diffs.mean())


def compute_corner_1_top3_agreement(predictions_df: pd.DataFrame) -> float:
    """Fraction of races where predicted top-3 by corner_1_pred matches actual top-3 by corner_1_norm."""
    eligible = predictions_df.dropna(subset=["target_corner_1_norm"]).copy()
    if eligible.empty:
        return float("nan")
    eligible["pred_rank"] = eligible.groupby("race_id")["corner_1_pred"].rank(method="dense", ascending=True)
    eligible["actual_rank"] = eligible.groupby("race_id")["target_corner_1_norm"].rank(method="dense", ascending=True)
    eligible["pred_top3"] = eligible["pred_rank"] <= TOP_K_AGREEMENT
    eligible["actual_top3"] = eligible["actual_rank"] <= TOP_K_AGREEMENT
    eligible["match"] = (eligible["pred_top3"] & eligible["actual_top3"]).astype(int)
    by_race = eligible.groupby("race_id").agg(
        matched=("match", "sum"),
        pred_count=("pred_top3", "sum"),
        actual_count=("actual_top3", "sum"),
    )
    denom = by_race[["pred_count", "actual_count"]].min(axis=1).clip(lower=1)
    return float((by_race["matched"] / denom).mean())


def build_predictions_df(
    valid_df: pd.DataFrame,
    head_predictions: dict[str, np.ndarray],
) -> pd.DataFrame:
    output = valid_df[
        ["race_id", "ketto_toroku_bango", "umaban", "race_year",
         "target_corner_1_norm", "target_corner_3_norm", "target_corner_4_norm"]
    ].copy()
    for head_name, predictions in head_predictions.items():
        output[f"{head_name}_pred"] = predictions
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
    head_predictions: dict[str, np.ndarray] = {}
    per_head_mae: dict[str, float] = {}
    for head_name, target_column in CORNER_HEAD_TARGETS.items():
        _booster, preds = train_head(
            train_df, valid_df, target_column, feature_columns, categorical_features, params
        )
        head_predictions[head_name] = preds
        per_head_mae[head_name] = mae_for_head(preds, valid_df[target_column])
    predictions_df = build_predictions_df(valid_df, head_predictions)
    metrics: FoldMetrics = {
        "validation_year": valid_year,
        "train_rows": int(len(train_df)),
        "valid_rows": int(len(valid_df)),
        "per_head_mae": per_head_mae,
        "corner_1_top3_agreement": compute_corner_1_top3_agreement(predictions_df),
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
            record = _sanitize_record_for_json(raw_record)
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_walk_forward_report(metrics_per_fold: list[FoldMetrics], output_path: Path) -> None:
    aggregate = {
        "per_head_mae_mean": {
            head: float(np.nanmean([fold["per_head_mae"][head] for fold in metrics_per_fold]))
            for head in CORNER_HEAD_TARGETS
        },
        "corner_1_top3_agreement_mean": float(
            np.nanmean([fold["corner_1_top3_agreement"] for fold in metrics_per_fold])
        ),
    }
    payload = {"folds": metrics_per_fold, "aggregate": aggregate}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="corner_position_lightgbm")
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
