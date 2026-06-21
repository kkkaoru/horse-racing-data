#!/usr/bin/env python3
# pyright: reportUnknownParameterType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportMissingParameterType=false
"""LambdaRank training and inference for finish-position prediction.

Run with:
  uv run --with lightgbm --with pandas --with numpy python \
    src/scripts/finish_position_lightgbm.py train \
    --train-csv tmp/finish-position-training/jra/train.csv \
    --output-model tmp/models/finish-jra-v1.lgb \
    --output-predictions tmp/predictions/jra-train.jsonl
"""
from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from pathlib import Path
from time import perf_counter
from typing import TypedDict, cast

import lightgbm as lgb
import numpy as np
import optuna
import pandas as pd
from numpy.typing import NDArray

from subgroup_diagnostics import compute_race_ndcg

LightgbmCallback = Callable[..., object]

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]

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
CATEGORICAL_FEATURE_COLUMNS: tuple[str, ...] = ("track_code", "grade_code")

RELEVANCE_TIERS: dict[int, int] = {1: 3, 2: 2, 3: 1}
RELEVANCE_TIER_PRESETS: dict[str, dict[int, int]] = {
    "default": {1: 3, 2: 2, 3: 1},
    "place_weighted": {1: 2, 2: 3, 3: 2},
    "sequence_aware": {1: 1, 2: 2, 3: 3},
    "graded_top6": {1: 6, 2: 5, 3: 4, 4: 3, 5: 2, 6: 1},
}
DEFAULT_RELEVANCE_TIER_NAME = "default"
DEFAULT_RELEVANCE = 0

DISTANCE_BAND_RANGES: dict[str, tuple[int, int]] = {
    "sprint": (0, 1200),
    "mile": (1200, 1600),
    "intermediate": (1600, 2000),
    "long": (2000, 2400),
    "extended": (2400, 99999),
}
TOP3_K = 3
MONOTONE_CONSTRAINTS: dict[str, int] = {
    "inverse_odds_implied_prob": 1,
    "inverse_odds_market_share": 1,
    "popularity_score": -1,
    "popularity_rank_in_race": -1,
    "inverse_odds_rank_in_race": -1,
    "tansho_odds_raw": -1,
    "tansho_ninkijun_raw": -1,
    "odds_score": -1,
}
SAMPLE_WEIGHT_MODE_NONE = "none"
SAMPLE_WEIGHT_MODE_TIME = "time"
SAMPLE_WEIGHT_MODES: tuple[str, ...] = (SAMPLE_WEIGHT_MODE_NONE, SAMPLE_WEIGHT_MODE_TIME)
DEFAULT_TIME_DECAY = 0.85
DEFAULT_NUM_LEAVES = 63
DEFAULT_LEARNING_RATE = 0.05
DEFAULT_MIN_CHILD_SAMPLES = 20
DEFAULT_LAMBDA_L2 = 0.0
DEFAULT_NUM_ITERATIONS = 500
DEFAULT_EARLY_STOPPING_ROUNDS = 50
DEFAULT_VERBOSE_EVAL = 50
HPO_NUM_ITERATIONS = 200
HPO_NUM_LEAVES_MIN = 15
HPO_NUM_LEAVES_MAX = 255
HPO_LEARNING_RATE_MIN = 0.01
HPO_LEARNING_RATE_MAX = 0.3
HPO_MIN_CHILD_SAMPLES_MIN = 5
HPO_MIN_CHILD_SAMPLES_MAX = 100
HPO_LAMBDA_L2_MIN = 0.0
HPO_LAMBDA_L2_MAX = 10.0
HPO_DEFAULT_N_TRIALS = 30
HPO_DEFAULT_SEED = 20260515

OBJECTIVE_LAMBDARANK = "lambdarank"
OBJECTIVE_BINARY_TOP1 = "binary-top1"
OBJECTIVE_BINARY_TOP3 = "binary-top3"
OBJECTIVE_BINARY_PLACE2 = "binary-place2"
OBJECTIVE_BINARY_PLACE3 = "binary-place3"
OBJECTIVE_CHOICES: tuple[str, ...] = (
    OBJECTIVE_LAMBDARANK,
    OBJECTIVE_BINARY_TOP1,
    OBJECTIVE_BINARY_TOP3,
    OBJECTIVE_BINARY_PLACE2,
    OBJECTIVE_BINARY_PLACE3,
)
BINARY_OBJECTIVES: tuple[str, ...] = (
    OBJECTIVE_BINARY_TOP1,
    OBJECTIVE_BINARY_TOP3,
    OBJECTIVE_BINARY_PLACE2,
    OBJECTIVE_BINARY_PLACE3,
)
TOP3_UPPER_BOUND = 3


class TrainingParams(TypedDict):
    lambda_l2: float
    learning_rate: float
    min_child_samples: int
    num_iterations: int
    num_leaves: int
    objective: str


class TrainingResult(TypedDict):
    best_binary_logloss: float | None
    best_iteration: int
    best_ndcg_at_3: float | None
    elapsed_seconds: float
    feature_columns: list[str]
    objective: str
    train_rows: int
    valid_rows: int


def to_relevance(finish_position: int, tiers: dict[int, int] | None = None) -> int:
    table = tiers if tiers is not None else RELEVANCE_TIERS
    return table.get(int(finish_position), DEFAULT_RELEVANCE)


def to_relevance_series(finish_positions: pd.Series, tiers: dict[int, int] | None = None) -> pd.Series:
    table = tiers if tiers is not None else RELEVANCE_TIERS
    positions = finish_positions.fillna(0).astype(int).to_numpy(dtype=np.int64)
    relevances = [to_relevance(int(pos), table) for pos in positions]
    return pd.Series(relevances, index=finish_positions.index, dtype=int)


def resolve_relevance_tiers(tier_name: str | None) -> dict[int, int]:
    if tier_name is None:
        return RELEVANCE_TIERS
    preset = RELEVANCE_TIER_PRESETS.get(tier_name)
    if preset is None:
        raise ValueError(f"unknown relevance tier preset: {tier_name!r}")
    return preset


def build_label_array(
    finish_positions: pd.Series,
    objective: str,
    relevance_tiers: dict[int, int] | None = None,
) -> IntArray:
    if objective == OBJECTIVE_BINARY_TOP1:
        fp_int = finish_positions.fillna(0).astype(int)
        return cast(IntArray, (fp_int == 1).astype(np.int64).to_numpy())
    if objective == OBJECTIVE_BINARY_TOP3:
        fp_int = finish_positions.fillna(0).astype(int)
        binary = (fp_int >= 1) & (fp_int <= TOP3_UPPER_BOUND)
        return cast(IntArray, binary.astype(np.int64).to_numpy())
    if objective == OBJECTIVE_BINARY_PLACE2:
        fp_int = finish_positions.fillna(0).astype(int)
        return cast(IntArray, (fp_int == 2).astype(np.int64).to_numpy())
    if objective == OBJECTIVE_BINARY_PLACE3:
        fp_int = finish_positions.fillna(0).astype(int)
        return cast(IntArray, (fp_int == 3).astype(np.int64).to_numpy())
    return cast(IntArray, to_relevance_series(finish_positions, relevance_tiers).to_numpy(dtype=np.int64))


def resolve_feature_columns(df_columns: list[str]) -> list[str]:
    excluded = set(META_COLUMNS) | set(LABEL_COLUMNS)
    return [column for column in df_columns if column not in excluded]


def build_group_sizes(df: pd.DataFrame) -> list[int]:
    return df.groupby("race_id", sort=False).size().tolist()


def rank_within_race(df: pd.DataFrame) -> pd.Series:
    return df.groupby("race_id")["predicted_score"].rank(method="dense", ascending=False).astype(int)


def load_dataset_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype={"track_code": "string", "grade_code": "string"})


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


def load_dataset(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet" or path.is_dir():
        return load_dataset_parquet(path)
    return load_dataset_csv(path)


def sort_for_grouping(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["race_id", "umaban"]).reset_index(drop=True)


def filter_by_distance_band(df: pd.DataFrame, band: str) -> pd.DataFrame:
    """Return rows whose ``kyori`` value falls within the named distance band.

    Raises ``ValueError`` for an unknown band name. The upper bound is
    exclusive so bands partition the space without overlap.
    """
    if band not in DISTANCE_BAND_RANGES:
        raise ValueError(
            f"Unknown distance band: {band!r}. Valid: {sorted(DISTANCE_BAND_RANGES)}"
        )
    lo, hi = DISTANCE_BAND_RANGES[band]
    return df[(df["kyori"] >= lo) & (df["kyori"] < hi)].reset_index(drop=True)


def select_feature_frame(df: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    return df[feature_columns].copy()


def detect_categorical_features(feature_columns: list[str]) -> list[str]:
    return [column for column in feature_columns if column in CATEGORICAL_FEATURE_COLUMNS]


def encode_categoricals(frame: pd.DataFrame, categorical_features: list[str]) -> pd.DataFrame:
    encoded = frame.copy()
    for column in categorical_features:
        encoded[column] = encoded[column].astype("category")
    return encoded


class LgbDatasetBundle(TypedDict):
    dataset: "lgb.Dataset"
    feature_columns: list[str]
    categorical_features: list[str]
    group_sizes: list[int]
    relevance: IntArray


def _year_series(df: pd.DataFrame) -> pd.Series:
    if "race_year" in df.columns:
        return pd.to_numeric(df["race_year"], errors="coerce")
    if "kaisai_nen" in df.columns:
        return pd.to_numeric(df["kaisai_nen"], errors="coerce")
    if "race_date" in df.columns:
        return pd.to_numeric(df["race_date"].astype(str).str.slice(0, 4), errors="coerce")
    raise KeyError("no year column (race_year / kaisai_nen / race_date) in df")


def compute_sample_weights(
    df: pd.DataFrame, mode: str, time_decay: float = DEFAULT_TIME_DECAY
) -> NDArray[np.float32] | None:
    if mode == SAMPLE_WEIGHT_MODE_NONE:
        return None
    if mode == SAMPLE_WEIGHT_MODE_TIME:
        year_series = _year_series(df)
        max_year = int(year_series.max()) if year_series.notna().any() else 0
        weights = np.power(time_decay, (max_year - year_series).fillna(0).to_numpy(dtype=np.float32))
        return weights.astype(np.float32)
    raise ValueError(f"unknown sample_weight mode: {mode!r}")


def prepare_lgb_dataset(
    df: pd.DataFrame,
    objective: str = OBJECTIVE_LAMBDARANK,
    sample_weight_mode: str = SAMPLE_WEIGHT_MODE_NONE,
    relevance_tier_name: str | None = None,
) -> LgbDatasetBundle:
    sorted_df = sort_for_grouping(df)
    feature_columns = resolve_feature_columns(list(sorted_df.columns))
    categorical_features = detect_categorical_features(feature_columns)
    frame = encode_categoricals(select_feature_frame(sorted_df, feature_columns), categorical_features)
    tiers = resolve_relevance_tiers(relevance_tier_name)
    label_array = build_label_array(sorted_df["finish_position"], objective, tiers)
    group_sizes = build_group_sizes(sorted_df)
    group_array = np.array(group_sizes, dtype=np.int64) if objective == OBJECTIVE_LAMBDARANK else None
    weight_array = compute_sample_weights(sorted_df, sample_weight_mode)
    dataset = lgb.Dataset(
        data=frame,
        label=label_array,
        group=group_array,
        weight=weight_array,
        categorical_feature=categorical_features,
        free_raw_data=False,
    )
    return {
        "categorical_features": categorical_features,
        "dataset": dataset,
        "feature_columns": feature_columns,
        "group_sizes": group_sizes,
        "relevance": label_array,
    }


def resolve_monotone_constraints(feature_columns: list[str]) -> list[int]:
    return [MONOTONE_CONSTRAINTS.get(column, 0) for column in feature_columns]


def build_lightgbm_params(
    params: TrainingParams, feature_columns: list[str] | None = None
) -> dict[str, object]:
    base: dict[str, object] = {
        "boosting_type": "gbdt",
        "learning_rate": params["learning_rate"],
        "num_leaves": params["num_leaves"],
        "min_child_samples": params["min_child_samples"],
        "lambda_l2": params["lambda_l2"],
        "verbose": -1,
    }
    if feature_columns is not None:
        constraints = resolve_monotone_constraints(feature_columns)
        if any(constraints):
            base["monotone_constraints"] = constraints
            base["monotone_constraints_method"] = "advanced"
    if params["objective"] == OBJECTIVE_LAMBDARANK:
        return {**base, "objective": "lambdarank", "metric": "ndcg", "eval_at": [TOP3_K]}
    return {**base, "objective": "binary", "metric": "binary_logloss"}


def train_lambdarank(
    train_bundle: LgbDatasetBundle,
    valid_bundle: LgbDatasetBundle | None,
    params: TrainingParams,
) -> tuple["lgb.Booster", TrainingResult]:
    started = perf_counter()
    lgb_params = build_lightgbm_params(params, train_bundle["feature_columns"])
    valid_sets: list[lgb.Dataset] = [train_bundle["dataset"]]
    valid_names: list[str] = ["train"]
    if valid_bundle is not None:
        valid_sets.append(valid_bundle["dataset"])
        valid_names.append("valid")
    callbacks: list[LightgbmCallback] = [cast(LightgbmCallback, lgb.log_evaluation(period=DEFAULT_VERBOSE_EVAL))]
    if valid_bundle is not None:
        callbacks.append(
            cast(
                LightgbmCallback,
                lgb.early_stopping(stopping_rounds=DEFAULT_EARLY_STOPPING_ROUNDS, first_metric_only=True),
            ),
        )
    booster = lgb.train(
        params=lgb_params,
        train_set=train_bundle["dataset"],
        num_boost_round=params["num_iterations"],
        valid_sets=valid_sets,
        valid_names=valid_names,
        callbacks=callbacks,
    )
    raw_best_iteration = booster.best_iteration
    best_iteration = (
        raw_best_iteration if raw_best_iteration > 0 else params["num_iterations"]
    )
    has_valid = valid_bundle is not None
    best_ndcg = extract_best_ndcg(booster, has_valid) if params["objective"] == OBJECTIVE_LAMBDARANK else None
    best_logloss = (
        extract_best_binary_logloss(booster, has_valid) if params["objective"] in BINARY_OBJECTIVES else None
    )
    valid_rows = sum(valid_bundle["group_sizes"]) if valid_bundle is not None else 0
    return booster, {
        "best_binary_logloss": best_logloss,
        "best_iteration": int(best_iteration),
        "best_ndcg_at_3": best_ndcg,
        "elapsed_seconds": perf_counter() - started,
        "feature_columns": train_bundle["feature_columns"],
        "objective": params["objective"],
        "train_rows": sum(train_bundle["group_sizes"]),
        "valid_rows": valid_rows,
    }


def extract_best_ndcg(booster: "lgb.Booster", has_valid: bool) -> float | None:
    if not has_valid:
        return None
    best_score = booster.best_score.get("valid", {})
    for key, value in best_score.items():
        if key.startswith("ndcg@"):
            return float(value)
    return None


def extract_best_binary_logloss(booster: "lgb.Booster", has_valid: bool) -> float | None:
    if not has_valid:
        return None
    best_score = booster.best_score.get("valid", {})
    value = best_score.get("binary_logloss")
    return float(value) if value is not None else None


class PredictionRow(TypedDict):
    ketto_toroku_bango: str
    predicted_rank: int
    predicted_score: float
    race_id: str
    umaban: int


def score_dataset(booster: "lgb.Booster", df: pd.DataFrame) -> pd.DataFrame:
    sorted_df = sort_for_grouping(df)
    feature_columns = resolve_feature_columns(list(sorted_df.columns))
    frame = encode_categoricals(
        select_feature_frame(sorted_df, feature_columns),
        detect_categorical_features(feature_columns),
    )
    scores = cast(FloatArray, booster.predict(frame, num_iteration=booster.best_iteration))
    sorted_df = sorted_df.copy()
    sorted_df["predicted_score"] = pd.Series(scores, index=sorted_df.index)
    sorted_df["predicted_rank"] = rank_within_race(sorted_df)
    return sorted_df[
        ["race_id", "ketto_toroku_bango", "umaban", "predicted_score", "predicted_rank"]
    ].reset_index(drop=True)


def write_predictions_jsonl(predictions: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in predictions.to_dict(orient="records"):
            handle.write(f"{json.dumps(record, ensure_ascii=False)}\n")


def save_booster(booster: "lgb.Booster", path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    booster.save_model(str(path))


def load_booster(path: Path) -> "lgb.Booster":
    return lgb.Booster(model_file=str(path))


def write_training_metadata(result: TrainingResult, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="finish_position_lightgbm")
    subparsers = parser.add_subparsers(dest="command", required=True)
    train = subparsers.add_parser("train")
    train.add_argument("--train-csv", type=Path, required=True)
    train.add_argument("--valid-csv", type=Path)
    train.add_argument("--output-model", type=Path, required=True)
    train.add_argument("--output-predictions", type=Path)
    train.add_argument("--output-metadata", type=Path)
    train.add_argument("--num-iterations", type=int, default=DEFAULT_NUM_ITERATIONS)
    train.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    train.add_argument("--num-leaves", type=int, default=DEFAULT_NUM_LEAVES)
    train.add_argument("--min-child-samples", type=int, default=DEFAULT_MIN_CHILD_SAMPLES)
    train.add_argument("--lambda-l2", type=float, default=DEFAULT_LAMBDA_L2)
    train.add_argument("--objective", choices=OBJECTIVE_CHOICES, default=OBJECTIVE_LAMBDARANK)
    walk = subparsers.add_parser("walk-forward")
    walk.add_argument("--csv", type=Path, required=True)
    walk.add_argument("--train-start-date", type=str, default="20160101")
    walk.add_argument("--validation-years", type=str, default="2021,2022,2023,2024,2025")
    walk.add_argument("--output-report", type=Path, required=True)
    walk.add_argument("--output-predictions-dir", type=Path)
    walk.add_argument("--num-iterations", type=int, default=DEFAULT_NUM_ITERATIONS)
    walk.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    walk.add_argument("--num-leaves", type=int, default=DEFAULT_NUM_LEAVES)
    walk.add_argument("--min-child-samples", type=int, default=DEFAULT_MIN_CHILD_SAMPLES)
    walk.add_argument("--lambda-l2", type=float, default=DEFAULT_LAMBDA_L2)
    walk.add_argument("--objective", choices=OBJECTIVE_CHOICES, default=OBJECTIVE_LAMBDARANK)
    walk.add_argument(
        "--sample-weight-mode",
        choices=SAMPLE_WEIGHT_MODES,
        default=SAMPLE_WEIGHT_MODE_NONE,
    )
    walk.add_argument(
        "--relevance-tier",
        choices=tuple(RELEVANCE_TIER_PRESETS.keys()),
        default=DEFAULT_RELEVANCE_TIER_NAME,
    )
    predict = subparsers.add_parser("predict")
    predict.add_argument("--model-path", type=Path, required=True)
    predict.add_argument("--input-csv", type=Path, required=True)
    predict.add_argument("--output-predictions", type=Path, required=True)
    hpo = subparsers.add_parser("hpo")
    hpo.add_argument("--csv", type=Path, required=True)
    hpo.add_argument("--train-start-date", type=str, default="20160101")
    hpo.add_argument("--validation-years", type=str, default="2021,2022,2023,2024,2025")
    hpo.add_argument("--output-best-params", type=Path, required=True)
    hpo.add_argument("--output-trials-csv", type=Path)
    hpo.add_argument("--n-trials", type=int, default=HPO_DEFAULT_N_TRIALS)
    hpo.add_argument("--num-iterations", type=int, default=HPO_NUM_ITERATIONS)
    hpo.add_argument("--seed", type=int, default=HPO_DEFAULT_SEED)
    hpo.add_argument("--objective", choices=OBJECTIVE_CHOICES, default=OBJECTIVE_LAMBDARANK)
    return parser.parse_args(argv)


def training_params_from_args(args: argparse.Namespace) -> TrainingParams:
    objective_value: str = getattr(args, "objective", OBJECTIVE_LAMBDARANK)
    return {
        "lambda_l2": float(args.lambda_l2),
        "learning_rate": float(args.learning_rate),
        "min_child_samples": int(args.min_child_samples),
        "num_iterations": int(args.num_iterations),
        "num_leaves": int(args.num_leaves),
        "objective": objective_value,
    }


def run_train_command(args: argparse.Namespace) -> None:
    params = training_params_from_args(args)
    train_df = load_dataset(args.train_csv)
    valid_df = load_dataset(args.valid_csv) if args.valid_csv is not None else None
    train_bundle = prepare_lgb_dataset(train_df, params["objective"])
    valid_bundle = (
        prepare_lgb_dataset(valid_df, params["objective"]) if valid_df is not None else None
    )
    booster, result = train_lambdarank(train_bundle, valid_bundle, params)
    save_booster(booster, args.output_model)
    if args.output_predictions is not None:
        source_df = valid_df if valid_df is not None else train_df
        predictions = score_dataset(booster, source_df)
        write_predictions_jsonl(predictions, args.output_predictions)
    if args.output_metadata is not None:
        write_training_metadata(result, args.output_metadata)
    print(json.dumps(result, ensure_ascii=False))


def parse_year_list(raw: str) -> list[int]:
    pieces = [piece.strip() for piece in raw.split(",") if piece.strip()]
    parsed = [int(piece) for piece in pieces]
    if not parsed:
        raise ValueError("validation years must be a non-empty comma-separated list")
    return sorted(set(parsed))


def filter_by_date_range(df: pd.DataFrame, from_date: str, to_date: str) -> pd.DataFrame:
    mask = (df["race_date"].astype(str) >= from_date) & (df["race_date"].astype(str) <= to_date)
    return df.loc[mask].reset_index(drop=True)


class FoldSplit(TypedDict):
    train_df: pd.DataFrame
    valid_df: pd.DataFrame
    valid_year: int


def split_walk_forward(df: pd.DataFrame, train_start: str, valid_year: int) -> FoldSplit:
    train_end = f"{valid_year - 1}1231"
    valid_start = f"{valid_year}0101"
    valid_end = f"{valid_year}1231"
    return {
        "train_df": filter_by_date_range(df, train_start, train_end),
        "valid_df": filter_by_date_range(df, valid_start, valid_end),
        "valid_year": valid_year,
    }


class FoldMetrics(TypedDict):
    ndcg_at_3: float
    race_count: int
    top1_accuracy: float
    top3_box_accuracy: float
    top3_exact_accuracy: float
    valid_rows: int
    valid_year: int


def compute_top_k_actuals(group: pd.DataFrame, k: int) -> list[str]:
    actual_top = group.dropna(subset=["finish_position"]).nsmallest(k, "finish_position").sort_values("finish_position")
    return actual_top["ketto_toroku_bango"].tolist()


def compute_top_k_predicted(group: pd.DataFrame, k: int) -> list[str]:
    predicted_top = group.dropna(subset=["predicted_rank"]).nsmallest(k, "predicted_rank").sort_values("predicted_rank")
    return predicted_top["ketto_toroku_bango"].tolist()


def race_top3_box_hit(actual_top3: list[str], predicted_top3: list[str]) -> bool:
    return set(actual_top3) == set(predicted_top3)


def race_top3_exact_hit(actual_top3: list[str], predicted_top3: list[str]) -> bool:
    return actual_top3 == predicted_top3


def race_top1_hit(actual_top3: list[str], predicted_top3: list[str]) -> bool:
    if not actual_top3 or not predicted_top3:
        return False
    return actual_top3[0] == predicted_top3[0]


def evaluate_predictions(predictions: pd.DataFrame, ground_truth: pd.DataFrame) -> dict[str, float | int]:
    joined = ground_truth[["race_id", "ketto_toroku_bango", "finish_position"]].merge(
        predictions,
        on=["race_id", "ketto_toroku_bango"],
        how="left",
    )
    box_hits = 0
    exact_hits = 0
    top1_hits = 0
    race_count = 0
    ndcg_scores: list[float] = []
    for _race_id, group in joined.groupby("race_id"):
        actual = compute_top_k_actuals(group, TOP3_K)
        predicted = compute_top_k_predicted(group, TOP3_K)
        race_count += 1
        ndcg_scores.append(compute_race_ndcg(group))
        if race_top3_box_hit(actual, predicted):
            box_hits += 1
        if race_top3_exact_hit(actual, predicted):
            exact_hits += 1
        if race_top1_hit(actual, predicted):
            top1_hits += 1
    safe_total = max(race_count, 1)
    return {
        "race_count": race_count,
        "ndcg_at_3": float(np.mean(ndcg_scores)) if ndcg_scores else 0.0,
        "top1_accuracy": top1_hits / safe_total,
        "top3_box_accuracy": box_hits / safe_total,
        "top3_exact_accuracy": exact_hits / safe_total,
    }


def run_walk_forward_fold(
    fold: FoldSplit,
    params: TrainingParams,
    sample_weight_mode: str = SAMPLE_WEIGHT_MODE_NONE,
    relevance_tier_name: str | None = None,
) -> tuple["lgb.Booster", pd.DataFrame, FoldMetrics]:
    train_bundle = prepare_lgb_dataset(
        fold["train_df"], params["objective"], sample_weight_mode, relevance_tier_name
    )
    valid_bundle = prepare_lgb_dataset(
        fold["valid_df"], params["objective"], SAMPLE_WEIGHT_MODE_NONE, relevance_tier_name
    )
    booster, training_result = train_lambdarank(train_bundle, valid_bundle, params)
    predictions = score_dataset(booster, fold["valid_df"])
    eval_metrics = evaluate_predictions(predictions, fold["valid_df"])
    metrics: FoldMetrics = {
        "ndcg_at_3": float(eval_metrics["ndcg_at_3"]),
        "race_count": int(eval_metrics["race_count"]),
        "top1_accuracy": float(eval_metrics["top1_accuracy"]),
        "top3_box_accuracy": float(eval_metrics["top3_box_accuracy"]),
        "top3_exact_accuracy": float(eval_metrics["top3_exact_accuracy"]),
        "valid_rows": training_result["valid_rows"],
        "valid_year": fold["valid_year"],
    }
    return booster, predictions, metrics


def aggregate_fold_metrics(folds: list[FoldMetrics]) -> dict[str, float | int]:
    if not folds:
        return {
            "fold_count": 0,
            "ndcg_at_3_mean": 0.0,
            "top1_accuracy_mean": 0.0,
            "top3_box_accuracy_mean": 0.0,
            "top3_exact_accuracy_mean": 0.0,
        }
    fold_count = len(folds)
    return {
        "fold_count": fold_count,
        "ndcg_at_3_mean": sum(fold["ndcg_at_3"] for fold in folds) / fold_count,
        "top1_accuracy_mean": sum(fold["top1_accuracy"] for fold in folds) / fold_count,
        "top3_box_accuracy_mean": sum(fold["top3_box_accuracy"] for fold in folds) / fold_count,
        "top3_exact_accuracy_mean": sum(fold["top3_exact_accuracy"] for fold in folds) / fold_count,
    }


def write_walk_forward_report(
    fold_metrics: list[FoldMetrics],
    aggregate: dict[str, float | int],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"aggregate": aggregate, "folds": fold_metrics}
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def run_walk_forward_command(args: argparse.Namespace) -> None:
    full_df = load_dataset(args.csv)
    validation_years = parse_year_list(args.validation_years)
    params = training_params_from_args(args)
    sample_weight_mode: str = getattr(args, "sample_weight_mode", SAMPLE_WEIGHT_MODE_NONE)
    relevance_tier_name: str | None = getattr(args, "relevance_tier", None)
    fold_metrics: list[FoldMetrics] = []
    for valid_year in validation_years:
        fold = split_walk_forward(full_df, args.train_start_date, valid_year)
        _booster, predictions, metrics = run_walk_forward_fold(
            fold, params, sample_weight_mode, relevance_tier_name
        )
        fold_metrics.append(metrics)
        if args.output_predictions_dir is not None:
            predictions_path = args.output_predictions_dir / f"{valid_year}.jsonl"
            write_predictions_jsonl(predictions, predictions_path)
    aggregate = aggregate_fold_metrics(fold_metrics)
    write_walk_forward_report(fold_metrics, aggregate, args.output_report)
    print(json.dumps({"aggregate": aggregate, "folds": fold_metrics}, ensure_ascii=False))


def suggest_hpo_params(
    trial: optuna.trial.Trial,
    num_iterations: int,
    objective: str = OBJECTIVE_LAMBDARANK,
) -> TrainingParams:
    return {
        "lambda_l2": trial.suggest_float("lambda_l2", HPO_LAMBDA_L2_MIN, HPO_LAMBDA_L2_MAX),
        "learning_rate": trial.suggest_float(
            "learning_rate", HPO_LEARNING_RATE_MIN, HPO_LEARNING_RATE_MAX, log=True
        ),
        "min_child_samples": trial.suggest_int(
            "min_child_samples", HPO_MIN_CHILD_SAMPLES_MIN, HPO_MIN_CHILD_SAMPLES_MAX
        ),
        "num_iterations": num_iterations,
        "num_leaves": trial.suggest_int("num_leaves", HPO_NUM_LEAVES_MIN, HPO_NUM_LEAVES_MAX),
        "objective": objective,
    }


def evaluate_fold_set(
    df: pd.DataFrame,
    train_start: str,
    validation_years: list[int],
    params: TrainingParams,
    relevance_tier_name: str | None = None,
) -> dict[str, float | int]:
    fold_metrics: list[FoldMetrics] = []
    for valid_year in validation_years:
        fold = split_walk_forward(df, train_start, valid_year)
        _booster, _predictions, metrics = run_walk_forward_fold(
            fold, params, relevance_tier_name=relevance_tier_name
        )
        fold_metrics.append(metrics)
    return aggregate_fold_metrics(fold_metrics)


class HpoSummary(TypedDict):
    best_params: TrainingParams
    best_value: float
    n_trials: int


def write_hpo_summary(summary: HpoSummary, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)


def write_optuna_trials_csv(study: optuna.Study, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    study.trials_dataframe().to_csv(output_path, index=False)


def run_hpo_command(args: argparse.Namespace) -> HpoSummary:
    df = load_dataset(args.csv)
    validation_years = parse_year_list(args.validation_years)
    objective_value: str = getattr(args, "objective", OBJECTIVE_LAMBDARANK)
    relevance_tier_name: str | None = getattr(args, "relevance_tier", None)

    def objective(trial: optuna.trial.Trial) -> float:
        params = suggest_hpo_params(trial, int(args.num_iterations), objective_value)
        aggregate = evaluate_fold_set(
            df, args.train_start_date, validation_years, params, relevance_tier_name
        )
        return float(aggregate["ndcg_at_3_mean"])

    sampler = optuna.samplers.TPESampler(seed=int(args.seed))
    study = optuna.create_study(direction="maximize", sampler=sampler)
    study.optimize(objective, n_trials=int(args.n_trials), show_progress_bar=False)
    best_params: TrainingParams = {
        "lambda_l2": float(study.best_params["lambda_l2"]),
        "learning_rate": float(study.best_params["learning_rate"]),
        "min_child_samples": int(study.best_params["min_child_samples"]),
        "num_iterations": int(args.num_iterations),
        "num_leaves": int(study.best_params["num_leaves"]),
        "objective": objective_value,
    }
    summary: HpoSummary = {
        "best_params": best_params,
        "best_value": float(study.best_value),
        "n_trials": len(study.trials),
    }
    write_hpo_summary(summary, args.output_best_params)
    if args.output_trials_csv is not None:
        write_optuna_trials_csv(study, args.output_trials_csv)
    print(json.dumps(summary, ensure_ascii=False))
    return summary


def run_predict_command(args: argparse.Namespace) -> None:
    booster = load_booster(args.model_path)
    df = load_dataset(args.input_csv)
    predictions = score_dataset(booster, df)
    write_predictions_jsonl(predictions, args.output_predictions)
    print(
        json.dumps(
            {
                "input_rows": len(df),
                "model_path": str(args.model_path),
                "output_predictions": str(args.output_predictions),
                "scored_rows": len(predictions),
            },
            ensure_ascii=False,
        )
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "train":
        run_train_command(args)
        return
    if args.command == "walk-forward":
        run_walk_forward_command(args)
        return
    if args.command == "hpo":
        run_hpo_command(args)
        return
    if args.command == "predict":
        run_predict_command(args)
        return
    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
