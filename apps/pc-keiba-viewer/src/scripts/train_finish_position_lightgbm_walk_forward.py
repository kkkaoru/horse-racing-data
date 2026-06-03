#!/usr/bin/env python3
"""LightGBM walk-forward trainer for the v8 iterative loop.

New module introduced in Stage 0C so iteration 3 (L1B +LGBM ensemble) can wire
in a 3-arch ranker. Mirrors the CatBoost/XGBoost walk-forward layout:

  * ``--objective {lambdarank,rank_xendcg}`` (default ``lambdarank``)
  * ``--lambdarank-truncation-level`` (Lever 17)
  * Stage 0C common flags: ``--iteration-id``, ``--alpha-bucket-weight``,
    ``--hpo-params-path``, ``--bucket-membership-parquet``,
    ``--resume-from-checkpoint``, ``--fine-tune-final-folds``,
    ``--fine-tune-lr-divisor``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TYPE_CHECKING, Final, Protocol, TypedDict, cast

import numpy as np
import pandas as pd

import walk_forward_common as wfc_common

if TYPE_CHECKING:
    from collections.abc import Callable

OBJECTIVE_LAMBDARANK: Final[str] = "lambdarank"
OBJECTIVE_RANK_XENDCG: Final[str] = "rank_xendcg"
SUPPORTED_OBJECTIVES: Final[tuple[str, ...]] = (OBJECTIVE_LAMBDARANK, OBJECTIVE_RANK_XENDCG)
DEFAULT_ITERATION_ID: Final[int] = 0
DEFAULT_ALPHA_BUCKET_WEIGHT: Final[float] = 0.0
DEFAULT_FINE_TUNE_FINAL_FOLDS: Final[int] = 0
DEFAULT_FINE_TUNE_LR_DIVISOR: Final[int] = 10
DEFAULT_LAMBDARANK_TRUNCATION_LEVEL: Final[int] = 3
RANDOM_SEED_BASE: Final[int] = 42
DEFAULT_TRAIN_START_DATE: Final[str] = "20060101"
METADATA_STATUS_COMPLETED: Final[str] = "completed"
METADATA_STATUS_SKIPPED: Final[str] = "skipped"
DEFAULT_NUM_ITERATIONS: Final[int] = 500
DEFAULT_NUM_LEAVES: Final[int] = 63
DEFAULT_MIN_CHILD_SAMPLES: Final[int] = 20
DEFAULT_LAMBDA_L2: Final[float] = 0.0
DEFAULT_LEARNING_RATE: Final[float] = 0.05
DEFAULT_EARLY_STOPPING_ROUNDS: Final[int] = 50

META_COLUMNS: Final[tuple[str, ...]] = (
    "race_id", "race_date", "race_year", "source", "kaisai_nen", "kaisai_tsukihi",
    "race_bango", "ketto_toroku_bango", "bamei",
    "kishumei_ryakusho", "chokyoshimei_ryakusho", "category",
)
LABEL_COLUMNS: Final[tuple[str, ...]] = ("finish_position", "finish_norm")
CATEGORICAL_FEATURE_NAMES: Final[tuple[str, ...]] = ("track_code", "grade_code")
RELEVANCE_RANK1: Final[int] = 3
RELEVANCE_RANK2: Final[int] = 2
RELEVANCE_RANK3: Final[int] = 1


class TrainLightgbmArgs(TypedDict):
    features_parquet: Path
    category: str
    walk_forward_namespace: str
    year_from: int
    year_to: int
    train_start_date: str
    model_root: Path
    iteration_id: int
    alpha_bucket_weight: float
    objective: str
    lambdarank_truncation_level: int
    hpo_params_path: Path | None
    bucket_membership_parquet: Path | None
    resume_from_checkpoint: bool
    fine_tune_final_folds: int
    fine_tune_lr_divisor: int
    num_iterations: int
    num_leaves: int
    learning_rate: float
    min_child_samples: int
    lambda_l2: float


class ParquetReaderLike(Protocol):
    def __call__(self, path: Path) -> pd.DataFrame: ...


class FoldTrainerLike(Protocol):
    def __call__(
        self,
        train_df: pd.DataFrame,
        valid_df: pd.DataFrame,
        feature_cols: list[str],
        args: argparse.Namespace,
    ) -> dict[str, object]: ...


class BucketMembershipReaderLike(Protocol):
    def __call__(self, path: Path) -> pd.DataFrame: ...


class TrainDeps(TypedDict):
    parquet_reader: ParquetReaderLike
    feature_resolver: Callable[[pd.DataFrame], list[str]]
    fold_trainer: FoldTrainerLike
    bucket_reader: BucketMembershipReaderLike


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="train_finish_position_lightgbm_walk_forward")
    parser.add_argument("--features-parquet", type=Path, required=True)
    parser.add_argument("--category", type=str, required=True)
    parser.add_argument("--walk-forward-namespace", type=str, required=True)
    parser.add_argument("--year-from", type=int, required=True)
    parser.add_argument("--year-to", type=int, required=True)
    parser.add_argument(
        "--train-start-date", type=str, default=DEFAULT_TRAIN_START_DATE,
    )
    parser.add_argument("--model-root", type=Path, required=True)
    parser.add_argument("--iteration-id", type=int, default=DEFAULT_ITERATION_ID)
    parser.add_argument(
        "--alpha-bucket-weight", type=float, default=DEFAULT_ALPHA_BUCKET_WEIGHT,
    )
    parser.add_argument(
        "--objective", type=str, choices=list(SUPPORTED_OBJECTIVES),
        default=OBJECTIVE_LAMBDARANK,
    )
    parser.add_argument(
        "--lambdarank-truncation-level",
        type=int,
        default=DEFAULT_LAMBDARANK_TRUNCATION_LEVEL,
    )
    parser.add_argument("--hpo-params-path", type=Path, default=None)
    parser.add_argument(
        "--bucket-membership-parquet", type=Path, default=None,
    )
    parser.add_argument(
        "--resume-from-checkpoint", action="store_true", default=False,
    )
    parser.add_argument(
        "--fine-tune-final-folds", type=int, default=DEFAULT_FINE_TUNE_FINAL_FOLDS,
    )
    parser.add_argument(
        "--fine-tune-lr-divisor", type=int, default=DEFAULT_FINE_TUNE_LR_DIVISOR,
    )
    parser.add_argument("--num-iterations", type=int, default=DEFAULT_NUM_ITERATIONS)
    parser.add_argument("--num-leaves", type=int, default=DEFAULT_NUM_LEAVES)
    parser.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    parser.add_argument(
        "--min-child-samples", type=int, default=DEFAULT_MIN_CHILD_SAMPLES,
    )
    parser.add_argument("--lambda-l2", type=float, default=DEFAULT_LAMBDA_L2)
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def normalize_args(args: argparse.Namespace) -> TrainLightgbmArgs:
    return {
        "features_parquet": Path(cast(str, args.features_parquet)),
        "category": cast(str, args.category),
        "walk_forward_namespace": cast(str, args.walk_forward_namespace),
        "year_from": int(cast(int, args.year_from)),
        "year_to": int(cast(int, args.year_to)),
        "train_start_date": cast(str, args.train_start_date),
        "model_root": Path(cast(str, args.model_root)),
        "iteration_id": int(cast(int, args.iteration_id)),
        "alpha_bucket_weight": float(cast(float, args.alpha_bucket_weight)),
        "objective": cast(str, args.objective),
        "lambdarank_truncation_level": int(cast(int, args.lambdarank_truncation_level)),
        "hpo_params_path": (
            Path(cast(str, args.hpo_params_path))
            if args.hpo_params_path is not None else None
        ),
        "bucket_membership_parquet": (
            Path(cast(str, args.bucket_membership_parquet))
            if args.bucket_membership_parquet is not None else None
        ),
        "resume_from_checkpoint": bool(cast(bool, args.resume_from_checkpoint)),
        "fine_tune_final_folds": int(cast(int, args.fine_tune_final_folds)),
        "fine_tune_lr_divisor": int(cast(int, args.fine_tune_lr_divisor)),
        "num_iterations": int(cast(int, args.num_iterations)),
        "num_leaves": int(cast(int, args.num_leaves)),
        "learning_rate": float(cast(float, args.learning_rate)),
        "min_child_samples": int(cast(int, args.min_child_samples)),
        "lambda_l2": float(cast(float, args.lambda_l2)),
    }


def load_hpo_params(path: Path | None) -> dict[str, object]:
    if path is None:
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"HPO params file must be a JSON object, got {type(parsed)!r}")
    return cast(dict[str, object], parsed)


def apply_hpo_params(args: TrainLightgbmArgs, params: dict[str, object]) -> TrainLightgbmArgs:
    merged = cast(TrainLightgbmArgs, dict(args))
    if "num_iterations" in params:
        merged["num_iterations"] = int(cast(int, params["num_iterations"]))
    if "num_leaves" in params:
        merged["num_leaves"] = int(cast(int, params["num_leaves"]))
    if "learning_rate" in params:
        merged["learning_rate"] = float(cast(float, params["learning_rate"]))
    if "min_child_samples" in params:
        merged["min_child_samples"] = int(cast(int, params["min_child_samples"]))
    if "lambda_l2" in params:
        merged["lambda_l2"] = float(cast(float, params["lambda_l2"]))
    return merged


def resolve_fold_random_seed(fold_year: int) -> int:
    return RANDOM_SEED_BASE + fold_year


def build_per_fold_model_dir(args: TrainLightgbmArgs, fold_year: int) -> Path:
    return args["model_root"] / args["category"] / f"iter{args['iteration_id']}" / f"fold-{fold_year}"


def resolve_fold_learning_rate(
    args: TrainLightgbmArgs, fold_year: int, fold_years: list[int],
) -> float:
    if args["fine_tune_final_folds"] <= 0:
        return args["learning_rate"]
    tail_count = min(args["fine_tune_final_folds"], len(fold_years))
    if tail_count == 0:
        return args["learning_rate"]
    fine_tune_threshold = fold_years[-tail_count]
    if fold_year < fine_tune_threshold:
        return args["learning_rate"]
    divisor = max(1, args["fine_tune_lr_divisor"])
    return args["learning_rate"] / float(divisor)


def resolve_feature_columns(df: pd.DataFrame) -> list[str]:
    excluded = set(META_COLUMNS) | set(LABEL_COLUMNS)
    return [
        c for c in df.columns
        if c not in excluded and pd.api.types.is_numeric_dtype(df[c])
    ]


def merge_bucket_weights_into_train(
    train_df: pd.DataFrame, bucket_df: pd.DataFrame | None,
) -> pd.DataFrame:
    if bucket_df is None:
        return train_df
    if "race_id" not in bucket_df.columns:
        raise ValueError("bucket membership parquet must contain race_id")
    if "is_weak_bucket_score" not in bucket_df.columns:
        raise ValueError(
            "bucket membership parquet must contain is_weak_bucket_score",
        )
    merge_cols = ["race_id", "is_weak_bucket_score"]
    return train_df.merge(bucket_df[merge_cols], on="race_id", how="left")


def attach_sample_weights(train_df: pd.DataFrame, alpha: float) -> pd.DataFrame:
    if "race_year" not in train_df.columns:
        raise ValueError("train_df must contain race_year for sample weighting")
    years = np.asarray(
        train_df["race_year"].astype("int64").to_numpy(), dtype=np.int64,
    )
    time_weights = wfc_common.compute_time_decay_weights(years)
    if "is_weak_bucket_score" in train_df.columns and alpha > 0:
        weak_scores = np.asarray(
            train_df["is_weak_bucket_score"].fillna(0.0).astype(float).to_numpy(),
            dtype=np.float64,
        )
        sample_weights = wfc_common.compute_bucket_aware_weights(
            time_weights, weak_scores, alpha=alpha,
        )
    else:
        sample_weights = time_weights
    out = train_df.copy()
    out["sample_weight"] = sample_weights
    return out


def split_train_valid(
    df: pd.DataFrame, train_start: str, valid_year: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    train_end = f"{valid_year - 1}1231"
    train_mask = (
        (df["race_date"] >= train_start)
        & (df["race_date"] <= train_end)
        & df["finish_position"].notna()
    )
    valid_mask = df["race_date"].str.startswith(str(valid_year)) & df["finish_position"].notna()
    return df[train_mask].copy(), df[valid_mask].copy()


def build_fold_namespace(
    args: TrainLightgbmArgs, fold_year: int, fold_years: list[int],
) -> argparse.Namespace:
    fold_lr = resolve_fold_learning_rate(args, fold_year, fold_years)
    return argparse.Namespace(
        objective=args["objective"],
        num_iterations=args["num_iterations"],
        num_leaves=args["num_leaves"],
        learning_rate=fold_lr,
        min_child_samples=args["min_child_samples"],
        lambda_l2=args["lambda_l2"],
        lambdarank_truncation_level=args["lambdarank_truncation_level"],
        early_stopping_rounds=DEFAULT_EARLY_STOPPING_ROUNDS,
        seed=resolve_fold_random_seed(fold_year),
    )


def make_to_relevance() -> Callable[[object], int]:
    rel_map = {1: RELEVANCE_RANK1, 2: RELEVANCE_RANK2, 3: RELEVANCE_RANK3}

    def _to(value: object) -> int:
        if value is None or pd.isna(cast(float, value)):
            return 0
        return rel_map.get(int(cast(float, value)), 0)

    return _to


def build_group_sizes(df: pd.DataFrame) -> list[int]:
    return df.groupby("race_id", sort=False).size().tolist()


def default_fold_trainer(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_cols: list[str],
    args: argparse.Namespace,
) -> dict[str, object]:
    """Default per-fold trainer using LightGBM Booster.

    Imports are lazy so unit tests that mock the trainer never load lightgbm.
    """
    import lightgbm as lgb

    train_df = train_df.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    valid_df = valid_df.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    to_relevance = make_to_relevance()
    train_labels = train_df["finish_position"].map(to_relevance).to_numpy(dtype=np.int32)
    valid_labels = valid_df["finish_position"].map(to_relevance).to_numpy(dtype=np.int32)
    weights = train_df["sample_weight"].to_numpy(dtype=np.float32) if "sample_weight" in train_df.columns else None
    train_ds = lgb.Dataset(
        train_df[feature_cols],
        label=train_labels,
        group=build_group_sizes(train_df),
        weight=weights,
    )
    valid_ds = lgb.Dataset(
        valid_df[feature_cols],
        label=valid_labels,
        group=build_group_sizes(valid_df),
        reference=train_ds,
    )
    params = {
        "objective": args.objective,
        "metric": "ndcg",
        "ndcg_eval_at": [1, 3],
        "num_leaves": args.num_leaves,
        "learning_rate": args.learning_rate,
        "min_child_samples": args.min_child_samples,
        "lambda_l2": args.lambda_l2,
        "lambdarank_truncation_level": args.lambdarank_truncation_level,
        "seed": args.seed,
        "verbose": -1,
    }
    booster = lgb.train(
        params,
        train_ds,
        num_boost_round=args.num_iterations,
        valid_sets=[valid_ds],
        callbacks=[lgb.early_stopping(args.early_stopping_rounds, verbose=False)],
    )
    predictions = cast(
        "np.ndarray[tuple[int], np.dtype[np.float64]]",
        booster.predict(valid_df[feature_cols]),
    )
    valid_df = valid_df.assign(predicted_score=predictions)
    valid_df["predicted_rank"] = (
        valid_df.groupby("race_id")["predicted_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    return {
        "booster": booster,
        "valid_predictions": valid_df,
    }


def train_fold(
    df: pd.DataFrame,
    feature_cols: list[str],
    args: TrainLightgbmArgs,
    fold_year: int,
    fold_years: list[int],
    deps: TrainDeps,
    bucket_df: pd.DataFrame | None,
) -> dict[str, object]:
    model_dir = build_per_fold_model_dir(args, fold_year)
    if args["resume_from_checkpoint"] and wfc_common.detect_completed_fold(model_dir, fold_year):
        return {
            "fold_year": fold_year,
            "status": METADATA_STATUS_COMPLETED,
            "resumed": True,
            "rows": 0,
        }
    train_df, valid_df = split_train_valid(df, args["train_start_date"], fold_year)
    if len(train_df) == 0 or len(valid_df) == 0:
        wfc_common.atomic_write_metadata(
            model_dir / wfc_common.METADATA_FILENAME,
            {
                "fold_year": fold_year,
                "status": METADATA_STATUS_SKIPPED,
                "reason": "empty-train-or-valid",
            },
        )
        return {
            "fold_year": fold_year,
            "status": METADATA_STATUS_SKIPPED,
            "resumed": False,
            "rows": 0,
        }
    train_with_buckets = merge_bucket_weights_into_train(train_df, bucket_df)
    weighted_train = attach_sample_weights(train_with_buckets, args["alpha_bucket_weight"])
    ns = build_fold_namespace(args, fold_year, fold_years)
    fold_result = deps["fold_trainer"](weighted_train, valid_df, feature_cols, ns)
    valid_predictions = cast(pd.DataFrame, fold_result["valid_predictions"])
    metadata = {
        "fold_year": fold_year,
        "status": METADATA_STATUS_COMPLETED,
        "iteration_id": args["iteration_id"],
        "random_seed": ns.seed,
        "learning_rate": ns.learning_rate,
        "objective": args["objective"],
        "lambdarank_truncation_level": args["lambdarank_truncation_level"],
        "alpha_bucket_weight": args["alpha_bucket_weight"],
        "rows": int(len(valid_predictions)),
    }
    wfc_common.atomic_write_metadata(
        model_dir / wfc_common.METADATA_FILENAME, metadata,
    )
    return {
        "fold_year": fold_year,
        "status": METADATA_STATUS_COMPLETED,
        "resumed": False,
        "rows": int(len(valid_predictions)),
    }


def resolve_fold_years(args: TrainLightgbmArgs) -> list[int]:
    if args["year_to"] < args["year_from"]:
        raise ValueError(
            f"--year-to ({args['year_to']}) must be >= --year-from ({args['year_from']})",
        )
    return list(range(args["year_from"], args["year_to"] + 1))


def run(args: TrainLightgbmArgs, deps: TrainDeps) -> dict[str, object]:
    hpo_params = load_hpo_params(args["hpo_params_path"])
    merged_args = apply_hpo_params(args, hpo_params)
    df = deps["parquet_reader"](merged_args["features_parquet"])
    feature_cols = deps["feature_resolver"](df)
    bucket_df = (
        deps["bucket_reader"](merged_args["bucket_membership_parquet"])
        if merged_args["bucket_membership_parquet"] is not None else None
    )
    fold_years = resolve_fold_years(merged_args)
    folds = [
        train_fold(df, feature_cols, merged_args, fy, fold_years, deps, bucket_df)
        for fy in fold_years
    ]
    return {
        "category": merged_args["category"],
        "walk_forward_namespace": merged_args["walk_forward_namespace"],
        "iteration_id": merged_args["iteration_id"],
        "alpha_bucket_weight": merged_args["alpha_bucket_weight"],
        "objective": merged_args["objective"],
        "lambdarank_truncation_level": merged_args["lambdarank_truncation_level"],
        "fold_count": len(folds),
        "folds": folds,
    }


def default_parquet_reader(path: Path) -> pd.DataFrame:
    parts = sorted(path.glob("race_year=*/*.parquet"))
    return pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)


def default_bucket_reader(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def build_default_deps() -> TrainDeps:
    return {
        "parquet_reader": default_parquet_reader,
        "feature_resolver": resolve_feature_columns,
        "fold_trainer": default_fold_trainer,
        "bucket_reader": default_bucket_reader,
    }


def main(argv: list[str] | None = None) -> None:
    args = normalize_args(parse_args(argv))
    result = run(args, build_default_deps())
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
