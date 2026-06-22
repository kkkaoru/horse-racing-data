#!/usr/bin/env python3
"""XGBoost walk-forward trainer for the v8 iterative loop.

Mirrors ``train_finish_position_catboost_walk_forward`` with the XGBoost
default trainer plus Lever 11's ``--objective {pairwise,ndcg}`` flag.

``ndcg`` selects ``rank:ndcg`` with ``lambdarank_pair_method=topk`` /
``lambdarank_num_pair_per_sample=3``; ``pairwise`` keeps the
production-prior ``rank:pairwise`` objective.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TYPE_CHECKING, Final, Protocol, TypedDict, cast

import pandas as pd

import walk_forward_common as wfc_common

if TYPE_CHECKING:
    from collections.abc import Callable

OBJECTIVE_PAIRWISE: Final[str] = "pairwise"
OBJECTIVE_NDCG: Final[str] = "ndcg"
SUPPORTED_OBJECTIVES: Final[tuple[str, ...]] = (OBJECTIVE_PAIRWISE, OBJECTIVE_NDCG)
LAMBDARANK_PAIR_METHOD: Final[str] = "topk"
LAMBDARANK_NUM_PAIR_PER_SAMPLE: Final[int] = 3
DEFAULT_ITERATION_ID: Final[int] = 0
DEFAULT_ALPHA_BUCKET_WEIGHT: Final[float] = 0.0
DEFAULT_FINE_TUNE_FINAL_FOLDS: Final[int] = 0
DEFAULT_FINE_TUNE_LR_DIVISOR: Final[int] = 10
DEFAULT_MIN_CHILD_WEIGHT: Final[int] = 30
DEFAULT_LAMBDA: Final[float] = 1.0
DEFAULT_SUBSAMPLE: Final[float] = 1.0
DEFAULT_COLSAMPLE_BYTREE: Final[float] = 1.0
RANDOM_SEED_BASE: Final[int] = 42
DEFAULT_TRAIN_START_DATE: Final[str] = "20060101"
METADATA_STATUS_COMPLETED: Final[str] = "completed"
METADATA_STATUS_SKIPPED: Final[str] = "skipped"


class TrainXgboostArgs(TypedDict):
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
    hpo_params_path: Path | None
    bucket_membership_parquet: Path | None
    resume_from_checkpoint: bool
    fine_tune_final_folds: int
    fine_tune_lr_divisor: int
    num_rounds: int
    max_depth: int
    min_child_weight: int
    reg_lambda: float
    subsample: float
    colsample_bytree: float
    learning_rate: float


class ParquetReaderLike(Protocol):
    def __call__(self, path: Path) -> pd.DataFrame: ...


class FoldTrainerLike(Protocol):
    def __call__(
        self,
        train_df: pd.DataFrame,
        valid_df: pd.DataFrame,
        feature_cols: list[str],
        args: argparse.Namespace,
    ) -> tuple[object, dict[str, object]]: ...


class BucketMembershipReaderLike(Protocol):
    def __call__(self, path: Path) -> pd.DataFrame: ...


class TrainDeps(TypedDict):
    parquet_reader: ParquetReaderLike
    feature_resolver: Callable[[pd.DataFrame], list[str]]
    fold_trainer: FoldTrainerLike
    bucket_reader: BucketMembershipReaderLike


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="train_finish_position_xgboost_walk_forward")
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
        default=OBJECTIVE_PAIRWISE,
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
    parser.add_argument("--num-rounds", type=int, default=450)
    parser.add_argument("--max-depth", type=int, default=6)
    parser.add_argument("--min-child-weight", type=int, default=DEFAULT_MIN_CHILD_WEIGHT)
    parser.add_argument("--reg-lambda", type=float, default=DEFAULT_LAMBDA)
    parser.add_argument("--subsample", type=float, default=DEFAULT_SUBSAMPLE)
    parser.add_argument("--colsample-bytree", type=float, default=DEFAULT_COLSAMPLE_BYTREE)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def normalize_args(args: argparse.Namespace) -> TrainXgboostArgs:
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
        "num_rounds": int(cast(int, args.num_rounds)),
        "max_depth": int(cast(int, args.max_depth)),
        "min_child_weight": int(cast(int, args.min_child_weight)),
        "reg_lambda": float(cast(float, args.reg_lambda)),
        "subsample": float(cast(float, args.subsample)),
        "colsample_bytree": float(cast(float, args.colsample_bytree)),
        "learning_rate": float(cast(float, args.learning_rate)),
    }


def load_hpo_params(path: Path | None) -> dict[str, object]:
    if path is None:
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"HPO params file must be a JSON object, got {type(parsed)!r}")
    top = cast(dict[str, object], parsed)
    if "params" in top and isinstance(top["params"], dict):
        return cast(dict[str, object], top["params"])
    return top


def apply_hpo_params(args: TrainXgboostArgs, params: dict[str, object]) -> TrainXgboostArgs:
    merged = cast(TrainXgboostArgs, dict(args))
    if "num_rounds" in params:
        merged["num_rounds"] = int(cast(int, params["num_rounds"]))
    if "n_estimators" in params:
        merged["num_rounds"] = int(cast(int, params["n_estimators"]))
    if "max_depth" in params:
        merged["max_depth"] = int(cast(int, params["max_depth"]))
    if "min_child_weight" in params:
        merged["min_child_weight"] = int(cast(int, params["min_child_weight"]))
    if "reg_lambda" in params:
        merged["reg_lambda"] = float(cast(float, params["reg_lambda"]))
    if "subsample" in params:
        merged["subsample"] = float(cast(float, params["subsample"]))
    if "colsample_bytree" in params:
        merged["colsample_bytree"] = float(cast(float, params["colsample_bytree"]))
    if "learning_rate" in params:
        merged["learning_rate"] = float(cast(float, params["learning_rate"]))
    return merged


def resolve_fold_random_seed(fold_year: int) -> int:
    return RANDOM_SEED_BASE + fold_year


def build_per_fold_model_dir(args: TrainXgboostArgs, fold_year: int) -> Path:
    return args["model_root"] / args["category"] / f"iter{args['iteration_id']}" / f"fold-{fold_year}"


def resolve_fold_learning_rate(
    args: TrainXgboostArgs, fold_year: int, fold_years: list[int],
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
    deduped = bucket_df[merge_cols].drop_duplicates("race_id")
    return train_df.merge(deduped, on="race_id", how="left")


def attach_sample_weights(train_df: pd.DataFrame, alpha: float) -> pd.DataFrame:
    import numpy as np

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
    args: TrainXgboostArgs, fold_year: int, fold_years: list[int],
) -> argparse.Namespace:
    fold_lr = resolve_fold_learning_rate(args, fold_year, fold_years)
    ns = argparse.Namespace(
        num_rounds=args["num_rounds"],
        max_depth=args["max_depth"],
        learning_rate=fold_lr,
        min_child_weight=args["min_child_weight"],
        reg_lambda=args["reg_lambda"],
        subsample=args["subsample"],
        colsample_bytree=args["colsample_bytree"],
        early_stopping_rounds=30,
        seed=resolve_fold_random_seed(fold_year),
        relevance_rank1=3,
        relevance_rank2=2,
        relevance_rank3=1,
        objective=args["objective"],
    )
    if args["objective"] == OBJECTIVE_NDCG:
        ns.lambdarank_pair_method = LAMBDARANK_PAIR_METHOD
        ns.lambdarank_num_pair_per_sample = LAMBDARANK_NUM_PAIR_PER_SAMPLE
    return ns


def train_fold(
    df: pd.DataFrame,
    feature_cols: list[str],
    args: TrainXgboostArgs,
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
    booster, fold_result = deps["fold_trainer"](weighted_train, valid_df, feature_cols, ns)
    model_dir.mkdir(parents=True, exist_ok=True)
    booster.save_model(str(model_dir / "model.json"))
    valid_predictions = cast(pd.DataFrame, fold_result["valid_predictions"])
    metadata = {
        "fold_year": fold_year,
        "status": METADATA_STATUS_COMPLETED,
        "iteration_id": args["iteration_id"],
        "random_seed": ns.seed,
        "learning_rate": ns.learning_rate,
        "objective": args["objective"],
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


def resolve_fold_years(args: TrainXgboostArgs) -> list[int]:
    if args["year_to"] < args["year_from"]:
        raise ValueError(
            f"--year-to ({args['year_to']}) must be >= --year-from ({args['year_from']})",
        )
    return list(range(args["year_from"], args["year_to"] + 1))


def run(args: TrainXgboostArgs, deps: TrainDeps) -> dict[str, object]:
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
        "fold_count": len(folds),
        "folds": folds,
    }


def default_parquet_reader(path: Path) -> pd.DataFrame:
    import finish_position_xgboost as xgb_walk
    return xgb_walk.load_parquet_dir(path)


def default_feature_resolver(df: pd.DataFrame) -> list[str]:
    import finish_position_xgboost as xgb_walk
    return xgb_walk.resolve_feature_columns(df)


def default_fold_trainer(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_cols: list[str],
    args: argparse.Namespace,
) -> tuple[object, dict[str, object]]:
    import finish_position_xgboost as xgb_walk
    return xgb_walk.train_xgboost_ranker(train_df, valid_df, feature_cols, args)


def default_bucket_reader(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def build_default_deps() -> TrainDeps:
    return {
        "parquet_reader": default_parquet_reader,
        "feature_resolver": default_feature_resolver,
        "fold_trainer": default_fold_trainer,
        "bucket_reader": default_bucket_reader,
    }


def main(argv: list[str] | None = None) -> None:
    args = normalize_args(parse_args(argv))
    result = run(args, build_default_deps())
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
