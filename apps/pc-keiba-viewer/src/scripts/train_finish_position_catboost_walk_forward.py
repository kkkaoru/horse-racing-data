#!/usr/bin/env python3
"""CatBoost walk-forward trainer for the v8 iterative loop.

Wraps the per-fold ``finish_position_catboost.train_catboost_ranker`` so each
Stage C iteration gets:

  * ``--iteration-id`` namespace stamping for output dirs / model versions
  * ``--alpha-bucket-weight`` (Lever 4) bucket-aware sample weight compose
  * ``--hpo-params-path`` Optuna-output JSON consumption
  * ``--resume-from-checkpoint`` reuse of completed per-fold metadata
  * ``--bucket-membership-parquet`` (per-race is_weak_bucket_score)
  * ``--fine-tune-final-folds`` + ``--fine-tune-lr-divisor`` (Lever 14)

Every helper is mockable; the CLI ``main`` builds default deps and the test
suite injects fakes.
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

DEFAULT_ITERATION_ID: Final[int] = 0
DEFAULT_ALPHA_BUCKET_WEIGHT: Final[float] = 0.0
DEFAULT_FINE_TUNE_FINAL_FOLDS: Final[int] = 0
DEFAULT_FINE_TUNE_LR_DIVISOR: Final[int] = 10
RANDOM_SEED_BASE: Final[int] = 42
DEFAULT_TRAIN_START_DATE: Final[str] = "20060101"
METADATA_STATUS_COMPLETED: Final[str] = "completed"
METADATA_STATUS_SKIPPED: Final[str] = "skipped"


class TrainCatBoostArgs(TypedDict):
    features_parquet: Path
    category: str
    walk_forward_namespace: str
    year_from: int
    year_to: int
    train_start_date: str
    model_root: Path
    iteration_id: int
    alpha_bucket_weight: float
    hpo_params_path: Path | None
    bucket_membership_parquet: Path | None
    resume_from_checkpoint: bool
    fine_tune_final_folds: int
    fine_tune_lr_divisor: int
    iterations: int
    depth: int
    l2_leaf_reg: float
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
    ) -> dict[str, object]: ...


class BucketMembershipReaderLike(Protocol):
    def __call__(self, path: Path) -> pd.DataFrame: ...


class TrainDeps(TypedDict):
    parquet_reader: ParquetReaderLike
    feature_resolver: Callable[[pd.DataFrame], list[str]]
    fold_trainer: FoldTrainerLike
    bucket_reader: BucketMembershipReaderLike


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="train_finish_position_catboost_walk_forward")
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
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--depth", type=int, default=8)
    parser.add_argument("--l2-leaf-reg", type=float, default=3.0)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def normalize_args(args: argparse.Namespace) -> TrainCatBoostArgs:
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
        "iterations": int(cast(int, args.iterations)),
        "depth": int(cast(int, args.depth)),
        "l2_leaf_reg": float(cast(float, args.l2_leaf_reg)),
        "learning_rate": float(cast(float, args.learning_rate)),
    }


def load_hpo_params(path: Path | None) -> dict[str, object]:
    if path is None:
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"HPO params file must be a JSON object, got {type(parsed)!r}")
    return cast(dict[str, object], parsed)


def apply_hpo_params(args: TrainCatBoostArgs, params: dict[str, object]) -> TrainCatBoostArgs:
    merged = cast(TrainCatBoostArgs, dict(args))
    if "iterations" in params:
        merged["iterations"] = int(cast(int, params["iterations"]))
    if "depth" in params:
        merged["depth"] = int(cast(int, params["depth"]))
    if "l2_leaf_reg" in params:
        merged["l2_leaf_reg"] = float(cast(float, params["l2_leaf_reg"]))
    if "learning_rate" in params:
        merged["learning_rate"] = float(cast(float, params["learning_rate"]))
    return merged


def resolve_fold_random_seed(fold_year: int) -> int:
    return RANDOM_SEED_BASE + fold_year


def build_per_fold_model_dir(args: TrainCatBoostArgs, fold_year: int) -> Path:
    return args["model_root"] / args["category"] / f"iter{args['iteration_id']}" / f"fold-{fold_year}"


def resolve_fold_learning_rate(
    args: TrainCatBoostArgs, fold_year: int, fold_years: list[int],
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
    train_df: pd.DataFrame,
    bucket_df: pd.DataFrame | None,
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
    args: TrainCatBoostArgs, fold_year: int, fold_years: list[int],
) -> argparse.Namespace:
    fold_lr = resolve_fold_learning_rate(args, fold_year, fold_years)
    return argparse.Namespace(
        iterations=args["iterations"],
        depth=args["depth"],
        l2_leaf_reg=args["l2_leaf_reg"],
        learning_rate=fold_lr,
        early_stopping_rounds=30,
        seed=resolve_fold_random_seed(fold_year),
        relevance_rank1=3,
        relevance_rank2=2,
        relevance_rank3=1,
        no_cat_features=False,
    )


def train_fold(
    df: pd.DataFrame,
    feature_cols: list[str],
    args: TrainCatBoostArgs,
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
    saved_model = fold_result.get("model")
    if saved_model is not None:
        saved_model.save_model(str(model_dir / "model.json"), format="json")
    valid_predictions = cast(pd.DataFrame, fold_result["valid_predictions"])
    metadata = {
        "fold_year": fold_year,
        "status": METADATA_STATUS_COMPLETED,
        "iteration_id": args["iteration_id"],
        "random_seed": ns.seed,
        "learning_rate": ns.learning_rate,
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


def resolve_fold_years(args: TrainCatBoostArgs) -> list[int]:
    if args["year_to"] < args["year_from"]:
        raise ValueError(
            f"--year-to ({args['year_to']}) must be >= --year-from ({args['year_from']})",
        )
    return list(range(args["year_from"], args["year_to"] + 1))


def run(args: TrainCatBoostArgs, deps: TrainDeps) -> dict[str, object]:
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
        "fold_count": len(folds),
        "folds": folds,
    }


def default_parquet_reader(path: Path) -> pd.DataFrame:
    import finish_position_catboost as cb_walk
    return cb_walk.load_parquet_dir(path)


def default_feature_resolver(df: pd.DataFrame) -> list[str]:
    import finish_position_catboost as cb_walk
    return cb_walk.resolve_feature_columns(df, use_cat_features=False)


def default_fold_trainer(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_cols: list[str],
    args: argparse.Namespace,
) -> dict[str, object]:
    import finish_position_catboost as cb_walk
    return cb_walk.train_catboost_ranker(train_df, valid_df, feature_cols, args)


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
