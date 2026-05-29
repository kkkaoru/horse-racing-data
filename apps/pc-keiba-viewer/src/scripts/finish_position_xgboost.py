#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""XGBoost ranker walk-forward for finish-position prediction.

Mirrors the I/O contract of finish_position_lightgbm.py:
  --csv (parquet dir), --train-start-date, --validation-years, --output-report, --output-predictions-dir

Uses rank:pairwise objective with NDCG@3 metric.

Run with:
  apps/pc-keiba-viewer/.venv/bin/python apps/pc-keiba-viewer/src/scripts/finish_position_xgboost.py walk-forward \\
    --csv tmp/feat-v20-merged-v5/jra \\
    --train-start-date 20160101 \\
    --validation-years 2024,2025 \\
    --output-report tmp/finish-position-eval/xgb/jra/walk.json \\
    --output-predictions-dir tmp/finish-position-eval/xgb/jra/predictions
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import xgboost as xgb

META_COLUMNS = (
    "race_id", "race_date", "race_year", "source", "kaisai_nen", "kaisai_tsukihi",
    "keibajo_code", "race_bango", "ketto_toroku_bango", "umaban", "bamei",
    "kishumei_ryakusho", "chokyoshimei_ryakusho", "category",
)
LABEL_COLUMNS = ("finish_position", "finish_norm")
DEFAULT_RELEVANCE_RANK1 = 3
DEFAULT_RELEVANCE_RANK2 = 2
DEFAULT_RELEVANCE_RANK3 = 1
DEFAULT_RELEVANCE = 0
DEFAULT_NUM_ROUNDS = 500
DEFAULT_LEARNING_RATE = 0.05
DEFAULT_MAX_DEPTH = 8
DEFAULT_MIN_CHILD_WEIGHT = 30
DEFAULT_LAMBDA = 1.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="finish_position_xgboost")
    sub = parser.add_subparsers(dest="cmd", required=True)
    walk = sub.add_parser("walk-forward")
    walk.add_argument("--csv", type=Path, required=True)
    walk.add_argument("--train-start-date", type=str, default="20160101")
    walk.add_argument("--validation-years", type=str, default="2024,2025")
    walk.add_argument("--output-report", type=Path, required=True)
    walk.add_argument("--output-predictions-dir", type=Path, required=True)
    walk.add_argument("--num-rounds", type=int, default=DEFAULT_NUM_ROUNDS)
    walk.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    walk.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    walk.add_argument("--min-child-weight", type=int, default=DEFAULT_MIN_CHILD_WEIGHT)
    walk.add_argument("--reg-lambda", type=float, default=DEFAULT_LAMBDA)
    walk.add_argument("--early-stopping-rounds", type=int, default=30)
    walk.add_argument("--seed", type=int, default=20260519)
    walk.add_argument("--train-end-date", type=str, default=None,
                      help="Override train end date (YYYYMMDD). Use with --validation-from-date for OOT hold-out.")
    walk.add_argument("--validation-from-date", type=str, default=None,
                      help="Override validation start date (YYYYMMDD). Overrides validation-years.")
    walk.add_argument("--validation-to-date", type=str, default=None,
                      help="Override validation end date (YYYYMMDD). Pairs with --validation-from-date.")
    walk.add_argument("--relevance-rank1", type=int, default=DEFAULT_RELEVANCE_RANK1)
    walk.add_argument("--relevance-rank2", type=int, default=DEFAULT_RELEVANCE_RANK2,
                      help="Relevance for finish_position=2 (boost to emphasize)")
    walk.add_argument("--relevance-rank3", type=int, default=DEFAULT_RELEVANCE_RANK3)
    return parser.parse_args(argv)


def load_parquet_dir(path: Path) -> pd.DataFrame:
    parts = sorted(path.glob("race_year=*/*.parquet"))
    return pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)


def resolve_feature_columns(df: pd.DataFrame) -> list[str]:
    excluded = set(META_COLUMNS) | set(LABEL_COLUMNS)
    return [c for c in df.columns if c not in excluded and pd.api.types.is_numeric_dtype(df[c])]


def make_to_relevance(rank1: int, rank2: int, rank3: int):
    rel_map = {1: rank1, 2: rank2, 3: rank3}

    def _to(value: object) -> int:
        if value is None or pd.isna(cast(float, value)):
            return DEFAULT_RELEVANCE
        return rel_map.get(int(cast(float, value)), DEFAULT_RELEVANCE)

    return _to


def filter_range(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    mask = (df["race_date"] >= start) & (df["race_date"] <= end) & df["finish_position"].notna()
    return df[mask].copy()


def filter_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    year_str = str(year)
    mask = df["race_date"].str.startswith(year_str) & df["finish_position"].notna()
    return df[mask].copy()


def build_group_sizes(df: pd.DataFrame) -> list[int]:
    return df.groupby("race_id", sort=False).size().tolist()


def train_xgboost_ranker(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_cols: list[str],
    args: argparse.Namespace,
) -> tuple[xgb.Booster, dict[str, object]]:
    train_df = train_df.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    valid_df = valid_df.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    to_relevance = make_to_relevance(
        int(args.relevance_rank1), int(args.relevance_rank2), int(args.relevance_rank3),
    )
    train_labels = train_df["finish_position"].map(to_relevance).to_numpy(dtype=np.int32)
    valid_labels = valid_df["finish_position"].map(to_relevance).to_numpy(dtype=np.int32)
    dtrain = xgb.DMatrix(train_df[feature_cols], label=train_labels)
    dtrain.set_group(build_group_sizes(train_df))
    dvalid = xgb.DMatrix(valid_df[feature_cols], label=valid_labels)
    dvalid.set_group(build_group_sizes(valid_df))
    params = {
        "objective": "rank:pairwise",
        "eval_metric": "ndcg@3",
        "learning_rate": args.learning_rate,
        "max_depth": args.max_depth,
        "min_child_weight": args.min_child_weight,
        "reg_lambda": args.reg_lambda,
        "tree_method": "hist",
        "seed": args.seed,
        "verbosity": 1,
    }
    evals_result: dict[str, dict[str, list[float]]] = {}
    booster = xgb.train(
        params,
        dtrain,
        num_boost_round=args.num_rounds,
        evals=[(dvalid, "valid")],
        early_stopping_rounds=args.early_stopping_rounds,
        evals_result=evals_result,
        verbose_eval=50,
    )
    valid_pred = booster.predict(dvalid, iteration_range=(0, booster.best_iteration + 1))
    valid_df = valid_df.assign(predicted_score=valid_pred)
    valid_df["predicted_rank"] = (
        valid_df.groupby("race_id")["predicted_score"].rank(method="first", ascending=False).astype(int)
    )
    metrics = compute_fold_metrics(valid_df)
    return booster, {
        "best_iteration": int(booster.best_iteration),
        "valid_predictions": valid_df,
        "metrics": metrics,
    }


def _top1_hit(g: pd.DataFrame) -> int:
    return int(((g["predicted_rank"] == 1) & (g["finish_position"] == 1)).any())


def _top3_box_hit(g: pd.DataFrame) -> int:
    return int(g[g["predicted_rank"] <= 3]["finish_position"].le(3).sum() == 3)


def _top3_exact_hit(g: pd.DataFrame) -> int:
    return int(
        ((g["predicted_rank"] == 1) & (g["finish_position"] == 1)).any()
        and ((g["predicted_rank"] == 2) & (g["finish_position"] == 2)).any()
        and ((g["predicted_rank"] == 3) & (g["finish_position"] == 3)).any()
    )


def compute_fold_metrics(valid_df: pd.DataFrame) -> dict[str, float]:
    groups = [g for _, g in valid_df.groupby("race_id")]
    race_count = len(groups)
    top1_hits = [_top1_hit(g) for g in groups]
    top3_box = [_top3_box_hit(g) for g in groups]
    top3_exact = [_top3_exact_hit(g) for g in groups]
    return {
        "race_count": race_count,
        "valid_rows": int(len(valid_df)),
        "top1_accuracy": float(np.mean(top1_hits)) if top1_hits else 0.0,
        "top3_box_accuracy": float(np.mean(top3_box)) if top3_box else 0.0,
        "top3_exact_accuracy": float(np.mean(top3_exact)) if top3_exact else 0.0,
    }


def write_predictions_jsonl(valid_df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = valid_df[["race_id", "ketto_toroku_bango", "umaban", "predicted_score", "predicted_rank"]]
    with output_path.open("w", encoding="utf-8") as fp:
        for row in rows.itertuples(index=False):
            umaban_value = cast(float, row.umaban)
            fp.write(json.dumps({
                "race_id": row.race_id,
                "ketto_toroku_bango": row.ketto_toroku_bango,
                "umaban": int(umaban_value) if pd.notna(umaban_value) else None,
                "predicted_score": float(cast(float, row.predicted_score)),
                "predicted_rank": int(cast(float, row.predicted_rank)),
            }) + "\n")


def run_walk_forward(args: argparse.Namespace) -> None:
    df = load_parquet_dir(args.csv)
    feature_cols = resolve_feature_columns(df)
    folds: list[dict[str, object]] = []
    if args.validation_from_date and args.validation_to_date:
        train_end = args.train_end_date or args.validation_from_date
        train_df = filter_range(df, args.train_start_date, train_end)
        valid_df = filter_range(df, args.validation_from_date, args.validation_to_date)
        if len(train_df) > 0 and len(valid_df) > 0:
            _, result = train_xgboost_ranker(train_df, valid_df, feature_cols, args)
            fold_label = f"{args.validation_from_date}-{args.validation_to_date}"
            write_predictions_jsonl(
                cast(pd.DataFrame, result["valid_predictions"]),
                args.output_predictions_dir / f"{fold_label}.jsonl",
            )
            folds.append({
                "fold_label": fold_label,
                "best_iteration": result["best_iteration"],
                **cast(dict[str, object], result["metrics"]),
            })
    else:
        validation_years = [int(y) for y in args.validation_years.split(",")]
        for valid_year in validation_years:
            train_end = args.train_end_date or f"{valid_year - 1}1231"
            train_df = filter_range(df, args.train_start_date, train_end)
            valid_df = filter_year(df, valid_year)
            if len(train_df) == 0 or len(valid_df) == 0:
                continue
            _, result = train_xgboost_ranker(train_df, valid_df, feature_cols, args)
            write_predictions_jsonl(
                cast(pd.DataFrame, result["valid_predictions"]),
                args.output_predictions_dir / f"{valid_year}.jsonl",
            )
            folds.append({
                "fold_year": valid_year,
                "best_iteration": result["best_iteration"],
                **cast(dict[str, object], result["metrics"]),
            })
    aggregate = {
        "fold_count": len(folds),
        "top1_accuracy_mean": sum(cast(float, f["top1_accuracy"]) for f in folds) / max(len(folds), 1),
        "top3_box_accuracy_mean": sum(cast(float, f["top3_box_accuracy"]) for f in folds) / max(len(folds), 1),
        "top3_exact_accuracy_mean": sum(cast(float, f["top3_exact_accuracy"]) for f in folds) / max(len(folds), 1),
    }
    report = {"aggregate": aggregate, "folds": folds}
    args.output_report.parent.mkdir(parents=True, exist_ok=True)
    args.output_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))


def main() -> None:
    args = parse_args()
    if args.cmd == "walk-forward":
        run_walk_forward(args)


if __name__ == "__main__":
    main()
