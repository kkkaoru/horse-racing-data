#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Train XGB ranker on labeled history, predict prospective (unlabeled) races.

Different from finish_position_xgboost.py walk-forward in that the validation
set is *not* filtered by `finish_position.notna()` — that lets us emit
predictions for races whose results haven't been recorded yet.

Run with:
  apps/pc-keiba-viewer/.venv/bin/python \\
    apps/pc-keiba-viewer/src/scripts/finish_position_xgboost_predict_only.py \\
    --csv tmp/feat-jra-2026-lineage \\
    --train-start-date 20070101 \\
    --train-end-date 20260523 \\
    --predict-date 20260524 \\
    --output-jsonl tmp/finish-position-eval/predictions-jra-xgb-v7-2026/jra/20260524.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

sys.path.insert(0, str(Path(__file__).parent))
from finish_position_xgboost import (  # noqa: E402
    DEFAULT_LAMBDA,
    DEFAULT_LEARNING_RATE,
    DEFAULT_MAX_DEPTH,
    DEFAULT_MIN_CHILD_WEIGHT,
    DEFAULT_NUM_ROUNDS,
    DEFAULT_RELEVANCE_RANK1,
    DEFAULT_RELEVANCE_RANK2,
    DEFAULT_RELEVANCE_RANK3,
    build_group_sizes,
    load_parquet_dir,
    make_to_relevance,
    resolve_feature_columns,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="finish_position_xgboost_predict_only")
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--train-start-date", type=str, required=True,
                        help="YYYYMMDD lower bound for training rows.")
    parser.add_argument("--train-end-date", type=str, required=True,
                        help="YYYYMMDD upper bound (inclusive) for training rows.")
    parser.add_argument("--predict-date", type=str, required=True,
                        help="YYYYMMDD of races to score (results allowed to be null).")
    parser.add_argument("--output-jsonl", type=Path, required=True)
    parser.add_argument("--num-rounds", type=int, default=DEFAULT_NUM_ROUNDS)
    parser.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    parser.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    parser.add_argument("--min-child-weight", type=int, default=DEFAULT_MIN_CHILD_WEIGHT)
    parser.add_argument("--reg-lambda", type=float, default=DEFAULT_LAMBDA)
    parser.add_argument("--seed", type=int, default=20260524)
    parser.add_argument("--relevance-rank1", type=int, default=DEFAULT_RELEVANCE_RANK1)
    parser.add_argument("--relevance-rank2", type=int, default=DEFAULT_RELEVANCE_RANK2)
    parser.add_argument("--relevance-rank3", type=int, default=DEFAULT_RELEVANCE_RANK3)
    return parser.parse_args()


def slice_train(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    mask = (
        (df["race_date"] >= start)
        & (df["race_date"] <= end)
        & df["finish_position"].notna()
    )
    return df[mask].sort_values(["race_id", "umaban"]).reset_index(drop=True)


def slice_predict(df: pd.DataFrame, predict_date: str) -> pd.DataFrame:
    mask = df["race_date"] == predict_date
    return df[mask].sort_values(["race_id", "umaban"]).reset_index(drop=True)


def train_booster(args: argparse.Namespace, train_df: pd.DataFrame, feature_cols: list[str]) -> xgb.Booster:
    to_rel = make_to_relevance(
        args.relevance_rank1,
        args.relevance_rank2,
        args.relevance_rank3,
    )
    labels = train_df["finish_position"].map(to_rel).to_numpy(dtype=np.int32)
    dtrain = xgb.DMatrix(train_df[feature_cols], label=labels)
    dtrain.set_group(build_group_sizes(train_df))
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
    return xgb.train(params, dtrain, num_boost_round=args.num_rounds, verbose_eval=50)


def write_predictions_jsonl(valid_df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = valid_df[["race_id", "ketto_toroku_bango", "umaban", "predicted_score", "predicted_rank"]]
    with output_path.open("w", encoding="utf-8") as fp:
        for row in rows.itertuples(index=False):
            fp.write(json.dumps({
                "race_id": row.race_id,
                "ketto_toroku_bango": row.ketto_toroku_bango,
                "umaban": int(row.umaban) if pd.notna(row.umaban) else None,
                "predicted_score": float(row.predicted_score),
                "predicted_rank": int(row.predicted_rank),
            }) + "\n")


def main() -> None:
    args = parse_args()
    df = load_parquet_dir(args.csv)
    feature_cols = resolve_feature_columns(df)
    train_df = slice_train(df, args.train_start_date, args.train_end_date)
    predict_df = slice_predict(df, args.predict_date)
    if len(train_df) == 0:
        raise SystemExit(f"train slice empty for {args.train_start_date}-{args.train_end_date}")
    if len(predict_df) == 0:
        raise SystemExit(f"predict slice empty for {args.predict_date}")
    booster = train_booster(args, train_df, feature_cols)
    dpredict = xgb.DMatrix(predict_df[feature_cols])
    scores = booster.predict(dpredict)
    predict_df = predict_df.assign(predicted_score=scores)
    predict_df["predicted_rank"] = (
        predict_df.groupby("race_id")["predicted_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    write_predictions_jsonl(predict_df, args.output_jsonl)
    print(json.dumps({
        "train_rows": len(train_df),
        "predict_rows": len(predict_df),
        "predict_races": predict_df["race_id"].nunique(),
        "output_jsonl": str(args.output_jsonl),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
