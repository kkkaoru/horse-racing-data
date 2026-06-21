#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""CatBoost ranker walk-forward for finish-position prediction.

Mirrors the I/O contract of finish_position_lightgbm.py walk-forward:
  --csv (parquet dir), --train-start-date, --validation-years,
  --output-report, --output-predictions-dir

Uses YetiRank objective with NDCG@3 metric.

Run with:
  apps/pc-keiba-viewer/.venv/bin/python apps/pc-keiba-viewer/src/scripts/finish_position_catboost.py walk-forward \\
    --csv tmp/feat-v20-merged-v5/jra \\
    --train-start-date 20160101 \\
    --validation-years 2024,2025 \\
    --output-report tmp/finish-position-eval/catboost/jra/walk.json \\
    --output-predictions-dir tmp/finish-position-eval/catboost/jra/predictions
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
from catboost import CatBoost, Pool  # pyright: ignore[reportMissingTypeStubs]

META_COLUMNS = (
    "race_id", "race_date", "race_year", "source", "kaisai_nen", "kaisai_tsukihi",
    "race_bango", "ketto_toroku_bango", "bamei",
    "kishumei_ryakusho", "chokyoshimei_ryakusho", "category",
)
LABEL_COLUMNS = ("finish_position", "finish_norm")
CATEGORICAL_FEATURE_NAMES = ("keibajo_code", "track_code", "grade_code", "umaban")
DEFAULT_RELEVANCE_RANK1 = 3
DEFAULT_RELEVANCE_RANK2 = 2
DEFAULT_RELEVANCE_RANK3 = 1
DEFAULT_ITERATIONS = 500
DEFAULT_LEARNING_RATE = 0.05
DEFAULT_DEPTH = 8
DEFAULT_L2 = 3.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="finish_position_catboost")
    sub = parser.add_subparsers(dest="cmd", required=True)
    walk = sub.add_parser("walk-forward")
    walk.add_argument("--csv", type=Path, required=True)
    walk.add_argument("--train-start-date", type=str, default="20160101")
    walk.add_argument("--validation-years", type=str, default="2024,2025")
    walk.add_argument("--output-report", type=Path, required=True)
    walk.add_argument("--output-predictions-dir", type=Path, required=True)
    walk.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    walk.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    walk.add_argument("--depth", type=int, default=DEFAULT_DEPTH)
    walk.add_argument("--l2-leaf-reg", type=float, default=DEFAULT_L2)
    walk.add_argument("--early-stopping-rounds", type=int, default=30)
    walk.add_argument("--seed", type=int, default=20260519)
    walk.add_argument("--train-end-date", type=str, default=None,
                      help="Override train end date (YYYYMMDD). Use with --validation-from-date for OOT hold-out.")
    walk.add_argument("--validation-from-date", type=str, default=None,
                      help="Override validation start date (YYYYMMDD). Overrides validation-years.")
    walk.add_argument("--validation-to-date", type=str, default=None,
                      help="Override validation end date (YYYYMMDD). Pairs with --validation-from-date.")
    walk.add_argument("--relevance-rank1", type=int, default=DEFAULT_RELEVANCE_RANK1,
                      help="Relevance for finish_position=1 (default 3)")
    walk.add_argument("--relevance-rank2", type=int, default=DEFAULT_RELEVANCE_RANK2,
                      help="Relevance for finish_position=2 (default 2; boost to 3+ to emphasize)")
    walk.add_argument("--relevance-rank3", type=int, default=DEFAULT_RELEVANCE_RANK3,
                      help="Relevance for finish_position=3 (default 1)")
    walk.add_argument("--no-cat-features", action="store_true",
                      help="Disable categorical feature handling (production-prior behavior)")
    return parser.parse_args(argv)


def load_parquet_dir(path: Path) -> pd.DataFrame:
    parts = sorted(path.glob("race_year=*/*.parquet"))
    if not parts:
        raise ValueError(f"no parquet files found under {path}")
    return pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)


def resolve_feature_columns(df: pd.DataFrame, use_cat_features: bool = True) -> list[str]:
    excluded = set(META_COLUMNS) | set(LABEL_COLUMNS)
    if not use_cat_features:
        excluded |= set(CATEGORICAL_FEATURE_NAMES)
    numeric = [
        c for c in df.columns
        if c not in excluded
        and c not in CATEGORICAL_FEATURE_NAMES
        and pd.api.types.is_numeric_dtype(df[c])
    ]
    cats = (
        [c for c in CATEGORICAL_FEATURE_NAMES if c in df.columns]
        if use_cat_features else []
    )
    return numeric + cats


def resolve_cat_feature_indices(
    df: pd.DataFrame, feature_cols: list[str], use_cat_features: bool = True,
) -> list[int]:
    if not use_cat_features:
        return []
    return [
        i for i, c in enumerate(feature_cols)
        if c in CATEGORICAL_FEATURE_NAMES
    ]


def make_to_relevance(rank1: int, rank2: int, rank3: int):
    rel_map = {1: rank1, 2: rank2, 3: rank3}

    def _to(value: object) -> int:
        if value is None or pd.isna(cast(float, value)):
            return 0
        return rel_map.get(int(cast(float, value)), 0)

    return _to


def subtract_one_day(date_str: str) -> str:
    """Return the date string for the day before date_str (YYYYMMDD format)."""
    dt = datetime.strptime(date_str, "%Y%m%d")
    return (dt - timedelta(days=1)).strftime("%Y%m%d")


def filter_range(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    mask = (df["race_date"] >= start) & (df["race_date"] <= end) & df["finish_position"].notna()
    return df[mask].copy()


def filter_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    year_str = str(year)
    mask = df["race_date"].str.startswith(year_str) & df["finish_position"].notna()
    return df[mask].copy()


def race_group_ids(df: pd.DataFrame) -> np.ndarray:
    return df["race_id"].astype("category").cat.codes.to_numpy()


def _prepare_feature_matrix(
    df: pd.DataFrame, feature_cols: list[str], cat_indices: list[int],
) -> pd.DataFrame:
    cat_set = {feature_cols[i] for i in cat_indices}
    out = df[feature_cols].copy()
    for col in feature_cols:
        if col in cat_set:
            out[col] = out[col].astype("string").fillna("__missing__").astype(str)
        else:
            out[col] = out[col].astype(np.float32)
    return out


def train_catboost_ranker(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_cols: list[str],
    args: argparse.Namespace,
) -> dict[str, object]:
    train_df = train_df.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    valid_df = valid_df.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    to_relevance = make_to_relevance(
        int(args.relevance_rank1), int(args.relevance_rank2), int(args.relevance_rank3),
    )
    train_labels = train_df["finish_position"].map(to_relevance).to_numpy(dtype=np.int32)
    valid_labels = valid_df["finish_position"].map(to_relevance).to_numpy(dtype=np.int32)
    use_cat = not getattr(args, "no_cat_features", False)
    cat_indices = resolve_cat_feature_indices(train_df, feature_cols, use_cat_features=use_cat)
    train_features = _prepare_feature_matrix(train_df, feature_cols, cat_indices)
    valid_features = _prepare_feature_matrix(valid_df, feature_cols, cat_indices)
    train_weights = train_df["sample_weight"].to_numpy() if "sample_weight" in train_df.columns else None
    train_pool = Pool(
        data=train_features,
        label=train_labels,
        group_id=race_group_ids(train_df),
        cat_features=cat_indices if cat_indices else None,
        weight=train_weights,
    )
    valid_pool = Pool(
        data=valid_features,
        label=valid_labels,
        group_id=race_group_ids(valid_df),
        cat_features=cat_indices if cat_indices else None,
    )
    params = {
        "loss_function": "YetiRank",
        "eval_metric": "NDCG:top=3",
        "iterations": args.iterations,
        "learning_rate": args.learning_rate,
        "depth": args.depth,
        "l2_leaf_reg": args.l2_leaf_reg,
        "od_type": "Iter",
        "od_wait": args.early_stopping_rounds,
        "random_seed": args.seed,
        "task_type": "CPU",
        "verbose": 50,
    }
    model = CatBoost(params)
    model.fit(train_pool, eval_set=valid_pool, verbose=False)
    pred = model.predict(valid_pool)
    valid_df = valid_df.assign(predicted_score=pred)
    valid_df["predicted_rank"] = (
        valid_df.groupby("race_id")["predicted_score"]
        .rank(method="first", ascending=False, na_option="bottom")
        .astype(int)
    )
    metrics = compute_fold_metrics(valid_df)
    best_iter = model.get_best_iteration()
    return {
        "best_iteration": int(best_iter if best_iter is not None else cast(int, model.tree_count_)),
        "valid_predictions": valid_df,
        "metrics": metrics,
        "model": model,
    }


def _cb_top1_hit(g: pd.DataFrame) -> int:
    return int(((g["predicted_rank"] == 1) & (g["finish_position"] == 1)).any())


def _cb_top3_box_hit(g: pd.DataFrame) -> int:
    return int(g[g["predicted_rank"] <= 3]["finish_position"].le(3).sum() == 3)


def compute_fold_metrics(valid_df: pd.DataFrame) -> dict[str, float]:
    groups = [g for _, g in valid_df.groupby("race_id")]
    race_count = len(groups)
    top1_hits = [_cb_top1_hit(g) for g in groups]
    top3_box = [_cb_top3_box_hit(g) for g in groups]
    return {
        "race_count": race_count,
        "valid_rows": int(len(valid_df)),
        "top1_accuracy": float(np.mean(top1_hits)) if top1_hits else 0.0,
        "top3_box_accuracy": float(np.mean(top3_box)) if top3_box else 0.0,
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
    use_cat = not getattr(args, "no_cat_features", False)
    feature_cols = resolve_feature_columns(df, use_cat_features=use_cat)
    folds: list[dict[str, object]] = []
    if args.validation_from_date and args.validation_to_date:
        train_end = args.train_end_date or subtract_one_day(args.validation_from_date)
        train_df = filter_range(df, args.train_start_date, train_end)
        valid_df = filter_range(df, args.validation_from_date, args.validation_to_date)
        if len(train_df) > 0 and len(valid_df) > 0:
            result = train_catboost_ranker(train_df, valid_df, feature_cols, args)
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
            result = train_catboost_ranker(train_df, valid_df, feature_cols, args)
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
