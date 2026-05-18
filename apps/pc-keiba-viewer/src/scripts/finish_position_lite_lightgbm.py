#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Train a 'lite' LGBM finish-position model with the 25-feature subset that
a Cloudflare Worker can recompute from `race_entry_corner_features` via
Hyperdrive at entry time. Reuses the column names that already exist in
the v15-rs parquet so we don't need a separate enrichment pipeline.

Run with:
  cd apps/pc-keiba-viewer && .venv/bin/python src/scripts/finish_position_lite_lightgbm.py \\
    --parquet ../../tmp/feat-v15-rs/jra \\
    --train-end-date 20251231 \\
    --model-version jra-lite-lgbm-v1.0 \\
    --output-model-dir ../../tmp/models/finish-position-lite/jra
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter

import lightgbm as lgb
import numpy as np
import pandas as pd

LITE_NUMERIC_FEATURES: tuple[str, ...] = (
    "kyori",
    "shusso_tosu",
    "umaban",
    "umaban_norm",
    "popularity_score",
)
LITE_CATEGORICAL_FEATURES: tuple[str, ...] = ()
LITE_AGGREGATE_FEATURES: tuple[str, ...] = (
    "career_win_rate",
    "career_place_rate",
    "past_corner_1_norm_avg_5",
    "past_nige_rate_self",
    "past_senkou_rate_self",
    "past_sashi_rate_self",
    "past_oikomi_rate_self",
    "jockey_career_win_rate",
    "jockey_nige_rate",
    "jockey_senkou_rate",
    "rs_p_nige",
    "rs_p_senkou",
    "rs_p_sashi",
    "rs_p_oikomi",
)

RELEVANCE_BY_RANK = {1: 3, 2: 2, 3: 1}
DEFAULT_RELEVANCE = 0
DEFAULT_NUM_LEAVES = 63
DEFAULT_LEARNING_RATE = 0.05
DEFAULT_MIN_CHILD_SAMPLES = 30
DEFAULT_NUM_ITERATIONS = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="finish_position_lite_lightgbm")
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--train-start-date", type=str, default="20160101")
    parser.add_argument("--train-end-date", type=str, required=True)
    parser.add_argument("--model-version", type=str, required=True)
    parser.add_argument("--output-model-dir", type=Path, required=True)
    return parser.parse_args()


def select_feature_columns() -> list[str]:
    return [*LITE_NUMERIC_FEATURES, *LITE_CATEGORICAL_FEATURES, *LITE_AGGREGATE_FEATURES]


def to_relevance(value: object) -> int:
    if value is None or pd.isna(value):
        return DEFAULT_RELEVANCE
    return RELEVANCE_BY_RANK.get(int(value), DEFAULT_RELEVANCE)


def encode_categoricals(frame: pd.DataFrame) -> pd.DataFrame:
    for column in LITE_CATEGORICAL_FEATURES:
        if column in frame.columns:
            frame[column] = frame[column].astype("category")
    return frame


def filter_train_range(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    mask = (df["race_date"] >= start) & (df["race_date"] <= end) & df["finish_position"].notna()
    return df[mask].copy()


def build_label_array(finish_positions: pd.Series) -> np.ndarray:
    return finish_positions.map(to_relevance).to_numpy(dtype=np.int64)


def build_group_sizes(df: pd.DataFrame) -> list[int]:
    return df.groupby("race_id", sort=False).size().tolist()


def write_model_artifacts(
    output_dir: Path,
    booster: lgb.Booster,
    model_version: str,
    feature_columns: list[str],
    categorical_features: list[str],
    train_rows: int,
    train_start: str,
    train_end: str,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    booster.save_model(str(output_dir / "model.txt"))
    metadata = {
        "model_version": model_version,
        "feature_columns": feature_columns,
        "categorical_features": categorical_features,
        "objective": "lambdarank",
        "train_rows": train_rows,
        "train_start_date": train_start,
        "train_end_date": train_end,
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    started = perf_counter()
    df = pd.read_parquet(args.parquet)
    train_df = filter_train_range(df, args.train_start_date, args.train_end_date)
    train_df = train_df.sort_values(["race_id", "umaban"]).reset_index(drop=True)
    feature_columns = select_feature_columns()
    train_df = encode_categoricals(train_df)
    feature_frame = train_df.loc[:, feature_columns]
    labels = build_label_array(train_df["finish_position"])
    group_sizes = build_group_sizes(train_df)
    dataset = lgb.Dataset(
        feature_frame,
        label=labels,
        group=group_sizes,
        categorical_feature=list(LITE_CATEGORICAL_FEATURES),
        free_raw_data=False,
    )
    params = {
        "objective": "lambdarank",
        "metric": "ndcg",
        "ndcg_eval_at": [3],
        "num_leaves": DEFAULT_NUM_LEAVES,
        "learning_rate": DEFAULT_LEARNING_RATE,
        "min_child_samples": DEFAULT_MIN_CHILD_SAMPLES,
        "verbose": -1,
    }
    booster = lgb.train(params, dataset, num_boost_round=DEFAULT_NUM_ITERATIONS)
    write_model_artifacts(
        args.output_model_dir,
        booster,
        args.model_version,
        feature_columns,
        list(LITE_CATEGORICAL_FEATURES),
        int(len(train_df)),
        args.train_start_date,
        args.train_end_date,
    )
    elapsed = perf_counter() - started
    print(json.dumps({
        "elapsed_seconds": elapsed,
        "rows": int(len(train_df)),
        "model_version": args.model_version,
        "output_dir": str(args.output_model_dir),
    }))


if __name__ == "__main__":
    main()
