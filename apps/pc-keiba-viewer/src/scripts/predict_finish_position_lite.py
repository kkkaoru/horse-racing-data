#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Score a lite finish-position LGBM model on a date window of v20-rs parquet
and emit JSONL + actuals CSV ready for compare-model-metrics.py.

Run with:
  apps/pc-keiba-viewer/.venv/bin/python apps/pc-keiba-viewer/src/scripts/predict_finish_position_lite.py \\
    --parquet tmp/feat-v20-rs-final/jra \\
    --model-dir tmp/models/finish-position-lite/jra-style20-eval \\
    --start-date 20240101 --end-date 20251231 \\
    --output-jsonl tmp/finish-position-eval/style20/jra-2024-2025.jsonl \\
    --output-actuals tmp/finish-position-eval/style20/jra-actuals-2024-2025.csv
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast

import lightgbm as lgb
import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="predict_finish_position_lite")
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--start-date", type=str, required=True)
    parser.add_argument("--end-date", type=str, required=True)
    parser.add_argument("--output-jsonl", type=Path, required=True)
    parser.add_argument("--output-actuals", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metadata = json.loads((args.model_dir / "metadata.json").read_text(encoding="utf-8"))
    feature_columns = metadata["feature_columns"]
    categorical_features = metadata.get("categorical_features", [])
    booster = lgb.Booster(model_file=str(args.model_dir / "model.txt"))
    df = pd.read_parquet(args.parquet)
    mask = (df["race_date"] >= args.start_date) & (df["race_date"] <= args.end_date)
    df = df[mask].copy()
    for col in categorical_features:
        if col in df.columns:
            df[col] = df[col].astype("category")
    feature_frame = df.loc[:, feature_columns]
    scores = cast(np.ndarray, booster.predict(feature_frame))
    df["lgbm_score"] = scores
    df["predicted_rank"] = (
        df.groupby("race_id")["lgbm_score"].rank(method="first", ascending=False).astype(int)
    )
    args.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    pred_records = df.loc[:, ["race_id", "ketto_toroku_bango", "predicted_rank"]]
    with args.output_jsonl.open("w", encoding="utf-8") as fp:
        for row in pred_records.itertuples(index=False):
            fp.write(
                json.dumps({
                    "race_id": row.race_id,
                    "ketto_toroku_bango": row.ketto_toroku_bango,
                    "predicted_rank": int(cast(float, row.predicted_rank)),
                }) + "\n"
            )
    actuals = df.loc[df["finish_position"].notna(), ["race_id", "ketto_toroku_bango", "finish_position"]].copy()
    actuals["finish_position"] = actuals["finish_position"].astype(int)
    actuals.to_csv(args.output_actuals, index=False)
    print(json.dumps({
        "rows_predicted": int(len(df)),
        "rows_with_actual_finish": int(len(actuals)),
        "model_version": metadata.get("model_version"),
        "output_jsonl": str(args.output_jsonl),
        "output_actuals": str(args.output_actuals),
    }))


if __name__ == "__main__":
    main()
