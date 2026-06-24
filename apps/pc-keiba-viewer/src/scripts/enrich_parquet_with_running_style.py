#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Add running-style soft probabilities (p_nige/p_senkou/p_sashi/p_oikomi) to
every row of a v15 feature parquet so the finish-position trainers can read
脚質 signals just like any other numeric feature. Reads the saved production
LightGBM running-style booster (model.txt + metadata.json) from
`--model-dir`, scores the parquet locally (no Cloudflare dependency), and
writes a Hive-partitioned parquet under `--output-dir`.

Run with:
  cd apps/pc-keiba-viewer && .venv/bin/python src/scripts/enrich_parquet_with_running_style.py \\
    --parquet ../../tmp/feat-v15/jra \\
    --model-dir ../../tmp/models/jra-running-style-lgbm-prod-v1.5 \\
    --output-dir ../../tmp/feat-v15-rs/jra
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter
from typing import cast

import lightgbm as lgb
import numpy as np
import polars as pl

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
PROBABILITY_COLUMNS: tuple[str, str, str, str] = ("p_nige", "p_senkou", "p_sashi", "p_oikomi")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="enrich_parquet_with_running_style")
    parser.add_argument("--parquet", type=Path, required=True, help="source parquet directory")
    parser.add_argument(
        "--model-dir",
        type=Path,
        required=True,
        help="dir containing model.txt + metadata.json",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--column-prefix",
        type=str,
        default="rs",
        help="prefix appended to the 4 probability columns to avoid collision",
    )
    return parser.parse_args(argv)


def load_model_artifacts(model_dir: Path) -> tuple[lgb.Booster, dict[str, object]]:
    metadata_path = model_dir / "metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"metadata.json missing in {model_dir}")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    booster = lgb.Booster(model_file=str(model_dir / "model.txt"))
    return booster, metadata


def encode_categoricals(frame: pl.DataFrame, categorical_features: list[str]) -> pl.DataFrame:
    present = [column for column in categorical_features if column in frame.columns]
    return frame.with_columns(pl.col(column).cast(pl.Categorical) for column in present)


def align_feature_frame(df: pl.DataFrame, feature_columns: list[str]) -> pl.DataFrame:
    missing = [column for column in feature_columns if column not in df.columns]
    if missing:
        raise ValueError(f"parquet missing feature columns: {missing[:5]}{'...' if len(missing) > 5 else ''}")
    return df.select(feature_columns)


def predict_probabilities(
    booster: lgb.Booster,
    frame: pl.DataFrame,
    feature_columns: list[str],
    categorical_features: list[str],
) -> np.ndarray:
    encoded = encode_categoricals(frame.select(feature_columns), categorical_features)
    return cast(np.ndarray, booster.predict(encoded, num_iteration=booster.best_iteration))


def build_probability_columns(probabilities: np.ndarray, prefix: str) -> dict[str, np.ndarray]:
    return {f"{prefix}_{name}": probabilities[:, idx] for idx, name in enumerate(PROBABILITY_COLUMNS)}


def write_partitioned_parquet(df: pl.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_dir, partition_by=["race_year"], mkdir=True)


def load_partitioned_parquet(parquet_dir: Path) -> pl.DataFrame:
    return pl.read_parquet(parquet_dir)


def run_enrichment(args: argparse.Namespace) -> dict[str, object]:
    started = perf_counter()
    booster, metadata = load_model_artifacts(args.model_dir)
    df = load_partitioned_parquet(args.parquet)
    feature_columns = [str(value) for value in cast(list[object], metadata.get("feature_columns", []))]
    categorical_features = [
        str(value) for value in cast(list[object], metadata.get("categorical_features", []))
    ]
    aligned = align_feature_frame(df, feature_columns)
    probabilities = predict_probabilities(booster, aligned, feature_columns, categorical_features)
    enriched = df.with_columns(
        pl.Series(column_name, column_values)
        for column_name, column_values in build_probability_columns(
            probabilities, args.column_prefix
        ).items()
    )
    write_partitioned_parquet(enriched, args.output_dir)
    elapsed = perf_counter() - started
    return {
        "elapsed_seconds": elapsed,
        "model_version": str(metadata.get("model_version", "unknown")),
        "output_dir": str(args.output_dir),
        "rows": int(enriched.height),
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    summary = run_enrichment(args)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
