"""Phase B: score running-style local parquet via existing LightGBM booster (no PG writeback).

Run with:
    uv run python src/scripts/score_running_style_local.py \
        --features-parquet apps/pc-keiba-viewer/tmp/bucket-eval/running-style/v1/features \
        --model-version running-style-lightgbm-jra-v7 \
        --output-parquet apps/pc-keiba-viewer/tmp/bucket-eval/running-style/v1/predictions \
        --running-style-feature-version v1 \
        --pg-url $DATABASE_URL_LOCAL \
        --category jra
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Protocol

import lightgbm as lgb
import numpy as np
import pandas as pd

from running_style_lightgbm import (
    CLASS_LABELS,
    PROBABILITY_COLUMNS,
    compute_predicted_labels,
    predict_softmax,
    resolve_feature_columns,
)

ACTIVE_MODELS_TABLE: str = "running_style_active_models"
LABEL_COLUMN: str = "predicted_label"
SUPPORTED_CATEGORIES: tuple[str, str, str] = ("jra", "nar", "ban-ei")


class BoosterLoaderLike(Protocol):
    def __call__(self, *, model_file: str) -> lgb.Booster: ...


class PsqlRunnerLike(Protocol):
    def __call__(self, pg_url: str, sql: str) -> str: ...


class PandasReaderLike(Protocol):
    def __call__(self, path: str) -> pd.DataFrame: ...


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Score running-style local features parquet using active LightGBM booster.",
    )
    parser.add_argument("--features-parquet", required=True)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--output-parquet", required=True)
    parser.add_argument("--running-style-feature-version", required=True)
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--category", required=True, choices=list(SUPPORTED_CATEGORIES))
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def build_active_model_query(category: str) -> str:
    return (
        "select json_build_object('model_version', model_version, 'artifact_path', artifact_path) "
        f"from {ACTIVE_MODELS_TABLE} where category = '{category}' limit 1"
    )


def run_psql(pg_url: str, sql: str) -> str:
    result = subprocess.run(
        ["psql", pg_url, "-v", "ON_ERROR_STOP=1", "-At", "-c", sql],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"psql failed: {result.stderr.strip()}")
    return result.stdout.strip()


def resolve_active_model(
    *, psql_runner: PsqlRunnerLike, pg_url: str, category: str,
) -> tuple[str, str]:
    raw = psql_runner(pg_url, build_active_model_query(category))
    if raw == "":
        raise RuntimeError(f"No active running-style model for category={category}.")
    parsed: dict[str, str] = json.loads(raw)
    model_version = parsed.get("model_version")
    artifact_path = parsed.get("artifact_path")
    if not isinstance(model_version, str) or not isinstance(artifact_path, str):
        raise RuntimeError("Active model row must contain string model_version and artifact_path.")
    return model_version, artifact_path


def build_label_series(probabilities: np.ndarray) -> list[str]:
    predicted = compute_predicted_labels(probabilities)
    return [CLASS_LABELS[int(idx)] for idx in predicted]


def attach_probability_columns(frame: pd.DataFrame, probabilities: np.ndarray) -> pd.DataFrame:
    for column_index, column_name in enumerate(PROBABILITY_COLUMNS):
        frame[column_name] = probabilities[:, column_index]
    return frame


def attach_label_and_versions(
    frame: pd.DataFrame,
    probabilities: np.ndarray,
    *,
    feature_version: str,
    model_version: str,
) -> pd.DataFrame:
    frame[LABEL_COLUMN] = build_label_series(probabilities)
    frame["running_style_feature_version"] = feature_version
    frame["model_version"] = model_version
    return frame


def score_frame(
    *,
    booster: lgb.Booster,
    frame: pd.DataFrame,
    feature_version: str,
    model_version: str,
) -> pd.DataFrame:
    feature_columns = resolve_feature_columns(list(frame.columns))
    probabilities = predict_softmax(booster, frame, feature_columns, [])
    frame = attach_probability_columns(frame, probabilities)
    frame = attach_label_and_versions(
        frame,
        probabilities,
        feature_version=feature_version,
        model_version=model_version,
    )
    return frame


def write_predictions_parquet(frame: pd.DataFrame, output_dir: str) -> None:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    frame.to_parquet(
        output_dir,
        partition_cols=["category", "race_year"],
        index=False,
    )


def run(
    args: argparse.Namespace,
    *,
    psql_runner: PsqlRunnerLike,
    booster_loader: BoosterLoaderLike,
    pandas_reader: PandasReaderLike,
) -> None:
    active_model_version, artifact_path = resolve_active_model(
        psql_runner=psql_runner, pg_url=args.pg_url, category=args.category,
    )
    if active_model_version != args.model_version:
        raise RuntimeError(
            "Requested --model-version does not match active model in PG; refusing to score.",
        )
    booster = booster_loader(model_file=artifact_path)
    frame = pandas_reader(args.features_parquet)
    scored = score_frame(
        booster=booster,
        frame=frame,
        feature_version=args.running_style_feature_version,
        model_version=active_model_version,
    )
    write_predictions_parquet(scored, args.output_parquet)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run(
        args,
        psql_runner=run_psql,
        booster_loader=lgb.Booster,
        pandas_reader=pd.read_parquet,
    )


if __name__ == "__main__":
    main()
