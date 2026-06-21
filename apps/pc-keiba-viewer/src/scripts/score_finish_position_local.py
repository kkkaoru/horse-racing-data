"""Phase B' of Agent G: score local finish-position features parquet via the
production v7-lineage LightGBM booster.

Reuses ``finish_position_lightgbm.load_booster`` + ``finish_position_lightgbm.score_dataset``
read-only, resolves the active model artifact path from PG
``finish_position_active_models`` via a local ``psql`` subprocess (so this
script does not require a Python psycopg driver), and writes a Hive-partitioned
parquet of predictions stamped with ``finish_position_version`` /
``running_style_feature_version`` / ``model_version``.

We never write back into PG ``race_finish_position_model_predictions`` and we
never touch the production scoring path. The output lives under
``apps/pc-keiba-viewer/tmp/bucket-eval/finish-position/<v>/predictions/``.

Run with: ``uv run python src/scripts/score_finish_position_local.py ...``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Protocol, TypedDict

import lightgbm as lgb
import pandas as pd

import finish_position_lightgbm as finish_position

ACTIVE_MODELS_TABLE: str = "finish_position_active_models"
SUPPORTED_CATEGORIES: tuple[str, str, str] = ("jra", "nar", "ban-ei")


class BoosterLoaderLike(Protocol):
    def __call__(self, path: Path) -> lgb.Booster: ...


class PsqlRunnerLike(Protocol):
    def __call__(self, pg_url: str, sql: str) -> str: ...


class PandasReaderLike(Protocol):
    def __call__(self, path: str) -> pd.DataFrame: ...


class ScoreDatasetLike(Protocol):
    def __call__(self, booster: lgb.Booster, df: pd.DataFrame) -> pd.DataFrame: ...


class PhaseBArguments(TypedDict):
    features_parquet: Path
    model_version: str
    output_parquet: Path
    finish_position_version: str
    running_style_feature_version: str
    pg_url: str
    category: str


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Score local finish-position features via active LightGBM booster.",
    )
    parser.add_argument("--features-parquet", type=Path, required=True)
    parser.add_argument("--model-version", type=str, required=True)
    parser.add_argument("--output-parquet", type=Path, required=True)
    parser.add_argument("--finish-position-version", type=str, required=True)
    parser.add_argument("--running-style-feature-version", type=str, required=True)
    parser.add_argument("--pg-url", type=str, required=True)
    parser.add_argument("--category", type=str, choices=list(SUPPORTED_CATEGORIES), required=True)
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def normalize_arguments(args: argparse.Namespace) -> PhaseBArguments:
    return {
        "features_parquet": Path(args.features_parquet),
        "model_version": args.model_version,
        "output_parquet": Path(args.output_parquet),
        "finish_position_version": args.finish_position_version,
        "running_style_feature_version": args.running_style_feature_version,
        "pg_url": args.pg_url,
        "category": args.category,
    }


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
        raise RuntimeError(f"No active finish-position model for category={category}.")
    parsed: dict[str, str] = json.loads(raw)
    model_version = parsed.get("model_version")
    artifact_path = parsed.get("artifact_path")
    if not isinstance(model_version, str) or not isinstance(artifact_path, str):
        raise RuntimeError(
            "Active finish-position model row must contain string model_version and artifact_path.",
        )
    return model_version, artifact_path


def resolve_model_version(active_model_version: str, requested_version: str) -> str:
    if active_model_version != requested_version:
        raise RuntimeError(
            "Requested --model-version does not match active finish-position model in PG; refusing to score.",
        )
    return active_model_version


def attach_versions(
    frame: pd.DataFrame,
    *,
    finish_position_version: str,
    running_style_feature_version: str,
    model_version: str,
) -> pd.DataFrame:
    frame["finish_position_version"] = finish_position_version
    frame["running_style_feature_version"] = running_style_feature_version
    frame["model_version"] = model_version
    return frame


def score_features_frame(
    *,
    booster: lgb.Booster,
    features: pd.DataFrame,
    finish_position_version: str,
    running_style_feature_version: str,
    model_version: str,
    score_dataset: ScoreDatasetLike,
) -> pd.DataFrame:
    scored = score_dataset(booster, features)
    return attach_versions(
        scored,
        finish_position_version=finish_position_version,
        running_style_feature_version=running_style_feature_version,
        model_version=model_version,
    )


def write_predictions_parquet(frame: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(
        output_dir.as_posix(),
        partition_cols=["category", "race_year"],
        index=False,
        existing_data_behavior="delete_matching",
    )


def run(
    args: PhaseBArguments,
    *,
    psql_runner: PsqlRunnerLike,
    booster_loader: BoosterLoaderLike,
    pandas_reader: PandasReaderLike,
    score_dataset: ScoreDatasetLike,
) -> dict[str, object]:
    active_model_version, artifact_path = resolve_active_model(
        psql_runner=psql_runner, pg_url=args["pg_url"], category=args["category"],
    )
    model_version = resolve_model_version(active_model_version, args["model_version"])
    booster = booster_loader(Path(artifact_path))
    features = pandas_reader(args["features_parquet"].as_posix())
    scored = score_features_frame(
        booster=booster,
        features=features,
        finish_position_version=args["finish_position_version"],
        running_style_feature_version=args["running_style_feature_version"],
        model_version=model_version,
        score_dataset=score_dataset,
    )
    write_predictions_parquet(scored, args["output_parquet"])
    return {
        "output_parquet": args["output_parquet"].as_posix(),
        "rows_written": int(len(scored)),
        "model_version": model_version,
        "finish_position_version": args["finish_position_version"],
        "running_style_feature_version": args["running_style_feature_version"],
        "category": args["category"],
    }


def main(argv: list[str] | None = None) -> None:
    args = normalize_arguments(parse_args(argv))
    result = run(
        args,
        psql_runner=run_psql,
        booster_loader=finish_position.load_booster,
        pandas_reader=pd.read_parquet,
        score_dataset=finish_position.score_dataset,
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
