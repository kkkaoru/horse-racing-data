"""Phase B (X5): score running-style local parquet via existing LightGBM booster and
write raw probabilities only. All post-processing (argmax, second_predicted_class,
predicted_label, race-level constraints) is intentionally NOT performed here; a
downstream TS script consumes the emitted probabilities and decides the label
schema. memory rule ``feedback_no_race_level_nige_constraint.md`` forbids any
nige cap in this pipeline.

The model artifact is resolved by convention from ``--model-version`` (each
artifact lives at ``tmp/models/<model_version>/model.txt`` relative to the
``pc-keiba-viewer`` package directory). The output parquet contains the
race-key 6-tuple, four softmax probabilities, and metadata columns only.

Run with:
    uv run python src/scripts/score_running_style_local.py \\
        --features-parquet tmp/bucket-eval/running-style/v1/features/category=jra/race_year=2006/data_0.parquet \\
        --output-parquet tmp/bucket-eval/running-style/v1/logits/category=jra/race_year=2006/data_0.parquet \\
        --running-style-feature-version v1 \\
        --model-version jra-running-style-lgbm-prod-v1.5 \\
        --category jra
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Protocol

import lightgbm as lgb
import numpy as np
import pandas as pd

from running_style_calibration import (
    RunningStyleCalibrators,
    apply_calibration,
    calibrators_path_for_model_version,
    load_calibrators,
)
from running_style_lightgbm import (
    PROBABILITY_COLUMNS,
    detect_categorical_features,
    predict_softmax,
    resolve_feature_columns,
)

SUPPORTED_CATEGORIES: tuple[str, str] = ("jra", "nar")
MODELS_DIR_NAME: str = "tmp/models"
MODEL_FILENAME: str = "model.txt"
RACE_KEY_COLUMNS: tuple[str, str, str, str, str, str] = (
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
)
FEATURE_VERSION_COLUMN: str = "running_style_feature_version"
MODEL_VERSION_COLUMN: str = "model_version"


class BoosterLoaderLike(Protocol):
    def __call__(self, *, model_file: str) -> lgb.Booster: ...


class PandasReaderLike(Protocol):
    def __call__(self, path: str) -> pd.DataFrame: ...


class PathExistsLike(Protocol):
    def __call__(self, path: str) -> bool: ...


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Score running-style local features parquet using a versioned LightGBM "
            "booster and write raw probabilities only."
        ),
    )
    parser.add_argument("--features-parquet", required=True)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--output-parquet", required=True)
    parser.add_argument("--running-style-feature-version", required=True)
    parser.add_argument("--category", required=True, choices=list(SUPPORTED_CATEGORIES))
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def resolve_artifact_path(model_version: str) -> str:
    return (repo_root() / MODELS_DIR_NAME / model_version / MODEL_FILENAME).as_posix()


def assert_artifact_exists(artifact_path: str, *, path_exists: PathExistsLike) -> str:
    if not path_exists(artifact_path):
        raise FileNotFoundError(
            f"Running-style model artifact not found at {artifact_path}; "
            "place model.txt under tmp/models/<model_version>/.",
        )
    return artifact_path


def select_race_key_frame(frame: pd.DataFrame) -> pd.DataFrame:
    present_columns = [column for column in RACE_KEY_COLUMNS if column in frame.columns]
    return frame[present_columns].reset_index(drop=True)


def build_probability_frame(probabilities: np.ndarray) -> pd.DataFrame:
    return pd.DataFrame(
        {column: probabilities[:, index] for index, column in enumerate(PROBABILITY_COLUMNS)},
    )


def attach_version_columns(
    frame: pd.DataFrame, *, feature_version: str, model_version: str,
) -> pd.DataFrame:
    frame[FEATURE_VERSION_COLUMN] = feature_version
    frame[MODEL_VERSION_COLUMN] = model_version
    return frame


def score_frame(
    *,
    booster: lgb.Booster,
    frame: pd.DataFrame,
    feature_version: str,
    model_version: str,
    calibrators: RunningStyleCalibrators | None = None,
) -> pd.DataFrame:
    feature_columns = resolve_feature_columns(list(frame.columns))
    probabilities = predict_softmax(booster, frame, feature_columns, detect_categorical_features(feature_columns))
    if calibrators is not None:
        probabilities = apply_calibration(probabilities, calibrators)
    race_keys = select_race_key_frame(frame)
    probability_frame = build_probability_frame(probabilities)
    combined = pd.concat([race_keys, probability_frame], axis=1)
    return attach_version_columns(
        combined, feature_version=feature_version, model_version=model_version,
    )


def write_logits_parquet(frame: pd.DataFrame, output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(output_path, index=False)


def default_path_exists(path: str) -> bool:
    return Path(path).exists()


def try_load_calibrators(
    model_version: str,
    path_exists: PathExistsLike,
) -> RunningStyleCalibrators | None:
    """Attempt to load calibrators for model_version; return None when absent."""
    calibrators_path = calibrators_path_for_model_version(model_version)
    if not path_exists(calibrators_path):
        return None
    return load_calibrators(calibrators_path)


def run(
    args: argparse.Namespace,
    *,
    booster_loader: BoosterLoaderLike,
    pandas_reader: PandasReaderLike,
    path_exists: PathExistsLike,
) -> None:
    artifact_path = assert_artifact_exists(
        resolve_artifact_path(args.model_version), path_exists=path_exists,
    )
    booster = booster_loader(model_file=artifact_path)
    frame = pandas_reader(args.features_parquet)
    calibrators = try_load_calibrators(args.model_version, path_exists=path_exists)
    scored = score_frame(
        booster=booster,
        frame=frame,
        feature_version=args.running_style_feature_version,
        model_version=args.model_version,
        calibrators=calibrators,
    )
    write_logits_parquet(scored, args.output_parquet)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run(
        args,
        booster_loader=lgb.Booster,
        pandas_reader=pd.read_parquet,
        path_exists=default_path_exists,
    )


if __name__ == "__main__":
    main()
