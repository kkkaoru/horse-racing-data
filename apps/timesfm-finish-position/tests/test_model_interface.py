from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

from timesfm_finish_position.model_interface import (
    ForecastDistribution,
    ModelDataset,
    ModelPrediction,
    PortableArtifactManifest,
    read_manifest,
    read_prediction,
    write_manifest,
    write_prediction,
)


def test_model_dataset_validates_supervised_alignment() -> None:
    dataset = ModelDataset(
        race_ids=np.asarray(["r1", "r1"], dtype=np.str_),
        horse_ids=np.asarray(["h1", "h2"], dtype=np.str_),
        race_dates=np.asarray(["20240101", "20240101"], dtype=np.str_),
        numeric=np.asarray([[1.0], [2.0]], dtype=np.float64),
        numeric_names=("speed",),
        categorical={"jockey": np.asarray(["j1", "j2"], dtype=np.str_)},
        decimal_odds=np.asarray([2.0, 3.0], dtype=np.float64),
        finish_positions=np.asarray([1, 2], dtype=np.int64),
    )
    dataset.validate(require_labels=True)
    with pytest.raises(ValueError, match="requires finish positions"):
        ModelDataset(
            dataset.race_ids,
            dataset.horse_ids,
            dataset.race_dates,
            dataset.numeric,
            dataset.numeric_names,
            dataset.categorical,
            dataset.decimal_odds,
        ).validate(require_labels=True)


def test_model_dataset_rejects_misaligned_inputs() -> None:
    with pytest.raises(ValueError, match="numeric model inputs"):
        ModelDataset(
            race_ids=np.asarray(["r1"], dtype=np.str_),
            horse_ids=np.asarray(["h1"], dtype=np.str_),
            race_dates=np.asarray(["20240101"], dtype=np.str_),
            numeric=np.asarray([[1.0, 2.0]], dtype=np.float64),
            numeric_names=("speed",),
            categorical={},
            decimal_odds=np.asarray([2.0], dtype=np.float64),
        ).validate(require_labels=False)


def test_prediction_writer_preserves_common_and_forecast_columns(tmp_path: Path) -> None:
    prediction = ModelPrediction(
        race_ids=np.asarray(["r1", "r1"], dtype=np.str_),
        horse_ids=np.asarray(["h1", "h2"], dtype=np.str_),
        prediction=np.asarray([0.8, 0.2], dtype=np.float64),
        probability=np.asarray([0.8, 0.2], dtype=np.float64),
        model_name="chronos2",
        model_version="v1",
        forecast=ForecastDistribution(
            forecast=np.asarray([0.7, 0.3], dtype=np.float64),
            p10=np.asarray([0.5, 0.1], dtype=np.float64),
            p50=np.asarray([0.7, 0.3], dtype=np.float64),
            p90=np.asarray([0.9, 0.5], dtype=np.float64),
            uncertainty=np.asarray([0.4, 0.4], dtype=np.float64),
        ),
    )
    path = tmp_path / "prediction.parquet"
    write_prediction(path, prediction)
    table = pq.read_table(path)
    assert table.column_names == [
        "race_id",
        "horse_id",
        "prediction",
        "probability",
        "model_name",
        "model_version",
        "forecast",
        "p10",
        "p50",
        "p90",
        "uncertainty",
    ]
    assert table.column("model_name").to_pylist() == ["chronos2", "chronos2"]
    restored = read_prediction(path)
    assert restored.model_name == "chronos2"
    assert restored.forecast is not None
    assert restored.forecast.uncertainty.tolist() == [0.4, 0.4]


def test_prediction_validation_rejects_bad_probability() -> None:
    prediction = ModelPrediction(
        race_ids=np.asarray(["r1"], dtype=np.str_),
        horse_ids=np.asarray(["h1"], dtype=np.str_),
        prediction=np.asarray([1.2], dtype=np.float64),
        probability=np.asarray([1.2], dtype=np.float64),
        model_name="mlp",
        model_version="v1",
    )
    with pytest.raises(ValueError, match="between zero and one"):
        prediction.validate()


def test_portable_manifest_round_trip_and_schema_guard(tmp_path: Path) -> None:
    manifest = PortableArtifactManifest(
        model_name="lometab",
        model_version="v1",
        feature_version="current-state-v1",
        random_seed=7,
        model_config={"rank": 4},
        feature_config={"windows": [30, 90]},
        training_metadata={"year": 2024},
    )
    write_manifest(tmp_path, manifest)
    assert read_manifest(tmp_path) == manifest
    payload = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    payload["schema"] = "unknown"
    (tmp_path / "manifest.json").write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="schema is unsupported"):
        read_manifest(tmp_path)
