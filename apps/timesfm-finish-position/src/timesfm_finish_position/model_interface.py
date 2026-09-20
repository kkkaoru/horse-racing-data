"""Portable data, prediction, and artifact contracts shared by model experiments."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol, Self, TypeVar

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .domain import FloatArray
from .lab_domain import LabIntArray, LabStringArray

PREDICTION_SCHEMA_VERSION = "finish-position-model-prediction-v1"
ARTIFACT_SCHEMA_VERSION = "finish-position-portable-model-v1"
ModelInputT = TypeVar("ModelInputT", contravariant=True)


@dataclass(frozen=True)
class ModelDataset:
    """One aligned model input partition with optional supervised labels."""

    race_ids: LabStringArray
    horse_ids: LabStringArray
    race_dates: LabStringArray
    numeric: FloatArray
    numeric_names: tuple[str, ...]
    categorical: Mapping[str, LabStringArray]
    decimal_odds: FloatArray
    finish_positions: LabIntArray | None = None

    @property
    def rows(self) -> int:
        """Return runner count."""
        return len(self.race_ids)

    def validate(self, *, require_labels: bool) -> None:
        """Validate alignment before model fitting or inference."""
        rows = self.rows
        if self.numeric.ndim != 2 or self.numeric.shape != (rows, len(self.numeric_names)):
            raise ValueError("numeric model inputs must align with names and runner rows")
        if (
            self.horse_ids.shape != (rows,)
            or self.race_dates.shape != (rows,)
            or self.decimal_odds.shape != (rows,)
        ):
            raise ValueError("model identity and odds columns must align")
        if any(values.shape != (rows,) for values in self.categorical.values()):
            raise ValueError("categorical model inputs must align")
        if require_labels and self.finish_positions is None:
            raise ValueError("supervised model fitting requires finish positions")
        if self.finish_positions is not None and self.finish_positions.shape != (rows,):
            raise ValueError("finish positions must align")


@dataclass(frozen=True)
class ForecastDistribution:
    """Optional portable TSFM forecast columns."""

    forecast: FloatArray
    p10: FloatArray
    p50: FloatArray
    p90: FloatArray
    uncertainty: FloatArray


@dataclass(frozen=True)
class ModelPrediction:
    """Unified output emitted by every independent experiment."""

    race_ids: LabStringArray
    horse_ids: LabStringArray
    prediction: FloatArray
    probability: FloatArray
    model_name: str
    model_version: str
    forecast: ForecastDistribution | None = None

    def validate(self) -> None:
        """Reject malformed or non-finite prediction artifacts."""
        rows = len(self.race_ids)
        arrays = (self.horse_ids, self.prediction, self.probability)
        if any(values.shape != (rows,) for values in arrays):
            raise ValueError("prediction columns must align")
        if not self.model_name or not self.model_version:
            raise ValueError("prediction model identity must not be empty")
        if np.any(~np.isfinite(self.prediction)) or np.any(~np.isfinite(self.probability)):
            raise ValueError("predictions and probabilities must be finite")
        if np.any((self.probability < 0.0) | (self.probability > 1.0)):
            raise ValueError("probabilities must be between zero and one")
        if self.forecast is not None:
            forecast_arrays = (
                self.forecast.forecast,
                self.forecast.p10,
                self.forecast.p50,
                self.forecast.p90,
                self.forecast.uncertainty,
            )
            if any(values.shape != (rows,) for values in forecast_arrays):
                raise ValueError("forecast columns must align")


@dataclass(frozen=True)
class PortableArtifactManifest:
    """Backend-neutral metadata stored beside NumPy weights."""

    model_name: str
    model_version: str
    feature_version: str
    random_seed: int
    model_config: Mapping[str, object]
    feature_config: Mapping[str, object]
    training_metadata: Mapping[str, object]
    schema: str = ARTIFACT_SCHEMA_VERSION


class ExperimentModel(Protocol[ModelInputT]):
    """Common fit/predict/save/load contract for experiment implementations."""

    def fit(self, train_data: ModelInputT) -> None:
        """Fit on one chronological training partition."""
        ...

    def predict(self, validation_data: ModelInputT) -> ModelPrediction:
        """Predict one later chronological partition."""
        ...

    def save(self, path: Path) -> None:
        """Save a portable artifact directory."""
        ...

    @classmethod
    def load(cls, path: Path) -> Self:
        """Load a portable artifact directory."""
        ...


def read_prediction(path: Path) -> ModelPrediction:
    """Read one common prediction Parquet artifact."""
    table = pq.read_table(path)
    required = {
        "race_id",
        "horse_id",
        "prediction",
        "probability",
        "model_name",
        "model_version",
    }
    if not required.issubset(table.column_names):
        raise ValueError("common prediction schema is incomplete")
    rows = table.num_rows
    model_names = set(table.column("model_name").to_pylist())
    model_versions = set(table.column("model_version").to_pylist())
    if len(model_names) != 1 or len(model_versions) != 1:
        raise ValueError("common prediction model identity is not uniform")
    forecast = None
    forecast_columns = ("forecast", "p10", "p50", "p90", "uncertainty")
    if any(name in table.column_names for name in forecast_columns):
        if not all(name in table.column_names for name in forecast_columns):
            raise ValueError("common forecast distribution schema is incomplete")
        forecast = ForecastDistribution(
            forecast=np.asarray(table.column("forecast").to_pylist(), dtype=np.float64),
            p10=np.asarray(table.column("p10").to_pylist(), dtype=np.float64),
            p50=np.asarray(table.column("p50").to_pylist(), dtype=np.float64),
            p90=np.asarray(table.column("p90").to_pylist(), dtype=np.float64),
            uncertainty=np.asarray(table.column("uncertainty").to_pylist(), dtype=np.float64),
        )
    prediction = ModelPrediction(
        race_ids=np.asarray(table.column("race_id").to_pylist(), dtype=np.str_),
        horse_ids=np.asarray(table.column("horse_id").to_pylist(), dtype=np.str_),
        prediction=np.asarray(table.column("prediction").to_pylist(), dtype=np.float64),
        probability=np.asarray(table.column("probability").to_pylist(), dtype=np.float64),
        model_name=str(next(iter(model_names))),
        model_version=str(next(iter(model_versions))),
        forecast=forecast,
    )
    if len(prediction.race_ids) != rows:
        raise RuntimeError("common prediction row count changed while reading")
    prediction.validate()
    return prediction


def write_prediction(path: Path, prediction: ModelPrediction) -> None:
    """Persist unified OOF output as portable Parquet."""
    prediction.validate()
    rows = len(prediction.race_ids)
    columns: dict[str, object] = {
        "race_id": prediction.race_ids,
        "horse_id": prediction.horse_ids,
        "prediction": prediction.prediction,
        "probability": prediction.probability,
        "model_name": np.repeat(prediction.model_name, rows),
        "model_version": np.repeat(prediction.model_version, rows),
    }
    if prediction.forecast is not None:
        columns.update(
            {
                "forecast": prediction.forecast.forecast,
                "p10": prediction.forecast.p10,
                "p50": prediction.forecast.p50,
                "p90": prediction.forecast.p90,
                "uncertainty": prediction.forecast.uncertainty,
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), path, compression="zstd")


def write_manifest(path: Path, manifest: PortableArtifactManifest) -> None:
    """Write deterministic portable artifact metadata."""
    if manifest.schema != ARTIFACT_SCHEMA_VERSION:
        raise ValueError("portable artifact schema is unsupported")
    path.mkdir(parents=True, exist_ok=True)
    (path / "manifest.json").write_text(
        json.dumps(asdict(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def read_manifest(path: Path) -> PortableArtifactManifest:
    """Load and validate portable artifact metadata."""
    payload = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema") != ARTIFACT_SCHEMA_VERSION:
        raise ValueError("portable artifact schema is unsupported")
    return PortableArtifactManifest(
        model_name=str(payload["model_name"]),
        model_version=str(payload["model_version"]),
        feature_version=str(payload["feature_version"]),
        random_seed=int(payload["random_seed"]),
        model_config=dict(payload["model_config"]),
        feature_config=dict(payload["feature_config"]),
        training_metadata=dict(payload["training_metadata"]),
        schema=str(payload["schema"]),
    )
