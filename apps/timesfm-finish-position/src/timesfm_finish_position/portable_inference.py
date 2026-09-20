"""NumPy/MLX inference for portable MLP artifacts and backend parity gates."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

import numpy as np
import numpy.typing as npt

from .domain import FloatArray
from .lab_domain import LabStringArray
from .model_interface import ModelDataset
from .neural_models import NeuralModelKind, PortableNeuralModel

Float32Array = npt.NDArray[np.float32]


class WeightsLike(Protocol):
    """String-keyed array archive boundary."""

    def __getitem__(self, key: str) -> object: ...


class MlxArray(Protocol):
    """Minimal MLX array operation boundary."""

    def __matmul__(self, other: MlxArray) -> MlxArray: ...

    def __add__(self, other: MlxArray) -> MlxArray: ...


class MlxModule(Protocol):
    """Minimal MLX operations used by feed-forward inference."""

    float32: object

    def array(self, values: object, *, dtype: object) -> MlxArray: ...

    def maximum(self, left: MlxArray, right: float) -> MlxArray: ...

    def transpose(self, values: MlxArray) -> MlxArray: ...

    def eval(self, values: MlxArray) -> None: ...


@dataclass(frozen=True)
class BackendParity:
    """Numerical and full-ranking agreement across inference backends."""

    max_abs_error: float
    ranking_equal: bool
    tolerance: float
    passed: bool


def _encoded_numpy(
    model: PortableNeuralModel, data: ModelDataset, weights: WeightsLike
) -> Float32Array:
    if model.preprocessor is None:
        raise RuntimeError("portable preprocessor is unavailable")
    numeric = model.preprocessor.transform_numeric(data.numeric)
    categorical = model.preprocessor.transform_categorical(data.categorical)
    columns: list[Float32Array] = [numeric.astype(np.float32)]
    for index in range(categorical.shape[1]):
        columns.append(
            np.asarray(weights[f"encoder.embeddings.{index}.weight"])[categorical[:, index]]
        )
    return np.column_stack(columns).astype(np.float32)


def _linear_keys(model: PortableNeuralModel) -> tuple[tuple[str, str], ...]:
    hidden = tuple(
        (f"backbone.{index * 3}.weight", f"backbone.{index * 3}.bias")
        for index in range(len(model.config.hidden_dimensions))
    )
    return (*hidden, ("head.weight", "head.bias"))


def _numpy_logits(
    values: Float32Array, weights: WeightsLike, keys: tuple[tuple[str, str], ...]
) -> FloatArray:
    result = values
    for index, (weight_key, bias_key) in enumerate(keys):
        result = result @ np.asarray(weights[weight_key]).T + np.asarray(weights[bias_key])
        if index + 1 < len(keys):
            result = np.maximum(result, 0.0)
    return result[:, 0].astype(np.float64)


def _mlx_logits(
    values: Float32Array, weights: WeightsLike, keys: tuple[tuple[str, str], ...]
) -> FloatArray:
    module = cast("MlxModule", importlib.import_module("mlx.core"))
    result = module.array(values, dtype=module.float32)
    for index, (weight_key, bias_key) in enumerate(keys):
        weight = module.array(np.asarray(weights[weight_key]), dtype=module.float32)
        bias = module.array(np.asarray(weights[bias_key]), dtype=module.float32)
        result = result @ module.transpose(weight) + bias
        if index + 1 < len(keys):
            result = module.maximum(result, 0.0)
    module.eval(result)
    return np.asarray(result, dtype=np.float64)[:, 0]


def predict_portable_mlp(
    artifact_path: Path,
    data: ModelDataset,
    *,
    backend: str = "numpy",
) -> FloatArray:
    """Predict from backend-neutral JSON/NPZ without relying on Torch execution."""
    if backend not in {"numpy", "mlx"}:
        raise ValueError(f"unsupported portable inference backend: {backend}")
    model = PortableNeuralModel.load(artifact_path)
    if model.config.kind == NeuralModelKind.LOMETAB:
        raise ValueError("portable MLP inference does not accept LoMETab artifacts")
    with np.load(artifact_path / "weights.npz") as weights:
        values = _encoded_numpy(model, data, weights)
        keys = _linear_keys(model)
        return (
            _numpy_logits(values, weights, keys)
            if backend == "numpy"
            else _mlx_logits(values, weights, keys)
        )


def evaluate_backend_parity(
    reference: FloatArray,
    candidate: FloatArray,
    race_ids: LabStringArray,
    *,
    tolerance: float = 1e-5,
) -> BackendParity:
    """Require score closeness and identical within-race complete rankings."""
    if reference.shape != candidate.shape or reference.shape != race_ids.shape:
        raise ValueError("backend parity rows must align")
    if tolerance <= 0.0:
        raise ValueError("backend parity tolerance must be positive")
    max_abs_error = float(np.max(np.abs(reference - candidate))) if len(reference) else 0.0
    ranking_equal = True
    for race_id in np.unique(race_ids):
        indices = np.flatnonzero(race_ids == race_id)
        if not np.array_equal(
            indices[np.argsort(-reference[indices], kind="stable")],
            indices[np.argsort(-candidate[indices], kind="stable")],
        ):
            ranking_equal = False
            break
    return BackendParity(
        max_abs_error=max_abs_error,
        ranking_equal=ranking_equal,
        tolerance=tolerance,
        passed=max_abs_error <= tolerance and ranking_equal,
    )
