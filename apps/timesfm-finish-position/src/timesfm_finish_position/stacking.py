"""Regularized logistic stacking over aligned OOF model predictions."""

from __future__ import annotations

import platform
from dataclasses import dataclass

import numpy as np

from .domain import FloatArray
from .lab_domain import LabStringArray
from .lab_metrics import normalize_probabilities_by_race

NEWTON_ITERATIONS = 25
MIN_STANDARD_DEVIATION = 1e-8
MIN_CURVATURE = 1e-6


@dataclass(frozen=True)
class LogisticStacker:
    """Frozen L2 logistic stacker with train-only normalization."""

    coefficients: FloatArray
    means: FloatArray
    scales: FloatArray
    backend: str

    def predict(
        self, features: FloatArray, race_ids: LabStringArray
    ) -> tuple[FloatArray, FloatArray]:
        """Return race-normalized win probabilities and unnormalized ranking logits."""
        if features.ndim != 2 or features.shape[1] != len(self.means):
            raise ValueError("stacking feature shape does not match fitted model")
        normalized = (features - self.means) / self.scales
        design = np.column_stack((normalized, np.ones(len(normalized), dtype=np.float64)))
        logits = design @ self.coefficients
        probabilities = 1.0 / (1.0 + np.exp(-np.clip(logits, -40.0, 40.0)))
        return normalize_probabilities_by_race(race_ids, probabilities), logits


def _fit_newton_numpy(design: FloatArray, labels: FloatArray, l2: float) -> FloatArray:
    coefficients = np.zeros(design.shape[1], dtype=np.float64)
    regularizer = np.eye(design.shape[1], dtype=np.float64) * l2
    regularizer[-1, -1] = 0.0
    for _ in range(NEWTON_ITERATIONS):
        logits = design @ coefficients
        probabilities = 1.0 / (1.0 + np.exp(-np.clip(logits, -40.0, 40.0)))
        curvature = np.maximum(probabilities * (1.0 - probabilities), MIN_CURVATURE)
        gradient = design.T @ (probabilities - labels) + regularizer @ coefficients
        hessian = (design.T * curvature) @ design + regularizer
        coefficients -= np.linalg.solve(hessian, gradient)
    return coefficients


def _fit_newton_mlx(design: FloatArray, labels: FloatArray, l2: float) -> FloatArray:
    import mlx.core as mx

    matrix = mx.array(design, dtype=mx.float32)
    targets = mx.array(labels, dtype=mx.float32)
    coefficients = mx.zeros(matrix.shape[1], dtype=mx.float32)
    regularizer_values = np.eye(matrix.shape[1], dtype=np.float32) * l2
    regularizer_values[-1, -1] = 0.0
    regularizer = mx.array(regularizer_values)
    for _ in range(NEWTON_ITERATIONS):
        logits = matrix @ coefficients
        probabilities = mx.sigmoid(logits)
        curvature = mx.maximum(probabilities * (1.0 - probabilities), MIN_CURVATURE)
        gradient = matrix.T @ (probabilities - targets) + regularizer @ coefficients
        hessian = (matrix.T * curvature) @ matrix + regularizer
        step = mx.linalg.solve(hessian, gradient, stream=mx.cpu)
        coefficients = coefficients - step
        mx.eval(coefficients)
    return np.asarray(coefficients, dtype=np.float64)


def stacking_backend() -> str:
    """Use MLX only on Apple Silicon and a portable NumPy fallback elsewhere."""
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        return "mlx"
    return "numpy"


def fit_logistic_stacker(
    features: FloatArray, labels: FloatArray, *, l2: float = 1.0
) -> LogisticStacker:
    """Fit the first-choice stacker on OOF features only."""
    if features.ndim != 2 or labels.shape != (len(features),):
        raise ValueError("stacking features and labels must align")
    if len(features) == 0:
        raise ValueError("stacking training data must not be empty")
    if not np.all(np.isin(labels, (0.0, 1.0))):
        raise ValueError("stacking labels must be binary")
    if l2 <= 0.0:
        raise ValueError("l2 must be positive")
    means = np.mean(features, axis=0)
    scales = np.std(features, axis=0)
    scales[scales < MIN_STANDARD_DEVIATION] = 1.0
    normalized = (features - means) / scales
    design = np.column_stack((normalized, np.ones(len(normalized), dtype=np.float64)))
    backend = stacking_backend()
    coefficients = (
        _fit_newton_mlx(design, labels, l2)
        if backend == "mlx"
        else _fit_newton_numpy(design, labels, l2)
    )
    return LogisticStacker(coefficients, means, scales, backend)
