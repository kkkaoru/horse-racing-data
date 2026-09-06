"""Pretrained TimesFM 3.0 and task-only scratch temporal forecasters."""

from __future__ import annotations

import platform
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

import numpy as np
from timesfm3 import ModelConfig, TimesFM3Evaluator

from .domain import FloatArray

MIN_AR_SAMPLES = 32
RIDGE_ALPHA = 1e-3


class TemporalForecaster(Protocol):
    """Forecast all action-gain variates for a shared horizon."""

    @property
    def backend(self) -> str:
        """Return an auditable runtime backend label."""
        ...

    def predict(self, contexts: Sequence[FloatArray], *, horizon: int) -> tuple[FloatArray, ...]:
        """Forecast each context without observing the evaluation year."""
        ...


@dataclass
class TimesFm3Forecaster:
    """Official frozen TimesFM 3.0 evaluator with platform device routing."""

    checkpoint: str
    batch_size: int
    device: str
    checkpoint_revision: str = "c71907076f28b1241d1fccc37efd183d0912cd13"
    _evaluator: TimesFM3Evaluator | None = field(default=None, init=False, repr=False)

    @property
    def backend(self) -> str:
        """Return the official inference backend."""
        return f"pytorch-{self.device}"

    def _load_evaluator(self) -> TimesFM3Evaluator:
        if self._evaluator is None:
            self._evaluator = TimesFM3Evaluator(
                ModelConfig(
                    checkpoint_path=self.checkpoint,
                    revision=self.checkpoint_revision,
                    per_core_batch_size=self.batch_size,
                    device=self.device,
                )
            )
        return self._evaluator

    def predict(self, contexts: Sequence[FloatArray], *, horizon: int) -> tuple[FloatArray, ...]:
        """Run one joint multivariate forecast per cell."""
        if not contexts:
            return ()
        if horizon < 1:
            raise ValueError("horizon must be positive")
        outputs = self._load_evaluator().predict_batch(
            contexts=[np.asarray(context, dtype=np.float32) for context in contexts],
            horizon=horizon,
            return_quantiles=False,
            use_symmetric_averaging=False,
        )
        forecasts: list[FloatArray] = []
        for output in outputs:
            if output.forecast is None:
                raise RuntimeError("TimesFM 3.0 returned no point forecast")
            forecast = np.asarray(output.forecast, dtype=np.float64)
            if forecast.shape != (contexts[len(forecasts)].shape[0], horizon):
                raise RuntimeError(f"unexpected TimesFM forecast shape: {forecast.shape}")
            forecasts.append(forecast)
        if len(forecasts) != len(contexts):
            raise RuntimeError("TimesFM 3.0 omitted a cell forecast")
        return tuple(forecasts)


def resolve_timesfm_device(requested: str | None) -> str:
    """Resolve the official TimesFM device without making non-Mac execution impossible."""
    if requested is not None:
        return requested
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        return "mps"
    try:
        import torch
    except ImportError:
        return "cpu"
    return "cuda" if torch.cuda.is_available() else "cpu"


def _ar_samples(contexts: Sequence[FloatArray], lags: int) -> tuple[FloatArray, FloatArray]:
    feature_blocks: list[FloatArray] = []
    target_blocks: list[FloatArray] = []
    for context in contexts:
        if context.shape[1] <= lags:
            continue
        windows = np.lib.stride_tricks.sliding_window_view(context, lags, axis=1)
        feature_blocks.append(windows[:, :-1, :].reshape(-1, lags))
        target_blocks.append(context[:, lags:].reshape(-1))
    if not feature_blocks:
        return np.empty((0, lags), dtype=np.float64), np.empty((0,), dtype=np.float64)
    return np.concatenate(feature_blocks), np.concatenate(target_blocks)


def _fit_numpy_ar(features: FloatArray, targets: FloatArray) -> FloatArray:
    design = np.column_stack((features, np.ones(len(features), dtype=np.float64)))
    regularizer = np.eye(design.shape[1], dtype=np.float64) * RIDGE_ALPHA
    regularizer[-1, -1] = 0.0
    return np.linalg.solve(design.T @ design + regularizer, design.T @ targets)


def _fit_mlx_ar(features: FloatArray, targets: FloatArray) -> FloatArray:
    import mlx.core as mx

    design = mx.array(
        np.column_stack((features, np.ones(len(features), dtype=np.float64))), dtype=mx.float32
    )
    target = mx.array(targets, dtype=mx.float32)
    regularizer_values = np.eye(design.shape[1], dtype=np.float32) * RIDGE_ALPHA
    regularizer_values[-1, -1] = 0.0
    regularizer = mx.array(regularizer_values)
    coefficients = mx.linalg.solve(
        design.T @ design + regularizer, design.T @ target, stream=mx.cpu
    )
    mx.eval(coefficients)
    return np.asarray(coefficients, dtype=np.float64)


def _recursive_forecast(context: FloatArray, coefficients: FloatArray, horizon: int) -> FloatArray:
    lags = len(coefficients) - 1
    rows: list[FloatArray] = []
    for series in context:
        history = list(np.asarray(series, dtype=np.float64))
        if len(history) < lags:
            history = [0.0] * (lags - len(history)) + history
        predicted: list[float] = []
        for _ in range(horizon):
            value = float(np.dot(np.asarray(history[-lags:]), coefficients[:-1]) + coefficients[-1])
            predicted.append(value)
            history.append(value)
        rows.append(np.asarray(predicted, dtype=np.float64))
    return np.asarray(rows, dtype=np.float64)


@dataclass(frozen=True)
class ScratchAutoregressiveForecaster:
    """Task-only no-pretraining comparator, accelerated with MLX on Apple Silicon."""

    lags: int

    @property
    def backend(self) -> str:
        """Return the scratch optimization backend."""
        return "mlx" if platform.system() == "Darwin" and platform.machine() == "arm64" else "numpy"

    def predict(self, contexts: Sequence[FloatArray], *, horizon: int) -> tuple[FloatArray, ...]:
        """Fit a pooled causal AR model and recursively forecast each cell."""
        if self.lags < 1:
            raise ValueError("lags must be positive")
        if horizon < 1:
            raise ValueError("horizon must be positive")
        if not contexts:
            return ()
        features, targets = _ar_samples(contexts, self.lags)
        if len(features) < MIN_AR_SAMPLES:
            coefficients = np.zeros(self.lags + 1, dtype=np.float64)
            coefficients[-1] = float(np.mean(targets)) if len(targets) else 0.0
        elif self.backend == "mlx":
            coefficients = _fit_mlx_ar(features, targets)
        else:
            coefficients = _fit_numpy_ar(features, targets)
        return tuple(_recursive_forecast(context, coefficients, horizon) for context in contexts)
