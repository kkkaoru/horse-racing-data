"""Train-only numeric encodings and categorical vocabularies for neural models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

import numpy as np
import numpy.typing as npt

from .domain import FloatArray
from .lab_domain import LabIntArray, LabStringArray
from .tabular_evaluation import apply_imputation, fit_imputation

MIN_SCALE = 1e-8
Float32Array = npt.NDArray[np.float32]


class NumericEncoding(StrEnum):
    """Portable numeric input representations."""

    NORMALIZED = "normalized"
    PIECEWISE_LINEAR = "piecewise-linear"
    PERIODIC = "periodic"


@dataclass(frozen=True)
class NeuralFeatureConfig:
    """Feature transformation choices shared across neural candidates."""

    numeric_encoding: NumericEncoding = NumericEncoding.NORMALIZED
    piecewise_bins: int = 8
    periodic_frequencies: int = 4
    categorical_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class NeuralPreprocessor:
    """Frozen train-only preprocessing state."""

    config: NeuralFeatureConfig
    medians: FloatArray
    means: FloatArray
    scales: FloatArray
    piecewise_edges: FloatArray
    categorical_vocabularies: Mapping[str, tuple[str, ...]]

    @property
    def numeric_output_features(self) -> int:
        """Return transformed numeric width."""
        feature_count = len(self.medians)
        widths = {
            NumericEncoding.NORMALIZED: feature_count,
            NumericEncoding.PIECEWISE_LINEAR: feature_count * self.config.piecewise_bins,
            NumericEncoding.PERIODIC: feature_count * self.config.periodic_frequencies * 2,
        }
        return widths[self.config.numeric_encoding]

    @property
    def categorical_cardinalities(self) -> tuple[int, ...]:
        """Return vocabulary sizes including the unknown bucket."""
        return tuple(
            len(self.categorical_vocabularies[name]) + 1 for name in self.config.categorical_names
        )

    def transform_numeric(self, values: FloatArray) -> Float32Array:
        """Apply frozen numeric transformation."""
        imputed = apply_imputation(values, self.medians)
        normalized = (imputed - self.means) / self.scales
        transforms = {
            NumericEncoding.NORMALIZED: lambda: normalized,
            NumericEncoding.PIECEWISE_LINEAR: lambda: _piecewise_transform(
                imputed, self.piecewise_edges
            ),
            NumericEncoding.PERIODIC: lambda: _periodic_transform(
                normalized, self.config.periodic_frequencies
            ),
        }
        return transforms[self.config.numeric_encoding]().astype(np.float32)

    def transform_categorical(self, values: Mapping[str, LabStringArray]) -> LabIntArray:
        """Map categories through train-only vocabularies with zero as unknown."""
        if not self.config.categorical_names:
            return np.empty((len(next(iter(values.values()), ())), 0), dtype=np.int64)
        columns = tuple(
            _map_categories(values[name], self.categorical_vocabularies[name])
            for name in self.config.categorical_names
        )
        return np.column_stack(columns).astype(np.int64)


def _fit_piecewise_edges(values: FloatArray, bins: int) -> FloatArray:
    quantiles = np.linspace(0.0, 1.0, bins + 1)
    return np.quantile(values, quantiles, axis=0).T.astype(np.float64)


def _piecewise_transform(values: FloatArray, edges: FloatArray) -> FloatArray:
    lower = edges[:, :-1]
    widths = edges[:, 1:] - lower
    safe_widths = np.where(widths > 0.0, widths, 1.0)
    encoded = np.clip((values[:, :, None] - lower[None, :, :]) / safe_widths[None, :, :], 0.0, 1.0)
    return encoded.reshape(len(values), -1).astype(np.float64)


def _periodic_transform(values: FloatArray, frequency_count: int) -> FloatArray:
    frequencies = np.pi * (2.0 ** np.arange(frequency_count, dtype=np.float64))
    phases = values[:, :, None] * frequencies[None, None, :]
    encoded = np.concatenate((np.sin(phases), np.cos(phases)), axis=2)
    return encoded.reshape(len(values), -1).astype(np.float64)


def _fit_vocabulary(values: LabStringArray) -> tuple[str, ...]:
    return tuple(str(value) for value in np.unique(values) if value != "")


def _map_categories(values: LabStringArray, vocabulary: tuple[str, ...]) -> LabIntArray:
    indices = {value: index + 1 for index, value in enumerate(vocabulary)}
    return np.asarray([indices.get(str(value), 0) for value in values], dtype=np.int64)


def fit_neural_preprocessor(
    numeric: FloatArray,
    categorical: Mapping[str, LabStringArray],
    config: NeuralFeatureConfig,
) -> NeuralPreprocessor:
    """Fit imputation, scaling, bins, and vocabularies on training rows only."""
    if numeric.ndim != 2 or len(numeric) == 0:
        raise ValueError("numeric training features must be a nonempty matrix")
    if config.piecewise_bins < 2 or config.periodic_frequencies < 1:
        raise ValueError("numeric embedding dimensions are invalid")
    if len(set(config.categorical_names)) != len(config.categorical_names):
        raise ValueError("categorical feature names must be unique")
    if any(name not in categorical for name in config.categorical_names):
        raise ValueError("configured categorical feature is missing")
    medians = fit_imputation(numeric)
    imputed = apply_imputation(numeric, medians)
    means = np.mean(imputed, axis=0)
    scales = np.std(imputed, axis=0)
    scales[scales < MIN_SCALE] = 1.0
    edges = _fit_piecewise_edges(imputed, config.piecewise_bins)
    vocabularies = {name: _fit_vocabulary(categorical[name]) for name in config.categorical_names}
    return NeuralPreprocessor(config, medians, means, scales, edges, vocabularies)
