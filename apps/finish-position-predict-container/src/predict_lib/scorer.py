"""Feature-vector preparation + booster scoring (architecture-aware).

The actual CatBoost / XGBoost model objects are injected as a ``BoosterLike``
protocol so this module stays free of heavy native imports and is unit-testable
with a fake booster. Per
``docs/finish-position-accuracy/legacy/FINISH_POSITION_MODEL_V7_LINEAGE.md``
section 10.4 the
NAR (XGBoost) path must cast features to float32 before scoring for bit-faithful
parity with the TypeScript scorer; CatBoost ranking is robust at float64.
"""

from __future__ import annotations

import struct
from collections.abc import Mapping, Sequence
from typing import Protocol, runtime_checkable

from .model_meta import Architecture


@runtime_checkable
class BoosterLike(Protocol):
    """Minimal surface the scorer needs from a loaded booster."""

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        """Return one raw score per input row."""
        ...


def _to_float32(value: float) -> float:
    """Round-trip a float through 32-bit precision (NumPy-free)."""
    packed = struct.pack("f", value)
    return struct.unpack("f", packed)[0]


def build_feature_row(
    entry: Mapping[str, object],
    feature_names: Sequence[str],
    architecture: Architecture,
) -> list[float]:
    """Project one race entry onto ``feature_names`` order as a float row.

    Missing features default to ``0.0`` (CatBoost/XGBoost treat absent numeric
    inputs as 0 in this pipeline). XGBoost rows are float32-quantised.
    """
    raw = [float(_coerce(entry.get(name))) for name in feature_names]
    if architecture == "xgboost":
        return [_to_float32(value) for value in raw]
    return raw


def _coerce(value: object) -> float:
    """Coerce an arbitrary cell to float, treating None/empty as 0.0."""
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text == "":
        return 0.0
    return float(text)


def build_feature_matrix(
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
    architecture: Architecture,
) -> list[list[float]]:
    """Build the full feature matrix for one race in ``feature_names`` order."""
    return [build_feature_row(entry, feature_names, architecture) for entry in entries]


def assert_feature_count(feature_names: Sequence[str], expected: int) -> None:
    """Raise when the metadata feature width does not match expectation."""
    if len(feature_names) != expected:
        message = f"expected {expected} features, metadata has {len(feature_names)}"
        raise ValueError(message)


def score_matrix(booster: BoosterLike, matrix: Sequence[Sequence[float]]) -> list[float]:
    """Run the injected booster over a feature matrix and return raw scores."""
    return list(booster.predict(matrix))
