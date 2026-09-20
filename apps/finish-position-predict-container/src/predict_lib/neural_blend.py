"""Bounded score blend from a portable neural ranker artifact.

The ranker is trained offline (PyTorch on MPS) on the exact feature rows the
Container already builds for its production tree models. Only the small portable
artifact is baked into the image, so the Container evaluates one plain matrix
forward pass and then reuses the shared centered-trend adjustment. Cells whose
per-cell weight is not enabled keep the existing Prophet behaviour untouched.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from .model_meta import Category
from .prophet_adjustment import (
    KETTO_FIELD,
    ProphetAdjustmentResult,
    apply_centered_trend_adjustment,
)
from .prophet_cell_policy import (
    ProphetCellPolicy,
    load_prophet_cell_policy,
    parse_prophet_cell_policy,
)

NEURAL_BLEND_ENABLED_ENV: Final[str] = "NEURAL_BLEND_ENABLED"
NEURAL_BLEND_WEIGHT_ENV: Final[str] = "NEURAL_BLEND_WEIGHT"
NEURAL_BLEND_ARTIFACT_ENV: Final[str] = "NEURAL_BLEND_ARTIFACT"
NEURAL_BLEND_POLICY_ENV: Final[str] = "NEURAL_BLEND_POLICY"
DEFAULT_ARTIFACT_PATH: Final[Path] = Path("/app/lookups/neural-cell-blend-jra.json")
DEFAULT_POLICY_PATH: Final[Path] = Path("/app/lookups/neural_cell_policy.json")
ARTIFACT_VERSION: Final[str] = "neural-cell-blend-v1"
MAXIMUM_WEIGHT: Final[float] = 1.0
DISABLED_ENV_VALUES: Final[frozenset[str]] = frozenset({"0", "false", "off", "disabled"})
MINIMUM_SCALE: Final[float] = 1e-12
GELU_CUBIC: Final[float] = 0.044715
GELU_SCALE: Final[float] = math.sqrt(2.0 / math.pi)


@dataclass(frozen=True, slots=True)
class NeuralBlendArtifact:
    """Train-only normalisation plus a small MLP over production features."""

    category: Category
    feature_order: tuple[str, ...]
    mean: tuple[float, ...]
    scale: tuple[float, ...]
    layers: tuple[tuple[tuple[tuple[float, ...], ...], tuple[float, ...]], ...]

    def score(self, entries: Sequence[Mapping[str, object]]) -> list[float]:
        """Return one finite rank score per entry, in entry order."""
        results: list[float] = []
        for entry in entries:
            vector = [
                ((_finite_number(entry.get(name)) or 0.0) - mean) / scale
                for name, mean, scale in zip(self.feature_order, self.mean, self.scale, strict=True)
            ]
            for index, (weights, bias) in enumerate(self.layers):
                vector = [
                    sum(weight * value for weight, value in zip(row, vector, strict=True)) + offset
                    for row, offset in zip(weights, bias, strict=True)
                ]
                if index + 1 < len(self.layers):
                    vector = [_gelu(value) for value in vector]
            results.append(vector[0])
        return results


def _gelu(value: float) -> float:
    return 0.5 * value * (1.0 + math.erf(value / math.sqrt(2.0)))


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _finite_vector(value: object, field: str) -> tuple[float, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} must be a non-empty list")
    numbers = tuple(_finite_number(item) for item in value)
    if any(number is None for number in numbers):
        raise ValueError(f"{field} must contain finite numbers")
    return tuple(number for number in numbers if number is not None)


def parse_neural_blend_artifact(value: object) -> NeuralBlendArtifact:
    """Validate a decoded artifact without accepting partial malformed data."""
    if not isinstance(value, dict):
        raise ValueError("neural artifact must be an object")
    version = value.get("version")
    if version != ARTIFACT_VERSION:
        raise ValueError("unexpected neural artifact version")
    category = value.get("category")
    if category not in ("jra", "nar", "ban-ei"):
        raise ValueError("neural artifact category is invalid")
    feature_order = value.get("feature_order")
    if (
        not isinstance(feature_order, list)
        or not feature_order
        or any(not isinstance(name, str) or not name for name in feature_order)
    ):
        raise ValueError("neural artifact feature_order is invalid")
    mean = _finite_vector(value.get("mean"), "mean")
    scale = _finite_vector(value.get("scale"), "scale")
    if len(feature_order) != len(mean) or len(feature_order) != len(scale):
        raise ValueError("neural artifact normalisation length mismatch")
    if any(value <= MINIMUM_SCALE for value in scale):
        raise ValueError("neural artifact scale must be positive")
    raw_layers = value.get("layers")
    if not isinstance(raw_layers, list) or not raw_layers:
        raise ValueError("neural artifact layers are missing")
    layers: list[tuple[tuple[tuple[float, ...], ...], tuple[float, ...]]] = []
    width = len(feature_order)
    for index, raw_layer in enumerate(raw_layers):
        if not isinstance(raw_layer, dict):
            raise ValueError("neural artifact layer must be an object")
        raw_weights = raw_layer.get("weight")
        if not isinstance(raw_weights, list) or not raw_weights:
            raise ValueError("neural artifact layer weights are missing")
        weights = tuple(_finite_vector(row, f"layers.{index}.weight") for row in raw_weights)
        if any(len(row) != width for row in weights):
            raise ValueError("neural artifact layer width mismatch")
        bias = _finite_vector(raw_layer.get("bias"), f"layers.{index}.bias")
        if len(bias) != len(weights):
            raise ValueError("neural artifact layer bias mismatch")
        layers.append((weights, bias))
        width = len(weights)
    if width != 1:
        raise ValueError("neural artifact must end in a single score")
    return NeuralBlendArtifact(
        category=category,
        feature_order=tuple(feature_order),
        mean=mean,
        scale=scale,
        layers=tuple(layers),
    )


def load_neural_blend_artifact(path: Path) -> NeuralBlendArtifact | None:
    """Load the baked artifact, returning None for a missing or invalid file."""
    try:
        decoded: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    try:
        return parse_neural_blend_artifact(decoded)
    except ValueError:
        return None


def configured_neural_weight(
    category: Category,
    environment: Mapping[str, str] | None = None,
    *,
    cell_variant: str = "sim",
    branch_variant: str | None = None,
    served_signature: str | None = None,
    policy: ProphetCellPolicy | None = None,
) -> float | None:
    """Resolve the neural blend weight, with environment as an emergency override."""
    values = os.environ if environment is None else environment
    raw_enabled = values.get(NEURAL_BLEND_ENABLED_ENV)
    if raw_enabled is not None and raw_enabled.strip().lower() in DISABLED_ENV_VALUES:
        return None
    resolved = policy or _default_policy()
    decision = resolved.resolve(category, cell_variant, branch_variant, served_signature)
    if not decision.enabled:
        return None
    raw_weight = values.get(NEURAL_BLEND_WEIGHT_ENV)
    if raw_weight is None or not raw_weight.strip():
        return decision.weight
    try:
        weight = float(raw_weight)
    except ValueError:
        return None
    if not math.isfinite(weight) or weight <= 0.0 or weight > MAXIMUM_WEIGHT:
        return None
    return weight


_policy_cache: ProphetCellPolicy | None = None


def _default_policy() -> ProphetCellPolicy:
    global _policy_cache
    if _policy_cache is None:
        path = Path(os.environ.get(NEURAL_BLEND_POLICY_ENV, str(DEFAULT_POLICY_PATH)))
        if not path.is_file():
            path = Path(__file__).with_name("neural_cell_policy.json")
        try:
            _policy_cache = load_prophet_cell_policy(path)
        except RuntimeError:
            _policy_cache = parse_prophet_cell_policy(
                {
                    "version": "neural-cell-policy-unavailable",
                    "default_enabled": False,
                    "default_weight": 0.05,
                    "categories": {},
                }
            )
    return _policy_cache


def adjust_prediction_rows_with_neural(
    rows: Sequence[Sequence[object]],
    entries: Sequence[Mapping[str, object]],
    category: Category,
    environment: Mapping[str, str] | None = None,
    *,
    cell_variant: str = "sim",
    branch_variant: str | None = None,
    served_signature: str | None = None,
    policy: ProphetCellPolicy | None = None,
    artifact: NeuralBlendArtifact | None = None,
) -> ProphetAdjustmentResult:
    """Apply the bounded neural blend when the cell policy enables it."""
    copied_rows = [list(row) for row in rows]
    weight = configured_neural_weight(
        category,
        environment,
        cell_variant=cell_variant,
        branch_variant=branch_variant,
        served_signature=served_signature,
        policy=policy,
    )
    if weight is None:
        return ProphetAdjustmentResult(copied_rows, False, "disabled")
    if not copied_rows or len(copied_rows) != len(entries):
        return ProphetAdjustmentResult(copied_rows, False, "row-entry-mismatch")
    values = os.environ if environment is None else environment
    loaded = artifact
    if loaded is None:
        path = Path(values.get(NEURAL_BLEND_ARTIFACT_ENV, str(DEFAULT_ARTIFACT_PATH)))
        loaded = load_neural_blend_artifact(path)
    if loaded is None or loaded.category != category:
        return ProphetAdjustmentResult(copied_rows, False, "artifact-unavailable")
    scores = loaded.score(entries)
    trends: dict[str, float] = {}
    for entry, score in zip(entries, scores, strict=True):
        horse_id = str(entry.get(KETTO_FIELD, ""))
        if horse_id and math.isfinite(score):
            trends[horse_id] = score
    return apply_centered_trend_adjustment(copied_rows, trends, weight)
