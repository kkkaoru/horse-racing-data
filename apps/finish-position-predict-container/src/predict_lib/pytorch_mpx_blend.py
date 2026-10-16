"""Linux-CPU ONNX Runtime blend of the Mac-trained race-set ranker.

Weights are an ONNX graph exported from PyTorch, not JSON floats and not a
hand-written numpy Transformer. The forward pass runs only after the cell
policy enables a weight.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Protocol

import numpy as np

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

ARTIFACT_VERSION: Final[str] = "pytorch-mpx-onnx-v1"
POLICY_FILE_NAME: Final[str] = "pytorch_mpx_cell_policy.json"
DEFAULT_POLICY_PATH: Final[Path] = Path(__file__).with_name(POLICY_FILE_NAME)
DEFAULT_ARTIFACT_PATH: Final[Path] = Path(__file__).with_name("pytorch_mpx_jra.json")
ENABLED_ENV: Final[str] = "PYTORCH_MPX_BLEND_ENABLED"
WEIGHT_ENV: Final[str] = "PYTORCH_MPX_BLEND_WEIGHT"
ARTIFACT_ENV: Final[str] = "PYTORCH_MPX_BLEND_ARTIFACT"
POLICY_ENV: Final[str] = "PYTORCH_MPX_BLEND_POLICY"
DISABLED_ENV_VALUES: Final[frozenset[str]] = frozenset({"0", "false", "off", "disabled"})
MAXIMUM_WEIGHT: Final[float] = 1.0
MINIMUM_SCALE: Final[float] = 1e-12
SIGNAL_SCORE: Final[str] = "score"
SIGNAL_RANK: Final[str] = "rank"

ScoreEngine = Callable[[np.ndarray], np.ndarray]


class _OnnxSession(Protocol):
    def run(
        self,
        output_names: Sequence[str] | None,
        input_feed: Mapping[str, np.ndarray],
    ) -> Sequence[object]: ...


@dataclass(frozen=True, slots=True)
class PytorchMpxArtifact:
    """Normalisation sidecar plus an ONNX graph next to the JSON."""

    category: Category
    feature_order: tuple[str, ...]
    mean: tuple[float, ...]
    scale: tuple[float, ...]
    onnx_path: Path
    signals: Mapping[str, str]


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


def _feature_value(entry: Mapping[str, object], name: str) -> float:
    number = _finite_number(entry.get(name))
    return 0.0 if number is None else number


def _finite_vector(value: object, field: str) -> tuple[float, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} must be a non-empty list")
    numbers = tuple(_finite_number(item) for item in value)
    if any(number is None for number in numbers):
        raise ValueError(f"{field} must contain finite numbers")
    return tuple(number for number in numbers if number is not None)


def parse_pytorch_mpx_artifact(value: object, *, base_dir: Path) -> PytorchMpxArtifact:
    """Validate a decoded sidecar without accepting partial malformed data."""
    if not isinstance(value, dict):
        raise ValueError("pytorch mpx artifact must be an object")
    if value.get("version") != ARTIFACT_VERSION:
        raise ValueError("unexpected pytorch mpx artifact version")
    category = value.get("category")
    if category not in {"jra", "nar", "ban-ei"}:
        raise ValueError("pytorch mpx artifact category is invalid")
    feature_order = value.get("feature_order")
    if (
        not isinstance(feature_order, list)
        or not feature_order
        or any(not isinstance(name, str) or not name for name in feature_order)
    ):
        raise ValueError("pytorch mpx artifact feature_order is invalid")
    mean = _finite_vector(value.get("mean"), "mean")
    scale = _finite_vector(value.get("scale"), "scale")
    if len(feature_order) != len(mean) or len(feature_order) != len(scale):
        raise ValueError("pytorch mpx artifact normalisation length mismatch")
    if any(item <= MINIMUM_SCALE for item in scale):
        raise ValueError("pytorch mpx artifact scale must be positive")
    onnx_name = value.get("onnx")
    if not isinstance(onnx_name, str) or not onnx_name.endswith(".onnx"):
        raise ValueError("pytorch mpx artifact onnx path is invalid")
    raw_signals = value.get("signals", {})
    if not isinstance(raw_signals, dict):
        raise ValueError("pytorch mpx artifact signals must be an object")
    signals: dict[str, str] = {}
    for cell, signal in raw_signals.items():
        if not isinstance(cell, str) or signal not in {SIGNAL_SCORE, SIGNAL_RANK}:
            raise ValueError("pytorch mpx artifact signals are invalid")
        signals[cell] = signal
    return PytorchMpxArtifact(
        category=category,
        feature_order=tuple(str(name) for name in feature_order),
        mean=mean,
        scale=scale,
        onnx_path=base_dir / onnx_name,
        signals=signals,
    )


def load_pytorch_mpx_artifact(path: Path) -> PytorchMpxArtifact | None:
    """Load the baked sidecar, returning None for a missing or invalid file."""
    try:
        decoded: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    try:
        artifact = parse_pytorch_mpx_artifact(decoded, base_dir=path.parent)
    except ValueError:
        return None
    if not artifact.onnx_path.is_file():
        return None
    return artifact


def score_race(
    artifact: PytorchMpxArtifact,
    numeric: np.ndarray,
    *,
    engine: ScoreEngine | None = None,
) -> np.ndarray:
    """Return one finite score per runner for a single race matrix."""
    if numeric.ndim != 2 or numeric.shape[1] != len(artifact.feature_order):
        raise ValueError("numeric race matrix width must match the artifact")
    if numeric.shape[0] < 1:
        raise ValueError("numeric race matrix must contain runners")
    centered = (
        (numeric - np.asarray(artifact.mean, dtype=np.float32))
        / np.asarray(artifact.scale, dtype=np.float32)
    ).astype(np.float32)
    if engine is not None:
        return engine(centered)
    return _onnx_score(artifact.onnx_path, centered)


def _onnx_score(onnx_path: Path, centered: np.ndarray) -> np.ndarray:
    session = _cached_session(onnx_path)
    mask = np.ones((1, centered.shape[0]), dtype=np.bool_)
    outputs = session.run(["scores"], {"features": centered[None, :, :], "mask": mask})
    if not outputs:
        raise ValueError("onnx runtime returned no scores")
    scores = np.asarray(outputs[0], dtype=np.float32)
    if scores.ndim != 2 or scores.shape[1] < centered.shape[0]:
        raise ValueError("onnx runtime score shape is invalid")
    return scores[0, : centered.shape[0]]


_session_cache: _OnnxSession | None = None
_session_path: Path | None = None


def _cached_session(onnx_path: Path) -> _OnnxSession:
    global _session_cache, _session_path
    if _session_cache is not None and _session_path == onnx_path:
        return _session_cache
    import onnxruntime as ort

    session = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
    _session_cache = session
    _session_path = onnx_path
    return session


def race_trends(
    scores: Sequence[float],
    *,
    signal: str,
) -> list[float]:
    """Convert raw scores into the cell's evaluation signal."""
    if signal == SIGNAL_SCORE:
        return [float(score) for score in scores]
    if signal != SIGNAL_RANK:
        raise ValueError("pytorch mpx signal must be score or rank")
    order = sorted(range(len(scores)), key=lambda index: -scores[index])
    ranks = [0] * len(scores)
    for rank, index in enumerate(order, start=1):
        ranks[index] = rank
    return [-float(rank) for rank in ranks]


def configured_pytorch_mpx_weight(
    category: Category,
    environment: Mapping[str, str] | None = None,
    *,
    cell_variant: str = "sim",
    branch_variant: str | None = None,
    served_signature: str | None = None,
    policy: ProphetCellPolicy | None = None,
) -> float | None:
    """Resolve the blend weight, skipping the forward pass when disabled."""
    values = os.environ if environment is None else environment
    raw_enabled = values.get(ENABLED_ENV)
    if raw_enabled is not None and raw_enabled.strip().lower() in DISABLED_ENV_VALUES:
        return None
    resolved = policy or _default_policy()
    decision = resolved.resolve(category, cell_variant, branch_variant, served_signature)
    if not decision.enabled:
        return None
    raw_weight = values.get(WEIGHT_ENV)
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
_artifact_cache: PytorchMpxArtifact | None = None
_artifact_loaded: bool = False


def _default_policy() -> ProphetCellPolicy:
    global _policy_cache
    if _policy_cache is None:
        path = Path(os.environ.get(POLICY_ENV, str(DEFAULT_POLICY_PATH)))
        if not path.is_file():
            path = DEFAULT_POLICY_PATH
        try:
            _policy_cache = load_prophet_cell_policy(path)
        except RuntimeError:
            _policy_cache = parse_prophet_cell_policy(
                {
                    "version": "pytorch-mpx-policy-unavailable",
                    "default_enabled": False,
                    "default_weight": 0.05,
                    "categories": {},
                }
            )
    return _policy_cache


def _cached_artifact(path: Path) -> PytorchMpxArtifact | None:
    global _artifact_cache, _artifact_loaded
    if not _artifact_loaded:
        _artifact_cache = load_pytorch_mpx_artifact(path)
        _artifact_loaded = True
    return _artifact_cache


def reset_pytorch_mpx_caches() -> None:
    """Clear process caches so tests can swap artifacts and policies."""
    global _policy_cache, _artifact_cache, _artifact_loaded, _session_cache, _session_path
    _policy_cache = None
    _artifact_cache = None
    _artifact_loaded = False
    _session_cache = None
    _session_path = None


def adjust_prediction_rows_with_pytorch_mpx(
    rows: Sequence[Sequence[object]],
    entries: Sequence[Mapping[str, object]],
    category: Category,
    environment: Mapping[str, str] | None = None,
    *,
    cell_variant: str = "sim",
    branch_variant: str | None = None,
    served_signature: str | None = None,
    policy: ProphetCellPolicy | None = None,
    artifact: PytorchMpxArtifact | None = None,
    engine: ScoreEngine | None = None,
) -> ProphetAdjustmentResult:
    """Apply the bounded ONNX blend when the cell policy enables it."""
    copied_rows = [list(row) for row in rows]
    weight = configured_pytorch_mpx_weight(
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
        path = Path(values.get(ARTIFACT_ENV, str(DEFAULT_ARTIFACT_PATH)))
        loaded = _cached_artifact(path)
    if loaded is None or loaded.category != category:
        return ProphetAdjustmentResult(copied_rows, False, "artifact-unavailable")
    numeric = np.asarray(
        [[_feature_value(entry, name) for name in loaded.feature_order] for entry in entries],
        dtype=np.float32,
    )
    try:
        scores = score_race(loaded, numeric, engine=engine)
    except Exception:
        # Process boundary: ONNX Runtime raises vendor Fail types; tree scores must still publish.
        return ProphetAdjustmentResult(copied_rows, False, "score-failed")
    signal = loaded.signals.get(cell_variant, SIGNAL_SCORE)
    converted = race_trends([float(value) for value in scores], signal=signal)
    trends: dict[str, float] = {}
    for entry, trend in zip(entries, converted, strict=True):
        horse_id = str(entry.get(KETTO_FIELD, ""))
        if horse_id:
            trends[horse_id] = trend
    return apply_centered_trend_adjustment(copied_rows, trends, weight)
