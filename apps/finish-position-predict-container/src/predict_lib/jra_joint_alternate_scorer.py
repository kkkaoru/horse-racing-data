"""NumPy serving for the shadow-only JRA joint additional Top5 candidate."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import numpy as np
from scipy.special import erf

MODEL_VERSION: Final[str] = "jra-joint-group-dro-alternate-top5-v1"
SPECIALIST_MODEL_VERSION: Final[str] = "jra-cb-high-payout-specialist235-2026-v1"
CHAMPION_MODEL_VERSION: Final[str] = "jra-cb-v9-sim-2013-clean-jockey-pedigree269"
MODEL_FILE_NAME: Final[str] = "model.json"
METADATA_FILE_NAME: Final[str] = "metadata.json"
_REQUIRED_ARRAYS: Final[frozenset[str]] = frozenset(
    {
        "runner.layers.0.weight",
        "runner.layers.0.bias",
        "runner.layers.2.weight",
        "runner.layers.2.bias",
        "residual.layers.0.weight",
        "residual.layers.0.bias",
        "residual.layers.2.weight",
        "residual.layers.2.bias",
        "defer.layers.0.weight",
        "defer.layers.0.bias",
        "defer.layers.2.weight",
        "defer.layers.2.bias",
        "mean",
        "std",
    }
)


@dataclass(frozen=True, slots=True)
class JointAlternateCandidate:
    horse_id: str | None
    score: float | None
    margin: float | None
    defer_probability: float
    reason: str

    @property
    def emitted(self) -> bool:
        return self.horse_id is not None


@dataclass(frozen=True, slots=True)
class JraJointAlternateScorer:
    feature_names: tuple[str, ...]
    minimum_candidate_margin: float
    weights: Mapping[str, np.ndarray]

    @classmethod
    def load(cls, artifact_dir: Path) -> JraJointAlternateScorer:
        metadata = json.loads((artifact_dir / METADATA_FILE_NAME).read_text(encoding="utf-8"))
        if not isinstance(metadata, dict) or metadata.get("model_version") != MODEL_VERSION:
            raise ValueError("joint alternate metadata identity mismatch")
        raw_names = metadata.get("feature_names")
        if (
            not isinstance(raw_names, list)
            or not raw_names
            or not all(isinstance(name, str) for name in raw_names)
        ):
            raise ValueError("joint alternate feature_names invalid")
        feature_names = tuple(raw_names)
        if metadata.get("feature_count") != len(feature_names):
            raise ValueError("joint alternate feature_count mismatch")
        if (
            metadata.get("shadow_only") is not True
            or metadata.get("changes_primary_rank") is not False
        ):
            raise ValueError("joint alternate must be shadow-only and preserve primary ranks")
        raw_margin = metadata.get("minimum_candidate_margin")
        if not isinstance(raw_margin, int | float) or isinstance(raw_margin, bool):
            raise ValueError("joint alternate minimum_candidate_margin invalid")
        minimum_margin = float(raw_margin)
        if not np.isfinite(minimum_margin) or minimum_margin < 0:
            raise ValueError("joint alternate minimum_candidate_margin invalid")
        with np.load(artifact_dir / MODEL_FILE_NAME, allow_pickle=False) as archive:
            if frozenset(archive.files) != _REQUIRED_ARRAYS:
                raise ValueError("joint alternate model arrays mismatch")
            weights = {name: np.asarray(archive[name], dtype=np.float32) for name in archive.files}
        cls._validate_shapes(weights, len(feature_names))
        return cls(feature_names, minimum_margin, weights)

    @staticmethod
    def _validate_shapes(weights: Mapping[str, np.ndarray], feature_count: int) -> None:
        expected = {
            "runner.layers.0.weight": (128, feature_count + 2),
            "runner.layers.0.bias": (128,),
            "runner.layers.2.weight": (64, 128),
            "runner.layers.2.bias": (64,),
            "residual.layers.0.weight": (64, 128),
            "residual.layers.0.bias": (64,),
            "residual.layers.2.weight": (1, 64),
            "residual.layers.2.bias": (1,),
            "defer.layers.0.weight": (32, 64),
            "defer.layers.0.bias": (32,),
            "defer.layers.2.weight": (1, 32),
            "defer.layers.2.bias": (1,),
            "mean": (feature_count,),
            "std": (feature_count,),
        }
        for name, shape in expected.items():
            if weights[name].shape != shape or not np.isfinite(weights[name]).all():
                raise ValueError(f"joint alternate array invalid: {name}")
        if (weights["std"] <= 0).any():
            raise ValueError("joint alternate normalization std must be positive")

    @staticmethod
    def _zscore(values: Sequence[float]) -> np.ndarray:
        array = np.asarray(values, dtype=np.float32)
        if array.ndim != 1 or not np.isfinite(array).all():
            raise ValueError("joint alternate expert scores must be finite vectors")
        std = float(np.std(array))
        if std < 1e-8:
            return np.zeros_like(array)
        return (array - float(np.mean(array))) / std

    @staticmethod
    def _gelu(value: np.ndarray) -> np.ndarray:
        return 0.5 * value * (1.0 + erf(value / np.sqrt(2.0)))

    def _linear(self, value: np.ndarray, name: str) -> np.ndarray:
        return value @ self.weights[f"{name}.weight"].T + self.weights[f"{name}.bias"]

    def _features(self, entries: Sequence[Mapping[str, object]]) -> np.ndarray:
        raw = np.empty((len(entries), len(self.feature_names)), dtype=np.float32)
        mean = self.weights["mean"]
        for row_index, entry in enumerate(entries):
            for column_index, name in enumerate(self.feature_names):
                value = entry.get(name)
                try:
                    number = (
                        float(value)
                        if isinstance(value, int | float | str) and not isinstance(value, bool)
                        else np.nan
                    )
                except (TypeError, ValueError):
                    number = np.nan
                raw[row_index, column_index] = number if np.isfinite(number) else mean[column_index]
        return (raw - mean) / self.weights["std"]

    def _forward(
        self,
        entries: Sequence[Mapping[str, object]],
        baseline: np.ndarray,
        specialist: np.ndarray,
    ) -> tuple[np.ndarray, float]:
        features = self._features(entries)
        inputs = np.concatenate([features, baseline[:, None], specialist[:, None]], axis=1)
        runner = self._gelu(self._linear(inputs, "runner.layers.0"))
        runner = self._gelu(self._linear(runner, "runner.layers.2"))
        context = np.mean(runner, axis=0, keepdims=True)
        expanded = np.broadcast_to(context, runner.shape)
        residual = self._gelu(
            self._linear(np.concatenate([runner, expanded], axis=1), "residual.layers.0")
        )
        residual = self._linear(residual, "residual.layers.2").reshape(-1)
        defer_hidden = self._gelu(self._linear(context, "defer.layers.0"))
        defer_logit = float(self._linear(defer_hidden, "defer.layers.2")[0, 0])
        defer_probability = float(1.0 / (1.0 + np.exp(-defer_logit)))
        return baseline + defer_probability * residual, defer_probability

    def select_candidate(
        self,
        entries: Sequence[Mapping[str, object]],
        baseline_scores: Sequence[float],
        specialist_scores: Sequence[float],
        served_top5: Sequence[str],
    ) -> JointAlternateCandidate:
        if len(entries) <= 5:
            return JointAlternateCandidate(None, None, None, 0.0, "field-size-le5")
        if len(entries) != len(baseline_scores) or len(entries) != len(specialist_scores):
            raise ValueError("joint alternate entries and score lengths differ")
        horse_ids = tuple(str(entry.get("ketto_toroku_bango") or "") for entry in entries)
        if any(not horse_id for horse_id in horse_ids) or len(set(horse_ids)) != len(horse_ids):
            raise ValueError("joint alternate horse identities are missing or duplicated")
        scores, defer_probability = self._forward(
            entries, self._zscore(baseline_scores), self._zscore(specialist_scores)
        )
        served = frozenset(served_top5)
        outside = [index for index, horse_id in enumerate(horse_ids) if horse_id not in served]
        if not outside:
            return JointAlternateCandidate(None, None, None, defer_probability, "no-outside-runner")
        ordered = sorted(outside, key=lambda index: (-float(scores[index]), index))
        best = ordered[0]
        margin = float("inf") if len(ordered) == 1 else float(scores[best] - scores[ordered[1]])
        if margin < self.minimum_candidate_margin:
            return JointAlternateCandidate(
                None, None, margin, defer_probability, "low-margin-abstain"
            )
        return JointAlternateCandidate(
            horse_ids[best], float(scores[best]), margin, defer_probability, "joint-margin-pass"
        )
