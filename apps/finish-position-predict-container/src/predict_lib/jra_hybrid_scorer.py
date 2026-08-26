#!/usr/bin/env python3
"""NumPy-only serving forward for the JRA dirt-small-005 hybrid artifact."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict, cast

import numpy as np

NEG_INF = -1e9


class StatsManifest(TypedDict):
    feature_names: list[str]
    mean: list[float]
    std: list[float]
    bin_edges: list[list[float]]
    categorical_columns: list[str]
    categorical_vocab: dict[str, list[str]]
    race_categorical_columns: list[str]
    race_categorical_vocab: dict[str, list[str]]


class ArchitectureManifest(TypedDict):
    layers: int
    attention_heads: int
    max_runners: int
    served_head_index: int


class RouteManifest(TypedDict):
    companion_weight: float
    seed_weights: list[float]


class SeedManifest(TypedDict):
    weight_file: str


class HybridManifest(TypedDict):
    stats: StatsManifest
    architecture: ArchitectureManifest
    route: RouteManifest
    seeds: list[SeedManifest]


def _linear(x: np.ndarray, weights: Mapping[str, np.ndarray], prefix: str) -> np.ndarray:
    output = x @ weights[f"{prefix}.weight"].T
    bias = weights.get(f"{prefix}.bias")
    return output + bias if bias is not None else output


def _layer_norm(
    x: np.ndarray,
    weights: Mapping[str, np.ndarray],
    prefix: str,
    eps: float = 1e-5,
) -> np.ndarray:
    mean = x.mean(axis=-1, keepdims=True)
    variance = x.var(axis=-1, keepdims=True)
    return (x - mean) / np.sqrt(variance + eps) * weights[f"{prefix}.weight"] + weights[
        f"{prefix}.bias"
    ]


def _softmax(x: np.ndarray) -> np.ndarray:
    shifted = x - x.max(axis=-1, keepdims=True)
    exponential = np.exp(shifted)
    return exponential / exponential.sum(axis=-1, keepdims=True)


def _attention(
    x: np.ndarray,
    weights: Mapping[str, np.ndarray],
    prefix: str,
    additive_mask: np.ndarray,
    heads: int,
) -> np.ndarray:
    batch, runners, dimensions = x.shape
    head_dimensions = dimensions // heads
    query = _linear(x, weights, f"{prefix}.query_proj")
    key = _linear(x, weights, f"{prefix}.key_proj")
    value = _linear(x, weights, f"{prefix}.value_proj")
    query = query.reshape(batch, runners, heads, head_dimensions).transpose(0, 2, 1, 3)
    key = key.reshape(batch, runners, heads, head_dimensions).transpose(0, 2, 1, 3)
    value = value.reshape(batch, runners, heads, head_dimensions).transpose(0, 2, 1, 3)
    scores = (query / math.sqrt(head_dimensions)) @ key.transpose(0, 1, 3, 2)
    scores = scores + additive_mask[:, None, None, :]
    attended = (_softmax(scores) @ value).transpose(0, 2, 1, 3)
    attended = attended.reshape(batch, runners, dimensions)
    return _linear(attended, weights, f"{prefix}.out_proj")


def _gelu(x: np.ndarray) -> np.ndarray:
    # Exact MLX nn.gelu definition. Race batches are small enough that the
    # Python ufunc fallback is acceptable and avoids a SciPy serving dependency.
    erf = np.frompyfunc(math.erf, 1, 1)
    erf_values = np.asarray(erf(x / math.sqrt(2.0)), dtype=x.dtype)
    values = x * (1.0 + erf_values) / 2.0
    return values.astype(x.dtype, copy=False)


def forward_heads(
    continuous: np.ndarray,
    bins: np.ndarray,
    categorical: np.ndarray,
    race_categorical: np.ndarray,
    umaban: np.ndarray,
    mask: np.ndarray,
    weights: Mapping[str, np.ndarray],
    *,
    layers: int = 2,
    attention_heads: int = 4,
) -> np.ndarray:
    """Return all five expert scores with shape ``(batch, runners, heads)``."""
    continuous_values = continuous.astype(np.float32)
    bins64 = bins.astype(np.int64)
    projected = _linear(continuous_values, weights, "continuous_projection")
    projected = projected * weights["continuous_scale"]
    missing = _linear((bins64 == 0).astype(np.float32), weights, "missing_projection")
    missing = missing * weights["missing_scale"]
    bin_sum = np.zeros_like(projected)
    for feature_index in range(bins64.shape[-1]):
        bin_sum += weights[f"bin_embeddings.{feature_index}.weight"][bins64[:, :, feature_index]]
    bin_sum = bin_sum / math.sqrt(bins64.shape[-1]) * weights["bin_scale"]
    category_sum = weights["umaban_embedding.weight"][umaban.astype(np.int64)]
    for column_index in range(categorical.shape[-1]):
        category_sum += weights[f"categorical_embeddings.{column_index}.weight"][
            categorical[:, :, column_index].astype(np.int64)
        ]
    for column_index in range(race_categorical.shape[-1]):
        race_embedding = weights[f"race_categorical_embeddings.{column_index}.weight"][
            race_categorical[:, column_index].astype(np.int64)
        ]
        category_sum += race_embedding[:, None, :]
    category_sum = category_sum * weights["categorical_scale"]
    encoded = _layer_norm(
        projected + missing + bin_sum + category_sum,
        weights,
        "input_norm",
    )
    additive_mask = np.where(mask, 0.0, NEG_INF).astype(np.float32)
    for layer_index in range(layers):
        prefix = f"encoder.layers.{layer_index}"
        normalized = _layer_norm(encoded, weights, f"{prefix}.ln1")
        encoded = encoded + _attention(
            normalized,
            weights,
            f"{prefix}.attention",
            additive_mask,
            attention_heads,
        )
        normalized = _layer_norm(encoded, weights, f"{prefix}.ln2")
        hidden = np.maximum(_linear(normalized, weights, f"{prefix}.linear1"), 0.0)
        encoded = encoded + _linear(hidden, weights, f"{prefix}.linear2")
    encoded = _layer_norm(encoded, weights, "encoder.ln")
    scores = []
    for head_index in range(5):
        hidden = _gelu(_linear(encoded, weights, f"expert_heads.{head_index}.input"))
        scores.append(_linear(hidden, weights, f"expert_heads.{head_index}.output").squeeze(-1))
    return np.stack(scores, axis=-1)


def within_race_zscore(scores: Sequence[float]) -> np.ndarray:
    values = np.asarray(scores, dtype=np.float64)
    if values.size == 0:
        return values
    standard_deviation = float(values.std())
    if standard_deviation <= 0.0:
        return np.zeros_like(values)
    return (values - values.mean()) / standard_deviation


@dataclass(frozen=True)
class JraHybridScorer:
    manifest: HybridManifest
    seeds: tuple[dict[str, np.ndarray], ...]

    @classmethod
    def load(cls, artifact: Path) -> JraHybridScorer:
        manifest = cast(
            HybridManifest,
            json.loads((artifact / "manifest.json").read_text(encoding="utf-8")),
        )
        seeds = tuple(
            {
                key: value.astype(np.float32)
                for key, value in np.load(artifact / seed["weight_file"]).items()
            }
            for seed in manifest["seeds"]
        )
        return cls(manifest=manifest, seeds=seeds)

    @property
    def feature_order(self) -> tuple[str, ...]:
        """Ordered continuous feature contract stored in the artifact."""
        return tuple(self.manifest["stats"]["feature_names"])

    @property
    def companion_weight(self) -> float:
        """Configured companion fusion weight."""
        return float(self.manifest["route"]["companion_weight"])

    def missing_feature_keys(self, entries: Sequence[Mapping[str, object]]) -> set[str]:
        """Return structural feature-contract gaps, excluding present null values."""
        if not entries:
            return set()
        stats = self.manifest["stats"]
        required = {
            *stats["feature_names"],
            *stats["categorical_columns"],
            *stats["race_categorical_columns"],
            "umaban",
        }
        return required - set(entries[0])

    def encode_entries(
        self, races: Sequence[Sequence[Mapping[str, object]]]
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        stats = self.manifest["stats"]
        features = stats["feature_names"]
        max_runners = int(self.manifest["architecture"]["max_runners"])
        race_count = len(races)
        continuous = np.zeros((race_count, max_runners, len(features)), dtype=np.float64)
        bins = np.zeros((race_count, max_runners, len(features)), dtype=np.int64)
        categorical_columns = stats["categorical_columns"]
        categorical = np.zeros((race_count, max_runners, len(categorical_columns)), dtype=np.int64)
        race_columns = stats["race_categorical_columns"]
        race_categorical = np.zeros((race_count, len(race_columns)), dtype=np.int64)
        umaban = np.zeros((race_count, max_runners), dtype=np.int64)
        mask = np.zeros((race_count, max_runners), dtype=np.bool_)
        category_lookups = {
            name: {value: index + 1 for index, value in enumerate(stats["categorical_vocab"][name])}
            for name in categorical_columns
        }
        race_lookups = {
            name: {
                value: index + 1
                for index, value in enumerate(stats["race_categorical_vocab"][name])
            }
            for name in race_columns
        }
        for race_index, entries in enumerate(races):
            if entries:
                first = entries[0]
                for column_index, name in enumerate(race_columns):
                    value = first.get(name)
                    race_categorical[race_index, column_index] = (
                        race_lookups[name].get(str(value).strip(), 0) if value is not None else 0
                    )
            for runner_index, entry in enumerate(entries[:max_runners]):
                mask[race_index, runner_index] = True
                raw_umaban = _finite_float(entry.get("umaban"))
                if raw_umaban is not None and 0 <= int(raw_umaban) <= max_runners:
                    umaban[race_index, runner_index] = int(raw_umaban)
                for feature_index, name in enumerate(features):
                    value = _finite_float(entry.get(name))
                    if value is None:
                        continue
                    continuous[race_index, runner_index, feature_index] = (
                        value - stats["mean"][feature_index]
                    ) / stats["std"][feature_index]
                    bins[race_index, runner_index, feature_index] = (
                        np.searchsorted(stats["bin_edges"][feature_index], value, side="right") + 1
                    )
                for column_index, name in enumerate(categorical_columns):
                    value = entry.get(name)
                    categorical[race_index, runner_index, column_index] = (
                        category_lookups[name].get(str(value).strip(), 0)
                        if value is not None
                        else 0
                    )
        return continuous, bins, categorical, race_categorical, umaban, mask

    def companion_scores(self, entries: Sequence[Mapping[str, object]]) -> list[float]:
        """Return the weighted three-seed venue02-head score for one race."""
        arrays = self.encode_entries([entries])
        scores = self.venue_score_ensemble(*arrays)[0, : len(entries)]
        return [float(score) for score in scores]

    def venue_score_ensemble(
        self,
        continuous: np.ndarray,
        bins: np.ndarray,
        categorical: np.ndarray,
        race_categorical: np.ndarray,
        umaban: np.ndarray,
        mask: np.ndarray,
    ) -> np.ndarray:
        architecture = self.manifest["architecture"]
        venue_index = int(architecture["served_head_index"])
        outputs = [
            forward_heads(
                continuous,
                bins,
                categorical,
                race_categorical,
                umaban,
                mask,
                weights,
                layers=int(architecture["layers"]),
                attention_heads=int(architecture["attention_heads"]),
            )[:, :, venue_index]
            for weights in self.seeds
        ]
        seed_weights = np.asarray(self.manifest["route"]["seed_weights"], dtype=np.float32)
        return np.average(outputs, axis=0, weights=seed_weights)


def fuse_jra_hybrid_scores(
    base_scores: Sequence[float],
    companion_scores: Sequence[float],
    *,
    companion_weight: float,
) -> list[float]:
    """Fuse current and companion scores after within-race z-normalization."""
    if len(base_scores) != len(companion_scores):
        raise ValueError("base and companion score lengths differ")
    base_z = within_race_zscore(base_scores)
    companion_z = within_race_zscore(companion_scores)
    current_weight = 1.0 - companion_weight
    return [
        float(current_weight * base_z[index] + companion_weight * companion_z[index])
        for index in range(len(base_scores))
    ]


def _finite_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        converted = 1.0 if value else 0.0
    elif isinstance(value, (int, float)):
        converted = float(value)
    else:
        try:
            converted = float(str(value).strip())
        except ValueError:
            return None
    return converted if math.isfinite(converted) else None
