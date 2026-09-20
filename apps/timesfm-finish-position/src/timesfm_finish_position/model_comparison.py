"""Correlation, diversity, and portable CPU-cost measurements."""

from __future__ import annotations

import platform
import resource
import time
from collections.abc import Callable
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path

import numpy as np

from .domain import FloatArray
from .lab_domain import LabIntArray, LabStringArray
from .model_interface import ModelPrediction

CORRELATION_EPSILON = 1e-12
PROBABILITY_EPSILON = 1e-7


@dataclass(frozen=True)
class PredictionRelationship:
    """Candidate relationship to a baseline on identical runners."""

    prediction_correlation: float
    error_correlation: float


@dataclass(frozen=True)
class MemberDiversity:
    """Output-level diversity of one implicit neural ensemble."""

    members: int
    mean_pairwise_correlation: float
    mean_symmetric_kl: float
    top1_disagreement_rate: float


@dataclass(frozen=True)
class CpuPerformance:
    """Portable CPU inference resource measurements."""

    runners: int
    races: int
    elapsed_seconds: float
    latency_per_horse_ms: float
    latency_per_race_ms: float
    throughput_horses_per_second: float
    peak_rss_mib: float
    peak_rss_delta_mib: float
    model_size_bytes: int
    cold_start_seconds: float


def _correlation(left: FloatArray, right: FloatArray) -> float:
    if left.shape != right.shape or left.ndim != 1:
        raise ValueError("correlation inputs must be aligned vectors")
    if len(left) < 2:
        raise ValueError("correlation needs at least two rows")
    if np.std(left) < CORRELATION_EPSILON or np.std(right) < CORRELATION_EPSILON:
        return 0.0
    return float(np.corrcoef(left, right)[0, 1])


def compare_to_baseline(
    baseline: ModelPrediction,
    candidate: ModelPrediction,
    finish_positions: LabIntArray,
) -> PredictionRelationship:
    """Measure probability and signed winner-error correlations."""
    baseline.validate()
    candidate.validate()
    if not np.array_equal(baseline.race_ids, candidate.race_ids) or not np.array_equal(
        baseline.horse_ids, candidate.horse_ids
    ):
        raise ValueError("baseline and candidate runner identities must align")
    if finish_positions.shape != baseline.probability.shape:
        raise ValueError("comparison labels must align")
    labels = (finish_positions == 1).astype(np.float64)
    return PredictionRelationship(
        prediction_correlation=_correlation(baseline.probability, candidate.probability),
        error_correlation=_correlation(
            baseline.probability - labels,
            candidate.probability - labels,
        ),
    )


def _symmetric_binary_kl(left: FloatArray, right: FloatArray) -> float:
    left_safe = np.clip(left, PROBABILITY_EPSILON, 1.0 - PROBABILITY_EPSILON)
    right_safe = np.clip(right, PROBABILITY_EPSILON, 1.0 - PROBABILITY_EPSILON)
    left_to_right = left_safe * np.log(left_safe / right_safe) + (1.0 - left_safe) * np.log(
        (1.0 - left_safe) / (1.0 - right_safe)
    )
    right_to_left = right_safe * np.log(right_safe / left_safe) + (1.0 - right_safe) * np.log(
        (1.0 - right_safe) / (1.0 - left_safe)
    )
    return float(np.mean((left_to_right + right_to_left) / 2.0))


def _member_top1(race_ids: LabStringArray, member_values: FloatArray) -> LabIntArray:
    starts = np.flatnonzero(np.r_[True, race_ids[1:] != race_ids[:-1]])
    ends = np.r_[starts[1:], len(race_ids)]
    top1 = np.empty((len(starts), member_values.shape[1]), dtype=np.int64)
    for race_index, (start, end) in enumerate(zip(starts, ends, strict=True)):
        top1[race_index] = start + np.argmax(member_values[start:end], axis=0)
    return top1


def evaluate_member_diversity(
    race_ids: LabStringArray, member_probabilities: FloatArray
) -> MemberDiversity:
    """Measure all unique member pairs on probabilities and race decisions."""
    if member_probabilities.ndim != 2 or len(member_probabilities) != len(race_ids):
        raise ValueError("member probabilities must align with race rows")
    members = member_probabilities.shape[1]
    if members < 2:
        raise ValueError("diversity requires at least two members")
    if np.any(~np.isfinite(member_probabilities)) or np.any(
        (member_probabilities < 0.0) | (member_probabilities > 1.0)
    ):
        raise ValueError("member probabilities must be finite probabilities")
    top1 = _member_top1(race_ids, member_probabilities)
    correlations: list[float] = []
    divergences: list[float] = []
    disagreements: list[float] = []
    for left, right in combinations(range(members), 2):
        correlations.append(
            _correlation(member_probabilities[:, left], member_probabilities[:, right])
        )
        divergences.append(
            _symmetric_binary_kl(member_probabilities[:, left], member_probabilities[:, right])
        )
        disagreements.append(float(np.mean(top1[:, left] != top1[:, right])))
    return MemberDiversity(
        members=members,
        mean_pairwise_correlation=float(np.mean(correlations)),
        mean_symmetric_kl=float(np.mean(divergences)),
        top1_disagreement_rate=float(np.mean(disagreements)),
    )


def _directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def benchmark_cpu_inference(
    *,
    infer: Callable[[], ModelPrediction],
    cold_load: Callable[[], object],
    artifact_path: Path,
) -> CpuPerformance:
    """Measure one CPU inference and artifact load without changing its dataset."""
    cold_started = time.perf_counter()
    cold_load()
    cold_seconds = time.perf_counter() - cold_started
    peak_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started = time.perf_counter()
    prediction = infer()
    elapsed = time.perf_counter() - started
    peak_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    races = len(np.unique(prediction.race_ids))
    runners = len(prediction.race_ids)
    peak_scale = 1024.0**2 if platform_peak_rss_is_bytes() else 1024.0
    return CpuPerformance(
        runners=runners,
        races=races,
        elapsed_seconds=elapsed,
        latency_per_horse_ms=elapsed * 1000.0 / runners,
        latency_per_race_ms=elapsed * 1000.0 / races,
        throughput_horses_per_second=runners / elapsed,
        peak_rss_mib=float(peak_after) / peak_scale,
        peak_rss_delta_mib=max(float(peak_after - peak_before), 0.0) / peak_scale,
        model_size_bytes=_directory_size(artifact_path),
        cold_start_seconds=cold_seconds,
    )


def platform_peak_rss_is_bytes() -> bool:
    """Return whether ru_maxrss uses bytes on the current platform."""
    return platform.system() == "Darwin"
