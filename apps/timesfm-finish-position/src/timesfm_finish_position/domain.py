"""Owned data contracts for the local TimesFM experiment."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import numpy as np
import numpy.typing as npt

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]
StringArray = npt.NDArray[np.str_]

ACTION_WEIGHTS: FloatArray = np.linspace(0.0, 1.0, 21, dtype=np.float64)
BASELINE_ACTION_INDEX = 10
EVALUATION_YEARS = (2024, 2025, 2026)
DEFAULT_TIMESFM_REVISION = "c71907076f28b1241d1fccc37efd183d0912cd13"


class IntegrationPattern(StrEnum):
    """The three requested TimesFM integration patterns."""

    POLICY_AND_DATA = "policy-and-data"
    ROUTER_ONLY = "router-only"
    DYNAMIC_ENSEMBLE = "dynamic-ensemble"


class PretrainingMode(StrEnum):
    """Temporal forecaster origin used by one experiment arm."""

    TIMESFM3 = "timesfm3-pretrained"
    SCRATCH = "scratch"


@dataclass(frozen=True)
class RaceDataset:
    """Chronological one-row-per-race evaluation data."""

    race_ids: StringArray
    race_dates: StringArray
    race_years: IntArray
    cell_ids: StringArray
    features: FloatArray
    winner_ranks: IntArray

    @property
    def rows(self) -> int:
        """Return the race count."""
        return len(self.race_ids)


@dataclass(frozen=True)
class CellQuerySet:
    """Leak-free temporal contexts and row-to-forecast positions."""

    cell_ids: tuple[str, ...]
    contexts: tuple[FloatArray, ...]
    horizon: int
    row_query_indices: IntArray
    row_horizon_indices: IntArray


@dataclass(frozen=True)
class ExperimentConfig:
    """Runtime configuration for a six-arm local experiment."""

    input_path: Path
    output_path: Path
    checkpoint: str = "google/timesfm-3.0-pytorch"
    checkpoint_revision: str = DEFAULT_TIMESFM_REVISION
    context_length: int = 512
    batch_size: int = 4
    scratch_lags: int = 16
    scratch_steps: int = 120
    bootstrap_repetitions: int = 2_000
    seed: int = 20260902
    device: str | None = None


@dataclass(frozen=True)
class ArmMetrics:
    """Accuracy metrics for one integration/pretraining/year arm."""

    races: int
    top1: float
    top2: float
    top3: float
    top4: float
    top5: float
    top123_mean: float
    mean_reciprocal_rank: float
    mean_winner_rank: float
    baseline_top123_mean: float
    delta_top123_pp: float
    delta_ci95_low_pp: float
    delta_ci95_high_pp: float
    activation_rate: float
    mean_selected_weight: float


@dataclass(frozen=True)
class ArmResult:
    """One evaluated arm in the six-way matrix."""

    pattern: IntegrationPattern
    pretraining: PretrainingMode
    year: int
    metrics: ArmMetrics
