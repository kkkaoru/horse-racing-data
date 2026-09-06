"""Contracts shared by the heterogeneous finish-position model lab."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

LabFloatArray = npt.NDArray[np.float64]
LabIntArray = npt.NDArray[np.int64]
LabStringArray = npt.NDArray[np.str_]


@dataclass(frozen=True)
class WalkForwardFold:
    """One point-in-time-correct outer fold."""

    train_start: str
    train_end: str
    validation_start: str
    validation_end: str
    label: str


@dataclass(frozen=True)
class PredictionFrame:
    """Aligned OOF predictions used by every model and stacker."""

    race_ids: LabStringArray
    race_dates: LabStringArray
    horse_ids: LabStringArray
    finish_positions: LabIntArray
    decimal_odds: LabFloatArray
    win_probabilities: LabFloatArray
    ranking_scores: LabFloatArray

    @property
    def rows(self) -> int:
        """Return runner count."""
        return len(self.race_ids)


@dataclass(frozen=True)
class ProbabilityMetrics:
    """Probability, ranking, and betting metrics for one OOF prediction set."""

    runners: int
    races: int
    log_loss: float
    brier_score: float
    expected_calibration_error: float
    ndcg_at_3: float
    top1_accuracy: float
    winner_in_top3: float
    top3_set_accuracy: float
    roi: float
    yield_rate: float
    maximum_drawdown: float
