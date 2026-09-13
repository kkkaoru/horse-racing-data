"""Positive chronological importance weights without dropping old race teachers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

import numpy as np
import numpy.typing as npt

DEFAULT_HALF_LIFE_DAYS: float = 1826.25


@dataclass(frozen=True)
class RecencyConfig:
    history_start: date
    cutoff: date
    half_life_days: float = DEFAULT_HALF_LIFE_DAYS

    def __post_init__(self) -> None:
        if self.history_start >= self.cutoff:
            raise ValueError("History start must precede cutoff")
        if not np.isfinite(self.half_life_days) or self.half_life_days <= 0:
            raise ValueError("Half life must be finite and positive")


def _age_days(race_id: str, dates: Mapping[str, date], config: RecencyConfig) -> int:
    if race_id not in dates:
        raise ValueError("Missing source race date")
    value = dates[race_id]
    if not config.history_start <= value < config.cutoff:
        raise ValueError("Race date lies outside the frozen training window")
    return (config.cutoff - value).days


def chronological_weights(
    *, race_ids: npt.NDArray[np.str_], dates: Mapping[str, date], config: RecencyConfig
) -> npt.NDArray[np.float64]:
    """Normalize over unique races, not horses; each runner keeps its race's weight."""
    if race_ids.ndim != 1 or len(race_ids) == 0:
        raise ValueError("A nonempty race identity vector is required")
    unique, inverse = np.unique(race_ids, return_inverse=True)
    ages = np.asarray([_age_days(str(key), dates, config) for key in unique], dtype=np.float64)
    weights = np.exp2(-ages / config.half_life_days)
    if not np.isfinite(weights).all() or np.any(weights <= 0):
        raise ValueError("Weights must stay positive; do not silently drop old races")
    weights /= weights.mean()
    return weights[inverse]
