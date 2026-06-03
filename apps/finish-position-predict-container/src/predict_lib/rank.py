"""Assign within-race ``predicted_rank`` from ``predicted_score``.

Higher score => better finish => lower (closer to 1) rank, mirroring the
CatBoost / XGBoost ranker convention used by ``finish_position_catboost``
``write_predictions_jsonl``. Ties break on ``ketto_toroku_bango`` so the output
is deterministic across runs (important for idempotent re-runs).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final, NamedTuple

FIRST_RANK: Final[int] = 1


class ScoredHorse(NamedTuple):
    """A single horse's raw score within one race, before ranking."""

    ketto_toroku_bango: str
    umaban: int
    predicted_score: float


class RankedHorse(NamedTuple):
    """A horse augmented with its 1-based within-race rank."""

    ketto_toroku_bango: str
    umaban: int
    predicted_score: float
    predicted_rank: int


def _sort_key(horse: ScoredHorse) -> tuple[float, str]:
    """Sort by descending score (negated) then ascending ketto for stable ties."""
    return (-horse.predicted_score, horse.ketto_toroku_bango)


def rank_within_race(horses: Sequence[ScoredHorse]) -> list[RankedHorse]:
    """Return ``horses`` ordered by score with a 1-based ``predicted_rank``."""
    ordered = sorted(horses, key=_sort_key)
    return [
        RankedHorse(
            ketto_toroku_bango=horse.ketto_toroku_bango,
            umaban=horse.umaban,
            predicted_score=horse.predicted_score,
            predicted_rank=index + FIRST_RANK,
        )
        for index, horse in enumerate(ordered)
    ]
