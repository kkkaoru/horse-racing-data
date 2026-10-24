"""Lock predicted rank-1 from a primary scorer; re-rank the rest by another.

The 新潟記念 serve cascade keeps the draw-wide QuerySoftMax winner at rank 1
and sorts every other horse by going-only scores. Returned scores sort into
that order under the production ranker (descending score, then ascending
horse id) so predicted_score and predicted_rank stay consistent.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Final

LOCK1_SCORE_GAP: Final[float] = 1.0


def _race_z(scores: Sequence[float]) -> list[float]:
    values = [float(score) for score in scores]
    mean = math.fsum(values) / len(values)
    std = math.sqrt(math.fsum((value - mean) ** 2 for value in values) / len(values))
    scale = 1.0 if std == 0.0 else std
    return [(value - mean) / scale for value in values]


def blend_lock_scores(
    lock_scores: Sequence[float],
    blend_scores: Sequence[float],
    weight: float,
) -> list[float]:
    """Return race_z(lock) + weight * race_z(blend) over one race's horses.

    race_z uses the population std (ddof=0); a constant race treats std as 1.
    Empty input returns an empty list; misaligned sequences fail closed.
    """
    if len(lock_scores) != len(blend_scores):
        raise ValueError("lock_scores and blend_scores must have equal lengths")
    if not lock_scores:
        return []
    lock_z = _race_z(lock_scores)
    blend_z = _race_z(blend_scores)
    return [lock + weight * blend for lock, blend in zip(lock_z, blend_z, strict=True)]


def _top_index(horse_ids: Sequence[str], scores: Sequence[float]) -> int:
    return min(range(len(scores)), key=lambda index: (-scores[index], horse_ids[index]))


def apply_lock1_rerank_rest(
    horse_ids: Sequence[str],
    lock_scores: Sequence[float],
    rest_scores: Sequence[float],
) -> list[float]:
    """Return rest_scores with the lock-model winner forced to rank 1.

    Empty input is valid and returns an empty list. Misaligned sequences fail
    closed. A single-horse field is returned unchanged.
    """
    if len(horse_ids) != len(lock_scores) or len(horse_ids) != len(rest_scores):
        raise ValueError("horse_ids, lock_scores, and rest_scores must have equal lengths")
    adjusted = [float(score) for score in rest_scores]
    if len(adjusted) < 2:
        return adjusted
    lock_top = _top_index(horse_ids, lock_scores)
    if _top_index(horse_ids, adjusted) == lock_top:
        return adjusted
    rest_max = max(score for index, score in enumerate(adjusted) if index != lock_top)
    adjusted[lock_top] = rest_max + LOCK1_SCORE_GAP
    return adjusted
