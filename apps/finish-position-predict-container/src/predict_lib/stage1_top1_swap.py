"""Top-1-only Stage-1 companion policy.

The established Stage-1 model remains the ranking floor for every position.
When the longer-trained companion selects a different winner, exchange only
the two horses' Stage-1 scores. Sorting those adjusted scores therefore changes
rank 1 and the companion winner's former rank while preserving every other
Stage-1 rank and keeping score/rank ordering internally consistent.
"""

from __future__ import annotations

from collections.abc import Sequence


def _top_index(horse_ids: Sequence[str], scores: Sequence[float]) -> int:
    return min(range(len(scores)), key=lambda index: (-scores[index], horse_ids[index]))


def apply_top1_score_swap(
    horse_ids: Sequence[str],
    base_scores: Sequence[float],
    companion_scores: Sequence[float],
) -> list[float]:
    """Return base scores with its top horse swapped with companion top-1.

    Ties use the production ranker's ascending horse-id tie-break. Empty input
    is valid and returns an empty list. Misaligned sequences fail closed.
    """
    if len(horse_ids) != len(base_scores) or len(horse_ids) != len(companion_scores):
        raise ValueError(
            "horse_ids, base_scores, and companion_scores must have equal lengths"
        )
    adjusted = [float(score) for score in base_scores]
    if not adjusted:
        return adjusted
    base_top = _top_index(horse_ids, base_scores)
    companion_top = _top_index(horse_ids, companion_scores)
    if base_top != companion_top:
        adjusted[base_top], adjusted[companion_top] = (
            adjusted[companion_top],
            adjusted[base_top],
        )
    return adjusted
