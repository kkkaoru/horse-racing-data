"""Pure-function ensemble scoring for per-class JRA ensembles (Phase B-2B).

Used at container inference time when a race's ``kyoso_joken_code`` resolves to
a ``PerClassEnsemble`` (``rank_blend`` type). Each member scores the race's
horses, scores are rank-normalized within the race, then blended with the
manifest's weights.

For offline ensemble optimization see
``apps/pc-keiba-viewer/src/scripts/finish-position-features/per_class_ensemble_lib.py``.
"""

from __future__ import annotations

from typing import Final

import numpy as np
import pandas as pd

# Within-race rank normalization: top horse gets 1.0, bottom gets 0.0.
# For 1-horse races (degenerate, shouldn't happen in JRA), return 0.5.
_SINGLE_RACE_SCORE: Final[float] = 0.5
_MIN_RACE_SIZE_FOR_RANK: Final[int] = 1


def normalize_within_race(
    race_id: pd.Series,
    scores: np.ndarray,
    tiebreak: pd.Series,
) -> np.ndarray:
    """Return normalized scores in ``[0, 1]`` within each race.

    Sort indices within each race by ``(-scores, +tiebreak)`` (descending score,
    ascending tiebreak) for stable, deterministic order. Top entry -> ``1.0``,
    bottom -> ``0.0``, linearly interpolated for middle ranks. Single-entry
    races -> ``0.5`` (degenerate, defensive — JRA should never hit this).

    All inputs MUST be aligned positionally (same length, same row order). The
    returned array is aligned with the input ordering (NOT sorted), so callers
    can blend across members without re-joining.
    """
    n = len(scores)
    if len(race_id) != n or len(tiebreak) != n:
        message = (
            f"normalize_within_race: length mismatch "
            f"(race_id={len(race_id)}, scores={n}, tiebreak={len(tiebreak)})"
        )
        raise ValueError(message)
    normalized = np.empty(n, dtype=np.float64)
    if n == 0:
        return normalized
    work = pd.DataFrame(
        {
            "_race_id": race_id.to_numpy(),
            "_score": scores,
            "_tiebreak": tiebreak.to_numpy(),
            "_orig_idx": np.arange(n),
        }
    )
    ordered = work.sort_values(
        by=["_race_id", "_score", "_tiebreak"],
        ascending=[True, False, True],
        kind="stable",
    )
    grouped = ordered.groupby("_race_id", sort=False)
    rank_idx = grouped.cumcount().to_numpy(dtype=np.float64)
    race_size = grouped["_race_id"].transform("size").to_numpy(dtype=np.float64)
    denom = race_size - 1.0
    safe_denom = np.where(denom > 0.0, denom, 1.0)
    sorted_normalized = np.where(
        race_size > _MIN_RACE_SIZE_FOR_RANK,
        (denom - rank_idx) / safe_denom,
        _SINGLE_RACE_SCORE,
    )
    orig_idx = ordered["_orig_idx"].to_numpy(dtype=np.int64)
    normalized[orig_idx] = sorted_normalized
    return normalized


def blend_normalized(
    normalized_per_member: list[np.ndarray],
    weights: list[float],
) -> np.ndarray:
    """Return weighted sum of normalized score arrays (positional alignment).

    All input arrays MUST be aligned (same length, same row order). Weights MUST
    sum to ~1.0 (caller's responsibility — manifest enforces simplex).
    """
    if not normalized_per_member:
        message = "blend_normalized requires at least one member"
        raise ValueError(message)
    if len(normalized_per_member) != len(weights):
        message = (
            f"blend_normalized: members ({len(normalized_per_member)}) and "
            f"weights ({len(weights)}) length mismatch"
        )
        raise ValueError(message)
    base_len = len(normalized_per_member[0])
    for idx, arr in enumerate(normalized_per_member):
        if len(arr) != base_len:
            message = (
                f"blend_normalized: array length mismatch at index {idx} "
                f"(expected {base_len}, got {len(arr)})"
            )
            raise ValueError(message)
    blended = np.zeros(base_len, dtype=np.float64)
    for arr, weight in zip(normalized_per_member, weights, strict=True):
        blended = blended + weight * arr.astype(np.float64, copy=False)
    return blended


def score_with_ensemble(
    member_scores: dict[str, np.ndarray],
    weights: dict[str, float],
    race_id: pd.Series,
    tiebreak: pd.Series,
) -> np.ndarray:
    """Per-member rank-normalize within race, then blend with manifest weights.

    ``member_scores`` and ``weights`` MUST share the same keys (raised by the
    explicit per-member weight lookup). The iteration order follows
    ``member_scores``'s key order, which Python dicts preserve since 3.7, so the
    output is deterministic for a given input dict construction.

    Returns the blended score array aligned positionally with the ``race_id`` /
    ``tiebreak`` / per-member-score row order.
    """
    if not member_scores:
        message = "score_with_ensemble requires at least one member"
        raise ValueError(message)
    member_versions = list(member_scores.keys())
    normalized_list = [
        normalize_within_race(race_id, member_scores[mv], tiebreak)
        for mv in member_versions
    ]
    weight_list = [weights[mv] for mv in member_versions]
    return blend_normalized(normalized_list, weight_list)
