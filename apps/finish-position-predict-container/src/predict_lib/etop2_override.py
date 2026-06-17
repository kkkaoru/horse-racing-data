"""E-top2 place-preserving XGBoost override for JRA (iter22-jra-etop2).

Logic (applied per race after CB iter20 and XGB scoring):

  Given CatBoost ranking CB#1, CB#2, CB#3, ... and XGBoost's rank-1 horse
  (XGB#1), the override fires when XGB#1 == CB#2 AND the race class is not
  701 (新馬):

    rank-1 = CB#2 (= XGB#1)   <- promoted from CB rank-2
    rank-2 = CB#1              <- demoted from CB rank-1
    rank-3 = CB#3              <- UNCHANGED (preserved by construction)
    rank-4+ = CB#4+ ...        <- UNCHANGED

  All other cases output pure CatBoost ranking unchanged:

    XGB#1 == CB#1: pure CatBoost (already agree)
    XGB#1 ∈ CB#3+: pure CatBoost (no override — preserves place3)
    class == "701": pure CatBoost (XGB winner less reliable in maiden races)

Override is implemented via score injection: the CB#2 horse receives
``max(cb_scores) + 1.0``, the CB#1 horse receives ``max(cb_scores) + 0.5``,
all other horses keep their original CB score. This matches the offline
evaluation logic in ``tmp/hybrid_e_place_preserving.py`` exactly.

Reference: docs/finish-position-accuracy/per-class/jra/place-preserving-override.md
Blind 2025 gate: top1 LB95 +0.58pp / place2 LB95 +0.06pp / place3 +0.00pp
Verdict: ADOPT (2026-06-18)
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

# Race class code that is EXCLUDED from the override (maiden races).
# Documented regression: XGB winner is less reliable in class 701 (-0.66pp
# top1, LB95 -3.62pp -- see per-class breakdown in place-preserving-override.md).
ETOP2_EXCLUDED_CLASS: Final[str] = "701"

# The version label written to the predictions table when E-top2 override
# is active. Allows distinguishing E-top2 rows from pure CB iter20 rows.
ETOP2_MODEL_VERSION: Final[str] = "iter22-jra-etop2"

# XGBoost model version key in the model registry.
ETOP2_XGB_MODEL_VERSION: Final[str] = "xgb-jra-2013-v8"


def apply_etop2_scores(
    cb_scores: Sequence[float],
    xgb_scores: Sequence[float],
    race_class: str | None,
) -> list[float]:
    """Apply E-top2 override and return modified scores for ranking.

    Pure function: given per-horse CB scores and XGB scores for one race,
    returns a new score list where rank-1 and rank-2 may be swapped when the
    E-top2 condition is met.

    The returned scores are intended only for ranking (higher = better). The
    absolute values are not meaningful — they are constructed to guarantee
    the correct ordering by injecting ``max(cb) + 1.0`` and ``max(cb) + 0.5``
    for the promoted pair.

    Parameters
    ----------
    cb_scores:
        CatBoost raw scores for each horse, in entry order. Higher = better rank.
    xgb_scores:
        XGBoost raw scores for each horse, in the same entry order as cb_scores.
    race_class:
        The race's ``kyoso_joken_code`` (e.g. ``"703"``, ``"701"``). ``None``
        is treated the same as any non-excluded class (override eligible).

    Returns
    -------
    list[float]
        Scores in the same entry order. Equal to ``list(cb_scores)`` when:
        - the race has fewer than 2 horses,
        - race_class is ETOP2_EXCLUDED_CLASS (701),
        - XGB#1 == CB#1 (already agree), or
        - XGB#1 ∈ CB#3+ (would disturb place3 — not eligible).
        Otherwise returns the swapped scores when XGB#1 == CB#2.
    """
    n = len(cb_scores)
    if n < 2:
        return list(cb_scores)

    if race_class == ETOP2_EXCLUDED_CLASS:
        return list(cb_scores)

    # Identify CB rank-1 and rank-2 indices (higher score = better)
    sorted_by_cb = sorted(range(n), key=lambda i: -cb_scores[i])
    cb_rank1_idx = sorted_by_cb[0]
    cb_rank2_idx = sorted_by_cb[1]

    # Identify XGB rank-1 index (higher score = better)
    xgb_rank1_idx = max(range(n), key=lambda i: xgb_scores[i])

    # E-top2 condition: XGB#1 matches CB#2 exactly
    if xgb_rank1_idx != cb_rank2_idx:
        return list(cb_scores)

    # Override: promote CB#2 to rank-1, demote CB#1 to rank-2
    cb_max = max(cb_scores)
    result = list(cb_scores)
    result[cb_rank2_idx] = cb_max + 1.0  # CB#2 -> rank-1 (highest score)
    result[cb_rank1_idx] = cb_max + 0.5  # CB#1 -> rank-2
    # CB#3+ keep original scores — their relative order and rank-3 are preserved
    return result


def is_etop2_override_active(
    cb_scores: Sequence[float],
    xgb_scores: Sequence[float],
    race_class: str | None,
) -> bool:
    """Return True when the E-top2 override would change the rank-1 horse.

    Useful for logging / smoke-test verification that the override fires
    in some races. Equivalent to checking whether :func:`apply_etop2_scores`
    changes the argmax, but without allocating the full score list.
    """
    n = len(cb_scores)
    if n < 2 or race_class == ETOP2_EXCLUDED_CLASS:
        return False
    sorted_by_cb = sorted(range(n), key=lambda i: -cb_scores[i])
    cb_rank2_idx = sorted_by_cb[1]
    xgb_rank1_idx = max(range(n), key=lambda i: xgb_scores[i])
    return xgb_rank1_idx == cb_rank2_idx
