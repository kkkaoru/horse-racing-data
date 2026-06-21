"""E-top2 place-preserving CatBoost override for NAR (iter23-nar-etop2).

The NAR variant is the MIRROR IMAGE of the JRA override
(:mod:`predict_lib.etop2_override`): NAR production scores with XGBoost
(``iter12-nar-xgb-hpo-v8``) as the BASE, and a CatBoost CB-2013 model
(``cb-nar-2013-v8``) supplies the override signal. So the roles of the two
boosters are swapped relative to JRA.

Logic (applied per race after XGB and CB scoring, only for ADOPT classes):

  Given the XGBoost base ranking XGB#1, XGB#2, XGB#3, ... and CatBoost's
  rank-1 horse (CB#1), the override fires when CB#1 == XGB#2:

    rank-1 = XGB#2 (= CB#1)   <- promoted from XGB rank-2
    rank-2 = XGB#1             <- demoted from XGB rank-1
    rank-3 = XGB#3             <- UNCHANGED (preserved by construction)
    rank-4+ = XGB#4+ ...       <- UNCHANGED

  All other cases output pure XGBoost ranking unchanged:

    CB#1 == XGB#1: pure XGB (already agree)
    CB#1 ∈ XGB#3+: pure XGB (no override — preserves place3)
    nar_class ∉ ADOPT classes: pure XGB (per-class routing)
    nar_class is None: pure XGB (safety — unknown class)

Per-class routing (``NAR_ETOP2_ADOPT_CLASSES`` = {A, B, NEW, other}): the
override is applied ONLY for those four NAR sub-classes, which were net
positive for top1 + place2 in offline evaluation. Classes C, OP, MUKATSU
showed a place2 regression and stay on the pure XGB base.

Override is implemented via score injection: the XGB#2 horse receives
``max(xgb_scores) + 1.0``, the XGB#1 horse receives ``max(xgb_scores) + 0.5``,
all other horses keep their original XGB score — mirroring the JRA injection
so the promoted pair lands at rank-1 / rank-2 while rank-3+ are preserved.

Reference: docs/finish-position-accuracy/per-class/nar/place-preserving-override.md
Verdict: ADOPT for {A, B, NEW, other} (2026-06-19)
"""

from __future__ import annotations

from collections.abc import Sequence

from predict_lib.model_meta import NAR_ETOP2_ADOPT_CLASSES


def apply_nar_etop2_scores(
    xgb_scores: Sequence[float],
    cb_scores: Sequence[float],
    nar_class: str | None,
) -> list[float]:
    """Apply the NAR E-top2 override and return modified scores for ranking.

    Pure function: given per-horse XGB (base) scores and CB (override) scores
    for one race plus the normalised NAR class code, returns a new score list
    where rank-1 and rank-2 may be swapped when the E-top2 condition is met
    AND the class is in ``NAR_ETOP2_ADOPT_CLASSES``.

    The returned scores are intended only for ranking (higher = better). The
    absolute values are not meaningful — they are constructed to guarantee the
    correct ordering by injecting ``max(xgb) + 1.0`` and ``max(xgb) + 0.5`` for
    the promoted pair.

    Parameters
    ----------
    xgb_scores:
        XGBoost (production NAR base) raw scores per horse, in entry order.
        Higher = better rank.
    cb_scores:
        CatBoost CB-2013 (override) raw scores per horse, in the same entry
        order as ``xgb_scores``.
    nar_class:
        The normalised NAR sub-class code (``NEW`` / ``MUKATSU`` / ``C`` /
        ``B`` / ``A`` / ``OP`` / ``other``) as produced by
        ``per_class.normalize_class_code``. ``None`` is treated as "unknown"
        and never triggers the override (safety).

    Returns
    -------
    list[float]
        Scores in the same entry order. Equal to ``list(xgb_scores)`` when:
        - ``nar_class`` is not in ``NAR_ETOP2_ADOPT_CLASSES`` (incl. ``None``),
        - the race has fewer than 2 horses,
        - CB#1 == XGB#1 (already agree), or
        - CB#1 ∈ XGB#3+ (would disturb place3 — not eligible).
        Otherwise returns the swapped scores when CB#1 == XGB#2.
    """
    if nar_class not in NAR_ETOP2_ADOPT_CLASSES:
        return list(xgb_scores)

    n = len(xgb_scores)
    if n < 2:
        return list(xgb_scores)

    # Identify XGB rank-1 and rank-2 indices (higher score = better)
    sorted_by_xgb = sorted(range(n), key=lambda i: -xgb_scores[i])
    xgb_rank1_idx = sorted_by_xgb[0]
    xgb_rank2_idx = sorted_by_xgb[1]

    # Identify CB rank-1 index (higher score = better)
    cb_rank1_idx = max(range(n), key=lambda i: cb_scores[i])

    # E-top2 condition: CB#1 matches XGB#2 exactly
    if cb_rank1_idx != xgb_rank2_idx:
        return list(xgb_scores)

    # Override: promote XGB#2 to rank-1, demote XGB#1 to rank-2
    xgb_max = max(xgb_scores)
    result = list(xgb_scores)
    result[xgb_rank2_idx] = xgb_max + 1.0  # XGB#2 -> rank-1 (highest score)
    result[xgb_rank1_idx] = xgb_max + 0.5  # XGB#1 -> rank-2
    # XGB#3+ keep original scores — their relative order and rank-3 are preserved
    return result


def is_nar_etop2_override_active(
    xgb_scores: Sequence[float],
    cb_scores: Sequence[float],
    nar_class: str | None,
) -> bool:
    """Return True when the NAR E-top2 override would change the rank-1 horse.

    Useful for logging / smoke-test verification that the override fires in
    some races. Equivalent to checking whether :func:`apply_nar_etop2_scores`
    changes the argmax, but without allocating the full score list.
    """
    if nar_class not in NAR_ETOP2_ADOPT_CLASSES:
        return False
    n = len(xgb_scores)
    if n < 2:
        return False
    sorted_by_xgb = sorted(range(n), key=lambda i: -xgb_scores[i])
    xgb_rank2_idx = sorted_by_xgb[1]
    cb_rank1_idx = max(range(n), key=lambda i: cb_scores[i])
    return cb_rank1_idx == xgb_rank2_idx
