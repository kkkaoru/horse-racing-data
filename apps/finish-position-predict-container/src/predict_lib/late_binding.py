"""Recompute the 5 late-binding (target-race-only) feature columns from fresh odds/bataiju.

Stage 2 of the per-race rebuild reads the early-binding history columns from the
R2 feature cache as-is and overwrites ONLY these 5 columns from the freshest
realtime odds/weight snapshot, so a re-score never re-runs the 21y Neon scan.

This is the Python twin of ``apps/finish-position-cron/src/scoring/late-binding.ts``
and must match the DuckDB feature builder
(``apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py``)
bit-for-bit.  Builder source-line provenance:

  - ``tansho_ninkijun``      rec COALESCE(rt.ninkijun_realtime, se.tansho_ninkijun)   L486-489
  - ``tansho_odds``          rec COALESCE(rt.tansho_odds_realtime, se.tansho_odds/10) L490-493
  - ``odds_score``           clamp(ln(greatest(odds, 1)) / ln(300), 0, 1)             L1481-1486
  - ``popularity_score``     clamp((ninkijun - 1) / (runner_count - 1), 0, 1)         L1475-1480
  - ``weight_diff_from_avg`` current_bataiju - weight_avg_5                           L1589

``odds_score`` / ``popularity_score`` fall back to the category training median
when the odds inputs are absent (UPCOMING races before odds publish).
``tansho_odds`` / ``tansho_ninkijun`` are NOT model features themselves (they do
not appear in the 244-feature list) — they are the raw inputs the scores are
derived from — but they are recomputed here so the cached row carries the fresh
raw odds too.  The realtime fetcher already returns the absolute odds (it does
NOT divide by 10), so the latest odds value is used directly.
"""

from __future__ import annotations

import math
from typing import Final, NamedTuple

from predict_lib.model_meta import Category

# ---------------------------------------------------------------------------
# Formula constants (mirror late-binding.ts + the DuckDB builder)
# ---------------------------------------------------------------------------

ODDS_LOG_DENOMINATOR_INPUT: Final[int] = 300
"""``ln(300)`` denominator input for odds_score (builder L1483)."""

ODDS_LOG_NUMERATOR_FLOOR: Final[float] = 1.0
"""``greatest(odds, 1)`` floor before ``ln`` (builder L1483)."""

POPULARITY_RUNNER_FLOOR: Final[int] = 1
"""Runner-count floor: popularity_score needs runner_count > 1 (builder L1476)."""

CLAMP_MIN: Final[float] = 0.0
CLAMP_MAX: Final[float] = 1.0

_ODDS_LOG_DENOMINATOR: Final[float] = math.log(ODDS_LOG_DENOMINATOR_INPUT)

# Empirical training-set medians, per category, mirroring the builder constants
# (POPULARITY_SCORE_MEDIAN_* / ODDS_SCORE_MEDIAN_*).  Ban-ei shares the NAR
# medians (both are NAR-feed races with similar odds distributions).
POPULARITY_SCORE_MEDIAN_JRA: Final[float] = 0.5
POPULARITY_SCORE_MEDIAN_NAR: Final[float] = 0.5
ODDS_SCORE_MEDIAN_JRA: Final[float] = 0.5664
ODDS_SCORE_MEDIAN_NAR: Final[float] = 0.5048

ODDS_SCORE_MEDIAN_BY_CATEGORY: Final[dict[Category, float]] = {
    "jra": ODDS_SCORE_MEDIAN_JRA,
    "nar": ODDS_SCORE_MEDIAN_NAR,
    "ban-ei": ODDS_SCORE_MEDIAN_NAR,
}
"""Category odds_score fallback median (ban-ei == nar)."""

POPULARITY_SCORE_MEDIAN_BY_CATEGORY: Final[dict[Category, float]] = {
    "jra": POPULARITY_SCORE_MEDIAN_JRA,
    "nar": POPULARITY_SCORE_MEDIAN_NAR,
    "ban-ei": POPULARITY_SCORE_MEDIAN_NAR,
}
"""Category popularity_score fallback median (ban-ei == nar)."""

# ---------------------------------------------------------------------------
# Feature column names overwritten by the late-binding recompute
# ---------------------------------------------------------------------------

ODDS_SCORE_FIELD: Final[str] = "odds_score"
POPULARITY_SCORE_FIELD: Final[str] = "popularity_score"
TANSHO_ODDS_FIELD: Final[str] = "tansho_odds"
TANSHO_NINKIJUN_FIELD: Final[str] = "tansho_ninkijun"
WEIGHT_DIFF_FROM_AVG_FIELD: Final[str] = "weight_diff_from_avg"

# Entry columns read (NOT overwritten) by the recompute: cache-derived inputs.
SHUSSO_TOSU_FIELD: Final[str] = "shusso_tosu"
WEIGHT_AVG_5_FIELD: Final[str] = "weight_avg_5"
UMABAN_FIELD: Final[str] = "umaban"


class OddsSnapshot(NamedTuple):
    """Latest realtime single-win odds snapshot for one horse.

    ``tansho_odds`` is the absolute odds (e.g. 3.5) — the realtime fetcher
    already divides the raw ``se.tansho_odds`` by 10, so callers pass the
    absolute odds here.  ``tansho_ninkijun`` is the 1-based popularity rank.
    Both are ``None`` when the odds board has not yet been published.
    """

    tansho_odds: float | None
    tansho_ninkijun: int | None


class WeightSnapshot(NamedTuple):
    """Latest realtime bataiju snapshot for one horse.

    ``current_bataiju`` is the declared bataiju (kg), available T-30..50min
    before post.  ``None`` before the weight board is published.
    """

    current_bataiju: float | None


def coerce_optional_float(value: object) -> float | None:
    """Coerce ``value`` to ``float`` or ``None`` (None / empty string -> None).

    Cache parquet cells arrive as ``str`` / ``None`` / numeric depending on the
    column dtype, so the recompute coerces defensively before arithmetic.
    """
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        try:
            return float(text)
        except ValueError:
            return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def coerce_optional_int(value: object) -> int | None:
    """Coerce ``value`` to ``int`` or ``None`` (None / empty string -> None).

    Floats are truncated via ``int(float(...))`` so a parquet cell of ``12.0``
    becomes ``12``.  Non-numeric strings collapse to ``None``.
    """
    coerced = coerce_optional_float(value)
    if coerced is None:
        return None
    return int(coerced)


def _clamp(value: float) -> float:
    """Clamp ``value`` to [0, 1] (builder greatest(0, least(1, ...)))."""
    return min(CLAMP_MAX, max(CLAMP_MIN, value))


def compute_odds_score(odds: float | None, category: Category) -> float:
    """Return ``clamp(ln(greatest(odds, 1)) / ln(300), 0, 1)`` or the category median.

    Mirrors the builder ``legacy_five_cte`` L1481-1486: when ``odds`` is present
    and strictly positive the log-ratio is clamped to [0, 1]; otherwise the
    empirical category training median is returned (UPCOMING / unpublished odds).
    """
    if odds is None or odds <= 0:
        return ODDS_SCORE_MEDIAN_BY_CATEGORY[category]
    numerator = math.log(max(odds, ODDS_LOG_NUMERATOR_FLOOR))
    return _clamp(numerator / _ODDS_LOG_DENOMINATOR)


def compute_popularity_score(
    ninkijun: int | None,
    runner_count: int | None,
    category: Category,
) -> float:
    """Return ``clamp((ninkijun - 1) / (runner_count - 1), 0, 1)`` or the median.

    Mirrors the builder ``legacy_five_cte`` L1475-1480: requires
    ``runner_count > 1`` and a non-None ``ninkijun``; otherwise the empirical
    category training median is returned.
    """
    if runner_count is None or runner_count <= POPULARITY_RUNNER_FLOOR or ninkijun is None:
        return POPULARITY_SCORE_MEDIAN_BY_CATEGORY[category]
    numerator = ninkijun - POPULARITY_RUNNER_FLOOR
    denominator = runner_count - POPULARITY_RUNNER_FLOOR
    return _clamp(numerator / denominator)


def compute_weight_diff(
    current_bataiju: float | None,
    weight_avg_5: float | None,
) -> float | None:
    """Return ``current_bataiju - weight_avg_5`` or ``None`` when either is None.

    Mirrors the builder ``weight_cte`` L1589 null-propagation: the scorer treats
    a ``None`` result as a missing feature cell (same as the builder's NULL).
    """
    if current_bataiju is None or weight_avg_5 is None:
        return None
    return current_bataiju - weight_avg_5


def apply_late_binding_to_entry(
    entry: dict[str, object],
    odds_snapshot: OddsSnapshot,
    weight_snapshot: WeightSnapshot,
    category: Category,
) -> dict[str, object]:
    """Return a copy of ``entry`` with the 5 late-binding columns recomputed.

    The early-binding columns are preserved verbatim from the cache.  The
    runner-count denominator for ``popularity_score`` is the entry's cached
    ``shusso_tosu`` (the field never changes between morning build and rescore),
    and the history side of ``weight_diff_from_avg`` is the entry's cached
    ``weight_avg_5``.  Only the latest odds / bataiju drive the recompute, so a
    ``None`` snapshot reproduces the builder's median / NULL fallback exactly.
    """
    runner_count = coerce_optional_int(entry.get(SHUSSO_TOSU_FIELD))
    weight_avg_5 = coerce_optional_float(entry.get(WEIGHT_AVG_5_FIELD))
    updated = dict(entry)
    updated[TANSHO_ODDS_FIELD] = odds_snapshot.tansho_odds
    updated[TANSHO_NINKIJUN_FIELD] = odds_snapshot.tansho_ninkijun
    updated[ODDS_SCORE_FIELD] = compute_odds_score(odds_snapshot.tansho_odds, category)
    updated[POPULARITY_SCORE_FIELD] = compute_popularity_score(
        odds_snapshot.tansho_ninkijun, runner_count, category
    )
    updated[WEIGHT_DIFF_FROM_AVG_FIELD] = compute_weight_diff(
        weight_snapshot.current_bataiju, weight_avg_5
    )
    return updated
