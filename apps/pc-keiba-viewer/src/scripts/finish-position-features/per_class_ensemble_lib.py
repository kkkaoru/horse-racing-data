"""Pure-function library for per-class ensemble optimization (iter 23).

Used by:
- optimize-per-class-ensemble.py (Optuna driver)
- blend-per-class-ensemble.py (offline blender, future)
- eval-per-class-ensemble.py (manifest evaluator, future)

All public functions are deterministic. Parquet reading is allowed for the
``load_*`` helpers; everything else is pure pandas/numpy / Python.
"""

from __future__ import annotations

import glob
import math
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Iterable


KNOWN_CLASS_CODES: Final[frozenset[str]] = frozenset(
    {"005", "010", "016", "703", "701"},
)
OTHER_CLASS_LABEL: Final[str] = "other"

WILSON_Z_SCORE: Final[float] = 1.96  # 95% CI

_SINGLE_RACE_SCORE: Final[float] = 0.5
_MIN_RACE_SIZE_FOR_RANK: Final[int] = 1
_PREDICTED_SCORE_COL: Final[str] = "predicted_score"
_NORMALIZED_SCORE_COL: Final[str] = "normalized_score"
_RACE_ID_COL: Final[str] = "race_id"
_HORSE_COL: Final[str] = "ketto_toroku_bango"
_ACTUAL_COL: Final[str] = "actual_finish_position"
_BLENDED_COL: Final[str] = "blended_score"
_TOPK_BOX_SIZE: Final[int] = 3
_DEFAULT_BLENDED_PLACE_K_BOUNDS: Final[tuple[int, int]] = (1, 99)
_PLACE_METRIC: Final[str] = "place"
_BOX_TOP3_METRIC: Final[str] = "box_top3"


def is_other_class(class_code: str) -> bool:
    """Return True iff ``class_code`` is the ``other`` catch-all label."""
    return class_code == OTHER_CLASS_LABEL


def class_filter_mask(
    kyoso_joken_codes: pd.Series, class_code: str,
) -> pd.Series:
    """Boolean mask selecting rows matching ``class_code``.

    For 'other': NOT IN ``KNOWN_CLASS_CODES``, also includes NaN/null.
    For a named class (e.g. '005'): exact string match.
    """
    if is_other_class(class_code):
        is_known = kyoso_joken_codes.isin(KNOWN_CLASS_CODES)
        is_null = kyoso_joken_codes.isna()
        return cast(pd.Series, (~is_known) | is_null)
    return cast(pd.Series, kyoso_joken_codes == class_code)


def _read_year_parquet(parquet_root: Path, year: int) -> pd.DataFrame | None:
    """Read the (single-or-multi-file) parquet partition for ``year``.

    Returns None if no parquet files are found under ``race_year=<year>``.
    """
    year_dir = parquet_root / f"race_year={year}"
    if not year_dir.exists():
        return None
    files = sorted(glob.glob(str(year_dir / "*.parquet")))
    if not files:
        return None
    parts = [pd.read_parquet(f) for f in files]
    if len(parts) == 1:
        return parts[0]
    return pd.concat(parts, ignore_index=True)


def load_class_predictions(
    parquet_root: Path,
    class_code: str,
    years: "Iterable[int]",
    pg_class_map_df: pd.DataFrame,
) -> pd.DataFrame:
    """Load predictions for ``years``, filter to ``class_code``.

    ``parquet_root`` must point at a directory whose direct children are
    ``race_year=<year>/*.parquet`` (matching the orchestrator output layout).

    ``pg_class_map_df`` must have columns ``(race_id, kyoso_joken_code)``.

    Returns a DataFrame with columns
    ``(race_id, ketto_toroku_bango, predicted_score, actual_finish_position)``.
    Empty DataFrame (with the same columns) when no rows survive the filter.
    """
    output_cols = [
        _RACE_ID_COL, _HORSE_COL, _PREDICTED_SCORE_COL, _ACTUAL_COL,
    ]
    frames: list[pd.DataFrame] = []
    for year in years:
        chunk = _read_year_parquet(parquet_root, int(year))
        if chunk is None or chunk.empty:
            continue
        frames.append(chunk)
    if not frames:
        return pd.DataFrame(columns=output_cols)
    joined = pd.concat(frames, ignore_index=True)
    keyed_map = pg_class_map_df[[_RACE_ID_COL, "kyoso_joken_code"]].drop_duplicates(
        subset=[_RACE_ID_COL],
    )
    merged = joined.merge(keyed_map, on=_RACE_ID_COL, how="left")
    mask = class_filter_mask(merged["kyoso_joken_code"], class_code)
    filtered = merged[mask]
    return filtered[output_cols].reset_index(drop=True)


def normalize_within_race(df: pd.DataFrame) -> pd.DataFrame:
    """Attach ``normalized_score`` to each row, ranked within race.

    Sort by ``predicted_score`` descending within race; tiebreak by
    ``ketto_toroku_bango`` ascending (deterministic). For a single-entry
    race, ``normalized_score`` is ``0.5``; otherwise
    ``(N - 1 - rank_idx) / (N - 1)`` for ``rank_idx in [0..N-1]``.

    The returned DataFrame is sorted (race_id asc, then within-race rank);
    callers should rejoin on ``(race_id, ketto_toroku_bango)`` if they need
    the original input order.
    """
    if df.empty:
        out = df.copy()
        out[_NORMALIZED_SCORE_COL] = pd.Series([], dtype=np.float64)
        return out
    ordered = df.sort_values(
        by=[_RACE_ID_COL, _PREDICTED_SCORE_COL, _HORSE_COL],
        ascending=[True, False, True],
        kind="stable",
    ).reset_index(drop=True)
    grouped = ordered.groupby(_RACE_ID_COL, sort=False)
    rank_idx = grouped.cumcount().to_numpy(dtype=np.float64)
    race_size = grouped[_RACE_ID_COL].transform("size").to_numpy(dtype=np.float64)
    denom = race_size - 1.0
    safe_denom = np.where(denom > 0.0, denom, 1.0)
    normalized = np.where(
        race_size > _MIN_RACE_SIZE_FOR_RANK,
        (denom - rank_idx) / safe_denom,
        _SINGLE_RACE_SCORE,
    )
    ordered[_NORMALIZED_SCORE_COL] = normalized
    return ordered


def _validate_blend_inputs(members: list[pd.DataFrame], weights: list[float]) -> None:
    if not members:
        raise ValueError("blend_normalized requires at least one member")
    if len(members) != len(weights):
        raise ValueError(
            f"members ({len(members)}) and weights ({len(weights)}) length mismatch",
        )


def _seed_first_member(first: pd.DataFrame, weight: float) -> pd.DataFrame:
    seed = pd.DataFrame({
        _RACE_ID_COL: first[_RACE_ID_COL].to_numpy(),
        _HORSE_COL: first[_HORSE_COL].to_numpy(),
        _NORMALIZED_SCORE_COL: weight * first[_NORMALIZED_SCORE_COL].to_numpy(),
        _ACTUAL_COL: first[_ACTUAL_COL].to_numpy(),
    })
    return seed.rename(columns={_NORMALIZED_SCORE_COL: _BLENDED_COL})


def _fold_next_member(
    acc: pd.DataFrame, member: pd.DataFrame, weight: float,
) -> pd.DataFrame:
    join_cols = [_RACE_ID_COL, _HORSE_COL]
    member_slice = member[[*join_cols, _NORMALIZED_SCORE_COL]].rename(
        columns={_NORMALIZED_SCORE_COL: "_member_norm"},
    )
    merged = acc.merge(member_slice, on=join_cols, how="inner")
    merged[_BLENDED_COL] = merged[_BLENDED_COL] + weight * merged["_member_norm"]
    return merged.drop(columns=["_member_norm"])


def blend_normalized(
    members: list[pd.DataFrame], weights: list[float],
) -> pd.DataFrame:
    """Inner-join all normalized members on (race_id, ketto_toroku_bango).

    Each member must carry columns
    ``(race_id, ketto_toroku_bango, normalized_score, actual_finish_position)``.
    Returns a DataFrame with
    ``(race_id, ketto_toroku_bango, blended_score, actual_finish_position)``.
    """
    _validate_blend_inputs(members, weights)
    acc = _seed_first_member(members[0], weights[0])
    for member, weight in zip(members[1:], weights[1:], strict=True):
        acc = _fold_next_member(acc, member, weight)
    return acc.reset_index(drop=True)


def _rank_blended_within_race(blended: pd.DataFrame) -> pd.DataFrame:
    """Return ``blended`` sorted within each race by blended_score desc."""
    return blended.sort_values(
        by=[_RACE_ID_COL, _BLENDED_COL, _HORSE_COL],
        ascending=[True, False, True],
        kind="stable",
    ).reset_index(drop=True)


def compute_top1(blended: pd.DataFrame) -> float:
    """Fraction of races whose top-1 blended entry has ``actual==1``.

    Races with no rows are skipped from the denominator. A race whose top-1
    actual value is missing (NaN) contributes a miss.
    """
    if blended.empty:
        return 0.0
    ranked = _rank_blended_within_race(blended)
    top1 = ranked.groupby(_RACE_ID_COL, sort=False).head(1)
    actual = pd.to_numeric(top1[_ACTUAL_COL], errors="coerce")
    hits = int((actual == 1).sum())
    total = int(top1.shape[0])
    return hits / float(total)


def _compute_place_at_k(ranked: pd.DataFrame, k: int) -> float:
    """``place`` metric at depth ``k``: actual at position ``k`` equals ``k``."""
    grouped = ranked.groupby(_RACE_ID_COL, sort=False)
    head = grouped.head(k)
    sizes = grouped.size()
    eligible_races = sizes[sizes >= k].index.tolist()
    if not eligible_races:
        return 0.0
    eligible_head = head[head[_RACE_ID_COL].isin(eligible_races)]
    kth_row = eligible_head.groupby(_RACE_ID_COL, sort=False).tail(1)
    actual = pd.to_numeric(kth_row[_ACTUAL_COL], errors="coerce")
    hits = int((actual == k).sum())
    total = len(eligible_races)
    return hits / float(total)


def _compute_box_top3(ranked: pd.DataFrame) -> float:
    """``box_top3``: top-3 predicted rows are exactly the actual top-3 set."""
    grouped = ranked.groupby(_RACE_ID_COL, sort=False)
    sizes = grouped.size()
    eligible_races = sizes[sizes >= _TOPK_BOX_SIZE].index.tolist()
    if not eligible_races:
        return 0.0
    eligible = ranked[ranked[_RACE_ID_COL].isin(eligible_races)]
    top3_actuals = eligible.groupby(_RACE_ID_COL, sort=False).head(_TOPK_BOX_SIZE)
    actual_series = pd.to_numeric(top3_actuals[_ACTUAL_COL], errors="coerce")
    top3_actuals = top3_actuals.assign(_actual_num=actual_series)
    matched = top3_actuals.groupby(_RACE_ID_COL, sort=False)["_actual_num"].apply(
        lambda values: set(int(v) for v in values.dropna()) == {1, 2, 3},
    )
    hits = int(matched.sum())
    total = len(eligible_races)
    return hits / float(total)


def compute_topk_metric(blended: pd.DataFrame, k: int, metric: str) -> float:
    """Compute one of the supported top-k metrics.

    ``metric == 'place'``: fraction of races whose predicted rank-``k`` entry has
    ``actual_finish_position == k``. Races shorter than ``k`` are excluded.

    ``metric == 'box_top3'``: fraction of races whose top-3 predicted entries
    coincide (as a set, ignoring order) with the actual {1, 2, 3} podium.
    """
    if blended.empty:
        return 0.0
    lo, hi = _DEFAULT_BLENDED_PLACE_K_BOUNDS
    if metric == _PLACE_METRIC and not (lo <= k <= hi):
        raise ValueError(f"place metric requires k in [{lo}, {hi}], got {k}")
    ranked = _rank_blended_within_race(blended)
    if metric == _PLACE_METRIC:
        return _compute_place_at_k(ranked, k)
    if metric == _BOX_TOP3_METRIC:
        return _compute_box_top3(ranked)
    raise ValueError(
        f"compute_topk_metric: unknown metric {metric!r} (want 'place' or 'box_top3')",
    )


def _softmax(logits: list[float]) -> list[float]:
    """Numerically stable softmax over a non-empty finite logit vector.

    Caller (``simplex_softmax``) guarantees ``logits`` is non-empty. After
    shifting by the max, at least one element is ``exp(0) = 1``, so the
    denominator is strictly positive — no defensive denom guard needed.
    """
    arr = np.asarray(logits, dtype=np.float64)
    shifted = arr - arr.max()
    exps = np.exp(shifted)
    denom = exps.sum()
    return cast(list[float], (exps / denom).tolist())


def simplex_softmax(
    z: list[float], min_anchor_idx: int, min_anchor_value: float,
) -> list[float]:
    """Convert R^n logits to simplex weights with an enforced anchor minimum.

    Algorithm:
    1. ``weights = softmax(z)``.
    2. If ``weights[min_anchor_idx] < min_anchor_value``: clamp the anchor
       to ``min_anchor_value`` and rescale the remaining weights so the
       total still sums to 1.0.

    Edge cases:
    - Empty ``z`` → ``[]``.
    - Single-member ``z`` (the anchor is the only member) → ``[1.0]``.
    - All-zero ``z`` after softmax (impossible normally; defensive) →
      uniform weights.
    """
    if not z:
        return []
    if len(z) == 1:
        return [1.0]
    if min_anchor_idx < 0 or min_anchor_idx >= len(z):
        raise ValueError(
            f"min_anchor_idx out of range: got {min_anchor_idx}, n={len(z)}",
        )
    if not (0.0 <= min_anchor_value <= 1.0):
        raise ValueError(
            f"min_anchor_value must be in [0, 1], got {min_anchor_value}",
        )
    weights = _softmax(z)
    if weights[min_anchor_idx] >= min_anchor_value:
        return weights
    # n >= 2 + softmax over finite logits ⇒ all weights strictly positive ⇒
    # others_sum > 0; no defensive guard needed.
    others_sum = sum(weights) - weights[min_anchor_idx]
    scale = (1.0 - min_anchor_value) / others_sum
    rescaled = [w * scale for w in weights]
    rescaled[min_anchor_idx] = min_anchor_value
    return rescaled


def wilson_lower_bound(p: float, n: int, z: float = WILSON_Z_SCORE) -> float:
    """Wilson score lower bound for a binomial proportion.

    Reference: Wilson (1927). Returns 0.0 for n=0 (defensive — caller should
    avoid hitting this branch). Clamps p into [0, 1] before computation.
    """
    if n == 0:
        return 0.0
    p_clamped = max(0.0, min(1.0, p))
    z_sq = z * z
    denom = 1.0 + z_sq / float(n)
    center = p_clamped + z_sq / (2.0 * float(n))
    margin = z * math.sqrt(
        (p_clamped * (1.0 - p_clamped) + z_sq / (4.0 * float(n))) / float(n),
    )
    lower = (center - margin) / denom
    return max(0.0, lower)


def _assign_within_race_rank(df: pd.DataFrame, score_col: str) -> pd.DataFrame:
    """Attach ``_within_rank`` (1-based) per race, sorted by ``score_col`` desc.

    Tiebreak: ``ketto_toroku_bango`` ascending (matches ``_rank_blended_within_race``).
    Returns the DataFrame sorted race_id / score desc / horse asc with an added
    integer column ``_within_rank`` starting from 1 per race group.
    """
    ordered = df.sort_values(
        by=[_RACE_ID_COL, score_col, _HORSE_COL],
        ascending=[True, False, True],
        kind="stable",
    ).reset_index(drop=True)
    ordered["_within_rank"] = ordered.groupby(_RACE_ID_COL, sort=False).cumcount() + 1
    return ordered


def compute_fukusho_2p(df: pd.DataFrame) -> float:
    """Set-membership fukusho-2p: mean over races of (≥2 of predicted top-3 hit actual top-3).

    Per race: 1 if |predicted_top3 ∩ actual_top3| ≥ 2 else 0, where
    predicted_top3 is the set of horses with the 3 highest blended_scores
    (tiebreak: ketto_toroku_bango ascending) and actual_top3 is the set of
    horses whose actual_finish_position ≤ 3.

    Races with < 3 finishers are included in the denominator (they can score 0
    or 1 depending on how many of their ≤3-field entries hit). Consistent with
    the I1 root-cause probe definition (fukusho_2p = fukusho_cnt >= 2 where
    fukusho_cnt counts predicted_rank<=3 rows with actual_finish_position<=3).

    Returns 0.0 for empty DataFrame.
    """
    if df.empty:
        return 0.0
    ranked = _assign_within_race_rank(df, _BLENDED_COL)
    # Keep predicted top-3 rows per race
    top3 = ranked[ranked["_within_rank"] <= _TOPK_BOX_SIZE].copy()
    actual_num = pd.to_numeric(top3[_ACTUAL_COL], errors="coerce")
    top3 = top3.assign(_actual_num=actual_num)
    # Per race: count predicted top-3 that actually finished top-3
    top3_hit = top3[top3["_actual_num"] <= _TOPK_BOX_SIZE]
    fukusho_cnt = top3_hit.groupby(_RACE_ID_COL, sort=False).size()
    # Races not in fukusho_cnt have count 0; reindex to all races
    all_races = ranked[_RACE_ID_COL].unique()
    fukusho_cnt = fukusho_cnt.reindex(all_races, fill_value=0)
    hits = int((fukusho_cnt >= 2).sum())
    total = len(all_races)
    return hits / float(total)


def compute_rentai_hit(df: pd.DataFrame) -> float:
    """Set-membership rentai-hit: mean over races of (predicted top-2 == actual top-2 set).

    Per race: 1 if the predicted top-2 SET equals the actual top-2 set (both
    predicted top-2 horses finished in the actual top-2, unordered) else 0.
    Predicted top-2 is determined by the 2 highest blended_scores (tiebreak:
    ketto_toroku_bango ascending). Consistent with the I1 root-cause probe
    definition: rentai_hit = actual_top2.issubset(pred_top2) and len(pred_top2) >= 2.

    Races with fewer than 2 finishers are included in the denominator and
    score 0. Returns 0.0 for empty DataFrame.
    """
    if df.empty:
        return 0.0
    ranked = _assign_within_race_rank(df, _BLENDED_COL)
    all_races = ranked[_RACE_ID_COL].unique()
    total = len(all_races)
    # Predicted top-2 rows
    top2 = ranked[ranked["_within_rank"] <= 2].copy()
    actual_num_top2 = pd.to_numeric(top2[_ACTUAL_COL], errors="coerce")
    top2 = top2.assign(_actual_num=actual_num_top2)
    # Per race: count predicted top-2 rows with valid actual value
    pred_top2_size = (
        top2.dropna(subset=["_actual_num"])
        .groupby(_RACE_ID_COL, sort=False)
        .size()
        .reindex(all_races, fill_value=0)
    )
    # Per race: count predicted top-2 rows whose actual finish ≤ 2
    top2_in_actual = top2[top2["_actual_num"] <= 2]
    pred_in_actual2_cnt = (
        top2_in_actual.groupby(_RACE_ID_COL, sort=False)
        .size()
        .reindex(all_races, fill_value=0)
    )
    # Per race: count actual top-2 horses (actual_finish_position ≤ 2)
    actual_num_all = pd.to_numeric(ranked[_ACTUAL_COL], errors="coerce")
    ranked_with_actual = ranked.assign(_actual_num=actual_num_all)
    actual_top2_cnt = (
        ranked_with_actual[ranked_with_actual["_actual_num"] <= 2]
        .groupby(_RACE_ID_COL, sort=False)
        .size()
        .reindex(all_races, fill_value=0)
    )
    # rentai_hit: pred_top2_size >= 2 AND pred_in_actual2_cnt == actual_top2_cnt
    hit_mask = (pred_top2_size >= 2) & (pred_in_actual2_cnt == actual_top2_cnt)
    hits = int(hit_mask.sum())
    return hits / float(total)
