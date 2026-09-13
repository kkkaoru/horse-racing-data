"""Restore margin features without hiding incomplete historical windows."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
from numpy.typing import NDArray

FloatArray = NDArray[np.float64]
MARGIN_FEATURE_NAMES: tuple[str, ...] = (
    "speed_index_avg_5",
    "speed_index_best_5",
    "last_race_margin_to_winner",
    "field_strength_avg_speed",
    "field_strength_top3_speed",
    "speed_index_avg_5_diff_from_race_avg",
    "speed_index_avg_5_rank_in_race",
    "speed_index_best_5_rank_in_race",
)


def restore_margin_features(
    race_ids: Sequence[str],
    *,
    mean_margin: FloatArray,
    best_margin: FloatArray,
    last_margin: FloatArray,
    incomplete_window: NDArray[np.generic],
) -> dict[str, FloatArray]:
    """Match canonical race aggregates, but withhold fields with truncated histories.

    An incomplete last-five window invalidates its mean/best and all race-relative
    aggregates, not its independently known most recent margin. Genuine absence
    of any prior start is represented by NaN with incomplete_window=False.
    """
    count = len(race_ids)
    for values in (mean_margin, best_margin, last_margin):
        if values.ndim != 1 or values.size != count or np.isinf(values).any():
            raise ValueError("Margins must be aligned one-dimensional finite-or-NaN arrays")
    if incomplete_window.shape != (count,) or incomplete_window.dtype != np.dtype(np.bool_):
        raise ValueError("Incomplete-window flags must be aligned boolean values")
    flags = np.asarray(incomplete_window, dtype=np.bool_)
    mean = mean_margin.copy()
    best = best_margin.copy()
    mean[flags] = np.nan
    best[flags] = np.nan
    field_mean = np.full(count, np.nan, dtype=np.float64)
    field_top = np.full(count, np.nan, dtype=np.float64)
    mean_ranks = np.full(count, np.nan, dtype=np.float64)
    best_ranks = np.full(count, np.nan, dtype=np.float64)
    groups: dict[str, list[int]] = {}
    for index, race_id in enumerate(race_ids):
        if not race_id:
            raise ValueError("Race identities must not be empty")
        groups.setdefault(race_id, []).append(index)
    for indices in groups.values():
        if flags[indices].any():
            continue
        means = mean[indices]
        bests = best[indices]
        finite_means = means[~np.isnan(means)]
        finite_bests = bests[~np.isnan(bests)]
        mean_ranks[indices] = np.searchsorted(np.sort(finite_means), means) + 1
        best_ranks[indices] = np.searchsorted(np.sort(finite_bests), bests) + 1
        if finite_means.size:
            field_mean[indices] = finite_means.mean()
        if finite_bests.size:
            field_top[indices] = np.sort(finite_bests)[:3].mean()
    return dict(
        zip(
            MARGIN_FEATURE_NAMES,
            (
                mean,
                best,
                last_margin.copy(),
                field_mean,
                field_top,
                mean - field_mean,
                mean_ranks,
                best_ranks,
            ),
            strict=True,
        )
    )
