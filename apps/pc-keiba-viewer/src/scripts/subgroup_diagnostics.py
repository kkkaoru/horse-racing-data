"""Subgroup NDCG@3 diagnostics for finish position predictions."""

from __future__ import annotations

from typing import Final, TypedDict

import numpy as np
import pandas as pd

KYORI_BAND_SPRINT_UPPER: Final[int] = 1200   # sprint: [0, 1200)
KYORI_BAND_MILE_UPPER: Final[int] = 1600     # mile:   [1200, 1600)
KYORI_BAND_INTERMEDIATE_UPPER: Final[int] = 2000  # intermediate: [1600, 2000)
KYORI_BAND_LONG_UPPER: Final[int] = 2400     # long:   [2000, 2400);  extended: [2400, ∞)

JRA_TURF_CODES: Final[frozenset[str]] = frozenset(str(i) for i in range(10, 23))
JRA_DIRT_CODES: Final[frozenset[str]] = frozenset(str(i) for i in range(23, 30))
BANEI_KEIBAJO_CODE: Final[str] = "83"

RELEVANCE_MAP: Final[dict[int, float]] = {1: 3.0, 2: 2.0, 3: 1.0}


class SubgroupMetrics(TypedDict):
    subgroup: str
    race_count: int
    ndcg_at_3: float
    top1_accuracy: float
    top3_box_accuracy: float


def get_source_label(source: str, keibajo_code: str) -> str:
    if keibajo_code == BANEI_KEIBAJO_CODE:
        return "banei"
    if source == "jra":
        return "jra"
    return "nar"


def get_surface_label(track_code: str, source_label: str) -> str:
    if source_label != "jra":
        return "dirt"
    if track_code in JRA_TURF_CODES:
        return "turf"
    if track_code in JRA_DIRT_CODES:
        return "dirt"
    return "other"


def get_distance_band(kyori: int) -> str:
    if kyori < KYORI_BAND_SPRINT_UPPER:
        return "sprint"
    if kyori < KYORI_BAND_MILE_UPPER:
        return "mile"
    if kyori < KYORI_BAND_INTERMEDIATE_UPPER:
        return "intermediate"
    if kyori < KYORI_BAND_LONG_UPPER:
        return "long"
    return "extended"


def make_subgroup_key(source_label: str, surface: str, distance_band: str) -> str:
    return f"{source_label}_{surface}_{distance_band}"


def assign_subgroup_keys(df: pd.DataFrame) -> pd.Series:
    """Return a Series of subgroup key strings aligned with df's index."""
    source_labels = df.apply(
        lambda r: get_source_label(str(r["source"]), str(r["keibajo_code"])),
        axis=1,
    )
    surfaces = df.apply(
        lambda r: get_surface_label(str(r["track_code"]), source_labels[r.name]),
        axis=1,
    )
    bands = df["kyori"].apply(lambda k: get_distance_band(int(k)))
    return pd.Series(
        [
            make_subgroup_key(src, surf, band)
            for src, surf, band in zip(source_labels, surfaces, bands, strict=True)
        ],
        index=df.index,
    )


def _dcg_at_3(sorted_finish_positions: list[int]) -> float:
    dcg = 0.0
    for rank_idx, finish_pos in enumerate(sorted_finish_positions[:3], start=1):
        rel = RELEVANCE_MAP.get(finish_pos, 0.0)
        dcg += rel / np.log2(rank_idx + 1)
    return dcg


def compute_race_ndcg(group: pd.DataFrame) -> float:
    valid_group = group.dropna(subset=["predicted_rank"])
    sorted_group = valid_group.sort_values("predicted_rank")
    finish_positions = sorted_group["finish_position"].tolist()
    dcg = _dcg_at_3(finish_positions)
    ideal_relevances = sorted(
        (RELEVANCE_MAP.get(int(fp), 0.0) for fp in group["finish_position"] if pd.notna(fp)),
        reverse=True,
    )[:3]
    ideal_dcg = sum(rel / np.log2(i + 2) for i, rel in enumerate(ideal_relevances))
    return dcg / ideal_dcg if ideal_dcg > 0 else 0.0


def compute_race_top1(group: pd.DataFrame) -> bool:
    predicted_rank = group["predicted_rank"]
    if predicted_rank.isna().all():
        return False
    best = group.loc[predicted_rank.idxmin()]
    fp = best["finish_position"]
    return pd.notna(fp) and int(fp) == 1


def compute_race_top3_box(group: pd.DataFrame) -> bool:
    predicted_top3 = set(
        group.nsmallest(3, "predicted_rank")["ketto_toroku_bango"].tolist()
    )
    actual_top3 = set(
        group.nsmallest(3, "finish_position")["ketto_toroku_bango"].tolist()
    )
    return predicted_top3 == actual_top3


def evaluate_subgroup(joined: pd.DataFrame) -> SubgroupMetrics:
    """Compute metrics for a single subgroup slice (already filtered)."""
    ndcg_scores: list[float] = []
    top1_hits = 0
    top3_hits = 0
    race_count = 0
    for _race_id, group in joined.groupby("race_id"):
        race_count += 1
        ndcg_scores.append(compute_race_ndcg(group))
        if compute_race_top1(group):
            top1_hits += 1
        if (
            group["finish_position"].notna().sum() >= 3
            and group["predicted_rank"].notna().sum() >= 3
            and compute_race_top3_box(group)
        ):
            top3_hits += 1
    safe = max(race_count, 1)
    return SubgroupMetrics(
        subgroup="",
        race_count=race_count,
        ndcg_at_3=float(np.mean(ndcg_scores)) if ndcg_scores else 0.0,
        top1_accuracy=top1_hits / safe,
        top3_box_accuracy=top3_hits / safe,
    )


def compute_subgroup_diagnostics(
    predictions: pd.DataFrame,
    ground_truth: pd.DataFrame,
) -> list[SubgroupMetrics]:
    """Break down prediction quality by subgroup.

    Parameters
    ----------
    predictions:
        DataFrame with columns (race_id, ketto_toroku_bango, predicted_rank).
    ground_truth:
        DataFrame with columns (race_id, ketto_toroku_bango, finish_position,
        source, keibajo_code, track_code, kyori).

    Returns
    -------
    list[SubgroupMetrics] sorted by subgroup key ascending.
    """
    joined = predictions.merge(
        ground_truth[
            ["race_id", "ketto_toroku_bango", "finish_position",
             "source", "keibajo_code", "track_code", "kyori"]
        ],
        on=["race_id", "ketto_toroku_bango"],
        how="inner",
    )
    if joined.empty:
        return []
    joined = joined.copy()
    joined["_subgroup"] = assign_subgroup_keys(joined)
    results: list[SubgroupMetrics] = []
    for subgroup_key, group_df in joined.groupby("_subgroup"):
        metrics = evaluate_subgroup(group_df)
        metrics["subgroup"] = str(subgroup_key)
        results.append(metrics)
    return sorted(results, key=lambda m: m["subgroup"])
