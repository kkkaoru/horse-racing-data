"""Subgroup NDCG@3 diagnostics for finish position predictions."""

from __future__ import annotations

from typing import Final, SupportsInt, TypedDict, cast

import numpy as np
import polars as pl

KYORI_BAND_SPRINT_UPPER: Final[int] = 1200   # sprint: [0, 1200)
KYORI_BAND_MILE_UPPER: Final[int] = 1600     # mile:   [1200, 1600)
KYORI_BAND_INTERMEDIATE_UPPER: Final[int] = 2000  # intermediate: [1600, 2000)
KYORI_BAND_LONG_UPPER: Final[int] = 2400     # long:   [2000, 2400);  extended: [2400, ∞)

JRA_TURF_CODES: Final[frozenset[str]] = frozenset(str(i) for i in range(10, 23))
JRA_DIRT_CODES: Final[frozenset[str]] = frozenset(str(i) for i in range(23, 30))
BANEI_KEIBAJO_CODE: Final[str] = "83"

RELEVANCE_MAP: Final[dict[int, float]] = {1: 3.0, 2: 2.0, 3: 1.0}

# DCG@3 position discounts 1/log2(rank+1) for ranks 1, 2, 3 — constant per race,
# so precompute once instead of recomputing log2 per element on every race. Kept as
# a plain tuple: the DCG@3 dot product spans at most 3 terms, where a pure-Python
# loop beats numpy array-construction overhead (measured ~2.5x faster per call).
_DISCOUNT_AT_3: Final[tuple[float, float, float]] = (
    1.0 / float(np.log2(2)),
    1.0 / float(np.log2(3)),
    1.0 / float(np.log2(4)),
)


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


def assign_subgroup_keys(df: pl.DataFrame) -> pl.Series:
    """Return a Series of subgroup key strings aligned with df's rows."""
    sources = df["source"].cast(pl.Utf8).to_list()
    keibajo_codes = df["keibajo_code"].cast(pl.Utf8).to_list()
    track_codes = df["track_code"].cast(pl.Utf8).to_list()
    kyoris = df["kyori"].to_list()
    keys: list[str] = []
    for source, keibajo_code, track_code, kyori in zip(
        sources, keibajo_codes, track_codes, kyoris, strict=True
    ):
        source_label = get_source_label(str(source), str(keibajo_code))
        surface = get_surface_label(str(track_code), source_label)
        band = get_distance_band(int(cast(SupportsInt, kyori)))
        keys.append(make_subgroup_key(source_label, surface, band))
    return pl.Series(values=keys)


def _dcg_at_3(sorted_finish_positions: list[float]) -> float:
    return sum(
        RELEVANCE_MAP.get(int(fp), 0.0) * disc
        for fp, disc in zip(sorted_finish_positions, _DISCOUNT_AT_3)
    )


def _ideal_dcg_at_3(ideal_relevances: list[float]) -> float:
    return sum(rel * disc for rel, disc in zip(ideal_relevances, _DISCOUNT_AT_3))


def compute_race_ndcg(group: pl.DataFrame) -> float:
    valid_group = group.drop_nulls(subset=["predicted_rank", "finish_position"])
    sorted_group = valid_group.sort("predicted_rank")
    finish_positions = sorted_group["finish_position"].to_list()
    dcg = _dcg_at_3(finish_positions)
    ideal_relevances = sorted(
        (
            RELEVANCE_MAP.get(int(fp), 0.0)
            for fp in group["finish_position"].drop_nulls().to_list()
        ),
        reverse=True,
    )[:3]
    ideal_dcg = _ideal_dcg_at_3(ideal_relevances)
    return dcg / ideal_dcg if ideal_dcg > 0 else 0.0


def compute_race_top1(group: pl.DataFrame) -> bool:
    valid = group.filter(pl.col("predicted_rank").is_not_null())
    if valid.is_empty():
        return False
    best = valid.sort("predicted_rank").row(0, named=True)
    fp = best["finish_position"]
    return fp is not None and int(cast(SupportsInt, fp)) == 1


def compute_race_top3_box(group: pl.DataFrame) -> bool:
    predicted_top3 = set(
        group.filter(pl.col("predicted_rank").is_not_null())
        .sort("predicted_rank")
        .head(3)["ketto_toroku_bango"]
        .to_list()
    )
    actual_top3 = set(
        group.filter(pl.col("finish_position").is_not_null())
        .sort("finish_position")
        .head(3)["ketto_toroku_bango"]
        .to_list()
    )
    return predicted_top3 == actual_top3


def evaluate_subgroup(joined: pl.DataFrame) -> SubgroupMetrics:
    """Compute metrics for a single subgroup slice (already filtered)."""
    ndcg_scores: list[float] = []
    top1_hits = 0
    top3_hits = 0
    race_count = 0
    for (_race_id,), group in joined.group_by("race_id", maintain_order=True):
        race_count += 1
        ndcg_scores.append(compute_race_ndcg(group))
        if compute_race_top1(group):
            top1_hits += 1
        if (
            group["finish_position"].is_not_null().sum() >= 3
            and group["predicted_rank"].is_not_null().sum() >= 3
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
    predictions: pl.DataFrame,
    ground_truth: pl.DataFrame,
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
    joined = ground_truth.select(
        ["race_id", "ketto_toroku_bango", "finish_position",
         "source", "keibajo_code", "track_code", "kyori"]
    ).join(
        predictions,
        on=["race_id", "ketto_toroku_bango"],
        how="left",
    )
    if joined.is_empty():
        return []
    joined = joined.with_columns(assign_subgroup_keys(joined).alias("_subgroup"))
    results: list[SubgroupMetrics] = []
    for (subgroup_key,), group_df in joined.group_by("_subgroup", maintain_order=True):
        metrics = evaluate_subgroup(group_df)
        metrics["subgroup"] = str(subgroup_key)
        results.append(metrics)
    return sorted(results, key=lambda m: m["subgroup"])
