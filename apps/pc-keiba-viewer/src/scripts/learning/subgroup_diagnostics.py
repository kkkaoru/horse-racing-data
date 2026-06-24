"""Subgroup NDCG@3 diagnostics for finish position predictions."""

from __future__ import annotations

import math
from typing import Final, SupportsFloat, SupportsInt, TypedDict, cast

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
# so precompute once instead of recomputing log2 per element on every race.
_DISCOUNT_AT_3: Final[tuple[float, float, float]] = (
    1.0 / math.log2(2),
    1.0 / math.log2(3),
    1.0 / math.log2(4),
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


def _source_label_expr() -> pl.Expr:
    return (
        pl.when(pl.col("keibajo_code").cast(pl.Utf8) == BANEI_KEIBAJO_CODE)
        .then(pl.lit("banei"))
        .when(pl.col("source").cast(pl.Utf8) == "jra")
        .then(pl.lit("jra"))
        .otherwise(pl.lit("nar"))
    )


def _surface_expr() -> pl.Expr:
    track_str = pl.col("track_code").cast(pl.Utf8)
    return (
        pl.when(pl.col("keibajo_code").cast(pl.Utf8) == BANEI_KEIBAJO_CODE)
        .then(pl.lit("dirt"))
        .when(pl.col("source").cast(pl.Utf8) != "jra")
        .then(pl.lit("dirt"))
        .when(track_str.is_in(list(JRA_TURF_CODES)))
        .then(pl.lit("turf"))
        .when(track_str.is_in(list(JRA_DIRT_CODES)))
        .then(pl.lit("dirt"))
        .otherwise(pl.lit("other"))
    )


def _distance_band_expr() -> pl.Expr:
    kyori = pl.col("kyori").cast(pl.Int64)
    return (
        pl.when(kyori < KYORI_BAND_SPRINT_UPPER)
        .then(pl.lit("sprint"))
        .when(kyori < KYORI_BAND_MILE_UPPER)
        .then(pl.lit("mile"))
        .when(kyori < KYORI_BAND_INTERMEDIATE_UPPER)
        .then(pl.lit("intermediate"))
        .when(kyori < KYORI_BAND_LONG_UPPER)
        .then(pl.lit("long"))
        .otherwise(pl.lit("extended"))
    )


def assign_subgroup_keys(df: pl.DataFrame) -> pl.Series:
    """Return a Series of subgroup key strings aligned with df's rows."""
    result = df.select(
        pl.concat_str(
            [
                _source_label_expr(),
                pl.lit("_"),
                _surface_expr(),
                pl.lit("_"),
                _distance_band_expr(),
            ]
        ).alias("key")
    )
    return result["key"]


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


def _relevance_expr(fp: pl.Expr) -> pl.Expr:
    return (
        pl.when(fp == 1)
        .then(RELEVANCE_MAP[1])
        .when(fp == 2)
        .then(RELEVANCE_MAP[2])
        .when(fp == 3)
        .then(RELEVANCE_MAP[3])
        .otherwise(0.0)
    )


def _discount_expr(slot: pl.Expr) -> pl.Expr:
    return (
        pl.when(slot == 1)
        .then(_DISCOUNT_AT_3[0])
        .when(slot == 2)
        .then(_DISCOUNT_AT_3[1])
        .when(slot == 3)
        .then(_DISCOUNT_AT_3[2])
        .otherwise(0.0)
    )


def _ndcg_per_race(joined: pl.DataFrame) -> pl.DataFrame:
    """Per-race NDCG@3 for every race in ``joined`` (ideal_dcg<=0 -> 0.0)."""
    dcg = (
        joined.drop_nulls(subset=["predicted_rank", "finish_position"])
        .with_columns(_slot=pl.col("predicted_rank").rank("ordinal").over("race_id"))
        .filter(pl.col("_slot") <= 3)
        .with_columns(
            _contrib=_relevance_expr(pl.col("finish_position"))
            * _discount_expr(pl.col("_slot"))
        )
        .group_by("race_id")
        .agg(pl.col("_contrib").sum().alias("dcg"))
    )
    ideal = (
        joined.drop_nulls(subset=["finish_position"])
        .with_columns(_rel=_relevance_expr(pl.col("finish_position")))
        .with_columns(_slot=pl.col("_rel").rank("ordinal", descending=True).over("race_id"))
        .filter(pl.col("_slot") <= 3)
        .with_columns(_contrib=pl.col("_rel") * _discount_expr(pl.col("_slot")))
        .group_by("race_id")
        .agg(pl.col("_contrib").sum().alias("ideal_dcg"))
    )
    races = joined.select("race_id").unique()
    return (
        races.join(ideal, on="race_id", how="left")
        .join(dcg, on="race_id", how="left")
        .with_columns(
            _ndcg=pl.when(pl.col("ideal_dcg") > 0.0)
            .then(pl.col("dcg").fill_null(0.0) / pl.col("ideal_dcg"))
            .otherwise(0.0)
        )
    )


def _top1_per_race(joined: pl.DataFrame) -> pl.DataFrame:
    """Per-race top1 hit: the predicted-#1 horse finishes 1st."""
    races = joined.select("race_id").unique()
    hits = (
        joined.filter(pl.col("predicted_rank").is_not_null())
        .with_columns(_slot=pl.col("predicted_rank").rank("ordinal").over("race_id"))
        .filter(pl.col("_slot") == 1)
        .group_by("race_id")
        .agg((pl.col("finish_position").first() == 1).fill_null(value=False).alias("top1"))
    )
    return races.join(hits, on="race_id", how="left").with_columns(
        pl.col("top1").fill_null(value=False)
    )


def _top3_box_per_race(joined: pl.DataFrame) -> pl.DataFrame:
    """Per-race top3-box hit: predicted top-3 horse set == actual top-3 set.

    Only races with >=3 scored finishers and >=3 predicted horses qualify; others
    are recorded as a miss (False), matching the scalar guard.
    """
    races = joined.select("race_id").unique()
    pred_top3 = (
        joined.filter(pl.col("predicted_rank").is_not_null())
        .with_columns(
            _slot=pl.col("predicted_rank").rank("ordinal").over("race_id"),
            _n_pred=pl.len().over("race_id"),
        )
        .filter((pl.col("_slot") <= 3) & (pl.col("_n_pred") >= 3))
        .group_by("race_id")
        .agg(pl.col("ketto_toroku_bango").sort().alias("_pred_set"))
    )
    actual_top3 = (
        joined.filter(pl.col("finish_position").is_not_null())
        .with_columns(
            _slot=pl.col("finish_position").rank("ordinal").over("race_id"),
            _n_actual=pl.len().over("race_id"),
        )
        .filter((pl.col("_slot") <= 3) & (pl.col("_n_actual") >= 3))
        .group_by("race_id")
        .agg(pl.col("ketto_toroku_bango").sort().alias("_actual_set"))
    )
    return (
        races.join(pred_top3, on="race_id", how="left")
        .join(actual_top3, on="race_id", how="left")
        .with_columns(
            top3=(pl.col("_pred_set").is_not_null())
            & (pl.col("_actual_set").is_not_null())
            & (pl.col("_pred_set") == pl.col("_actual_set"))
        )
    )


def evaluate_subgroup(joined: pl.DataFrame) -> SubgroupMetrics:
    """Compute metrics for a single subgroup slice (already filtered)."""
    race_count = joined.select(pl.col("race_id").n_unique()).item()
    if race_count == 0:
        return SubgroupMetrics(
            subgroup="",
            race_count=0,
            ndcg_at_3=0.0,
            top1_accuracy=0.0,
            top3_box_accuracy=0.0,
        )
    ndcg_mean = float(cast(SupportsFloat, _ndcg_per_race(joined)["_ndcg"].mean()))
    top1_hits = int(cast(SupportsInt, _top1_per_race(joined)["top1"].sum()))
    top3_hits = int(cast(SupportsInt, _top3_box_per_race(joined)["top3"].sum()))
    return SubgroupMetrics(
        subgroup="",
        race_count=race_count,
        ndcg_at_3=ndcg_mean,
        top1_accuracy=top1_hits / race_count,
        top3_box_accuracy=top3_hits / race_count,
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
