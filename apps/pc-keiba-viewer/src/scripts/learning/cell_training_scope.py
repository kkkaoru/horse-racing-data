"""Separate venue-aware evaluation cells from causal entrant-history training.

Input history contains complete race groups with string YYYYMMDD ``race_date``,
``race_id``, ``horse_id``, ``category`` and routing dimension columns. The
caller must supply all available prior races, not an already cell-filtered
store. Coverage of the upstream source must be audited separately.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl


@dataclass(frozen=True)
class CellScopeConfig:
    cell_id: str
    category: str
    dimensions: tuple[tuple[str, str], ...]
    training_cutoff: date
    evaluation_end: date
    seed_years: int = 20
    evaluation_start_year: int = 2020

    def __post_init__(self) -> None:
        if self.category not in {"nar", "ban-ei"}:
            raise ValueError("Only nar and ban-ei scopes are supported")
        names = [name for name, _ in self.dimensions]
        if len(names) != len(set(names)):
            raise ValueError("Cell dimensions must be unique")
        if self.category == "nar" and not dict(self.dimensions).get("venue"):
            raise ValueError("NAR cells require an explicit venue")
        if self.seed_years < 1 or self.training_cutoff.year <= self.seed_years:
            raise ValueError("Seed years must define a valid historical window")
        if self.evaluation_end < self.training_cutoff:
            raise ValueError("Evaluation end must not precede training cutoff")
        if not 2020 <= self.evaluation_start_year <= self.evaluation_end.year <= 2026:
            raise ValueError("Evaluation years must be within 2020 through 2026")


@dataclass(frozen=True)
class CellTrainingScope:
    config: CellScopeConfig
    seed_race_ids: pl.DataFrame
    training_rows: pl.DataFrame
    evaluation_rows: pl.DataFrame
    race_universe: pl.DataFrame


def build_cell_training_scope(
    history: pl.DataFrame, *, config: CellScopeConfig
) -> CellTrainingScope:
    """Expand pre-cutoff seed entrants to all prior races and full competitors.

    Evaluation membership is never used to select training horses. This avoids
    using future participation to construct historical validation folds. Seed
    years only bound seed membership: a seed horse's older history is retained.
    The union manifest contains every evaluation race, but training rows never
    include any outcome at or after the training cutoff.
    """
    required = {"race_id", "horse_id", "race_date", "category"} | {
        name for name, _ in config.dimensions
    }
    if not required.issubset(history.columns):
        raise ValueError(
            f"Missing scope columns: {sorted(required - set(history.columns))}"
        )
    if history.select(
        pl.any_horizontal(pl.col(sorted(required)).is_null()).any()
    ).item():
        raise ValueError("Scope identities and dimensions must not be null")
    cutoff = config.training_cutoff.strftime("%Y%m%d")
    seed_start = f"{config.training_cutoff.year - config.seed_years:04d}{cutoff[4:]}"
    cell = pl.col("category") == config.category
    for name, value in config.dimensions:
        cell = cell & (pl.col(name) == value)
    past = history.filter(pl.col("race_date") < cutoff)
    seeds = past.filter(cell & (pl.col("race_date") >= seed_start))
    horses = seeds.select("horse_id").unique()
    expanded_races = (
        past.join(horses, on="horse_id", how="semi").select("race_id").unique()
    )
    training = past.join(expanded_races, on="race_id", how="semi")
    evaluation = history.filter(
        cell
        & (pl.col("race_date") >= f"{config.evaluation_start_year}0101")
        & (pl.col("race_date") >= cutoff)
        & (pl.col("race_date") <= config.evaluation_end.strftime("%Y%m%d"))
    )
    universe = (
        pl.concat([training.select("race_id"), evaluation.select("race_id")])
        .unique()
        .sort("race_id")
    )
    return CellTrainingScope(
        config=config,
        seed_race_ids=seeds.select("race_id").unique().sort("race_id"),
        training_rows=training.sort("race_date", "race_id", "horse_id"),
        evaluation_rows=evaluation.sort("race_date", "race_id", "horse_id"),
        race_universe=universe,
    )
