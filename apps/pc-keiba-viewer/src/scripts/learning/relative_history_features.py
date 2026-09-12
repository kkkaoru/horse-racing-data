"""Lagged within-race speed contrasts, computed in memory without scratch files."""

from __future__ import annotations

from typing import Final

import duckdb
import polars as pl

RELATIVE_SPEED_FEATURES: Final[tuple[str, ...]] = (
    "past_relative_speed_mean",
    "past_relative_speed_365d",
    "past_relative_speed_28d",
)
RESERVED_COLUMNS: Final[frozenset[str]] = frozenset(
    (*RELATIVE_SPEED_FEATURES, "_context_speed", "_context_z")
)
RELATIVE_HISTORY_SQL: Final[str] = """
with speeds as (
  select *, case when finish>0 and distance_m>0 and clock_seconds>0
    then distance_m / clock_seconds end as _context_speed
  from input_features
), contrasts as (
  select *, (_context_speed - avg(_context_speed) over (partition by race_id))
    / nullif(stddev_pop(_context_speed) over (partition by race_id),0) as _context_z
  from speeds
)
select * exclude (_context_speed,_context_z),
  avg(_context_z) over horse_past as past_relative_speed_mean,
  avg(_context_z) over horse_year as past_relative_speed_365d,
  avg(_context_z) over horse_month as past_relative_speed_28d
from contrasts
window
  horse_past as (partition by horse_id order by observed_date
    range between unbounded preceding and interval 1 day preceding),
  horse_year as (partition by horse_id order by observed_date
    range between interval 365 day preceding and interval 1 day preceding),
  horse_month as (partition by horse_id order by observed_date
    range between interval 28 day preceding and interval 1 day preceding)
"""


def add_relative_history_features(frame: pl.DataFrame) -> pl.DataFrame:
    """Retain all cross-venue history until the caller selects its training scope.

    No current-race speed contrast is returned. Zero-variance/single-observation
    races yield null contrasts, not invented performance. DuckDB spill is
    disabled: an oversized operation fails rather than creating scratch files.
    """
    if RESERVED_COLUMNS.intersection(frame.columns):
        raise ValueError("Input already contains reserved relative-speed columns")
    if frame.select("race_id", "horse_id").is_duplicated().any():
        raise ValueError("Each race/horse observation must be unique")
    with duckdb.connect(
        config={"temp_directory": "", "memory_limit": "4GB", "threads": 2}
    ) as connection:
        connection.register("input_features", frame)
        return connection.execute(RELATIVE_HISTORY_SQL).pl()
