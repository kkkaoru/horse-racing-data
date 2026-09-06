"""Causal compact tabular frame derived from immutable local horse histories."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Protocol, cast

from .history_export import DuckDbModule, duckdb_literal

ROLLING_WINDOWS = (3, 5, 10)
HISTORY_METRICS = (
    "performance_rating",
    "speed_figure",
    "final_3f_rating",
    "pace_rating",
    "margin_seconds",
)


class ExecutableConnection(Protocol):
    """Minimal connection needed for local feature generation."""

    def execute(self, query: str) -> object:
        """Execute one statement."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...


def rolling_feature_expressions() -> tuple[str, ...]:
    """Create outcome windows ending exactly one horse-day before the target."""
    expressions = [
        "count(*) over history_window as horse_prior_starts",
        "date_diff('day', lag(race_day) over horse_days, race_day) as days_since_last",
    ]
    for metric in HISTORY_METRICS:
        expressions.append(f"lag({metric}) over horse_days as last_{metric}")
        for window in ROLLING_WINDOWS:
            window_clause = (
                "partition by horse_id order by race_day "
                f"rows between {window} preceding and 1 preceding"
            )
            expressions.extend(
                (
                    f"avg({metric}) over ({window_clause}) as avg_{metric}_{window}",
                    f"stddev_pop({metric}) over ({window_clause}) as std_{metric}_{window}",
                )
            )
    return tuple(expressions)


def build_tabular_feature_sql(history_path: Path, output_path: Path) -> str:
    """Return one auditable DuckDB COPY statement."""
    expressions = ",\n      ".join(rolling_feature_expressions())
    source = duckdb_literal(str(history_path))
    output = duckdb_literal(str(output_path))
    return f"""
copy (
  with source as (
    select *, strptime(race_date, '%Y%m%d')::date as race_day
    from read_parquet({source})
  ), horse_days_base as (
    select
      horse_id,
      race_day,
      avg(performance_rating) as performance_rating,
      avg(speed_figure) as speed_figure,
      avg(final_3f_rating) as final_3f_rating,
      avg(pace_rating) as pace_rating,
      avg(margin_seconds) as margin_seconds
    from source
    group by horse_id, race_day
  ), horse_days as (
    select
      *,
      {expressions}
    from horse_days_base
    window
      horse_days as (partition by horse_id order by race_day),
      history_window as (
        partition by horse_id order by race_day
        rows between unbounded preceding and 1 preceding
      )
  )
  select
    source.race_id,
    source.race_date,
    source.horse_id,
    source.finish_position,
    source.decimal_odds,
    source.field_size,
    source.distance,
    try_cast(source.track_code as integer) as track_code,
    try_cast(source.going_code as integer) as going_code,
    try_cast(source.class_code as integer) as class_code,
    try_cast(source.jockey_code as integer) as jockey_code,
    source.carried_weight,
    source.body_weight,
    try_cast(split_part(source.race_id, ':', 3) as integer) as venue_code,
    horse_days.* exclude (horse_id, race_day, performance_rating, speed_figure,
                          final_3f_rating, pace_rating, margin_seconds)
  from source
  join horse_days using (horse_id, race_day)
  order by source.race_date, source.race_id, source.horse_id
) to {output} (format parquet, compression zstd)
""".strip()


def build_tabular_feature_parquet(
    history_path: Path,
    output_path: Path,
    *,
    connection: ExecutableConnection | None = None,
) -> None:
    """Materialize local PIT features; no cloud or production path is used."""
    own_connection = connection is None
    if connection is None:
        module = cast("DuckDbModule", importlib.import_module("duckdb"))
        active = module.connect()
    else:
        active = connection
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        active.execute(build_tabular_feature_sql(history_path, output_path))
    finally:
        if own_connection:
            active.close()
