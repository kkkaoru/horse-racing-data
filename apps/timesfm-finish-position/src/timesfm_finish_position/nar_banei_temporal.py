"""Past-only horse queries for exact-position NAR/Ban-ei experiments."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np
import polars as pl

from .domain import FloatArray
from .forecasting import TemporalForecaster


@dataclass(frozen=True)
class HorseQuery:
    context: FloatArray
    target_rows: tuple[int, ...]
    latest_date: str
    component_counts: tuple[int, ...]


def observation_index(observations: pl.DataFrame, targets: pl.DataFrame) -> dict[str, pl.DataFrame]:
    selected = observations.join(targets.select("horse_id").unique(), on="horse_id", how="semi")
    return {
        str(frame["horse_id"][0]): frame.sort("race_date", "race_id")
        for frame in selected.partition_by("horse_id")
    }


def build_queries(
    index: Mapping[str, pl.DataFrame],
    targets: pl.DataFrame,
    *,
    columns: tuple[str, ...],
    max_history: int | None = None,
    frozen_year: bool = False,
) -> tuple[HorseQuery, ...]:
    """Forward-fill only within the strictly earlier-day prefix; never backfill."""
    if not columns or any(
        column not in ("performance", "relative_speed", "day_speed", "body_weight")
        for column in columns
    ):
        raise ValueError("Unsupported temporal columns")
    if max_history is not None and max_history < 1:
        raise ValueError("max_history must be positive")
    grouped: dict[tuple[str, str], list[int]] = {}
    for row, (horse, race_date) in enumerate(targets.select("horse_id", "race_date").iter_rows()):
        cutoff = f"{str(race_date)[:4]}0101" if frozen_year else str(race_date)
        grouped.setdefault((str(horse), cutoff), []).append(row)
    queries: list[HorseQuery] = []
    for (horse, cutoff), rows in grouped.items():
        source = index.get(horse)
        if source is None:
            continue
        prior = source.filter(pl.col("race_date") < cutoff)
        matrix = np.asarray(prior.select(columns).to_numpy(), dtype=np.float64).T
        usable = np.any(np.isfinite(matrix), axis=0)
        if not bool(np.any(usable)):
            continue
        latest_date = str(prior["race_date"][int(np.flatnonzero(usable)[-1])])
        matrix = matrix[:, usable].copy()
        observed_window = matrix if max_history is None else matrix[:, -max_history:]
        component_counts = tuple(int(value) for value in np.isfinite(observed_window).sum(axis=1))
        for component, column in enumerate(columns):
            values = matrix[component]
            previous = np.maximum.accumulate(
                np.where(np.isfinite(values), np.arange(len(values)), -1)
            )
            neutral = 0.5 if column == "performance" else 0.0
            matrix[component] = np.where(previous >= 0, values[np.maximum(previous, 0)], neutral)
        if max_history is not None:
            matrix = matrix[:, -max_history:]
        queries.append(HorseQuery(matrix, tuple(rows), latest_date, component_counts))
    return tuple(queries)


def forecast_targets(
    targets: pl.DataFrame,
    queries: tuple[HorseQuery, ...],
    forecaster: TemporalForecaster,
    *,
    columns: tuple[str, ...],
    chunk_size: int = 128,
) -> pl.DataFrame:
    """Cache official one-step forecasts and matched non-foundation controls."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")
    values = np.full((targets.height, len(columns)), np.nan, dtype=np.float64)
    persistence = values.copy()
    mean_five = values.copy()
    counts = np.zeros(targets.height, dtype=np.int64)
    component_counts = np.zeros((targets.height, len(columns)), dtype=np.int64)
    latest = [""] * targets.height
    for start in range(0, len(queries), chunk_size):
        batch = queries[start : start + chunk_size]
        forecasts = forecaster.predict(tuple(query.context for query in batch), horizon=1)
        if len(forecasts) != len(batch):
            raise ValueError("Forecaster omitted queries")
        for query, forecast in zip(batch, forecasts, strict=True):
            if forecast.shape != (len(columns), 1) or not bool(np.isfinite(forecast).all()):
                raise ValueError("Invalid temporal forecast")
            positions = np.asarray(query.target_rows, dtype=np.int64)
            values[positions] = forecast[:, 0]
            persistence[positions] = query.context[:, -1]
            mean_five[positions] = query.context[:, -5:].mean(axis=1)
            counts[positions] = query.context.shape[1]
            component_counts[positions] = query.component_counts
            for row in query.target_rows:
                latest[row] = query.latest_date
    additions = [pl.Series("history_count", counts), pl.Series("latest_history_date", latest)]
    for component, column in enumerate(columns):
        additions.append(pl.Series(f"history_count_{column}", component_counts[:, component]))
        additions.extend(
            (
                pl.Series(f"timesfm_{column}", values[:, component]),
                pl.Series(f"last_{column}", persistence[:, component]),
                pl.Series(f"mean5_{column}", mean_five[:, component]),
            )
        )
    return targets.with_columns(additions)
