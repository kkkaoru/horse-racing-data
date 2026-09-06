"""Strict outer-year horse-series queries for TimesFM and Chronos features."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

import numpy as np

from .domain import FloatArray
from .forecasting import TemporalForecaster
from .lab_domain import LabStringArray


@dataclass(frozen=True)
class HorseYearQueries:
    """One pre-year context and all target-year starts for each known horse."""

    contexts: tuple[FloatArray, ...]
    target_indices: tuple[np.ndarray[tuple[int], np.dtype[np.int64]], ...]
    all_target_indices: np.ndarray[tuple[int], np.dtype[np.int64]]
    variates: int


@dataclass(frozen=True)
class HorseYearForecast:
    """Forecast matrix aligned to target rows in source order."""

    target_indices: np.ndarray[tuple[int], np.dtype[np.int64]]
    values: FloatArray
    history_available: np.ndarray[tuple[int], np.dtype[np.bool_]]


def build_horse_year_queries(
    *,
    horse_ids: LabStringArray,
    race_dates: LabStringArray,
    history_values: FloatArray,
    year: int,
    max_history: int = 10,
) -> HorseYearQueries:
    """Build one context per horse without consuming any target-year outcome."""
    rows = len(horse_ids)
    if race_dates.shape != (rows,) or history_values.ndim != 2 or len(history_values) != rows:
        raise ValueError("horse series columns must align")
    if max_history < 1:
        raise ValueError("max_history must be positive")
    if np.any(race_dates[1:] < race_dates[:-1]):
        raise ValueError("horse series rows must be chronological")
    target_prefix = str(year)
    prior: dict[str, list[int]] = defaultdict(list)
    targets: dict[str, list[int]] = defaultdict(list)
    for index, (horse_id, race_date) in enumerate(zip(horse_ids, race_dates, strict=True)):
        if str(race_date)[:4] < target_prefix:
            prior[str(horse_id)].append(index)
        elif str(race_date)[:4] == target_prefix:
            targets[str(horse_id)].append(index)
    contexts: list[FloatArray] = []
    target_indices: list[np.ndarray[tuple[int], np.dtype[np.int64]]] = []
    for horse_id in sorted(targets):
        history = prior.get(horse_id, [])[-max_history:]
        if not history:
            continue
        contexts.append(history_values[history].T.astype(np.float64))
        target_indices.append(np.asarray(targets[horse_id], dtype=np.int64))
    all_target_indices = np.asarray(
        sorted(index for indices in targets.values() for index in indices), dtype=np.int64
    )
    return HorseYearQueries(
        contexts=tuple(contexts),
        target_indices=tuple(target_indices),
        all_target_indices=all_target_indices,
        variates=history_values.shape[1],
    )


def forecast_horse_year(
    queries: HorseYearQueries,
    forecaster: TemporalForecaster,
    *,
    fallback: FloatArray,
) -> HorseYearForecast:
    """Forecast target starts grouped by horizon and retain explicit fallback status."""
    if fallback.shape != (queries.variates,):
        raise ValueError("fallback must contain one value per variate")
    positions = {
        int(source): position for position, source in enumerate(queries.all_target_indices)
    }
    values = np.broadcast_to(fallback, (len(queries.all_target_indices), queries.variates)).copy()
    history_available = np.zeros(len(queries.all_target_indices), dtype=np.bool_)
    by_horizon: dict[int, list[int]] = defaultdict(list)
    for query_index, target in enumerate(queries.target_indices):
        by_horizon[len(target)].append(query_index)
    for horizon, query_indices in sorted(by_horizon.items()):
        contexts = tuple(queries.contexts[index] for index in query_indices)
        forecasts = forecaster.predict(contexts, horizon=horizon)
        if len(forecasts) != len(query_indices):
            raise RuntimeError("temporal forecaster omitted a horse query")
        for query_index, forecast in zip(query_indices, forecasts, strict=True):
            expected = (queries.variates, horizon)
            if forecast.shape != expected:
                raise RuntimeError(f"unexpected horse forecast shape: {forecast.shape}")
            for step, source_index in enumerate(queries.target_indices[query_index]):
                position = positions[int(source_index)]
                values[position] = forecast[:, step]
                history_available[position] = True
    return HorseYearForecast(queries.all_target_indices, values, history_available)
