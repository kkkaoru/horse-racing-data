"""Point-in-time entity trend features with a Prophet-compatible fitting boundary."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import numpy as np

from .domain import FloatArray
from .lab_domain import LabStringArray

TrendForecaster = Callable[[LabStringArray, FloatArray, LabStringArray], FloatArray]


@dataclass(frozen=True)
class EntityTrendFeatures:
    """Trend matrix aligned to source target rows."""

    target_indices: np.ndarray[tuple[int], np.dtype[np.int64]]
    values: FloatArray
    selected_entities: tuple[int, ...]


def _monthly_aggregates(
    dates: LabStringArray, values: FloatArray
) -> tuple[LabStringArray, FloatArray]:
    months = np.asarray([f"{date[:4]}-{date[4:6]}-01" for date in dates], dtype=np.str_)
    unique, inverse = np.unique(months, return_inverse=True)
    sums = np.bincount(inverse, weights=values)
    counts = np.bincount(inverse)
    return unique, sums / counts


def build_entity_trend_features(
    *,
    race_dates: LabStringArray,
    entity_columns: Sequence[LabStringArray],
    performance: FloatArray,
    year: int,
    forecaster: TrendForecaster,
    max_entities: int = 128,
    minimum_history_rows: int = 100,
) -> EntityTrendFeatures:
    """Fit entity trends on pre-year rows and forecast target-year race dates."""
    rows = len(race_dates)
    if performance.shape != (rows,) or any(column.shape != (rows,) for column in entity_columns):
        raise ValueError("entity trend columns must align")
    if max_entities < 1 or minimum_history_rows < 1:
        raise ValueError("entity selection limits must be positive")
    cutoff = f"{year}0101"
    next_cutoff = f"{year + 1}0101"
    history_mask = race_dates < cutoff
    target_indices = np.flatnonzero((race_dates >= cutoff) & (race_dates < next_cutoff)).astype(
        np.int64
    )
    fallback = float(np.mean(performance[history_mask]))
    features = np.full((len(target_indices), len(entity_columns)), fallback, dtype=np.float64)
    selected_counts: list[int] = []
    target_dates = race_dates[target_indices]
    for column_index, entities in enumerate(entity_columns):
        history_entities = entities[history_mask]
        codes, counts = np.unique(history_entities[history_entities != ""], return_counts=True)
        eligible = [
            (str(code), int(count))
            for code, count in zip(codes, counts, strict=True)
            if count >= minimum_history_rows
        ]
        eligible.sort(key=lambda item: (-item[1], item[0]))
        selected = {code for code, _count in eligible[:max_entities]}
        selected_counts.append(len(selected))
        for code in selected:
            entity_history = history_mask & (entities == code)
            monthly_dates, monthly_values = _monthly_aggregates(
                race_dates[entity_history], performance[entity_history]
            )
            entity_target = entities[target_indices] == code
            if not np.any(entity_target):
                continue
            predictions = forecaster(monthly_dates, monthly_values, target_dates[entity_target])
            expected = (int(np.sum(entity_target)),)
            if predictions.shape != expected or not np.all(np.isfinite(predictions)):
                raise RuntimeError("entity trend forecaster returned invalid predictions")
            features[entity_target, column_index] = predictions
    return EntityTrendFeatures(target_indices, features, tuple(selected_counts))
