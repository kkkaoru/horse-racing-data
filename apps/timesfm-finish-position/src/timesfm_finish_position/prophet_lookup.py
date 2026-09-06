"""Build frozen production lookup rows from point-in-time Prophet forecasts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

from .domain import FloatArray
from .lab_domain import LabStringArray
from .prophet_features import TrendForecaster


@dataclass(frozen=True)
class ProphetLookupRows:
    """Column arrays for the production lookup parquet."""

    category: LabStringArray
    forecast_date: LabStringArray
    entity_type: LabStringArray
    entity_code: LabStringArray
    yhat: FloatArray
    selected_entities: tuple[int, ...]


def _monthly_series(dates: LabStringArray, values: FloatArray) -> tuple[LabStringArray, FloatArray]:
    months = np.asarray([f"{date[:4]}-{date[4:6]}-01" for date in dates], dtype=np.str_)
    unique, inverse = np.unique(months, return_inverse=True)
    return unique, np.bincount(inverse, weights=values) / np.bincount(inverse)


def _daily_dates(year: int) -> LabStringArray:
    start = np.datetime64(f"{year}-01-01")
    stop = np.datetime64(f"{year + 1}-01-01")
    values = np.arange(start, stop, dtype="datetime64[D]")
    return np.asarray([str(value).replace("-", "") for value in values], dtype=np.str_)


def build_prophet_lookup_rows(
    *,
    race_dates: LabStringArray,
    entity_columns: Sequence[LabStringArray],
    entity_types: Sequence[str],
    performance: FloatArray,
    year: int,
    forecaster: TrendForecaster,
    categories: Sequence[str] = ("nar", "ban-ei"),
    max_entities: int = 32,
    minimum_history_rows: int = 100,
) -> ProphetLookupRows:
    """Fit pre-year entity series and forecast every date in ``year``."""
    rows = len(race_dates)
    if len(entity_columns) != len(entity_types):
        raise ValueError("entity columns and types must align")
    if performance.shape != (rows,) or any(column.shape != (rows,) for column in entity_columns):
        raise ValueError("lookup source columns must align")
    if max_entities < 1 or minimum_history_rows < 1 or not categories:
        raise ValueError("lookup limits and categories must be non-empty")
    history_mask = race_dates < f"{year}0101"
    if not np.any(history_mask):
        raise ValueError("lookup requires pre-year history")
    target_dates = _daily_dates(year)
    fallback = float(np.mean(performance[history_mask]))
    base_rows: list[tuple[str, str, float]] = []
    selected_counts: list[int] = []
    for entity_type, entities in zip(entity_types, entity_columns, strict=True):
        historical_entities = entities[history_mask]
        codes, counts = np.unique(
            historical_entities[historical_entities != ""], return_counts=True
        )
        eligible = [
            (str(code), int(count))
            for code, count in zip(codes, counts, strict=True)
            if count >= minimum_history_rows
        ]
        eligible.sort(key=lambda item: (-item[1], item[0]))
        selected = eligible[:max_entities]
        selected_counts.append(len(selected))
        base_rows.extend((entity_type, "__fallback__", fallback) for _date in target_dates)
        for code, _count in selected:
            entity_history = history_mask & (entities == code)
            monthly_dates, monthly_values = _monthly_series(
                race_dates[entity_history], performance[entity_history]
            )
            predictions = forecaster(monthly_dates, monthly_values, target_dates)
            if predictions.shape != target_dates.shape or not np.all(np.isfinite(predictions)):
                raise RuntimeError("Prophet lookup forecaster returned invalid predictions")
            base_rows.extend((entity_type, code, float(value)) for value in predictions)
    forecast_dates = np.tile(target_dates, len(entity_types) + sum(selected_counts))
    expanded = [
        (category, date, entity_type, entity_code, yhat)
        for category in categories
        for (entity_type, entity_code, yhat), date in zip(base_rows, forecast_dates, strict=True)
    ]
    return ProphetLookupRows(
        category=np.asarray([row[0] for row in expanded], dtype=np.str_),
        forecast_date=np.asarray([row[1] for row in expanded], dtype=np.str_),
        entity_type=np.asarray([row[2] for row in expanded], dtype=np.str_),
        entity_code=np.asarray([row[3] for row in expanded], dtype=np.str_),
        yhat=np.asarray([row[4] for row in expanded], dtype=np.float64),
        selected_entities=tuple(selected_counts),
    )
