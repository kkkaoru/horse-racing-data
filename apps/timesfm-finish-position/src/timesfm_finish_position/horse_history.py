"""Point-in-time-correct per-horse sequence construction for history encoders."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date

import numpy as np

from .lab_domain import LabFloatArray, LabStringArray

MAX_HISTORY_LENGTH = 10
HISTORY_FEATURE_NAMES = (
    "finish_position_norm",
    "speed_figure",
    "margin",
    "final_3f_rating",
    "pace_rating",
    "distance_norm",
    "surface_code",
    "going_code",
    "class_rating",
    "carried_weight_norm",
    "body_weight_norm",
    "jockey_code_norm",
    "days_since_previous_race_norm",
)


@dataclass(frozen=True)
class HorseHistoryRows:
    """Chronological source rows available to a sequence builder."""

    horse_ids: LabStringArray
    race_dates: LabStringArray
    values_without_interval: LabFloatArray


@dataclass(frozen=True)
class HorseHistoryBatch:
    """Left-padded histories aligned to selected target rows."""

    target_indices: np.ndarray[tuple[int], np.dtype[np.int64]]
    values: np.ndarray[tuple[int, int, int], np.dtype[np.float64]]
    mask: np.ndarray[tuple[int, int], np.dtype[np.bool_]]
    target_days_since_last: LabFloatArray


def _parse_date(raw: str) -> date:
    return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))


def _validate_rows(rows: HorseHistoryRows, target_mask: np.ndarray) -> None:
    if rows.values_without_interval.ndim != 2:
        raise ValueError("history values must be two-dimensional")
    expected_rows = len(rows.horse_ids)
    if (
        rows.race_dates.shape != (expected_rows,)
        or rows.values_without_interval.shape[0] != expected_rows
    ):
        raise ValueError("horse history columns must align")
    if target_mask.shape != (expected_rows,):
        raise ValueError("target_mask must align with history rows")
    if np.any(rows.race_dates[1:] < rows.race_dates[:-1]):
        raise ValueError("history rows must be globally chronological")
    if np.any(~np.isfinite(rows.values_without_interval)):
        raise ValueError("history values must be finite")


def build_horse_history_batch(
    rows: HorseHistoryRows,
    target_mask: np.ndarray[tuple[int], np.dtype[np.bool_]],
    *,
    max_history: int = MAX_HISTORY_LENGTH,
) -> HorseHistoryBatch:
    """Build prior-date-only histories and an explicit irregular-time channel."""
    _validate_rows(rows, target_mask)
    if max_history < 1:
        raise ValueError("max_history must be positive")
    target_indices = np.flatnonzero(target_mask).astype(np.int64)
    feature_count = rows.values_without_interval.shape[1] + 1
    values = np.zeros((len(target_indices), max_history, feature_count), dtype=np.float64)
    mask = np.zeros((len(target_indices), max_history), dtype=np.bool_)
    target_days_since_last = np.zeros(len(target_indices), dtype=np.float64)
    target_position = {int(row): position for position, row in enumerate(target_indices)}
    histories: dict[str, deque[int]] = defaultdict(lambda: deque(maxlen=max_history))
    dates = np.asarray([_parse_date(str(raw)) for raw in rows.race_dates], dtype=object)
    by_date: dict[str, list[int]] = defaultdict(list)
    for index, raw_date in enumerate(rows.race_dates):
        by_date[str(raw_date)].append(index)
    for raw_date in sorted(by_date):
        date_indices = by_date[raw_date]
        for index in date_indices:
            position = target_position.get(index)
            if position is None:
                continue
            horse_history = tuple(histories[str(rows.horse_ids[index])])
            start = max_history - len(horse_history)
            if horse_history:
                target_days_since_last[position] = float(
                    (dates[index] - dates[horse_history[-1]]).days
                )
            previous_date = None
            for offset, history_index in enumerate(horse_history):
                history_date = dates[history_index]
                interval = (
                    0.0 if previous_date is None else float((history_date - previous_date).days)
                )
                values[position, start + offset, :-1] = rows.values_without_interval[history_index]
                values[position, start + offset, -1] = interval / 365.0
                mask[position, start + offset] = True
                previous_date = history_date
        for index in date_indices:
            histories[str(rows.horse_ids[index])].append(index)
    return HorseHistoryBatch(
        target_indices=target_indices,
        values=values,
        mask=mask,
        target_days_since_last=target_days_since_last,
    )
