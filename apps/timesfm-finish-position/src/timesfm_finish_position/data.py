"""Load and transform the existing NAR ensemble/router evaluation data."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

import numpy as np
import numpy.typing as npt
import pyarrow.parquet as pq

from .domain import (
    ACTION_WEIGHTS,
    BASELINE_ACTION_INDEX,
    CellQuerySet,
    FloatArray,
    IntArray,
    RaceDataset,
)

ACTION_COLUMNS = tuple(f"winner_rank_w{round(weight * 100):03d}" for weight in ACTION_WEIGHTS)
SOURCE_COLUMNS = (
    "race_id",
    "race_date",
    "race_year",
    "keibajo_code",
    "kyori",
    "track_code",
    "current_baba_condition",
    "field_size",
    "favorite_market_share",
    "market_entropy",
    "model_disagrees",
    "rank_disagreement_mean",
    "top3_overlap",
    "current_top_margin",
    *ACTION_COLUMNS,
)
MIN_SOURCE_YEAR = 2023
MAX_SOURCE_YEAR = 2026
TOP_K = 3


class ArrowColumnLike(Protocol):
    """Narrow PyArrow column boundary used by the loader."""

    def to_numpy(self, *, zero_copy_only: bool) -> object:
        """Return the column as an array-like value."""
        ...

    def to_pylist(self) -> list[object]:
        """Return scalar Python values."""
        ...


class ArrowTableLike(Protocol):
    """Narrow PyArrow table boundary used by the loader."""

    def column(self, name: str) -> ArrowColumnLike:
        """Return one named column."""
        ...


def _as_float_column(table: ArrowTableLike, name: str) -> FloatArray:
    column = table.column(name)
    return np.asarray(column.to_numpy(zero_copy_only=False), dtype=np.float64)


def _as_int_column(table: ArrowTableLike, name: str) -> IntArray:
    column = table.column(name)
    return np.asarray(column.to_numpy(zero_copy_only=False), dtype=np.int64)


def _as_string_column(
    table: ArrowTableLike, name: str
) -> np.ndarray[tuple[int], np.dtype[np.str_]]:
    column = table.column(name)
    return np.asarray(column.to_pylist(), dtype=np.str_)


def _distance_band(distance: float) -> str:
    if distance < 1_200:
        return "sprint"
    if distance < 1_600:
        return "mile"
    if distance < 2_000:
        return "middle"
    return "long"


def _season(month: int) -> str:
    if month in (12, 1, 2):
        return "winter"
    if month <= 5:
        return "spring"
    if month <= 8:
        return "summer"
    return "autumn"


def _market_band(share: float) -> str:
    if share < 0.25:
        return "flat"
    if share < 0.4:
        return "normal"
    return "dominant"


def _build_cell_ids(
    *,
    dates: np.ndarray[tuple[int], np.dtype[np.str_]],
    venues: np.ndarray[tuple[int], np.dtype[np.str_]],
    distances: FloatArray,
    tracks: np.ndarray[tuple[int], np.dtype[np.str_]],
    goings: np.ndarray[tuple[int], np.dtype[np.str_]],
    market_shares: FloatArray,
    disagreements: FloatArray,
) -> np.ndarray[tuple[int], np.dtype[np.str_]]:
    cells = [
        "|".join(
            (
                str(venue).zfill(2),
                _distance_band(float(distance)),
                str(track)[:1],
                str(going),
                _season(int(str(date)[4:6])),
                _market_band(float(share)),
                "disagree" if float(disagree) > 0.5 else "agree",
            )
        )
        for date, venue, distance, track, going, share, disagree in zip(
            dates,
            venues,
            distances,
            tracks,
            goings,
            market_shares,
            disagreements,
            strict=True,
        )
    ]
    return np.asarray(cells, dtype=np.str_)


def _build_features(
    *,
    dates: np.ndarray[tuple[int], np.dtype[np.str_]],
    venues: np.ndarray[tuple[int], np.dtype[np.str_]],
    distances: FloatArray,
    tracks: np.ndarray[tuple[int], np.dtype[np.str_]],
    goings: np.ndarray[tuple[int], np.dtype[np.str_]],
    field_sizes: FloatArray,
    market_shares: FloatArray,
    entropies: FloatArray,
    disagreements: FloatArray,
    rank_disagreements: FloatArray,
    overlaps: FloatArray,
    margins: FloatArray,
) -> FloatArray:
    months = np.asarray([int(str(date)[4:6]) for date in dates], dtype=np.float64)
    venue_numbers = np.asarray([int(str(venue)) for venue in venues], dtype=np.float64)
    surface_numbers = np.asarray(
        [int(str(track)[:1]) if str(track)[:1].isdigit() else 0 for track in tracks],
        dtype=np.float64,
    )
    going_numbers = np.asarray(
        [int(str(going)) if str(going).isdigit() else 0 for going in goings], dtype=np.float64
    )
    angle = months * (2.0 * np.pi / 12.0)
    return np.column_stack(
        (
            venue_numbers / 99.0,
            distances / 4_000.0,
            field_sizes / 20.0,
            market_shares,
            entropies,
            disagreements,
            rank_disagreements,
            overlaps / 3.0,
            margins,
            np.sin(angle),
            np.cos(angle),
            surface_numbers / 9.0,
            going_numbers / 9.0,
        )
    ).astype(np.float64)


def load_race_dataset(path: Path) -> RaceDataset:
    """Load the immutable race-level blend ledger and validate its identity."""
    if not path.is_file():
        raise FileNotFoundError(path)
    schema_names = frozenset(pq.read_schema(path).names)
    missing = tuple(column for column in SOURCE_COLUMNS if column not in schema_names)
    if missing:
        raise ValueError(f"input parquet is missing columns: {missing}")
    table = pq.read_table(path, columns=list(SOURCE_COLUMNS))
    years = _as_int_column(table, "race_year")
    selected = (years >= MIN_SOURCE_YEAR) & (years <= MAX_SOURCE_YEAR)
    race_ids = _as_string_column(table, "race_id")[selected]
    dates = _as_string_column(table, "race_date")[selected]
    years = years[selected]
    venues = _as_string_column(table, "keibajo_code")[selected]
    distances = _as_float_column(table, "kyori")[selected]
    tracks = _as_string_column(table, "track_code")[selected]
    goings = _as_string_column(table, "current_baba_condition")[selected]
    field_sizes = _as_float_column(table, "field_size")[selected]
    market_shares = _as_float_column(table, "favorite_market_share")[selected]
    entropies = _as_float_column(table, "market_entropy")[selected]
    disagreements = _as_float_column(table, "model_disagrees")[selected]
    rank_disagreements = _as_float_column(table, "rank_disagreement_mean")[selected]
    overlaps = _as_float_column(table, "top3_overlap")[selected]
    margins = _as_float_column(table, "current_top_margin")[selected]
    winner_ranks = np.column_stack(
        tuple(_as_int_column(table, column)[selected] for column in ACTION_COLUMNS)
    )
    order = np.lexsort((race_ids, dates))
    if len(set(race_ids.tolist())) != len(race_ids):
        raise ValueError("race_id must be unique")
    if winner_ranks.shape[1] != len(ACTION_WEIGHTS) or np.any(winner_ranks < 1):
        raise ValueError("winner ranks must contain 21 positive action outcomes")
    features = _build_features(
        dates=dates,
        venues=venues,
        distances=distances,
        tracks=tracks,
        goings=goings,
        field_sizes=field_sizes,
        market_shares=market_shares,
        entropies=entropies,
        disagreements=disagreements,
        rank_disagreements=rank_disagreements,
        overlaps=overlaps,
        margins=margins,
    )
    cells = _build_cell_ids(
        dates=dates,
        venues=venues,
        distances=distances,
        tracks=tracks,
        goings=goings,
        market_shares=market_shares,
        disagreements=disagreements,
    )
    return RaceDataset(
        race_ids=race_ids[order],
        race_dates=dates[order],
        race_years=years[order],
        cell_ids=cells[order],
        features=features[order],
        winner_ranks=winner_ranks[order],
    )


def subset(dataset: RaceDataset, mask: npt.NDArray[np.bool_]) -> RaceDataset:
    """Return a row-preserving subset."""
    return RaceDataset(
        race_ids=dataset.race_ids[mask],
        race_dates=dataset.race_dates[mask],
        race_years=dataset.race_years[mask],
        cell_ids=dataset.cell_ids[mask],
        features=dataset.features[mask],
        winner_ranks=dataset.winner_ranks[mask],
    )


def arm_gains(winner_ranks: IntArray) -> FloatArray:
    """Return per-race Top1..3 utility gain versus the deployed 0.50 blend."""
    cutoffs = np.arange(1, TOP_K + 1, dtype=np.int64)
    utility = (winner_ranks[:, :, None] <= cutoffs).mean(axis=2)
    return utility - utility[:, [BASELINE_ACTION_INDEX]]


def _temporal_cell(cell: str) -> str:
    return "|".join(cell.split("|")[:3])


def _daily_cell_series(dataset: RaceDataset, gains: FloatArray) -> dict[str, FloatArray]:
    grouped: dict[str, dict[str, list[FloatArray]]] = defaultdict(lambda: defaultdict(list))
    for cell, date, gain in zip(dataset.cell_ids, dataset.race_dates, gains, strict=True):
        grouped[_temporal_cell(str(cell))][str(date)].append(gain)
    result: dict[str, FloatArray] = {}
    for cell, by_date in grouped.items():
        ordered = [np.mean(by_date[date], axis=0) for date in sorted(by_date)]
        result[cell] = np.asarray(ordered, dtype=np.float64).T
    return result


def _eval_date_positions(dataset: RaceDataset) -> tuple[dict[str, dict[str, int]], int]:
    dates_by_cell: dict[str, set[str]] = defaultdict(set)
    for cell, date in zip(dataset.cell_ids, dataset.race_dates, strict=True):
        dates_by_cell[_temporal_cell(str(cell))].add(str(date))
    positions: dict[str, dict[str, int]] = {}
    horizon = 1
    for cell, dates in dates_by_cell.items():
        positions[cell] = {date: index for index, date in enumerate(sorted(dates))}
        horizon = max(horizon, len(dates))
    return positions, horizon


def build_cell_queries(
    train: RaceDataset, evaluation: RaceDataset, *, context_length: int
) -> CellQuerySet:
    """Build prior-years-only cell contexts for one outer evaluation year."""
    if context_length < 32:
        raise ValueError("context_length must be at least one TimesFM input patch (32)")
    train_gains = arm_gains(train.winner_ranks)
    series = _daily_cell_series(train, train_gains)
    global_series = np.asarray(train_gains, dtype=np.float64).T[:, -context_length:]
    positions, horizon = _eval_date_positions(evaluation)
    cell_ids = tuple(sorted(positions))
    contexts = tuple(series.get(cell, global_series)[:, -context_length:] for cell in cell_ids)
    query_index = {cell: index for index, cell in enumerate(cell_ids)}
    row_query_indices = np.asarray(
        [query_index[_temporal_cell(str(cell))] for cell in evaluation.cell_ids], dtype=np.int64
    )
    row_horizon_indices = np.asarray(
        [
            positions[_temporal_cell(str(cell))][str(date)]
            for cell, date in zip(evaluation.cell_ids, evaluation.race_dates, strict=True)
        ],
        dtype=np.int64,
    )
    return CellQuerySet(
        cell_ids=cell_ids,
        contexts=contexts,
        horizon=horizon,
        row_query_indices=row_query_indices,
        row_horizon_indices=row_horizon_indices,
    )


def map_query_forecasts(queries: CellQuerySet, forecasts: Sequence[FloatArray]) -> FloatArray:
    """Map cell-horizon forecasts back to chronological race rows."""
    if len(forecasts) != len(queries.contexts):
        raise ValueError("forecast count does not match cell query count")
    rows = [
        forecasts[query_index][:, horizon_index]
        for query_index, horizon_index in zip(
            queries.row_query_indices, queries.row_horizon_indices, strict=True
        )
    ]
    return np.asarray(rows, dtype=np.float64)
