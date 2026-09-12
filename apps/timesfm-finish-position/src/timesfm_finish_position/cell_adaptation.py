"""Cell-specific, entrant-complete training scopes for temporal models."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .lab_domain import LabStringArray

IndexArray = np.ndarray[tuple[int], np.dtype[np.int64]]


@dataclass(frozen=True)
class CellTemporalProfile:
    """One auditable TimesFM readout hypothesis for a structural JRA cell."""

    name: str
    value_columns: tuple[str, ...]
    horse_normalize: bool = False
    rolling_origin: bool = False


@dataclass(frozen=True)
class EntrantHistoryScope:
    """Every eligible prior row for evaluation entrants plus optional peers."""

    training_indices: IndexArray
    entrant_history_indices: IndexArray
    evaluation_indices: IndexArray
    entrant_horse_ids: frozenset[str]
    target_cell_training_race_count: int
    cross_cell_training_race_count: int
    additional_training_row_count: int


def temporal_profiles_for_cell(
    *,
    distance: int,
    surface: str,
    condition_code: str,
    race_identity: str,
) -> tuple[CellTemporalProfile, ...]:
    """Return dimension-aware candidates; development evidence chooses per cell."""
    performance = CellTemporalProfile("performance", ("performance_rating",))
    market_residual = CellTemporalProfile("performance-over-market", ("performance_over_market",))
    normalized_market_residual = CellTemporalProfile(
        "performance-over-market-horse-normalized",
        ("performance_over_market",),
        horse_normalize=True,
    )
    rolling_market_residual = CellTemporalProfile(
        "performance-over-market-rolling",
        ("performance_over_market",),
        rolling_origin=True,
    )
    if condition_code == "701":
        return (
            performance,
            CellTemporalProfile("newcomer-pace", ("performance_rating", "pace_rating")),
        )
    if condition_code == "999" and race_identity.startswith("name:"):
        return (
            CellTemporalProfile(
                "named-open-complete",
                ("performance_rating", "speed_figure", "final_3f_rating", "pace_rating"),
            ),
            CellTemporalProfile(
                "named-open-finish",
                ("performance_rating", "speed_figure", "final_3f_rating"),
            ),
            market_residual,
            normalized_market_residual,
            rolling_market_residual,
        )
    if distance <= 1400:
        emphasis = "sprint-turf" if surface == "turf" else "sprint-dirt"
        return (
            performance,
            CellTemporalProfile(
                emphasis,
                ("performance_rating", "speed_figure", "pace_rating"),
            ),
            market_residual,
            normalized_market_residual,
            rolling_market_residual,
        )
    if surface == "turf" and distance >= 1800:
        return (
            performance,
            CellTemporalProfile(
                "turf-route-finish",
                ("performance_rating", "speed_figure", "final_3f_rating"),
            ),
            market_residual,
            normalized_market_residual,
            rolling_market_residual,
        )
    return (
        performance,
        CellTemporalProfile("balanced", ("performance_rating", "speed_figure", "pace_rating")),
        market_residual,
        normalized_market_residual,
        rolling_market_residual,
    )


def scope_strategies_for_cell(
    *, distance: int, surface: str, condition_code: str, race_identity: str
) -> tuple[str, ...]:
    """Choose one cell-relevant expansion in addition to mandatory entrant history."""
    if condition_code == "701" or distance <= 1400:
        expansion = "venue-surface-track-bias"
    elif (condition_code == "999" and race_identity.startswith("name:")) or (
        surface == "turf" and distance >= 1800
    ):
        expansion = "related-distance-track-bias"
    else:
        expansion = "venue-surface-track-bias"
    return ("entrant-history", expansion)


def build_entrant_history_scope(
    *,
    horse_ids: LabStringArray,
    race_dates: LabStringArray,
    race_ids: LabStringArray,
    evaluation_indices: IndexArray,
    cutoff: str,
    history_start: str,
    target_cell_race_ids: frozenset[str],
    additional_training_indices: IndexArray | None = None,
) -> EntrantHistoryScope:
    """Select all pre-cutoff starts of evaluation entrants across every venue/cell."""
    rows = len(horse_ids)
    if race_dates.shape != (rows,) or race_ids.shape != (rows,):
        raise ValueError("scope columns must align")
    if (
        evaluation_indices.ndim != 1
        or np.any(evaluation_indices < 0)
        or np.any(evaluation_indices >= rows)
    ):
        raise ValueError("evaluation indices are invalid")
    if history_start >= cutoff:
        raise ValueError("history_start must be before cutoff")
    entrants = frozenset(str(horse_ids[index]) for index in evaluation_indices)
    eligible_date = (race_dates >= history_start) & (race_dates < cutoff)
    entrant_mask = eligible_date & np.isin(horse_ids, tuple(entrants))
    entrant_history = np.flatnonzero(entrant_mask).astype(np.int64)
    selected = entrant_mask.copy()
    if additional_training_indices is not None:
        if (
            additional_training_indices.ndim != 1
            or np.any(additional_training_indices < 0)
            or np.any(additional_training_indices >= rows)
        ):
            raise ValueError("additional training indices are invalid")
        selected[additional_training_indices] |= eligible_date[additional_training_indices]
    training = np.flatnonzero(selected).astype(np.int64)
    if not set(entrant_history.tolist()).issubset(training.tolist()):
        raise AssertionError("entrant history must be a subset of training scope")
    training_races = {str(race_ids[index]) for index in training}
    target_count = len(training_races & target_cell_race_ids)
    return EntrantHistoryScope(
        training_indices=training,
        entrant_history_indices=entrant_history,
        evaluation_indices=evaluation_indices.copy(),
        entrant_horse_ids=entrants,
        target_cell_training_race_count=target_count,
        cross_cell_training_race_count=len(training_races) - target_count,
        additional_training_row_count=len(training) - len(entrant_history),
    )
