"""Walk-forward split and point-in-time correctness guards."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np

from .lab_domain import LabStringArray, WalkForwardFold

DEFAULT_WALK_FORWARD_FOLDS = (
    WalkForwardFold("20210101", "20231231", "20240101", "20241231", "validation-2024"),
    WalkForwardFold("20210101", "20241231", "20250101", "20251231", "validation-2025"),
    WalkForwardFold("20210101", "20251231", "20260101", "20261231", "final-test-2026"),
)


def validate_folds(folds: Sequence[WalkForwardFold]) -> None:
    """Reject overlapping, backwards, or non-chronological folds."""
    if not folds:
        raise ValueError("at least one walk-forward fold is required")
    labels: set[str] = set()
    previous_validation_start = ""
    for fold in folds:
        if not (fold.train_start <= fold.train_end < fold.validation_start <= fold.validation_end):
            raise ValueError(f"invalid chronological boundaries for {fold.label}")
        if fold.validation_start <= previous_validation_start:
            raise ValueError("validation folds must be strictly chronological")
        if fold.label in labels:
            raise ValueError(f"duplicate fold label: {fold.label}")
        labels.add(fold.label)
        previous_validation_start = fold.validation_start


def fold_masks(
    race_dates: LabStringArray, fold: WalkForwardFold
) -> tuple[np.ndarray[tuple[int], np.dtype[np.bool_]], np.ndarray[tuple[int], np.dtype[np.bool_]]]:
    """Return train/validation masks with no boundary overlap."""
    train = (race_dates >= fold.train_start) & (race_dates <= fold.train_end)
    validation = (race_dates >= fold.validation_start) & (race_dates <= fold.validation_end)
    if np.any(train & validation):
        raise ValueError("train and validation masks overlap")
    if not np.any(train) or not np.any(validation):
        raise ValueError(f"fold {fold.label} has an empty partition")
    return train, validation


def validate_history_cutoffs(
    *, target_dates: LabStringArray, history_dates: LabStringArray
) -> None:
    """Require every paired history row to predate its target race."""
    if target_dates.shape != history_dates.shape:
        raise ValueError("target_dates and history_dates must be aligned")
    violations = history_dates >= target_dates
    if np.any(violations):
        raise ValueError(f"history contains {int(np.sum(violations))} non-causal rows")
