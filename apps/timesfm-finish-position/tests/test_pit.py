from __future__ import annotations

import numpy as np
import pytest

from timesfm_finish_position.lab_domain import WalkForwardFold
from timesfm_finish_position.pit import (
    DEFAULT_WALK_FORWARD_FOLDS,
    fold_masks,
    validate_folds,
    validate_history_cutoffs,
)


def test_default_walk_forward_folds_are_chronological() -> None:
    validate_folds(DEFAULT_WALK_FORWARD_FOLDS)
    assert DEFAULT_WALK_FORWARD_FOLDS[0].label == "validation-2024"
    assert DEFAULT_WALK_FORWARD_FOLDS[2].label == "final-test-2026"


def test_validate_folds_rejects_empty_duplicate_and_backwards() -> None:
    with pytest.raises(ValueError, match="at least one"):
        validate_folds(())
    duplicate = (
        WalkForwardFold("20210101", "20231231", "20240101", "20241231", "same"),
        WalkForwardFold("20210101", "20241231", "20250101", "20251231", "same"),
    )
    with pytest.raises(ValueError, match="duplicate fold label"):
        validate_folds(duplicate)
    backwards = (WalkForwardFold("20240101", "20241231", "20230101", "20231231", "bad"),)
    with pytest.raises(ValueError, match="invalid chronological boundaries"):
        validate_folds(backwards)


def test_validate_folds_rejects_non_increasing_validation_start() -> None:
    folds = (
        WalkForwardFold("20210101", "20221231", "20240101", "20241231", "first"),
        WalkForwardFold("20210101", "20221231", "20230101", "20231231", "second"),
    )
    with pytest.raises(ValueError, match="strictly chronological"):
        validate_folds(folds)


def test_fold_masks_separate_train_and_validation() -> None:
    dates = np.asarray(["20231231", "20240101", "20241231", "20250101"], dtype=np.str_)
    train, validation = fold_masks(dates, DEFAULT_WALK_FORWARD_FOLDS[0])
    assert train.tolist() == [True, False, False, False]
    assert validation.tolist() == [False, True, True, False]


def test_fold_masks_rejects_empty_partition() -> None:
    dates = np.asarray(["20240101"], dtype=np.str_)
    with pytest.raises(ValueError, match="empty partition"):
        fold_masks(dates, DEFAULT_WALK_FORWARD_FOLDS[0])


def test_validate_history_cutoffs_accepts_past_and_rejects_future() -> None:
    validate_history_cutoffs(
        target_dates=np.asarray(["20240102", "20250102"], dtype=np.str_),
        history_dates=np.asarray(["20240101", "20240101"], dtype=np.str_),
    )
    with pytest.raises(ValueError, match="non-causal rows"):
        validate_history_cutoffs(
            target_dates=np.asarray(["20240102"], dtype=np.str_),
            history_dates=np.asarray(["20240102"], dtype=np.str_),
        )
    with pytest.raises(ValueError, match="must be aligned"):
        validate_history_cutoffs(
            target_dates=np.asarray(["20240102"], dtype=np.str_),
            history_dates=np.asarray(["20240101", "20240101"], dtype=np.str_),
        )
