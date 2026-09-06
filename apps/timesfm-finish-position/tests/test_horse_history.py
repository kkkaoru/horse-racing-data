from __future__ import annotations

import numpy as np
import pytest

from timesfm_finish_position.horse_history import (
    HorseHistoryRows,
    build_horse_history_batch,
)


def test_build_horse_history_batch_uses_only_strictly_prior_dates() -> None:
    rows = HorseHistoryRows(
        horse_ids=np.asarray(["h1", "h2", "h1", "h2", "h1"], dtype=np.str_),
        race_dates=np.asarray(
            ["20240101", "20240101", "20240201", "20240201", "20240301"], dtype=np.str_
        ),
        values_without_interval=np.asarray(
            [[1.0, 10.0], [2.0, 20.0], [3.0, 30.0], [4.0, 40.0], [5.0, 50.0]],
            dtype=np.float64,
        ),
    )
    targets = np.asarray([False, False, True, False, True], dtype=np.bool_)
    batch = build_horse_history_batch(rows, targets, max_history=3)
    assert batch.target_indices.tolist() == [2, 4]
    assert batch.mask.tolist() == [[False, False, True], [False, True, True]]
    assert batch.values[0, 2].tolist() == [1.0, 10.0, 0.0]
    assert batch.values[1, 1].tolist() == [1.0, 10.0, 0.0]
    assert batch.values[1, 2].tolist() == pytest.approx([3.0, 30.0, 31.0 / 365.0])
    assert batch.target_days_since_last.tolist() == [31.0, 29.0]


def test_build_horse_history_batch_truncates_to_latest_history() -> None:
    rows = HorseHistoryRows(
        horse_ids=np.asarray(["h1", "h1", "h1", "h1"], dtype=np.str_),
        race_dates=np.asarray(["20240101", "20240102", "20240103", "20240104"], dtype=np.str_),
        values_without_interval=np.asarray([[1.0], [2.0], [3.0], [4.0]], dtype=np.float64),
    )
    batch = build_horse_history_batch(
        rows, np.asarray([False, False, False, True], dtype=np.bool_), max_history=2
    )
    assert batch.values[0, :, 0].tolist() == [2.0, 3.0]
    assert batch.mask.tolist() == [[True, True]]
    assert batch.target_days_since_last.tolist() == [1.0]


def test_build_horse_history_batch_validates_contracts() -> None:
    valid = HorseHistoryRows(
        horse_ids=np.asarray(["h1"], dtype=np.str_),
        race_dates=np.asarray(["20240101"], dtype=np.str_),
        values_without_interval=np.asarray([[1.0]], dtype=np.float64),
    )
    with pytest.raises(ValueError, match="max_history must be positive"):
        build_horse_history_batch(valid, np.asarray([True], dtype=np.bool_), max_history=0)
    with pytest.raises(ValueError, match="target_mask must align"):
        build_horse_history_batch(valid, np.asarray([True, False], dtype=np.bool_))
    misaligned = HorseHistoryRows(
        horse_ids=np.asarray(["h1"], dtype=np.str_),
        race_dates=np.asarray(["20240101", "20240102"], dtype=np.str_),
        values_without_interval=np.asarray([[1.0]], dtype=np.float64),
    )
    with pytest.raises(ValueError, match="columns must align"):
        build_horse_history_batch(misaligned, np.asarray([True], dtype=np.bool_))
    backwards = HorseHistoryRows(
        horse_ids=np.asarray(["h1", "h1"], dtype=np.str_),
        race_dates=np.asarray(["20240102", "20240101"], dtype=np.str_),
        values_without_interval=np.asarray([[1.0], [2.0]], dtype=np.float64),
    )
    with pytest.raises(ValueError, match="globally chronological"):
        build_horse_history_batch(backwards, np.asarray([False, True], dtype=np.bool_))
    nonfinite = HorseHistoryRows(
        horse_ids=np.asarray(["h1"], dtype=np.str_),
        race_dates=np.asarray(["20240101"], dtype=np.str_),
        values_without_interval=np.asarray([[np.nan]], dtype=np.float64),
    )
    with pytest.raises(ValueError, match="must be finite"):
        build_horse_history_batch(nonfinite, np.asarray([True], dtype=np.bool_))
