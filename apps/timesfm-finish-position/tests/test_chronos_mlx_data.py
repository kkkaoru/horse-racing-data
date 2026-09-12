"""Strict date exclusion, sparse fallback identity and static resumption tests."""

import numpy as np
import pytest

from timesfm_finish_position.chronos_mlx_data import (
    HorseWindows,
    StaticWindowBatches,
    WindowConfig,
    build_windows,
    filter_window_targets,
)


def test_windows_exclude_same_day_and_future_and_keep_identity() -> None:
    windows = build_windows(
        horse_ids=np.array(["a", "b", "a", "a", "a", "b"]),
        dates=np.array(["20200101", "20200101", "20210101", "20220101", "20220101", "20230101"]),
        values=np.array([1, 0, 0.5, 0.25, 0.75, 1], dtype=np.float32),
        config=WindowConfig("20220101", "20230101", context_length=3),
    )
    np.testing.assert_allclose(
        windows.contexts, [[np.nan, 1, 0.5], [np.nan, 1, 0.5]], equal_nan=True
    )
    np.testing.assert_array_equal(windows.targets, [[0.25], [0.75]])
    np.testing.assert_array_equal(windows.source_indices, [3, 4])
    np.testing.assert_array_equal(windows.history_counts, [2, 2])


def test_label_filter_keeps_other_venue_context() -> None:
    windows = build_windows(
        horse_ids=np.array(["a", "a", "a", "a"]),
        dates=np.array(["20200101", "20210101", "20220101", "20230101"]),
        values=np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32),
        config=WindowConfig("20220101", "20230101", context_length=3),
    )
    selected = filter_window_targets(windows, np.array([True, False, False, True]))
    np.testing.assert_array_equal(selected.source_indices, [3])
    np.testing.assert_allclose(selected.contexts, [[0.1, 0.2, 0.3]])
    np.testing.assert_allclose(selected.targets, [[0.4]])


def test_history_cap_and_missing_values() -> None:
    windows = build_windows(
        horse_ids=np.array(["a", "a", "a", "a"]),
        dates=np.array(["20200101", "20210101", "20220101", "20230101"]),
        values=np.array([1, np.nan, 0.5, 0.25], dtype=np.float32),
        config=WindowConfig("20230101", "20230101", context_length=1),
    )
    np.testing.assert_array_equal(windows.contexts, [[0.5]])
    np.testing.assert_array_equal(windows.history_counts, [2])


def test_inference_does_not_require_realized_target() -> None:
    windows = build_windows(
        horse_ids=np.array(["a", "a", "a", "b"]),
        dates=np.array(["20200101", "20210101", "20220101", "20220101"]),
        values=np.array([0.25, np.nan, 0.8, np.nan], dtype=np.float32),
        config=WindowConfig("20210101", "20221231", 4, 1, require_finite_targets=False),
    )
    assert windows.source_indices.tolist() == [1, 2]
    assert windows.history_counts.tolist() == [1, 1]
    np.testing.assert_equal(
        windows.contexts,
        np.array(
            [[np.nan, np.nan, np.nan, 0.25], [np.nan, np.nan, np.nan, 0.25]], dtype=np.float32
        ),
    )
    assert np.isnan(windows.targets[0, 0])


def test_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="align"):
        build_windows(
            horse_ids=np.array(["a"]),
            dates=np.array([]),
            values=np.array([], dtype=np.float32),
            config=WindowConfig("2020", "2021"),
        )
    with pytest.raises(ValueError, match="chronological"):
        build_windows(
            horse_ids=np.array(["a", "a"]),
            dates=np.array(["2021", "2020"]),
            values=np.array([1, 0], dtype=np.float32),
            config=WindowConfig("2020", "2021"),
        )
    with pytest.raises(ValueError, match="Invalid window"):
        WindowConfig("2021", "2020")


def test_static_batches_resume_and_drop_tail() -> None:
    windows = HorseWindows(
        np.arange(15, dtype=np.float32).reshape(5, 3),
        np.arange(5, dtype=np.float32).reshape(5, 1),
        np.arange(5),
        np.ones(5, dtype=np.int64),
    )
    sampler = StaticWindowBatches(windows, batch_size=2, accumulation=2)
    assert sampler.steps_per_epoch == 1
    first, target = sampler.at_step(0)
    assert first.shape == (2, 2, 3)
    assert target.shape == (2, 2, 1)
    next_epoch, _ = sampler.at_step(1)
    resumed = StaticWindowBatches(windows, batch_size=2, accumulation=2)
    np.testing.assert_array_equal(resumed.at_step(1)[0], next_epoch)
    np.testing.assert_array_equal(sampler.at_step(0)[0], first)
    with pytest.raises(ValueError, match="nonnegative"):
        sampler.at_step(-1)
    with pytest.raises(ValueError, match="positive"):
        StaticWindowBatches(windows, batch_size=0)
    with pytest.raises(ValueError, match="Not enough"):
        StaticWindowBatches(windows, batch_size=6)
