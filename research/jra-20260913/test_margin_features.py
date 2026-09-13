"""Known absence and truncated history must have different aggregate semantics."""

import numpy as np
import pytest
from margin_features import restore_margin_features
from numpy.typing import NDArray


def test_complete_field_uses_best_three_and_ignores_genuine_absence() -> None:
    result = restore_margin_features(
        ["a", "a", "a", "a", "a"],
        mean_margin=np.array([1.0, 3.0, 2.0, 6.0, np.nan]),
        best_margin=np.array([0.0, 2.0, 1.0, 5.0, np.nan]),
        last_margin=np.array([1.0, 2.0, 3.0, 4.0, np.nan]),
        incomplete_window=np.array([False, False, False, False, False]),
    )
    np.testing.assert_allclose(result["field_strength_avg_speed"], [3, 3, 3, 3, 3])
    np.testing.assert_allclose(result["field_strength_top3_speed"], [1, 1, 1, 1, 1])
    np.testing.assert_allclose(result["speed_index_avg_5_rank_in_race"], [1, 3, 2, 4, 5])
    np.testing.assert_allclose(result["speed_index_best_5_rank_in_race"], [1, 3, 2, 4, 5])
    np.testing.assert_allclose(
        result["speed_index_avg_5_diff_from_race_avg"], [-2, 0, -1, 3, np.nan]
    )


def test_truncated_window_withholds_whole_field_but_preserves_latest() -> None:
    result = restore_margin_features(
        ["a", "a"],
        mean_margin=np.array([2.0, 4.0]),
        best_margin=np.array([1.0, 3.0]),
        last_margin=np.array([2.0, 4.0]),
        incomplete_window=np.array([True, False]),
    )
    np.testing.assert_allclose(result["speed_index_avg_5"], [np.nan, 4])
    np.testing.assert_allclose(result["speed_index_best_5"], [np.nan, 3])
    np.testing.assert_allclose(result["last_race_margin_to_winner"], [2, 4])
    np.testing.assert_allclose(result["field_strength_avg_speed"], [np.nan, np.nan])
    np.testing.assert_allclose(result["field_strength_top3_speed"], [np.nan, np.nan])


def test_races_are_separate_and_empty_history_stays_missing() -> None:
    result = restore_margin_features(
        ["a", "b"],
        mean_margin=np.array([np.nan, -1.0]),
        best_margin=np.array([np.nan, -2.0]),
        last_margin=np.array([np.nan, 0.0]),
        incomplete_window=np.array([False, False]),
    )
    np.testing.assert_allclose(result["field_strength_avg_speed"], [np.nan, -1])
    np.testing.assert_allclose(result["field_strength_top3_speed"], [np.nan, -2])
    np.testing.assert_allclose(result["last_race_margin_to_winner"], [np.nan, 0])


def test_equal_margins_use_sql_rank_not_dense_rank() -> None:
    result = restore_margin_features(
        ["a", "a", "a"],
        mean_margin=np.array([1.0, 1.0, 3.0]),
        best_margin=np.array([0.0, 0.0, 2.0]),
        last_margin=np.array([1.0, 1.0, 3.0]),
        incomplete_window=np.array([False, False, False]),
    )
    np.testing.assert_allclose(result["speed_index_avg_5_rank_in_race"], [1, 1, 3])
    np.testing.assert_allclose(result["speed_index_best_5_rank_in_race"], [1, 1, 3])


def test_input_arrays_are_not_modified() -> None:
    margins = np.array([1.0])
    restore_margin_features(
        ["a"],
        mean_margin=margins,
        best_margin=margins,
        last_margin=margins,
        incomplete_window=np.array([True]),
    )
    np.testing.assert_allclose(margins, [1])


@pytest.mark.parametrize("bad", [np.array([[1.0]]), np.array([]), np.array([np.inf])])
def test_invalid_margin_arrays_fail(bad: NDArray[np.float64]) -> None:
    with pytest.raises(ValueError, match="aligned one-dimensional"):
        restore_margin_features(
            ["a"],
            mean_margin=bad,
            best_margin=np.array([1.0]),
            last_margin=np.array([1.0]),
            incomplete_window=np.array([False]),
        )


def test_invalid_flag_shape_fails() -> None:
    with pytest.raises(ValueError, match="aligned boolean"):
        restore_margin_features(
            ["a"],
            mean_margin=np.array([1.0]),
            best_margin=np.array([1.0]),
            last_margin=np.array([1.0]),
            incomplete_window=np.array([], dtype=np.bool_),
        )


def test_invalid_flag_dtype_fails() -> None:
    flags = np.array([0], dtype=np.int8)
    with pytest.raises(ValueError, match="aligned boolean"):
        restore_margin_features(
            ["a"],
            mean_margin=np.array([1.0]),
            best_margin=np.array([1.0]),
            last_margin=np.array([1.0]),
            incomplete_window=flags,
        )


def test_blank_race_identity_fails() -> None:
    with pytest.raises(ValueError, match="identities"):
        restore_margin_features(
            [""],
            mean_margin=np.array([1.0]),
            best_margin=np.array([1.0]),
            last_margin=np.array([1.0]),
            incomplete_window=np.array([False]),
        )
