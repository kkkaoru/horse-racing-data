"""Chronological weighting retains all old teachers and respects fold boundaries."""

from datetime import date

import numpy as np
import numpy.typing as npt
import pytest
from recency_weights import RecencyConfig, chronological_weights


def test_weights_are_normalized_per_race_not_per_horse() -> None:
    result = chronological_weights(
        race_ids=np.array(["old", "new", "new"], dtype=np.str_),
        dates={"old": date(2020, 1, 1), "new": date(2020, 1, 11)},
        config=RecencyConfig(date(2020, 1, 1), date(2020, 1, 21), 10.0),
    )
    assert result.tolist() == pytest.approx(
        [0.6666666666666667, 1.3333333333333333, 1.3333333333333333]
    )


@pytest.mark.parametrize("value", [0.0, float("inf"), float("nan")])
def test_invalid_half_life_fails(value: float) -> None:
    with pytest.raises(ValueError, match="Half life"):
        RecencyConfig(date(2000, 1, 1), date(2020, 1, 1), value)


def test_invalid_window_fails() -> None:
    with pytest.raises(ValueError, match="History start"):
        RecencyConfig(date(2020, 1, 1), date(2020, 1, 1))


@pytest.mark.parametrize("ids", [np.array([], dtype=np.str_), np.array([["a"]], dtype=np.str_)])
def test_invalid_identity_vector_fails(ids: npt.NDArray[np.str_]) -> None:
    with pytest.raises(ValueError, match="identity vector"):
        chronological_weights(
            race_ids=ids, dates={}, config=RecencyConfig(date(2000, 1, 1), date(2020, 1, 1))
        )


def test_missing_date_is_not_imputed() -> None:
    with pytest.raises(ValueError, match="Missing source race date"):
        chronological_weights(
            race_ids=np.array(["a"], dtype=np.str_),
            dates={},
            config=RecencyConfig(date(2000, 1, 1), date(2020, 1, 1)),
        )


@pytest.mark.parametrize("value", [date(1999, 12, 31), date(2020, 1, 1)])
def test_out_of_window_labels_fail(value: date) -> None:
    with pytest.raises(ValueError, match="outside"):
        chronological_weights(
            race_ids=np.array(["a"], dtype=np.str_),
            dates={"a": value},
            config=RecencyConfig(date(2000, 1, 1), date(2020, 1, 1)),
        )


def test_numerical_underflow_cannot_drop_old_teachers() -> None:
    with pytest.raises(ValueError, match="stay positive"):
        chronological_weights(
            race_ids=np.array(["a"], dtype=np.str_),
            dates={"a": date(2000, 1, 1)},
            config=RecencyConfig(date(2000, 1, 1), date(2020, 1, 1), 0.0001),
        )
