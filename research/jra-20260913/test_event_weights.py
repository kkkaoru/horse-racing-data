"""Group mass is balanced without dropping or duplicating broad-history teachers."""

import numpy as np
import pytest
from event_weights import balanced_event_weights
from numpy.typing import NDArray


def test_group_mass_not_row_mass_is_balanced() -> None:
    result = balanced_event_weights(np.array(["a", "a", "b", "c"]), {"a"})
    np.testing.assert_allclose(result, [1.5, 1.5, 0.75, 0.75])


def test_original_row_order_is_preserved() -> None:
    result = balanced_event_weights(np.array(["c", "b", "a", "b"]), {"a", "c"})
    np.testing.assert_allclose(result, [0.75, 1.5, 0.75, 1.5])


def test_duplicate_event_ids_do_not_change_mass() -> None:
    result = balanced_event_weights(np.array(["a", "b", "c"]), ["a", "a"])
    np.testing.assert_allclose(result, [1.5, 0.75, 0.75])


@pytest.mark.parametrize(
    "race_ids",
    [np.array([], dtype=np.str_), np.array([["a"]]), np.array(["a", ""])],
)
def test_invalid_race_array_fails(race_ids: NDArray[np.str_]) -> None:
    with pytest.raises(ValueError, match="nonempty one-dimensional"):
        balanced_event_weights(race_ids, {"a"})


def test_future_or_unknown_event_cannot_enter_training_weights() -> None:
    with pytest.raises(ValueError, match="training population"):
        balanced_event_weights(np.array(["a", "b"]), {"future"})


def test_missing_event_stratum_fails() -> None:
    with pytest.raises(ValueError, match="Both event and other-race"):
        balanced_event_weights(np.array(["a", "b"]), set())


def test_missing_background_stratum_fails() -> None:
    with pytest.raises(ValueError, match="Both event and other-race"):
        balanced_event_weights(np.array(["a", "b"]), {"a", "b"})
