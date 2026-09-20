"""Unranked relevance must come from explicit status, never missingness alone."""

import numpy as np
import pytest

from predict_lib.rank_relevance import RelevanceMode
from predict_lib.training_admission import RunnerOutcome
from predict_lib.training_labels import outcome_rank_gains


def test_top3_keeps_nonfinishes_explicit_without_mutating_outcomes() -> None:
    outcomes = (
        RunnerOutcome("a", 1, "classified", 1),
        RunnerOutcome("b", 19, "dnf", None),
        RunnerOutcome("c", 2, "classified", 2),
        RunnerOutcome("d", 3, "dq", None),
        RunnerOutcome("e", 4, "classified", 3),
        RunnerOutcome("f", 5, "classified", 4),
    )
    np.testing.assert_array_equal(outcome_rank_gains(outcomes, mode="top3"), [3, 0, 2, 0, 1, 0])
    assert outcomes[1].finish is None
    assert outcomes[3].finish is None
    assert outcomes[5].finish == 4


def test_reciprocal_gains_preserve_ties_and_explicit_unranked_zeros() -> None:
    np.testing.assert_array_equal(
        outcome_rank_gains(
            (
                RunnerOutcome("a", 1, "classified", 1),
                RunnerOutcome("b", 2, "classified", 1),
                RunnerOutcome("c", 3, "dnf", None),
                RunnerOutcome("d", 4, "classified", 4),
                RunnerOutcome("e", 5, "dq", None),
            ),
            mode="reciprocal-rank",
        ),
        [1.0, 1.0, 0.0, 0.25, 0.0],
    )


@pytest.mark.parametrize("mode", ["top3", "reciprocal-rank"])
def test_all_unranked_is_encoded_without_fake_zero_finishes(mode: RelevanceMode) -> None:
    np.testing.assert_array_equal(
        outcome_rank_gains(
            (RunnerOutcome("a", 1, "dnf", None), RunnerOutcome("b", 2, "dq", None)),
            mode=mode,
        ),
        [0.0, 0.0],
    )


def test_empty_encoding_is_not_a_roster_admission() -> None:
    result = outcome_rank_gains((), mode="top3")
    assert result.tolist() == []
    assert result.dtype == np.dtype("float64")


@pytest.mark.parametrize(
    "outcome",
    [
        RunnerOutcome("a", 1, "classified", None),
        RunnerOutcome("a", 1, "classified", True),
        RunnerOutcome("a", 1, "withdrawn", None),
        RunnerOutcome("a", 1, "unresolved", None),
        RunnerOutcome("a", 1, "unknown", 1),
    ],
)
def test_unknown_or_ineligible_outcomes_are_not_zero_label_fallbacks(
    outcome: RunnerOutcome,
) -> None:
    with pytest.raises(ValueError, match="explicit classified or unranked outcome"):
        outcome_rank_gains((outcome,), mode="top3")


@pytest.mark.parametrize(
    "outcome",
    [
        RunnerOutcome("a", 1, "dnf", 1),
        RunnerOutcome("a", 1, "dq", 0),
    ],
)
def test_contradictory_nonfinish_rank_is_rejected(outcome: RunnerOutcome) -> None:
    with pytest.raises(ValueError, match="cannot also have a classified rank"):
        outcome_rank_gains((outcome,), mode="top3")


@pytest.mark.parametrize("finish", [0, -1])
def test_existing_classified_positive_rank_validation_is_preserved(finish: int) -> None:
    with pytest.raises(ValueError, match="positive finite integer finishes"):
        outcome_rank_gains((RunnerOutcome("a", 1, "classified", finish),), mode="top3")
