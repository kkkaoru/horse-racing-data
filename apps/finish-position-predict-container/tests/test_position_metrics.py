from __future__ import annotations

import pytest

from predict_lib.position_metrics import (
    ExactPositionMetrics,
    PositionObservation,
    combine_exact_position_metrics,
    exact_position_metrics,
    parse_exact_position_metrics,
)


def test_winner_in_top_two_is_not_exact_top_two() -> None:
    result = exact_position_metrics(
        (
            PositionObservation("r", 1, 2),
            PositionObservation("r", 2, 1),
            PositionObservation("r", 3, 4),
            PositionObservation("r", 4, 5),
            PositionObservation("r", 5, 3),
        )
    )
    assert result.race_count == 1
    assert result.support == (1, 1, 1, 1, 1)
    assert result.hits == (0, 0, 0, 0, 0)
    assert result.accuracy == (0.0, 0.0, 0.0, 0.0, 0.0)


def test_ties_and_nonfinishers_have_explicit_position_support() -> None:
    result = exact_position_metrics(
        (
            PositionObservation("r", 1, 1),
            PositionObservation("r", 2, 1),
            PositionObservation("r", 3, 3),
            PositionObservation("r", 4, None),
        )
    )
    assert result.support == (1, 0, 1, 0, 0)
    assert result.hits == (1, 0, 1, 0, 0)
    assert result.accuracy == (1.0, None, 1.0, None, None)


def test_races_have_equal_weight_with_per_position_denominators() -> None:
    result = exact_position_metrics(
        (
            PositionObservation("a", 1, 1),
            PositionObservation("a", 2, None),
            PositionObservation("b", 1, 2),
            PositionObservation("b", 2, 1),
            PositionObservation("b", 3, None),
        )
    )
    assert result.race_count == 2
    assert result.support == (2, 1, 0, 0, 0)
    assert result.hits == (1, 0, 0, 0, 0)
    assert result.accuracy == (0.5, 0.0, None, None, None)


def test_empty_population_has_no_supported_positions() -> None:
    result = exact_position_metrics(())
    assert result.race_count == 0
    assert result.support == (0, 0, 0, 0, 0)
    assert result.hits == (0, 0, 0, 0, 0)
    assert result.accuracy == (None, None, None, None, None)


@pytest.mark.parametrize("race_id", ["", " ", " r"])
def test_invalid_race_identity(race_id: str) -> None:
    with pytest.raises(ValueError, match="Race identity"):
        exact_position_metrics((PositionObservation(race_id, 1, 1),))


@pytest.mark.parametrize("rank", [0, -1, True])
def test_invalid_predicted_rank(rank: int) -> None:
    with pytest.raises(ValueError, match="Predicted rank must"):
        exact_position_metrics((PositionObservation("r", rank, 1),))


@pytest.mark.parametrize("finish", [0, -1, True])
def test_invalid_actual_finish(finish: int) -> None:
    with pytest.raises(ValueError, match="Actual finish must"):
        exact_position_metrics((PositionObservation("r", 1, finish),))


def test_duplicate_predicted_ranks_are_not_silently_overwritten() -> None:
    with pytest.raises(ValueError, match="complete unique race permutation"):
        exact_position_metrics((PositionObservation("r", 1, 1), PositionObservation("r", 1, 2)))


def test_missing_predicted_rank_is_rejected() -> None:
    with pytest.raises(ValueError, match="complete unique race permutation"):
        exact_position_metrics((PositionObservation("r", 2, 1),))


def test_finish_outside_population_is_rejected() -> None:
    with pytest.raises(ValueError, match="Actual finish exceeds"):
        exact_position_metrics((PositionObservation("r", 1, 2),))


def test_aggregate_uses_position_denominators_not_average_fold_accuracy() -> None:
    first = parse_exact_position_metrics(
        {"race_count": 1, "support": [1, 1, 0, 0, 0], "hits": [1, 0, 0, 0, 0]}
    )
    second = parse_exact_position_metrics(
        {"race_count": 3, "support": [3, 1, 0, 0, 0], "hits": [0, 1, 0, 0, 0]}
    )
    result = combine_exact_position_metrics((first, second))
    assert result.race_count == 4
    assert result.support == (4, 2, 0, 0, 0)
    assert result.hits == (1, 1, 0, 0, 0)
    assert result.accuracy == (0.25, 0.5, None, None, None)


def test_empty_aggregate_has_no_supported_positions() -> None:
    result = combine_exact_position_metrics(())
    assert result.race_count == 0
    assert result.support == (0, 0, 0, 0, 0)
    assert result.hits == (0, 0, 0, 0, 0)
    assert result.accuracy == (None, None, None, None, None)


def test_parser_ignores_cached_ratios_and_recomputes_counts() -> None:
    result = parse_exact_position_metrics(
        {"race_count": 3, "support": (3, 0, 0, 0, 0), "hits": (1, 0, 0, 0, 0), "accuracy": [99]}
    )
    assert result.accuracy == (1 / 3, None, None, None, None)


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"race_count": True, "support": [0, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
        {"race_count": -1, "support": [0, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
        {"race_count": "1", "support": [1, 0, 0, 0, 0], "hits": [1, 0, 0, 0, 0]},
        {"race_count": 1, "support": None, "hits": [0, 0, 0, 0, 0]},
        {"race_count": 1, "support": [1], "hits": [0, 0, 0, 0, 0]},
        {"race_count": 1, "support": [1, 1, 1, 1, 1, 1], "hits": [0, 0, 0, 0, 0]},
        {"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": None},
        {"race_count": 1, "support": [True, 0, 0, 0, 0], "hits": [0, 0, 0, 0, 0]},
        {"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": [True, 0, 0, 0, 0]},
        {"race_count": 1, "support": [1, 0, 0, 0, 0], "hits": [2, 0, 0, 0, 0]},
        {"race_count": 1, "support": [2, 0, 0, 0, 0], "hits": [1, 0, 0, 0, 0]},
    ],
)
def test_parser_rejects_missing_or_invalid_count_evidence(payload: object) -> None:
    with pytest.raises(ValueError):
        parse_exact_position_metrics(payload)


def test_combiner_revalidates_hand_constructed_summary() -> None:
    invalid = ExactPositionMetrics(
        1, (1, 0, 0, 0, 0), (2, 0, 0, 0, 0), (2.0, None, None, None, None)
    )
    with pytest.raises(ValueError, match="denominator"):
        combine_exact_position_metrics((invalid,))
