from __future__ import annotations

import pytest

from timesfm_finish_position.prophet_policy_optimization import (
    LinearRaceScores,
    LinearRunnerScore,
    optimize_linear_race_weights,
    select_optimal_cell_weight,
)


def test_selects_top1_improvement_before_larger_top2_gain() -> None:
    selected = select_optimal_cell_weight(
        {
            0.0: (10, 20, 30, 40, 50),
            0.1: (11, 19, 29, 39, 49),
            0.2: (10, 25, 35, 45, 55),
        },
        has_support=True,
        default_weight=0.05,
    )

    assert selected.enabled is True
    assert selected.weight == 0.1
    assert selected.selected_weight == 0.1
    assert selected.baseline_hits == (10, 20, 30, 40, 50)
    assert selected.selected_hits == (11, 19, 29, 39, 49)


def test_uses_top2_then_smallest_weight_to_break_ties() -> None:
    selected = select_optimal_cell_weight(
        {
            0.0: (10, 20, 30, 40, 50),
            0.1: (10, 21, 31, 41, 51),
            0.2: (10, 21, 31, 41, 51),
        },
        has_support=True,
        default_weight=0.05,
    )

    assert selected.enabled is True
    assert selected.weight == 0.1
    assert selected.selected_weight == 0.1


def test_disables_observed_cell_when_no_positive_weight_improves() -> None:
    selected = select_optimal_cell_weight(
        {
            0.0: (10, 20, 30, 40, 50),
            0.1: (9, 21, 31, 41, 51),
        },
        has_support=True,
        default_weight=0.05,
    )

    assert selected.enabled is False
    assert selected.weight == 0.05
    assert selected.selected_weight == 0.0
    assert selected.selected_hits == (10, 20, 30, 40, 50)


def test_keeps_no_support_cell_on_with_default_weight() -> None:
    selected = select_optimal_cell_weight(
        {0.0: (0, 0, 0, 0, 0), 0.1: (0, 0, 0, 0, 0)},
        has_support=False,
        default_weight=0.05,
    )

    assert selected.enabled is True
    assert selected.weight == 0.05
    assert selected.selected_weight is None
    assert selected.selected_hits == (0, 0, 0, 0, 0)


def test_rejects_missing_weight_zero_baseline() -> None:
    with pytest.raises(ValueError, match="weight-zero baseline"):
        select_optimal_cell_weight(
            {0.1: (1, 2, 3, 4, 5)},
            has_support=True,
            default_weight=0.05,
        )


def test_rejects_non_monotonic_topk_hits() -> None:
    with pytest.raises(ValueError, match="monotonically non-decreasing"):
        select_optimal_cell_weight(
            {0.0: (1, 2, 1, 4, 5)},
            has_support=True,
            default_weight=0.05,
        )


def test_rejects_invalid_default_weight() -> None:
    with pytest.raises(ValueError, match="Default weight"):
        select_optimal_cell_weight(
            {0.0: (1, 2, 3, 4, 5)},
            has_support=True,
            default_weight=0.0,
        )


def test_crossing_sweep_selects_stable_interval_without_grid_search() -> None:
    selected = optimize_linear_race_weights(
        (
            LinearRaceScores(
                winners=(LinearRunnerScore("winner", 0.0, 1.0),),
                competitors=(LinearRunnerScore("other", 0.5, 0.0),),
            ),
        ),
        default_weight=0.05,
    )

    assert selected.enabled is True
    assert selected.weight == 0.75
    assert selected.selected_weight == 0.75
    assert selected.baseline_hits == (0, 1, 1, 1, 1)
    assert selected.selected_hits == (1, 1, 1, 1, 1)


def test_crossing_sweep_disables_cell_without_improving_interval() -> None:
    selected = optimize_linear_race_weights(
        (
            LinearRaceScores(
                winners=(LinearRunnerScore("winner", 1.0, 1.0),),
                competitors=(LinearRunnerScore("other", 0.5, 0.0),),
            ),
        ),
        default_weight=0.05,
    )

    assert selected.enabled is False
    assert selected.weight == 0.05
    assert selected.selected_weight == 0.0
    assert selected.baseline_hits == (1, 1, 1, 1, 1)
    assert selected.selected_hits == (1, 1, 1, 1, 1)


def test_crossing_sweep_handles_multiple_official_winners() -> None:
    selected = optimize_linear_race_weights(
        (
            LinearRaceScores(
                winners=(
                    LinearRunnerScore("winner-1", 0.0, 1.0),
                    LinearRunnerScore("winner-2", -0.2, 2.0),
                ),
                competitors=(LinearRunnerScore("other", 0.5, 0.0),),
            ),
        ),
        default_weight=0.05,
    )

    assert selected.enabled is True
    assert selected.weight == pytest.approx(0.425)
    assert selected.selected_weight == pytest.approx(0.425)
    assert selected.baseline_hits == (0, 1, 1, 1, 1)
    assert selected.selected_hits == (1, 1, 1, 1, 1)


def test_crossing_sweep_keeps_empty_cell_on_at_default_weight() -> None:
    selected = optimize_linear_race_weights((), default_weight=0.05)

    assert selected.enabled is True
    assert selected.weight == 0.05
    assert selected.selected_weight is None
    assert selected.baseline_hits == (0, 0, 0, 0, 0)
    assert selected.selected_hits == (0, 0, 0, 0, 0)
