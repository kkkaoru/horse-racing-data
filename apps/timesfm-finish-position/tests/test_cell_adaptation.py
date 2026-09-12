from __future__ import annotations

import numpy as np
import pytest

from timesfm_finish_position.cell_adaptation import (
    build_entrant_history_scope,
    scope_strategies_for_cell,
    temporal_profiles_for_cell,
)


def test_profiles_are_customized_by_cell_dimensions() -> None:
    newcomer = temporal_profiles_for_cell(
        distance=1800, surface="dirt", condition_code="701", race_identity="none"
    )
    named = temporal_profiles_for_cell(
        distance=2000,
        surface="turf",
        condition_code="999",
        race_identity="name:チャレンジカップ",
    )
    sprint = temporal_profiles_for_cell(
        distance=1200, surface="dirt", condition_code="703", race_identity="none"
    )
    route = temporal_profiles_for_cell(
        distance=2400, surface="turf", condition_code="703", race_identity="none"
    )
    balanced = temporal_profiles_for_cell(
        distance=1800, surface="dirt", condition_code="703", race_identity="none"
    )

    assert [profile.name for profile in newcomer] == ["performance", "newcomer-pace"]
    assert [profile.name for profile in named] == [
        "named-open-complete",
        "named-open-finish",
        "performance-over-market",
        "performance-over-market-horse-normalized",
        "performance-over-market-rolling",
    ]
    assert [profile.name for profile in sprint] == [
        "performance",
        "sprint-dirt",
        "performance-over-market",
        "performance-over-market-horse-normalized",
        "performance-over-market-rolling",
    ]
    assert [profile.name for profile in route] == [
        "performance",
        "turf-route-finish",
        "performance-over-market",
        "performance-over-market-horse-normalized",
        "performance-over-market-rolling",
    ]
    assert [profile.name for profile in balanced] == [
        "performance",
        "balanced",
        "performance-over-market",
        "performance-over-market-horse-normalized",
        "performance-over-market-rolling",
    ]
    assert named[-2].horse_normalize is True
    assert named[-1].rolling_origin is True


def test_scope_expansion_is_customized_without_removing_entrant_history() -> None:
    assert scope_strategies_for_cell(
        distance=1200, surface="dirt", condition_code="703", race_identity="none"
    ) == ("entrant-history", "venue-surface-track-bias")
    assert scope_strategies_for_cell(
        distance=2000,
        surface="turf",
        condition_code="999",
        race_identity="name:チャレンジカップ",
    ) == ("entrant-history", "related-distance-track-bias")
    assert scope_strategies_for_cell(
        distance=1800, surface="dirt", condition_code="703", race_identity="none"
    ) == ("entrant-history", "venue-surface-track-bias")


def test_scope_contains_every_prior_entrant_race_and_optional_peers() -> None:
    horses = np.asarray(["h1", "h2", "h1", "h3", "h2", "h1", "h4"], dtype=np.str_)
    dates = np.asarray(
        ["20200101", "20200102", "20210101", "20210102", "20220101", "20240101", "20200103"],
        dtype=np.str_,
    )
    races = np.asarray(["c1", "x1", "x2", "x3", "c2", "eval", "peer"], dtype=np.str_)
    scope = build_entrant_history_scope(
        horse_ids=horses,
        race_dates=dates,
        race_ids=races,
        evaluation_indices=np.asarray([4, 5], dtype=np.int64),
        cutoff="20230101",
        history_start="20200101",
        target_cell_race_ids=frozenset({"c1", "c2"}),
        additional_training_indices=np.asarray([3, 6], dtype=np.int64),
    )

    assert scope.entrant_horse_ids == frozenset({"h1", "h2"})
    assert scope.entrant_history_indices.tolist() == [0, 1, 2, 4]
    assert scope.training_indices.tolist() == [0, 1, 2, 3, 4, 6]
    assert scope.target_cell_training_race_count == 2
    assert scope.cross_cell_training_race_count == 4
    assert scope.additional_training_row_count == 2


def test_scope_validates_alignment_dates_and_indices() -> None:
    horses = np.asarray(["h1"], dtype=np.str_)
    dates = np.asarray(["20200101"], dtype=np.str_)
    races = np.asarray(["r1"], dtype=np.str_)
    evaluation = np.asarray([0], dtype=np.int64)
    with pytest.raises(ValueError, match="must align"):
        build_entrant_history_scope(
            horse_ids=horses,
            race_dates=dates,
            race_ids=races[:0],
            evaluation_indices=evaluation,
            cutoff="20210101",
            history_start="20200101",
            target_cell_race_ids=frozenset({"r1"}),
        )
    with pytest.raises(ValueError, match="evaluation indices"):
        build_entrant_history_scope(
            horse_ids=horses,
            race_dates=dates,
            race_ids=races,
            evaluation_indices=np.asarray([1], dtype=np.int64),
            cutoff="20210101",
            history_start="20200101",
            target_cell_race_ids=frozenset({"r1"}),
        )
    with pytest.raises(ValueError, match="history_start"):
        build_entrant_history_scope(
            horse_ids=horses,
            race_dates=dates,
            race_ids=races,
            evaluation_indices=evaluation,
            cutoff="20210101",
            history_start="20210101",
            target_cell_race_ids=frozenset({"r1"}),
        )
    with pytest.raises(ValueError, match="additional training"):
        build_entrant_history_scope(
            horse_ids=horses,
            race_dates=dates,
            race_ids=races,
            evaluation_indices=evaluation,
            cutoff="20210101",
            history_start="20200101",
            target_cell_race_ids=frozenset({"r1"}),
            additional_training_indices=np.asarray([-1], dtype=np.int64),
        )
