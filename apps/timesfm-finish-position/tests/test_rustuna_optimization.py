from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from timesfm_finish_position.rustuna_optimization import (
    FoldForecastSurface,
    evaluate_surface,
    optimize_cell_surfaces,
)


def _surface(*, year: int = 2023) -> FoldForecastSurface:
    return FoldForecastSurface(
        profile="performance",
        strategy="entrant-history",
        year=year,
        value_columns=("performance_rating",),
        timesfm_components=np.asarray([[2.0], [-1.0], [2.0], [-1.0]], dtype=np.float64),
        market_scores=np.asarray([-1.0, 2.0, -1.0, 2.0], dtype=np.float64),
        race_ids=np.asarray(["r1", "r1", "r2", "r2"], dtype=np.str_),
        horse_ids=np.asarray(["h1", "h2", "h3", "h4"], dtype=np.str_),
        finish_positions=np.asarray([1, 2, 1, 2], dtype=np.int64),
        history_counts=np.asarray([3, 0, 3, 0], dtype=np.int64),
    )


def test_evaluate_surface_blends_cached_forecasts_without_rerunning_model() -> None:
    surface = _surface()
    temporal = evaluate_surface(
        surface, component_weights={"performance_rating": 1.0}, market_weight=0.0
    )
    market = evaluate_surface(
        surface, component_weights={"performance_rating": 0.0}, market_weight=1.0
    )

    assert temporal.race_count == 2
    assert temporal.delta_hits == (2, 0, 0, 0, 0)
    assert market.delta_hits == (0, 0, 0, 0, 0)


def test_minimum_history_gate_leaves_sparse_horses_on_market() -> None:
    gated = evaluate_surface(
        _surface(),
        component_weights={"performance_rating": 1.0},
        market_weight=0.0,
        minimum_history_count=5,
    )

    assert gated.delta_hits == (0, 0, 0, 0, 0)


def test_surface_and_optimization_validate_inputs() -> None:
    with pytest.raises(ValueError, match="must align"):
        FoldForecastSurface(
            profile="p",
            strategy="s",
            year=2023,
            value_columns=("performance_rating",),
            timesfm_components=np.ones((2, 1)),
            market_scores=np.ones(1),
            race_ids=np.asarray(["r1", "r1"], dtype=np.str_),
            horse_ids=np.asarray(["h1", "h2"], dtype=np.str_),
            finish_positions=np.asarray([1, 2], dtype=np.int64),
            history_counts=np.asarray([1, 1], dtype=np.int64),
        )
    with pytest.raises(ValueError, match="finite"):
        FoldForecastSurface(
            profile="p",
            strategy="s",
            year=2023,
            value_columns=("performance_rating",),
            timesfm_components=np.asarray([[np.nan]], dtype=np.float64),
            market_scores=np.ones(1),
            race_ids=np.asarray(["r1"], dtype=np.str_),
            horse_ids=np.asarray(["h1"], dtype=np.str_),
            finish_positions=np.asarray([1], dtype=np.int64),
            history_counts=np.asarray([1], dtype=np.int64),
        )
    with pytest.raises(ValueError, match="market_weight"):
        evaluate_surface(
            _surface(), component_weights={"performance_rating": 1.0}, market_weight=2.0
        )
    with pytest.raises(ValueError, match="minimum_history_count"):
        evaluate_surface(
            _surface(),
            component_weights={"performance_rating": 1.0},
            market_weight=0.5,
            minimum_history_count=0,
        )
    with pytest.raises(ValueError, match="n_trials"):
        optimize_cell_surfaces([_surface()], n_trials=0, seed=1, study_name="invalid")
    with pytest.raises(ValueError, match="at least one"):
        optimize_cell_surfaces([], n_trials=1, seed=1, study_name="empty")


def test_optimizer_can_select_exact_market_baseline() -> None:
    market_wins = replace(_surface(), finish_positions=np.asarray([2, 1, 2, 1], dtype=np.int64))

    result = optimize_cell_surfaces(
        [market_wins], n_trials=100, seed=20260912, study_name="market-baseline"
    )

    assert result.best_value == 0.0
    assert result.best_params["market_only"] is True


def test_rustuna_runs_many_cached_trials_and_resumes_sqlite(tmp_path: Path) -> None:
    storage = tmp_path / "cell.sqlite3"
    first = optimize_cell_surfaces(
        [_surface(year=2022), _surface(year=2023)],
        n_trials=100,
        seed=20260912,
        study_name="cell-1",
        storage_path=storage,
    )
    second = optimize_cell_surfaces(
        [_surface(year=2022), _surface(year=2023)],
        n_trials=50,
        seed=20260912,
        study_name="cell-1",
        storage_path=storage,
    )

    assert first.n_trials == 100
    assert first.trials_per_second > 0
    assert first.best_params["profile_scope"] == "performance__entrant-history"
    assert first.best_value > 0
    assert first.best_params.get("market_only") is not True
    assert first.feasible_timesfm_trial_count > 0
    assert first.improving_timesfm_trial_count > 0
    assert first.best_feasible_timesfm_value is not None
    assert first.best_feasible_timesfm_params is not None
    assert second.n_trials == 50
    assert second.best_value >= first.best_value
