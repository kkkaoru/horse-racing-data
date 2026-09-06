from __future__ import annotations

import numpy as np
import pytest

from timesfm_finish_position.prophet_features import build_entity_trend_features


def mean_forecaster(dates: np.ndarray, values: np.ndarray, target_dates: np.ndarray) -> np.ndarray:
    assert np.all(dates < "2024-01-01")
    return np.repeat(np.mean(values), len(target_dates))


def test_entity_trends_use_only_pre_year_monthly_aggregates() -> None:
    dates = np.asarray(
        ["20230101", "20230102", "20230201", "20230103", "20240101", "20240201"],
        dtype=np.str_,
    )
    entity = np.asarray(["a", "a", "a", "b", "a", "b"], dtype=np.str_)
    performance = np.asarray([0.2, 0.4, 0.6, 0.9, 0.0, 0.0], dtype=np.float64)
    result = build_entity_trend_features(
        race_dates=dates,
        entity_columns=(entity,),
        performance=performance,
        year=2024,
        forecaster=mean_forecaster,
        max_entities=1,
        minimum_history_rows=2,
    )
    assert result.target_indices.tolist() == [4, 5]
    assert result.selected_entities == (1,)
    assert result.values[:, 0].tolist() == pytest.approx([0.45, 0.525])


def test_entity_trends_validate_inputs_and_forecast_contract() -> None:
    dates = np.asarray(["20230101", "20240101"], dtype=np.str_)
    entity = np.asarray(["a", "a"], dtype=np.str_)
    performance = np.asarray([0.5, 0.0], dtype=np.float64)
    with pytest.raises(ValueError, match="must align"):
        build_entity_trend_features(
            race_dates=dates,
            entity_columns=(entity[:-1],),
            performance=performance,
            year=2024,
            forecaster=mean_forecaster,
        )
    with pytest.raises(ValueError, match="must be positive"):
        build_entity_trend_features(
            race_dates=dates,
            entity_columns=(entity,),
            performance=performance,
            year=2024,
            forecaster=mean_forecaster,
            max_entities=0,
        )

    def invalid_forecaster(
        _dates: np.ndarray, _values: np.ndarray, _targets: np.ndarray
    ) -> np.ndarray:
        return np.asarray([], dtype=np.float64)

    with pytest.raises(RuntimeError, match="invalid predictions"):
        build_entity_trend_features(
            race_dates=dates,
            entity_columns=(entity,),
            performance=performance,
            year=2024,
            forecaster=invalid_forecaster,
            minimum_history_rows=1,
        )
