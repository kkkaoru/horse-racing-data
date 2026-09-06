from __future__ import annotations

import numpy as np
import pytest

from timesfm_finish_position.prophet_lookup import build_prophet_lookup_rows


def _forecaster(dates: np.ndarray, values: np.ndarray, target_dates: np.ndarray) -> np.ndarray:
    del dates, target_dates
    return np.full(365, float(np.mean(values)), dtype=np.float64)


def test_build_lookup_uses_pre_year_history_and_daily_fallbacks() -> None:
    result = build_prophet_lookup_rows(
        race_dates=np.asarray(["20240101", "20240201", "20260101"], dtype=np.str_),
        entity_columns=(
            np.asarray(["30", "30", "99"], dtype=np.str_),
            np.asarray(["j1", "j1", "future"], dtype=np.str_),
        ),
        entity_types=("venue", "jockey"),
        performance=np.asarray([0.2, 0.4, 99.0], dtype=np.float64),
        year=2025,
        forecaster=_forecaster,
        categories=("nar",),
        max_entities=1,
        minimum_history_rows=2,
    )
    assert result.selected_entities == (1, 1)
    assert len(result.yhat) == 4 * 365
    assert set(result.entity_code.tolist()) == {"__fallback__", "30", "j1"}
    assert len(set(result.forecast_date.tolist())) == 365
    assert min(result.forecast_date) == "20250101"
    assert max(result.forecast_date) == "20251231"
    assert np.allclose(result.yhat, 0.3)


def test_build_lookup_validates_alignment_limits_history_and_forecasts() -> None:
    dates = np.asarray(["20240101"], dtype=np.str_)
    entities = np.asarray(["30"], dtype=np.str_)
    values = np.asarray([0.2], dtype=np.float64)
    with pytest.raises(ValueError, match="columns and types"):
        build_prophet_lookup_rows(
            race_dates=dates,
            entity_columns=(entities,),
            entity_types=(),
            performance=values,
            year=2025,
            forecaster=_forecaster,
        )
    with pytest.raises(ValueError, match="source columns"):
        build_prophet_lookup_rows(
            race_dates=dates,
            entity_columns=(np.asarray([], dtype=np.str_),),
            entity_types=("venue",),
            performance=values,
            year=2025,
            forecaster=_forecaster,
        )
    with pytest.raises(ValueError, match="limits and categories"):
        build_prophet_lookup_rows(
            race_dates=dates,
            entity_columns=(entities,),
            entity_types=("venue",),
            performance=values,
            year=2025,
            forecaster=_forecaster,
            max_entities=0,
        )
    with pytest.raises(ValueError, match="pre-year history"):
        build_prophet_lookup_rows(
            race_dates=np.asarray(["20250101"], dtype=np.str_),
            entity_columns=(entities,),
            entity_types=("venue",),
            performance=values,
            year=2025,
            forecaster=_forecaster,
        )

    def invalid_forecast(
        dates: np.ndarray, history: np.ndarray, target_dates: np.ndarray
    ) -> np.ndarray:
        del dates, history, target_dates
        return np.asarray([np.nan], dtype=np.float64)

    with pytest.raises(RuntimeError, match="invalid predictions"):
        build_prophet_lookup_rows(
            race_dates=dates,
            entity_columns=(entities,),
            entity_types=("venue",),
            performance=values,
            year=2025,
            forecaster=invalid_forecast,
            minimum_history_rows=1,
        )
