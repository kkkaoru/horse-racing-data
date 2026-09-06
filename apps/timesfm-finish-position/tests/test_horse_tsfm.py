from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pytest

from timesfm_finish_position.domain import FloatArray
from timesfm_finish_position.horse_tsfm import (
    build_horse_year_queries,
    forecast_horse_year,
)


class FakeForecaster:
    @property
    def backend(self) -> str:
        return "fake"

    def predict(self, contexts: Sequence[FloatArray], *, horizon: int) -> tuple[FloatArray, ...]:
        return tuple(
            np.repeat(context[:, -1:], horizon, axis=1) + np.arange(1, horizon + 1)
            for context in contexts
        )


class OmittingForecaster(FakeForecaster):
    def predict(self, contexts: Sequence[FloatArray], *, horizon: int) -> tuple[FloatArray, ...]:
        return ()


class WrongShapeForecaster(FakeForecaster):
    def predict(self, contexts: Sequence[FloatArray], *, horizon: int) -> tuple[FloatArray, ...]:
        return tuple(np.zeros((context.shape[0], horizon + 1)) for context in contexts)


def _source() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    return (
        np.asarray(["h1", "h2", "h1", "h2", "h1", "h3", "h1"], dtype=np.str_),
        np.asarray(
            ["20230101", "20230101", "20240101", "20240101", "20240201", "20240201", "20240301"],
            dtype=np.str_,
        ),
        np.asarray(
            [
                [1.0, 10.0],
                [2.0, 20.0],
                [3.0, 30.0],
                [4.0, 40.0],
                [5.0, 50.0],
                [6.0, 60.0],
                [7.0, 70.0],
            ],
            dtype=np.float64,
        ),
    )


def test_build_and_forecast_horse_year_never_uses_target_year_outcomes() -> None:
    horses, dates, values = _source()
    queries = build_horse_year_queries(
        horse_ids=horses, race_dates=dates, history_values=values, year=2024
    )
    assert len(queries.contexts) == 2
    assert queries.contexts[0].tolist() == [[1.0], [10.0]]
    assert queries.all_target_indices.tolist() == [2, 3, 4, 5, 6]
    result = forecast_horse_year(
        queries, FakeForecaster(), fallback=np.asarray([0.5, 0.0], dtype=np.float64)
    )
    assert result.target_indices.tolist() == [2, 3, 4, 5, 6]
    assert result.history_available.tolist() == [True, True, True, False, True]
    assert result.values[0].tolist() == [2.0, 11.0]
    assert result.values[2].tolist() == [3.0, 12.0]
    assert result.values[3].tolist() == [0.5, 0.0]


def test_horse_year_queries_and_forecasts_validate_contracts() -> None:
    horses, dates, values = _source()
    with pytest.raises(ValueError, match="must align"):
        build_horse_year_queries(
            horse_ids=horses[:-1], race_dates=dates, history_values=values, year=2024
        )
    with pytest.raises(ValueError, match="must be positive"):
        build_horse_year_queries(
            horse_ids=horses, race_dates=dates, history_values=values, year=2024, max_history=0
        )
    with pytest.raises(ValueError, match="must be chronological"):
        build_horse_year_queries(
            horse_ids=horses,
            race_dates=dates[::-1].copy(),
            history_values=values,
            year=2024,
        )
    queries = build_horse_year_queries(
        horse_ids=horses, race_dates=dates, history_values=values, year=2024
    )
    with pytest.raises(ValueError, match="one value per variate"):
        forecast_horse_year(queries, FakeForecaster(), fallback=np.asarray([0.0]))
    with pytest.raises(RuntimeError, match="omitted a horse query"):
        forecast_horse_year(queries, OmittingForecaster(), fallback=np.asarray([0.0, 0.0]))
    with pytest.raises(RuntimeError, match="unexpected horse forecast shape"):
        forecast_horse_year(queries, WrongShapeForecaster(), fallback=np.asarray([0.0, 0.0]))
