"""Tests for the XGBoost runtime adapter."""

from __future__ import annotations

import sys
from collections.abc import Iterable
from pathlib import Path
from typing import final

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from xgboost_adapter import XgboostBooster


@final
class _BestIterationBooster:
    best_iteration: int
    calls: list[tuple[object, tuple[int, int] | None]]

    def __init__(self, best_iteration: int) -> None:
        self.best_iteration = best_iteration
        self.calls = []

    def predict(
        self,
        data: object,
        *,
        iteration_range: tuple[int, int] | None = None,
    ) -> Iterable[float]:
        self.calls.append((data, iteration_range))
        return [1.5, 2.5]


@final
class _NoBestIterationBooster:
    calls: list[tuple[object, tuple[int, int] | None]]

    def __init__(self) -> None:
        self.calls = []

    def predict(
        self,
        data: object,
        *,
        iteration_range: tuple[int, int] | None = None,
    ) -> Iterable[float]:
        self.calls.append((data, iteration_range))
        return [3.5]


def test_xgboost_booster_predict_uses_best_iteration_range() -> None:
    native = _BestIterationBooster(best_iteration=2)
    booster = XgboostBooster(native)

    scores = booster.predict([[0.1], [0.2]])

    assert scores == [1.5, 2.5]
    assert native.calls[0][1] == (0, 3)


def test_xgboost_booster_predict_omits_iteration_range_without_best_iteration() -> None:
    native = _NoBestIterationBooster()
    booster = XgboostBooster(native)

    scores = booster.predict([[0.1]])

    assert scores == [3.5]
    assert native.calls[0][1] is None
