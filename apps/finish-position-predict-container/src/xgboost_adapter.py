"""Load an XGBoost JSON model and adapt it to ``BoosterLike``.

I/O wrapper around the native XGBoost runtime — not unit-tested (it needs the
compiled library + a real model.json), exercised at deploy time per DEPLOY.md.
Feature rows are already float32-quantised by ``predict_lib.scorer`` per
docs/finish-position-accuracy/legacy/FINISH_POSITION_MODEL_V7_LINEAGE.md section 10.4.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Protocol, runtime_checkable


class XgboostBoosterLike(Protocol):
    def predict(
        self,
        data: object,
        *,
        iteration_range: tuple[int, int] | None = None,
    ) -> Iterable[float]: ...


@runtime_checkable
class XgboostBestIterationLike(Protocol):
    best_iteration: int | str | None


def _best_iteration_range(booster: object) -> tuple[int, int] | None:
    if not isinstance(booster, XgboostBestIterationLike):
        return None
    try:
        best_iteration = booster.best_iteration
    except AttributeError:
        return None
    if best_iteration is None:
        return None
    best_iteration_int = int(best_iteration)
    if best_iteration_int < 0:
        return None
    return (0, best_iteration_int + 1)


class XgboostBooster:
    """Adapt a loaded XGBoost Booster to the scorer's ``predict`` signature."""

    _booster: XgboostBoosterLike
    _iteration_range: tuple[int, int] | None

    def __init__(self, booster: XgboostBoosterLike) -> None:
        self._booster = booster
        self._iteration_range = _best_iteration_range(booster)

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        import xgboost

        data = xgboost.DMatrix(matrix)
        if self._iteration_range is None:
            scores = self._booster.predict(data)
        else:
            scores = self._booster.predict(data, iteration_range=self._iteration_range)
        return [float(score) for score in scores]


def load_xgboost_booster(model_path: str) -> XgboostBooster:
    """Load ``model.json`` (XGBoost booster.save_model) into a booster.

    Clears the booster's ``feature_names`` after load so predictions made
    against a positional DMatrix (built in ``predict_lib.scorer`` from the
    canonical metadata feature order) are not rejected by XGBoost's strict
    feature-name validation. The matrix column order already matches the
    metadata ``feature_names`` order from the same model artifact, so
    positional alignment is guaranteed; the booster's internal name list
    would otherwise force every caller to also build a named DMatrix.
    """
    import xgboost

    booster = xgboost.Booster()
    booster.load_model(model_path)
    booster.feature_names = None
    return XgboostBooster(booster)
